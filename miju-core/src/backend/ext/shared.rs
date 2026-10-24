use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Waker};

use crate::backend::*;
use crate::task::Task;
use crate::worker::context::WorkerContext;
use dashmap::DashMap;
use futures_core::ready;
use futures_sink::Sink;
use futures_util::lock::Mutex;
use futures_util::{FutureExt, SinkExt, StreamExt};

#[derive(Debug)]
struct Inner<B> {
    backend: Mutex<B>,
    wakers: DashMap<usize, Waker>,
    next_key: std::sync::atomic::AtomicUsize,
}

/// Cloneable handles that share one backend behind an asynchronous mutex.
///
/// Each handle owns its codec clone and pending-send buffer. Sink `poll_ready`,
/// `poll_flush`, and `poll_close` drain that buffer before delegating to the
/// underlying backend.
///
/// Calling [`BackendConfig::config`] panics. Middleware construction also
/// panics if another handle currently holds the backend lock.
#[derive(Debug)]
pub struct Shared<B: WireFormatBackend> {
    inner: Arc<Inner<B>>,
    waker_key: usize,
    codec: B::Codec,
    sink: VecDeque<Task<B::Compact>>,
}

impl<B> Clone for Shared<B>
where
    B::Codec: Clone,
    B: WireFormatBackend,
{
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        let waker_key = inner
            .next_key
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            inner,
            waker_key,
            codec: self.codec.clone(),
            sink: Default::default(),
        }
    }
}

impl<B: WireFormatBackend> Shared<B>
where
    B::Codec: Clone,
{
    /// Build a new Sharable backend
    pub fn new(backend: B) -> Self {
        let codec = backend.codec().clone();
        Self {
            inner: Inner {
                backend: Mutex::new(backend),
                next_key: AtomicUsize::new(0),
                wakers: Default::default(),
            }
            .into(),
            waker_key: 0,
            codec,
            sink: VecDeque::new(),
        }
    }
}

impl<B: WireFormatBackend> Shared<B> {
    fn poll_drain_buffer(
        sink: &mut VecDeque<Task<B::Compact>>,
        backend: &mut B,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), B::Error>>
    where
        B: Sink<Task<B::Compact>> + Unpin,
    {
        while !sink.is_empty() {
            ready!(backend.poll_ready_unpin(cx))?;
            backend.start_send_unpin(sink.pop_front().unwrap())?;
        }
        Poll::Ready(Ok(()))
    }

    fn register_waker(inner: &Inner<B>, key: usize, cx: &Context<'_>) {
        let res = &inner.wakers;
        res.insert(key, cx.waker().clone());
    }

    fn wake_others(inner: &Inner<B>, except: usize) {
        let wakers = &inner.wakers;
        wakers.retain(|key, waker| {
            if *key != except {
                waker.wake_by_ref();
            }
            true
        });
    }
}

impl<B> Backend for Shared<B>
where
    B: Backend + WireFormatBackend,
{
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(mut guard) = self.inner.backend.try_lock() {
            let result = guard.poll_ready(cx, worker);
            drop(guard);

            if result.is_ready() {
                Self::wake_others(&self.inner, self.waker_key);
            }
            result
        } else {
            Self::register_waker(&self.inner, self.waker_key, cx);
            Poll::Pending
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let inner = &self.inner;

        if let Some(mut guard) = inner.backend.try_lock() {
            let result = guard.poll_next(cx, worker);
            drop(guard);
            Self::wake_others(inner, self.waker_key);
            result
        } else {
            Self::register_waker(inner, self.waker_key, cx);
            Poll::Pending
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let inner = self.inner.as_ref();

        if let Some(mut guard) = inner.backend.try_lock() {
            let result = guard.poll_close(cx, worker);
            drop(guard);
            if result.is_ready() {
                inner.wakers.remove(&self.waker_key);
                Self::wake_others(inner, self.waker_key);
            } else {
                Self::wake_others(inner, self.waker_key);
            }
            result
        } else {
            Self::register_waker(inner, self.waker_key, cx);
            Poll::Pending
        }
    }
}

impl<B: WireFormatBackend> WireFormatBackend for Shared<B> {
    type Codec = B::Codec;

    type Compact = B::Compact;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<B, Err> Sink<Task<B::Compact>> for Shared<B>
where
    B: Backend<Error = Err> + Sink<Task<B::Compact>, Error = Err> + Unpin,
    B: WireFormatBackend,
    B::Codec: Unpin,
    B::Compact: Unpin,
{
    type Error = Err;
    fn start_send(self: Pin<&mut Self>, item: Task<B::Compact>) -> Result<(), Self::Error> {
        let this = self.get_mut();

        if let Some(mut guard) = this.inner.backend.try_lock() {
            let result = guard.start_send_unpin(item);
            drop(guard);
            if result.is_ok() {
                Self::wake_others(&this.inner, this.waker_key);
            }
            result
        } else {
            this.sink.push_back(item);
            Ok(())
        }
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut guard = ready!(this.inner.backend.lock().poll_unpin(cx));
        ready!(Self::poll_drain_buffer(&mut this.sink, &mut guard, cx))?;
        guard.poll_ready_unpin(cx)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut guard = ready!(this.inner.backend.lock().poll_unpin(cx));
        ready!(Self::poll_drain_buffer(&mut this.sink, &mut guard, cx))?;
        guard.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut guard = ready!(this.inner.backend.lock().poll_unpin(cx));
        ready!(Self::poll_drain_buffer(&mut this.sink, &mut guard, cx))?;
        guard.poll_close_unpin(cx)
    }
}

// delegate_deref!(Shared<B>, inner);

impl<B> BackendConfig for Shared<B>
where
    B: BackendConfig + WireFormatBackend,
{
    type Id = B::Id;
    type Args = B::Args;
    type Kind = B::Kind;
    type Config = B::Config;
    type Layer = B::Layer;
    fn config(&self) -> &Self::Config {
        unreachable!("Dont call config on shared")
    }
    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        self.inner.backend.try_lock().unwrap().middleware(worker)
    }
}

impl<B> FetchById for Shared<B>
where
    B: FetchById,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn fetch_by_id(
        &mut self,
        task_id: &crate::task::task_id::TaskId,
    ) -> Result<Option<B::Task>, Self::Error> {
        self.inner.backend.lock().await.fetch_by_id(task_id).await
    }
}
impl<B> Update for Shared<B>
where
    B: Update + Send,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn update(&mut self, task: Self::Task) -> Result<(), Self::Error> {
        self.inner.backend.lock().await.update(task).await
    }
}
impl<B> Reschedule for Shared<B>
where
    B: Reschedule,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn reschedule(
        &mut self,
        task: Self::Task,
        wait: std::time::Duration,
    ) -> Result<(), Self::Error> {
        self.inner.backend.lock().await.reschedule(task, wait).await
    }
}
impl<B> Vacuum for Shared<B>
where
    B: Vacuum,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        self.inner.backend.lock().await.vacuum().await
    }
    async fn vacuum_before(&mut self, duration: Duration) -> Result<usize, Self::Error> {
        self.inner
            .backend
            .lock()
            .await
            .vacuum_before(duration)
            .await
    }
}
impl<B> ResumeById for Shared<B>
where
    B: ResumeById,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn resume_by_id(&mut self, id: TaskId) -> Result<bool, Self::Error> {
        self.inner.backend.lock().await.resume_by_id(id).await
    }
}
impl<B> ResumeAbandoned for Shared<B>
where
    B: ResumeAbandoned,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn resume_abandoned(&mut self) -> Result<usize, Self::Error> {
        self.inner.backend.lock().await.resume_abandoned().await
    }
}
impl<B> RegisterWorker for Shared<B>
where
    B: RegisterWorker,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn register_worker(&mut self, worker_id: String) -> Result<(), Self::Error> {
        self.inner
            .backend
            .lock()
            .await
            .register_worker(worker_id)
            .await
    }
}
impl<Output, B> WaitForCompletion<Output> for Shared<B>
where
    B: WaitForCompletion<Output> + Sync + 'static,
    Output: Send + 'static,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Sync,
    B::Codec: Send,
    B::Compact: Send + Sync,
{
    type ResultStream =
        futures_core::stream::BoxStream<'static, Result<TaskResult<Output>, Self::Error>>;
    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let inner = self.inner.clone();
        let task_ids: Vec<_> = task_ids.into_iter().collect();
        futures_util::stream::once(async move {
            let mut backend = inner.backend.lock().await;
            backend.wait_for(task_ids)
        })
        .flatten()
        .boxed()
    }
    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Output>>, Self::Error> {
        self.inner.backend.lock().await.check_status(task_ids).await
    }
}

impl<B: WireFormatBackend> Drop for Shared<B> {
    fn drop(&mut self) {
        if let Some((_key, waker)) = self.inner.wakers.remove(&self.waker_key) {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        codec::Codec,
        custom::{BackendBuilder, CustomBackend},
        dequeue::VecDequeBackend,
        ext::{BackendExt, with_codec::WithCodec},
    };
    use futures_channel::mpsc;
    use futures_util::stream::{self, Empty};
    use futures_util::task::noop_waker_ref;
    use std::convert::Infallible;

    #[derive(Clone)]
    struct IdentityCodec;
    impl Codec<u32> for IdentityCodec {
        type Compact = u32;
        type Error = Infallible;
        fn encode(&self, value: &u32) -> Result<u32, Infallible> {
            Ok(*value)
        }
        fn decode(&self, value: &u32) -> Result<u32, Infallible> {
            Ok(*value)
        }
    }

    #[test]
    fn contended_send_buffer_is_drained_by_readiness() {
        let mut shared = VecDequeBackend::<u32>::new()
            .with_codec(IdentityCodec)
            .shared();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            shared.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let inner = shared.inner.clone();
        let guard = inner.backend.try_lock().unwrap();
        shared.start_send_unpin(Task::new(7)).unwrap();
        assert_eq!(shared.sink.len(), 1);
        drop(guard);
        assert!(matches!(
            shared.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(shared.sink.is_empty());
        let Poll::Ready(Some(Ok(task))) =
            shared.poll_next(&mut cx, &WorkerContext::new("shared-buffer"))
        else {
            panic!("missing buffered task")
        };
        assert_eq!(task.args, 7);
    }

    fn assert_contended_send_is_completed(close: bool) {
        let mut receiver = VecDequeBackend::<u32>::new();
        let mut shared = receiver.clone().with_codec(IdentityCodec).shared();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            shared.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let inner = shared.inner.clone();
        let guard = inner.backend.try_lock().unwrap();
        shared.start_send_unpin(Task::new(7)).unwrap();
        drop(guard);
        let completion = poll_completion(&mut shared, &mut cx, close);
        assert!(matches!(completion, Poll::Ready(Ok(()))));
        drop(shared);
        let worker = WorkerContext::new("shared-completion");
        let Poll::Ready(Some(Ok(task))) = receiver.poll_next(&mut cx, &worker) else {
            panic!("successfully completed send lost its buffered task")
        };
        assert_eq!(task.args, 7);
        assert!(matches!(
            receiver.poll_next(&mut cx, &worker),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn contended_send_is_delivered_before_flush_completes() {
        assert_contended_send_is_completed(false);
    }

    #[test]
    fn contended_send_is_delivered_before_close_completes() {
        assert_contended_send_is_completed(true);
    }

    type ChannelBackend = WithCodec<
        CustomBackend<
            u32,
            Option<mpsc::Sender<Task<u32>>>,
            Empty<Result<Option<Task<u32>>, mpsc::SendError>>,
            mpsc::Sender<Task<u32>>,
        >,
        IdentityCodec,
    >;

    fn channel_backend() -> (Shared<ChannelBackend>, mpsc::Receiver<Task<u32>>) {
        let (sender, receiver) = mpsc::channel(0);
        let backend = BackendBuilder::new()
            .database(Some(sender))
            .fetcher(|_, _, _| stream::empty())
            .sink(|sender, _| sender.take().unwrap())
            .build()
            .unwrap()
            .with_codec(IdentityCodec)
            .shared();
        (backend, receiver)
    }

    fn poll_completion<S: Sink<Task<u32>> + Unpin>(
        sink: &mut S,
        cx: &mut Context<'_>,
        close: bool,
    ) -> Poll<Result<(), S::Error>> {
        if close {
            sink.poll_close_unpin(cx)
        } else {
            sink.poll_flush_unpin(cx)
        }
    }

    #[test]
    fn buffered_completion_respects_backpressure_and_preserves_order() {
        for close in [false, true] {
            let (mut shared, mut receiver) = channel_backend();
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(matches!(
                shared.poll_ready_unpin(&mut cx),
                Poll::Ready(Ok(()))
            ));
            let inner = shared.inner.clone();
            let mut guard = inner.backend.try_lock().unwrap();
            // Another sender fills the channel after this handle becomes ready.
            assert!(matches!(
                guard.poll_ready_unpin(&mut cx),
                Poll::Ready(Ok(()))
            ));
            guard.start_send_unpin(Task::new(1)).unwrap();
            shared.start_send_unpin(Task::new(7)).unwrap();
            drop(guard);

            assert!(poll_completion(&mut shared, &mut cx, close).is_pending());
            let Poll::Ready(Some(first)) = receiver.poll_next_unpin(&mut cx) else {
                panic!("missing first task")
            };
            assert_eq!(first.args, 1);

            let completion = poll_completion(&mut shared, &mut cx, close);
            if close {
                // Closing must not wait for capacity to accept an additional task.
                assert!(matches!(completion, Poll::Ready(Ok(()))));
            } else {
                assert!(completion.is_pending());
            }
            let Poll::Ready(Some(second)) = receiver.poll_next_unpin(&mut cx) else {
                panic!("missing buffered task")
            };
            assert_eq!(second.args, 7);
            if !close {
                assert!(matches!(
                    shared.poll_flush_unpin(&mut cx),
                    Poll::Ready(Ok(()))
                ));
            }
            drop(shared);
            drop(inner);
            assert!(matches!(
                receiver.poll_next_unpin(&mut cx),
                Poll::Ready(None)
            ));
        }
    }

    #[test]
    fn buffered_completion_propagates_readiness_errors() {
        for close in [false, true] {
            let (mut shared, receiver) = channel_backend();
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(matches!(
                shared.poll_ready_unpin(&mut cx),
                Poll::Ready(Ok(()))
            ));
            let inner = shared.inner.clone();
            let guard = inner.backend.try_lock().unwrap();
            shared.start_send_unpin(Task::new(7)).unwrap();
            drop(guard);
            drop(receiver);
            assert!(matches!(
                poll_completion(&mut shared, &mut cx, close),
                Poll::Ready(Err(_))
            ));
        }
    }

    #[test]
    fn empty_completion_does_not_require_send_readiness() {
        for close in [false, true] {
            let (mut shared, receiver) = channel_backend();
            drop(receiver);
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(matches!(
                poll_completion(&mut shared, &mut cx, close),
                Poll::Ready(Ok(()))
            ));
        }
    }

    #[test]
    fn first_clone_reuses_waker_key_and_removes_original_registration_on_drop() {
        let shared = VecDequeBackend::<u32>::new()
            .with_codec(IdentityCodec)
            .shared();
        let clone = shared.clone();
        assert_eq!(shared.waker_key, clone.waker_key);
        let cx = Context::from_waker(noop_waker_ref());
        Shared::register_waker(&shared.inner, shared.waker_key, &cx);
        Shared::register_waker(&clone.inner, clone.waker_key, &cx);
        assert_eq!(shared.inner.wakers.len(), 1);
        drop(clone);
        assert!(shared.inner.wakers.is_empty());
    }

    #[test]
    fn configuration_and_contended_middleware_panic() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let mut shared = VecDequeBackend::<u32>::new()
            .with_codec(IdentityCodec)
            .shared();
        assert!(catch_unwind(AssertUnwindSafe(|| shared.config())).is_err());
        let inner = shared.inner.clone();
        let _guard = inner.backend.try_lock().unwrap();
        assert!(
            catch_unwind(AssertUnwindSafe(
                || shared.middleware(&mut WorkerContext::new("shared-config"))
            ))
            .is_err()
        );
    }
}
