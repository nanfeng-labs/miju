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
use futures_util::lock::{Mutex, MutexGuard};
use futures_util::{SinkExt, StreamExt};

#[derive(Debug)]
struct Inner<B> {
    backend: Mutex<B>,
    wakers: DashMap<usize, Waker>,
    next_key: std::sync::atomic::AtomicUsize,
}

impl<B> Inner<B> {
    fn wake_all(&self) {
        let keys: Vec<_> = self.wakers.iter().map(|entry| *entry.key()).collect();
        // Consume the batch before callbacks can reenter and notify again.
        let wakers: Vec<_> = keys
            .into_iter()
            .filter_map(|key| self.wakers.remove(&key).map(|(_, waker)| waker))
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }
}

// Declare before the backend guard, initialize only after acquiring the lock.
struct NotifyOnUnlock<'a, B>(&'a Inner<B>);

impl<B> Drop for NotifyOnUnlock<'_, B> {
    fn drop(&mut self) {
        self.0.wake_all();
    }
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
                next_key: AtomicUsize::new(1),
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

    fn poll_lock<'a>(inner: &'a Inner<B>, key: usize, cx: &Context<'_>) -> Poll<MutexGuard<'a, B>> {
        if let Some(guard) = inner.backend.try_lock() {
            inner.wakers.remove(&key);
            return Poll::Ready(guard);
        }
        Self::register_waker(inner, key, cx);
        // An unlock may have happened before registration; check again.
        if let Some(guard) = inner.backend.try_lock() {
            inner.wakers.remove(&key);
            Poll::Ready(guard)
        } else {
            Poll::Pending
        }
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
        let _notify;
        let mut guard = ready!(Self::poll_lock(&self.inner, self.waker_key, cx));
        _notify = NotifyOnUnlock(&self.inner);
        guard.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let _notify;
        let mut guard = ready!(Self::poll_lock(&self.inner, self.waker_key, cx));
        _notify = NotifyOnUnlock(&self.inner);
        guard.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let _notify;
        let mut guard = ready!(Self::poll_lock(&self.inner, self.waker_key, cx));
        _notify = NotifyOnUnlock(&self.inner);
        guard.poll_close(cx, worker)
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

        let _notify;
        if let Some(mut guard) = this.inner.backend.try_lock() {
            _notify = NotifyOnUnlock(&this.inner);
            this.inner.wakers.remove(&this.waker_key);
            guard.start_send_unpin(item)
        } else {
            this.sink.push_back(item);
            Ok(())
        }
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _notify;
        let mut guard = ready!(Self::poll_lock(&this.inner, this.waker_key, cx));
        _notify = NotifyOnUnlock(&this.inner);
        ready!(Self::poll_drain_buffer(&mut this.sink, &mut guard, cx))?;
        guard.poll_ready_unpin(cx)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _notify;
        let mut guard = ready!(Self::poll_lock(&this.inner, this.waker_key, cx));
        _notify = NotifyOnUnlock(&this.inner);
        ready!(Self::poll_drain_buffer(&mut this.sink, &mut guard, cx))?;
        guard.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _notify;
        let mut guard = ready!(Self::poll_lock(&this.inner, this.waker_key, cx));
        _notify = NotifyOnUnlock(&this.inner);
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
        let _notify;
        let mut backend = self.inner.backend.try_lock().unwrap();
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.middleware(worker)
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.fetch_by_id(task_id).await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.update(task).await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.reschedule(task, wait).await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.vacuum().await
    }
    async fn vacuum_before(&mut self, duration: Duration) -> Result<usize, Self::Error> {
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.vacuum_before(duration).await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.resume_by_id(id).await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.resume_abandoned().await
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
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.register_worker(worker_id).await
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
            let _notify;
            let mut backend = inner.backend.lock().await;
            _notify = NotifyOnUnlock(&inner);
            backend.wait_for(task_ids)
        })
        .flatten()
        .boxed()
    }
    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Output>>, Self::Error> {
        let _notify;
        let mut backend = self.inner.backend.lock().await;
        _notify = NotifyOnUnlock(&self.inner);
        self.inner.wakers.remove(&self.waker_key);
        backend.check_status(task_ids).await
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
        dequeue::{VecDequeBackend, VecDequeError},
        ext::{BackendExt, with_codec::WithCodec},
    };
    use futures_channel::{mpsc, oneshot};
    use futures_util::stream::{self, Empty};
    use futures_util::task::noop_waker_ref;
    use std::convert::Infallible;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::Ordering;

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
        assert!(receiver.poll_next(&mut cx, &worker).is_pending());
        assert!(matches!(
            receiver.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        receiver.start_send_unpin(Task::new(8)).unwrap();
        let Poll::Ready(Some(Ok(task))) = receiver.poll_next(&mut cx, &worker) else {
            panic!("the live receiver must accept a later task")
        };
        assert_eq!(task.args, 8);
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
    fn first_clone_has_an_independent_wait_registration() {
        let shared = VecDequeBackend::<u32>::new()
            .with_codec(IdentityCodec)
            .shared();
        let clone = shared.clone();
        assert_ne!(shared.waker_key, clone.waker_key);
        let cx = Context::from_waker(noop_waker_ref());
        Shared::register_waker(&shared.inner, shared.waker_key, &cx);
        Shared::register_waker(&clone.inner, clone.waker_key, &cx);
        assert_eq!(shared.inner.wakers.len(), 2);
        drop(clone);
        assert!(shared.inner.wakers.contains_key(&shared.waker_key));
    }

    #[test]
    fn configuration_and_contended_middleware_panic() {
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

    // The queue is real. The one-shot stands in for a database lookup awaiting I/O.
    struct LookupBackend {
        queue: VecDequeBackend<u32>,
        lookup_complete: Option<oneshot::Receiver<()>>,
        on_wait_for: Option<Box<dyn FnMut() + Send + Sync>>,
    }
    impl WireFormatBackend for LookupBackend {
        type Codec = IdentityCodec;
        type Compact = u32;
        fn codec(&self) -> &IdentityCodec {
            &IdentityCodec
        }
    }
    impl Backend for LookupBackend {
        type Task = Task<u32>;
        type Error = VecDequeError;
        fn poll_ready(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            Backend::poll_ready(&mut self.queue, cx, worker)
        }
        fn poll_next(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
            match Backend::poll_ready(&mut self.queue, cx, worker) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(())) => self.queue.poll_next(cx, worker),
            }
        }
        fn poll_close(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            Backend::poll_close(&mut self.queue, cx, worker)
        }
    }
    impl Sink<Task<u32>> for LookupBackend {
        type Error = VecDequeError;
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.get_mut().queue.poll_ready_unpin(cx)
        }
        fn start_send(self: Pin<&mut Self>, item: Task<u32>) -> Result<(), Self::Error> {
            self.get_mut().queue.start_send_unpin(item)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.get_mut().queue.poll_flush_unpin(cx)
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.get_mut().queue.poll_close_unpin(cx)
        }
    }
    impl FetchById for LookupBackend {
        async fn fetch_by_id(&mut self, _: &TaskId) -> Result<Option<Task<u32>>, Self::Error> {
            self.gate().await?;
            Ok(None)
        }
    }

    impl LookupBackend {
        async fn gate(&mut self) -> Result<(), VecDequeError> {
            self.lookup_complete
                .take()
                .unwrap()
                .await
                .map_err(|e| VecDequeError::PollError(Arc::new(e.into())))
        }
    }
    impl Update for LookupBackend {
        async fn update(&mut self, _: Task<u32>) -> Result<(), VecDequeError> {
            self.gate().await
        }
    }
    impl Reschedule for LookupBackend {
        async fn reschedule(
            &mut self,
            _: Task<u32>,
            _: std::time::Duration,
        ) -> Result<(), VecDequeError> {
            self.gate().await
        }
    }
    impl Vacuum for LookupBackend {
        async fn vacuum(&mut self) -> Result<usize, VecDequeError> {
            self.gate().await?;
            Ok(0)
        }
        async fn vacuum_before(&mut self, _: std::time::Duration) -> Result<usize, VecDequeError> {
            self.gate().await?;
            Ok(0)
        }
    }
    impl ResumeById for LookupBackend {
        async fn resume_by_id(&mut self, _: TaskId) -> Result<bool, VecDequeError> {
            self.gate().await?;
            Ok(false)
        }
    }
    impl ResumeAbandoned for LookupBackend {
        async fn resume_abandoned(&mut self) -> Result<usize, VecDequeError> {
            self.gate().await?;
            Ok(0)
        }
    }
    impl RegisterWorker for LookupBackend {
        async fn register_worker(&mut self, _: String) -> Result<(), VecDequeError> {
            self.gate().await
        }
    }
    impl WaitForCompletion<u32> for LookupBackend {
        type ResultStream = futures_util::stream::Empty<Result<TaskResult<u32>, VecDequeError>>;
        fn wait_for(&mut self, _: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
            if let Some(mut callback) = self.on_wait_for.take() {
                callback();
            }
            futures_util::stream::empty()
        }
        async fn check_status(
            &mut self,
            _: impl IntoIterator<Item = TaskId> + Send,
        ) -> Result<Vec<TaskResult<u32>>, VecDequeError> {
            self.gate().await?;
            Ok(vec![])
        }
    }
    fn management<'a>(
        s: &'a mut Shared<LookupBackend>,
        id: &'a TaskId,
        op: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), VecDequeError>> + 'a>> {
        Box::pin(async move {
            match op {
                "fetch_by_id" => s.fetch_by_id(id).await.map(|_| ()),
                "update" => s.update(Task::new(1)).await,
                "reschedule" => s.reschedule(Task::new(1), std::time::Duration::ZERO).await,
                "vacuum" => s.vacuum().await.map(|_| ()),
                "vacuum_before" => s.vacuum_before(std::time::Duration::ZERO).await.map(|_| ()),
                "resume_by_id" => s.resume_by_id(id.clone()).await.map(|_| ()),
                "resume_abandoned" => s.resume_abandoned().await.map(|_| ()),
                "register_worker" => s.register_worker("test".into()).await,
                "check_status" => s.check_status([id.clone()]).await.map(|_| ()),
                _ => unreachable!(),
            }
        })
    }
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl std::task::Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn counter() -> (Arc<Counter>, std::task::Waker) {
        let c = Arc::new(Counter::default());
        (c.clone(), std::task::Waker::from(c))
    }
    fn setup() -> (Shared<LookupBackend>, oneshot::Sender<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Shared::new(LookupBackend {
                queue: VecDequeBackend::new(),
                lookup_complete: Some(rx),
                on_wait_for: None,
            }),
            tx,
        )
    }
    fn sink_poll(
        s: &mut Shared<LookupBackend>,
        cx: &mut Context<'_>,
        op: &str,
    ) -> Poll<Result<(), VecDequeError>> {
        match op {
            "ready" => s.poll_ready_unpin(cx),
            "flush" => s.poll_flush_unpin(cx),
            _ => s.poll_close_unpin(cx),
        }
    }

    #[test]
    fn contended_operations_wake_after_management_releases_lock() {
        for outcome in ["success", "error", "cancel"] {
            for op in [
                "ready",
                "flush",
                "close",
                "worker_ready",
                "worker_next",
                "worker_close",
            ] {
                let (mut shared, tx) = setup();
                let mut manager = shared.clone();
                let (count, waker) = counter();
                let mut cx = Context::from_waker(&waker);
                let mut quiet = Context::from_waker(noop_waker_ref());
                let id = TaskId::from(1_u64);
                assert!(shared.poll_ready_unpin(&mut cx).is_ready());
                let mut lookup = Box::pin(manager.fetch_by_id(&id));
                assert!(lookup.as_mut().poll(&mut quiet).is_pending());
                shared.start_send_unpin(Task::new(7)).unwrap();
                let worker = WorkerContext::new("contended");
                let pending = match op {
                    "worker_ready" => {
                        Backend::poll_ready(&mut shared, &mut cx, &worker).is_pending()
                    }
                    "worker_next" => shared.poll_next(&mut cx, &worker).is_pending(),
                    "worker_close" => {
                        Backend::poll_close(&mut shared, &mut cx, &worker).is_pending()
                    }
                    _ => sink_poll(&mut shared, &mut cx, op).is_pending(),
                };
                assert!(pending);
                assert_eq!(count.0.load(Ordering::SeqCst), 0);
                match outcome {
                    "success" => {
                        tx.send(()).unwrap();
                        assert!(matches!(
                            lookup.as_mut().poll(&mut quiet),
                            Poll::Ready(Ok(None))
                        ));
                    }
                    "error" => {
                        drop(tx);
                        assert!(matches!(
                            lookup.as_mut().poll(&mut quiet),
                            Poll::Ready(Err(_))
                        ));
                    }
                    _ => {}
                }
                drop(lookup);
                assert!(count.0.load(Ordering::SeqCst) > 0, "{op}/{outcome}");
                assert!(shared.poll_flush_unpin(&mut cx).is_ready());
                let Poll::Ready(Some(Ok(task))) = shared.poll_next(&mut cx, &worker) else {
                    panic!("missing buffered task")
                };
                assert_eq!(task.args, 7);
            }
        }
    }

    #[test]
    fn every_management_operation_notifies_lock_waiters() {
        for method in [
            "fetch_by_id",
            "update",
            "reschedule",
            "vacuum",
            "vacuum_before",
            "resume_by_id",
            "resume_abandoned",
            "register_worker",
            "check_status",
        ] {
            for outcome in ["success", "error", "cancel"] {
                let (mut shared, tx) = setup();
                let mut manager = shared.clone();
                let id = TaskId::from(1_u64);
                let mut quiet = Context::from_waker(noop_waker_ref());
                let mut operation = management(&mut manager, &id, method);
                assert!(operation.as_mut().poll(&mut quiet).is_pending());
                let (count, waker) = counter();
                let mut cx = Context::from_waker(&waker);
                assert!(
                    shared
                        .poll_next(&mut cx, &WorkerContext::new("management"))
                        .is_pending()
                );
                match outcome {
                    "success" => {
                        tx.send(()).unwrap();
                        assert!(matches!(
                            operation.as_mut().poll(&mut quiet),
                            Poll::Ready(Ok(()))
                        ));
                    }
                    "error" => {
                        drop(tx);
                        assert!(matches!(
                            operation.as_mut().poll(&mut quiet),
                            Poll::Ready(Err(_))
                        ));
                    }
                    _ => {}
                }
                drop(operation);
                assert!(count.0.load(Ordering::SeqCst) > 0, "{method}/{outcome}");
            }
        }
    }

    #[test]
    fn cancelling_a_sink_operation_does_not_block_other_waiters() {
        for op in ["ready", "flush", "close"] {
            let (mut shared, tx) = setup();
            let mut manager = shared.clone();
            let mut active = shared.clone();
            let id = TaskId::from(1_u64);
            let mut quiet = Context::from_waker(noop_waker_ref());
            let mut lookup = Box::pin(manager.fetch_by_id(&id));
            assert!(lookup.as_mut().poll(&mut quiet).is_pending());
            let (cancelled_count, cancelled_waker) = counter();
            let mut cancelled = Box::pin(futures_util::future::poll_fn(|cx| {
                sink_poll(&mut shared, cx, op)
            }));
            assert!(
                cancelled
                    .as_mut()
                    .poll(&mut Context::from_waker(&cancelled_waker))
                    .is_pending()
            );
            drop(cancelled);
            let (count, waker) = counter();
            let mut result = shared.wait_for([id.clone()]);
            assert!(
                result
                    .poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let (active_count, active_waker) = counter();
            assert!(
                active
                    .poll_flush_unpin(&mut Context::from_waker(&active_waker))
                    .is_pending()
            );
            tx.send(()).unwrap();
            assert!(lookup.as_mut().poll(&mut quiet).is_ready());
            drop(lookup);
            assert!(count.0.load(Ordering::SeqCst) > 0);
            assert!(active_count.0.load(Ordering::SeqCst) > 0);
            assert!(
                result
                    .poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_ready()
            );
            assert!(
                active
                    .poll_flush_unpin(&mut Context::from_waker(&active_waker))
                    .is_ready()
            );
            assert!(shared.poll_ready_unpin(&mut quiet).is_ready());
            assert_eq!(Arc::strong_count(&cancelled_count), 2);
        }
    }

    #[test]
    fn cancelled_lock_acquisition_does_not_notify_before_unlock() {
        let (mut shared, tx) = setup();
        let mut holder = shared.clone();
        let mut manager = shared.clone();
        let id = TaskId::from(1_u64);
        let mut quiet = Context::from_waker(noop_waker_ref());
        let mut lookup = Box::pin(holder.fetch_by_id(&id));
        assert!(lookup.as_mut().poll(&mut quiet).is_pending());
        let (count, waker) = counter();
        assert!(
            shared
                .poll_flush_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut pending = Box::pin(manager.fetch_by_id(&id));
        assert!(pending.as_mut().poll(&mut quiet).is_pending());
        drop(pending);
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        tx.send(()).unwrap();
        assert!(lookup.as_mut().poll(&mut quiet).is_ready());
        drop(lookup);
        assert!(count.0.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn consumed_wait_registrations_do_not_keep_idle_workers_running() {
        let (mut first, tx) = setup();
        let _unused = first.clone();
        let mut second = first.clone();
        let mut manager = first.clone();
        let mut trigger = first.clone();
        let id = TaskId::from(1_u64);
        let mut quiet = Context::from_waker(noop_waker_ref());
        let mut lookup = Box::pin(manager.fetch_by_id(&id));
        assert!(lookup.as_mut().poll(&mut quiet).is_pending());
        let (a, aw) = counter();
        let (b, bw) = counter();
        let worker = WorkerContext::new("idle");
        assert!(
            first
                .poll_next(&mut Context::from_waker(&aw), &worker)
                .is_pending()
        );
        assert!(
            second
                .poll_next(&mut Context::from_waker(&bw), &worker)
                .is_pending()
        );
        tx.send(()).unwrap();
        assert!(lookup.as_mut().poll(&mut quiet).is_ready());
        drop(lookup);
        assert!(Backend::poll_close(&mut trigger, &mut quiet, &worker).is_ready());
        assert!(
            first
                .poll_next(&mut Context::from_waker(&aw), &worker)
                .is_pending()
        );
        assert!(
            second
                .poll_next(&mut Context::from_waker(&bw), &worker)
                .is_pending()
        );
        a.0.store(0, Ordering::SeqCst);
        b.0.store(0, Ordering::SeqCst);
        assert!(Backend::poll_close(&mut trigger, &mut quiet, &worker).is_ready());
        assert_eq!(a.0.load(Ordering::SeqCst), 0);
        assert_eq!(b.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn detached_wait_for_notifies_its_parent_handle() {
        let (mut shared, _tx) = setup();
        let inner = shared.inner.clone();
        let mut result = shared.wait_for([TaskId::from(1_u64)]);
        let parent = Arc::new(std::sync::Mutex::new(shared));
        let (count, waker) = counter();
        let callback_parent = parent.clone();
        let callback_waker = waker.clone();
        inner.backend.try_lock().unwrap().on_wait_for = Some(Box::new(move || {
            // Stream construction holds the backend lock while the parent is reused.
            assert!(
                callback_parent
                    .lock()
                    .unwrap()
                    .poll_flush_unpin(&mut Context::from_waker(&callback_waker))
                    .is_pending()
            );
        }));
        assert!(matches!(
            result.poll_next_unpin(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(None)
        ));
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(
            parent
                .lock()
                .unwrap()
                .poll_flush_unpin(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    struct LockCheckingWake<B> {
        inner: Arc<Inner<B>>,
        count: AtomicUsize,
    }

    impl<B: Send> std::task::Wake for LockCheckingWake<B> {
        fn wake(self: Arc<Self>) {
            assert!(
                self.inner.backend.try_lock().is_some(),
                "notified before unlocking"
            );
            // Reenter the registration table as well as the backend.
            self.inner
                .wakers
                .insert(usize::MAX, noop_waker_ref().clone());
            self.inner.wakers.remove(&usize::MAX);
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn notifications_follow_unlock_on_pending_errors_and_cancellation() {
        for outcome in ["success", "error", "cancel"] {
            let (mut shared, tx) = setup();
            let mut manager = shared.clone();
            let count = Arc::new(LockCheckingWake {
                inner: shared.inner.clone(),
                count: AtomicUsize::new(0),
            });
            let waker = Waker::from(count.clone());
            let id = TaskId::from(1_u64);
            let mut quiet = Context::from_waker(noop_waker_ref());
            let mut lookup = Box::pin(manager.fetch_by_id(&id));
            assert!(lookup.as_mut().poll(&mut quiet).is_pending());
            assert!(
                shared
                    .poll_flush_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            match outcome {
                "success" => {
                    tx.send(()).unwrap();
                    assert!(lookup.as_mut().poll(&mut quiet).is_ready());
                }
                "error" => {
                    drop(tx);
                    assert!(lookup.as_mut().poll(&mut quiet).is_ready());
                }
                _ => {}
            }
            drop(lookup);
            assert_eq!(count.count.load(Ordering::SeqCst), 1);
        }
        for fail in [false, true] {
            let (mut shared, receiver) = channel_backend();
            let mut waiting = shared.clone();
            let inner = shared.inner.clone();
            let count = Arc::new(LockCheckingWake {
                inner: inner.clone(),
                count: AtomicUsize::new(0),
            });
            let waker = Waker::from(count.clone());
            let mut quiet = Context::from_waker(noop_waker_ref());
            assert!(shared.poll_ready_unpin(&mut quiet).is_ready());
            shared.start_send_unpin(Task::new(1)).unwrap();
            let mut receiver = Some(receiver);
            if fail {
                receiver.take();
            }
            let guard = inner.backend.try_lock().unwrap();
            assert!(
                waiting
                    .poll_flush_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(guard);
            let result = shared.poll_ready_unpin(&mut quiet);
            if fail {
                assert!(matches!(result, Poll::Ready(Err(_))));
            } else {
                assert!(result.is_pending());
            }
            assert_eq!(count.count.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn waiters_update_clean_up_and_do_not_spin_while_locked() {
        let (mut shared, tx) = setup();
        let mut manager = shared.clone();
        let mut dropped = shared.clone();
        let (old_count, old_waker) = counter();
        let (count, waker) = counter();
        let id = TaskId::from(1_u64);
        let mut quiet = Context::from_waker(noop_waker_ref());
        let mut lookup = Box::pin(manager.fetch_by_id(&id));
        assert!(lookup.as_mut().poll(&mut quiet).is_pending());
        assert!(
            shared
                .poll_ready_unpin(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
        assert!(
            shared
                .poll_ready_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            dropped
                .poll_ready_unpin(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
        drop(dropped);
        old_count.0.store(0, Ordering::SeqCst);
        for _ in 0..3 {
            assert!(
                shared
                    .poll_flush_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        tx.send(()).unwrap();
        assert!(lookup.as_mut().poll(&mut quiet).is_ready());
        drop(lookup);
        assert_eq!(old_count.0.load(Ordering::SeqCst), 0);
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(shared.inner.wakers.is_empty());
        assert_eq!(Arc::strong_count(&count), 2);
    }

    #[test]
    fn registering_against_a_concurrent_unlock_preserves_progress() {
        for _ in 0..200 {
            let (mut shared, tx) = setup();
            let mut manager = shared.clone();
            let id = TaskId::from(1_u64);
            let mut lookup = Box::pin(manager.fetch_by_id(&id));
            assert!(
                lookup
                    .as_mut()
                    .poll(&mut Context::from_waker(noop_waker_ref()))
                    .is_pending()
            );
            let (count, waker) = counter();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let barrier = &barrier;
                let unlock = scope.spawn(move || {
                    barrier.wait();
                    tx.send(()).unwrap();
                    assert!(
                        lookup
                            .as_mut()
                            .poll(&mut Context::from_waker(noop_waker_ref()))
                            .is_ready()
                    );
                });
                barrier.wait();
                let result = shared.poll_flush_unpin(&mut Context::from_waker(&waker));
                unlock.join().unwrap();
                if result.is_pending() {
                    assert!(count.0.load(Ordering::SeqCst) > 0);
                    assert!(
                        shared
                            .poll_flush_unpin(&mut Context::from_waker(&waker))
                            .is_ready()
                    );
                }
            });
            assert!(shared.inner.wakers.is_empty());
        }
    }

    #[test]
    fn reentrant_notifications_do_not_recurse_through_the_remaining_batch() {
        struct Reenter {
            shared: std::sync::Mutex<Shared<WithCodec<VecDequeBackend<u32>, IdentityCodec>>>,
            depth: Arc<AtomicUsize>,
            maximum: Arc<AtomicUsize>,
        }
        impl std::task::Wake for Reenter {
            fn wake(self: Arc<Self>) {
                let depth = self.depth.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum.fetch_max(depth, Ordering::SeqCst);
                self.shared
                    .lock()
                    .unwrap()
                    .middleware(&mut WorkerContext::new("reentrant"));
                self.depth.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let mut trigger = VecDequeBackend::<u32>::new()
            .with_codec(IdentityCodec)
            .shared();
        let inner = trigger.inner.clone();
        let depth = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let guard = inner.backend.try_lock().unwrap();
        let mut waiters = Vec::new();
        for _ in 0..64 {
            let mut waiter = trigger.clone();
            let waker = Waker::from(Arc::new(Reenter {
                shared: std::sync::Mutex::new(trigger.clone()),
                depth: depth.clone(),
                maximum: maximum.clone(),
            }));
            assert!(
                waiter
                    .poll_flush_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            waiters.push(waiter);
        }
        drop(guard);
        trigger.middleware(&mut WorkerContext::new("trigger"));
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        assert!(inner.wakers.is_empty());
    }
}
