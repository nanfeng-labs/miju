//! Exercise typed node adapters without graph construction or scheduling.

use std::{
    collections::BTreeMap,
    future::{Ready, ready},
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use miju_codec::json::JsonCodec;
use miju_core::task::{
    Task, builder::TaskBuilder, metadata::Metadata, status::Status, task_fn::task_fn,
    task_id::TaskId,
};
use miju_workflow::{
    graph::{GraphFlowContext, NodeInput, node::GraphNodeService},
    in_memory::InMemoryWorkflow,
};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;
use tower::{Service, ServiceExt, service_fn};

type BytesBackend = InMemoryWorkflow<u32>;

fn input(args: NodeInput<Vec<u8>>) -> Task<NodeInput<Vec<u8>>> {
    TaskBuilder::new(args)
        .data(JsonCodec::<Vec<u8>>::default())
        .build()
}

#[tokio::test]
async fn single_inputs_keep_the_execution_context_and_return_both_outputs() {
    let task = TaskBuilder::new(NodeInput::Single(b"7".to_vec()))
        .data(JsonCodec::<Vec<u8>>::default())
        .data("marker".to_owned())
        .task_id(TaskId::from_int(17))
        .attempt(3)
        .status(Status::Running)
        .run_at_timestamp(123)
        .metadata(&GraphFlowContext::new(None))
        .build();
    let (_, original) = task.clone().take();
    let handler = service_fn(move |task: Task<u32>| {
        let (value, context) = task.take();
        assert!(Arc::ptr_eq(&context, &original));
        assert_eq!(context.task_id(), Some(&TaskId::from_int(17)));
        assert_eq!(context.attempt(), 3);
        assert_eq!(context.status(), Status::Running);
        assert_eq!(context.run_at(), Some(123));
        assert_eq!(context.data().get::<String>().unwrap(), "marker");
        assert!(
            GraphFlowContext::extract(context.metadata())
                .unwrap()
                .is_initial
        );
        ready(Ok::<_, io::Error>(value + 1))
    });
    let node = GraphNodeService::<_, BytesBackend, u32>::new(handler);
    let first = node.clone().oneshot(task.clone()).await.unwrap();
    let second = node.oneshot(task).await.unwrap();
    assert_eq!(first, (b"8".to_vec(), json!(8)));
    assert_eq!(second, first);
}

#[tokio::test]
async fn fan_in_deserializes_ordered_json_into_tuples_and_vectors() {
    let handler =
        task_fn(|(label, count): (String, u32)| async move { format!("{label}-{count}") });
    let node = GraphNodeService::<_, BytesBackend, (String, u32)>::new(handler);
    let (compact, value) = node
        .oneshot(input(NodeInput::FanIn(vec![json!("items"), json!(3)])))
        .await
        .unwrap();
    assert_eq!(compact, b"\"items-3\"");
    assert_eq!(value, json!("items-3"));

    let node =
        GraphNodeService::<_, BytesBackend, Vec<u32>>::new(task_fn(|items: Vec<u32>| async move {
            items
        }));
    let (compact, value) = node
        .clone()
        .oneshot(input(NodeInput::FanIn(vec![json!(3), json!(1), json!(2)])))
        .await
        .unwrap();
    assert_eq!(compact, b"[3,1,2]");
    assert_eq!(value, json!([3, 1, 2]));
    assert_eq!(
        node.oneshot(input(NodeInput::FanIn(vec![]))).await.unwrap(),
        (b"[]".to_vec(), json!([]))
    );
}

#[tokio::test]
async fn invalid_inputs_skip_the_handler_and_handler_errors_keep_their_type() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let handler = service_fn(move |_: Task<u32>| {
        observed.fetch_add(1, Ordering::SeqCst);
        ready(Err::<u32, _>(io::Error::other("handler failed")))
    });
    let node = GraphNodeService::<_, BytesBackend, u32>::new(handler);
    for args in [
        NodeInput::Single(b"\"invalid\"".to_vec()),
        NodeInput::FanIn(vec![json!(7)]),
    ] {
        let error = node.clone().oneshot(input(args)).await.unwrap_err();
        assert!(error.is::<serde_json::Error>());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let error = node
        .oneshot(input(NodeInput::Single(b"7".to_vec())))
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().to_string(),
        "handler failed"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn readiness_pending_and_errors_are_forwarded() {
    struct ReadinessProbe {
        polled: bool,
    }

    impl Service<Task<u32>> for ReadinessProbe {
        type Response = u32;
        type Error = io::Error;
        type Future = Ready<Result<u32, io::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.polled {
                Poll::Ready(Err(io::Error::other("not available")))
            } else {
                self.polled = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        fn call(&mut self, _: Task<u32>) -> Self::Future {
            panic!("an unready service must not be called");
        }
    }

    let mut node = GraphNodeService::<_, BytesBackend, u32>::new(ReadinessProbe { polled: false });
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(node.poll_ready(&mut cx).is_pending());
    let Poll::Ready(Err(error)) = node.poll_ready(&mut cx) else {
        panic!("expected readiness error")
    };
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().to_string(),
        "not available"
    );
}

#[test]
fn missing_codec_panics_synchronously_for_both_input_kinds() {
    for args in [
        NodeInput::Single(b"7".to_vec()),
        NodeInput::FanIn(vec![json!(7)]),
    ] {
        let handler = service_fn(|_: Task<u32>| ready(Ok::<_, io::Error>(7_u32)));
        let mut node = GraphNodeService::<_, BytesBackend, u32>::new(handler);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _future = node.call(Task::new(args));
        }))
        .unwrap_err();
        let message = panic.downcast_ref::<String>().unwrap();
        assert!(message.contains("GraphExecutor should be injected"));
    }
}

#[tokio::test]
async fn output_encoding_and_json_errors_happen_after_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let handler = service_fn(move |_: Task<u32>| {
        observed.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, io::Error>(BTreeMap::from([(vec![1_u8], 2_u32)])))
    });
    let node = GraphNodeService::<_, BytesBackend, u32>::new(handler);
    let error = node
        .oneshot(input(NodeInput::Single(b"7".to_vec())))
        .await
        .unwrap_err();
    assert!(error.is::<serde_json::Error>());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    #[derive(Deserialize)]
    struct FailsSecondSerialization {
        #[serde(skip)]
        calls: Arc<AtomicUsize>,
    }
    impl Serialize for FailsSecondSerialization {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                serializer.serialize_u32(7)
            } else {
                Err(serde::ser::Error::custom("JSON view failed"))
            }
        }
    }
    let serializations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&serializations);
    let handler = service_fn(move |_: Task<u32>| {
        ready(Ok::<_, io::Error>(FailsSecondSerialization {
            calls: Arc::clone(&observed),
        }))
    });
    let node = GraphNodeService::<_, BytesBackend, u32>::new(handler);
    let error = node
        .oneshot(input(NodeInput::Single(b"7".to_vec())))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "JSON view failed");
    assert_eq!(serializations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ordinary_tuple_output_differs_from_single_input_graph_encoding() {
    let node = GraphNodeService::<_, BytesBackend, u32>::new(task_fn(|value: u32| async move {
        Ok::<_, io::Error>((value, "next".to_owned()))
    }));
    let (compact, value) = node
        .oneshot(input(NodeInput::Single(b"7".to_vec())))
        .await
        .unwrap();
    assert_eq!(compact, b"[7,\"next\"]");
    assert_eq!(value, json!([7, "next"]));

    let next = GraphNodeService::<_, BytesBackend, (u32, String)>::new(task_fn(
        |(value, label): (u32, String)| async move { format!("{label}-{value}") },
    ));
    let error = next
        .clone()
        .oneshot(input(NodeInput::Single(compact)))
        .await
        .unwrap_err();
    assert!(error.is::<serde_json::Error>());
    let (_, output) = next
        .oneshot(input(NodeInput::FanIn(vec![json!(7), json!("next")])))
        .await
        .unwrap();
    assert_eq!(output, json!("next-7"));
}
