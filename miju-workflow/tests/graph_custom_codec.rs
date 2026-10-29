//! Custom graph validation errors survive registration and node adaptation.

use std::{
    future::ready,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use miju_codec::json::JsonCodec;
use miju_core::{
    backend::{
        codec::Codec,
        ext::{shared::Shared, with_codec::WithCodec},
        memory::MemoryStorage,
    },
    error::BoxDynError,
    task::{Task, builder::TaskBuilder},
    worker::service::IntoWorkerService,
};
use miju_workflow::{
    GraphFlow,
    graph::{NodeInput, decode::GraphCodec, error::GraphFlowError},
    in_memory::InMemoryWorkflow,
};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tower::{ServiceExt, service_fn};

type Backend = Shared<InMemoryWorkflow<u32>>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Positive(u32);

#[derive(Debug, thiserror::Error)]
enum InputError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("input must be positive")]
    NotPositive,
}

impl GraphCodec<Backend> for Positive {
    type Error = InputError;

    fn encode(self, _: &JsonCodec) -> Result<Vec<u8>, Self::Error> {
        Ok(serde_json::to_vec(&self)?)
    }

    fn decode(input: &Vec<u8>, _: &JsonCodec) -> Result<Self, Self::Error> {
        let value: Self = serde_json::from_slice(input)?;
        if value.0 == 0 {
            return Err(InputError::NotPositive);
        }
        Ok(value)
    }
}

fn input(compact: &[u8]) -> Task<NodeInput<Vec<u8>>> {
    TaskBuilder::new(NodeInput::Single(compact.to_vec()))
        .data(JsonCodec::<Vec<u8>>::default())
        .build()
}

#[tokio::test]
async fn all_registration_methods_accept_distinct_graph_errors_and_preserve_validation() {
    let calls = Arc::new(AtomicUsize::new(0));
    // Separate graphs avoid type-name collisions between closure labels.
    for registration in 0..3 {
        let graph = GraphFlow::<Backend>::new("custom-errors");
        let observed = Arc::clone(&calls);
        let before = calls.load(Ordering::SeqCst);
        let name = match registration {
            0 => {
                let _node = graph.add_node(
                    "named",
                    service_fn(move |task: Task<Positive>| {
                        observed.fetch_add(1, Ordering::SeqCst);
                        ready(Ok::<_, io::Error>(task.args.0))
                    }),
                );
                "named"
            }
            1 => {
                let task = move |value: Positive| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { value.0 }
                };
                let name = std::any::type_name_of_val(&task);
                let _task = graph.add_task(task);
                name
            }
            _ => {
                let router = move |value: Positive| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { Ok::<_, io::Error>(NodeIndex::new(value.0 as usize)) }
                };
                let name = std::any::type_name_of_val(&router);
                let _route = graph.route(router);
                name
            }
        };
        assert_eq!(calls.load(Ordering::SeqCst), before);
        let mut service = graph
            .into_service(InMemoryWorkflow::<u32>::create())
            .service;
        let node = service.get_node_by_name_mut(name).unwrap();
        let error = node.clone().oneshot(input(b"0")).await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<InputError>(),
            Some(InputError::NotPositive)
        ));
        let error = node.clone().oneshot(input(b"]")).await.unwrap_err();
        assert!(
            matches!(error.downcast_ref::<InputError>(), Some(InputError::Json(source)) if source.is_syntax())
        );
        assert_eq!(calls.load(Ordering::SeqCst), before);

        let (_, output) = node.clone().oneshot(input(b"3")).await.unwrap();
        assert_eq!(output, json!(3));
        assert_eq!(calls.load(Ordering::SeqCst), before + 1);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("codec failure marker")]
struct Marker(Arc<()>);

struct FailingCodec {
    calls: AtomicUsize,
    fail_at: usize,
    marker: Arc<()>,
}

impl FailingCodec {
    fn step(&self) -> Result<(), BoxDynError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == self.fail_at {
            return Err(Box::new(Marker(Arc::clone(&self.marker))));
        }
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned> Codec<T> for FailingCodec {
    type Compact = Vec<u8>;
    type Error = BoxDynError;

    fn encode(&self, value: &T) -> Result<Vec<u8>, Self::Error> {
        self.step()?;
        Ok(serde_json::to_vec(value)?)
    }

    fn decode(&self, value: &Vec<u8>) -> Result<T, Self::Error> {
        self.step()?;
        Ok(serde_json::from_slice(value)?)
    }
}

#[test]
fn element_and_envelope_failures_stop_immediately_and_keep_boxed_error_identity() {
    type Backend = WithCodec<MemoryStorage<Vec<u8>>, FailingCodec>;
    type Pair = (u32, String);
    let wire = serde_json::to_vec(&vec![b"7".to_vec(), b"\"ready\"".to_vec()]).unwrap();
    for encode in [true, false] {
        for fail_at in 0..3 {
            let codec = FailingCodec {
                calls: AtomicUsize::new(0),
                fail_at,
                marker: Arc::new(()),
            };
            let error = if encode {
                <Pair as GraphCodec<Backend>>::encode((7, "ready".to_owned()), &codec).unwrap_err()
            } else {
                <Pair as GraphCodec<Backend>>::decode(&wire, &codec).unwrap_err()
            };
            let GraphFlowError::Codec(source) = error else {
                panic!("expected a wrapped codec error");
            };
            let original = source
                .downcast_ref::<Marker>()
                .expect("boxed error must not gain another wrapper");
            assert!(Arc::ptr_eq(&original.0, &codec.marker));
            assert_eq!(codec.calls.load(Ordering::SeqCst), fail_at + 1);
        }
    }
}
