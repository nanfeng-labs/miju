#![allow(missing_docs)]

use miju::{layers::catch_panic::PanicError, prelude::*};
use std::time::Duration;
use tokio::time::sleep;

async fn task(value: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
    sleep(Duration::from_secs(1)).await;
    assert_eq!(value, 42);
    worker.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let concurrency = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    let mut backend = MemoryStorage::new();
    backend.push(42u32).await.unwrap();

    let worker = WorkerBuilder::new("basic")
        .backend(backend)
        // Admit up to 100 tasks per second without counting retry attempts again.
        .rate_limit(100, Duration::from_secs(1))
        // Use the available parallelism as the limit, holding a slot across retries.
        .concurrency(concurrency)
        .retry(
            // Allow up to three retries after the initial attempt.
            RetryPolicy::retries(3)
                // Skip errors exposed directly as PanicError.
                .retry_if(|error: &BoxDynError| error.downcast_ref::<PanicError>().is_none()),
        )
        // Catch unwinding panics in the wrapped service call and future.
        .catch_panic()
        // Spawn each attempt in a Tokio task; its panics surface as JoinError.
        .parallelize(tokio::spawn)
        // Give each attempt its own tracing span.
        .enable_tracing()
        .build(task);

    worker.run().await.unwrap();
}
