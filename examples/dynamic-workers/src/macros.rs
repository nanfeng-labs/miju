//! Register differently typed workers with optional middleware settings.

use std::{fmt::Debug, time::Duration};

use miju::{backend::dequeue::VecDequeBackend, prelude::*};
use miju_file_storage::JsonStorage;

macro_rules! workers {
    ($monitor:expr; $(
        ($name:literal, $backend:expr, $handler:expr)
        $( [concurrency = $limit:expr] )?
        $( [rate = ($count:expr, $period:expr)] )?
    ),+ $(,)?) => {{
        let monitor = $monitor;
        $(
            let monitor = monitor.register(move |_| {
                let builder = WorkerBuilder::new($name)
                    .backend($backend).catch_panic().enable_tracing();
                $(let builder = builder.concurrency($limit);)?
                $(let builder = builder.rate_limit($count, $period);)?
                builder.build($handler)
            });
        )+
        monitor
    }};
}

async fn handle<T: Debug>(value: T, worker: WorkerContext) -> Result<(), BoxDynError> {
    tracing::info!(worker = worker.name(), ?value, "Handled configured payload");
    worker.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let directory = tempfile::tempdir()?;
    let mut text = JsonStorage::new(directory.path().join("text.json"))?;
    // Factories may be called again: these handles share cloneable in-memory queues.
    let mut small = VecDequeBackend::new();
    let mut medium = JsonStorage::new(directory.path().join("numbers.json"))?;
    let mut large = VecDequeBackend::new();
    text.push("Hello macro".to_owned()).await?;
    small.push(1usize).await?;
    medium.push(2u32).await?;
    large.push(3u64).await?;

    workers!(Monitor::new();
        ("text", text.clone(), handle::<String>) [concurrency = 1],
        ("small", small.clone(), handle::<usize>),
        ("medium", medium.clone(), handle::<u32>) [rate = (10, Duration::from_secs(5))],
        ("large", large.clone(), handle::<u64>) [concurrency = 2] [rate = (40, Duration::from_secs(5))],
    )
    .on_event(|worker, event| tracing::info!(worker = worker.name(), %event, "Worker event"))
    .should_restart(|worker, error| worker.name() == "medium"
        && worker.restarts() < 5 && error.to_string().contains("recoverable backend error"))
    .shutdown_timeout(Duration::from_secs(5))
    .run_with_signal(tokio::signal::ctrl_c()).await?;
    Ok(())
}
