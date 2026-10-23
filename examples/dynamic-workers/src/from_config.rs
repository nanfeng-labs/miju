//! Parse a worker configuration and enqueue a task after its backend starts.

use miju::{
    config::{WorkerConfig, WorkerFromConfig},
    prelude::*,
};
use miju_file_storage::JsonStorage;

async fn handle(value: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(value, 42);
    println!("Configured worker processed {value}");
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
    // Give each demonstration run a fresh file that is removed on normal exit.
    let directory = tempfile::tempdir()?;
    let json = serde_json::json!({
        "name": "configured-worker",
        "backend": directory.path().join("jobs.json"),
        "middleware": ["CatchPanic", "Tracing"]
    });
    // Middleware descriptors are a set; their array order is not execution order.
    let config: WorkerConfig<JsonStorage<u32>> = serde_json::from_value(json)?;
    WorkerBuilder::try_config(config)?
        .map_backend(|backend| {
            backend.after_start(|backend| {
                let mut writer = backend.clone();
                async move {
                    writer.push(42).await.map_err(std::io::Error::other)?;
                    Ok(())
                }
            })
        })
        .build(handle)
        .run()
        .await?;
    Ok(())
}
