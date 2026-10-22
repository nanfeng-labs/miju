//! Collect concurrent order, analytics, and profile exports through TaskRunner.

use std::time::Duration;

use futures_util::TryStreamExt;
use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct ExportTask {
    user_id: u32,
    from: String,
    through: String,
}

#[derive(Debug)]
enum ExportPart {
    Orders(Vec<String>),
    Analytics(String),
    Profile(String),
}

async fn export_orders(task: ExportTask) -> Result<ExportPart, BoxDynError> {
    tokio::time::sleep(Duration::from_secs(9)).await;
    Ok(ExportPart::Orders(vec![format!(
        "Orders: user {}, {} through {}",
        task.user_id, task.from, task.through
    )]))
}

async fn export_analytics(task: ExportTask) -> Result<ExportPart, BoxDynError> {
    tokio::time::sleep(Duration::from_secs(12)).await;
    Ok(ExportPart::Analytics(format!(
        "Analytics: user {}, {} through {}",
        task.user_id, task.from, task.through
    )))
}

async fn export_profile(user_id: u32) -> Result<ExportPart, BoxDynError> {
    tokio::time::sleep(Duration::from_secs(8)).await;
    Ok(ExportPart::Profile(format!("Profile: user {user_id}")))
}

async fn process_export(
    task: ExportTask,
    mut runner: TaskRunner<Result<ExportPart, BoxDynError>>,
) -> Result<String, BoxDynError> {
    // Spawn independent operations so their simulated I/O can overlap.
    runner.execute(tokio::spawn(export_orders(task.clone())));
    runner.execute(tokio::spawn(export_analytics(task.clone())));
    runner.execute(tokio::spawn(export_profile(task.user_id)));
    info!("Export operations started");

    // The outer result reports runner/executor errors; each item holds an export result.
    let results = runner.try_collect::<Vec<_>>().await?;
    let mut orders = None;
    let mut analytics = None;
    let mut profile = None;
    for result in results {
        // Completion order is unspecified, so identify each part by its variant.
        match result? {
            ExportPart::Orders(value) => orders = Some(value),
            ExportPart::Analytics(value) => analytics = Some(value),
            ExportPart::Profile(value) => profile = Some(value),
        }
    }

    let orders = orders.ok_or("orders export is missing")?;
    let analytics = analytics.ok_or("analytics export is missing")?;
    let profile = profile.ok_or("profile export is missing")?;
    let summary = format!(
        "Export complete: {} order batch, {analytics}, {profile}",
        orders.len()
    );
    info!(%summary);
    Ok(summary)
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let mut backend = MemoryStorage::new();
    backend
        .push(ExportTask {
            user_id: 42,
            from: "2026-10-01".to_owned(),
            through: "2026-10-21".to_owned(),
        })
        .await?;
    let concurrency = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    info!("Press Ctrl+C to stop accepting tasks and wait for the active export");
    WorkerBuilder::new("long-running")
        .backend(backend)
        .concurrency(concurrency)
        .enable_tracing()
        // Inject the TaskRunner configuration used by the handler's extractor.
        .long_running()
        .on_event(|_, event| info!(%event, "Worker event"))
        .build(process_export)
        .run_with_ctx(|context| async move {
            tokio::signal::ctrl_c().await?;
            info!("Shutdown requested; waiting for the active export");
            // Keep polling the handler until it has collected its spawned operations.
            context.stop()?;
            Ok(())
        })
        .await?;

    Ok(())
}
