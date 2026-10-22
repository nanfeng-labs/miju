//! Pass typed results between workflow steps and configure a continuation delay.
//!
//! The memory backend does not enforce scheduled times, so this example runs
//! immediately even though the delay step records a one-second offset.

use std::time::Duration;

use miju::prelude::*;
use miju_workflow::{SteppedFlow, in_memory::InMemoryWorkflow};
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize, Serialize)]
struct Welcome {
    user_id: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct Campaign {
    campaign_id: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct Completion {
    completion_id: usize,
}

type Store = Shared<InMemoryWorkflow<Welcome>>;

// Data comes from the worker builder, independently of each step's input type.
async fn welcome(job: Welcome, _store: Data<Store>) -> Result<Campaign, BoxDynError> {
    info!(user_id = job.user_id, "Welcome step");
    Ok(Campaign {
        campaign_id: job.user_id + 1,
    })
}

async fn campaign(job: Campaign, _store: Data<Store>) -> Result<Completion, BoxDynError> {
    info!(campaign_id = job.campaign_id, "Campaign step");
    Ok(Completion {
        completion_id: job.campaign_id + 1,
    })
}

async fn complete(
    job: Completion,
    _store: Data<Store>,
    worker: WorkerContext,
) -> Result<String, BoxDynError> {
    info!(completion_id = job.completion_id, "Campaign completed");
    worker.stop()?;
    Ok(format!("Campaign {} completed", job.completion_id))
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let mut backend = InMemoryWorkflow::create();
    backend.push(Welcome { user_id: 1 }).await?;

    // Serialized output becomes the next typed input. DelayFor records run_after;
    // a backend with scheduling support is needed to actually wait for that time.
    let flow = SteppedFlow::new("welcome-campaign")
        .and_then(welcome)
        .delay_for(Duration::from_secs(1))
        .and_then(campaign)
        .and_then(complete);

    WorkerBuilder::new("workflow-stepped")
        .backend(backend.clone())
        .data(backend)
        .parallelize(tokio::spawn)
        .enable_tracing()
        .concurrency(2)
        .on_event(|_, event| info!(%event, "Workflow event"))
        .build(flow)
        .run()
        .await?;
    Ok(())
}
