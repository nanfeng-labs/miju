//! Submit simulated email jobs and scrape their completion metrics over HTTP.

use std::{fmt::Debug, time::Duration};

use axum::{
    Extension, Router,
    extract::{Form, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse},
    routing::get,
};
use email_service::{Email, FORM_HTML, send_email};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use miju::{layers::prometheus::PrometheusLayer, prelude::*};
use miju_file_storage::JsonStorage;
use serde::{Serialize, de::DeserializeOwned};

async fn enqueue<T>(
    Extension(mut storage): Extension<JsonStorage<T>>,
    Form(payload): Form<T>,
) -> impl IntoResponse
where
    T: Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    // Form rejects missing/malformed fields before the task reaches storage.
    // A successful enqueue does not mean the handler accepted the address.
    match storage.push(payload).await {
        Ok(()) => (StatusCode::CREATED, "Queued a simulated email".to_owned()),
        Err(error) => {
            tracing::error!(%error, "Task enqueue failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not queue the job".to_owned(),
            )
        }
    }
}

async fn metrics(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        handle.render(),
    )
}

fn recorder() -> Result<PrometheusHandle, BoxDynError> {
    // Match the actual duration instrument; a prefix for HTTP requests would
    // leave worker durations as summaries instead of this histogram.
    let buckets = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];
    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Full("task_duration_seconds".to_owned()), buckets)?
        .install_recorder()?)
}

// The file backend uses synchronous try-lock operations. Keep its cloned handles
// on one thread so concurrent HTTP requests cannot interrupt a storage operation.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let directory = tempfile::tempdir()?;
    let backend = JsonStorage::<Email>::new(directory.path().join("jobs.json"))?;
    let handle = recorder()?;
    let app = Router::new()
        .route("/", get(async || Html(FORM_HTML)).post(enqueue::<Email>))
        .layer(Extension(backend.clone()))
        .route("/metrics", get(metrics))
        .with_state(handle.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!("Form: http://127.0.0.1:3000/ ; metrics: /metrics ; Ctrl+C to stop");

    // install_recorder does not spawn an exporter task. Keep histogram storage
    // maintained even when nobody is scraping the endpoint.
    let upkeep = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            handle.run_upkeep();
        }
    });
    let worker = async {
        WorkerBuilder::new("email-metrics")
            // Retain periodic polling alongside the storage's append notifications.
            .backend(backend.poll_with_interval(Duration::from_millis(100)))
            .layer(PrometheusLayer::default())
            .enable_tracing()
            .build(send_email)
            .run_until(tokio::signal::ctrl_c())
            .await?;
        Ok::<(), BoxDynError>(())
    };
    let http = async {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "Could not listen for shutdown");
                }
            })
            .await?;
        Ok::<(), BoxDynError>(())
    };
    let result = tokio::try_join!(worker, http);
    upkeep.abort();
    let _ = upkeep.await;
    result?;
    Ok(())
}
