//! Export worker metrics from an OpenTelemetry SDK through a local scrape endpoint.

use std::{fmt::Debug, sync::OnceLock, time::Duration};

use axum::{
    Extension, Router,
    extract::{Form, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use email_service::{Email, FORM_HTML, send_email};
use miju::{layers::opentelemetry::OpenTelemetryMetricsLayer, prelude::*};
use miju_file_storage::JsonStorage;
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{Serialize, de::DeserializeOwned};

fn resource() -> Resource {
    static RESOURCE: OnceLock<Resource> = OnceLock::new();
    RESOURCE
        .get_or_init(|| Resource::builder().with_service_name("miju-email").build())
        .clone()
}

fn metrics_provider() -> Result<(SdkMeterProvider, Registry), BoxDynError> {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;
    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_resource(resource())
        .build();
    // An application using an OTLP collector would configure an OTLP exporter
    // and a periodic reader here instead. This example needs no external collector.
    Ok((provider, registry))
}

async fn metrics(State(registry): State<Registry>) -> Response {
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    match encoder.encode(&registry.gather(), &mut body) {
        Ok(()) => ([(CONTENT_TYPE, encoder.format_type().to_owned())], body).into_response(),
        Err(error) => {
            tracing::error!(%error, "Metrics encoding failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not encode metrics",
            )
                .into_response()
        }
    }
}

async fn enqueue<T>(
    Extension(mut storage): Extension<JsonStorage<T>>,
    Form(payload): Form<T>,
) -> impl IntoResponse
where
    T: Debug + Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
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

// The file backend uses synchronous try-lock operations. Keep its cloned handles
// on one thread so concurrent HTTP requests cannot interrupt a storage operation.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let directory = tempfile::tempdir()?;
    let backend = JsonStorage::<Email>::new(directory.path().join("jobs.json"))?;
    let (provider, registry) = metrics_provider()?;
    // Install before creating the worker's instruments. Enabling a feature alone
    // does not configure a metrics pipeline.
    global::set_meter_provider(provider.clone());
    let app = Router::new()
        .route("/", get(async || Html(FORM_HTML)).post(enqueue::<Email>))
        .layer(Extension(backend.clone()))
        .route("/metrics", get(metrics))
        .with_state(registry);
    tracing::info!("Form: http://127.0.0.1:3000/ ; metrics: /metrics ; Ctrl+C to stop");

    let worker = async {
        WorkerBuilder::new("email-otel-metrics")
            // Retain periodic polling alongside the storage's append notifications.
            .backend(backend.poll_with_interval(Duration::from_millis(100)))
            .layer(OpenTelemetryMetricsLayer::default())
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
    provider.shutdown()?;
    result?;
    Ok(())
}
