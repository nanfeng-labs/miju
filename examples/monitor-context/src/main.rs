//! Observe two active workers without giving the display ownership of them.

use std::{
    io::{self, IsTerminal, Write},
    time::Duration,
};

use email_service::{Email, example_good_email};
use miju::{
    layers::tracing::{ContextualTaskSpan, OtelTraceContext, TraceLayer, TracingContext},
    prelude::*,
};
use miju_file_storage::JsonStorage;
use rand::RngExt;

async fn deliver(_: Email, task: TaskContext) -> Result<(), BoxDynError> {
    let millis = rand::rng().random_range(1_000..=20_000);
    // The wrapper checks execution state when polled. A sleeping sub-task
    // may therefore remain allocated until its timer next wakes it.
    tokio::spawn(task.run_until_executed(tokio::time::sleep(Duration::from_secs(12))));
    if millis > 7_000 {
        task.cancel()?;
        return Ok(());
    }
    tokio::time::sleep(Duration::from_millis(millis)).await;
    Err("simulated delivery failure".into())
}

async fn produce(
    mut backend: JsonStorage<Email>,
    context: &MonitorContext,
    worker_name: &str,
    attach_context: bool,
) -> Result<(), BoxDynError> {
    loop {
        if attach_context {
            // Wiring only: no OpenTelemetry provider is installed in this panel.
            let metadata = TracingContext::from(OtelTraceContext::current());
            backend
                .push_task(
                    TaskBuilder::new(example_good_email())
                        .metadata(&metadata)
                        .build(),
                )
                .await?;
        } else {
            backend.push(example_good_email()).await?;
        }
        // File storage does not wake a parked reader when a clone pushes a task.
        context.wake_worker(worker_name)?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn render(workers: &[WorkerContext], interactive: bool) -> io::Result<()> {
    let mut out = io::stdout().lock();
    if interactive {
        write!(out, "\x1b[H\x1b[2J")?;
    }
    writeln!(out, "miju workers — Ctrl+C to stop")?;
    writeln!(out, "Name          Running Tasks Ready Restarts Elapsed")?;
    for worker in workers {
        writeln!(
            out,
            "{:<13} {:<7} {:>5} {:<5} {:>8} {:>6}s",
            worker.name(),
            worker.is_running(),
            worker.task_count(),
            worker.is_ready(),
            worker.restarts(),
            worker.elapsed().as_secs()
        )?;
    }
    out.flush()
}

async fn observe(context: &MonitorContext, interactive: bool) -> io::Result<()> {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        render(context.workers(), interactive)?;
    }
}

// Only real terminals receive control sequences; redirected output is plain text.
struct TerminalGuard(bool);

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        let guard = Self(io::stdout().is_terminal());
        if guard.0 {
            let mut out = io::stdout().lock();
            write!(out, "\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H")?;
            out.flush()?;
        }
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.0 {
            let mut out = io::stdout().lock();
            let _ = write!(out, "\x1b[?25h\x1b[?1049l");
            let _ = out.flush();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let directory = tempfile::tempdir()?;
    let plain = JsonStorage::new(directory.path().join("plain.json"))?;
    let contextual = JsonStorage::new(directory.path().join("contextual.json"))?;
    let plain_writer = plain.clone();
    let contextual_writer = contextual.clone();
    let monitor = Monitor::new()
        .register(move |_| {
            WorkerBuilder::new("plain")
                .backend(plain.clone())
                .concurrency(30)
                .enable_tracing()
                .build(deliver)
        })
        .register(move |_| {
            WorkerBuilder::new("contextual")
                .backend(contextual.clone())
                .concurrency(20)
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(deliver)
        })
        .should_restart(|worker, error| {
            worker.name() == "contextual"
                && worker.restarts() < 5
                && error.to_string().contains("recoverable backend error")
        })
        .shutdown_timeout(Duration::from_secs(5));
    let context = monitor.context();
    let terminal = TerminalGuard::enter()?;

    // These independent futures run alongside the monitor. Leaving select!
    // drops producers and the observer before TerminalGuard restores the screen.
    tokio::select! {
        result = monitor.run_with_signal(tokio::signal::ctrl_c()) => result?,
        result = produce(plain_writer, &context, "plain", false) => result?,
        result = produce(contextual_writer, &context, "contextual", true) => result?,
        result = observe(&context, terminal.0) => result?,
    }
    Ok(())
}
