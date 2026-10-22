<p align="center">
  <img src="../../assets/miju-file-storage-header.svg" alt="miju-file-storage" width="480" />
</p>

<h1 align="center">miju-file-storage</h1>

File-backed task storage implementing the backend contracts in `miju-core`.
`JsonStorage<Args>` stores JSON records, while `CsvStorage<Args>` uses a flattened
CSV representation. Both use the JSON codec for task arguments.

## Usage

```rust,no_run
use miju_core::{backend::TaskSink, worker::builder::WorkerBuilder};
use miju_file_storage::JsonStorage;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = JsonStorage::<u32>::new_temp()?;
    storage.push(42).await?;
    let worker = WorkerBuilder::new("file-worker")
        .backend(storage)
        .build(|value: u32| async move { value + 1 });
    worker.run().await?;
    Ok(())
}
```

Workers remain active while waiting for more tasks. Use the worker context or a
controller to stop them when appropriate.

## Storage and Completion

`FileStorage<Args, A>` accepts a record `Adapter`. `JsonAdapter` and `CsvAdapter`
provide the built-in formats. Tasks can be pushed through `TaskSink`; the backend
adds a line identifier to execution metadata and acknowledgement stores the
handler result. `WaitForCompletion` exposes result polling and status queries.

`SharedJsonStore` creates typed `MemoryStorage` handles backed by one temporary
JSON store. It separates streams by task type metadata. These handles use the
memory backend's execution contract; they do not provide the file backend's
completion acknowledgement middleware.

## Current Limitations

This backend is intended for development, testing, and low-volume workloads.
File operations are synchronous, including those invoked through async traits.

- **Opening an existing path with `new` or `try_new` truncates the file.** Do not
  use these constructors to recover persisted tasks or open valuable files.
- Use a private, trusted storage directory. Rewrites use a predictable sibling
  `.tmp` path and follow existing symlinks, which can overwrite unrelated files
  if another user or process can modify that directory.
- Temporary storage currently leaves its recreated file behind and does not
  preserve the temporary file's restrictive permissions. Avoid sensitive data
  with `new_temp`; use a caller-owned private temporary directory and `new`.
- New instances use immediate flushing. `SyncPolicy` also describes periodic
  and manual policies, but the public API currently has no policy setter.
- Clones share entries and pending changes, but have independent read cursors.
  They do not implement exclusive task claiming. After a rewrite, file handles
  can diverge between clones; pending changes can also be lost on write errors.
  Avoid concurrent writers and do not rely on crash-safe durability.
- Exhausted streams do not register a wake-up when tasks are appended. Enqueue
  tasks before running workers; live producer/consumer use can stall.
- The CSV adapter splits on commas without quoting or escaping. Its flattened
  representation does not preserve all JSON types, keys, or changing schemas.
  Prefer JSON for structured data. CSV is not a general-purpose interchange
  format in this implementation.
- Completion queries can report `Done` even when the stored handler result is
  `Err`; inspect the returned result as well as its status.
- Some completion and shared-store error paths can panic or terminate a stream.
  Inputs and stored result types must match the expected task types.

These limitations are retained in this implementation and require separate
changes before use as a durable production queue.
