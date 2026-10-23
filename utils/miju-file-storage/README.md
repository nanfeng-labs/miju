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

`new` and `try_new` load existing records and results without truncating the
file. Invalid records return an error without changing its contents. Each new
instance starts reading at the first record, including records with results.
`new_temp` retains the temporary file's restrictive permissions and removes it
when the last storage clone is dropped.

Clones share entries, pending changes, and the active file handle. Appending a
task wakes waiting readers. Result updates replace the file using a securely
created temporary file in the same directory, retaining file permissions.
Failed writes retain pending changes for a later `flush`; retrying an append
resumes at its original offset. A failed flush can leave a partial record on
disk, so keep the storage alive and retry after correcting the I/O failure.

`SharedJsonStore` creates typed `MemoryStorage` handles backed by one temporary
JSON store. It separates streams by task type metadata. These handles use the
memory backend's execution contract; they do not provide the file backend's
completion acknowledgement middleware.

## Current Limitations

This backend is intended for development, testing, and low-volume workloads.
File operations are synchronous, including those invoked through async traits.

- Use a private, trusted storage directory. External changes to the file or its
  directory are not coordinated with the in-memory state.
- New instances use immediate flushing. `SyncPolicy` also describes periodic
  and manual policies, but the public API currently has no policy setter.
- Clones have independent read cursors and do not implement exclusive task
  claiming. Independently opening the same path does not share in-memory state
  or file-handle replacement; use one storage and its clones per file.
- Concurrent operations on clones can return `WouldBlockLock`. An I/O failure
  can occur after a mutation is staged in memory; retry `flush` to persist it.
- Flushing does not sync the file or directory to durable media. Cross-process
  coordination, crash or power-loss recovery, and exactly-once execution are
  not provided. Dropping storage does not flush pending changes.
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
