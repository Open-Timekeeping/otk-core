# adapter-event-log-segment

Segment-file event log backend for Open Timekeeping. Implements the `EventLog` trait from [`port-out-event-log`](../port-out-event-log) using fixed-budget segment files on local disk.

> **Status: active.** Core implementation is complete: segment format, offset index, retention, crash recovery, live subscriptions. See [open questions](#open-questions) for deferred items.

## What this is

`adapter-event-log-segment` is an outbound adapter in the OTK hexagonal architecture. It implements the `EventLog` trait from `port-out-event-log` using an append-only sequence of fixed-budget segment files on local disk, with companion offset indexes, retention enforcement by age and size, and live-subscribe semantics. It is the v0 storage backend; alternative backends can be added later behind the same port contract.

## Where this sits in the architecture

```text
port-out-event-log/          outbound port contract (EventLog trait)
adapter-event-log-segment/   implements port-out-event-log     <-- this crate
timing-node/                 injects this adapter at startup
```

The timing node's **pipeline logic** depends on the `EventLog` trait from [`port-out-event-log`](../port-out-event-log), never on this crate's concrete `SegmentLog` type. `timing-node` itself, as the composition root, does pull this crate in as a Cargo dependency to construct the concrete backend and hand it to the pipeline behind the trait object. Swapping in an alternative storage backend (a different `port-out-event-log` impl) only touches the composition root; the pipeline is unchanged.

## Design decisions

**Segment file format.** Each segment file (`{base_offset:020}.seg`) starts with a 24-byte header (magic `OTKS`, version, flags, base_offset, created_at_ns). Records are length-prefixed: `payload_len (u32)` + `offset (u64)` + `appended_at_ns (u64)` + `event_len (u32)` + CBOR-encoded `OtkEvent` + `crc32 (u32)`. CRC32 covers all bytes from `offset` through the end of the CBOR payload. Closed segments end with a 4-byte zero sentinel.

**Offset index.** Each closed segment has a companion `{base_offset:020}.idx` file: a flat array of u64 LE file positions, one per record. Written atomically (temp-file + rename) at segment close. The active segment keeps its index in memory.

**No time index for v0.** `read_range` takes `Offset`; timestamp-range reads are deferred until the API grows that variant.

**fsync policy.** `sync_all()` is called once per `append()` when `flush_interval_ms == 0` (default). Setting `flush_interval_ms > 0` skips per-append fsync.

**Segment rolling.** The active segment rolls when it exceeds `max_segment_bytes` (default 64 MiB) or `max_segment_age_secs` (default 3600 s). Retention enforcement runs after every roll.

**Live subscriptions.** `Arc<Notify>` fired on every successful `append()`. Each subscription reads from the filesystem independently; segment rolls are transparent.

**Crash recovery.** On open, the active segment is scanned record-by-record, CRC32 is verified, and the file is truncated at the first corrupt or incomplete record.

## Usage

```rust
use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
use port_out_event_log::{EventLog, RetentionPolicy};
use std::path::PathBuf;

async fn open_log() -> Result<(), port_out_event_log::StorageError> {
    let config = SegmentLogConfig {
        dir: PathBuf::from("/var/lib/otk-node/log"),
        retention: RetentionPolicy::TimeBased { max_age_secs: 86400 },
        ..Default::default()
    };
    let mut log = SegmentLog::open(config).await?;
    Ok(())
}
```

## Dependencies

**Depends on:** [`port-out-event-log`](../port-out-event-log), [`event-model`](../event-model), `async-trait`, `minicbor`, `crc32fast`, `tokio`.

**Used by:** [`timing-node`](../timing-node) as its default storage backend.

## Open questions

- **Background fsync task.** `flush_interval_ms > 0` skips per-append `sync_all()`, but there is no background flusher yet. Durability relies on OS write-back with no timer-based safety net.
- **Time index.** Index by `appended_at_ns` for timestamp-range reads. Deferred until `port-out-event-log` adds a timestamp-range variant.
- **Periodic retention enforcement.** Currently only runs after a segment roll.
- **`read_range` pagination.** Large replay reads return `Vec<LogEntry>`. A streaming variant is planned.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
