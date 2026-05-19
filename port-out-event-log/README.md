# port-out-event-log

Outbound port contract for OTK event log persistence: the trait every storage backend implements.

> **Status: active.** Trait shape and lifecycle semantics are settled. See [open questions](#open-questions) for what remains.

## What this is

`port-out-event-log` is an outbound port in the OTK hexagonal architecture. It defines the boundary between the timing-node application and its persistence layer. The application calls this contract; concrete storage backends implement it.

An **event log** is an append-only sequence of `OtkEvent` values. Each event is assigned a monotonic `Offset` by the backend on append. Downstream consumers reconnect by supplying the offset after the last one they successfully processed; the log replays from that point or returns a structured `RetentionExpired` error if the range has been evicted.

Concretely:

- **`EventLog`**: the core persistence trait. Append events, read ranges, subscribe to live delivery, query current bounds.
- **`LogSubscription`**: a live subscription returned by `EventLog::subscribe`. Poll `next_entry` until `None` (closed) or `Some(Err(_))` (terminal error).
- **`LogEntry`**: a stored event with its `Offset` and receipt timestamp.
- **`Offset`**: a monotonic `u64` position in the log.
- **`RetentionPolicy`**: `Indefinite`, `TimeBased`, `SizeBased`, `Hybrid`.
- **`StorageError`**: error vocabulary, including the structured `RetentionExpired` variant.

## Where this sits in the architecture

```text
server/
  core/
    event-model/              domain DTOs (OtkEvent, Detection, ...)
  ports/
    port-out-event-log/       outbound port contract   <-- this repo
  adapters/
    adapter-event-log-segment/  implements port-out-event-log (v0 backend)
  app/
    timing-node/              depends on this port; injects the adapter
```

## Design decisions

**Poll-based `next_entry()`**, not a Stream. Consistent with `DetectorAdapter::next_event()` and `Timebase::next_event()`. Returns `Option<Result<...>>`: `None` = closed, `Some(Err(_))` = terminal error.

**`&mut self` on all methods.** The runtime wraps the backend in a `Mutex` if it needs shared access across tasks.

**`OtkEvent` as the stored type.** The log stores canonical decoded events, not raw OTK frames.

**`RetentionExpired` is a structured error.** Carries the requested offset and `earliest_available: Option<Offset>` so consumers can re-establish their position.

**`Offset` is a newtype over `u64`.** Provides ordering, `Display`, and prevents raw integer misuse.

**std-only.** Async traits via `async-trait` require `std`.

**No transport dependency.** Only depends on `event-model`.

## Source layout

```
src/
  lib.rs        crate doc, re-exports
  offset.rs     Offset
  error.rs      StorageError
  entry.rs      LogEntry
  retention.rs  RetentionPolicy
  log.rs        LogSubscription, EventLog
```

## Dependencies

**Depends on:** `event-model`.

**Implemented by:** [`adapter-event-log-segment`](../adapter-event-log-segment) (v0 segment-file backend).

**Used by:** [`timing-node`](../timing-node) (injects the adapter at startup).

## Open questions

- **`read_range` pagination.** Large replay reads currently return `Vec<LogEntry>`. A paginated or streaming variant may be needed for consumers replaying hours of data.
- **Conformance helper shape.** A test suite backends can run against themselves to verify correct implementation.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
