# Open questions

Unresolved technical decisions, tracked here so we stop guessing and decide deliberately. Each item belongs to one or more crates / repos; when resolved, the answer lands in the relevant code's spec or README, the open bullet is dropped, and a short "Resolved:" line is added under the same section as historical context so reviewers can see what was decided without leaving this file. Once a section's resolved notes have outlived their reference value (typically once they're cross-referenced from the source/README they describe), they get pruned in a later sweep.

---

## OTK Protocol stack

The OTK Protocol is split across four layers, three of which live as crates inside [`otk-core`](https://github.com/Open-Timekeeping/otk-core) (`event-model`, `otk-protocol`, `frame-codec`); the fourth (transport binding) lives in per-transport adapter repos (`adapter-ingest-tcp`, `adapter-ingest-unix-socket`, …).

### Wire protocol, `otk-core/protocol` (crate `otk-protocol`)

- **Backward-compatibility policy across protocol versions.** The handshake now performs version negotiation; what's the long-term shape for breaking vs additive protocol revisions?
- **In-process plugin path: does it use the same envelope (for testability symmetry) or skip the envelope and use direct trait calls?**

Resolved:
- Serialization format: CBOR via `minicbor`. Single format across embedded firmware and server.
- Handshake / registration message shape: `Connect` / `ConnectAck` / `ConnectReject` envelopes carrying CBOR-encoded payloads, with optional `auth_token` field on `Connect`. (Defined in [`otk-core/protocol/src/handshake.rs`](https://github.com/Open-Timekeeping/otk-core/blob/main/protocol/src/handshake.rs).)

### Frame codec, `otk-core/frame-codec`

Resolved:
- Stream framing for reliable transports: length-prefixed (`u32` BE). Implemented in `StreamFrameDecoder`.
- Resynchronizable framing for byte streams: COBS + CRC-16/CCITT-FALSE, `0x00` delimiter. Implemented in `SerialFrameDecoder`.
- `no_std` + `alloc`; same crate covers server and firmware.

Open:
- **Max-message-size policy on embedded targets.** Length-prefix and COBS both accept a configurable cap; what's the recommended default for resource-constrained firmware?

### Transport bindings, `adapter-ingest-*`

- **Which bindings are first-class for v0.** Current prioritization: `adapter-ingest-tcp` (shipped), `adapter-ingest-unix-socket` (shipped). Deferred: serial, USB-CDC, raw Ethernet, CAN, RS-485, QUIC, MQTT, WebSocket.
- **WebSocket as a separate binding** for browser-debuggable producers, or out of scope at the producer layer entirely.

Resolved:
- Common abstraction shape: [`port-in-ingest`](https://github.com/Open-Timekeeping/otk-core/tree/main/port-in-ingest)'s `EventIngestPort` + `IngestSession` traits. Handshake and post-handshake dispatch reusable via [`ingest-protocol`](https://github.com/Open-Timekeeping/otk-core/tree/main/ingest-protocol).

## Event model, `otk-core/event-model`

- **Uncertainty model.** Bounded interval, standard deviation, or distribution-aware?
- **`detected_at` vs `received_at` semantics.** Which is required vs optional on each event class? When can `detected_at` be omitted?
- **Confidence representation.** Float 0..1, enum of buckets, or both with the float as canonical?

Resolved:
- Timestamp representation: flat fields `detected_at_ns: u64` + `detected_at_uncertainty_ns: Option<u64>`.
- Identifier types: typed string newtype wrappers (`DetectorId(String)`, etc.).
- Event shape: single `Detection` type. The stream (topic) carries the resolution level, not the type. `SensorData` enum on `Detection` handles sensor-specific fields without spurious nullables.
- Sequence-number scope: per-`(producer_id, detector_id)`. Enforced at runtime by the `SequenceGate` middleware in `timing-node`.

## Detector adapter / timebase contracts, `otk-core/otk-contracts`

- **Required-vs-optional sub-traits for capability tiers** (raw-hits-only, passings-only, hits-and-passings, replay-only, manual-only).
- **Minimum health-reporting cadence.**

Resolved:
- Trait shape: async `next_event` returning a small `AdapterEvent` / `TimebaseEvent` enum. The first event after `start` must be `Metadata`. (Conformance asserts the invariant.)
- Producer-side reconnect resume: monotonic per-`(producer_id, detector_id)` sequence numbers; runtime side is authoritative via `SequenceGate`. Restart persistence (gate seeded from segment-log replay) is a follow-up.
- Timebase contract: lives alongside `DetectorAdapter` in `otk-contracts`. (`timebase-api` as a separate repo was deleted; timebases are producers using `otk-sdk`.)

## Timebase profiles

The timebase model is settled in [architecture.md § Timebases are physical references](architecture.md). What's still open:

- **Granularity of `sync_state`.** Five-state enum (`Locked`, `Holdover`, `FreeRun`, `Unsynchronized`, `Unknown`) is the current shipped shape; richer state machine if a deployment forces our hand.
- **Runtime-discovery contract per profile.** What exactly does a PTP timebase read from `ptp4l`/`pmc` to expose the grandmaster Clock-ID? What does a GNSS timebase read from the receiver to expose constellation identity? What does an NTP timebase read from `chronyd` for the refid chain?
- **Operator-policy hooks for "accept this degraded timebase as official."**
- **Whether two detectors may legitimately claim the same `runtime_discovered` identity from different receivers** (e.g., two GNSS receivers both reporting "GPS system time"). Probably yes, since they truly are on the same constellation, but the spec should be explicit.

## Plugin API, `plugin-api` (not yet specified)

- **Plugin loading model.** Statically linked (recompile to add a plugin), dynamic loading (`.so` / `.dll`), or WebAssembly modules.
- **Hot-reload vs restart-only.**
- **Plugin-to-plugin communication policy.**
- **Sandboxing for untrusted plugins** (probably out of scope at first).

## Runtime, `timing-node`

- **Consumer offset tracking model.** Two viable shapes for the outbound API: (a) **consumer-tracked offsets**, Kafka-style: the consumer remembers where it left off, supplies it on reconnect, and the node serves from there; the node holds no per-consumer state. (b) **node-tracked offsets**, AMQP-style: the node tracks a "last acknowledged offset" per named consumer, so a fresh process under the same identity can pick up cleanly with no out-of-band coordination. (a) is simpler and stateless in the node; (b) is friendlier to scoring-app-style consumers that may be restarted by an operator and don't want to persist their own state.
- **Default retention policy and trade-offs.** Retention is the bound on consumer outage tolerance. What's the shipped default, and how is it expressed in config (time / size / hybrid)? `RetentionPolicy::Indefinite` is the current default.
- **Bootstrap response payload.** Confirmed in [architecture.md § Multi-node deployments are partitioning, not clustering](architecture.md) that nodes answer a bootstrap query with a directory of the deployment. The exact response schema (per-node fields, capability declarations, freshness/TTL semantics, whether bootstrap can return partial views) is still to be designed.
- **Configuration format and hot-reload policy.** TOML format is shipped; hot-reload is not.
- **Sequence-gate restart persistence.** The gate's high-water marks are in-memory; on restart, a producer that reconnects with a regressed sequence number could re-enter events that were previously deduplicated. Seeding the gate from a segment-log replay on startup is a follow-up.
- **TLS for the TCP transport.** Deferred; deployments needing wire encryption today should run OTK over an SSH tunnel or WireGuard. Native rustls support is planned.
- **W3C `traceparent` propagation through `OtkEnvelope`.** Deferred; today each side carries its own `tracing` span without cross-wire context.

Resolved:
- Multi-listener config: `[[listeners]]` array of `ListenerConfig::Tcp { … }` / `UnixSocket { … }`. All listeners feed the same canonical ingest pipeline.
- Auth: shared-secret tokens in `Connect.auth_token`; server-side `ConnectAuthoriser` trait with `AllowAll` default + token allow-list config. Bearer-token middleware on the REST/SSE API.
- Operational endpoints: `/healthz`, `/readyz`, `/metrics` (Prometheus text). Hand-rolled labelled counters and gauges.
- Stream naming convention: `StreamKind` in `event-model` carries the semantic level (`Raw`, `Detections`, `Processed`); concrete `StreamId` is a configurable per-deployment value.

## Storage, `port-out-event-log` and `adapter-event-log-segment`

The v0 backend is a custom segment-file log ([`adapter-event-log-segment`](https://github.com/Open-Timekeeping/adapter-event-log-segment)). Storage stays pluggable behind [`port-out-event-log`](https://github.com/Open-Timekeeping/otk-core/tree/main/port-out-event-log); alternative backends (embedded SQL, server SQL, object-store-tiered) can be added behind the same trait when there is a concrete need.

- **Background fsync task.** Per-`append` `sync_all()` is the default. Setting `flush_interval_ms > 0` skips fsync and currently has no timer-based safety net; a background flusher is planned.
- **Time index.** Index by `appended_at_ns` for timestamp-range reads. Deferred until `port-out-event-log` adds a timestamp-range read variant.
- **Periodic retention enforcement.** Currently only runs after a segment roll; long-lived runtime with no rolls drifts past its time-based retention budget.
- **`read_range` streaming variant.** Large replay reads return `Vec<LogEntry>`; a streaming/paginated variant is planned.
- **Actor-vs-mutex for the runtime's storage path.** The current `NodePipeline` uses `tokio::sync::Mutex<Box<dyn EventLog>>` with a documented lock discipline (the synchronous `CrossingProcessor` mutex is never held across an `.await`). A single-owner actor task could outperform the mutex under high-rate ingest; deferred until benchmarks justify it.

Resolved:
- Segment file format: 24-byte header (magic `OTKS`, version, flags, base_offset, created_at_ns); length-prefixed records with CRC32; closed segments end with a 4-byte zero sentinel. Companion `.idx` offset index written atomically on segment close.
- `fsync` policy: per-append `sync_all()` by default; configurable via `flush_interval_ms`.
- Retention enforcement: `RetentionPolicy::{Indefinite, TimeBased, SizeBased, Hybrid}`; eviction runs after every segment roll; `read_range` / `subscribe` against an evicted offset returns `StorageError::RetentionExpired { requested, earliest_available }`.
- Crash recovery: scan active segment, verify CRC32 per record, truncate at first corrupt or incomplete record.
- Time index key: `appended_at_ns` is the canonical storage time. Consumers re-sort by `detected_at` when they need to.

## Conformance, `conformance`

- **How conformance tests connect to physical devices.** Soft-real-time loop with a programmable signal source + reference timestamps? Hardware-in-the-loop rig at one venue, reproducible recordings for everyone else?
- **Are vendor-specific conformance test packs maintained here or in the vendor's adapter repo?**

Resolved:
- The suite ships as a single crate with per-contract test files (`event_model_roundtrip`, `wire_protocol_handshake`, `event_log_contract`, `event_log_retention`, `frame_codec_contract`, `ingest_protocol_contract`, `contracts_dyn_safety`). Built around an in-crate `MemLog` reference `EventLog` impl with `evict_below` / `evict_all` test helpers so retention paths are deterministic.

## Embedded

The reference firmware toolkit (HAL, target-specific crates, native detector firmware) is not yet shipped. The protocol-layer crates that firmware would consume (`event-model`, `otk-protocol`, `frame-codec`) are all `no_std` + `alloc` and ready for embedded use.

- **Which embedded target comes first?** RP2040 (cheap, hobbyist-friendly, excellent PIO) vs STM32 (mature timer capture, industrial availability).
- **Upstream embedded ecosystem choices.** `embassy-*` (async) vs `rtic` vs traditional `*-hal` crates.
- **Allocator policy: `alloc` allowed? `heapless`-only? Conditional?**

## First-class adapters

- **Which adapters are first-class beyond what's stubbed now**: serial / USB-CDC ingest, manual-entry, CSV replay. Vendor-specific adapters (MYLAPS, RaceResult, etc.) are explicitly **out of scope** at this stage and may never be appropriate depending on protocol, licensing, and access constraints.

## Hardware

- **Reference hardware license.** Apache-2.0 covers the source files; final hardware-design license may shift to CERN-OHL-S or TAPR OHL.
- **Loop / RF front-end design**, TBD with electrical-engineering review.

---

## Resolution discipline

When an item is decided:

1. The decision lands in the relevant crate's spec, README, or trait file.
2. The corresponding bullet is removed from this file.
3. The PR that lands the decision references this file's previous entry in its description.

If a decision is reversed, add it back with a note about why.
