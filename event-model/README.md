# event-model

The **Event Model** layer of the OTK Protocol stack: canonical Open Timekeeping event types and identifiers, transport-independent.

> **Status: active.** Core types are defined. See [open questions](#open-questions) for what remains unsettled.

## What this is

The shared, transport-neutral definition of every canonical event and identifier in the system: detector events (`Detection`), detector health events, timebase status events, adapter metadata events, stream descriptors, and the common identifiers tying them together (`DetectorId`, `TimingPointId`, `SubjectId`, sequence numbers, timestamp fields, etc.).

Used by detector adapters, timing-node, timing-core, simulators, conformance fixtures, and the ingest/plugin client libraries. Stays free of runtime, server, networking, framing, transport, and storage logic.

## Where this sits in the OTK Protocol stack

```text
Event Model      -> event-model               <-- this crate
Wire Protocol    -> otk-protocol
Frame Codec      -> frame-codec
Transport Binding-> timing-core::ports::inbound + adapter-ingest-* implementations
```

The types defined here are what the OTK message envelope wraps. They have no transport assumptions; the same `Detection` is canonical whether it travels over TCP, USB CDC, or never leaves a runtime node's process at all.

## Design principles

**One event shape.** All detector events, raw sensor signals, firmware-processed passings, and timing-core output, are `Detection`. The stream (topic) they live on carries the resolution level, not the type. Raw signal streams, processed detection streams, and timing-core output streams are all `Detection` events on different streams.

**Separation of mechanism and policy.** Events carry what happened and how trustworthy it is. The event model does not filter or suppress based on quality. Policy on what is acceptable for official results is decided at the app, scorer, or operator layer.

**Serialization: CBOR via `minicbor`.** Single format across embedded firmware and server-side code. `no_std`-friendly.

## What belongs here

- `Detection`: the single canonical event type for all timing observations.
- `SensorData`: sensor-specific metadata as an enum field on `Detection` (loop/transponder, beam break, manual). No spurious nullables.
- `StreamDescriptor` and `StreamKind`: define what a stream is and its resolution level.
- Common identifier types (`DetectorId`, `TimingPointId`, `SubjectId`, `TimebaseId`, etc.) as typed string newtypes.
- Timestamp fields (`detected_at_ns: u64`, `detected_at_uncertainty_ns: Option<u64>`, `received_at_ns: Option<u64>`).
- `TimestampingMethod`, `SourceAttestation`, `SyncState` enums.
- Provenance fields (timestamping method, timebase reference, source attestation).
- Timebase references are deployment-level identities, addressable across nodes. Two events that reference the same timebase identity assert directly comparable timestamps. See [`spec/architecture.md § Timebases are physical references`](../spec/architecture.md).
- `DetectorHealthEvent`, `TimebaseStatusEvent`, `AdapterMetadataEvent`.
- `OtkEvent`: top-level enum wrapping all event kinds for the wire protocol.

## What does not belong here

- OTK message envelope and protocol-level message types: [`otk-protocol`](../otk-protocol).
- Encode/decode of OTK messages into byte frames: [`frame-codec`](../frame-codec). Used by every transport-binding adapter; also `no_std` + `alloc`-friendly so it can run inside firmware producers.
- Transport-specific code (sockets, USB enumeration, etc.): [`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) in `timing_core::ports::inbound` and the per-transport `adapter-ingest-*` crates ([`adapter-ingest-tcp`](../adapter-ingest-tcp), [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket)).
- Trait contracts for detector adapters and timebases: [`otk-contracts`](../otk-contracts).
- Producer-side connection / retry helpers: [`otk-sdk`](../otk-sdk) (its `producer` feature).
- Runtime ingestion, projection, or storage logic: [`timing-node`](../timing-node), [`timing-core`](../timing-core).

## Dependencies

**Depends on:** [`spec/`](../spec) for terminology.

**Commonly depended on by:** [`otk-protocol`](../otk-protocol), [`frame-codec`](../frame-codec), [`otk-contracts`](../otk-contracts), [`otk-sdk`](../otk-sdk), [`timing-core`](../timing-core), [`timing-node`](../timing-node), every adapter, the simulator, conformance.

Within this Cargo workspace, members depend on `event-model` via `event-model = { path = "../event-model" }` or `event-model = { workspace = true }`.

## Relationship to the architecture

Every event flowing through the Timing Fabric, produced by any detector adapter, consumed by any runtime node, is described by a type defined here. If a type isn't in `event-model`, it isn't part of the canonical contract.

## Open questions

- `detected_at` vs `received_at` required-vs-optional rules per sensor tier.
- Sequence-number scope: per-detector monotonic is the current assumption; confirm before the OTK Wire Protocol (see [`otk-protocol`](../otk-protocol)) stabilises.
- Confidence representation: float 0..1 or enum of quality buckets.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
