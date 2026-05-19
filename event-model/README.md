# event-model

The **Event Model** layer of the OTK Protocol stack: canonical Open Timekeeping event types and identifiers, transport-independent.

> **Status: active.** Core types are defined. See [open questions](#open-questions) for what remains unsettled.

## What this is

The shared, transport-neutral definition of every canonical event and identifier in the system: detector events (`Detection`), detector health events, timebase status events, adapter metadata events, stream descriptors, and the common identifiers tying them together (`DetectorId`, `TimingPointId`, `SubjectId`, sequence numbers, timestamp fields, etc.).

Used by detector adapters, timing-node, timing-core, simulators, conformance fixtures, and the ingest/plugin client libraries. Stays free of runtime, server, networking, framing, transport, and storage logic.

## Where this sits in the OTK Protocol stack

```text
Event Model      -> event-model               <-- this crate
Wire Protocol    -> protocol
Frame Codec      -> adapter-ingest-tcp (server), embedded-wire (firmware)
Transport Binding-> port-in-ingest + adapter-* implementations
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
- Timebase references are deployment-level identities, addressable across nodes. Two events that reference the same timebase identity assert directly comparable timestamps. See [`spec/architecture.md § Timebases are physical references`](https://github.com/Open-Timekeeping/spec/blob/main/architecture.md).
- `DetectorHealthEvent`, `TimebaseStatusEvent`, `AdapterMetadataEvent`.
- `OtkEvent`: top-level enum wrapping all event kinds for the wire protocol.

## What does not belong here

- OTK message envelope and protocol-level message types, in [`protocol`](../protocol).
- Encode/decode of OTK messages into byte frames, in `adapter-ingest-tcp` (server side) and [`embedded-wire`](../embedded-wire) (firmware side).
- Transport-specific code (sockets, USB enumeration, etc.), in [`port-in-ingest`](../port-in-ingest) and `adapter-*` implementations.
- Trait definitions for adapters or plugins, in [`detector-adapter-api`](../detector-adapter-api), [`timebase-api`](../timebase-api), [`plugin-api`](../plugin-api).
- Producer-side connection / retry helpers, in [`otk-ingest-client`](../otk-ingest-client).
- Application-layer DTOs for apps and external consumers, in [`api-model`](../api-model).
- Runtime ingestion, projection, or storage logic, in [`timing-node`](../timing-node), [`timing-core`](../timing-core).

## Dependencies

**Depends on:** [`spec`](../spec) for terminology.

**Commonly depended on by:** [`protocol`](../protocol), [`embedded-wire`](../embedded-wire), [`otk-sdk`](https://github.com/Open-Timekeeping/otk-sdk), [`timebase-api`](../timebase-api), [`plugin-api`](../plugin-api), [`timing-core`](../timing-core), [`timing-node`](https://github.com/Open-Timekeeping/timing-node), every adapter, the simulator, conformance.

For local Rust development, sibling crates depend via `event-model = { path = "../event-model" }`.

## Relationship to the architecture

Every event flowing through the Timing Fabric, produced by any detector adapter, consumed by any runtime node, is described by a type defined here. If a type isn't in `event-model`, it isn't part of the canonical contract.

## Open questions

- `detected_at` vs `received_at` required-vs-optional rules per sensor tier.
- Sequence-number scope: per-detector monotonic is the current assumption; confirm before wire-protocol stabilises.
- Confidence representation: float 0..1 or enum of quality buckets.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
