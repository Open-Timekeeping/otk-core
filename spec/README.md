# spec

The Open Timekeeping standard and conceptual model. Defines terminology, semantics, and the architectural shape every implementation must conform to.

> **Status: standards seed.** Initial terminology and conceptual content lives here; technical specs are added as decisions land. Implementation details belong in their respective implementation crates within this workspace, not here.

## What this is

`spec` is the human-readable, implementation-neutral source of truth for what *Open Timekeeping* means. It explains the conceptual model behind the system, detectors, timing points, detections, crossings, timebases, runtime nodes, the Timing Fabric, without prescribing how any particular implementation builds them.

A correct implementation of any Open Timekeeping role is one that can be described in the terms this directory defines and that passes the [`conformance`](../conformance) suite.

## What belongs here

- Terminology and glossary (detection, hit, crossing, timing point, detector, transponder, entrant, timebase, OTK Protocol layers, downlink, standings, etc.).
- Conceptual architecture (the Timing Fabric, runtime nodes, detector adapters, timebase, producers, the four OTK Protocol layers).
- Bidirectional / downlink architecture (fabric-to-decoder standings push, decoder-to-transponder instant downlink, in-vehicle CAN-out, asymmetric device capabilities, decoder and transponder firmwares as first-class applications). See [downlink.md](downlink.md).
- Networking and deployment topologies (native detector producer speaking OTK directly, edge gateway, hub plugin, same-host adapters over Unix socket, per-stack node, central hub with multiple ingest listeners).
- Compatibility and conformance overview (what it means to be Open Timekeeping compatible).
- Cross-cutting principles (honest provenance, immutability, mechanism vs policy, domain neutrality, transport-independent protocol).

## What does not belong here

- OTK message envelope byte layouts (→ [`otk-protocol`](../otk-protocol)).
- Frame encode/decode and framing format, including the `no_std` + `alloc` path used by firmware (→ [`frame-codec`](../frame-codec)).
- Transport-binding-specific code (→ per-transport adapter crates: [`adapter-ingest-tcp`](../adapter-ingest-tcp), [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket), and future serial / USB-CDC / RS-485 / etc.).
- Concrete event/schema definitions (→ [`event-model`](../event-model)).
- Trait signatures, function names, or other implementation API surface (→ the respective contract crate: [`otk-contracts`](../otk-contracts), [`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) in `timing_core::ports::inbound`, [`EventLog`](../timing-core/src/ports/outbound/event_log.rs) in `timing_core::ports::outbound`, [`ingest-protocol`](../ingest-protocol)).
- Runtime configuration, deployment guides, or operational runbooks (→ [`timing-node`](../timing-node) and apps).
- Vendor-specific protocol details (→ vendor-specific adapter crates, if and when they exist).

## Dependencies

**Depends on:** none. `spec` is at the top of the conceptual graph.

**Commonly depended on by:** every implementation crate references `spec` for terminology; [`conformance`](../conformance) bases its suite on it.

## Relationship to the architecture

`spec` is the standard. Everything else is an implementation of, against, or in service of what it describes. If `spec` and code disagree, that is a bug, either the spec needs amending or the code needs correcting.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
