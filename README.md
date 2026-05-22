# Open Timekeeping

An open-source timing system spanning hardware, firmware, detector adapters, timebase / clock sync, a runtime node, the timing core, APIs, and conformance. Designed first for motorsport; the same primitives serve athletics, cycling, rowing, karting, RC racing, industrial checkpoints, and similar contexts.

This repository is a single Cargo workspace containing every Rust crate that ships together at v0. The conceptual model lives in [`spec/`](spec/), start with [`spec/architecture.md`](spec/architecture.md).

## Workspace layout

```
open-timekeeping/
├── spec/                       Design specification (markdown, normative)
│
│   Contract crates (no_std + alloc where applicable; no transport or runtime deps)
├── event-model/                Canonical event types and identifiers
├── otk-protocol/               Wire protocol DTOs: envelope, handshake, messages
├── frame-codec/                Frame encode/decode (length-prefix stream + COBS serial)
├── ingest-protocol/            Transport-agnostic server-side handshake / dispatch
├── port-in-ingest/             Inbound port trait: EventIngestPort, IngestSession
├── port-out-event-log/         Outbound port trait: EventLog, LogSubscription
├── otk-contracts/              Detector adapter and timebase trait contracts
├── timing-core/                Detection-to-crossing engine (a library, not a server)
│
│   Adapters (concrete implementations of the port traits)
├── adapter-ingest-tcp/         TCP transport binding
├── adapter-ingest-unix-socket/ Unix-socket transport binding (cfg(unix))
├── adapter-event-log-segment/  Append-only segment-file storage backend
│
│   SDK and producers
├── otk-sdk/                    Producer + consumer SDK
├── producer-simulated/         Reference producer (binary: otk-simulator)
│
│   Runtime
├── timing-node/                Timing Runtime Node (binary: otk-node)
│
│   Conformance
├── conformance/                Test suite verifying implementations against the contracts
└── conformance-fixtures/       Test data corpus (stub)
```

## Quick start

```bash
# Build everything
cargo build --workspace

# Run the full test suite
cargo test --workspace --all-targets

# Run a node with a TOML config
cargo run -p timing-node -- --config example.toml

# Run the simulated producer against a running node (uses one of the
# shipped example configs; producer-simulated reads its node address,
# event count, and timing pattern from the TOML).
cargo run -p producer-simulated -- --config producer-simulated/sim-start.toml
```

## Binaries

| Binary | Crate | Role |
|---|---|---|
| `otk-node` | `timing-node` | The Timing Runtime Node. Hosts ingest listeners, the crossing processor, the event log, and the HTTP API. |
| `otk-simulator` | `producer-simulated` | Synthetic detector producer for development, testing, and demos. |

## License

Apache-2.0.
