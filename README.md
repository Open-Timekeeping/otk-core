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
├── otk-contracts/              Detector adapter and timebase trait contracts
├── timing-core/                Domain engine, port traits (inbound + outbound), and application services
│                               (the hexagon: domain/, ports/{inbound,outbound}/, services/)
│
│   Adapters (concrete implementations of the port traits)
├── adapter-ingest-tcp/         TCP transport binding (optional TLS / mTLS)
├── adapter-ingest-unix-socket/ Unix-socket transport binding (cfg(unix))
├── adapter-event-log-segment/  Append-only segment-file storage backend
│
│   SDK, producers, and tooling
├── otk-sdk/                    Producer + consumer SDK
├── producer-simulated/         Reference producer (binary: otk-simulator)
├── otk-devcerts/               One-shot dev cert generator (TLS / mTLS demos)
│
│   Runtime
├── timing-node/                Timing Runtime Node and composition root (binary: otk-node)
│
│   Conformance
└── conformance/                Test suite verifying implementations against the contracts
```

## Getting started

The three demos below are the supported way to see a node, a producer,
and the event log working together. Every command **runs from the
workspace root** (this directory); relative paths in the shipped sample
configs (`./dev-certs/`, `./data/`) resolve from there. The dev-cert
bundle (`./dev-certs/`) and the runtime's segment log (`./data/`) are
both git-ignored and safe to delete between runs.

### 1. Plaintext TCP (zero-config)

The simplest path. The runtime node binds plain TCP on `127.0.0.1:8463`
by default, the simulator connects to the same address by default. Two
terminals:

```sh
# Terminal 1: run the node with built-in defaults.
cargo run -p timing-node --bin otk-node

# Terminal 2: run the simulator with one of the shipped sample configs.
cargo run -p producer-simulated --bin otk-simulator -- \
    --config producer-simulated/sim-start.toml
```

You can launch additional simulator instances against the same node
with different sample configs (e.g. `sim-finish.toml`); each one
becomes a separate producer session.

Inspect what landed in the log over HTTP:

```sh
curl http://127.0.0.1:8080/api/v1/status
curl 'http://127.0.0.1:8080/api/v1/events?from=0&limit=10'
```

### 2. Server-authenticated TLS

The node presents a TLS server certificate; the producer trusts the
matching CA. No client cert. Edit the shipped TLS configs to comment out
the client-cert lines if you only want server-auth, or just run the
mTLS flow below (mTLS is a strict superset).

```sh
# 1. Emit a fresh dev cert bundle into ./dev-certs/.
cargo run -p otk-devcerts -- --out ./dev-certs

# 2. Start the TLS-enabled node.
cargo run -p timing-node --bin otk-node -- \
    --config timing-node/node-tls.toml

# 3. In another terminal, start a producer over TLS.
cargo run -p producer-simulated --bin otk-simulator -- \
    --config producer-simulated/sim-start-tls.toml
```

To take the deployment from mTLS down to server-auth-only, comment out
the `client_ca` line in [`timing-node/node-tls.toml`](timing-node/node-tls.toml)
and the `client_cert` / `client_key` lines in
[`producer-simulated/sim-start-tls.toml`](producer-simulated/sim-start-tls.toml).

### 3. Mutual TLS

The shipped TLS sample configs are wired for mTLS out of the box. The
same three commands as the server-auth flow:

```sh
cargo run -p otk-devcerts -- --out ./dev-certs
cargo run -p timing-node --bin otk-node -- --config timing-node/node-tls.toml
cargo run -p producer-simulated --bin otk-simulator -- --config producer-simulated/sim-start-tls.toml
```

Connection requirements (handled by the shipped configs):

- The producer's `[tls] trust_roots` matches the server CA in
  `./dev-certs/server-ca.pem`.
- The producer's `[tls] client_cert` + `client_key` matches the client
  CA the node trusts via `[listeners.tls] client_ca`.
- The producer's `[tls] server_name` (`localhost`) matches a Subject
  Alternative Name on the server leaf (which `otk-devcerts` emits with
  `DNS:localhost,IP:127.0.0.1,IP:::1` by default).

If you regenerated certs with non-default `--server-cn` /
`--server-san`, update `server_name` to match.

## Going further

| Topic | Where |
|---|---|
| Runtime config schema (listeners, auth, TLS, CORS, hot-reload) | [`timing-node/README.md`](timing-node/README.md) |
| Simulator config schema and library API | [`producer-simulated/README.md`](producer-simulated/README.md) |
| Dev cert bundle layout and customisation | [`otk-devcerts/README.md`](otk-devcerts/README.md) |
| Architecture, hexagonal layout, role placement | [`spec/architecture.md`](spec/architecture.md) |
| Open design questions | [`spec/open-questions.md`](spec/open-questions.md) |
| Cross-implementation compatibility rules | [`spec/compatibility.md`](spec/compatibility.md) |
| Workspace conventions for contributors | [`AGENTS.md`](AGENTS.md) |

## Build and test

```sh
cargo build  --workspace
cargo test   --workspace --all-targets
cargo clippy --workspace --all-targets
cargo fmt    --all -- --check
cargo doc    --workspace --no-deps
```

## Binaries

| Binary | Crate | Role |
|---|---|---|
| `otk-node` | `timing-node` | The Timing Runtime Node. Hosts ingest listeners, the application service, the event log, and the HTTP API. |
| `otk-simulator` | `producer-simulated` | Synthetic detector producer for development, testing, and demos. |
| `otk-devcerts` | `otk-devcerts` | One-shot dev cert generator for the TLS / mTLS demos. |

## License

Apache-2.0.
