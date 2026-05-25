# producer-simulated

Simulated OTK producer. Generates configurable synthetic detections and publishes
them to a timing node. Useful for development, integration testing, and demos without
physical detector hardware. Binary: `otk-simulator`.

## Role

`producers/` layer. Depends only on `otk-sdk` (producer feature). Zero server-side
dependencies; no knowledge of ports, adapters, or timing node internals.

## Usage

End-to-end demo flows (plain TCP, TLS, mTLS) live at the workspace
root: see the [Getting started](../README.md#getting-started) section
of the top-level [README](../README.md). This page documents the
simulator's config schema and its library API; the shipped sample
configs (`sim-start.toml`, `sim-start-tls.toml`, etc.) are commented
inline with full field documentation.

The simulator's TLS-aware sample (`sim-start-tls.toml`) references its
PEM material at relative paths under `./dev-certs/` (the directory
`otk-devcerts` writes to by default). **Run from the workspace root**
so those relative paths resolve. Both `auth_token` (shared-secret) and
`[tls]` (cert-based) are optional and independent of each other.

## Library usage

```rust,ignore
use producer_simulated::{SimulatorAdapter, SimulatorConfig, runner};
use otk_sdk::producer::{ProducerConfig, Transport};

let sim_config = SimulatorConfig { count: Some(10), ..SimulatorConfig::default() };
let transport = Transport::Tcp(sim_config.node_addr);
let producer_config = ProducerConfig::new(sim_config.producer_id.clone());
let adapter = SimulatorAdapter::new(sim_config);
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
runner::run(adapter, transport, producer_config, shutdown_rx).await?;
```

## Dependencies

**Depends on:** [`otk-sdk`](../otk-sdk) (`producer` + `producer-tls` features). The `producer-tls` feature is on unconditionally so the simulator can connect to either plain-TCP or TLS-enabled nodes from a single binary.

**Does not depend on:** any server-side port, adapter, or timing-node crate.

## Development

This crate is a member of the workspace at the repository root and depends on `otk-sdk` via an intra-workspace path:

```toml
otk-sdk = { path = "../otk-sdk", default-features = false, features = ["producer"] }
```

Build and test from the workspace root:

```bash
cargo build -p producer-simulated
cargo test  -p producer-simulated
```

When the contract crates eventually publish to crates.io, the path deps in this workspace can switch to `version = "x.y"` with a workspace-level `[patch.crates-io]` block for local development.

## License

Apache-2.0
