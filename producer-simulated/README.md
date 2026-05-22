# producer-simulated

Simulated OTK producer. Generates configurable synthetic detections and publishes
them to a timing node. Useful for development, integration testing, and demos without
physical detector hardware. Binary: `otk-simulator`.

## Role

`producers/` layer. Depends only on `otk-sdk` (producer feature). Zero server-side
dependencies; no knowledge of ports, adapters, or timing node internals.

## Usage

```bash
cargo run --bin otk-simulator
cargo run --bin otk-simulator -- --config sim-start.toml
```

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

**Depends on:** [`otk-sdk`](../otk-sdk) (producer feature only).

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
