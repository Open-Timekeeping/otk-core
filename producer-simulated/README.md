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

**Depends on:** [`otk-sdk`](https://github.com/Open-Timekeeping/otk-sdk) (producer feature only).

**Does not depend on:** any server-side port, adapter, or timing-node crate.

## Development

This crate uses a sibling-relative path dep to `otk-sdk`:

```toml
otk-sdk = { path = "../otk-sdk", default-features = false, features = ["producer"] }
```

Local development expects the standard Open Timekeeping workspace layout: each repo cloned as a sibling under one parent directory (so `../otk-sdk` resolves alongside this crate). From a fresh single-repo clone, `cargo build` will fail until `otk-sdk` is present alongside it.

Once `otk-sdk` publishes to crates.io, the dep will switch to a versioned crates.io spec with an optional `[patch]` override for local sibling development (the cargo-native way to support both standalone and workspace builds).

## License

Apache-2.0
