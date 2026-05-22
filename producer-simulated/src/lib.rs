//! Simulated OTK detector producer.
//!
//! Generates configurable synthetic detections and publishes them to a
//! timing node via `otk-sdk`. Useful for development, integration testing,
//! and demos without physical detector hardware.
//!
//! # Binary
//!
//! ```text
//! cargo run --bin otk-simulator
//! cargo run --bin otk-simulator -- --config simulator.toml
//! ```
//!
//! # Library usage
//!
//! ```rust,ignore
//! use producer_simulated::{SimulatorAdapter, SimulatorConfig, runner};
//! use otk_sdk::producer::{ProducerConfig, Transport};
//!
//! let sim_config = SimulatorConfig { count: Some(10), ..SimulatorConfig::default() };
//! let transport = Transport::Tcp(sim_config.node_addr);
//! let producer_config = ProducerConfig::new(sim_config.producer_id.clone());
//! let adapter = SimulatorAdapter::new(sim_config);
//! let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
//! runner::run(adapter, transport, producer_config, shutdown_rx).await?;
//! ```

pub mod config;
pub mod runner;

mod adapter;

pub use adapter::SimulatorAdapter;
pub use config::SimulatorConfig;
