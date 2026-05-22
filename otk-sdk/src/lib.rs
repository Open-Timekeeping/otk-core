//! Open Timekeeping SDK.
//!
//! Two feature sets:
//!
//! - **`client`** (default): consumer-side API for reading events from a timing node
//!   over HTTP/SSE.
//! - **`producer`**: producer-side API for connecting to a timing node and publishing
//!   events. Includes the `DetectorAdapter` and `Timebase` trait contracts, builder
//!   helpers, and the `Producer` connection type.
//!
//! The SDK re-exports `event-model` so dependents need only add `otk-sdk` to their
//! `Cargo.toml`.
//!
//! # Feature selection
//!
//! ```toml
//! # Consumer only (default)
//! otk-sdk = { git = "..." }
//!
//! # Producer only (no HTTP client code)
//! otk-sdk = { git = "...", default-features = false, features = ["producer"] }
//!
//! # Both roles
//! otk-sdk = { git = "...", features = ["producer"] }
//! ```

pub use event_model;

#[cfg(feature = "producer")]
pub mod producer;

#[cfg(feature = "client")]
pub mod client;
