//! Consumer-side API for reading events from a timing node.
//!
//! Phase 2: HTTP/SSE client for `GET /api/v1/events` and
//! `GET /api/v1/events/stream`. Currently a stub pending the timing-node
//! REST API implementation.

// The client::client naming mirrors producer/producer: `OtkClient` is the
// headline type, re-exported below as `client::OtkClient`.
#[allow(clippy::module_inception)]
pub mod client;
pub mod error;

pub use client::OtkClient;
pub use error::ClientError;
