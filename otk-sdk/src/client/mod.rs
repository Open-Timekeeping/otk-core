//! Consumer-side API for reading events from a timing node.
//!
//! Phase 2: HTTP/SSE client for `GET /api/v1/events` and
//! `GET /api/v1/events/stream`. Currently a stub pending the timing-node
//! REST API implementation.

pub mod client;
pub mod error;

pub use client::OtkClient;
pub use error::ClientError;
