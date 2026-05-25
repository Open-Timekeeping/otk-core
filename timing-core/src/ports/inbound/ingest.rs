//! Inbound port: server-side ingestion of producer-supplied events.
//!
//! The runtime calls [`EventIngestPort::accept`] to obtain producer sessions;
//! per-transport adapters (e.g. `adapter-ingest-tcp`,
//! `adapter-ingest-unix-socket`) implement the traits in this module and own
//! all framing, CBOR decoding, and handshake mechanics.
//!
//! # Roles
//!
//! - [`EventIngestPort`]: server-side listener; accept sessions from producers.
//! - [`IngestSession`]: a single connected producer; poll [`IngestSession::next_event`]
//!   until done.
//! - [`IngestError`]: error vocabulary for both accept and session operations.

use async_trait::async_trait;
use event_model::OtkEvent;
use thiserror::Error;

/// Errors that can surface from [`EventIngestPort::accept`] or
/// [`IngestSession::next_event`].
#[derive(Debug, Error)]
pub enum IngestError {
    #[error("connection refused: {0}")]
    ConnectionRefused(String),
    #[error("connection reset")]
    ConnectionReset,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("port is closed")]
    Closed,
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("decode error: {0}")]
    Decode(String),
}

/// One event delivered up from an [`IngestSession`].
///
/// Carries the canonical [`OtkEvent`] plus any per-message metadata the
/// transport learned during decode but the event itself doesn't carry.
/// Today that means just the optional W3C `traceparent` from the envelope
/// (already format-validated upstream by `ingest-protocol`); future
/// metadata (e.g. server-side receive timestamp, frame size for metrics)
/// can land here without another trait-signature change.
#[derive(Debug, Clone)]
pub struct IncomingEvent {
    pub event: OtkEvent,
    /// W3C Trace Context `traceparent` value from the envelope, when the
    /// producer set one and it passed validation. Consumers use this to
    /// parent the per-event tracing span on the producer's trace so logs
    /// stitch across the wire in any OpenTelemetry-aware backend.
    pub traceparent: Option<String>,
}

/// A single connected producer session.
///
/// Call `next_event` in a loop to receive typed events. Returns `None` when
/// the producer disconnects cleanly. Returns `Err` on a terminal error.
///
/// `producer_id` and `peer_addr` are available for the lifetime of the session.
#[async_trait]
pub trait IngestSession: Send {
    async fn next_event(&mut self) -> Result<Option<IncomingEvent>, IngestError>;
    fn producer_id(&self) -> &str;
    fn peer_addr(&self) -> &str;
}

/// Server-side inbound port: accept typed event sessions from producers.
///
/// Each call to `accept` suspends until the next producer connects and completes
/// the OTK handshake, then returns a ready [`IngestSession`]. The caller drives
/// `next_event` on the session until it returns `None` (clean disconnect) or
/// `Err` (terminal error).
///
/// Framing, CBOR decoding, and handshake mechanics are adapter concerns and are
/// not visible through this port.
#[async_trait]
pub trait EventIngestPort: Send + Sync {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError>;
}
