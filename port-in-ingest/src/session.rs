use async_trait::async_trait;

use event_model::OtkEvent;

use crate::error::IngestError;

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
