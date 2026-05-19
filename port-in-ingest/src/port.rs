use async_trait::async_trait;

use crate::error::IngestError;
use crate::session::IngestSession;

/// Server-side inbound port: accept typed event sessions from producers.
///
/// Each call to `accept` suspends until the next producer connects and completes
/// the OTK handshake, then returns a ready `IngestSession`. The caller drives
/// `next_event` on the session until it returns `None` (clean disconnect) or
/// `Err` (terminal error).
///
/// Framing, CBOR decoding, and handshake mechanics are adapter concerns and are
/// not visible through this port.
#[async_trait]
pub trait EventIngestPort: Send + Sync {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError>;
}
