use async_trait::async_trait;

use event_model::OtkEvent;

use crate::error::IngestError;

/// A single connected producer session.
///
/// Call `next_event` in a loop to receive typed events. Returns `None` when
/// the producer disconnects cleanly. Returns `Err` on a terminal error.
///
/// `producer_id` and `peer_addr` are available for the lifetime of the session.
#[async_trait]
pub trait IngestSession: Send {
    async fn next_event(&mut self) -> Result<Option<OtkEvent>, IngestError>;
    fn producer_id(&self) -> &str;
    fn peer_addr(&self) -> &str;
}
