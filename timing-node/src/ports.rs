use std::pin::Pin;

use event_model::OtkEvent;
use futures_util::Stream;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct EventEntry {
    pub offset: u64,
    pub event: OtkEvent,
}

#[derive(Debug, Serialize)]
pub struct EventPage {
    pub entries: Vec<EventEntry>,
    pub latest_offset: Option<u64>,
}

/// API-shaped error vocabulary for the query port.
///
/// The query port deliberately does not leak the storage backend's error type.
/// Storage errors are mapped to these variants at the pipeline boundary.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("requested offset {requested} is not available (earliest_available={earliest_available:?})")]
    RetentionExpired {
        requested: u64,
        earliest_available: Option<u64>,
    },
    #[error("not found")]
    #[allow(dead_code)]
    NotFound,
    #[error("internal error: {0}")]
    Internal(String),
}

pub type EventStream = Pin<Box<dyn Stream<Item = Result<EventEntry, QueryError>> + Send>>;

/// Read-only query port for the event log. Implemented by [`NodePipeline`].
///
/// The API layer depends only on this trait; it has no direct dependency on
/// storage adapters or the pipeline implementation.
///
/// [`NodePipeline`]: crate::pipeline::NodePipeline
#[async_trait::async_trait]
pub trait EventQueryPort: Send + Sync {
    async fn latest_offset(&self) -> Result<Option<u64>, QueryError>;
    async fn read_events(&self, from: u64, limit: usize) -> Result<EventPage, QueryError>;
    async fn subscribe_events(&self, from: u64) -> Result<EventStream, QueryError>;
}
