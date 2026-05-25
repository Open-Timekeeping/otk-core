//! In-test helpers shared across `timing-core`'s unit tests.
//!
//! Behind `#[cfg(test)]`: nothing here ships in the public API.
//!
//! ## Why a mock instead of a real backend
//!
//! `timing-core`'s tests would naturally reach for `adapter-event-log-segment`
//! as a real `EventLog` implementation. That would create a workspace
//! dependency cycle (the adapter depends on `timing-core` for the port
//! traits), which trips Rust's trait-identity check when both copies of
//! `timing-core` co-exist in the test build. A purpose-built in-memory
//! mock sidesteps the cycle and keeps these tests fast.
//!
//! The end-to-end "real backend + restart" behaviour is exercised in
//! `timing-node/tests/restart_resume_test.rs`, which can depend on the
//! adapter directly.

use async_trait::async_trait;
use event_model::OtkEvent;

use crate::ports::outbound::{EventLog, LogEntry, LogSubscription, Offset, StorageError};

/// In-memory `EventLog` that retains every appended entry in a `Vec`.
///
/// Supports `append`, `read_range`, `latest_offset`, `earliest_offset`.
/// `subscribe` is unimplemented: none of the unit tests using this mock
/// need live tailing (the segment-log adapter's own tests cover that).
pub(crate) struct MockEventLog {
    entries: Vec<LogEntry>,
}

impl MockEventLog {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

#[async_trait]
impl EventLog for MockEventLog {
    async fn append(
        &mut self,
        producer_id: &str,
        events: &[OtkEvent],
    ) -> Result<Offset, StorageError> {
        if producer_id.is_empty() {
            return Err(StorageError::InvalidInput(
                "producer_id must not be empty".into(),
            ));
        }
        if events.is_empty() {
            return Err(StorageError::InvalidInput("events slice empty".into()));
        }
        let mut last = Offset::new(0);
        for event in events {
            let offset = Offset::new(self.entries.len() as u64);
            self.entries.push(LogEntry {
                offset,
                appended_at_ns: 0,
                producer_id: producer_id.to_string(),
                event: event.clone(),
            });
            last = offset;
        }
        Ok(last)
    }

    async fn read_range(
        &mut self,
        from: Offset,
        to: Option<Offset>,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let start = from.as_u64() as usize;
        let end = to
            .map(|o| (o.as_u64() as usize).min(self.entries.len()))
            .unwrap_or(self.entries.len());
        if start > self.entries.len() {
            return Ok(Vec::new());
        }
        Ok(self.entries[start..end].to_vec())
    }

    async fn latest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        Ok(self.entries.last().map(|e| e.offset))
    }

    async fn earliest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        Ok(self.entries.first().map(|e| e.offset))
    }

    async fn subscribe(&mut self, _from: Offset) -> Result<Box<dyn LogSubscription>, StorageError> {
        unimplemented!(
            "MockEventLog does not support subscribe; use the real adapter in integration tests"
        )
    }
}
