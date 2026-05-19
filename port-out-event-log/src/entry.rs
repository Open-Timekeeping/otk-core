use event_model::OtkEvent;

use crate::offset::Offset;

/// A stored event together with its log position and receipt timestamp.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// The position of this entry in the log.
    pub offset: Offset,

    /// Wall-clock time at which this entry was appended, in nanoseconds since
    /// the Unix epoch. Assigned by the backend on append.
    pub appended_at_ns: u64,

    /// The canonical event.
    pub event: OtkEvent,
}
