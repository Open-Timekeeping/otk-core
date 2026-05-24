use event_model::OtkEvent;

use crate::offset::Offset;

/// A stored event together with its log position, receipt timestamp, and
/// the id of the producer that delivered (or triggered) it.
///
/// # `producer_id` semantics
///
/// `producer_id` records *which producer's process caused this entry to be
/// written*. For a `Detection` event arriving over the wire, that is the
/// producer that sent it. For a `Crossing` event synthesized server-side
/// by `timing-core` from one or more producer-supplied detections, the
/// `producer_id` is inherited from the originating detection's producer:
/// runtime-synthesized events are not given a synthetic producer id at
/// the storage layer (see the runtime metrics for that distinction).
///
/// This field is the source of truth used by the runtime's sequence gate
/// to rebuild its per-`(producer_id, detector_id)` high-water marks on
/// startup. Without it, restart-resume idempotence would not be possible.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// The position of this entry in the log.
    pub offset: Offset,

    /// Wall-clock time at which this entry was appended, in nanoseconds since
    /// the Unix epoch. Assigned by the backend on append.
    pub appended_at_ns: u64,

    /// The producer that delivered or triggered this entry. See the struct
    /// doc for the Detection-vs-Crossing semantics.
    pub producer_id: String,

    /// The canonical event.
    pub event: OtkEvent,
}
