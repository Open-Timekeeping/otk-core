//! Multi-event scenario streams.
//!
//! Each function returns a `Vec<Detection>` representing one
//! conceptually-meaningful sequence of events. Tests replay them
//! through the system under test (the application service, the
//! event log, a transport adapter wrapped in an end-to-end harness)
//! and assert the expected outcome.
//!
//! Today this module ships two starter scenarios. The README's wider
//! corpus (timebase degradation, multi-detector races, pit-lane /
//! start-finish topologies) lands as the conformance harness grows
//! the drivers to consume them.

use event_model::Detection;

use crate::detections::beam_break_at_loop;

/// Happy path: `len` monotonically-increasing beam-break detections
/// from a single detector. Sequence numbers run `1..=len`.
///
/// Use this to verify that an `EventLog` accepts a clean stream and
/// reports `latest_offset` advancing correctly, or that an ingest
/// pipeline maps one Detection per accepted envelope.
pub fn single_detector_happy_path(len: u64) -> Vec<Detection> {
    (1..=len).map(beam_break_at_loop).collect()
}

/// Reconnect-with-replay: sequences `1, 2, 3` followed by `3, 4`.
///
/// The repeated `3` simulates a producer that reconnected after a
/// transient network outage and re-sent its last-acknowledged
/// sequence. The runtime's `SequenceGate` must drop the duplicate
/// `3` and accept the fresh `4`; an `EventLog` storing the stream
/// directly will see five entries unless wrapped in the gate.
pub fn reconnect_with_replay() -> Vec<Detection> {
    vec![
        beam_break_at_loop(1),
        beam_break_at_loop(2),
        beam_break_at_loop(3),
        beam_break_at_loop(3),
        beam_break_at_loop(4),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_length_matches_request() {
        let stream = single_detector_happy_path(5);
        assert_eq!(stream.len(), 5);
        assert_eq!(stream[0].sequence_number, 1);
        assert_eq!(stream[4].sequence_number, 5);
    }

    #[test]
    fn reconnect_carries_a_duplicate() {
        let stream = reconnect_with_replay();
        let seqs: Vec<u64> = stream.iter().map(|d| d.sequence_number).collect();
        assert_eq!(seqs, vec![1, 2, 3, 3, 4]);
    }
}
