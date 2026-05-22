use minicbor::{Decode, Encode};

use crate::ids::{DetectorId, StreamId, TimingPointId};

/// The semantic resolution level of a stream. The stream name/address is a configurable
/// `StreamId`; `StreamKind` declares what that stream carries.
///
/// Stream naming conventions (e.g. `<detector_id>/raw`) are a deployment/runtime-node
/// concern, not an event-model concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum StreamKind {
    /// One `Detection` per sensor pulse. Optional: only present if the adapter declares
    /// support for raw signal emission. Policy on whether to expose this stream to
    /// consumers is a deployment decision.
    #[n(0)]
    Raw,

    /// One `Detection` per passage, firmware or adapter processed. The primary ingest
    /// stream for adapters that perform their own grouping and interpolation.
    #[n(1)]
    Detections,

    /// timing-core output. One `Detection` per passage per timing point, possibly
    /// consolidated from multiple detectors at the same point.
    #[n(2)]
    Processed,
}

/// Describes a stream published by an adapter or produced by timing-core.
/// Declared in `AdapterMetadataEvent` so consumers know what streams to subscribe to.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamDescriptor {
    /// The stream's addressable identity within the deployment.
    #[n(0)]
    pub stream_id: StreamId,

    /// The resolution level this stream carries.
    #[n(1)]
    pub kind: StreamKind,

    /// The detector this stream originates from. `None` for timing-point-level streams
    /// (e.g. timing-core processed output consolidated from multiple detectors).
    #[n(2)]
    pub detector_id: Option<DetectorId>,

    /// The timing point this stream is associated with.
    #[n(3)]
    pub timing_point_id: Option<TimingPointId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DetectorId, StreamId, TimingPointId};

    fn round_trip(sd: &StreamDescriptor) -> StreamDescriptor {
        let encoded = minicbor::to_vec(sd).expect("encode failed");
        minicbor::decode(&encoded).expect("decode failed")
    }

    #[test]
    fn stream_descriptor_round_trip_all_some() {
        let sd = StreamDescriptor {
            stream_id: StreamId::new("loop-a/detections"),
            kind: StreamKind::Detections,
            detector_id: Some(DetectorId::new("loop-a")),
            timing_point_id: Some(TimingPointId::new("tp-finish")),
        };
        assert_eq!(sd, round_trip(&sd));
    }

    #[test]
    fn stream_descriptor_round_trip_none_optionals() {
        // timing-core processed streams have no single detector_id.
        let sd = StreamDescriptor {
            stream_id: StreamId::new("tp-finish/processed"),
            kind: StreamKind::Processed,
            detector_id: None,
            timing_point_id: Some(TimingPointId::new("tp-finish")),
        };
        assert_eq!(sd, round_trip(&sd));
    }

    #[test]
    fn stream_kind_round_trips_all_variants() {
        for kind in [
            StreamKind::Raw,
            StreamKind::Detections,
            StreamKind::Processed,
        ] {
            let encoded = minicbor::to_vec(kind).expect("encode failed");
            let decoded: StreamKind = minicbor::decode(&encoded).expect("decode failed");
            assert_eq!(kind, decoded);
        }
    }
}
