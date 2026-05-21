use event_model::OtkEvent;
use protocol::{ids::ProducerId, Heartbeat, MessageType, OtkEnvelope};

use crate::error::ProtocolError;

/// What the protocol machine wants the transport adapter to do with an
/// inbound envelope.
#[derive(Debug)]
pub enum InboundAction {
    /// Deliver this canonical event to the runtime.
    Event(OtkEvent),
    /// A heartbeat: keep reading, do not surface to the runtime.
    Heartbeat,
    /// The producer asked to disconnect cleanly.
    Disconnect,
}

/// Post-handshake protocol machine.
///
/// Validates protocol version and source identity on every envelope, decodes
/// `Event` payloads into [`OtkEvent`], and reduces the message-type universe
/// to the small [`InboundAction`] vocabulary.
#[derive(Debug)]
pub struct PostHandshakeProcessor {
    producer_id: ProducerId,
    negotiated_version: u8,
}

impl PostHandshakeProcessor {
    pub fn new(producer_id: ProducerId, negotiated_version: u8) -> Self {
        Self { producer_id, negotiated_version }
    }

    pub fn producer_id(&self) -> &ProducerId {
        &self.producer_id
    }

    pub fn negotiated_version(&self) -> u8 {
        self.negotiated_version
    }

    pub fn process(&self, envelope: OtkEnvelope) -> Result<InboundAction, ProtocolError> {
        if envelope.protocol_version != self.negotiated_version {
            return Err(ProtocolError::VersionMismatch {
                negotiated: self.negotiated_version,
                found: envelope.protocol_version,
            });
        }
        if envelope.source_id != self.producer_id {
            return Err(ProtocolError::SourceMismatch {
                registered: self.producer_id.to_string(),
                found: envelope.source_id.to_string(),
            });
        }

        match envelope.message_type {
            MessageType::Event => {
                let payload = envelope.payload.ok_or(ProtocolError::MissingEventPayload)?;
                let event: OtkEvent = minicbor::decode(&payload)
                    .map_err(|e| ProtocolError::DecodeFailed(format!("OtkEvent: {e}")))?;
                Ok(InboundAction::Event(event))
            }
            MessageType::Heartbeat => {
                // Per the OtkEnvelope contract, every message type except Disconnect
                // carries a CBOR-encoded payload of its corresponding inner type.
                // Validate the Heartbeat decodes cleanly; the sent_at_ns value isn't
                // surfaced to callers (heartbeats are keep-alives, not data) but
                // catching malformed heartbeats here protects against buggy producers.
                let payload = envelope
                    .payload
                    .ok_or(ProtocolError::MissingHeartbeatPayload)?;
                let _: Heartbeat = minicbor::decode(&payload)
                    .map_err(|e| ProtocolError::DecodeFailed(format!("Heartbeat: {e}")))?;
                Ok(InboundAction::Heartbeat)
            }
            MessageType::Disconnect => Ok(InboundAction::Disconnect),
            other => Err(ProtocolError::UnexpectedMessageType(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_model::{
        Detection, DetectionId, DetectorId, SensorData, SourceAttestation, TimebaseId,
        TimestampingMethod, TimingPointId,
    };
    use protocol::{ids::ProducerId, MessageType, OtkEnvelope, PROTOCOL_VERSION};

    fn p() -> PostHandshakeProcessor {
        PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION)
    }

    fn envelope(mt: MessageType, payload: Option<Vec<u8>>) -> OtkEnvelope {
        OtkEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_type: mt,
            source_id: ProducerId::from("p-1"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload,
        }
    }

    fn test_event() -> OtkEvent {
        OtkEvent::Detection(Detection {
            detection_id: DetectionId::new("det-1"),
            detector_id: DetectorId::new("loop-1"),
            timing_point_id: TimingPointId::new("tp-start"),
            subject_id: None,
            detected_at_ns: 1_000_000_000,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("tb-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 0,
            sensor: SensorData::BeamBreak,
        })
    }

    #[test]
    fn event_returned_as_action() {
        let env = envelope(MessageType::Event, Some(minicbor::to_vec(&test_event()).unwrap()));
        match p().process(env).unwrap() {
            InboundAction::Event(_) => {}
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn heartbeat_with_valid_payload_yields_heartbeat_action() {
        let hb = protocol::Heartbeat { sent_at_ns: 1 };
        let env = envelope(MessageType::Heartbeat, Some(minicbor::to_vec(&hb).unwrap()));
        assert!(matches!(p().process(env).unwrap(), InboundAction::Heartbeat));
    }

    #[test]
    fn heartbeat_with_missing_payload_errors() {
        let env = envelope(MessageType::Heartbeat, None);
        assert!(matches!(
            p().process(env).unwrap_err(),
            ProtocolError::MissingHeartbeatPayload
        ));
    }

    #[test]
    fn disconnect_yields_disconnect_action() {
        let env = envelope(MessageType::Disconnect, None);
        assert!(matches!(p().process(env).unwrap(), InboundAction::Disconnect));
    }

    #[test]
    fn version_mismatch_errors() {
        let mut env = envelope(MessageType::Heartbeat, None);
        env.protocol_version = PROTOCOL_VERSION.wrapping_add(1);
        assert!(matches!(
            p().process(env).unwrap_err(),
            ProtocolError::VersionMismatch { .. }
        ));
    }

    #[test]
    fn source_mismatch_errors() {
        let mut env = envelope(MessageType::Heartbeat, None);
        env.source_id = ProducerId::from("evil");
        assert!(matches!(
            p().process(env).unwrap_err(),
            ProtocolError::SourceMismatch { .. }
        ));
    }

    #[test]
    fn event_with_missing_payload_errors() {
        let env = envelope(MessageType::Event, None);
        assert!(matches!(
            p().process(env).unwrap_err(),
            ProtocolError::MissingEventPayload
        ));
    }
}
