//! Ingest Protocol conformance.
//!
//! Verifies the public-API contract that every transport adapter consuming
//! `ingest-protocol` relies on: the handshake outcomes a server can produce,
//! and the processor's dispatch into [`InboundAction`] variants.

use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, TimebaseId,
    TimestampingMethod, TimingPointId,
};
use ingest_protocol::{
    perform_server_handshake, perform_server_handshake_with_auth, ConnectAuthoriser,
    HandshakeOutcome, InboundAction, PostHandshakeProcessor, ProtocolError,
};
use otk_protocol::{
    ids::ProducerId, Connect, ConnectRejectReason, MessageType, OtkEnvelope, PROTOCOL_VERSION,
};

fn connect_envelope(min: u8, max: u8, producer: &str) -> OtkEnvelope {
    connect_envelope_with_token(min, max, producer, None)
}

fn connect_envelope_with_token(
    min: u8,
    max: u8,
    producer: &str,
    token: Option<&str>,
) -> OtkEnvelope {
    let connect = Connect {
        protocol_version_min: min,
        protocol_version_max: max,
        streams: vec![],
        auth_token: token.map(|s| s.to_string()),
    };
    OtkEnvelope {
        protocol_version: max,
        message_type: MessageType::Connect,
        source_id: ProducerId::from(producer),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(minicbor::to_vec(&connect).unwrap()),
        traceparent: None,
    }
}

fn data_envelope(mt: MessageType, payload: Option<Vec<u8>>, producer: &str) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: mt,
        source_id: ProducerId::from(producer),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload,
        traceparent: None,
    }
}

fn test_event() -> OtkEvent {
    OtkEvent::Detection(Detection {
        detection_id: DetectionId::new("det-1"),
        detector_id: DetectorId::new("d-1"),
        timing_point_id: TimingPointId::new("tp-1"),
        subject_id: None,
        detected_at_ns: 1,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("tb-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: 1,
        sensor: SensorData::BeamBreak,
    })
}

#[test]
fn handshake_accepts_overlapping_version_range() {
    let env = connect_envelope(0, u8::MAX, "p-1");
    let outcome = perform_server_handshake(env).expect("handshake");
    assert!(matches!(outcome, HandshakeOutcome::Accepted { .. }));
}

#[test]
fn handshake_rejects_non_overlapping_range() {
    // Producer wants only v99..v99. Server is PROTOCOL_VERSION (0).
    let env = connect_envelope(99, 99, "p-1");
    let outcome = perform_server_handshake(env).expect("handshake");
    match outcome {
        HandshakeOutcome::Rejected { reason, .. } => {
            assert!(matches!(reason, ConnectRejectReason::VersionNotSupported));
        }
        HandshakeOutcome::Accepted { .. } => panic!("expected Rejected"),
    }
}

#[test]
fn processor_dispatches_event_payload_to_action() {
    let proc = PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION);
    let env = data_envelope(
        MessageType::Event,
        Some(minicbor::to_vec(test_event()).unwrap()),
        "p-1",
    );
    match proc.process(env).expect("process") {
        InboundAction::Event {
            event: OtkEvent::Detection(_),
            traceparent,
        } => {
            assert_eq!(traceparent, None, "envelope had no traceparent");
        }
        other => panic!("expected Event/Detection, got {other:?}"),
    }
}

#[test]
fn processor_forwards_valid_traceparent() {
    let proc = PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION);
    let mut env = data_envelope(
        MessageType::Event,
        Some(minicbor::to_vec(test_event()).unwrap()),
        "p-1",
    );
    env.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
    match proc.process(env).expect("process") {
        InboundAction::Event { traceparent, .. } => {
            assert_eq!(
                traceparent.as_deref(),
                Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
                "valid W3C traceparent must pass through to the runtime"
            );
        }
        other => panic!("expected Event, got {other:?}"),
    }
}

#[test]
fn processor_drops_malformed_traceparent_without_failing() {
    let proc = PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION);
    let mut env = data_envelope(
        MessageType::Event,
        Some(minicbor::to_vec(test_event()).unwrap()),
        "p-1",
    );
    // Wrong length, wrong format. The processor MUST drop this silently
    // (not reject the event) so a buggy producer's instrumentation can't
    // break the data path.
    env.traceparent = Some("not-a-traceparent".to_string());
    match proc.process(env).expect("process") {
        InboundAction::Event { traceparent, .. } => {
            assert_eq!(
                traceparent, None,
                "malformed traceparent must be silently dropped"
            );
        }
        other => panic!("expected Event, got {other:?}"),
    }
}

#[test]
fn processor_collapses_heartbeat_and_disconnect() {
    let proc = PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION);
    // Per the OtkEnvelope contract, Heartbeat carries a CBOR-encoded Heartbeat payload;
    // only Disconnect is payload-less.
    let hb_payload = minicbor::to_vec(otk_protocol::Heartbeat { sent_at_ns: 0 }).unwrap();
    let hb = proc
        .process(data_envelope(
            MessageType::Heartbeat,
            Some(hb_payload),
            "p-1",
        ))
        .unwrap();
    assert!(matches!(hb, InboundAction::Heartbeat));
    let dc = proc
        .process(data_envelope(MessageType::Disconnect, None, "p-1"))
        .unwrap();
    assert!(matches!(dc, InboundAction::Disconnect));
}

#[test]
fn processor_rejects_source_spoofing() {
    let proc = PostHandshakeProcessor::new(ProducerId::from("p-1"), PROTOCOL_VERSION);
    // Carry a *valid* Heartbeat payload so the only contract violation is
    // the spoofed source_id. Without this, a bare Heartbeat would be
    // rejected with MissingHeartbeatPayload and the test would pass for
    // the wrong reason (masking a regression in the source-id check).
    let hb_payload = minicbor::to_vec(otk_protocol::Heartbeat { sent_at_ns: 0 }).unwrap();
    let env = data_envelope(MessageType::Heartbeat, Some(hb_payload), "evil");
    let err = proc.process(env).expect_err("source mismatch must error");
    assert!(
        matches!(err, ProtocolError::SourceMismatch { .. }),
        "expected SourceMismatch, got {err:?}"
    );
}

// ── Auth conformance ────────────────────────────────────────────────────────

struct TokenAllowList(Vec<String>);

impl ConnectAuthoriser for TokenAllowList {
    fn authorise(
        &self,
        _producer_id: &ProducerId,
        token: Option<&str>,
    ) -> Result<(), ConnectRejectReason> {
        match token {
            Some(t) if self.0.iter().any(|allowed| allowed == t) => Ok(()),
            _ => Err(ConnectRejectReason::Unauthorized),
        }
    }
}

#[test]
fn handshake_with_auth_accepts_listed_token() {
    let env = connect_envelope_with_token(0, u8::MAX, "p-1", Some("secret"));
    let auth = TokenAllowList(vec!["secret".into()]);
    let outcome = perform_server_handshake_with_auth(env, &auth).expect("handshake");
    assert!(matches!(outcome, HandshakeOutcome::Accepted { .. }));
}

#[test]
fn handshake_with_auth_rejects_missing_token_as_unauthorized() {
    let env = connect_envelope_with_token(0, u8::MAX, "p-1", None);
    let auth = TokenAllowList(vec!["secret".into()]);
    match perform_server_handshake_with_auth(env, &auth).expect("handshake") {
        HandshakeOutcome::Rejected { reason, .. } => {
            assert!(matches!(reason, ConnectRejectReason::Unauthorized));
        }
        HandshakeOutcome::Accepted { .. } => panic!("expected Rejected"),
    }
}

#[test]
fn handshake_with_auth_rejects_wrong_token_as_unauthorized() {
    let env = connect_envelope_with_token(0, u8::MAX, "p-1", Some("nope"));
    let auth = TokenAllowList(vec!["secret".into()]);
    match perform_server_handshake_with_auth(env, &auth).expect("handshake") {
        HandshakeOutcome::Rejected { reason, .. } => {
            assert!(matches!(reason, ConnectRejectReason::Unauthorized));
        }
        HandshakeOutcome::Accepted { .. } => panic!("expected Rejected"),
    }
}
