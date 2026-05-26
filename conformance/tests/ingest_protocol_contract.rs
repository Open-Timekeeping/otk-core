//! Ingest Protocol conformance.
//!
//! Verifies the public-API contract that every transport adapter consuming
//! `ingest-protocol` relies on: the handshake outcomes a server can produce,
//! and the processor's dispatch into [`InboundAction`] variants.

use conformance_fixtures::envelopes;
use conformance_fixtures::events::beam_break_event;
use event_model::OtkEvent;
use ingest_protocol::{
    perform_server_handshake, perform_server_handshake_with_auth, ConnectAuthoriser,
    HandshakeOutcome, InboundAction, PostHandshakeProcessor, ProtocolError,
};
use otk_protocol::{
    ids::ProducerId, ConnectRejectReason, MessageType, OtkEnvelope, PROTOCOL_VERSION,
};

/// Convenience wrapper around [`envelopes::connect`] that keeps the
/// `(min, max, producer)` argument order this file already used, so
/// the test bodies don't have to reshuffle.
fn connect_envelope(min: u8, max: u8, producer: &str) -> OtkEnvelope {
    envelopes::connect(producer, min, max)
}

/// Same shape as [`connect_envelope`] but threads an optional auth
/// token through to either [`envelopes::connect`] or
/// [`envelopes::connect_with_token`].
fn connect_envelope_with_token(
    min: u8,
    max: u8,
    producer: &str,
    token: Option<&str>,
) -> OtkEnvelope {
    match token {
        None => envelopes::connect(producer, min, max),
        Some(t) => envelopes::connect_with_token(producer, min, max, t),
    }
}

fn data_envelope(mt: MessageType, payload: Option<Vec<u8>>, producer: &str) -> OtkEnvelope {
    envelopes::data(producer, mt, payload)
}

fn test_event() -> OtkEvent {
    // sequence 1 keeps the previous fixture's value; identifiers
    // come from `conformance_fixtures::detections::beam_break_at_loop`.
    beam_break_event(1)
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
