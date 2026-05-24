//! Wire Protocol conformance: [`OtkEnvelope`] schema evolution.
//!
//! Envelope-level tests covering schema additions that landed after the
//! initial wire-protocol shape. Separate from `wire_protocol_handshake`
//! because the concern is the envelope itself, not any specific
//! message payload.
//!
//! The current additions exercised:
//!
//! - [`OtkEnvelope::traceparent`] at CBOR index 7 (added in the W3C
//!   trace-context propagation work). Older encoders, written before
//!   the field existed, must continue to decode cleanly with the new
//!   decoder; missing index 7 must decode as `None`.

use minicbor::Encode;
use otk_protocol::{
    envelope::MessageType,
    ids::{CorrelationId, ProducerId},
    OtkEnvelope, PROTOCOL_VERSION,
};

/// A pre-traceparent encoder writes a CBOR map keyed by the same field
/// indices the current envelope uses (0..=6) and simply omits the new
/// index-7 field. The decoder for the current envelope must accept that
/// shape and decode `traceparent` as `None`.
///
/// We emulate the old encoder by declaring a parallel struct here that
/// carries the same indices as `OtkEnvelope` minus the new field. This
/// is exactly the test that lives inside `otk-protocol` as a unit test;
/// duplicating it here promotes the property from "this implementation
/// works" to "any conforming implementation must work this way", which
/// is the conformance-suite contract for non-implementation-specific
/// behaviour.
#[derive(Encode)]
struct LegacyEnvelope<'a> {
    #[n(0)]
    protocol_version: u8,
    #[n(1)]
    message_type: MessageType,
    #[n(2)]
    source_id: &'a ProducerId,
    #[n(3)]
    stream_id: Option<&'a event_model::ids::StreamId>,
    #[n(4)]
    sequence_number: Option<u64>,
    #[n(5)]
    correlation_id: Option<&'a CorrelationId>,
    #[n(6)]
    payload: Option<&'a [u8]>,
}

#[test]
fn envelope_without_traceparent_decodes_as_none() {
    let producer = ProducerId::from("p-legacy");
    let bytes = minicbor::to_vec(LegacyEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Heartbeat,
        source_id: &producer,
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(&[1, 2, 3]),
    })
    .expect("legacy envelope encodes");

    let decoded: OtkEnvelope = minicbor::decode(&bytes).expect("current decoder accepts legacy");
    assert_eq!(
        decoded.source_id.as_str(),
        "p-legacy",
        "fields up to index 6 must round-trip"
    );
    assert_eq!(decoded.payload.as_deref(), Some(&[1u8, 2, 3][..]));
    assert_eq!(
        decoded.traceparent, None,
        "missing CBOR index 7 must decode as None for backward-compatible envelope evolution"
    );
}

#[test]
fn envelope_with_traceparent_round_trips() {
    // Sanity: the current decoder also handles envelopes WITH traceparent
    // (the typical case). A test elsewhere proves the validator drops
    // malformed values; here we only verify that a well-formed value
    // survives encode → decode → re-encode unchanged.
    let env = OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Heartbeat,
        source_id: ProducerId::from("p-current"),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(vec![9, 9, 9]),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
    };

    let bytes = minicbor::to_vec(&env).expect("encode");
    let decoded: OtkEnvelope = minicbor::decode(&bytes).expect("decode");
    let re_encoded = minicbor::to_vec(&decoded).expect("re-encode");
    assert_eq!(
        bytes, re_encoded,
        "envelope CBOR must be stable across re-encode"
    );
    assert_eq!(
        decoded.traceparent.as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
}
