//! Wire Protocol conformance: handshake message shapes.
//!
//! These tests cover the static side of the handshake (envelope shape, version
//! negotiation logic, reject reason vocabulary). The dynamic side (running the
//! handshake over a real transport binding) lands in M5 as multi-listener parity.

use otk_protocol::{
    ids::ProducerId, Connect, ConnectAck, ConnectReject, ConnectRejectReason, MessageType,
    OtkEnvelope, PROTOCOL_VERSION,
};

#[test]
fn connect_envelope_roundtrip() {
    let connect = Connect {
        protocol_version_min: PROTOCOL_VERSION,
        protocol_version_max: PROTOCOL_VERSION,
        streams: vec![],
        auth_token: None,
    };
    let connect_bytes = minicbor::to_vec(&connect).expect("encode Connect");
    let envelope = OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Connect,
        source_id: ProducerId::from("test-producer"),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(connect_bytes.clone()),
    };
    let env_bytes = minicbor::to_vec(&envelope).expect("encode envelope");
    let decoded: OtkEnvelope = minicbor::decode(&env_bytes).expect("decode envelope");
    assert_eq!(decoded.message_type, MessageType::Connect);
    assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
    let connect_again: Connect =
        minicbor::decode(decoded.payload.as_deref().expect("payload present"))
            .expect("decode Connect");
    assert_eq!(connect_again.protocol_version_min, PROTOCOL_VERSION);
    assert_eq!(connect_again.protocol_version_max, PROTOCOL_VERSION);
}

#[test]
fn connect_ack_roundtrip_preserves_negotiated_version() {
    // This test covers the wire-protocol layer in isolation: ConnectAck CBOR
    // encode/decode preserves `negotiated_version`. The live handshake
    // negotiation logic (server picks a version within the producer's range,
    // returns Accepted, etc.) is exercised by `ingest_protocol_contract` in
    // its `handshake_*` tests, which call `perform_server_handshake_with_auth`
    // end-to-end.
    let producer = Connect {
        protocol_version_min: 0,
        protocol_version_max: u8::MAX,
        streams: vec![],
        auth_token: None,
    };
    assert!(
        (producer.protocol_version_min..=producer.protocol_version_max).contains(&PROTOCOL_VERSION),
        "premise: a sensible producer's advertised range must contain PROTOCOL_VERSION"
    );

    let ack = ConnectAck { negotiated_version: PROTOCOL_VERSION };
    let bytes = minicbor::to_vec(&ack).expect("encode ack");
    let decoded: ConnectAck = minicbor::decode(&bytes).expect("decode ack");
    assert_eq!(decoded.negotiated_version, PROTOCOL_VERSION);
}

#[test]
fn connect_reject_version_not_supported_roundtrip() {
    // Producer advertises a range that does not include PROTOCOL_VERSION ⇒ server must reject.
    let connect = Connect {
        protocol_version_min: PROTOCOL_VERSION.wrapping_add(10),
        protocol_version_max: PROTOCOL_VERSION.wrapping_add(11),
        streams: vec![],
        auth_token: None,
    };
    let server_supports = PROTOCOL_VERSION;
    assert!(
        !(connect.protocol_version_min..=connect.protocol_version_max).contains(&server_supports),
        "test premise: ranges must not overlap"
    );

    let reject = ConnectReject {
        reason: ConnectRejectReason::VersionNotSupported,
        supported_version_min: PROTOCOL_VERSION,
        supported_version_max: PROTOCOL_VERSION,
    };
    let bytes = minicbor::to_vec(&reject).expect("encode reject");
    let decoded: ConnectReject = minicbor::decode(&bytes).expect("decode reject");
    assert!(matches!(decoded.reason, ConnectRejectReason::VersionNotSupported));
    assert_eq!(decoded.supported_version_min, PROTOCOL_VERSION);
    assert_eq!(decoded.supported_version_max, PROTOCOL_VERSION);
}
