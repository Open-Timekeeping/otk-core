//! [`OtkEnvelope`] builders for the Wire Protocol layer.
//!
//! Covers the two envelope shapes every transport-binding ingest
//! adapter needs to produce or consume in conformance tests:
//!
//! - [`connect`] / [`connect_with_token`]: producer-side `Connect`
//!   handshake envelopes, optionally carrying a shared-secret auth
//!   token.
//! - [`data`]: a generic post-handshake envelope (Event, Heartbeat,
//!   Disconnect) with caller-supplied payload bytes.
//!
//! The builders intentionally do not pick a `sequence_number` or
//! `correlation_id` for the caller; both are `None` by default.
//! Tests that care about per-envelope sequencing should set them
//! explicitly after construction.

use otk_protocol::{ids::ProducerId, Connect, MessageType, OtkEnvelope, PROTOCOL_VERSION};

/// Build a `Connect` envelope advertising the protocol version range
/// `[min, max]` and the given producer id, with no auth token.
///
/// `protocol_version` on the envelope is set to `max` so the receiver
/// sees the producer's preferred version.
pub fn connect(producer: &str, min: u8, max: u8) -> OtkEnvelope {
    connect_with_optional_token(producer, min, max, None)
}

/// Build a `Connect` envelope with a shared-secret auth token. Used
/// to exercise the authoriser path on the server side.
pub fn connect_with_token(producer: &str, min: u8, max: u8, token: &str) -> OtkEnvelope {
    connect_with_optional_token(producer, min, max, Some(token))
}

fn connect_with_optional_token(
    producer: &str,
    min: u8,
    max: u8,
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
        payload: Some(minicbor::to_vec(&connect).expect("Connect encodes")),
        traceparent: None,
    }
}

/// Build a generic post-handshake envelope of the given message type
/// with caller-supplied payload bytes. The payload may be `None` for
/// message types that don't carry one (Disconnect, Heartbeat).
///
/// `protocol_version` is set to [`PROTOCOL_VERSION`]; tests covering
/// version-mismatch paths should construct [`OtkEnvelope`] directly.
pub fn data(producer: &str, mt: MessageType, payload: Option<Vec<u8>>) -> OtkEnvelope {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_envelope_has_no_token_by_default() {
        let env = connect("p-1", 0, PROTOCOL_VERSION);
        let connect: Connect = minicbor::decode(env.payload.as_deref().unwrap()).unwrap();
        assert!(connect.auth_token.is_none());
        assert_eq!(env.message_type, MessageType::Connect);
        assert_eq!(env.protocol_version, PROTOCOL_VERSION);
    }

    #[test]
    fn connect_with_token_carries_token() {
        let env = connect_with_token("p-1", 0, PROTOCOL_VERSION, "shh");
        let connect: Connect = minicbor::decode(env.payload.as_deref().unwrap()).unwrap();
        assert_eq!(connect.auth_token.as_deref(), Some("shh"));
    }

    #[test]
    fn data_envelope_carries_message_type_and_payload() {
        let env = data("p-1", MessageType::Heartbeat, None);
        assert_eq!(env.message_type, MessageType::Heartbeat);
        assert!(env.payload.is_none());

        let env = data("p-1", MessageType::Event, Some(vec![0x42]));
        assert_eq!(env.payload.as_deref(), Some(&[0x42_u8][..]));
    }
}
