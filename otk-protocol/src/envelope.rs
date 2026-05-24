use alloc::string::String;
use alloc::vec::Vec;
use event_model::ids::StreamId;
use minicbor::{Decode, Encode};

use crate::ids::{CorrelationId, ProducerId};

/// The full catalog of OTK wire protocol message kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum MessageType {
    /// Carries a canonical [`event_model::OtkEvent`] payload.
    #[n(0)]
    Event,
    /// Handshake initiation from producer to server.
    #[n(1)]
    Connect,
    /// Handshake acceptance from server to producer.
    #[n(2)]
    ConnectAck,
    /// Handshake rejection from server to producer.
    #[n(3)]
    ConnectReject,
    /// Keep-alive; either party may send.
    #[n(4)]
    Heartbeat,
    /// Error notification; server sends when it detects a problem with a producer's messages.
    #[n(5)]
    Error,
    /// Graceful disconnect; producer sends before closing the connection.
    #[n(6)]
    Disconnect,
}

/// Header that wraps every OTK message on the wire.
///
/// The `payload` field is `Some(cbor_bytes)` for all message types except
/// [`MessageType::Disconnect`], which sets it to `None`. For [`MessageType::Event`]
/// messages the bytes are a CBOR-encoded [`event_model::OtkEvent`]. For other protocol
/// messages they are the CBOR encoding of the corresponding struct from this crate
/// ([`Connect`], [`ConnectAck`], [`Heartbeat`], etc.).
///
/// Frame encoding (adding a length prefix and writing bytes to a transport) is the
/// responsibility of `frame-codec`, not this type.
///
/// [`Connect`]: crate::handshake::Connect
/// [`ConnectAck`]: crate::handshake::ConnectAck
/// [`Heartbeat`]: crate::heartbeat::Heartbeat
#[derive(Debug, Clone, Encode, Decode)]
pub struct OtkEnvelope {
    #[n(0)]
    pub protocol_version: u8,

    #[n(1)]
    pub message_type: MessageType,

    #[n(2)]
    pub source_id: ProducerId,

    /// `None` for most protocol messages (Connect, Heartbeat, Disconnect, etc.).
    /// Set to the relevant stream for [`MessageType::Error`] messages when
    /// `related_sequence` in the payload identifies an event on a specific stream.
    #[n(3)]
    pub stream_id: Option<StreamId>,

    /// Per-stream monotonic counter for gap detection. `None` for protocol messages.
    #[n(4)]
    pub sequence_number: Option<u64>,

    /// Links a request to its response. Set by the sender; echoed in the reply.
    #[n(5)]
    pub correlation_id: Option<CorrelationId>,

    /// CBOR-encoded payload bytes. `None` for [`MessageType::Disconnect`], which carries
    /// no payload. All other message types carry `Some(bytes)` where `bytes` is the
    /// CBOR encoding of the corresponding inner type (`OtkEvent`, `Connect`, etc.).
    #[n(6)]
    pub payload: Option<Vec<u8>>,

    /// Optional W3C Trace Context `traceparent` header value carrying the
    /// producer's distributed-trace identity for this message.
    ///
    /// When present, the runtime parents the per-event tracing span on the
    /// supplied trace + span id so logs span producer and node under one
    /// trace in any OpenTelemetry-aware backend.
    ///
    /// Validate with [`crate::is_valid_traceparent`] before trusting the
    /// contents; this field is a free-form `String` for forward compatibility
    /// with future `traceparent` versions, and a malformed value from the
    /// wire should be dropped (warn + carry on), not propagated. CBOR index
    /// 7 was added after the initial schema; older encoders simply omit it
    /// and the decoder treats it as `None`.
    #[n(7)]
    pub traceparent: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn base_envelope() -> OtkEnvelope {
        OtkEnvelope {
            protocol_version: 0,
            message_type: MessageType::Heartbeat,
            source_id: ProducerId::from("p-1"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: Some(vec![1, 2, 3]),
            traceparent: None,
        }
    }

    #[test]
    fn round_trip_without_traceparent() {
        let env = base_envelope();
        let bytes = minicbor::to_vec(&env).unwrap();
        let decoded: OtkEnvelope = minicbor::decode(&bytes).unwrap();
        assert_eq!(decoded.traceparent, None);
        assert_eq!(decoded.payload, env.payload);
    }

    #[test]
    fn round_trip_with_traceparent() {
        let mut env = base_envelope();
        env.traceparent = Some(alloc::string::String::from(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ));
        let bytes = minicbor::to_vec(&env).unwrap();
        let decoded: OtkEnvelope = minicbor::decode(&bytes).unwrap();
        assert_eq!(
            decoded.traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    /// A pre-traceparent encoder would produce a CBOR array with the 7
    /// original fields and stop. A new decoder must still parse that as
    /// `traceparent = None` so old producers stay compatible with a
    /// newer node. We emulate the old wire shape by reusing minicbor on
    /// a pre-traceparent envelope struct definition embedded here.
    #[test]
    fn old_encoder_decodes_with_none_traceparent() {
        // Mirror of `OtkEnvelope` minus the new field. Same indices.
        #[derive(Encode)]
        struct LegacyEnvelope<'a> {
            #[n(0)]
            protocol_version: u8,
            #[n(1)]
            message_type: MessageType,
            #[n(2)]
            source_id: &'a ProducerId,
            #[n(3)]
            stream_id: Option<&'a StreamId>,
            #[n(4)]
            sequence_number: Option<u64>,
            #[n(5)]
            correlation_id: Option<&'a CorrelationId>,
            #[n(6)]
            payload: Option<&'a [u8]>,
        }

        let producer = ProducerId::from("p-legacy");
        let bytes = minicbor::to_vec(LegacyEnvelope {
            protocol_version: 0,
            message_type: MessageType::Heartbeat,
            source_id: &producer,
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: Some(&[1, 2, 3]),
        })
        .unwrap();

        let decoded: OtkEnvelope = minicbor::decode(&bytes).unwrap();
        assert_eq!(decoded.source_id.as_str(), "p-legacy");
        assert_eq!(decoded.payload.as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(
            decoded.traceparent, None,
            "missing index-7 field must decode as None"
        );
    }
}
