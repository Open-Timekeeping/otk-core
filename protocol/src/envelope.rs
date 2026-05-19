extern crate alloc;

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
/// responsibility of `frame-codec` / `embedded-wire`, not this type.
///
/// [`Connect`]: crate::handshake::Connect
/// [`ConnectAck`]: crate::handshake::ConnectAck
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
}
