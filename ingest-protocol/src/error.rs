use thiserror::Error;

/// A handshake-phase failure. Transport-agnostic.
#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("expected Connect envelope, got {0:?}")]
    UnexpectedMessageType(protocol::MessageType),

    #[error("Connect envelope had no payload")]
    MissingConnectPayload,

    #[error("CBOR decode failed: {0}")]
    DecodeFailed(String),

    /// Failed to encode a server-side reply (ConnectAck / ConnectReject).
    /// Practically only occurs on allocator failure; surfaced rather than
    /// `.expect()`-panicking so long-running runtimes can report it.
    #[error("CBOR encode failed: {0}")]
    EncodeFailed(String),

    #[error(
        "no overlap between producer protocol-version range [{producer_min}, {producer_max}] \
         and server version {server_version}"
    )]
    VersionNotSupported {
        producer_min: u8,
        producer_max: u8,
        server_version: u8,
    },
}

/// A post-handshake envelope-processing failure. Transport-agnostic.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("protocol version mismatch: negotiated {negotiated}, envelope {found}")]
    VersionMismatch { negotiated: u8, found: u8 },

    #[error("source_id mismatch: producer registered as {registered}, envelope from {found}")]
    SourceMismatch { registered: String, found: String },

    #[error("Event message had no payload")]
    MissingEventPayload,

    #[error("Heartbeat message had no payload (protocol envelope contract requires one)")]
    MissingHeartbeatPayload,

    #[error("CBOR decode failed: {0}")]
    DecodeFailed(String),

    #[error("unexpected message type for ingest session: {0:?}")]
    UnexpectedMessageType(protocol::MessageType),
}
