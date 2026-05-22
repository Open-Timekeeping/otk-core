use otk_protocol::ConnectRejectReason;
use thiserror::Error;

/// Errors that can occur during producer operations.
#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("server rejected connection: {reason:?} (server supports versions {server_min}..={server_max})")]
    Rejected {
        reason: ConnectRejectReason,
        server_min: u8,
        server_max: u8,
    },

    #[error("handshake failed: {0}")]
    Handshake(String),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("connection closed")]
    Closed,

    #[error("invalid configuration: {0}")]
    Config(String),
}
