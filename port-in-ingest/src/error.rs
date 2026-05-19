use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("connection refused: {0}")]
    ConnectionRefused(String),
    #[error("connection reset")]
    ConnectionReset,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("port is closed")]
    Closed,
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("decode error: {0}")]
    Decode(String),
}
