use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("connection closed")]
    Closed,
}
