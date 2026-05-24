use std::net::SocketAddr;
use std::time::Duration;

/// Default maximum CBOR payload length per frame.
///
/// Matches [`frame_codec::DEFAULT_MAX_FRAME_SIZE`] but typed as `u32` to align
/// with the stream-framing length prefix.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 65_535;

/// Configuration for [`crate::TcpIngestPort`].
///
/// Validated at `bind` time: `max_frame_bytes == 0` and
/// `handshake_timeout == Duration::ZERO` are rejected with
/// `IngestError::Io(InvalidInput)` so misconfigurations surface at startup,
/// not as confusing handshake failures later.
#[derive(Debug, Clone)]
pub struct TcpIngestConfig {
    /// Address to bind the ingest listener on.
    pub bind_addr: SocketAddr,
    /// Maximum CBOR payload length per frame. Frames declaring more bytes are rejected.
    pub max_frame_bytes: u32,
    /// Maximum time allowed for the OTK handshake to complete after a TCP connection is accepted.
    /// A client that connects but never sends data is dropped after this duration.
    pub handshake_timeout: Duration,
}

impl Default for TcpIngestConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8463".parse().unwrap(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}
