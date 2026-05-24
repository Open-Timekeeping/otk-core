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
/// not as confusing handshake failures later. `bind_addr` is **not**
/// validated by this layer; it is handed to `TcpListener::bind`, which
/// surfaces address/parse problems (e.g. `AddrInUse`, permission denied
/// on a privileged port) as its own `io::Error`.
#[derive(Debug, Clone)]
pub struct TcpIngestConfig {
    /// Address to bind the ingest listener on. Delegated to
    /// `TcpListener::bind`; not validated by this crate.
    pub bind_addr: SocketAddr,
    /// Maximum CBOR payload length per frame. Frames declaring more bytes
    /// are rejected. **Enforced lower bound: `>= 1` (`0` is rejected at
    /// `bind` time).** Practical minimum is considerably higher: a
    /// Connect handshake serialised with a token and capabilities is
    /// typically several hundred bytes, so values below ~1024 will
    /// cause every real handshake to fail.
    pub max_frame_bytes: u32,
    /// Maximum time allowed for the OTK handshake to complete after a
    /// TCP connection is accepted. A client that connects but never
    /// sends data is dropped after this duration. **Enforced lower
    /// bound: `> 0` (`Duration::ZERO` is rejected at `bind` time).**
    /// Practical minimum is round-trip latency plus handshake decode
    /// time; sub-millisecond values will time out instantly on real
    /// networks.
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
