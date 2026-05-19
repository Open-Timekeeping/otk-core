use minicbor::{Decode, Encode};

/// Keep-alive message. Either party may send a `Heartbeat` to confirm the connection is live
/// at the application layer.
///
/// Transport-layer keepalives (TCP keepalive, etc.) handle link liveness. This message is
/// for cases where the transport keepalive interval is too coarse, or where the transport
/// does not provide keepalives at all (e.g., raw serial).
#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct Heartbeat {
    /// Wall-clock time when this heartbeat was sent, in nanoseconds since Unix epoch.
    #[n(0)]
    pub sent_at_ns: u64,
}
