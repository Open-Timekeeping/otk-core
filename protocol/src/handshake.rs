extern crate alloc;

use alloc::vec::Vec;
use event_model::stream::StreamDescriptor;
use minicbor::{Decode, Encode};

/// Handshake initiation. The producer sends this as its first message after opening a connection.
///
/// Producer identity is carried in the envelope's `source_id` field, not repeated here.
/// The server selects the highest protocol version within the declared `[min, max]` range.
/// If no overlap exists with the server's supported range, the server replies with
/// [`ConnectReject`] carrying `reason = VersionNotSupported`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Connect {
    /// Minimum protocol version this producer supports.
    #[n(0)]
    pub protocol_version_min: u8,

    /// Maximum protocol version this producer supports.
    #[n(1)]
    pub protocol_version_max: u8,

    /// Streams this producer intends to publish to during this session.
    #[n(2)]
    pub streams: Vec<StreamDescriptor>,
}

/// Handshake acceptance. The server sends this in response to a successful [`Connect`].
#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct ConnectAck {
    /// The protocol version the server has selected (within the producer's declared range).
    #[n(0)]
    pub negotiated_version: u8,
}

/// Handshake rejection. The server sends this when it cannot accept the connection.
#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct ConnectReject {
    #[n(0)]
    pub reason: ConnectRejectReason,

    /// Lowest protocol version the server supports; helps the producer decide whether to upgrade.
    #[n(1)]
    pub supported_version_min: u8,

    /// Highest protocol version the server supports.
    #[n(2)]
    pub supported_version_max: u8,
}

/// Why a [`ConnectReject`] was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ConnectRejectReason {
    #[n(0)]
    VersionNotSupported,
    #[n(1)]
    ProducerIdAlreadyConnected,
    #[n(2)]
    Unauthorized,
    #[n(3)]
    ServerFull,
}
