use event_model::OtkEvent;

use crate::error::ErrorMessage;
use crate::handshake::{Connect, ConnectAck, ConnectReject};
use crate::heartbeat::Heartbeat;

/// The decoded, typed form of an [`OtkEnvelope`]'s payload.
///
/// After receiving an [`OtkEnvelope`], inspect `message_type` to know which type
/// to decode from `payload`, then construct the matching variant here.
///
/// This enum is **not** itself CBOR-encoded. The on-wire representation is always
/// the inner type encoded directly into `payload` bytes; `message_type` in the
/// envelope is the discriminant. Encoding `OtkMessage` as a CBOR enum would add
/// a redundant outer wrapper and diverge from the envelope contract.
///
/// [`OtkEnvelope`]: crate::envelope::OtkEnvelope
#[derive(Debug, Clone)]
pub enum OtkMessage {
    /// A canonical timing event.
    Event(OtkEvent),

    /// Handshake initiation from producer.
    Connect(Connect),

    /// Handshake acceptance from server.
    ConnectAck(ConnectAck),

    /// Handshake rejection from server.
    ConnectReject(ConnectReject),

    /// Keep-alive from either party.
    Heartbeat(Heartbeat),

    /// Error notification from server.
    Error(ErrorMessage),

    /// Graceful disconnect; producer sends before closing the connection.
    /// Envelope `payload` is `None` for this variant.
    Disconnect,
}
