use protocol::{
    ids::{CorrelationId, ProducerId},
    Connect, ConnectAck, ConnectReject, ConnectRejectReason, MessageType, OtkEnvelope,
    PROTOCOL_VERSION,
};

use crate::error::HandshakeError;
use crate::processor::PostHandshakeProcessor;

/// The server-side outcome of processing a producer's initial envelope.
#[derive(Debug)]
pub enum HandshakeOutcome {
    /// Handshake succeeded. Send `reply` back to the producer, then drive
    /// `processor` over every subsequent inbound envelope.
    Accepted {
        reply: OtkEnvelope,
        processor: PostHandshakeProcessor,
    },
    /// Handshake refused. Send `reply` back to the producer and close.
    Rejected {
        reply: OtkEnvelope,
        reason: ConnectRejectReason,
    },
}

/// Authorization decision for a producer's `Connect`.
///
/// Adapters call into an authoriser before [`perform_server_handshake`] returns
/// `Accepted`. A runtime that accepts all producers passes [`AllowAll`]; a
/// runtime with a token allow-list passes a custom implementation.
pub trait ConnectAuthoriser: Send + Sync {
    /// Return `Ok` to accept the producer, `Err(reason)` to reject.
    fn authorise(&self, producer_id: &ProducerId, token: Option<&str>) -> Result<(), ConnectRejectReason>;
}

/// Default authoriser: accept every producer regardless of token.
pub struct AllowAll;

impl ConnectAuthoriser for AllowAll {
    fn authorise(&self, _producer_id: &ProducerId, _token: Option<&str>) -> Result<(), ConnectRejectReason> {
        Ok(())
    }
}

/// Process the first envelope on a new ingest session as a `Connect`.
///
/// Equivalent to [`perform_server_handshake_with_auth`] with the [`AllowAll`]
/// authoriser. Retained for adapters that don't need auth wiring yet.
pub fn perform_server_handshake(
    envelope: OtkEnvelope,
) -> Result<HandshakeOutcome, HandshakeError> {
    handshake_inner(envelope, PROTOCOL_VERSION, &AllowAll)
}

/// Auth-aware variant of [`perform_server_handshake`].
pub fn perform_server_handshake_with_auth(
    envelope: OtkEnvelope,
    authoriser: &dyn ConnectAuthoriser,
) -> Result<HandshakeOutcome, HandshakeError> {
    handshake_inner(envelope, PROTOCOL_VERSION, authoriser)
}

/// Variant exposing the server version explicitly. Useful for tests that need
/// to simulate an older or newer server build.
pub fn handshake_with_server_version(
    envelope: OtkEnvelope,
    server_version: u8,
) -> Result<HandshakeOutcome, HandshakeError> {
    handshake_inner(envelope, server_version, &AllowAll)
}

fn handshake_inner(
    envelope: OtkEnvelope,
    server_version: u8,
    authoriser: &dyn ConnectAuthoriser,
) -> Result<HandshakeOutcome, HandshakeError> {
    if envelope.message_type != MessageType::Connect {
        return Err(HandshakeError::UnexpectedMessageType(envelope.message_type));
    }

    // Echo the producer's correlation_id (if any) back in the reply per the
    // OtkEnvelope contract: the sender sets correlation_id, the responder
    // echoes it so request/reply pairs match.
    let echo_correlation = envelope.correlation_id.clone();
    let producer_id = envelope.source_id;

    let connect_bytes = envelope
        .payload
        .ok_or(HandshakeError::MissingConnectPayload)?;
    let connect: Connect = minicbor::decode(&connect_bytes)
        .map_err(|e| HandshakeError::DecodeFailed(format!("Connect: {e}")))?;

    if !(connect.protocol_version_min..=connect.protocol_version_max).contains(&server_version) {
        let reject = ConnectReject {
            reason: ConnectRejectReason::VersionNotSupported,
            supported_version_min: server_version,
            supported_version_max: server_version,
        };
        let reply = build_reject_envelope(&reject, server_version, echo_correlation)?;
        return Ok(HandshakeOutcome::Rejected {
            reply,
            reason: ConnectRejectReason::VersionNotSupported,
        });
    }

    if let Err(reason) = authoriser.authorise(&producer_id, connect.auth_token.as_deref()) {
        let reject = ConnectReject {
            reason,
            supported_version_min: server_version,
            supported_version_max: server_version,
        };
        let reply = build_reject_envelope(&reject, server_version, echo_correlation)?;
        return Ok(HandshakeOutcome::Rejected { reply, reason });
    }

    let ack = ConnectAck { negotiated_version: server_version };
    let reply = build_ack_envelope(&ack, server_version, echo_correlation)?;
    let processor = PostHandshakeProcessor::new(producer_id, server_version);

    Ok(HandshakeOutcome::Accepted { reply, processor })
}

fn server_envelope(
    message_type: MessageType,
    payload: Option<Vec<u8>>,
    version: u8,
    correlation_id: Option<CorrelationId>,
) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: version,
        message_type,
        source_id: ProducerId::from("server"),
        stream_id: None,
        sequence_number: None,
        correlation_id,
        payload,
    }
}

fn build_ack_envelope(
    ack: &ConnectAck,
    version: u8,
    correlation_id: Option<CorrelationId>,
) -> Result<OtkEnvelope, HandshakeError> {
    let payload = minicbor::to_vec(ack)
        .map_err(|e| HandshakeError::EncodeFailed(format!("ConnectAck: {e}")))?;
    Ok(server_envelope(MessageType::ConnectAck, Some(payload), version, correlation_id))
}

fn build_reject_envelope(
    reject: &ConnectReject,
    version: u8,
    correlation_id: Option<CorrelationId>,
) -> Result<OtkEnvelope, HandshakeError> {
    let payload = minicbor::to_vec(reject)
        .map_err(|e| HandshakeError::EncodeFailed(format!("ConnectReject: {e}")))?;
    Ok(server_envelope(MessageType::ConnectReject, Some(payload), version, correlation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{Connect, MessageType, OtkEnvelope};

    fn connect_envelope(min: u8, max: u8, producer_id: &str) -> OtkEnvelope {
        let connect = Connect {
            protocol_version_min: min,
            protocol_version_max: max,
            streams: vec![],
            auth_token: None,
        };
        OtkEnvelope {
            protocol_version: max,
            message_type: MessageType::Connect,
            source_id: ProducerId::from(producer_id),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: Some(minicbor::to_vec(&connect).unwrap()),
        }
    }

    #[test]
    fn accepts_version_in_range() {
        let env = connect_envelope(0, u8::MAX, "p-1");
        match perform_server_handshake(env).unwrap() {
            HandshakeOutcome::Accepted { reply, processor: _ } => {
                assert_eq!(reply.message_type, MessageType::ConnectAck);
                let ack: ConnectAck = minicbor::decode(reply.payload.as_deref().unwrap()).unwrap();
                assert_eq!(ack.negotiated_version, PROTOCOL_VERSION);
            }
            HandshakeOutcome::Rejected { .. } => panic!("expected Accepted"),
        }
    }

    #[test]
    fn rejects_version_out_of_range() {
        // Producer requires v99..v99; server is PROTOCOL_VERSION (0); no overlap.
        let env = connect_envelope(99, 99, "p-1");
        match perform_server_handshake(env).unwrap() {
            HandshakeOutcome::Rejected { reply, reason } => {
                assert!(matches!(reason, ConnectRejectReason::VersionNotSupported));
                assert_eq!(reply.message_type, MessageType::ConnectReject);
            }
            HandshakeOutcome::Accepted { .. } => panic!("expected Rejected"),
        }
    }

    #[test]
    fn errors_when_first_envelope_is_not_connect() {
        let env = OtkEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_type: MessageType::Heartbeat,
            source_id: ProducerId::from("p-1"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: None,
        };
        let err = perform_server_handshake(env).expect_err("must error");
        assert!(matches!(err, HandshakeError::UnexpectedMessageType(_)));
    }
}
