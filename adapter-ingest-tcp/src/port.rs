use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ingest_protocol::{AllowAll, ConnectAuthoriser};
use timing_core::ports::inbound::{EventIngestPort, IngestError, IngestSession};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::config::TcpIngestConfig;
use crate::session::TcpIngestSession;

#[cfg(feature = "tls")]
use crate::tls::{build_tls_acceptor, TlsAcceptError};
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

pub struct TcpIngestPort {
    listener: TcpListener,
    config: Arc<TcpIngestConfig>,
    authoriser: Arc<dyn ConnectAuthoriser>,
    /// `Some` iff the config asked for TLS and the cert/key loaded
    /// cleanly. Plain-TCP listeners hold `None` and skip the TLS
    /// wrap on accept.
    #[cfg(feature = "tls")]
    tls_acceptor: Option<TlsAcceptor>,
}

impl TcpIngestPort {
    /// Bind with the default [`AllowAll`] authoriser (development).
    pub async fn bind(config: TcpIngestConfig) -> Result<Self, IngestError> {
        Self::bind_with_auth(config, Arc::new(AllowAll)).await
    }

    /// Bind with an explicit authoriser. The runtime supplies a token-allow-list
    /// authoriser when [`crate::config::TcpIngestConfig`]'s deployment requires it.
    pub async fn bind_with_auth(
        config: TcpIngestConfig,
        authoriser: Arc<dyn ConnectAuthoriser>,
    ) -> Result<Self, IngestError> {
        // Reject obviously-broken config up front so failure is surfaced at
        // bind time, not later as a confusing handshake error. Mirrors the
        // validation on `UnixSocketIngestConfig` for parity.
        if config.max_frame_bytes == 0 {
            return Err(IngestError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_frame_bytes must be > 0",
            )));
        }
        if config.handshake_timeout == Duration::ZERO {
            return Err(IngestError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "handshake_timeout must be > 0",
            )));
        }

        // Load + validate TLS material BEFORE binding the listener so a
        // misconfigured cert path surfaces as a clear "bind failed"
        // error rather than as a TLS handshake error on the first
        // connection.
        #[cfg(feature = "tls")]
        let tls_acceptor = match &config.tls {
            Some(tls_cfg) => Some(build_tls_acceptor(tls_cfg).map_err(tls_err_to_ingest)?),
            None => None,
        };

        let listener = TcpListener::bind(config.bind_addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            authoriser,
            #[cfg(feature = "tls")]
            tls_acceptor,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}

#[cfg(feature = "tls")]
fn tls_err_to_ingest(e: TlsAcceptError) -> IngestError {
    IngestError::Io(io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
}

#[async_trait]
impl EventIngestPort for TcpIngestPort {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError> {
        let (stream, peer) = self.listener.accept().await?;
        let peer_addr = peer.to_string();

        // Branch on TLS-or-not. The session is generic over the byte
        // stream, so the only difference is whether we wrap the
        // TcpStream in a TlsStream before handing it to the session.
        #[cfg(feature = "tls")]
        if let Some(acceptor) = self.tls_acceptor.as_ref() {
            let tls_stream = timeout(self.config.handshake_timeout, acceptor.accept(stream))
                .await
                .map_err(|_| IngestError::Handshake("TLS handshake timed out".into()))?
                .map_err(|e| IngestError::Handshake(format!("TLS handshake failed: {e}")))?;
            let session = timeout(
                self.config.handshake_timeout,
                TcpIngestSession::handshake(
                    tls_stream,
                    peer_addr,
                    self.config.clone(),
                    Arc::clone(&self.authoriser),
                ),
            )
            .await
            .map_err(|_| IngestError::Handshake("handshake timed out".into()))??;
            return Ok(Box::new(session));
        }

        let session = timeout(
            self.config.handshake_timeout,
            TcpIngestSession::handshake(
                stream,
                peer_addr,
                self.config.clone(),
                Arc::clone(&self.authoriser),
            ),
        )
        .await
        .map_err(|_| IngestError::Handshake("handshake timed out".into()))??;
        Ok(Box::new(session))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ephemeral_config() -> TcpIngestConfig {
        TcpIngestConfig {
            // Port 0 = let the OS pick an unused port. The bind never
            // happens in the rejection paths below; the validator returns
            // before TcpListener::bind is called.
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
            handshake_timeout: Duration::from_secs(5),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    #[tokio::test]
    async fn rejects_zero_max_frame_bytes() {
        let cfg = TcpIngestConfig {
            max_frame_bytes: 0,
            ..ephemeral_config()
        };
        // `expect_err` would require `TcpIngestPort: Debug`, which it isn't
        // (it carries `Arc<dyn ConnectAuthoriser>`). Pattern-match instead.
        match TcpIngestPort::bind(cfg).await {
            Err(IngestError::Io(io_err)) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidInput);
                assert!(
                    io_err.to_string().contains("max_frame_bytes"),
                    "error message should mention the offending field, got {io_err}"
                );
            }
            Err(other) => panic!("expected IngestError::Io, got {other:?}"),
            Ok(_) => panic!("zero max_frame_bytes should be rejected"),
        }
    }

    #[tokio::test]
    async fn rejects_zero_handshake_timeout() {
        let cfg = TcpIngestConfig {
            handshake_timeout: Duration::ZERO,
            ..ephemeral_config()
        };
        match TcpIngestPort::bind(cfg).await {
            Err(IngestError::Io(io_err)) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidInput);
                assert!(
                    io_err.to_string().contains("handshake_timeout"),
                    "error message should mention the offending field, got {io_err}"
                );
            }
            Err(other) => panic!("expected IngestError::Io, got {other:?}"),
            Ok(_) => panic!("zero handshake_timeout should be rejected"),
        }
    }

    #[tokio::test]
    async fn accepts_valid_config() {
        // Sanity check: the validator does not reject a well-formed config.
        let port = match TcpIngestPort::bind(ephemeral_config()).await {
            Ok(p) => p,
            Err(e) => panic!("valid config should bind, got {e:?}"),
        };
        // Confirm the listener really came up.
        let addr = port.local_addr().expect("local_addr");
        assert_ne!(addr.port(), 0, "OS should have assigned a real port");
    }
}
