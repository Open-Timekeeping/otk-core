use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ingest_protocol::{AllowAll, ConnectAuthoriser};
use port_in_ingest::{EventIngestPort, IngestError, IngestSession};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::config::TcpIngestConfig;
use crate::session::TcpIngestSession;

pub struct TcpIngestPort {
    listener: TcpListener,
    config: Arc<TcpIngestConfig>,
    authoriser: Arc<dyn ConnectAuthoriser>,
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

        let listener = TcpListener::bind(config.bind_addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            authoriser,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}

#[async_trait]
impl EventIngestPort for TcpIngestPort {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError> {
        let (stream, peer) = self.listener.accept().await?;
        let session = timeout(
            self.config.handshake_timeout,
            TcpIngestSession::handshake(
                stream,
                peer.to_string(),
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
