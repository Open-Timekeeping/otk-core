//! Client-side TLS for [`crate::producer::Producer`], behind the
//! `producer-tls` feature.
//!
//! Loads PEM trust roots (and optional client cert + key for mutual
//! TLS) from disk and runs a tokio-rustls TLS handshake against the
//! server before handing the encrypted stream back to the OTK
//! handshake / framing layers.
//!
//! # Cert lifetime
//!
//! Material is read once per `Producer::connect` call. To rotate
//! certs, reconnect the producer.

use std::fs;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::producer::error::ProducerError;
use crate::producer::transport::TlsClientConfig;

/// TCP-connect to `addr` then run a TLS handshake using the supplied
/// trust roots, SNI, and optional client cert/key. Returns the
/// connected `TlsStream<TcpStream>` ready for the OTK Connect frame.
pub(crate) async fn connect_tls(
    addr: SocketAddr,
    cfg: &TlsClientConfig,
) -> Result<TlsStream<TcpStream>, ProducerError> {
    // rustls 0.23 requires an explicit default CryptoProvider when more
    // than one provider feature is compiled in (the multi-provider case
    // happens in workspace tests where reqwest brings in `ring` and we
    // explicitly want `aws_lc_rs`). `install_default` is idempotent.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Validate cert/key pairing up front: a half-configured mTLS pair
    // is a misconfiguration that's better caught here than in the
    // middle of the TLS handshake.
    match (cfg.client_cert.as_ref(), cfg.client_key.as_ref()) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(ProducerError::Config(
                "TlsClientConfig: client_cert and client_key must both be set or both be None"
                    .into(),
            ));
        }
        _ => {}
    }

    let mut roots = RootCertStore::empty();
    for cert in load_certs(&cfg.trust_roots)? {
        roots
            .add(cert)
            .map_err(|e| ProducerError::Config(format!("trust root rejected: {e}")))?;
    }

    let client_config = if let (Some(cert_path), Some(key_path)) =
        (cfg.client_cert.as_ref(), cfg.client_key.as_ref())
    {
        let chain = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| ProducerError::Config(format!("rustls rejected client cert: {e}")))?
    } else {
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    let server_name = ServerName::try_from(cfg.server_name.clone()).map_err(|e| {
        ProducerError::Config(format!(
            "invalid SNI server name {:?}: {e}",
            cfg.server_name
        ))
    })?;

    let connector = TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(addr).await?;
    let tls = connector.connect(server_name, tcp).await.map_err(|e| {
        ProducerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("TLS handshake failed: {e}"),
        ))
    })?;
    Ok(tls)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, ProducerError> {
    let file = fs::File::open(path)
        .map_err(|e| ProducerError::Config(format!("open {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| ProducerError::Config(format!("read {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(ProducerError::Config(format!(
            "{}: no PEM certificates",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, ProducerError> {
    let file = fs::File::open(path)
        .map_err(|e| ProducerError::Config(format!("open {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| ProducerError::Config(format!("read {}: {e}", path.display())))?
        .ok_or_else(|| ProducerError::Config(format!("{}: no PEM private key", path.display())))
}
