//! TLS support for [`crate::TcpIngestPort`], behind the `tls` feature.
//!
//! Loads a PEM cert chain + private key (and an optional client-CA
//! bundle for mutual TLS) at bind time and produces a
//! [`tokio_rustls::TlsAcceptor`] for the listener to wrap each
//! accepted `TcpStream` in. The session code itself is generic over
//! the byte stream and treats the wrapped `TlsStream<TcpStream>` the
//! same as a plain `TcpStream`.
//!
//! # Cert rotation
//!
//! Material is read once at bind time. To rotate certs, restart the
//! listener (or restart the node). Online rotation via a watcher is
//! tracked under "config hot-reload" in `spec/open-questions.md` and
//! is out of scope for the initial TLS landing.

use std::fs;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use thiserror::Error;
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;

/// Errors that surface at `bind` time when TLS material can't be loaded.
///
/// Folded into `IngestError::Io(InvalidInput)` by the caller so the
/// existing accept-loop error vocabulary doesn't grow another variant.
#[derive(Debug, Error)]
pub enum TlsAcceptError {
    #[error("failed to read TLS file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("TLS cert chain at {0} contained no certificates")]
    NoCertificates(String),
    #[error("TLS key file at {0} contained no usable private key")]
    NoPrivateKey(String),
    #[error("TLS client-CA file at {0} contained no certificates")]
    NoClientCaCerts(String),
    #[error("rustls rejected the loaded material: {0}")]
    RustlsBuild(String),
}

/// Build a [`TlsAcceptor`] from on-disk PEM files.
///
/// Reads the cert chain and private key from the paths in `cfg`. If
/// `cfg.client_ca` is set, also configures a client-cert verifier
/// against that CA bundle (mutual TLS).
pub fn build_tls_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor, TlsAcceptError> {
    install_default_crypto_provider();
    let cert_chain = load_certs(&cfg.cert_chain)?;
    let private_key = load_private_key(&cfg.private_key)?;

    let builder = ServerConfig::builder();

    let server_config = if let Some(ca_path) = cfg.client_ca.as_ref() {
        let mut roots = RootCertStore::empty();
        let ca_certs = load_certs(ca_path)?;
        if ca_certs.is_empty() {
            return Err(TlsAcceptError::NoClientCaCerts(display(ca_path)));
        }
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|e| TlsAcceptError::RustlsBuild(e.to_string()))?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TlsAcceptError::RustlsBuild(e.to_string()))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TlsAcceptError::RustlsBuild(e.to_string()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TlsAcceptError::RustlsBuild(e.to_string()))?
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsAcceptError> {
    let file = fs::File::open(path).map_err(|source| TlsAcceptError::Io {
        path: display(path),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs.map_err(|source| TlsAcceptError::Io {
        path: display(path),
        source,
    })?;
    if certs.is_empty() {
        return Err(TlsAcceptError::NoCertificates(display(path)));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsAcceptError> {
    let file = fs::File::open(path).map_err(|source| TlsAcceptError::Io {
        path: display(path),
        source,
    })?;
    let mut reader = BufReader::new(file);
    // `private_key` accepts PKCS#8, RSA, and SEC1 in one pass and is the
    // documented "I don't care which format" entry point.
    let key = rustls_pemfile::private_key(&mut reader).map_err(|source| TlsAcceptError::Io {
        path: display(path),
        source,
    })?;
    key.ok_or_else(|| TlsAcceptError::NoPrivateKey(display(path)))
}

fn display(path: &Path) -> String {
    path.display().to_string()
}

/// Make sure rustls has a process-default crypto provider installed.
///
/// rustls 0.23 dropped automatic provider selection when more than one
/// provider feature is compiled into the binary (it can happen in
/// workspace tests where reqwest brings in `ring` and we explicitly
/// want `aws_lc_rs`). Installing once at the top of every TLS entry
/// point is harmless: `install_default` is idempotent (returns
/// `Err(already)` after the first call, which we discard).
fn install_default_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
