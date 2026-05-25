/// Transport endpoint for a producer connection.
///
/// The `Tls` variant is only available with the `producer-tls` feature.
/// Serial port support is planned (see `producer-serial` in the open
/// questions section of the README).
#[derive(Debug, Clone)]
pub enum Transport {
    /// Plain TCP to the given address.
    Tcp(std::net::SocketAddr),
    /// TLS over TCP to the given address, with rustls config (server
    /// trust roots, SNI, and optional client cert for mutual TLS) from
    /// [`TlsClientConfig`].
    #[cfg(feature = "producer-tls")]
    Tls {
        addr: std::net::SocketAddr,
        config: TlsClientConfig,
    },
}

/// Client-side TLS configuration for [`Transport::Tls`].
///
/// PEM-encoded inputs are loaded once at `Producer::connect` time and
/// rejected with `ProducerError::Config` if any path is missing or
/// unreadable. The SNI server name is sent in the TLS ClientHello and
/// must match a Subject Alternative Name on the server's leaf cert.
///
/// `client_cert` and `client_key` are paired: present both for mutual
/// TLS, absent for server-auth-only TLS. Passing one without the other
/// is rejected at connect time.
#[cfg(feature = "producer-tls")]
#[derive(Debug, Clone)]
pub struct TlsClientConfig {
    /// Path to a PEM file containing the trust roots (the server's CA
    /// chain). Self-signed deployments point this at the single
    /// self-signed cert; production deployments point it at the
    /// issuing CA bundle.
    pub trust_roots: std::path::PathBuf,
    /// SNI server name presented in the TLS ClientHello. Must match a
    /// Subject Alternative Name on the server's leaf cert; for
    /// self-signed deployments this is usually a stable internal
    /// hostname (e.g. `otk-node.lan`).
    pub server_name: String,
    /// Optional PEM file containing this client's certificate chain
    /// (leaf first). Required for mTLS deployments.
    pub client_cert: Option<std::path::PathBuf>,
    /// Optional PEM file containing this client's private key. Paired
    /// with `client_cert`.
    pub client_key: Option<std::path::PathBuf>,
}
