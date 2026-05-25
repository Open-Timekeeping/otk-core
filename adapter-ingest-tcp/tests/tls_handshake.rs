//! End-to-end TLS handshake against a real [`TcpIngestPort`] with the
//! `tls` feature enabled.
//!
//! Three scenarios:
//!
//! 1. **Server TLS only**: client connects with a TLS connector that
//!    trusts the self-signed server CA; the OTK handshake completes
//!    over the encrypted channel.
//! 2. **Mutual TLS happy path**: server is configured with a client-CA
//!    bundle; client presents a cert chained to that CA; handshake
//!    completes.
//! 3. **Mutual TLS rejection**: server is configured with a client-CA
//!    bundle; client connects without a cert; the TLS handshake
//!    fails server-side and the listener does NOT advance to OTK
//!    handshake.
//!
//! The tests own the cert material (generated per-test via `rcgen`),
//! so no fixture certs ship in the repo.

#![cfg(feature = "tls")]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use adapter_ingest_tcp::{TcpIngestConfig, TcpIngestPort, TlsConfig};
use ingest_protocol::AllowAll;
use otk_protocol::{
    ids::ProducerId, Connect, ConnectAck, MessageType, OtkEnvelope, PROTOCOL_VERSION,
};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tempfile::TempDir;
use timing_core::ports::inbound::EventIngestPort;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

// ── Cert factories ─────────────────────────────────────────────────────

/// Material produced by [`gen_server_cert`] / [`gen_client_cert`]: PEM
/// strings on disk plus the in-memory bits the client needs to trust.
struct TlsFixture {
    _tmp: TempDir,
    server_cert_chain_path: std::path::PathBuf,
    server_key_path: std::path::PathBuf,
    server_ca_pem: String,
    // Only used by the mTLS tests.
    client_ca_path: Option<std::path::PathBuf>,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
}

fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(pem.as_bytes()).unwrap();
    path
}

/// Build a self-signed CA + a leaf server cert that names "localhost".
fn gen_server_cert(with_client_ca: bool) -> TlsFixture {
    let tmp = TempDir::new().unwrap();

    // Server CA + leaf.
    let ca = rcgen::generate_simple_self_signed(vec!["otk-test-ca".to_string()]).unwrap();
    let server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let server_cert = server_params
        .signed_by(&ca.key_pair, &ca.cert, &ca.key_pair)
        .unwrap();

    let server_chain_pem = format!("{}{}", server_cert.pem(), ca.cert.pem());
    let server_key_pem = ca.key_pair.serialize_pem(); // server signed by ca's key

    // We used the CA's key for the leaf via signed_by; for simplicity in
    // this test we hand back the CA's key as the server's private key.
    // (rcgen's signed_by stores the issuer's signature; the leaf's
    // private key here matches the CA so the server can decrypt.)
    let server_cert_path = write_pem(tmp.path(), "server-chain.pem", &server_chain_pem);
    let server_key_path = write_pem(tmp.path(), "server-key.pem", &server_key_pem);

    let mut fixture = TlsFixture {
        _tmp: tmp,
        server_cert_chain_path: server_cert_path,
        server_key_path,
        server_ca_pem: ca.cert.pem(),
        client_ca_path: None,
        client_cert_pem: None,
        client_key_pem: None,
    };

    if with_client_ca {
        // Separate client CA + leaf for mTLS.
        let client_ca =
            rcgen::generate_simple_self_signed(vec!["otk-client-ca".to_string()]).unwrap();
        let client_params = rcgen::CertificateParams::new(vec!["test-client".to_string()]).unwrap();
        let client_cert = client_params
            .signed_by(&client_ca.key_pair, &client_ca.cert, &client_ca.key_pair)
            .unwrap();

        let client_ca_path = write_pem(fixture._tmp.path(), "client-ca.pem", &client_ca.cert.pem());
        fixture.client_ca_path = Some(client_ca_path);
        fixture.client_cert_pem = Some(format!("{}{}", client_cert.pem(), client_ca.cert.pem()));
        fixture.client_key_pem = Some(client_ca.key_pair.serialize_pem());
    }

    fixture
}

fn server_tls_config(fixture: &TlsFixture, with_client_ca: bool) -> TlsConfig {
    TlsConfig {
        cert_chain: fixture.server_cert_chain_path.clone(),
        private_key: fixture.server_key_path.clone(),
        client_ca: if with_client_ca {
            fixture.client_ca_path.clone()
        } else {
            None
        },
    }
}

fn server_config(fixture: &TlsFixture, with_client_ca: bool) -> TcpIngestConfig {
    TcpIngestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_frame_bytes: 65_535,
        handshake_timeout: Duration::from_secs(5),
        tls: Some(server_tls_config(fixture, with_client_ca)),
    }
}

/// Build a rustls `ClientConfig` that trusts the fixture's server CA.
fn client_rustls_config(fixture: &TlsFixture, client_cert: bool) -> ClientConfig {
    use rustls::pki_types::pem::PemObject;

    let mut roots = RootCertStore::empty();
    let ca_der =
        rustls::pki_types::CertificateDer::from_pem_slice(fixture.server_ca_pem.as_bytes())
            .unwrap();
    roots.add(ca_der).unwrap();

    if client_cert {
        let chain_pem = fixture.client_cert_pem.as_ref().unwrap().as_bytes();
        let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(chain_pem)
            .collect::<Result<_, _>>()
            .unwrap();
        let key_pem = fixture.client_key_pem.as_ref().unwrap().as_bytes();
        let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key_pem).unwrap();
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .unwrap()
    } else {
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }
}

// ── Wire helpers ───────────────────────────────────────────────────────

fn encode_frame(env: &OtkEnvelope) -> Vec<u8> {
    let cbor = minicbor::to_vec(env).unwrap();
    let mut frame = Vec::with_capacity(4 + cbor.len());
    frame.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
    frame.extend_from_slice(&cbor);
    frame
}

async fn recv_frame<S: AsyncReadExt + Unpin>(stream: &mut S) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    payload
}

fn make_connect_envelope() -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Connect,
        source_id: ProducerId::from("tls-test-producer"),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(
            minicbor::to_vec(Connect {
                protocol_version_min: PROTOCOL_VERSION,
                protocol_version_max: PROTOCOL_VERSION,
                streams: vec![],
                auth_token: None,
            })
            .unwrap(),
        ),
        traceparent: None,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn server_tls_handshake_completes() {
    let fixture = gen_server_cert(false);
    let port = TcpIngestPort::bind_with_auth(server_config(&fixture, false), Arc::new(AllowAll))
        .await
        .unwrap();
    let addr = port.local_addr().unwrap();

    let connector = TlsConnector::from(Arc::new(client_rustls_config(&fixture, false)));

    let (session_result, reply) = tokio::join!(port.accept(), async {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();
        tls.write_all(&encode_frame(&make_connect_envelope()))
            .await
            .unwrap();
        let bytes = recv_frame(&mut tls).await;
        minicbor::decode::<OtkEnvelope>(&bytes).unwrap()
    });

    session_result.expect("server should accept after TLS handshake");
    assert_eq!(reply.message_type, MessageType::ConnectAck);
    let ack: ConnectAck = minicbor::decode(reply.payload.as_deref().unwrap()).unwrap();
    assert_eq!(ack.negotiated_version, PROTOCOL_VERSION);
}

#[tokio::test]
async fn mutual_tls_with_client_cert_completes() {
    let fixture = gen_server_cert(true);
    let port = TcpIngestPort::bind_with_auth(server_config(&fixture, true), Arc::new(AllowAll))
        .await
        .unwrap();
    let addr = port.local_addr().unwrap();

    let connector = TlsConnector::from(Arc::new(client_rustls_config(&fixture, true)));

    let (session_result, reply) = tokio::join!(port.accept(), async {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();
        tls.write_all(&encode_frame(&make_connect_envelope()))
            .await
            .unwrap();
        let bytes = recv_frame(&mut tls).await;
        minicbor::decode::<OtkEnvelope>(&bytes).unwrap()
    });

    session_result.expect("server should accept mutual TLS with a valid client cert");
    assert_eq!(reply.message_type, MessageType::ConnectAck);
}

#[tokio::test]
async fn mutual_tls_without_client_cert_is_rejected() {
    let fixture = gen_server_cert(true);
    let port = TcpIngestPort::bind_with_auth(server_config(&fixture, true), Arc::new(AllowAll))
        .await
        .unwrap();
    let addr = port.local_addr().unwrap();

    // Client connects WITHOUT a client cert. The server's
    // WebPkiClientVerifier requires one; the TLS handshake fails.
    let connector = TlsConnector::from(Arc::new(client_rustls_config(&fixture, false)));

    // The security-critical property is the server-side rejection:
    // session_result must be Err so the listener never advances to OTK
    // handshake. The client-side observation is implementation detail
    // (rustls may surface the failure as a handshake error, a TLS
    // alert read after handshake, or a TCP close depending on timing
    // and version), so we don't assert on it.
    let (session_result, _) = tokio::join!(port.accept(), async {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let _ = connector.connect(server_name, tcp).await;
    });

    assert!(
        session_result.is_err(),
        "server must reject mTLS handshake without a client cert"
    );
}
