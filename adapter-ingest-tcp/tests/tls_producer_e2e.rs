//! End-to-end TLS: `otk-sdk::Producer` (client) ↔ `TcpIngestPort` (server)
//! over a real rustls handshake plus the OTK Connect/ConnectAck flow.
//!
//! Complements `tls_handshake.rs` (which uses a raw rustls client to
//! exercise the server side in isolation). This test proves the
//! producer SDK's `Transport::Tls` path interoperates with the server
//! crate end-to-end.

#![cfg(feature = "tls")]

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use adapter_ingest_tcp::{TcpIngestConfig, TcpIngestPort, TlsConfig};
use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, SubjectId,
    TimebaseId, TimestampingMethod, TimingPointId,
};
use ingest_protocol::AllowAll;
use otk_sdk::producer::{Producer, ProducerConfig, TlsClientConfig, Transport};
use tempfile::TempDir;
use timing_core::ports::inbound::EventIngestPort;

struct E2eFixture {
    _tmp: TempDir,
    server_cert_chain: PathBuf,
    server_key: PathBuf,
    client_trust_roots: PathBuf,
}

fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(pem.as_bytes())
        .unwrap();
    path
}

fn gen_fixture() -> E2eFixture {
    let tmp = TempDir::new().unwrap();
    // Self-signed CA + leaf cert with SAN = "localhost".
    let ca = rcgen::generate_simple_self_signed(vec!["otk-e2e-ca".to_string()]).unwrap();
    let server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let server_cert = server_params
        .signed_by(&ca.key_pair, &ca.cert, &ca.key_pair)
        .unwrap();
    let server_chain_pem = format!("{}{}", server_cert.pem(), ca.cert.pem());
    let server_key_pem = ca.key_pair.serialize_pem();

    let server_cert_chain = write_pem(tmp.path(), "server-chain.pem", &server_chain_pem);
    let server_key = write_pem(tmp.path(), "server-key.pem", &server_key_pem);
    let client_trust_roots = write_pem(tmp.path(), "trust-roots.pem", &ca.cert.pem());

    E2eFixture {
        _tmp: tmp,
        server_cert_chain,
        server_key,
        client_trust_roots,
    }
}

fn detection() -> OtkEvent {
    OtkEvent::Detection(Detection {
        detection_id: DetectionId::new("d-1"),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new("bib-42")),
        detected_at_ns: 1_700_000_000_000_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: 1,
        sensor: SensorData::BeamBreak,
    })
}

#[tokio::test]
async fn otk_sdk_producer_connects_via_tls() {
    let fx = gen_fixture();

    let server_config = TcpIngestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_frame_bytes: 65_535,
        handshake_timeout: Duration::from_secs(5),
        tls: Some(TlsConfig {
            cert_chain: fx.server_cert_chain.clone(),
            private_key: fx.server_key.clone(),
            client_ca: None,
        }),
    };
    let port = TcpIngestPort::bind_with_auth(server_config, Arc::new(AllowAll))
        .await
        .unwrap();
    let addr = port.local_addr().unwrap();

    let producer_config = ProducerConfig::new("e2e-tls-producer");
    let transport = Transport::Tls {
        addr,
        config: TlsClientConfig {
            trust_roots: fx.client_trust_roots.clone(),
            server_name: "localhost".to_string(),
            client_cert: None,
            client_key: None,
        },
    };

    let (server_session_result, producer_result) =
        tokio::join!(port.accept(), Producer::connect(transport, producer_config));

    let mut session = server_session_result.expect("server accepts TLS producer");
    let mut producer = producer_result.expect("producer completes TLS + OTK handshake");

    // Send one event through the TLS channel and confirm the server
    // delivers it to the IngestSession layer.
    producer.send_event(detection()).await.expect("send_event");

    let incoming = session
        .next_event()
        .await
        .expect("next_event")
        .expect("Some(event)");
    match incoming.event {
        OtkEvent::Detection(d) => {
            assert_eq!(d.detection_id.as_str(), "d-1");
        }
        other => panic!("expected Detection, got {other:?}"),
    }

    producer.disconnect().await.expect("disconnect");
}
