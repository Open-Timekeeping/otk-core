use std::path::PathBuf;

use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, SubjectId,
    TimebaseId, TimestampingMethod, TimingPointId,
};
use otk_protocol::{
    ids::ProducerId, Connect, ConnectAck, MessageType, OtkEnvelope, PROTOCOL_VERSION,
};
use timing_core::ports::outbound::{EventLog, Offset};
use timing_node::{ListenerConfig, Node, NodeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn encode_frame(envelope: &OtkEnvelope) -> Vec<u8> {
    let cbor = minicbor::to_vec(envelope).expect("envelope encodes");
    let mut frame = Vec::with_capacity(4 + cbor.len());
    frame.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
    frame.extend_from_slice(&cbor);
    frame
}

/// Read one length-prefixed frame, with a per-read timeout so a
/// silent or truncated server can't hang the test under
/// `cargo test`'s parallel runner. 5 s is well beyond any legitimate
/// handshake reply (sub-millisecond locally) but short enough that a
/// failing test fails deterministically.
async fn recv_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let read_timeout = tokio::time::Duration::from_secs(5);
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(read_timeout, stream.read_exact(&mut len_buf))
        .await
        .expect("recv_frame: timed out reading length prefix")
        .expect("recv_frame: I/O error reading length prefix");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    tokio::time::timeout(read_timeout, stream.read_exact(&mut payload))
        .await
        .expect("recv_frame: timed out reading payload")
        .expect("recv_frame: I/O error reading payload");
    payload
}

fn make_connect_envelope(producer_id: &str) -> OtkEnvelope {
    let connect = Connect {
        protocol_version_min: PROTOCOL_VERSION,
        protocol_version_max: PROTOCOL_VERSION,
        streams: vec![],
        auth_token: None,
    };
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Connect,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(minicbor::to_vec(&connect).expect("Connect encodes")),
        traceparent: None,
    }
}

fn make_event_envelope(producer_id: &str, seq: u64, event: OtkEvent) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Event,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: Some(seq),
        correlation_id: None,
        payload: Some(minicbor::to_vec(&event).expect("OtkEvent encodes")),
        traceparent: None,
    }
}

fn make_disconnect_envelope(producer_id: &str) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Disconnect,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: None,
        traceparent: None,
    }
}

fn make_detection() -> Detection {
    Detection {
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
        sensor: SensorData::LoopTransponder {
            rssi_dbm: Some(-60),
            pulse_count: None,
        },
    }
}

#[tokio::test]
async fn handshake_and_detection_stored() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_dir: PathBuf = tmp.path().to_path_buf();

    let config = NodeConfig {
        node_id: "test-node".into(),
        listeners: vec![ListenerConfig::Tcp {
            id: "tcp-main".into(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
            tls: None,
        }],
        api_addr: "127.0.0.1:0".parse().unwrap(),
        storage_dir: storage_dir.clone(),
        auth: Default::default(),
        api: Default::default(),
    };

    let node = Node::new(config).await.unwrap();
    let addr = node.local_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest_tasks, api_task) = node.run_with_shutdown(shutdown_rx);

    // Connect and handshake.
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

    let connect_frame = encode_frame(&make_connect_envelope("test-adapter"));
    stream.write_all(&connect_frame).await.unwrap();

    let ack_bytes = recv_frame(&mut stream).await;
    let ack_env: OtkEnvelope = minicbor::decode(&ack_bytes).expect("ConnectAck decodes");
    assert_eq!(ack_env.message_type, MessageType::ConnectAck);
    let ack: ConnectAck =
        minicbor::decode(ack_env.payload.as_deref().unwrap()).expect("ConnectAck payload decodes");
    assert_eq!(ack.negotiated_version, PROTOCOL_VERSION);

    // Send one detection event.
    let event = OtkEvent::Detection(make_detection());
    let event_frame = encode_frame(&make_event_envelope("test-adapter", 1, event));
    stream.write_all(&event_frame).await.unwrap();

    // Graceful disconnect.
    let disc_frame = encode_frame(&make_disconnect_envelope("test-adapter"));
    stream.write_all(&disc_frame).await.unwrap();

    // Shut down; run_listener now drains all connection tasks before returning,
    // so the event is guaranteed to be persisted by the time we assert below.
    let _ = shutdown_tx.send(true);
    for t in ingest_tasks {
        tokio::time::timeout(tokio::time::Duration::from_secs(5), t)
            .await
            .expect("listener shutdown timed out")
            .expect("listener task panicked");
    }
    // Drain the API task too: dropping a JoinHandle does not cancel the task,
    // and under `cargo test`'s parallel runner an orphaned API server with
    // an open port can flake the next-run test by holding state alive.
    tokio::time::timeout(tokio::time::Duration::from_secs(5), api_task)
        .await
        .expect("api task shutdown timed out")
        .expect("api task panicked");

    // Verify the detection was persisted.
    let log_config = SegmentLogConfig {
        dir: storage_dir,
        ..SegmentLogConfig::default()
    };
    let mut log = SegmentLog::open(log_config).await.unwrap();
    let latest = log.latest_offset().await.unwrap();
    assert_eq!(
        latest,
        Some(Offset::new(0)),
        "one event should have been stored"
    );
}

#[tokio::test]
async fn version_mismatch_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let config = NodeConfig {
        node_id: "test-node".into(),
        listeners: vec![ListenerConfig::Tcp {
            id: "tcp-main".into(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
            tls: None,
        }],
        api_addr: "127.0.0.1:0".parse().unwrap(),
        storage_dir: tmp.path().to_path_buf(),
        auth: Default::default(),
        api: Default::default(),
    };

    let node = Node::new(config).await.unwrap();
    let addr = node.local_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest_tasks, api_task) = node.run_with_shutdown(shutdown_rx);

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Send Connect with a version range that does not include PROTOCOL_VERSION.
    let connect = Connect {
        protocol_version_min: 99,
        protocol_version_max: 99,
        streams: vec![],
        auth_token: None,
    };
    let env = OtkEnvelope {
        protocol_version: 99,
        message_type: MessageType::Connect,
        source_id: ProducerId::new("bad-adapter"),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(minicbor::to_vec(&connect).unwrap()),
        traceparent: None,
    };
    stream.write_all(&encode_frame(&env)).await.unwrap();

    let reject_bytes = recv_frame(&mut stream).await;
    let reject_env: OtkEnvelope = minicbor::decode(&reject_bytes).expect("ConnectReject decodes");
    assert_eq!(reject_env.message_type, MessageType::ConnectReject);

    let _ = shutdown_tx.send(true);
    for t in ingest_tasks {
        tokio::time::timeout(tokio::time::Duration::from_secs(5), t)
            .await
            .expect("listener shutdown timed out")
            .expect("listener task panicked");
    }
    tokio::time::timeout(tokio::time::Duration::from_secs(5), api_task)
        .await
        .expect("api task shutdown timed out")
        .expect("api task panicked");
}
