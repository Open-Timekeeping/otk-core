//! End-to-end restart-resume: kill the runtime mid-stream, start a fresh
//! Node against the same storage directory, and verify a reconnecting
//! producer with the same producer_id cannot replay a previously-acked
//! sequence number.
//!
//! This exercises the full stack: producer connects over TCP, sends
//! detections, runtime persists to the segment log (capturing the
//! producer_id added in PR #11), runtime shuts down, fresh runtime
//! opens the same log, [`timing_node::sequence_gate::seed_from_log_box`]
//! rebuilds the high-water marks during `Node::new`, and the gate
//! rejects replays as soon as the new listener is reachable.

use std::path::{Path, PathBuf};

use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, SubjectId,
    TimebaseId, TimestampingMethod, TimingPointId,
};
use otk_protocol::{
    ids::ProducerId, Connect, ConnectAck, MessageType, OtkEnvelope, PROTOCOL_VERSION,
};
use port_out_event_log::{EventLog, Offset};
use timing_node::{ListenerConfig, Node, NodeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PRODUCER_ID: &str = "loop-adapter-1";
const DETECTOR_ID: &str = "loop-1";

// ── Wire helpers (copied from ingest_test.rs deliberately; this is a
//    different test file with its own setup, and shared helpers would
//    pull in a non-trivial test fixture crate. The duplication is
//    bounded and stable.) ───────────────────────────────────────────

fn encode_frame(envelope: &OtkEnvelope) -> Vec<u8> {
    let cbor = minicbor::to_vec(envelope).expect("envelope encodes");
    let mut frame = Vec::with_capacity(4 + cbor.len());
    frame.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
    frame.extend_from_slice(&cbor);
    frame
}

async fn recv_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let to = tokio::time::Duration::from_secs(5);
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(to, stream.read_exact(&mut len_buf))
        .await
        .expect("recv_frame: timed out reading length prefix")
        .expect("recv_frame: I/O error reading length prefix");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    tokio::time::timeout(to, stream.read_exact(&mut payload))
        .await
        .expect("recv_frame: timed out reading payload")
        .expect("recv_frame: I/O error reading payload");
    payload
}

fn make_connect_envelope() -> OtkEnvelope {
    let connect = Connect {
        protocol_version_min: PROTOCOL_VERSION,
        protocol_version_max: PROTOCOL_VERSION,
        streams: vec![],
        auth_token: None,
    };
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Connect,
        source_id: ProducerId::new(PRODUCER_ID),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(minicbor::to_vec(&connect).expect("Connect encodes")),
        traceparent: None,
    }
}

fn make_event_envelope(seq: u64, event: OtkEvent) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Event,
        source_id: ProducerId::new(PRODUCER_ID),
        stream_id: None,
        sequence_number: Some(seq),
        correlation_id: None,
        payload: Some(minicbor::to_vec(&event).expect("OtkEvent encodes")),
        traceparent: None,
    }
}

fn make_disconnect_envelope() -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Disconnect,
        source_id: ProducerId::new(PRODUCER_ID),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: None,
        traceparent: None,
    }
}

fn detection(seq: u64) -> Detection {
    Detection {
        detection_id: DetectionId::new(format!("d-{seq}")),
        detector_id: DetectorId::new(DETECTOR_ID),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new("bib-42")),
        // Bump the detection wall-clock alongside seq so the timing-core
        // window can group them; the exact values don't matter to the
        // gate, only the sequence_number does.
        detected_at_ns: 1_700_000_000_000_000_000 + seq * 1_000_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: seq,
        sensor: SensorData::BeamBreak,
    }
}

fn node_config_for(storage_dir: &Path) -> NodeConfig {
    NodeConfig {
        node_id: "test-node".into(),
        listeners: vec![ListenerConfig::Tcp {
            id: "tcp-main".into(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
        }],
        api_addr: "127.0.0.1:0".parse().unwrap(),
        storage_dir: storage_dir.to_path_buf(),
        auth: Default::default(),
        api: Default::default(),
    }
}

/// Spin up a Node, do one handshake + send the given detection sequences,
/// graceful disconnect, then shut down and wait for the listener + API
/// tasks to drain. Returns the storage_dir so the caller can reopen it
/// for the second run.
async fn run_round(storage_dir: PathBuf, sequences: &[u64]) {
    let node = Node::new(node_config_for(&storage_dir)).await.unwrap();
    let addr = node.local_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest_tasks, api_task) = node.run_with_shutdown(shutdown_rx);

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(&encode_frame(&make_connect_envelope()))
        .await
        .unwrap();
    let ack_bytes = recv_frame(&mut stream).await;
    let ack_env: OtkEnvelope = minicbor::decode(&ack_bytes).expect("ConnectAck decodes");
    assert_eq!(ack_env.message_type, MessageType::ConnectAck);
    let ack: ConnectAck = minicbor::decode(ack_env.payload.as_deref().unwrap()).unwrap();
    assert_eq!(ack.negotiated_version, PROTOCOL_VERSION);

    for &seq in sequences {
        let frame = encode_frame(&make_event_envelope(
            seq,
            OtkEvent::Detection(detection(seq)),
        ));
        stream.write_all(&frame).await.unwrap();
    }
    stream
        .write_all(&encode_frame(&make_disconnect_envelope()))
        .await
        .unwrap();
    drop(stream);

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

#[tokio::test]
async fn duplicate_sequence_after_restart_is_dropped() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_dir: PathBuf = tmp.path().to_path_buf();

    // Round 1: producer sends seqs 1, 2, 3. All persisted.
    run_round(storage_dir.clone(), &[1, 2, 3]).await;

    // Verify the storage state before restart so a regression in the
    // first-round persistence path can't masquerade as a seed bug.
    let pre_restart_latest = {
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: storage_dir.clone(),
            ..SegmentLogConfig::default()
        })
        .await
        .unwrap();
        log.latest_offset().await.unwrap()
    };
    assert_eq!(
        pre_restart_latest,
        Some(Offset::new(2)),
        "round 1 should have persisted exactly 3 detections (offsets 0..=2)"
    );

    // Round 2: restart the node, reconnect with the same producer_id,
    // try to replay seq 3 then send a fresh seq 4. The gate, seeded
    // from the log during Node::new, must reject the replay and accept
    // the fresh one.
    run_round(storage_dir.clone(), &[3, 4]).await;

    // After round 2: only seq 4 should have been newly persisted (the
    // replay of seq 3 must have been dropped). Total persisted: 4.
    let mut log = SegmentLog::open(SegmentLogConfig {
        dir: storage_dir.clone(),
        ..SegmentLogConfig::default()
    })
    .await
    .unwrap();
    let latest = log.latest_offset().await.unwrap();
    assert_eq!(
        latest,
        Some(Offset::new(3)),
        "replay of seq 3 must not have been persisted; only seq 4 should be new"
    );

    // Confirm the new entry is for seq 4 (not a duplicate of seq 3).
    let entries = log.read_range(Offset::new(0), None).await.unwrap();
    assert_eq!(entries.len(), 4);
    let last = match &entries[3].event {
        OtkEvent::Detection(d) => d,
        other => panic!("expected Detection at offset 3, got {other:?}"),
    };
    assert_eq!(
        last.sequence_number, 4,
        "the only event written in round 2 must be seq 4"
    );
    assert_eq!(
        entries[3].producer_id, PRODUCER_ID,
        "round-2 entry must carry the producer_id of the reconnecting client"
    );
}

#[tokio::test]
async fn fresh_storage_dir_accepts_any_sequence() {
    // Negative control: without prior persisted state, the gate is empty
    // and any first sequence is accepted. Catches a regression where the
    // seed accidentally seeds a phantom high-water from a missing-file
    // path.
    let tmp = tempfile::tempdir().unwrap();
    let storage_dir: PathBuf = tmp.path().to_path_buf();

    // Send a single detection with a deliberately-not-1 sequence number.
    // First-detection semantics say it must be accepted as the seed.
    run_round(storage_dir.clone(), &[100]).await;

    let mut log = SegmentLog::open(SegmentLogConfig {
        dir: storage_dir,
        ..SegmentLogConfig::default()
    })
    .await
    .unwrap();
    let latest = log.latest_offset().await.unwrap();
    assert_eq!(
        latest,
        Some(Offset::new(0)),
        "fresh log should hold the single detection at offset 0"
    );
}
