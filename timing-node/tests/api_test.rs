use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, SubjectId,
    TimebaseId, TimestampingMethod, TimingPointId,
};
use futures_util::StreamExt;
use protocol::{ids::ProducerId, Connect, MessageType, OtkEnvelope, PROTOCOL_VERSION};
use reqwest::Client;
use timing_node::{ListenerConfig, Node, NodeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── wire helpers ─────────────────────────────────────────────────────────────

fn encode_frame(envelope: &OtkEnvelope) -> Vec<u8> {
    let cbor = minicbor::to_vec(envelope).expect("encode");
    let mut frame = Vec::with_capacity(4 + cbor.len());
    frame.extend_from_slice(&(cbor.len() as u32).to_be_bytes());
    frame.extend_from_slice(&cbor);
    frame
}

async fn recv_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.expect("read length");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.expect("read payload");
    payload
}

fn connect_envelope(producer_id: &str) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Connect,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: Some(
            minicbor::to_vec(&Connect {
                protocol_version_min: PROTOCOL_VERSION,
                protocol_version_max: PROTOCOL_VERSION,
                streams: vec![],
                auth_token: None,
            })
            .unwrap(),
        ),
    }
}

fn event_envelope(producer_id: &str, seq: u64, event: OtkEvent) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Event,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: Some(seq),
        correlation_id: None,
        payload: Some(minicbor::to_vec(&event).unwrap()),
    }
}

fn disconnect_envelope(producer_id: &str) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Disconnect,
        source_id: ProducerId::new(producer_id),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: None,
    }
}

// ── event builders ────────────────────────────────────────────────────────────

fn named_detection(idx: u64, detected_at_ns: u64) -> Detection {
    Detection {
        detection_id: DetectionId::new(&format!("d-{idx}")),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new("bib-42")),
        detected_at_ns,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: idx,
        sensor: SensorData::LoopTransponder { rssi_dbm: Some(-60), pulse_count: None },
    }
}

fn anonymous_detection(idx: u64) -> Detection {
    Detection {
        detection_id: DetectionId::new(&format!("anon-{idx}")),
        detector_id: DetectorId::new("beam-1"),
        timing_point_id: TimingPointId::new("tp-beam"),
        subject_id: None,
        detected_at_ns: 1_700_005_000_000_000_000 + idx * 1_000_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: idx,
        sensor: SensorData::BeamBreak,
    }
}

// ── SSE helper ────────────────────────────────────────────────────────────────

async fn read_sse_events(client: &Client, url: &str, count: usize) -> Vec<serde_json::Value> {
    let response = client.get(url).send().await.unwrap();
    let mut stream = response.bytes_stream();
    let mut buf = String::new();
    let mut events = Vec::new();

    while events.len() < count {
        let chunk = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            stream.next(),
        )
        .await
        .expect("SSE read timed out")
        .expect("SSE stream ended before expected event count")
        .expect("SSE stream error");

        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buf.find("\n\n") {
            let event_text = buf[..pos].to_string();
            buf = buf[pos + 2..].to_string();

            for line in event_text.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(data) {
                        events.push(val);
                        if events.len() >= count {
                            return events;
                        }
                    }
                }
            }
        }
    }

    events
}

// ── test ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn api_endpoints_work() {
    let tmp = tempfile::tempdir().unwrap();
    let config = NodeConfig {
        node_id: "test-node".into(),
        listeners: vec![ListenerConfig::Tcp {
            id: "tcp-main".into(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
        }],
        api_addr: "127.0.0.1:0".parse().unwrap(),
        storage_dir: tmp.path().to_path_buf(),
        auth: Default::default(),
        api: Default::default(),
    };

    let node = Node::new(config).await.unwrap();
    let ingest_addr = node.local_addr();
    let api_addr = node.api_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest_tasks, api_task) = node.run_with_shutdown(shutdown_rx);

    let base = format!("http://{api_addr}");
    let client = Client::new();

    // Status with empty log.
    let status: serde_json::Value = client
        .get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["node_id"], "test-node");
    assert_eq!(status["latest_offset"], serde_json::Value::Null);

    // Ingest 5 named detections (all within grouping window → no crossings).
    {
        let mut tcp = tokio::net::TcpStream::connect(ingest_addr).await.unwrap();
        tcp.write_all(&encode_frame(&connect_envelope("prod-1"))).await.unwrap();
        recv_frame(&mut tcp).await; // ConnectAck

        let base_ns: u64 = 1_700_000_000_000_000_000;
        // Use the SAME sequence number for both the wire envelope and
        // the embedded `Detection.sequence_number`. The runtime's sequence
        // gate keys off `Detection.sequence_number`, so a mismatched
        // envelope header would silently exercise a different sequence
        // than the gate sees and let a regression in the gate hide
        // behind a still-monotonic envelope stream. `seq` starts at 1
        // because real detector adapters number from 1 (sequence_number=0
        // is reserved as a sentinel).
        for i in 0..5u64 {
            let seq = i + 1;
            let det = named_detection(seq, base_ns + i * 400_000_000);
            tcp.write_all(&encode_frame(&event_envelope("prod-1", seq, OtkEvent::Detection(det))))
                .await
                .unwrap();
        }
        tcp.write_all(&encode_frame(&disconnect_envelope("prod-1"))).await.unwrap();
    }

    // Wait until all 5 events are visible via the API (poll with timeout).
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let s: serde_json::Value = client
            .get(format!("{base}/api/v1/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if s["latest_offset"] == serde_json::json!(4) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for 5 events");
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Paginated query: all 5 entries.
    let page: serde_json::Value = client
        .get(format!("{base}/api/v1/events?from=0&limit=10"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = page["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 5);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry["offset"], i as u64);
    }
    assert_eq!(page["latest_offset"], serde_json::json!(4));

    // SSE stream: read 5 historical events.
    let sse = read_sse_events(&client, &format!("{base}/api/v1/events/stream?from=0"), 5).await;
    assert_eq!(sse.len(), 5);
    for (i, ev) in sse.iter().enumerate() {
        assert_eq!(ev["offset"], i as u64, "offset mismatch at position {i}");
    }

    // Ingest 2 anonymous detections; each produces an immediate crossing.
    {
        let mut tcp = tokio::net::TcpStream::connect(ingest_addr).await.unwrap();
        tcp.write_all(&encode_frame(&connect_envelope("prod-2"))).await.unwrap();
        recv_frame(&mut tcp).await; // ConnectAck

        // Same envelope/embedded alignment as the prod-1 block above:
        // a single `seq` drives both header and payload so the gate is
        // exercised exactly as the wire frame implies.
        for i in 0..2u64 {
            let seq = i + 1;
            let det = anonymous_detection(seq);
            tcp.write_all(&encode_frame(&event_envelope("prod-2", seq, OtkEvent::Detection(det))))
                .await
                .unwrap();
        }
        tcp.write_all(&encode_frame(&disconnect_envelope("prod-2"))).await.unwrap();
    }

    // Wait for 4 new log entries (2 detections + 2 crossings → latest_offset = 8).
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let s: serde_json::Value = client
            .get(format!("{base}/api/v1/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if s["latest_offset"] == serde_json::json!(8) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for crossing events");
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // SSE stream from offset 5 → 4 events; at least one is a Crossing.
    let tail = read_sse_events(&client, &format!("{base}/api/v1/events/stream?from=5"), 4).await;
    assert_eq!(tail.len(), 4);
    let has_crossing = tail.iter().any(|ev| ev["event"].get("Crossing").is_some());
    assert!(has_crossing, "expected at least one Crossing event in the tail stream");

    let _ = shutdown_tx.send(true);
    // Drain spawned tasks with a timeout so background workers don't
    // outlive this test under cargo's parallel runner, and assert on
    // both the timeout and join result so a panicked or wedged task
    // fails the test loudly instead of silently leaking. Matches the
    // strict pattern in tests/ingest_test.rs.
    for t in ingest_tasks {
        tokio::time::timeout(tokio::time::Duration::from_secs(5), t)
            .await
            .expect("listener shutdown timed out")
            .expect("listener task panicked");
    }
    tokio::time::timeout(tokio::time::Duration::from_secs(5), api_task)
        .await
        .expect("api shutdown timed out")
        .expect("api task panicked");
}
