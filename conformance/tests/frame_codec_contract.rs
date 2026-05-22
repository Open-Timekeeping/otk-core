//! Frame Codec conformance.
//!
//! Verifies the contract surface that every consumer of `frame-codec` relies on:
//!
//! - **Stream framing** (`StreamFrameDecoder`): length-prefix round-trip,
//!   incremental byte-at-a-time decode, oversize-frame error + resync, multiple
//!   frames in one push, partial-frame buffering.
//! - **Serial framing** (`SerialFrameDecoder`): COBS round-trip, CRC-mismatch
//!   detection, resync after corruption, oversize-frame discard-and-resume.

use frame_codec::{
    encode_serial, encode_stream, FrameError, SerialFrameDecoder, StreamFrameDecoder,
};
use otk_protocol::{ids::ProducerId, MessageType, OtkEnvelope, PROTOCOL_VERSION};

/// Build a minimal envelope for framing tests.
///
/// frame-codec doesn't validate inner-payload contracts (it just frames CBOR
/// bytes), but using `MessageType::Disconnect` here keeps the suite
/// internally consistent with the `OtkEnvelope` contract that only Disconnect
/// is payload-less. A `Heartbeat` with `payload: None` would contradict the
/// invariant the ingest-protocol contract tests assert and would be a foot-
/// gun for anyone copying this helper into a non-framing test.
fn envelope(producer: &str) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type: MessageType::Disconnect,
        source_id: ProducerId::from(producer),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload: None,
    }
}

// ── Stream framing ───────────────────────────────────────────────────────────

#[test]
fn stream_round_trip_single_frame() {
    let env = envelope("p-1");
    let frame = encode_stream(&env, 65_535).expect("encode");
    let mut dec = StreamFrameDecoder::new(65_535);
    let results = dec.push(&frame);
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
}

#[test]
fn stream_decodes_byte_at_a_time() {
    let env = envelope("p-1");
    let frame = encode_stream(&env, 65_535).expect("encode");
    let mut dec = StreamFrameDecoder::new(65_535);
    let mut all = Vec::new();
    for byte in &frame {
        all.extend(dec.push(std::slice::from_ref(byte)));
    }
    assert_eq!(all.len(), 1);
    assert!(all[0].is_ok());
}

#[test]
fn stream_decodes_multiple_frames_in_one_push() {
    let env = envelope("p-1");
    let mut combined = Vec::new();
    for _ in 0..5 {
        combined.extend(encode_stream(&env, 65_535).expect("encode"));
    }
    let mut dec = StreamFrameDecoder::new(65_535);
    let results = dec.push(&combined);
    assert_eq!(results.len(), 5);
    assert!(results.iter().all(|r| r.is_ok()));
}

#[test]
fn stream_oversize_frame_yields_error_and_resyncs() {
    let env = envelope("p-1");
    let small_frame = encode_stream(&env, 65_535).expect("encode");

    // Construct a frame whose header declares 1_000 bytes when the decoder limit
    // is 256. Header + filler bytes are followed by a valid small frame.
    let mut combined = Vec::new();
    combined.extend(&1_000u32.to_be_bytes());
    combined.extend(vec![0xAAu8; 1_000]);
    combined.extend(&small_frame);

    let mut dec = StreamFrameDecoder::new(256);
    let results = dec.push(&combined);
    assert_eq!(results.len(), 2, "expected one error + one ok envelope");
    assert!(matches!(results[0], Err(FrameError::OversizeFrame { .. })));
    assert!(results[1].is_ok());
}

#[test]
fn stream_partial_frame_buffers_across_pushes() {
    let env = envelope("p-1");
    let frame = encode_stream(&env, 65_535).expect("encode");
    let (head, tail) = frame.split_at(frame.len() / 2);

    let mut dec = StreamFrameDecoder::new(65_535);
    let first = dec.push(head);
    assert!(first.is_empty(), "no complete frame from partial bytes");
    let second = dec.push(tail);
    assert_eq!(second.len(), 1);
    assert!(second[0].is_ok());
}

// ── Serial framing ────────────────────────────────────────────────────────────

#[test]
fn serial_round_trip_single_frame() {
    let env = envelope("p-1");
    let frame = encode_serial(&env, 65_535).expect("encode");
    let mut dec = SerialFrameDecoder::new(65_535);
    let results = dec.push(&frame);
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
}

#[test]
fn serial_decodes_multiple_frames() {
    let env = envelope("p-1");
    let mut combined = Vec::new();
    for _ in 0..3 {
        combined.extend(encode_serial(&env, 65_535).expect("encode"));
    }
    let mut dec = SerialFrameDecoder::new(65_535);
    let results = dec.push(&combined);
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.is_ok()));
}

#[test]
fn serial_corrupt_byte_inside_frame_reports_corrupt_then_resyncs() {
    let env = envelope("p-1");
    let frame_a = encode_serial(&env, 65_535).expect("encode");
    // Premise: the frame must be long enough that index 3 lies inside the
    // COBS-encoded payload (not the leading overhead byte and not the
    // trailing 0x00 delimiter). If a future encode change produces shorter
    // frames, fail loudly here instead of silently degrading the test.
    assert!(
        frame_a.len() >= 6,
        "test premise broken: encode_serial produced a frame of len {} (< 6); \
         pick a corruption index inside the encoded payload",
        frame_a.len()
    );
    let mut corrupted = frame_a.clone();
    // Flip a payload byte (index 3 is past the COBS overhead byte at 0 and
    // well before the trailing 0x00 delimiter at len-1).
    corrupted[3] ^= 0xFF;
    let frame_b = encode_serial(&env, 65_535).expect("encode");

    let mut combined = Vec::new();
    combined.extend(&corrupted);
    combined.extend(&frame_b);

    let mut dec = SerialFrameDecoder::new(65_535);
    let results = dec.push(&combined);
    assert!(
        results.iter().any(|r| matches!(r, Err(FrameError::CorruptFrame))),
        "expected a CorruptFrame from CRC mismatch, got: {:?}",
        results.iter().map(|r| r.is_ok()).collect::<Vec<_>>()
    );
    assert!(
        results.iter().any(|r| r.is_ok()),
        "expected the following good frame to decode after resync"
    );
}

#[test]
fn serial_extra_zero_delimiters_ignored() {
    let env = envelope("p-1");
    let frame = encode_serial(&env, 65_535).expect("encode");
    let mut padded = Vec::new();
    padded.push(0x00); // pre-pad: empty inter-frame
    padded.extend(&frame);
    padded.push(0x00); // extra trailing delimiter
    padded.push(0x00);

    let mut dec = SerialFrameDecoder::new(65_535);
    let results = dec.push(&padded);
    assert_eq!(results.len(), 1, "extra delimiters must not produce phantom frames");
    assert!(results[0].is_ok());
}
