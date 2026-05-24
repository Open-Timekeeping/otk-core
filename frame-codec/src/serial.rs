use alloc::vec;
use alloc::vec::Vec;
use otk_protocol::OtkEnvelope;

use crate::error::FrameError;

/// Encode an [`OtkEnvelope`] as a COBS-framed serial frame.
///
/// Frame format: `COBS(cbor_bytes || CRC-16/CCITT-FALSE(cbor_bytes)) || 0x00`.
///
/// COBS removes all `0x00` bytes from the encoded data so the terminating `0x00`
/// unambiguously marks the end of the frame. CRC-16/CCITT-FALSE is appended to
/// the payload before COBS encoding so corruption is detectable after decoding.
///
/// Returns an error if the CBOR-encoded envelope exceeds `max_frame_size`.
pub fn encode_serial(envelope: &OtkEnvelope, max_frame_size: usize) -> Result<Vec<u8>, FrameError> {
    let payload = minicbor::to_vec(envelope).map_err(|_| FrameError::EncodeFailed)?;
    if payload.len() > max_frame_size {
        return Err(FrameError::OversizeFrame {
            len: Some(payload.len()),
            max: max_frame_size,
        });
    }
    let crc = crc16_ccitt_false(&payload).to_be_bytes();
    let mut with_crc = Vec::with_capacity(payload.len() + 2);
    with_crc.extend_from_slice(&payload);
    with_crc.extend_from_slice(&crc);

    let max_encoded = cobs::max_encoding_length(with_crc.len());
    let mut encoded = vec![0u8; max_encoded];
    let encoded_len = cobs::encode(&with_crc, &mut encoded);
    encoded.truncate(encoded_len);
    encoded.push(0x00);
    Ok(encoded)
}

/// Streaming decoder for COBS-framed serial frames.
///
/// Feed incoming bytes with [`SerialFrameDecoder::push`]; a `0x00` byte triggers
/// COBS decoding and CRC-16/CCITT-FALSE verification of the accumulated frame
/// data. Returns every complete [`OtkEnvelope`] (or framing error) produced.
///
/// Multiple consecutive `0x00` bytes are treated as empty inter-frame gaps and
/// ignored.
///
/// When a frame exceeds `max_frame_size`, an `OversizeFrame` error is returned
/// and the decoder enters a discard state, silently dropping all subsequent bytes
/// until the next `0x00` delimiter. Resync resumes cleanly at the real frame
/// boundary.
///
/// `max_frame_size` is the maximum **raw CBOR payload** length in bytes, matching
/// the same parameter on [`encode_serial`]. Internally the decoder derives the
/// corresponding on-wire (COBS + CRC) limit so a frame accepted by the encoder is
/// never rejected by a decoder configured with the same value. The `max` field in
/// any returned `OversizeFrame` error always reflects the original `max_frame_size`
/// value, not the internal on-wire limit.
pub struct SerialFrameDecoder {
    buf: Vec<u8>,
    /// Reusable COBS decode scratch buffer; retained across frames to avoid per-frame allocs.
    scratch: Vec<u8>,
    /// Original raw-payload limit; reported in OversizeFrame errors.
    max_frame_size: usize,
    /// Maximum on-wire byte count before `0x00`: COBS(payload || CRC-16).
    max_wire_size: usize,
    discarding: bool,
}

impl SerialFrameDecoder {
    /// Returns `true` if the decoder is holding bytes from a frame that
    /// hasn't yet decoded to a complete envelope (in-progress COBS data
    /// awaiting a `0x00` delimiter, or bytes being skipped during oversize
    /// discard).
    ///
    /// Transports use this on EOF to distinguish a clean close at a frame
    /// boundary from a truncated frame.
    pub fn has_pending(&self) -> bool {
        !self.buf.is_empty() || self.discarding
    }

    pub fn new(max_frame_size: usize) -> Self {
        // Derive the on-wire limit: add 2 bytes for CRC-16, then apply COBS overhead.
        let max_wire_size = cobs::max_encoding_length(max_frame_size.saturating_add(2));
        Self {
            buf: Vec::new(),
            scratch: Vec::new(),
            max_frame_size,
            max_wire_size,
            discarding: false,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Vec<Result<OtkEnvelope, FrameError>> {
        let mut results = Vec::new();
        for &b in bytes {
            if b == 0x00 {
                // Frame delimiter: reset discard state regardless.
                self.discarding = false;
                if self.buf.is_empty() {
                    continue;
                }
                self.scratch.resize(self.buf.len(), 0);
                let decode_result = cobs::decode(&self.buf, &mut self.scratch);
                self.buf.clear(); // retain capacity for next frame
                match decode_result {
                    Ok(decoded_len) => {
                        let decoded = &self.scratch[..decoded_len];
                        if decoded_len < 2 {
                            results.push(Err(FrameError::CorruptFrame));
                            continue;
                        }
                        let (payload, crc_bytes) = decoded.split_at(decoded_len - 2);
                        let received_crc = u16::from_be_bytes([crc_bytes[0], crc_bytes[1]]);
                        if crc16_ccitt_false(payload) != received_crc {
                            results.push(Err(FrameError::CorruptFrame));
                            continue;
                        }
                        // max_wire_size is a worst-case COBS bound; a payload heavy with
                        // zero bytes can encode shorter than that bound even when the raw
                        // payload exceeds max_frame_size. Check explicitly after decode.
                        if payload.len() > self.max_frame_size {
                            results.push(Err(FrameError::OversizeFrame {
                                len: Some(payload.len()),
                                max: self.max_frame_size,
                            }));
                            continue;
                        }
                        match minicbor::decode::<OtkEnvelope>(payload) {
                            Ok(envelope) => results.push(Ok(envelope)),
                            Err(e) => results.push(Err(FrameError::DecodeFailed(e))),
                        }
                    }
                    Err(_) => results.push(Err(FrameError::LostSync)),
                }
            } else if self.discarding {
                // Silently discard bytes from an oversize frame.
            } else if self.buf.len() >= self.max_wire_size {
                // This byte would exceed the on-wire limit: emit error (reporting
                // the original max_frame_size, not the internal max_wire_size) and
                // enter discard mode until the next 0x00 delimiter.
                self.buf.clear();
                self.discarding = true;
                results.push(Err(FrameError::OversizeFrame {
                    len: None,
                    max: self.max_frame_size,
                }));
            } else {
                self.buf.push(b);
            }
        }
        results
    }
}

/// CRC-16/CCITT-FALSE: polynomial 0x1021, initial value 0xFFFF, no reflection,
/// no final XOR.
///
/// The same variant used by HDLC/SDLC, XMODEM, and many serial protocols. The two-byte
/// CRC is appended to the payload in big-endian order before COBS encoding.
pub fn crc16_ccitt_false(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use otk_protocol::{ids::ProducerId, MessageType, OtkEnvelope};

    fn test_envelope() -> OtkEnvelope {
        OtkEnvelope {
            protocol_version: 0,
            message_type: MessageType::Disconnect,
            source_id: ProducerId::from("test"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: None,
            traceparent: None,
        }
    }

    /// Known-good CRC-16/CCITT-FALSE test vector from the CRC catalogue.
    #[test]
    fn crc16_known_vector() {
        assert_eq!(crc16_ccitt_false(b"123456789"), 0x29B1);
    }

    #[test]
    fn serial_has_pending_false_when_empty() {
        let dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        assert!(!dec.has_pending());
    }

    #[test]
    fn serial_has_pending_true_with_partial_frame() {
        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        // Feed a few bytes that don't contain the 0x00 delimiter; decoder buffers them.
        let results = dec.push(&[0xAA, 0xBB, 0xCC]);
        assert!(results.is_empty());
        assert!(dec.has_pending());
    }

    #[test]
    fn serial_has_pending_false_after_complete_frame() {
        let original = test_envelope();
        let frame = encode_serial(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();
        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let _ = dec.push(&frame);
        assert!(
            !dec.has_pending(),
            "decoder buffer should be empty after a complete frame"
        );
    }

    #[test]
    fn serial_round_trip() {
        let original = test_envelope();
        let frame = encode_serial(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();

        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let results = dec.push(&frame);

        assert_eq!(results.len(), 1);
        let decoded = results[0].as_ref().expect("round-trip failed");
        assert_eq!(
            minicbor::to_vec(&original).unwrap(),
            minicbor::to_vec(decoded).unwrap(),
        );
    }

    #[test]
    fn serial_incremental_push() {
        let original = test_envelope();
        let frame = encode_serial(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();

        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let mut all_results = Vec::new();
        for byte in &frame {
            all_results.extend(dec.push(core::slice::from_ref(byte)));
        }

        assert_eq!(all_results.len(), 1);
        let decoded = all_results[0]
            .as_ref()
            .expect("incremental round-trip failed");
        assert_eq!(
            minicbor::to_vec(&original).unwrap(),
            minicbor::to_vec(decoded).unwrap(),
        );
    }

    #[test]
    fn serial_crc_corruption_detected() {
        // Build a frame with a deliberately wrong CRC by constructing the wire bytes
        // directly. This guarantees COBS decodes successfully so we always get
        // CorruptFrame (not LostSync), which is what CRC corruption must produce.
        let original = test_envelope();
        let payload = minicbor::to_vec(&original).unwrap();
        let correct_crc = crc16_ccitt_false(&payload).to_be_bytes();
        let wrong_crc = [correct_crc[0] ^ 0xFF, correct_crc[1] ^ 0xFF];
        let mut with_wrong_crc = alloc::vec![0u8; payload.len() + 2];
        with_wrong_crc[..payload.len()].copy_from_slice(&payload);
        with_wrong_crc[payload.len()..].copy_from_slice(&wrong_crc);
        let max_encoded = cobs::max_encoding_length(with_wrong_crc.len());
        let mut encoded = alloc::vec![0u8; max_encoded];
        let encoded_len = cobs::encode(&with_wrong_crc, &mut encoded);
        encoded.truncate(encoded_len);
        encoded.push(0x00);

        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let results = dec.push(&encoded);

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(FrameError::CorruptFrame)));
    }

    #[test]
    fn serial_encoder_decoder_boundary() {
        let envelope = test_envelope();
        let cbor_len = minicbor::to_vec(&envelope).unwrap().len();

        // Exactly at the limit: encoder accepts, decoder accepts.
        let frame = encode_serial(&envelope, cbor_len).unwrap();
        let mut dec = SerialFrameDecoder::new(cbor_len);
        let results = dec.push(&frame);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].is_ok(),
            "decoder rejected a frame at the encoder limit"
        );

        // One byte under: encoder must reject.
        assert!(
            matches!(
                encode_serial(&envelope, cbor_len - 1),
                Err(FrameError::OversizeFrame { .. })
            ),
            "encoder accepted a payload exceeding max_frame_size"
        );
    }

    #[test]
    fn serial_resync_after_stray_zero() {
        let envelope = test_envelope();
        let frame2 = encode_serial(&envelope, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();

        // Build a truncated copy of a valid frame (without its 0x00 terminator),
        // then inject a stray 0x00 mid-stream to simulate a spurious delimiter.
        let mut frame1 = encode_serial(&envelope, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();
        frame1.pop(); // remove real delimiter; we'll add it after the stray 0x00 below
        let split = frame1.len() / 2;
        let mut stream = alloc::vec::Vec::new();
        stream.extend_from_slice(&frame1[..split]);
        stream.push(0x00); // stray delimiter: truncates frame1, decoder gets LostSync
        stream.extend_from_slice(&frame1[split..]); // tail of frame1 (also fails)
        stream.push(0x00);
        stream.extend_from_slice(&frame2); // clean frame follows

        let mut dec = SerialFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let results = dec.push(&stream);

        // The two corrupt fragments each produce an error; the clean frame succeeds
        // and decodes to the original envelope.
        assert!(
            results.len() >= 2,
            "expected errors from corrupt fragments and a success"
        );
        assert!(
            results[..results.len() - 1].iter().all(|r| r.is_err()),
            "corrupt fragments must error"
        );
        let decoded = results
            .last()
            .unwrap()
            .as_ref()
            .expect("clean frame after resync must decode");
        assert_eq!(
            minicbor::to_vec(&envelope).unwrap(),
            minicbor::to_vec(decoded).unwrap(),
            "decoded envelope after resync must match original"
        );
    }

    #[test]
    fn serial_post_decode_oversize_rejected() {
        // max_wire_size is derived from cobs::max_encoding_length(max_frame_size + 2), which
        // is a worst-case (maximum overhead) bound. A payload heavy with zero bytes can
        // COBS-encode shorter than that bound even when raw payload.len() > max_frame_size,
        // slipping past the wire-size guard. The explicit post-decode size check must catch it.
        //
        // max_encoding_length(N) = N + ceil(N/254) (worst case: all non-zero input).
        // All-zero input encodes more compactly: each 0x00 maps to a 0x01 code byte (1:1),
        // while the appended non-zero CRC bytes add only 1 code byte for both. The gap opens
        // when ceil((max_frame_size+3)/254) > ceil((max_frame_size+2)/254), i.e. max_frame_size > 506.
        //
        // Concrete case (max_frame_size = 507):
        //   max_wire_size = max_encoding_length(509) = 509 + ceil(509/254) = 509 + 3 = 512
        //   Oversized payload: 508 all-zero bytes (1 over the limit)
        //   with_crc: 510 bytes (508 zeros + 2 non-zero CRC bytes)
        //   Actual COBS: 508 code bytes (0x01) + 1 code byte + 2 data bytes = 511 < 512 ✓
        let max_frame_size = 507usize;
        let mut dec = SerialFrameDecoder::new(max_frame_size);

        let raw_payload = alloc::vec![0u8; max_frame_size + 1]; // 508 bytes, 1 over limit
        let crc = crc16_ccitt_false(&raw_payload).to_be_bytes();
        let mut with_crc = raw_payload.clone();
        with_crc.extend_from_slice(&crc); // 510 bytes total (508 zeros + 2 non-zero CRC)

        let max_encoded = cobs::max_encoding_length(with_crc.len());
        let mut encoded = alloc::vec![0u8; max_encoded];
        let encoded_len = cobs::encode(&with_crc, &mut encoded);
        encoded.truncate(encoded_len);

        // Verify the test prerequisite: encoded frame fits inside max_wire_size.
        let max_wire_size = cobs::max_encoding_length(max_frame_size + 2);
        assert!(
            encoded_len < max_wire_size,
            "prerequisite failed: encoded_len={encoded_len} must be < max_wire_size={max_wire_size}"
        );

        encoded.push(0x00); // frame delimiter

        let results = dec.push(&encoded);
        assert_eq!(results.len(), 1);
        assert!(
            matches!(results[0], Err(FrameError::OversizeFrame { len: Some(l), max: m })
                if l == max_frame_size + 1 && m == max_frame_size),
            "expected OversizeFrame, got {:?}",
            results[0]
        );
    }
}
