use alloc::vec;
use alloc::vec::Vec;
use protocol::OtkEnvelope;

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
        return Err(FrameError::OversizeFrame { len: Some(payload.len()), max: max_frame_size });
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
    /// Original raw-payload limit; reported in OversizeFrame errors.
    max_frame_size: usize,
    /// Maximum on-wire byte count before `0x00`: COBS(payload || CRC-16).
    max_wire_size: usize,
    discarding: bool,
}

impl SerialFrameDecoder {
    pub fn new(max_frame_size: usize) -> Self {
        // Derive the on-wire limit: add 2 bytes for CRC-16, then apply COBS overhead.
        let max_wire_size = cobs::max_encoding_length(max_frame_size + 2);
        Self { buf: Vec::new(), max_frame_size, max_wire_size, discarding: false }
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
                let frame_bytes = core::mem::take(&mut self.buf);
                let mut decoded_buf = vec![0u8; frame_bytes.len()];
                match cobs::decode(&frame_bytes, &mut decoded_buf) {
                    Ok(decoded_len) => {
                        let decoded = &decoded_buf[..decoded_len];
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
                results.push(Err(FrameError::OversizeFrame { len: None, max: self.max_frame_size }));
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
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{MessageType, OtkEnvelope, ids::ProducerId};

    fn test_envelope() -> OtkEnvelope {
        OtkEnvelope {
            protocol_version: 0,
            message_type: MessageType::Disconnect,
            source_id: ProducerId::from("test"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: None,
        }
    }

    /// Known-good CRC-16/CCITT-FALSE test vector from the CRC catalogue.
    #[test]
    fn crc16_known_vector() {
        assert_eq!(crc16_ccitt_false(b"123456789"), 0x29B1);
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
        let decoded = all_results[0].as_ref().expect("incremental round-trip failed");
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
        assert!(results[0].is_ok(), "decoder rejected a frame at the encoder limit");

        // One byte under: encoder must reject.
        assert!(
            matches!(encode_serial(&envelope, cbor_len - 1), Err(FrameError::OversizeFrame { .. })),
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
        assert!(results.len() >= 2, "expected errors from corrupt fragments and a success");
        assert!(results[..results.len() - 1].iter().all(|r| r.is_err()), "corrupt fragments must error");
        let decoded = results.last().unwrap().as_ref().expect("clean frame after resync must decode");
        assert_eq!(
            minicbor::to_vec(&envelope).unwrap(),
            minicbor::to_vec(decoded).unwrap(),
            "decoded envelope after resync must match original"
        );
    }
}
