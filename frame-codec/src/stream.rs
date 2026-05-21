use alloc::vec::Vec;
use protocol::OtkEnvelope;

use crate::error::FrameError;

/// Encode an [`OtkEnvelope`] as a length-prefixed stream frame.
///
/// Frame format: `u32_be(payload_len) || cbor_bytes`.
///
/// Returns an error if the CBOR-encoded envelope exceeds `max_frame_size`.
/// No checksum is added; reliable transports (TCP, Unix socket) provide
/// their own integrity guarantees.
pub fn encode_stream(envelope: &OtkEnvelope, max_frame_size: usize) -> Result<Vec<u8>, FrameError> {
    let payload = minicbor::to_vec(envelope).map_err(|_| FrameError::EncodeFailed)?;
    // The 4-byte length prefix caps the on-wire payload at u32::MAX regardless of
    // what the caller passes as max_frame_size, so clamp to produce an accurate error.
    let effective_max = max_frame_size.min(u32::MAX as usize);
    if payload.len() > effective_max {
        return Err(FrameError::OversizeFrame { len: Some(payload.len()), max: effective_max });
    }
    // payload.len() <= effective_max <= u32::MAX, so the cast is exact on all targets.
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Streaming decoder for length-prefixed stream frames.
///
/// Feed incoming bytes with [`StreamFrameDecoder::push`]; it returns every
/// complete [`OtkEnvelope`] decoded from the bytes fed so far. Partial frames
/// are buffered internally between calls.
///
/// When an oversize frame is detected the declared payload length from the
/// 4-byte header is used to skip forward to the next frame boundary, so the
/// stream remains usable after the error. The `OversizeFrame` error is returned
/// in the result list and the decoder continues.
///
/// **Corrupted length prefix.** Stream framing has no resync delimiter; if the
/// 4-byte length field is corrupted (e.g. a bit-flip that TCP's own checksum
/// did not catch, or a misbehaving sender), the skip distance may be arbitrarily
/// large and the decoder will silently discard all bytes until that many have
/// been consumed. Callers that receive an `OversizeFrame` error with a `len`
/// much larger than `max_frame_size` should treat the stream as unrecoverable
/// and close the connection.
///
/// On a CBOR decode error the offending payload is discarded; the decoder
/// continues from the next frame boundary.
pub struct StreamFrameDecoder {
    buf: Vec<u8>,
    max_frame_size: usize,
    /// Bytes remaining to skip from an oversize frame's payload.
    skip_bytes: u64,
}

impl StreamFrameDecoder {
    pub fn new(max_frame_size: usize) -> Self {
        Self { buf: Vec::new(), max_frame_size, skip_bytes: 0 }
    }

    /// Returns `true` if the decoder is holding bytes from a frame that
    /// hasn't yet decoded to a complete envelope (either a partial length
    /// prefix / payload in `buf`, or remaining bytes to skip from an
    /// oversize-frame detection).
    ///
    /// Transports use this on EOF to distinguish a clean close at a frame
    /// boundary (`!has_pending()` → caller can return `Ok(None)`) from a
    /// truncated frame (`has_pending()` → caller should report a decode /
    /// truncation error).
    pub fn has_pending(&self) -> bool {
        !self.buf.is_empty() || self.skip_bytes > 0
    }

    /// Feed incoming bytes into the decoder.
    ///
    /// Returns every complete [`OtkEnvelope`] (or framing error) that can be
    /// produced from the bytes fed so far.
    ///
    /// **Corrupted length prefix warning:** if `skip_bytes` is set from a
    /// previous oversize detection, bytes are consumed directly from the input
    /// slice without buffering. However, a corrupted 4-byte length near
    /// `u32::MAX` means the decoder may skip for a very long time. When the
    /// returned `OversizeFrame.len` is much larger than `max_frame_size`, close
    /// the connection rather than waiting for the skip to complete.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Result<OtkEnvelope, FrameError>> {
        // Fast path: consume to-be-skipped bytes directly from the input slice
        // without copying them into self.buf. self.buf is always empty when
        // skip_bytes > 0 at the start of a call (invariant maintained below).
        let bytes = if self.skip_bytes > 0 {
            debug_assert!(self.buf.is_empty());
            let skip = bytes.len().min(usize::try_from(self.skip_bytes).unwrap_or(usize::MAX));
            self.skip_bytes -= skip as u64;
            &bytes[skip..]
        } else {
            bytes
        };

        self.buf.extend_from_slice(bytes);
        let mut results = Vec::new();
        // Cursor into self.buf: advance pos rather than draining on every frame.
        // A single drain(..pos) at the end avoids O(n²) byte-shifting when many
        // small frames arrive in one push() call.
        let mut pos = 0usize;
        loop {
            // Consume oversize payload bytes that are already in the buffer.
            if self.skip_bytes > 0 {
                let available = self.buf.len() - pos;
                let skip = available.min(usize::try_from(self.skip_bytes).unwrap_or(usize::MAX));
                pos += skip;
                self.skip_bytes -= skip as u64;
                if self.skip_bytes > 0 {
                    break; // exhausted buffered bytes; wait for next push
                }
            }
            if self.buf.len() - pos < 4 {
                break;
            }
            let len_u32 = u32::from_be_bytes(self.buf[pos..pos + 4].try_into().unwrap());
            // Compare via u64 so the guard is safe on 16-bit targets (usize = u16):
            // a direct `as usize` cast would silently truncate a value like 0x10000
            // to 0, turning a corrupt-length frame into an apparent empty payload.
            // After the guard, len_u32 <= max_frame_size <= usize::MAX, so the cast
            // is exact on all targets.
            if (len_u32 as u64) > (self.max_frame_size as u64) {
                let len = usize::try_from(len_u32).ok(); // None only on 16-bit targets
                results.push(Err(FrameError::OversizeFrame { len, max: self.max_frame_size }));
                pos += 4; // consume the header; payload bytes follow
                self.skip_bytes = len_u32 as u64;
                continue;
            }
            let len = len_u32 as usize;
            if self.buf.len() - pos < 4 + len {
                break;
            }
            let result = minicbor::decode::<OtkEnvelope>(&self.buf[pos + 4..pos + 4 + len])
                .map_err(FrameError::DecodeFailed);
            pos += 4 + len;
            results.push(result);
        }
        if pos > 0 {
            self.buf.drain(..pos);
        }
        results
    }
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

    #[test]
    fn stream_round_trip() {
        let original = test_envelope();
        let frame = encode_stream(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();

        let mut dec = StreamFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let results = dec.push(&frame);

        assert_eq!(results.len(), 1);
        let decoded = results[0].as_ref().expect("round-trip failed");
        assert_eq!(
            minicbor::to_vec(&original).unwrap(),
            minicbor::to_vec(decoded).unwrap(),
        );
    }

    #[test]
    fn has_pending_false_when_empty() {
        let dec = StreamFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        assert!(!dec.has_pending());
    }

    #[test]
    fn has_pending_true_with_partial_length_prefix() {
        let mut dec = StreamFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        // Push 2 of the 4 length-prefix bytes; decoder buffers them, returns no envelopes.
        let results = dec.push(&[0u8, 0u8]);
        assert!(results.is_empty());
        assert!(dec.has_pending());
    }

    #[test]
    fn has_pending_false_after_complete_frame() {
        let original = test_envelope();
        let frame = encode_stream(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();
        let mut dec = StreamFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let _ = dec.push(&frame);
        assert!(!dec.has_pending(), "decoder buffer should be empty after a complete frame");
    }

    #[test]
    fn has_pending_true_while_skipping_oversize() {
        let mut dec = StreamFrameDecoder::new(16);
        // Header declares 1_000 bytes (oversize for max=16); feed header + a few payload bytes.
        let mut input = Vec::new();
        input.extend_from_slice(&1_000u32.to_be_bytes());
        input.extend_from_slice(&[0xAAu8; 10]);
        let results = dec.push(&input);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(crate::FrameError::OversizeFrame { .. })));
        // Still has ~990 bytes left to skip from the oversize frame.
        assert!(dec.has_pending());
    }

    #[test]
    fn stream_incremental_push() {
        let original = test_envelope();
        let frame = encode_stream(&original, crate::DEFAULT_MAX_FRAME_SIZE).unwrap();

        let mut dec = StreamFrameDecoder::new(crate::DEFAULT_MAX_FRAME_SIZE);
        let mut all_results = Vec::new();
        // Feed one byte at a time to exercise partial-read buffering.
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
}
