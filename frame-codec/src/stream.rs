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
    if payload.len() > max_frame_size {
        return Err(FrameError::OversizeFrame { len: Some(payload.len()), max: max_frame_size });
    }
    // The length prefix is a u32; on 64-bit targets max_frame_size may legally
    // exceed u32::MAX, which would silently truncate the prefix and desync receivers.
    let len_u32 = u32::try_from(payload.len())
        .map_err(|_| FrameError::OversizeFrame { len: Some(payload.len()), max: max_frame_size })?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len_u32.to_be_bytes());
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
        loop {
            // Drain any leftover oversize bytes already in the buffer (set by
            // the oversize branch below within the same push call).
            if self.skip_bytes > 0 {
                let drain = self.buf.len().min(usize::try_from(self.skip_bytes).unwrap_or(usize::MAX));
                self.buf.drain(..drain);
                self.skip_bytes -= drain as u64;
                if self.skip_bytes > 0 {
                    break; // buf is now empty; wait for next push
                }
            }
            if self.buf.len() < 4 {
                break;
            }
            let len_u32 = u32::from_be_bytes(self.buf[..4].try_into().unwrap());
            // Compare via u64 so the guard is safe on 16-bit targets (usize = u16):
            // a direct `as usize` cast would silently truncate a value like 0x10000
            // to 0, turning a corrupt-length frame into an apparent empty payload.
            // After the guard, len_u32 <= max_frame_size <= usize::MAX, so the cast
            // is exact on all targets.
            if (len_u32 as u64) > (self.max_frame_size as u64) {
                let len = usize::try_from(len_u32).ok(); // None only on 16-bit targets
                results.push(Err(FrameError::OversizeFrame { len, max: self.max_frame_size }));
                self.buf.drain(..4); // consume the header; payload bytes follow
                self.skip_bytes = len_u32 as u64;
                continue; // drain payload bytes from buf on next iteration
            }
            let len = len_u32 as usize;
            if self.buf.len() < 4 + len {
                break;
            }
            let result = minicbor::decode::<OtkEnvelope>(&self.buf[4..4 + len])
                .map_err(FrameError::DecodeFailed);
            self.buf.drain(..4 + len);
            results.push(result);
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
