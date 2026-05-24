use std::io::SeekFrom;
use std::path::Path;

use event_model::OtkEvent;
use port_out_event_log::{LogEntry, Offset, StorageError};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const MAGIC: [u8; 4] = [0x4F, 0x54, 0x4B, 0x53]; // "OTKS"
pub const VERSION: u8 = 1;
pub const HEADER_LEN: u64 = 24;

// Maximum CBOR payload size per event: 4 MiB. Prevents corrupt length fields
// from causing arbitrarily large allocations.
const MAX_EVENT_CBOR: usize = 4 * 1024 * 1024;

/// Error message written into [`StorageError::Corrupted`] when `read_record`
/// encounters the zero sentinel at the end of a closed segment.
///
/// Stored as a constant so callers can match on it without fragile string literals.
pub const SENTINEL_MSG: &str = "end-of-segment sentinel";

// ── Header ────────────────────────────────────────────────────────────────────

pub struct SegmentHeader {
    pub base_offset: u64,
    pub created_at_ns: u64,
}

impl SegmentHeader {
    pub async fn write(file: &mut File, h: &SegmentHeader) -> Result<(), StorageError> {
        let mut buf = [0u8; 24];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = VERSION;
        buf[5] = 0; // flags
                    // buf[6..8] padding = 0
        buf[8..16].copy_from_slice(&h.base_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&h.created_at_ns.to_le_bytes());
        file.write_all(&buf).await?;
        Ok(())
    }

    pub async fn read(file: &mut File) -> Result<SegmentHeader, StorageError> {
        let mut buf = [0u8; 24];
        file.read_exact(&mut buf).await?;
        if buf[0..4] != MAGIC {
            return Err(StorageError::Corrupted(format!(
                "bad magic: {:?}",
                &buf[0..4]
            )));
        }
        if buf[4] != VERSION {
            return Err(StorageError::Corrupted(format!(
                "unknown segment version {}",
                buf[4]
            )));
        }
        let base_offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let created_at_ns = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        Ok(SegmentHeader {
            base_offset,
            created_at_ns,
        })
    }
}

// ── Record I/O ────────────────────────────────────────────────────────────────

/// Write one [`LogEntry`] at the current file position.
///
/// Returns the byte offset where the `payload_len` field was written (suitable
/// for storing in the offset index). On any write failure the partial record is
/// truncated so the file is left at the same length as before the call.
pub async fn write_record(file: &mut File, entry: &LogEntry) -> Result<u64, StorageError> {
    let event_cbor = minicbor::to_vec(&entry.event)
        .map_err(|e| StorageError::Corrupted(format!("CBOR encode: {e}")))?;

    if event_cbor.len() > MAX_EVENT_CBOR {
        return Err(StorageError::InvalidInput(format!(
            "event CBOR ({} bytes) exceeds maximum record size ({MAX_EVENT_CBOR} bytes)",
            event_cbor.len()
        )));
    }

    // payload = offset(8) + appended_at_ns(8) + event_len(4) + event_cbor(N)
    let event_cbor_len = event_cbor.len() as u32;
    let payload_len: u32 = 8 + 8 + 4 + event_cbor_len;

    let crc = {
        let mut h = crc32fast::Hasher::new();
        h.update(&entry.offset.as_u64().to_le_bytes());
        h.update(&entry.appended_at_ns.to_le_bytes());
        h.update(&event_cbor_len.to_le_bytes());
        h.update(&event_cbor);
        h.finalize()
    };

    // Build the full record in a buffer so the write is as close to atomic as
    // possible.  On write failure, truncate back to record_start so the file
    // does not contain a partial record.
    let record_start = file.stream_position().await?;
    let mut buf = Vec::with_capacity(4 + payload_len as usize + 4);
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.extend_from_slice(&entry.offset.as_u64().to_le_bytes());
    buf.extend_from_slice(&entry.appended_at_ns.to_le_bytes());
    buf.extend_from_slice(&event_cbor_len.to_le_bytes());
    buf.extend_from_slice(&event_cbor);
    buf.extend_from_slice(&crc.to_le_bytes());

    if let Err(e) = file.write_all(&buf).await {
        let _ = file.set_len(record_start).await;
        return Err(StorageError::Io(e));
    }

    Ok(record_start)
}

/// Read and verify one record whose `payload_len` field starts at `pos`.
///
/// Returns [`StorageError::Corrupted`] on CRC mismatch or CBOR decode failure.
/// Returns an [`std::io::Error`] with `UnexpectedEof` kind when `pos` is at or
/// past EOF (the active segment has not been written yet).
pub async fn read_record(file: &mut File, pos: u64) -> Result<LogEntry, StorageError> {
    file.seek(SeekFrom::Start(pos)).await?;

    let mut payload_len_buf = [0u8; 4];
    file.read_exact(&mut payload_len_buf).await?;
    let payload_len = u32::from_le_bytes(payload_len_buf);

    if payload_len == 0 {
        return Err(StorageError::Corrupted(SENTINEL_MSG.into()));
    }

    // Validate length fields before allocating: payload must be at least
    // offset(8) + appended_at_ns(8) + event_len(4) = 20 bytes.
    let event_cbor_len_expected = payload_len.checked_sub(20).ok_or_else(|| {
        StorageError::Corrupted(format!(
            "payload_len {payload_len} too small at pos {pos} (minimum 20)"
        ))
    })?;

    if event_cbor_len_expected as usize > MAX_EVENT_CBOR {
        return Err(StorageError::Corrupted(format!(
            "event CBOR length {event_cbor_len_expected} at pos {pos} exceeds maximum {MAX_EVENT_CBOR}"
        )));
    }

    let mut offset_buf = [0u8; 8];
    file.read_exact(&mut offset_buf).await?;
    let raw_offset = u64::from_le_bytes(offset_buf);

    let mut appended_buf = [0u8; 8];
    file.read_exact(&mut appended_buf).await?;
    let appended_at_ns = u64::from_le_bytes(appended_buf);

    let mut event_len_buf = [0u8; 4];
    file.read_exact(&mut event_len_buf).await?;
    let event_cbor_len_stored = u32::from_le_bytes(event_len_buf);

    if event_cbor_len_stored != event_cbor_len_expected {
        return Err(StorageError::Corrupted(format!(
            "event_len mismatch at pos {pos}: payload_len implies {event_cbor_len_expected}, stored {event_cbor_len_stored}"
        )));
    }

    let event_len = event_cbor_len_stored as usize;
    let mut event_cbor = vec![0u8; event_len];
    file.read_exact(&mut event_cbor).await?;

    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).await?;
    let stored_crc = u32::from_le_bytes(crc_buf);

    let computed_crc = {
        let mut h = crc32fast::Hasher::new();
        h.update(&raw_offset.to_le_bytes());
        h.update(&appended_at_ns.to_le_bytes());
        h.update(&event_cbor_len_stored.to_le_bytes());
        h.update(&event_cbor);
        h.finalize()
    };
    if computed_crc != stored_crc {
        return Err(StorageError::Corrupted(format!(
            "CRC mismatch at pos {pos}: expected {computed_crc:#010x}, got {stored_crc:#010x}"
        )));
    }

    let event: OtkEvent = minicbor::decode(&event_cbor)
        .map_err(|e| StorageError::Corrupted(format!("CBOR decode at pos {pos}: {e}")))?;

    Ok(LogEntry {
        offset: Offset::new(raw_offset),
        appended_at_ns,
        event,
    })
}

// ── Index I/O ─────────────────────────────────────────────────────────────────

/// Write an offset index atomically: write to a temp file, sync, then rename.
pub async fn write_index(path: &Path, positions: &[u64]) -> Result<(), StorageError> {
    let tmp = path.with_extension("idx.tmp");

    let bytes: Vec<u8> = positions.iter().flat_map(|p| p.to_le_bytes()).collect();

    let mut tmp_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .await?;
    tmp_file.write_all(&bytes).await?;
    tmp_file.sync_all().await?;
    drop(tmp_file);

    tokio::fs::rename(&tmp, path).await?;
    // Fsync the parent directory so the rename is durable on Linux/macOS.
    // Errors are ignored on platforms (e.g. Windows) that do not support it.
    if let Some(parent) = path.parent() {
        if let Ok(dir_file) = tokio::fs::File::open(parent).await {
            let _ = dir_file.sync_all().await;
        }
    }
    Ok(())
}

/// Maximum index file size accepted by [`read_index`]: 256 MiB = 32 M entries.
///
/// Chosen so that the default 64 MiB `max_segment_bytes` (min record ≈ 28 bytes
/// → at most ~2.4 M records → ~19 MiB index) can never approach the limit even
/// with large batch appends that temporarily exceed the soft segment size.
/// Segments requiring a larger index need a correspondingly larger segment size
/// that is not intended for this implementation.
const MAX_INDEX_BYTES: usize = 256 * 1024 * 1024;

/// Read an offset index file into a `Vec<u64>`.
pub async fn read_index(path: &Path) -> Result<Vec<u64>, StorageError> {
    // Stat first so a corrupt or huge .idx cannot cause an unbounded allocation.
    let file_len = tokio::fs::metadata(path)
        .await
        .map_err(StorageError::Io)?
        .len();
    if file_len > MAX_INDEX_BYTES as u64 {
        return Err(StorageError::Corrupted(format!(
            "index file {} is {} bytes, exceeding the {MAX_INDEX_BYTES}-byte limit",
            path.display(),
            file_len
        )));
    }
    let bytes = tokio::fs::read(path).await.map_err(StorageError::Io)?;
    if bytes.len() % 8 != 0 {
        return Err(StorageError::Corrupted(format!(
            "index file {} has length {} which is not a multiple of 8",
            path.display(),
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect())
}

// ── Crash recovery ────────────────────────────────────────────────────────────

/// Scan the active segment from [`HEADER_LEN`] forward, verifying each record.
///
/// Truncates the file at the first bad, incomplete, or sentinel record so that
/// future appends always go to a clean tail.
///
/// Returns `(positions, next_file_pos, record_count)` where `positions[i]` is
/// the byte offset of the `payload_len` field for the i-th record.
pub async fn recover_active(
    file: &mut File,
    base_offset: u64,
) -> Result<(Vec<u64>, u64, u64), StorageError> {
    let file_len = file.metadata().await?.len();
    let mut positions: Vec<u64> = Vec::new();
    let mut pos = HEADER_LEN;

    loop {
        if pos >= file_len {
            break;
        }

        file.seek(SeekFrom::Start(pos)).await?;
        let mut plen_buf = [0u8; 4];
        match file.read_exact(&mut plen_buf).await {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Partial payload_len: torn write at the very start of a record.
                file.set_len(pos).await?;
                file.seek(SeekFrom::Start(pos)).await?;
                break;
            }
            Err(e) => return Err(StorageError::Io(e)),
            Ok(_) => {}
        }
        let payload_len = u32::from_le_bytes(plen_buf);

        if payload_len == 0 {
            // Sentinel from an incomplete roll (crash between sentinel write and
            // idx write).  Remove it so future appends go right after valid records.
            file.set_len(pos).await?;
            file.seek(SeekFrom::Start(pos)).await?;
            break;
        }

        match read_record(file, pos).await {
            Ok(entry) => {
                // Verify the logical offset stored in the record matches the
                // position we expect. A mismatch means two segment files were
                // concatenated or the file was otherwise corrupted.
                let expected =
                    base_offset
                        .checked_add(positions.len() as u64)
                        .ok_or_else(|| {
                            StorageError::Corrupted(format!(
                                "segment base_offset {base_offset} + record count overflows u64"
                            ))
                        })?;
                if entry.offset.as_u64() != expected {
                    file.set_len(pos).await?;
                    file.seek(SeekFrom::Start(pos)).await?;
                    break;
                }
                positions.push(pos);
                let next_pos = pos
                    .checked_add(payload_len as u64)
                    .and_then(|p| p.checked_add(8));
                match next_pos {
                    Some(p) => pos = p,
                    None => {
                        file.set_len(pos).await?;
                        file.seek(SeekFrom::Start(pos)).await?;
                        break;
                    }
                }
            }
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(pos).await?;
                file.seek(SeekFrom::Start(pos)).await?;
                break;
            }
            Err(StorageError::Corrupted(_)) => {
                file.set_len(pos).await?;
                file.seek(SeekFrom::Start(pos)).await?;
                break;
            }
            Err(e) => return Err(e),
        }
    }

    let record_count = positions.len() as u64;
    Ok((positions, pos, record_count))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use event_model::{
        detection::SensorData, Detection, DetectionId, DetectorId, OtkEvent, SourceAttestation,
        TimebaseId, TimestampingMethod, TimingPointId,
    };
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;

    fn make_event() -> OtkEvent {
        OtkEvent::Detection(Detection {
            detection_id: DetectionId::new("d1"),
            detector_id: DetectorId::new("loop-a"),
            timing_point_id: TimingPointId::new("tp-finish"),
            subject_id: None,
            detected_at_ns: 1_700_000_000_000_000_000,
            detected_at_uncertainty_ns: Some(500),
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("gps-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 1,
            sensor: SensorData::BeamBreak,
        })
    }

    fn make_entry(offset: u64) -> LogEntry {
        LogEntry {
            offset: Offset::new(offset),
            appended_at_ns: 1_700_000_000_000_000_000 + offset,
            event: make_event(),
        }
    }

    async fn open_rw(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn round_trip_single_record() {
        let dir = tempdir().unwrap();
        let seg_path = dir.path().join("test.seg");
        let mut file = open_rw(&seg_path).await;

        let header = SegmentHeader {
            base_offset: 0,
            created_at_ns: 42,
        };
        SegmentHeader::write(&mut file, &header).await.unwrap();

        let entry = make_entry(0);
        let record_pos = write_record(&mut file, &entry).await.unwrap();
        assert_eq!(record_pos, HEADER_LEN);

        let read_back = read_record(&mut file, record_pos).await.unwrap();
        assert_eq!(read_back.offset, entry.offset);
        assert_eq!(read_back.appended_at_ns, entry.appended_at_ns);
        assert_eq!(
            format!("{:?}", read_back.event),
            format!("{:?}", entry.event)
        );
    }

    #[tokio::test]
    async fn crc_mismatch_detected() {
        let dir = tempdir().unwrap();
        let seg_path = dir.path().join("test.seg");
        let mut file = open_rw(&seg_path).await;

        let header = SegmentHeader {
            base_offset: 0,
            created_at_ns: 0,
        };
        SegmentHeader::write(&mut file, &header).await.unwrap();

        let entry = make_entry(0);
        write_record(&mut file, &entry).await.unwrap();

        // flip a byte in the CBOR payload (at HEADER_LEN + 4 + 8 + 8 + 4 = HEADER_LEN + 24)
        let corrupt_pos = HEADER_LEN + 24;
        file.seek(SeekFrom::Start(corrupt_pos)).await.unwrap();
        file.write_all(&[0xFF]).await.unwrap();

        let result = read_record(&mut file, HEADER_LEN).await;
        assert!(
            matches!(result, Err(StorageError::Corrupted(_))),
            "expected Corrupted, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn index_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join("00000000000000000000.idx");
        let positions: Vec<u64> = vec![24, 128, 256, 512];

        write_index(&idx_path, &positions).await.unwrap();
        let read_back = read_index(&idx_path).await.unwrap();
        assert_eq!(read_back, positions);
    }

    #[tokio::test]
    async fn recover_detects_torn_write() {
        let dir = tempdir().unwrap();
        let seg_path = dir.path().join("test.seg");
        let mut file = open_rw(&seg_path).await;

        let header = SegmentHeader {
            base_offset: 0,
            created_at_ns: 0,
        };
        SegmentHeader::write(&mut file, &header).await.unwrap();

        write_record(&mut file, &make_entry(0)).await.unwrap();
        let second_pos = write_record(&mut file, &make_entry(1)).await.unwrap();
        let full_len = file.stream_position().await.unwrap();

        // Truncate 5 bytes into the second record (well past the boundary)
        let torn_pos = second_pos + 5;
        assert!(
            torn_pos < full_len,
            "torn_pos must be inside the second record"
        );
        file.set_len(torn_pos).await.unwrap();

        let (positions, next_file_pos, record_count) = recover_active(&mut file, 0).await.unwrap();

        assert_eq!(
            record_count, 1,
            "should have recovered only the first record"
        );
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], HEADER_LEN);

        let file_len = file.metadata().await.unwrap().len();
        assert_eq!(file_len, next_file_pos);
        assert!(
            file_len < torn_pos,
            "file should be truncated back past the torn point"
        );
    }

    #[tokio::test]
    async fn recover_removes_sentinel() {
        let dir = tempdir().unwrap();
        let seg_path = dir.path().join("test.seg");
        let mut file = open_rw(&seg_path).await;

        let header = SegmentHeader {
            base_offset: 0,
            created_at_ns: 0,
        };
        SegmentHeader::write(&mut file, &header).await.unwrap();

        let record_pos = write_record(&mut file, &make_entry(0)).await.unwrap();
        let after_record = file.stream_position().await.unwrap();

        // Write zero sentinel (as if a roll was interrupted before the .idx was written)
        file.write_all(&0u32.to_le_bytes()).await.unwrap();
        // Flush so `recover_active`'s `file.metadata().await?.len()` sees the
        // sentinel. `tokio::fs::File::write_all` only fills the internal write
        // buffer; without an explicit flush the on-disk length can still be
        // `after_record` when the metadata is read, the loop's
        // `if pos >= file_len { break; }` check exits before the sentinel is
        // scanned, no truncation happens, and the buffer flushes after the
        // assertion fires. Reproducible on Linux CI; Windows happened to mask
        // it. See PR #7 CI for the failing run.
        file.flush().await.unwrap();

        let (positions, next_file_pos, record_count) = recover_active(&mut file, 0).await.unwrap();

        assert_eq!(record_count, 1);
        assert_eq!(positions[0], record_pos);
        // Sentinel must be removed
        assert_eq!(next_file_pos, after_record);
        assert_eq!(file.metadata().await.unwrap().len(), after_record);
    }
}
