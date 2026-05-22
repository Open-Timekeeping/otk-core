use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use port_out_event_log::{LogEntry, LogSubscription, Offset, StorageError};
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::Notify;

use tokio::io::AsyncReadExt;

use crate::segment::{self, HEADER_LEN, SENTINEL_MSG};

pub(crate) struct SegmentLogSubscription {
    next_offset: u64,
    dir: PathBuf,
    notify: Arc<Notify>,
    /// Current open segment file, byte position of the next record to read,
    /// and a flag indicating whether the file is an active (still-being-written)
    /// segment. EOF on an active segment means "wait for more data"; EOF on a
    /// closed segment means the file is corrupt or truncated.
    current_file: Option<(File, u64, bool)>,
    closed: bool,
}

impl SegmentLogSubscription {
    pub fn new(from: u64, dir: PathBuf, notify: Arc<Notify>) -> Self {
        Self {
            next_offset: from,
            dir,
            notify,
            current_file: None,
            closed: false,
        }
    }

    /// Scan `dir` for `.seg` files.
    ///
    /// Returns `(best, min)` where `best` is the largest base_offset <=
    /// `offset` (the segment that should contain it), and `min` is the
    /// smallest base_offset of any `.seg` file in the directory.
    async fn scan_segments(&self, offset: u64) -> Result<(Option<u64>, Option<u64>), StorageError> {
        let mut rd = tokio::fs::read_dir(&self.dir)
            .await
            .map_err(StorageError::Io)?;

        let mut best: Option<u64> = None;
        let mut min_base: Option<u64> = None;

        loop {
            let entry = match rd.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => return Err(StorageError::Io(e)),
            };
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if let Some(stem) = s.strip_suffix(".seg") {
                // Only accept canonical zero-padded 20-digit names so the
                // filename-derived base matches the path we will open.
                if stem.len() == 20 {
                    if let Ok(base) = stem.parse::<u64>() {
                        if format!("{base:020}") == stem {
                            min_base = Some(min_base.map_or(base, |m: u64| m.min(base)));
                            if base <= offset {
                                best = Some(best.map_or(base, |b: u64| b.max(base)));
                            }
                        }
                    }
                }
            }
        }
        Ok((best, min_base))
    }

    fn seg_path(dir: &Path, base_offset: u64) -> PathBuf {
        dir.join(format!("{base_offset:020}.seg"))
    }

    fn idx_path(dir: &Path, base_offset: u64) -> PathBuf {
        dir.join(format!("{base_offset:020}.idx"))
    }

    /// Open the segment containing `self.next_offset` and seek to the correct
    /// byte position.
    ///
    /// Returns:
    /// - `Ok(Some((file, pos, is_active)))` when the segment was found and opened;
    ///   `is_active` is `true` if no `.idx` companion exists (live segment).
    /// - `Ok(None)` when no data exists yet (caller should wait).
    /// - `Err(RetentionExpired)` when segments exist but none cover the
    ///   requested offset (retention has evicted it).
    async fn open_to_offset(&self) -> Result<Option<(File, u64, bool)>, StorageError> {
        let (best, min_base) = self.scan_segments(self.next_offset).await?;

        let base = match best {
            Some(b) => b,
            None => {
                if let Some(min) = min_base {
                    // Segments exist but none cover next_offset: retention expired.
                    return Err(StorageError::RetentionExpired {
                        requested: Offset::new(self.next_offset),
                        earliest_available: Some(Offset::new(min)),
                    });
                } else {
                    // No .seg files at all. Check the WATERMARK to distinguish a
                    // fully-evicted log (where waiting would never unblock) from
                    // one that was never written (where waiting is correct).
                    if let Some(wm) = read_watermark(&self.dir).await? {
                        if wm > self.next_offset {
                            return Err(StorageError::RetentionExpired {
                                requested: Offset::new(self.next_offset),
                                earliest_available: None,
                            });
                        }
                    }
                    return Ok(None);
                }
            }
        };

        let seg_path = Self::seg_path(&self.dir, base);
        let idx_path = Self::idx_path(&self.dir, base);

        // The segment can be deleted by retention enforcement between scan_segments
        // and this open. Treat NotFound as "no data yet" so the caller waits.
        let mut file = match tokio::fs::OpenOptions::new()
            .read(true)
            .open(&seg_path)
            .await
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Segment was deleted by retention between scan and open. Rescan
                // to determine whether next_offset was evicted or another segment
                // now covers it; do not blindly wait for the next append.
                let (best2, min_base2) = self.scan_segments(self.next_offset).await?;
                if best2.is_none() {
                    if let Some(min) = min_base2 {
                        return Err(StorageError::RetentionExpired {
                            requested: Offset::new(self.next_offset),
                            earliest_available: Some(Offset::new(min)),
                        });
                    }
                }
                return Ok(None);
            }
            Err(e) => return Err(StorageError::Io(e)),
        };

        // Use metadata() rather than exists() so stat failures propagate as
        // errors instead of silently falling through to the active-segment path.
        let idx_exists = match tokio::fs::metadata(&idx_path).await {
            Ok(_) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(StorageError::Io(e)),
        };

        let byte_pos = if idx_exists {
            // closed segment: use the index
            let positions = match segment::read_index(&idx_path).await {
                Ok(p) => p,
                // Index deleted by retention between the metadata check and
                // read_index. Rescan to determine eviction status.
                Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    let (best2, min_base2) = self.scan_segments(self.next_offset).await?;
                    if best2.is_none() {
                        if let Some(min) = min_base2 {
                            return Err(StorageError::RetentionExpired {
                                requested: Offset::new(self.next_offset),
                                earliest_available: Some(Offset::new(min)),
                            });
                        }
                    }
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };
            let i = (self.next_offset - base) as usize;
            if i >= positions.len() {
                // next_offset is past the end of this closed segment; the next
                // segment hasn't appeared yet (or was evicted).
                return Ok(None);
            }
            positions[i]
        } else {
            // active segment: scan linearly from HEADER_LEN to find the record.
            // Use stream_position() after each read_record to advance pos without
            // re-encoding the event CBOR.
            let mut pos = HEADER_LEN;
            let mut current = base;
            loop {
                if current == self.next_offset {
                    break;
                }
                match segment::read_record(&mut file, pos).await {
                    Ok(_) => {
                        pos = file.stream_position().await.map_err(StorageError::Io)?;
                        current += 1;
                    }
                    Err(StorageError::Corrupted(ref msg)) if msg == SENTINEL_MSG => {
                        // The segment was rolled while we were scanning. The
                        // target offset is in the next segment; let the caller
                        // re-scan.
                        return Ok(None);
                    }
                    Err(StorageError::Io(ref e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        // next_offset is beyond the current active tail.
                        // Return the file handle at the current scan position
                        // so next_entry() can keep it across wakeups; without
                        // this, every wakeup would re-scan from HEADER_LEN,
                        // making a subscription ahead of the tail O(n²).
                        file.seek(std::io::SeekFrom::Start(pos))
                            .await
                            .map_err(StorageError::Io)?;
                        return Ok(Some((file, pos, true)));
                    }
                    Err(e) => return Err(e),
                }
            }
            pos
        };

        file.seek(std::io::SeekFrom::Start(byte_pos)).await?;
        Ok(Some((file, byte_pos, !idx_exists)))
    }
}

#[async_trait]
impl LogSubscription for SegmentLogSubscription {
    async fn next_entry(&mut self) -> Option<Result<LogEntry, StorageError>> {
        if self.closed {
            return None;
        }

        loop {
            // Arm the wakeup notification BEFORE the read attempt.  If the
            // appending side calls notify_waiters() between our EOF check and
            // the .await below, the pre-armed future still resolves immediately
            // rather than sleeping until the next write.
            let notified = self.notify.notified();

            // Ensure we have an open file at the right position.
            if self.current_file.is_none() {
                match self.open_to_offset().await {
                    Ok(Some(pair)) => self.current_file = Some(pair),
                    Ok(None) => {
                        // no data yet -- fall through to wait
                    }
                    Err(e @ StorageError::RetentionExpired { .. }) => {
                        self.closed = true;
                        return Some(Err(e));
                    }
                    Err(e) => {
                        self.closed = true;
                        return Some(Err(e));
                    }
                }
            }

            if let Some((ref mut file, ref mut byte_pos, is_active)) = self.current_file {
                match segment::read_record(file, *byte_pos).await {
                    Ok(entry) => {
                        // Advance byte_pos using the file cursor position after
                        // read_record, avoiding a CBOR re-encode for the length.
                        let new_pos = match file.stream_position().await {
                            Ok(p) => p,
                            Err(e) => {
                                self.closed = true;
                                return Some(Err(StorageError::Io(e)));
                            }
                        };
                        *byte_pos = new_pos;

                        // When open_to_offset preserved an EOF position on the
                        // active segment, that position may be before next_offset
                        // if new records were appended in between. Skip past any
                        // intermediate records until we reach next_offset.
                        if entry.offset.as_u64() < self.next_offset && is_active {
                            continue;
                        }

                        if entry.offset.as_u64() != self.next_offset {
                            self.closed = true;
                            return Some(Err(StorageError::Corrupted(format!(
                                "subscription offset mismatch: expected {}, got {}",
                                self.next_offset,
                                entry.offset.as_u64()
                            ))));
                        }
                        self.next_offset += 1;
                        return Some(Ok(entry));
                    }
                    Err(StorageError::Corrupted(ref msg)) if msg == SENTINEL_MSG => {
                        // Segment was rolled. The next segment's base_offset ==
                        // self.next_offset (segments are consecutive), so open
                        // it directly to avoid an O(segments) directory scan on
                        // every segment boundary during backfill.
                        let seg_path = Self::seg_path(&self.dir, self.next_offset);
                        let idx_path = Self::idx_path(&self.dir, self.next_offset);
                        match tokio::fs::OpenOptions::new()
                            .read(true)
                            .open(&seg_path)
                            .await
                        {
                            Ok(next_file) => {
                                let is_active = match tokio::fs::metadata(&idx_path).await {
                                    Ok(_) => false,
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                                    Err(e) => {
                                        self.closed = true;
                                        return Some(Err(StorageError::Io(e)));
                                    }
                                };
                                self.current_file = Some((next_file, HEADER_LEN, is_active));
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // Next segment not yet written or deleted; fall
                                // back to a full scan so retention is detected.
                                self.current_file = None;
                            }
                            Err(e) => {
                                self.closed = true;
                                return Some(Err(StorageError::Io(e)));
                            }
                        }
                        continue;
                    }
                    Err(StorageError::Io(ref e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        if is_active {
                            // Reached the live tail. Keep the file handle and
                            // byte position so the next wakeup retries from the
                            // same spot without re-scanning from the header.
                        } else {
                            // EOF on a closed segment means the file is truncated
                            // or the index points past the end of valid data.
                            self.closed = true;
                            return Some(Err(StorageError::Corrupted(format!(
                                "unexpected EOF on closed segment at byte position {byte_pos}"
                            ))));
                        }
                    }
                    Err(e) => {
                        self.closed = true;
                        return Some(Err(e));
                    }
                }
            }

            // At EOF or no segment yet -- wait for the appending side.
            notified.await;
        }
    }

    async fn close(&mut self) -> Result<(), StorageError> {
        self.closed = true;
        self.current_file = None;
        Ok(())
    }
}

async fn read_watermark(dir: &std::path::Path) -> Result<Option<u64>, StorageError> {
    let path = dir.join("WATERMARK");
    let mut f = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StorageError::Io(e)),
    };
    let mut buf = [0u8; 8];
    match f.read_exact(&mut buf).await {
        Ok(_) => Ok(Some(u64::from_le_bytes(buf))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(StorageError::Io(e)),
    }
}
