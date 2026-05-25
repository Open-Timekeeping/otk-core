use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use event_model::OtkEvent;
use timing_core::ports::outbound::{
    EventLog, LogEntry, LogSubscription, Offset, RetentionPolicy, StorageError,
};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Notify;

use crate::config::SegmentLogConfig;
use crate::segment::{self, SegmentHeader, HEADER_LEN};
use crate::subscription::SegmentLogSubscription;

// ── Internal types ────────────────────────────────────────────────────────────

struct ClosedSegment {
    base_offset: u64,
    last_offset: u64,
    last_appended_at_ns: u64,
    byte_size: u64,
    path: PathBuf,
    positions: Vec<u64>,
}

struct ActiveSegment {
    base_offset: u64,
    path: PathBuf,
    file: File,
    positions: Vec<u64>,
    /// Bytes written past the 24-byte header (== next_file_pos - HEADER_LEN).
    byte_size: u64,
    created_at_ns: u64,
    next_file_pos: u64,
    /// `appended_at_ns` of the most recently written record; 0 if none yet.
    last_appended_at_ns: u64,
}

// ── SegmentLog ────────────────────────────────────────────────────────────────

/// Segment-file event log. The v0 implementation of [`timing_core::ports::outbound::EventLog`].
///
/// Open with [`SegmentLog::open`]. Wrap in a `Mutex` if shared across tasks.
pub struct SegmentLog {
    config: SegmentLogConfig,
    closed: VecDeque<ClosedSegment>,
    active: Option<ActiveSegment>,
    next_offset: u64,
    notify: Arc<Notify>,
    /// Set when a position query fails after a durable write, leaving the
    /// active segment's on-disk bytes unaccounted for in memory. Further
    /// appends are rejected to prevent `ensure_active` from truncating the
    /// durable file. Reopen the log to recover.
    poisoned: bool,
}

impl SegmentLog {
    /// Open (or create) a segment log at the directory in `config`.
    ///
    /// Creates the directory if it does not exist. Scans for existing segment
    /// files, loads their indexes, and recovers the active segment (truncating
    /// at the first corrupt record).
    pub async fn open(config: SegmentLogConfig) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&config.dir).await?;

        let mut seg_bases: Vec<u64> = Vec::new();
        let mut rd = tokio::fs::read_dir(&config.dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name();
            let s = name.to_string_lossy().into_owned();
            if let Some(stem) = s.strip_suffix(".seg") {
                // Only accept canonical zero-padded 20-digit names. Any other
                // .seg file is not ours: accepting it would select it during
                // the scan but then open a different (zero-padded) path.
                if stem.len() == 20 {
                    if let Ok(base) = stem.parse::<u64>() {
                        // Round-trip check: u64::from_str accepts a leading '+',
                        // so "+00000000000000001" parses as 1 but does not format
                        // back to the canonical zero-padded name we would open.
                        if format!("{base:020}") == stem {
                            seg_bases.push(base);
                        }
                    }
                }
            }
        }
        seg_bases.sort_unstable();

        let mut closed: VecDeque<ClosedSegment> = VecDeque::new();
        let mut active: Option<ActiveSegment> = None;
        let mut next_offset: u64 = 0;

        for base in seg_bases {
            let seg_path = config.dir.join(format!("{base:020}.seg"));
            let idx_path = config.dir.join(format!("{base:020}.idx"));

            // Use metadata() rather than exists() so a stat failure propagates
            // as an error instead of silently misclassifying the segment.
            let idx_exists = match tokio::fs::metadata(&idx_path).await {
                Ok(_) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => return Err(StorageError::Io(e)),
            };

            if idx_exists {
                // closed segment: open .seg before reading .idx so an
                // orphaned/corrupt index (crash after .seg deletion) cannot
                // prevent open() from loading the directory.
                let mut file = match File::open(&seg_path).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(StorageError::Io(e)),
                };

                let positions = segment::read_index(&idx_path).await?;
                let record_count = positions.len() as u64;
                if record_count == 0 {
                    continue;
                }
                let next_seg_base = base.checked_add(record_count).ok_or_else(|| {
                    StorageError::Corrupted(format!(
                        "segment {base:020}: base + index_len overflows u64"
                    ))
                })?;
                let last_offset = next_seg_base - 1;

                // Validate magic, version, and base_offset before trusting the
                // index. A mismatched base_offset means the file was renamed or
                // the header is corrupted.
                let hdr = segment::SegmentHeader::read(&mut file).await?;
                if hdr.base_offset != base {
                    return Err(StorageError::Corrupted(format!(
                        "closed segment {base:020}: header base_offset {} does not match filename",
                        hdr.base_offset
                    )));
                }

                let last_pos = *positions.last().unwrap();
                let last_entry = segment::read_record(&mut file, last_pos).await?;
                if last_entry.offset.as_u64() != last_offset {
                    return Err(StorageError::Corrupted(format!(
                        "closed segment {base:020}: index claims last offset {last_offset} \
                         but record contains {}",
                        last_entry.offset.as_u64()
                    )));
                }

                let seg_size = tokio::fs::metadata(&seg_path).await?.len();

                closed.push_back(ClosedSegment {
                    base_offset: base,
                    last_offset,
                    last_appended_at_ns: last_entry.appended_at_ns,
                    byte_size: seg_size,
                    path: seg_path,
                    positions,
                });
                next_offset = next_seg_base;
            } else {
                // If a previous unindexed segment was already found, this one
                // has a higher base_offset, meaning the earlier segment was not
                // properly rolled. An empty orphan (crash during ensure_active
                // before any records were written) is safe to discard. Any
                // prior unindexed segment that contains records is unexpected.
                if let Some(prev) = active.take() {
                    if !prev.positions.is_empty() {
                        return Err(StorageError::Corrupted(format!(
                            "segment {:020} has no index but is not the highest \
                             unindexed segment; {} record(s) would be lost",
                            prev.base_offset,
                            prev.positions.len()
                        )));
                    }
                    // Empty orphan: close the handle then delete the file.
                    drop(prev.file);
                    let _ = tokio::fs::remove_file(&prev.path).await;
                }

                // active segment
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&seg_path)
                    .await?;

                let file_len = file.metadata().await?.len();
                let created_at_ns = if file_len >= HEADER_LEN {
                    let hdr = SegmentHeader::read(&mut file).await?;
                    if hdr.base_offset != base {
                        return Err(StorageError::Corrupted(format!(
                            "active segment {base:020}: header base_offset {} does not match filename",
                            hdr.base_offset
                        )));
                    }
                    hdr.created_at_ns
                } else {
                    // File is shorter than the header: truncate and write a clean
                    // header so future appends find a valid segment on disk.
                    let ts = now_ns();
                    file.set_len(0).await?;
                    file.seek(SeekFrom::Start(0)).await?;
                    let hdr = SegmentHeader {
                        base_offset: base,
                        created_at_ns: ts,
                    };
                    SegmentHeader::write(&mut file, &hdr).await?;
                    ts
                };

                let (positions, next_file_pos, record_count) =
                    segment::recover_active(&mut file, base).await?;

                // Read the last record's timestamp so time-based retention is
                // correct if this segment is rolled on the first append after
                // a restart.
                let last_appended_at_ns = if let Some(&last_pos) = positions.last() {
                    segment::read_record(&mut file, last_pos)
                        .await?
                        .appended_at_ns
                } else {
                    0
                };

                let byte_size = next_file_pos.saturating_sub(HEADER_LEN);
                next_offset = base.checked_add(record_count).ok_or_else(|| {
                    StorageError::Corrupted(format!(
                        "active segment {base:020}: base + record_count overflows u64"
                    ))
                })?;

                active = Some(ActiveSegment {
                    base_offset: base,
                    path: seg_path,
                    file,
                    positions,
                    byte_size,
                    created_at_ns,
                    next_file_pos,
                    last_appended_at_ns,
                });
            }
        }

        // If no segments were found, consult the persisted watermark to
        // distinguish a fully-evicted log (next_offset > 0) from one that
        // was never written to (next_offset == 0).
        if next_offset == 0 && closed.is_empty() && active.is_none() {
            if let Some(wm) = read_watermark(&config.dir).await? {
                next_offset = wm;
            }
        }

        Ok(Self {
            config,
            closed,
            active,
            next_offset,
            notify: Arc::new(Notify::new()),
            poisoned: false,
        })
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn seg_path(&self, base: u64) -> PathBuf {
        self.config.dir.join(format!("{base:020}.seg"))
    }

    fn idx_path(&self, base: u64) -> PathBuf {
        self.config.dir.join(format!("{base:020}.idx"))
    }

    /// Ensure there is an active segment, creating one if needed.
    async fn ensure_active(&mut self) -> Result<(), StorageError> {
        if self.active.is_some() {
            return Ok(());
        }
        let base = self.next_offset;
        let path = self.seg_path(base);
        let ts = now_ns();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;

        let header = SegmentHeader {
            base_offset: base,
            created_at_ns: ts,
        };
        SegmentHeader::write(&mut file, &header).await?;

        // Fsync the parent directory so the new .seg directory entry is
        // durable before the first append commits. Best-effort: ignored on
        // platforms (e.g. Windows) that do not support directory fsync.
        if let Some(parent) = path.parent() {
            if let Ok(dir_file) = tokio::fs::File::open(parent).await {
                let _ = dir_file.sync_all().await;
            }
        }

        self.active = Some(ActiveSegment {
            base_offset: base,
            path,
            file,
            positions: Vec::new(),
            byte_size: 0,
            created_at_ns: ts,
            next_file_pos: HEADER_LEN,
            last_appended_at_ns: 0,
        });
        Ok(())
    }

    /// Roll the active segment: fsync, write sentinel, write index, push to
    /// `self.closed`, then enforce retention.
    ///
    /// All fallible I/O operations run before `self.active` is taken so that a
    /// failure leaves the active segment in a consistent, recoverable state.
    async fn roll_segment(&mut self) -> Result<(), StorageError> {
        match &self.active {
            None => return Ok(()),
            Some(a) if a.positions.is_empty() => {
                let path = a.path.clone();
                self.active = None;
                let _ = tokio::fs::remove_file(&path).await;
                return Ok(());
            }
            _ => {}
        }

        let base_offset = self.active.as_ref().unwrap().base_offset;
        let idx_path = self.idx_path(base_offset);

        // fsync -- failure leaves self.active intact.
        self.active.as_mut().unwrap().file.sync_all().await?;

        // Record where the sentinel will be written so we can undo it if the
        // subsequent index write fails.
        let sentinel_pos = self.active.as_ref().unwrap().next_file_pos;

        // Write sentinel.
        self.active
            .as_mut()
            .unwrap()
            .file
            .write_all(&0u32.to_le_bytes())
            .await?;

        // Sync the sentinel before making the index visible. Without this, a
        // crash after write_index renames the .idx can leave a closed segment
        // (per the index) whose sentinel was never written to disk; subscribers
        // reading to the end of that segment hit EOF and return a false
        // corruption error instead of transitioning to the next segment.
        if let Err(e) = self.active.as_mut().unwrap().file.sync_all().await {
            let active = self.active.as_mut().unwrap();
            let _ = active.file.set_len(sentinel_pos).await;
            let _ = active.file.seek(SeekFrom::Start(sentinel_pos)).await;
            return Err(StorageError::Io(e));
        }

        // Write index atomically (temp + rename).  On failure, undo the
        // sentinel so future appends write to a clean tail.
        {
            let positions = &self.active.as_ref().unwrap().positions;
            if let Err(e) = segment::write_index(&idx_path, positions).await {
                let active = self.active.as_mut().unwrap();
                let _ = active.file.set_len(sentinel_pos).await;
                let _ = active.file.seek(SeekFrom::Start(sentinel_pos)).await;
                return Err(e);
            }
        }

        // Gather per-segment metadata. These use fallbacks so that any I/O
        // error after write_index succeeds never leaves self.active set while
        // an .idx already exists on disk. Once the index is committed the
        // transition to closed must complete.
        let seg_size = self
            .active
            .as_ref()
            .unwrap()
            .file
            .metadata()
            .await
            .map(|m| m.len())
            .unwrap_or_else(|_| self.active.as_ref().unwrap().next_file_pos);
        let last_offset = {
            let a = self.active.as_ref().unwrap();
            a.base_offset + a.positions.len() as u64 - 1
        };
        // Use the in-memory timestamp set by append; no disk read needed.
        let last_appended_at_ns = self.active.as_ref().unwrap().last_appended_at_ns;

        // All fallible ops succeeded; consume the active segment.
        let active = self.active.take().unwrap();
        self.closed.push_back(ClosedSegment {
            base_offset,
            last_offset,
            last_appended_at_ns,
            byte_size: seg_size,
            path: active.path,
            positions: active.positions,
        });

        // Persist next_offset BEFORE retention so that if retention deletes the
        // last segment and then the process crashes, the watermark still records
        // the correct next_offset when open() finds no .seg files.
        write_watermark(&self.config.dir, self.next_offset).await?;

        self.enforce_retention().await?;

        Ok(())
    }

    /// Enforce the configured retention policy by deleting old closed segments.
    async fn enforce_retention(&mut self) -> Result<(), StorageError> {
        match &self.config.retention {
            RetentionPolicy::Indefinite => {}

            RetentionPolicy::TimeBased { max_age_secs } => {
                let cutoff = now_ns().saturating_sub(max_age_secs.saturating_mul(1_000_000_000));
                loop {
                    match self.closed.front() {
                        Some(seg) if seg.last_appended_at_ns < cutoff => {
                            let path = seg.path.clone();
                            let idx = self.idx_path(seg.base_offset);
                            delete_segment(&path, &idx).await?;
                            self.closed.pop_front();
                        }
                        _ => break,
                    }
                }
            }

            RetentionPolicy::SizeBased { max_bytes } => {
                let mut total: u64 = self.closed.iter().map(|s| s.byte_size).sum::<u64>()
                    + self.active.as_ref().map_or(0, |a| a.byte_size);
                while total > *max_bytes {
                    let Some(front) = self.closed.front() else {
                        break;
                    };
                    let evicted_size = front.byte_size;
                    let path = front.path.clone();
                    let idx = self.idx_path(front.base_offset);
                    delete_segment(&path, &idx).await?;
                    self.closed.pop_front();
                    total = total.saturating_sub(evicted_size);
                }
            }

            RetentionPolicy::Hybrid {
                max_age_secs,
                max_bytes,
            } => {
                let cutoff = now_ns().saturating_sub(max_age_secs.saturating_mul(1_000_000_000));
                loop {
                    match self.closed.front() {
                        Some(seg) if seg.last_appended_at_ns < cutoff => {
                            let path = seg.path.clone();
                            let idx = self.idx_path(seg.base_offset);
                            delete_segment(&path, &idx).await?;
                            self.closed.pop_front();
                        }
                        _ => break,
                    }
                }
                let mut total: u64 = self.closed.iter().map(|s| s.byte_size).sum::<u64>()
                    + self.active.as_ref().map_or(0, |a| a.byte_size);
                while total > *max_bytes {
                    let Some(front) = self.closed.front() else {
                        break;
                    };
                    let evicted_size = front.byte_size;
                    let path = front.path.clone();
                    let idx = self.idx_path(front.base_offset);
                    delete_segment(&path, &idx).await?;
                    self.closed.pop_front();
                    total = total.saturating_sub(evicted_size);
                }
            }
        }
        Ok(())
    }

    fn should_roll(&self) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        if active.byte_size >= self.config.max_segment_bytes {
            return true;
        }
        let elapsed_ns = now_ns().saturating_sub(active.created_at_ns);
        elapsed_ns
            >= self
                .config
                .max_segment_age_secs
                .saturating_mul(1_000_000_000)
    }

    /// Return the raw earliest offset value, or `None` if the log is empty.
    fn earliest_offset_raw(&self) -> Option<u64> {
        if let Some(first) = self.closed.front() {
            return Some(first.base_offset);
        }
        // Only return the active segment's base_offset if it has records.
        // An empty active segment (header only) has no readable entries.
        self.active.as_ref().and_then(|a| {
            if a.positions.is_empty() {
                None
            } else {
                Some(a.base_offset)
            }
        })
    }

    /// Return the raw latest offset value (last appended), or `None`.
    fn latest_offset_raw(&self) -> Option<u64> {
        if let Some(active) = &self.active {
            if !active.positions.is_empty() {
                return Some(active.base_offset + active.positions.len() as u64 - 1);
            }
        }
        if let Some(back) = self.closed.back() {
            return Some(back.last_offset);
        }
        // All segments have been evicted: derive from the watermark.
        if self.next_offset > 0 {
            return Some(self.next_offset - 1);
        }
        None
    }

    /// True if the log was populated at some point but all retained entries are gone.
    fn is_fully_evicted(&self) -> bool {
        let active_empty = self.active.as_ref().is_none_or(|a| a.positions.is_empty());
        self.closed.is_empty() && active_empty && self.next_offset > 0
    }
}

// ── EventLog impl ─────────────────────────────────────────────────────────────

#[async_trait]
impl EventLog for SegmentLog {
    /// Append a batch of events.
    ///
    /// Returns the offset of the last event on success. Once in-memory state is
    /// committed, the records are visible via `read_range` regardless of whether
    /// the subsequent segment roll succeeds. With `flush_interval_ms=0` the
    /// records are also durable at that point; with `flush_interval_ms>0` they
    /// are OS-buffered only (at-least-once write contract). If any error is
    /// returned after the commit point (e.g. a roll failure), callers must check
    /// `latest_offset()` before deciding whether to retry -- submitting the same
    /// batch again would duplicate events.
    async fn append(
        &mut self,
        producer_id: &str,
        events: &[OtkEvent],
    ) -> Result<Offset, StorageError> {
        if self.poisoned {
            return Err(StorageError::Corrupted(
                "log is poisoned: position query failed after a durable write; \
                 reopen the log to recover"
                    .into(),
            ));
        }
        if events.is_empty() {
            return Err(StorageError::InvalidInput(
                "events slice must not be empty".into(),
            ));
        }
        if producer_id.is_empty() {
            return Err(StorageError::InvalidInput(
                "producer_id must not be empty".into(),
            ));
        }

        self.ensure_active().await?;

        let ts = now_ns();

        // Seek to the current write position.  read_range may have moved the
        // file cursor; this guarantees writes land at the right offset.
        let batch_start_file_pos = {
            let active = self.active.as_mut().unwrap();
            let pos = active.next_file_pos;
            active.file.seek(SeekFrom::Start(pos)).await?;
            pos
        };

        let mut local_next_offset = self.next_offset;
        let mut new_positions: Vec<u64> = Vec::with_capacity(events.len());

        for event in events {
            // u64 offset space is practically inexhaustible, but guard against
            // silent wrap-around on a log that has been running for centuries.
            if local_next_offset == u64::MAX {
                let active = self.active.as_mut().unwrap();
                let _ = active.file.set_len(batch_start_file_pos).await;
                return Err(StorageError::InvalidInput("offset space exhausted".into()));
            }

            let entry = LogEntry {
                offset: Offset::new(local_next_offset),
                appended_at_ns: ts,
                producer_id: producer_id.to_string(),
                event: event.clone(),
            };

            let active = self.active.as_mut().unwrap();
            match segment::write_record(&mut active.file, &entry).await {
                Ok(pos) => {
                    new_positions.push(pos);
                    local_next_offset += 1;
                }
                Err(e) => {
                    // Truncate back to the start of this batch. The set_len
                    // error is intentionally ignored: if truncation fails the
                    // partial bytes will be discarded by recover_active on the
                    // next open(), so data integrity is preserved either way.
                    let _ = active.file.set_len(batch_start_file_pos).await;
                    return Err(e);
                }
            }
        }

        // fsync (synchronous mode) before committing in-memory state.
        {
            let active = self.active.as_mut().unwrap();
            if self.config.flush_interval_ms == 0 {
                if let Err(e) = active.file.sync_all().await {
                    let _ = active.file.set_len(batch_start_file_pos).await;
                    return Err(StorageError::Io(e));
                }
            }
        }

        // Query the write position once to derive byte_size without re-encoding.
        // If stream_position fails, fall back to seek(End(0)) which returns the
        // same value. Both are I/O-free on most OS/FS implementations.
        // Do NOT truncate on failure here: the batch bytes have already been
        // written (and synced if flush_interval_ms=0; OS-buffered only if >0).
        //
        // If both queries fail, poison the log so ensure_active cannot later
        // truncate the segment file when creating a new one at the same path.
        let new_file_pos_result: Result<u64, std::io::Error> = {
            let file = &mut self.active.as_mut().unwrap().file;
            match file.stream_position().await {
                Ok(pos) => Ok(pos),
                Err(e1) => match file.seek(SeekFrom::End(0)).await {
                    Ok(pos) => Ok(pos),
                    Err(_) => Err(e1),
                },
            }
        };
        let new_file_pos = match new_file_pos_result {
            Ok(pos) => pos,
            Err(e) => {
                // Both position queries failed after a durable write. Poison the
                // log so ensure_active cannot later truncate the durable file by
                // recreating a segment at the same base_offset.
                self.poisoned = true;
                return Err(StorageError::Io(e));
            }
        };

        // Commit in-memory state. With flush_interval_ms=0 the sync above
        // guarantees durability; with flush_interval_ms>0 fsync is skipped
        // and records are buffered in the OS (at-least-once write contract --
        // see append docs).
        {
            let active = self.active.as_mut().unwrap();
            active.positions.extend_from_slice(&new_positions);
            active.next_file_pos = new_file_pos;
            active.byte_size = new_file_pos.saturating_sub(HEADER_LEN);
            active.last_appended_at_ns = ts;
        }
        self.next_offset = local_next_offset;

        self.notify.notify_waiters();

        if self.should_roll() {
            self.roll_segment().await?;
        }

        Ok(Offset::new(self.next_offset - 1))
    }

    async fn read_range(
        &mut self,
        from: Offset,
        to: Option<Offset>,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let from_raw = from.as_u64();
        let to_raw = to.map(|o| o.as_u64());

        // Check for an empty range first, before any retention checks, so that
        // read_range(from, Some(from)) always returns Ok([]) regardless of whether
        // `from` is inside or outside the retained window.
        if let Some(to_raw) = to_raw {
            if to_raw <= from_raw {
                return Ok(vec![]);
            }
        }

        // From is at or beyond the tail (includes a never-populated log where
        // next_offset == 0): no records exist there yet.
        if from_raw >= self.next_offset {
            return Ok(vec![]);
        }

        // All previously written events have been evicted.
        if self.is_fully_evicted() {
            return Err(StorageError::RetentionExpired {
                requested: from,
                earliest_available: None,
            });
        }

        // From is before the retained window.
        if let Some(earliest) = self.earliest_offset_raw() {
            if from_raw < earliest {
                return Err(StorageError::RetentionExpired {
                    requested: from,
                    earliest_available: Some(Offset::new(earliest)),
                });
            }
        }

        let effective_to = to_raw.unwrap_or(u64::MAX);
        let mut entries: Vec<LogEntry> = Vec::new();

        // Read from closed segments.
        for seg in &self.closed {
            if seg.last_offset < from_raw {
                continue;
            }
            if seg.base_offset >= effective_to {
                break;
            }
            let start = from_raw.max(seg.base_offset);
            let end = effective_to.min(seg.last_offset + 1);

            let mut file = File::open(&seg.path).await?;
            for offset in start..end {
                let i = (offset - seg.base_offset) as usize;
                if i >= seg.positions.len() {
                    break;
                }
                let entry = segment::read_record(&mut file, seg.positions[i]).await?;
                if entry.offset.as_u64() != offset {
                    return Err(StorageError::Corrupted(format!(
                        "index points to wrong record at position {}: expected offset {offset}, got {}",
                        seg.positions[i], entry.offset.as_u64()
                    )));
                }
                entries.push(entry);
            }
        }

        // Read from active segment.
        if let Some(active) = &mut self.active {
            if active.base_offset < effective_to
                && !active.positions.is_empty()
                && active.base_offset + active.positions.len() as u64 > from_raw
            {
                let start = from_raw.max(active.base_offset);
                let end = effective_to.min(active.base_offset + active.positions.len() as u64);

                for offset in start..end {
                    let i = (offset - active.base_offset) as usize;
                    if i >= active.positions.len() {
                        break;
                    }
                    let entry = segment::read_record(&mut active.file, active.positions[i]).await?;
                    if entry.offset.as_u64() != offset {
                        return Err(StorageError::Corrupted(format!(
                            "index points to wrong record at position {}: expected offset {offset}, got {}",
                            active.positions[i], entry.offset.as_u64()
                        )));
                    }
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }

    async fn latest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        Ok(self.latest_offset_raw().map(Offset::new))
    }

    async fn earliest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        Ok(self.earliest_offset_raw().map(Offset::new))
    }

    async fn subscribe(&mut self, from: Offset) -> Result<Box<dyn LogSubscription>, StorageError> {
        let from_raw = from.as_u64();

        // Subscribing at or beyond the tail is always valid; the subscription
        // will wait for events that have not been written yet.
        if from_raw >= self.next_offset {
            return Ok(Box::new(SegmentLogSubscription::new(
                from_raw,
                self.config.dir.clone(),
                Arc::clone(&self.notify),
            )));
        }

        // All previously written events have been evicted.
        if self.is_fully_evicted() {
            return Err(StorageError::RetentionExpired {
                requested: from,
                earliest_available: None,
            });
        }

        // From is before the retained window.
        if let Some(earliest) = self.earliest_offset_raw() {
            if from_raw < earliest {
                return Err(StorageError::RetentionExpired {
                    requested: from,
                    earliest_available: Some(Offset::new(earliest)),
                });
            }
        }

        Ok(Box::new(SegmentLogSubscription::new(
            from_raw,
            self.config.dir.clone(),
            Arc::clone(&self.notify),
        )))
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

async fn delete_segment(seg_path: &Path, idx_path: &Path) -> Result<(), StorageError> {
    // Delete the data file first. An orphaned .idx (if we crash between the two
    // removes) is safe: open() skips closed segments with missing .seg files.
    match tokio::fs::remove_file(seg_path).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StorageError::Io(e)),
    }
    // Once .seg is gone the segment is logically deleted. Remove .idx on a
    // best-effort basis; a failure here leaves an orphaned .idx that open()
    // will skip, so it is safe to ignore rather than returning an error that
    // would leave the caller with a missing .seg but an entry in self.closed.
    let _ = tokio::fs::remove_file(idx_path).await;
    // Fsync the parent directory so the unlinks are durable. Without this,
    // a crash can resurrect deleted segments on filesystems that require
    // directory fsync for metadata durability. Best-effort: ignored on
    // platforms that do not support directory fsync.
    if let Some(parent) = seg_path.parent() {
        if let Ok(dir_file) = tokio::fs::File::open(parent).await {
            let _ = dir_file.sync_all().await;
        }
    }
    Ok(())
}

/// Atomically persist `next_offset` so a fully-evicted log can be distinguished
/// from a never-populated one after a restart with no segment files on disk.
async fn write_watermark(dir: &Path, next_offset: u64) -> Result<(), StorageError> {
    let path = dir.join("WATERMARK");
    let tmp = dir.join("WATERMARK.tmp");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .await?;
    f.write_all(&next_offset.to_le_bytes()).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp, &path).await?;
    // Fsync the parent directory so the rename is durable on Linux/macOS.
    // Opening a directory and calling sync_all is not supported on Windows;
    // the error is intentionally ignored so the code stays cross-platform.
    if let Ok(dir_file) = tokio::fs::File::open(dir).await {
        let _ = dir_file.sync_all().await;
    }
    Ok(())
}

/// Read the persisted watermark, returning `None` if the file does not exist.
async fn read_watermark(dir: &Path) -> Result<Option<u64>, StorageError> {
    use tokio::io::AsyncReadExt;
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use event_model::{
        detection::SensorData, Detection, DetectionId, DetectorId, OtkEvent, SourceAttestation,
        TimebaseId, TimestampingMethod, TimingPointId,
    };
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn make_event() -> OtkEvent {
        OtkEvent::Detection(Detection {
            detection_id: DetectionId::new("d1"),
            detector_id: DetectorId::new("loop-a"),
            timing_point_id: TimingPointId::new("tp-1"),
            subject_id: None,
            detected_at_ns: 1_700_000_000_000_000_000,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("gps-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 1,
            sensor: SensorData::BeamBreak,
        })
    }

    async fn open_log(dir: &std::path::Path) -> SegmentLog {
        SegmentLog::open(SegmentLogConfig {
            dir: dir.to_path_buf(),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn append_and_read_single() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;

        let last = log.append("test-producer", &[make_event()]).await.unwrap();
        assert_eq!(last, Offset::new(0));

        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].offset, Offset::new(0));
        assert_eq!(entries[0].producer_id, "test-producer");
    }

    #[tokio::test]
    async fn producer_id_round_trips_per_batch() {
        // Each append call tags every event in its batch with the same
        // producer_id; consecutive appends with different ids must be
        // distinguishable on read-back so the runtime can rebuild
        // per-producer state from the log.
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;

        log.append("loop-1", &[make_event(), make_event()])
            .await
            .unwrap();
        log.append("loop-2", &[make_event()]).await.unwrap();
        log.append("loop-1", &[make_event()]).await.unwrap();

        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].producer_id, "loop-1");
        assert_eq!(entries[1].producer_id, "loop-1");
        assert_eq!(entries[2].producer_id, "loop-2");
        assert_eq!(entries[3].producer_id, "loop-1");
    }

    #[tokio::test]
    async fn append_rejects_empty_producer_id() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        let err = log.append("", &[make_event()]).await.unwrap_err();
        assert!(
            matches!(err, StorageError::InvalidInput(ref m) if m.contains("producer_id")),
            "expected InvalidInput about producer_id, got {err:?}"
        );
    }

    #[tokio::test]
    async fn producer_id_survives_reopen() {
        // The whole point of persisting producer_id: it must be there
        // after the process restarts, which is what the runtime's
        // sequence-gate restart-resume hooks into.
        let dir = tempdir().unwrap();
        {
            let mut log = open_log(dir.path()).await;
            log.append("p-a", &[make_event()]).await.unwrap();
            log.append("p-b", &[make_event()]).await.unwrap();
        }
        // Drop and reopen.
        let mut log = open_log(dir.path()).await;
        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].producer_id, "p-a");
        assert_eq!(entries[1].producer_id, "p-b");
    }

    #[tokio::test]
    async fn append_multiple_read_range() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;

        log.append("test-producer", &[make_event(), make_event(), make_event()])
            .await
            .unwrap();

        let entries = log
            .read_range(Offset::new(1), Some(Offset::new(3)))
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, Offset::new(1));
        assert_eq!(entries[1].offset, Offset::new(2));
    }

    #[tokio::test]
    async fn read_range_empty_log_never_populated() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn latest_and_earliest_offset_empty() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        assert!(log.latest_offset().await.unwrap().is_none());
        assert!(log.earliest_offset().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn latest_and_earliest_after_append() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        log.append("test-producer", &[make_event(), make_event()])
            .await
            .unwrap();
        assert_eq!(log.latest_offset().await.unwrap(), Some(Offset::new(1)));
        assert_eq!(log.earliest_offset().await.unwrap(), Some(Offset::new(0)));
    }

    #[tokio::test]
    async fn read_range_from_beyond_tail() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        log.append("test-producer", &[make_event(), make_event(), make_event()])
            .await
            .unwrap();
        let entries = log.read_range(Offset::new(5), None).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn subscribe_live_delivery() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(open_log(dir.path()).await));
        let mut sub = log.lock().await.subscribe(Offset::new(0)).await.unwrap();

        let log2 = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            log2.lock()
                .await
                .append("test-producer", &[make_event()])
                .await
                .unwrap();
        });

        let entry = sub.next_entry().await.unwrap().unwrap();
        assert_eq!(entry.offset, Offset::new(0));
        sub.close().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_backfill_then_wait() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(open_log(dir.path()).await));

        log.lock()
            .await
            .append("test-producer", &[make_event(), make_event(), make_event()])
            .await
            .unwrap();

        let mut sub = log.lock().await.subscribe(Offset::new(0)).await.unwrap();

        // Consume backfill.
        for i in 0u64..3 {
            let entry = sub.next_entry().await.unwrap().unwrap();
            assert_eq!(entry.offset, Offset::new(i));
        }

        // Append a 4th event from a background task; the subscription must
        // wake and deliver it rather than hanging at the live tail.
        let log2 = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            log2.lock()
                .await
                .append("test-producer", &[make_event()])
                .await
                .unwrap();
        });

        let entry = sub.next_entry().await.unwrap().unwrap();
        assert_eq!(entry.offset, Offset::new(3));

        sub.close().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_returns_error_if_retention_expired() {
        let dir = tempdir().unwrap();
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::SizeBased { max_bytes: 0 },
            ..Default::default()
        })
        .await
        .unwrap();

        // Two appends: each rolls immediately and the closed segment is
        // evicted, so offset 0 is no longer available.
        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();

        let result = log.subscribe(Offset::new(0)).await;
        assert!(
            matches!(result, Err(StorageError::RetentionExpired { .. })),
            "expected RetentionExpired, got Ok"
        );
    }

    #[tokio::test]
    async fn segment_roll_on_size() {
        let dir = tempdir().unwrap();
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();

        assert!(
            !log.closed.is_empty(),
            "expected at least one closed segment after size-triggered rolls"
        );
    }

    #[tokio::test]
    async fn retention_size_based() {
        let dir = tempdir().unwrap();
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::SizeBased { max_bytes: 0 },
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();

        // With max_bytes=0 every closed segment is deleted immediately after
        // each roll, so nothing survives in the closed list.
        assert_eq!(log.next_offset, 3, "three events were appended");
        assert!(
            log.closed.is_empty(),
            "all closed segments should be evicted with max_bytes=0"
        );
    }

    #[tokio::test]
    async fn reopen_recovers_state() {
        let dir = tempdir().unwrap();

        {
            let mut log = open_log(dir.path()).await;
            log.append("test-producer", &[make_event(), make_event()])
                .await
                .unwrap();
        }

        let mut log2 = open_log(dir.path()).await;
        let entries = log2.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, Offset::new(0));
        assert_eq!(entries[1].offset, Offset::new(1));
    }

    #[tokio::test]
    async fn reopen_recovers_torn_write() {
        let dir = tempdir().unwrap();

        {
            let mut log = open_log(dir.path()).await;
            log.append("test-producer", &[make_event(), make_event()])
                .await
                .unwrap();
        }

        // Locate the .seg file.
        let mut seg_path = None;
        let mut rd = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(entry) = rd.next_entry().await.unwrap() {
            if entry.file_name().to_string_lossy().ends_with(".seg") {
                seg_path = Some(entry.path());
                break;
            }
        }
        let seg_path = seg_path.unwrap();

        // Read the first record's payload_len so we can compute exactly where
        // the second record starts, then tear it 5 bytes in.
        let second_record_start = {
            let mut f = tokio::fs::File::open(&seg_path).await.unwrap();
            f.seek(SeekFrom::Start(HEADER_LEN)).await.unwrap();
            let mut buf = [0u8; 4];
            f.read_exact(&mut buf).await.unwrap();
            let payload_len = u32::from_le_bytes(buf);
            HEADER_LEN + 4 + payload_len as u64 + 4
        };
        let torn_pos = second_record_start + 5;

        tokio::fs::OpenOptions::new()
            .write(true)
            .open(&seg_path)
            .await
            .unwrap()
            .set_len(torn_pos)
            .await
            .unwrap();

        let mut log2 = open_log(dir.path()).await;
        let entries = log2.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 1, "torn second record must be discarded");
        assert_eq!(entries[0].offset, Offset::new(0));
    }

    #[tokio::test]
    async fn append_empty_slice_returns_invalid_input() {
        let dir = tempdir().unwrap();
        let mut log = open_log(dir.path()).await;
        let result = log.append("test-producer", &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn retention_time_based() {
        let dir = tempdir().unwrap();
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::TimeBased { max_age_secs: 0 },
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        // Give the first segment's timestamp time to fall strictly before the
        // cutoff computed inside enforce_retention. 50 ms is enough headroom
        // on Windows where the system clock has ~15 ms resolution.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        log.append("test-producer", &[make_event()]).await.unwrap();

        // With max_age_secs=0, segments whose last_appended_at_ns is strictly
        // before the cutoff are evicted. The first segment (appended 50+ ms
        // ago) is guaranteed evicted. The just-rolled second segment may or may
        // not be evicted depending on clock resolution; do not assert
        // closed.is_empty(). What matters is that offset 0 is no longer readable.
        assert_eq!(log.next_offset, 2, "two events were appended");
        let result = log.read_range(Offset::new(0), None).await;
        assert!(
            matches!(result, Err(StorageError::RetentionExpired { .. })),
            "expected RetentionExpired for evicted offset 0"
        );
    }

    #[tokio::test]
    async fn retention_hybrid() {
        let dir = tempdir().unwrap();
        // max_bytes: u64::MAX disables size-based eviction so only the age
        // branch of the hybrid policy runs, ensuring this test exercises that
        // code path rather than relying on the size policy to do the work.
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::Hybrid {
                max_age_secs: 0,
                max_bytes: u64::MAX,
            },
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        log.append("test-producer", &[make_event()]).await.unwrap();

        // The first segment (appended 50+ ms ago) is guaranteed evicted by
        // the age branch. Do not assert closed.is_empty() for the same reason
        // as retention_time_based: the just-rolled segment's timestamp may
        // equal the cutoff on coarse-resolution clocks.
        assert_eq!(log.next_offset, 2);
        let result = log.read_range(Offset::new(0), None).await;
        assert!(matches!(result, Err(StorageError::RetentionExpired { .. })));
    }

    #[tokio::test]
    async fn retention_hybrid_size_based() {
        let dir = tempdir().unwrap();
        // max_age_secs: u64::MAX disables age-based eviction; only the size
        // branch of the hybrid policy runs, ensuring this test exercises that
        // code path rather than relying on the age policy to do the work.
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::Hybrid {
                max_age_secs: u64::MAX,
                max_bytes: 1,
            },
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();

        // Both segments were evicted by the size branch of the Hybrid policy.
        // With max_bytes=1 every closed segment exceeds the limit immediately.
        assert_eq!(log.next_offset, 2);
        let result = log.read_range(Offset::new(0), None).await;
        assert!(matches!(result, Err(StorageError::RetentionExpired { .. })));
    }

    #[tokio::test]
    async fn read_range_across_rolled_segments() {
        let dir = tempdir().unwrap();
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();
        log.append("test-producer", &[make_event()]).await.unwrap();

        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 3, "all three events should be readable");
        assert_eq!(entries[0].offset, Offset::new(0));
        assert_eq!(entries[1].offset, Offset::new(1));
        assert_eq!(entries[2].offset, Offset::new(2));
    }

    #[tokio::test]
    async fn reopen_reads_closed_segment_via_index() {
        let dir = tempdir().unwrap();

        {
            let mut log = SegmentLog::open(SegmentLogConfig {
                dir: dir.path().to_path_buf(),
                max_segment_bytes: 1,
                ..Default::default()
            })
            .await
            .unwrap();
            // Each append forces a roll, so both events end up in closed segments.
            log.append("test-producer", &[make_event()]).await.unwrap();
            log.append("test-producer", &[make_event()]).await.unwrap();
            assert!(
                !log.closed.is_empty(),
                "should have at least one closed segment"
            );
        }

        let mut log2 = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            ..Default::default()
        })
        .await
        .unwrap();
        let entries = log2.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(entries.len(), 2, "both events should survive reopen");
        assert_eq!(entries[0].offset, Offset::new(0));
        assert_eq!(entries[1].offset, Offset::new(1));
    }

    #[tokio::test]
    async fn fully_evicted_log_watermark_survives_reopen() {
        let dir = tempdir().unwrap();

        {
            let mut log = SegmentLog::open(SegmentLogConfig {
                dir: dir.path().to_path_buf(),
                max_segment_bytes: 1,
                retention: RetentionPolicy::SizeBased { max_bytes: 0 },
                ..Default::default()
            })
            .await
            .unwrap();

            log.append("test-producer", &[make_event()]).await.unwrap();
            log.append("test-producer", &[make_event()]).await.unwrap();

            assert!(log.closed.is_empty(), "all segments should be evicted");
            assert_eq!(log.next_offset, 2);
        }

        // Reopen with no .seg files on disk. The watermark must tell open() that
        // next_offset is 2, so read_range returns RetentionExpired, not Ok([]).
        let mut log2 = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1,
            retention: RetentionPolicy::SizeBased { max_bytes: 0 },
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(log2.next_offset, 2, "watermark must survive reopen");
        let result = log2.read_range(Offset::new(0), None).await;
        assert!(
            matches!(result, Err(StorageError::RetentionExpired { .. })),
            "expected RetentionExpired after reopen of fully-evicted log, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn segment_roll_on_age() {
        let dir = tempdir().unwrap();
        // max_segment_age_secs=0 means elapsed_ns >= 0 is always true, so every
        // append triggers a roll -- exercises the time-based roll path without
        // needing a real-time sleep.
        let mut log = SegmentLog::open(SegmentLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_age_secs: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        log.append("test-producer", &[make_event()]).await.unwrap();
        assert!(
            !log.closed.is_empty(),
            "segment should roll after first append with age=0"
        );

        log.append("test-producer", &[make_event()]).await.unwrap();
        assert_eq!(
            log.closed.len(),
            2,
            "second append should roll into a second closed segment"
        );

        let entries = log.read_range(Offset::new(0), None).await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "both events readable after age-based rolls"
        );
        assert_eq!(entries[0].offset, Offset::new(0));
        assert_eq!(entries[1].offset, Offset::new(1));
    }

    #[tokio::test]
    async fn subscribe_ahead_of_tail_skips_to_requested_offset() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(open_log(dir.path()).await));

        // Subscribe at offset 3 before any events exist.
        let mut sub = log.lock().await.subscribe(Offset::new(3)).await.unwrap();

        let log2 = Arc::clone(&log);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            // Append offsets 0-3 in one batch; subscription should skip 0-2
            // and deliver only offset 3.
            log2.lock()
                .await
                .append(
                    "test-producer",
                    &[make_event(), make_event(), make_event(), make_event()],
                )
                .await
                .unwrap();
        });

        let entry = sub.next_entry().await.unwrap().unwrap();
        assert_eq!(
            entry.offset,
            Offset::new(3),
            "subscription must skip intermediate offsets and deliver the requested one"
        );

        sub.close().await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_across_rolled_segment() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            SegmentLog::open(SegmentLogConfig {
                dir: dir.path().to_path_buf(),
                max_segment_bytes: 1,
                ..Default::default()
            })
            .await
            .unwrap(),
        ));

        // Append two events, each in its own rolled segment.
        {
            let mut l = log.lock().await;
            l.append("test-producer", &[make_event()]).await.unwrap();
            l.append("test-producer", &[make_event()]).await.unwrap();
        }

        let mut sub = log.lock().await.subscribe(Offset::new(0)).await.unwrap();

        // Both events should be delivered in order, crossing the segment boundary.
        for i in 0u64..2 {
            let entry = sub.next_entry().await.unwrap().unwrap();
            assert_eq!(entry.offset, Offset::new(i), "offset mismatch at i={i}");
        }

        sub.close().await.unwrap();
    }
}
