use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use event_model::{CrossingEvent, CrossingId, OtkEvent};
use futures_util::stream;
use port_out_event_log::{EventLog, Offset, StorageError};
use timing_core::{Crossing, CrossingProcessor, ProcessorConfig};
use tracing::{debug, info, warn};

use crate::metrics::Metrics;
use crate::ports::{EventEntry, EventPage, EventQueryPort, EventStream, QueryError};
use crate::sequence_gate::{GateDecision, SequenceGate};

/// Shared node state: event log and crossing processor.
///
/// The log is held behind the `EventLog` trait, not a concrete backend; the
/// composition root (`Node::new`) constructs the backend and injects it here.
///
/// # Concurrency discipline
///
/// Both halves are `Arc`-wrapped so connection handler tasks can hold clones.
/// `EventLog::append` is async, so the log uses `tokio::sync::Mutex`.
/// `CrossingProcessor` uses a `std::sync::Mutex` and is **never held across an
/// `.await`** — `append_event` acquires it, runs `push_detection` (a fully
/// synchronous call), drops it, and only then takes the async log lock.
///
/// The two mutexes are never held simultaneously, so deadlock is impossible
/// regardless of lock ordering. The single bottleneck under high-rate ingest
/// is the log mutex; replacing it with a single-owner actor task is a
/// recognised future optimisation (see `spec/open-questions.md`).
///
/// # Batching
///
/// `append_event` builds a `[detection, ...crossings]` slice and submits it in
/// one `EventLog::append` call. This halves lock acquisitions (and fsync, when
/// per-append is on) whenever a detection produces crossings, and makes the
/// detection + its derived crossings atomic at the storage layer.
pub struct NodePipeline {
    log: Arc<tokio::sync::Mutex<Box<dyn EventLog>>>,
    processor: Arc<Mutex<CrossingProcessor>>,
    gate: Arc<SequenceGate>,
    metrics: Arc<Metrics>,
}

/// Outcome of a successful `append_event` call.
#[derive(Debug, Clone, Copy)]
pub enum AppendOutcome {
    /// Event (and any derived crossings) were persisted; `offset` is the offset
    /// of the last record in the batch. The detection's own offset is recoverable
    /// as `offset - n_crossings` if needed.
    Appended(Offset),
    /// Event was a duplicate per the sequence gate and was dropped without
    /// persisting. The runtime treats this as a success at the boundary so
    /// the producer's session stays open.
    DroppedDuplicate,
}

impl NodePipeline {
    /// Build a pipeline around an externally-constructed event log and a
    /// pre-seeded sequence gate.
    ///
    /// The log is type-erased so the runtime depends on the `EventLog` port,
    /// not on any concrete storage backend. The gate is passed in (rather
    /// than constructed here) so the composition root can call
    /// [`crate::sequence_gate::seed_from_log_box`] against the opened log
    /// before any ingest listener spawns, restoring per-producer high-water
    /// marks across node restarts.
    pub fn new(log: Box<dyn EventLog>, metrics: Arc<Metrics>, gate: Arc<SequenceGate>) -> Self {
        let processor = CrossingProcessor::new(ProcessorConfig::default());
        Self {
            log: Arc::new(tokio::sync::Mutex::new(log)),
            processor: Arc::new(Mutex::new(processor)),
            gate,
            metrics,
        }
    }

    /// Append an event to the log, scoped to a specific producer.
    ///
    /// `Detection` events are filtered through the sequence gate: duplicates
    /// are dropped silently (returns [`AppendOutcome::DroppedDuplicate`]),
    /// gaps are logged + metered (in M7) but the event is still persisted.
    /// Non-`Detection` events bypass the gate and are persisted unconditionally.
    ///
    /// On `Detection` accept, the event is also pushed through the crossing
    /// processor; resulting crossings are appended as `OtkEvent::Crossing`.
    pub async fn append_event(
        &self,
        producer_id: &str,
        event: OtkEvent,
    ) -> Result<AppendOutcome, StorageError> {
        // ── Phase 1: decide ───────────────────────────────────────────
        // Peek the gate so we know whether to drop the detection without
        // advancing the high-water mark. The gate only commits in Phase 3
        // after a successful storage append. Without this split, an
        // append failure would have left the gate advanced and any
        // producer retry of the same sequence would have been silently
        // treated as a duplicate.
        //
        // For an observed `Gap`, remember the (expected, got) here but
        // do NOT increment `sequence_gaps` yet. A storage failure followed
        // by a producer retry would re-observe the same gap (gate didn't
        // commit) and double-count it, turning transient storage outages
        // into spurious gap alerts. The metric increment + warn log are
        // deferred to Phase 3 alongside the gate commit so each true gap
        // counts exactly once.
        let mut pending_gap: Option<(u64, u64)> = None;
        if let OtkEvent::Detection(ref det) = event {
            match self.gate.peek(producer_id, det) {
                GateDecision::Accept | GateDecision::Advance { .. } => {}
                GateDecision::Gap { expected, got } => {
                    pending_gap = Some((expected, got));
                }
                GateDecision::Duplicate { high_water, got } => {
                    debug!(
                        producer = %producer_id,
                        detector = %det.detector_id,
                        high_water,
                        got_seq = got,
                        "duplicate detection dropped"
                    );
                    self.metrics.events_dropped_duplicates.incr(&[
                        ("producer_id", producer_id),
                        ("detector_id", det.detector_id.as_str()),
                    ]);
                    return Ok(AppendOutcome::DroppedDuplicate);
                }
            }
        }

        let event_kind = event_kind_label(&event);

        // ── Phase 2: compute crossings (peek) + try append ────────────
        //
        // `CrossingProcessor::peek_detection` returns the crossings that
        // a subsequent `commit_detection` would emit, without mutating
        // the processor's grouping window. The actual `commit_detection`
        // call is deferred to Phase 3 alongside the gate commit so a
        // storage append failure can't leave the processor advanced past
        // what was persisted. Without this split, a producer retry after
        // an append failure would observe a different crossing shape
        // than the first attempt would have (the original group would
        // have been consumed and any new detections with the same key
        // would start a fresh group).
        let crossings: Vec<OtkEvent> = if let OtkEvent::Detection(ref det) = event {
            self.processor
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .peek_detection(det)
                .into_iter()
                .map(|c| OtkEvent::Crossing(map_crossing(&c)))
                .collect()
        } else {
            Vec::new()
        };

        let mut batch: Vec<OtkEvent> = Vec::with_capacity(1 + crossings.len());
        batch.push(event);
        batch.extend(crossings);

        let offset = self.log.lock().await.append(producer_id, &batch).await?;

        // ── Phase 3: commit ───────────────────────────────────────────
        // Storage append succeeded. Three things happen in this phase,
        // all of which would have been wrong to do in Phase 2:
        //
        // 1. Advance the gate's high-water mark so future deliveries of
        //    this sequence are dropped as duplicates.
        // 2. Apply the same grouping logic the peek used, but mutating
        //    the processor's pending state (`commit_detection`). The
        //    return value is intentionally discarded: the crossings
        //    already went into the batch via the Phase 2 peek, and in
        //    single-threaded use (this mutex guarantees that) the two
        //    calls return identical Vecs. A `debug_assert_eq` would be
        //    appealing but would require deriving `PartialEq` on
        //    `Crossing` across the workspace; we accept the contract on
        //    `CrossingProcessor::commit_detection` instead.
        // 3. Meter any gap we observed in Phase 1. Deferring the metric
        //    until after the append guarantees each true gap counts
        //    exactly once: a transient storage failure that forces the
        //    producer to retry won't re-meter the same gap on the
        //    second peek.
        if let Some(OtkEvent::Detection(det)) = batch.first() {
            self.gate.commit(producer_id, det);
            let _ = self
                .processor
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .commit_detection(det.clone());
            if let Some((expected, got)) = pending_gap {
                warn!(
                    producer = %producer_id,
                    detector = %det.detector_id,
                    expected_seq = expected,
                    got_seq = got,
                    gap = got - expected,
                    "sequence gap observed; event persisted"
                );
                self.metrics.sequence_gaps.incr(&[
                    ("producer_id", producer_id),
                    ("detector_id", det.detector_id.as_str()),
                ]);
            }
        }

        // Detection is at index 0; everything after is a Crossing.
        self.metrics
            .events_appended
            .incr(&[("producer_id", producer_id), ("event_kind", event_kind)]);
        for c in batch.iter().skip(1) {
            if let OtkEvent::Crossing(ce) = c {
                self.metrics
                    .events_appended
                    .incr(&[("producer_id", "<timing-core>"), ("event_kind", "Crossing")]);
                info!(
                    crossing_id = %ce.crossing_id,
                    timing_point = %ce.timing_point_id,
                    subject_id = ce.subject_id.as_ref().map(|s| s.as_str()),
                    crossed_at_ns = ce.crossed_at_ns,
                    detections = ce.detection_ids.len(),
                    "crossing committed"
                );
            }
        }
        Ok(AppendOutcome::Appended(offset))
    }

    /// Returns a clone of the log handle for use in tests or subscriptions.
    pub fn log(&self) -> Arc<tokio::sync::Mutex<Box<dyn EventLog>>> {
        Arc::clone(&self.log)
    }
}

fn event_kind_label(event: &OtkEvent) -> &'static str {
    match event {
        OtkEvent::Detection(_) => "Detection",
        OtkEvent::DetectorHealth(_) => "DetectorHealth",
        OtkEvent::TimebaseStatus(_) => "TimebaseStatus",
        OtkEvent::AdapterMetadata(_) => "AdapterMetadata",
        OtkEvent::Crossing(_) => "Crossing",
    }
}

fn map_crossing(c: &Crossing) -> CrossingEvent {
    CrossingEvent {
        crossing_id: CrossingId::new(c.crossing_id.as_str()),
        timing_point_id: c.timing_point_id.clone(),
        subject_id: c.subject_id.clone(),
        crossed_at_ns: c.crossed_at_ns,
        crossed_at_uncertainty_ns: c.crossed_at_uncertainty_ns,
        timebase_id: c.timebase_id.clone(),
        timestamping_method: c.timestamping_method,
        source_attestation: c.source_attestation,
        detection_ids: c.detection_ids.clone(),
    }
}

fn map_storage_err(e: StorageError) -> QueryError {
    match e {
        StorageError::RetentionExpired {
            requested,
            earliest_available,
        } => QueryError::RetentionExpired {
            requested: requested.as_u64(),
            earliest_available: earliest_available.map(|o| o.as_u64()),
        },
        other => QueryError::Internal(other.to_string()),
    }
}

#[async_trait]
impl EventQueryPort for NodePipeline {
    async fn latest_offset(&self) -> Result<Option<u64>, QueryError> {
        let mut log = self.log.lock().await;
        Ok(log
            .latest_offset()
            .await
            .map_err(map_storage_err)?
            .map(|o| o.as_u64()))
    }

    async fn read_events(&self, from: u64, limit: usize) -> Result<EventPage, QueryError> {
        let mut log = self.log.lock().await;
        let latest = log
            .latest_offset()
            .await
            .map_err(map_storage_err)?
            .map(|o| o.as_u64());
        let to = Offset::new(from.saturating_add(limit as u64));
        let entries = log
            .read_range(Offset::new(from), Some(to))
            .await
            .map_err(map_storage_err)?;
        Ok(EventPage {
            entries: entries
                .into_iter()
                .map(|e| EventEntry {
                    offset: e.offset.as_u64(),
                    event: e.event,
                })
                .collect(),
            latest_offset: latest,
        })
    }

    async fn subscribe_events(&self, from: u64) -> Result<EventStream, QueryError> {
        let sub = self
            .log
            .lock()
            .await
            .subscribe(Offset::new(from))
            .await
            .map_err(map_storage_err)?;
        let s = stream::unfold(sub, |mut sub| async move {
            match sub.next_entry().await {
                Some(Ok(entry)) => Some((
                    Ok(EventEntry {
                        offset: entry.offset.as_u64(),
                        event: entry.event,
                    }),
                    sub,
                )),
                Some(Err(e)) => Some((Err(map_storage_err(e)), sub)),
                None => None,
            }
        });
        Ok(Box::pin(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
    use event_model::{
        Detection, DetectionId, DetectorId, SensorData, SourceAttestation, SubjectId, TimebaseId,
        TimestampingMethod, TimingPointId,
    };

    async fn pipeline_in(tmp: &tempfile::TempDir) -> NodePipeline {
        let log_config = SegmentLogConfig {
            dir: tmp.path().to_path_buf(),
            ..SegmentLogConfig::default()
        };
        let log = SegmentLog::open(log_config).await.expect("open log");
        let metrics = Arc::new(Metrics::new());
        let gate = Arc::new(SequenceGate::new());
        NodePipeline::new(Box::new(log), metrics, gate)
    }

    fn make_detection() -> Detection {
        Detection {
            detection_id: DetectionId::new("d-1"),
            detector_id: DetectorId::new("loop-1"),
            timing_point_id: TimingPointId::new("tp-start"),
            subject_id: Some(SubjectId::new("bib-42")),
            detected_at_ns: 1_700_000_000_000_000_000,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("gps-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 1,
            sensor: SensorData::LoopTransponder {
                rssi_dbm: Some(-60),
                pulse_count: None,
            },
        }
    }

    fn make_detection_with_seq(seq: u64) -> Detection {
        let mut d = make_detection();
        d.sequence_number = seq;
        d.detection_id = DetectionId::new(format!("d-{seq}"));
        d
    }

    #[tokio::test]
    async fn append_detection_increments_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        let outcome = pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(1)))
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended(o) if o == Offset::new(0)));
    }

    #[tokio::test]
    async fn second_append_returns_next_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(1)))
            .await
            .unwrap();
        let outcome = pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(2)))
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended(o) if o == Offset::new(1)));
    }

    #[tokio::test]
    async fn duplicate_sequence_is_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(5)))
            .await
            .unwrap();
        let outcome = pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(5)))
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::DroppedDuplicate));
        assert_eq!(
            pipeline.latest_offset().await.unwrap(),
            Some(0),
            "log must not grow on duplicate"
        );
    }

    #[tokio::test]
    async fn gap_is_accepted_and_log_grows() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(1)))
            .await
            .unwrap();
        // Skip seq 2, jump to 5; gate logs the gap but accepts.
        let outcome = pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(5)))
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended(_)));
        assert_eq!(pipeline.latest_offset().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn latest_offset_reflects_appends() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        assert!(pipeline.latest_offset().await.unwrap().is_none());
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(1)))
            .await
            .unwrap();
        assert_eq!(pipeline.latest_offset().await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn read_events_returns_appended() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = pipeline_in(&tmp).await;
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(1)))
            .await
            .unwrap();
        pipeline
            .append_event("p", OtkEvent::Detection(make_detection_with_seq(2)))
            .await
            .unwrap();
        let page = pipeline.read_events(0, 10).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].offset, 0);
        assert_eq!(page.entries[1].offset, 1);
    }
}
