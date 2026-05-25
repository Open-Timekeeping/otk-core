//! Outbound port for ingest-pipeline metrics.
//!
//! The application service ([`crate::services::EventIngestService`]) emits
//! three counter events as it processes inbound traffic: an append, a
//! dropped duplicate, and an observed sequence gap. The shape of those
//! events is defined here; the concrete sink (Prometheus text format,
//! OpenTelemetry, a test spy, …) is supplied by the composition root via
//! [`EventIngestService::new`](crate::services::EventIngestService::new).
//!
//! This is a small, deliberately-narrow port. It exists because:
//!
//! - the application service must not depend on a specific metrics backend
//!   (composition-root concern), and
//! - inlining the three counter calls behind an `Option<&dyn Sink>` at
//!   each call site is more friction than defining one trait once.
//!
//! The trait is not exhaustive. If a future service needs a fourth counter
//! it gets added here as a new default-implemented method so existing
//! sinks keep compiling.

/// Sink for ingest-pipeline counters.
///
/// Implementors are typically held as `Arc<dyn IngestMetrics>` and shared
/// across the runtime. Methods take label parameters as `&str` so the
/// trait stays object-safe and the impl is free to clone, intern, or
/// short-circuit on cardinality caps as it sees fit.
pub trait IngestMetrics: Send + Sync {
    /// One event persisted to the log. Called once per appended event,
    /// including any derived crossings.
    ///
    /// `event_kind` is the canonical kind label: `"Detection"`,
    /// `"Crossing"`, `"DetectorHealth"`, `"TimebaseStatus"`,
    /// `"AdapterMetadata"`. For derived crossings the service uses
    /// `producer_id = "<timing-core>"` to disambiguate them from
    /// producer-sourced events; concrete sinks should treat that string
    /// like any other producer id.
    fn record_event_appended(&self, producer_id: &str, event_kind: &str);

    /// One detection rejected by the sequence gate as a duplicate.
    fn record_duplicate_dropped(&self, producer_id: &str, detector_id: &str);

    /// One detection accepted past a sequence-number gap. The gate
    /// observes gaps but does not reject them; this counter exists so
    /// operators can alert on producers that are dropping events upstream.
    fn record_sequence_gap(&self, producer_id: &str, detector_id: &str);
}

/// No-op `IngestMetrics`. Useful in tests and when an embedder doesn't
/// care about telemetry. Constructed with `NoopIngestMetrics`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopIngestMetrics;

impl IngestMetrics for NoopIngestMetrics {
    fn record_event_appended(&self, _producer_id: &str, _event_kind: &str) {}
    fn record_duplicate_dropped(&self, _producer_id: &str, _detector_id: &str) {}
    fn record_sequence_gap(&self, _producer_id: &str, _detector_id: &str) {}
}
