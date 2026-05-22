//! Minimal Prometheus-text-format metrics for `otk-node`.
//!
//! Hand-rolled to avoid pulling in the full `prometheus`/`metrics-exporter-prometheus`
//! dependency tree for a small surface. If/when histograms or higher-cardinality
//! labels become a real need, swap to a proper client library.
//!
//! ## Exposed metrics
//!
//! | Name | Type | Labels |
//! |---|---|---|
//! | `otk_events_appended_total` | counter | `producer_id`, `event_kind` |
//! | `otk_events_dropped_duplicates_total` | counter | `producer_id`, `detector_id` |
//! | `otk_sequence_gaps_total` | counter | `producer_id`, `detector_id` |
//! | `otk_ingest_sessions_active` | gauge | `listener_id` |
//! | `otk_ingest_sessions_total` | counter | `listener_id` |
//!
//! ## Cardinality-cap overflow series
//!
//! Each labelled metric also exposes a sibling **`<basename>_overflow`**
//! (or `<basename>_overflow_total` if the parent name already ends in
//! `_total`) that counts label-sets dropped after the per-metric
//! cardinality cap was hit. The overflow series is a distinct metric
//! name with **no labels**, rather than the same metric name with a
//! `{series="_overflow"}` label, so all series within a metric family
//! share the same label-key schema (a Prometheus best practice that
//! prom-tooling validators enforce).
//!
//! Example: `otk_events_appended_total{producer_id, event_kind}` has
//! companion `otk_events_appended_overflow_total` (no labels).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::RwLock;

/// All runtime metrics, shared via `Arc<Metrics>`.
#[derive(Default)]
pub struct Metrics {
    pub events_appended: LabeledCounter,
    pub events_dropped_duplicates: LabeledCounter,
    pub sequence_gaps: LabeledCounter,
    pub ingest_sessions_active: LabeledGauge,
    pub ingest_sessions_total: LabeledCounter,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Render every metric in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.events_appended.render(
            &mut out,
            "otk_events_appended_total",
            "counter",
            "Events appended to the event log.",
        );
        self.events_dropped_duplicates.render(
            &mut out,
            "otk_events_dropped_duplicates_total",
            "counter",
            "Detections rejected by the sequence gate as duplicates.",
        );
        self.sequence_gaps.render(
            &mut out,
            "otk_sequence_gaps_total",
            "counter",
            "Detection sequence-number gaps observed by the sequence gate.",
        );
        self.ingest_sessions_active.render(
            &mut out,
            "otk_ingest_sessions_active",
            "gauge",
            "Currently-connected ingest sessions per listener.",
        );
        self.ingest_sessions_total.render(
            &mut out,
            "otk_ingest_sessions_total",
            "counter",
            "Total ingest sessions accepted per listener since start.",
        );
        out
    }
}

/// Labelled counter; lazily allocates an `AtomicU64` per label-set.
#[derive(Default)]
pub struct LabeledCounter {
    series: RwLock<HashMap<Vec<(String, String)>, AtomicU64>>,
    overflow: AtomicU64,
}

/// Cardinality cap per labelled counter / gauge. Several of the labels
/// in this crate (`producer_id`, `detector_id`) come from ingest input,
/// so a misconfigured or malicious producer that sprays unique IDs
/// would otherwise force unbounded growth of the metrics map (a real
/// DoS / memory-pressure vector).
///
/// Once a counter has this many distinct series, further new label-sets
/// are not stored; they're rolled into the counter's own `overflow`
/// bucket and a warn-log fires the first time the cap is hit. Existing
/// series continue to increment normally so already-seen producers
/// don't lose telemetry.
///
/// 10_000 is generous enough that legitimate deployments (dozens of
/// producers × dozens of detectors) never hit it, and tight enough to
/// bound worst-case memory at ~a few MB even if the labels are long.
const MAX_SERIES_PER_METRIC: usize = 10_000;

/// Build a canonical `Vec<(String, String)>` key from caller-provided labels.
///
/// Prometheus treats label sets as unordered: `{a="1",b="2"}` and
/// `{b="2",a="1"}` are the same series. The internal map uses the key
/// directly, so two `incr` calls with the same labels in different
/// order would otherwise spawn two distinct series and silently split
/// the counter. Sorting by label name makes the key canonical and
/// matches the wire-format equivalence Prometheus assumes.
///
/// Sorting is stable on `&str` and we only do it once per call (the
/// hot fast path is the read-lock lookup); the cost is negligible
/// against the existing allocation.
fn canonical_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut owned: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    owned.sort_by(|a, b| a.0.cmp(&b.0));
    owned
}

impl LabeledCounter {
    pub fn incr(&self, labels: &[(&str, &str)]) {
        let key = canonical_labels(labels);
        if let Some(c) = self.series.read().unwrap().get(&key) {
            c.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut w = self.series.write().unwrap();
        // Re-check under the write lock in case another thread inserted
        // between our read and write.
        if let Some(c) = w.get(&key) {
            c.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if w.len() >= MAX_SERIES_PER_METRIC {
            // Cap reached: bump the overflow bucket instead of growing
            // the map further. Log a warning the first time we cross
            // the cap so the operator notices the cardinality issue.
            let prev = self.overflow.fetch_add(1, Ordering::Relaxed);
            if prev == 0 {
                tracing::warn!(
                    cap = MAX_SERIES_PER_METRIC,
                    "metric cardinality cap reached; further new label-sets folded into _overflow series"
                );
            }
            return;
        }
        w.entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, kind: &str, help: &str) {
        let map = self.series.read().unwrap();
        out.push_str(&format!("# HELP {name} {help}\n"));
        out.push_str(&format!("# TYPE {name} {kind}\n"));
        let overflow = self.overflow.load(Ordering::Relaxed);
        if map.is_empty() && overflow == 0 {
            out.push_str(&format!("{name} 0\n"));
        } else {
            for (labels, counter) in map.iter() {
                // Counters MUST render as u64. The previous code cast to
                // i64 and would wrap to a negative value if a counter ever
                // crossed i64::MAX (a 64-bit Prometheus counter sample is
                // supposed to be a non-negative integer; a negative value
                // is "invalid counter" per the exposition format and
                // breaks scrapers).
                out.push_str(&render_series(
                    name,
                    labels,
                    &counter.load(Ordering::Relaxed),
                ));
            }
        }
        // Cardinality-cap overflow is rendered as a SEPARATE metric name
        // (`<name>_overflow_total` for counters, since the parent already
        // ends in `_total` per Prometheus naming), not as the parent
        // metric with a `{series="_overflow"}` label. Mixing label-key
        // schemas within a single metric family violates the exposition
        // spec and trips prom-tooling validators; a sibling metric keeps
        // each family's label schema consistent.
        let overflow_name = overflow_metric_name(name);
        out.push_str(&format!(
            "# HELP {overflow_name} Label-sets dropped because {name} reached the per-metric cardinality cap of {MAX_SERIES_PER_METRIC}.\n"
        ));
        out.push_str(&format!("# TYPE {overflow_name} counter\n"));
        out.push_str(&format!("{overflow_name} {overflow}\n"));
    }
}

/// Labelled gauge; supports +/- increments per label-set.
#[derive(Default)]
pub struct LabeledGauge {
    series: RwLock<HashMap<Vec<(String, String)>, AtomicI64>>,
    overflow: AtomicI64,
}

impl LabeledGauge {
    pub fn add(&self, delta: i64, labels: &[(&str, &str)]) {
        let key = canonical_labels(labels);
        if let Some(g) = self.series.read().unwrap().get(&key) {
            g.fetch_add(delta, Ordering::Relaxed);
            return;
        }
        let mut w = self.series.write().unwrap();
        if let Some(g) = w.get(&key) {
            g.fetch_add(delta, Ordering::Relaxed);
            return;
        }
        if w.len() >= MAX_SERIES_PER_METRIC {
            let prev = self.overflow.fetch_add(delta, Ordering::Relaxed);
            if prev == 0 && delta > 0 {
                tracing::warn!(
                    cap = MAX_SERIES_PER_METRIC,
                    "gauge cardinality cap reached; further new label-sets folded into _overflow series"
                );
            }
            return;
        }
        w.entry(key)
            .or_insert_with(|| AtomicI64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }

    pub fn inc(&self, labels: &[(&str, &str)]) {
        self.add(1, labels);
    }

    pub fn dec(&self, labels: &[(&str, &str)]) {
        self.add(-1, labels);
    }

    fn render(&self, out: &mut String, name: &str, kind: &str, help: &str) {
        let map = self.series.read().unwrap();
        out.push_str(&format!("# HELP {name} {help}\n"));
        out.push_str(&format!("# TYPE {name} {kind}\n"));
        let overflow = self.overflow.load(Ordering::Relaxed);
        if map.is_empty() && overflow == 0 {
            out.push_str(&format!("{name} 0\n"));
        } else {
            for (labels, gauge) in map.iter() {
                out.push_str(&render_series(name, labels, &gauge.load(Ordering::Relaxed)));
            }
        }
        // See LabeledCounter::render for the rationale. Gauges don't
        // need a `_total` suffix, so the overflow sibling is just
        // `<name>_overflow`. Typed as `gauge` because the underlying
        // value sums signed deltas (a `dec()` against a label-set that
        // never made it into the map would otherwise be silently lost,
        // so the overflow rolls in the same direction).
        let overflow_name = overflow_metric_name(name);
        out.push_str(&format!(
            "# HELP {overflow_name} Net delta sum for label-sets dropped because {name} reached the per-metric cardinality cap of {MAX_SERIES_PER_METRIC}.\n"
        ));
        out.push_str(&format!("# TYPE {overflow_name} gauge\n"));
        out.push_str(&format!("{overflow_name} {overflow}\n"));
    }
}

/// Build the sibling overflow metric name for `name`.
///
/// Counter names per Prometheus convention end in `_total`. To preserve
/// that convention on the overflow companion, replace the trailing
/// `_total` with `_overflow_total`. Names without `_total` (gauges) just
/// gain a `_overflow` suffix. Result:
///   `otk_events_appended_total`  -> `otk_events_appended_overflow_total`
///   `otk_ingest_sessions_active` -> `otk_ingest_sessions_active_overflow`
fn overflow_metric_name(name: &str) -> String {
    match name.strip_suffix("_total") {
        Some(stem) => format!("{stem}_overflow_total"),
        None => format!("{name}_overflow"),
    }
}

/// Render a single Prometheus exposition line for `name` with the given
/// `labels` and `value`.
///
/// `value` is passed as `&dyn Display` so counters can render natively as
/// `u64` (no lossy `as i64` cast that would wrap negative once a counter
/// crosses `i64::MAX`) while gauges render natively as `i64`. Either type
/// satisfies the trait via its standard `Display` impl, so the caller
/// just passes a reference to the appropriate atomic-load value.
fn render_series(name: &str, labels: &[(String, String)], value: &dyn std::fmt::Display) -> String {
    if labels.is_empty() {
        return format!("{name} {value}\n");
    }
    let label_str = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}{{{label_str}}} {value}\n")
}

/// Escape a label value for the Prometheus text exposition format.
///
/// Per the format spec, label values are enclosed in double-quotes and
/// must escape backslash (`\\`), double-quote (`\"`), newline (`\n`),
/// and carriage return (`\r`). Missing the `\r` case (as an earlier
/// version did) is not just a cosmetic bug: a producer-supplied label
/// such as `producer_id` containing a stray `\r` would emit a literal
/// CR mid-line, which most scrapers reject as malformed exposition and
/// drop the entire scrape, taking out unrelated metrics with it.
fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_renders_with_labels() {
        let m = Metrics::new();
        m.events_appended
            .incr(&[("producer_id", "p1"), ("event_kind", "Detection")]);
        m.events_appended
            .incr(&[("producer_id", "p1"), ("event_kind", "Detection")]);
        m.events_appended
            .incr(&[("producer_id", "p2"), ("event_kind", "Crossing")]);
        let text = m.render();
        assert!(text.contains("# TYPE otk_events_appended_total counter"));
        // Labels are emitted in canonical (sorted-by-name) order, so
        // `event_kind` comes before `producer_id` regardless of how the
        // caller listed them. The covering test `label_order_is_canonical`
        // exercises the equivalence directly.
        assert!(
            text.contains(
                "otk_events_appended_total{event_kind=\"Detection\",producer_id=\"p1\"} 2"
            ),
            "rendered:\n{text}"
        );
        assert!(text
            .contains("otk_events_appended_total{event_kind=\"Crossing\",producer_id=\"p2\"} 1"));
    }

    #[test]
    fn gauge_increments_and_decrements() {
        let m = Metrics::new();
        m.ingest_sessions_active.inc(&[("listener_id", "tcp-main")]);
        m.ingest_sessions_active.inc(&[("listener_id", "tcp-main")]);
        m.ingest_sessions_active.dec(&[("listener_id", "tcp-main")]);
        let text = m.render();
        assert!(text.contains("otk_ingest_sessions_active{listener_id=\"tcp-main\"} 1"));
    }

    #[test]
    fn empty_metric_renders_zero() {
        let m = Metrics::new();
        let text = m.render();
        assert!(text.contains("otk_events_appended_total 0"));
        assert!(text.contains("otk_ingest_sessions_active 0"));
        // The sibling overflow metric is always exported (zero when
        // no overflow has occurred). Counters with a `_total` suffix
        // get the `_overflow_total` companion; gauges without `_total`
        // get a plain `_overflow`.
        assert!(text.contains("otk_events_appended_overflow_total 0"));
        assert!(text.contains("otk_ingest_sessions_active_overflow 0"));
    }

    #[test]
    fn overflow_is_separate_metric_name_not_a_label() {
        // Cardinality overflow must NOT mix label-key schemas within
        // the parent metric family. Driving the parent past the cap
        // and asserting:
        //   - parent metric never gets a `{series="_overflow"}` label
        //   - the overflow shows up under a dedicated sibling metric
        //     name with no labels
        //   - the sibling carries its own # HELP and # TYPE lines
        let counter = LabeledCounter::default();
        // Fill exactly to the cap.
        for i in 0..MAX_SERIES_PER_METRIC {
            let producer = format!("p{i}");
            counter.incr(&[("producer_id", producer.as_str())]);
        }
        // Two more attempts should land in the overflow bucket.
        counter.incr(&[("producer_id", "p_overflow_a")]);
        counter.incr(&[("producer_id", "p_overflow_b")]);
        let mut out = String::new();
        counter.render(&mut out, "otk_test_total", "counter", "test");
        assert!(
            !out.contains("series=\"_overflow\""),
            "must not use sentinel label:\n{out}"
        );
        assert!(
            out.contains("# HELP otk_test_overflow_total"),
            "missing HELP:\n{out}"
        );
        assert!(
            out.contains("# TYPE otk_test_overflow_total counter"),
            "missing TYPE:\n{out}"
        );
        assert!(
            out.contains("otk_test_overflow_total 2"),
            "wrong overflow count:\n{out}"
        );
    }

    #[test]
    fn overflow_metric_name_appends_or_splices_total() {
        assert_eq!(
            overflow_metric_name("otk_events_appended_total"),
            "otk_events_appended_overflow_total"
        );
        assert_eq!(
            overflow_metric_name("otk_ingest_sessions_active"),
            "otk_ingest_sessions_active_overflow"
        );
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::new();
        m.events_appended.incr(&[("producer_id", r#"weird"name"#)]);
        let text = m.render();
        assert!(
            text.contains(r#"producer_id="weird\"name""#),
            "got:\n{text}"
        );
    }

    #[test]
    fn label_order_is_canonical() {
        // Prometheus treats {a="1",b="2"} and {b="2",a="1"} as the same
        // series, so both call orderings must hit a single counter.
        let m = Metrics::new();
        m.events_appended
            .incr(&[("producer_id", "p1"), ("event_kind", "Detection")]);
        m.events_appended
            .incr(&[("event_kind", "Detection"), ("producer_id", "p1")]);
        let text = m.render();
        // The rendered series is canonical (sorted by label name), and
        // it shows a count of 2 because both increments folded into one
        // series despite the caller passing labels in different orders.
        assert!(
            text.contains(
                "otk_events_appended_total{event_kind=\"Detection\",producer_id=\"p1\"} 2"
            ),
            "expected canonical-ordered single series with count 2:\n{text}"
        );
    }

    #[test]
    fn counter_does_not_wrap_negative_above_i64_max() {
        // Counters render as u64. The previous version cast to i64 and
        // would wrap to a negative number once a counter crossed i64::MAX.
        // Simulate that with a direct atomic store; cheap and avoids
        // calling incr() one billion times.
        let m = Metrics::new();
        let labels: Vec<(String, String)> = vec![
            ("producer_id".into(), "p1".into()),
            ("event_kind".into(), "Detection".into()),
        ];
        // Pre-seed the series via a normal incr so the entry exists, then
        // crank the underlying atomic past i64::MAX.
        m.events_appended
            .incr(&[("producer_id", "p1"), ("event_kind", "Detection")]);
        {
            let canonical = canonical_labels(&[("producer_id", "p1"), ("event_kind", "Detection")]);
            let map = m.events_appended.series.read().unwrap();
            let atom = map.get(&canonical).expect("series exists");
            atom.store(u64::MAX - 1, Ordering::Relaxed);
            let _ = labels;
        }
        let text = m.render();
        assert!(
            text.contains(&format!("{}", u64::MAX - 1)),
            "expected counter rendered as {} but got:\n{text}",
            u64::MAX - 1
        );
        assert!(
            !text.contains(" -"),
            "rendered exposition must not contain negative counter values:\n{text}"
        );
    }

    #[test]
    fn label_values_escape_cr_lf_and_backslash() {
        // Per Prometheus text format: backslash, double-quote, LF, and CR
        // must all be escaped. Earlier versions missed CR, which let a
        // malformed producer_id break an entire scrape.
        let m = Metrics::new();
        m.events_appended
            .incr(&[("producer_id", "carriage\rreturn\nand\\back")]);
        let text = m.render();
        assert!(
            text.contains(r#"producer_id="carriage\rreturn\nand\\back""#),
            "got:\n{text}"
        );
    }
}
