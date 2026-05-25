# timing-core

The OTK hexagon. One library crate that owns the domain model, the
domain engines, the application services that drive ingest end-to-end,
**and** the inbound/outbound port traits adapters compile against.

> **Status: active.**

## What this is

`timing-core` is the hexagonal **core** of the OTK runtime. It owns
four sibling modules:

- **`domain/`** : pure domain. [`Crossing`] (the derived passage
  record), [`CrossingProcessor`] (detection-to-crossing grouping),
  [`SequenceGate`] (per-`(producer, detector)` sequence monotonicity,
  with restart-resume via [`seed_from_log`] / [`seed_from_log_box`]),
  and [`ProcessorConfig`]. Other domain primitives (`Detection`,
  `SubjectId`, `TimingPointId`, etc.) are imported from `event-model`,
  the wire-schema crate, until a future split separates wire from
  domain types.
- **`ports/inbound/`** : driving ports the core implements.
  [`EventIngestPort`] (every transport adapter implements this) and
  [`EventQueryPort`] (the REST/SSE API depends on this).
- **`ports/outbound/`** : driven ports the core consumes.
  [`EventLog`] (storage) and [`IngestMetrics`] (telemetry). The
  composition root injects concrete implementations of both.
- **`services/`** : application services. At v0 there is one,
  [`EventIngestService`], which implements `EventQueryPort` and takes
  the outbound ports as constructor arguments.

`timing-core` is a library, not a server. The v0 composition root that
deploys it is [`timing-node`](../timing-node), but the same library is
usable from any host that wants to apply Open Timekeeping timing logic
to a stream of canonical events: an offline analyzer, a re-projection
tool, a conformance harness.

## Hexagonal shape

| Direction | Port (in `ports/...`) | Who implements | How it's used |
|---|---|---|---|
| Inbound (driving) | `EventIngestPort` (`ports::inbound::ingest`) | per-transport ingest adapters | `timing-node`'s listener loop calls `accept` and routes the resulting `IngestSession`s into `EventIngestService::append_event`. |
| Inbound (driving) | `EventQueryPort` (`ports::inbound::query`) | `EventIngestService` | The REST/SSE API layer in `timing-node` depends on the trait, not on the service type. |
| Outbound (driven) | `EventLog` (`ports::outbound::event_log`) | storage adapters | Taken by `EventIngestService::new`. The v0 backend is [`adapter-event-log-segment`](../adapter-event-log-segment); alternatives plug in behind the same trait. |
| Outbound (driven) | `IngestMetrics` (`ports::outbound::metrics`) | `timing-node`'s Prometheus `Metrics` | Taken by `EventIngestService::new`. Tests use [`NoopIngestMetrics`] from this crate. |

### Adapter boundary

Adapter crates and `conformance` depend on `timing-core` as a whole,
but a per-crate `clippy.toml` denies imports of
`timing_core::domain::*` and `timing_core::services::*`. The runtime
composition root (`timing-node`) is the only crate in the workspace
that touches every layer (domain + services + ports).

Earlier revisions of this codebase carried each port trait in its own
crate (`port-in-ingest`, `port-in-query`, `port-out-event-log`), so
the boundary was enforced by Cargo's dependency graph: an adapter
literally could not see `timing-core`'s domain types. Folding the
ports into `timing-core` reduced ceremony (one crate to publish, four
fewer `Cargo.toml`s) at the cost of trading dep-graph enforcement for
the convention-enforced clippy fence. The shape of the property is
unchanged.

## Domain scope

`timing-core` knows about **subjects**, **timing points**,
**detections**, and **crossings**. It does not know about laps, races,
flags, or any sport-specific structure. The domain is precision timing
in general; sport-specific application logic is built on top of this
library.

## What belongs here (v0)

- Detection-to-crossing grouping (configurable time window,
  peak-signal selection, deduplication).
- `CrossingProcessor`: the streaming stateful engine that accepts
  detections and emits crossings via a `peek_detection` /
  `commit_detection` split so the caller can persist before advancing
  state.
- `Crossing`: the primary derived timing record.
- `SequenceGate`: per-`(producer_id, detector_id)` sequence-number
  enforcement, with `seed_from_log` for restart resume.
- `EventIngestService`: the application service that drives ingest
  end-to-end (peek-gate → peek-processor → append → commit-gate →
  commit-processor → record metrics → return outcome).
- The four port traits adapters and the API layer compile against.

## What is deferred (future work)

- Out-of-order detection handling and late-arrival corrections.
- Amendment events (operator corrections, re-projection).
- Crossing to higher-level structures (those belong to
  application-layer code).
- A separate domain crate that owns `Detection`, `Subject`,
  `TimingPoint` etc. with explicit wire ↔ domain mapping. Today
  `event-model` is both wire schema and de facto domain primitives.

## What does not belong here

- Raw device parsing or chipset code (adapter repos).
- Serial / USB / Ethernet I/O (adapter repos or `timing-node`).
- Server / API / lifecycle / process supervision (`timing-node`).
- UI / dashboard code (future per-app repos).
- Concrete storage backends (`adapter-event-log-*`).

## Design

### Detection grouping

Multiple detections from the same subject at the same timing point
that arrive within `ProcessorConfig::grouping_window_ns` (default: 2
seconds) of the first detection in a group are merged into one
`Crossing`. Grouping key: `(timing_point_id, subject_id)`.

Detections with no `subject_id` (anonymous, e.g. beam breaks that do
not identify the entrant) are never grouped. Each produces a
`Crossing` immediately on arrival.

### Peak-signal selection

When a group has more than one detection, the crossing timestamp is
chosen from the "peak" detection:

- **Loop transponder with RSSI present**: the detection with the
  highest `rssi_dbm` (strongest signal).
- **All other cases** (beam break, manual entry, loop with no RSSI):
  the detection with the earliest `detected_at_ns`.

The `crossed_at_uncertainty_ns` is widened to cover the full span of
the group: `max(peak_uncertainty, last_detected_at_ns - first_detected_at_ns)`.

### Sequence gate and restart resume

[`SequenceGate`] is an in-memory map of per-`(producer_id, detector_id)`
high-water marks. The application service consults it on every
`Detection` and drops duplicates (sequence ≤ high-water) without
persisting. Gaps (sequence > high-water + 1) are observed but
accepted; the gate cannot tell the difference between a genuine
producer error and a transient outage at the source, so it leaves
that decision to the operator alerting on the metric.

The gate is rebuilt from the persisted log at process start via
[`seed_from_log`], so a producer reconnecting with the same
`producer_id` after a node restart cannot replay an acknowledged
sequence.

### Engine API

```rust
use timing_core::{CrossingProcessor, ProcessorConfig};

let mut processor = CrossingProcessor::new(ProcessorConfig::default());

// Feed detections as they arrive. The peek/commit split lets the
// caller persist the crossings before advancing processor state:
for detection in incoming {
    let crossings = processor.peek_detection(&detection);
    // ... persist `crossings` (and `detection`) downstream ...
    // On success, advance the processor's grouping window:
    processor.commit_detection(detection);
}

// Commit any remaining groups at end of session
for crossing in processor.flush() {
    // handle final crossings
}
```

### Service wiring

```rust,ignore
use std::sync::Arc;
use timing_core::{
    EventIngestService, IngestMetrics, NoopIngestMetrics,
    ProcessorConfig, SequenceGate, seed_from_log_box,
    ports::outbound::EventLog,
};

async fn wire(mut log: Box<dyn EventLog>) -> Arc<EventIngestService> {
    let gate = Arc::new(SequenceGate::new());
    seed_from_log_box(&gate, &mut log).await.unwrap();

    let metrics: Arc<dyn IngestMetrics> = Arc::new(NoopIngestMetrics);
    Arc::new(EventIngestService::new(
        log,
        ProcessorConfig::default(),
        gate,
        metrics,
    ))
}
```

The real composition root in [`timing-node`](../timing-node) does the
same thing with a Prometheus-text `Metrics` impl instead of
`NoopIngestMetrics`.

## Dependencies

**Depends on:** [`event-model`](../event-model). Plus `tokio` (sync
primitives), `tracing`, `async-trait`, `futures-util`, `serde`,
`thiserror`, `uuid`. No adapter or runtime crate is in the dep graph;
that direction is asymmetric on purpose.

**Commonly depended on by:** every adapter crate
([`adapter-ingest-tcp`](../adapter-ingest-tcp),
[`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket),
[`adapter-event-log-segment`](../adapter-event-log-segment)),
[`timing-node`](../timing-node) (the composition root), and
[`conformance`](../conformance).

## License

Apache-2.0. See [`LICENSE`](./LICENSE).

[`Crossing`]: ./src/domain/crossing.rs
[`CrossingId`]: ./src/domain/crossing.rs
[`CrossingProcessor`]: ./src/domain/crossing_processor.rs
[`ProcessorConfig`]: ./src/domain/processor_config.rs
[`SequenceGate`]: ./src/domain/sequence_gate.rs
[`seed_from_log`]: ./src/domain/sequence_gate.rs
[`seed_from_log_box`]: ./src/domain/sequence_gate.rs
[`EventIngestPort`]: ./src/ports/inbound/ingest.rs
[`EventQueryPort`]: ./src/ports/inbound/query.rs
[`EventLog`]: ./src/ports/outbound/event_log.rs
[`IngestMetrics`]: ./src/ports/outbound/metrics.rs
[`NoopIngestMetrics`]: ./src/ports/outbound/metrics.rs
[`EventIngestService`]: ./src/services/event_ingest.rs
