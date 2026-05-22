# timing-core

The timing-domain engine. Converts raw [`Detection`](../event-model) events into `Crossing` events.

> **Status: active.**

## What this is

A pure library that takes raw canonical detector events and turns them into timing-domain meaning. Its v0 scope is detection-to-crossing grouping: the process of merging multiple detections for the same subject at the same timing point (from redundant sensors, multi-loop readers, etc.) into a single authoritative passage record.

`timing-core` is a library, not a server. It is consumed by [`timing-node`](../timing-node), but is reusable from any host that wants to apply Open Timekeeping timing logic to a stream of canonical events: an offline analyzer, a test harness, a re-projection tool.

## Domain scope

`timing-core` knows about **subjects**, **timing points**, **detections**, and **crossings**. It does not know about laps, races, flags, or any sport-specific structure. The domain is precision timing in general; sport-specific application logic is built on top of this library.

## What belongs here (v0)

- Detection to crossing grouping (configurable time window, peak-signal selection, deduplication).
- `CrossingProcessor`: the streaming stateful engine that accepts detections and emits crossings.
- `Crossing`: the primary derived timing record.

## What is deferred (future work)

- Out-of-order detection handling and late-arrival corrections.
- Amendment events (operator corrections, re-projection).
- Crossing to higher-level structures (those belong to application-layer code).

## What does not belong here

- Raw device parsing or chipset code (adapter repos).
- Serial / USB / Ethernet I/O (adapter repos or `timing-node`).
- Server / API / lifecycle (`timing-node`).
- UI / dashboard code (`app-live-timing`, `app-diagnostics`).
- Storage backends (`storage-*`).

## Design

### Detection grouping

Multiple detections from the same subject at the same timing point that arrive within `ProcessorConfig::grouping_window_ns` (default: 2 seconds) of the first detection in a group are merged into one `Crossing`. Grouping key: `(timing_point_id, subject_id)`.

Detections with no `subject_id` (anonymous, e.g. beam breaks that do not identify the entrant) are never grouped. Each produces a `Crossing` immediately on arrival.

### Peak-signal selection

When a group has more than one detection, the crossing timestamp is chosen from the "peak" detection:

- **Loop transponder with RSSI present**: the detection with the highest `rssi_dbm` (strongest signal).
- **All other cases** (beam break, manual entry, loop with no RSSI): the detection with the earliest `detected_at_ns`.

The `crossed_at_uncertainty_ns` is widened to cover the full span of the group: `max(peak_uncertainty, last_detected_at_ns - first_detected_at_ns)`.

### Processor API

```rust
use timing_core::{CrossingProcessor, ProcessorConfig};

let mut processor = CrossingProcessor::new(ProcessorConfig::default());

// Feed detections as they arrive; handle any immediately-committed crossings
for detection in incoming {
    for crossing in processor.push_detection(detection) {
        // crossing committed (old group displaced by out-of-window arrival,
        // or anonymous detection)
    }
}

// At end of session: commit all remaining pending groups
for crossing in processor.flush() {
    // handle final crossings
}
```

## Dependencies

**Depends on:** [`event-model`](../event-model).

**Commonly depended on by:** [`timing-node`](../timing-node), offline analysis tools, [`conformance`](../conformance).

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
