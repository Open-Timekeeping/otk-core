# Downlink: timing-fabric to in-vehicle

Bidirectional Open Timekeeping: telemetry from the runtime node back
to the timed subject, and from the timed subject into the vehicle's
existing instrumentation.

> **Status: strawman.** This document is the starting point for design,
> not a settled standard. Decisions marked **(open)** still need to be
> made; decisions marked **(strawman)** are this document's
> recommendation and require user-and-team agreement before they bind
> any implementation. Issues found while implementing should round-trip
> back here.

---

## Why downlink exists

The receive-side stack ([architecture.md](architecture.md)) handles
the unambiguous part of Open Timekeeping: observe a passage, persist
it, serve it. That stack works for every timing role today.

There is a second motion that pro motorsport and many championship
running deployments rely on: **send the result back to the subject.**
Specifically, give the driver / competitor near-real-time information
about their own performance and the wider race state, so they can
react during the session rather than after it.

In motorsport this typically takes the form of a transponder that
displays (or pushes to the dashboard) the most recent lap time, the
gap to the car ahead, the gap to the car behind, the leader's pace,
and venue-level information (yellow flags, pit-window state). The
canonical commercial reference is the MyLaps X2 Pro transponder,
which combines a basic uplink transponder with a sub-GHz downlink
receiver and a small driver display.

Open Timekeeping is a full-stack system; the downlink path is in
scope for the same reasons the uplink path is. This document specifies
how it fits.

---

## Asymmetric capabilities are first-class

The bidirectional architecture must not assume every device speaks
both directions. Real deployments mix:

| Device class | Uplink | Downlink |
|---|---|---|
| Transmit-only transponder (basic, low cost) | required | none |
| Receive-only decoder (basic, low cost) | required | none |
| Two-way transponder (pro) | required | required |
| Two-way decoder (pro) | required | required |

A pro decoder may need to coexist with basic transponders (the pro
features simply don't reach them); a pro transponder may sit on a
network where only some decoders carry downlink transmitters (the pro
features work only where the venue has invested). OTK must model
uplink and downlink as **independently optional capabilities a device
advertises**, never as "you build a bidirectional system and the
unidirectional cases are degraded."

This applies recursively: a single venue may have downlink-equipped
decoders only at start-finish and at one mid-track point, with the
rest of the timing loops being uplink-only.

---

## Where downlink fits in the OTK Protocol stack

The four-layer OTK Protocol (Event Model, Wire Protocol, Frame Codec,
Transport Binding) was designed direction-agnostic. Downlink uses
the same four layers; only the transport binding is different per
direction.

**(strawman)** One OTK Protocol stack, with shared upper layers
(Event Model, Wire Protocol, Frame Codec) and per-direction transport
bindings.

- **Event Model** ([`event-model`](../event-model)) gains a new event
  family for downlink directives. The existing event types (Detection,
  Crossing, DetectorHealth, TimebaseStatus, AdapterMetadata) all
  describe **observations made by the fabric**; the new types
  describe **directives the fabric sends to its subjects** (telemetry
  payloads, race-state announcements, configuration changes).
- **Wire Protocol** ([`otk-protocol`](../otk-protocol)) gains a new
  message-type variant (`MessageType::Directive` or similar) so the
  same envelope can carry either direction. The envelope's
  `source_id` is the **producer** of the message; on uplink that's
  the detector / adapter / producer, on downlink that's the runtime
  node.
- **Frame Codec** ([`frame-codec`](../frame-codec)) is byte-shape
  invariant under direction. The same length-prefix and COBS+CRC
  variants apply.
- **Transport Binding** is per-direction. The uplink bindings
  (`adapter-ingest-tcp`, `adapter-ingest-unix-socket`, future serial)
  carry frames upstream. The downlink bindings carry frames over RF
  to receivers; their physical layer is fundamentally different.

The alternative considered and rejected was a **parallel sibling
protocol stack** (`otk-downlink-protocol`). That would have meant
duplicating envelope structure, frame codecs, and conformance
boilerplate for marginal benefit, and would have made bidirectional
devices implement two non-interoperating wire vocabularies.

### Addressing on the downlink

Uplink envelopes use `source_id` to name the producer. Downlink
envelopes need a target. **(strawman)** Reuse the envelope's existing
fields with directional semantics:

- `source_id`: the runtime node id when sending a directive (so the
  receiver can ignore directives from foreign nodes if its operator
  chose to scope it).
- A new optional `target_id` field on the envelope: present on
  directives, absent on uplink. `target_id` may be a **subject id**
  (the specific transponder / bibtag the message is for) or a
  **broadcast id** reserved for messages every receiver in the venue
  should consume (race-state, flag announcements).

**(open)** Whether `target_id` collapses with `source_id` into a
single `peer_id` field, or stays separate. Single field is simpler;
separate fields make uplink-vs-downlink intent visible without
inspecting `message_type`. This document recommends separate fields;
the trade is small.

---

## The downlink data path

```text
Uplink (today, unchanged)
========================
Sensor / detector / transponder
        |
        v
Detector adapter
        |
        v   canonical events (Detection, ...)
        v
Timing Runtime Node
   - ingest pipeline → event log → projections → APIs

Downlink (new)
==============
Timing Runtime Node
   - TelemetryService subscribes to the event log
   - Computes per-subject telemetry (gap to ahead, gap to behind,
     last lap, sector splits, leaderboard position, ...)
   - Computes venue-wide announcements (flag state, pit window,
     leader's pace, ...)
        |
        v   directives (per-subject or broadcast)
        v
TelemetryTransmitter outbound port
        |
        v
Downlink adapter (per-RF-stack)
        |
        v   OTK frames over the downlink transport binding
        v
Downlink-capable transponder
   - Receives directive
   - Decodes the targeted-or-broadcast payload
        |
        v   per OTK CAN Map (see below)
        v
Vehicle CAN bus
        |
        v
In-vehicle dashboard
   (AIM MXP / MXG / MXS, MoTeC C125 / C127, Stack ST8920, Race
    Technology DASH, OEM motorsport dash, custom)
```

The TelemetryService is a normal `timing-core` application service:
implements no inbound port itself (it's driven by the event log
subscription it owns), takes the `TelemetryTransmitter` outbound
port as a constructor argument, follows the same hexagonal pattern
as `EventIngestService`.

### What gets sent

Two flavours of downlink message:

1. **Per-subject directives** (unicast on the air): the bits of
   information that are interesting to one driver in particular.
   Lap time, gap to car ahead, gap to car behind, sector splits,
   personal best, race position. The TelemetryService computes a
   per-subject directive on each new Crossing for that subject (and
   periodically thereafter for slow updates like position changes).
2. **Venue directives** (broadcast on the air): information every
   receiver should consume regardless of identity. Flag state
   (green / yellow / red / safety car), pit-window state, leader's
   lap time, session time remaining, weather. The TelemetryService
   emits these on event triggers (race control changes a flag, the
   leader sets a new lap) or on periodic schedules.

The TelemetryService policy (which subject gets what, how often, what
the broadcast cadence is) is configurable at the composition root,
not hardcoded into the service.

### Latency budget

End-to-end target: ≤ 50 ms from physical passage to dashboard update.
Achievable with reasonable hardware. Hop budget:

| Hop | Budget |
|---|---|
| Loop antenna → detector adapter → ingest event in node | ≤ 5 ms |
| Sequence gate, crossing processor, telemetry service computation | ≤ 5 ms |
| TelemetryService → outbound port → downlink adapter | ≤ 1 ms |
| Over-the-air (sub-GHz packet at modest data rate) | 1–10 ms |
| Transponder RX → decode → emit on CAN | ≤ 10 ms |
| Dashboard reads CAN, updates display | ≤ 20 ms |

Nothing in the OTK Rust stack is a structural bottleneck; the
constraints are the radio and the dashboard, both of which are
external choices.

---

## CAN-out: bridging into in-vehicle instrumentation

**The downlink-capable transponder does not have its own display.**
It bridges the received telemetry onto the vehicle's CAN bus, where
the existing dashboard (factory-fit or aftermarket) already sits.

This is how every pro race car already works: an AIM MXG, MoTeC C127,
or equivalent reads RPM, oil pressure, fuel, lap counter and timing
splits off CAN and renders them on the dash. Adding OTK telemetry to
that view is a matter of giving the dash a CAN message map for the
OTK message family.

### OTK CAN Map

**(strawman)** OTK ships a normative CAN message map (`spec/can-map.md`,
to be drafted alongside this document or shortly after) that defines
the CAN IDs, byte layouts, scaling, and units for every standard
downlink payload. Dashboard configuration files (AIM "race studio,"
MoTeC dash manager, etc.) consume the map to display OTK telemetry
alongside the rest of the vehicle data.

The CAN map design constraints:

- **Standard CAN 2.0B (29-bit identifiers)** as the primary; classical
  CAN 2.0A (11-bit) variant for older installations.
- **CAN-FD** support optional, used only when a payload genuinely
  needs it (most telemetry messages fit comfortably in 8-byte
  classical frames).
- **Address space**: reserve a contiguous block of CAN IDs for OTK
  use. **(open)** Which block. Common motorsport practice puts
  customer telemetry in `0x600`–`0x6FF`; OTK could claim a sub-range
  there.
- **Endianness**: little-endian (Intel byte order), as standard in
  most modern motorsport CAN deployments.
- **Cycle times**: per-message-type cadence (gap updates per
  passage; flag state on change + once per second; etc.).
- **Versioning**: a single byte in a known position of every OTK CAN
  frame names the map version, so an in-car dash configured for OTK
  CAN Map v1 doesn't silently misinterpret v2 frames.

The alternative considered and rejected was **emit existing
vendor-specific dash formats** (i.e. the transponder pretends to be
an AIM data logger, a MoTeC ECU, etc.). That would have meant
maintaining per-vendor adapter logic in OTK firmware and inheriting
each vendor's IP / licensing posture. An OTK-native map is a clean
contract any dash can be configured for.

---

## Hexagonal placement

```
                    ┌──────────────────────────────────────────────────┐
                    │  timing-core                                     │
                    │  ┌────────────┐    ┌─────────────────────────┐   │
   Uplink ─────────►│  │ EventIngest│───►│ event log + subscribe   │   │
   (today)          │  │  Service   │    └────────────┬────────────┘   │
                    │  └────────────┘                 │                │
                    │                                 ▼                │
                    │                      ┌──────────────────────┐    │
                    │                      │ TelemetryService     │    │
                    │                      │  (subscribes to log) │    │
                    │                      └──────────┬───────────┘    │
                    │                                 │                │
                    │                                 ▼                │
                    │             ports::outbound::TelemetryTransmitter│
                    └────────────────────────┬─────────────────────────┘
                                             │
                                             ▼
                                ┌───────────────────────────────┐
                                │  adapter-telemetry-<radio>    │
                                │  (e.g. adapter-telemetry-sub-ghz)
                                │   - drives the venue-side RF
                                │     transmitter near each
                                │     downlink-capable decoder
                                └───────────────┬───────────────┘
                                                │
                                                ▼   OTK frames on
                                                ▼   the downlink
                                                ▼   transport binding
                                                ▼
                                ┌───────────────────────────────┐
                                │  Downlink-capable transponder │
                                │   (reference firmware in      │
                                │    reference-transponder-     │
                                │    firmware)                  │
                                │                               │
                                │   ┌─────────────────────────┐ │
                                │   │ downlink RF receiver    │ │
                                │   └───────────┬─────────────┘ │
                                │               │               │
                                │               ▼               │
                                │   ┌─────────────────────────┐ │
                                │   │ CAN-out (OTK CAN Map)   │ │
                                │   └───────────┬─────────────┘ │
                                └───────────────│───────────────┘
                                                ▼
                                          vehicle CAN bus
```

### New port

A single new outbound port in `timing_core::ports::outbound`:

```rust
#[async_trait]
pub trait TelemetryTransmitter: Send + Sync {
    /// Send a downlink directive. The implementation owns delivery
    /// semantics (unicast, broadcast, fire-and-forget, ack-and-retry)
    /// per the message addressing.
    ///
    /// `deadline_ns` is a soft deadline: if the implementation cannot
    /// deliver before this Unix-epoch nanosecond, dropping is
    /// preferable to delivering stale telemetry. The service computes
    /// the deadline from the originating Crossing's wall clock + a
    /// configured TTL.
    async fn send(&self, directive: DownlinkDirective) -> Result<(), TelemetryError>;
}
```

`DownlinkDirective`, `TelemetryError`, and the directive payload
enums live in `event-model` (data shape) and `timing_core::ports`
(port trait).

### New application service

A single new service in `timing_core::services`:
`TelemetryService`. Constructor takes:

- the `Arc<dyn EventLog>` (or a read-only subscription handle) so it
  can compute telemetry from the live event stream;
- `Arc<dyn TelemetryTransmitter>` as the outbound port;
- a `TelemetryPolicy` value carrying the configurable bits (which
  payloads to compute per subject, broadcast cadences, TTL budget).

The service runs as a background task spawned by the composition
root. It is not on the ingest hot path; an ingest failure does not
take down telemetry, and vice versa.

### New adapter crates (eventual)

`adapter-telemetry-sub-ghz` (or whatever RF stack is chosen)
implements the outbound port over the chosen physical link. Adapter
crate naming follows the existing `adapter-<role>-<binding>`
convention.

### Resurrected stub crates (firmware track)

The hardware/firmware side resurrects several of the planning stubs
that were dropped in M10 once work begins:

| Crate | Role |
|---|---|
| `embedded-core` | Shared `no_std` logic for OTK firmware (framing, ids, OTK Protocol uplink + downlink, sequence numbering, configuration). |
| `embedded-hal-otk` | Trait layer for the hardware capabilities OTK firmware needs (RF radio, timer, CAN controller, GPIO, persistent storage). Per-target ports implement it. |
| `target-rp2040`, `target-stm32`, … | Per-MCU ports of `embedded-hal-otk`. |
| `reference-decoder-firmware` | Firmware for a reference loop decoder (uplink RX, optional downlink TX). |
| `reference-transponder-firmware` | Firmware for a reference transponder (uplink TX, optional downlink RX, optional CAN-out). |

The protocol-layer crates (`event-model`, `otk-protocol`,
`frame-codec`) are already `no_std` + `alloc` and consumable from
firmware unchanged.

---

## Open questions

These need resolution before implementation locks them in. Each is
listed with its decision-deadline relative to the milestones that
depend on it.

### RF physical layer (blocks any downlink hardware)

- **Band.** Sub-GHz (868/915 MHz ISM), 2.4 GHz proprietary, 2.4 GHz
  BLE, or a custom narrowband on venue-licensed spectrum. Sub-GHz has
  the cleanest range / penetration trade-off for trackside use;
  2.4 GHz makes hardware cheaper but more crowded; venue-licensed is
  best but only viable at the venue.
- **Modulation and data rate.** Sets the over-the-air time budget per
  message and the link budget for marginal-signal cases.
- **Module family.** Once band + modulation are fixed, a small list
  of off-the-shelf radio modules (Semtech SX126x, Nordic nRF52840,
  Espressif ESP32-C6, …) covers the design space.

### Addressing and identity

- **Subject id shape on the downlink.** Per the glossary, `SubjectId`
  on uplink is whatever the detector observes (transponder id, bib
  number, etc.). The downlink needs to address a physical receiver;
  the receiver's identity may or may not match the subject id the
  uplink uses. Two reasonable answers: (a) downlink-capable
  transponders carry a hardware-rooted `ReceiverId` distinct from
  `SubjectId`, with the runtime maintaining a mapping; (b) the
  receiver's `SubjectId` is also its downlink address. (a) is more
  flexible; (b) is simpler.
- **Broadcast id.** Reserve a well-known sentinel value (e.g. all-bits-
  set, or a specific reserved id) for venue-broadcast messages.

### Authentication and trust

- **Per-message signing?** A motivated attacker with the right radio
  can spoof a downlink transmitter and feed false flag states or
  false gap data to a transponder. For v1 OTK could rely on the
  "venue is the trust boundary" model (no per-message crypto); for
  later versions, sign payloads with a short symmetric MAC keyed per
  event.
- **Privacy.** A driver's gap-to-driver-behind is visible to anyone
  with a receiver tuned to the broadcast. Most operators consider
  this fine (the leaderboard is public anyway); some serious
  motorsport categories will want the per-driver unicast encrypted.

### Message vocabulary

- **What payloads ship in v1.** Minimum viable: last-lap time, gap
  to ahead, gap to behind, flag state, position. Beyond v1: sector
  splits, predicted finish time, weather, driver-specific custom
  data.
- **Driver-to-pit / pit-to-driver direction.** This document is
  scoped to fabric-to-subject directives. A symmetric "subject sends
  data back to the fabric" channel (driver acknowledgements, driver-
  initiated commands) is an obvious extension but is not specified
  here.

### CAN map specifics

- **CAN identifier range.** Pick a block.
- **CAN-FD or classical only.** Most motorsport dashes accept both,
  but classical CAN 2.0B is the universal subset.
- **Per-vendor configuration files.** OTK provides the map; the
  per-dash configuration files (.aim, .mtc, .stack) can ship as
  reference artifacts in this workspace or in a sibling
  `otk-can-configs` repo.

These are all also tracked in
[`open-questions.md`](open-questions.md) under a new
"Downlink and telemetry" section.

---

## Implementation roadmap

In order of dependency, not necessarily chronology. Each is a normal
OTK milestone.

1. **Spec lock-in.** This document moves from strawman to active.
   Open questions above get answers via separate decisions, recorded
   here.
2. **CAN map** (`spec/can-map.md`). Normative, doc-only.
3. **Event-model extensions.** Add `DownlinkDirective` and its payload
   variants. Backward-compatible CBOR addition; existing decoders that
   don't know the new variants ignore them.
4. **Wire-protocol extensions.** Add the `Directive` message type and
   the `target_id` envelope field. Backward-compatible per the
   existing compatibility rules in [`otk-protocol`](../otk-protocol).
5. **`TelemetryTransmitter` outbound port + `TelemetryService` in
   `timing-core`.** No-op `TelemetryTransmitter` ships for tests and
   for runtimes that don't have downlink wired up; same pattern as
   `NoopIngestMetrics`.
6. **First downlink adapter.** Adapter for the chosen RF stack,
   parameterised over a HAL trait so a test fixture can stand in for
   the real radio.
7. **Firmware track, in parallel.** `embedded-core`,
   `embedded-hal-otk`, first `target-<mcu>` port. Reference decoder
   and transponder firmware. CAN-out implementation against the
   ratified CAN map.
8. **Conformance.** New fixtures in
   [`conformance-fixtures`](../conformance-fixtures) for downlink
   directives; new contract tests in
   [`conformance`](../conformance) for the `TelemetryTransmitter`
   port and the OTK CAN Map round-trip.

Software steps 3–6 are normal Rust workspace milestones at OTK's
established pace. The firmware track (step 7) is a much larger
engineering project, paced by RF hardware availability and embedded
capacity.

---

## Out of scope

The following are explicitly **not** what this document specifies,
even though they are related and important:

- **Vehicle telemetry back to pit** (engine data, GPS traces, in-car
  video). That's a separate motorsport-telemetry stack OTK does not
  duplicate.
- **Driver-to-pit voice / data link.** Different latency profile,
  different regulatory posture.
- **Spectator data feed to apps** (mobile leaderboards, broadcast TV
  graphics). Handled by the runtime node's existing query APIs
  ([`EventQueryPort`](../timing-core/src/ports/inbound/query.rs)),
  not by the downlink path.
- **Vehicle-to-vehicle communication** (cooperative ADAS-style use
  cases). Out of OTK's role.

---

## Why this stays in the spec, not in code, until decided

The strawman shape above touches:

- The wire protocol envelope (new field, backward-compatible
  addition).
- The event model (new top-level variant family).
- A new `timing-core` port and service.
- A new adapter crate category.
- Several resurrected firmware crates.

Getting the strawman wrong is expensive to undo across all of those.
Getting it written down before the first PR exists is cheap. This
document is the design lock; code lands once the open questions
have answers.
