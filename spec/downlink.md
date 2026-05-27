# Downlink: fabric to subject

The bidirectional half of Open Timekeeping: information flowing from the
runtime node back to the subject being timed. Specifies the
fabric-to-decoder standings push, the decoder-to-transponder instant
downlink, the three-application model that makes both work at racing
speeds, and the reference prototype hardware for bringing it up
end-to-end.

> **Status: strawman.** This document is the starting point for design,
> not a settled standard. Decisions are tagged:
>
> - **(locked)**: settled by this document; downstream work depends on it.
> - **(strawman)**: this document's recommendation; needs sign-off
>   before it binds any implementation.
> - **(open)**: genuinely unresolved; needs a separate decision call,
>   tracked in [`spec/open-questions.md`](open-questions.md).
>
> The architectural shape (three-app model, edge-computed gap on the
> decoder, server pushes standings only) is **(locked)**. Physical-layer
> details (loop geometry, downlink PHY choice, addressing scheme, auth
> model, exhaustive payload list) are largely **(open)**.

---

## 1. Why this exists

The receive-side stack ([architecture.md](architecture.md)) covers the
unambiguous part of Open Timekeeping: observe a passage, persist it,
serve it. That stack works for every timing role today and is enough
to ship a usable open-source timing system.

There is a second motion that pro motorsport and many championship
running deployments rely on: **send the result back to the subject.**
Give the driver / competitor near-real-time information about their
own performance and the wider race state, so they can react during
the session rather than after it.

In motorsport this typically takes the form of an active transponder
that bridges live data onto the vehicle's CAN bus, where the existing
dashboard (factory or aftermarket) renders it alongside engine RPM,
oil pressure, and fuel. The driver sees gap to ahead, gap to behind,
last lap time, and current position update on the steering-wheel
display the moment they cross any timing loop.

Open Timekeeping is a full-stack system. The downlink path is in
scope for the same reasons the uplink path is. This document
specifies how it fits.

### The latency constraint

Cars at racing speeds travel 55 m/s (200 km/h) or more. The driver's
display needs to update before they reach the next corner. A
server-mediated computation path (detection → ingest → server →
compute gap → server → transmitter → over the air → transponder)
adds tens of milliseconds of network round trip. At 55 m/s, every
10 ms is over half a meter of car travel; 50 ms is nearly three
meters. Past a certain budget the data is no longer useful.

The architecture below pushes computation to the edge specifically to
honour this constraint. Servers maintain state and push it ahead of
when it will be needed; the decoder at each loop computes the actual
per-crossing payload locally, the instant a crossing is detected.

---

## 2. Three applications, not two

The naive model of timing is "decoders are sensors that push events
to a server." That model is wrong for the downlink path. The decoder
is itself an application that runs on embedded hardware and hosts
real application logic. There are **three** first-class applications
in the deployed system:

```text
Timing Node                Decoder (firmware app)              Transponder (firmware app)
─────────────              ────────────────────                 ──────────────────────────
maintains              ←── uplink hits ───                 ←── uplink ──
current standings                                                          (instant, edge-
                       ──→ standings ──→                  ──→ downlink ──→  computed)
                          (low freq,                                                  │
                           pushed by                                                  │
                           server)                                                    ▼
                                                                              outbound port:
                                                                              CAN-out / display / BLE
                                                                                                  │
                                                                                                  ▼
                                                                                          vehicle CAN bus
                                                                                                  │
                                                                                                  ▼
                                                                                            in-vehicle dashboard
```

The hexagonal pattern of [`timing-core`](../timing-core) (domain
types, inbound and outbound ports, application services) applies
recursively to the decoder and the transponder firmware applications:

- The **decoder application** has inbound ports (loop-crossing
  capture, standings push from the server), outbound ports (uplink
  to the server, instant downlink to the transponder), local state
  (rolling crossing-time cache per subject, last-received standings),
  and real application logic (gap computation on every crossing).
- The **transponder application** has inbound ports (downlink RF
  receive), outbound ports (vehicle binding: CAN-out, BLE, USB,
  on-device display), and modest application logic (decode
  directive, format for the chosen output binding).

Neither is a dumb peripheral. Both ship their own firmware crates
when the implementation work begins (see §11 below).

---

## 3. The latency constraint forces edge computation

The gap computation has to live on the decoder, not the server. The
math:

- Cars at 200 km/h cover 55 m/s.
- A round trip through the timing-node server (detector → ingest →
  application service → outbound transmitter → RF → transponder)
  realistically spends 20–50 ms in network and processing hops, even
  with local LAN to the transmitter.
- 20 ms is 1.1 m of car travel. 50 ms is 2.8 m.
- A driver's display update that lags by more than ~30 ms is
  measurably late, and at faster classes the budget tightens.

Pushing the computation to the decoder eliminates the round trip
entirely. The decoder already observed the crossing; it already
holds the cache it needs (per §5); the only over-the-air hop left
is the instant downlink from the decoder's RF transmitter to the
transponder ~5 m away.

End-to-end budget with edge computation:

| Hop | Budget |
|---|---|
| Loop antenna sees transponder; decoder front-end peak-detects | 1–5 ms |
| Decoder looks up subject's rank in cached standings, looks up the ahead-subject's last crossing time in local cache, computes gap | <100 µs |
| Decoder formats downlink directive and hands to its 2.4 GHz radio | <1 ms |
| Over-the-air to transponder (2.4 GHz BLE Coded PHY, ~20-byte packet) | 1–2 ms |
| Transponder receives, parses, emits on CAN | <5 ms |
| Dashboard reads CAN and updates display | depends on dash, ~5–20 ms |

Total: 15–35 ms achievable. Comfortably inside the trackside
requirement.

This budget is **(strawman)**: confirmed by the architectural review
that informed this document, not yet measured against a working
prototype.

---

## 4. Standings are the only thing the server distributes

If the gap is computed on the decoder, what does the server have to
push down to it? The answer is small: **the current standings**, as
an ordered list of subject IDs with each subject's lap count.

The decoder's gap computation works like this, on every detected
crossing:

1. Record the new crossing in the local cache (subject ID → timestamp).
2. Look up the just-crossed subject in the cached standings to find
   its rank.
3. Find the subject one rank ahead in the standings.
4. Look up that ahead-subject's last crossing time in the local cache
   (the cache is keyed on this decoder's own observations).
5. Compute `gap_to_ahead = current_crossing_time - ahead_last_crossing_time`.
6. Do the same for the subject one rank behind.
7. Format the result into a `DownlinkDirective` and transmit.

The subject's own observations are its state. No cross-decoder
propagation is needed because each decoder only computes gaps for
crossings at its own loop. The standings tell it who is ahead of who;
its own cache tells it when those subjects last crossed this loop.

The standings payload is tiny:

- A list of `(SubjectId, lap_count)` pairs, in rank order.
- For a 40-car field: <500 bytes serialized.
- Pushed once per overtake (rank change) or on a low-frequency
  heartbeat (a few times per second worst case).
- Latency tolerance: the cache just needs to be reasonably fresh by
  the time the next subject crosses. There is no instant requirement
  on this path.

This is **(locked)**. The doc commits to standings-only for the
server-to-decoder push. Race control messaging (flag state, safety
car, pit window, weather, race-director comms) is a separate concern
and is explicitly **out of scope** for this document, even when it
shares a deployment node. See §17.

---

## 5. Asymmetric capabilities are first-class

The bidirectional architecture must not assume every device speaks
both directions. Real deployments mix:

| Device class | Uplink (subject → fabric) | Downlink (fabric → subject) |
|---|---|---|
| Transmit-only transponder (basic, low cost) | required | none |
| Receive-only decoder (basic, low cost) | required | none |
| Two-way transponder (pro) | required | required |
| Two-way decoder (pro) | required | required |

A pro decoder may need to coexist with basic transponders at the same
venue (the pro features simply do not reach them). A pro transponder
may sit on a fabric where only some decoders carry downlink
transmitters (the pro features work only at the loops the venue has
invested in).

The OTK protocol must model uplink and downlink as **independently
optional capabilities a device advertises**, never as "the system is
bidirectional and unidirectional cases are degraded." A v1 deployment
might have downlink-equipped decoders only at start-finish and one
mid-track point; the rest of the loops remain uplink-only. Subjects
without downlink receivers happily coexist; they just do not get
in-vehicle feedback.

This is **(locked)**.

---

## 6. Domain neutrality vs standardized payload

OTK is meant to span motorsport, athletics, cycling, rowing, RC
racing, industrial checkpoints, and similar. But the downlink only
delivers value if the payloads are standardized enough that
off-the-shelf in-device hardware (a pro motorsport dashboard, a
runner's wrist display, a coaching screen) can be configured against
them. Standardization tends to drag in domain-specific terminology.

The resolution is layering:

- The **architecture, transport, dispatch, and addressing** are all
  domain-neutral. They make no assumption about whether the timed
  subjects are cars, runners, rowers, or industrial parts on a
  conveyor.
- The **payload vocabulary** has two layers:
  - **Core payloads** in [`event-model`](../event-model), v1 starter
    set: standardized, multi-sport, timing-derived only.
    `StandingsUpdate { ranking: Vec<(SubjectId, lap_count)> }` for the
    server-to-decoder direction; `DownlinkDirective { gap_to_ahead,
    gap_to_behind, last_lap_time, position }` for the
    decoder-to-transponder direction. These are concepts recognisable
    to anyone running a ranked timed event regardless of sport.
  - **Sport-specific timing extensions** (future): a namespaced
    extension mechanism for timing-related payloads that do not
    generalise. For example, motorsport-only `ClassStandings` for
    multi-class endurance racing. This document commits to the
    *existence* of the extension mechanism, not its v1 contents.

The v1 vocabulary stays intentionally minimal: the last known order
of competitors and the local gap math derived from it. That is
enough to bootstrap and ship; richer payloads land as separate
vocabulary milestones.

**Explicitly NOT in the vocabulary**: flag state, safety car, pit
window, weather, race-director messaging. Those belong to a separate
race-control messaging system, which is a sibling capability that
may share a deployment node but is not what this document specifies.
See §17.

---

## 7. Where this fits in the OTK Protocol stack

The four-layer OTK Protocol (Event Model, Wire Protocol, Frame
Codec, Transport Binding) was designed direction-agnostic. Downlink
uses the same four layers; only the transport binding is per
direction.

**(strawman)** One OTK Protocol stack, with shared upper layers and
per-direction transport bindings.

- **Event Model** ([`event-model`](../event-model)) gains new event
  families. `StandingsUpdate` for the server-to-decoder push;
  `DownlinkDirective` for the decoder-to-transponder instant push.
  Both alongside the existing uplink families (Detection, Crossing,
  DetectorHealth, TimebaseStatus, AdapterMetadata).
- **Wire Protocol** ([`otk-protocol`](../otk-protocol)) gains a new
  message-type variant per direction, plus an optional `target_id`
  field on the envelope. Uplink envelopes have `source_id` only
  (the producer that emitted it); downlink envelopes have both
  `source_id` (the originator, e.g. the timing-node for standings,
  the decoder for directives) and `target_id` (the intended
  receiver, e.g. a specific decoder or a specific transponder, or a
  reserved sentinel for broadcast).
- **Frame Codec** ([`frame-codec`](../frame-codec)) is byte-shape
  invariant under direction. The same length-prefix and COBS+CRC
  variants apply.
- **Transport Binding** is per-direction. The uplink bindings
  ([`adapter-ingest-tcp`](../adapter-ingest-tcp),
  [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket),
  future serial) carry frames upstream. The downlink bindings carry
  frames over RF; their physical layer is fundamentally different
  (see §16).

The alternative considered and rejected was a parallel sibling
protocol stack (`otk-downlink-protocol`). That would have duplicated
envelope structure, frame codecs, and conformance boilerplate for
marginal benefit, and would have forced bidirectional devices to
implement two non-interoperating wire vocabularies.

### Addressing on the downlink

Uplink envelopes use `source_id` to name the producer. Downlink
envelopes need a target. **(strawman)** Add a new optional
`target_id` field to the envelope: present on directives, absent on
uplink. `target_id` may be a specific receiver (a single decoder or
a single transponder) or a well-known broadcast sentinel.

**(open)** Whether `target_id` and `source_id` are separate fields
or collapsed into a single `peer_id`. Single field is simpler;
separate fields make uplink-vs-downlink intent visible without
inspecting `message_type`. This document recommends separate fields;
the trade is small.

---

## 8. The data path

Uplink is unchanged from
[architecture.md § The data path](architecture.md). What is new is
the bidirectional half:

```text
Uplink (existing, see architecture.md)
======================================
Sensor / detector / transponder
        |
        v
Detector adapter (decoder firmware)
        |
        v   canonical events (Detection, ...)
        v
Timing Runtime Node
   - ingest pipeline → event log → projections → APIs

Server-to-decoder push (new, low frequency)
===========================================
Timing Runtime Node
   - StandingsService maintains current standings from uplinks
   - Pushes StandingsUpdate to each decoder when standings change
        |
        v   StandingsUpdate { ranking: Vec<(SubjectId, lap)> }
        v
Decoder firmware (caches received standings)

Decoder-to-transponder instant downlink (new, edge-computed, fast)
==================================================================
Decoder firmware
   - Detects crossing
   - Looks up just-crossed subject's rank in cached standings
   - Looks up ahead-subject's last crossing time in local cache
   - Computes gap locally; formats DownlinkDirective
        |
        v   DownlinkDirective { gap_to_ahead, gap_to_behind,
        v                        last_lap_time, position }
        v
Transponder firmware
   - Receives directive
   - Bridges to vehicle output via the configured OutputBinding
        |
        v   per OTK CAN Map (separate doc, see §11 deferred)
        v
Vehicle CAN bus → in-vehicle dashboard
```

---

## 9. Hexagonal placement, all three sides

### Timing-node (server-side)

- **New application service** in `timing-core::services`:
  `StandingsService`. Subscribes to the existing event log; updates
  the maintained `Standings` value as it observes Crossings; pushes
  `StandingsUpdate` to all connected decoders when the standings
  change.
- **New outbound port** in `timing_core::ports::outbound`:
  `StandingsPublisher`. The application service emits through this
  port; the composition root (`timing-node`) injects a concrete
  adapter implementation that handles the actual fan-out over
  whatever transport the deployment chose.

### Decoder firmware

- **Likely new core crate**: `decoder-core` (analogous to
  `timing-core`), with its own `domain/`, `ports/`, and `services/`
  layout. Hosts the gap computation, the standings cache, the
  crossing-time cache.
- **Inbound ports**:
  - Detection capture (the existing role; loop antenna sees a
    transponder).
  - `StandingsReceiver` (consumes server-pushed `StandingsUpdate`).
- **Outbound ports**:
  - Uplink (the existing role; push `Detection` events to the
    timing-node).
  - `DownlinkTransmitter` (transmit `DownlinkDirective` to the
    just-crossed transponder).

### Transponder firmware

- **Likely new core crate**: `transponder-core`, with the same
  hexagonal pattern.
- **Inbound port**: `DownlinkReceiver` (receives `DownlinkDirective`
  over the downlink RF link).
- **Outbound port**: `OutputBinding` (bridges to the vehicle).
  Canonical implementation is `CanOutBinding` per the deferred
  `spec/can-map.md`; alternative implementations include BLE
  (companion app on the driver's phone), USB (to a connected data
  logger), and an on-device display for transponders that ship with
  one.

### Adapter boundary discipline

The fence pattern established in
[`timing-core`](../timing-core) applies recursively. Decoder-side
adapter crates (per-radio implementations of
`DownlinkTransmitter`, transport implementations of
`StandingsReceiver`) depend only on the relevant port traits in
`decoder-core`, not on its domain or services. Per-adapter
`clippy.toml` fences enforce the boundary, same shape as the
server-side fence today.

---

## 10. Resurrected firmware crates

Several of the planning stubs that were dropped in M10 come back
once firmware-track work begins. The crate names below are
**(strawman)** and may shift as design lands.

| Crate | Role |
|---|---|
| `embedded-core` | Shared `no_std` logic for OTK firmware (framing, ids, sequence numbering, configuration). |
| `embedded-hal-otk` | Trait layer for hardware capabilities OTK firmware needs (LF receive front-end, 2.4 GHz radio, timer / input-capture, CAN controller, GPIO, persistent storage). Per-target ports implement it. |
| `target-rp2040`, `target-stm32`, … | Per-MCU ports of `embedded-hal-otk`. |
| `decoder-core` | Decoder-side domain + ports + services. Standings cache, crossing-time cache, gap computation. |
| `transponder-core` | Transponder-side domain + ports + services. Directive decode, output-binding dispatch. |
| `reference-decoder-firmware` | Firmware application for a reference decoder. Uplink RX (LF receive chain), optional downlink TX (2.4 GHz). |
| `reference-transponder-firmware` | Firmware application for a reference transponder. Uplink TX (LF inductive broadcast), optional downlink RX (2.4 GHz), optional CAN-out. |

The protocol-layer crates ([`event-model`](../event-model),
[`otk-protocol`](../otk-protocol),
[`frame-codec`](../frame-codec)) are already `no_std` + `alloc` and
consumable from firmware unchanged.

These crates are **not created in this milestone**. This document
specifies their intended responsibilities; actual crate creation
happens when the firmware work begins.

---

## 11. Open questions

Tracked in [`spec/open-questions.md`](open-questions.md) under
"Downlink and standings." Each needs resolution before the
corresponding implementation work can start. Summary:

- **LF uplink physical-layer details.** Frequency is settled at
  ~125 kHz per industry practice. Loop geometry, conductor gauge,
  tuning capacitor sizing, midpoint termination value are all open.
- **2.4 GHz downlink PHY choice.** BLE 5 Coded PHY, IEEE 802.15.4,
  or Nordic ESB. Each has trade-offs for latency, addressing, and
  silicon availability.
- **Addressing scheme on both directions.** Subject id shape on the
  downlink (reuse the uplink `SubjectId` or carry a separate
  hardware-rooted `ReceiverId` with a runtime-maintained mapping).
  Broadcast sentinel value.
- **Authentication and trust model.** Per-message signing, or
  venue-as-trust-boundary for v1.
- **Exhaustive v1 payload vocabulary.** Beyond the core
  `StandingsUpdate` / `DownlinkDirective`, what else lands in v1.
- **Sport-specific extension mechanism shape.** How extensions get
  namespaced and discovered.
- **Multi-pass collision handling.** Pack racing puts 5–10
  transponders on one loop within ~50 ms. How does the LF uplink
  protocol disambiguate (per-transponder pseudo-random TX slots,
  narrowband channelisation, CDMA-style codes)? How does the
  downlink protocol address the 5–10 owed directives in rapid
  sequence?
- **ADC sampling rate and peak-detection filter design** for the LF
  receive chain on the decoder. This is the genuinely
  hard-real-time work; a Cortex-M7 in software handles the gap math
  in microseconds, so there is no "hardware vs firmware" split for
  the gap computation. The real question is the sampling design for
  the receive chain.
- **EMI envelope** per ISO 7637-2 / ISO 11452 for the transponder's
  in-vehicle hardware design.
- **Antenna polarization mismatch budget.** Chassis orientation
  varies; either circularly-polarized antennas on the decoder or a
  6 dB polarization-mismatch margin in the link budget.
- **2.4 GHz multipath mitigation.** Frequency hopping (BLE native
  FHSS) or diversity reception with two decoder antennas.
- **Ground-loop discipline** in multi-loop installations.
- **GPS-disciplined oscillator** on each decoder for cross-decoder
  time consistency.
- **Transponder low-battery UX** via CAN.

---

## 12. Implementation roadmap

In dependency order; chronology depends on contributor capacity.

1. **Spec lock-in.** This document moves from strawman to active.
   Open questions above get answers via separate decision PRs,
   recorded here.
2. **CAN map.** `spec/can-map.md` drafted (separate normative doc).
3. **Event-model extensions.** `StandingsUpdate` and
   `DownlinkDirective` added. Backward-compatible CBOR additions;
   existing implementations ignore unknown variants.
4. **Wire-protocol extensions.** `target_id` envelope field added.
   Backward-compatible.
5. **`StandingsService` + `StandingsPublisher` in `timing-core`.**
   No-op `StandingsPublisher` ships for tests and for runtimes that
   do not have downlink wired up; same pattern as
   `NoopIngestMetrics`.
6. **First downlink adapter implementation.** The composition-root
   side of the server's standings push, parameterised over a
   transport so test fixtures can stand in for the real radio.
7. **Firmware track, in parallel.** `embedded-core`,
   `embedded-hal-otk`, first `target-<mcu>` port, `decoder-core`,
   `transponder-core`. Reference decoder and transponder firmware.
   CAN-out implementation against the ratified CAN map.
8. **Conformance.** New fixtures in
   [`conformance-fixtures`](../conformance-fixtures) for standings
   updates and downlink directives. New contract tests in
   [`conformance`](../conformance) for `StandingsPublisher`,
   `DownlinkTransmitter`, and the OTK CAN Map round-trip.

Software steps 3–6 are normal Rust workspace milestones. The
firmware track (step 7) is paced separately by RF hardware
availability and embedded capacity.

---

## 13. Deployment density

Gap-update frequency is a **deployment decision**, not an
architectural one. The architecture handles 1 loop or 50 loops
identically.

| Loops on a 5 km track | Gap update interval at race pace |
|---|---|
| 1 (start-finish only) | ~90 s, once per lap |
| 3 (start-finish + 2 sector loops) | ~30 s |
| 10 | ~9 s |
| 20 (broadcast-quality) | ~4.5 s |

Each loop is independent. Each runs its own copy of the decoder
firmware. Each computes gaps locally on crossing using the
server-pushed standings. The server does not care how many loops
there are; it pushes standings updates to all of them. The
transponder does not care either; it just receives directives
whenever any loop transmits one to it.

The path to "broadcast-quality" gap update frequency in OTK is
"deploy more loops." No protocol change is needed.

---

## 14. Possible future direction: continuous interpolation

Some top-flight motorsport categories deliver gap updates many times
per second, not at every timing loop. They do this by having each
vehicle carry a high-rate GPS receiver and stream position back via
the team-telemetry channel; a server-side interpolator computes
continuously-updated gaps between authoritative loop crossings.

This is real and works, but it requires:

- A second uplink data plane (subjects publishing GPS position at
  10+ Hz).
- Interpolation logic somewhere (timing-node or transponder).
- More on-board hardware (GPS receiver on each subject).

It is **deliberately out of scope** for v1 of this document. The
architecture does not preclude it. Adding it later means defining a
new uplink event type (position fix) and a new interpolation
service alongside `StandingsService`. The decoder-to-transponder
instant downlink path and the standings push path stay unchanged.

This section exists to be honest about what high-end pro systems do
and to tell readers that an OTK deployment using more loops (per
§13) is the simpler approach for most contexts.

---

## 15. Reference prototype hardware

Concrete shopping list for bringing up a working prototype of the
full stack. Two tiers, with very different fidelity profiles. Both
tiers are **reference, not normative**: implementers can substitute
equivalent parts.

### Foundational fact

The uplink (transponder → decoder loop) is **near-field magnetic
induction at low frequency**, not UHF radio. Legacy installations
use single-digit kHz; modern installations use ~125 kHz. The "loop
antenna" is a rectangular coax loop with a midpoint termination
resistor, laid flush with or shallow-buried under the racing
surface. Industry uses this band because the regulatory path is
trivial worldwide (essentially zero radiated power).

The downlink (decoder → transponder) is a **separate 2.4 GHz radio
link**. The two are independent radios, not a single duplex link.

Establishing this up front matters because the obvious "build a
loop + active transponder" mental model defaults to thinking of
both as UHF radio. They are not. The uplink is closer in spirit to
near-field RFID than to a sub-GHz radio link.

### Tier 1: Protocol Bring-Up Rig

**~$130–180 BOM, fits on a workbench, suitable for protocol
bring-up, firmware development, CI fixtures, demos.**

The name "prototype" is deliberately avoided: this rig exercises the
software pipeline thoroughly but does not validate any
motorsport-grade RF concern. The MFRC522 is a magnetic-induction
stand-in for the LF loop only at the topology level (near-field
induction is correct); the physics (5 cm vs 1.5 m range,
request-response vs continuous broadcast, 13.56 MHz vs 125 kHz)
differ enough that nothing tuned on Tier 1 transfers to Tier 2.

| Role | Hardware | Approx. cost | Notes |
|---|---|---|---|
| Timing-node host | Raspberry Pi Zero 2 W or Pi 5 | $15–80 | Runs the timing-node Rust workspace. Ethernet or WiFi to decoder. |
| Decoder MCU + 2.4 GHz radio | Adafruit Feather nRF52840 Express (or Nordic nRF52840 DK) | $25 | nRF52840 integrates Cortex-M4 + 2.4 GHz radio + BLE + 802.15.4 in one chip, removing the need for a separate downlink radio module. Tier 1 → Tier 2 RF transition becomes "same chip, better antenna." |
| Decoder uplink stand-in | MFRC522 13.56 MHz NFC reader module | $3 | Conceptual stand-in only for the LF inductive loop. Same magnetic-induction topology, wildly different physics. Sufficient to exercise crossing-detection software; insufficient for tuning anything that transfers to Tier 2. |
| Transponder MCU + 2.4 GHz radio | Adafruit Feather nRF52840 Express | $25 | Same chip family as the decoder side. |
| Transponder uplink stand-in | Passive NFC tag glued to the transponder MCU board | $0.50 | The "transponder broadcasting to the loop" is replaced by the NFC tag being read by the MFRC522. |
| Transponder CAN-out | MCP2515 SPI-to-CAN module | $5 | Real CAN-bus output. Exercises the CAN-map binding. |
| CAN verification | USB-CAN dongle (CANable clone or equivalent) | $30 | Plug into a PC to inspect what the transponder is emitting. |
| 2.4 GHz traffic verification | nRF Sniffer dongle | $15 | Captures BLE / 802.15.4 / proprietary 2.4 GHz traffic into Wireshark. |
| Internal-timing verification | USB logic analyzer (Saleae clone) | $15 | Captures SPI traffic and validates end-to-end latency claims. |
| Optional second decoder | MFRC522 + nRF52840 pair (duplicate) | $30 | Exercises the "decoder is independent, server pushes standings to each" architectural claim. |
| Misc | Small OLED, jumper wires, breadboard, USB cables, 5V power | $20 | Status displays, wiring. |

**Debuggable interfaces (required, not optional):**

- USB-CDC serial console on every embedded board (decoder,
  transponder). Prints state, accepts commands.
- Manual-trigger mode on the decoder: "as if subject X just
  crossed, NOW" — for testing without needing a physical tag
  swipe.
- Loopback / dump-received mode on the transponder: prints every
  received downlink directive to serial for inspection.
- Status LEDs on each board (power, link, last-event-timestamp).
- Web UI on the timing-node server (already exists via the API
  layer): shows live standings, recent crossings, decoder health.
- Wireshark / tcpdump on the uplink LAN segment (TCP) and nRF
  Sniffer on the 2.4 GHz downlink, for inspecting the wire
  protocol in both directions.

**Tier 1 explicitly does NOT validate:**

- Continuous-broadcast peak detection (MFRC522 is
  request/response; real active transponders broadcast
  continuously).
- Multi-pass collision behaviour (pack racing with 5–10
  transponders crossing within 50 ms).
- Doppler at racing speeds.
- Multipath in real RF environments (concrete grandstands, pit
  walls).
- Antenna polarisation issues.
- Vehicle EMI (race-car ignition, alternator transients).
- Anything about the LF receive chain on the decoder side.

### Tier 2: Track-Scale Prototype

OTK is an open-source-from-the-ground-up project and **does not**
integrate with, depend on, or borrow from any proprietary
chip-timing ecosystem. Both Tier 2 stages build OTK-native
hardware end-to-end.

#### Stage 2A: Minimum-Viable LF Link

**~$200–400 BOM, weeks of effort, suitable for proving the LF
physical layer at bench / parking-lot scale before scaling to
motorsport-grade.**

| Role | Hardware | Notes |
|---|---|---|
| Timing-node host | Mini PC or rugged industrial Linux box | Field-deployable. |
| Decoder LF receive chain (minimum-viable) | Small rectangular loop (~50 cm × 50 cm) of single-conductor wire on a wooden frame, tuned with a parallel capacitor to ~125 kHz, into an op-amp current-sense front-end → comparator or low-cost ADC → Cortex-M MCU for peak detection. Bench-scale dimensions, not motorsport-scale. | Validates the LF receive concept end-to-end without the motorsport-grade civil engineering (no buried-coax loop, no track install). |
| Bench transponder (minimum-viable) | Cortex-M MCU + LC tank tuned to 125 kHz + simple modulator emitting a fixed ID + LiPo or USB power, in a 3D-printed enclosure | A signal-generator-grade "fake transponder": broadcasts a known ID at low duty cycle. Sufficient to exercise the decoder's identity-detection and timing-capture logic at human-walking speeds. Range: ~30 cm above the loop. |
| 2.4 GHz downlink | nRF52840 boards from Tier 1, integrated into the bench decoder + bench transponder | Validates the OTK downlink path end-to-end at the new physical scale. |
| Vehicle integration (optional) | Skip for Stage 2A. The bench transponder does not go in a car. | CAN-out continues to be validated against the USB-CAN dongle from Tier 1. |

The point of Stage 2A: prove the LF inductive physical layer
concept on OTK-native hardware at a scale and cost that an
individual contributor can build on a workbench in a few weekends.
Walk over the loop with the bench transponder in hand; see the
decoder detect; see the standings push; see the downlink fire;
see the CAN frame. This is the first stage where the OTK RF stack
is fully self-contained.

**Stage 2A explicitly does NOT validate:**

- Real motorsport-grade loop range (1.5 m through tarmac, vehicle
  speeds, packed-loop traffic).
- Motorsport-grade active transponder design (battery life,
  vibration, EMI, automotive enclosure).
- 2.4 GHz multipath in real RF environments.
- Anything requiring an actual race car.

#### Stage 2B: Full Motorsport-Scale Prototype

**~$3k+ BOM, multi-month EE collaboration, suitable for end-to-end
validation including the OTK downlink path at racing speeds.**

| Role | Hardware | Notes |
|---|---|---|
| Decoder LF receive chain | Production-class: op-amp current-sense front-end + 200 kSPS ADC + Cortex-M (e.g. STM32H7 or STM32G4 with hardware timer input-capture) for peak detection. Loop is 6 m × 1 m rectangular RG-213 with 50 Ω midpoint termination, tuned to 125 kHz. | Real RF engineering. Scales the Stage 2A bench loop up to track geometry. |
| Decoder 2.4 GHz downlink TX | nRF52840 module + antenna, integrated with the LF receive chain board | Same family as Tier 1 nRF52840. |
| Active transponder (production-class custom) | MCU (RP2040 / STM32G0 / nRF52) + LF tank-coil driver + 2.4 GHz RX + LiPo + charge IC + boost + automotive-grade enclosure with potting compound | BOM ~$25 in singletons, ~$8 in 1k. Scales the Stage 2A bench transponder up to in-vehicle-grade. **No mature open-source motorsport active transponder design exists** — this is a meaningful open-hardware contribution opportunity. The closest cousins (Sportiduino, OpenRaceTiming, PikaTimer) are all at human-running-speed RFID. |
| Vehicle integration | CAN harness, dashboard with a configurable CAN message map (e.g. AIM MXP/MXG/MXS, MoTeC C125/C127, Stack ST8920, Race Technology DASH, OEM motorsport dash, or hobby-grade with documented CAN like Holley/Haltech), 12V power | OTK transponder firmware in the car. Dashboard configured against `spec/can-map.md` (deferred). |

**Stage 2B caveats:**

- Country-of-deployment regulatory check (FCC Part 15 in US,
  ETSI EN 300 220 / RED in EU, MIC in Japan, ACMA in AU). The LF
  uplink is trivially compliant at these frequencies; the 2.4 GHz
  downlink is governed by per-band ISM rules.
- Transponder mounting per chassis material: above carbon /
  aluminium, at chassis edge, vibration-isolated. Reference
  published motorsport-electronics mounting practice; do not name
  specific commercial brands in OTK docs.
- In-vehicle environmental envelope: −20 to +85 °C ambient, +125 °C
  survival; 20 g random vibration; conducted EMI per ISO 7637-2
  (load-dump, jump-start, alternator transients); RF coexistence
  with team radio at the 5/8 W level.
- Antenna polarisation on the transponder: chassis orientation is
  unpredictable. Either use circularly-polarised antennas on the
  decoder, or design the link budget with a 6 dB
  polarisation-mismatch margin.
- 2.4 GHz multipath near concrete grandstands and pit walls is
  severe. Mitigate with frequency hopping (BLE native FHSS) or
  diversity reception (two decoder antennas + RX combining).
- Ground loops in multi-loop installations: practice single-point
  grounding discipline.
- GPS-disciplined oscillator on each decoder for inter-decoder
  time consistency.

**OTK contribution opportunity:** Stage 2B's active transponder
design is a real open-source gap. No mature design exists; building
one is a multi-month EE project but produces a meaningful
contribution to the open hardware ecosystem.

---

## 16. Why this stays in the spec, not in code, until decided

The architectural shape above touches:

- The wire protocol envelope (new `target_id` field,
  backward-compatible addition).
- The event model (two new top-level variant families).
- A new `timing-core` port and service.
- A new adapter category.
- Several new firmware crates.

Getting the shape wrong is expensive to undo across all of those.
Getting it written down before the first PR exists is cheap. This
document is the design lock; code lands once the open questions
have answers.

---

## 17. Out of scope

The following are explicitly **not** what this document specifies,
even though they are related and may share a deployment node:

- **Race control messaging.** Flag state, safety car, pit window,
  weather, race-director comms are a separate system entirely. A
  sibling capability OTK may eventually grow, with its own
  architecture and its own design doc. It does not share the
  downlink path defined here; conflating them would muddy both.
- **Vehicle-to-pit telemetry.** Engine data, GPS traces, in-car
  video. That is a separate motorsport-telemetry stack OTK does not
  duplicate.
- **Driver-to-pit voice / data link.** Different latency profile,
  different regulatory posture.
- **Spectator data feed to apps** (mobile leaderboards, broadcast
  TV graphics). Handled by the runtime node's existing query APIs
  ([`EventQueryPort`](../timing-core/src/ports/inbound/query.rs)),
  not by the downlink path.
- **Vehicle-to-vehicle communication** (cooperative ADAS-style
  use cases). Out of OTK's role.
- **`spec/can-map.md`.** The byte-level CAN message map that
  CAN-out transponder firmware emits is a separate normative doc,
  to be drafted alongside the first transponder firmware
  milestone.
