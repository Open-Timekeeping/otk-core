# Glossary

The terminology of Open Timekeeping. These terms are normative, if you write a docstring, a README, or a spec section that conflicts with a definition here, fix the docstring.

The glossary is intentionally short. Where a concept needs detail, link out to the dedicated spec file.

---

## Core actors and roles

**Detector**
A device or process that observes a physical event at a timing point and produces detection data. Loop detectors, RFID mat readers, beam-break gates, camera triggers, manual buttons, simulators, and replay sources are all detectors. The role is observation, not signal-decoding.

**Detector adapter**
The software role that turns detector-specific data into canonical Open Timekeeping events. Every detector event source (embedded native firmware, standalone producer process, edge gateway, runtime-node plugin, simulator, replay) implements the same `DetectorAdapter` trait from [`otk-contracts`](../otk-contracts). Packaging differs; the contract does not.

**Timing point**
A fixed location at which detections happen. A start/finish line, a sector boundary, a pit entry, a pit exit. A detector belongs to exactly one timing point at a time; a timing point can have one or more detectors (redundancy).

**Timebase**
The upstream physical time reference that one or more detectors are disciplined against. Examples: a GNSS receiver distributing PPS over coax, a PTP grandmaster, the GPS constellation seen by per-detector receivers. A timebase has an identity that is addressable across the deployment (so detectors on different nodes can declare they're on the same reference), a declared kind/profile, and an expected uncertainty. Each detector reports its own actual sync state and uncertainty against the timebase at runtime. A timebase is *not* a node's local clock; node clocks matter only for `appended_at`.

**Timing Runtime Node** (also: *runtime node*, binary name `otk-node`)
The deployable server process. Ingests canonical events from producers and plugins, hosts plugins, maintains the event log, runs `timing-core`, persists state, and serves APIs. Broker-like in deployment shape; never referred to as "the broker" in normative documentation.

**Timing Fabric**
The deployed topology: one or more runtime nodes plus their connected detector producers, adapters, timebase sources, apps, APIs, storage, and operator tools. "Fabric" is *not* a synonym for any single repo or process; it names the system in deployment.

**Timing Core**
The timing-domain engine ([`timing-core`](../timing-core)). A library, not a server, that turns canonical detector events into crossings, laps, sectors, and timing results.

**Producer**
Any process that sends canonical detector events to a runtime node as OTK frames over a supported transport binding. A producer hosts one or more detector adapters. A producer may use [`otk-sdk`](../otk-sdk)'s `producer` feature for convenience, or build directly on `event-model` + `otk-protocol` + `frame-codec` + a transport binding.

**Plugin**
An in-process module loaded by a runtime node. Detector adapters, timebases, storage backends, and exports can all be packaged as plugins. Plugins submit canonical events to the runtime in-process; no OTK frames cross a wire for plugin-loaded adapters.

**Entrant**
The subject being timed (a car, a runner, a boat, a transponder, a bib number). Identification (mapping transponder → entrant, bib → entrant) is a domain-layer concern; the timing layer carries an `entrant_id` and lets the app layer interpret it.

**App**
A user-facing surface consuming the runtime node's outward APIs. `app-live-timing` is the live timing UI for operators and spectators; `app-diagnostics` is the diagnostics UI for operators.

---

## Protocol terminology

**OTK Protocol**
The transport-independent wire protocol used by Open Timekeeping components to exchange canonical timing messages. Defined as four stacked layers (Event Model, Wire Protocol, Frame Codec, Transport Binding), each with its own contract. The first three layers live as crates in this workspace; the fourth (Transport Binding) lives in per-transport adapter crates. OTK is not synonymous with TCP, HTTP, or any specific transport.

**Event Model** ([`event-model`](../event-model))
The canonical Open Timekeeping event types and identifiers: detection, hit, crossing, detector health, timebase status, adapter metadata, runtime control, and the identifier types tying them together. Has no transport assumptions. `no_std` + `alloc`.

**Wire Protocol** ([`otk-protocol`](../otk-protocol), crate `otk-protocol`)
The OTK message envelope: versioning, message types, source identity, sequence numbers, acknowledgements (where applicable), error messages, compatibility rules, and the optional W3C `traceparent` field for cross-wire distributed-trace propagation. Transport-agnostic.

**Frame Codec** ([`frame-codec`](../frame-codec))
How OTK messages are encoded into byte frames and decoded back. Provides length-prefixed stream framing for reliable transports (TCP, Unix socket) and COBS + CRC-16/CCITT-FALSE serial framing for unreliable byte streams (UART, RS-232, RS-485). `no_std` + `alloc`; shared between server and firmware.

**Transport Binding** (per-transport adapter repos)
How OTK frames move over a specific physical or logical link. Each binding lives as a per-transport adapter crate: [`adapter-ingest-tcp`](../adapter-ingest-tcp), [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket), and future serial / USB-CDC / RS-485 / etc. The common abstraction every binding implements is [`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) in `timing_core::ports::inbound`.

**OTK frame**
A single encoded OTK message ready to traverse a transport binding, including whatever delimiter/framing/CRC the binding requires.

**Ingest listener**
A configured endpoint inside a runtime node that accepts incoming OTK frames over one transport binding. A runtime node may run multiple ingest listeners concurrently, each bound to a different transport; all listeners feed the same canonical ingest pipeline.

---

## Event terminology

**Stream**
The canonical event sequence from a single detector. The unit per-detector sequence numbers namespace; the unit consumers subscribe to or range-read; the unit cross-node addressing names as `<node_id>:<detector_id>`. Coarser groupings (per-timing-point, per-session) are queries *over* streams, not separate streams.

**Detection**
A primary observation event produced by a detector at a timing point. The umbrella term for what a detector emits, before any timing-domain interpretation.

**Hit**
A single low-level detection received from a detector, before grouping. A transponder pass over a loop may produce many hits (one per received packet during the pass). Hits are an audit trail and a reprocessing source.

**Crossing**
A derived event representing one passage of an entrant across a timing point. Built by `timing-core` from one or more hits (or accepted directly from a detector that emits already-grouped crossings). The durable primary timing record.

**Pass / Passing**
Synonym for crossing where context demands the more colloquial term.

**Amendment**
A correction event that supersedes a previously-emitted crossing, usually due to a late-arriving hit, an operator correction, or a regrouping replay. Records are immutable; corrections are explicit amendments, not in-place edits.

**Detector health event**
A periodic or state-change event published by a detector adapter about its own health: `healthy`, `degraded`, `failed`, last-seen timestamps, error counters, etc. First-class events, distinct from detection events.

**Timebase status event**
A periodic or state-change event published by a timebase implementation about its sync state, resolution, uncertainty, holdover, and drift. First-class, distinct from detection events.

**Adapter metadata event**
A registration or capability-declaration event published by an adapter at startup and on configuration change.

---

## Timestamp terminology

**`observed_at` / `detected_at`**
When the physical event happened, as best as the detector hardware can determine. Assigned as close to the physical event as possible.

**`received_at`**
When the detector adapter (or the runtime node) received the data. Always later than `detected_at`. Useful for back-pressure / latency analysis; never a substitute for `detected_at` in timing computations.

**`appended_at`**
When the runtime node persisted the event to its event log. Storage time. Used for audit and replication, never as official timing time.

**Timestamping method**
How a timestamp was produced. Required values include: `hardware_event_capture`, `firmware_timer_read`, `adapter_receive_time`, `broker_append_time` (always degraded), `manual_entry`, `replay_recorded`. The honest-provenance rule means an adapter that lacks a capability must say so via this field; pretending precision is forbidden.

**Source attestation**
How the timebase identity carried in an event's provenance was determined. `runtime_discovered` means the adapter read it from its sync source (PTP grandmaster Clock-ID, GNSS constellation, NTP refid chain) and the assertion is hardware-grounded. `operator_asserted` means the identity comes from configuration only (typical for PPS-over-coax fanout, where the detector sees a pulse with no metadata). Consumers needing strict trust filter on `runtime_discovered`; honest-provenance forbids claiming `runtime_discovered` when the source was not actually self-identifying.

**Declared resolution**
The nominal granularity the timestamp is reported at (e.g., 1 ns). Does not imply accuracy at that level; uncertainty is separate.

**Timestamp uncertainty**
The estimated error bound on a timestamp, in the same unit as the timestamp. Reported per timebase and propagated into per-event metadata.

**Sequence number**
A monotonic counter, per detector adapter, attached to every event the adapter emits. Lets consumers detect gaps, duplicates, and reorders without depending on timestamps.

---

## Quality and confidence

**Detection confidence**
A normalized [0..1] score the detector adapter attaches to a detection when its hardware supports it (signal strength, packet success rate, etc.). Honest reporting required; if confidence isn't known, report `unknown`, do not invent.

**Duplicate detection**
Same physical event reported twice by the same detector, or by redundant detectors at the same timing point. Handled by `timing-core` rules and operator policy; deduplication is a timing-domain concern, not a detector-layer concern.

**Event ordering**
The runtime's stream is ordered by append-time within each logical channel. `timing-core` applies ordering rules (e.g., by `detected_at`, with tie-breaking) when computing timing results.

**Detector health**
The state machine describing whether a detector is producing trustworthy data. Reported by the adapter, not inferred by the runtime.

**Sync state**
The disciplined state of a timebase: `locked`, `holdover`, `free-run`, `unsynchronized`, `unknown`. Reported by the timebase, not inferred by detectors.

**Compatibility**
A claim that an implementation conforms to the contracts in this spec, demonstrated by passing the [`conformance`](../conformance) suite.

---

## Downlink and standings terminology

**Uplink**
The subject-to-fabric direction: detector / adapter / producer sends canonical events to a runtime node. This is the data path documented in [architecture.md § The data path](architecture.md). Every OTK deployment has uplink; this is what timing-fabric ingest means.

**Downlink**
The fabric-to-subject direction: the decoder at a timing loop transmits a per-crossing directive back to the just-crossed transponder, the instant the crossing is detected. Distinct from uplink in both content (per-crossing timing data: gap to ahead, gap to behind, last lap, position) and transport (typically 2.4 GHz RF over a separate radio from the loop's LF inductive uplink). Downlink is an opt-in capability per device; see [downlink.md](downlink.md).

**Standings**
The current order of subjects in a session, paired with each subject's lap count. The thing the runtime node distributes to decoders so they can compute gaps locally on every crossing. Standings change rarely (only when an overtake or a completed lap shifts the order); the push is low-frequency and latency-tolerant. NOT to be confused with "race state," which would include flag, safety car, pit window, and other race-control content; standings are timing-derived only.

**Standings push**
The server-to-decoder message family that distributes the current standings. Carried by `StandingsUpdate` messages over the OTK Protocol envelope. Low frequency, eventually consistent.

**Downlink directive**
A single decoder-to-transponder message addressed to one transponder, carrying the per-crossing timing data computed locally by the decoder. Format: `DownlinkDirective { gap_to_ahead, gap_to_behind, last_lap_time, position }`. Computed and transmitted within milliseconds of the physical passage.

**StandingsPublisher**
The outbound port in `timing_core::ports::outbound` that the server-side `StandingsService` emits standings updates through. Adapter implementations target whatever transport reaches the decoders.

**StandingsReceiver**
The inbound port on the decoder firmware application that consumes standings pushes from the server. Updates the decoder's local standings cache.

**DownlinkTransmitter**
The outbound port on the decoder firmware application that emits `DownlinkDirective` messages over the downlink RF link. Adapter implementations target the chosen 2.4 GHz radio.

**DownlinkReceiver**
The inbound port on the transponder firmware application that consumes downlink directives. Hands them to the configured output binding.

**OutputBinding**
The outbound port on the transponder firmware application that bridges downlink directives to the vehicle's in-device output. The canonical implementation is `CanOutBinding` (per the deferred `spec/can-map.md`); alternative implementations include BLE, USB, and on-device display.

**Decoder application**
The first-class application that runs on each timing loop's decoder hardware. Hosts inbound ports (loop-crossing capture, standings receive), outbound ports (uplink to server, instant downlink to transponder), local state (standings cache, crossing-time cache), and real application logic (gap computation on every crossing). Not a dumb sensor.

**Transponder application**
The first-class application that runs on each transponder. Hosts an inbound port (downlink receive) and one or more outbound ports (output bindings to the vehicle). Modest application logic (decode directive, format for chosen binding). Not a dumb display.
