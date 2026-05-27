# Architecture

Conceptual model of Open Timekeeping. Implementation-neutral.

For terminology, see [glossary.md](glossary.md). For deployment shapes, see [topologies.md](topologies.md). For what conformance means, see [compatibility.md](compatibility.md). For the bidirectional half (fabric-to-decoder standings push, decoder-to-transponder instant downlink, in-vehicle CAN-out), see [downlink.md](downlink.md).

---

## Open Timekeeping is a full-stack system

Open Timekeeping is not just an integration framework. It is a full-stack timing system spanning:

- physical sensors / detector hardware
- firmware for native detector devices
- detector adapter implementations
- timebase / clock sync
- runtime nodes
- timing core (the timing-domain engine)
- storage
- APIs
- apps (live timing, diagnostics)
- conformance

Each layer has a defined boundary and contract; layers above can be replaced without reaching below.

---

## The data path

```text
Sensors / detector hardware / simulators / manual inputs / replay sources
        |
        v
Detector adapter role
  (in firmware, in an external process, or in a timing-node plugin)
        |
        v   canonical Open Timekeeping events
        v   (detection, hit, crossing, detector health,
        v    timebase status, adapter metadata, runtime control)
        v
Timing Runtime Node
  - ingest listeners (per-transport-binding)
  - canonical ingest pipeline
  - event log / stream
  - detector registry
  - timebase registry + clock monitor
  - timing-core orchestration
  - projections + storage
  - APIs
  - diagnostics
        |
        v
Apps, APIs, external consumers
```

Four things to note:

- The runtime node does not care **where** an adapter lives, only that incoming data is canonical Open Timekeeping events.
- Timebase is a first-class peer of detector adapters, not a private implementation detail of detector adapters.
- Storage is a port. So are each of the protocol layers below. So is the plugin API. Open Timekeeping is built around explicit boundaries.
- The path above is the **uplink** direction (subject to fabric). A symmetric **downlink** direction (fabric to subject: server pushes standings to decoders; decoders compute gap-to-ahead and gap-to-behind locally on every crossing and push the result instantly back to the just-crossed transponder; transponder bridges to vehicle CAN) is part of OTK's full-stack scope. The decoder and transponder firmwares are first-class applications hosting real application logic, not dumb peripherals. See [downlink.md](downlink.md).

---

## OTK Protocol is transport-independent

The **OTK Protocol** is the transport-independent wire protocol used by Open Timekeeping components to exchange canonical timing messages. It is **not** TCP, not HTTP, not a specific socket type. It is defined as four stacked layers; each layer has its own contract and its own crate. The top three (Event Model, Wire Protocol, Frame Codec) live as crates in this workspace; transport-binding crates live in per-transport adapter crates.

```text
+----------------------------------------------------+
| Event Model                                        |
|   Canonical Open Timekeeping events and identifiers|
|   (crate: event-model)                             |
+----------------------------------------------------+
| Wire Protocol                                      |
|   OTK message envelope: versioning, message types, |
|   source identity, sequence numbers,               |
|   acknowledgements, error messages, compatibility  |
|   (crate: otk-protocol)                            |
+----------------------------------------------------+
| Frame Codec                                        |
|   How OTK messages are encoded into frames and     |
|   decoded from frames; stream-oriented and         |
|   resynchronizable framing                         |
|   (crate: frame-codec)                             |
+----------------------------------------------------+
| Transport Binding                                  |
|   How frames move over a specific link             |
|   (adapter-ingest-tcp, adapter-ingest-unix-socket, |
|    future adapter-ingest-serial / -usb-cdc, ...)   |
+----------------------------------------------------+
```

A fifth crate, [`ingest-protocol`](../ingest-protocol), holds the transport-agnostic server-side state machine that drives the handshake and post-handshake envelope dispatch on top of those four layers. Per-transport ingest adapters consume `frame-codec` + `ingest-protocol` and are reduced to socket lifecycle plus byte I/O.

The layering matters:

- A device that can emit OTK frames directly already implements the adapter role; no node-side adapter is required.
- A device that cannot emit OTK frames needs an adapter (firmware, external process, or plugin) that translates device-native output into Open Timekeeping events and frames.
- A runtime node can ingest from multiple transport bindings concurrently. Internally, every transport binding feeds the same canonical ingest pipeline.
- Adding a new transport binding (e.g., RS-485, CAN, raw Ethernet) is a new crate at the Transport Binding layer. The Event Model, Wire Protocol, and Frame Codec do not change.

---

## The contracts every implementation meets

Open Timekeeping is held together by a small set of normative contracts. They live as crates in this workspace, plus a set of port traits inside [`timing-core`](../timing-core) that adapters and the API layer compile against.

1. **`event-model`**, canonical event/data shapes. Every event, every identifier, every provenance block in the system is defined here. No transport assumptions; `no_std` + `alloc`.
2. **`otk-protocol`** (crate `otk-protocol`), the OTK message envelope: versioning, message types, source identity, sequence numbers, acknowledgements where applicable, error messages, and compatibility rules. Not bound to any single transport.
3. **`frame-codec`**, encode/decode of OTK messages into byte frames (and back). Provides both length-prefixed stream framing (reliable transports: TCP, Unix socket) and COBS + CRC-16/CCITT-FALSE serial framing (unreliable byte streams: UART, RS-232, RS-485). `no_std` + `alloc`.
4. **`ingest-protocol`**, the transport-agnostic server-side state machine consumed by ingest adapters: handshake negotiation (with pluggable authoriser), post-handshake envelope validation and message-type dispatch.
5. **`otk-contracts`**, the universal trait contracts a third-party implementer of a *producer-side* role compiles against: `DetectorAdapter` and `Timebase`. Dependency-light (no `tokio`, no `minicbor` direct dep) so a vendor adapter can target this surface without inheriting the SDK's transport stack.

Server-side port traits, all inside [`timing-core`](../timing-core):

- **`timing_core::ports::inbound::EventIngestPort`**, the common abstraction every transport-binding ingest adapter implements (listener accept loop, session lifecycle, error vocabulary). Implementers: `adapter-ingest-tcp`, `adapter-ingest-unix-socket`.
- **`timing_core::ports::inbound::EventQueryPort`**, the API-shaped query surface (`latest_offset`, `read_events`, `subscribe_events`) the REST/SSE layer depends on. Implemented by `timing-core::services::EventIngestService`; alternative implementers (an offline analyzer, a replay tool) can be substituted at the composition root.
- **`timing_core::ports::outbound::EventLog`**, the persistence boundary. The v0 backend is [`adapter-event-log-segment`](../adapter-event-log-segment); alternatives plug in behind the same trait.
- **`timing_core::ports::outbound::IngestMetrics`**, the counter-emission boundary for the application service. v0 implementer: the Prometheus text-format `Metrics` type in `timing-node`. `NoopIngestMetrics` ships in `timing-core` for tests and metrics-less embedders.

**Boundary enforcement.** Adapters and conformance crates depend on `timing-core` as a whole, but a per-crate `clippy.toml` denies imports of `timing_core::domain::*` and `timing_core::services::*`. The runtime composition root (`timing-node`) is the only crate that touches every layer (domain + services + ports). When the port traits lived in their own crates (`port-in-ingest`, `port-in-query`, `port-out-event-log`) the equivalent fence was enforced by Cargo's dependency graph; folding them into `timing-core` reduced ceremony at the cost of trading dep-graph enforcement for the convention-enforced clippy fence.

**`plugin-api`** is reserved for in-process plugin loading. Not yet specified; see [`open-questions.md`](open-questions.md).

If an implementation satisfies the contracts that apply to its role and passes the corresponding [`conformance`](../conformance) suite, it is Open Timekeeping compatible.

---

## Roles and where they live

Most contracts live as crates in this single Cargo workspace.

### Protocol-layer and contract crates
| Crate | Role boundary |
|---|---|
| `event-model` | Canonical event types and identifiers. No transport assumptions. `no_std` + `alloc`. |
| `otk-protocol` | OTK message envelope: versioning, sequencing, acks, errors. Transport-agnostic. |
| `frame-codec` | Encode/decode of OTK messages into byte frames. Stream framing (length-prefix) and serial framing (COBS + CRC-16/CCITT-FALSE). `no_std` + `alloc`. |
| `ingest-protocol` | Transport-agnostic server-side state machine: handshake negotiation, envelope validation, message-type dispatch. Pluggable `ConnectAuthoriser` for runtime auth. |
| `otk-contracts` | Universal trait contracts for producer-side roles: `DetectorAdapter`, `Timebase`. Dependency-light surface for third-party implementers. |
| `timing-core` | The hexagon. Three sibling modules: **`domain/`** (`Crossing`, `CrossingProcessor`, `SequenceGate`, `ProcessorConfig`), **`ports/inbound/`** (`EventIngestPort`, `EventQueryPort`) + **`ports/outbound/`** (`EventLog`, `IngestMetrics`) — the typed boundary adapters and the API layer compile against, **`services/`** (`EventIngestService`, which implements `EventQueryPort` and takes the outbound ports as constructor arguments). Adapter crates and `conformance` depend on `timing-core` for the port types only; a per-crate `clippy.toml` denies reaching into `domain` or `services`. The composition root (`timing-node`) is the only crate that touches every layer. |

### Adapter and runtime crates

| Role | Crate | Role boundary |
|---|---|---|
| Standard / conceptual model | [`spec`](../spec) | What Open Timekeeping means. (Docs, not a Rust crate.) |
| TCP ingest adapter | [`adapter-ingest-tcp`](../adapter-ingest-tcp) | OTK frames over TCP. |
| Unix-socket ingest adapter | [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket) | OTK frames over AF_UNIX (same-host producer/runtime). |
| Segment-file event log | [`adapter-event-log-segment`](../adapter-event-log-segment) | The v0 storage backend; implements `timing_core::ports::outbound::EventLog`. |
| Producer/consumer SDK | [`otk-sdk`](../otk-sdk) | Producer-side `connect`/`send_event` helpers, consumer HTTP/SSE client, builders. Re-exports `otk-contracts`. |
| Simulated producer | [`producer-simulated`](../producer-simulated) | `otk-simulator` binary: synthetic detector events. |
| Runtime node | [`timing-node`](../timing-node) | The deployable server (`otk-node` binary) and composition root. Builds the storage and ingest adapters, constructs `timing-core`'s `EventIngestService` with those adapters injected, supervises listeners, and hosts the REST/SSE API (`/api/v1/...`) plus operational endpoints (`/healthz`, `/readyz`, `/metrics`). Owns operational concerns the domain does not: shared-secret auth, config hot-reload, Prometheus exposition, trace-context propagation. |
| Conformance | [`conformance`](../conformance), [`conformance-fixtures`](../conformance-fixtures) | Verifies any implementation. |

### Planned / not yet shipped

| Role | Status |
|---|---|
| Serial / USB-CDC ingest adapters | Planned. Re-use `frame-codec` (serial mode) + `ingest-protocol`. |
| Live-timing and diagnostics apps | Planned. |
| Embedded toolkit (RP2040 / STM32 targets, HAL, native detector firmware crates) | Planned. The protocol-layer crates firmware would consume (`event-model`, `otk-protocol`, `frame-codec`) already ship as `no_std` + `alloc`, so a `target-*` firmware crate is the only missing piece. |
| Plugin loading (`plugin-api`) | Open question; see [`open-questions.md`](open-questions.md). |
| Additional storage backends (embedded SQL, server SQL, object-store-tiered) | Plug behind `timing_core::ports::outbound::EventLog` when there's a concrete need. |

`timing-core` is the *domain library + application service*. `timing-node` is the *binary + composition root*. Do not conflate them: `timing-core` owns what an OTK runtime *does* (group detections into crossings, gate by sequence number, persist and serve events through injected ports); `timing-node` owns how a deployment *boots* (load config, build adapters, supervise listeners, expose HTTP). An alternative composition root (e.g. an offline analyzer, a replay tool, an embedded variant) builds its own adapters and constructs `EventIngestService` directly without depending on `timing-node`. The producer-side `Producer` connection helper in `otk-sdk` is a *convenience library*; producers may use it, or build directly on `event-model` + `otk-protocol` + `frame-codec` + a transport binding.

---

## Where the adapter role can live

The detector adapter is a logical role, not a deployment constraint. Open Timekeeping supports these cases as first-class:

**Case 1, device speaks OTK directly.**

```text
sensor / detector firmware
        |
        v
firmware-side detector adapter
        |
        v   OTK frames (over USB CDC, TCP, UART, ...)
        v
timing-node ingest listener
        |
        v
canonical ingest pipeline
```

The adapter role is implemented in firmware. No node-side detector adapter is required. The firmware encodes canonical events using `event-model` + `otk-protocol` + `frame-codec` (all `no_std` + `alloc`) and sends them over a supported transport binding.

**Case 2, device does not speak OTK; an external adapter translates.**

```text
raw detector / decoder output
        |
        v
adapter process (or adapter plugin)
        |
        v   OTK frames
        v
timing-node ingest listener
```

The adapter translates the device-native output into canonical Open Timekeeping events and publishes frames over a transport binding.

**Case 3, adapter runs inside timing-node as a plugin.**

```text
raw device (USB / serial / GPIO / Ethernet / custom)
        |
        v
timing-node plugin (adapter)
        |
        v   canonical events (in-process)
        v
canonical ingest pipeline
```

The adapter is loaded as a plugin via `plugin-api`. The device connects directly to the host the runtime node runs on. No OTK frames cross a wire; canonical events are submitted in-process.

**Case 4, adapter runs outside timing-node.**

```text
raw device or simulator
        |
        v
external adapter process
        |
        v   OTK frames (over any supported binding)
        v
timing-node ingest listener
```

The external adapter may use [`otk-sdk`](../otk-sdk)'s `producer` feature for convenience, or build directly on `event-model` + `otk-protocol` + `frame-codec` + a transport binding.

The four cases combine freely in a single deployment.

---

## Timing-node ingest is listener-driven

A runtime node hosts one or more **ingest listeners**, each bound to one transport binding. Every listener feeds the same canonical ingest pipeline.

Example configuration shape (TOML, matching `timing-node`'s shipped config format):

```toml
[[listeners]]
id        = "tcp-main"
transport = "tcp"
bind_addr = "0.0.0.0:7420"

[[listeners]]
id        = "start-finish-usb"
transport = "usb-cdc"
device    = "/dev/ttyACM0"

[[listeners]]
id          = "local-adapters"
transport   = "unix-socket"
socket_path = "/var/run/otk-node.sock"
```

For v0, the prioritized transport bindings are:

- OTK over TCP (default for IP-capable producers, gateways, and server-to-server cases)
- OTK over USB CDC / serial (native firmware and locally attached devices)
- OTK over Unix socket (same-host adapters and local development)

Other bindings (raw Ethernet, CAN, RS-485, QUIC, MQTT, etc.) are deferred until there is a concrete need.

---

## Durability and resume

Open Timekeeping is designed so that a temporary disconnection at any boundary in the data path does not cause data loss. The guarantees are symmetric on both sides of the runtime node, and they are part of the conformance contract, not best-effort behavior.

**Producer-side resume (detector adapter to timing-node).**
A detector adapter that disconnects from a runtime node and later reconnects must rejoin, re-register, and resume from a known sequence number with no gaps and no duplicates. The adapter is responsible for buffering its own output locally for the duration of the outage. Firmware, external adapter processes, and plugin-loaded adapters all meet this contract. Sequence numbers are strictly monotonic per detector and persist across reconnects. This is the analogue of a detector device storing passings locally while the upstream link is down.

**Consumer-side resume (timing-node to downstream consumer).**
A downstream consumer (live timing app, diagnostics app, external integrator, federated node) that disconnects from a runtime node and later reconnects must be able to read every event the node accepted during the outage, in order, with no gaps and no duplicates, provided the events are still within the configured retention window. The runtime node is responsible for buffering on the consumer's behalf; consumers do not buffer for each other. The durable event log lives behind the [`EventLog`](../timing-core/src/ports/outbound/event_log.rs) outbound port in `timing-core` (in v0, [`adapter-event-log-segment`](../adapter-event-log-segment) is the only implementer). When a consumer requests a range that has fallen out of retention, the node returns a structured `retention_expired` error rather than silent gaps. This is the analogue of a central timing server holding all received events for the scoring application to pick up when it reconnects.

**What this means for the adapter role.**
Adapters are responsible for the producer-to-node link. They do not buffer on behalf of downstream consumers. A detector adapter that has delivered an event to the runtime node has done its job; the runtime node owns durability from that point forward.

**What this means for retention.**
Retention is the maximum outage a consumer can survive without help. A consumer that may go offline for hours during a session must have a retention window longer than that outage. Retention is configurable per node; the default policy and its trade-offs are settled in [`timing-node`](../timing-node).

**Cross-node durability is intentionally out of scope for v0.**
Replication and consensus across runtime nodes are deliberately not in the v0 design. The reasoning: timing data has no cross-event invariants that a consensus protocol exists to protect; per-detector sequence numbers make events idempotent at the source; and order matters only within a logical channel, not across the cluster. Within a single runtime node, durability is covered by the segment-log backend ([`adapter-event-log-segment`](../adapter-event-log-segment)) and the producer/consumer resume contracts above. Cross-node redundancy is an operational concern at v0: RAID-1 on the timing host, frequent rsync to a peer machine, and the per-detector-stack topology in [topologies.md](topologies.md) (each stack survives on its own).

If a future deployment genuinely requires cross-node durability (broadcast-grade SLAs, hot-standby with zero data loss, multi-venue federation), the intended path is **producer-side dual-write**: detector adapters publish OTK frames to two listeners at once, each node commits independently to its own segment log, and deduplication relies on the existing per-detector sequence numbers. This composes with the contracts already in place; it does not require a consensus protocol. Open Timekeeping does not reach for Raft / Paxos / KRaft to solve this problem.

---

## Multi-node deployments are partitioning, not clustering

A deployment may have one runtime node or many. Multi-node is first-class in the design: required for geographically distributed timing (rally stages, road races, point-to-point, large venues with unreliable backhaul) and useful for failure-isolation and organizational ownership at single venues.

The shape is **partitioning, not clustering**:

- Each node is sovereign for the detectors that connect to it. There is no shared write-side state across nodes.
- Per-detector sequence numbers make each detector's stream globally identifiable without coordination.
- "Federation" is read-side union: an aggregator (or any consumer) reads from multiple nodes and presents the union. There is no consensus or coordinated commit.
- Cross-node durability is not provided by clustering. See "Cross-node durability" above for the operational and dual-write paths.

### Bootstrap directory

To make multi-node deployments navigable without hand-coding node addresses into every producer and consumer, a node responds to a **bootstrap query** with a directory of the deployment it belongs to:

- Each known node, by id and reachable address(es) per transport binding.
- For each node, what it serves (which detectors, which timing points, which timebases).
- Per-node capabilities (live subscribe, range read, aggregation).

Any node can answer the bootstrap query for the deployment it knows about. The bootstrap response is not cluster-state in the consensus sense; it's a directory. A producer or consumer needs the address of at least one node, queries it, and finds the rest.

### Cross-node addressing

Stream and timebase identifiers are addressable across nodes using hierarchical naming. The operator assigns a unique id per node within a deployment (typically a small set, easy to keep unique). Local entity ids (`detector_id`, local timebase id) can repeat across nodes; the addressing layer disambiguates as `<node_id>:<local_id>`.

A **stream** in Open Timekeeping is the canonical event sequence from a single detector. The unit per-detector sequence numbers already namespace; the unit consumers subscribe to or range-read; the unit cross-node addressing names as `<node_id>:<detector_id>`. Coarser groupings (per-timing-point, per-session) are queries *over* streams, not separate streams.

---

## Timebases are physical references

A **timebase** in Open Timekeeping is the upstream physical time reference that one or more detectors are disciplined against. It is **not** the local clock-and-PTP-daemon view of any single host. A node's own clock matters only for `appended_at` (audit timestamp); it does not enter timing math.

Examples of timebases:

- A GNSS receiver at a venue, with PPS distributed by coax to many detectors. One timebase (`venue-x:gps-pps`).
- A PTP grandmaster on the management VLAN that detectors with PTP-capable hardware slave to. One timebase, identified by the grandmaster's Clock Identity.
- A constellation of per-detector GNSS receivers. All reference GPS system time; one timebase (`gps-system-time`).

### Deployment declares timebases; detectors claim one

Deployment configuration declares the timebases that exist (id, kind, expected uncertainty). Each detector's configuration declares which timebase it claims to be disciplined from. Two detectors that declare the same timebase identity assert their timestamps are directly comparable.

### Runtime status is the truth; config claim is a label

What consumers actually trust is the timebase status event the detector emits at runtime: locked / holdover / free-run, current measured uncertainty against the reference, last successful sync. Config is the *claim*; status events are the *truth*. A mismatch (config says locked, status says unsynchronized) surfaces as degraded state, not silent fiction.

### Runtime-discovered identity is preferred where possible

For some sync mechanisms, the timebase identity can be discovered at runtime rather than asserted by operator label:

| Reference | Identity is... |
|---|---|
| **PTP / White Rabbit** | Discoverable. Grandmaster Clock-ID (EUI-64) is in every Announce message. Adapter reads it from the local PTP daemon. Genuine cross-detector identity. |
| **GNSS-per-device** | Discoverable in a soft sense. Receiver reports constellation in use; two GNSS-locked devices share the constellation by definition. |
| **NTP** | Partially discoverable. Adapter reads server IP + stratum-1 refid (e.g., `GPS`, `PPS`) via `chronyc tracking`. |
| **PPS over coax** | Not discoverable. The detector sees a pulse on a wire; no metadata. Operator label is the only option. This is the dominant case in motorsport timing today. |

The design supports both paths. Event provenance carries the timebase id *and* a source-attestation field distinguishing `runtime_discovered` from `operator_asserted`. Consumers needing strict trust filter on `runtime_discovered`; PPS-coax deployments accept `operator_asserted` and rely on operational discipline.

### Hardware timestamping is the primary case

The high-trust path is `hardware_event_capture`: the detector's MCU or NIC stamps the event at the moment the physical event occurs, against its own hardware counter, which is disciplined from a timebase reference. Adapter-receive-time and node-append-time are honestly-degraded fallbacks and report themselves as such via [`timestamping_method`](glossary.md).

### Signal-level sync is operator/OS work

Running `chronyd` or `ptp4l`, wiring PPS coax, choosing the GNSS receiver model, configuring PTP-capable NIC firmware: none of this is OTK Protocol's job. OTK declares timebase identity and expectations in config; it observes and reports actual sync state at runtime; it does not drive the sync daemons or distribute the sync signals.

---

## Embedded support is part of the same model

Embedded support is **not** an alternative path that bypasses detector adapters. It is the lower-level use of the same the workspace crates from firmware: `event-model`, `otk-protocol`, and `frame-codec` are all `no_std` + `alloc`, so a native detector built around them satisfies the same `DetectorAdapter` contract (from `otk-contracts`) as a process running on Linux, and emits OTK frames over a transport binding the device supports (USB CDC, TCP, UART, etc.). The HAL/target/embedded-HAL toolkit for building such firmware is planned (see "Planned / not yet shipped" above) but not part of the current shipped surface.

This means:

- The reference firmware emits canonical Open Timekeeping events as OTK frames.
- Third-party hardware can be Open Timekeeping compatible by using these crates and passing the conformance suite. No special "embedded path" is required.
- The conformance suite has the same expectations for a native detector as for a Linux producer process.
- A detector device is not required to have a TCP stack. Any supported transport binding is sufficient.

---

## Operating principles

These principles bind every part of the system:

- **Honest provenance over false precision.** If timestamp resolution, uncertainty, or sync state is unknown, report it as unknown or degraded. Never pretend a nanosecond representation means nanosecond accuracy.
- **Records are immutable; corrections are amendments.** Once a canonical event is appended, it is never edited in place. Corrections are explicit `*.amended.v1`-style events that supersede a prior offset.
- **Detectors observe. Timing-core interprets. Apps present.** Each layer respects the others' boundaries.
- **Mechanism vs policy.** The event model and runtime record what happened and how trustworthy it is. They do not filter, suppress, or demote events based on quality thresholds. Policy (what timestamping method is acceptable for official results, whether a degraded timebase is trusted, what confidence score is required, whether raw streams are exposed to consumers) is decided at the app, scorer, or operator layer. Every event carries honest provenance so that any policy can be applied consistently downstream.
- **AI is advisory, not authoritative.** Any AI insight published into the stream is marked `official: false`. Results, penalties, and flags are not AI-owned.
- **Domain neutrality in the timing layer.** The runtime node, `timing-core`, and the adapter / timebase contracts know about subjects, timing points, detections, and crossings, not about laps, drivers, races, or flags. Domain concepts live in the app layer.
- **Transport-independent protocol.** OTK has canonical messages and frames that can travel over multiple bindings. Detectors are not required to speak TCP, and HTTP is not the detector ingest data plane.
