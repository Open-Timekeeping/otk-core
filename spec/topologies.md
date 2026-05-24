# Networking and deployment topologies

The Timing Fabric supports a small family of deployment shapes. All use the same contracts; the difference is where adapters and runtime nodes physically live, and which transport bindings carry OTK frames between them.

A detector adapter is a logical role, not a deployment constraint. It can be packaged as:

- firmware inside a native detector device that speaks OTK directly
- a standalone external adapter process
- a sidecar / gateway process running near the sensor
- a plugin loaded directly into `timing-node`
- a simulator / test fixture
- a replay / import source

The runtime node does not care where the adapter lives. It cares that incoming data becomes canonical detector events, detector health events, timebase status events, and metadata events. When data crosses a process boundary, it does so as OTK frames over a supported transport binding.

---

## Model A, Native detector producer (firmware speaks OTK)

```text
sensor / loop / RF / firmware
        |
        v
embedded native detector adapter
  (the workspace's `event-model` + `otk-protocol` + `frame-codec`, all `no_std` + `alloc`)
        |
        v   OTK frames
        v   (over USB CDC, TCP, UART, ... depending on the device)
        v
timing-node ingest listener
        |
        v
canonical ingest pipeline
```

The adapter lives in firmware on the detector device itself. The device implements the adapter role directly: it encodes canonical events using `event-model` + `otk-protocol` + `frame-codec` (all `no_std` + `alloc`) and sends OTK frames over whatever transport binding the device supports.

A native detector device is **not required to run a TCP stack**. Any supported transport binding (USB CDC, UART, Ethernet, etc.) is sufficient.

Best for: custom hardware, dedicated detectors with onboard MCU, minimum-latency capture-time timestamping.

---

## Model B, Edge adapter client (external adapter process)

```text
raw sensor / device
        |
        v
external adapter process (Pi / mini-PC / small gateway)
        |
        v   OTK frames (commonly over TCP)
        v
timing-node ingest listener
```

A raw device with a proprietary or proprietary-ish protocol is attached to a small local host. The host runs an adapter process that translates the device-native output into canonical events and publishes OTK frames over a transport binding, typically TCP. The adapter process may use [`otk-sdk`](../otk-sdk)'s `producer` feature for convenience, or build directly on `event-model` + `otk-protocol` + `frame-codec` + a transport binding.

Best for: existing detector hardware with a proprietary protocol; field-replaceable gateways near each sensor.

---

## Model C, Hub-side adapter plugin

```text
raw detector / device  --(serial / USB / Ethernet / custom)-->  timing-node
                                                                    |
                                                                    v
                                                            adapter plugin
                                                                    |
                                                                    v   canonical events
                                                                    v   (submitted in-process)
                                                                    v
                                                            canonical ingest pipeline
```

The runtime node hosts the adapter directly as a plugin (`plugin-api`). The device connects to the same host the runtime node runs on. No OTK frames cross a wire; canonical events are produced in-process and committed to the log.

Best for: single-host venues where the operator wants one process to manage everything.

---

## Model D, Local same-host adapters over Unix socket

```text
adapter process A  -->\
adapter process B  -->|  OTK frames over Unix socket  -->  timing-node
adapter process C  -->/
```

Adapters run as separate processes on the same host as the runtime node and connect over a Unix domain socket. Useful when adapters benefit from process isolation but you do not want a network stack between them and the runtime.

Best for: local development, single-host venues with multiple language stacks, or when adapter crashes should not crash the runtime.

---

## Model E, One timing node per detector stack

```text
detector stack A --> timing-node A   |
detector stack B --> timing-node B   |---> optional upstream aggregation / federation
detector stack C --> timing-node C   |
```

Each detector stack (or each timing point) has its own runtime node. Each node persists locally and exposes APIs. Upstream aggregation / federation is optional and may not exist in the first release.

Best for: physically distributed timing points; survivable-with-network-partition deployments; per-stack ownership.

---

## Model F, Central hub node

```text
many detector producers
  (firmware speaking OTK, external adapter processes, plugins)
                              |
                              v
                       one central timing-node
                       (multiple ingest listeners,
                        e.g. TCP + USB CDC + Unix socket)
```

A single runtime node ingests from many sources concurrently, across multiple transport bindings. All persistence and APIs live on that one node.

Example listener configuration (TOML, matching `timing-node`'s shipped config format). The `tcp` and `unix-socket` variants ship at v0; the `usb-cdc` variant is **illustrative of the planned multi-binding shape**, not yet implemented by the runtime (`ListenerConfig` will reject it at startup):

```toml
[[listeners]]
id        = "tcp-main"
transport = "tcp"
bind_addr = "0.0.0.0:7420"

[[listeners]]
id          = "local-adapters"
transport   = "unix-socket"
socket_path = "/var/run/otk-node.sock"

# Planned (not yet a v0 ListenerConfig variant): host-attached
# USB-CDC detectors. The shape below is the intended TOML once the
# adapter-ingest-usb-cdc crate lands; `timing-node` rejects this
# variant today.
# [[listeners]]
# id        = "start-finish-usb"
# transport = "usb-cdc"
# device    = "/dev/ttyACM0"
```

Best for: venues with reliable LAN, where centralization simplifies ops.

---

## Combining models

Models freely combine. A real deployment might:

- run **Model A** (firmware producers speaking OTK over USB CDC) for some detectors,
- run **Model B** (edge adapter clients over TCP) for legacy hardware on a Pi,
- run **Model C** (hub plugins) for the manual-entry adapter and the CSV replay adapter,
- run **Model D** (local adapters over Unix socket) for a Python-based experimental adapter,
- and **Model F** the rest into a single central runtime node with multiple ingest listeners.

The Timing Fabric is the union of all of it.

---

## What the runtime node cares about

Regardless of model:

- **Inbound data must be canonical events.** Producers and plugins emit canonical detector events, detector health events, timebase status events, and adapter metadata events as defined in [`event-model`](../event-model).
- **Cross-process data is OTK frames.** Across any process boundary, canonical events are wrapped in the OTK message envelope and encoded into frames per [`otk-protocol`](../otk-protocol) and [`frame-codec`](../frame-codec), then carried by a transport binding.
- **Adapter and timebase identity is registered.** Every producer / plugin announces what it is at startup.
- **Health is reported continuously.** Detectors and timebases report their own state; the runtime does not invent it.
- **Sequence numbers persist across reconnects.** Producers that disconnect and return resume cleanly from a known sequence number. The runtime side of the contract survives **node** restarts too: the gate's per-`(producer_id, detector_id)` high-water marks are rebuilt from the segment log on startup, so an acknowledged sequence is still recognised as a duplicate after the node has been bounced.

If those things hold, the runtime node treats every model identically.
