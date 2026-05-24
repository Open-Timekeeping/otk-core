# timing-node

The deployable Open Timekeeping runtime node. Binary: `otk-node`.

> **Status: active.**

## What this is

A Timing Runtime Node is the deployable server process in the Timing Fabric. It accepts OTK
producer connections over TCP, runs the OTK handshake, decodes canonical events from OTK frames,
persists them to a durable segment log, and routes detections through the timing-domain engine to
produce crossings.

A node does not care where a detector adapter lives: in firmware, an external process, or a plugin.
It cares that incoming data becomes canonical detector events on the wire.

## Running

```sh
# With built-in defaults (binds 0.0.0.0:8463, stores to ./data/)
cargo run --bin otk-node

# With a config file
cargo run --bin otk-node -- --config otk-node.toml
```

Logging level is controlled by the `RUST_LOG` environment variable:

```sh
RUST_LOG=debug cargo run --bin otk-node
```

## Configuration

TOML format. All fields are optional; omitted fields use the built-in defaults.

```toml
node_id     = "venue-main"
api_addr    = "0.0.0.0:8080"
storage_dir = "/var/lib/otk-node/data"

# Plaintext TCP listener (works on every target).
[[listeners]]
transport       = "tcp"
id              = "tcp-main"
bind_addr       = "0.0.0.0:8463"
max_frame_bytes = 65535

# Unix-domain-socket listener for same-host producers. Unix-only:
# parsed on every target but `Node::new` refuses to bind on Windows.
[[listeners]]
transport       = "unix-socket"
id              = "local-adapters"
socket_path     = "/var/run/otk-node.sock"
max_frame_bytes = 65535

# Authentication. Empty allow-lists = open (development) with a
# startup warning. Non-empty = the listed shared secret must match.
[auth]
producer_tokens = ["producer-secret-1"]
api_tokens      = ["api-secret-1"]

# REST/SSE API policy.
[api]
allowed_origins = ["https://dashboard.example.com"]
```

| Field | Default | Description |
|---|---|---|
| `node_id` | `"otk-node"` | Stable identifier for this node in the OTK deployment. |
| `api_addr` | `0.0.0.0:8080` | Address the REST/SSE API server binds to. |
| `storage_dir` | `./data` | Directory for the segment log. Created if absent. |
| `listeners` | one TCP on `0.0.0.0:8463` | One entry per ingest listener; every entry feeds the same canonical ingest pipeline. See [Listeners](#listeners) below. |
| `auth` | empty (open) | Shared-secret allow-lists for producers and API clients. See [Authentication](#authentication) below. |
| `api` | empty (CORS closed) | API server policy (currently CORS allow-list). See [API policy](#api-policy) below. |

### Listeners

Each `[[listeners]]` entry picks one transport binding via the `transport`
discriminator. v0 ships `"tcp"` (every target) and `"unix-socket"`
(Unix targets only).

`transport = "tcp"`:

| Field | Default | Description |
|---|---|---|
| `id` | `"tcp-main"` | Stable id used in metrics and logs. Must be unique across all listeners. |
| `bind_addr` | — | Address the listener binds to. Required. |
| `max_frame_bytes` | `65535` | Maximum incoming frame size in bytes. |

`transport = "unix-socket"` (Unix targets only; configs containing this
variant parse cleanly on Windows but `Node::new` fails the build at
startup time with a clear error):

| Field | Default | Description |
|---|---|---|
| `id` | `"unix-main"` | Stable id used in metrics and logs. Must be unique across all listeners. |
| `socket_path` | — | Filesystem path for the AF_UNIX socket. Required. Created on bind; cleaned up at process exit. |
| `max_frame_bytes` | `65535` | Maximum incoming frame size in bytes. |
| `socket_permissions` | unset (process umask) | Octal mode bits applied to the socket file after bind, e.g. `0o660` for owner+group read/write. TOML accepts octal integer literals natively. Leave unset only when the umask is already tight enough; the default umask is typically too permissive for an ingest endpoint. |
| `force_rebind` | `false` | If `true`, forcibly removes any existing AF_UNIX socket at `socket_path`, even if another process appears to own it. The default is the safe behaviour: probe with `connect()`, refuse to bind if a live listener responds (`AddrInUse`), remove only stale entries from crashed prior runs. Set to `true` only for intentional takeover scenarios (e.g. blue/green deploys where the prior process is being killed in lockstep). |

Mixed configs (e.g. TCP for remote producers + Unix socket for same-host
detectors) are first-class. All listeners feed the same canonical
ingest pipeline.

### Authentication

| Field | Default | Description |
|---|---|---|
| `auth.producer_tokens` | `[]` | Allow-list of shared secrets accepted in `Connect.auth_token`. Empty = accept any Connect (development; warned at startup). Non-empty = reject any Connect whose token is missing or not on the list. |
| `auth.api_tokens` | `[]` | Allow-list of bearer tokens accepted on `/api/v1/*` requests. Empty = unauthenticated (development; warned at startup). Non-empty = require `Authorization: Bearer <token>`. |

Operational endpoints (`/healthz`, `/readyz`, `/metrics`) are always
unauthenticated so external probes and Prometheus scrapers can reach them.

### API policy

| Field | Default | Description |
|---|---|---|
| `api.allowed_origins` | `[]` | CORS allow-list. Empty = no CORS header emitted (browsers will block cross-origin requests). `"*"` = open to all origins. Otherwise: each entry is parsed as an `Origin` value and added to the allow-list (typos are logged at warn, not silently dropped). |

## v0 scope

**What works:**
- One or more TCP ingest listeners (plaintext). One or more Unix-socket
  ingest listeners on Unix targets (cfg-gated; mixed TCP + Unix configs supported).
- OTK handshake: `Connect` / `ConnectAck` with protocol version 0.
- Producer authentication via shared-secret tokens (`Connect.auth_token`,
  `NodeConfig.auth.producer_tokens` allow-list). Empty allow-list = open
  for development with a startup warning.
- `Event` messages decoded and persisted to the segment log (batched: a
  detection and its derived crossings commit in one append).
- `Detection` events routed through `CrossingProcessor`; resulting
  crossings persisted as `OtkEvent::Crossing` log entries.
- Sequence-gate enforcement per-`(producer_id, detector_id)`. Duplicates
  dropped silently; gaps logged and metered. **Restart resume**: on
  `Node::new`, the gate's high-water marks are rebuilt from every
  Detection still in the persisted log so a producer that reconnects
  after a node restart cannot replay a previously-acknowledged
  sequence. The seed runs before any ingest listener accepts a
  connection.
- REST and SSE query API (`/api/v1/status`, `/api/v1/events`,
  `/api/v1/events/stream`) with bearer-token authentication when
  `auth.api_tokens` is non-empty.
- Operational endpoints: `/healthz`, `/readyz`, `/metrics` (Prometheus
  text format, unauthenticated).
- Configurable CORS allow-list for the API.
- Graceful shutdown on `Ctrl-C`; join errors surfaced via `tracing`.
- W3C `traceparent` propagation through `OtkEnvelope`: producers using
  `otk-sdk` auto-extract from the current `tracing::Span` via the
  `tracing-opentelemetry` bridge; the node parents each per-event
  `tracing::Span` on the producer's remote span context, so logs
  stitch across the wire under one trace id in any OpenTelemetry-aware
  backend. With no OTel SDK configured at runtime, the field is
  silently absent and the per-event span becomes a local root, so the
  default ops experience is unchanged.

**Deferred:**
- TLS on the ingest listener (rustls plumbing planned; current advice
  for wire-encryption deployments is to run OTK over an SSH tunnel or
  WireGuard).
- Non-TCP/Unix transport bindings (USB CDC, serial, raw Ethernet).
- Plugin loading (`plugin-api` not yet specified).
- Detector and timebase registry.
- Config hot-reload.

## Ingest is listener-driven

A node hosts a configurable set of ingest listeners, each bound to one
transport binding from the OTK Protocol stack. For v0, TCP and Unix
socket are supported; mixed-listener configs (e.g. TCP for remote
producers + Unix socket for same-host adapters) are first-class. All
listeners feed the same canonical ingest pipeline; downstream behaviour
(sequence-gate, crossing processor, storage) is identical regardless of
which listener accepted the connection.

## What belongs here

- The `otk-node` binary entry point and lifecycle.
- Ingest pipeline for canonical events (multi-listener; future in-process plugin path).
- Listener configuration loading and supervision.
- Event log integration (via the [`port-out-event-log`](../port-out-event-log) trait, backed at v0 by [`adapter-event-log-segment`](../adapter-event-log-segment)).
- Timing-domain orchestration (via [`timing-core`](../timing-core)).
- Configuration loading, signal handling, and graceful shutdown.

## What does not belong here

- OTK Protocol layer definitions: [`event-model`](../event-model), [`otk-protocol`](../otk-protocol), [`frame-codec`](../frame-codec).
- The transport-binding ingest port trait: [`port-in-ingest`](../port-in-ingest). Implemented by the per-transport adapter crates.
- The detector-adapter / timebase trait contracts: [`otk-contracts`](../otk-contracts).
- Specific detector adapter implementations: `adapter-ingest-*` crates ([`adapter-ingest-tcp`](../adapter-ingest-tcp), [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket)).
- Timing-domain logic: [`timing-core`](../timing-core).
- Concrete storage backends: `adapter-event-log-*` crates ([`adapter-event-log-segment`](../adapter-event-log-segment) at v0).
- Frontend applications: future per-app repos (TypeScript stack, not in this Rust workspace).

## Dependencies

**Depends on:** [`event-model`](../event-model), [`otk-protocol`](../otk-protocol), [`frame-codec`](../frame-codec), [`ingest-protocol`](../ingest-protocol), [`port-in-ingest`](../port-in-ingest), [`port-out-event-log`](../port-out-event-log), [`timing-core`](../timing-core), [`adapter-ingest-tcp`](../adapter-ingest-tcp), [`adapter-ingest-unix-socket`](../adapter-ingest-unix-socket) (cfg(unix)), [`adapter-event-log-segment`](../adapter-event-log-segment).

**Commonly depended on by:** runtime end-users via the `otk-node` binary. No other workspace crate depends on `timing-node`; it sits at the top of the dependency graph as the composition root.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
