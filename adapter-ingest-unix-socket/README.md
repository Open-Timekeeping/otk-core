# adapter-ingest-unix-socket

Unix-socket ingest adapter for Open Timekeeping. Implements
[`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) in `timing_core::ports::inbound` over a local AF_UNIX listener.

> **Status: active.** v0.

## What this is

A thin wrapper around AF_UNIX socket lifecycle code that reuses the OTK protocol
machine and frame codec. It exists primarily as the second transport binding
after `adapter-ingest-tcp`, and proves that the four-layer OTK Protocol stack
is real in code: framing lives in `frame-codec`, handshake and envelope
dispatch live in `ingest-protocol`, and a transport adapter is what's left.

Adding `adapter-ingest-tcp` and this crate side by side, the duplicated code is
~zero: each is a few hundred lines of socket lifecycle around the same two
upstream crates.

## When to use it

- Same-host producer / runtime topologies, where the operator wants process
  isolation without a network stack between them.
- Plugin-like architectures where an external adapter runs in its own
  process for crash isolation but shares the host with the runtime.
- Local development against a long-running `otk-node` instance.

For cross-host producers, use `adapter-ingest-tcp` instead.

## Platform support

Compiled on Unix targets only. On Windows the crate compiles to an empty stub
so the workspace builds end-to-end, but the public types do not exist and
attempting to bind a unix-socket listener at runtime returns an error from
`Node::new`.

## Configuration

When loaded by `timing-node`, listener configuration looks like:

```toml
[[listeners]]
transport       = "unix-socket"
id              = "local-adapters"
socket_path     = "/var/run/otk-node.sock"
max_frame_bytes = 65535
```

A stale socket file at `socket_path` is removed and re-created on bind.

## Dependencies

**Depends on:** `timing_core::ports::inbound`, `otk-protocol`, `event-model`, `frame-codec`,
`ingest-protocol` (all in this workspace).

**Commonly depended on by:** `timing-node`.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
