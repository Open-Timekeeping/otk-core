# otk-core

Cargo workspace containing the shared core crates of the Open Timekeeping server.

## Members

| Crate | Role |
|---|---|
| [`event-model`](event-model/) | Canonical event types and identifiers. No OTK deps. |
| [`protocol`](protocol/) | Wire DTOs: envelope, handshake, message types. Depends on `event-model`. |
| [`timing-core`](timing-core/) | Detection-to-crossing engine. Depends on `event-model`. |
| [`port-in-ingest`](port-in-ingest/) | Inbound port trait: `EventIngestPort`, `IngestSession`. Depends on `event-model`. |
| [`port-out-event-log`](port-out-event-log/) | Outbound port trait: `EventLog`, `LogSubscription`. Depends on `event-model`. |
| [`frame-codec`](frame-codec/) | Frame encoding/decoding for reliable (stream) and unreliable (serial/COBS) transports. `no_std`. Depends on `protocol`. |

## Dependency rules

All members use path deps within this workspace. No member may depend on anything outside `otk-core` except third-party crates.

Downstream crates reference members individually via the workspace git URL:

```toml
event-model        = { git = "https://github.com/Open-Timekeeping/otk-core", package = "event-model" }
protocol           = { git = "https://github.com/Open-Timekeeping/otk-core", package = "otk-protocol" }
timing-core        = { git = "https://github.com/Open-Timekeeping/otk-core", package = "timing-core" }
port-in-ingest     = { git = "https://github.com/Open-Timekeeping/otk-core", package = "port-in-ingest" }
port-out-event-log = { git = "https://github.com/Open-Timekeeping/otk-core", package = "port-out-event-log" }
frame-codec        = { git = "https://github.com/Open-Timekeeping/otk-core", package = "frame-codec" }
```

## License

Apache-2.0
