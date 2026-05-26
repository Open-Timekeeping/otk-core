# conformance-fixtures

Canonical test inputs for the Open Timekeeping conformance suite.

> **Status: active (starter corpus).** A small handful of seed fixtures
> ship today; the wider corpus (timebase degradation, multi-detector
> races, pit-lane / start-finish topology streams) grows incrementally
> as the conformance harness gains the drivers to consume them.

## What this is

The shared, data-only Rust crate that holds the canonical fixtures
used by [`conformance`](../conformance) and by any adapter / runtime
author who wants to exercise their implementation against the same
inputs.

Data only: no harness, no assertions, no async runtime. Every public
item is a constructor (`fn` or `const`) returning an `event-model` or
`otk-protocol` value, or a `Vec` of them.

## What lives here

```
src/
├── lib.rs           crate-root re-exports
├── detections.rs    `Detection` constructors (beam_break_at_loop, loop_read_with_rssi)
├── events.rs        `OtkEvent` wrappers + `canon::*` exhaustive samples (one per variant)
├── envelopes.rs     `OtkEnvelope` builders (connect, connect_with_token, data)
└── streams.rs       multi-event scenarios (single_detector_happy_path, reconnect_with_replay)
```

`events::canon` is the exhaustiveness corpus the event-model
round-trip suite iterates: one canonical `OtkEvent` value per
variant, plus a `one_of_each_variant()` helper so adding a new
variant gives every round-trip test free coverage.

## What does not belong here

- Test harnesses, assertion logic, async drivers → [`conformance`](../conformance).
- Implementation code. Fixtures are data; impls go in adapter crates
  or `timing-core`.
- Vendor / venue-specific scenarios. The corpus stays general-purpose
  at the contract level; vendor packs (MYLAPS, RaceResult, etc.) are
  out of scope.

## Why a separate crate

Three reasons:

1. **Dependency hygiene.** Anyone wanting the corpus can compile
   against `conformance-fixtures` without inheriting the conformance
   harness's deps (tokio runtime, mocks). The fixtures crate's
   transitive footprint is just `event-model`, `otk-protocol`, and
   `minicbor`.
2. **Cross-language portability.** A future non-Rust implementation
   (TypeScript SDK, firmware running detector adapters) can read the
   same fixtures either by porting the constructors or by capturing
   their CBOR-encoded outputs as a binary corpus.
3. **Stable versioning.** The fixture surface is a contract: an
   implementation that passed `conformance-fixtures` v0.1.0 should
   keep passing as the harness grows, as long as the data semantics
   don't break. Independent versioning makes that guarantee
   inspectable.

## Adding fixtures

Pick the right module:

- A new sensor variant or detector shape → `detections.rs`.
- A new event-model variant or representative wrapper → `events.rs`
  (and add an entry to `events::canon::one_of_each_variant()` so the
  exhaustiveness loop picks it up).
- A new envelope shape → `envelopes.rs`.
- A new multi-event scenario → `streams.rs`. Document the *what* and
  the *what's being exercised* in the function doc; keep the function
  body composed of helpers from the other three modules.

Avoid one-off fixtures used by exactly one test. Those stay inline in
the test file; the crate is for fixtures with cross-test value.

## Roadmap

The wider corpus the original stub README scoped (still aspirational,
landing as the harness drivers materialise):

- Bad event streams (schema-invalid, missing required provenance,
  illegal sequence numbers).
- Edge-case streams (out-of-order packets, missed hits).
- Timebase degradation scenarios (lock lost mid-event, holdover
  entered, drift excursion, free-run, unknown state).
- Multi-detector scenarios (redundant detectors, split-second close
  crossings, transponder conflicts).
- Pit-lane / start-finish multi-timing-point scenarios.
- Expected canonical outputs (post-`timing-core` crossings, laps,
  sectors) for each input, so a third-party implementation can
  diff against ground truth without re-running the engine.

## Dependencies

**Depends on:** [`event-model`](../event-model),
[`otk-protocol`](../otk-protocol), `minicbor`.

**Commonly depended on by:** [`conformance`](../conformance).
Third-party implementers exercising their own runtime / adapter
against the corpus can depend on this crate directly.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
