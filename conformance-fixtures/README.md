# conformance-fixtures

Shared fixtures for the Open Timekeeping conformance suite.

> **Status: stub.** Fixtures grow alongside [`conformance`](../conformance).

## What this is

The canonical collection of test inputs and expected outputs used by [`conformance`](../conformance) and by adapter / runtime authors who want to exercise their implementations against realistic Open Timekeeping scenarios.

## What belongs here

- Sample event streams (well-formed, happy-path).
- Bad event streams (schema-invalid, missing required provenance, illegal sequence numbers).
- Edge-case streams (duplicate hits, late packets, out-of-order packets, reconnect-and-resume, missed hits).
- Timebase degradation scenarios (lock lost mid-event, holdover entered, drift excursion, free-run, unknown state).
- Multi-detector scenarios (redundant detectors, split-second close crossings, transponder conflicts).
- Pit-lane / start-finish multi-timing-point scenarios.
- Expected canonical outputs (post-`timing-core` crossings, laps, sectors) for each input.

## What does not belong here

- Test harnesses or assertion logic → [`conformance`](../conformance).
- Implementation code, fixtures are data only.

## Dependencies

**Depends on:** [`event-model`](../event-model), [`spec`](../spec).

**Commonly depended on by:** [`conformance`](../conformance), runtime / adapter test suites. A replay-from-fixtures simulator is planned (would extend [`producer-simulated`](../producer-simulated) or live alongside it) but does not exist yet.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
