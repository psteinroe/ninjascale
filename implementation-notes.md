# Implementation Notes

Running notes on how the implementation interprets or diverges from the complete-window autoscaling handoff.

## Design decisions

- Metric history is keyed by `{service, local metric name}` and stored as event-time-sorted samples. Immutable per-reconciliation snapshots select the latest sample in each epoch-aligned half-open bucket.
- Retention is computed from configuration as the largest sustained window plus two bucket durations; the direct-construction fallback is 30 seconds. Expired bucket history is pruned during writes and snapshots; one latest sample per series remains available so target policies keep their prior latest-value semantics. Hard limits of 10,000 samples per series and 100,000 total samples evict oldest event-time observations under abnormal load, causing windows to fail closed rather than allowing unbounded memory. The store rejects zero-time, non-finite, future, and already-expired arrivals rather than manufacturing values.
- Window resets record a policy-local event-time cutoff; globally shared metric history remains intact.

## Deviations

- For a reset that occurs inside a bucket, the implementation conservatively requires the full bucket to start at or after the reset cutoff. This is stricter than checking only the bucket end and prevents a pre-scale sample in a straddling bucket from being reused.

## Tradeoffs

- The store keeps sorted per-series slices. Windows are small and bounded by configured retention, making snapshot copies and binary searches straightforward and race-safe at the cost of copying retained history once per service reconciliation.

## Open questions

- None yet.
