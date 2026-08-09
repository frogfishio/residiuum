# Performance qualification — current state

Date: 2026-08-04 (durable smart-client update: 2026-08-09)
Status: **authoritative rebaseline**

> The durable smart-client write path has since gained deployment-wide group
> commit and crash-safe outcome reconciliation. Current implementation truth,
> residuals and the next measurement gate are recorded in
> [DURABLE_GROUP_COMMIT_BASELINE_2026_08_09.md](DURABLE_GROUP_COMMIT_BASELINE_2026_08_09.md).
> The historical rates below do not measure that path.

## Bottom line

Residiuum has demonstrated `~100K–130K`-class execution on substantial portions
of the write path. The fast-path hypothesis is credible.

The peer benchmark does not distinguish acknowledged-write time from
drain/seal/close time. Its `ops_per_sec` includes finalisation, boundary timing
is captured before sealing, and the `seal_active()` result is discarded. The
repeated `~9.5K acknowledged puts/s` interpretation is not established.

## Evidence retained

| Path | Observed rate | Meaning |
|---|---:|---|
| Same-sized raw growing appends | `~129K ops/s` | Device/OS accepts the approximate append shape |
| Residiuum with media discarded or `/dev/null` | `~115K–126K` | Cooking/framing alone is not a `10K` ceiling |
| Residiuum overwriting existing pages | `~96K` | Real file writes can remain in the high band |
| Real append with indexing disabled | `~30K` | Real append introduces material cost |
| Full benchmark lifecycle | `~9.5K` | Workload plus finalisation is slow; ack rate is not isolated |

These are diagnostic observations, not product SLOs. Raw evidence is archived.

## AWO truth

Q1 supplies useful concurrent-admission and reopen-correctness foundations.
However, the independent-write collector batches by queue depth, delay and
maximum entries. Adaptive `select_plan` is not wired into that collector.
Static and Adaptive therefore share mechanics on the independent-write path.
Existing Q2 evidence does not establish adaptive decision quality.

AWO-Q2 is paused. No default-on or adaptive-performance claim is permitted.

## Claim boundary

Allowed: substantial headroom exists; persistent append and finalisation are
the measured region of interest; saturated collection can amortise durability
barriers; Q1 correctness tests exist.

Not allowed: `~9.5K` is the acknowledged-write ceiling; the `12×` gap has an
exact `4× × 3×` decomposition; Adaptive beats Static; or AWO is qualified.

## Decision

Do not create another explainer. Execute [NEXT_MEASUREMENT.md](NEXT_MEASUREMENT.md),
update this page from executable evidence, and only then choose an optimisation.
