# RQL qualification recovery baseline — 2026-08-09

Status: **repository recovery authority** · Gate 1 **not passed** · Q5 **HOLD**

This document reconstructs the RQL programme after loss of the working Kanban.
It records the state found in the repository at `9852717`, including the
committed Q3/Q4 tranche and `4f738ce` concurrency work. It is the baseline for
new RQL labor until superseded by a dated, reviewed baseline.

Authority remains
[RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md).
This file records delivery truth; it does not amend the frozen target.

## 1. Objective

Gate 1 requires all three of:

1. complete Tier-A practical document-query capability;
2. semantic correctness across oracle, scan, admitted index, reopen and
   comparator paths; and
3. competitive controlled performance without weaker durability, hidden debt,
   false absence or false completeness.

No performance campaign is admissible until its query family is Q3-green and
the Q4 harness executes equivalent real work on the frozen lane pairing.

## 2. Reconstructed package state

| Package | Baseline state | Measured truth |
|---|---|---|
| Q0 target freeze | `accept` | only accepted package |
| Q1 corpus | `active` / not accepted | 153 cases; A=147/B=2/C=4; structural floors green |
| Q2 capability | `active` / blocked | 134/147 execute; 2 expected refusals; 11 gaps |
| Q3 semantics | `in_review` / partial | 101/147 Tier-A oracle+differential green; 5 unsupported; 41 outside green denominator |
| Q4 harness | `in_review` scaffold | 12 logical smoke cells; no real comparator execution |
| Q5 baseline | `not_started` / HOLD | no competitive repetitions |
| Q6 optimisation | `not_started` | no Q5 bottleneck queue |
| Q7 final qualification | `not_started` | no Gate-1 decision pack |

Headline ratios at this baseline:

- Tier-A product execution: **134/147 (91.2%)**;
- Tier-A closed outcome including two expected refusals: **136/147 (92.5%)**;
- Tier-A Q3 oracle+differential green: **101/147 (68.7%)**;
- accepted packages: **1/8**; competitive evidence: **none**.

## 3. Verification at baseline

The following passed on 2026-08-09:

| Command | Result and scope |
|---|---|
| `bash scripts/verify-rql-q1-corpus.sh` | PASS; schema and distribution floors only |
| `bash scripts/verify-rql-q3.sh` | PASS; Q3 labor floor, not package exit |
| `bash scripts/verify-rql-q4-harness.sh` | PASS; scaffold structure, not cross-engine or competitive readiness |
| Q4 crate tests | 51 default / 53 with `residiuum-embedded` |

The word PASS must always retain the scope above. A floor/scaffold pass must not
be presented as package acceptance.

## 4. Blocking delta before Q5

### 4.1 Q1 freeze

- replace at least two invented domains with versioned real dogfood-derived
  cases (the available Gremlin dataset is a candidate source);
- populate canonical QVM identity for every applicable case;
- complete principal comparator/corpus review and freeze the corpus.

### 4.2 Q2 capability and architecture

- computed conditional projection: 5 cases;
- textual `after $cursor`: 5 cases;
- enrich/within residual: 1 case;
- Full RQL over the server wire (`Q2-BLOCK-FULL-WIRE`);
- principal Decision-0 disposition before the one-runtime exit claim.

### 4.3 Q3 completion

- replace the `>=90` labor floor with complete Tier-A accounting for package
  exit;
- bring the 40 non-`oracle_rule` and one no-source cases into an explicit green,
  expected-refusal or blocking-defect outcome;
- clear the five source-cursor cases after Q2;
- strengthen semantic independence where oracle and product currently share
  compiler/predicate evaluation;
- extend product reopen/index/damage coverage to every admitted family.

### 4.4 Q4 product faithfulness

Completed scaffold labor: F1-F11, including deterministic variants and actual
OS worker creation. Remaining pre-Q5 work:

| Item | Required correction |
|---|---|
| F12 | make `many` enrichment generate and return genuine 1:N matches |
| F13 | drain the product continuation API for first/deep cursor cells |
| F14 | campaign-grade evidence fields and strict schema/verifier |
| F15 | explain identity independent from result identity |
| F16 | explicit refused/unsupported outcome status |
| Product concurrency | concurrent clients against one prepared deployment, not logical simulators |
| Product indexes | create/verify declared indexes and record the actual chosen plan |
| Mixed R/W | perform the stated 90/10 and 70/30 operations on product/comparator stores |
| Lifecycle | prepare and verify warm, reopen, larger-than-memory, rotation/compaction and damage states |
| Comparators | execute CBL and Mongo adapters; execute Residiuum server lane |
| Metrics | CPU/RSS, physical I/O/amplification, index cost, correct aggregate throughput and raw repetitions |

F17 repository hygiene: move `ringtail-sda-starter.zip` out of the tracked
source tree without losing the hand-off artifact.

### 4.5 Delta completed after reconstruction

The historical counts above describe the recovery point, not current labor.
The subsequent tranche has closed the listed F12–F16 mechanics, brought Q3 to
147/147 Tier-A outcomes, and exercised all 12 Q4 product cells through the smart
client. The current product scaling evidence contains **60/60 Ready** rows at
concurrency **1/2/4/8/20**, with exact observed concurrency and synchronized
aggregate throughput. Current remaining Q4 blockers are lifecycle and resource
probes and configured comparator/server lanes. A later rehearsal added 84/84
valid raw repetitions with stable identities across same-deployment reopen and
same-client warm-up, plus 12/12 Product Ready fixtures at 4× smoke size. True
memory saturation and sampled CPU/I/O remain open. A subsequent lifecycle
rehearsal closed the product rotation/compaction and declared-damage delta:
12/12 cells preserve their result digest through seal+compaction; targeted
corruption of 17 verified ItemEvent frames returns 51/64 healthy survivors with
explicit incomplete coverage, while strict coverage fails closed on 13 typed
locator holes. The controlled-host R400 and evidenced device-cold campaigns are
still required. Safe in-process resource sampling subsequently closed current
RSS, 1 ms sampled peak RSS, and physical-I/O interval deltas across all 24
repetition/larger-fixture rows. Accumulated process CPU time and logical-byte
read amplification remain open and are not inferred.
See `RQL_Q4_FAITHFULNESS_FINDINGS.md` and `NEXT_BUILD_STATUS.md` for current truth.

## 5. Claim order

1. Repair qualification accounting and the remaining Q4 logical/evidence
   defects.
2. Close Q2 capability and Full-over-wire.
3. Freeze Q1 with dogfood and QVM identities.
4. Re-run Q3 over the complete frozen Tier-A denominator.
5. Finish real product/comparator Q4 adapters, contention and lifecycle paths.
6. Run a non-competitive dress rehearsal and principal Q3/Q4 review.
7. Admit Q5 only after the preceding gates are green.
8. Run Q5, freeze numeric gates, then perform evidence-led Q6 and fresh Q7.

## 6. Non-claims

- Q3 labor green is not Q3 package accept.
- Q4 logical simulation is not product or comparator evidence.
- OS threads around independent simulators are not database contention proof.
- A populated metric field is not proof that the named quantity was measured.
- This recovery baseline is not Gate-1 acceptance and does not unlock Atomics.

## 7. Progress after recording this baseline

First recovery tranche, same day:

- Q3 reports and verifier now expose the complete 147-case denominator and
  print `LABOR PASS / PACKAGE HOLD` at 101/147 rather than a bare PASS;
- F12 genuine 1:N enrich fixture/evaluation implemented;
- F13 embedded product cursor drains authenticated continuations to exhaustion;
- declared embedded indexes are created before measured queries;
- F14 campaign protocol/raw-repetition model and fail-closed Q5 validation
  implemented (collectors still need to populate campaign records);
- F15 logical plan identity separated from result identity;
- F16 explicit `Refused` / `Unsupported` states implemented; a refusal can no
  longer be `Ready`;
- mixed R/W and Full enrich now refuse honestly instead of masquerading as
  successful product measurements.

Verification after the tranche: Q3 `LABOR PASS / PACKAGE HOLD`; Q4 56 default
and 60 embedded-feature tests; Q4 `SCAFFOLD PASS / PACKAGE HOLD`.

Second recovery tranche, same day — Q2 embedded capability closure:

- Tier-A embedded product execution moved from **134/147** to **145/147**;
- the remaining **2/147** are the frozen offset-discard stable refusals, so the
  Q2.1 audit now has **147/147 explicit closed outcomes and zero case gaps**;
- bounded computed conditional projection (five cases) is represented in typed
  QVM projection immediates and evaluates predicates through the canonical SDA
  kernel; arbitrary expression evaluation remains excluded;
- textual `after $cursor` (five cases) resolves a host-issued string/byte token
  only at execution, removes the cursor binding from semantic parameter identity,
  and preserves the same plan hash as the first page;
- corpus `rql-q1-corpus-v0.4.2` records a pending, versioned correction to the
  unread `within` case: enrich creates the carrier, within filters it, and brace
  projection preserves nesting;
- Q3 advanced from **101/147** to **106/147** oracle+differential green; all five
  source-cursor cases are now green. The remaining **41/147** are complete-
  denominator semantics work, not Q2 execution gaps;
- Q3.4 now contains a direct textual-cursor page-concat law in addition to the
  run-option continuation laws.

Verification: Q1 corpus PASS; Q2.1 audit 145 execute / 2 expected refusal / 0
gaps; Q3 `LABOR PASS / PACKAGE HOLD` at 106/147. Full RQL over the server wire,
Q1 principal disposition, Decision 0 and the remaining Q3 denominator still
block RQL-Q2/Q3 package exit and Q5 admission.

### Full-over-wire boundary confirmed

This tranche deliberately did not smuggle Full semantics through Core op 118.
The remaining transport package has three coupled deliverables:

1. a versioned wire request/response profile that distinguishes Core
   `QueryPage` from Full attach/project results;
2. a server-side Full executor over collection-qualified host capabilities
   (the current Full façade is bound to embedded `HeapClient` for foreign
   collection discovery/index loading); and
3. remote SDK parity tests for enrich, within, computed project, continuation,
   cardinality refusal and capability isolation.

Until those land together, `source_uses_rql_full_constructs` classifies bounded
computed projection alongside enrich/within/brace project and the Core wire
fails closed with `rql_feature_unavailable`. Widening op 118 without a frozen
response/profile would turn a visible blocker into an ambiguous API.

Third recovery tranche, same day — bounded Full RQL over product wire:

- op 118 now keeps backward-compatible Core semantics when `profile` is omitted
  or `core`, and admits Full only through the explicit `profile: "full"` request;
- the frozen Full response distinguishes attached `rows` from pre-attachment
  `base_rows`, preserves the Core page continuation/coverage evidence, and
  reports each root enrich load mode;
- server execution compiles Full source and executes verified QVM against a
  collection-qualified host restricted to the authorised Heap catalogue;
- the backend-neutral public surface is `HeapClient::rql_full`, because Full
  queries may span several collections; `CollectionClient::rql` remains Core
  and fails closed rather than silently widening;
- real qualified TLS wire coverage now proves enrich, `within`, brace and
  computed projection, continuation, `exactly_one` refusal, unknown-collection
  isolation, and Full explain/execution QVM identity;
- Full explain identity is now the hash of the canonical QVM actually executed,
  not an unrelated hash of its diagnostic tree.
- Full transformations re-check `max_result_bytes` after attachment/projection;
  foreign scans/index loads and the retained result+cache set are hard-bounded
  by host materialisation limits. The server preserves
  `query_budget_required` rather than collapsing this refusal to unavailable.

Verification after this tranche: remote product-wire scenario **1/1** green;
APP-0 contract lock **6/6**; HAR-4 gate **7/7**; representative embedded Full
tests **7/7**; Q3 remains `LABOR PASS / PACKAGE HOLD` at **106/147**, with
`unsupported=0`, 16 adversarial dimensions and five page-concat laws.

`Q2-BLOCK-FULL-WIRE` is therefore **labor closed**. It does not self-accept Q2:
the principal Q1/corpus amendment, principal Decision-0 disposition, and the
remaining 41-case Q3 semantic denominator still block package exit and Q5.

Fourth recovery tranche, same day — Q3 group/aggregate admission:

- all 16 Tier-A group/aggregate cases now run through the independent logical-
  fixture oracle and both forced-scan and admitted-index QVM arms;
- the pending string-prefix case also enters qualification because its RQL
  source is present and executable;
- aggregate equality compares values and multiplicity but deliberately ignores
  executor-internal synthetic group row keys; no document identity is discarded;
- Q3 advances from **106/147** to **123/147** with zero divergence, zero
  unsupported cases and zero errors.

The exact remaining Q3 denominator is now **24**: 15 enrich, five computed
project, one within, two stable refusals and one explain case. Q3 remains
`LABOR PASS / PACKAGE HOLD`; the corpus's historical `deferred_q2` labels remain
pending principal Q1 amendment rather than being silently rewritten by Q3.

Fifth recovery tranche, same day — complete Q3 Tier-A denominator:

- independent pure semantics now cover all five computed conditional
  projections, all 15 cardinality-aware enrich cases and the nested `within`
  filter/projection case;
- the Full-QVM differential host binds every referenced collection by immutable
  id and keeps equality indexes collection-qualified, preventing candidate
  leakage between collections with the same field names;
- both offset/skip cases now return the frozen
  `rql_offset_discard_unsupported` diagnostic with cursor guidance and are
  classified as required stable refusals;
- the explain case exposed and fixed a real identity defect: Core explain had
  returned a logical-plan hash while execution returned a QVM hash. Explain now
  identifies the default executable QVM and retains the logical tree for
  diagnostics;
- Q3.1 has **144** deterministic semantic results plus **2** stable refusals and
  **1** explain contract; Q3.2 has **147/147** equal outcomes, zero divergence,
  zero unsupported cases and zero errors.

The complete Tier-A Q3 denominator is therefore **147/147 labor green**. This
is labor exit readiness, not self-acceptance: principal package review, the Q1
amendment/acceptance and Q4 comparator/product-faithfulness work remain outside
this result and still block Q5 admission.

Sixth recovery tranche, same day — Q4 embedded product and smart-client query
boundary:

- the embedded Q4 adapter no longer reports fake residuals for Full enrich or
  mixed read/write; it executes the bounded Full-QVM path and deterministic
  90/10 and 70/30 real reads and durable writes;
- the enrichment workload itself was corrected: optional and exactly-one use
  the immutable unique customer key, while only `many` uses the deliberate
  fan-out field. The logical harness may no longer hide an optional-cardinality
  violation by selecting the first match;
- `driver::Collection<T>::rql` and `driver::HeapClient::rql_full` now dispatch
  canonical Core/Full QVM pages through the connection's one bounded scheduler;
- scheduler inspection reports `peak_running`; a four-worker contract test
  proves overlapping Core query work plus Full cross-collection attach on one
  physical connection, writer and shutdown domain;
- bounded query pages are implemented, but the DRV-3 lazy typed stream/cursor
  façade remains explicit future work.

Q4 still holds Q5: the measured-cell runner must consume the new shared-client
concurrency path, lifecycle and raw-repetition collectors remain incomplete,
and Mongo/CBL/server comparison adapters are not configured.

Seventh recovery tranche, same day — first controlled-host R400 execution:

- Bonzo provided a fixed 16 GiB M2 host with 218 GiB initially free; the
  campaign admitted a 64 GiB logical fixture and generated 1,069,548 documents
  through the real product loader without retaining the fixture in memory;
- the loader remained bounded in observed RSS, but its preserved deployment is
  141,864,036 KiB (135.3 GiB), proving the 1.5x filesystem estimate unsafe for
  dual authoritative media. Admission is corrected to 2.25x;
- the unwarmed ordered/projected cursor scan did not complete. macOS recorded
  `low swap: killing largest compressed process` for `residiuum_rql_qu`, with
  a compressed-process size of 46,893 MB, and delivered SIGKILL;
- swap-out growth during the run was approximately 46 GiB. Sampled RSS hid the
  compressed heap growth, so RSS alone is not an adequate saturation guard;
- contemporaneous operator observation in macOS process monitoring recorded a
  peak near 5,500 filesystem writes/s at about 55 MB/s, roughly 50% CPU, and
  about 2.59 GB visible process memory when physical writes reached 128 GB.
  Reads were saw-toothed: bursts near 2,500 reads/s followed by long intervals
  near 56 reads/s. These are host counters rather than harness-normalised
  metrics, but they corroborate dual-media amplification, a non-CPU bottleneck,
  and burst/drain behaviour across cursor pages;
- no final evidence report was written because the campaign and evidence
  publisher shared the killed process. The failed deployment is preserved on
  Bonzo at
  `/Users/rumpel/rql-qualification-residiuum-20260809-01/target/rql-q4/r400-work`.

R400 is therefore **failed, not proved**. Before rerun, the key-ordered cursor
path must avoid scan-page allocation retention for large documents, the
campaign must supervise the product worker out of process so SIGKILL still
produces evidence, and admission must monitor compressed memory/swap as well as
RSS and disk.
