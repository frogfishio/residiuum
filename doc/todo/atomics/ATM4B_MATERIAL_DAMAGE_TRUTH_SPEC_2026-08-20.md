# ATM-4B material-damage truth specification — 2026-08-20

Status: `delivered_and_qualified`

This block proves that Residiuum reports what durable Atomic evidence says and
what surviving material permits independently. Damage may reduce material
availability. It may not rewrite, invent, or erase a logical decision.

## 1. Acceptance invariant

For one Heap-qualified identity `(heap_id, atomic_id)`, status is the product:

```text
LogicalTruth × MaterialTruth
```

The logical axis is established only by authenticated decision authority. The
material axis is established only by the surviving prepare, member, payload,
chunk, seal, order and publication material. Neither axis is inferred from the
other.

Consequently:

- a valid commit with damaged or missing material remains `committed`;
- complete members without a decision never imply commit;
- missing members never imply abort;
- a valid lifetime tombstone may establish the decision after detailed
  evidence has been lawfully retired;
- conflicting valid decision authorities produce
  `conflicting_decision_evidence`;
- conflicting non-decision material does not rewrite an independently valid
  decision;
- damaged or incomplete coverage never proves `not_found`, uniqueness, or a
  reusable Atomic identity.

## 2. Logical truth precedence

Evaluation is Heap-qualified and deterministic:

| Surviving authority | Logical result |
|---|---|
| One valid decision/tombstone summary, no contradictory valid authority | `committed` or `not_committed` exactly as recorded |
| Two valid decision authorities disagree | `conflicting_decision_evidence` |
| Identifiable damaged/partial decision candidate and no exact authority | `unknown_commit` |
| Valid prepare, no decision, complete coverage, read-only examination | `unknown_commit` |
| Valid prepare, no decision, normal writer recovery | durable `not_committed/recovery_abort` before reuse or resolution |
| No identity evidence and complete authenticated coverage | `not_found` |
| Coverage incomplete and no exact authority | `unknown_commit` |

An exact decision that survives alongside damaged material outranks the
material damage for the logical axis. It does not make that material healthy.

## 3. Material truth lattice

| Material result | Required meaning |
|---|---|
| `complete` | Every item required for the requested examination is authenticated and mutually consistent |
| `partial` | At least one required item is healthy and at least one is missing, partial, or corrupt |
| `missing` | No required member/value material survives for an identity whose required cardinality is known |
| `conflicting` | Two authenticated non-decision material claims disagree, or material contradicts the authenticated prepare/manifest |
| `coverage_incomplete` | The authenticated search frontier is incomplete, so absence or uniqueness cannot be proved |

Global incomplete coverage dominates an absence/uniqueness query, but it does
not erase an exact surviving decision. The status may therefore retain the
known logical decision while reporting `coverage_incomplete` material.

## 4. Evidence-cut matrix

Each row must be qualified for deletion, truncation, byte corruption and a
conflicting valid record where the format permits one:

| Cut | Exact decision survives | No exact decision survives |
|---|---|---|
| prepare | preserve logical decision; material `partial`/`missing`/`conflicting` | `unknown_commit`; never `not_found` when the candidate is identifiable |
| member | preserve logical decision; retain every healthy member for examination | preserve prepare truth; material reflects healthy/missing cardinality |
| payload/chunk | preserve logical decision; do not invent a body | preserve prepare/member truth; material `partial` or `missing` |
| seal/order witness | preserve logical decision; material degrades | never infer commit from members |
| decision | use an agreeing exact tombstone if present; otherwise `unknown_commit` | `unknown_commit` or conflicting decision evidence, never guessed abort |
| tombstone | use exact detailed decision if present and repair only on a writer path | after lawful detail retirement, damaged lifetime authority is degraded coverage, never reusable absence |
| tombstone index | rebuild from authenticated media when possible; otherwise fail closed | no index failure may prove absence or uniqueness |

## 5. Healthy-member examination

For committed Atomics with partial damage, catalogue examination must continue
to expose authenticated surviving members and payload counts. Publication and
receipt reconstruction must not fabricate missing values. Damage to one member
must not discard or hide a healthy sibling.

## 6. Conflict scope

Every attributable finding carries both `heap_id` and `atomic_id`. A conflict
in one Heap cannot change status for the same caller-selected Atomic ID in a
different Heap. Unattributable damage degrades global coverage instead of
being attached to a guessed identity.

Decision conflict and material conflict are separate facts:

- disagreement between valid decision/tombstone authorities affects the
  logical axis;
- disagreement among prepare/member/payload/chunk/seal/order material affects
  the material axis;
- a valid decision plus material conflict is a known logical decision with
  `conflicting` material.

## 7. Required qualification

`ATM-DMG` acceptance requires deterministic tests covering:

1. every evidence cut in section 4;
2. byte flips at the head, middle and tail of each authenticated record;
3. every meaningful truncation boundary and torn final frame;
4. holes before, inside and after otherwise healthy Atomic evidence;
5. two contradictory valid decisions in both file orders;
6. one damaged member among at least two members, proving the healthy sibling
   remains examinable;
7. restart and checkpoint-reopen persistence of every degraded result;
8. same Atomic ID in two Heaps, proving damage isolation;
9. damaged/missing lifetime index with and without reconstructible sidecars;
10. negative controls demonstrating that each mutant would be detected if a
    classifier or durability edge were removed.

Acceptance is recorded by a clean full verifier run for the delivery commit.
Public Atomics remain disabled until ATM-5.
