# RQL-Q3.4 — Page-concat metamorphic law (`after` continuation)

Status: **labor complete → board `in_review`** (2026-08-08) · package **not accepted**  
Package: RQL-Q3 · Feature `019fda4c-5994-77e2-a2c9-aaa0c3097b29` · Task Q3.4  
Depends: Q3.1–Q3.3  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §6.3  

## 1. Goal

Close the Q3.1/Q3.2 residual for **product multipage continuation**:

```text
page_1 ++ page_2 ++ …  =  unpaged(Q)
```

Compare keys, values, multiplicity, declared order, and coverage — not row count alone.

## 2. Scope split (honesty)

| Mechanism | Status |
|---|---|
| `QueryRunOptions.after` + authenticated continuation from `QueryPage.next` | **Law green** (this labor) |
| RQL source `after $cursor` compile/oracle (5 corpus cases) | **Green** — authenticated host-issued token, semantic parameter identity preserved |

Source-level cursor tokens are host-minted and bound in the corpus fixture path;
the cursor parameter is page identity, not query semantic identity.

## 3. Implementation

| Artefact | Path |
|---|---|
| Suite | `crates/residiuum-sdk/tests/rql_q3_page_concat.rs` |
| Command | `cargo test -p residiuum-sdk --test rql_q3_page_concat` |
| Report | `spec/rql/qualification/corpus-v1/q3_4_page_concat_report.json` |
| One-command | `bash scripts/verify-rql-q3.sh` (includes Q3.4) |

### Laws

1. Key-order scan: multipage page_size=3 equals unpaged (11 docs).  
2. Field order (`order by score desc, _key asc`): multipage page_size=2 equals unpaged.  
3. Filtered + ordered: `where status = "paid"` multipage equals unpaged.  
4. Single-page when page_size ≥ cardinality equals unpaged.
5. Textual `after $cursor` page concatenation equals unpaged.

Product path: `CollectionClient::rql` only (QVM1 / Core).

## 4. Evidence

| Metric | Value |
|---:|---:|
| Unit laws | **6/6** (5 laws + report writer) |
| False absence / holey complete | **0** (healthy media) |
| Source `after $cursor` corpus residual | **0** |

## 5. Non-claims

- Not Gate-1; not RQL-Q3 package accept.  
- Does not close Decision 0 / RQL-C1.  
- Does not clear the remaining 41-case complete Tier-A semantic denominator.
- Inter-page writes under SI not claimed (Available residual in Q3.3).

## 6. Exit checklist (Q3.4)

- [x] Metamorphic page-concat laws as hard tests  
- [x] Ordered + filtered + single-page cases  
- [x] Machine report + human pack  
- [x] verify-rql-q3.sh includes suite  
- [ ] Principal package accept (not labor)  

## Evidence write policy (F8)

Default tests write under `target/rql-q3/` only. Checked-in `spec/` snapshots update only with `RESIDIUUM_WRITE_SPEC_EVIDENCE=1` or `scripts/publish-rql-q3-evidence.sh`.
