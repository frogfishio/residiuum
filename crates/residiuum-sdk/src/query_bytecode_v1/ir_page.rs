//! Core page / coverage IR phase (RQL-IR3).
//!
//! Profile: **`residiuum-query-ir-page-v1`**
//! Normative: [QUERY_IR_PAGE_V1.md](../../../../../doc/todo/rql/QUERY_IR_PAGE_V1.md)
//!
//! Application Core page-size clamping, coverage policy merge, and cursor
//! mint/decode live here — not as private helpers inside the scan loop. Still a
//! **Rust IR residual** (not an opcode machine). Decision 0 remains OPEN;
//! RQL-C1 must not be accepted.

use crate::app_v1::{
    ConsistencyMode, Continuation, CoverageEvidence, CoveragePolicy, HoleEvidence,
};
use crate::cursor_v1::{
    active_cursor_key_ring, mint, CursorLogical, VerifyContext, PROFILE as CURSOR_PROFILE,
};
use crate::error::Error;
use crate::plan_v1::{NullsOrder, OrderDir, OrderTerm};
use residiuum_heap::{CollectionId, HeapId};
use serde_json::Value as JsonValue;

/// IR profile id for Core page / coverage.
pub const PAGE_IR_PROFILE: &str = "residiuum-query-ir-page-v1";

/// Effective page size: run option overrides plan, clamped to `[1, 4096]`.
pub(crate) fn resolve_page_size(plan_page_size: u32, options_page_size: Option<u32>) -> usize {
    options_page_size.unwrap_or(plan_page_size).clamp(1, 4_096) as usize
}

/// Rows needed this page given remaining limit and page size.
pub(crate) fn rows_needed(remaining_limit: Option<u64>, page_size: usize) -> usize {
    match remaining_limit {
        Some(n) => (n as usize).min(page_size),
        None => page_size,
    }
}

/// Merge plan + run coverage (run may only stay IncompleteAllowed when plan allows).
pub(crate) fn resolve_coverage_mode(
    plan_coverage: CoveragePolicy,
    options_coverage: CoveragePolicy,
) -> CoveragePolicy {
    match (plan_coverage, options_coverage) {
        (CoveragePolicy::IncompleteAllowed, CoveragePolicy::IncompleteAllowed) => {
            CoveragePolicy::IncompleteAllowed
        }
        // Plan IncompleteAllowed + run default Complete still honors plan (RQL source).
        (CoveragePolicy::IncompleteAllowed, _) => CoveragePolicy::IncompleteAllowed,
        _ => CoveragePolicy::Complete,
    }
}

/// Fail closed on holes under Complete; otherwise build coverage evidence.
pub(crate) fn finish_coverage(
    mode: CoveragePolicy,
    known_holes: &[HoleEvidence],
    examined_docs: u64,
) -> Result<CoverageEvidence, Error> {
    let hole_count = known_holes.len() as u32;
    if hole_count > 0 && matches!(mode, CoveragePolicy::Complete) {
        let sample: Vec<&str> = known_holes
            .iter()
            .take(3)
            .map(|h| h.code.as_str())
            .collect();
        return Err(Error::CoverageIncomplete(format!(
            "complete coverage required but {hole_count} known hole(s); \
             sample codes={sample:?} (set coverage incomplete_allowed to allow)"
        )));
    }
    Ok(if hole_count == 0 {
        CoverageEvidence::complete(mode, examined_docs)
    } else {
        CoverageEvidence::incomplete(mode, examined_docs, hole_count)
    })
}

/// Mint a multipage continuation carrying sort-tuple + remaining limit.
pub(crate) fn mint_page_cursor(
    heap_id: HeapId,
    collection_id: CollectionId,
    plan_hash: &[u8; 32],
    parameter_hash: &str,
    order: &[OrderTerm],
    last_sort_tuple: &JsonValue,
    remaining_limit: Option<u64>,
    page_size: u32,
    coverage: CoveragePolicy,
    consistency: ConsistencyMode,
    group_spool_id: Option<&str>,
) -> Result<Continuation, Error> {
    // APB-7 T10: product ring when installed; otherwise vector-lock default.
    let ring = active_cursor_key_ring();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| Error::Internal(format!("clock: {e}")))?;
    let logical = CursorLogical {
        cursor_profile: CURSOR_PROFILE.into(),
        key_id: ring.current.key_id.clone(),
        heap_id: format_uuid(heap_id.as_bytes()),
        collection_id: format_uuid(collection_id.as_bytes()),
        authority_epoch: 1,
        plan_hash: hex32(plan_hash),
        parameter_hash: parameter_hash.to_string(),
        order_normalized: order_normalized_json(order),
        last_sort_tuple: last_sort_tuple.clone(),
        source_frontier: match group_spool_id {
            Some(id) => serde_json::json!({"generation": 0, "group_spool_id": id}),
            None => serde_json::json!({"generation": 0}),
        },
        remaining_limit: remaining_limit.unwrap_or(u64::MAX),
        page_size,
        coverage_mode: match coverage {
            CoveragePolicy::Complete => "complete".into(),
            CoveragePolicy::IncompleteAllowed => "incomplete_allowed".into(),
        },
        consistency_mode: match consistency {
            ConsistencyMode::Available => "available".into(),
            ConsistencyMode::Current => "current".into(),
        },
        issued_at: now,
        expires_at: now.saturating_add(crate::cursor_v1::TTL_SECONDS),
    };
    mint(&logical, &ring)
}

/// Decode continuation → (`last_sort_tuple`, remaining limit, group spool id).
pub(crate) fn decode_after(
    cont: &Continuation,
    heap_id: HeapId,
    collection_id: CollectionId,
    plan_hash: &[u8; 32],
    parameter_hash: &str,
) -> Result<(Option<JsonValue>, Option<u64>, Option<String>), Error> {
    let ring = active_cursor_key_ring();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ctx = VerifyContext {
        heap_id: format_uuid(heap_id.as_bytes()),
        collection_id: format_uuid(collection_id.as_bytes()),
        plan_hash: Some(hex32(plan_hash)),
        // APB-7 T10: bind parameters into resume (fail-closed on mismatch).
        parameter_hash: Some(parameter_hash.to_string()),
    };
    let logical = crate::cursor_v1::verify(&cont.token, &ctx, &ring, now)?;
    let rem = if logical.remaining_limit == u64::MAX {
        None
    } else {
        Some(logical.remaining_limit)
    };
    let group_spool_id = logical
        .source_frontier
        .get("group_spool_id")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    Ok((Some(logical.last_sort_tuple), rem, group_spool_id))
}

fn order_normalized_json(order: &[OrderTerm]) -> JsonValue {
    JsonValue::Array(
        order
            .iter()
            .map(|t| {
                serde_json::json!({
                    "path": t.path.0,
                    "dir": match t.dir {
                        OrderDir::Asc => "asc",
                        OrderDir::Desc => "desc",
                    },
                    "nulls": match t.nulls {
                        NullsOrder::Last => "last",
                        NullsOrder::First => "first",
                    },
                    "tie_break": t.tie_break,
                })
            })
            .collect(),
    )
}

fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_ir_profile_constant() {
        assert_eq!(PAGE_IR_PROFILE, "residiuum-query-ir-page-v1");
    }

    #[test]
    fn resolve_page_size_clamps() {
        assert_eq!(resolve_page_size(10, None), 10);
        assert_eq!(resolve_page_size(10, Some(0)), 1);
        assert_eq!(resolve_page_size(10, Some(9_000)), 4_096);
    }

    #[test]
    fn rows_needed_respects_remaining() {
        assert_eq!(rows_needed(None, 64), 64);
        assert_eq!(rows_needed(Some(3), 64), 3);
        assert_eq!(rows_needed(Some(100), 16), 16);
    }

    #[test]
    fn coverage_plan_incomplete_honored() {
        assert_eq!(
            resolve_coverage_mode(CoveragePolicy::IncompleteAllowed, CoveragePolicy::Complete),
            CoveragePolicy::IncompleteAllowed
        );
        assert_eq!(
            resolve_coverage_mode(CoveragePolicy::Complete, CoveragePolicy::IncompleteAllowed),
            CoveragePolicy::Complete
        );
    }

    #[test]
    fn finish_coverage_fails_closed() {
        let holes = vec![HoleEvidence {
            code: "key_listed_absent".into(),
            key: Some("k".into()),
        }];
        let err = finish_coverage(CoveragePolicy::Complete, &holes, 1).unwrap_err();
        assert!(matches!(err, Error::CoverageIncomplete(_)));
        let ok = finish_coverage(CoveragePolicy::IncompleteAllowed, &holes, 1).unwrap();
        assert_eq!(ok.hole_count, 1);
    }
}
