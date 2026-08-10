//! Secondary indexes (DX_SPEC §8, Stage 6; DEF-027 lifecycle).
//!
//! Indexes are derived, online, resumable, and deletable. Queries remain
//! correct without them via scan (+ optional budget). Work over both embedded
//! stores and remote `residiuum serve` connections (Stage 7 remote parity).
//!
//! ## Lifecycle (DEF-027)
//!
//! Durable states: `building` → `ready` (or `partial` / `failed`); writes mark
//! usable indexes `stale`; rebuild uses `rebuilding`. Build progress (source
//! frontier, resume subject, failure reason, build id) is persisted on the
//! `.six` file so a crash mid-build can resume without blocking writes.
//!
//! Absence is never proven from a stale, partial, building, rebuilding, or
//! failed index — only `ready` with `complete_coverage`.

use crate::error::Error;
#[cfg(feature = "legacy-flat-sdk")]
use crate::filter::{resolve_path_value, Filter, Pred};
#[cfg(feature = "legacy-flat-sdk")]
use crate::residiuum::Backend;
#[cfg(feature = "legacy-flat-sdk")]
use crate::subject::collection_prefix;
#[cfg(feature = "legacy-flat-sdk")]
use crate::value::decode_json;
#[cfg(feature = "legacy-flat-sdk")]
use residiuum_store::Store;
use residiuum_store::{IndexState, SecondaryIndex};
#[cfg(feature = "legacy-flat-sdk")]
use serde_json::Value as JsonValue;

/// Subjects processed between durable checkpoints during an index build.
const BUILD_CHECKPOINT_EVERY: usize = 32;
/// Page size for unfenced live walks during index construction.
const BUILD_PAGE_SIZE: usize = 64;
/// Failpoint: after durable Building/Rebuilding plan is written.
pub const FP_INDEX_BUILD_AFTER_PLAN: &str = "index.build.after_plan";
/// Failpoint: mid-build after a progress checkpoint.
pub const FP_INDEX_BUILD_MID: &str = "index.build.mid";
/// Failpoint: immediately before marking Ready.
pub const FP_INDEX_BUILD_BEFORE_READY: &str = "index.build.before_ready";

/// Public view of a secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// Index name within the collection.
    pub name: String,
    /// Collection name.
    pub collection: String,
    /// Indexed field paths.
    pub fields: Vec<String>,
    /// Lifecycle state.
    pub state: IndexState,
    /// Number of postings.
    pub entry_count: u64,
    /// Whether the index claims complete coverage of live keys.
    pub complete_coverage: bool,
    /// Last failure reason (empty unless state is failed / partial detail).
    pub failure_reason: String,
    /// Hex build id for the current or last build attempt.
    pub build_id_hex: String,
}

impl IndexInfo {
    /// Project a store secondary index record into the SDK view.
    ///
    /// `collection` is the store scope key (legacy flat) or heap scope key.
    pub fn from_store(idx: &SecondaryIndex) -> Self {
        Self::from_store_with_collection(idx, idx.meta.collection.clone())
    }

    /// Project with an explicit display collection name (APB-1 façade).
    pub fn from_store_with_collection(idx: &SecondaryIndex, collection: impl Into<String>) -> Self {
        Self {
            name: idx.meta.name.clone(),
            collection: collection.into(),
            fields: idx.meta.fields.clone(),
            state: idx.meta.state,
            entry_count: idx.meta.entry_count,
            complete_coverage: idx.meta.complete_coverage,
            failure_reason: idx.meta.failure_reason.clone(),
            build_id_hex: residiuum_store::hex16(&idx.meta.build_id),
        }
    }
}

/// Handle for managing indexes on a collection (embedded or remote).
#[cfg(feature = "legacy-flat-sdk")]
pub struct Indexes<'a> {
    pub(crate) backend: &'a mut Backend,
    pub(crate) collection: String,
}

#[cfg(feature = "legacy-flat-sdk")]
impl<'a> Indexes<'a> {
    /// Create (or rebuild) a field index. Online: builds from current live docs.
    ///
    /// If a prior build for the same name/fields was interrupted (`building` /
    /// `rebuilding`), this resumes from the durable resume point.
    pub fn create(&mut self, name: &str, fields: &[&str]) -> Result<IndexInfo, Error> {
        match self.backend {
            Backend::Local(store) => create_index_on_store(store, &self.collection, name, fields),
            Backend::Remote(client) => client.index_create(&self.collection, name, fields),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported(
                "secondary indexes on cluster (Stage 8e+)",
            )),
        }
    }

    /// List indexes on this collection.
    pub fn list(&mut self) -> Result<Vec<IndexInfo>, Error> {
        match self.backend {
            Backend::Local(store) => Ok(store
                .list_secondary_indexes(&self.collection)?
                .iter()
                .map(IndexInfo::from_store)
                .collect()),
            Backend::Remote(client) => client.index_list(&self.collection),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Ok(Vec::new()),
        }
    }

    /// Get one index by name.
    pub fn get(&mut self, name: &str) -> Result<Option<IndexInfo>, Error> {
        match self.backend {
            Backend::Local(store) => Ok(store
                .load_secondary_index(&self.collection, name)?
                .map(|i| IndexInfo::from_store(&i))),
            Backend::Remote(client) => {
                let all = client.index_list(&self.collection)?;
                Ok(all.into_iter().find(|i| i.name == name))
            }
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Ok(None),
        }
    }

    /// Delete an index (never deletes authoritative data).
    pub fn drop(&mut self, name: &str) -> Result<(), Error> {
        match self.backend {
            Backend::Local(store) => {
                store.delete_secondary_index(&self.collection, name)?;
                Ok(())
            }
            Backend::Remote(client) => client.index_drop(&self.collection, name),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported(
                "secondary indexes on cluster (Stage 8e+)",
            )),
        }
    }

    /// Rebuild an existing index definition from live data.
    pub fn rebuild(&mut self, name: &str) -> Result<IndexInfo, Error> {
        match self.backend {
            Backend::Local(store) => {
                let existing = store
                    .load_secondary_index(&self.collection, name)?
                    .ok_or_else(|| Error::QueryInvalid(format!("index not found: {name}")))?;
                let fields: Vec<&str> = existing.meta.fields.iter().map(|s| s.as_str()).collect();
                // Force a fresh rebuild (not resume of a partial mid-build of
                // different generation) by clearing and using rebuilding state.
                create_index_on_store_inner(store, &self.collection, name, &fields, true)
            }
            Backend::Remote(client) => client.index_rebuild(&self.collection, name),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported(
                "secondary indexes on cluster (Stage 8e+)",
            )),
        }
    }

    /// Resume an interrupted `building` / `rebuilding` index without changing fields.
    pub fn continue_build(&mut self, name: &str) -> Result<IndexInfo, Error> {
        match self.backend {
            Backend::Local(store) => {
                let existing = store
                    .load_secondary_index(&self.collection, name)?
                    .ok_or_else(|| Error::QueryInvalid(format!("index not found: {name}")))?;
                if !existing.is_build_in_progress() {
                    return Err(Error::QueryInvalid(format!(
                        "index {name} is not building (state={})",
                        existing.meta.state.as_str()
                    )));
                }
                let fields: Vec<&str> = existing.meta.fields.iter().map(|s| s.as_str()).collect();
                create_index_on_store(store, &self.collection, name, &fields)
            }
            Backend::Remote(_) => Err(Error::RemoteUnsupported(
                "continue_build is embedded-only; remote create/rebuild resume server-side",
            )),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported(
                "secondary indexes on cluster (Stage 8e+)",
            )),
        }
    }
}

/// Build (or rebuild) a secondary field index on an open store.
#[cfg(feature = "legacy-flat-sdk")]
pub fn create_index_on_store(
    store: &mut Store,
    collection: &str,
    name: &str,
    fields: &[&str],
) -> Result<IndexInfo, Error> {
    create_index_on_store_inner(store, collection, name, fields, false)
}

#[cfg(feature = "legacy-flat-sdk")]
fn create_index_on_store_inner(
    store: &mut Store,
    collection: &str,
    name: &str,
    fields: &[&str],
    force_rebuild: bool,
) -> Result<IndexInfo, Error> {
    if name.is_empty() {
        return Err(Error::InvalidKey("index name empty"));
    }
    if fields.is_empty() {
        return Err(Error::QueryInvalid(
            "index requires at least one field".into(),
        ));
    }
    let field_owned: Vec<String> = fields.iter().map(|s| (*s).to_string()).collect();

    let existing = store.load_secondary_index(collection, name)?;
    let resume = existing
        .as_ref()
        .filter(|idx| {
            !force_rebuild && idx.is_build_in_progress() && idx.meta.fields == field_owned
        })
        .cloned();

    let mut idx = if let Some(prior) = resume {
        prior
    } else {
        let had_definition = existing.is_some();
        let mut fresh = SecondaryIndex::new_building(collection, name, field_owned.clone());
        let build_id = residiuum_store::random_id().map_err(Error::from)?;
        let frontier = store.segment_fingerprint()?;
        fresh.begin_build(build_id, frontier, force_rebuild || had_definition);
        // Durable plan before any scan work (crash → resume from empty progress).
        store.write_secondary_index(&fresh)?;
        residiuum_store::hit_failpoint(FP_INDEX_BUILD_AFTER_PLAN)?;
        fresh
    };

    match run_index_build(store, &mut idx) {
        Ok(()) => {
            store.write_secondary_index(&idx)?;
            Ok(IndexInfo::from_store(&idx))
        }
        Err(e) => {
            // Persist Failed with reason so operators and resume paths see it.
            // Failpoints that simulate crash (Panic) will not reach here.
            if !matches!(&e, Error::Store(residiuum_store::StoreError::Failpoint(_))) {
                idx.mark_failed(e.to_string());
                let _ = store.write_secondary_index(&idx);
            }
            Err(e)
        }
    }
}

/// Snapshot walk + optional catch-up, then Ready / Partial (DEF-027).
#[cfg(feature = "legacy-flat-sdk")]
fn run_index_build(store: &mut Store, idx: &mut SecondaryIndex) -> Result<(), Error> {
    let collection = idx.meta.collection.clone();
    let fields = idx.meta.fields.clone();
    let prefix = collection_prefix(&collection)?;

    let mut incomplete_payloads = fill_index_from_live(store, idx, &prefix, &fields)?;

    // Catch-up: if the frontier moved during the walk, rebuild once from a
    // fresh snapshot so concurrent writes do not leave silent gaps.
    let fp_now = store.segment_fingerprint()?;
    if fp_now != idx.meta.source_frontier {
        idx.clear_entries();
        let build_id = residiuum_store::random_id().map_err(Error::from)?;
        idx.begin_build(build_id, fp_now, true);
        store.write_secondary_index(idx)?;
        incomplete_payloads = fill_index_from_live(store, idx, &prefix, &fields)?;
    }

    let fp_final = store.segment_fingerprint()?;
    residiuum_store::hit_failpoint(FP_INDEX_BUILD_BEFORE_READY)?;

    if incomplete_payloads {
        idx.mark_partial(
            fp_final,
            "incomplete payloads during build; absence not proven",
        );
    } else if fp_final == idx.meta.source_frontier {
        idx.mark_ready(fp_final);
    } else {
        // Still drifting after one catch-up: Partial — usable for hits only.
        idx.mark_partial(
            fp_final,
            "source frontier drifted after catch-up; absence not proven",
        );
    }
    Ok(())
}

/// Walk live subjects and insert postings. Returns true if any payload was incomplete.
#[cfg(feature = "legacy-flat-sdk")]
fn fill_index_from_live(
    store: &mut Store,
    idx: &mut SecondaryIndex,
    prefix: &[u8],
    fields: &[String],
) -> Result<bool, Error> {
    let mut after = if idx.meta.resume_after_subject.is_empty() {
        None
    } else {
        Some(idx.meta.resume_after_subject.clone())
    };
    let mut since_checkpoint = 0usize;
    let mut saw_incomplete = false;

    loop {
        let page =
            store.scan_live_bodies_for_build(Some(prefix), after.as_deref(), BUILD_PAGE_SIZE)?;
        if !page.incomplete.is_empty() {
            saw_incomplete = true;
        }
        for (subject, body) in page.entries {
            let Ok(doc) = decode_json(&body) else {
                // Non-JSON bodies are not field-indexed; still advance resume.
                after = Some(subject.clone());
                idx.set_resume_after(&subject);
                since_checkpoint = since_checkpoint.saturating_add(1);
                continue;
            };
            if let Some(ikey) = index_key_for_doc(&doc, fields) {
                idx.insert(ikey, subject.clone());
            }
            after = Some(subject.clone());
            idx.set_resume_after(&subject);
            since_checkpoint = since_checkpoint.saturating_add(1);
            if since_checkpoint >= BUILD_CHECKPOINT_EVERY {
                store.write_secondary_index(idx)?;
                residiuum_store::hit_failpoint(FP_INDEX_BUILD_MID)?;
                since_checkpoint = 0;
            }
        }
        // Advance past incomplete subjects so builds do not stall.
        if let Some(ref last) = page.after {
            after = Some(last.clone());
            idx.set_resume_after(last);
        }
        if !page.has_more {
            break;
        }
    }

    // Final checkpoint of scan progress before readiness decision.
    store.write_secondary_index(idx)?;
    Ok(saw_incomplete)
}

/// Mark usable secondary indexes on `collection` as stale after a write.
///
/// Failures are returned to the caller (DEF-027: do not silently drop
/// stale-marking errors — they affect health/diagnostics).
#[cfg(feature = "legacy-flat-sdk")]
pub fn mark_indexes_stale(store: &mut Store, collection: &str) -> Result<(), Error> {
    let indexes = store.list_secondary_indexes(collection)?;
    for mut idx in indexes {
        let before = idx.meta.state;
        let before_cov = idx.meta.complete_coverage;
        idx.mark_stale();
        if idx.meta.state != before || idx.meta.complete_coverage != before_cov {
            store.write_secondary_index(&idx)?;
        }
    }
    Ok(())
}

/// Build opaque index key bytes from ordered field values (JSON text).
#[cfg(feature = "legacy-flat-sdk")]
pub(crate) fn index_key_for_doc(doc: &JsonValue, fields: &[String]) -> Option<Vec<u8>> {
    let mut parts = Vec::new();
    for f in fields {
        let v = resolve_path_value(doc, f)?;
        // Stable JSON encoding for the field value.
        let enc = serde_json::to_vec(v).ok()?;
        parts.push(enc);
    }
    // Join with 0x1f unit separator so multi-field keys stay unambiguous.
    let mut out = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            out.push(0x1f);
        }
        out.extend_from_slice(p);
    }
    Some(out)
}

/// Index acceleration candidates: metadata plus subject keys.
#[cfg(feature = "legacy-flat-sdk")]
pub(crate) type IndexLookupCandidates = (IndexInfo, Vec<Vec<u8>>);

/// If the filter is a simple equality (or AND of equalities) on fields that
/// match an index prefix, return candidate subjects from that index.
#[cfg(feature = "legacy-flat-sdk")]
pub(crate) fn try_index_lookup(
    store: &Store,
    collection: &str,
    filter: &Filter,
) -> Result<Option<IndexLookupCandidates>, Error> {
    let eqs = equality_fields(filter);
    if eqs.is_empty() {
        return Ok(None);
    }
    let indexes = store.list_secondary_indexes(collection)?;
    // Prefer a Ready+complete index (exclusive complete candidate set only).
    // Partial hit lists silently omit peers skipped at construction (DEF-SCAN-001).
    for idx in indexes {
        if !idx.meta.may_supply_exclusive_candidates() {
            continue;
        }
        if idx.meta.fields.is_empty() {
            continue;
        }
        // All index fields must appear as equalities.
        let mut values = Vec::new();
        let mut ok = true;
        for f in &idx.meta.fields {
            match eqs.iter().find(|(path, _)| path == f) {
                Some((_, v)) => values.push(v.clone()),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        let key = {
            let mut out = Vec::new();
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(0x1f);
                }
                out.extend_from_slice(&serde_json::to_vec(v)?);
            }
            out
        };
        // Ready+complete_coverage: empty miss and non-empty hits are exclusive.
        let subjects = idx.lookup(&key).to_vec();
        let info = IndexInfo::from_store(&idx);
        return Ok(Some((info, subjects)));
    }
    Ok(None)
}

/// Extract field equality constraints from a filter (shallow AND of Eq only).
#[cfg(feature = "legacy-flat-sdk")]
fn equality_fields(filter: &Filter) -> Vec<(String, JsonValue)> {
    match filter {
        Filter::Field {
            path,
            pred: Pred::Eq(v),
        } => vec![(path.clone(), v.clone())],
        Filter::And(parts) => {
            let mut out = Vec::new();
            for p in parts {
                out.extend(equality_fields(p));
            }
            out
        }
        Filter::Always => Vec::new(),
        _ => Vec::new(),
    }
}

/// Encode a collection key into a store subject (helper for tests/SDK).
#[allow(dead_code)]
pub(crate) fn subject_for(collection: &str, key: &str) -> Result<Vec<u8>, Error> {
    crate::subject::encode_subject(collection, key)
}
