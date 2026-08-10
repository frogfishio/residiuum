//! Coverage records for distributed results (CLUSTER_SPEC §6.7, §17).
//!
//! DEF-040 extends Stage 8e with multi-page continuation; DEF-097 binds the
//! integrity tag to **cluster-local secret key material** (not the public
//! cluster id alone).
//! deterministic merge independent of worker visit order, and end-to-end
//! index/tier/resource limitation fields on every page.

use crate::error::ClusterError;
use crate::id::{ClusterId, LogPosition, PartitionId, Term};
use crate::modes::ReadMode;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Profile tag for distributed query continuation tokens (DEF-040 + DEF-097).
pub const QUERY_CONTINUATION_PROFILE: &str = "residiuum-query-continuation-v2";

/// Domain separation for cluster continuation MAC keys (DEF-097).
const MAC_DOMAIN: &[u8] = b"residiuum-query-continuation-v2-mac\0";

const TOKEN_MAGIC: &[u8; 8] = b"RQRY0002";
const MAC_LEN: usize = 16;
const MAX_TOKEN_BYTES: usize = 16_384;
const MAX_SUBJECT_IN_TOKEN: usize = 4096;
const MAX_PARTITIONS_IN_TOKEN: usize = 4096;

/// Default page size when paged scan is requested without an explicit size.
pub const DEFAULT_FIND_PAGE_SIZE: usize = 64;

/// Hard cap on a single distributed find page.
pub const MAX_FIND_PAGE_SIZE: usize = 4096;

/// Per-partition frontier observed during an operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionFrontier {
    /// Partition that was contacted or requested.
    pub partition: PartitionId,
    /// Leadership term observed (0 if unknown).
    pub term: Term,
    /// Log / event position observed (0 if unknown).
    pub position: LogPosition,
    /// Node that served this partition, if any.
    pub served_by: Option<u32>,
}

/// Coverage evidence attached to every distributed query/scan/recovery result.
///
/// An unavailable partition MUST NOT be represented as an empty successful
/// partition (CLUSTER_SPEC §6.7, §17.2). A partial result is valid data with
/// incomplete coverage — never a silent complete empty success.
///
/// Every distributed **page** carries a full coverage record (DEF-040).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Coverage {
    /// Partitions required by the declared scope.
    pub requested: Vec<PartitionId>,
    /// Partitions that completed successfully.
    pub completed: Vec<PartitionId>,
    /// Partitions that could not be covered (offline, no quorum, …).
    pub unavailable: Vec<PartitionId>,
    /// Frontiers for completed (and optionally partial) partitions.
    pub frontiers: Vec<PartitionFrontier>,
    /// Human-readable notes (e.g. development-profile warnings).
    pub notes: Vec<String>,
    /// Read mode used for the distributed operation, when applicable.
    pub read_mode: Option<String>,
    /// True when a declared resource budget truncated the scan/query.
    pub resource_limit_reached: bool,
    /// Indexes consulted while building this result (DEF-040).
    ///
    /// Empty or `["primary-scan"]` means a primary subject scan; named
    /// secondary indexes are listed when used for pruning/pushdown.
    pub indexes_used: Vec<String>,
    /// Tiers examined for this page (e.g. `hot`, `warm`, `cold`, `archive`).
    pub tiers_searched: Vec<String>,
    /// Tiers excluded, offline, timed out, or otherwise not searched.
    pub tiers_excluded: Vec<String>,
}

impl Coverage {
    /// Empty coverage builder for a declared scope.
    pub fn for_partitions(requested: impl IntoIterator<Item = PartitionId>) -> Self {
        let mut requested: Vec<PartitionId> = requested.into_iter().collect();
        requested.sort();
        requested.dedup();
        Self {
            requested,
            completed: Vec::new(),
            unavailable: Vec::new(),
            frontiers: Vec::new(),
            notes: Vec::new(),
            read_mode: None,
            resource_limit_reached: false,
            indexes_used: Vec::new(),
            tiers_searched: Vec::new(),
            tiers_excluded: Vec::new(),
        }
    }

    /// Single-partition scope.
    pub fn single(partition: PartitionId) -> Self {
        Self::for_partitions([partition])
    }

    /// Mark a partition completed with frontier evidence.
    pub fn mark_completed(
        &mut self,
        partition: PartitionId,
        term: Term,
        position: LogPosition,
        served_by: Option<u32>,
    ) {
        if !self.completed.contains(&partition) {
            self.completed.push(partition);
            self.completed.sort();
        }
        self.unavailable.retain(|p| *p != partition);
        self.frontiers.retain(|f| f.partition != partition);
        self.frontiers.push(PartitionFrontier {
            partition,
            term,
            position,
            served_by,
        });
        self.frontiers.sort_by_key(|f| f.partition);
    }

    /// Mark a partition unavailable (must not look like empty success).
    pub fn mark_unavailable(&mut self, partition: PartitionId) {
        if !self.unavailable.contains(&partition) {
            self.unavailable.push(partition);
            self.unavailable.sort();
        }
        self.completed.retain(|p| *p != partition);
        self.frontiers.retain(|f| f.partition != partition);
    }

    /// True when every requested partition completed and none are unavailable.
    pub fn is_complete(&self) -> bool {
        !self.resource_limit_reached
            && self.unavailable.is_empty()
            && self.tiers_excluded.is_empty()
            && self.requested.iter().all(|p| self.completed.contains(p))
    }

    /// True when at least one requested partition is missing or truncated.
    pub fn is_incomplete(&self) -> bool {
        !self.is_complete()
    }

    /// Attach a free-form note (e.g. profile warning).
    pub fn note(&mut self, msg: impl Into<String>) {
        self.notes.push(msg.into());
    }

    /// Record the read mode used for this result.
    pub fn with_read_mode(&mut self, mode: ReadMode) {
        self.read_mode = Some(mode.as_str().to_string());
    }

    /// Mark that a resource budget stopped the scan before full coverage.
    pub fn mark_resource_limit(&mut self, detail: impl Into<String>) {
        self.resource_limit_reached = true;
        self.note(detail);
    }

    /// Record an index consulted for this page (deduped, sorted).
    pub fn use_index(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.indexes_used.contains(&name) {
            self.indexes_used.push(name);
            self.indexes_used.sort();
        }
    }

    /// Record a tier examined for this page (deduped, sorted).
    pub fn search_tier(&mut self, tier: impl Into<String>) {
        let tier = tier.into();
        if !self.tiers_searched.contains(&tier) {
            self.tiers_searched.push(tier);
            self.tiers_searched.sort();
        }
    }

    /// Record a tier excluded / offline / timed out (deduped, sorted).
    pub fn exclude_tier(&mut self, tier: impl Into<String>) {
        let tier = tier.into();
        if !self.tiers_excluded.contains(&tier) {
            self.tiers_excluded.push(tier);
            self.tiers_excluded.sort();
        }
    }
}

/// Result of a cluster get: value plus separate coverage claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResult {
    /// Live body when found; `None` only means absence when coverage is complete
    /// under a linearizable (or otherwise conclusive) read.
    pub value: Option<Vec<u8>>,
    /// Coverage for the partitions involved.
    pub coverage: Coverage,
    /// Whether the implementation claims absence is proven (`None` + complete).
    pub absence_proven: bool,
}

/// Result of a multi-partition scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanResult {
    /// Live `(subject, body)` pairs from completed partitions only.
    pub entries: Vec<(String, Vec<u8>)>,
    /// Coverage — incomplete scans still return whatever was found.
    pub coverage: Coverage,
}

/// Result of a distributed find/query (CLUSTER_SPEC §17, DEF-040).
///
/// Partial results remain valid data. Callers MUST inspect [`Coverage::is_complete`]
/// before treating absence of matches as proof that no matching subjects exist.
///
/// Multi-page scans attach coverage and continuation state on **every** page so
/// a replacement coordinator can resume without silent duplicates or omissions
/// (§17.4). Attacker-resistant token authentication remains DEF-097.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindResult {
    /// Matching `(subject, body)` pairs from completed partitions only.
    ///
    /// Ordered by subject ascending (deterministic merge; never worker completion
    /// order — CLUSTER_SPEC §17.3).
    pub entries: Vec<(String, Vec<u8>)>,
    /// Coverage for the declared partition scope (per-page).
    pub coverage: Coverage,
    /// Stable query identity for pagination / coordinator replacement (§17.4).
    pub query_id: String,
    /// True when a limit, page boundary, or budget truncated the match list.
    pub truncated: bool,
    /// True when a further page is available via [`Self::continuation`].
    pub has_more: bool,
    /// Integrity-tagged continuation for the next page (`None` when done).
    pub continuation: Option<Vec<u8>>,
}

impl FindResult {
    /// Build a deterministic query id from scope and options.
    pub fn make_query_id(
        scope_partitions: &[PartitionId],
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> String {
        let mut h = DefaultHasher::new();
        "residiuum-find-v1".hash(&mut h);
        for p in scope_partitions {
            p.get().hash(&mut h);
        }
        prefix.unwrap_or("").hash(&mut h);
        limit.unwrap_or(usize::MAX).hash(&mut h);
        format!("q-{:016x}", h.finish())
    }
}

/// Decoded continuation for multi-page distributed find (DEF-040).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryContinuation {
    /// Query identity that must match across pages.
    pub query_id: String,
    /// Exclusive lower bound: return subjects strictly greater than this.
    pub after_subject: String,
    /// Page size requested by the original query.
    pub page_size: usize,
    /// Optional subject prefix scope.
    pub prefix: Option<String>,
    /// Partition scope (sorted, unique).
    pub partitions: Vec<PartitionId>,
    /// Read mode captured at first page.
    pub read_mode: ReadMode,
    /// Optional overall row limit remaining after prior pages (`None` = unbounded).
    pub remaining_limit: Option<usize>,
    /// Optional docs-scanned budget remaining (`None` = unbounded).
    pub remaining_max_docs: Option<usize>,
}

impl QueryContinuation {
    /// Encode and MAC a continuation for `cluster_id` using the secret keyring.
    pub fn encode(
        &self,
        cluster_id: ClusterId,
        keyring: &residiuum_store::ContinuationKeyring,
    ) -> Result<Vec<u8>, ClusterError> {
        if self.after_subject.len() > MAX_SUBJECT_IN_TOKEN {
            return Err(ClusterError::ContinuationInvalid(
                "after_subject exceeds token budget".into(),
            ));
        }
        if self.partitions.len() > MAX_PARTITIONS_IN_TOKEN {
            return Err(ClusterError::ContinuationInvalid(
                "partition scope exceeds token budget".into(),
            ));
        }
        if self.page_size == 0 || self.page_size > MAX_FIND_PAGE_SIZE {
            return Err(ClusterError::ContinuationInvalid(
                "page_size out of range".into(),
            ));
        }
        let prefix = self.prefix.as_deref().unwrap_or("");
        let qid = self.query_id.as_bytes();
        let after = self.after_subject.as_bytes();
        let pfx = prefix.as_bytes();
        let key_gen = keyring.active_generation_id();

        let mut body = Vec::with_capacity(
            8 + 16
                + 4
                + 4
                + qid.len()
                + 4
                + after.len()
                + 4
                + 4
                + pfx.len()
                + 4
                + self.partitions.len() * 4
                + 1
                + 8
                + 8,
        );
        body.extend_from_slice(TOKEN_MAGIC);
        body.extend_from_slice(&cluster_id.0);
        body.extend_from_slice(&key_gen.to_le_bytes());
        body.extend_from_slice(&(qid.len() as u32).to_le_bytes());
        body.extend_from_slice(qid);
        body.extend_from_slice(&(after.len() as u32).to_le_bytes());
        body.extend_from_slice(after);
        body.extend_from_slice(&(self.page_size as u32).to_le_bytes());
        body.extend_from_slice(&(pfx.len() as u32).to_le_bytes());
        body.extend_from_slice(pfx);
        body.extend_from_slice(&(self.partitions.len() as u32).to_le_bytes());
        for p in &self.partitions {
            body.extend_from_slice(&p.get().to_le_bytes());
        }
        body.push(read_mode_byte(self.read_mode));
        body.extend_from_slice(&opt_usize_le(self.remaining_limit));
        body.extend_from_slice(&opt_usize_le(self.remaining_max_docs));

        if body.len() + MAC_LEN > MAX_TOKEN_BYTES {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token would exceed size budget".into(),
            ));
        }
        let key = keyring.active_mac_key(MAC_DOMAIN, &cluster_id.0);
        let tag = blake3::keyed_hash(&key, &body);
        body.extend_from_slice(&tag.as_bytes()[..MAC_LEN]);
        Ok(body)
    }

    /// Decode and authenticate a continuation token for `cluster_id` + keyring.
    pub fn decode(
        cluster_id: ClusterId,
        keyring: &residiuum_store::ContinuationKeyring,
        token: &[u8],
    ) -> Result<Self, ClusterError> {
        if token.len() < 8 + 16 + 4 + 4 + 4 + 4 + 4 + 1 + 8 + 8 + MAC_LEN
            || token.len() > MAX_TOKEN_BYTES
        {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token length out of range".into(),
            ));
        }
        let (payload, mac) = token.split_at(token.len() - MAC_LEN);
        if &payload[..8] != TOKEN_MAGIC.as_slice() {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token magic/version mismatch (need residiuum-query-continuation-v2)"
                    .into(),
            ));
        }
        if &payload[8..24] != cluster_id.0.as_slice() {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token cluster_id mismatch".into(),
            ));
        }
        let key_gen = u32::from_le_bytes(payload[24..28].try_into().unwrap());
        let Some(key) = keyring.mac_key_for(key_gen, MAC_DOMAIN, &cluster_id.0) else {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token key generation retired or unknown".into(),
            ));
        };
        let expected = blake3::keyed_hash(&key, payload);
        if !constant_time_eq(mac, &expected.as_bytes()[..MAC_LEN]) {
            return Err(ClusterError::ContinuationInvalid(
                "continuation token MAC mismatch (tampered, forged, or wrong cluster)".into(),
            ));
        }
        let mut o = 28usize;
        let qlen = read_u32(payload, &mut o)? as usize;
        if o + qlen > payload.len() {
            return Err(ClusterError::ContinuationInvalid(
                "query_id truncated".into(),
            ));
        }
        let query_id = std::str::from_utf8(&payload[o..o + qlen])
            .map_err(|_| ClusterError::ContinuationInvalid("query_id not utf-8".into()))?
            .to_string();
        o += qlen;

        let alen = read_u32(payload, &mut o)? as usize;
        if o + alen > payload.len() {
            return Err(ClusterError::ContinuationInvalid(
                "after_subject truncated".into(),
            ));
        }
        let after_subject = std::str::from_utf8(&payload[o..o + alen])
            .map_err(|_| ClusterError::ContinuationInvalid("after_subject not utf-8".into()))?
            .to_string();
        o += alen;

        let page_size = read_u32(payload, &mut o)? as usize;
        if page_size == 0 || page_size > MAX_FIND_PAGE_SIZE {
            return Err(ClusterError::ContinuationInvalid(
                "page_size invalid".into(),
            ));
        }

        let plen = read_u32(payload, &mut o)? as usize;
        if o + plen > payload.len() {
            return Err(ClusterError::ContinuationInvalid("prefix truncated".into()));
        }
        let prefix = if plen == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&payload[o..o + plen])
                    .map_err(|_| ClusterError::ContinuationInvalid("prefix not utf-8".into()))?
                    .to_string(),
            )
        };
        o += plen;

        let pcount = read_u32(payload, &mut o)? as usize;
        if pcount > MAX_PARTITIONS_IN_TOKEN || o + pcount * 4 + 1 + 8 + 8 > payload.len() {
            return Err(ClusterError::ContinuationInvalid(
                "partition list truncated or too large".into(),
            ));
        }
        let mut partitions = Vec::with_capacity(pcount);
        for _ in 0..pcount {
            let id = read_u32(payload, &mut o)?;
            partitions.push(PartitionId::new(id));
        }
        partitions.sort();
        partitions.dedup();

        if o >= payload.len() {
            return Err(ClusterError::ContinuationInvalid(
                "token missing read_mode".into(),
            ));
        }
        let read_mode = read_mode_from_byte(payload[o])?;
        o += 1;
        let remaining_limit = read_opt_usize(payload, &mut o)?;
        let remaining_max_docs = read_opt_usize(payload, &mut o)?;
        if o != payload.len() {
            return Err(ClusterError::ContinuationInvalid(
                "token trailing bytes".into(),
            ));
        }

        Ok(Self {
            query_id,
            after_subject,
            page_size,
            prefix,
            partitions,
            read_mode,
            remaining_limit,
            remaining_max_docs,
        })
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn opt_usize_le(v: Option<usize>) -> [u8; 8] {
    match v {
        None => u64::MAX.to_le_bytes(),
        Some(n) => (n as u64).to_le_bytes(),
    }
}

fn read_u32(buf: &[u8], o: &mut usize) -> Result<u32, ClusterError> {
    if *o + 4 > buf.len() {
        return Err(ClusterError::ContinuationInvalid(
            "token truncated at u32".into(),
        ));
    }
    let v = u32::from_le_bytes(buf[*o..*o + 4].try_into().unwrap());
    *o += 4;
    Ok(v)
}

fn read_opt_usize(buf: &[u8], o: &mut usize) -> Result<Option<usize>, ClusterError> {
    if *o + 8 > buf.len() {
        return Err(ClusterError::ContinuationInvalid(
            "token truncated at u64".into(),
        ));
    }
    let raw = u64::from_le_bytes(buf[*o..*o + 8].try_into().unwrap());
    *o += 8;
    if raw == u64::MAX {
        Ok(None)
    } else {
        Ok(Some(raw as usize))
    }
}

fn read_mode_byte(mode: ReadMode) -> u8 {
    match mode {
        ReadMode::Linearizable => 1,
        ReadMode::Available => 2,
        ReadMode::Salvage => 3,
    }
}

fn read_mode_from_byte(b: u8) -> Result<ReadMode, ClusterError> {
    match b {
        1 => Ok(ReadMode::Linearizable),
        2 => Ok(ReadMode::Available),
        3 => Ok(ReadMode::Salvage),
        _ => Err(ClusterError::ContinuationInvalid(
            "unknown read_mode in continuation".into(),
        )),
    }
}

/// Options for distributed scan/find (Stage 8e + DEF-040 paging).
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Only include subjects with this UTF-8 prefix (collection routing).
    pub subject_prefix: Option<String>,
    /// Cap the number of returned entries across the whole query (one-shot) or
    /// remaining rows when paging. Deterministic subject order.
    pub limit: Option<usize>,
    /// Cap how many live subjects may be examined before stopping (budget).
    pub max_docs_scanned: Option<usize>,
    /// Optional subset of partitions; default is the full virtual map.
    pub partitions: Option<Vec<PartitionId>>,
    /// Read mode for partition contact (default: available-style scan).
    pub read_mode: ReadMode,
    /// When set, enable multi-page find with this page size (DEF-040).
    ///
    /// Merge order remains subject-ascending regardless of worker visit order.
    pub page_size: Option<usize>,
    /// Opaque integrity-tagged continuation from a previous page.
    pub continuation: Option<Vec<u8>>,
    /// Optional partition visit order (tests / worker simulation).
    ///
    /// Must be a permutation of the resolved scope. Results are **always**
    /// merged by subject order; this only changes contact order.
    pub visit_order: Option<Vec<PartitionId>>,
    /// Exclusive lower bound on subject (internal / advanced resume without token).
    pub after_subject: Option<String>,
}

impl ScanOptions {
    /// Full-map scan with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict to subjects with this prefix.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.subject_prefix = Some(prefix.into());
        self
    }

    /// Cap returned rows (one-shot) or remaining rows (paging).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Cap documents examined (resource budget).
    pub fn max_docs_scanned(mut self, n: usize) -> Self {
        self.max_docs_scanned = Some(n);
        self
    }

    /// Restrict partition scope.
    pub fn partitions(mut self, parts: impl IntoIterator<Item = PartitionId>) -> Self {
        self.partitions = Some(parts.into_iter().collect());
        self
    }

    /// Set the read mode for partition contact.
    pub fn read_mode(mut self, mode: ReadMode) -> Self {
        self.read_mode = mode;
        self
    }

    /// Enable multi-page scan with the given page size (DEF-040).
    pub fn page_size(mut self, n: usize) -> Self {
        self.page_size = Some(n.clamp(1, MAX_FIND_PAGE_SIZE));
        self
    }

    /// Resume from a prior continuation token.
    pub fn continuation(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.continuation = Some(token.into());
        self
    }

    /// Override partition visit order (does not affect merge order).
    pub fn visit_order(mut self, parts: impl IntoIterator<Item = PartitionId>) -> Self {
        self.visit_order = Some(parts.into_iter().collect());
        self
    }

    /// Exclusive lower bound on subject keys (advanced).
    pub fn after_subject(mut self, subject: impl Into<String>) -> Self {
        self.after_subject = Some(subject.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_is_not_complete() {
        let p0 = PartitionId::new(0);
        let p1 = PartitionId::new(1);
        let mut c = Coverage::for_partitions([p0, p1]);
        c.mark_completed(p0, Term(1), LogPosition(3), Some(0));
        c.mark_unavailable(p1);
        assert!(!c.is_complete());
        assert!(c.unavailable.contains(&p1));
        assert!(!c.completed.contains(&p1));
    }

    #[test]
    fn complete_when_all_done() {
        let p0 = PartitionId::new(0);
        let mut c = Coverage::single(p0);
        c.mark_completed(p0, Term(1), LogPosition(1), Some(0));
        assert!(c.is_complete());
    }

    #[test]
    fn resource_limit_makes_incomplete() {
        let p0 = PartitionId::new(0);
        let mut c = Coverage::single(p0);
        c.mark_completed(p0, Term(1), LogPosition(1), Some(0));
        c.mark_resource_limit("budget");
        assert!(c.is_incomplete());
    }

    #[test]
    fn excluded_tier_makes_incomplete() {
        let p0 = PartitionId::new(0);
        let mut c = Coverage::single(p0);
        c.mark_completed(p0, Term(1), LogPosition(1), Some(0));
        c.exclude_tier("archive");
        assert!(c.is_incomplete());
    }

    #[test]
    fn query_id_stable() {
        let p = [PartitionId::new(1), PartitionId::new(2)];
        let a = FindResult::make_query_id(&p, Some("users/"), Some(10));
        let b = FindResult::make_query_id(&p, Some("users/"), Some(10));
        assert_eq!(a, b);
        let c = FindResult::make_query_id(&p, Some("other/"), Some(10));
        assert_ne!(a, c);
    }

    #[test]
    fn continuation_roundtrip() {
        let cid = ClusterId::from_seed(b"test-cluster");
        let ring = residiuum_store::ContinuationKeyring::mint_new().unwrap();
        let cont = QueryContinuation {
            query_id: "q-abc".into(),
            after_subject: "users/alice".into(),
            page_size: 16,
            prefix: Some("users/".into()),
            partitions: vec![PartitionId::new(0), PartitionId::new(3)],
            read_mode: ReadMode::Linearizable,
            remaining_limit: Some(100),
            remaining_max_docs: None,
        };
        let tok = cont.encode(cid, &ring).unwrap();
        let back = QueryContinuation::decode(cid, &ring, &tok).unwrap();
        assert_eq!(back, cont);
    }

    #[test]
    fn continuation_rejects_wrong_cluster() {
        let a = ClusterId::from_seed(b"cluster-a");
        let b = ClusterId::from_seed(b"cluster-b");
        let ring = residiuum_store::ContinuationKeyring::mint_new().unwrap();
        let cont = QueryContinuation {
            query_id: "q-1".into(),
            after_subject: "k".into(),
            page_size: 8,
            prefix: None,
            partitions: vec![PartitionId::new(0)],
            read_mode: ReadMode::Available,
            remaining_limit: None,
            remaining_max_docs: None,
        };
        let tok = cont.encode(a, &ring).unwrap();
        assert!(QueryContinuation::decode(b, &ring, &tok).is_err());
    }

    #[test]
    fn continuation_rejects_tamper() {
        let cid = ClusterId::from_seed(b"t");
        let ring = residiuum_store::ContinuationKeyring::mint_new().unwrap();
        let cont = QueryContinuation {
            query_id: "q-1".into(),
            after_subject: "k".into(),
            page_size: 8,
            prefix: None,
            partitions: vec![PartitionId::new(0)],
            read_mode: ReadMode::Available,
            remaining_limit: None,
            remaining_max_docs: None,
        };
        let mut tok = cont.encode(cid, &ring).unwrap();
        let mid = tok.len() / 2;
        tok[mid] ^= 0xff;
        assert!(QueryContinuation::decode(cid, &ring, &tok).is_err());
    }

    #[test]
    fn continuation_rejects_foreign_keyring() {
        let cid = ClusterId::from_seed(b"c");
        let ring = residiuum_store::ContinuationKeyring::mint_new().unwrap();
        let cont = QueryContinuation {
            query_id: "q-1".into(),
            after_subject: "k".into(),
            page_size: 8,
            prefix: None,
            partitions: vec![PartitionId::new(0)],
            read_mode: ReadMode::Available,
            remaining_limit: None,
            remaining_max_docs: None,
        };
        let tok = cont.encode(cid, &ring).unwrap();
        let attacker = residiuum_store::ContinuationKeyring::mint_new().unwrap();
        assert!(QueryContinuation::decode(cid, &attacker, &tok).is_err());
    }
}
