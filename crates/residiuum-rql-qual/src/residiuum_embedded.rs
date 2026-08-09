//! Residiuum embedded product adapter (feature `residiuum-embedded`).
//!
//! Seeds shared logical work into a temp Heap and runs `CollectionClient::rql`.
//! Labor scaffold — not a Gate-1 competitive claim.

#![cfg(feature = "residiuum-embedded")]

use crate::canonicalize::{canonicalize_rows, ResultRow};
use crate::cell_plan::{MeasuredCellPlan, ReadWriteMix};
use crate::engine::{AdapterError, AdapterStatus, EngineRunOutcome, ExecutionKind};
use crate::lane::EngineId;
use crate::metrics::{
    assemble_metrics, LatencyCollector, ProcessResourceSampler, QueryPathMetrics, QueryTimer,
};
use crate::shared_work::SharedLogicalWork;
use crate::{
    cells::MandatoryCell,
    dataset::DatasetSpec,
    generator::{generated_docs, SplitMix64},
};
use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::driver::{
    Client as DriverClient, Collection as DriverCollection, EmbeddedOptions,
};
use residiuum_sdk::{Parameters, QueryRunOptions, ResidiuumDeployment, RqlFullExecuteOptions};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Wake, Waker},
};
use tempfile::tempdir;

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7u8; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

fn uuid_bytes() -> [u8; 16] {
    *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes()
}

/// Execute a measured cell plan on product Residiuum embedded path.
///
/// Core, bounded Full enrich and mixed read/write cells execute real product
/// operations. Competitive claims still require campaign protocol and principal
/// acceptance.
pub fn execute_plan_embedded(
    work: Option<&SharedLogicalWork>,
    plan: &MeasuredCellPlan,
) -> Result<EngineRunOutcome, AdapterError> {
    let work = work.ok_or_else(|| AdapterError::Fixture("shared work not loaded".into()))?;
    if plan.concurrency > 1 {
        return execute_plan_embedded_smart_client(work, plan);
    }

    let dir = tempdir().map_err(|e| AdapterError::Execute(e.to_string()))?;
    let root = dir.path();
    let deployment =
        ResidiuumDeployment::create(root).map_err(|e| AdapterError::Execute(e.to_string()))?;
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid_bytes(), "heap-q4-3")
        .map_err(|e| AdapterError::Execute(e.to_string()))?;
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
        .map_err(|e| AdapterError::Execute(e.to_string()))?;
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client =
        residiuum_sdk::HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

    for (name, docs) in &work.dataset.collections {
        let mut col = client
            .create_collection(name)
            .map_err(|e| AdapterError::Execute(format!("create {name}: {e}")))?
            .collection;
        for (key, doc) in docs {
            let mut body = doc.clone();
            if let Value::Object(ref mut m) = body {
                m.remove("_key");
            }
            col.put(key, &body)
                .map_err(|e| AdapterError::Execute(format!("put {name}/{key}: {e}")))?;
        }
    }

    let root_name = if plan.rql_source.contains("from docs") {
        "docs"
    } else {
        "docs"
    };
    let mut col = client
        .open_collection(root_name)
        .map_err(|e| AdapterError::Execute(e.to_string()))?;

    // Materialise the exact declared index entitlement before measurement.
    // A cell that claims an index must never silently measure a scan-only store.
    let index_timer = QueryTimer::start();
    let mut index_entries_built = 0u64;
    for index in &plan.indexes {
        let fields: Vec<&str> = index.fields.iter().map(String::as_str).collect();
        let info = col
            .indexes()
            .create(&index.name, &fields)
            .map_err(|e| AdapterError::Execute(format!("create index {}: {e}", index.name)))?;
        index_entries_built = index_entries_built.saturating_add(info.entry_count);
    }
    let index_build_ns = if plan.indexes.is_empty() {
        None
    } else {
        Some(index_timer.elapsed_ns())
    };

    let mut opts = QueryRunOptions::default();
    if let Some(ps) = plan.page_size {
        opts.page_size = Some(ps);
    }

    let mut lat = LatencyCollector::new();
    let mut rows = Vec::<ResultRow>::new();
    let mut examined_documents = 0u64;
    let mut coverage_complete = true;
    let mut plan_hash = None;
    let mut pages = 0u32;
    let mut operation_detail = String::new();
    if plan.cell == MandatoryCell::EnrichCardinalities {
        loop {
            let timer = QueryTimer::start();
            let page = client
                .rql_full(&plan.rql_source, &Parameters::default(), opts.clone())
                .map_err(|e| AdapterError::Execute(format!("full rql: {e}")))?;
            lat.record_duration(timer.elapsed());
            pages += 1;
            plan_hash.get_or_insert(page.base.plan_hash);
            examined_documents =
                examined_documents.saturating_add(page.base.coverage.examined_documents);
            coverage_complete &= page.base.coverage.complete;
            rows.extend(page.rows.iter().map(|(key, value)| ResultRow {
                key: key.clone(),
                value: value.clone(),
            }));
            let done = page.base.exhausted || page.base.next.is_none();
            if !plan.require_deep_cursor || done {
                break;
            }
            if pages >= 4_096 {
                return Err(AdapterError::Execute("cursor page drain safety cap".into()));
            }
            opts.after = page.base.next;
        }
        operation_detail = "full_rql=true".into();
    } else if plan.cell == MandatoryCell::MixedReadWrite {
        let mix = plan.rw_mix.unwrap_or(ReadWriteMix::R90W10);
        let mut rng = SplitMix64::new(plan.dataset.seed ^ 0xA11CE);
        let mut reads = 0u32;
        let mut writes = 0u32;
        for op in 0..100u32 {
            if rng.next_u32() % 100 < mix.read_pct() as u32 {
                reads += 1;
                let timer = QueryTimer::start();
                let page = col
                    .rql(
                        &plan.rql_source,
                        &Parameters::default(),
                        QueryRunOptions::default(),
                    )
                    .map_err(|e| AdapterError::Execute(format!("mixed read: {e}")))?;
                lat.record_duration(timer.elapsed());
                examined_documents =
                    examined_documents.saturating_add(page.coverage.examined_documents);
                coverage_complete &= page.coverage.complete;
                plan_hash.get_or_insert(page.plan_hash);
            } else {
                writes += 1;
                let key = format!("w-{op:08}");
                let timer = QueryTimer::start();
                col.put(
                    &key,
                    &serde_json::json!({
                        "status": "open",
                        "sel_bucket": "MISS",
                        "amount": 1,
                        "region": "r0",
                        "score": 0,
                        "i": op,
                    }),
                )
                .map_err(|e| AdapterError::Execute(format!("mixed write {key}: {e}")))?;
                lat.record_duration(timer.elapsed());
            }
        }
        if reads == 0 || writes == 0 {
            return Err(AdapterError::Execute(format!(
                "mixed R/W schedule did not exercise both paths: reads={reads} writes={writes}"
            )));
        }
        let page = col
            .rql(
                &plan.rql_source,
                &Parameters::default(),
                QueryRunOptions::default(),
            )
            .map_err(|e| AdapterError::Execute(format!("mixed final read: {e}")))?;
        pages = 1;
        plan_hash.get_or_insert(page.plan_hash);
        examined_documents = examined_documents.saturating_add(page.coverage.examined_documents);
        coverage_complete &= page.coverage.complete;
        rows.extend(page.rows.iter().map(|row| ResultRow {
            key: row.key.clone(),
            value: row.value.clone(),
        }));
        operation_detail = format!("mixed_rw={} reads={reads} writes={writes}", mix.as_str());
    } else {
        loop {
            let timer = QueryTimer::start();
            let page = match col.rql(&plan.rql_source, &Parameters::default(), opts.clone()) {
                Ok(p) => p,
                Err(e) => {
                    return Ok(EngineRunOutcome {
                        engine: EngineId::ResidiuumEmbedded,
                        execution_kind: ExecutionKind::Product,
                        status: AdapterStatus::Refused,
                        result: None,
                        metrics: None,
                        refuse_code: Some(format!("product_rql_refuse:{}", e.code().as_str())),
                        detail: Some(e.to_string()),
                        shared_work_hash: Some(work.content_hash.clone()),
                    });
                }
            };
            lat.record_duration(timer.elapsed());
            pages += 1;
            plan_hash.get_or_insert(page.plan_hash);
            examined_documents =
                examined_documents.saturating_add(page.coverage.examined_documents);
            coverage_complete &= page.coverage.complete;
            rows.extend(page.rows.iter().map(|row| ResultRow {
                key: row.key.clone(),
                value: row.value.clone(),
            }));
            let done = page.exhausted || page.next.is_none();
            if !plan.require_deep_cursor || done {
                break;
            }
            if pages >= 4_096 {
                return Err(AdapterError::Execute("cursor page drain safety cap".into()));
            }
            opts.after = page.next;
        }
    }
    if plan.require_deep_cursor && pages < 2 {
        return Ok(EngineRunOutcome {
            engine: EngineId::ResidiuumEmbedded,
            execution_kind: ExecutionKind::Product,
            status: AdapterStatus::Unsupported,
            result: None,
            metrics: None,
            refuse_code: Some("harness_deep_cursor_not_reached".into()),
            detail: Some(format!("plan={} pages={pages}", plan.plan_id)),
            shared_work_hash: Some(work.content_hash.clone()),
        });
    }

    let canon = canonicalize_rows(&rows, plan.order_sensitive, coverage_complete);
    let digest = canon.values_digest.clone();
    let plan_hash = plan_hash.unwrap_or([0u8; 32]);
    let metrics = assemble_metrics(
        &lat,
        QueryPathMetrics {
            documents_examined: Some(examined_documents),
            logical_bytes_examined: None,
            // Build entry count is not query probe count; do not conflate them.
            index_entries_examined: None,
            index_size_bytes: None,
            index_build_ns,
            indexed_write_penalty_ns: None,
            explain_plan_digest: Some(format!("product_plan_hash:{}", hex::encode(plan_hash))),
        },
        Some(plan.lifecycle.class),
        Some(plan.lifecycle.cold_method.as_str().into()),
        Some(digest),
        Some(coverage_complete),
        Some(true),
    );

    Ok(EngineRunOutcome {
        engine: EngineId::ResidiuumEmbedded,
        execution_kind: ExecutionKind::Product,
        status: AdapterStatus::Ready,
        result: Some(canon),
        metrics: Some(metrics),
        refuse_code: None,
        detail: Some(format!(
            "product_embedded plan={} pages={} indexes={} index_entries_built={} {}",
            plan.plan_id,
            pages,
            plan.indexes.len(),
            index_entries_built,
            operation_detail,
        )),
        shared_work_hash: Some(work.content_hash.clone()),
    })
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

pub(crate) struct PreparedConcurrentProduct {
    _directory: tempfile::TempDir,
    capability: residiuum_heap::HeapCap,
    index_build_ns: Option<u64>,
    index_entries_built: u64,
}

impl PreparedConcurrentProduct {
    pub(crate) fn root(&self) -> &std::path::Path {
        self._directory.path()
    }
}

fn filesystem_available_bytes(path: &Path) -> Option<u64> {
    let output = std::process::Command::new("df")
        .args(["-Pk"])
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .next_back()?;
    let available_kib = line.split_whitespace().nth(3)?.parse::<u64>().ok()?;
    Some(available_kib.saturating_mul(1024))
}

/// Build a campaign-sized primary collection without retaining the logical
/// fixture in memory. The rolling digest preserves cross-adapter identity and
/// the live disk-floor check fails before exhausting the volume.
pub(crate) fn prepare_streaming_product(
    campaign_root: &Path,
    spec: &DatasetSpec,
    filesystem_floor_bytes: u64,
) -> Result<(PreparedConcurrentProduct, String, u64), AdapterError> {
    std::fs::create_dir_all(campaign_root)
        .map_err(|error| AdapterError::Execute(format!("campaign root: {error}")))?;
    let directory = tempfile::tempdir_in(campaign_root)
        .map_err(|error| AdapterError::Execute(error.to_string()))?;
    let root = directory.path();
    let deployment = ResidiuumDeployment::create(root)
        .map_err(|error| AdapterError::Execute(error.to_string()))?;
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().unwrap();
    let heap_id = HeapId::new_random().unwrap();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        uuid_bytes(),
        "heap-q4-r400",
    )
    .map_err(|error| AdapterError::Execute(error.to_string()))?;
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
        .map_err(|error| AdapterError::Execute(error.to_string()))?;
    let capability = mint_cap_for(heap_id, deployment_id);
    let mut heap = residiuum_sdk::HeapClient::from(deployment.open_heap(capability.clone()));
    let mut docs = heap
        .create_collection("docs")
        .map_err(|error| AdapterError::Execute(format!("create docs: {error}")))?
        .collection;
    let mut digest = Sha256::new();
    digest.update(b"docs\n");
    let mut logical_bytes = 0u64;
    for (ordinal, (key, mut document)) in generated_docs(spec).enumerate() {
        let encoded = serde_json::to_vec(&document)
            .map_err(|error| AdapterError::Execute(format!("encode {key}: {error}")))?;
        logical_bytes = logical_bytes.saturating_add(encoded.len() as u64);
        digest.update(key.as_bytes());
        digest.update(b"\t");
        digest.update(&encoded);
        digest.update(b"\n");
        if let Value::Object(map) = &mut document {
            map.remove("_key");
        }
        docs.put(&key, &document)
            .map_err(|error| AdapterError::Execute(format!("put docs/{key}: {error}")))?;
        if ordinal % 4_096 == 0 {
            let free = filesystem_available_bytes(root).ok_or_else(|| {
                AdapterError::Execute("streaming loader lost filesystem probe".into())
            })?;
            if free < filesystem_floor_bytes {
                return Err(AdapterError::Execute(format!(
                    "streaming loader disk floor: free={free} floor={filesystem_floor_bytes}"
                )));
            }
        }
    }
    drop(docs);
    drop(heap);
    drop(deployment);
    Ok((
        PreparedConcurrentProduct {
            _directory: directory,
            capability,
            index_build_ns: None,
            index_entries_built: 0,
        },
        hex::encode(digest.finalize()),
        logical_bytes,
    ))
}

pub(crate) fn prepare_concurrent_product(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
) -> Result<PreparedConcurrentProduct, AdapterError> {
    let directory = tempdir().map_err(|error| AdapterError::Execute(error.to_string()))?;
    let root = directory.path();
    let deployment =
        ResidiuumDeployment::create(root).map_err(|e| AdapterError::Execute(e.to_string()))?;
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().unwrap();
    let heap_id = HeapId::new_random().unwrap();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        uuid_bytes(),
        "heap-q4-concurrent",
    )
    .map_err(|error| AdapterError::Execute(error.to_string()))?;
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
        .map_err(|error| AdapterError::Execute(error.to_string()))?;
    let capability = mint_cap_for(heap_id, deployment_id);
    let mut heap = residiuum_sdk::HeapClient::from(deployment.open_heap(capability.clone()));
    for (name, documents) in &work.dataset.collections {
        let mut collection = heap
            .create_collection(name)
            .map_err(|error| AdapterError::Execute(format!("create {name}: {error}")))?
            .collection;
        for (key, document) in documents {
            let mut body = document.clone();
            if let Value::Object(map) = &mut body {
                map.remove("_key");
            }
            collection
                .put(key, &body)
                .map_err(|error| AdapterError::Execute(format!("put {name}/{key}: {error}")))?;
        }
    }
    let mut docs = heap
        .open_collection("docs")
        .map_err(|error| AdapterError::Execute(error.to_string()))?;
    let timer = QueryTimer::start();
    let mut index_entries_built = 0u64;
    for index in &plan.indexes {
        let fields: Vec<&str> = index.fields.iter().map(String::as_str).collect();
        let info = docs
            .indexes()
            .create(&index.name, &fields)
            .map_err(|error| {
                AdapterError::Execute(format!("create index {}: {error}", index.name))
            })?;
        index_entries_built = index_entries_built.saturating_add(info.entry_count);
    }
    let index_build_ns = (!plan.indexes.is_empty()).then(|| timer.elapsed_ns());
    drop(docs);
    drop(heap);
    drop(deployment);
    Ok(PreparedConcurrentProduct {
        _directory: directory,
        capability,
        index_build_ns,
        index_entries_built,
    })
}

#[derive(Debug)]
struct ProductWorkerResult {
    rows: Vec<ResultRow>,
    latency_ns: Vec<u64>,
    examined_documents: u64,
    logical_bytes_examined: u64,
    coverage_complete: bool,
    plan_hash: [u8; 32],
    pages: u32,
    detail: String,
}

fn execute_driver_worker(
    heap: residiuum_sdk::driver::HeapClient,
    docs: DriverCollection<Value>,
    plan: &MeasuredCellPlan,
    worker_id: u32,
) -> Result<ProductWorkerResult, AdapterError> {
    let mut rows = Vec::new();
    let mut latency_ns = Vec::new();
    let mut examined_documents = 0u64;
    let mut logical_bytes_examined = 0u64;
    let mut coverage_complete = true;
    let mut plan_hash = None;
    let mut pages = 0u32;
    let mut options = QueryRunOptions::default();
    options.page_size = plan.page_size;
    let detail;

    if plan.cell == MandatoryCell::EnrichCardinalities {
        loop {
            let timer = QueryTimer::start();
            let page = block_on(heap.rql_full(
                &plan.rql_source,
                &Parameters::default(),
                RqlFullExecuteOptions {
                    query: options.clone(),
                    force_enrich_scan: false,
                },
            ))
            .map_err(|error| AdapterError::Execute(format!("worker full rql: {error}")))?;
            latency_ns.push(timer.elapsed_ns());
            pages += 1;
            plan_hash.get_or_insert(page.base.plan_hash);
            examined_documents =
                examined_documents.saturating_add(page.base.coverage.examined_documents);
            logical_bytes_examined =
                logical_bytes_examined.saturating_add(page.base.logical_bytes_examined);
            coverage_complete &= page.base.coverage.complete;
            rows.extend(
                page.rows
                    .into_iter()
                    .map(|(key, value)| ResultRow { key, value }),
            );
            let done = page.base.exhausted || page.base.next.is_none();
            if !plan.require_deep_cursor || done {
                break;
            }
            if pages >= 4_096 {
                return Err(AdapterError::Execute("cursor page drain safety cap".into()));
            }
            options.after = page.base.next;
        }
        detail = "full_rql=true".into();
    } else if plan.cell == MandatoryCell::MixedReadWrite {
        let mix = plan.rw_mix.unwrap_or(ReadWriteMix::R90W10);
        let mut rng = SplitMix64::new(plan.dataset.seed ^ 0xA11CE ^ worker_id as u64);
        let mut reads = 0u32;
        let mut writes = 0u32;
        for operation in 0..100u32 {
            let timer = QueryTimer::start();
            if rng.next_u32() % 100 < mix.read_pct() as u32 {
                reads += 1;
                let page = block_on(docs.rql(
                    &plan.rql_source,
                    &Parameters::default(),
                    QueryRunOptions::default(),
                ))
                .map_err(|error| AdapterError::Execute(format!("worker mixed read: {error}")))?;
                examined_documents =
                    examined_documents.saturating_add(page.coverage.examined_documents);
                logical_bytes_examined =
                    logical_bytes_examined.saturating_add(page.logical_bytes_examined);
                coverage_complete &= page.coverage.complete;
                plan_hash.get_or_insert(page.plan_hash);
            } else {
                writes += 1;
                let key = format!("w-{worker_id:04}-{operation:08}");
                block_on(docs.put(
                    key,
                    &serde_json::json!({
                        "status": "open",
                        "sel_bucket": "MISS",
                        "amount": 1,
                        "region": "r0",
                        "score": 0,
                        "i": operation,
                        "worker": worker_id,
                    }),
                ))
                .map_err(|error| AdapterError::Execute(format!("worker mixed write: {error}")))?;
            }
            latency_ns.push(timer.elapsed_ns());
        }
        if reads == 0 || writes == 0 {
            return Err(AdapterError::Execute(format!(
                "worker {worker_id} mixed schedule did not exercise both paths: reads={reads} writes={writes}"
            )));
        }
        let page = block_on(docs.rql(
            &plan.rql_source,
            &Parameters::default(),
            QueryRunOptions::default(),
        ))
        .map_err(|error| AdapterError::Execute(format!("worker mixed final read: {error}")))?;
        pages = 1;
        plan_hash.get_or_insert(page.plan_hash);
        examined_documents = examined_documents.saturating_add(page.coverage.examined_documents);
        logical_bytes_examined = logical_bytes_examined.saturating_add(page.logical_bytes_examined);
        coverage_complete &= page.coverage.complete;
        rows.extend(page.rows.into_iter().map(|row| ResultRow {
            key: row.key,
            value: row.value,
        }));
        detail = format!("mixed_rw={} reads={reads} writes={writes}", mix.as_str());
    } else {
        loop {
            let timer = QueryTimer::start();
            let page =
                block_on(docs.rql(&plan.rql_source, &Parameters::default(), options.clone()))
                    .map_err(|error| AdapterError::Execute(format!("worker core rql: {error}")))?;
            latency_ns.push(timer.elapsed_ns());
            pages += 1;
            plan_hash.get_or_insert(page.plan_hash);
            examined_documents =
                examined_documents.saturating_add(page.coverage.examined_documents);
            logical_bytes_examined =
                logical_bytes_examined.saturating_add(page.logical_bytes_examined);
            coverage_complete &= page.coverage.complete;
            rows.extend(page.rows.into_iter().map(|row| ResultRow {
                key: row.key,
                value: row.value,
            }));
            let done = page.exhausted || page.next.is_none();
            if !plan.require_deep_cursor || done {
                break;
            }
            if pages >= 4_096 {
                return Err(AdapterError::Execute("cursor page drain safety cap".into()));
            }
            options.after = page.next;
        }
        detail = "core_rql=true".into();
    }

    Ok(ProductWorkerResult {
        rows,
        latency_ns,
        examined_documents,
        logical_bytes_examined,
        coverage_complete,
        plan_hash: plan_hash.unwrap_or([0; 32]),
        pages,
        detail,
    })
}

/// Execute one measured product cell through the bounded smart client. Kept
/// crate-visible so the explicit scaling campaign can include concurrency one
/// without falling back to the legacy synchronous adapter path.
pub(crate) fn execute_plan_embedded_smart_client(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
) -> Result<EngineRunOutcome, AdapterError> {
    let prepared = prepare_concurrent_product(work, plan)?;
    execute_prepared_smart_client(work, plan, &prepared)
}

/// Execute against a previously prepared deployment. Each call opens and
/// closes a fresh physical client; callers use this to prove real reopen while
/// retaining the same authoritative media and capability.
pub(crate) fn execute_prepared_smart_client(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
    prepared: &PreparedConcurrentProduct,
) -> Result<EngineRunOutcome, AdapterError> {
    execute_prepared_smart_client_inner(work, plan, prepared, true)
}

/// Execute without the ordinary same-connection warm-up. Reserved for a
/// campaign which has independently evidenced a successful page-cache drop.
pub(crate) fn execute_prepared_smart_client_without_warmup(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
    prepared: &PreparedConcurrentProduct,
) -> Result<EngineRunOutcome, AdapterError> {
    execute_prepared_smart_client_inner(work, plan, prepared, false)
}

fn execute_prepared_smart_client_inner(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
    prepared: &PreparedConcurrentProduct,
    warm_before_measurement: bool,
) -> Result<EngineRunOutcome, AdapterError> {
    let requested = plan.concurrency.max(1);
    let connection = block_on(DriverClient::open_embedded(
        EmbeddedOptions::new(prepared._directory.path())
            .workers(requested as usize)
            .queue_capacity((requested as usize).saturating_mul(4)),
    ))
    .map_err(|error| AdapterError::Execute(format!("driver open: {error}")))?;
    let heap = block_on(connection.open_heap(prepared.capability.clone()))
        .map_err(|error| AdapterError::Execute(format!("driver heap: {error}")))?;
    let docs: DriverCollection<Value> = block_on(heap.open_collection("docs"))
        .map_err(|error| AdapterError::Execute(format!("driver docs: {error}")))?;

    // Establish an actual warm state on the same connection before the timed
    // interval. The warm-up result is deliberately discarded.
    let warmup_operations = if warm_before_measurement {
        execute_driver_worker(heap.clone(), docs.clone(), plan, u32::MAX)?
            .latency_ns
            .len() as u64
    } else {
        0
    };

    // The coordinator participates so the measured wall interval starts only
    // after every worker has been created and is ready at the same gate.
    let barrier = Arc::new(std::sync::Barrier::new(requested as usize + 1));
    let plan = Arc::new(plan.clone());
    let mut handles = Vec::with_capacity(requested as usize);
    for worker_id in 0..requested {
        let barrier = Arc::clone(&barrier);
        let heap = heap.clone();
        let docs = docs.clone();
        let plan = Arc::clone(&plan);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            execute_driver_worker(heap, docs, &plan, worker_id)
        }));
    }
    let resource_sampler = ProcessResourceSampler::start();
    let workload_start = std::time::Instant::now();
    barrier.wait();
    let mut worker_results = Vec::with_capacity(requested as usize);
    for handle in handles {
        worker_results.push(
            handle
                .join()
                .map_err(|_| AdapterError::Execute("product worker panicked".into()))??,
        );
    }
    let workload_wall_ns = workload_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let measured_resources = resource_sampler.and_then(ProcessResourceSampler::finish);
    let inspection = connection.inspect();
    if inspection.peak_running < requested as usize {
        return Err(AdapterError::Execute(format!(
            "product concurrency dishonest: requested={requested} peak_running={} workers={}",
            inspection.peak_running, inspection.workers
        )));
    }

    let mut canonical = Vec::with_capacity(worker_results.len());
    for worker in &worker_results {
        canonical.push(canonicalize_rows(
            &worker.rows,
            plan.order_sensitive,
            worker.coverage_complete,
        ));
    }
    let expected = canonical
        .first()
        .ok_or_else(|| AdapterError::Execute("no product worker result".into()))?;
    if canonical.iter().any(|result| {
        result.values_digest != expected.values_digest || result.row_count != expected.row_count
    }) {
        return Err(AdapterError::Execute(
            "product worker result divergence under concurrency".into(),
        ));
    }

    let mut latency = LatencyCollector::new();
    let mut examined_documents = 0u64;
    let mut logical_bytes_examined = 0u64;
    let mut coverage_complete = true;
    for worker in &worker_results {
        for sample in &worker.latency_ns {
            latency.record_ns(*sample);
        }
        examined_documents = examined_documents.saturating_add(worker.examined_documents);
        logical_bytes_examined =
            logical_bytes_examined.saturating_add(worker.logical_bytes_examined);
        coverage_complete &= worker.coverage_complete;
    }
    let plan_hash = worker_results[0].plan_hash;
    let mut metrics = assemble_metrics(
        &latency,
        QueryPathMetrics {
            documents_examined: Some(examined_documents),
            logical_bytes_examined: Some(logical_bytes_examined),
            index_entries_examined: None,
            index_size_bytes: None,
            index_build_ns: prepared.index_build_ns,
            indexed_write_penalty_ns: None,
            explain_plan_digest: Some(format!("product_plan_hash:{}", hex::encode(plan_hash))),
        },
        Some(plan.lifecycle.class),
        Some(plan.lifecycle.cold_method.as_str().into()),
        Some(expected.values_digest.clone()),
        Some(coverage_complete),
        Some(true),
    );
    if let Some(resources) = measured_resources {
        metrics.resource = resources;
    }
    if logical_bytes_examined > 0 {
        if let Some(physical_bytes_read) = metrics.resource.physical_bytes_read {
            metrics.resource.read_amplification =
                Some(physical_bytes_read as f64 / logical_bytes_examined as f64);
        }
    }
    let worker_pages: u32 = worker_results.iter().map(|worker| worker.pages).sum();
    let workload_operations: u64 = worker_results
        .iter()
        .map(|worker| worker.latency_ns.len() as u64)
        .sum();
    let aggregate_ops_per_s = if workload_wall_ns == 0 {
        0.0
    } else {
        workload_operations as f64 * 1_000_000_000.0 / workload_wall_ns as f64
    };
    let worker_detail = worker_results
        .first()
        .map(|worker| worker.detail.as_str())
        .unwrap_or("");
    block_on(connection.close())
        .map_err(|error| AdapterError::Execute(format!("driver close: {error}")))?;

    Ok(EngineRunOutcome {
        engine: EngineId::ResidiuumEmbedded,
        execution_kind: ExecutionKind::Product,
        status: AdapterStatus::Ready,
        result: Some(expected.clone()),
        metrics: Some(metrics),
        refuse_code: None,
        detail: Some(format!(
            "product_embedded_shared plan={} concurrency_requested={} concurrency_achieved={} workers={} warmup_operations={} worker_pages={} workload_wall_ns={} workload_operations={} aggregate_ops_per_s={:.6} indexes={} index_entries_built={} {}",
            plan.plan_id,
            requested,
            inspection.peak_running,
            inspection.workers,
            warmup_operations,
            worker_pages,
            workload_wall_ns,
            workload_operations,
            aggregate_ops_per_s,
            plan.indexes.len(),
            prepared.index_entries_built,
            worker_detail,
        )),
        shared_work_hash: Some(work.content_hash.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_plan::MeasuredCellPlan;
    use crate::cells::MandatoryCell;
    use crate::dataset::DatasetSpec;
    use crate::engine::{AdapterStatus, EngineAdapter, ResidiuumEmbeddedAdapter};
    use crate::generator::generate_dataset;
    use crate::shared_work::SharedLogicalWork;

    #[test]
    fn product_embedded_key_get_compiles_and_runs() {
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 42);
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter.load_shared_work(&work).expect("load shared");
        assert_eq!(adapter.status(), AdapterStatus::Ready);
        let out = adapter.execute_plan(&plan).expect("execute");
        assert_eq!(out.engine, EngineId::ResidiuumEmbedded);
        assert_eq!(out.status, AdapterStatus::Ready);
        assert_eq!(
            out.shared_work_hash.as_deref(),
            Some(work.content_hash.as_str())
        );
        // Either a successful page or an honest product refuse code (not a panic).
        match (&out.result, &out.refuse_code) {
            (Some(r), None) => {
                assert_eq!(r.row_count, 1, "key get should hit d-00000000");
                assert!(out.metrics.is_some());
            }
            (None, Some(code)) => {
                assert!(
                    code.starts_with("product_rql_refuse:")
                        || code.starts_with("product_residual:"),
                    "unexpected refuse {code}"
                );
            }
            other => panic!("unexpected outcome {other:?}"),
        }
    }

    #[test]
    fn product_refuse_code_uses_stable_errorcode_str() {
        // Compile-time + runtime: Error::code().as_str() path is exercised when
        // rql refuses; for a known-bad source we still expect a refuse code.
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 1);
        plan.rql_source = "from docs where this is not valid rql !!!".into();
        let ds = generate_dataset(&DatasetSpec::smoke_default(1));
        let work = SharedLogicalWork::from_dataset(ds);
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter.load_shared_work(&work).unwrap();
        let out = adapter.execute_plan(&plan).unwrap();
        if let Some(code) = out.refuse_code {
            assert_eq!(out.status, AdapterStatus::Refused);
            assert!(
                code.starts_with("product_rql_refuse:") || code.starts_with("product_residual:"),
                "{code}"
            );
            // Must not be Debug-style "QueryInvalid" alone without stable mapping path.
            assert!(!code.contains("ErrorCode"));
        }
    }

    #[test]
    fn product_deep_cursor_drains_multiple_pages() {
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::FirstAndDeepCursor, 42);
        plan.page_size = Some(8);
        plan.require_deep_cursor = true;
        let ds = generate_dataset(&plan.dataset);
        let expected = ds.collections["docs"].len();
        let work = SharedLogicalWork::from_dataset(ds);
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter.load_shared_work(&work).unwrap();
        let out = adapter.execute_plan(&plan).unwrap();
        assert_eq!(out.status, AdapterStatus::Ready, "{:?}", out.detail);
        assert_eq!(out.result.unwrap().row_count, expected as u64);
        assert!(out.detail.as_deref().unwrap().contains("pages=8"));
    }

    #[test]
    fn product_full_enrich_executes_real_attach() {
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::EnrichCardinalities, 42);
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter.load_shared_work(&work).unwrap();
        let out = adapter.execute_plan(&plan).unwrap();
        assert_eq!(out.status, AdapterStatus::Ready, "{out:?}");
        assert!(out
            .result
            .as_ref()
            .is_some_and(|result| result.row_count > 0));
        assert!(out.detail.as_deref().unwrap().contains("full_rql=true"));
        assert!(out.refuse_code.is_none());
    }

    #[test]
    fn product_mixed_rw_executes_reads_and_writes() {
        for mix in [ReadWriteMix::R90W10, ReadWriteMix::R70W30] {
            let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::MixedReadWrite, 42);
            plan.rw_mix = Some(mix);
            let ds = generate_dataset(&plan.dataset);
            let work = SharedLogicalWork::from_dataset(ds);
            let mut adapter = ResidiuumEmbeddedAdapter::default();
            adapter.load_shared_work(&work).unwrap();
            let out = adapter.execute_plan(&plan).unwrap();
            assert_eq!(out.status, AdapterStatus::Ready, "{out:?}");
            let detail = out.detail.as_deref().unwrap();
            assert!(detail.contains("mixed_rw="), "{detail}");
            assert!(detail.contains("reads="), "{detail}");
            assert!(detail.contains("writes="), "{detail}");
            assert!(out.metrics.is_some());
        }
    }

    #[test]
    fn product_concurrency_uses_one_shared_driver_connection() {
        for (cell, concurrency) in [
            (MandatoryCell::KeyGet, 4),
            (MandatoryCell::EnrichCardinalities, 4),
            (MandatoryCell::FirstAndDeepCursor, 2),
            (MandatoryCell::MixedReadWrite, 2),
        ] {
            let plan = MeasuredCellPlan::smoke_for(cell, 42).with_concurrency(concurrency, false);
            let work = SharedLogicalWork::from_dataset(generate_dataset(&plan.dataset));
            let mut adapter = ResidiuumEmbeddedAdapter::default();
            adapter.load_shared_work(&work).unwrap();
            let out = adapter.execute_plan(&plan).unwrap();
            assert_eq!(out.status, AdapterStatus::Ready, "{out:?}");
            let detail = out.detail.as_deref().unwrap();
            assert!(
                detail.contains(&format!("concurrency_requested={concurrency}")),
                "{detail}"
            );
            assert!(
                detail.contains(&format!("concurrency_achieved={concurrency}")),
                "{detail}"
            );
            assert!(detail.contains("product_embedded_shared"), "{detail}");
            assert!(out.result.is_some());
            let resource = &out.metrics.as_ref().unwrap().resource;
            assert!(resource.cpu_time_ns.unwrap_or(0) > 0, "{resource:?}");
            assert!(resource.rss_bytes.unwrap_or(0) > 0, "{resource:?}");
            assert!(
                resource.peak_rss_bytes.unwrap_or(0) >= resource.rss_bytes.unwrap_or(0),
                "{resource:?}"
            );
            assert!(resource.physical_bytes_read.is_some(), "{resource:?}");
            assert!(resource.physical_bytes_written.is_some(), "{resource:?}");
            assert!(resource.read_amplification.is_some(), "{resource:?}");
            assert!(
                out.metrics
                    .as_ref()
                    .unwrap()
                    .path
                    .logical_bytes_examined
                    .unwrap_or(0)
                    > 0,
                "{:?}",
                out.metrics
            );
        }
    }

    #[test]
    fn streaming_campaign_loader_and_unwarmed_scan_are_product_real() {
        let directory = tempfile::tempdir().unwrap();
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::FirstAndDeepCursor, 77);
        plan.rql_source = "from docs order by _key asc project i".into();
        plan.page_size = Some(16);
        plan.require_deep_cursor = true;
        let spec = plan.dataset.clone();
        let (prepared, content_hash, loaded_logical_bytes) =
            prepare_streaming_product(directory.path(), &spec, 1).unwrap();
        let work = SharedLogicalWork {
            content_hash: content_hash.clone(),
            doc_count: spec.doc_count,
            seed: spec.seed,
            dataset: crate::generator::LogicalDataset {
                spec,
                collections: std::collections::BTreeMap::new(),
                content_hash,
            },
        };
        let outcome =
            execute_prepared_smart_client_without_warmup(&work, &plan, &prepared).unwrap();
        assert_eq!(outcome.status, AdapterStatus::Ready, "{outcome:?}");
        assert_eq!(outcome.result.as_ref().unwrap().row_count, work.doc_count);
        assert!(
            outcome
                .metrics
                .as_ref()
                .unwrap()
                .path
                .logical_bytes_examined
                .unwrap_or(0)
                >= loaded_logical_bytes
        );
        assert!(outcome
            .detail
            .as_deref()
            .unwrap()
            .contains("warmup_operations=0"));
    }

    #[test]
    fn product_refusal_is_not_ready() {
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 1);
        plan.rql_source = "from docs where this is not valid rql !!!".into();
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter.load_shared_work(&work).unwrap();
        let out = adapter.execute_plan(&plan).unwrap();
        assert_eq!(out.status, AdapterStatus::Refused);
        assert!(!out.is_product_ready());
    }
}
