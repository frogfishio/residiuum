//! HP-009 Accept: purge receipt, payload-only restore, damage isolation,
//! data-key destruction, tombstones, DR retain-ID takeover,
//! incomplete media-domain purge, retention scheduler,
//! live filesystem multi-tier media wipe.

use residiuum_heap::{DeploymentId, HeapAdministrativeState, HeapId, HeapSlot, SecurityRevision};
use residiuum_store::{
    active_snapshot, build_backup_manifest, classify_mixed_heap_frame, decode_purge_receipt,
    destroy_data_key, disaster_recovery_restore_retaining_id, encode_purge_receipt,
    heap_binding_envelope, heap_label_envelope, heap_object_media_dir, labelled_unit_readable,
    load_identity_tombstone, old_deployment_credential_invalid, refuse_access_from_payload_restore,
    refuse_clear_tombstone_via_payload_restore, refuse_retain_id_without_ceremony,
    restore_payload_to_new_heap, verify_purge_receipt, DataKeyHandle, DataKeyProvider,
    DisasterRecoveryCeremony, DisasterRecoveryPackage, HeapLifecycle, HeapRetentionPolicy,
    HsmDataKeyConfig, HsmDataKeyProvider, InProcessDataKeyProvider, MediaDomain,
    MixedHeapSalvageClass, PurgeCoverageUnit, TierClass, TombstoneKind,
};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

fn uuidish(seed: u8) -> [u8; 16] {
    let mut id = [seed; 16];
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    id
}

fn op(seed: u8) -> [u8; 16] {
    uuidish(seed)
}

fn slot_for(heap_seed: u8) -> Arc<HeapSlot> {
    let deployment = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(heap_seed)).unwrap();
    let snap = active_snapshot(deployment, heap, [0xab; 32]).unwrap();
    Arc::new(HeapSlot::new(snap))
}

#[test]
fn purge_emits_verifiable_receipt_and_holds_block() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0x10);
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));

    life.suspend(op(0x21)).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Suspended);
    life.resume(op(0x22)).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Active);

    life.retire(op(0x23)).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Retired);

    life.place_hold("legal-1").unwrap();
    let blocked = life.begin_purge(op(0x24), vec![uuidish(0x30)]).unwrap_err();
    assert!(blocked.to_string().contains("hold"));

    life.release_hold("legal-1").unwrap();
    let coverage = vec![uuidish(0x31), uuidish(0x32)];
    let plan = life.begin_purge(op(0x25), coverage.clone()).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Purging);

    // Incomplete coverage cannot complete.
    life.destroy_coverage_unit(coverage[0]).unwrap();
    assert!(life.complete_purge(plan.operation_id).is_err());

    life.destroy_coverage_unit(coverage[1]).unwrap();
    // Idempotent destroy.
    life.destroy_coverage_unit(coverage[1]).unwrap();

    let receipt = life.complete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Purged);
    assert!(slot.load().security_revision.get() > 1);
    verify_purge_receipt(&receipt, &coverage).unwrap();

    let encoded = encode_purge_receipt(&receipt).unwrap();
    let decoded = decode_purge_receipt(&encoded).unwrap();
    assert_eq!(decoded, receipt);
    verify_purge_receipt(&decoded, &coverage).unwrap();

    // Tampered coverage fails verification.
    assert!(verify_purge_receipt(&receipt, &[uuidish(0x99)]).is_err());
}

#[test]
fn payload_only_restore_cannot_grant_access() {
    let tmp = TempDir::new().unwrap();
    let source = uuidish(0x40);
    let restored =
        restore_payload_to_new_heap(tmp.path(), source, b"secret-bytes", "users").unwrap();
    assert_ne!(restored.new_heap_id, source);
    assert_eq!(restored.source_heap_id, source);

    let deny = refuse_access_from_payload_restore(&restored).unwrap_err();
    assert!(deny.to_string().contains("cannot grant access"), "{deny}");

    // Manifest lists exact heap ids and carries no authority fields.
    let manifest = build_backup_manifest(uuidish(0x02), &[source, uuidish(0x41)]).unwrap();
    assert_eq!(manifest.heap_ids.len(), 2);
    assert!(!manifest.manifest_hash.iter().all(|b| *b == 0));
}

#[test]
fn damage_to_one_heap_leaves_other_readable() {
    let heap_a = uuidish(0x50);
    let heap_b = uuidish(0x51);
    let env_a = heap_label_envelope(&heap_a).unwrap();
    let env_b = heap_label_envelope(&heap_b).unwrap();

    assert!(labelled_unit_readable(&heap_a, &env_a, &env_a));
    assert!(labelled_unit_readable(&heap_b, &env_b, &env_b));

    // Cross-heap damage / mix: A envelope against B binding fails closed.
    assert!(!labelled_unit_readable(&heap_a, &env_b, &env_b));
    assert!(!labelled_unit_readable(&heap_b, &env_a, &env_a));

    // Corrupt heap A envelope (simulate damaged unit).
    let mut damaged = env_a.clone();
    if let Some(last) = damaged.last_mut() {
        *last ^= 0xff;
    }
    assert!(!labelled_unit_readable(&heap_a, &damaged, &damaged));

    // Independently owned heap B remains readable.
    assert!(labelled_unit_readable(&heap_b, &env_b, &env_b));

    // Security revision bump on purge invalidates prior revision binding conceptually.
    let slot = slot_for(0x52);
    let before = slot.load().security_revision;
    let mut life = HeapLifecycle::open(TempDir::new().unwrap().path(), Arc::clone(&slot));
    life.retire(op(0x60)).unwrap();
    let cov = vec![uuidish(0x61)];
    let plan = life.begin_purge(op(0x62), cov.clone()).unwrap();
    life.destroy_coverage_unit(cov[0]).unwrap();
    life.complete_purge(plan.operation_id).unwrap();
    assert!(slot.load().security_revision.get() > before.get());
    assert_eq!(
        slot.load().administrative_state,
        HeapAdministrativeState::Purged
    );
    let _ = SecurityRevision::new(1);
}

#[test]
fn data_key_destruction_and_permanent_tombstone() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0x70);
    let heap = slot.load().heap_id.to_bytes();
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));

    let mut key = DataKeyHandle::generate(heap, b"wrap-me-secret").unwrap();
    assert!(!key.is_destroyed());
    let receipt = destroy_data_key(tmp.path(), &mut key).unwrap();
    assert!(key.is_destroyed());
    assert!(key.material().is_none());
    assert_eq!(receipt.heap_id, heap);
    assert!(!receipt.destroyed_fingerprint.iter().all(|b| *b == 0));
    assert!(destroy_data_key(tmp.path(), &mut key).is_err());

    life.retire(op(0x71)).unwrap();
    let retired = load_identity_tombstone(tmp.path(), &heap).unwrap();
    assert_eq!(retired.kind, TombstoneKind::Retired);

    let cov = vec![uuidish(0x72)];
    let plan = life.begin_purge(op(0x73), cov.clone()).unwrap();
    life.destroy_coverage_unit(cov[0]).unwrap();
    life.complete_purge(plan.operation_id).unwrap();

    let purged = load_identity_tombstone(tmp.path(), &heap).unwrap();
    assert_eq!(purged.kind, TombstoneKind::Purged);
    let blocked = refuse_clear_tombstone_via_payload_restore(tmp.path(), &heap).unwrap_err();
    assert!(blocked.to_string().contains("permanent"), "{blocked}");

    // DR retain-ID cannot revive a purged identity.
    let pkg = DisasterRecoveryPackage {
        heap_id: heap,
        backup_deployment_id: uuidish(0x01),
        backup_authority_epoch: 1,
        payload: b"x".to_vec(),
    };
    let ceremony = DisasterRecoveryCeremony {
        heap_id: heap,
        old_deployment_id: uuidish(0x01),
        new_deployment_id: uuidish(0x02),
        old_authority_epoch: 1,
        new_authority_epoch: 2,
        new_master_public_key: [0xcd; 32],
        recovery_authority_evidence: [0x11; 32],
    };
    let err =
        disaster_recovery_restore_retaining_id(tmp.path(), &pkg, &ceremony, None).unwrap_err();
    assert!(err.to_string().contains("purged"), "{err}");
}

#[test]
fn disaster_recovery_retain_id_takeover_fences_old_deployment() {
    let tmp = TempDir::new().unwrap();
    let old_dep = uuidish(0x81);
    let new_dep = uuidish(0x82);
    let heap = uuidish(0x83);
    let deployment = DeploymentId::from_bytes(old_dep).unwrap();
    let heap_id = HeapId::from_bytes(heap).unwrap();
    let snap = active_snapshot(deployment, heap_id, [0xab; 32]).unwrap();
    let slot = Arc::new(HeapSlot::new(snap));

    let package = DisasterRecoveryPackage {
        heap_id: heap,
        backup_deployment_id: old_dep,
        backup_authority_epoch: 1,
        payload: b"dr-payload".to_vec(),
    };

    // Concurrent live authority without ceremony MUST stop.
    let stop = refuse_retain_id_without_ceremony(&slot, &package).unwrap_err();
    assert!(stop.to_string().contains("ceremony"), "{stop}");

    let ceremony = DisasterRecoveryCeremony {
        heap_id: heap,
        old_deployment_id: old_dep,
        new_deployment_id: new_dep,
        old_authority_epoch: 1,
        new_authority_epoch: 2,
        new_master_public_key: [0xef; 32],
        recovery_authority_evidence: [0x22; 32],
    };
    let result =
        disaster_recovery_restore_retaining_id(tmp.path(), &package, &ceremony, Some(&slot))
            .unwrap();

    assert_eq!(result.snapshot.heap_id.to_bytes(), heap);
    assert_eq!(result.snapshot.deployment_id.to_bytes(), new_dep);
    assert_eq!(result.snapshot.authority_epoch.get(), 2);
    assert_eq!(result.fenced_deployment_id, old_dep);
    assert!(!result.takeover_evidence_hash.iter().all(|b| *b == 0));

    // Slot published the new head.
    let live = slot.load();
    assert_eq!(live.deployment_id.to_bytes(), new_dep);
    assert_eq!(live.authority_epoch.get(), 2);
    assert_eq!(live.master_public_key, [0xef; 32]);

    assert!(old_deployment_credential_invalid(&result, &old_dep));
    assert!(!old_deployment_credential_invalid(&result, &new_dep));

    // Zero evidence rejected.
    let mut bad = ceremony.clone();
    bad.recovery_authority_evidence = [0; 32];
    assert!(disaster_recovery_restore_retaining_id(tmp.path(), &package, &bad, None).is_err());
    // Epoch must advance by exactly one.
    bad = ceremony.clone();
    bad.recovery_authority_evidence = [0x33; 32];
    bad.new_authority_epoch = 5;
    assert!(disaster_recovery_restore_retaining_id(tmp.path(), &package, &bad, None).is_err());
}

#[test]
fn incomplete_purge_unavailable_tier_or_replica_stays_retired() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0x90);
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.retire(op(0x91)).unwrap();

    let hot_copy = uuidish(0x92);
    let replica_copy = uuidish(0x93);
    let replica_id = uuidish(0x94);
    let units = vec![
        PurgeCoverageUnit {
            object_id: hot_copy,
            domain: MediaDomain::Tier(TierClass::Hot),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: replica_copy,
            domain: MediaDomain::Replica { replica_id },
            available: false,
        },
        PurgeCoverageUnit {
            object_id: uuidish(0x95),
            domain: MediaDomain::Tier(TierClass::Cold),
            available: false,
        },
    ];
    let plan = life
        .begin_purge_media(op(0x96), units, 1_700_000_000)
        .unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Purging);
    assert_eq!(life.unavailable_purge_domains().len(), 2);

    life.destroy_coverage_unit(hot_copy).unwrap();
    // Unavailable replica/tier units cannot be destroyed.
    let blocked = life.destroy_coverage_unit(replica_copy).unwrap_err();
    assert!(blocked.to_string().contains("unavailable"), "{blocked}");

    let complete_err = life.complete_purge(plan.operation_id).unwrap_err();
    assert!(
        complete_err.to_string().contains("unavailable"),
        "{complete_err}"
    );
    assert_eq!(life.state(), HeapAdministrativeState::Purging);

    let incomplete = life.abort_incomplete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Retired);
    assert_ne!(life.state(), HeapAdministrativeState::Purged);
    assert_eq!(incomplete.destroyed_ids, vec![hot_copy]);
    assert!(incomplete.remaining_ids.contains(&replica_copy));
    assert_eq!(incomplete.unavailable_domains.len(), 2);
    assert!(incomplete
        .unavailable_domains
        .contains(&MediaDomain::Replica { replica_id }));
    assert!(incomplete
        .unavailable_domains
        .contains(&MediaDomain::Tier(TierClass::Cold)));
    assert!(!incomplete.result_hash.iter().all(|b| *b == 0));

    // Tombstone remains retired (not purged).
    let heap = slot.load().heap_id.to_bytes();
    let ts = load_identity_tombstone(tmp.path(), &heap).unwrap();
    assert_eq!(ts.kind, TombstoneKind::Retired);
}

#[test]
fn retention_scheduler_blocks_purge_until_window_elapses() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0xa0);
    let heap = slot.load().heap_id.to_bytes();
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.retire(op(0xa1)).unwrap();

    let retain_until = 1_700_000_100;
    life.retention_mut()
        .save_policy(
            tmp.path(),
            &HeapRetentionPolicy {
                heap_id: heap,
                minimum_retain_until_unix_s: retain_until,
            },
        )
        .unwrap();

    // Too early: purge blocked.
    let early = life
        .begin_purge_media(
            op(0xa2),
            vec![PurgeCoverageUnit {
                object_id: uuidish(0xa3),
                domain: MediaDomain::Tier(TierClass::Hot),
                available: true,
            }],
            retain_until - 1,
        )
        .unwrap_err();
    assert!(early.to_string().contains("minimum retention"), "{early}");
    assert_eq!(life.state(), HeapAdministrativeState::Retired);

    // Scheduler tick: not yet eligible.
    assert!(life
        .retention_mut()
        .tick_eligible(retain_until - 1)
        .is_empty());
    assert_eq!(life.retention_mut().tick_eligible(retain_until), vec![heap]);

    // After window: purge proceeds and can complete when all domains available.
    let units = vec![
        PurgeCoverageUnit {
            object_id: uuidish(0xa4),
            domain: MediaDomain::Tier(TierClass::Hot),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: uuidish(0xa5),
            domain: MediaDomain::Tier(TierClass::Warm),
            available: true,
        },
    ];
    let plan = life
        .begin_purge_media(op(0xa6), units.clone(), retain_until)
        .unwrap();
    for u in &units {
        life.destroy_coverage_unit(u.object_id).unwrap();
    }
    let receipt = life.complete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Purged);
    verify_purge_receipt(
        &receipt,
        &units.iter().map(|u| u.object_id).collect::<Vec<_>>(),
    )
    .unwrap();

    // Durable policy round-trip.
    let mut sched = residiuum_store::RetentionScheduler::new();
    let loaded = sched.load_policy(tmp.path(), &heap).unwrap().unwrap();
    assert_eq!(loaded.minimum_retain_until_unix_s, retain_until);
}

/// H4 / CPR-003: live multi-tier filesystem wipe under real media roots.
#[test]
fn live_filesystem_multi_tier_media_wipe() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0xb0);
    let heap = slot.load().heap_id.to_bytes();
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.retire(op(0xb1)).unwrap();

    // Three live tier roots on the filesystem (hot / warm / cold).
    let hot_root = tmp.path().join("media").join("hot");
    let warm_root = tmp.path().join("media").join("warm");
    let cold_root = tmp.path().join("media").join("cold");
    for r in [&hot_root, &warm_root, &cold_root] {
        fs::create_dir_all(r).unwrap();
    }

    let hot_obj = uuidish(0xb2);
    let warm_obj = uuidish(0xb3);
    let cold_obj = uuidish(0xb4);

    // Plant heap-scoped object media on each tier.
    for (root, oid, payload) in [
        (&hot_root, hot_obj, b"hot-copy" as &[u8]),
        (&warm_root, warm_obj, b"warm-copy"),
        (&cold_root, cold_obj, b"cold-copy"),
    ] {
        let dir = heap_object_media_dir(root, &heap, &oid);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("seg.residiuum"), payload).unwrap();
        assert!(dir.join("seg.residiuum").is_file());
    }

    let units = vec![
        PurgeCoverageUnit {
            object_id: hot_obj,
            domain: MediaDomain::Tier(TierClass::Hot),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: warm_obj,
            domain: MediaDomain::Tier(TierClass::Warm),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: cold_obj,
            domain: MediaDomain::Tier(TierClass::Cold),
            available: true,
        },
    ];
    let plan = life
        .begin_purge_media(op(0xb5), units.clone(), 1_700_000_000)
        .unwrap();

    life.destroy_coverage_unit_on_media(hot_obj, &hot_root)
        .unwrap();
    life.destroy_coverage_unit_on_media(warm_obj, &warm_root)
        .unwrap();
    life.destroy_coverage_unit_on_media(cold_obj, &cold_root)
        .unwrap();

    // Filesystem media gone.
    assert!(!heap_object_media_dir(&hot_root, &heap, &hot_obj).exists());
    assert!(!heap_object_media_dir(&warm_root, &heap, &warm_obj).exists());
    assert!(!heap_object_media_dir(&cold_root, &heap, &cold_obj).exists());

    let receipt = life.complete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Purged);
    verify_purge_receipt(
        &receipt,
        &units.iter().map(|u| u.object_id).collect::<Vec<_>>(),
    )
    .unwrap();
}

/// H4: AWS KMS config surface is live-ready (feature `aws-kms` builds connector).
#[test]
fn aws_kms_config_surface() {
    let cfg = HsmDataKeyConfig::aws_kms(
        "us-east-1",
        "alias/residiuum-heap",
        Some("https://kms.us-east-1.amazonaws.com".into()),
    );
    assert_eq!(cfg.backend, residiuum_store::HsmBackendKind::AwsKms);
    assert_eq!(cfg.key_label.as_deref(), Some("alias/residiuum-heap"));
    assert_eq!(cfg.slot_or_region.as_deref(), Some("us-east-1"));
    // Without feature, live provider is compiled out; config still documents the path.
    #[cfg(feature = "aws-kms")]
    {
        // Missing credentials → connect fails honestly (not mock).
        let err = residiuum_store::AwsKmsDataKeyProvider::from_config(&cfg);
        // May succeed building if env has AWS_ACCESS_KEY_ID from developer machine.
        // Either way provider_id path is aws-kms when constructed with creds.
        if let Ok(p) = err {
            assert_eq!(p.provider_id(), "aws-kms");
            assert!(p.capabilities().production_hsm);
        }
    }
}

/// H4 / C2: HSM scaffold refuses until configured; mock + in-process work.
#[test]
fn data_key_provider_hsm_scaffold_and_in_process() {
    let tmp = TempDir::new().unwrap();
    let heap = uuidish(0xd0);

    // Real backends refuse until a live connector is wired.
    let hsm = HsmDataKeyProvider::new("pkcs11");
    assert_eq!(hsm.provider_id(), "hsm-scaffold");
    assert!(!hsm.is_configured());
    assert!(!hsm.capabilities().generate);
    assert!(hsm.capabilities().production_hsm);
    let err = hsm.generate(heap).unwrap_err();
    assert!(err.to_string().contains("not configured"), "{err}");

    let aws = HsmDataKeyProvider::from_config(HsmDataKeyConfig::unconfigured(
        residiuum_store::HsmBackendKind::AwsKms,
    ));
    assert!(aws.generate(heap).is_err());

    // Mock HSM path for Accept: operational but not production_hsm.
    let mock = HsmDataKeyProvider::mock_for_tests();
    assert!(mock.is_configured());
    assert_eq!(mock.provider_id(), "hsm-mock-in-process");
    let caps = mock.capabilities();
    assert!(caps.generate && caps.destroy);
    assert!(!caps.production_hsm);
    let mut mh = mock.generate(heap).unwrap();
    let mrec = mock.destroy(tmp.path(), &mut mh).unwrap();
    assert!(mh.is_destroyed());
    assert_eq!(mrec.heap_id, heap);

    // Plain in-process product path.
    let proc = InProcessDataKeyProvider;
    assert_eq!(proc.provider_id(), "in-process");
    let mut handle = proc.generate(heap).unwrap();
    assert!(!handle.is_destroyed());
    let receipt = proc.destroy(tmp.path(), &mut handle).unwrap();
    assert!(handle.is_destroyed());
    assert_eq!(receipt.heap_id, heap);
    // Second destroy refuses.
    assert!(proc.destroy(tmp.path(), &mut handle).is_err());
}

/// H4 / C3: mixed multi-heap media classification for salvage (no reassignment).
#[test]
fn mixed_heap_salvage_classification_drill() {
    let heap_a = uuidish(0xe1);
    let heap_b = uuidish(0xe2);
    let env_a = heap_binding_envelope(&heap_a).unwrap();
    let env_b = heap_binding_envelope(&heap_b).unwrap();

    // Simulated volume inventory: frames labelled for A, B, cross-conflict, garbage.
    let empty: &[u8] = &[];
    let inventory: Vec<(&str, &[u8], &[u8])> = vec![
        ("a1", env_a.as_slice(), env_a.as_slice()),
        ("b1", env_b.as_slice(), env_b.as_slice()),
        ("a2", env_a.as_slice(), env_a.as_slice()),
        ("cross", env_a.as_slice(), env_b.as_slice()),
        ("empty", empty, empty),
    ];

    let mut for_a = Vec::new();
    let mut foreign = Vec::new();
    let mut conflict = Vec::new();
    let mut other = Vec::new();
    for (tag, seg, frame) in inventory {
        match classify_mixed_heap_frame(&heap_a, seg, frame, None) {
            MixedHeapSalvageClass::BelongingToBound => for_a.push(tag),
            MixedHeapSalvageClass::Foreign { claimed } => {
                assert_eq!(claimed, heap_b);
                foreign.push(tag);
            }
            MixedHeapSalvageClass::Conflict => conflict.push(tag),
            MixedHeapSalvageClass::Unknown | MixedHeapSalvageClass::Malformed => other.push(tag),
        }
    }
    assert_eq!(for_a, vec!["a1", "a2"]);
    assert_eq!(foreign, vec!["b1"]);
    assert_eq!(conflict, vec!["cross"]);
    assert_eq!(other, vec!["empty"]);
    // Bound-B salvage sees B frames and treats A as foreign.
    assert_eq!(
        classify_mixed_heap_frame(&heap_b, &env_b, &env_b, None),
        MixedHeapSalvageClass::BelongingToBound
    );
    assert_eq!(
        classify_mixed_heap_frame(&heap_b, &env_a, &env_a, None),
        MixedHeapSalvageClass::Foreign { claimed: heap_a }
    );
}

/// Unmounted tier root refuses live wipe; incomplete purge stays retired.
#[test]
fn live_media_wipe_unavailable_tier_root_stays_retired() {
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0xc0);
    let heap = slot.load().heap_id.to_bytes();
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.retire(op(0xc1)).unwrap();

    let hot_root = tmp.path().join("media").join("hot");
    fs::create_dir_all(&hot_root).unwrap();
    // cold root intentionally missing (unmounted).
    let cold_root = tmp.path().join("media").join("cold-unmounted");

    let hot_obj = uuidish(0xc2);
    let cold_obj = uuidish(0xc3);
    let dir = heap_object_media_dir(&hot_root, &heap, &hot_obj);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("seg.residiuum"), b"hot").unwrap();

    let units = vec![
        PurgeCoverageUnit {
            object_id: hot_obj,
            domain: MediaDomain::Tier(TierClass::Hot),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: cold_obj,
            domain: MediaDomain::Tier(TierClass::Cold),
            available: false, // domain flagged unavailable
        },
    ];
    let plan = life
        .begin_purge_media(op(0xc4), units, 1_700_000_000)
        .unwrap();

    life.destroy_coverage_unit_on_media(hot_obj, &hot_root)
        .unwrap();
    assert!(!heap_object_media_dir(&hot_root, &heap, &hot_obj).exists());

    // Unavailable domain cannot be wiped even if we point at a path.
    let err = life
        .destroy_coverage_unit_on_media(cold_obj, &cold_root)
        .unwrap_err();
    assert!(err.to_string().contains("unavailable"), "{err}");

    let incomplete = life.abort_incomplete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Retired);
    assert_eq!(incomplete.destroyed_ids, vec![hot_obj]);
    assert!(incomplete.remaining_ids.contains(&cold_obj));
}
