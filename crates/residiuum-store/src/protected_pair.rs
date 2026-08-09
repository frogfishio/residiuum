//! Crash recovery for Protected Seal-Pair Pipeline partial states.

use crate::error::StoreError;
use crate::ids::segment_seq_from_id;
use crate::layout::{list_residiuum_files, segment_id_from_filename, StorePaths};
use crate::recovery_shadow::{
    load_protected_coverage, publish_prepared_shadow, publish_protected_coverage, shadow_dir,
    shadow_path, try_load_shadow, PreparedShadowPublish, ShadowLoad,
};
use crate::seal_pipeline::list_pending_paths;
use std::fs;
use std::path::PathBuf;

/// Complete or clean partial auth+Shadow pairs after crash.
///
/// States:
/// - `pending` + `*.rsh.dual.tmp` → finish auth rename, publish Shadow, frontier
/// - `sealed` + `*.rsh.dual.tmp` → publish Shadow, frontier
/// - `sealed` + verified `.rsh` but coverage missing durable → claim P★
/// - `pending` only → finish auth (no P★ until Shadow rebuilt elsewhere)
/// - orphan `*.rsh.dual.tmp` without pending/sealed → delete staging
pub fn recover_protected_pairs(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<usize, StoreError> {
    let mut n = 0usize;
    let shadow = shadow_dir(paths);
    let mut tmps: Vec<(PathBuf, [u8; 16])> = Vec::new();
    if shadow.is_dir() {
        for ent in fs::read_dir(&shadow)? {
            let p = ent?.path();
            let name = p
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if let Some(stem) = name.strip_suffix(".rsh.dual.tmp") {
                if let Some(id) = crate::layout::unhex16(stem) {
                    tmps.push((p, id));
                }
            }
        }
    }

    for (tmp, segment_id) in tmps {
        let pending = paths.pending_segment(&segment_id);
        let sealed = paths.sealed_segment(&segment_id);
        let rsh = shadow_path(paths, &segment_id);

        if rsh.is_file() {
            // Shadow already published — drop orphan staging.
            let _ = fs::remove_file(&tmp);
            continue;
        }

        if pending.is_file() && !sealed.is_file() {
            crate::media_inventory::rename_exclusive(&pending, &sealed, segment_id)?;
            let _ = crate::atomic_file::sync_dir(&paths.segments_dir());
        }

        if !sealed.is_file() {
            // No authoritative media — abandon staging (do not claim P★).
            let _ = fs::remove_file(&tmp);
            continue;
        }

        let meta = fs::metadata(&sealed)?;
        let shard = PreparedShadowPublish::load_shard_meta(paths, &segment_id)
            .or_else(|| shard_hint_from_coverage(paths, store_id, &segment_id))
            .unwrap_or(0);
        let prepared = PreparedShadowPublish {
            store_id,
            segment_id,
            shard,
            tmp_path: tmp,
            encoded_len: meta.len().saturating_sub(40 + 32), // best-effort; publish reopens
            staging_write_operations: 0,
            staging_write_bytes: 0,
            staging_write_ns: 0,
        };
        // encoded_len is only for telemetry in publish_prepared_shadow.
        publish_prepared_shadow(prepared, paths)?;
        claim_pair_protection(paths, store_id, &segment_id, shard)?;
        n = n.saturating_add(1);
    }

    // Deferred 1 MiB mirror intents have no write-time Shadow temp. Complete
    // them from authoritative pending/sealed media before claiming protection.
    if shadow.is_dir() {
        let mut intents = Vec::new();
        for ent in fs::read_dir(&shadow)? {
            let p = ent?.path();
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            let Some(stem) = name.strip_suffix(".rsh.dual.shard") else {
                continue;
            };
            if let Some(segment_id) = crate::layout::unhex16(stem) {
                intents.push((p, segment_id));
            }
        }
        for (intent, segment_id) in intents {
            let pending = paths.pending_segment(&segment_id);
            let sealed = paths.sealed_segment(&segment_id);
            let rsh = shadow_path(paths, &segment_id);
            if rsh.is_file() {
                continue; // claim_missing_frontiers verifies then clears intent
            }
            if pending.is_file() && !sealed.is_file() {
                crate::media_inventory::rename_exclusive(&pending, &sealed, segment_id)?;
                let _ = crate::atomic_file::sync_dir(&paths.segments_dir());
            }
            if !sealed.is_file() {
                let _ = fs::remove_file(&intent);
                continue;
            }
            let shard = PreparedShadowPublish::load_shard_meta(paths, &segment_id).unwrap_or(0);
            crate::recovery_shadow::publish_mirror_shadow_from_path(
                paths,
                store_id,
                &segment_id,
                &sealed,
            )?;
            claim_pair_protection(paths, store_id, &segment_id, shard)?;
            n = n.saturating_add(1);
        }
    }

    // Sealed + verified `.rsh` but frontier never published (e.g. crash after
    // rename, before coverage write). Claim P★ only when both sides verify.
    n = n.saturating_add(claim_missing_frontiers(paths, store_id)?);

    // Pending without Shadow staging: finish auth only (Materialized dual-run /
    // non-pair). Do not advance frontier.
    for pending_path in list_pending_paths(paths)? {
        let Some(segment_id) = segment_id_from_filename(&pending_path) else {
            continue;
        };
        let sealed = paths.sealed_segment(&segment_id);
        if sealed.is_file() {
            // Auth already published — drop pending without replacing sealed.
            let _ = fs::remove_file(&pending_path);
            continue;
        }
        crate::media_inventory::rename_exclusive(&pending_path, &sealed, segment_id)?;
        n = n.saturating_add(1);
    }

    let _ = list_residiuum_files;
    Ok(n)
}

fn claim_pair_protection(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    shard: u16,
) -> Result<(), StoreError> {
    let _ = crate::recovery_shadow::note_segment_sealed(paths, store_id, segment_id, shard);
    let seq = segment_seq_from_id(segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    cov.store_id = store_id;
    cov.note_durable(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    PreparedShadowPublish::clear_shard_meta(paths, segment_id);
    Ok(())
}

/// Prefer durable sidecar; else sealed coverage note; else `None`.
fn shard_hint_from_coverage(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
) -> Option<u16> {
    let seq = segment_seq_from_id(segment_id);
    let cov = load_protected_coverage(paths, store_id).ok()?;
    for (&shard, sealed) in &cov.sealed_by_shard {
        if sealed.contains(&seq) {
            return Some(shard);
        }
    }
    None
}

/// Advance coverage for sealed segments that already have a verified `.rsh`.
fn claim_missing_frontiers(paths: &StorePaths, store_id: [u8; 16]) -> Result<usize, StoreError> {
    let mut n = 0usize;
    let shadow = shadow_dir(paths);
    if !shadow.is_dir() {
        return Ok(0);
    }
    let cov = load_protected_coverage(paths, store_id)?;
    for ent in fs::read_dir(&shadow)? {
        let path = ent?.path();
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if !name.ends_with(".rsh") || name.contains(".dual.tmp") {
            continue;
        }
        let Some(stem) = name.strip_suffix(".rsh") else {
            continue;
        };
        let Some(segment_id) = crate::layout::unhex16(stem) else {
            continue;
        };
        if !paths.sealed_segment(&segment_id).is_file() {
            continue;
        }
        let seq = segment_seq_from_id(&segment_id);
        let shard = PreparedShadowPublish::load_shard_meta(paths, &segment_id)
            .or_else(|| {
                for (&sh, sealed) in &cov.sealed_by_shard {
                    if sealed.contains(&seq) {
                        return Some(sh);
                    }
                }
                None
            })
            .unwrap_or(0);
        if cov
            .durable_by_shard
            .get(&shard)
            .is_some_and(|s| s.contains(&seq))
        {
            continue;
        }
        // Also skip if any shard already claimed this seq durable (legacy).
        if cov.durable_by_shard.values().any(|s| s.contains(&seq)) {
            continue;
        }
        match try_load_shadow(&path, Some(store_id))? {
            ShadowLoad::Ok(_) => {
                claim_pair_protection(paths, store_id, &segment_id, shard)?;
                n = n.saturating_add(1);
            }
            _ => continue,
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery_shadow::{
        load_protected_coverage, protection_lag_from_coverage, ShadowDualStream,
    };
    use residiuum_format::{
        encode_frame, ActiveSegment, FrameHeader, FrameKind, FrameParts, SafetyLimits, SegmentId,
        WIRE_MAJOR, WIRE_MINOR,
    };
    use std::io::Write;
    use tempfile::tempdir;

    fn tiny_image(store_id: [u8; 16], segment_id: [u8; 16]) -> Vec<u8> {
        let ids = SegmentId::new(store_id, segment_id);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        let env = residiuum_format::EMPTY_ENVELOPE;
        let body = b"pair-recover";
        let header = FrameHeader {
            wire_major: WIRE_MAJOR,
            wire_minor: WIRE_MINOR,
            frame_kind: FrameKind::ItemEvent.as_u8(),
            flags: Default::default(),
            envelope_len: env.len() as u32,
            body_len: body.len() as u64,
            logical_len: body.len() as u64,
            writer_sequence: active.writer_sequence(),
            event_id: [3u8; 16],
        };
        let frame = encode_frame(&FrameParts {
            header,
            envelope: env.to_vec(),
            body: body.to_vec(),
        })
        .unwrap();
        active.append_preencoded_frame(&frame).unwrap();
        active.as_bytes().to_vec()
    }

    #[test]
    fn recover_sealed_plus_staging_claims_p_star() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [7u8; 16];
        let segment_id = crate::ids::mint_sortable_segment_id(1, &store_id);
        let image = tiny_image(store_id, segment_id);
        fs::create_dir_all(paths.segments_dir()).unwrap();
        fs::write(paths.sealed_segment(&segment_id), &image).unwrap();

        let mut dual = ShadowDualStream::begin(&paths, store_id, segment_id).unwrap();
        dual.append_image_chunk(&image).unwrap();
        let prepared = dual.prepare_async_publish(&[], 0).unwrap();
        let tmp = prepared.tmp_path.clone();
        prepared.persist_shard_meta(&paths).unwrap();
        // Keep staging on disk (simulate crash before worker publish).
        std::mem::forget(prepared);
        assert!(tmp.is_file());

        let n = recover_protected_pairs(&paths, store_id).unwrap();
        assert!(n >= 1);
        assert!(shadow_path(&paths, &segment_id).is_file());
        assert!(!tmp.exists());
        let lag = protection_lag_from_coverage(&load_protected_coverage(&paths, store_id).unwrap());
        assert_eq!(lag.protected_frontier, 1);
        assert_eq!(lag.lag, 0);
    }

    #[test]
    fn recover_deferred_intent_builds_shadow_before_claiming_p_star() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [0xD1u8; 16];
        let segment_id = crate::ids::mint_sortable_segment_id(4, &store_id);
        let image = tiny_image(store_id, segment_id);
        fs::create_dir_all(paths.segments_dir()).unwrap();
        fs::write(paths.sealed_segment(&segment_id), &image).unwrap();
        PreparedShadowPublish::persist_shard_meta_for(&paths, &segment_id, 5).unwrap();

        let n = recover_protected_pairs(&paths, store_id).unwrap();
        assert!(n >= 1);
        assert!(matches!(
            try_load_shadow(&shadow_path(&paths, &segment_id), Some(store_id)).unwrap(),
            ShadowLoad::Ok(_)
        ));
        let cov = load_protected_coverage(&paths, store_id).unwrap();
        assert!(cov
            .durable_by_shard
            .get(&5)
            .is_some_and(|seqs| seqs.contains(&4)));
        assert!(!PreparedShadowPublish::shard_meta_path(&paths, &segment_id).exists());
    }

    #[test]
    fn recover_uses_shard_sidecar_not_zero() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [0xAAu8; 16];
        let segment_id = crate::ids::mint_sortable_segment_id(2, &store_id);
        let image = tiny_image(store_id, segment_id);
        fs::create_dir_all(paths.segments_dir()).unwrap();
        fs::write(paths.sealed_segment(&segment_id), &image).unwrap();

        let mut dual = ShadowDualStream::begin(&paths, store_id, segment_id).unwrap();
        dual.append_image_chunk(&image).unwrap();
        let prepared = dual.prepare_async_publish(&[], 3).unwrap();
        prepared.persist_shard_meta(&paths).unwrap();
        crate::recovery_shadow::publish_prepared_shadow(prepared, &paths).unwrap();
        // Crash before frontier — shard sidecar may remain if publish cleared it;
        // re-note sealed on shard 3 then recover durable.
        let _ = crate::recovery_shadow::note_segment_sealed(&paths, store_id, &segment_id, 3);
        // Re-write sidecar to simulate crash after rename before clear.
        fs::write(
            PreparedShadowPublish::shard_meta_path(&paths, &segment_id),
            3u16.to_le_bytes(),
        )
        .unwrap();

        let n = recover_protected_pairs(&paths, store_id).unwrap();
        assert!(n >= 1);
        let cov = load_protected_coverage(&paths, store_id).unwrap();
        assert!(
            cov.durable_by_shard.get(&3).is_some_and(|s| s.contains(&2)),
            "durable must land on shard 3, not 0: {cov:?}"
        );
        assert!(!cov.durable_by_shard.get(&0).is_some_and(|s| s.contains(&2)));
    }

    #[test]
    fn recover_orphan_staging_without_auth_deletes_tmp() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [8u8; 16];
        let segment_id = crate::ids::mint_sortable_segment_id(3, &store_id);
        fs::create_dir_all(shadow_dir(&paths)).unwrap();
        let tmp = shadow_dir(&paths).join(format!(
            "{}.rsh.dual.tmp",
            crate::layout::hex16(&segment_id)
        ));
        {
            let mut f = fs::File::create(&tmp).unwrap();
            f.write_all(b"orphan").unwrap();
        }
        let _ = recover_protected_pairs(&paths, store_id).unwrap();
        assert!(!tmp.exists());
        let lag = protection_lag_from_coverage(&load_protected_coverage(&paths, store_id).unwrap());
        assert_eq!(lag.protected_frontier, 0);
    }

    #[test]
    fn recover_sealed_plus_rsh_without_frontier_claims() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [9u8; 16];
        let segment_id = crate::ids::mint_sortable_segment_id(1, &store_id);
        let image = tiny_image(store_id, segment_id);
        fs::create_dir_all(paths.segments_dir()).unwrap();
        fs::write(paths.sealed_segment(&segment_id), &image).unwrap();

        let mut dual = ShadowDualStream::begin(&paths, store_id, segment_id).unwrap();
        dual.append_image_chunk(&image).unwrap();
        let prepared = dual.prepare_async_publish(&[], 0).unwrap();
        crate::recovery_shadow::publish_prepared_shadow(prepared, &paths).unwrap();
        // Deliberately skip frontier publish (crash after Shadow rename).
        let lag0 =
            protection_lag_from_coverage(&load_protected_coverage(&paths, store_id).unwrap());
        assert_eq!(lag0.protected_frontier, 0);

        let n = recover_protected_pairs(&paths, store_id).unwrap();
        assert!(n >= 1);
        let lag = protection_lag_from_coverage(&load_protected_coverage(&paths, store_id).unwrap());
        assert_eq!(lag.protected_frontier, 1);
        assert_eq!(lag.lag, 0);
    }
}
