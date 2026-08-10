//! CSE-3 Stage 2l — A/B measurement: fresh-default CompactShadow vs activate path.
//!
//! Measurement only. Does **not** change the product default or reopen the 2k flip.
//! Product SoT remains **~21–23K** life=ack; ~30.9K stays a candidate figure.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store --release \
//!   --test cse3_stage2_step2l_tps_ab -- --nocapture
//!
//! CSE3_STEP2L_TARGET_BYTES=2147483648 CSE3_STEP2L_WORK=/tmp/cse3-2l \
//! cargo test -p residiuum-store --features legacy-raw-store --release \
//!   --test cse3_stage2_step2l_tps_ab step2l_tps_ab -- --nocapture
//! ```

use residiuum_store::{DurabilityMode, RecoveryMode, SealStageBreakdown, Store};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const PAYLOAD: usize = 8192;
const SEAL_EVERY: u64 = 64 * 1024 * 1024;
/// Default 256 MiB — enough to compare arms; use 2 GiB env for Step 9 parity.
const DEFAULT_TARGET: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Arm {
    FreshDefault,
    ActivatePath,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Arm::FreshDefault => "A_fresh_default",
            Arm::ActivatePath => "B_activate_path",
        }
    }
}

struct ArmReport {
    label: &'static str,
    target_bytes: u64,
    ack_ops_per_sec: f64,
    lifecycle_ops_per_sec: f64,
    dual_published: u64,
    dual_finalize_ns: u64,
    recovery_mode: RecoveryMode,
    shadow_dual_stream: bool,
    enrichment_enabled: bool,
    seal_threshold: u64,
    seal_breakdown: SealStageBreakdown,
}

fn parse_target() -> u64 {
    std::env::var("CSE3_STEP2L_TARGET_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TARGET)
}

fn work_root(target: u64, arm: Arm) -> PathBuf {
    let base = if let Ok(p) = std::env::var("CSE3_STEP2L_WORK") {
        PathBuf::from(p)
    } else {
        std::env::temp_dir().join(format!("cse3-2l-{target}"))
    };
    base.join(arm.label())
}

fn soft_seal_threshold(target_bytes: u64) -> u64 {
    target_bytes.saturating_mul(4).max(SEAL_EVERY * 4)
}

/// Seed + reopen outside the ack window — both arms end CompactShadow + dual.
fn setup_arm(root: &Path, arm: Arm) -> Store {
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root).unwrap();

    let mut store = match arm {
        Arm::FreshDefault => {
            let s = Store::create_with_shards(root, 1).unwrap();
            assert_eq!(s.recovery_mode(), RecoveryMode::CompactShadow);
            assert!(s.shadow_dual_stream());
            s
        }
        Arm::ActivatePath => {
            let mut s =
                Store::create_with_shards_mode(root, 1, RecoveryMode::Materialized).unwrap();
            assert_eq!(s.recovery_mode(), RecoveryMode::Materialized);
            assert!(!s.shadow_dual_stream());
            s.set_enrichment_enabled(true);
            s.set_seal_threshold(256 * 1024);
            let seed = vec![0x22u8; 64];
            for i in 0..8u64 {
                s.put(&format!("pre/{i}"), &seed, DurabilityMode::Buffered)
                    .unwrap();
            }
            s.seal_active().unwrap();
            s.wait_seals_applied().unwrap();
            let _ = s.prepare_flip_to_compact_shadow().unwrap();
            s.activate_compact_shadow_mode().unwrap();
            assert_eq!(s.recovery_mode(), RecoveryMode::CompactShadow);
            assert!(s.shadow_dual_stream());
            drop(s);
            let s = Store::open(root).unwrap();
            assert_eq!(s.recovery_mode(), RecoveryMode::CompactShadow);
            assert!(s.shadow_dual_stream());
            s
        }
    };

    store.set_enrichment_enabled(true);
    store.set_seal_threshold(256 * 1024);
    let seed = vec![0x11u8; 64];
    for i in 0..8u64 {
        store
            .put(&format!("seed/{i}"), &seed, DurabilityMode::Buffered)
            .unwrap();
    }
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
    drop(store);

    let store = Store::open(root).unwrap();
    assert_eq!(store.recovery_mode(), RecoveryMode::CompactShadow);
    assert!(store.shadow_dual_stream());
    store
}

fn accumulate(dst: &mut SealStageBreakdown, b: SealStageBreakdown) {
    dst.drain_lifecycle_ns = dst.drain_lifecycle_ns.saturating_add(b.drain_lifecycle_ns);
    dst.final_active_seal_ns = dst
        .final_active_seal_ns
        .saturating_add(b.final_active_seal_ns);
    dst.shadow_dual_ns = dst.shadow_dual_ns.saturating_add(b.shadow_dual_ns);
    dst.catalog_publication_ns = dst
        .catalog_publication_ns
        .saturating_add(b.catalog_publication_ns);
    dst.content_hash_ns = dst.content_hash_ns.saturating_add(b.content_hash_ns);
    dst.hydra_ns = dst.hydra_ns.saturating_add(b.hydra_ns);
    dst.chimera_ns = dst.chimera_ns.saturating_add(b.chimera_ns);
    dst.reopen_active_ns = dst.reopen_active_ns.saturating_add(b.reopen_active_ns);
}

fn run_arm(arm: Arm, target_bytes: u64) -> ArmReport {
    let root = work_root(target_bytes, arm);
    let mut store = setup_arm(&root, arm);
    store.set_enrichment_enabled(true);
    let seal_threshold = soft_seal_threshold(target_bytes);
    store.set_seal_threshold(seal_threshold);

    let recovery_mode = store.recovery_mode();
    let shadow_dual_stream = store.shadow_dual_stream();
    let enrichment_enabled = store.enrichment_enabled();

    let payload = vec![0x5Au8; PAYLOAD];
    let mut ops = 0u64;
    let mut written = 0u64;
    let mut since_seal = 0u64;
    let mut seal_breakdown = SealStageBreakdown::default();

    // Timed window starts after create / seed / activate / reopen.
    let ack_t0 = Instant::now();
    while written < target_bytes {
        let k = format!("step2l/{}/{ops:020}", arm.label());
        store.put(&k, &payload, DurabilityMode::Buffered).unwrap();
        ops += 1;
        written += PAYLOAD as u64;
        since_seal += PAYLOAD as u64;
        if since_seal >= SEAL_EVERY {
            accumulate(
                &mut seal_breakdown,
                store.seal_active_with_breakdown().unwrap(),
            );
            since_seal = 0;
        }
    }
    for i in 0..16u64 {
        let k = format!("continue/{}/{i:04}", arm.label());
        store.put(&k, &payload, DurabilityMode::Buffered).unwrap();
        ops += 1;
    }
    accumulate(
        &mut seal_breakdown,
        store.seal_active_with_breakdown().unwrap(),
    );
    store.wait_seals_applied().unwrap();
    let lifecycle_wall = ack_t0.elapsed();

    let dual_finalize_ns = store.shadow_dual_finalize_ns();
    let dual_published = store.shadow_dual_published();
    assert!(
        dual_published >= 1,
        "{} must publish ≥1 dual Shadow",
        arm.label()
    );

    // Drain outside the ack window — product figure is puts + seal/P★, not derived.
    store.drain_lifecycle().unwrap();
    store
        .drain_enrichment(std::time::Duration::from_secs(120))
        .unwrap();

    let lifecycle_ops_per_sec = ops as f64 / lifecycle_wall.as_secs_f64().max(1e-12);
    let ack_ops_per_sec = lifecycle_ops_per_sec;

    ArmReport {
        label: arm.label(),
        target_bytes,
        ack_ops_per_sec,
        lifecycle_ops_per_sec,
        dual_published,
        dual_finalize_ns,
        recovery_mode,
        shadow_dual_stream,
        enrichment_enabled,
        seal_threshold,
        seal_breakdown,
    }
}

fn print_report(r: &ArmReport) {
    let b = &r.seal_breakdown;
    eprintln!(
        "step2l {} target={}MiB ack={:.0} life={:.0} dual_pub={} dual_fin_s={:.3} mode={:?} dual={} enrich={} seal_thr={}",
        r.label,
        r.target_bytes / (1024 * 1024),
        r.ack_ops_per_sec,
        r.lifecycle_ops_per_sec,
        r.dual_published,
        r.dual_finalize_ns as f64 / 1e9,
        r.recovery_mode,
        r.shadow_dual_stream,
        r.enrichment_enabled,
        r.seal_threshold,
    );
    eprintln!(
        "step2l {} seal stages (s): drain={:.3} auth={:.3} shadow={:.3} catalog={:.3} hash={:.3} hydra_enq={:.3} chimera={:.3} reopen={:.3} total={:.3}",
        r.label,
        b.drain_lifecycle_ns as f64 / 1e9,
        b.final_active_seal_ns as f64 / 1e9,
        b.shadow_dual_ns as f64 / 1e9,
        b.catalog_publication_ns as f64 / 1e9,
        b.content_hash_ns as f64 / 1e9,
        b.hydra_ns as f64 / 1e9,
        b.chimera_ns as f64 / 1e9,
        b.reopen_active_ns as f64 / 1e9,
        b.total_ns() as f64 / 1e9,
    );
}

#[test]
fn step2l_tps_ab() {
    let target = parse_target();
    eprintln!(
        "step2l A/B start target={}MiB (set CSE3_STEP2L_TARGET_BYTES for Step9-parity 2GiB)",
        target / (1024 * 1024)
    );

    let a = run_arm(Arm::FreshDefault, target);
    print_report(&a);
    let b = run_arm(Arm::ActivatePath, target);
    print_report(&b);

    assert_eq!(a.recovery_mode, RecoveryMode::CompactShadow);
    assert_eq!(b.recovery_mode, RecoveryMode::CompactShadow);
    assert!(a.shadow_dual_stream && b.shadow_dual_stream);
    assert!(a.enrichment_enabled && b.enrichment_enabled);
    assert_eq!(a.seal_threshold, b.seal_threshold);

    let ratio = if a.lifecycle_ops_per_sec > 0.0 {
        b.lifecycle_ops_per_sec / a.lifecycle_ops_per_sec
    } else {
        0.0
    };
    eprintln!(
        "step2l SUMMARY life_A={:.0} life_B={:.0} B/A={:.3} (campaign SoT ~21–23K; not hardware floor; no opt during experiment)",
        a.lifecycle_ops_per_sec, b.lifecycle_ops_per_sec, ratio
    );
}
