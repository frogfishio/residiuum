//! Diagnostic phase breakdown: pure compute vs raw write vs store puts.
//!
//! Purpose: falsify "we're Blake/CPU bound" when Activity Monitor shows
//! ~40% of one core. If pure Blake is huge ops/s and store put is slow with
//! low process CPU, wall time is spent **waiting** (typically blocking I/O),
//! not computing.

use residiuum_format::{
    body_hash, encode_frame_into, FrameHeader, FrameKind, WIRE_MAJOR, WIRE_MINOR,
};
use residiuum_store::{DiagnosticIoSink, DurabilityMode, Store};
use serde_json::json;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PhaseBenchConfig {
    pub work: PathBuf,
    pub ops: u64,
    pub payload_size: usize,
    pub batch: usize,
    pub json_out: bool,
}

#[derive(Debug, Clone)]
struct Phase {
    name: &'static str,
    wall_ms: f64,
    ops_per_sec: f64,
    mib_per_sec: f64,
    note: String,
}

fn fill_payload(buf: &mut [u8], seed: u64) {
    let mut state = seed ^ 0xD1_160_B17_u64;
    for (i, b) in buf.iter_mut().enumerate() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = ((state >> 33) as u8).wrapping_add((i & 0xff) as u8);
    }
}

fn mib_s(bytes: u64, secs: f64) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / secs.max(1e-12)
}

fn phase(
    name: &'static str,
    ops: u64,
    bytes_per_op: u64,
    wall_s: f64,
    note: impl Into<String>,
) -> Phase {
    Phase {
        name,
        wall_ms: wall_s * 1000.0,
        ops_per_sec: ops as f64 / wall_s.max(1e-12),
        mib_per_sec: mib_s(ops.saturating_mul(bytes_per_op), wall_s),
        note: note.into(),
    }
}

/// Best-effort process CPU samples via `ps` (macOS: 100% ≈ one core).
fn sample_cpu_pct() -> Option<f64> {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "%cpu=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse().ok()
}

pub fn run_phase_bench(cfg: &PhaseBenchConfig) -> Result<(), String> {
    if cfg.ops == 0 {
        return Err("ops must be > 0".into());
    }
    if cfg.payload_size == 0 {
        return Err("payload-size must be > 0".into());
    }
    let batch = cfg.batch.max(1);
    fs::create_dir_all(&cfg.work).map_err(|e| format!("create work: {e}"))?;

    let mut payload = vec![0u8; cfg.payload_size];
    fill_payload(&mut payload, 42);
    let payload_bytes = cfg.payload_size as u64;
    let ops = cfg.ops;

    let mut phases: Vec<Phase> = Vec::new();

    // --- 1. Pure BLAKE3 (format body_hash) — should saturate ~1 core if "Blake bound" ---
    {
        let t0 = Instant::now();
        let mut sink = [0u8; 32];
        for _ in 0..ops {
            sink = body_hash(&payload);
        }
        // prevent optimize-away
        if sink[0] == 0xFF && sink[1] == 0x00 {
            eprintln!("unlikely");
        }
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "blake3_body_hash_only",
            ops,
            payload_bytes,
            s,
            "pure user CPU; if store is Blake-bound, put rates should be near this order",
        ));
    }

    // --- 1b. Full frame encode into growing Vec (Blake + prefix/suffix + memcpy) ---
    {
        let env = b"\xa0"; // empty CBOR map
        let mut event_id = [0u8; 16];
        let mut buf = Vec::with_capacity(ops as usize * (cfg.payload_size + 256));
        let t0 = Instant::now();
        for i in 0..ops {
            event_id[0] = (i & 0xff) as u8;
            event_id[1] = ((i >> 8) & 0xff) as u8;
            let header = FrameHeader {
                wire_major: WIRE_MAJOR,
                wire_minor: WIRE_MINOR,
                frame_kind: FrameKind::ItemEvent.as_u8(),
                flags: Default::default(),
                envelope_len: env.len() as u32,
                body_len: payload.len() as u64,
                logical_len: payload.len() as u64,
                writer_sequence: i,
                event_id,
            };
            encode_frame_into(&mut buf, &header, env, &payload)
                .map_err(|e| format!("encode_frame_into: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        let _ = buf.len();
        phases.push(phase(
            "encode_frame_into_growing_vec",
            ops,
            payload_bytes,
            s,
            "format encode only (includes Blake); no store index, no OS write",
        ));
    }

    // --- 1c. Seek+write_all per op (store tail pattern) vs append-only write_all ---
    {
        let frame_pad = 256usize;
        let mut frame = vec![0u8; cfg.payload_size + frame_pad];
        frame[..cfg.payload_size].copy_from_slice(&payload);
        let path = cfg.work.join("raw-seek-write.bin");
        let _ = fs::remove_file(&path);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| format!("open seek-write: {e}"))?;
        let mut off = 0u64;
        let t0 = Instant::now();
        for _ in 0..ops {
            f.seek(SeekFrom::Start(off))
                .map_err(|e| format!("seek: {e}"))?;
            f.write_all(&frame)
                .map_err(|e| format!("seek write: {e}"))?;
            off += frame.len() as u64;
        }
        f.flush().map_err(|e| format!("seek flush: {e}"))?;
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "raw_seek_plus_write_all_per_op",
            ops,
            frame.len() as u64,
            s,
            "mimics write_segment_tail: seek(durable_len)+write_all each put",
        ));
        drop(f);
        let _ = fs::remove_file(&path);
    }

    // --- 2a. Raw write_all → /dev/null (syscall/VFS, no media) ---
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .map_err(|e| format!("open /dev/null: {e}"))?;
        let t0 = Instant::now();
        for _ in 0..ops {
            f.write_all(&payload)
                .map_err(|e| format!("null write: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "raw_write_all_dev_null",
            ops,
            payload_bytes,
            s,
            "write_all to /dev/null — OS path without durable media (disk detached)",
        ));
    }

    // --- 2b. Raw sequential write of payload-sized chunks (real path under work) ---
    {
        let path = cfg.work.join("raw-seq.bin");
        let _ = fs::remove_file(&path);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| format!("open raw-seq: {e}"))?;
        let t0 = Instant::now();
        for _ in 0..ops {
            f.write_all(&payload)
                .map_err(|e| format!("raw write: {e}"))?;
        }
        f.flush().map_err(|e| format!("raw flush: {e}"))?;
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "raw_write_all_payload_only",
            ops,
            payload_bytes,
            s,
            "OS write path only (no fsync); device + page cache ceiling for small writes",
        ));
        drop(f);
        let _ = fs::remove_file(&path);
    }

    // --- 3. Raw write of ~frame-sized chunks (payload + ~256 B overhead stand-in) ---
    {
        let frame_pad = 256usize;
        let mut frame = vec![0u8; cfg.payload_size + frame_pad];
        frame[..cfg.payload_size].copy_from_slice(&payload);
        let path = cfg.work.join("raw-frame.bin");
        let _ = fs::remove_file(&path);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| format!("open raw-frame: {e}"))?;
        let t0 = Instant::now();
        for _ in 0..ops {
            f.write_all(&frame)
                .map_err(|e| format!("raw frame write: {e}"))?;
        }
        f.flush().map_err(|e| format!("raw frame flush: {e}"))?;
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "raw_write_all_frame_sized",
            ops,
            (cfg.payload_size + frame_pad) as u64,
            s,
            "closer to segment tail write size (~payload+envelope)",
        ));
        drop(f);
        let _ = fs::remove_file(&path);
    }

    // --- 4. Store Memory puts (visibility only — no segment file append) ---
    {
        let store_path = cfg.work.join("mem-store");
        let _ = fs::remove_dir_all(&store_path);
        let mut store = Store::create(&store_path).map_err(|e| format!("create mem store: {e}"))?;
        let t0 = Instant::now();
        for i in 0..ops {
            let key = format!("m/{i:020}");
            store
                .put(&key, &payload, DurabilityMode::Memory)
                .map_err(|e| format!("memory put: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        phases.push(phase(
            "store_put_memory",
            ops,
            payload_bytes,
            s,
            "index/visibility path only — NO durable segment write_all",
        ));
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
    }

    // --- 5. Disk bisection: full Buffered put with I/O sink variants ---
    //
    // Ladder (same encode/index/append path; only tail write destination changes):
    //   Memory          — no segment tail write at all
    //   Buffered+Discard — full path, durable_len advances, **no write_all**
    //   Buffered+DevNull — full path, write_all → /dev/null (no media)
    //   Buffered+Real    — full path, real active segment file under work/
    //
    // If Discard ≈ Real ⇒ wall is **store-side** (not disk/media).
    // If Discard ≫ Real ⇒ wall is on the **write/OS/media** side.
    // If DevNull ≈ Real and both ≪ raw_null ⇒ cost is store, not media.
    fn run_buffered_sink(
        phases: &mut Vec<Phase>,
        cfg: &PhaseBenchConfig,
        payload: &[u8],
        ops: u64,
        payload_bytes: u64,
        sink: DiagnosticIoSink,
        phase_name: &'static str,
        note_prefix: &str,
    ) -> Result<(), String> {
        let store_path = cfg.work.join(format!("buf-{}", phase_name));
        let _ = fs::remove_dir_all(&store_path);
        let mut store =
            Store::create(&store_path).map_err(|e| format!("create {phase_name}: {e}"))?;
        store.set_seal_threshold(512 * 1024 * 1024);
        store
            .set_diagnostic_io_sink(sink)
            .map_err(|e| format!("set_diagnostic_io_sink: {e}"))?;
        store.enable_boundary_probe();
        let cpu0 = sample_cpu_pct();
        let t0 = Instant::now();
        for i in 0..ops {
            let key = format!("b/{i:020}");
            store
                .put(&key, payload, DurabilityMode::Buffered)
                .map_err(|e| format!("{phase_name} put: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        let cpu1 = sample_cpu_pct();
        let snap = store.boundary_snapshot();
        let wall_ms = s * 1000.0;
        let prep_ms = snap.prep_latency.sum_ns as f64 / 1e6;
        let enc_ms = snap.encode_latency.sum_ns as f64 / 1e6;
        let app_ms = snap.append_latency.sum_ns as f64 / 1e6;
        let pub_ms = snap.publish_latency.sum_ns as f64 / 1e6;
        let post_ms = snap.post_latency.sum_ns as f64 / 1e6;
        let wr_ms = snap.write_latency.sum_ns as f64 / 1e6;
        let sync_ms = snap.sync_latency.sum_ns as f64 / 1e6;
        let seal_ms = snap.seal_latency.sum_ns as f64 / 1e6;
        let accounted_ms = prep_ms + enc_ms + app_ms + pub_ms + post_ms + wr_ms + sync_ms + seal_ms;
        let other_ms = (wall_ms - accounted_ms).max(0.0);
        let pct = |part: f64| 100.0 * part / wall_ms.max(1e-9);
        let probe_note = format!(
            " MODE_A: prep={prep_ms:.1}ms({:.0}%) enc={enc_ms:.1}({:.0}%) append={app_ms:.1}({:.0}%) pub={pub_ms:.1}({:.0}%) write={wr_ms:.1}({:.0}%) seal_n={} other={other_ms:.1}({:.0}%)",
            pct(prep_ms),
            pct(enc_ms),
            pct(app_ms),
            pct(pub_ms),
            pct(wr_ms),
            snap.seal_latency.samples,
            pct(other_ms),
        );
        phases.push(phase(
            phase_name,
            ops,
            payload_bytes,
            s,
            format!(
                "{note_prefix}; wall_ms={wall_ms:.1} accounted={accounted_ms:.1}; ps%cpu≈{:?}→{:?};{probe_note}",
                cpu0, cpu1
            ),
        ));
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
        Ok(())
    }

    run_buffered_sink(
        &mut phases,
        cfg,
        &payload,
        ops,
        payload_bytes,
        DiagnosticIoSink::Discard,
        "store_put_buffered_discard_io",
        "DISK DETACHED: full Buffered path, no write_all (sink=Discard)",
    )?;
    run_buffered_sink(
        &mut phases,
        cfg,
        &payload,
        ops,
        payload_bytes,
        DiagnosticIoSink::DevNull,
        "store_put_buffered_dev_null",
        "DISK DETACHED: full Buffered path, write_all→/dev/null (syscall only)",
    )?;
    run_buffered_sink(
        &mut phases,
        cfg,
        &payload,
        ops,
        payload_bytes,
        DiagnosticIoSink::Real,
        "store_put_buffered_batch1",
        "REAL FILE: full Buffered path on work volume (page cache + media)",
    )?;

    // --- 5b. Index vs data-cooking bisection (REAL disk) ---
    // Full path vs skip dual-index publish (encode+append+write only).
    {
        let store_path = cfg.work.join("buf-no-index");
        let _ = fs::remove_dir_all(&store_path);
        let mut store =
            Store::create(&store_path).map_err(|e| format!("create no-index store: {e}"))?;
        store.set_seal_threshold(512 * 1024 * 1024);
        store.set_diagnostic_skip_index(true);
        store.enable_boundary_probe();
        let cpu0 = sample_cpu_pct();
        let t0 = Instant::now();
        for i in 0..ops {
            let key = format!("x/{i:020}");
            store
                .put(&key, &payload, DurabilityMode::Buffered)
                .map_err(|e| format!("no-index put: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        let cpu1 = sample_cpu_pct();
        let snap = store.boundary_snapshot();
        let wall_ms = s * 1000.0;
        let app_ms = snap.append_latency.sum_ns as f64 / 1e6;
        let wr_ms = snap.write_latency.sum_ns as f64 / 1e6;
        let pub_ms = snap.publish_latency.sum_ns as f64 / 1e6;
        let enc_ms = snap.encode_latency.sum_ns as f64 / 1e6;
        phases.push(phase(
            "store_put_buffered_no_index",
            ops,
            payload_bytes,
            s,
            format!(
                "REAL FILE + skip dual-index publish (data cooking only); wall_ms={wall_ms:.1} enc={enc_ms:.1} append={app_ms:.1} write={wr_ms:.1} pub={pub_ms:.1}; ps%cpu≈{:?}→{:?}",
                cpu0, cpu1
            ),
        ));
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
    }

    // --- 5c. Short-circuit BLAKE3 only (still copy body + write_all) ---
    {
        let store_path = cfg.work.join("buf-no-blake");
        let _ = fs::remove_dir_all(&store_path);
        let mut store =
            Store::create(&store_path).map_err(|e| format!("create no-blake store: {e}"))?;
        store.set_seal_threshold(512 * 1024 * 1024);
        store.set_diagnostic_skip_blake(true);
        store.enable_boundary_probe();
        let cpu0 = sample_cpu_pct();
        let t0 = Instant::now();
        for i in 0..ops {
            let key = format!("h/{i:020}");
            store
                .put(&key, &payload, DurabilityMode::Buffered)
                .map_err(|e| format!("no-blake put: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        let cpu1 = sample_cpu_pct();
        let snap = store.boundary_snapshot();
        let wall_ms = s * 1000.0;
        let app_ms = snap.append_latency.sum_ns as f64 / 1e6;
        let wr_ms = snap.write_latency.sum_ns as f64 / 1e6;
        let enc_ms = snap.encode_latency.sum_ns as f64 / 1e6;
        phases.push(phase(
            "store_put_buffered_no_blake",
            ops,
            payload_bytes,
            s,
            format!(
                "SHORT-CIRCUIT Blake only (still memcpy body + real write); wall_ms={wall_ms:.1} enc={enc_ms:.1} append={app_ms:.1} write={wr_ms:.1}; ps%cpu≈{:?}→{:?}",
                cpu0, cpu1
            ),
        ));
        // Leave flag off for subsequent stores on this thread.
        store.set_diagnostic_skip_blake(false);
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
    }

    // --- 5d. Short-circuit append_frame (Blake+buffer cook) ---
    // Proves whether append_frame is the data-cooking hog.
    for (label, skip_idx, phase_name, note) in [
        (
            "no-append",
            false,
            "store_put_buffered_no_append",
            "SHORT-CIRCUIT append_frame only (prep+env+index+no Blake/segment/write)",
        ),
        (
            "no-append-no-index",
            true,
            "store_put_buffered_no_append_no_index",
            "SHORT-CIRCUIT append+index (prep+env encode only)",
        ),
    ] {
        let store_path = cfg.work.join(format!("buf-{label}"));
        let _ = fs::remove_dir_all(&store_path);
        let mut store =
            Store::create(&store_path).map_err(|e| format!("create {label} store: {e}"))?;
        store.set_seal_threshold(512 * 1024 * 1024);
        store.set_diagnostic_skip_append_frame(true);
        store.set_diagnostic_skip_index(skip_idx);
        store.enable_boundary_probe();
        let cpu0 = sample_cpu_pct();
        let t0 = Instant::now();
        for i in 0..ops {
            let key = format!("a/{i:020}");
            store
                .put(&key, &payload, DurabilityMode::Buffered)
                .map_err(|e| format!("{label} put: {e}"))?;
        }
        let s = t0.elapsed().as_secs_f64();
        let cpu1 = sample_cpu_pct();
        let snap = store.boundary_snapshot();
        let wall_ms = s * 1000.0;
        let app_ms = snap.append_latency.sum_ns as f64 / 1e6;
        let enc_ms = snap.encode_latency.sum_ns as f64 / 1e6;
        let pub_ms = snap.publish_latency.sum_ns as f64 / 1e6;
        let prep_ms = snap.prep_latency.sum_ns as f64 / 1e6;
        phases.push(phase(
            phase_name,
            ops,
            payload_bytes,
            s,
            format!(
                "{note}; wall_ms={wall_ms:.1} prep={prep_ms:.1} enc={enc_ms:.1} append={app_ms:.1} pub={pub_ms:.1}; ps%cpu≈{:?}→{:?}",
                cpu0, cpu1
            ),
        ));
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
    }

    // --- 6. Store Buffered put_many: serial cook vs parallel cooker (Option C) ---
    for (workers, phase_name) in [
        (1usize, "store_put_many_cook1"),
        (2usize, "store_put_many_cook2"),
        (4usize, "store_put_many_cook4"),
    ] {
        let store_path = cfg.work.join(format!("bufn-cook{workers}"));
        let _ = fs::remove_dir_all(&store_path);
        let mut store =
            Store::create(&store_path).map_err(|e| format!("create cook{workers}: {e}"))?;
        // Seal above this phase's logical payload so parallel install is not
        // interrupted by rotate mid-batch (still real write_all on work volume).
        let seal_need = ops
            .saturating_mul(payload_bytes)
            .saturating_add(64 * 1024 * 1024)
            .max(512 * 1024 * 1024);
        store.set_seal_threshold(seal_need);
        store.set_cook_parallelism(workers);
        let cpu0 = sample_cpu_pct();
        let t0 = Instant::now();
        let mut i = 0u64;
        while i < ops {
            let end = (i + batch as u64).min(ops);
            let keys: Vec<String> = (i..end).map(|k| format!("c{workers}/{k:020}")).collect();
            let items: Vec<(&str, &[u8])> = keys
                .iter()
                .map(|k| (k.as_str(), payload.as_slice()))
                .collect();
            store
                .put_many(&items, DurabilityMode::Buffered)
                .map_err(|e| format!("put_many cook{workers}: {e}"))?;
            i = end;
        }
        let s = t0.elapsed().as_secs_f64();
        let cpu1 = sample_cpu_pct();
        phases.push(phase(
            phase_name,
            ops,
            payload_bytes,
            s,
            format!(
                "PARALLEL COOKER workers={workers} batch={batch}; real disk; ps%cpu≈{:?}→{:?}",
                cpu0, cpu1
            ),
        ));
        drop(store);
        let _ = fs::remove_dir_all(&store_path);
    }

    // Keep legacy name as alias of cook1 for older dashboards.
    if let Some(p) = phases
        .iter()
        .find(|p| p.name == "store_put_many_cook1")
        .cloned()
    {
        phases.push(Phase {
            name: "store_put_many_buffered",
            wall_ms: p.wall_ms,
            ops_per_sec: p.ops_per_sec,
            mib_per_sec: p.mib_per_sec,
            note: format!("alias of cook1; {}", p.note),
        });
    }

    // Interpretation helper rates — disk bisection first
    let blake = phases.iter().find(|p| p.name == "blake3_body_hash_only");
    let raw = phases
        .iter()
        .find(|p| p.name == "raw_write_all_payload_only");
    let raw_null = phases.iter().find(|p| p.name == "raw_write_all_dev_null");
    let mem = phases.iter().find(|p| p.name == "store_put_memory");
    let disc = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_discard_io");
    let dnull = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_dev_null");
    let buf1 = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_batch1");
    let noidx = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_no_index");
    let noapp = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_no_append");
    let noapp_ni = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_no_append_no_index");
    let noblake = phases
        .iter()
        .find(|p| p.name == "store_put_buffered_no_blake");

    let mut interpretation = Vec::new();
    if let (Some(full), Some(nb)) = (buf1, noblake) {
        let ratio = nb.ops_per_sec / full.ops_per_sec.max(1.0);
        if ratio >= 1.5 {
            interpretation.push(format!(
                "BLAKE SHORT-CIRCUIT: no-blake {:.0} vs full {:.0} ops/s ({:.1}×) — **Blake is a major cook cost** (still memcpy+write).",
                nb.ops_per_sec, full.ops_per_sec, ratio
            ));
        } else if ratio >= 1.15 {
            interpretation.push(format!(
                "BLAKE SHORT-CIRCUIT: no-blake {:.0} vs full {:.0} ops/s ({:.1}×) — Blake is a meaningful but partial cook cost.",
                nb.ops_per_sec, full.ops_per_sec, ratio
            ));
        } else {
            interpretation.push(format!(
                "BLAKE SHORT-CIRCUIT: no-blake {:.0} ≈ full {:.0} ops/s ({:.1}×) — Blake is NOT the main remaining cook cost; look at memcpy/write.",
                nb.ops_per_sec, full.ops_per_sec, ratio
            ));
        }
    }
    if let (Some(full), Some(na)) = (buf1, noapp) {
        let ratio = na.ops_per_sec / full.ops_per_sec.max(1.0);
        interpretation.push(format!(
            "APPEND SHORT-CIRCUIT: no-append {:.0} vs full {:.0} ops/s ({:.1}×) — if ratio ≫1, append_frame (Blake+buffer) is the data-cooking hog.",
            na.ops_per_sec, full.ops_per_sec, ratio
        ));
    }
    if let (Some(nb), Some(na)) = (noblake, noapp) {
        interpretation.push(format!(
            "Blake vs full append: no-blake {:.0} / no-append {:.0} — gap is memcpy+frame framing+write after Blake is removed.",
            nb.ops_per_sec, na.ops_per_sec
        ));
    }
    if let (Some(na), Some(nani)) = (noapp, noapp_ni) {
        interpretation.push(format!(
            "APPEND SC + no-index: {:.0} ops/s (vs no-append-with-index {:.0}) — residual after skipping Blake/append.",
            nani.ops_per_sec, na.ops_per_sec
        ));
    }
    if let (Some(full), Some(ni)) = (buf1, noidx) {
        let ratio = ni.ops_per_sec / full.ops_per_sec.max(1.0);
        if ratio < 1.15 {
            interpretation.push(format!(
                "INDEX BISECT: no-index {:.0} ≈ full {:.0} ops/s (ratio {:.2}×) — dual-index publish is NOT the killer; **data cooking** (encode/append/Blake+copy + write) dominates.",
                ni.ops_per_sec, full.ops_per_sec, ratio
            ));
        } else {
            interpretation.push(format!(
                "INDEX BISECT: no-index {:.0} vs full {:.0} ops/s (ratio {:.2}×) — indexing takes a large share of wall; dual-index publish is a primary drag.",
                ni.ops_per_sec, full.ops_per_sec, ratio
            ));
        }
    }
    if let (Some(d), Some(r), Some(n)) = (disc, buf1, dnull) {
        let ratio_real_vs_disc = r.ops_per_sec / d.ops_per_sec.max(1.0);
        let ratio_null_vs_disc = n.ops_per_sec / d.ops_per_sec.max(1.0);
        if ratio_real_vs_disc > 0.85 && ratio_null_vs_disc > 0.85 {
            interpretation.push(format!(
                "DISK BISECT: Discard {:.0} ≈ DevNull {:.0} ≈ Real {:.0} ops/s — wall is on the **store CPU path** (encode/append/index), not media. Small-write disk hate is NOT the Mode A micro limiter.",
                d.ops_per_sec, n.ops_per_sec, r.ops_per_sec
            ));
        } else if ratio_real_vs_disc < 0.7 {
            interpretation.push(format!(
                "DISK BISECT: Discard {:.0} vs Real {:.0} ops/s (Real is {:.0}% of Discard) — real-file write_all/page-cache/media adds substantial wall; OS/media side matters.",
                d.ops_per_sec,
                r.ops_per_sec,
                100.0 * ratio_real_vs_disc
            ));
        } else {
            interpretation.push(format!(
                "DISK BISECT: Discard {:.0} / DevNull {:.0} / Real {:.0} ops/s — partial OS cost (Real/Discard={:.2}×).",
                d.ops_per_sec,
                n.ops_per_sec,
                r.ops_per_sec,
                ratio_real_vs_disc
            ));
        }
    }
    if let (Some(rn), Some(r)) = (raw_null, raw) {
        interpretation.push(format!(
            "Raw /dev/null {:.0} vs raw work-file {:.0} ops/s — pure write_all media delta without Residiuum.",
            rn.ops_per_sec, r.ops_per_sec
        ));
    }
    if let (Some(b), Some(p)) = (blake, buf1) {
        let ratio = p.ops_per_sec / b.ops_per_sec.max(1.0);
        if ratio < 0.15 {
            interpretation.push(format!(
                "Buffered put is only {:.1}% of pure-Blake ops/s — NOT Blake-bound alone (Blake is far faster).",
                100.0 * ratio
            ));
        } else {
            interpretation.push(format!(
                "Buffered put is {:.0}% of pure-Blake ops/s — Blake-scale cost may matter.",
                100.0 * ratio
            ));
        }
    }
    if let (Some(m), Some(d), Some(p)) = (mem, disc, buf1) {
        interpretation.push(format!(
            "Memory {:.0} → Discard-Buffered {:.0} → Real-Buffered {:.0}: gap Memory→Discard is **pre-disk store work**; Discard→Real is **tail I/O**.",
            m.ops_per_sec, d.ops_per_sec, p.ops_per_sec
        ));
    }
    if let (Some(r), Some(p)) = (raw, buf1) {
        interpretation.push(format!(
            "Raw write_all {:.0} ops/s vs Buffered put {:.0} — store adds {:.1}× wall vs raw same-size writes.",
            r.ops_per_sec,
            p.ops_per_sec,
            r.ops_per_sec / p.ops_per_sec.max(1.0)
        ));
    }
    interpretation.push(
        "Note: long peer multi-seal runs can still add seal_rotate wall; this micro uses 512MiB seal so mid seals are off.".into(),
    );
    let c1 = phases.iter().find(|p| p.name == "store_put_many_cook1");
    let c4 = phases.iter().find(|p| p.name == "store_put_many_cook4");
    if let (Some(a), Some(b)) = (c1, c4) {
        interpretation.push(format!(
            "COOKER POOL: cook1 {:.0} vs cook4 {:.0} ops/s ({:.2}×) — parallel full-record cook (env+Blake+encode) then ordered install.",
            a.ops_per_sec,
            b.ops_per_sec,
            b.ops_per_sec / a.ops_per_sec.max(1.0)
        ));
    }

    let report = json!({
        "prong": "phase_bench",
        "ok": true,
        "work": cfg.work.display().to_string(),
        "ops": ops,
        "payload_size": cfg.payload_size,
        "batch": batch,
        "phases": phases.iter().map(|p| json!({
            "name": p.name,
            "wall_ms": p.wall_ms,
            "ops_per_sec": p.ops_per_sec,
            "logical_mib_per_sec": p.mib_per_sec,
            "note": p.note,
        })).collect::<Vec<_>>(),
        "interpretation": interpretation,
        "disclosure": "Diagnostic only — not a published SLO.",
    });

    if cfg.json_out {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        println!("=== phase-bench (falsify Blake-bound narrative) ===");
        println!(
            "work={} ops={} payload={} batch={}",
            cfg.work.display(),
            ops,
            cfg.payload_size,
            batch
        );
        println!();
        println!(
            "{:<32} {:>10} {:>12} {:>12}  {}",
            "phase", "wall_ms", "ops/s", "MiB/s", "note"
        );
        for p in &phases {
            println!(
                "{:<32} {:>10.1} {:>12.0} {:>12.1}  {}",
                p.name, p.wall_ms, p.ops_per_sec, p.mib_per_sec, p.note
            );
        }
        println!();
        println!("interpretation:");
        for line in &interpretation {
            println!("  • {line}");
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn _touch(path: &Path) -> std::io::Result<File> {
    File::create(path)
}
