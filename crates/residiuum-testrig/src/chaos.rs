//! Prong 2 — chaos monkey: offline random garbage writes into segment files.
//!
//! Intentionally **filesystem-level** damage (same family as demos/02_punch_a_hole.sh).
//! Must run while no writer holds the store lock.

use crate::size::format_bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ChaosConfig {
    pub store: PathBuf,
    pub hits: u32,
    pub bytes_per_hit: usize,
    pub seed: u64,
    /// Skip this many leading bytes of each file (protect segment headers a bit).
    pub protect_head: u64,
    /// Skip this many trailing bytes.
    pub protect_tail: u64,
    /// If true, allow punching anywhere including headers.
    pub brutal: bool,
    pub json_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosHit {
    pub path: String,
    pub offset: u64,
    pub bytes: usize,
    pub file_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosReport {
    pub prong: String,
    pub ok: bool,
    pub hits_requested: u32,
    pub hits_applied: u32,
    pub candidate_files: usize,
    pub candidate_bytes: u64,
    pub seed: u64,
    pub hits: Vec<ChaosHit>,
    pub note: String,
}

pub fn run_chaos(cfg: &ChaosConfig) -> Result<ChaosReport, String> {
    if cfg.hits == 0 {
        return Err("hits must be > 0".into());
    }
    if cfg.bytes_per_hit == 0 {
        return Err("bytes-per-hit must be > 0".into());
    }
    if !cfg.store.is_dir() {
        return Err(format!(
            "store path is not a directory: {}",
            cfg.store.display()
        ));
    }

    let candidates =
        collect_residiuum_files(&cfg.store).map_err(|e| format!("scan segments: {e}"))?;
    if candidates.is_empty() {
        return Err("no .residiuum segment files found under store (run pump first?)".into());
    }

    let candidate_bytes: u64 = candidates.iter().map(|c| c.size).sum();
    // Prefer files large enough for a protected punch; fall back to all if none qualify.
    let punchable: Vec<&Cand> = candidates
        .iter()
        .filter(|c| usable_span(c.size, cfg).0 >= cfg.bytes_per_hit as u64)
        .collect();
    let pool: Vec<&Cand> = if punchable.is_empty() {
        candidates.iter().collect()
    } else {
        punchable
    };

    let mut rng = Rng::new(cfg.seed);
    let mut hits = Vec::new();
    let mut skipped = 0u32;

    for _ in 0..cfg.hits {
        let file = pool[rng.gen_range(pool.len())];
        let (min_off, usable) = usable_span(file.size, cfg);
        if usable < cfg.bytes_per_hit as u64 {
            skipped += 1;
            continue;
        }
        let max_start = usable - cfg.bytes_per_hit as u64;
        let offset = min_off
            + if max_start == 0 {
                0
            } else {
                rng.gen_range_u64(max_start + 1)
            };

        let garbage = make_garbage(cfg.bytes_per_hit, rng.next_u64());
        punch_file(&file.path, offset, &garbage)
            .map_err(|e| format!("punch {}@{offset}: {e}", file.path.display()))?;

        hits.push(ChaosHit {
            path: file.path.display().to_string(),
            offset,
            bytes: cfg.bytes_per_hit,
            file_size: file.size,
        });
    }

    let ok = !hits.is_empty();
    let note = if hits.is_empty() {
        format!(
            "no hits applied (skipped={skipped}); files too small for bytes-per-hit / protect margins"
        )
    } else if skipped > 0 {
        format!(
            "applied {} hits; skipped {skipped} (file too small)",
            hits.len()
        )
    } else {
        format!("applied {} offline garbage punches", hits.len())
    };

    let report = ChaosReport {
        prong: "chaos".into(),
        ok,
        hits_requested: cfg.hits,
        hits_applied: hits.len() as u32,
        candidate_files: candidates.len(),
        candidate_bytes,
        seed: cfg.seed,
        hits: hits.clone(),
        note: note.clone(),
    };

    if cfg.json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "prong": "chaos",
                "ok": ok,
                "hits_requested": cfg.hits,
                "hits_applied": hits.len(),
                "candidate_files": candidates.len(),
                "candidate_bytes": candidate_bytes,
                "seed": cfg.seed,
                "hits": hits,
                "note": note,
            }))
            .unwrap()
        );
    } else {
        println!(
            "chaos: {}/{} hits on {} files ({}); seed={}",
            hits.len(),
            cfg.hits,
            candidates.len(),
            format_bytes(candidate_bytes),
            cfg.seed
        );
        for h in &hits {
            println!("  punch {} offset={} len={}", h.path, h.offset, h.bytes);
        }
        if !note.is_empty() {
            println!("note: {note}");
        }
    }

    if !ok {
        return Err(note);
    }
    Ok(report)
}

struct Cand {
    path: PathBuf,
    size: u64,
}

/// Returns (min_offset, usable_length) for a punch under the configured margins.
fn usable_span(file_size: u64, cfg: &ChaosConfig) -> (u64, u64) {
    if cfg.brutal {
        return (0, file_size);
    }
    // Scale protect margins down for small segment files so smoke stores still get hits.
    let head = cfg.protect_head.min(file_size / 8).min(file_size);
    let tail = cfg
        .protect_tail
        .min(file_size / 16)
        .min(file_size.saturating_sub(head));
    let min_off = head;
    let max_end = file_size.saturating_sub(tail);
    (min_off, max_end.saturating_sub(min_off))
}

fn collect_residiuum_files(store: &Path) -> std::io::Result<Vec<Cand>> {
    let mut out = Vec::new();
    // Prefer authoritative segment media: active/ + segments/ (+ tier media if present).
    let roots = [
        store.join("active"),
        store.join("segments"),
        store.join("tiers"),
        store.join("chunks"),
    ];
    for root in roots {
        if root.is_dir() {
            walk_residiuum(&root, &mut out)?;
        }
    }
    // Fallback: any .residiuum under store root.
    if out.is_empty() {
        walk_residiuum(store, &mut out)?;
    }
    Ok(out)
}

fn walk_residiuum(dir: &Path, out: &mut Vec<Cand>) -> std::io::Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for ent in fs::read_dir(&d)? {
            let ent = ent?;
            let path = ent.path();
            let ft = ent.file_type()?;
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                if path.extension().and_then(|e| e.to_str()) == Some("residiuum") {
                    let size = ent.metadata()?.len();
                    if size > 0 {
                        out.push(Cand { path, size });
                    }
                }
            }
        }
    }
    Ok(())
}

fn punch_file(path: &Path, offset: u64, garbage: &[u8]) -> std::io::Result<()> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(garbage)?;
    f.sync_all()?;
    Ok(())
}

fn make_garbage(len: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let header = b"CHAOS-MONKEY-GARBAGE\n";
    let n = header.len().min(len);
    out[..n].copy_from_slice(&header[..n]);
    let mut state = seed ^ 0xC4A05_u64;
    for b in out.iter_mut().skip(n) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(7);
        *b = (state >> 32) as u8;
    }
    out
}

/// Tiny deterministic RNG (xorshift64*).
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0xDEAD_BEEF_CAFE_BABE
            } else {
                seed
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn gen_range(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() as usize) % n
    }

    fn gen_range_u64(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }
}
