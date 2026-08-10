//! Independent byte reader CLI for CSQ-1.
//!
//! Does **not** import `residiuum-store` or call production format scanners.
//! Implements a minimal RESIDFRM forward scan from FORMAT_SPEC prefix layout.

use blake3::Hasher;
use clap::Parser;
use serde::Serialize;
use std::path::PathBuf;

const START_MAGIC: &[u8; 8] = b"RESIDFRM";
const END_MAGIC: &[u8; 8] = b"RESIDEND";
const FRAME_PREFIX_LEN: usize = 64;
const FRAME_SUFFIX_LEN: usize = 56;

#[derive(Parser, Debug)]
#[command(name = "core-storage-reference-reader")]
#[command(about = "CSQ-1 independent Residiuum frame scanner (oracle firewall)")]
struct Args {
    /// File or directory to scan as raw bytes.
    path: PathBuf,
    /// Emit JSON observations.
    #[arg(long, default_value_t = true)]
    json: bool,
}

#[derive(Debug, Serialize)]
struct FrameHit {
    offset: u64,
    frame_len: u64,
    wire_major: u8,
    wire_minor: u8,
    frame_kind: u8,
    envelope_len: u32,
    body_len: u64,
    event_id_hex: String,
    body_hash_hex: String,
    end_magic_ok: bool,
}

#[derive(Debug, Serialize)]
struct ScanReport {
    path: String,
    bytes: u64,
    frames: Vec<FrameHit>,
    /// Known-bad scanners that collapse holes incorrectly are not used here.
    oracle: &'static str,
}

fn hex_encode(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(H[(b >> 4) as usize] as char);
        out.push(H[(b & 0xf) as usize] as char);
    }
    out
}

/// Independent forward scan: find RESIDFRM, parse lengths, verify end magic region if present.
fn scan_bytes(data: &[u8]) -> Vec<FrameHit> {
    let mut hits = Vec::new();
    let mut i = 0usize;
    while i + FRAME_PREFIX_LEN <= data.len() {
        if &data[i..i + 8] != START_MAGIC.as_slice() {
            i += 1;
            continue;
        }
        let prefix = &data[i..i + FRAME_PREFIX_LEN];
        let wire_major = prefix[8];
        let wire_minor = prefix[9];
        let frame_kind = prefix[10];
        let envelope_len = u32::from_le_bytes(prefix[12..16].try_into().unwrap());
        let body_len = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
        let event_id: [u8; 16] = prefix[40..56].try_into().unwrap();

        // Bound absurd lengths to avoid OOM on garbage.
        if envelope_len as u64 > 16 * 1024 * 1024 || body_len > 64 * 1024 * 1024 {
            i += 1;
            continue;
        }
        let frame_len =
            FRAME_PREFIX_LEN as u64 + envelope_len as u64 + body_len + FRAME_SUFFIX_LEN as u64;
        if i as u64 + frame_len > data.len() as u64 {
            // Truncated candidate — still report header evidence, mark end magic false.
            hits.push(FrameHit {
                offset: i as u64,
                frame_len,
                wire_major,
                wire_minor,
                frame_kind,
                envelope_len,
                body_len,
                event_id_hex: hex_encode(&event_id),
                body_hash_hex: String::new(),
                end_magic_ok: false,
            });
            i += 1;
            continue;
        }
        let body_start = i + FRAME_PREFIX_LEN + envelope_len as usize;
        let body_end = body_start + body_len as usize;
        let body = &data[body_start..body_end];
        let mut hasher = Hasher::new();
        hasher.update(body);
        let hash = hasher.finalize();
        let suffix_start = body_end;
        let end_magic_ok = data.get(suffix_start..suffix_start + 8) == Some(END_MAGIC.as_slice());

        hits.push(FrameHit {
            offset: i as u64,
            frame_len,
            wire_major,
            wire_minor,
            frame_kind,
            envelope_len,
            body_len,
            event_id_hex: hex_encode(&event_id),
            body_hash_hex: hex_encode(hash.as_bytes()),
            end_magic_ok,
        });
        // Advance past this frame if complete, else by 1 to find later islands (CSQ-FMT-002).
        if end_magic_ok {
            i = (i as u64 + frame_len) as usize;
        } else {
            i += 1;
        }
    }
    hits
}

fn main() {
    let args = Args::parse();
    let data = std::fs::read(&args.path).unwrap_or_else(|e| {
        eprintln!("read {}: {e}", args.path.display());
        std::process::exit(2);
    });
    let frames = scan_bytes(&data);
    let report = ScanReport {
        path: args.path.display().to_string(),
        bytes: data.len() as u64,
        frames,
        oracle: "CSQ-ORACLE-READER",
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_hand_built_minimal_header() {
        // Hand-built 64-byte prefix only (not a full valid production frame).
        let mut buf = vec![0u8; 64 + 8];
        buf[0..8].copy_from_slice(START_MAGIC);
        buf[8] = 1;
        buf[9] = 0;
        buf[10] = 1; // kind
        buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // env
        buf[16..24].copy_from_slice(&0u64.to_le_bytes()); // body
                                                          // event id
        buf[40] = 0xab;
        // fake end magic at suffix start for env=0 body=0
        let suffix = 64;
        buf.resize(suffix + FRAME_SUFFIX_LEN, 0);
        buf[suffix..suffix + 8].copy_from_slice(END_MAGIC);
        let hits = scan_bytes(&buf);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].event_id_hex.starts_with("ab"), true);
        assert!(hits[0].end_magic_ok);
    }

    #[test]
    fn later_island_after_garbage() {
        let mut buf = vec![0xFFu8; 20];
        let mut frame = vec![0u8; 64 + FRAME_SUFFIX_LEN];
        frame[0..8].copy_from_slice(START_MAGIC);
        frame[8] = 1;
        frame[12..16].copy_from_slice(&0u32.to_le_bytes());
        frame[16..24].copy_from_slice(&0u64.to_le_bytes());
        frame[40] = 0x11;
        frame[64..72].copy_from_slice(END_MAGIC);
        buf.extend_from_slice(&frame);
        let hits = scan_bytes(&buf);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].offset, 20);
    }
}
