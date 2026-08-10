//! On-disk size helpers and human size parsing (`1G`, `256M`, raw bytes).

use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;

/// Recursively sum file lengths under `root` (follows neither symlinks as new roots).
pub fn dir_size_bytes(root: &Path) -> io::Result<u64> {
    let mut total = 0u64;
    if !root.exists() {
        return Ok(0);
    }
    let meta = fs::symlink_metadata(root)?;
    if meta.file_type().is_file() {
        return Ok(meta.len());
    }
    if !meta.file_type().is_dir() {
        return Ok(0);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for ent in entries {
            let ent = ent?;
            let ft = ent.file_type()?;
            if ft.is_dir() {
                stack.push(ent.path());
            } else if ft.is_file() {
                total = total.saturating_add(ent.metadata()?.len());
            }
        }
    }
    Ok(total)
}

/// Parse sizes like `1073741824`, `1G`, `1GB`, `256M`, `512k`.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size".into());
    }
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(rest) = lower.strip_suffix("gb") {
        (rest, 1024u64 * 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix('g') {
        (rest, 1024u64 * 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix("mb") {
        (rest, 1024u64 * 1024)
    } else if let Some(rest) = lower.strip_suffix('m') {
        (rest, 1024u64 * 1024)
    } else if let Some(rest) = lower.strip_suffix("kb") {
        (rest, 1024u64)
    } else if let Some(rest) = lower.strip_suffix('k') {
        (rest, 1024u64)
    } else if let Some(rest) = lower.strip_suffix('b') {
        (rest, 1u64)
    } else {
        (lower.as_str(), 1u64)
    };
    let num = num.trim();
    if num.is_empty() {
        return Err(format!("missing number in size `{s}`"));
    }
    let n: f64 = num
        .parse()
        .map_err(|_| format!("invalid size number in `{s}`"))?;
    if !n.is_finite() || n < 0.0 {
        return Err(format!("size out of range: `{s}`"));
    }
    let bytes = (n * mult as f64).round();
    if bytes > u64::MAX as f64 {
        return Err(format!("size too large: `{s}`"));
    }
    Ok(bytes as u64)
}

/// Format bytes for human logs.
pub fn format_bytes(n: u64) -> String {
    const K: f64 = 1024.0;
    let n = n as f64;
    if n >= K * K * K {
        format!("{:.2} GiB", n / (K * K * K))
    } else if n >= K * K {
        format!("{:.2} MiB", n / (K * K))
    } else if n >= K {
        format!("{:.2} KiB", n / K)
    } else {
        format!("{n:.0} B")
    }
}

/// Free space (available bytes) on the volume containing `path`.
///
/// Uses `df -k` (macOS/Linux) so we need no extra crates. Walks parents until an
/// existing path is found; falls back to `.` if the whole chain is missing.
pub fn free_space_bytes(path: &Path) -> io::Result<u64> {
    let mut probe = path.to_path_buf();
    loop {
        if probe.exists() {
            break;
        }
        match probe.parent() {
            Some(p) if p != probe => probe = p.to_path_buf(),
            _ => {
                probe = Path::new(".").to_path_buf();
                break;
            }
        }
    }
    let output = Command::new("df")
        .args(["-k", &probe.to_string_lossy()])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "df -k failed for {}",
            probe.display()
        )));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    // Last non-empty line: Filesystem 1024-blocks Used Available Capacity ...
    let line = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .last()
        .ok_or_else(|| io::Error::other("df produced no lines"))?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    // Available is column 4 on both macOS and Linux `df -k` data lines.
    if parts.len() < 4 {
        return Err(io::Error::other(format!("unexpected df line: {line}")));
    }
    let kib: u64 = parts[3]
        .parse()
        .map_err(|_| io::Error::other(format!("df available not a number: {}", parts[3])))?;
    Ok(kib.saturating_mul(1024))
}

/// Default free-space floor before a pump: cover ~2.05× footprint + 512 MiB headroom.
///
/// Prevents silent near-full-disk contamination (first seal-threshold survey).
pub fn default_min_free_for_target(target_bytes: u64) -> u64 {
    let footprint = target_bytes.saturating_mul(25).saturating_div(10); // 2.5×
    let headroom = 512 * 1024 * 1024;
    footprint.saturating_add(headroom)
}

/// Refuse a pump when free space is below the floor.
pub fn ensure_free_space(path: &Path, min_free: u64) -> Result<u64, String> {
    if min_free == 0 {
        return free_space_bytes(path).map_err(|e| format!("free-space probe: {e}"));
    }
    let free = free_space_bytes(path).map_err(|e| format!("free-space probe: {e}"))?;
    if free < min_free {
        return Err(format!(
            "refuse pump: free space {} < min-free {} on volume for {} \
             (near-full disk contaminates rates; free space or pass --min-free 0 to override)",
            format_bytes(free),
            format_bytes(min_free),
            path.display()
        ));
    }
    Ok(free)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_common_sizes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1k").unwrap(), 1024);
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1.5M").unwrap(), (1.5 * 1024.0 * 1024.0) as u64);
    }

    #[test]
    fn default_min_free_covers_footprint() {
        let t = 1024u64 * 1024 * 1024; // 1G
        let m = default_min_free_for_target(t);
        assert!(m > t * 2);
        assert!(m < t * 4);
    }

    #[test]
    fn free_space_on_tmp_is_positive() {
        let f = free_space_bytes(Path::new(".")).expect("df");
        assert!(f > 0);
    }
}
