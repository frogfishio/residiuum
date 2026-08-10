//! Path-compressed radix / ART-style index for ordered byte keys (strings).
//!
//! Nodes store a compressed path prefix and either a leaf offset or a sorted
//! edge table (byte → child). Lookups walk the path; scans walk leaves in order.

/// Compact radix tree over sorted unique keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedRadixIndex {
    /// Arena of nodes; root is index 0 when non-empty.
    nodes: Vec<Node>,
    /// Full keys for exact match and ordered scan (insertion = sorted order).
    leaves: Vec<Leaf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Leaf {
    key: Vec<u8>,
    offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node {
    /// Shared prefix relative to the path depth at this node.
    prefix: Vec<u8>,
    /// If `Some`, a key ends at this node (may still have edges for longer keys).
    leaf: Option<u32>,
    /// Sorted edges by first differing byte.
    edges: Vec<Edge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    byte: u8,
    child: u32,
}

impl CompressedRadixIndex {
    /// Build from unique keys sorted lexicographically.
    pub fn build(sorted: &[(Vec<u8>, u64)]) -> Self {
        if sorted.is_empty() {
            return Self {
                nodes: Vec::new(),
                leaves: Vec::new(),
            };
        }
        let leaves: Vec<Leaf> = sorted
            .iter()
            .map(|(k, o)| Leaf {
                key: k.clone(),
                offset: *o,
            })
            .collect();
        let mut nodes = vec![Node {
            prefix: Vec::new(),
            leaf: None,
            edges: Vec::new(),
        }];
        for (i, leaf) in leaves.iter().enumerate() {
            insert(&mut nodes, &leaf.key, i as u32);
        }
        Self { nodes, leaves }
    }

    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    pub fn get(&self, key: &[u8]) -> Option<u64> {
        if self.nodes.is_empty() {
            return None;
        }
        let mut ni = 0usize;
        let mut depth = 0usize;
        loop {
            let node = &self.nodes[ni];
            if depth + node.prefix.len() > key.len() {
                return None;
            }
            if &key[depth..depth + node.prefix.len()] != node.prefix.as_slice() {
                return None;
            }
            depth += node.prefix.len();
            if depth == key.len() {
                return node
                    .leaf
                    .map(|li| self.leaves[li as usize].offset)
                    .filter(|_| self.leaves[node.leaf.unwrap() as usize].key.as_slice() == key);
            }
            let b = key[depth];
            match node.edges.binary_search_by_key(&b, |e| e.byte) {
                Ok(ei) => {
                    ni = node.edges[ei].child as usize;
                    depth += 1;
                }
                Err(_) => return None,
            }
        }
    }

    pub fn scan_after(&self, after: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, u64)> {
        if limit == 0 || self.leaves.is_empty() {
            return Vec::new();
        }
        let start = match after {
            None => 0,
            Some(a) => {
                let mut lo = 0usize;
                let mut hi = self.leaves.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if self.leaves[mid].key.as_slice() <= a {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo
            }
        };
        self.leaves[start..]
            .iter()
            .take(limit)
            .map(|l| (l.key.clone(), l.offset))
            .collect()
    }
}

fn insert(nodes: &mut Vec<Node>, key: &[u8], leaf_idx: u32) {
    let mut ni = 0usize;
    let mut depth = 0usize;
    loop {
        let prefix = nodes[ni].prefix.clone();
        let common = common_prefix(&prefix, &key[depth..]);
        if common < prefix.len() {
            // Split this node at the first mismatch inside its prefix.
            let split_byte = prefix[common];
            let rest_prefix = prefix[common + 1..].to_vec();
            let old_leaf = nodes[ni].leaf;
            let old_edges = std::mem::take(&mut nodes[ni].edges);
            let child = Node {
                prefix: rest_prefix,
                leaf: old_leaf,
                edges: old_edges,
            };
            let child_i = nodes.len() as u32;
            nodes.push(child);
            nodes[ni].prefix = prefix[..common].to_vec();
            nodes[ni].leaf = None;
            nodes[ni].edges = vec![Edge {
                byte: split_byte,
                child: child_i,
            }];
        }
        depth += nodes[ni].prefix.len();
        if depth == key.len() {
            nodes[ni].leaf = Some(leaf_idx);
            return;
        }
        let b = key[depth];
        match nodes[ni].edges.binary_search_by_key(&b, |e| e.byte) {
            Ok(ei) => {
                ni = nodes[ni].edges[ei].child as usize;
                depth += 1;
            }
            Err(ins) => {
                let child = Node {
                    prefix: key[depth + 1..].to_vec(),
                    leaf: Some(leaf_idx),
                    edges: Vec::new(),
                };
                let child_i = nodes.len() as u32;
                nodes.push(child);
                nodes[ni].edges.insert(
                    ins,
                    Edge {
                        byte: b,
                        child: child_i,
                    },
                );
                return;
            }
        }
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

pub(crate) fn encode(index: &CompressedRadixIndex, out: &mut Vec<u8>) {
    // Encode leaves only; rebuild tree on decode (compact + simple).
    out.extend_from_slice(&(index.leaves.len() as u64).to_le_bytes());
    for leaf in &index.leaves {
        let kl = u16::try_from(leaf.key.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&kl.to_le_bytes());
        out.extend_from_slice(&leaf.key[..kl as usize]);
        out.extend_from_slice(&leaf.offset.to_le_bytes());
    }
}

pub(crate) fn decode(bytes: &[u8]) -> Option<CompressedRadixIndex> {
    if bytes.len() < 8 {
        return None;
    }
    let n = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
    let mut off = 8;
    let mut sorted = Vec::with_capacity(n);
    for _ in 0..n {
        if off + 2 > bytes.len() {
            return None;
        }
        let kl = u16::from_le_bytes(bytes[off..off + 2].try_into().ok()?) as usize;
        off += 2;
        if off + kl + 8 > bytes.len() {
            return None;
        }
        let key = bytes[off..off + kl].to_vec();
        off += kl;
        let offset = u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
        off += 8;
        sorted.push((key, offset));
    }
    Some(CompressedRadixIndex::build(&sorted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_string_keys() {
        let mut sorted: Vec<_> = (0..100u64)
            .map(|i| (format!("user/{i}/meta").into_bytes(), i * 9))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let idx = CompressedRadixIndex::build(&sorted);
        for (k, o) in &sorted {
            assert_eq!(idx.get(k), Some(*o), "key {:?}", String::from_utf8_lossy(k));
        }
        assert!(idx.get(b"user/x/meta").is_none());
        let page = idx.scan_after(None, 3);
        assert_eq!(page.len(), 3);
    }

    #[test]
    fn prefix_and_sibling_keys() {
        let sorted = vec![
            (b"a".to_vec(), 1),
            (b"ab".to_vec(), 2),
            (b"abc".to_vec(), 3),
            (b"b".to_vec(), 4),
            (b"ba".to_vec(), 5),
        ];
        let idx = CompressedRadixIndex::build(&sorted);
        for (k, o) in &sorted {
            assert_eq!(idx.get(k), Some(*o));
        }
        assert!(idx.get(b"ac").is_none());
    }
}
