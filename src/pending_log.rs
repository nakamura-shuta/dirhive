//! 受信 change log (= peer から受信した change の中性的な記録)。
//!
//! design.md §4.4 / §6.6 参照。`~/.local/share/p2p-dir-sync/pending/<repo_hash>/`
//! 配下に entry を 1 file 1 entry で append する。

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// 現在の schema version。
pub const PENDING_SCHEMA_VERSION: u32 = 1;

/// 受信 change の中性的な記録。Upsert / Tombstone どちらも扱う。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingEntry {
    Upsert {
        schema_version: u32,
        rel_path: String,
        received_at: i64,
        source_peer: String,
        blob_hash: String,
        bytes: u64,
    },
    Tombstone {
        schema_version: u32,
        rel_path: String,
        received_at: i64,
        source_peer: String,
    },
}

impl PendingEntry {
    pub fn rel_path(&self) -> &str {
        match self {
            Self::Upsert { rel_path, .. } | Self::Tombstone { rel_path, .. } => rel_path,
        }
    }

    pub fn received_at(&self) -> i64 {
        match self {
            Self::Upsert { received_at, .. } | Self::Tombstone { received_at, .. } => *received_at,
        }
    }

    pub fn source_peer(&self) -> &str {
        match self {
            Self::Upsert { source_peer, .. } | Self::Tombstone { source_peer, .. } => source_peer,
        }
    }
}

/// 受信 entry を 1 file として記録する (atomic write)。file 名は
/// `<received_at>-<source_peer 先頭 8 chars>.json`。
pub fn record_receive(root: &Path, repo_hash: &str, entry: &PendingEntry) -> Result<()> {
    let dir = root.join(repo_hash);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create_dir_all {}", dir.display()))?;
    let short_peer: String = entry.source_peer().chars().take(8).collect();
    let file_name = format!("{}-{}.json", entry.received_at(), short_peer);
    let target = dir.join(file_name);

    let body = serde_json::to_vec_pretty(entry).context("serialize PendingEntry")?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".pending.")
        .suffix(".tmp")
        .tempfile_in(&dir)
        .with_context(|| format!("tempfile_in {}", dir.display()))?;
    use std::io::Write;
    tmp.as_file_mut()
        .write_all(&body)
        .context("write pending tempfile")?;
    tmp.persist(&target)
        .map_err(|e| anyhow::anyhow!("persist {}: {}", target.display(), e.error))?;
    Ok(())
}

/// `root/<repo_hash>/*.json` を全部読んで Vec で返す (received_at 新しい順)。
pub fn list_pending(root: &Path, repo_hash: &str) -> Result<Vec<PendingEntry>> {
    let dir = root.join(repo_hash);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for ent in std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let ent = ent?;
        let path = ent.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if path
            .file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }
        let bytes = std::fs::read(&path)
            .with_context(|| format!("read {}", path.display()))?;
        let e: PendingEntry = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", path.display()))?;
        entries.push(e);
    }
    entries.sort_by(|a, b| b.received_at().cmp(&a.received_at()));
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn upsert(t: i64, peer: &str, path: &str) -> PendingEntry {
        PendingEntry::Upsert {
            schema_version: PENDING_SCHEMA_VERSION,
            rel_path: path.into(),
            received_at: t,
            source_peer: peer.into(),
            blob_hash: "blake3:abc".into(),
            bytes: 100,
        }
    }

    fn tombstone(t: i64, peer: &str, path: &str) -> PendingEntry {
        PendingEntry::Tombstone {
            schema_version: PENDING_SCHEMA_VERSION,
            rel_path: path.into(),
            received_at: t,
            source_peer: peer.into(),
        }
    }

    #[test]
    fn upsert_serializes_with_kind_tag() {
        let e = upsert(100, "abcd1234efgh", "foo.md");
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], "upsert");
        assert_eq!(v["schema_version"], 1);
        assert_eq!(v["rel_path"], "foo.md");
        assert_eq!(v["blob_hash"], "blake3:abc");
        assert_eq!(v["bytes"], 100);
    }

    #[test]
    fn tombstone_serializes_without_blob_fields() {
        let e = tombstone(200, "peer1", "old.md");
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], "tombstone");
        assert!(v.get("blob_hash").is_none());
        assert!(v.get("bytes").is_none());
    }

    #[test]
    fn round_trip_upsert() {
        let e = upsert(100, "p1", "a.md");
        let s = serde_json::to_string(&e).unwrap();
        let e2: PendingEntry = serde_json::from_str(&s).unwrap();
        assert_eq!(e, e2);
    }

    #[test]
    fn round_trip_tombstone() {
        let e = tombstone(101, "p1", "a.md");
        let s = serde_json::to_string(&e).unwrap();
        let e2: PendingEntry = serde_json::from_str(&s).unwrap();
        assert_eq!(e, e2);
    }

    #[test]
    fn accessors() {
        let e = upsert(50, "peer-xyz-12345", "p.md");
        assert_eq!(e.rel_path(), "p.md");
        assert_eq!(e.received_at(), 50);
        assert_eq!(e.source_peer(), "peer-xyz-12345");
    }

    #[test]
    fn record_and_list_round_trip() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        record_receive(root, "repo1", &upsert(100, "peer1234", "a.md")).unwrap();
        record_receive(root, "repo1", &tombstone(200, "peer5678", "b.md")).unwrap();
        record_receive(root, "repo2", &upsert(300, "peer1234", "c.md")).unwrap();

        let entries = list_pending(root, "repo1").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].received_at(), 200);
        assert_eq!(entries[1].received_at(), 100);
    }

    #[test]
    fn list_pending_missing_dir_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let r = list_pending(tmp.path(), "noexist").unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn record_receive_does_not_leak_tempfile() {
        let tmp = TempDir::new().unwrap();
        record_receive(tmp.path(), "r", &upsert(1, "x", "a.md")).unwrap();
        let count = std::fs::read_dir(tmp.path().join("r")).unwrap().count();
        assert_eq!(count, 1);
    }
}
