//! 共有 state (SyncState) + pending metadata tracker (PendingTracker)。
//!
//! - `SyncState`: receive ↔ watcher の self-loop 防止に使う in-memory state
//!   (pending_written / last_written / last_removed / recently_broadcast_tombstones)
//! - `PendingTracker`: P2P 受信 file を `~/.local/share/dirhive/pending/<repo_hash>/`
//!   に記録するための pre-resolved context

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use iroh_blobs::Hash;

/// 「relative path → BLAKE3 hash」のレジストリ型。
pub type WriteRegistry = Arc<Mutex<HashMap<PathBuf, Hash>>>;

/// 双方向同期で必要な共有 state。
#[derive(Debug, Clone, Default)]
pub struct SyncState {
    /// 受信 → write 中 の rel_path → expected hash。
    /// `receive_loop` が download 開始前に insert、成功時に `last_written` へ
    /// 移動、失敗時に remove する。
    pub pending_written: WriteRegistry,

    /// 受信 → write 完了済 の rel_path → 書いた hash。
    /// `watcher_loop` は現在の file hash を計算して、ここに同じ (path, hash) が
    /// あれば「自分が書いた直後の同一内容 = 再放送不要」と判定する。
    pub last_written: WriteRegistry,

    /// 受信 Tombstone → 削除直後 の rel_path 集合。
    /// `handle_tombstone` が unlink した直後に insert する。watcher の Remove
    /// event handler はここに入っている path を consume + skip することで、
    /// 自分の daemon が起こした削除を peer に再 broadcast する self-loop を防ぐ。
    pub last_removed: Arc<Mutex<HashSet<PathBuf>>>,

    /// 直近 Tombstone broadcast の rel_path → Instant 記録 (time-bounded dedup)。
    /// macOS で `rm` 1 回が複数 event を吐くケースでの Tombstone 二重 broadcast を抑止。
    pub recently_broadcast_tombstones: Arc<Mutex<HashMap<PathBuf, Instant>>>,
}

/// `recently_broadcast_tombstones` の TTL。
pub const TOMBSTONE_DEDUP_TTL: Duration = Duration::from_secs(2);

impl SyncState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// 受信 file ごとに pending log を書き出すための pre-resolved context。
///
/// `pending_root` (= `~/.local/share/dirhive/pending`) と `repo_hash` (out_dir
/// realpath の BLAKE3 16 hex) を daemon 起動時に 1 度だけ計算して保持し、
/// receive 経路で毎回 alloc する負荷を避ける。
#[derive(Debug)]
pub struct PendingTracker {
    pub pending_root: PathBuf,
    pub repo_hash: String,
}

impl PendingTracker {
    /// `out_dir` (= watched_dir、既に存在する前提) から `repo_hash` を計算し、
    /// `pending_root` と組み合わせた tracker を作る。
    ///
    /// **`out_dir` を勝手に create しない** (M2 review fix): watched_dir の不在は
    /// daemon の起動チェック / watcher_loop で扱う責務であって、PendingTracker
    /// が黙って dir を作ると `sync.health-check` の `watched_dir_exists` 判定が
    /// 崩れる。canonicalize が ENOENT で失敗した場合はそのまま error を返す。
    pub fn new(out_dir: &Path) -> Result<Self> {
        let pending_root =
            crate::paths::default_pending_dir().context("resolving pending dir")?;
        let realpath = out_dir
            .canonicalize()
            .with_context(|| format!("canonicalize {}", out_dir.display()))?;
        let repo_hash = compute_repo_hash(&realpath);
        Ok(Self {
            pending_root,
            repo_hash,
        })
    }
}

/// realpath を BLAKE3 hash の先頭 16 hex に。pending log の per-repo dir 名に使う。
pub fn compute_repo_hash(realpath: &Path) -> String {
    let bytes = realpath.as_os_str().as_encoded_bytes();
    let hash = blake3::hash(bytes);
    hex::encode(&hash.as_bytes()[..8])
}

// blake3 の hex encode は手書きする (= hex crate を入れない、シンプル方針)
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", b);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn sync_state_default_is_empty() {
        let s = SyncState::default();
        assert!(s.pending_written.lock().unwrap().is_empty());
        assert!(s.last_written.lock().unwrap().is_empty());
        assert!(s.last_removed.lock().unwrap().is_empty());
        assert!(s.recently_broadcast_tombstones.lock().unwrap().is_empty());
    }

    #[test]
    fn sync_state_can_clone_shared_state() {
        let s1 = SyncState::new();
        let s2 = s1.clone();
        // 同 Arc を共有していることを mutation で確認
        s1.last_removed
            .lock()
            .unwrap()
            .insert(PathBuf::from("a.md"));
        assert!(s2.last_removed.lock().unwrap().contains(Path::new("a.md")));
    }

    #[test]
    fn tombstone_dedup_ttl_is_2_seconds() {
        assert_eq!(TOMBSTONE_DEDUP_TTL, Duration::from_secs(2));
    }

    #[test]
    fn compute_repo_hash_is_16_hex() {
        let h = compute_repo_hash(Path::new("/tmp/foo"));
        assert_eq!(h.len(), 16, "16 hex chars (= 8 bytes BLAKE3 prefix)");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn compute_repo_hash_deterministic() {
        let h1 = compute_repo_hash(Path::new("/tmp/foo"));
        let h2 = compute_repo_hash(Path::new("/tmp/foo"));
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_repo_hash_differs_by_path() {
        let h1 = compute_repo_hash(Path::new("/tmp/foo"));
        let h2 = compute_repo_hash(Path::new("/tmp/bar"));
        assert_ne!(h1, h2);
    }

    #[test]
    #[serial(state_env)]
    fn pending_tracker_new_succeeds_when_out_dir_exists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let state = tmp.path().join("state");
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();
        unsafe {
            std::env::set_var("DIRHIVE_STATE_DIR", &state);
        }
        let tracker = PendingTracker::new(&out).unwrap();
        assert_eq!(tracker.repo_hash.len(), 16);
        unsafe {
            std::env::remove_var("DIRHIVE_STATE_DIR");
        }
    }

    /// M2 review fix: out_dir 不在なら error (= 勝手に作らない)。
    #[test]
    fn pending_tracker_new_fails_when_out_dir_missing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let out = tmp.path().join("missing");
        let e = PendingTracker::new(&out).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("canonicalize") || msg.contains("No such file"),
            "expected ENOENT-style error, got: {msg}"
        );
        assert!(!out.exists(), "PendingTracker::new must not create dir");
    }
}
