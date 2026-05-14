//! local change の broadcast (= design.md §5.2)。
//!
//! - `send_file(rel_path, ...)`: blob add → `SyncUpdate::Upsert` → gossip broadcast
//! - `broadcast_tombstone(rel_path, ...)`: 削除を `SyncUpdate::Tombstone` で broadcast
//!
//! 防御線 (design §6.2):
//! - `validate_relative_path` (= message.rs) で `..` / absolute / backslash 拒否
//! - `resolve_safe_path` で symlink escape 拒否 (= watched_dir 外への参照防止)
//! - `MAX_FILE_SIZE = 10 MiB` で巨大 file は skip + warn (= DoS 防止)
//! - tombstone は `recently_broadcast_tombstones` で TTL 内 dedup (= 連発防止)
//!
//! self-loop 防止 (= 受信して書いた file を「自分が編集」 と誤認して再 broadcast)
//! は本 module では行わず、 caller の watcher_loop が `state.last_written` /
//! `state.last_removed` を check して send_* を呼ばない判断をする責務。

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use iroh_blobs::{BlobFormat, BlobsProtocol};
use iroh_gossip::api::GossipSender;

use crate::message::{PeerRef, SyncUpdate, validate_relative_path};
use crate::state::{SyncState, TOMBSTONE_DEDUP_TTL};

/// 1 file あたりの上限 (= design.md §6.2 「DoS 防止」)。
/// 超過 file は send 経路で skip + warn、 受信側でも size guard で drop。
pub const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// `watched_dir` 配下の `rel_path` を canonical absolute path に解決する。
///
/// symlink escape 防止: `rel_path` の各 component を `watched_dir` から順に
/// `symlink_metadata` で覗き込み、 中間に **symlink が居れば即 reject**。
///
/// なぜ「 parent.exists() で canonicalize すれば足りる」 では不十分なのか:
/// - `watched/link → /tmp/outside` が既存で、 受信 path = `link/new/file.md`
///   の場合、 parent = `watched/link/new` は **不在** 扱い (= ENOENT) で
///   canonicalize できない
/// - 旧 logic は parent 不在なら watched_dir を「 safe 」 とみなして path を
///   返していたが、 caller (= receive::handle_upsert) が `create_dir_all` を
///   呼ぶと OS は symlink を辿って outside にdir 作成 → 外部書込が成立
///
/// 修正後の logic (= Phase 2 review High 1):
///
/// 1. `validate_relative_path` で構文 check (`..` / absolute / backslash 拒否)
/// 2. watched_dir 自体を `symlink_metadata` で見て、 dir である事を確認 (= caller
///    側で canonicalize 済前提でも、 念のため defense-in-depth)
/// 3. rel_path の component を 1 つずつ accumulate しながら symlink_metadata
///    で見る。 中間 component が **存在しかつ symlink** なら即 reject。
///    不在は OK (= 末端 file はまだ無くてもよい)
/// 4. file 名 (= 末端 component) 自体は symlink でも OK にしない (= file が
///    別所を指す状況も拒否)
///
/// 呼ぶ側は `watched_dir` を **既に canonicalize 済の path** で渡す前提
/// (= daemon 起動時に 1 度だけ canonicalize、 以降使い回す)。
pub fn resolve_safe_path(watched_dir_canonical: &Path, rel_path: &str) -> Result<PathBuf> {
    validate_relative_path(rel_path)?;

    // watched_dir 自体の sanity check (= 不在 / symlink/file への置き換えを検知)
    let root_meta = std::fs::symlink_metadata(watched_dir_canonical)
        .with_context(|| format!("symlink_metadata {}", watched_dir_canonical.display()))?;
    if !root_meta.file_type().is_dir() {
        return Err(anyhow!(
            "watched_dir is not a directory: {}",
            watched_dir_canonical.display()
        ));
    }

    // rel_path の component を 1 つずつ確認しながら walk。 中間 symlink を拒否。
    let mut current = watched_dir_canonical.to_path_buf();
    let components: Vec<&str> = rel_path.split('/').collect();
    let last_index = components.len() - 1;
    for (i, comp) in components.iter().enumerate() {
        current.push(comp);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) => {
                let ft = meta.file_type();
                if ft.is_symlink() {
                    return Err(anyhow!(
                        "path escapes watched_dir via symlink at {}",
                        current.display()
                    ));
                }
                // 中間 component が file になっていた = 衝突状況。 末端 file ならOK
                if i < last_index && !ft.is_dir() {
                    return Err(anyhow!(
                        "intermediate component is not a directory: {}",
                        current.display()
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // 不在 = OK。 ただし以降の component を見る意味はない (= path 末端まで
                // 不在 prefix 内、 caller の create_dir_all が新規作成する)
                break;
            }
            Err(e) => {
                return Err(anyhow!(
                    "symlink_metadata {}: {e}",
                    current.display()
                ));
            }
        }
    }

    Ok(watched_dir_canonical.join(rel_path))
}

/// local file を blob 化して gossip mesh に Upsert broadcast する。
///
/// 引数:
/// - `rel_path`: watched_dir 相対 path (validate される)
/// - `watched_dir_canonical`: 既に canonicalize 済の watched_dir
/// - `blobs`: BlobsProtocol (= `runtime.blobs()`)
/// - `sender`: GossipSender (= split 後の send 半分)
/// - `self_peer`: 自分の `PeerRef` (= broadcast の from 欄)
/// - `state`: self-loop 防止用 SyncState
pub async fn send_file(
    rel_path: &str,
    watched_dir_canonical: &Path,
    blobs: &BlobsProtocol,
    sender: &GossipSender,
    self_peer: PeerRef,
    state: &SyncState,
) -> Result<()> {
    let abs = resolve_safe_path(watched_dir_canonical, rel_path)?;
    let metadata = std::fs::metadata(&abs)
        .with_context(|| format!("metadata {}", abs.display()))?;
    if !metadata.is_file() {
        return Err(anyhow!("not a regular file: {}", abs.display()));
    }
    if metadata.len() > MAX_FILE_SIZE {
        tracing::warn!(
            path = rel_path,
            size = metadata.len(),
            max = MAX_FILE_SIZE,
            "file exceeds MAX_FILE_SIZE; skipping broadcast"
        );
        return Err(anyhow!(
            "file too large: {} bytes > {} bytes",
            metadata.len(),
            MAX_FILE_SIZE
        ));
    }

    let bytes = std::fs::read(&abs)
        .with_context(|| format!("read {}", abs.display()))?;
    let tag = blobs
        .add_bytes(bytes)
        .await
        .map_err(|e| anyhow!("blob add_bytes: {e}"))?;

    let update = SyncUpdate::upsert(rel_path.into(), tag.hash, BlobFormat::Raw, self_peer)?;
    let payload = update.to_bytes()?;

    sender
        .broadcast(payload.into())
        .await
        .map_err(|e| anyhow!("gossip broadcast: {e}"))?;

    // self-loop 防止 hook: 自分が書いた file が watcher で再 broadcast されないよう、
    // last_written に (path, hash) を記録する。 watcher 側は同 hash を見て skip する。
    state
        .last_written
        .lock()
        .expect("last_written lock")
        .insert(PathBuf::from(rel_path), tag.hash);

    tracing::debug!(
        path = rel_path,
        hash = %tag.hash,
        bytes = metadata.len(),
        "broadcast Upsert"
    );
    Ok(())
}

/// 削除を Tombstone broadcast。 TTL 内なら dedup で skip。
pub async fn broadcast_tombstone(
    rel_path: &str,
    sender: &GossipSender,
    self_peer: PeerRef,
    state: &SyncState,
) -> Result<bool> {
    validate_relative_path(rel_path)?;
    let key = PathBuf::from(rel_path);

    // dedup window 内なら skip (= macOS で `rm` 1 回が複数 event を吐くケース)。
    {
        let now = Instant::now();
        let mut dedup = state
            .recently_broadcast_tombstones
            .lock()
            .expect("tombstone dedup lock");
        if let Some(t) = dedup.get(&key)
            && now.duration_since(*t) < TOMBSTONE_DEDUP_TTL
        {
            tracing::debug!(path = rel_path, "tombstone dedup window hit; skipping");
            return Ok(false);
        }
        dedup.insert(key, now);
    }

    let update = SyncUpdate::tombstone(rel_path.into(), self_peer)?;
    sender
        .broadcast(update.to_bytes()?.into())
        .await
        .map_err(|e| anyhow!("gossip broadcast: {e}"))?;

    tracing::debug!(path = rel_path, "broadcast Tombstone");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_file(dir: &Path, rel: &str, content: &[u8]) -> PathBuf {
        let p = dir.join(rel);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn resolve_safe_path_accepts_simple_relative() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_file(&root, "foo.md", b"hi");
        let p = resolve_safe_path(&root, "foo.md").unwrap();
        assert_eq!(p, root.join("foo.md"));
    }

    #[test]
    fn resolve_safe_path_accepts_nested() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_file(&root, "a/b/c.md", b"x");
        let p = resolve_safe_path(&root, "a/b/c.md").unwrap();
        assert_eq!(p, root.join("a/b/c.md"));
    }

    #[test]
    fn resolve_safe_path_accepts_new_file_in_existing_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let p = resolve_safe_path(&root, "sub/new.md").unwrap();
        assert_eq!(p, root.join("sub/new.md"));
    }

    #[test]
    fn resolve_safe_path_rejects_dotdot() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let e = resolve_safe_path(&root, "../escape.md").unwrap_err();
        assert!(format!("{e:#}").contains(".."));
    }

    #[test]
    fn resolve_safe_path_rejects_absolute() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let e = resolve_safe_path(&root, "/etc/passwd").unwrap_err();
        assert!(format!("{e:#}").contains("absolute"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_safe_path_rejects_symlink_escape() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        let outside_canonical = outside.path().canonicalize().unwrap();
        // root/escape_dir → outside_canonical
        std::os::unix::fs::symlink(&outside_canonical, root.join("escape_dir")).unwrap();
        let e = resolve_safe_path(&root, "escape_dir/foo.md").unwrap_err();
        assert!(format!("{e:#}").contains("escapes watched_dir"));
    }

    /// High 1 review fix の regression test: `watched/link → /tmp/outside` が
    /// 既存で、 受信 path が `link/new/file.md` のように **link 配下の不在
    /// subdir** を指すケース。 旧 logic では parent 不在 → watched_dir を safe
    /// 扱い → caller の create_dir_all が symlink を辿って outside に作成、 と
    /// なる脱出経路が成立していた。 修正後は最初の component で symlink を
    /// 検出して reject する。
    #[cfg(unix)]
    #[test]
    fn resolve_safe_path_rejects_nested_path_through_symlink() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        let outside_canonical = outside.path().canonicalize().unwrap();
        std::os::unix::fs::symlink(&outside_canonical, root.join("link")).unwrap();

        let e = resolve_safe_path(&root, "link/new_subdir/file.md").unwrap_err();
        assert!(
            format!("{e:#}").contains("symlink"),
            "expected symlink reject, got: {e:#}"
        );
        // 「 outside dir に new_subdir が誤って作られていない」 ことも assert
        assert!(
            !outside_canonical.join("new_subdir").exists(),
            "must not have leaked into outside dir"
        );
    }

    /// 末端 component が symlink でも reject (= file が別所を指す状況拒否)。
    #[cfg(unix)]
    #[test]
    fn resolve_safe_path_rejects_terminal_symlink() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let outside = tempfile::TempDir::new().unwrap();
        let outside_file = outside.path().join("target.md");
        std::fs::write(&outside_file, b"secret").unwrap();
        std::os::unix::fs::symlink(&outside_file, root.join("link.md")).unwrap();

        let e = resolve_safe_path(&root, "link.md").unwrap_err();
        assert!(format!("{e:#}").contains("symlink"));
    }

    #[test]
    fn max_file_size_is_10mib() {
        assert_eq!(MAX_FILE_SIZE, 10 * 1024 * 1024);
    }

    // -------- send / broadcast 系 (real Endpoint + Gossip 要) --------

    use crate::message::SyncUpdateBody;
    use crate::runtime::SyncRuntime;
    use crate::allowlist::AllowList;
    use std::sync::Arc;
    use iroh::{Endpoint, SecretKey, endpoint::presets};
    use futures_lite::StreamExt;

    async fn build_runtime(secret_byte: u8, folder_secret: [u8; 16]) -> (SyncRuntime, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[secret_byte; 32]))
            .bind()
            .await
            .unwrap();
        let rt = SyncRuntime::build(
            endpoint,
            tmp.path(),
            Arc::new(AllowList::empty_strict()),
            Some(&folder_secret),
        )
        .await
        .unwrap();
        (rt, tmp)
    }

    #[tokio::test]
    async fn send_file_broadcasts_upsert() {
        let (mut rt, _store_tmp) = build_runtime(11, [0xAAu8; 16]).await;
        let endpoint_id = rt.endpoint().id();
        let topic = rt.take_topic().unwrap();
        let (sender, _receiver) = topic.split();

        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        write_file(&watch_root, "foo.md", b"hello world");

        let state = SyncState::new();
        let self_peer = PeerRef { id: endpoint_id };
        send_file(
            "foo.md",
            &watch_root,
            rt.blobs(),
            &sender,
            self_peer,
            &state,
        )
        .await
        .unwrap();

        let contained = {
            let last = state.last_written.lock().unwrap();
            last.contains_key(std::path::Path::new("foo.md"))
        };
        assert!(contained, "last_written should contain foo.md");

        drop(sender);
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn send_file_rejects_oversized() {
        let (mut rt, _store_tmp) = build_runtime(12, [0xBBu8; 16]).await;
        let topic = rt.take_topic().unwrap();
        let (sender, _receiver) = topic.split();

        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        // MAX_FILE_SIZE + 1 byte
        let big = vec![0u8; (MAX_FILE_SIZE + 1) as usize];
        write_file(&watch_root, "big.bin", &big);

        let state = SyncState::new();
        let self_peer = PeerRef { id: rt.endpoint().id() };
        let e = send_file(
            "big.bin",
            &watch_root,
            rt.blobs(),
            &sender,
            self_peer,
            &state,
        )
        .await
        .unwrap_err();
        assert!(format!("{e:#}").contains("too large"));

        drop(sender);
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broadcast_tombstone_dedup_within_ttl() {
        let (mut rt, _store_tmp) = build_runtime(13, [0xCCu8; 16]).await;
        let topic = rt.take_topic().unwrap();
        let (sender, _receiver) = topic.split();

        let state = SyncState::new();
        let self_peer = PeerRef { id: rt.endpoint().id() };

        // 1 回目: broadcast 成立
        let r1 = broadcast_tombstone("removed.md", &sender, self_peer.clone(), &state)
            .await
            .unwrap();
        assert!(r1, "first call should broadcast");

        // 2 回目 (TTL 内): dedup で skip
        let r2 = broadcast_tombstone("removed.md", &sender, self_peer.clone(), &state)
            .await
            .unwrap();
        assert!(!r2, "second call within TTL should be deduped");

        drop(sender);
        rt.shutdown().await.unwrap();
    }

    /// SyncUpdate を gossip 送信 → 同 peer の receiver で受信できることを
    /// 確認 (= self-receive smoke test、 wire format の round-trip)。
    #[tokio::test]
    async fn upsert_payload_decodes_on_self() {
        let (mut rt, _store_tmp) = build_runtime(14, [0xDDu8; 16]).await;
        let endpoint_id = rt.endpoint().id();
        let topic = rt.take_topic().unwrap();
        let (sender, mut receiver) = topic.split();

        // 1 peer gossip では neighbor 不在で broadcast は届かないことが多い。
        // ので、 本 test は SyncUpdate::to_bytes / from_bytes を直接 round-trip
        // するレベルで wire 互換を確認する (= send 経路で組み立てる payload を
        // decode しても同等 PeerRef + path 取れる)。
        let self_peer = PeerRef { id: endpoint_id };
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        write_file(&watch_root, "foo.md", b"abc");

        let state = SyncState::new();
        send_file(
            "foo.md",
            &watch_root,
            rt.blobs(),
            &sender,
            self_peer.clone(),
            &state,
        )
        .await
        .unwrap();

        // last_written に hash が入ったので、 同 hash で SyncUpdate を再構築できる。
        let hash = *state
            .last_written
            .lock()
            .unwrap()
            .get(std::path::Path::new("foo.md"))
            .unwrap();
        let reconstructed =
            SyncUpdate::upsert("foo.md".into(), hash, BlobFormat::Raw, self_peer).unwrap();
        let bytes = reconstructed.to_bytes().unwrap();
        let decoded = SyncUpdate::from_bytes(&bytes).unwrap();
        match decoded.body {
            SyncUpdateBody::Upsert { path, hash: h, .. } => {
                assert_eq!(path, "foo.md");
                assert_eq!(h, hash);
            }
            _ => panic!("expected Upsert"),
        }

        // 1 peer 環境では receiver から自分 broadcast は届かない (= 期待動作)
        // ので short timeout で何も来ないことを確認。
        let none = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            receiver.next(),
        )
        .await;
        assert!(
            none.is_err() || none.as_ref().unwrap().is_none(),
            "self-broadcast should not loop back in 1-peer mesh"
        );

        drop(sender);
        drop(receiver);
        rt.shutdown().await.unwrap();
    }
}
