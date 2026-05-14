//! peer から受信した SyncUpdate を local fs に適用 (= design.md §5.3)。
//!
//! - `receive_loop`: GossipReceiver の event stream を回す
//! - `handle_upsert`: blob download → size guard → conflict backup → atomic write
//!   → last_written 更新 → pending_log 記録
//! - `handle_tombstone`: validate → last_removed mark → atomic remove → pending_log 記録
//!
//! 防御線 (design §6.2):
//! - SyncUpdate v2 schema validate (= `from_bytes` で version check)
//! - allowlist check: `from.id` が AllowList::contains を満たさない update は drop
//! - rel_path validate (= `validate_relative_path` を `from_bytes` 内で実施)
//! - symlink escape 防止 = `resolve_safe_path` (send.rs と同じ helper)
//! - size guard: blob 取得後の `tag.bytes > MAX_FILE_SIZE` は drop + warn
//! - atomic write: sibling tempfile + persist (POSIX rename)
//!
//! ## 限界: gossip allowlist は **DoS layer**、 adversary layer ではない
//!
//! `update.from().id` は **payload 内の自己申告 field** で署名検証していない。
//! folder_secret を知る peer は誰でも任意の `from.id` を名乗れる。 つまり:
//!
//! - mesh に居る未許可 peer が「 自分は許可済 peer 」 と詐称して Upsert / Tombstone
//!   を broadcast 可能 (特に Tombstone は blob fetch を経由しないので削除まで通る)
//!
//! MVP では:
//! 1. `folder_secret` を **「 mesh への入場券 」** として 1 つの信頼境界とみなす
//! 2. folder_secret を知る peer は「 群れの一員 」 として既に信頼済の扱い
//! 3. allowlist は「 既知 peer の管理 」 + 「 mesh に居るだけの未許可 peer から
//!    の DoS / noise を落とす 」 ための運用 layer であって、 暗号学的な認証
//!    境界ではない
//!
//! design §6.1 「 既に許可した peer が後で悪意ある change を流す経路 = 信頼境界、
//! revoke で対応 」 と同じ整理。 真の adversary 耐性 (= folder_secret を漏らした
//! peer が悪意化した状況) は Phase 3 以降で SyncUpdate に endpoint-key 署名を
//! 追加することで対応する想定 (= 各 SyncUpdate を `from.id` の secret_key で
//! 署名し、 受信側で公開鍵検証)。
//!
//! ## test 範囲
//!
//! 本 module は **fetch (= blob download)** の完全な path 統合は Phase 3 で
//! 行う。 unit test で testable な範囲は dispatch policy + 個別 write/remove
//! 経路。 blob fetch は 2-peer integration test で確認する想定。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use futures_lite::StreamExt;
use iroh::{Endpoint, EndpointId};
use iroh_blobs::{BlobFormat, BlobsProtocol, Hash, HashAndFormat};
use iroh_gossip::api::{Event, GossipReceiver};

use crate::allowlist::AllowList;
use crate::conflict::compute_conflict_backup_path;
use crate::message::{SyncUpdate, SyncUpdateBody};
use crate::pending_log::{PENDING_SCHEMA_VERSION, PendingEntry, record_receive};
use crate::send::{MAX_FILE_SIZE, resolve_safe_path};
use crate::state::{PendingTracker, SyncState};

/// data-plane 成立観測 callback (= Phase 3 review L4)。
///
/// `handle_upsert` / `handle_tombstone` が Applied を返した直後に caller (= daemon)
/// が DaemonState::mark_peer_seen を呼べるようにする hook 型。 None なら何もしない。
///
/// 呼ばれる契機:
/// - Upsert 成功 (= blob fetch + atomic write 完了): from_id を渡す
/// - Tombstone 成功 (= unlink or NotFound 完了): from_id を渡す
///
/// blob serve 側 (= AllowlistBlobs::accept で provider session 完了) は別 callback
/// で繋ぐ想定 (= Phase 3 後半で AllowlistBlobs に追加予定)。
pub type PeerSeenCallback = Arc<dyn Fn(EndpointId, i64) + Send + Sync>;

/// receive 経路の動作結果 (= test で観測しやすくするため enum 化)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiveOutcome {
    Applied,
    Skipped { reason: &'static str },
}

/// receive_loop の per-event ハンドラ。 本体 loop は GossipReceiver から
/// `Event::Received` を取り出して `dispatch_update` に渡し、 他 event は log。
///
/// `on_peer_seen` は Applied 時の data-plane 成功通知 callback (= L4)。 None なら
/// 何もしない (= 単独 use case では callback 不要)。
#[allow(clippy::too_many_arguments)]
pub async fn receive_loop(
    mut receiver: GossipReceiver,
    endpoint: Endpoint,
    blobs: BlobsProtocol,
    allowlist: Arc<AllowList>,
    state: SyncState,
    watched_dir_canonical: PathBuf,
    pending: Arc<PendingTracker>,
    on_peer_seen: Option<PeerSeenCallback>,
) -> Result<()> {
    while let Some(event) = receiver.next().await {
        let event = match event {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("gossip stream error: {e}");
                continue;
            }
        };
        match event {
            Event::Received(msg) => {
                let outcome = dispatch_update(
                    &msg.content,
                    &endpoint,
                    &blobs,
                    &allowlist,
                    &state,
                    &watched_dir_canonical,
                    &pending,
                    on_peer_seen.as_ref(),
                )
                .await;
                match outcome {
                    Ok(ReceiveOutcome::Applied) => {}
                    Ok(ReceiveOutcome::Skipped { reason }) => {
                        tracing::debug!(reason, "skipped incoming update")
                    }
                    Err(e) => tracing::warn!("dispatch error: {e:#}"),
                }
            }
            Event::NeighborUp(id) => {
                tracing::debug!(peer = %id.fmt_short(), "neighbor up")
            }
            Event::NeighborDown(id) => {
                tracing::debug!(peer = %id.fmt_short(), "neighbor down")
            }
            Event::Lagged => tracing::warn!("gossip receiver lagged"),
        }
    }
    Ok(())
}

/// 1 件の gossip payload を dispatch する。
///
/// 経路:
/// 1. `SyncUpdate::from_bytes` で wire schema validate (version + path)
/// 2. `allowlist.contains(from.id)` で受信認可 check (= drop or 続行)
/// 3. body によって handle_upsert / handle_tombstone に dispatch
/// 4. pending_log に entry を 1 件 append
/// 5. `on_peer_seen` callback を invoke (= L4 mark_peer_seen hook)
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_update(
    payload: &[u8],
    endpoint: &Endpoint,
    blobs: &BlobsProtocol,
    allowlist: &Arc<AllowList>,
    state: &SyncState,
    watched_dir_canonical: &Path,
    pending: &Arc<PendingTracker>,
    on_peer_seen: Option<&PeerSeenCallback>,
) -> Result<ReceiveOutcome> {
    let update = match SyncUpdate::from_bytes(payload) {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!("invalid SyncUpdate: {e:#}");
            return Ok(ReceiveOutcome::Skipped {
                reason: "invalid_wire_format",
            });
        }
    };

    let from_id = update.from().id;
    if !allowlist.contains(&from_id) {
        tracing::warn!(
            peer = %from_id.fmt_short(),
            "rejected update from peer not in allowlist"
        );
        return Ok(ReceiveOutcome::Skipped {
            reason: "peer_not_allowed",
        });
    }

    let received_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    match update.body {
        SyncUpdateBody::Upsert { path, hash, format, from } => {
            let outcome = handle_upsert(
                &path,
                hash,
                format,
                from.id,
                endpoint,
                blobs,
                state,
                watched_dir_canonical,
            )
            .await?;
            if matches!(outcome, ReceiveOutcome::Applied) {
                let bytes = blob_size(blobs, hash).await.unwrap_or(0);
                let entry = PendingEntry::Upsert {
                    schema_version: PENDING_SCHEMA_VERSION,
                    rel_path: path.clone(),
                    received_at,
                    source_peer: from.id.to_string(),
                    blob_hash: hash.to_string(),
                    bytes,
                };
                if let Err(e) = record_receive(&pending.pending_root, &pending.repo_hash, &entry) {
                    tracing::warn!("pending_log record_receive failed: {e:#}");
                }
                if let Some(cb) = on_peer_seen {
                    cb(from.id, received_at);
                }
            }
            Ok(outcome)
        }
        SyncUpdateBody::Tombstone { path, from } => {
            let outcome = handle_tombstone(&path, state, watched_dir_canonical).await?;
            if matches!(outcome, ReceiveOutcome::Applied) {
                let entry = PendingEntry::Tombstone {
                    schema_version: PENDING_SCHEMA_VERSION,
                    rel_path: path.clone(),
                    received_at,
                    source_peer: from.id.to_string(),
                };
                if let Err(e) = record_receive(&pending.pending_root, &pending.repo_hash, &entry) {
                    tracing::warn!("pending_log record_receive failed: {e:#}");
                }
                if let Some(cb) = on_peer_seen {
                    cb(from.id, received_at);
                }
            }
            Ok(outcome)
        }
    }
}

/// `state.pending_written` への mark の RAII guard。
///
/// drop で entry を自動削除する。 `commit()` を呼ぶと drop 時の削除を skip
/// する (= 成功 path で「 pending → last_written へ move 」 した直後に呼ぶ)。
///
/// Phase 2 review Medium 4 で導入: 旧 logic では blob fetch / conflict
/// rename / mkdir / tempfile / persist の各 ? 経路で pending_written
/// remove を呼び忘れ、 watcher が「 永遠に書込中扱い 」 で event を skip し続ける
/// state leak が起きていた。 guard で全 error path を網羅。
struct PendingWriteGuard<'a> {
    state: &'a SyncState,
    rel_path: PathBuf,
    committed: bool,
}

impl<'a> PendingWriteGuard<'a> {
    fn new(state: &'a SyncState, rel_path: &str, hash: Hash) -> Self {
        state
            .pending_written
            .lock()
            .expect("pending_written lock")
            .insert(PathBuf::from(rel_path), hash);
        Self {
            state,
            rel_path: PathBuf::from(rel_path),
            committed: false,
        }
    }

    /// 成功時に呼ぶ: drop 時の自動 remove を抑止する (= caller が明示的に
    /// last_written に move する想定)。
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for PendingWriteGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.state
                .pending_written
                .lock()
                .expect("pending_written lock")
                .remove(&self.rel_path);
        }
    }
}

/// Upsert を local に反映: blob fetch → size guard → conflict backup → atomic write。
///
/// `pending_written` mark は `PendingWriteGuard` で管理し、 全 error path で
/// drop により自動 remove される (= state leak 防止)。
///
/// (Endpoint + BlobsProtocol + state + dir + 識別子 4 つ) で arg は多めだが、
/// blob fetch path がそれぞれ独立した依存なので bundle すると別 struct を
/// 作るだけで本質的な簡略化にならない。 ので clippy の `too_many_arguments`
/// は allow する。
#[allow(clippy::too_many_arguments)]
pub async fn handle_upsert(
    rel_path: &str,
    hash: Hash,
    format: BlobFormat,
    from_id: iroh::EndpointId,
    endpoint: &Endpoint,
    blobs: &BlobsProtocol,
    state: &SyncState,
    watched_dir_canonical: &Path,
) -> Result<ReceiveOutcome> {
    let abs = match resolve_safe_path(watched_dir_canonical, rel_path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(path = rel_path, "unsafe path rejected: {e:#}");
            return Ok(ReceiveOutcome::Skipped {
                reason: "unsafe_path",
            });
        }
    };

    // pending_written に mark (= watcher 側で「 同 hash 書込中 」 を区別)。
    // guard が drop されるまでは全 error path で自動 remove される。
    let guard = PendingWriteGuard::new(state, rel_path, hash);

    // blob fetch: local store に未保持なら source peer から download。
    if let Err(e) = ensure_blob_local(endpoint, blobs, from_id, hash, format).await {
        tracing::warn!(path = rel_path, "blob fetch failed: {e:#}");
        return Ok(ReceiveOutcome::Skipped {
            reason: "blob_fetch_failed",
        });
    }

    let size = blob_size(blobs, hash)
        .await
        .map_err(|e| anyhow!("blob size: {e}"))?;
    if size > MAX_FILE_SIZE {
        tracing::warn!(
            path = rel_path,
            size,
            max = MAX_FILE_SIZE,
            "incoming blob exceeds MAX_FILE_SIZE; dropping"
        );
        return Ok(ReceiveOutcome::Skipped {
            reason: "too_large",
        });
    }

    // blob bytes を読み出す (= AsyncRead 経由)
    let bytes = read_blob_bytes(blobs, hash, size).await?;

    // conflict backup
    if let Some(backup) = compute_conflict_backup_path(&abs, &bytes, from_id).await {
        tracing::info!(
            path = rel_path,
            backup = %backup.display(),
            "conflict detected; backing up local"
        );
        std::fs::rename(&abs, &backup)
            .with_context(|| format!("rename {} -> {}", abs.display(), backup.display()))?;
    }

    // parent dir 用意
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }

    // atomic write (sibling tempfile + persist = POSIX rename)
    let parent = abs.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".p2p-sync.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .with_context(|| format!("tempfile_in {}", parent.display()))?;
    use std::io::Write;
    tmp.as_file_mut()
        .write_all(&bytes)
        .context("write tempfile")?;
    tmp.persist(&abs)
        .map_err(|e| anyhow!("persist {}: {}", abs.display(), e.error))?;

    // ここまで来たら成功 = pending_written guard を commit (drop 時の自動 remove 停止)
    // → 続いて手動で last_written に move する (= guard とは別の集合なので)
    guard.commit();
    state
        .pending_written
        .lock()
        .expect("pending_written lock")
        .remove(Path::new(rel_path));
    state
        .last_written
        .lock()
        .expect("last_written lock")
        .insert(PathBuf::from(rel_path), hash);

    tracing::debug!(
        path = rel_path,
        bytes = size,
        peer = %from_id.fmt_short(),
        "applied Upsert"
    );
    Ok(ReceiveOutcome::Applied)
}

/// Tombstone を local に反映: validate → last_removed mark → unlink。
///
/// `last_removed` mark の扱い (= Phase 2 review Medium 5):
/// - **unlink 前** に mark する (= watcher が先に Remove event 拾った race
///   でも skip 判定できるようにする)
/// - unlink 成功 → mark をそのまま残す (= watcher の Remove event が来たら consume)
/// - unlink 失敗 (= 任意 IO error) → marker 撤回 (= retry の余地を残す)
/// - **`NotFound`** → marker 撤回 (= file が元から無いので watcher は Remove event
///   を発火しない → marker を残すと「 後で同 path に正当な local delete が来た
///   とき、 古い marker で skip される 」 stale state になる)
pub async fn handle_tombstone(
    rel_path: &str,
    state: &SyncState,
    watched_dir_canonical: &Path,
) -> Result<ReceiveOutcome> {
    let abs = match resolve_safe_path(watched_dir_canonical, rel_path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(path = rel_path, "unsafe tombstone rejected: {e:#}");
            return Ok(ReceiveOutcome::Skipped {
                reason: "unsafe_path",
            });
        }
    };

    state
        .last_removed
        .lock()
        .expect("last_removed lock")
        .insert(PathBuf::from(rel_path));

    match std::fs::remove_file(&abs) {
        Ok(()) => {
            tracing::debug!(path = rel_path, "applied Tombstone");
            Ok(ReceiveOutcome::Applied)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // file が元から無い: watcher は Remove event を発火しないので、
            // marker を残しても consume されず stale 化する → 撤回する。
            state
                .last_removed
                .lock()
                .expect("last_removed lock")
                .remove(Path::new(rel_path));
            tracing::debug!(
                path = rel_path,
                "tombstone but file already gone; cleared marker"
            );
            Ok(ReceiveOutcome::Applied)
        }
        Err(e) => {
            // remove 失敗時は last_removed mark を撤回 (= retry の余地を残す)
            state
                .last_removed
                .lock()
                .expect("last_removed lock")
                .remove(Path::new(rel_path));
            Err(anyhow!("remove_file {}: {e}", abs.display()))
        }
    }
}

/// blob が local store に無ければ source peer から download する。 既にあれば no-op。
async fn ensure_blob_local(
    endpoint: &Endpoint,
    blobs: &BlobsProtocol,
    source: iroh::EndpointId,
    hash: Hash,
    format: BlobFormat,
) -> Result<()> {
    let content = HashAndFormat { hash, format };
    if let Ok(info) = blobs.remote().local(content).await
        && info.is_complete()
    {
        return Ok(());
    }
    let addr = iroh::EndpointAddr::new(source);
    let conn = endpoint
        .connect(addr, iroh_blobs::ALPN)
        .await
        .map_err(|e| anyhow!("connect to {}: {e}", source.fmt_short()))?;
    blobs
        .remote()
        .fetch(conn, content)
        .await
        .map_err(|e| anyhow!("remote fetch: {e}"))?;
    Ok(())
}

/// `blobs.observe(hash)` で Bitfield::size を取得する。 blob が local store に
/// 完全揃ってなければ partial size を返すが、 caller 側で size guard を行うので
/// best-effort で良い。
async fn blob_size(blobs: &BlobsProtocol, hash: Hash) -> Result<u64> {
    let bitfield = blobs
        .blobs()
        .observe(hash)
        .await
        .map_err(|e| anyhow!("blob observe: {e}"))?;
    Ok(bitfield.size())
}

async fn read_blob_bytes(blobs: &BlobsProtocol, hash: Hash, size: u64) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut reader = blobs.reader(hash);
    let mut out = Vec::with_capacity(size as usize);
    reader
        .read_to_end(&mut out)
        .await
        .context("read blob bytes")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allowlist::{AllowList, PeerInfo};
    use crate::message::{PeerRef, SyncUpdate};
    use crate::runtime::SyncRuntime;
    use crate::state::PendingTracker;
    use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};

    fn fixture_peer_id(byte: u8) -> EndpointId {
        SecretKey::from_bytes(&[byte; 32]).public()
    }

    async fn build_minimal_runtime() -> (SyncRuntime, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[0x55; 32]))
            .bind()
            .await
            .unwrap();
        let rt = SyncRuntime::build(
            endpoint,
            tmp.path(),
            Arc::new(AllowList::empty_strict()),
            Some(&[0u8; 16]),
        )
        .await
        .unwrap();
        (rt, tmp)
    }

    /// per-test isolated PendingTracker。 watch_root 内に置くことで test 終了時に
    /// 一緒に rm される (= system temp の共有 dir を使わない)。
    fn setup_pending(watch_root: &Path) -> Arc<PendingTracker> {
        let pending_root = watch_root.join(".test-pending");
        std::fs::create_dir_all(&pending_root).unwrap();
        Arc::new(PendingTracker {
            pending_root,
            repo_hash: "testrepo".to_string(),
        })
    }

    #[tokio::test]
    async fn dispatch_skips_invalid_wire_format() {
        let (rt, _store_tmp) = build_minimal_runtime().await;
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let allowlist = Arc::new(AllowList::empty_strict());
        let state = SyncState::new();
        let pending = setup_pending(&watch_root);

        let outcome = dispatch_update(
            b"not a sync update",
            rt.endpoint(),
            rt.blobs(),
            &allowlist,
            &state,
            &watch_root,
            &pending,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            ReceiveOutcome::Skipped {
                reason: "invalid_wire_format"
            }
        );
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_skips_unallowed_peer() {
        let (rt, _store_tmp) = build_minimal_runtime().await;
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let allowlist = Arc::new(AllowList::empty_strict()); // 何も入ってない
        let state = SyncState::new();
        let pending = setup_pending(&watch_root);

        let from = PeerRef { id: fixture_peer_id(7) };
        let tombstone = SyncUpdate::tombstone("a.md".into(), from).unwrap();
        let payload = tombstone.to_bytes().unwrap();

        let outcome = dispatch_update(
            &payload,
            rt.endpoint(),
            rt.blobs(),
            &allowlist,
            &state,
            &watch_root,
            &pending,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            ReceiveOutcome::Skipped {
                reason: "peer_not_allowed"
            }
        );
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn handle_tombstone_removes_existing_file() {
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        std::fs::write(watch_root.join("gone.md"), b"bye").unwrap();
        let state = SyncState::new();

        let outcome = handle_tombstone("gone.md", &state, &watch_root)
            .await
            .unwrap();
        assert_eq!(outcome, ReceiveOutcome::Applied);
        assert!(!watch_root.join("gone.md").exists());
        // last_removed に mark されている = watcher の self-loop 防止 hook
        assert!(state
            .last_removed
            .lock()
            .unwrap()
            .contains(Path::new("gone.md")));
    }

    #[tokio::test]
    async fn handle_tombstone_idempotent_when_already_gone() {
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let state = SyncState::new();
        // 不在 file の tombstone → Applied として扱う (= 既に sync 済)
        let outcome = handle_tombstone("never.md", &state, &watch_root)
            .await
            .unwrap();
        assert_eq!(outcome, ReceiveOutcome::Applied);

        // Medium 5 review fix: marker は撤回されている (= 後で正当な local delete
        // が来たとき watcher が誤 skip しない)
        assert!(
            !state
                .last_removed
                .lock()
                .unwrap()
                .contains(Path::new("never.md")),
            "marker must be cleared when file did not exist"
        );
    }

    #[tokio::test]
    async fn handle_tombstone_rejects_unsafe_path() {
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let state = SyncState::new();
        let outcome = handle_tombstone("../escape.md", &state, &watch_root)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReceiveOutcome::Skipped {
                reason: "unsafe_path"
            }
        );
    }

    /// Medium 4 review fix: blob fetch 失敗で early return しても、 pending_written
    /// に entry が leak しない (= PendingWriteGuard の Drop が cleanup する)。
    #[tokio::test]
    async fn handle_upsert_clears_pending_written_on_fetch_failure() {
        let (rt, _store_tmp) = build_minimal_runtime().await;
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let state = SyncState::new();

        // 存在しない (= local 未保持) hash + 接続不可能 fake peer。
        // ensure_blob_local が必ず失敗する → guard が drop で remove する。
        let fake_hash = Hash::new(b"never-added");
        let fake_peer = fixture_peer_id(0x99);

        let outcome = handle_upsert(
            "ghost.md",
            fake_hash,
            BlobFormat::Raw,
            fake_peer,
            rt.endpoint(),
            rt.blobs(),
            &state,
            &watch_root,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, ReceiveOutcome::Skipped { reason } if reason == "blob_fetch_failed"),
            "expected fetch failure, got {outcome:?}"
        );

        // pending_written に「 書込中マーク 」 が leak していない
        let leaked = {
            let pw = state.pending_written.lock().unwrap();
            pw.contains_key(Path::new("ghost.md"))
        };
        assert!(
            !leaked,
            "PendingWriteGuard must remove entry on early return"
        );
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn handle_upsert_self_round_trip_writes_file() {
        let (rt, _store_tmp) = build_minimal_runtime().await;
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let state = SyncState::new();

        // 自分の store に blob を予め add しておき、 fetch path は no-op で通す。
        let tag = rt.blobs().add_bytes(b"hello world".to_vec()).await.unwrap();
        let hash = tag.hash;

        // handle_upsert は from_id を fetch source として使うが、 既に local に
        // ある blob なら ensure_blob_local が `is_complete` で early return。
        let from = rt.endpoint().id();
        let outcome = handle_upsert(
            "incoming.md",
            hash,
            BlobFormat::Raw,
            from,
            rt.endpoint(),
            rt.blobs(),
            &state,
            &watch_root,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ReceiveOutcome::Applied);

        let got = std::fs::read(watch_root.join("incoming.md")).unwrap();
        assert_eq!(got, b"hello world");
        let stored_hash = {
            let last = state.last_written.lock().unwrap();
            last.get(Path::new("incoming.md")).copied()
        };
        assert_eq!(stored_hash, Some(hash));

        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_upsert_full_round_trip_with_allowlist() {
        let (rt, _store_tmp) = build_minimal_runtime().await;
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watch_root = watch_tmp.path().canonicalize().unwrap();
        let state = SyncState::new();
        let pending = setup_pending(&watch_root);

        // self_peer を allowlist に入れる
        let allowlist = Arc::new(AllowList::empty_strict());
        let from_id = rt.endpoint().id();
        allowlist.add(from_id, PeerInfo::new(Some("self".into()), 0));

        // blob を local store に置く (= fetch path noop)
        let tag = rt.blobs().add_bytes(b"payload".to_vec()).await.unwrap();
        let update =
            SyncUpdate::upsert("dispatched.md".into(), tag.hash, BlobFormat::Raw, PeerRef { id: from_id })
                .unwrap();
        let payload = update.to_bytes().unwrap();

        // L4 review fix: data-plane 成立 callback も検証する
        let seen: Arc<std::sync::Mutex<Vec<(iroh::EndpointId, i64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let cb: PeerSeenCallback = Arc::new(move |id, t| {
            seen2.lock().unwrap().push((id, t));
        });

        let outcome = dispatch_update(
            &payload,
            rt.endpoint(),
            rt.blobs(),
            &allowlist,
            &state,
            &watch_root,
            &pending,
            Some(&cb),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ReceiveOutcome::Applied);
        assert_eq!(
            std::fs::read(watch_root.join("dispatched.md")).unwrap(),
            b"payload"
        );
        // peer_seen callback が Upsert 成功で 1 回呼ばれる
        let seen_vec = seen.lock().unwrap().clone();
        assert_eq!(seen_vec.len(), 1, "callback should fire once");
        assert_eq!(seen_vec[0].0, from_id);
        // pending_log entry も 1 件できた
        let entries = std::fs::read_dir(pending.pending_root.join(&pending.repo_hash))
            .unwrap()
            .count();
        assert_eq!(entries, 1);

        rt.shutdown().await.unwrap();
    }
}
