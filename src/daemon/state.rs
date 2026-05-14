//! `DaemonState`: daemon process が hold する shared state (= RPC handler や
//! send / receive loop から参照する読み取り中心の context)。
//!
//! design.md §3.1 / §3.2 / §5.1 に従い、 以下を 1 struct に集約:
//!
//! - **path 系**: `watched_dir_canonical`, `allowlist_path`, `socket_path`, etc.
//! - **state 系**: `Arc<AllowList>`, `Arc<PendingTracker>`, `SyncState` (Clone 可)
//! - **runtime 系**: `Endpoint`, `BlobsProtocol`, gossip `GossipSender` (= split 後)
//! - **時刻 系**: `start_instant` (uptime 計算用)、 `last_seen_at` map
//! - **runtime status**: `group_initialized` (= folder_secret あり) と
//!   `gossip_subscribed` (= 現 runtime が topic を持っている) の 2 flag
//!
//! 「 dispatch handler が必要とする最小集合 」 で持つ。 send / receive loop は
//! 別に loop handle を持つ (= daemon main で spawn 後 hold)。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use iroh::{Endpoint, EndpointId};
use iroh_blobs::BlobsProtocol;
use iroh_gossip::api::GossipSender;

use crate::allowlist::AllowList;
use crate::state::{PendingTracker, SyncState};

/// `sync.list-peers` の `last_seen_at` を保持する map (= data-plane 成功時刻)。
///
/// design §3.5 に従い、 NeighborUp などの control-plane event は **入れない**。
/// 入れる契機は次の 3 つだけ:
/// - blob fetch 成功 (= receive で Upsert を atomic write した時)
/// - blob serve 成功 (= AllowlistBlobs::accept で provider session が終了した時)
/// - Tombstone 受信成功 (= handle_tombstone Applied)
///
/// 値は Unix epoch 秒。 `Option<i64>` ではなく entry の存否で「 一度も成立してない 」
/// を表現する (= `sync.list-peers` で missing なら null として返す)。
pub type LastSeenMap = Arc<Mutex<std::collections::HashMap<EndpointId, i64>>>;

/// daemon の起動 path 一式。 RPC handler から参照する。
#[derive(Debug, Clone)]
pub struct DaemonPaths {
    pub watched_dir_canonical: PathBuf,
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
    pub allowlist_path: PathBuf,
    pub folder_secret_path: PathBuf,
    pub key_path: PathBuf,
    pub blobs_dir: PathBuf,
    pub log_path: PathBuf,
    /// gossip bootstrap peer addresses (= Phase 3 review H1)。
    pub bootstrap_peers_path: PathBuf,
}

/// 「 folder_secret は持っているか 」 「 gossip topic は subscribe しているか 」 を
/// 表現する小さな enum。 `restart_required = group_initialized && !gossip_subscribed`
/// を導く根拠 (= design §3.1 HealthInfoDynamic)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeStatus {
    /// folder_secret 未持ち → mesh に居ない (= invite / accept 待ち)
    Uninitialized,
    /// folder_secret 持ちだが gossip subscribe してない (= daemon 再起動が必要)
    InitializedButNotSubscribed,
    /// folder_secret + gossip subscribe 揃い (= 正常稼働)
    Active,
}

impl RuntimeStatus {
    pub fn group_initialized(&self) -> bool {
        !matches!(self, Self::Uninitialized)
    }

    pub fn gossip_subscribed(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn restart_required(&self) -> bool {
        matches!(self, Self::InitializedButNotSubscribed)
    }
}

/// runtime_status + gossip_sender を **一体管理** する内部 state。
///
/// invariant (= Phase 3 review M3):
/// - `RuntimeStatus::Active`           ⇔ `gossip_sender: Some(_)`
/// - `RuntimeStatus::Uninitialized`     ⇔ `gossip_sender: None`
/// - `RuntimeStatus::InitializedButNotSubscribed` ⇔ `gossip_sender: None`
///
/// この invariant を constructor / setter で型 level + 1 lock 内で保証する。
/// 個別 setter (= 旧 set_runtime_status / set_gossip_sender) は廃止し、
/// 「 状態 transition の意味 」 を表す method 経由のみ受け付ける。
#[derive(Debug)]
struct GossipRuntimeInner {
    status: RuntimeStatus,
    sender: Option<GossipSender>,
}

impl GossipRuntimeInner {
    fn assert_invariant(&self) {
        let expected_some = matches!(self.status, RuntimeStatus::Active);
        debug_assert_eq!(
            self.sender.is_some(),
            expected_some,
            "RuntimeStatus = {:?} but gossip_sender.is_some() = {}",
            self.status,
            self.sender.is_some()
        );
    }
}

/// daemon 全体で共有する state。
///
/// **Clone 不可** (= per-process singleton)。 RPC handler には `Arc<DaemonState>`
/// 経由で配布する。
#[derive(Debug)]
pub struct DaemonState {
    pub paths: DaemonPaths,
    pub allowlist: Arc<AllowList>,
    pub pending: Arc<PendingTracker>,
    pub sync_state: SyncState,

    /// 自分の EndpointId (= `endpoint.id()` のキャッシュ、 RPC で頻繁に返すため)。
    pub self_endpoint_id: EndpointId,
    /// iroh stack 本体。
    pub endpoint: Endpoint,
    /// blob store。 send_file / handle_upsert から共有。
    pub blobs: BlobsProtocol,

    /// runtime_status と gossip_sender を一体管理 (= 不整合 invariant 防止)。
    /// 直接 access はできず、 `current_runtime_status` / `enter_active` 等の
    /// transition method 経由のみ。
    gossip_runtime: Arc<Mutex<GossipRuntimeInner>>,

    /// uptime 計算用。
    pub start_instant: Instant,
    /// peer → 直近 data-plane 成立 epoch 秒。
    pub last_seen_at: LastSeenMap,
}

impl DaemonState {
    /// 全 field を渡して構築する。 caller (= daemon main) が iroh stack を
    /// 組み立てた後で 1 度だけ呼ぶ想定。
    ///
    /// `runtime_status` と `gossip_sender` の組合せは invariant を満たす必要が
    /// あり、 違反すると panic する (= caller の bug)。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        paths: DaemonPaths,
        allowlist: Arc<AllowList>,
        pending: Arc<PendingTracker>,
        sync_state: SyncState,
        endpoint: Endpoint,
        blobs: BlobsProtocol,
        gossip_sender: Option<GossipSender>,
        runtime_status: RuntimeStatus,
    ) -> Self {
        let self_endpoint_id = endpoint.id();
        let inner = GossipRuntimeInner {
            status: runtime_status,
            sender: gossip_sender,
        };
        // 構築時 invariant を panic で検査 (= test debug build + release の両方で守る)
        let expected_some = matches!(inner.status, RuntimeStatus::Active);
        assert_eq!(
            inner.sender.is_some(),
            expected_some,
            "DaemonState::new invariant violation: RuntimeStatus = {:?} requires \
             gossip_sender.is_some() = {expected_some}, but got is_some() = {}",
            inner.status,
            inner.sender.is_some()
        );
        Self {
            paths,
            allowlist,
            pending,
            sync_state,
            self_endpoint_id,
            endpoint,
            blobs,
            gossip_runtime: Arc::new(Mutex::new(inner)),
            start_instant: Instant::now(),
            last_seen_at: Arc::new(Mutex::new(Default::default())),
        }
    }

    /// uptime 秒 (= sync.status / health-check 用)。
    pub fn uptime_secs(&self) -> u64 {
        self.start_instant.elapsed().as_secs()
    }

    /// data-plane 成立を記録 (= design §3.5 の唯一の入り口)。 receive 側で blob
    /// 書込 / tombstone 適用成功時、 send 側で blob serve 完了時に呼ぶ。
    pub fn mark_peer_seen(&self, peer: EndpointId, now_epoch_secs: i64) {
        let mut g = self.last_seen_at.lock().expect("last_seen lock");
        let cur = g.get(&peer).copied().unwrap_or(0);
        if now_epoch_secs > cur {
            g.insert(peer, now_epoch_secs);
        }
    }

    /// 現在の runtime_status を read-only で参照 (lock を握って Copy で返す)。
    pub fn current_runtime_status(&self) -> RuntimeStatus {
        let g = self.gossip_runtime.lock().expect("gossip_runtime lock");
        g.status
    }

    /// folder_secret を adopt したが gossip 未 subscribe な状態に遷移。
    /// invite / accept-invite 経路で呼ぶ (= restart_required = true の根拠)。
    /// gossip_sender は None のまま (= 起動時 subscribe してなかったので Some
    /// であるはずがない、 ただし冪等性のため明示的に None にする)。
    pub fn enter_initialized_but_not_subscribed(&self) {
        let mut g = self.gossip_runtime.lock().expect("gossip_runtime lock");
        g.status = RuntimeStatus::InitializedButNotSubscribed;
        g.sender = None;
        g.assert_invariant();
    }

    /// gossip subscribe 完了で Active 状態に遷移。 sender は必須引数 (= None
    /// での Active は invariant 違反なので型 level で防ぐ)。
    pub fn enter_active(&self, sender: GossipSender) {
        let mut g = self.gossip_runtime.lock().expect("gossip_runtime lock");
        g.status = RuntimeStatus::Active;
        g.sender = Some(sender);
        g.assert_invariant();
    }

    /// 強制 Uninitialized 化 (= folder-secret.bin 手動削除後の reset 用)。
    /// テスト用 helper でもある。
    pub fn enter_uninitialized(&self) {
        let mut g = self.gossip_runtime.lock().expect("gossip_runtime lock");
        g.status = RuntimeStatus::Uninitialized;
        g.sender = None;
        g.assert_invariant();
    }

    /// `gossip_sender` の clone を返す (= broadcast / Tombstone 送信用)。
    /// Active 以外なら None。
    pub fn gossip_sender_cloned(&self) -> Option<GossipSender> {
        let g = self.gossip_runtime.lock().expect("gossip_runtime lock");
        g.sender.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PeerRef;
    use crate::runtime::SyncRuntime;
    use crate::state::compute_repo_hash;
    use iroh::SecretKey;

    fn fixture_paths(tmp: &std::path::Path, watched: &std::path::Path) -> DaemonPaths {
        DaemonPaths {
            watched_dir_canonical: watched.to_path_buf(),
            socket_path: tmp.join("daemon.sock"),
            lock_path: tmp.join("daemon.lock"),
            allowlist_path: tmp.join("allowlist.json"),
            folder_secret_path: tmp.join("folder-secret.bin"),
            key_path: tmp.join("endpoint.key"),
            blobs_dir: tmp.join("blobs"),
            log_path: tmp.join("p2p-dir-sync.log"),
            bootstrap_peers_path: tmp.join("bootstrap-peers.json"),
        }
    }

    #[test]
    fn runtime_status_transitions() {
        let u = RuntimeStatus::Uninitialized;
        assert!(!u.group_initialized());
        assert!(!u.gossip_subscribed());
        assert!(!u.restart_required());

        let i = RuntimeStatus::InitializedButNotSubscribed;
        assert!(i.group_initialized());
        assert!(!i.gossip_subscribed());
        assert!(i.restart_required());

        let a = RuntimeStatus::Active;
        assert!(a.group_initialized());
        assert!(a.gossip_subscribed());
        assert!(!a.restart_required());
    }

    /// DaemonState の構築 = real Endpoint + FsStore + Gossip 必要なので tokio test。
    /// 起動直後 uptime / runtime_status / mark_peer_seen 動作を確認。
    #[tokio::test]
    async fn daemon_state_construct_and_mutate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watched = watch_tmp.path().canonicalize().unwrap();
        let paths = fixture_paths(tmp.path(), &watched);

        let allowlist = Arc::new(AllowList::empty_strict());
        let pending = Arc::new(PendingTracker {
            pending_root: tmp.path().join("pending"),
            repo_hash: compute_repo_hash(&watched),
        });
        std::fs::create_dir_all(&pending.pending_root).unwrap();

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[0x21; 32]))
            .bind()
            .await
            .unwrap();
        let mut rt = SyncRuntime::build(
            endpoint.clone(),
            &paths.blobs_dir,
            allowlist.clone(),
            Some(&[0u8; 16]),
            Vec::new(),
        )
        .await
        .unwrap();
        let topic = rt.take_topic().unwrap();
        let (gossip_sender, _receiver) = topic.split();

        let st = DaemonState::new(
            paths,
            allowlist,
            pending,
            SyncState::new(),
            rt.endpoint().clone(),
            rt.blobs().clone(),
            Some(gossip_sender),
            RuntimeStatus::Active,
        );

        assert_eq!(st.current_runtime_status(), RuntimeStatus::Active);
        assert!(st.uptime_secs() < 5, "uptime should be ~0 just after build");

        // mark_peer_seen が monotonic に animate する
        let p = PeerRef { id: endpoint.id() }.id;
        st.mark_peer_seen(p, 100);
        st.mark_peer_seen(p, 50); // older event は無視
        let seen = st.last_seen_at.lock().unwrap().get(&p).copied();
        assert_eq!(seen, Some(100));

        st.enter_initialized_but_not_subscribed();
        assert!(st.current_runtime_status().restart_required());
        assert!(st.gossip_sender_cloned().is_none(), "Initialized は sender None");

        rt.shutdown().await.unwrap();
    }

    /// Phase 3 review M3: status と sender の invariant 違反は constructor で panic。
    #[tokio::test]
    #[should_panic(expected = "invariant violation")]
    async fn daemon_state_new_panics_on_active_without_sender() {
        let tmp = tempfile::TempDir::new().unwrap();
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watched = watch_tmp.path().canonicalize().unwrap();
        let paths = fixture_paths(tmp.path(), &watched);

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[0x99; 32]))
            .bind()
            .await
            .unwrap();
        let allowlist = Arc::new(AllowList::empty_strict());
        let pending = Arc::new(PendingTracker {
            pending_root: tmp.path().join("pending"),
            repo_hash: compute_repo_hash(&watched),
        });
        std::fs::create_dir_all(&pending.pending_root).unwrap();
        let mut rt = SyncRuntime::build(
            endpoint.clone(),
            &paths.blobs_dir,
            allowlist.clone(),
            None,
            Vec::new(),
        )
        .await
        .unwrap();
        let _ = rt.take_topic();
        // Active + sender=None は invariant 違反で panic するはず
        DaemonState::new(
            paths,
            allowlist,
            pending,
            SyncState::new(),
            rt.endpoint().clone(),
            rt.blobs().clone(),
            None,
            RuntimeStatus::Active,
        );
    }

    #[test]
    fn last_seen_map_is_thread_safe_arc_mutex() {
        // 「 Arc<Mutex<...>> なので clone して並行 thread から mutate できる 」 を
        // 型 level で表明する smoke test。
        let m: LastSeenMap = Arc::new(Mutex::new(Default::default()));
        let c = m.clone();
        c.lock().unwrap().insert(
            SecretKey::from_bytes(&[1; 32]).public(),
            42,
        );
        assert_eq!(m.lock().unwrap().len(), 1);
    }
}
