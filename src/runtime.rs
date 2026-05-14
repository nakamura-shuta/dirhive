//! Iroh stack の組み立て (= Endpoint + FsStore + BlobsProtocol + Gossip + Router)
//! と `derive_topic_id` (= folder_secret → TopicId)。
//!
//! design.md §3.1 / §4.3 / §5.1 step 9 を実装する module。
//!
//! ## SyncRuntime のライフサイクル
//!
//! ```text
//! 1. daemon 起動:
//!    let store    = FsStore::load(blobs_dir).await?
//!    let blobs    = BlobsProtocol::new(&store, None)
//!    let wrapped  = AllowlistBlobs::new(blobs.clone(), allowlist)
//!    let gossip   = Gossip::builder().spawn(endpoint.clone())
//!    let router   = Router::builder(endpoint).accept(BLOB_ALPN, wrapped)
//!                                            .accept(GOSSIP_ALPN, gossip.clone())
//!                                            .spawn()
//!    SyncRuntime::build(...) はこれ一式を組み立てて Hold する。
//!
//! 2. folder_secret あり: derive_topic_id で subscribe、 GossipTopic を hold
//!    folder_secret なし: subscribe skip (= group_initialized = false 状態)
//!
//! 3. daemon 停止 (§5.4): router.shutdown().await → endpoint.close().await
//! ```

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::address_lookup::MemoryLookup;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::BlobsProtocol;
use iroh_blobs::store::fs::FsStore;
use iroh_gossip::api::GossipTopic;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;

use crate::allowlist::AllowList;
use crate::allowlist_blobs::{AllowlistBlobs, BlobServeCallback};

/// `derive_topic_id` で使う固定 prefix (= protocol versioning + domain separation)。
/// `\0` までを 1 つの byte string 扱いするので `b"p2p-dir-sync/v1/topic\0"` を
/// そのまま hash input にする。
pub const TOPIC_DOMAIN: &[u8] = b"p2p-dir-sync/v1/topic\0";

/// folder_secret から TopicId を導出する (= design.md §4.3)。
///
/// `BLAKE3(TOPIC_DOMAIN || folder_secret)` の 32 byte を TopicId とする。
///
/// 性質:
/// - **第三者導出不可**: folder_secret (16 byte entropy) を知らない peer は
///   topic_id を計算できないので gossip mesh に join できない
/// - **deterministic**: 同 folder_secret → 同 topic_id、 3-peer chain でも 1 mesh
/// - **prefix で v2 拡張可**: 将来 prefix を `v2/` に bump すれば旧 topic と
///   collide しない (= protocol version skew 検知)
pub fn derive_topic_id(folder_secret: &[u8; 16]) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(TOPIC_DOMAIN);
    hasher.update(folder_secret);
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}

/// Iroh stack を hold する runtime。 build 後 drop で Router / Gossip が
/// stop する (= AbortOnDrop 系の挙動)。 graceful shutdown が必要なら
/// `shutdown` を明示的に呼ぶ。
pub struct SyncRuntime {
    router: Router,
    gossip: Gossip,
    blobs: BlobsProtocol,
    /// `Some` なら group_initialized = true で gossip topic を subscribe 済。
    /// `None` なら folder_secret 未確定 (= invite / accept 前) で gossip 上で
    /// idle。 health-check で `gossip_subscribed = false` を返す根拠。
    topic: Option<GossipTopic>,
    topic_id: Option<TopicId>,
    #[allow(dead_code)]
    store: FsStore,
}

impl std::fmt::Debug for SyncRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRuntime")
            .field("gossip_subscribed", &self.topic.is_some())
            .field("topic_id", &self.topic_id.map(|t| t.fmt_short()))
            .finish_non_exhaustive()
    }
}

impl SyncRuntime {
    /// Iroh stack を組み立てて起動する。
    ///
    /// `folder_secret = None` (= group_initialized = false) でも build は
    /// 成功し、 Router は両 ALPN を accept する。 gossip topic だけ subscribe
    /// 未済になる (= mesh に居ない)。 invite / accept で folder_secret 確定
    /// 後の `daemon` 再起動で改めて build し直す想定 (= design §3.4)。
    ///
    /// `bootstrap_peers` (= Phase 3 review H1): gossip.subscribe に渡す既知
    /// peer 一覧。 invite/accept-invite 経由で得た inviter の EndpointAddr を
    /// 永続化したものを load して渡す。 空でも build は通るが、 そのとき他
    /// peer と接続が成立せず file sync が動かない。
    pub async fn build(
        endpoint: Endpoint,
        blobs_dir: &Path,
        allowlist: Arc<AllowList>,
        folder_secret: Option<&[u8; 16]>,
        bootstrap_peers: Vec<EndpointAddr>,
    ) -> Result<Self> {
        Self::build_with_serve_callback(
            endpoint,
            blobs_dir,
            allowlist,
            folder_secret,
            bootstrap_peers,
            None,
        )
        .await
    }

    /// `on_blob_serve_success` callback 付きの builder (= daemon main 用、
    /// blob serve 成功時に DaemonState::mark_peer_seen を呼ぶ hook を渡す)。
    pub async fn build_with_serve_callback(
        endpoint: Endpoint,
        blobs_dir: &Path,
        allowlist: Arc<AllowList>,
        folder_secret: Option<&[u8; 16]>,
        bootstrap_peers: Vec<EndpointAddr>,
        on_blob_serve_success: Option<BlobServeCallback>,
    ) -> Result<Self> {
        let store = FsStore::load(blobs_dir)
            .await
            .with_context(|| format!("FsStore::load {}", blobs_dir.display()))?;
        let blobs = BlobsProtocol::new(&store, None);
        let allow_wrapped = match on_blob_serve_success {
            Some(cb) => AllowlistBlobs::with_serve_callback(blobs.clone(), allowlist, cb),
            None => AllowlistBlobs::new(blobs.clone(), allowlist),
        };

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, allow_wrapped)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        let (topic, topic_id) = if let Some(secret) = folder_secret {
            let id = derive_topic_id(secret);
            // bootstrap peers の EndpointAddr を endpoint の address_lookup に
            // 教え込む (= relay URL / direct addrs を MemoryLookup で登録)。
            // これがないと gossip dialer は EndpointId だけでは peer を reach
            // できず、 subscribe(topic, ids) を渡しても mesh が成立しない。
            if !bootstrap_peers.is_empty() {
                let memory = MemoryLookup::from_endpoint_info(bootstrap_peers.clone());
                match endpoint.address_lookup() {
                    Ok(lookups) => lookups.add(memory),
                    Err(e) => {
                        tracing::warn!(
                            "endpoint.address_lookup() unavailable, bootstrap MemoryLookup not registered: {e}"
                        );
                    }
                }
            }
            let bootstrap_ids: Vec<_> = bootstrap_peers.iter().map(|a| a.id).collect();
            let t = gossip
                .subscribe(id, bootstrap_ids)
                .await
                .map_err(|e| anyhow::anyhow!("gossip subscribe: {e}"))?;
            (Some(t), Some(id))
        } else {
            (None, None)
        };

        Ok(Self {
            router,
            gossip,
            blobs,
            topic,
            topic_id,
            store,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    pub fn blobs(&self) -> &BlobsProtocol {
        &self.blobs
    }

    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    /// 現在 subscribe 中の topic (= group_initialized なら Some)。
    pub fn topic_id(&self) -> Option<TopicId> {
        self.topic_id
    }

    /// `health-check` 用: gossip topic を subscribe 中か。
    pub fn gossip_subscribed(&self) -> bool {
        self.topic.is_some()
    }

    /// 取り出して send.rs / receive.rs から使う handle。
    /// `take` するので 1 度しか呼べない (= sender / receiver split 後の流用に向く)。
    pub fn take_topic(&mut self) -> Option<GossipTopic> {
        self.topic.take()
    }

    /// graceful shutdown (= design.md §5.4 step 4)。
    /// Router → Gossip → Endpoint::close の順で停止する。
    /// `Router::shutdown` が `ProtocolHandler::shutdown` を呼ぶので、
    /// `BlobsProtocol::shutdown` (= store.shutdown) も走る。
    pub async fn shutdown(self) -> Result<()> {
        // GossipTopic を先に drop して subscribe を leave させる
        drop(self.topic);
        self.router
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("router shutdown: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, SecretKey, endpoint::presets};

    #[test]
    fn topic_domain_is_versioned() {
        assert_eq!(TOPIC_DOMAIN, b"p2p-dir-sync/v1/topic\0");
    }

    #[test]
    fn derive_topic_id_deterministic() {
        let secret = [7u8; 16];
        let a = derive_topic_id(&secret);
        let b = derive_topic_id(&secret);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_topic_id_differs_by_secret() {
        let a = derive_topic_id(&[0u8; 16]);
        let b = derive_topic_id(&[1u8; 16]);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_topic_id_matches_manual_blake3() {
        let secret = [0xABu8; 16];
        let expected = {
            let mut h = blake3::Hasher::new();
            h.update(b"p2p-dir-sync/v1/topic\0");
            h.update(&secret);
            *h.finalize().as_bytes()
        };
        let got = derive_topic_id(&secret);
        assert_eq!(got.as_bytes(), &expected);
    }

    #[test]
    fn topic_domain_byte_layout() {
        // 改ざんによる silent topic-id 変化を防ぐため、 prefix の bytes を
        // 直接 assert する (= 「`v1/`」を消す PR を CI で落とす役割)。
        let bytes = b"p2p-dir-sync/v1/topic\0";
        assert_eq!(bytes.len(), 22);
        assert_eq!(bytes[bytes.len() - 1], 0);
        assert!(bytes.windows(3).any(|w| w == b"v1/"));
    }

    /// build は real Endpoint を要する重 test。 `presets::Minimal` で online
    /// 接続せず local bind だけする。
    #[tokio::test]
    async fn build_without_folder_secret_skips_subscribe() {
        let tmp = tempfile::TempDir::new().unwrap();
        let secret = SecretKey::from_bytes(&[1u8; 32]);
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret)
            .bind()
            .await
            .expect("endpoint bind");

        let allowlist = Arc::new(AllowList::empty_strict());
        let rt = SyncRuntime::build(endpoint, tmp.path(), allowlist, None, Vec::new())
            .await
            .expect("runtime build");

        assert!(!rt.gossip_subscribed());
        assert!(rt.topic_id().is_none());

        rt.shutdown().await.expect("graceful shutdown");
    }

    #[tokio::test]
    async fn build_with_folder_secret_subscribes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let secret = SecretKey::from_bytes(&[2u8; 32]);
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret)
            .bind()
            .await
            .expect("endpoint bind");

        let allowlist = Arc::new(AllowList::empty_strict());
        let folder_secret = [0x42u8; 16];
        let rt = SyncRuntime::build(endpoint, tmp.path(), allowlist, Some(&folder_secret), Vec::new())
            .await
            .expect("runtime build");

        assert!(rt.gossip_subscribed());
        assert_eq!(rt.topic_id(), Some(derive_topic_id(&folder_secret)));

        rt.shutdown().await.expect("graceful shutdown");
    }

    #[tokio::test]
    async fn take_topic_is_one_shot() {
        let tmp = tempfile::TempDir::new().unwrap();
        let secret = SecretKey::from_bytes(&[3u8; 32]);
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(secret)
            .bind()
            .await
            .expect("endpoint bind");

        let allowlist = Arc::new(AllowList::empty_strict());
        let mut rt = SyncRuntime::build(endpoint, tmp.path(), allowlist, Some(&[0u8; 16]), Vec::new())
            .await
            .expect("runtime build");

        let first = rt.take_topic();
        let second = rt.take_topic();
        assert!(first.is_some());
        assert!(second.is_none(), "take_topic must not yield twice");
        // GossipTopic を握ったまま runtime を drop しないため、 ここで drop。
        drop(first);
        rt.shutdown().await.expect("graceful shutdown");
    }
}
