//! `BlobsProtocol` を allowlist で wrap し、未許可 peer の ALPN connection を
//! reject する (= design.md §6.2 「blob ALPN accept 制限」)。
//!
//! 役割:
//! - `iroh-blobs` の `BlobsProtocol` を `AllowlistBlobs` で包む
//! - accept 側で `conn.remote_id()` を `AllowList::contains` で check
//! - 未許可なら `conn.close(VarInt::from_u32(401), b"unauthorized")` で reject
//!   (= peer 側で `ConnectionError::ApplicationClosed(401)` が観測される)
//! - 許可済なら `BlobsProtocol::accept` に委譲
//!
//! **これは accept side**: 自分が blob を serve するときに「相手は誰?」を
//! 見るための wrapper。送信 (= `store.get(...)`) 側は別経路で、自分から
//! 接続する peer は invite / accept フローで既に許可済になっている前提。
//!
//! self-loop / mesh 全体での bilateral allowlist は design §3.4 を参照。
//! 片側が allow-peer を呼んでない場合、 こちらの blob ALPN は通るが、
//! 相手側で同じ wrapper が動いていて reject するため両方向止まる。

use std::sync::Arc;
use std::time::SystemTime;

use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh_base::EndpointId;
use iroh_blobs::BlobsProtocol;

use crate::allowlist::AllowList;

/// blob serve 成功時に呼ぶ callback (= Phase 3 review M3 / L4 と整合)。
///
/// `AllowlistBlobs::accept` が `inner.accept(conn).await` 完了で `Ok(())` を
/// 返した直後に呼ぶ。 引数は (`remote_id`, epoch 秒)。 daemon main から
/// `DaemonState::mark_peer_seen` に繋いで `sync.list-peers.last_seen_at` を
/// 更新する。
pub type BlobServeCallback = Arc<dyn Fn(EndpointId, i64) + Send + Sync>;

/// reject 時に peer に送る application error code (= HTTP 401 を borrow した値)。
/// peer 側で `ConnectionError::ApplicationClosed { error_code: 401, .. }` として
/// 観測できる。peer 側 log で「許可されていない」 と区別しやすくする目的。
pub const REJECT_CODE_UNAUTHORIZED: u32 = 401;

/// `BlobsProtocol` を allowlist 認可 layer で wrap する。
#[derive(Clone)]
pub struct AllowlistBlobs {
    inner: BlobsProtocol,
    allowlist: Arc<AllowList>,
    /// blob serve 成功時に呼ぶ callback (= mark_peer_seen 連携)。
    on_serve_success: Option<BlobServeCallback>,
}

impl std::fmt::Debug for AllowlistBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllowlistBlobs")
            .field("has_on_serve_success", &self.on_serve_success.is_some())
            .finish_non_exhaustive()
    }
}

impl AllowlistBlobs {
    pub fn new(inner: BlobsProtocol, allowlist: Arc<AllowList>) -> Self {
        Self {
            inner,
            allowlist,
            on_serve_success: None,
        }
    }

    /// callback 付きで構築 (= daemon main 用)。
    pub fn with_serve_callback(
        inner: BlobsProtocol,
        allowlist: Arc<AllowList>,
        on_serve_success: BlobServeCallback,
    ) -> Self {
        Self {
            inner,
            allowlist,
            on_serve_success: Some(on_serve_success),
        }
    }

    /// peer が許可されているか (= `open_all` or 明示登録)。
    /// `accept` 経路と test の両方から使う pure function。
    pub fn is_authorized(&self, remote_id: &EndpointId) -> bool {
        self.allowlist.contains(remote_id)
    }

    /// 内部の `BlobsProtocol` を露出 (= main 側で `store()` を呼ぶため)。
    pub fn inner(&self) -> &BlobsProtocol {
        &self.inner
    }
}

impl ProtocolHandler for AllowlistBlobs {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote_id = conn.remote_id();
        if !self.is_authorized(&remote_id) {
            tracing::warn!(
                peer = %remote_id.fmt_short(),
                "blob ALPN connection rejected: peer not in allowlist"
            );
            conn.close(
                VarInt::from_u32(REJECT_CODE_UNAUTHORIZED),
                b"unauthorized",
            );
            return Ok(());
        }
        tracing::debug!(
            peer = %remote_id.fmt_short(),
            "blob ALPN connection accepted"
        );
        let result = self.inner.accept(conn).await;
        // serve 完了 (= provider session 終了) → mark_peer_seen hook
        if result.is_ok()
            && let Some(cb) = &self.on_serve_success
        {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            cb(remote_id, now);
        }
        result
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allowlist::PeerInfo;
    use iroh::SecretKey;

    fn fixture_id(byte: u8) -> EndpointId {
        SecretKey::from_bytes(&[byte; 32]).public()
    }

    /// `AllowlistBlobs` 構築には `BlobsProtocol` (= 内部に Store 必要) が
    /// 必要だが、 unit test では `is_authorized` の policy のみ検証したい。
    /// ので `BlobsProtocol` を構築せずに済む形で direct test する helper。
    fn check_authorized(allowlist: Arc<AllowList>, id: EndpointId) -> bool {
        allowlist.contains(&id)
    }

    #[test]
    fn reject_code_is_401() {
        assert_eq!(REJECT_CODE_UNAUTHORIZED, 401);
    }

    #[test]
    fn unknown_peer_not_authorized_in_strict_empty() {
        let al = Arc::new(AllowList::empty_strict());
        assert!(!check_authorized(al, fixture_id(1)));
    }

    #[test]
    fn explicitly_added_peer_is_authorized() {
        let al = AllowList::empty_strict();
        let id = fixture_id(2);
        al.add(id, PeerInfo::new(Some("bob".into()), 0));
        assert!(check_authorized(Arc::new(al), id));
    }

    #[test]
    fn open_all_authorizes_any_peer() {
        let al = Arc::new(AllowList::open_all());
        assert!(check_authorized(al.clone(), fixture_id(3)));
        assert!(check_authorized(al, fixture_id(99)));
    }

    #[test]
    fn revoked_peer_no_longer_authorized() {
        let al = AllowList::empty_strict();
        let id = fixture_id(4);
        al.add(id, PeerInfo::new(None, 0));
        assert!(al.contains(&id));
        al.remove(&id);
        assert!(!check_authorized(Arc::new(al), id));
    }

    /// `is_authorized` は `AllowList::contains` の薄い wrap。 直接 method 経由
    /// でも同じ判定になることを確認 (= wrapper の policy 経路を test)。
    #[tokio::test]
    async fn is_authorized_matches_contains() {
        // BlobsProtocol を実構築するには tokio runtime + Store が必要。
        // ここでは BlobsProtocol を渡す箇所だけ test を skip するために、
        // FsStore を作って渡す軽量 setup を組む。
        let tmp = tempfile::TempDir::new().unwrap();
        let store = iroh_blobs::store::fs::FsStore::load(tmp.path().join("blobs"))
            .await
            .unwrap();
        let blobs = BlobsProtocol::new(&store, None);

        let al = Arc::new(AllowList::empty_strict());
        let id1 = fixture_id(10);
        let id2 = fixture_id(11);
        al.add(id1, PeerInfo::new(None, 0));

        let wrap = AllowlistBlobs::new(blobs, al);
        assert!(wrap.is_authorized(&id1));
        assert!(!wrap.is_authorized(&id2));
    }

    /// M3 review fix: with_serve_callback で構築すると on_serve_success が
    /// 保持される (= 構築 path の smoke、 実 dispatch は 2-peer e2e で確認)。
    #[tokio::test]
    async fn with_serve_callback_holds_callback() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = iroh_blobs::store::fs::FsStore::load(tmp.path().join("blobs"))
            .await
            .unwrap();
        let blobs = BlobsProtocol::new(&store, None);
        let al = Arc::new(AllowList::empty_strict());
        let counter = Arc::new(std::sync::Mutex::new(0u32));
        let counter2 = counter.clone();
        let cb: BlobServeCallback = Arc::new(move |_id, _t| {
            *counter2.lock().unwrap() += 1;
        });
        let wrap = AllowlistBlobs::with_serve_callback(blobs, al, cb);
        // Debug にも反映されている
        let dbg = format!("{wrap:?}");
        assert!(dbg.contains("has_on_serve_success: true"));
        assert_eq!(*counter.lock().unwrap(), 0);
    }
}
