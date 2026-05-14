//! daemon の sync.* RPC dispatch (= design.md §3.2)。
//!
//! `Dispatcher` は `DaemonState` を hold して `DynDispatcher` trait を実装する。
//! listener が受けた `RpcRequest` を method 名で分岐 → 個別 handler → `RpcResponse`。
//!
//! 9 method を実装:
//! 1. `sync.health-check` → HealthInfo (= static + dynamic)
//! 2. `sync.status`       → status summary
//! 3. `sync.invite`       → {ticket, restart_required}
//! 4. `sync.accept-invite`→ {peer_id, label, my_peer_id, restart_required}
//! 5. `sync.allow-peer`   → {added, peer_id, label}
//! 6. `sync.list-peers`   → {peers: [PeerInfo...], open_all}
//! 7. `sync.revoke`       → {removed, peer_id}
//! 8. `sync.list-pending` → {entries: [PendingEntry...]}
//! 9. `sync.recent-log`   → {lines: [String...]} (= ticket / folder_secret は redact)

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use iroh::EndpointId;
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::allowlist::PeerInfo;
use crate::bootstrap_peers;
use crate::keystore;
use crate::message::InviteTicket;
use crate::pending_log;

use super::listener::{DynDispatcher, RpcRequest, RpcResponse};
use super::state::DaemonState;

/// sync.* RPC dispatcher。
#[derive(Debug, Clone)]
pub struct Dispatcher {
    pub state: Arc<DaemonState>,
}

impl Dispatcher {
    pub fn new(state: Arc<DaemonState>) -> Self {
        Self { state }
    }
}

impl DynDispatcher for Dispatcher {
    fn dispatch<'a>(
        &'a self,
        req: RpcRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = RpcResponse> + Send + 'a>> {
        let s = self.clone();
        Box::pin(async move {
            let r: Result<Value> = match req.method.as_str() {
                "sync.health-check" => s.health_check().await,
                "sync.status" => s.status().await,
                "sync.invite" => s.invite().await,
                "sync.accept-invite" => s.accept_invite(req.params).await,
                "sync.allow-peer" => s.allow_peer(req.params).await,
                "sync.list-peers" => s.list_peers().await,
                "sync.revoke" => s.revoke(req.params).await,
                "sync.list-pending" => s.list_pending(req.params).await,
                "sync.recent-log" => s.recent_log(req.params).await,
                other => Err(anyhow!("unknown method: {other}")),
            };
            match r {
                Ok(v) => RpcResponse::ok(v),
                Err(e) => RpcResponse::err(format!("{e:#}")),
            }
        })
    }
}

// --------------------------------------------------------------------------
// individual handlers
// --------------------------------------------------------------------------

impl Dispatcher {
    async fn health_check(&self) -> Result<Value> {
        // `lib::run_health_check` の戻り値 (= 公開型 HealthInfo) を serialize する。
        // HealthInfo は static_info を `#[serde(flatten)]`、 dynamic_info を Option<...>
        // で持つので、 JSON 上は static field が top-level に出て、 dynamic_info
        // だけが nested object になる (= Phase 3 review M2)。
        let info = crate::run_health_check(Some(&self.state))
            .context("run_health_check")?;
        serde_json::to_value(info).context("serialize HealthInfo")
    }

    async fn status(&self) -> Result<Value> {
        let paths = &self.state.paths;
        let status = self.state.current_runtime_status();
        let pending_count = pending_log::list_pending(
            &self.state.pending.pending_root,
            &self.state.pending.repo_hash,
        )
        .map(|v| v.len())
        .unwrap_or(0);
        Ok(serde_json::json!({
            "watched_dir": paths.watched_dir_canonical,
            "peer_count": self.state.allowlist.len() as u32,
            "open_all": self.state.allowlist.is_open_all(),
            "recent_pending_count": pending_count,
            "key_exists": paths.key_path.exists(),
            "uptime_secs": self.state.uptime_secs(),
            "group_initialized": status.group_initialized(),
            "gossip_subscribed": status.gossip_subscribed(),
            "restart_required": status.restart_required(),
        }))
    }

    async fn invite(&self) -> Result<Value> {
        let paths = &self.state.paths;
        // folder_secret 既存 → 既存 ticket、 不在 → 新規 generate + persist
        let folder_secret = match keystore::try_load_folder_secret(&paths.folder_secret_path)? {
            Some(s) => s,
            None => {
                let s = keystore::generate_and_persist_folder_secret(&paths.folder_secret_path)?;
                // group_initialized = true へ昇格 (gossip_subscribed はまだ false)
                self.state.enter_initialized_but_not_subscribed();
                s
            }
        };

        let addr = self.state.endpoint.addr();
        let endpoint_ticket = EndpointTicket::new(addr);
        let ticket = InviteTicket::new(endpoint_ticket, folder_secret).encode()?;

        // restart_required は **「 今回 secret を作ったか 」 ではなく現在の真の
        // runtime_status から導く** (= Phase 3 review H1)。 初回 generate 後の
        // 2 回目 invite でも、 daemon 再起動するまで restart_required = true の
        // ままにする (= 7-step bilateral flow の再起動指示を消さない)。
        let restart_required = self.state.current_runtime_status().restart_required();
        Ok(serde_json::json!({
            "ticket": ticket,
            "restart_required": restart_required,
        }))
    }

    async fn accept_invite(&self, params: Value) -> Result<Value> {
        #[derive(Deserialize)]
        struct P {
            ticket: String,
            label: Option<String>,
        }
        let p: P = serde_json::from_value(params).context("parse accept-invite params")?;

        let invite = InviteTicket::decode(&p.ticket)?;
        let inviter_addr = invite.endpoint.endpoint_addr().clone();
        let inviter_id = inviter_addr.id;

        // folder_secret 整合 check + adopt:
        // - 不在 → adopt + persist + enter_initialized_but_not_subscribed
        // - 既存 == invite.folder_secret → noop (= 既に同 group)
        // - 既存 != invite.folder_secret → reject (= group merge は MVP scope 外)
        let paths = &self.state.paths;
        match keystore::try_load_folder_secret(&paths.folder_secret_path)? {
            None => {
                keystore::persist_folder_secret(&paths.folder_secret_path, &invite.folder_secret)?;
                self.state.enter_initialized_but_not_subscribed();
            }
            Some(existing) if existing == invite.folder_secret => {}
            Some(_) => {
                return Err(anyhow!(
                    "this daemon is already initialized with a different folder_secret; \
                     delete {} to reset",
                    paths.folder_secret_path.display()
                ));
            }
        };

        // inviter を local allowlist に追加 (= bilateral の片半分)。
        let now_secs = current_epoch_secs();
        let info = PeerInfo::new(p.label.clone(), now_secs);
        self.state
            .allowlist
            .add_and_save(inviter_id, info, &paths.allowlist_path)?;

        // inviter の EndpointAddr を bootstrap_peers に永続化 (= H1 review fix)。
        // 次回起動時 SyncRuntime::build がこの addr を gossip.subscribe + endpoint
        // address_lookup に渡して、 mesh discovery を成立させる。
        bootstrap_peers::add_or_replace(&paths.bootstrap_peers_path, inviter_addr)
            .with_context(|| {
                format!(
                    "persist inviter addr to {}",
                    paths.bootstrap_peers_path.display()
                )
            })?;

        // restart_required は真の runtime_status から (= H1 review fix)。
        let restart_required = self.state.current_runtime_status().restart_required();
        Ok(serde_json::json!({
            "peer_id": inviter_id.to_string(),
            "label": p.label,
            "my_peer_id": self.state.self_endpoint_id.to_string(),
            "restart_required": restart_required,
        }))
    }

    async fn allow_peer(&self, params: Value) -> Result<Value> {
        #[derive(Deserialize)]
        struct P {
            peer_id: String,
            label: Option<String>,
        }
        let p: P = serde_json::from_value(params).context("parse allow-peer params")?;
        let id = EndpointId::from_str(&p.peer_id)
            .map_err(|e| anyhow!("invalid peer_id '{}': {e}", p.peer_id))?;

        let already_in = self.state.allowlist.contains(&id)
            && !self.state.allowlist.is_open_all();
        let added = !already_in;
        if added {
            let info = PeerInfo::new(p.label.clone(), current_epoch_secs());
            self.state
                .allowlist
                .add_and_save(id, info, &self.state.paths.allowlist_path)?;
        }
        Ok(serde_json::json!({
            "added": added,
            "peer_id": p.peer_id,
            "label": p.label,
        }))
    }

    async fn list_peers(&self) -> Result<Value> {
        let last_seen = self.state.last_seen_at.lock().expect("last_seen lock").clone();
        let peers: Vec<Value> = self
            .state
            .allowlist
            .list()
            .into_iter()
            .map(|(id, info)| {
                serde_json::json!({
                    "peer_id": id.to_string(),
                    "label": info.label,
                    "added_at": info.added_at,
                    "last_seen_at": last_seen.get(&id).copied(),
                })
            })
            .collect();
        Ok(serde_json::json!({
            "peers": peers,
            "open_all": self.state.allowlist.is_open_all(),
        }))
    }

    async fn revoke(&self, params: Value) -> Result<Value> {
        #[derive(Deserialize)]
        struct P {
            peer_id: String,
        }
        let p: P = serde_json::from_value(params).context("parse revoke params")?;
        let id = EndpointId::from_str(&p.peer_id)
            .map_err(|e| anyhow!("invalid peer_id '{}': {e}", p.peer_id))?;
        let removed = self
            .state
            .allowlist
            .remove_and_save(&id, &self.state.paths.allowlist_path)?;
        Ok(serde_json::json!({
            "removed": removed.is_some(),
            "peer_id": p.peer_id,
        }))
    }

    async fn list_pending(&self, params: Value) -> Result<Value> {
        #[derive(Deserialize, Default)]
        struct P {
            limit: Option<usize>,
        }
        let p: P = serde_json::from_value(params).unwrap_or_default();
        let mut entries = pending_log::list_pending(
            &self.state.pending.pending_root,
            &self.state.pending.repo_hash,
        )?;
        if let Some(n) = p.limit {
            entries.truncate(n);
        }
        Ok(serde_json::json!({ "entries": entries }))
    }

    async fn recent_log(&self, params: Value) -> Result<Value> {
        #[derive(Deserialize, Default)]
        struct P {
            lines: Option<u32>,
        }
        let p: P = serde_json::from_value(params).unwrap_or_default();
        let lines_cap = p.lines.unwrap_or(50).min(100) as usize;
        let log_path = &self.state.paths.log_path;
        let lines = tail_lines_redacted(log_path, lines_cap)?;
        Ok(serde_json::json!({ "lines": lines }))
    }
}

/// log file の末尾 N 行を読み出し、 secrets を `<redacted>` に置換する。
/// 不在 file は空 Vec を返す (= log がまだ書き出されてないケース)。
fn tail_lines_redacted(log_path: &std::path::Path, n: usize) -> Result<Vec<String>> {
    if !log_path.exists() {
        return Ok(Vec::new());
    }
    let body = std::fs::read_to_string(log_path)
        .with_context(|| format!("read log {}", log_path.display()))?;
    let mut lines: Vec<String> = body
        .lines()
        .rev()
        .take(n)
        .map(redact_secrets)
        .collect();
    lines.reverse();
    Ok(lines)
}

/// `p2psync1-` envelope と 32+ 文字の連続 hex token を `<redacted>` に置換。
///
/// design §6.2 の secret 漏洩防止 layer:
/// - `p2psync1-` envelope: invite ticket (= folder_secret を含む) 漏れ防止
/// - 32+ hex token: endpoint key / folder_secret / blob hash 等の長 hex 漏れ防止
///
/// 完全な検査は難しいので「 design で機密と決めた prefix / 長 hex 」 を粗く隠す。
/// 短い hex (= blob short id 等) は保つ (= debug log の useful information を残す)。
fn redact_secrets(line: impl AsRef<str>) -> String {
    let s = line.as_ref();
    let s = redact_pattern(s, "p2psync1-");
    redact_hex_token(&s, MIN_REDACT_HEX_LEN)
}

/// hex token を redact する最小文字数 (= Phase 3 review L5)。
///
/// 32 hex = 16 bytes = folder_secret サイズ / 4。 これより短い token (= 8 hex の
/// short peer id 等) は redact しない (= log の可読性維持)。
pub const MIN_REDACT_HEX_LEN: usize = 32;

/// 連続する hex 文字が `min_len` 以上の token を `<redacted>` に置換する。
fn redact_hex_token(s: &str, min_len: usize) -> String {
    let mut out = String::with_capacity(s.len());
    let mut buf = String::new();
    let flush = |buf: &mut String, out: &mut String| {
        if buf.len() >= min_len {
            out.push_str("<redacted>");
        } else {
            out.push_str(buf);
        }
        buf.clear();
    };
    for c in s.chars() {
        if c.is_ascii_hexdigit() {
            buf.push(c);
        } else {
            flush(&mut buf, &mut out);
            out.push(c);
        }
    }
    flush(&mut buf, &mut out);
    out
}

fn redact_pattern(s: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find(prefix) {
        out.push_str(&rest[..pos]);
        out.push_str("<redacted>");
        // prefix 以降の token を skip (空白 / 制御文字まで)
        let after = &rest[pos + prefix.len()..];
        let end = after
            .find(|c: char| c.is_whitespace() || c == '"' || c == ',')
            .unwrap_or(after.len());
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

fn current_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// `PendingEntry` を serde_json で直接 serialize するための helper trait は不要
// (= pending_log の PendingEntry は既に Serialize 派生済)。
#[derive(Debug, Serialize)]
struct _PingPong;

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::state::RuntimeStatus;
    use crate::allowlist::AllowList;
    use crate::message::PeerRef;
    use crate::runtime::SyncRuntime;
    use crate::state::{PendingTracker, SyncState, compute_repo_hash};
    use iroh::SecretKey;
    use std::path::PathBuf;

    async fn build_dispatcher(
        secret_byte: u8,
        with_folder_secret: bool,
    ) -> (Dispatcher, tempfile::TempDir, tempfile::TempDir) {
        let state_tmp = tempfile::TempDir::new().unwrap();
        let watch_tmp = tempfile::TempDir::new().unwrap();
        let watched = watch_tmp.path().canonicalize().unwrap();

        let paths = super::super::state::DaemonPaths {
            watched_dir_canonical: watched.clone(),
            socket_path: state_tmp.path().join("daemon.sock"),
            lock_path: state_tmp.path().join("daemon.lock"),
            allowlist_path: state_tmp.path().join("allowlist.json"),
            folder_secret_path: state_tmp.path().join("folder-secret.bin"),
            key_path: state_tmp.path().join("endpoint.key"),
            blobs_dir: state_tmp.path().join("blobs"),
            log_path: state_tmp.path().join("p2p-dir-sync.log"),
            bootstrap_peers_path: state_tmp.path().join("bootstrap-peers.json"),
        };

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[secret_byte; 32]))
            .bind()
            .await
            .unwrap();
        let folder_secret = if with_folder_secret {
            let secret = [0xABu8; 16];
            // **disk にも persist** する: daemon 起動時の load-or-create と整合させ、
            // 後から invite が `try_load_folder_secret` で正しく既存値を拾えるようにする。
            keystore::persist_folder_secret(&paths.folder_secret_path, &secret).unwrap();
            Some(secret)
        } else {
            None
        };
        let allowlist = Arc::new(AllowList::empty_strict());
        let pending = Arc::new(PendingTracker {
            pending_root: state_tmp.path().join("pending"),
            repo_hash: compute_repo_hash(&watched),
        });
        std::fs::create_dir_all(&pending.pending_root).unwrap();
        let mut rt = SyncRuntime::build(
            endpoint.clone(),
            &paths.blobs_dir,
            allowlist.clone(),
            folder_secret.as_ref(),
            Vec::new(),
        )
        .await
        .unwrap();
        let (gossip_sender, runtime_status) = if folder_secret.is_some() {
            let topic = rt.take_topic().unwrap();
            let (s, _r) = topic.split();
            (Some(s), RuntimeStatus::Active)
        } else {
            (None, RuntimeStatus::Uninitialized)
        };

        let state = DaemonState::new(
            paths.clone(),
            allowlist,
            pending,
            SyncState::new(),
            rt.endpoint().clone(),
            rt.blobs().clone(),
            gossip_sender,
            runtime_status,
        );
        std::mem::forget(rt); // Keep runtime alive for the duration of the test
        (Dispatcher::new(Arc::new(state)), state_tmp, watch_tmp)
    }

    /// M2 review fix: static_info は top-level flatten、 dynamic_info だけ nested。
    /// 公開型 HealthInfo の serde shape と一致する。
    #[tokio::test]
    async fn health_check_returns_static_flat_and_dynamic_nested() {
        let (d, _s, _w) = build_dispatcher(0x11, true).await;
        let v = d.health_check().await.unwrap();
        // static fields は top-level (flatten 済)
        assert!(v["watched_dir_exists"].as_bool().unwrap());
        assert!(v["key_path"].is_string());
        assert!(v["blobs_dir"].is_string());
        assert!(v["pending_dir"].is_string());
        // dynamic_info は nested object
        assert!(v["dynamic_info"]["group_initialized"].as_bool().unwrap());
        assert!(v["dynamic_info"]["gossip_subscribed"].as_bool().unwrap());
        assert!(!v["dynamic_info"]["restart_required"].as_bool().unwrap());
        // 旧 shape の `static_info` nested は無くなった
        assert!(v.get("static_info").is_none(), "static_info nested key must be gone");
    }

    #[tokio::test]
    async fn health_check_reports_uninitialized_when_no_folder_secret() {
        let (d, _s, _w) = build_dispatcher(0x12, false).await;
        let v = d.health_check().await.unwrap();
        assert!(!v["dynamic_info"]["group_initialized"].as_bool().unwrap());
        assert!(!v["dynamic_info"]["gossip_subscribed"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn status_returns_summary_fields() {
        let (d, _s, _w) = build_dispatcher(0x13, true).await;
        let v = d.status().await.unwrap();
        assert!(v["watched_dir"].is_string());
        assert_eq!(v["peer_count"], 0);
        assert_eq!(v["open_all"], false);
        assert_eq!(v["recent_pending_count"], 0);
    }

    /// H1 review fix: restart_required は runtime_status から導く。 初回 generate
    /// 後の 2 回目 invite でも、 daemon 再起動するまで true のまま (= 7-step
    /// bilateral flow の再起動指示を消さない)。
    #[tokio::test]
    async fn invite_first_call_generates_secret_and_sets_restart_required() {
        let (d, _s, _w) = build_dispatcher(0x14, false).await;
        let v = d.invite().await.unwrap();
        assert!(v["ticket"].as_str().unwrap().starts_with("p2psync1-"));
        assert_eq!(v["restart_required"], true);

        // 2 回目: 既存 secret を使うが、 daemon 自身は InitializedButNotSubscribed
        // のまま (= 再起動してない) → restart_required も true のまま
        let v2 = d.invite().await.unwrap();
        assert_eq!(v2["restart_required"], true, "must stay true until daemon restart");
        // ticket 内容は同じ (= 同じ secret から再構築)
        assert_eq!(v["ticket"], v2["ticket"]);
    }

    /// H1 review fix: daemon が既に Active (= 起動時に subscribe 済) なら、
    /// invite は restart_required = false を返す。
    #[tokio::test]
    async fn invite_returns_no_restart_when_already_active() {
        let (d, _s, _w) = build_dispatcher(0x17, true).await;
        // build_dispatcher の with_folder_secret=true で起動時 Active になっている
        assert!(!d.state.current_runtime_status().restart_required());
        let v = d.invite().await.unwrap();
        assert_eq!(v["restart_required"], false);
    }

    #[tokio::test]
    async fn allow_peer_and_list_and_revoke_round_trip() {
        let (d, _s, _w) = build_dispatcher(0x15, false).await;
        let peer_id =
            SecretKey::from_bytes(&[0x77; 32]).public().to_string();

        // add
        let v = d
            .allow_peer(serde_json::json!({
                "peer_id": peer_id, "label": "bob"
            }))
            .await
            .unwrap();
        assert_eq!(v["added"], true);
        assert_eq!(v["peer_id"], peer_id);

        // list
        let lp = d.list_peers().await.unwrap();
        assert_eq!(lp["peers"].as_array().unwrap().len(), 1);
        assert_eq!(lp["peers"][0]["label"], "bob");
        assert!(lp["peers"][0]["last_seen_at"].is_null());

        // revoke
        let r = d
            .revoke(serde_json::json!({"peer_id": peer_id}))
            .await
            .unwrap();
        assert_eq!(r["removed"], true);
        let lp2 = d.list_peers().await.unwrap();
        assert_eq!(lp2["peers"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn allow_peer_rejects_invalid_peer_id() {
        let (d, _s, _w) = build_dispatcher(0x16, false).await;
        let e = d
            .allow_peer(serde_json::json!({"peer_id": "not-a-valid-id"}))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("invalid peer_id"));
    }

    /// H1 review fix: accept-invite で inviter の EndpointAddr が
    /// bootstrap_peers に永続化される (= 次回起動時に gossip.subscribe に渡される)。
    #[tokio::test]
    async fn accept_invite_persists_inviter_addr_for_bootstrap() {
        let (inviter, _s1, _w1) = build_dispatcher(0x31, false).await;
        let inv = inviter.invite().await.unwrap();
        let ticket = inv["ticket"].as_str().unwrap().to_string();

        let (acceptor, _s2, _w2) = build_dispatcher(0x32, false).await;
        acceptor
            .accept_invite(serde_json::json!({"ticket": ticket}))
            .await
            .unwrap();

        let saved = crate::bootstrap_peers::load_bootstrap_peers(
            &acceptor.state.paths.bootstrap_peers_path,
        )
        .unwrap();
        assert_eq!(saved.len(), 1, "inviter addr must be persisted");
        assert_eq!(saved[0].id, inviter.state.self_endpoint_id);
    }

    #[tokio::test]
    async fn accept_invite_adopts_secret_and_adds_inviter() {
        // 別 dispatcher (= inviter side) で ticket を作る
        let (inviter, _s1, _w1) = build_dispatcher(0x21, false).await;
        let inv = inviter.invite().await.unwrap();
        let ticket = inv["ticket"].as_str().unwrap().to_string();
        let inviter_id = inviter.state.self_endpoint_id.to_string();

        // acceptor (folder_secret 未初期化)
        let (acceptor, _s2, _w2) = build_dispatcher(0x22, false).await;
        let r = acceptor
            .accept_invite(serde_json::json!({
                "ticket": ticket, "label": "alice"
            }))
            .await
            .unwrap();
        assert_eq!(r["peer_id"], inviter_id);
        assert_eq!(r["restart_required"], true);
        assert_eq!(r["label"], "alice");
        assert!(acceptor.state.paths.folder_secret_path.exists());

        // allowlist にも入っている
        assert_eq!(
            acceptor
                .state
                .allowlist
                .list()
                .iter()
                .filter(|(id, _)| id.to_string() == inviter_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unknown_method_returns_error_response() {
        let (d, _s, _w) = build_dispatcher(0x23, false).await;
        let resp = d
            .dispatch(RpcRequest {
                method: "sync.unknown".into(),
                params: serde_json::json!({}),
            })
            .await;
        assert!(resp.error.unwrap().contains("unknown method"));
    }

    #[tokio::test]
    async fn list_pending_returns_empty_when_no_entries() {
        let (d, _s, _w) = build_dispatcher(0x24, false).await;
        let v = d.list_pending(serde_json::json!({})).await.unwrap();
        assert_eq!(v["entries"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn list_pending_returns_recorded_entries() {
        let (d, _s, _w) = build_dispatcher(0x25, false).await;
        let from = PeerRef {
            id: SecretKey::from_bytes(&[0x88; 32]).public(),
        };
        for i in 1..=3 {
            let e = crate::pending_log::PendingEntry::Tombstone {
                schema_version: 1,
                rel_path: format!("a/{i}.md"),
                received_at: 100 + i as i64,
                source_peer: from.id.to_string(),
            };
            crate::pending_log::record_receive(
                &d.state.pending.pending_root,
                &d.state.pending.repo_hash,
                &e,
            )
            .unwrap();
        }
        let v = d.list_pending(serde_json::json!({"limit": 2})).await.unwrap();
        assert_eq!(v["entries"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn redact_pattern_replaces_token_in_text() {
        let s = "got ticket p2psync1-ABC123XYZ for bob";
        let r = redact_pattern(s, "p2psync1-");
        assert!(r.contains("<redacted>"));
        assert!(!r.contains("ABC123XYZ"));
        assert!(r.contains("for bob"));
    }

    /// L5 review fix: 32+ hex token も redact される。
    #[test]
    fn redact_hex_token_replaces_long_hex() {
        let long_hex = "deadbeefcafebabe0123456789abcdef"; // 32 chars hex
        let s = format!("key={long_hex} owner=alice");
        let r = redact_hex_token(&s, MIN_REDACT_HEX_LEN);
        assert!(r.contains("<redacted>"));
        assert!(!r.contains(long_hex));
        assert!(r.contains("owner=alice"));
    }

    /// 32 文字未満の hex は維持される (= short id / 部分 hash 等の log 可読性維持)。
    #[test]
    fn redact_hex_token_keeps_short_hex_intact() {
        let short = "abcd1234"; // 8 hex
        let s = format!("peer={short} status=ok");
        let r = redact_hex_token(&s, MIN_REDACT_HEX_LEN);
        assert_eq!(r, s);
    }

    /// 連続 hex の途中に区切り (`-` / `:`) があれば、 個別 segment 長で判定される。
    #[test]
    fn redact_hex_token_segments_at_non_hex_boundary() {
        // 16 hex + ":" + 16 hex は両方 < 32 で redact されない
        let s = "id=01234567abcdef89:9876543210abcdef done";
        let r = redact_hex_token(s, MIN_REDACT_HEX_LEN);
        assert_eq!(r, s);
    }

    /// redact_secrets 統合 path: p2psync1- envelope と長 hex の両方が消える。
    #[test]
    fn redact_secrets_handles_both_patterns() {
        let s = "warn invite=p2psync1-AAAABBBBCCCC then hash=00112233445566778899aabbccddeeff00112233";
        let r = redact_secrets(s);
        assert!(!r.contains("AAAABBBBCCCC"));
        assert!(!r.contains("00112233445566778899aabbccddeeff00112233"));
        assert!(r.matches("<redacted>").count() >= 2);
    }

    #[test]
    fn recent_log_returns_empty_when_log_absent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let p = tmp.path().join("not-yet.log");
        let v = tail_lines_redacted(&p, 10).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn recent_log_tails_and_redacts() {
        let tmp = tempfile::TempDir::new().unwrap();
        let p: PathBuf = tmp.path().join("p2p.log");
        std::fs::write(
            &p,
            "line1\nline2 p2psync1-SECRET more\nline3\nline4\nline5\n",
        )
        .unwrap();
        let v = tail_lines_redacted(&p, 3).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], "line3");
        assert_eq!(v[2], "line5");
        let body = v.join("\n");
        assert!(!body.contains("SECRET"));
    }
}
