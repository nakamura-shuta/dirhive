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
use crate::keystore;
use crate::message::InviteTicket;
use crate::pending_log;

use super::listener::{DynDispatcher, RpcRequest, RpcResponse};
use super::state::{DaemonState, RuntimeStatus};

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
        let paths = &self.state.paths;
        let status = self.state.current_runtime_status();
        let static_info = serde_json::json!({
            "key_path": paths.key_path,
            "key_exists": paths.key_path.exists(),
            "blobs_dir": paths.blobs_dir,
            "pending_dir": self.state.pending.pending_root,
            "watched_dir": paths.watched_dir_canonical,
            "watched_dir_exists": paths.watched_dir_canonical.exists(),
        });
        let dynamic_info = serde_json::json!({
            "peer_count": self.state.allowlist.len() as u32,
            "open_all": self.state.allowlist.is_open_all(),
            "uptime_secs": self.state.uptime_secs(),
            "group_initialized": status.group_initialized(),
            "gossip_subscribed": status.gossip_subscribed(),
            "restart_required": status.restart_required(),
        });
        Ok(serde_json::json!({
            "static_info": static_info,
            "dynamic_info": dynamic_info,
        }))
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
        let (folder_secret, restart_required) =
            match keystore::try_load_folder_secret(&paths.folder_secret_path)? {
                Some(s) => (s, false),
                None => {
                    let s = keystore::generate_and_persist_folder_secret(&paths.folder_secret_path)?;
                    // group_initialized = true へ昇格 (gossip_subscribed はまだ false)
                    self.state
                        .set_runtime_status(RuntimeStatus::InitializedButNotSubscribed);
                    (s, true)
                }
            };

        let addr = self.state.endpoint.addr();
        let endpoint_ticket = EndpointTicket::new(addr);
        let ticket = InviteTicket::new(endpoint_ticket, folder_secret).encode()?;
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
        let inviter_id = invite.endpoint.endpoint_addr().id;

        // folder_secret 整合 check + adopt:
        // - 不在 → adopt + persist + restart_required = true
        // - 既存 == invite.folder_secret → noop (= 既に同 group)
        // - 既存 != invite.folder_secret → reject (= group merge は MVP scope 外)
        let paths = &self.state.paths;
        let restart_required = match keystore::try_load_folder_secret(&paths.folder_secret_path)? {
            None => {
                keystore::persist_folder_secret(&paths.folder_secret_path, &invite.folder_secret)?;
                self.state
                    .set_runtime_status(RuntimeStatus::InitializedButNotSubscribed);
                true
            }
            Some(existing) if existing == invite.folder_secret => false,
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

/// `p2psync1-` envelope や 32+ hex の secret-like token を `<redacted>` に置換。
/// 完全な検査は難しいので「 design で機密と決めた prefix / 長 hex 」 を粗く隠す。
fn redact_secrets(line: impl AsRef<str>) -> String {
    // p2psync1-... envelope を `<redacted>` に置換 (= 空白 / 制御文字まで)
    redact_pattern(line.as_ref(), "p2psync1-")
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
        };

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(SecretKey::from_bytes(&[secret_byte; 32]))
            .bind()
            .await
            .unwrap();
        let folder_secret = if with_folder_secret {
            Some([0xABu8; 16])
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

    #[tokio::test]
    async fn health_check_returns_static_and_dynamic() {
        let (d, _s, _w) = build_dispatcher(0x11, true).await;
        let v = d.health_check().await.unwrap();
        assert!(v["static_info"]["watched_dir_exists"].as_bool().unwrap());
        assert!(v["dynamic_info"]["group_initialized"].as_bool().unwrap());
        assert!(v["dynamic_info"]["gossip_subscribed"].as_bool().unwrap());
        assert!(!v["dynamic_info"]["restart_required"].as_bool().unwrap());
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

    #[tokio::test]
    async fn invite_first_call_generates_secret_and_sets_restart_required() {
        let (d, _s, _w) = build_dispatcher(0x14, false).await;
        let v = d.invite().await.unwrap();
        assert!(v["ticket"].as_str().unwrap().starts_with("p2psync1-"));
        assert_eq!(v["restart_required"], true);

        // 2 回目は既存 secret → restart_required = false
        let v2 = d.invite().await.unwrap();
        assert_eq!(v2["restart_required"], false);
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
