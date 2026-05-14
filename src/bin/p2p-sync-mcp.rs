//! p2p-sync-mcp MCP server binary (= L3、 design.md §3.3)。
//!
//! 10 tool: sync.ping (= MCP server 内で完結) + sync.* 9 個 (= daemon Unix socket
//! へ rpc() を投げる薄い wrapper)。 stdio transport (= Claude Code / Codex plugin)。
//!
//! design §3.3 の sync.ping vs sync.health-check 区別:
//! - sync.ping: 不要、 MCP server 起動失敗 / plugin install 不全の検出
//! - sync.health-check: daemon が落ちている / 設定不全 の検出
//!
//! 設定:
//! - socket_path: env `P2P_SYNC_SOCKET` で override、 default は paths::default_socket_path

use std::path::PathBuf;

use anyhow::Result;
use p2p_dir_sync::daemon::client::rpc;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::{ErrorData, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct P2pSyncMcp {
    socket_path: PathBuf,
    /// `#[tool_handler]` の生成 code が内部で参照する (= dead_code 誤検出を抑止)。
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

impl P2pSyncMcp {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            tool_router: Self::tool_router(),
        }
    }

    /// daemon RPC を投げて `CallToolResult` に変換する共通 helper。
    /// daemon の error response は `is_error: true` の Content として返す
    /// (= MCP client / AI agent から「 失敗 」 を見える形にする)。
    async fn call_daemon(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<CallToolResult, ErrorData> {
        match rpc(&self.socket_path, method, params).await {
            Ok(v) => {
                let text = serde_json::to_string_pretty(&v)
                    .unwrap_or_else(|_| v.to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(format!("{e:#}"))])),
        }
    }
}

// ---------------------------------------------------------------------------
// tool parameter types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AcceptInviteParams {
    /// `p2psync1-` envelope の invite ticket
    pub ticket: String,
    /// 表示名 (= sync.list-peers でこの peer 識別に使う)
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AllowPeerParams {
    /// 対象 peer の EndpointId (= ed25519 公開鍵 32 byte の hex)
    pub peer_id: String,
    /// 表示名
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RevokeParams {
    pub peer_id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListPendingParams {
    /// 取得件数の上限 (= 省略時 = 全件)
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RecentLogParams {
    /// 取得行数 (= 1-100、 省略時 = 50)
    #[serde(default)]
    pub lines: Option<u32>,
}

// ---------------------------------------------------------------------------
// #[tool_router]: 10 tools 定義
// ---------------------------------------------------------------------------

#[tool_router]
impl P2pSyncMcp {
    /// MCP server readiness check (daemon 不要)。 plugin install / MCP server 起動
    /// 確認用。
    #[tool(name = "sync.ping", description = "Ping the MCP server itself (no daemon required)")]
    async fn sync_ping(&self) -> String {
        "pong".to_string()
    }

    #[tool(
        name = "sync.health-check",
        description = "Check daemon liveness and config (paths, key, runtime status)"
    )]
    async fn sync_health_check(&self) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.health-check", serde_json::json!({}))
            .await
    }

    #[tool(
        name = "sync.status",
        description = "Summary view: peer count, open_all, uptime, group state, recent pending"
    )]
    async fn sync_status(&self) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.status", serde_json::json!({}))
            .await
    }

    #[tool(
        name = "sync.invite",
        description = "Generate or reuse an invite ticket; first call needs daemon restart"
    )]
    async fn sync_invite(&self) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.invite", serde_json::json!({}))
            .await
    }

    #[tool(
        name = "sync.accept-invite",
        description = "Adopt a peer's invite ticket; first call needs daemon restart"
    )]
    async fn sync_accept_invite(
        &self,
        Parameters(p): Parameters<AcceptInviteParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.accept-invite", serde_json::to_value(p).unwrap())
            .await
    }

    #[tool(
        name = "sync.allow-peer",
        description = "Add a peer to the local allowlist (counter-half of bilateral allowlist)"
    )]
    async fn sync_allow_peer(
        &self,
        Parameters(p): Parameters<AllowPeerParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.allow-peer", serde_json::to_value(p).unwrap())
            .await
    }

    #[tool(
        name = "sync.list-peers",
        description = "List currently allowed peers with last_seen_at (data-plane proxy)"
    )]
    async fn sync_list_peers(&self) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.list-peers", serde_json::json!({}))
            .await
    }

    #[tool(
        name = "sync.revoke",
        description = "Remove a peer from the local allowlist (best-effort, no force-close)"
    )]
    async fn sync_revoke(
        &self,
        Parameters(p): Parameters<RevokeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.revoke", serde_json::to_value(p).unwrap())
            .await
    }

    #[tool(
        name = "sync.list-pending",
        description = "Return recent incoming change log entries (Upsert/Tombstone)"
    )]
    async fn sync_list_pending(
        &self,
        Parameters(p): Parameters<ListPendingParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.list-pending", serde_json::to_value(p).unwrap())
            .await
    }

    #[tool(
        name = "sync.recent-log",
        description = "Tail recent daemon log lines (secrets redacted)"
    )]
    async fn sync_recent_log(
        &self,
        Parameters(p): Parameters<RecentLogParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_daemon("sync.recent-log", serde_json::to_value(p).unwrap())
            .await
    }
}

#[tool_handler]
impl ServerHandler for P2pSyncMcp {}

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path = match std::env::var_os("P2P_SYNC_SOCKET") {
        Some(s) => PathBuf::from(s),
        None => p2p_dir_sync::paths::default_socket_path()?,
    };

    // stderr に minimal init message (= stdio は MCP protocol で占有)
    eprintln!(
        "p2p-sync-mcp v{} starting (socket = {})",
        env!("CARGO_PKG_VERSION"),
        socket_path.display()
    );

    let server = P2pSyncMcp::new(socket_path);
    let (stdin, stdout) = rmcp::transport::io::stdio();
    let service = server
        .serve((stdin, stdout))
        .await
        .map_err(|e| anyhow::anyhow!("MCP serve failed: {e}"))?;
    service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("MCP wait failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CallToolRequestParams;

    fn fixture() -> P2pSyncMcp {
        P2pSyncMcp::new(PathBuf::from("/tmp/nonexistent.sock"))
    }

    #[test]
    fn server_exposes_ten_tools() {
        let s = fixture();
        let info = s.get_info();
        assert!(info.capabilities.tools.is_some(), "tools capability must be enabled");
        // tool 一覧は tool_router 経由で取得 (= ToolRouter::list_all を直接叩く)
        let names: Vec<String> = s
            .tool_router
            .list_all()
            .into_iter()
            .map(|t| t.name.into_owned())
            .collect();
        assert_eq!(names.len(), 10, "expected exactly 10 tools, got {names:?}");
        let must_have = [
            "sync.ping",
            "sync.health-check",
            "sync.status",
            "sync.invite",
            "sync.accept-invite",
            "sync.allow-peer",
            "sync.list-peers",
            "sync.revoke",
            "sync.list-pending",
            "sync.recent-log",
        ];
        for m in must_have {
            assert!(names.iter().any(|n| n == m), "missing tool {m}");
        }
    }

    #[tokio::test]
    async fn sync_ping_returns_pong_without_daemon() {
        let s = fixture();
        let resp = s.sync_ping().await;
        assert_eq!(resp, "pong");
    }

    #[tokio::test]
    async fn sync_health_check_returns_error_when_daemon_absent() {
        let s = fixture();
        let result = s.sync_health_check().await.unwrap();
        // daemon 不在 → call_daemon が anyhow::Error → CallToolResult::error
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn accept_invite_params_schema_has_required_ticket() {
        // schemars 派生で生成される schema を JSON Value として取り出して確認
        let s = schemars::schema_for!(AcceptInviteParams);
        let v = serde_json::to_value(&s).unwrap();
        let required = v["required"].as_array().expect("required array");
        assert!(required.iter().any(|r| r == "ticket"));
        // label は default で optional
        assert!(!required.iter().any(|r| r == "label"));
    }

    /// `CallToolRequestParams::new` で tool 呼び出しが構築できる (= macro 経由
    /// で `call_tool` が dispatch される、 という型 level の smoke)。
    #[allow(dead_code)]
    fn _compile_check_call_param() {
        let _p = CallToolRequestParams::new("sync.ping");
    }
}
