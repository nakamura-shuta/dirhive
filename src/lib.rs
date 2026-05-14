//! p2p-dir-sync: 任意 dir の P2P 同期 daemon + MCP server
//!
//! 詳細仕様は [`docs/design.md`](../docs/design.md) を参照。
//!
//! ## crate 構造 (design v7 §2)
//!
//! ```text
//! src/
//! ├── lib.rs              ← 本 file: pub mod 宣言 + run_health_check
//! ├── runtime.rs          ← Iroh stack (Endpoint + Gossip + Blobs + Router)
//! ├── state.rs            ← SyncState + PendingTracker
//! ├── send.rs             ← send_file (Upsert broadcast)
//! ├── receive.rs          ← receive_loop + handle_upsert/tombstone
//! ├── conflict.rs         ← compute_conflict_backup_path
//! ├── watcher.rs          ← fsnotify + PollWatcher
//! ├── allowlist.rs        ← peer allowlist (open_all / strict)
//! ├── allowlist_blobs.rs  ← blob ALPN allowlist wrap
//! ├── message.rs          ← SyncUpdate / InviteTicket wire format
//! ├── keystore.rs         ← endpoint.key + folder-secret.bin 永続化
//! ├── paths.rs            ← path 規約 (~/.local/share/p2p-dir-sync/)
//! ├── pending_log.rs      ← 受信 change log
//! ├── daemon/             ← Unix socket listener + dispatch + client
//! └── bin/
//!     ├── p2p-sync.rs     ← daemon binary
//!     └── p2p-sync-mcp.rs ← MCP server binary
//! ```

// Phase 1 では空 module 宣言のみ。Phase 2 以降で実装する。
pub mod allowlist;
pub mod allowlist_blobs;
pub mod conflict;
pub mod daemon;
pub mod keystore;
pub mod message;
pub mod paths;
pub mod pending_log;
pub mod receive;
pub mod runtime;
pub mod send;
pub mod state;
pub mod watcher;

use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// `sync.health-check` の静的部 (= path 解決 + file exists、daemon 経由なしで呼べる)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfoStatic {
    pub key_path: PathBuf,
    pub key_exists: bool,
    pub blobs_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub watched_dir: Option<PathBuf>,
    pub watched_dir_exists: bool,
}

/// `sync.health-check` の動的 daemon state (= daemon が知っている runtime 情報)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfoDynamic {
    pub peer_count: u32,
    pub open_all: bool,
    pub uptime_secs: u64,
    /// file 存在ベース (= folder-secret.bin の有無、static)
    pub group_initialized: bool,
    /// current runtime が gossip topic を subscribe 中か (J1)
    pub gossip_subscribed: bool,
    /// group_initialized && !gossip_subscribed (= invite/accept 直後など、J1)
    pub restart_required: bool,
}

/// `run_health_check` の戻り値: 静的部は常に埋まる、動的部は daemon_state ありの時のみ。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    #[serde(flatten)]
    pub static_info: HealthInfoStatic,
    pub dynamic_info: Option<HealthInfoDynamic>,
}

/// 健全性チェック。`daemon_state` ありなら `HealthInfoDynamic` も埋める。
///
/// Phase 1 では skeleton のみ、Phase 2-3 で実装する。
pub fn run_health_check(
    _daemon_state: Option<&daemon::state::DaemonState>,
) -> Result<HealthInfo> {
    anyhow::bail!("not implemented (Phase 2-3 で実装予定)")
}
