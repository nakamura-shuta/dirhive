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
pub mod bootstrap_peers;
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
/// - **static_info** は常に埋まる: daemon_state があればその path を、 なければ
///   `paths::default_*` を見る (= MCP server 経由で daemon が落ちている場合の
///   probe 用途)
/// - **dynamic_info** は daemon_state がある時だけ Some
///
/// シリアライズ shape は `HealthInfo` 型の派生に従う:
/// - static fields は `#[serde(flatten)]` で top-level に展開される
/// - `dynamic_info` だけ nested object として残る
///   (= Phase 3 review M2、 旧 dispatch::health_check の手書き shape は廃止)
pub fn run_health_check(
    daemon_state: Option<&daemon::state::DaemonState>,
) -> Result<HealthInfo> {
    let (key_path, blobs_dir, pending_dir, watched_dir) = match daemon_state {
        Some(s) => (
            s.paths.key_path.clone(),
            s.paths.blobs_dir.clone(),
            s.pending.pending_root.clone(),
            Some(s.paths.watched_dir_canonical.clone()),
        ),
        None => (
            paths::default_key_path()?,
            paths::default_blobs_dir()?,
            paths::default_pending_dir()?,
            None,
        ),
    };
    let key_exists = key_path.exists();
    let watched_dir_exists = watched_dir.as_ref().is_some_and(|p| p.exists());

    let static_info = HealthInfoStatic {
        key_path,
        key_exists,
        blobs_dir,
        pending_dir,
        watched_dir,
        watched_dir_exists,
    };

    let dynamic_info = daemon_state.map(|s| {
        let status = s.current_runtime_status();
        HealthInfoDynamic {
            peer_count: s.allowlist.len() as u32,
            open_all: s.allowlist.is_open_all(),
            uptime_secs: s.uptime_secs(),
            group_initialized: status.group_initialized(),
            gossip_subscribed: status.gossip_subscribed(),
            restart_required: status.restart_required(),
        }
    });

    Ok(HealthInfo {
        static_info,
        dynamic_info,
    })
}
