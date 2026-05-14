//! p2p-sync daemon binary (= L2 daemon、 design.md §5.1)。
//!
//! ## 起動シーケンス (= design §5.1)
//!
//! 1. CLI arg parse
//! 2. watched_dir canonicalize
//! 3. tracing 初期化 (= log file への JSON 出力)
//! 4. endpoint.key load / generate
//! 5. folder_secret lazy load (= 不在なら group_initialized = false で起動)
//! 6. Endpoint::bind + online
//! 7. blobs_dir prepare 0o700
//! 8. allowlist load (file 不在 → strict empty、 --allow-open-all で open_all)
//! 9. SyncRuntime::build (blob + Router + AllowlistBlobs wrap + 必要なら gossip subscribe)
//! 10. DaemonState 構築 + Dispatcher 構築
//! 11. spawn_listener (Unix socket、 0o600)
//! 12. spawn watcher + receive_loop (group_initialized 不問で RPC は応答)
//! 13. tokio::select! で SIGINT / SIGTERM 待機 → graceful shutdown

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use p2p_dir_sync::allowlist::AllowList;
use p2p_dir_sync::daemon::dispatch::Dispatcher;
use p2p_dir_sync::daemon::listener::{DynDispatcher, bind_listener};
use p2p_dir_sync::daemon::state::{DaemonPaths, DaemonState, RuntimeStatus};
use p2p_dir_sync::keystore;
use p2p_dir_sync::paths;
use p2p_dir_sync::runtime::SyncRuntime;
use p2p_dir_sync::state::{PendingTracker, SyncState};

#[derive(Debug, Parser)]
#[command(name = "p2p-sync", version, about = "P2P directory sync daemon")]
struct Cli {
    /// 同期対象 dir (= canonicalize される、 既存である必要)
    #[arg(long, value_name = "DIR")]
    watch: PathBuf,

    /// 全 peer を許可する開発用 mode (= production では使わない)。
    /// design §6.3: 使用時は warning banner を log/stderr に出す
    #[arg(long)]
    allow_open_all: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // (1, 2) watched_dir canonicalize
    let watched_dir = cli
        .watch
        .canonicalize()
        .with_context(|| format!("canonicalize --watch {}", cli.watch.display()))?;

    // (3) tracing 初期化 (best-effort、 log file に書く)
    init_tracing()?;

    if cli.allow_open_all {
        tracing::warn!(
            "--allow-open-all enabled: this daemon will accept blob fetch from ANY \
             peer that knows folder_secret. Use only for development."
        );
        eprintln!("WARNING: --allow-open-all is set; this is a dev-only mode.");
    }

    // (4, 5, 7, 8 の path 解決)
    let daemon_paths = build_daemon_paths(&watched_dir)?;

    // (4) endpoint.key
    let secret_key = keystore::load_or_create_endpoint_key(&daemon_paths.key_path)
        .context("load or create endpoint.key")?;

    // (5) folder_secret lazy load
    let folder_secret = keystore::try_load_folder_secret(&daemon_paths.folder_secret_path)
        .context("try_load_folder_secret")?;

    // (6) Endpoint::bind + online
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .secret_key(secret_key)
        .bind()
        .await
        .context("Endpoint::bind")?;
    endpoint.online().await;
    tracing::info!(endpoint_id = %endpoint.id(), "endpoint online");

    // (7) blobs_dir 0o700
    paths::ensure_dir_700(&daemon_paths.blobs_dir)?;

    // (8) allowlist load
    let allowlist = if cli.allow_open_all {
        Arc::new(AllowList::open_all())
    } else {
        Arc::new(
            AllowList::load_or_strict_empty(&daemon_paths.allowlist_path)
                .context("load allowlist")?,
        )
    };

    // (9) SyncRuntime::build
    let mut sync_runtime = SyncRuntime::build(
        endpoint.clone(),
        &daemon_paths.blobs_dir,
        allowlist.clone(),
        folder_secret.as_ref(),
    )
    .await
    .context("SyncRuntime::build")?;

    // (10) DaemonState 構築 + Dispatcher 構築
    let (gossip_sender_opt, runtime_status) = match folder_secret {
        Some(_) => {
            // group_initialized = true、 subscribe 済 → Active。
            // GossipTopic を split して sender/receiver を取り出す
            let topic = sync_runtime
                .take_topic()
                .context("SyncRuntime::take_topic (group_initialized なのに None)")?;
            let (sender, receiver) = topic.split();
            // receiver は Phase 3 後半で receive_loop に渡す予定 (= ここでは drop しない)
            // 一旦 leak で hold する → Phase 3 後半で正規 spawn に書き換える
            std::mem::forget(receiver);
            (Some(sender), RuntimeStatus::Active)
        }
        None => (None, RuntimeStatus::Uninitialized),
    };

    let pending = Arc::new(
        PendingTracker::new(&watched_dir).context("PendingTracker::new")?,
    );

    let state = Arc::new(DaemonState::new(
        daemon_paths.clone(),
        allowlist,
        pending,
        SyncState::new(),
        sync_runtime.endpoint().clone(),
        sync_runtime.blobs().clone(),
        gossip_sender_opt,
        runtime_status,
    ));

    // (11) listener spawn
    let dispatcher: Arc<dyn DynDispatcher> = Arc::new(Dispatcher::new(state.clone()));
    let listener_handle = bind_listener(
        &daemon_paths.socket_path,
        &daemon_paths.lock_path,
        dispatcher,
    )
    .await
    .context("bind_listener")?;
    tracing::info!(
        socket = %daemon_paths.socket_path.display(),
        "daemon listening"
    );

    // (12) watcher + receive_loop:
    // Phase 3 後半で正規実装 (= watcher_loop が send_file を呼ぶ wiring、
    // receive_loop が mark_peer_seen callback で DaemonState を更新する wiring)。
    // 本 step では「 daemon が RPC に応答できる骨格 」 までを完成させる。

    // (13) SIGINT / SIGTERM 待機 → graceful shutdown
    wait_for_shutdown_signal().await?;
    tracing::info!("shutdown signal received");
    listener_handle.shutdown().await.context("listener shutdown")?;
    sync_runtime
        .shutdown()
        .await
        .context("sync_runtime shutdown")?;
    tracing::info!("daemon stopped cleanly");
    Ok(())
}

fn build_daemon_paths(watched_dir: &std::path::Path) -> Result<DaemonPaths> {
    Ok(DaemonPaths {
        watched_dir_canonical: watched_dir.to_path_buf(),
        socket_path: paths::default_socket_path()?,
        lock_path: paths::default_lock_path()?,
        allowlist_path: paths::default_allowlist_path()?,
        folder_secret_path: paths::default_folder_secret_path()?,
        key_path: paths::default_key_path()?,
        blobs_dir: paths::default_blobs_dir()?,
        log_path: paths::default_log_path()?,
    })
}

fn init_tracing() -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("P2P_SYNC_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,p2p_dir_sync=debug"));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init()
        .ok(); // 重複 init は ignore
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut int_stream = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut term_stream = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    tokio::select! {
        _ = int_stream.recv() => tracing::debug!("got SIGINT"),
        _ = term_stream.recv() => tracing::debug!("got SIGTERM"),
    }
    Ok(())
}
