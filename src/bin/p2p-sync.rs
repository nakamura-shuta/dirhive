//! p2p-sync daemon binary (= L2 daemon、 design.md §5.1)。
//!
//! ## 起動シーケンス (= design §5.1 の順序を遵守)
//!
//! 1. CLI arg parse
//! 2. watched_dir canonicalize
//! 3. paths 解決 + log file 用 dir 用意
//! 4. tracing 初期化 (= stderr + log file の両方に出力、 M3)
//! 5. **早期** daemon_lock 取得 + stale socket recover (= M4: Iroh bind より前)
//! 6. endpoint.key load / generate
//! 7. folder_secret lazy load
//! 8. Endpoint::bind + online
//! 9. blobs_dir 0o700 prepare
//! 10. allowlist load (file 不在 → strict empty、 --allow-open-all で open_all)
//! 11. SyncRuntime::build
//! 12. DaemonState 構築
//! 13. listener spawn (`bind_listener_with_lock` で early lock を pass-through)
//! 14. (Phase 3 後半で wire) watcher + receive_loop
//! 15. SIGINT / SIGTERM 待機 → graceful shutdown

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use p2p_dir_sync::allowlist::AllowList;
use p2p_dir_sync::daemon::dispatch::Dispatcher;
use p2p_dir_sync::daemon::listener::{
    DynDispatcher, acquire_daemon_lock, bind_listener_with_lock, probe_existing_socket,
};
use p2p_dir_sync::daemon::state::{DaemonPaths, DaemonState, RuntimeStatus};
use p2p_dir_sync::keystore;
use p2p_dir_sync::paths;
use p2p_dir_sync::runtime::SyncRuntime;
use p2p_dir_sync::state::{PendingTracker, SyncState};
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

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

    // (3) paths 解決
    let daemon_paths = build_daemon_paths(&watched_dir)?;

    // (4) tracing 初期化 (stderr + log file、 sync.recent-log が tail する file)
    init_tracing(&daemon_paths.log_path)?;

    if cli.allow_open_all {
        tracing::warn!(
            "--allow-open-all enabled: this daemon will accept blob fetch from ANY \
             peer that knows folder_secret. Use only for development."
        );
        eprintln!("WARNING: --allow-open-all is set; this is a dev-only mode.");
    }

    // (5) **早期** lock + stale recover (= M4 review fix)。
    // Iroh の relay 接続 / endpoint key 生成 など side effect の起きる処理より
    // 前に多重起動 check を済ませ、 2 個目の daemon は network に触らずに exit する。
    let lock_file = acquire_daemon_lock(&daemon_paths.lock_path)
        .context("acquire daemon.lock (another daemon may already be running)")?;
    if daemon_paths.socket_path.exists() {
        if probe_existing_socket(&daemon_paths.socket_path).await {
            anyhow::bail!(
                "another daemon is already listening on {}",
                daemon_paths.socket_path.display()
            );
        }
        std::fs::remove_file(&daemon_paths.socket_path).with_context(|| {
            format!("unlink stale socket {}", daemon_paths.socket_path.display())
        })?;
        tracing::info!(
            socket = %daemon_paths.socket_path.display(),
            "removed stale socket"
        );
    }

    // (6) endpoint.key
    let secret_key = keystore::load_or_create_endpoint_key(&daemon_paths.key_path)
        .context("load or create endpoint.key")?;

    // (7) folder_secret lazy load
    let folder_secret = keystore::try_load_folder_secret(&daemon_paths.folder_secret_path)
        .context("try_load_folder_secret")?;

    // (8) Endpoint::bind + online
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .secret_key(secret_key)
        .bind()
        .await
        .context("Endpoint::bind")?;
    endpoint.online().await;
    tracing::info!(endpoint_id = %endpoint.id(), "endpoint online");

    // (9) blobs_dir 0o700
    paths::ensure_dir_700(&daemon_paths.blobs_dir)?;

    // (10) allowlist load
    let allowlist = if cli.allow_open_all {
        Arc::new(AllowList::open_all())
    } else {
        Arc::new(
            AllowList::load_or_strict_empty(&daemon_paths.allowlist_path)
                .context("load allowlist")?,
        )
    };

    // (11) SyncRuntime::build
    let mut sync_runtime = SyncRuntime::build(
        endpoint.clone(),
        &daemon_paths.blobs_dir,
        allowlist.clone(),
        folder_secret.as_ref(),
    )
    .await
    .context("SyncRuntime::build")?;

    // (12) DaemonState 構築
    let (gossip_sender_opt, runtime_status) = match folder_secret {
        Some(_) => {
            let topic = sync_runtime
                .take_topic()
                .context("SyncRuntime::take_topic (group_initialized なのに None)")?;
            let (sender, receiver) = topic.split();
            // receiver は Phase 3 後半で receive_loop に渡す予定 (= ここでは hold する)
            // 一旦 leak で hold (= drop すると topic から leave してしまう)
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

    // (13) listener spawn (early lock を pass-through)
    let dispatcher: Arc<dyn DynDispatcher> = Arc::new(Dispatcher::new(state.clone()));
    let listener_handle = bind_listener_with_lock(
        &daemon_paths.socket_path,
        lock_file,
        dispatcher,
    )
    .await
    .context("bind_listener_with_lock")?;
    tracing::info!(
        socket = %daemon_paths.socket_path.display(),
        "daemon listening"
    );

    // (14) Phase 3 後半で watcher + receive_loop を spawn する。

    // (15) SIGINT / SIGTERM 待機 → graceful shutdown
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

fn build_daemon_paths(watched_dir: &Path) -> Result<DaemonPaths> {
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

/// stderr と log file の両方に出力する tracing initialization (= M3 review fix)。
/// sync.recent-log が log_path を tail するので、 launchd で stderr redirect しない
/// 環境 (= binary 直起動 / integration test) でも log entry が file に残る。
fn init_tracing(log_path: &Path) -> Result<()> {
    let filter = EnvFilter::try_from_env("P2P_SYNC_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,p2p_dir_sync=debug"));

    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create log parent {}", parent.display()))?;
    }
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| format!("open log file {}", log_path.display()))?;
    let shared = SharedFileWriter(Arc::new(Mutex::new(log_file)));

    let stderr_layer = fmt::layer()
        .with_target(false)
        .with_writer(io::stderr);

    let file_layer = fmt::layer()
        .with_target(false)
        .with_ansi(false)
        .with_writer(shared);

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer)
        .try_init()
        .map_err(|e| anyhow!("tracing init: {e}"))?;
    Ok(())
}

/// 1 個の `File` を複数 layer / 複数 event で共有するための `MakeWriter` 実装。
#[derive(Clone)]
struct SharedFileWriter(Arc<Mutex<File>>);

impl<'a> MakeWriter<'a> for SharedFileWriter {
    type Writer = SharedFileGuard<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard(self.0.lock().expect("log file mutex poisoned"))
    }
}

struct SharedFileGuard<'a>(MutexGuard<'a, File>);

impl Write for SharedFileGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
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
