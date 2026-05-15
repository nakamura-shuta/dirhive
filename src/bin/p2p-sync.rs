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
use p2p_dir_sync::bootstrap_peers;
use p2p_dir_sync::daemon::dispatch::Dispatcher;
use p2p_dir_sync::daemon::listener::{
    DynDispatcher, acquire_daemon_lock, bind_listener_with_lock, probe_existing_socket,
};
use p2p_dir_sync::daemon::state::{DaemonPaths, DaemonState, RuntimeStatus};
use p2p_dir_sync::keystore;
use p2p_dir_sync::message::PeerRef;
use p2p_dir_sync::paths;
use p2p_dir_sync::receive::{OnNeighborUpCallback, PeerSeenCallback, receive_loop};
use p2p_dir_sync::runtime::SyncRuntime;
use p2p_dir_sync::send::{broadcast_tombstone, send_file};
use p2p_dir_sync::state::{PendingTracker, SyncState};
use p2p_dir_sync::watcher::{WatcherBackend, spawn_watcher, watcher_loop};
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
    // bootstrap_peers (= H1 review fix): accept-invite で永続化された inviter の
    // EndpointAddr を load し、 起動時 gossip.subscribe + address_lookup 登録に渡す。
    let bootstrap = bootstrap_peers::load_bootstrap_peers(&daemon_paths.bootstrap_peers_path)
        .context("load bootstrap-peers.json")?;
    if !bootstrap.is_empty() {
        tracing::info!(count = bootstrap.len(), "loaded bootstrap peers");
    }
    // blob serve callback (= Phase 3 review M3): DaemonState を取得した後に
    // 改めて register したいが、 AllowlistBlobs は Router 構築時に確定する必要が
    // あるので「 Arc<OnceLock<...>> 経由 」 にして後から実体を差し込む形にする。
    let mark_peer_seen_slot: Arc<std::sync::OnceLock<Arc<DaemonState>>> =
        Arc::new(std::sync::OnceLock::new());
    let slot_for_cb = mark_peer_seen_slot.clone();
    let serve_cb: p2p_dir_sync::allowlist_blobs::BlobServeCallback =
        Arc::new(move |peer, t| {
            if let Some(state) = slot_for_cb.get() {
                state.mark_peer_seen(peer, t);
            }
        });

    let mut sync_runtime = SyncRuntime::build_with_serve_callback(
        endpoint.clone(),
        &daemon_paths.blobs_dir,
        allowlist.clone(),
        folder_secret.as_ref(),
        bootstrap,
        Some(serve_cb),
    )
    .await
    .context("SyncRuntime::build")?;

    // (12) DaemonState 構築 + topic split (sender → DaemonState、 receiver は後で
    // receive_loop に渡す)。
    let (gossip_sender_opt, gossip_receiver_opt, runtime_status) = match folder_secret {
        Some(_) => {
            let topic = sync_runtime
                .take_topic()
                .context("SyncRuntime::take_topic (group_initialized なのに None)")?;
            let (sender, receiver) = topic.split();
            (Some(sender), Some(receiver), RuntimeStatus::Active)
        }
        None => (None, None, RuntimeStatus::Uninitialized),
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
    // blob serve callback の slot に DaemonState を差し込む (= 起動後の serve
    // 成功で mark_peer_seen が呼ばれる)。
    mark_peer_seen_slot
        .set(state.clone())
        .map_err(|_| anyhow!("mark_peer_seen_slot already set"))?;

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

    // (14) watcher + receive_loop spawn (= Phase 3 Step 3g)。
    // group_initialized のとき (= gossip_receiver_opt が Some) のみ実行。
    let mut bg_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let Some(receiver) = gossip_receiver_opt {
        // (14a) receive_loop: peer から SyncUpdate を受信 → local fs に適用
        // mark_peer_seen callback で last_seen_at を更新 (= L4)
        let state_for_cb = state.clone();
        let peer_seen_cb: PeerSeenCallback =
            std::sync::Arc::new(move |peer, t| state_for_cb.mark_peer_seen(peer, t));

        // on_neighbor_up callback (= Phase 3 review H1): 初回 NeighborUp で
        // pending_initial_broadcasts を drain して send_file を発火する。
        // 2 回目以降の NeighborUp (= peer 追加 / 再接続) では何もしない。
        let state_for_njup = state.clone();
        let watched_for_njup = watched_dir.clone();
        let sender_for_njup = state
            .gossip_sender_cloned()
            .context("gossip_sender must be Some when runtime_status = Active")?;
        let self_peer_for_njup = PeerRef { id: state.self_endpoint_id };
        let njup_cb: OnNeighborUpCallback = std::sync::Arc::new(move |peer| {
            if !state_for_njup.mark_first_join() {
                return; // 既に join 済 = 何もしない
            }
            tracing::info!(
                peer = %peer.fmt_short(),
                "first gossip neighbor up; flushing pending initial broadcasts"
            );
            let pending = state_for_njup.drain_pending_initial();
            if pending.is_empty() {
                return;
            }
            // tokio::spawn で send_file を逐次呼ぶ (= callback 自体は同期境界)
            let state = state_for_njup.clone();
            let watched = watched_for_njup.clone();
            let sender = sender_for_njup.clone();
            let self_peer = self_peer_for_njup.clone();
            tokio::spawn(async move {
                for rel in pending {
                    let Some(rel_str) = rel.to_str().map(|s| s.to_string()) else {
                        continue;
                    };
                    let abs_target = watched.join(&rel);
                    if !abs_target.exists() {
                        // 既に削除済 = tombstone broadcast
                        if let Err(e) = broadcast_tombstone(
                            &rel_str,
                            &sender,
                            self_peer.clone(),
                            &state.sync_state,
                        )
                        .await
                        {
                            tracing::warn!(
                                path = %rel_str,
                                "delayed tombstone broadcast failed: {e:#}"
                            );
                        }
                    } else if let Err(e) = send_file(
                        &rel_str,
                        &watched,
                        &state.blobs,
                        &sender,
                        self_peer.clone(),
                        &state.sync_state,
                    )
                    .await
                    {
                        tracing::warn!(path = %rel_str, "delayed send_file failed: {e:#}");
                    }
                }
            });
        });

        let allowlist_for_recv = state.allowlist.clone();
        let pending_for_recv = state.pending.clone();
        let state_for_recv = state.sync_state.clone();
        let watched_for_recv = watched_dir.clone();
        let endpoint_for_recv = state.endpoint.clone();
        let blobs_for_recv = state.blobs.clone();
        bg_tasks.push(tokio::spawn(async move {
            if let Err(e) = receive_loop(
                receiver,
                endpoint_for_recv,
                blobs_for_recv,
                allowlist_for_recv,
                state_for_recv,
                watched_for_recv,
                pending_for_recv,
                Some(peer_seen_cb),
                Some(njup_cb),
            )
            .await
            {
                tracing::warn!("receive_loop exited with error: {e:#}");
            }
        }));

        // (14b) watcher: fsnotify event → send_file / broadcast_tombstone
        // gossip_sender は DaemonState 経由で clone (= invariant 上 Active なら Some)
        let sender = state
            .gossip_sender_cloned()
            .context("gossip_sender must be Some when runtime_status = Active")?;
        let (watcher_handle, watcher_rx) = spawn_watcher(&watched_dir, WatcherBackend::Recommended)
            .context("spawn_watcher")?;
        let watched_for_w = watched_dir.clone();
        let state_for_w = state.clone();
        let self_peer = PeerRef { id: state.self_endpoint_id };

        bg_tasks.push(tokio::spawn(async move {
            let _hold = watcher_handle; // drop で debouncer 停止
            let _ = watcher_loop(watcher_rx, watched_for_w.clone(), |ev, rel| {
                let watched = watched_for_w.clone();
                let state = state_for_w.clone();
                let sender = sender.clone();
                let self_peer = self_peer.clone();
                async move {
                    use notify_debouncer_full::notify::EventKind;
                    let rel_str = match rel.to_str() {
                        Some(s) => s.to_string(),
                        None => {
                            tracing::warn!("rel path not UTF-8: {}", rel.display());
                            return;
                        }
                    };
                    let abs_target = watched.join(&rel);

                    // === self-loop 防止: last_written marker を **必ず remove** する
                    // (= Phase 3 review M2)。 旧 logic は hash 一致時のみ consume だったので、
                    // hash mismatch / read error で marker が残り後続の正当 edit を
                    // 誤 suppress する穴があった。 一度 take して、 hash 一致なら suppress、
                    // 不一致 / read error ならそのまま send 経路へ進む (= one-shot 性確保)。
                    let prev_hash = state
                        .sync_state
                        .last_written
                        .lock()
                        .expect("lock")
                        .remove(&rel);
                    let suppress_self_loop = if let Some(expected) = prev_hash {
                        if abs_target.exists() {
                            match std::fs::read(&abs_target) {
                                Ok(bytes) => iroh_blobs::Hash::new(&bytes) == expected,
                                Err(_) => false,
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if suppress_self_loop {
                        tracing::debug!(
                            path = %rel_str,
                            "watcher Modify suppressed (= self-loop, hash match)"
                        );
                        return;
                    }

                    if matches!(ev.event.kind, EventKind::Remove(_)) || !abs_target.exists() {
                        // self-loop 防止: last_removed に入っているなら consume + skip
                        let suppress = {
                            let mut g = state.sync_state.last_removed.lock().expect("lock");
                            g.remove(&rel)
                        };
                        if suppress {
                            tracing::debug!(
                                path = %rel_str,
                                "watcher Remove suppressed (= self-loop from receive)"
                            );
                            return;
                        }

                        // H1 review fix: 初回 join 待ちなら queue へ
                        if state.enqueue_initial_broadcast(rel.clone()) {
                            tracing::debug!(
                                path = %rel_str,
                                "queued initial Tombstone (gossip not joined yet)"
                            );
                            return;
                        }

                        if let Err(e) =
                            broadcast_tombstone(&rel_str, &sender, self_peer, &state.sync_state)
                                .await
                        {
                            tracing::warn!(path = %rel_str, "broadcast_tombstone failed: {e:#}");
                        }
                    } else {
                        // H1 review fix: 初回 join 待ちなら queue へ
                        if state.enqueue_initial_broadcast(rel.clone()) {
                            tracing::debug!(
                                path = %rel_str,
                                "queued initial Upsert (gossip not joined yet)"
                            );
                            return;
                        }

                        if let Err(e) = send_file(
                            &rel_str,
                            &watched,
                            &state.blobs,
                            &sender,
                            self_peer,
                            &state.sync_state,
                        )
                        .await
                        {
                            tracing::warn!(path = %rel_str, "send_file failed: {e:#}");
                        }
                    }
                }
            })
            .await;
        }));
    } else {
        tracing::info!(
            "daemon started uninitialized (folder_secret absent). \
             Call sync.invite or sync.accept-invite then restart to join mesh."
        );
    }

    // (15) SIGINT / SIGTERM 待機 → graceful shutdown
    wait_for_shutdown_signal().await?;
    tracing::info!("shutdown signal received");
    listener_handle.shutdown().await.context("listener shutdown")?;
    // bg_tasks (watcher / receive_loop) は abort して exit を急ぐ。
    // 個別 spawn の future が drop されると notify debouncer / gossip receiver が
    // 内部 cancellation で停止する。
    for t in bg_tasks {
        t.abort();
    }
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
        bootstrap_peers_path: paths::default_bootstrap_peers_path()?,
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
