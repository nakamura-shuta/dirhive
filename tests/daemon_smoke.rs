//! `p2p-sync` daemon binary の smoke test。
//!
//! - daemon を spawn (= --watch + state-dir env override)
//! - sync.health-check probe で起動完了を確認
//! - sync.status / sync.invite を round-trip
//! - SIGTERM で graceful shutdown
//!
//! Phase 3 review への直接的な response: 「 daemon が CLI から起動して RPC に
//! 応答する 」 を 1 fixture で押さえる integration test。

use std::process::Stdio;
use std::time::Duration;

use p2p_dir_sync::daemon::client::rpc;

/// daemon binary を spawn し、 socket が準備できるまで polling する helper。
async fn spawn_daemon(
    watched: &std::path::Path,
    state_dir: &std::path::Path,
) -> (tokio::process::Child, std::path::PathBuf) {
    let bin = env!("CARGO_BIN_EXE_p2p-sync");
    let socket = state_dir.join("daemon.sock");

    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--watch")
        .arg(watched)
        .env("P2P_SYNC_STATE_DIR", state_dir)
        .env("P2P_SYNC_CONFIG_DIR", state_dir.join("config"))
        .env("P2P_SYNC_LOG_DIR", state_dir.join("logs"))
        .env("P2P_SYNC_LOG", "warn") // 静かめ
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let child = cmd.spawn().expect("spawn p2p-sync daemon");

    // socket 出現まで polling (最大 15s)
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if socket.exists() {
            // socket が出来た直後はまだ accept_loop spawn 前の可能性あり。 連続して
            // health-check が成功するまで再試行。
            if rpc(&socket, "sync.health-check", serde_json::json!({}))
                .await
                .is_ok()
            {
                return (child, socket);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("daemon did not become ready within 15s");
}

#[tokio::test]
#[ignore = "spawns daemon binary; requires N0 relay reachability"]
async fn daemon_starts_and_responds_to_health_check() {
    let watch_tmp = tempfile::TempDir::new().unwrap();
    let state_tmp = tempfile::TempDir::new().unwrap();

    let (mut child, socket) = spawn_daemon(watch_tmp.path(), state_tmp.path()).await;

    // health-check 応答内容を確認
    // M2 review fix: static fields は top-level flatten、 dynamic_info だけ nested。
    let v = rpc(&socket, "sync.health-check", serde_json::json!({}))
        .await
        .expect("health-check rpc");
    assert!(v["watched_dir_exists"].as_bool().unwrap());
    assert!(v["key_path"].is_string());
    // folder_secret 未生成 → group_initialized = false
    assert_eq!(v["dynamic_info"]["group_initialized"], false);

    // status も応答する
    let s = rpc(&socket, "sync.status", serde_json::json!({}))
        .await
        .expect("status rpc");
    assert_eq!(s["peer_count"], 0);
    assert_eq!(s["open_all"], false);

    // SIGTERM → graceful shutdown
    let pid = child.id().expect("child pid") as i32;
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let status =
        tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .expect("daemon must stop within 15s")
            .expect("daemon wait");
    assert!(status.success() || status.code().is_some());
    assert!(!socket.exists(), "socket must be cleaned up on shutdown");
}

#[tokio::test]
#[ignore = "spawns daemon binary; requires N0 relay reachability"]
async fn daemon_invite_first_call_sets_restart_required() {
    let watch_tmp = tempfile::TempDir::new().unwrap();
    let state_tmp = tempfile::TempDir::new().unwrap();
    let (mut child, socket) = spawn_daemon(watch_tmp.path(), state_tmp.path()).await;

    let v = rpc(&socket, "sync.invite", serde_json::json!({}))
        .await
        .expect("invite rpc");
    assert!(v["ticket"].as_str().unwrap().starts_with("p2psync1-"));
    assert_eq!(v["restart_required"], true);

    // 2 回目: 既存 secret を使うが daemon は再起動してない (=
    // InitializedButNotSubscribed のまま) → restart_required も true のまま
    // (= H1 review fix: 7-step bilateral flow の再起動指示を消さない)
    let v2 = rpc(&socket, "sync.invite", serde_json::json!({}))
        .await
        .expect("invite second");
    assert_eq!(v2["restart_required"], true);
    assert_eq!(v["ticket"], v2["ticket"]);

    let pid = child.id().unwrap() as i32;
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let _ = tokio::time::timeout(Duration::from_secs(15), child.wait()).await;
}
