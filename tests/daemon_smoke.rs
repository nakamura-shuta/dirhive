//! `dirhive` daemon binary の smoke test。
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

use dirhive::daemon::client::rpc;

/// daemon binary を spawn し、 socket が準備できるまで polling する helper。
async fn spawn_daemon(
    watched: &std::path::Path,
    state_dir: &std::path::Path,
) -> (tokio::process::Child, std::path::PathBuf) {
    let bin = env!("CARGO_BIN_EXE_dirhive");
    let socket = state_dir.join("daemon.sock");

    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--watch")
        .arg(watched)
        .env("DIRHIVE_STATE_DIR", state_dir)
        .env("DIRHIVE_CONFIG_DIR", state_dir.join("config"))
        .env("DIRHIVE_LOG_DIR", state_dir.join("logs"))
        .env("DIRHIVE_LOG", "warn") // 静かめ
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let child = cmd.spawn().expect("spawn dirhive daemon");

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
    assert!(v["ticket"].as_str().unwrap().starts_with("dirhive1-"));
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

/// 2nd-round M3 review fix: daemon 直起動 (launchd 経由でない) でも `init_tracing`
/// の file appender が log file に出力し、 sync.recent-log が tail できることを
/// e2e で確認する (= 旧 smoke は人工 file の tail/redact しか検証していなかった)。
#[tokio::test]
#[ignore = "spawns daemon binary; requires N0 relay reachability"]
async fn daemon_recent_log_reads_actual_file_appender_output() {
    let watch_tmp = tempfile::TempDir::new().unwrap();
    let state_tmp = tempfile::TempDir::new().unwrap();
    let mut child = {
        let bin = env!("CARGO_BIN_EXE_dirhive");
        let mut cmd = tokio::process::Command::new(bin);
        cmd.arg("--watch")
            .arg(watch_tmp.path())
            .env("DIRHIVE_STATE_DIR", state_tmp.path())
            .env("DIRHIVE_CONFIG_DIR", state_tmp.path().join("config"))
            .env("DIRHIVE_LOG_DIR", state_tmp.path().join("logs"))
            // 「 endpoint online 」 等の info ログを確実に file に流すため
            .env("DIRHIVE_LOG", "info,dirhive=debug")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        cmd.spawn().expect("spawn dirhive")
    };

    let socket = state_tmp.path().join("daemon.sock");
    // wait until ready (~15s)
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if socket.exists()
            && rpc(&socket, "sync.health-check", serde_json::json!({}))
                .await
                .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(socket.exists(), "daemon did not become ready");

    // log file の場所を解決して中身を直接 read してみる (= file appender 動作確認)
    let log_path = state_tmp.path().join("logs").join("dirhive.log");
    assert!(
        log_path.exists(),
        "init_tracing file appender did not create log file at {}",
        log_path.display()
    );
    let on_disk = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        on_disk.contains("endpoint online") || on_disk.contains("daemon listening"),
        "log file should contain startup messages, got: {on_disk:.500}"
    );

    // sync.recent-log RPC でも同じ内容が読める (= dispatch 経由)
    let v = rpc(&socket, "sync.recent-log", serde_json::json!({"lines": 50}))
        .await
        .expect("recent-log rpc");
    let lines = v["lines"].as_array().expect("lines array");
    let joined = lines
        .iter()
        .filter_map(|l| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        joined.contains("endpoint online") || joined.contains("daemon listening"),
        "sync.recent-log should surface startup messages from file appender"
    );

    let pid = child.id().unwrap() as i32;
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let _ = tokio::time::timeout(Duration::from_secs(15), child.wait()).await;
}
