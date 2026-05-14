//! 2-peer 同期 e2e (= design.md §3.4 の bilateral invite フロー)。
//!
//! sandbox や CI 環境では N0 relay 到達 / OS sandbox 制限で動かないので
//! `#[ignore]` で marker。 実機で `cargo test --test two_peer_sync -- --ignored
//! --test-threads=1` で走らせる。
//!
//! 流れ (= design §3.4 の 7 step):
//! 1. Alice 起動 (folder-secret 不在 = Uninitialized)
//! 2. Alice: sync.invite → ticket + restart_required: true
//! 3. Alice 再起動 (= folder_secret 既存で起動 → Active)
//! 4. Bob 起動 (folder-secret 不在)
//! 5. Bob: sync.accept-invite ticket → restart_required: true、 Alice を allowlist 追加
//! 6. Bob 再起動 (= folder_secret adopt 済で起動 → Active)
//! 7. Alice: sync.allow-peer <Bob_id>
//! 8. Alice の watched_dir に file 作成 → 数秒後 Bob 側に伝搬

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use p2p_dir_sync::daemon::client::rpc;

struct Daemon {
    child: tokio::process::Child,
    socket: PathBuf,
    state_dir: tempfile::TempDir,
    watch_dir: tempfile::TempDir,
}

impl Daemon {
    async fn spawn() -> Self {
        let watch_dir = tempfile::TempDir::new().unwrap();
        let state_dir = tempfile::TempDir::new().unwrap();
        let bin = env!("CARGO_BIN_EXE_p2p-sync");
        let socket = state_dir.path().join("daemon.sock");

        let mut cmd = tokio::process::Command::new(bin);
        cmd.arg("--watch")
            .arg(watch_dir.path())
            .env("P2P_SYNC_STATE_DIR", state_dir.path())
            .env("P2P_SYNC_CONFIG_DIR", state_dir.path().join("config"))
            .env("P2P_SYNC_LOG_DIR", state_dir.path().join("logs"))
            .env("P2P_SYNC_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = cmd.spawn().expect("spawn daemon");

        // poll for readiness
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            if socket.exists()
                && rpc(&socket, "sync.health-check", serde_json::json!({}))
                    .await
                    .is_ok()
            {
                return Self {
                    child,
                    socket,
                    state_dir,
                    watch_dir,
                };
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        panic!("daemon never became ready");
    }

    /// SIGTERM + wait。
    async fn stop(mut self) -> (tempfile::TempDir, tempfile::TempDir) {
        let pid = self.child.id().expect("pid") as i32;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let _ = tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await;
        (self.state_dir, self.watch_dir)
    }

    /// 同 state_dir / watch_dir で daemon を再起動 (= invite/accept 後の reboot)。
    async fn restart_in_place(self) -> Daemon {
        let bin = env!("CARGO_BIN_EXE_p2p-sync");
        let state_dir = self.state_dir;
        let watch_dir = self.watch_dir;

        // 旧 daemon の SIGTERM
        let mut child = self.child;
        let pid = child.id().expect("pid") as i32;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let _ = tokio::time::timeout(Duration::from_secs(15), child.wait()).await;

        // 再 spawn
        let socket = state_dir.path().join("daemon.sock");
        let mut cmd = tokio::process::Command::new(bin);
        cmd.arg("--watch")
            .arg(watch_dir.path())
            .env("P2P_SYNC_STATE_DIR", state_dir.path())
            .env("P2P_SYNC_CONFIG_DIR", state_dir.path().join("config"))
            .env("P2P_SYNC_LOG_DIR", state_dir.path().join("logs"))
            .env("P2P_SYNC_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = cmd.spawn().expect("respawn daemon");

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            if socket.exists()
                && rpc(&socket, "sync.health-check", serde_json::json!({}))
                    .await
                    .is_ok()
            {
                return Daemon {
                    child,
                    socket,
                    state_dir,
                    watch_dir,
                };
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        panic!("daemon never became ready after restart");
    }
}

/// `watch_dir/rel` に内容が `expected` で現れるまで polling する。
async fn wait_for_file(watch_dir: &Path, rel: &str, expected: &[u8], timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    let path = watch_dir.join(rel);
    while std::time::Instant::now() < deadline {
        if path.exists()
            && let Ok(b) = std::fs::read(&path)
            && b == expected
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

#[tokio::test]
#[ignore = "2 peer e2e: requires N0 relay + ~60s; run with --ignored"]
async fn two_peer_invite_accept_file_sync() {
    // (1) Alice 起動 (Uninitialized)
    let alice = Daemon::spawn().await;

    // (2) Alice invite
    let inv = rpc(&alice.socket, "sync.invite", serde_json::json!({}))
        .await
        .expect("alice invite");
    let ticket = inv["ticket"].as_str().unwrap().to_string();
    assert_eq!(inv["restart_required"], true);

    // (3) Alice 再起動 → Active
    let alice = alice.restart_in_place().await;
    let h = rpc(&alice.socket, "sync.health-check", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(h["dynamic_info"]["gossip_subscribed"], true);

    // (4) Bob 起動
    let bob = Daemon::spawn().await;

    // (5) Bob accept-invite
    let ar = rpc(
        &bob.socket,
        "sync.accept-invite",
        serde_json::json!({ "ticket": ticket, "label": "alice" }),
    )
    .await
    .expect("bob accept");
    assert_eq!(ar["restart_required"], true);
    let bob_id = ar["my_peer_id"].as_str().unwrap().to_string();

    // (6) Bob 再起動 → Active
    let bob = bob.restart_in_place().await;
    let h2 = rpc(&bob.socket, "sync.health-check", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(h2["dynamic_info"]["gossip_subscribed"], true);

    // (7) Alice: sync.allow-peer <Bob_id>
    let ap = rpc(
        &alice.socket,
        "sync.allow-peer",
        serde_json::json!({ "peer_id": bob_id, "label": "bob" }),
    )
    .await
    .expect("alice allow-peer");
    assert_eq!(ap["added"], true);

    // (8) Alice 側 watched_dir に file を作る
    let alice_watch = alice.watch_dir.path().to_path_buf();
    let bob_watch = bob.watch_dir.path().to_path_buf();
    std::fs::write(alice_watch.join("hello.md"), b"hello from alice").unwrap();

    // Bob 側に伝搬まで待つ
    let propagated = wait_for_file(
        &bob_watch,
        "hello.md",
        b"hello from alice",
        Duration::from_secs(45),
    )
    .await;
    assert!(propagated, "alice's edit did not reach bob within 45s");

    // cleanup
    let _ = alice.stop().await;
    let _ = bob.stop().await;
}
