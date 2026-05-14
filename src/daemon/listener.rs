//! Unix socket listener (= daemon の RPC 入口、 design.md §5.1 step 10)。
//!
//! 機能:
//! - bind + chmod 0o600 (= 同 uid 以外を遮断)
//! - 多重起動防止: `daemon.lock` を `flock(LOCK_EX|LOCK_NB)` で握りっぱなしにする
//! - stale socket recover: 既存 socket に `sync.health-check` を 1s timeout で
//!   投げ、 応答あれば exit 1、 timeout / ECONNREFUSED なら stale → unlink
//! - newline-delimited JSON (= 1 connection 1 RPC × N の単純化)
//! - MAX_REQUEST_BYTES = 1 MiB cap (= design §6.2 DoS 防止)
//! - graceful shutdown: cancellation token + Drop で socket file unlink
//!
//! dispatch (= sync.* RPC の実処理) は `daemon/dispatch.rs` に分離。 本 module
//! は「 受信 RpcRequest を dispatcher に渡し、 返値を newline JSON で返す 」 だけ。

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 1 RPC body の上限 (= design §6.2 DoS 防止)。
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// `sync.health-check` probe の timeout。 stale socket recover で使う。
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// 1 connection あたりの read timeout (= Phase 3 review M2)。
///
/// MCP / CLI / 統合 test が誤動作して newline を送らないケースで fd / task が
/// 溜まるのを防ぐ。 same-uid threat 外でも防御 (= dispatch が iroh stack を
/// 叩いて時間かかる前提で十分余裕を取る)。
pub const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// 同時 accept 中 connection の上限 (= Phase 3 review M2)。
///
/// MCP / CLI からの呼び出しは普段 1-2 connection でしか到来しない。
/// 32 は「 burst で来ても捌けて、 fd 制限内に収まる 」 妥協値。
/// 超過した accept は connection を即 close する (= response 返さない)。
pub const MAX_IN_FLIGHT_CONNECTIONS: usize = 32;

/// JSON-RPC-lite request (= `method` + `params` の 2 field、 id は持たない)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcRequest {
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// response。 `result` か `error` のどちらかが入る (= JSON で `null` の方は省略)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RpcResponse {
    pub fn ok(result: serde_json::Value) -> Self {
        Self { result: Some(result), error: None }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self { result: None, error: Some(msg.into()) }
    }
}

/// dispatcher: `RpcRequest` を受けて `RpcResponse` を返す async fn の trait 表現。
///
/// 実体は `daemon/dispatch.rs::Dispatcher` (= DaemonState を内部 hold)。
/// listener は dispatch 内容を知らずに dyn trait 越しに呼ぶ。
pub trait DynDispatcher: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        req: RpcRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RpcResponse> + Send + 'a>>;
}

/// listener のハンドル。 drop 時 socket は cleanup される。 lock file は drop で
/// OS が flock を release → 別 daemon が起動可能になる。
///
/// `task` / `_lock_file` を `Option` で持つのは、 shutdown が `&mut self` で
/// 取り出した後に Drop が走っても二重 take しないため (= Drop impl と共存)。
pub struct ListenerHandle {
    task: Option<JoinHandle<()>>,
    pub shutdown: CancellationToken,
    socket_path: PathBuf,
    _lock_file: Option<std::fs::File>,
}

impl std::fmt::Debug for ListenerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListenerHandle")
            .field("socket_path", &self.socket_path)
            .finish_non_exhaustive()
    }
}

impl ListenerHandle {
    /// graceful shutdown (= design §5.4 step 1)。
    /// `self` を consume したい が Drop 共存のため `&mut self` を取る。
    /// 呼び出し後の handle は drop してよい (= socket unlink + lock release は
    /// drop が引き継ぐ)。
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for ListenerHandle {
    fn drop(&mut self) {
        // shutdown を呼ばない経路 (panic / 早期 return) の cleanup。
        // task は abort する (= まだ動いていれば)。
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.socket_path);
        // _lock_file は Option<File> の drop が flock release を行う
    }
}

/// daemon の lock file を取得する。 `LOCK_EX | LOCK_NB` 相当を `File::try_lock`
/// 経由で取得する。 既に他 process が握っていれば error。
///
/// File は handle として返し、 caller (= `bind_listener`) が daemon 寿命の間
/// hold する。 drop で OS が自動 release。
pub fn acquire_daemon_lock(lock_path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = lock_path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .with_context(|| format!("open lock file {}", lock_path.display()))?;
    let mut perm = file.metadata()?.permissions();
    perm.set_mode(0o600);
    file.set_permissions(perm)?;
    file.try_lock()
        .map_err(|e| anyhow!("another daemon holds {}: {e}", lock_path.display()))?;
    Ok(file)
}

/// 既存 socket に sync.health-check probe を投げ、 応答あれば true (= live daemon)。
/// connect 失敗 / 1s timeout / EOF なら false (= stale socket と判定)。
pub async fn probe_existing_socket(socket_path: &Path) -> bool {
    let result = tokio::time::timeout(PROBE_TIMEOUT, async {
        let mut stream = UnixStream::connect(socket_path).await.ok()?;
        let req = RpcRequest {
            method: "sync.health-check".into(),
            params: serde_json::json!({}),
        };
        let mut body = serde_json::to_vec(&req).ok()?;
        body.push(b'\n');
        stream.write_all(&body).await.ok()?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        let n = reader.read_line(&mut line).await.ok()?;
        if n == 0 {
            return None;
        }
        Some(line)
    })
    .await;
    matches!(result, Ok(Some(_)))
}

/// socket bind + chmod 0o600 + accept_loop spawn。
pub async fn bind_listener(
    socket_path: &Path,
    lock_path: &Path,
    dispatcher: Arc<dyn DynDispatcher>,
) -> Result<ListenerHandle> {
    // 1. flock を握る (= socket probe より先に、 race を狭めるため)
    let lock_file = acquire_daemon_lock(lock_path)?;

    // 2. stale socket recover
    if socket_path.exists() {
        if probe_existing_socket(socket_path).await {
            return Err(anyhow!(
                "another daemon is already listening on {}",
                socket_path.display()
            ));
        }
        std::fs::remove_file(socket_path)
            .with_context(|| format!("unlink stale socket {}", socket_path.display()))?;
    }

    // 3. parent dir + bind + chmod 0o600
    if let Some(parent) = socket_path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("bind {}", socket_path.display()))?;
    let mut perm = std::fs::metadata(socket_path)?.permissions();
    perm.set_mode(0o600);
    std::fs::set_permissions(socket_path, perm)?;

    // 4. accept_loop spawn (= connection cap semaphore を共有)
    let shutdown = CancellationToken::new();
    let shutdown2 = shutdown.clone();
    let socket_path_buf = socket_path.to_path_buf();
    let conn_sem = Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONNECTIONS));
    let task = tokio::spawn(async move {
        accept_loop(listener, dispatcher, shutdown2, conn_sem).await;
    });

    Ok(ListenerHandle {
        task: Some(task),
        shutdown,
        socket_path: socket_path_buf,
        _lock_file: Some(lock_file),
    })
}

async fn accept_loop(
    listener: UnixListener,
    dispatcher: Arc<dyn DynDispatcher>,
    shutdown: CancellationToken,
    conn_sem: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::debug!("listener accept_loop received shutdown");
                return;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, _)) => {
                        // semaphore で同時 in-flight connection 数を制限。
                        // try_acquire_owned で「 即取れなければ drop 」。
                        let permit = match conn_sem.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                tracing::warn!(
                                    cap = MAX_IN_FLIGHT_CONNECTIONS,
                                    "connection cap exceeded; dropping new connection"
                                );
                                drop(stream); // close immediately
                                continue;
                            }
                        };
                        let d = dispatcher.clone();
                        tokio::spawn(async move {
                            let _permit = permit; // hold for lifetime of this task
                            if let Err(e) = handle_one(stream, d).await {
                                tracing::warn!("rpc handler error: {e:#}");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept failed: {e}");
                    }
                }
            }
        }
    }
}

async fn handle_one(stream: UnixStream, dispatcher: Arc<dyn DynDispatcher>) -> Result<()> {
    let (rx, mut tx) = stream.into_split();
    let mut reader = BufReader::new(rx);
    let mut line = String::new();

    // read 全体に CONNECTION_READ_TIMEOUT を被せる (= newline を送らない client
    // による fd / task 蓄積を防ぐ、 Phase 3 review M2)。
    // 上限 MAX_REQUEST_BYTES + 1 まで読み (= 超過なら request too large)。
    let read_fut = async {
        let mut limited = (&mut reader).take(MAX_REQUEST_BYTES as u64 + 1);
        limited.read_line(&mut line).await
    };
    let n = match tokio::time::timeout(CONNECTION_READ_TIMEOUT, read_fut).await {
        Ok(r) => r.context("read RPC line")?,
        Err(_) => {
            // timeout: best-effort で error response を送り、 connection close
            let resp = RpcResponse::err(format!(
                "request read timed out after {}s",
                CONNECTION_READ_TIMEOUT.as_secs()
            ));
            let _ = send_response(&mut tx, &resp).await;
            return Ok(());
        }
    };
    if n == 0 {
        return Ok(()); // EOF (= client が即切断)
    }
    if line.len() > MAX_REQUEST_BYTES {
        let resp = RpcResponse::err("request too large");
        send_response(&mut tx, &resp).await?;
        return Ok(());
    }

    let req: RpcRequest = match serde_json::from_str(line.trim_end_matches('\n')) {
        Ok(r) => r,
        Err(e) => {
            let resp = RpcResponse::err(format!("invalid JSON request: {e}"));
            send_response(&mut tx, &resp).await?;
            return Ok(());
        }
    };

    let resp = dispatcher.dispatch(req).await;
    send_response(&mut tx, &resp).await?;
    Ok(())
}

async fn send_response(
    w: &mut (impl AsyncWriteExt + Unpin),
    resp: &RpcResponse,
) -> Result<()> {
    let mut body = serde_json::to_vec(resp).context("serialize RpcResponse")?;
    body.push(b'\n');
    w.write_all(&body).await.context("write response")?;
    w.flush().await.context("flush response")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;

    struct EchoDispatcher;
    impl DynDispatcher for EchoDispatcher {
        fn dispatch<'a>(
            &'a self,
            req: RpcRequest,
        ) -> Pin<Box<dyn std::future::Future<Output = RpcResponse> + Send + 'a>> {
            Box::pin(async move {
                if req.method == "sync.health-check" {
                    RpcResponse::ok(serde_json::json!({"ok": true}))
                } else {
                    RpcResponse::ok(serde_json::json!({"method": req.method, "params": req.params}))
                }
            })
        }
    }

    fn fresh_paths(prefix: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join(format!("{prefix}.sock"));
        let lock = tmp.path().join(format!("{prefix}.lock"));
        (tmp, sock, lock)
    }

    #[test]
    fn max_request_bytes_is_1mib() {
        assert_eq!(MAX_REQUEST_BYTES, 1024 * 1024);
    }

    #[test]
    fn rpc_response_serializes_with_only_one_field() {
        let ok = RpcResponse::ok(serde_json::json!({"x": 1}));
        let s = serde_json::to_string(&ok).unwrap();
        assert!(s.contains("\"result\""));
        assert!(!s.contains("\"error\""));

        let err = RpcResponse::err("boom");
        let s = serde_json::to_string(&err).unwrap();
        assert!(s.contains("\"error\""));
        assert!(!s.contains("\"result\""));
    }

    #[test]
    fn rpc_request_parses_with_default_params() {
        let r: RpcRequest = serde_json::from_str(r#"{"method":"sync.ping"}"#).unwrap();
        assert_eq!(r.method, "sync.ping");
        assert!(r.params.is_null());
    }

    #[tokio::test]
    async fn acquire_daemon_lock_rejects_second_lock() {
        let (_tmp, _sock, lock) = fresh_paths("lock1");
        let f1 = acquire_daemon_lock(&lock).unwrap();
        let e = acquire_daemon_lock(&lock).unwrap_err();
        assert!(format!("{e:#}").contains("another daemon holds"));
        drop(f1);
        // f1 を drop すれば再取得できる
        let _f2 = acquire_daemon_lock(&lock).unwrap();
    }

    #[tokio::test]
    async fn bind_listener_chmods_socket_0o600() {
        let (_tmp, sock, lock) = fresh_paths("perm");
        let handle =
            bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();
        let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        handle.shutdown().await.unwrap();
        assert!(!sock.exists(), "socket must be unlinked on shutdown");
    }

    #[tokio::test]
    async fn rpc_round_trip_echo() {
        let (_tmp, sock, lock) = fresh_paths("echo");
        let handle =
            bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let mut stream = UnixStream::connect(&sock).await.unwrap();
        let req = RpcRequest {
            method: "sync.health-check".into(),
            params: serde_json::json!({}),
        };
        let mut body = serde_json::to_vec(&req).unwrap();
        body.push(b'\n');
        stream.write_all(&body).await.unwrap();
        stream.flush().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let resp: RpcResponse = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(resp.result, Some(serde_json::json!({"ok": true})));
        assert!(resp.error.is_none());

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_invalid_json_returns_error_response() {
        let (_tmp, sock, lock) = fresh_paths("badjson");
        let handle =
            bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let mut stream = UnixStream::connect(&sock).await.unwrap();
        stream.write_all(b"not json\n").await.unwrap();
        stream.flush().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let resp: RpcResponse = serde_json::from_str(line.trim()).unwrap();
        assert!(resp.error.unwrap().contains("invalid JSON"));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_socket_is_unlinked_and_rebound() {
        let (_tmp, sock, lock) = fresh_paths("stale");
        // 残骸 socket を仕込む (= bind せず file だけ作る)
        std::fs::write(&sock, b"stale").unwrap();
        assert!(sock.exists());

        let handle =
            bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();
        // bind 成立 → socket は再作成されている
        assert!(sock.exists());
        // 実際に rpc が通る (= 新規 socket 上で listening) を smoke check
        let mut stream = UnixStream::connect(&sock).await.unwrap();
        stream.write_all(b"{\"method\":\"sync.health-check\"}\n").await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let resp: RpcResponse = serde_json::from_str(line.trim()).unwrap();
        assert!(resp.error.is_none());

        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn second_bind_attempt_fails_while_first_runs() {
        let (_tmp, sock, lock) = fresh_paths("dup");
        let h1 = bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let e = bind_listener(&sock, &lock, Arc::new(EchoDispatcher))
            .await
            .unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("another daemon") || msg.contains("already") || msg.contains("lock"),
            "expected lock / already-running error, got: {msg}"
        );

        h1.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn probe_existing_socket_returns_false_for_no_listener() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("nothing.sock");
        assert!(!probe_existing_socket(&sock).await);
    }

    #[test]
    fn connection_timeout_and_cap_constants() {
        assert_eq!(CONNECTION_READ_TIMEOUT, Duration::from_secs(10));
        assert_eq!(MAX_IN_FLIGHT_CONNECTIONS, 32);
    }

    /// Phase 3 review M2: newline を送らない slow client は CONNECTION_READ_TIMEOUT
    /// 後に error response を受け、 connection が close される。
    /// (= virtual time + fresh socket でテスト)
    #[tokio::test(start_paused = true)]
    async fn slow_client_gets_read_timeout_error() {
        let (_tmp, sock, lock) = fresh_paths("slow");
        let h = bind_listener(&sock, &lock, Arc::new(EchoDispatcher))
            .await
            .unwrap();

        let mut stream = UnixStream::connect(&sock).await.unwrap();
        // 部分 byte を送るが newline は送らない
        stream.write_all(b"partial").await.unwrap();
        stream.flush().await.unwrap();

        // 仮想時間を timeout より先に進める
        tokio::time::advance(CONNECTION_READ_TIMEOUT + Duration::from_secs(1)).await;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let resp: RpcResponse = serde_json::from_str(line.trim()).unwrap();
        assert!(resp.error.as_deref().unwrap().contains("timed out"));

        h.shutdown().await.unwrap();
    }

    /// Phase 3 review M2: connection cap 超過時、 新規 connection は accept 直後
    /// drop され、 client 側は read で EOF を観測する。
    ///
    /// この test 用に slow dispatcher を使い、 既存 connection を 32 個 hold
    /// した状態で 33 個目を打つ。
    #[tokio::test(start_paused = true)]
    async fn connection_cap_drops_extra_connections() {
        // 永遠に block する dispatcher (= test 用)
        struct HangDispatcher;
        impl DynDispatcher for HangDispatcher {
            fn dispatch<'a>(
                &'a self,
                _req: RpcRequest,
            ) -> Pin<Box<dyn std::future::Future<Output = RpcResponse> + Send + 'a>> {
                Box::pin(async move {
                    std::future::pending::<()>().await;
                    unreachable!()
                })
            }
        }

        let (_tmp, sock, lock) = fresh_paths("capdrop");
        let h = bind_listener(&sock, &lock, Arc::new(HangDispatcher))
            .await
            .unwrap();

        // MAX_IN_FLIGHT_CONNECTIONS 個の連接を hold (= dispatch で hang)
        let mut held = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_CONNECTIONS {
            let mut s = UnixStream::connect(&sock).await.unwrap();
            s.write_all(b"{\"method\":\"sync.x\"}\n").await.unwrap();
            s.flush().await.unwrap();
            held.push(s);
        }

        // accept_loop が permit を hand off するのを待つ。 ここで時間を少し進める。
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;

        // 33 個目: accept 直後 stream は drop される (= read で EOF)
        let mut extra = UnixStream::connect(&sock).await.unwrap();
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let mut buf = [0u8; 1];
        let n = match tokio::time::timeout(
            Duration::from_secs(1),
            extra.read(&mut buf),
        )
        .await
        {
            Ok(r) => r.unwrap_or(0),
            Err(_) => {
                tokio::time::advance(Duration::from_secs(2)).await;
                tokio::task::yield_now().await;
                extra.read(&mut buf).await.unwrap_or(0)
            }
        };
        assert_eq!(n, 0, "extra connection should be closed (EOF)");

        // hold 中の connection を drop して permit を返す
        drop(held);
        h.shutdown().await.unwrap();
    }
}
