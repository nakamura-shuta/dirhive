//! daemon Unix socket への client (= MCP server / 統合 test / CLI から使う)。
//!
//! 1 RPC = 1 connection の単純化:
//! - UnixStream connect (連続失敗時の retry はしない、 caller の責務)
//! - newline JSON Request 送信
//! - newline JSON Response 受信 (= 1 行で完結)
//! - error response (= `{"error": "..."}`) は `Err` に変換、 result は `Ok(Value)`
//!
//! timeout:
//! - 全体 (connect + write + read) を `RPC_TIMEOUT` (= 5s) で囲む
//! - 上位 (MCP tool) で更に短く絞りたければ `tokio::time::timeout` を caller 側で

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::listener::{RpcRequest, RpcResponse};

/// 1 RPC の総合 timeout (= connect + write + read)。 health-check probe より
/// 緩い (= dispatch 処理が iroh stack を叩く可能性あるため)。
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// `socket` に対して 1 RPC を投げて、 `result` value を返す。
///
/// 戻り値:
/// - `Ok(value)`: response が `{"result": value}` だった
/// - `Err(_)`: connect 失敗 / timeout / wire format 不正 /
///   `{"error": "..."}` の error string を anyhow で wrap
pub async fn rpc(
    socket: &Path,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    tokio::time::timeout(RPC_TIMEOUT, rpc_inner(socket, method, params))
        .await
        .map_err(|_| anyhow!("RPC timeout ({}s) on {}", RPC_TIMEOUT.as_secs(), method))?
}

async fn rpc_inner(
    socket: &Path,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to daemon socket {}", socket.display()))?;
    let (rx, mut tx) = stream.into_split();

    let req = RpcRequest {
        method: method.to_string(),
        params,
    };
    let mut body = serde_json::to_vec(&req).context("serialize RpcRequest")?;
    body.push(b'\n');
    tx.write_all(&body)
        .await
        .context("write RpcRequest to socket")?;
    tx.flush().await.context("flush RpcRequest")?;

    let mut reader = BufReader::new(rx);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .await
        .context("read RpcResponse line")?;
    if n == 0 {
        return Err(anyhow!("daemon closed connection without response"));
    }
    let resp: RpcResponse =
        serde_json::from_str(line.trim_end_matches('\n')).with_context(|| {
            format!("parse RpcResponse JSON (first 200 bytes): {}", &line[..line.len().min(200)])
        })?;

    if let Some(err) = resp.error {
        return Err(anyhow!("daemon RPC error: {err}"));
    }
    resp.result
        .ok_or_else(|| anyhow!("RpcResponse had neither result nor error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::listener::{DynDispatcher, bind_listener};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;

    fn fresh_paths(prefix: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join(format!("{prefix}.sock"));
        let lock = tmp.path().join(format!("{prefix}.lock"));
        (tmp, sock, lock)
    }

    struct EchoDispatcher;
    impl DynDispatcher for EchoDispatcher {
        fn dispatch<'a>(
            &'a self,
            req: RpcRequest,
        ) -> Pin<Box<dyn std::future::Future<Output = RpcResponse> + Send + 'a>> {
            Box::pin(async move {
                match req.method.as_str() {
                    "sync.health-check" => RpcResponse::ok(serde_json::json!({"ok": true})),
                    "sync.fail" => RpcResponse::err("planned failure"),
                    _ => RpcResponse::ok(serde_json::json!({
                        "method": req.method,
                        "params": req.params
                    })),
                }
            })
        }
    }

    #[test]
    fn rpc_timeout_is_5_seconds() {
        assert_eq!(RPC_TIMEOUT, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn rpc_returns_result_for_ok_response() {
        let (_tmp, sock, lock) = fresh_paths("rpc_ok");
        let h = bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let v = rpc(&sock, "sync.health-check", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(v, serde_json::json!({"ok": true}));

        h.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_returns_err_for_error_response() {
        let (_tmp, sock, lock) = fresh_paths("rpc_err");
        let h = bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let e = rpc(&sock, "sync.fail", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("planned failure"));

        h.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_passes_params_through() {
        let (_tmp, sock, lock) = fresh_paths("rpc_params");
        let h = bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();

        let v = rpc(
            &sock,
            "sync.foo",
            serde_json::json!({"a": 1, "b": "two"}),
        )
        .await
        .unwrap();
        assert_eq!(v["method"], "sync.foo");
        assert_eq!(v["params"]["a"], 1);
        assert_eq!(v["params"]["b"], "two");

        h.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_fails_when_socket_absent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("noexist.sock");
        let e = rpc(&sock, "sync.health-check", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("connect to daemon socket"));
    }

    /// daemon が shutdown → socket unlink された後の接続は connect エラー。
    #[tokio::test]
    async fn rpc_fails_after_daemon_shutdown() {
        let (_tmp, sock, lock) = fresh_paths("after_down");
        let h = bind_listener(&sock, &lock, Arc::new(EchoDispatcher)).await.unwrap();
        // 1 回成功
        rpc(&sock, "sync.health-check", serde_json::json!({})).await.unwrap();
        h.shutdown().await.unwrap();
        assert!(!sock.exists());
        // 2 回目は失敗
        let e = rpc(&sock, "sync.health-check", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("connect"));
    }
}
