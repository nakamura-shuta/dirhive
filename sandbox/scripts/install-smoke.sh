#!/usr/bin/env bash
# 1-peer install smoke (= 仮想クリーン HOME での install → verify → daemon → MCP probe)。
#
# 目的: 「 新規 user 環境で install から agent 接続まで通る 」 ことを自動確認する。
# 実 user の ~/.local/bin / ~/.local/share / Claude Code GUI 設定を汚さずに、
# tmp HOME + 最小 PATH で plugin/scripts/install.sh を走らせ、 binary 設置 / verify /
# daemon 起動 / MCP server stdio handshake までを 1 連の step で押さえる。
#
# 注: cargo toolchain (~/.cargo, ~/.rustup) は本物を流用する (= build cache を使う)。
# 「 cargo build が clean 環境でも通る 」 確認は別 phase でやる。

set -euo pipefail

cleanup() {
  if [[ -n "${DAEMON_PID:-}" ]]; then
    kill "${DAEMON_PID}" 2>/dev/null || true
    wait "${DAEMON_PID}" 2>/dev/null || true
  fi
  if [[ -n "${TMPHOME:-}" && -d "${TMPHOME}" ]]; then
    # KEEP_TMPHOME=1 で debug 用に保持
    if [[ -z "${KEEP_TMPHOME:-}" ]]; then
      rm -rf "${TMPHOME}"
    else
      echo "(keeping ${TMPHOME} for inspection — set KEEP_TMPHOME='' to clean)"
    fi
  fi
}
trap cleanup EXIT

# --- locate workspace -----------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PLUGIN_DIR="${WORK_ROOT}/plugin"
test -f "${WORK_ROOT}/Cargo.toml"
test -d "${PLUGIN_DIR}/.claude-plugin"

# --- setup tmp HOME -------------------------------------------------------
# AF_UNIX の sun_path 上限 (= macOS 104 byte) があるので tmp HOME を **短い path**
# (`/tmp/...`) に切る。 macOS の `mktemp -t` は `/var/folders/...` を返すので
# socket path (= `${HOME}/.local/share/p2p-dir-sync/daemon.sock`) が 100 byte 超え
# になり `bind` が失敗する。 `/tmp` ベースなら ~80 byte に収まる。
TMPHOME=$(mktemp -d /tmp/p2p-sync-smoke.XXXXXX)
ORIG_HOME="${HOME}"
WATCH_DIR="${TMPHOME}/watched"
mkdir -p "${WATCH_DIR}"

# 最小 PATH。 install.sh は cargo / std unix utils が要る。 cargo は実 user の
# rustup 配下にあるので、 そのディレクトリだけ通す。
MIN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"
CARGO_BIN_DIR="$(dirname "$(command -v cargo)")"

# 実 user の cargo / rustup cache を流用 (= clean cargo cache build は別 phase)
CARGO_HOME_REAL="${CARGO_HOME:-${ORIG_HOME}/.cargo}"
RUSTUP_HOME_REAL="${RUSTUP_HOME:-${ORIG_HOME}/.rustup}"

# install / daemon は HOME を clean に。 cargo の env も継承させる。
install_env() {
  env -i \
    HOME="${TMPHOME}" \
    PATH="${CARGO_BIN_DIR}:${MIN_PATH}" \
    CARGO_HOME="${CARGO_HOME_REAL}" \
    RUSTUP_HOME="${RUSTUP_HOME_REAL}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    "$@"
}

# step 3 専用: CARGO_BIN_DIR を含まない pure minimal PATH。
# 旧 install_env は ~/.cargo/bin 等を含むので、 そこに古い p2p-sync が残っている
# user 環境では verify.sh が「 PATH 経由で binary 発見 」 してしまい step 3 の
# 「 PATH 未追加で fail する 」 確認が誤って通る。 step 3 は cargo 不要なので、
# CARGO_BIN_DIR を入れない version で隔離する。
minimal_env() {
  env -i \
    HOME="${TMPHOME}" \
    PATH="${MIN_PATH}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    "$@"
}

# user PATH に ~/.local/bin を追加した状態 (= install 後)
user_env() {
  env -i \
    HOME="${TMPHOME}" \
    PATH="${TMPHOME}/.local/bin:${MIN_PATH}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    "$@"
}

# 上記 + claude CLI が居る場所も載せた変種 (= plugin validate 用)
user_env_with_claude() {
  local extra=""
  if command -v claude >/dev/null 2>&1; then
    extra="$(dirname "$(command -v claude)"):"
  fi
  env -i \
    HOME="${TMPHOME}" \
    PATH="${extra}${TMPHOME}/.local/bin:${MIN_PATH}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    "$@"
}

# --- log helpers ----------------------------------------------------------
section() { printf '\n==> %s\n' "$*"; }
ok() { printf '    \033[32m✓ %s\033[0m\n' "$*"; }
fail_msg() { printf '    \033[31m✗ %s\033[0m\n' "$*"; exit 1; }
warn() { printf '    \033[33m⚠ %s\033[0m\n' "$*"; }

# --- 1. install ------------------------------------------------------------
section "1. install.sh in tmp HOME=${TMPHOME}"
if install_env "${PLUGIN_DIR}/scripts/install.sh" >"${TMPHOME}/install.log" 2>&1; then
  ok "install.sh exited 0"
else
  echo "--- install.log tail ---"
  tail -30 "${TMPHOME}/install.log"
  fail_msg "install.sh failed"
fi

# --- 2. binary 配置 --------------------------------------------------------
section "2. binaries at \$HOME/.local/bin"
for bin in p2p-sync p2p-sync-mcp; do
  if [[ -x "${TMPHOME}/.local/bin/${bin}" ]]; then
    ok "${TMPHOME}/.local/bin/${bin}"
  else
    fail_msg "${bin} not installed"
  fi
done

# --- 3. PATH 未追加で verify.sh が「 binary が PATH 上に無い 」 で fail する -----
section "3. verify.sh fails when PATH does not include ~/.local/bin"
# minimal_env (= CARGO_BIN_DIR 含まない) で起動する。 旧 install_env だと
# ~/.cargo/bin に古い p2p-sync が残っている user 環境で verify.sh が誤って通る。
if minimal_env "${PLUGIN_DIR}/verify.sh" >"${TMPHOME}/verify-bare.log" 2>&1; then
  fail_msg "verify.sh should FAIL when ~/.local/bin is not on PATH, but it passed"
else
  ok "verify.sh correctly exited non-zero"
fi

# --- 4. PATH 追加後で verify.sh が pass する -------------------------------
section "4. verify.sh after PATH adds ~/.local/bin (no claude CLI)"
if user_env "${PLUGIN_DIR}/verify.sh" >"${TMPHOME}/verify-pass.log" 2>&1; then
  ok "verify.sh passed"
  # step 5 が「 claude CLI 不在で skip 」 した行を確認 (= 環境差を可視化)
  if grep -q "claude CLI not on PATH" "${TMPHOME}/verify-pass.log"; then
    ok "step 5 correctly skipped (claude CLI not on PATH in this env)"
  fi
else
  echo "--- verify-pass.log tail ---"
  tail -30 "${TMPHOME}/verify-pass.log"
  fail_msg "verify.sh failed with PATH including ~/.local/bin"
fi

# --- 5. claude plugin validate (= claude CLI が手元にあるなら) ---------------
section "5. claude plugin validate (= schema check)"
if command -v claude >/dev/null 2>&1; then
  if user_env_with_claude claude plugin validate "${PLUGIN_DIR}" \
      >"${TMPHOME}/validate.log" 2>&1; then
    ok "claude plugin validate plugin/ ✔"
  else
    cat "${TMPHOME}/validate.log"
    fail_msg "claude plugin validate failed"
  fi
else
  warn "claude CLI not available on host; skipping schema validate"
fi

# --- 6. daemon を起動 ------------------------------------------------------
section "6. spawn p2p-sync daemon (--watch ${WATCH_DIR})"
# **bash function + `&` は使わない**: `func &` は subshell を介して関数を呼ぶため
# `$!` が subshell PID になり、 後の `kill $DAEMON_PID` が daemon ではなく
# subshell を SIGTERM する → daemon が orphan + exit 143 観測。 `env -i` を直接
# 起動して `$!` を daemon PID に確定させる。
env -i \
  HOME="${TMPHOME}" \
  PATH="${TMPHOME}/.local/bin:${MIN_PATH}" \
  TMPDIR=/tmp \
  TERM="${TERM:-xterm-256color}" \
  "${TMPHOME}/.local/bin/p2p-sync" --watch "${WATCH_DIR}" \
  >"${TMPHOME}/daemon.stdout" 2>"${TMPHOME}/daemon.stderr" &
DAEMON_PID=$!

SOCK="${TMPHOME}/.local/share/p2p-dir-sync/daemon.sock"
# socket 出現 + RPC 応答まで待つ (= max 30s polling)
ready=0
for _ in $(seq 1 60); do
  if [[ -S "${SOCK}" ]]; then
    # RPC 応答も確認 (= nc が無い環境向けに python3 socket で投げる)
    if user_env python3 - <<EOF "${SOCK}"
import socket, sys, json
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(2)
s.connect(sys.argv[1])
s.sendall(b'{"method":"sync.health-check"}\n')
data = b""
while b"\n" not in data:
    chunk = s.recv(4096)
    if not chunk: sys.exit(1)
    data += chunk
sys.exit(0)
EOF
    then
      ready=1
      break
    fi
  fi
  sleep 0.5
done
if [[ ${ready} -ne 1 ]]; then
  echo "--- daemon.stderr tail ---"
  tail -30 "${TMPHOME}/daemon.stderr"
  fail_msg "daemon socket did not become ready"
fi
ok "daemon socket ready at ${SOCK}"

# --- 7. MCP probe (stdio handshake + sync.ping + sync.health-check) --------
section "7. MCP probe (p2p-sync-mcp stdio)"
if user_env python3 "${SCRIPT_DIR}/lib/mcp_probe.py" "${TMPHOME}/.local/bin/p2p-sync-mcp"; then
  ok "MCP probe passed"
else
  echo "--- daemon.stderr tail ---"
  tail -30 "${TMPHOME}/daemon.stderr"
  fail_msg "MCP probe failed"
fi

# --- 8. shutdown ----------------------------------------------------------
section "8. SIGTERM daemon"
# 直前まで MCP probe が走っていたので、 signal handler が install 済になるまで
# 念のため少し待つ (= probe 終了直後の race 回避)。
sleep 0.5
kill "${DAEMON_PID}"
daemon_exit=0
wait "${DAEMON_PID}" || daemon_exit=$?
DAEMON_PID=""
# graceful shutdown / socket cleanup の regression を **fail** で検出する
# (= 旧 warn 扱いだと SIGTERM race の re-introduction が smoke で見逃される)。
if [[ ${daemon_exit} -eq 0 ]]; then
  ok "daemon exited 0 (graceful shutdown)"
else
  echo "    daemon.stderr tail:"
  tail -10 "${TMPHOME}/daemon.stderr" 2>/dev/null | sed 's/^/      /'
  if [[ ${daemon_exit} -eq 143 ]]; then
    fail_msg "daemon exited 143 (= SIGTERM without handler; signal install regressed?)"
  else
    fail_msg "daemon exited ${daemon_exit} (= non-graceful)"
  fi
fi
# socket unlink は graceful shutdown 内の Drop で行われる。 一部 OS では
# `wait` 戻り時点で fs entry がまだ propagate していないことがあるので、 短い
# polling で再確認する。
for _ in $(seq 1 10); do
  [[ ! -S "${SOCK}" ]] && break
  sleep 0.1
done
if [[ ! -S "${SOCK}" ]]; then
  ok "socket cleaned up on shutdown"
else
  # graceful shutdown 後に socket が残るのは Drop 内 unlink の regression。
  # warn ではなく fail にして smoke で検出する。
  echo "    daemon.stderr tail:"
  tail -10 "${TMPHOME}/daemon.stderr" | sed 's/^/      /'
  fail_msg "socket still present at ${SOCK} (= Drop unlink failed?)"
fi

printf '\n\033[32m✅ install-smoke: ALL CHECKS PASSED\033[0m\n'
