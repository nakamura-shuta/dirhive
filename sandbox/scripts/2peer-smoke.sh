#!/usr/bin/env bash
# 2-peer install + sync e2e smoke.
#
# 2 つの独立 HOME (= alice / bob) で install から bilateral invite flow を回し、
# alice 側 file write が bob 側 watched_dir に伝搬することを確認する。
#
# 7-step bilateral flow (= design.md §3.4):
#   1. Alice: spawn → sync.invite → ticket + restart_required:true
#   2. Alice: restart (= folder_secret 既存で起動 → Active)
#   3. Bob: spawn → sync.accept-invite(ticket) → my_peer_id + restart_required:true
#   4. Bob: restart
#   5. Alice: sync.allow-peer(bob_id) → 双方向 allowlist 確定
#   6. Alice: watched_dir に file 書込
#   7. Bob: watched_dir に同 file が伝搬してくる (polling 90s)
#
# 注: N0 relay 経由なので実機接続が必要。 sandbox / CI で外向き UDP / TCP が
# 通らない場合 step 1 spawn 直後の relay homing で失敗することがある。

set -euo pipefail

# --- cleanup --------------------------------------------------------------
cleanup() {
  # `SPAWN_PID` も対象に含める (= spawn_daemon が socket polling 中に fail_msg
  # した場合、 SPAWN_PID は set されているが呼出側 ALICE_PID / BOB_PID への代入
  # 前 → ALICE_PID 経由だけでは半起動 daemon が orphan に。 重複 kill は noop。
  for pid in "${ALICE_PID:-}" "${BOB_PID:-}" "${SPAWN_PID:-}"; do
    [[ -n "${pid}" ]] || continue
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  done
  for h in "${ALICE_HOME:-}" "${BOB_HOME:-}"; do
    [[ -z "${h}" || ! -d "${h}" ]] && continue
    if [[ -z "${KEEP_TMPHOME:-}" ]]; then
      rm -rf "${h}"
    else
      echo "(keeping ${h} for inspection — unset KEEP_TMPHOME to clean)"
    fi
  done
}
trap cleanup EXIT

# --- locate workspace -----------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PLUGIN_DIR="${WORK_ROOT}/plugin"
RPC_PY="${SCRIPT_DIR}/lib/rpc.py"
test -f "${WORK_ROOT}/Cargo.toml"
test -x "${PLUGIN_DIR}/scripts/install.sh"
test -f "${RPC_PY}"

# --- setup tmp HOMEs ------------------------------------------------------
# AF_UNIX sun_path 104 byte 上限を避けるため `/tmp/...` 直下。
ALICE_HOME=$(mktemp -d /tmp/dirhive-alice.XXXXXX)
BOB_HOME=$(mktemp -d /tmp/dirhive-bob.XXXXXX)
mkdir -p "${ALICE_HOME}/watched" "${BOB_HOME}/watched"
ORIG_HOME="${HOME}"

MIN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"
CARGO_BIN_DIR="$(dirname "$(command -v cargo)")"
CARGO_HOME_REAL="${CARGO_HOME:-${ORIG_HOME}/.cargo}"
RUSTUP_HOME_REAL="${RUSTUP_HOME:-${ORIG_HOME}/.rustup}"

# --- helpers --------------------------------------------------------------
section() { printf '\n==> %s\n' "$*"; }
ok() { printf '    \033[32m✓ %s\033[0m\n' "$*"; }
fail_msg() { printf '    \033[31m✗ %s\033[0m\n' "$*"; exit 1; }
warn() { printf '    \033[33m⚠ %s\033[0m\n' "$*"; }

# install.sh を Alice 側に走らせる。 binary は target/release/ にも残るので、
# Bob 側は cp で済ます (= cargo build 1 回で 2 peer 分の install)。
install_with_home() {
  local home="$1"
  env -i \
    HOME="${home}" \
    PATH="${CARGO_BIN_DIR}:${MIN_PATH}" \
    CARGO_HOME="${CARGO_HOME_REAL}" \
    RUSTUP_HOME="${RUSTUP_HOME_REAL}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    "${PLUGIN_DIR}/scripts/install.sh"
}

# daemon を foreground で spawn し、 socket ready まで polling。
# **PID は global `SPAWN_PID` に書く** (= `$(spawn_daemon ...)` 経由だと subshell
# で daemon が spawn される → 親 shell から `wait $!` できない。 daemon を必ず
# 本 shell の direct child にする必要がある)。
SPAWN_PID=""
spawn_daemon() {
  local home="$1" log="$2"
  env -i \
    HOME="${home}" \
    PATH="${home}/.local/bin:${MIN_PATH}" \
    TMPDIR=/tmp \
    TERM="${TERM:-xterm-256color}" \
    DIRHIVE_LOG="info,dirhive=debug" \
    "${home}/.local/bin/dirhive" --watch "${home}/watched" \
    >"${log}.stdout" 2>"${log}.stderr" &
  SPAWN_PID=$!
  local sock="${home}/.local/share/dirhive/daemon.sock"
  for _ in $(seq 1 60); do
    if [[ -S "${sock}" ]] && \
        python3 "${RPC_PY}" "${sock}" sync.health-check >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "--- ${log}.stderr tail ---" >&2
  tail -20 "${log}.stderr" >&2
  fail_msg "daemon under ${home} did not become ready"
}

# daemon を SIGTERM で停止 (= graceful shutdown 確認込み)
stop_daemon() {
  local pid="$1"
  kill "${pid}"
  local ec=0
  wait "${pid}" || ec=$?
  if [[ ${ec} -ne 0 ]]; then
    fail_msg "daemon ${pid} exited non-graceful (${ec})"
  fi
}

# `${home}/watched/${rel}` が指定 content になるまで polling。
wait_for_file() {
  local home="$1" rel="$2" expected="$3" timeout="${4:-90}"
  local path="${home}/watched/${rel}"
  local deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    if [[ -f "${path}" ]] && [[ "$(cat "${path}")" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# `${home}` の daemon socket に対して RPC 投げる。 result JSON を stdout に。
rpc_alice() { python3 "${RPC_PY}" "${ALICE_HOME}/.local/share/dirhive/daemon.sock" "$@"; }
rpc_bob()   { python3 "${RPC_PY}" "${BOB_HOME}/.local/share/dirhive/daemon.sock"   "$@"; }

# JSON value 抽出 (= jq 無し env 向け)
json_field() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }

# --- 0. release build (= alice install で兼ねる、 bob は cp で済ます) ---
section "0. release build via install.sh (alice HOME)"
if install_with_home "${ALICE_HOME}" >"${ALICE_HOME}/install.log" 2>&1; then
  ok "install.sh exited 0"
else
  tail -30 "${ALICE_HOME}/install.log"
  fail_msg "install.sh failed for alice"
fi
test -x "${ALICE_HOME}/.local/bin/dirhive"
test -x "${ALICE_HOME}/.local/bin/dirhive-mcp"

section "0b. copy binaries to bob HOME (cargo build skip)"
mkdir -p "${BOB_HOME}/.local/bin"
cp -f "${ALICE_HOME}/.local/bin/dirhive"     "${BOB_HOME}/.local/bin/"
cp -f "${ALICE_HOME}/.local/bin/dirhive-mcp" "${BOB_HOME}/.local/bin/"
chmod 0755 "${BOB_HOME}/.local/bin/"*
ok "binaries copied to ${BOB_HOME}/.local/bin/"

# --- 1. Alice spawn + invite -----------------------------------------------
section "1. Alice: spawn (uninitialized) + sync.invite"
spawn_daemon "${ALICE_HOME}" "${ALICE_HOME}/daemon1"
ALICE_PID="${SPAWN_PID}"
ok "alice daemon PID=${ALICE_PID}"

invite_resp=$(rpc_alice sync.invite)
TICKET=$(echo "${invite_resp}" | json_field ticket)
restart=$(echo "${invite_resp}" | json_field restart_required)
[[ "${TICKET}" == dirhive1-* ]] || fail_msg "ticket prefix wrong: ${TICKET:0:30}..."
[[ "${restart}" == "True" ]] || fail_msg "restart_required != True: ${restart}"
ok "got ticket (${#TICKET} chars)"

# --- 2. Alice restart → Active --------------------------------------------
section "2. Alice: restart so gossip subscribes"
stop_daemon "${ALICE_PID}"
ALICE_PID=""  # cleanup trap が旧 PID を kill しないよう即時クリア (= L2 review fix)
spawn_daemon "${ALICE_HOME}" "${ALICE_HOME}/daemon2"
ALICE_PID="${SPAWN_PID}"
hc=$(rpc_alice sync.health-check)
gs=$(echo "${hc}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dynamic_info"]["gossip_subscribed"])')
[[ "${gs}" == "True" ]] || fail_msg "alice gossip_subscribed != True after restart: ${gs}"
ok "alice gossip_subscribed=True"

# --- 3. Bob spawn + accept-invite -----------------------------------------
section "3. Bob: spawn + sync.accept-invite"
spawn_daemon "${BOB_HOME}" "${BOB_HOME}/daemon1"
BOB_PID="${SPAWN_PID}"
ok "bob daemon PID=${BOB_PID}"

# JSON 内 ticket は escape 不要 (python の json.loads が処理)
accept_resp=$(rpc_bob sync.accept-invite "$(python3 -c 'import json,sys; print(json.dumps({"ticket": sys.argv[1], "label": "alice"}))' "${TICKET}")")
BOB_ID=$(echo "${accept_resp}" | json_field my_peer_id)
restart=$(echo "${accept_resp}" | json_field restart_required)
[[ "${restart}" == "True" ]] || fail_msg "bob restart_required != True: ${restart}"
ok "bob accepted (my_peer_id=${BOB_ID:0:12}...)"

# --- 4. Bob restart → Active ----------------------------------------------
section "4. Bob: restart so gossip subscribes"
stop_daemon "${BOB_PID}"
BOB_PID=""  # cleanup trap が旧 PID を kill しないよう即時クリア (= L2 review fix)
spawn_daemon "${BOB_HOME}" "${BOB_HOME}/daemon2"
BOB_PID="${SPAWN_PID}"
hc=$(rpc_bob sync.health-check)
gs=$(echo "${hc}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dynamic_info"]["gossip_subscribed"])')
[[ "${gs}" == "True" ]] || fail_msg "bob gossip_subscribed != True after restart: ${gs}"
ok "bob gossip_subscribed=True"

# --- 5. Alice allow-peer Bob -----------------------------------------------
section "5. Alice: sync.allow-peer <bob_id>"
ap=$(rpc_alice sync.allow-peer "$(python3 -c 'import json,sys; print(json.dumps({"peer_id": sys.argv[1], "label": "bob"}))' "${BOB_ID}")")
added=$(echo "${ap}" | json_field added)
[[ "${added}" == "True" ]] || fail_msg "alice allow-peer added != True: ${added}"
ok "alice allowlisted bob"

# --- 6. Alice → Bob direction (= hello.md) --------------------------------
section "6. Alice → Bob: write hello.md"
echo -n "hello from alice via 2peer-smoke" > "${ALICE_HOME}/watched/hello.md"
ok "wrote ${ALICE_HOME}/watched/hello.md"

section "7. Bob: wait for hello.md (max 90s)"
if wait_for_file "${BOB_HOME}" "hello.md" "hello from alice via 2peer-smoke" 90; then
  ok "hello.md propagated to bob's watched_dir"
else
  echo "    --- alice daemon2.stderr tail ---"
  tail -20 "${ALICE_HOME}/daemon2.stderr" | sed 's/^/      /'
  echo "    --- bob daemon2.stderr tail ---"
  tail -20 "${BOB_HOME}/daemon2.stderr"   | sed 's/^/      /'
  fail_msg "alice→bob: file did not reach bob within 90s"
fi

# --- 8. Bob → Alice direction (= reply.md、 = M1 review fix) ----------------
# mesh は対称構造だが、 実装 path (= receive_loop on alice、 blob serve on bob、
# AllowlistBlobs::on_serve_success callback) が片方向だけ動いて逆方向が壊れる
# regression を検出するため bilateral 検証する。
section "8. Bob → Alice: write reply.md"
echo -n "reply from bob via 2peer-smoke" > "${BOB_HOME}/watched/reply.md"
ok "wrote ${BOB_HOME}/watched/reply.md"

section "9. Alice: wait for reply.md (max 90s)"
if wait_for_file "${ALICE_HOME}" "reply.md" "reply from bob via 2peer-smoke" 90; then
  ok "reply.md propagated to alice's watched_dir"
else
  echo "    --- alice daemon2.stderr tail ---"
  tail -20 "${ALICE_HOME}/daemon2.stderr" | sed 's/^/      /'
  echo "    --- bob daemon2.stderr tail ---"
  tail -20 "${BOB_HOME}/daemon2.stderr"   | sed 's/^/      /'
  fail_msg "bob→alice: reply.md did not reach alice within 90s"
fi

# --- 10. tear down ---------------------------------------------------------
section "10. SIGTERM both daemons"
stop_daemon "${ALICE_PID}"; ALICE_PID=""
stop_daemon "${BOB_PID}";   BOB_PID=""
ok "both daemons exited 0 (graceful)"

printf '\n\033[32m✅ 2peer-smoke: ALL CHECKS PASSED\033[0m\n'
