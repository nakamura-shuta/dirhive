#!/usr/bin/env bash
# 3-peer e2e smoke (= alice / bob / carol)。
#
# 同 folder_secret 下で完全 mesh (= 3 ペア bilateral allow-peer) を組み、
# 各 peer の file 書込が他 2 peer に伝搬することを **6 方向** 確認する。
#
# シナリオ:
#   - alice が group founder → invite ticket 発行 → restart
#   - bob, carol が同 ticket で accept-invite → それぞれ restart
#     (= design §4.3: 「 3 peer chain (A→B→C) でも全員 同 folder_secret =
#       同 topic = 1 mesh」)
#   - bilateral allowlist を 3 ペア全部組む:
#       alice ↔ bob, alice ↔ carol, bob ↔ carol
#   - 各 peer が 1 file 書く: alice.md, bob.md, carol.md
#   - 全 peer の watched_dir に 3 file 全部現れる (= 6 propagation 経路を確認)
#
# 注: N0 relay 経由なので実機接続が必要。 完全 mesh は 2peer の倍以上時間がかかる
# (= ~2 分目安)。

set -euo pipefail

# --- cleanup --------------------------------------------------------------
cleanup() {
  # `SPAWN_PID` も対象に含める (= spawn_daemon が socket polling 中に fail_msg
  # した場合、 SPAWN_PID は set されているが呼出側 ALICE_PID / BOB_PID / CAROL_PID
  # への代入前 → 呼出側変数だけでは半起動 daemon が orphan に。 重複 kill は noop。
  for pid in "${ALICE_PID:-}" "${BOB_PID:-}" "${CAROL_PID:-}" "${SPAWN_PID:-}"; do
    [[ -n "${pid}" ]] || continue
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  done
  for h in "${ALICE_HOME:-}" "${BOB_HOME:-}" "${CAROL_HOME:-}"; do
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
CAROL_HOME=$(mktemp -d /tmp/dirhive-carol.XXXXXX)
mkdir -p "${ALICE_HOME}/watched" "${BOB_HOME}/watched" "${CAROL_HOME}/watched"
ORIG_HOME="${HOME}"

MIN_PATH="/usr/bin:/bin:/usr/sbin:/sbin"
CARGO_BIN_DIR="$(dirname "$(command -v cargo)")"
CARGO_HOME_REAL="${CARGO_HOME:-${ORIG_HOME}/.cargo}"
RUSTUP_HOME_REAL="${RUSTUP_HOME:-${ORIG_HOME}/.rustup}"

# --- helpers (= 2peer-smoke.sh と同パターン) ------------------------------
section() { printf '\n==> %s\n' "$*"; }
ok() { printf '    \033[32m✓ %s\033[0m\n' "$*"; }
fail_msg() { printf '    \033[31m✗ %s\033[0m\n' "$*"; exit 1; }

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

stop_daemon() {
  local pid="$1"
  kill "${pid}"
  local ec=0
  wait "${pid}" || ec=$?
  if [[ ${ec} -ne 0 ]]; then
    fail_msg "daemon ${pid} exited non-graceful (${ec})"
  fi
}

wait_for_file() {
  local home="$1" rel="$2" expected="$3" timeout="${4:-120}"
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

rpc_to_home() { python3 "${RPC_PY}" "$1/.local/share/dirhive/daemon.sock" "${@:2}"; }
rpc_alice() { rpc_to_home "${ALICE_HOME}" "$@"; }
rpc_bob()   { rpc_to_home "${BOB_HOME}"   "$@"; }
rpc_carol() { rpc_to_home "${CAROL_HOME}" "$@"; }

json_field() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }
json_params() { python3 -c 'import json,sys; print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))' "$@"; }

assert_gossip_subscribed() {
  local who="$1" hc="$2"
  local gs
  gs=$(echo "${hc}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dynamic_info"]["gossip_subscribed"])')
  [[ "${gs}" == "True" ]] || fail_msg "${who} gossip_subscribed != True after restart: ${gs}"
}

# --- 0. release build (1 回) → 3 HOME へ binary 配布 ----------------------
section "0. release build via install.sh (alice HOME)"
if install_with_home "${ALICE_HOME}" >"${ALICE_HOME}/install.log" 2>&1; then
  ok "install.sh exited 0"
else
  tail -30 "${ALICE_HOME}/install.log"
  fail_msg "install.sh failed for alice"
fi

section "0b. copy binaries to bob / carol HOME"
for H in "${BOB_HOME}" "${CAROL_HOME}"; do
  mkdir -p "${H}/.local/bin"
  cp -f "${ALICE_HOME}/.local/bin/dirhive"     "${H}/.local/bin/"
  cp -f "${ALICE_HOME}/.local/bin/dirhive-mcp" "${H}/.local/bin/"
  chmod 0755 "${H}/.local/bin/"*
done
ok "binaries copied to bob + carol"

# --- 1. Alice: spawn + sync.invite (ticket は bob/carol で共有) -----------
section "1. Alice: spawn + sync.invite"
spawn_daemon "${ALICE_HOME}" "${ALICE_HOME}/daemon1"
ALICE_PID="${SPAWN_PID}"
invite_resp=$(rpc_alice sync.invite)
TICKET=$(echo "${invite_resp}" | json_field ticket)
[[ "${TICKET}" == dirhive1-* ]] || fail_msg "ticket prefix wrong"
ok "got ticket (${#TICKET} chars)"

# --- 2. Alice restart → Active --------------------------------------------
section "2. Alice: restart → gossip subscribed"
stop_daemon "${ALICE_PID}"; ALICE_PID=""
spawn_daemon "${ALICE_HOME}" "${ALICE_HOME}/daemon2"
ALICE_PID="${SPAWN_PID}"
assert_gossip_subscribed "alice" "$(rpc_alice sync.health-check)"
ok "alice gossip_subscribed=True"

# --- 3. Bob: spawn + accept --------------------------------------------
section "3. Bob: spawn + accept-invite (alice's ticket)"
spawn_daemon "${BOB_HOME}" "${BOB_HOME}/daemon1"
BOB_PID="${SPAWN_PID}"
accept_resp=$(rpc_bob sync.accept-invite "$(json_params ticket "${TICKET}" label "alice")")
BOB_ID=$(echo "${accept_resp}" | json_field my_peer_id)
ok "bob accepted (my_peer_id=${BOB_ID:0:12}...)"

# --- 4. Bob restart → Active ----------------------------------------------
section "4. Bob: restart → gossip subscribed"
stop_daemon "${BOB_PID}"; BOB_PID=""
spawn_daemon "${BOB_HOME}" "${BOB_HOME}/daemon2"
BOB_PID="${SPAWN_PID}"
assert_gossip_subscribed "bob" "$(rpc_bob sync.health-check)"
ok "bob gossip_subscribed=True"

# --- 5. Carol: spawn + accept (alice's same ticket) ------------------------
section "5. Carol: spawn + accept-invite (alice's same ticket)"
spawn_daemon "${CAROL_HOME}" "${CAROL_HOME}/daemon1"
CAROL_PID="${SPAWN_PID}"
accept_resp=$(rpc_carol sync.accept-invite "$(json_params ticket "${TICKET}" label "alice")")
CAROL_ID=$(echo "${accept_resp}" | json_field my_peer_id)
ok "carol accepted (my_peer_id=${CAROL_ID:0:12}...)"

# --- 6. Carol restart → Active --------------------------------------------
section "6. Carol: restart → gossip subscribed"
stop_daemon "${CAROL_PID}"; CAROL_PID=""
spawn_daemon "${CAROL_HOME}" "${CAROL_HOME}/daemon2"
CAROL_PID="${SPAWN_PID}"
assert_gossip_subscribed "carol" "$(rpc_carol sync.health-check)"
ok "carol gossip_subscribed=True"

# --- 7. 完全 mesh の bilateral allowlist (= 3 ペア × 2 方向 = 6 allow-peer) -
# bob ↔ carol は ticket 経由で互いを認識していないので、 ここで明示的に
# bilateral 追加する (= 各側が他側の my_peer_id を allowlist 入れる)。
section "7. Build full-mesh allowlist (= 3 pairs bilateral)"
rpc_alice sync.allow-peer "$(json_params peer_id "${BOB_ID}"   label "bob")"   > /dev/null
rpc_alice sync.allow-peer "$(json_params peer_id "${CAROL_ID}" label "carol")" > /dev/null
rpc_bob   sync.allow-peer "$(json_params peer_id "${CAROL_ID}" label "carol")" > /dev/null
rpc_carol sync.allow-peer "$(json_params peer_id "${BOB_ID}"   label "bob")"   > /dev/null
# bob/carol 側は accept-invite で alice を allowlist 済 → これで完全 mesh
ok "allowlist: alice↔bob, alice↔carol, bob↔carol (3 pairs all bilateral)"

# --- 8. 各 peer が file 書込 ---------------------------------------------
section "8. Each peer writes one file"
echo -n "from alice via 3peer-smoke" > "${ALICE_HOME}/watched/alice.md"
echo -n "from bob via 3peer-smoke"   > "${BOB_HOME}/watched/bob.md"
echo -n "from carol via 3peer-smoke" > "${CAROL_HOME}/watched/carol.md"
ok "wrote alice.md / bob.md / carol.md on respective peers"

# --- 9. 全 peer に 3 file 全部現れる (= 6 propagation を 2 分以内) ----------
section "9. Wait until all 3 files visible on all 3 peers (max 120s)"
check_propagation() {
  local home="$1" who="$2"
  # 自分 origin の file は当然存在するので skip、 他 2 peer 由来 file を待つ
  for spec in "alice ${ALICE_HOME} alice.md|from alice via 3peer-smoke" \
              "bob   ${BOB_HOME}   bob.md|from bob via 3peer-smoke" \
              "carol ${CAROL_HOME} carol.md|from carol via 3peer-smoke"; do
    local from=$(echo "${spec}" | awk '{print $1}')
    [[ "${from}" == "${who}" ]] && continue
    local rel content
    rel=$(echo "${spec}" | awk -F'|' '{print $1}' | awk '{print $3}')
    content=$(echo "${spec}" | awk -F'|' '{print $2}')
    if wait_for_file "${home}" "${rel}" "${content}" 120; then
      ok "${who}: received ${rel} from ${from}"
    else
      echo "    --- ${who} daemon2.stderr tail ---"
      tail -30 "${home}/daemon2.stderr" | sed 's/^/      /'
      fail_msg "${who} did not receive ${rel} from ${from} within 120s"
    fi
  done
}
check_propagation "${ALICE_HOME}" "alice"
check_propagation "${BOB_HOME}"   "bob"
check_propagation "${CAROL_HOME}" "carol"

# --- 10. tear down ---------------------------------------------------------
section "10. SIGTERM all 3 daemons"
stop_daemon "${ALICE_PID}"; ALICE_PID=""
stop_daemon "${BOB_PID}";   BOB_PID=""
stop_daemon "${CAROL_PID}"; CAROL_PID=""
ok "all 3 daemons exited 0 (graceful)"

printf '\n\033[32m✅ 3peer-smoke: ALL CHECKS PASSED\033[0m\n'
