#!/usr/bin/env bash
# install p2p-dir-sync as a launchd user agent.
#
# Usage:
#   install-launchd.sh --watch <DIR> [--bin <PATH>] [--dry-run]
#
# Steps:
#   1. plist の placeholder (__BIN__ / __WATCH__ / __HOME__) を実値に置換
#   2. ~/Library/LaunchAgents/com.user.p2p-dir-sync.plist に書き出し
#   3. launchctl bootstrap gui/$UID で boot
#   4. 状態確認 + 次手順を表示
#
# --dry-run: plist の中身を stdout に出すだけで file 設置 / launchctl 実行は skip
#
# 既に installed なら error 終了。 まず uninstall-launchd.sh を実行してから。

set -euo pipefail

# --- arg parse ------------------------------------------------------------
WATCH_DIR=""
BIN_PATH=""
DRY_RUN=0

usage() {
  cat <<EOF
usage: $(basename "$0") --watch <DIR> [--bin <PATH>] [--dry-run]

  --watch <DIR>   同期対象 dir (= 必須、 canonicalize される)
  --bin <PATH>    p2p-sync binary の絶対 path (default: \$HOME/.local/bin/p2p-sync)
  --dry-run       plist 内容を stdout に出すだけ。 file 設置 / launchctl 実行 skip
  -h, --help      この help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch) WATCH_DIR="$2"; shift 2 ;;
    --bin)   BIN_PATH="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${WATCH_DIR}" ]]; then
  echo "error: --watch <DIR> is required" >&2
  usage >&2
  exit 2
fi

if [[ ! -d "${WATCH_DIR}" ]]; then
  echo "error: --watch dir does not exist: ${WATCH_DIR}" >&2
  exit 2
fi
WATCH_ABS="$(cd "${WATCH_DIR}" && pwd)"

if [[ -z "${BIN_PATH}" ]]; then
  BIN_PATH="${HOME}/.local/bin/p2p-sync"
fi
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "error: --bin not executable: ${BIN_PATH}" >&2
  echo "       run plugin/scripts/install.sh first." >&2
  exit 2
fi

# --- substitute -----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="${SCRIPT_DIR}/com.user.p2p-dir-sync.plist.template"
test -f "${TEMPLATE}"

PLIST_BODY=$(
  sed \
    -e "s|__BIN__|${BIN_PATH}|g" \
    -e "s|__WATCH__|${WATCH_ABS}|g" \
    -e "s|__HOME__|${HOME}|g" \
    "${TEMPLATE}"
)

if [[ ${DRY_RUN} -eq 1 ]]; then
  echo "==> rendered plist (dry-run):"
  echo "${PLIST_BODY}"
  exit 0
fi

# --- install --------------------------------------------------------------
PLIST_TARGET="${HOME}/Library/LaunchAgents/com.user.p2p-dir-sync.plist"
mkdir -p "${HOME}/Library/LaunchAgents"
mkdir -p "${HOME}/Library/Logs"

if [[ -f "${PLIST_TARGET}" ]] && launchctl print "gui/${UID}/com.user.p2p-dir-sync" >/dev/null 2>&1; then
  cat <<EOF >&2
error: a launchd service is already loaded (gui/${UID}/com.user.p2p-dir-sync).
       run sandbox/scripts/launchd/uninstall-launchd.sh first.
EOF
  exit 1
fi

echo "${PLIST_BODY}" > "${PLIST_TARGET}"
chmod 0644 "${PLIST_TARGET}"

echo "==> wrote ${PLIST_TARGET}"

# --- bootstrap ------------------------------------------------------------
echo "==> launchctl bootstrap gui/${UID} (start daemon)"
if launchctl bootstrap "gui/${UID}" "${PLIST_TARGET}"; then
  echo "    ✓ bootstrapped"
else
  echo "    ✗ bootstrap failed (= already loaded? path typo?)" >&2
  exit 1
fi

# --- post-flight ----------------------------------------------------------
echo
echo "==> verify"
sleep 1
launchctl print "gui/${UID}/com.user.p2p-dir-sync" 2>/dev/null | head -20 | sed 's/^/    /' || true

cat <<EOF

==> Done.

Next:
  - watch logs:
      tail -f ${HOME}/Library/Logs/p2p-dir-sync.stdout.log
      tail -f ${HOME}/Library/Logs/p2p-dir-sync.stderr.log
  - probe daemon:
      p2p-sync-mcp     # MCP server (for AI agents)
      python3 sandbox/scripts/lib/rpc.py ${HOME}/.local/share/p2p-dir-sync/daemon.sock sync.health-check
  - stop:
      sandbox/scripts/launchd/uninstall-launchd.sh
  - restart after invite/accept:
      launchctl kickstart -k gui/${UID}/com.user.p2p-dir-sync

EOF
