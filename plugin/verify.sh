#!/usr/bin/env bash
# p2p-dir-sync plugin: verify script
#
# Quick sanity-check after install.sh:
#   1. binaries are on PATH
#   2. plugin manifest files exist
#   3. .mcp.json points to a runnable command
#   4. (best-effort) daemon socket exists / sync.health-check responds
#
# Non-zero exit on any failure. Designed to be safe to run repeatedly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="${SCRIPT_DIR}"

red() { printf '\033[31m%s\033[0m\n' "$*"; }
grn() { printf '\033[32m%s\033[0m\n' "$*"; }
yel() { printf '\033[33m%s\033[0m\n' "$*"; }

fail=0

check_file() {
  local path="$1"
  if [[ -f "${path}" ]]; then
    grn "    ✓ ${path}"
  else
    red "    ✗ ${path} (missing)"
    fail=1
  fi
}

check_bin() {
  local name="$1"
  if command -v "${name}" >/dev/null 2>&1; then
    grn "    ✓ ${name} ($(command -v "${name}"))"
  else
    red "    ✗ ${name} (not on PATH)"
    fail=1
  fi
}

echo "==> 1. binaries on PATH"
check_bin p2p-sync
check_bin p2p-sync-mcp

echo
echo "==> 2. plugin manifest files"
check_file "${PLUGIN_DIR}/.claude-plugin/plugin.json"
check_file "${PLUGIN_DIR}/.claude-plugin/marketplace.json"
check_file "${PLUGIN_DIR}/.codex-plugin/plugin.json"
check_file "${PLUGIN_DIR}/.mcp.json"
check_file "${PLUGIN_DIR}/skills/sync/SKILL.md"
for cmd in setup-doctor status invite accept allow-peer peers revoke pending; do
  check_file "${PLUGIN_DIR}/commands/${cmd}.md"
done

echo
echo "==> 3. .mcp.json command resolves"
mcp_cmd=$(python3 -c '
import json, sys
with open(sys.argv[1]) as f:
    j = json.load(f)
print(j["mcpServers"]["p2p-dir-sync"]["command"])
' "${PLUGIN_DIR}/.mcp.json" 2>/dev/null || true)

if [[ -z "${mcp_cmd}" ]]; then
  red "    ✗ could not parse .mcp.json (python3 / jq missing?)"
  fail=1
elif command -v "${mcp_cmd}" >/dev/null 2>&1; then
  grn "    ✓ ${mcp_cmd} ($(command -v "${mcp_cmd}"))"
else
  red "    ✗ ${mcp_cmd} (declared in .mcp.json but not on PATH)"
  fail=1
fi

echo
echo "==> 4. daemon socket / health-check (best-effort)"
SOCK="${HOME}/.local/share/p2p-dir-sync/daemon.sock"
if [[ -S "${SOCK}" ]]; then
  grn "    ✓ ${SOCK} exists"
  # Try a real RPC if `nc -U` is around.
  if command -v nc >/dev/null 2>&1; then
    resp=$(printf '{"method":"sync.health-check"}\n' | nc -U -w 2 "${SOCK}" || true)
    if [[ -n "${resp}" ]]; then
      grn "    ✓ health-check responded"
    else
      yel "    ⚠ health-check did not respond (daemon may be stopped)"
    fi
  else
    yel "    ⚠ nc not available; skipping live RPC probe"
  fi
else
  yel "    ⚠ ${SOCK} not present yet (daemon not started?)"
fi

echo
if [[ ${fail} -ne 0 ]]; then
  red "verify.sh: FAIL"
  exit 1
fi
grn "verify.sh: all checks passed"
