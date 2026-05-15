#!/usr/bin/env bash
# uninstall the dirhive launchd user agent.
#
# Usage: uninstall-launchd.sh
#
# Steps:
#   1. launchctl bootout gui/$UID/com.user.dirhive (= SIGTERM → graceful)
#   2. plist file 削除
#
# Idempotent: 何度実行しても 0 exit、 既に bootout 済 / plist 不在でも error なし。

set -euo pipefail

LABEL="com.user.dirhive"
PLIST_TARGET="${HOME}/Library/LaunchAgents/${LABEL}.plist"

# --- bootout --------------------------------------------------------------
if launchctl print "gui/${UID}/${LABEL}" >/dev/null 2>&1; then
  echo "==> launchctl bootout gui/${UID}/${LABEL}"
  if launchctl bootout "gui/${UID}/${LABEL}" 2>/dev/null; then
    echo "    ✓ booted out"
  else
    echo "    ⚠ bootout returned non-zero (= already gone? still tearing down?)"
  fi
else
  echo "==> service not loaded; skipping bootout"
fi

# --- remove plist ---------------------------------------------------------
if [[ -f "${PLIST_TARGET}" ]]; then
  rm -f "${PLIST_TARGET}"
  echo "==> removed ${PLIST_TARGET}"
else
  echo "==> plist already absent: ${PLIST_TARGET}"
fi

cat <<EOF

==> Done.

Note:
  - daemon process が居なくなるまで 数 s 〜 15 s かかります (= ExitTimeOut=15)。
  - daemon 自身が write した state 系 file (~/.local/share/dirhive/,
    ~/.config/dirhive/, ~/Library/Logs/dirhive.*.log) は残っています。
    削除する場合は plugin/README.md "Uninstall" セクションを参照。

EOF
