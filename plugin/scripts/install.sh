#!/usr/bin/env bash
# p2p-dir-sync plugin: install script
#
# 1. cargo build --release in the workspace root
# 2. copy `p2p-sync` and `p2p-sync-mcp` to ~/.local/bin/
# 3. print follow-up instructions for the user (launchd plist / plugin enable)
#
# Idempotent: re-running re-copies the binaries.

set -euo pipefail

# --- locate workspace root -------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${PLUGIN_DIR}/.." && pwd)"

if [[ ! -f "${WORKSPACE_ROOT}/Cargo.toml" ]]; then
  echo "ERROR: cannot find Cargo.toml at ${WORKSPACE_ROOT}" >&2
  exit 1
fi

# --- build ----------------------------------------------------------------
echo "==> Building p2p-sync + p2p-sync-mcp (release)"
cd "${WORKSPACE_ROOT}"
cargo build --release --bin p2p-sync --bin p2p-sync-mcp

# --- install binaries -----------------------------------------------------
BIN_DIR="${HOME}/.local/bin"
mkdir -p "${BIN_DIR}"

for bin in p2p-sync p2p-sync-mcp; do
  src="${WORKSPACE_ROOT}/target/release/${bin}"
  dst="${BIN_DIR}/${bin}"
  if [[ ! -x "${src}" ]]; then
    echo "ERROR: build artifact missing: ${src}" >&2
    exit 1
  fi
  cp -f "${src}" "${dst}"
  chmod 0755 "${dst}"
  echo "    installed ${dst}"
done

# --- PATH check -----------------------------------------------------------
if ! command -v p2p-sync >/dev/null 2>&1; then
  cat <<EOF

WARNING: ${BIN_DIR} is not in your PATH.
  Add the following to your shell rc:

    export PATH="\${HOME}/.local/bin:\${PATH}"

EOF
fi

# --- follow-up hints ------------------------------------------------------
cat <<EOF

==> Done.

Next steps:

1. Start the daemon (foreground for the first run):

     p2p-sync --watch ~/notes

   For background / auto-start on macOS, drop a launchd plist into
   ~/Library/LaunchAgents/com.user.p2p-dir-sync.plist. A reference plist is in
   docs/design.md §5.4.

2. Wire the plugin into your AI agent of choice:

     # Claude Code
     /plugin install ${PLUGIN_DIR}

     # Codex / others
     point them at ${PLUGIN_DIR}/.codex-plugin/plugin.json

3. From the agent, run /p2p-dir-sync:setup-doctor to verify everything is wired.

4. (Optional) Pin the MCP command to an absolute path.

   The staged .mcp.json declares `"command": "p2p-sync-mcp"`, which relies on
   the MCP host's \$PATH. Some hosts (Claude Code GUI / launchd) start with a
   minimal \$PATH and may fail to find it. If you hit this, edit
   ${PLUGIN_DIR}/.mcp.json and set:

       "command": "${BIN_DIR}/p2p-sync-mcp"

   ./verify.sh prints a warning when the command is relative; this is benign in
   shell contexts but worth pinning for GUI launches.

EOF
