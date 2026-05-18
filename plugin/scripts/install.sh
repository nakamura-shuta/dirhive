#!/usr/bin/env bash
# dirhive plugin: install script
#
# 1. cargo build --release in the workspace root
# 2. copy `dirhive` and `dirhive-mcp` to ~/.local/bin/
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
echo "==> Building dirhive + dirhive-mcp (release)"
cd "${WORKSPACE_ROOT}"
cargo build --release --bin dirhive --bin dirhive-mcp

# --- install binaries -----------------------------------------------------
BIN_DIR="${HOME}/.local/bin"
mkdir -p "${BIN_DIR}"

for bin in dirhive dirhive-mcp; do
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
if ! command -v dirhive >/dev/null 2>&1; then
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

     mkdir -p ~/notes
     dirhive --watch ~/notes

2. Connect Claude Code (= register the MCP server at user scope):

     claude mcp add --scope user --transport stdio dirhive \\
       -- ${BIN_DIR}/dirhive-mcp

   Then in chat: ask Claude to "sync.ping を呼んで" — you should see "pong".

3. (Optional) Install the plugin for /dirhive:* slash commands + SKILL.md
   guidance. MCP-only users do not need this.

     # Claude Code
     /plugin install ${PLUGIN_DIR}

     # Codex / others
     point them at ${PLUGIN_DIR}/.codex-plugin/plugin.json

4. (Optional) Background daemon via launchd:

     ./sandbox/scripts/launchd/install-launchd.sh --watch ~/notes

5. (Optional) If Claude Code GUI cannot find dirhive-mcp at boot, pin the
   command's absolute path in ${PLUGIN_DIR}/.mcp.json:

       "command": "${BIN_DIR}/dirhive-mcp"

   ./verify.sh warns when the command is relative; benign in shell, but
   worth pinning for GUI launches.

EOF
