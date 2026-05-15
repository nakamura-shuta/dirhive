---
description: Diagnose p2p-dir-sync setup with a fixed 4-step probe (ping → health-check → status → recent-log)
allowed-tools:
  - mcp__p2p-dir-sync__sync.ping
  - mcp__p2p-dir-sync__sync.health-check
  - mcp__p2p-dir-sync__sync.status
  - mcp__p2p-dir-sync__sync.recent-log
---

Run this fixed sequence and stop at the first failure, printing a recovery hint:

1. **Step 1 — sync.ping**
   Call `mcp__p2p-dir-sync__sync.ping`.
   - On error: the MCP server itself is unreachable. Tell the user the plugin install is broken (`/plugin install` and the `p2p-sync-mcp` binary must be on `PATH`). Stop here.
   - On success: print `✓ Step 1 sync.ping            : MCP server reachable`.

2. **Step 2 — sync.health-check**
   Call `mcp__p2p-dir-sync__sync.health-check`.
   - On error: the daemon is not reachable. Tell the user to start `p2p-sync --watch <dir>` (or `launchctl bootstrap` if installed). Stop here.
   - On success: print `✓ Step 2 sync.health-check    : daemon connected` then summarise the response (socket / key path / watched dir / group_initialized / gossip_subscribed / restart_required). If `restart_required` is true, **emphasise** that the daemon needs a restart.

3. **Step 3 — sync.status**
   Call `mcp__p2p-dir-sync__sync.status`.
   - Print peer count, open_all flag, uptime, recent_pending_count. If `peer_count == 0` and `open_all == false`, suggest the user run `/p2p-dir-sync:invite` or `/p2p-dir-sync:accept` to bootstrap a peer.

4. **Step 4 — sync.recent-log**
   Call `mcp__p2p-dir-sync__sync.recent-log` with `lines: 20`.
   - Show the tail as an `ℹ Step 4 sync.recent-log (last 20 lines, redacted):` block. Mention that `p2psync1-...` and 32+ hex tokens are redacted at the daemon.

Format the whole output with one line per step, prefixed by `✓` / `✗` / `ℹ`, the same way that `setup-doctor` is documented in `docs/design.md` §7.
