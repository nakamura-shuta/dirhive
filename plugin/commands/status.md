---
description: Show a summary of p2p-dir-sync daemon state (peers, uptime, group, restart_required)
allowed-tools:
  - mcp__p2p-dir-sync__sync.status
---

Call `mcp__p2p-dir-sync__sync.status` and present the result as a short, human-friendly summary.

Fields to highlight:

- **watched_dir** — the directory currently being synced
- **peer_count** + **open_all** — how many peers we'd accept blob fetches from
- **uptime_secs** — daemon uptime
- **group_initialized** — whether `folder-secret.bin` exists
- **gossip_subscribed** — whether the current daemon process is in the mesh
- **restart_required** — `group_initialized && !gossip_subscribed`; if `true`, tell the user to restart the daemon (e.g. `launchctl kickstart -k gui/$UID/com.user.p2p-dir-sync`) before any sync will run
- **recent_pending_count** — number of incoming changes the daemon has logged recently

If `peer_count == 0` and `open_all == false`, recommend running `/p2p-dir-sync:invite` (if this is the first peer) or `/p2p-dir-sync:accept` (if a ticket was received from someone else).
