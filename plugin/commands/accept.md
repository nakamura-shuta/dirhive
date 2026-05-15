---
description: Accept an invite ticket received from another peer
argument-hint: <ticket> [label]
allowed-tools:
  - mcp__p2p-dir-sync__sync.accept-invite
---

Parse the arguments:

- `$1` = the ticket string (starts with `p2psync1-`)
- `$2` (optional) = a human-readable label for the inviter (e.g. "alice")

If the user did not provide a ticket, stop and ask them for it explicitly.

Call `mcp__p2p-dir-sync__sync.accept-invite` with `{ticket: $1, label: $2}`.

The response shape is `{peer_id, label, my_peer_id, restart_required}`:

- **peer_id** = the inviter's EndpointId. They are now in *your* allowlist.
- **my_peer_id** = your own EndpointId. The inviter needs this to run their `allow-peer` step.
- **restart_required** = `true` on first accept (= folder_secret was just adopted). Until you restart, nothing will sync.

After showing the result, print a numbered next-step hint:

1. (If `restart_required: true`) Restart your daemon, e.g.:
   - launchd: `launchctl kickstart -k gui/$UID/com.user.p2p-dir-sync`
   - foreground: stop `p2p-sync --watch <dir>` with Ctrl+C and start it again
2. Tell the inviter to run `/p2p-dir-sync:allow-peer <my_peer_id>` on their side, supplying *your* `my_peer_id` value.
3. Once their `allow-peer` is done, both sides are in the mesh and edits should start propagating. Verify with `/p2p-dir-sync:peers`.

If `accept-invite` returns an error mentioning a different `folder_secret`, the daemon is already initialised with another group. Either keep using the existing group, or delete `~/.local/share/p2p-dir-sync/folder-secret.bin` and restart to reset (this wipes the current group identity).
