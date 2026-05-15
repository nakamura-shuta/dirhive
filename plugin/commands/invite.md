---
description: Generate (or re-show) an invite ticket; output also includes the next-step hint
allowed-tools:
  - mcp__dirhive__sync.invite
---

Call `mcp__dirhive__sync.invite` and present the result.

The response shape is `{ticket, restart_required}`:

- **ticket** starts with `dirhive1-` and contains the `folder_secret` for this group. Treat it as a credential. Print it in a code block but warn the user to share it through a trusted channel only.
- **restart_required** is `true` when the daemon has not yet joined the gossip mesh under the current group identity. Until it restarts, no sync will flow.

After showing the ticket, print a numbered next-step hint:

1. (If `restart_required: true`) Restart the daemon, e.g.:
   - launchd: `launchctl kickstart -k gui/$UID/com.user.dirhive`
   - foreground: stop `dirhive --watch <dir>` with Ctrl+C and start it again
2. Send the ticket to the peer through a trusted channel.
3. After the peer runs `/dirhive:accept`, ask them for their `my_peer_id` value, then run `/dirhive:allow-peer <their_id>` here to complete the bilateral allowlist.

Remind the user that **the ticket is sensitive** — anyone with it can join the group.
