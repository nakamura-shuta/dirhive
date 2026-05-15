---
description: Remove a peer from the local allowlist (best-effort, does not force-close existing connections)
argument-hint: <peer_id>
allowed-tools:
  - mcp__dirhive__sync.revoke
---

Parse the argument:

- `$1` = the peer's EndpointId (64 hex chars) to revoke

If the user did not provide a peer_id, stop and ask them for it explicitly (e.g. via `/dirhive:peers` first).

Call `mcp__dirhive__sync.revoke` with `{peer_id: $1}`.

The response shape is `{removed, peer_id}`:

- **removed** = `true` if the peer was actually in the allowlist, `false` if not.

After showing the result, remind the user:

- This is **one-sided**. The peer may still know your `folder_secret` and will still see the gossip mesh; further blob fetch from your side is blocked, but they will not be force-disconnected from existing connections.
- For a stronger revoke (= rotate the group identity), delete `~/.local/share/dirhive/folder-secret.bin` on every honest peer and re-bootstrap with new invites. There is no in-band "rotate" yet.
- Verify with `/dirhive:peers` that the entry is gone.
