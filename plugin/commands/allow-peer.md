---
description: Add a peer (= the inviter or invitee's my_peer_id) to the local allowlist
argument-hint: <peer_id> [label]
allowed-tools:
  - mcp__p2p-dir-sync__sync.allow-peer
---

Parse the arguments:

- `$1` = the peer's EndpointId (64 hex chars)
- `$2` (optional) = a human-readable label (e.g. "bob")

If the user did not provide a peer_id, stop and ask them for it explicitly.

Call `mcp__p2p-dir-sync__sync.allow-peer` with `{peer_id: $1, label: $2}`.

The response shape is `{added, peer_id, label}`:

- **added** = `true` if this is a new entry, `false` if the peer was already in the allowlist (idempotent).

After showing the result, briefly remind the user:

- This call alone does **not** require a daemon restart (the in-memory allowlist is updated and persisted atomically).
- Bilateral sync needs **both sides** to allow each other. If you just ran this, the other side should have called `/p2p-dir-sync:invite` (if you are accepting) or `/p2p-dir-sync:accept` (if you invited). If you ran this without the other side's reciprocal step, blob fetch will still be one-way blocked.
- Verify with `/p2p-dir-sync:peers`. The `last_seen_at` field will stay `null` until the data plane actually succeeds (= first blob fetch / Tombstone in either direction).
