---
description: List currently allowed peers with their last_seen_at (data-plane health proxy)
allowed-tools:
  - mcp__p2p-dir-sync__sync.list-peers
---

Call `mcp__p2p-dir-sync__sync.list-peers` and present the result.

The response shape is `{peers: [{peer_id, label, added_at, last_seen_at}], open_all}`.

Format each peer on one line, with:

- short peer_id (= first 8 chars of the EndpointId hex)
- the label (if set)
- `last_seen_at` relative time (e.g. "12s ago", "5m ago"). If `last_seen_at == null`, render as `last_seen=null (data plane never succeeded)`.

After the list, summarise:

- `open_all=true` → the daemon currently accepts blob fetch from **any** peer that knows the folder_secret. Useful for development; production deployments should leave this `false`.
- If any peer has `last_seen_at == null`, hint that the other side may not have completed their `accept` / `allow-peer` step, or that no edits have flowed yet. Suggest verifying with `/p2p-dir-sync:status` on both sides.
- An empty list means no peer is currently allowed. Use `/p2p-dir-sync:invite` or `/p2p-dir-sync:accept` to bootstrap.
