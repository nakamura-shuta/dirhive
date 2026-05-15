---
name: sync
description: Use when the user wants to sync a directory with peers via P2P. Triggers on phrases like "sync this dir", "share my notes with bob", "invite a peer", "accept a sync invitation", "what peers do I have", "show sync status".
---

# dirhive

This skill exposes a P2P directory sync daemon (running on the local machine) that watches a specified directory and propagates file changes to a small set of trusted peers over Iroh. Use it when the user asks anything related to file sharing between trusted devices/people that is **not** mediated by a centralised cloud.

## How sync forms (= bilateral invite flow, 7 steps)

This is the canonical flow. If the user's intent matches any step, run the corresponding tool.

```text
[Alice]  (daemon running, no folder-secret yet)
1. /dirhive:invite
   → returns {ticket, restart_required: true}
2. Alice restarts the daemon so it joins the gossip mesh
3. Alice hands the ticket to Bob out-of-band (Slack, iMessage, ...)

[Bob]  (daemon running, no folder-secret yet)
4. /dirhive:accept <ticket>
   → returns {peer_id, my_peer_id, restart_required: true}
5. Bob restarts the daemon
6. Bob asks Alice to run /dirhive:allow-peer <Bob's my_peer_id>

[Alice]
7. /dirhive:allow-peer <Bob_id>
   → bilateral allowlist completed, sync starts
```

Forgetting **any** of step 2 / 5 / 6/7 leaves the channel half-open and nothing propagates. Always read each tool's `restart_required` field and instruct the user accordingly.

## Tools (= sync.*)

| tool | when to use |
|---|---|
| `sync.ping` | Verify the MCP server itself is reachable. Does not require the daemon. |
| `sync.health-check` | Verify the daemon is running and configured. Returns paths + dynamic status. |
| `sync.status` | Quick summary of peers, uptime, group state. Use for "what's the state". |
| `sync.invite` | Generate (first time) or re-show the invite ticket. First call returns `restart_required: true`. |
| `sync.accept-invite` | Adopt a peer's invite ticket. Needs `{ticket, label?}`. |
| `sync.allow-peer` | Add the counter-half of bilateral allowlist. Needs `{peer_id, label?}`. |
| `sync.list-peers` | Show currently allowed peers, with `last_seen_at` as data-plane health proxy. |
| `sync.revoke` | Remove a peer from the local allowlist. Needs `{peer_id}`. |
| `sync.list-pending` | Show the recent incoming change log (Upsert / Tombstone). |
| `sync.recent-log` | Tail the daemon log (secrets are redacted). |

## Security notes (= things to tell the user)

- The invite ticket starts with `dirhive1-` and contains a `folder_secret`. **Anyone who has the ticket can join the mesh.** Hand it through a trusted channel and treat it as a credential.
- Sync is bilateral: both peers must run `allow-peer` (or `accept-invite`) of the other side. If file changes are not reaching the peer, check both `list-peers` outputs.
- `last_seen_at == null` for a peer means the data plane has never succeeded. If the user reports "I added bob but nothing happens", that's the first place to look.
- The daemon does **not** trust the gossip mesh as an adversary boundary — a peer that knows the `folder_secret` can spoof the `from.id` field. The allowlist is a DoS / hygiene layer, not a cryptographic one.

## Default behaviour

When the user asks `"sync this directory with my other machine"` without further context, walk them through the 7-step bilateral flow. Use the slash commands (`/dirhive:invite`, `/dirhive:accept`, etc.) so each step is visible in the chat.
