---
description: Show recent incoming change log entries (Upsert / Tombstone) recorded by the daemon
argument-hint: [limit]
allowed-tools:
  - mcp__dirhive__sync.list-pending
---

Parse the optional argument:

- `$1` (optional) = max number of entries to fetch. Default = no truncation.

Call `mcp__dirhive__sync.list-pending` with `{limit: $1}` (omit `limit` if no arg).

The response shape is `{entries: [PendingEntry...]}`. Each entry is either:

- **Upsert**: `{kind: "upsert", schema_version, rel_path, received_at, source_peer, blob_hash, bytes}`
- **Tombstone**: `{kind: "tombstone", schema_version, rel_path, received_at, source_peer}`

Format newest first. For each entry, render one line:

```
<received_at> <peer8> <kind> <rel_path> [<bytes>B]
```

After the list, briefly say:

- An empty list means the daemon has not recorded any incoming change since startup.
- The log is **per-watched-dir** (= keyed by the BLAKE3 hash of the canonical path). If the user changes `--watch <dir>`, this list resets.
- For server-side log lines (errors, neighbor up/down, daemon-internal events), use `/dirhive:status` → `recent-log` instead.
