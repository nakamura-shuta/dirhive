# dirhive plugin

**Optional integration. MCP-only users do not need this plugin.** dirhive MCP
server だけなら repo root `README.md` の `claude mcp add` で十分。 この plugin
を入れるのは:

- `/dirhive:*` slash command で 7-step bilateral invite を walk-through したい場合
- `SKILL.md` 経由で AI agent に手順遵守を促したい場合

Claude Code / Codex plugin that wraps the `dirhive` daemon and the `dirhive-mcp`
MCP server. Lets an AI agent run the canonical 7-step bilateral invite flow and
inspect the daemon state without leaving the chat.

## Layout

```
plugin/
├── .claude-plugin/
│   ├── plugin.json          # Claude Code plugin manifest
│   └── marketplace.json     # Optional marketplace registration
├── .codex-plugin/
│   └── plugin.json          # Codex plugin manifest (= mcpServers + skills pointers)
├── .mcp.json                # MCP server registration (shared by both manifests)
├── skills/
│   └── sync/SKILL.md        # AI-agent instructions (= when to invoke each tool)
├── commands/                # 8 slash commands
│   ├── setup-doctor.md
│   ├── status.md
│   ├── invite.md
│   ├── accept.md
│   ├── allow-peer.md
│   ├── peers.md
│   ├── revoke.md
│   └── pending.md
├── scripts/
│   └── install.sh           # build + install binaries to ~/.local/bin
├── verify.sh                # post-install sanity check
└── README.md                # this file
```

## Install

```sh
./scripts/install.sh
./verify.sh
```

`install.sh` builds `dirhive` and `dirhive-mcp` in `--release` mode and
copies them to `~/.local/bin/`. `verify.sh` then checks that the binaries are
on `PATH`, all manifest files are present, the command declared in `.mcp.json`
is resolvable, and (best-effort) the daemon socket responds.

After installing, point your AI agent at this directory:

```sh
# Claude Code
/plugin install path/to/dirhive/plugin

# Codex
# Use the .codex-plugin/plugin.json manifest per Codex CLI docs.
```

## Slash commands

| command | what it does |
|---|---|
| `/dirhive:setup-doctor` | 4-step probe (ping → health-check → status → recent-log) |
| `/dirhive:status` | One-shot summary of peer count, uptime, group state |
| `/dirhive:invite` | Generate or re-show your invite ticket |
| `/dirhive:accept <ticket> [label]` | Accept a peer's invite ticket |
| `/dirhive:allow-peer <peer_id> [label]` | Add the counter-half of the bilateral allowlist |
| `/dirhive:peers` | List allowed peers with `last_seen_at` |
| `/dirhive:revoke <peer_id>` | Remove a peer from the local allowlist |
| `/dirhive:pending [limit]` | Show recent incoming change log |

## The 7-step bilateral invite flow (= the design.md §3.4 canon)

Run from each side's AI agent:

```text
[Alice]                                        [Bob]
1. /dirhive:invite       → ticket
2. (restart alice's daemon)
3. (hand ticket to bob)        → → →           4. /dirhive:accept <ticket>
                                                  → records bob's my_peer_id
                                               5. (restart bob's daemon)
                                               6. (tell alice my_peer_id)
7. /dirhive:allow-peer <bob_id>           ← ← ←

sync is now bilateral; alice ↔ bob will sync the watched dirs.
```

Forgetting any of step 2 / 5 / 7 leaves the channel half-open. The plugin's
`SKILL.md` reminds the agent of this each time.

## Security notes

- The invite ticket (`dirhive1-...`) contains the `folder_secret` — anyone with
  the ticket can join the mesh. Treat it as a credential.
- The daemon enforces a per-peer allowlist for blob fetch (= AllowlistBlobs in
  `src/allowlist_blobs.rs`) and a `from.id` allowlist for gossip receive. **Both
  are DoS / hygiene layers, not cryptographic identity** — design.md §6.3 calls
  this out.
- `~/Library/Logs/dirhive.log` redacts `dirhive1-...` envelopes and 32+
  char hex tokens before exposing log lines through `/dirhive:setup-doctor`
  step 4.

## Uninstall

```sh
rm ~/.local/bin/dirhive ~/.local/bin/dirhive-mcp
rm -rf ~/.local/share/dirhive ~/.config/dirhive
# launchd users:
launchctl bootout gui/$UID/com.user.dirhive || true
rm ~/Library/LaunchAgents/com.user.dirhive.plist
```

Removing `~/.local/share/dirhive` wipes the group identity (= folder
secret), allowlist, blobs, and the change log. The plugin scripts and slash
commands are stateless and live alongside this directory.
