# dirhive

iroh で任意のディレクトリを同期する MCP サーバ。

- daemon (`dirhive`) — `--watch <dir>` で fsnotify + gossip broadcast + 受信側 atomic write
- MCP server (`dirhive-mcp`) — Claude Code / Codex 等から `sync.*` tool を呼べる
- plugin (`plugin/`) — 7-step bilateral invite を walk-through する slash commands

## Install

```sh
git clone https://github.com/nakamura-shuta/dirhive.git
cd dirhive
./plugin/scripts/install.sh           # cargo build + ~/.local/bin に 2 binary 配置
export PATH="$HOME/.local/bin:$PATH"
```

事前に Rust toolchain (1.89+) が必要。 daemon + MCP server の基本利用には Python 不要。

> launchd 常駐 (`sandbox/scripts/launchd/install-launchd.sh`) / smoke test (`sandbox/scripts/*-smoke.sh`) / 手動 RPC (`sandbox/scripts/lib/rpc.py`) を使う場合のみ Python 3 が必要。

## Quickstart (= 2 peers)

**[Alice]**

```sh
dirhive --watch ~/notes &              # daemon 起動

# AI agent で:
/plugin install $(pwd)/plugin
/dirhive:invite                        # ticket 出力 + restart 指示
# daemon を再起動 (foreground なら Ctrl-C + 再起動)
# ticket を Bob に out-of-band で渡す
```

**[Bob]** (ticket 受領後)

```sh
dirhive --watch ~/notes &

/plugin install $(pwd)/plugin
/dirhive:accept dirhive1-...           # restart 指示 + my_peer_id 出力
# daemon を再起動
# my_peer_id を Alice に伝える
```

**[Alice]**

```
/dirhive:allow-peer <bob_id>
```

→ bilateral mesh 成立。 `~/notes` 配下が両 peer で同期される。

## For AI agents (= MCP / slash commands)

`dirhive-mcp` は stdio MCP server。 Claude Code / Codex から `sync.*` tool を呼んで daemon を操作できる。 plugin (`plugin/`) を install すると以下の slash command が使える:

| command | 用途 |
|---|---|
| `/dirhive:setup-doctor` | 4-step probe (ping → health-check → status → recent-log) |
| `/dirhive:status` | peer 数 / uptime / group state の要約 |
| `/dirhive:invite` | 招待 ticket 生成 (= mesh 参加 credential) |
| `/dirhive:accept <ticket> [label]` | 受信した ticket を承認 |
| `/dirhive:allow-peer <peer_id> [label]` | bilateral allowlist の反対側を追加 (= 7-step の step 7) |
| `/dirhive:peers` | 許可済み peer 一覧 + `last_seen_at` |
| `/dirhive:revoke <peer_id>` | peer を allowlist から削除 |
| `/dirhive:pending [limit]` | 直近の受信変更 log |

7-step bilateral invite の正確な walk-through と MCP 直叩き (= `mcp__dirhive__sync_*`) の使い分けは [`plugin/README.md`](plugin/README.md) を参照。

## Documentation

- [`docs/design.md`](docs/design.md) — 設計判断
- [`docs/operations.md`](docs/operations.md) — 運用手順 (= launchd 常駐 / トラブルシュート / 復旧 / uninstall)
- [`plugin/README.md`](plugin/README.md) — plugin と slash command 一覧
- [`sandbox/scripts/`](sandbox/scripts/) — 1/2/3-peer smoke test + launchd plist sample

## License

MIT. See [`LICENSE`](LICENSE).
