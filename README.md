# dirhive

P2P directory sync daemon + MCP server. 任意 dir を信頼できる小さな peer 集団で同期する。 中央サーバなし、 Iroh QUIC mesh の bilateral invite フロー。

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

事前に Rust toolchain (1.89+) と Python 3 が必要。

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

## Documentation

- [`docs/design.md`](docs/design.md) — 設計判断
- [`docs/operations.md`](docs/operations.md) — 運用手順 (= launchd 常駐 / トラブルシュート / 復旧 / uninstall)
- [`plugin/README.md`](plugin/README.md) — plugin と slash command 一覧
- [`sandbox/scripts/`](sandbox/scripts/) — 1/2/3-peer smoke test + launchd plist sample

## License

MIT. See [`LICENSE`](LICENSE).
