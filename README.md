# dirhive

iroh で任意のディレクトリを同期する MCP サーバ。

- daemon (`dirhive`) — `--watch <dir>` で fsnotify + gossip broadcast + 受信側 atomic write
- MCP server (`dirhive-mcp`) — Claude Code / Codex 等から `sync.*` tool を呼べる
- plugin (`plugin/`) — `/dirhive:*` slash command + SKILL.md (= optional)

## Install

```sh
git clone https://github.com/nakamura-shuta/dirhive.git
cd dirhive
./plugin/scripts/install.sh           # cargo build + ~/.local/bin に 2 binary 配置
export PATH="$HOME/.local/bin:$PATH"
```

事前に Rust toolchain (1.89+) が必要。 daemon + MCP server の基本利用には Python 不要。

## Start the daemon

同期したい dir を foreground で開始。

```sh
mkdir -p ~/notes
dirhive --watch ~/notes
```

別 terminal で動かしっぱなしにする。 `~/.local/share/dirhive/daemon.sock` が作られて Claude Code から接続できるようになる。

## Connect Claude Code

dirhive-mcp を Claude Code の MCP server として登録。

```sh
claude mcp add --scope user --transport stdio dirhive -- "$(command -v dirhive-mcp)"
```

これで Claude Code から `sync.*` 10 tool が呼べる。 確認:

> Claude Code chat: 「sync.ping を呼んで」 → `pong`
> Claude Code chat: 「sync.health-check を呼んで」 → daemon の health 情報

## Quickstart: 2 peers

bilateral invite で 2 台を同期させる手順。 Claude Code chat に自然文で頼むと、 内部で MCP tool が呼ばれる。

**[Alice]**

```
Claude Code chat: 「dirhive で招待 ticket を作って」
  → Claude が sync.invite を呼ぶ → ticket (= dirhive1-...) + restart 指示が出る
daemon を再起動 (Ctrl-C + 再度 dirhive --watch ~/notes)
ticket を Bob に out-of-band で渡す (= Slack / Signal 等)
```

**[Bob]** (ticket 受領後、 先に Install / Start / Connect を済ませた前提)

```
Claude Code chat: 「<ticket> を受け入れて。 label は alice」
  → Claude が sync.accept-invite を呼ぶ → my_peer_id + restart 指示が出る
daemon を再起動
my_peer_id を Alice に伝える
```

**[Alice]**

```
Claude Code chat: 「<bob_id> を許可して。 label は bob」
  → Claude が sync.allow-peer を呼ぶ
```

→ bilateral mesh 成立。 `~/notes` 配下が両 peer で同期される。

Prefer slash commands? Install the optional plugin (= Advanced 参照) and use `/dirhive:invite`, `/dirhive:accept`, `/dirhive:allow-peer` instead.

## Advanced

### Optional plugin (= slash commands + SKILL.md)

Claude Code に plugin を入れると `/dirhive:*` slash command が使え、 SKILL.md が agent に 7-step フローの遵守を促す。

```sh
# Claude Code chat 内で:
/plugin install /path/to/dirhive/plugin
```

提供される slash command 一覧と詳細は [`plugin/README.md`](plugin/README.md) 参照。

### Background daemon (= launchd で常駐化)

macOS で daemon を auto-start させる場合:

```sh
./sandbox/scripts/launchd/install-launchd.sh --watch ~/notes
# uninstall:
./sandbox/scripts/launchd/uninstall-launchd.sh
```

plist 生成に Python 3 + `plistlib` を使う。

### Smoke test (= 開発者向け)

```sh
./sandbox/scripts/2peer-smoke.sh         # 2 peer 同期確認
./sandbox/scripts/3peer-smoke.sh         # 3 peer full mesh + 6 propagation paths
```

Python 3 が必要。

## Documentation

- [`docs/design.md`](docs/design.md) — 設計判断
- [`docs/operations.md`](docs/operations.md) — 詳細 reference / トラブルシュート / 復旧
- [`plugin/README.md`](plugin/README.md) — plugin と slash command 一覧 (= optional)

## License

MIT. See [`LICENSE`](LICENSE).
