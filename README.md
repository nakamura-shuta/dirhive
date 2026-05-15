# p2p-dir-sync

任意 dir を 信頼できる小さな peer 集団 (= 2-5 名想定) で同期する **macOS daemon + MCP server**。 中央サーバなし、 Iroh QUIC mesh の bilateral invite フローで成立。

```
┌──────────┐         ┌──────────┐
│  alice   │◀────────│   bob    │
│ ~/notes  │  Iroh   │ ~/notes  │
└────┬─────┘  gossip └────┬─────┘
     │     + blob ALPN    │
     │   (via N0 relay)   │
     ▼                    ▼
   bilateral allowlist (= step 5,7 of design §3.4)
```

## What you get

- **daemon (`p2p-sync`)** — `--watch <dir>` で fsnotify + gossip broadcast + 受信側 atomic write
- **MCP server (`p2p-sync-mcp`)** — Claude Code / Codex から 10 個の `sync.*` tool が呼べる
- **plugin (`plugin/`)** — slash commands で 7-step bilateral invite を walk-through
- **launchd integration** — macOS user agent として常駐 / auto-restart
- **smoke scripts (`sandbox/scripts/`)** — 1 / 2 / 3-peer 全自動 e2e 確認

## Quickstart (= 2 peers、 約 1 分)

事前準備: 両 peer で `cargo` (Rust 1.89+) と Python 3 が使えること。

**[Alice]** ── `~/notes` を共有したい側

```sh
git clone <this-repo> && cd p2p-dir-sync
./plugin/scripts/install.sh                       # ~/.local/bin に 2 binary を install
export PATH="$HOME/.local/bin:$PATH"
p2p-sync --watch ~/notes &                        # daemon 起動

# AI agent (= Claude Code / Codex) で:
/plugin install $(pwd)/plugin
/p2p-dir-sync:invite                              # ticket を出力 + restart 指示
launchctl kickstart -k gui/$UID/com.user.p2p-dir-sync  # or daemon を foreground 再起動
# → ticket を Bob に out-of-band で渡す
```

**[Bob]** ── ticket を受け取った後

```sh
./plugin/scripts/install.sh
p2p-sync --watch ~/notes &

# AI agent で:
/plugin install $(pwd)/plugin
/p2p-dir-sync:accept p2psync1-...                 # restart 指示 + my_peer_id を出力
# → daemon を再起動
# → 自分の my_peer_id を Alice に out-of-band で伝える
```

**[Alice]** ── Bob の `my_peer_id` を受け取った後

```
/p2p-dir-sync:allow-peer <bob_id>
```

→ **bilateral mesh 成立**。 以降 `~/notes` 配下の file 変更が両 peer で自動同期される。

詳細は [`docs/operations.md`](docs/operations.md) を、 設計判断は [`docs/design.md`](docs/design.md) を参照。

## Documentation

| doc | 役割 |
|---|---|
| [`docs/design.md`](docs/design.md) | 設計判断 (= **why**)。 layer 構造、 bilateral invite フロー、 security 境界、 持たない feature |
| [`docs/operations.md`](docs/operations.md) | 運用手順 (= **how**)。 install、 daemon 起動、 launchd 常駐、 トラブルシュート、 復旧、 uninstall |
| [`plugin/README.md`](plugin/README.md) | AI agent plugin (= Claude Code / Codex) の slash command 一覧 |
| [`sandbox/scripts/launchd/README.md`](sandbox/scripts/launchd/README.md) | macOS launchd plist sample の install / uninstall 手順 |

## Smoke tests (= regression 検出 / 動作確認)

```sh
./sandbox/scripts/install-smoke.sh    # 1-peer: install → verify → daemon → MCP probe
./sandbox/scripts/2peer-smoke.sh      # 2-peer: 7-step invite + 双方向 file 伝搬
./sandbox/scripts/3peer-smoke.sh      # 3-peer: 完全 mesh allowlist + 6 経路 propagation
```

実機 N0 relay 経由で 1-peer ~15s、 2-peer ~40s、 3-peer ~80s で完走。

## Status

**MVP** — 1-peer install / 2-peer 双方向 / 3-peer 完全 mesh が実機で動作確認済。

| layer | 状態 |
|---|---|
| L1 Iroh (= QUIC + gossip + blob ALPN) | iroh 1.0.0-rc.0 + iroh-blobs 0.101 + iroh-gossip 0.99 |
| L2 daemon (`p2p-sync`) | 9 sync.* RPC + watcher + receive_loop + bootstrap_peers 永続化 |
| L3 MCP server (`p2p-sync-mcp`) | 10 tools (= sync.ping + 9 daemon bridge)、 rmcp 1.7 stdio |
| L4 plugin (`plugin/`) | Claude Code + Codex 両対応 manifest、 8 slash commands |
| L5 sandbox + ops | 1/2/3-peer smoke + launchd plist + operations.md |

CI 観点:
- `cargo test --lib` 170 / 170
- `cargo test --bin p2p-sync-mcp` 4 / 4
- `cargo clippy --all-targets --all-features -- -D warnings` clean
- 3 sandbox smoke すべて green

## Limitations (= 既知の制約、 MVP では意図的に scope 外)

- **folder_secret 漏洩耐性は不完全**: `SyncUpdate.from.id` は payload 内自己申告で署名検証していない。 secret を知る peer は他 peer の id を詐称しうる (= design.md §6.3、 Phase 6 で endpoint-key 署名追加予定)
- **内容暗号化なし**: FileVault 等 fs 暗号化が責務 (= QUIC transport 暗号化はある)
- **大 file 非対応**: `MAX_FILE_SIZE = 10 MiB`、 送受信どちらも超過は skip + warn
- **conflict resolution は手動**: 並行編集で local が異なる内容を受信した場合は `<orig>.conflict-local-<peer8>` に rename して退避。 手動 merge が必要 (operations.md §5.5)
- **revoke は best-effort**: 既存 connection は次回 handshake で reject、 強制 disconnect なし

## Project layout

```
.
├── src/                     # Rust crate
│   ├── allowlist*.rs / send.rs / receive.rs / watcher.rs / runtime.rs / ...
│   ├── daemon/              # Unix socket RPC + dispatch + DaemonState
│   └── bin/
│       ├── p2p-sync.rs      # daemon binary
│       └── p2p-sync-mcp.rs  # MCP server binary
├── docs/                    # design.md + operations.md + schema/
├── plugin/                  # AI agent plugin staging (.claude-plugin/ + .codex-plugin/ etc.)
├── sandbox/scripts/         # 1/2/3-peer smoke + launchd plist sample + lib/
└── tests/                   # integration tests (daemon_smoke / two_peer_sync / pending_schema)
```

## License

(= 公開時に追記)
