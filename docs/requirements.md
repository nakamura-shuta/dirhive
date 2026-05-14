# p2p-dir-sync 要件定義書

作成日: 2026-05-14

## 1. 目的

`p2p-dir-sync` は、任意のローカルディレクトリを P2P で同期するための、シンプルで安全な同期 daemon + MCP plugin である。

主目的は、Syncthing のような高機能な汎用同期ツールを置き換えることではない。AI agent から扱いやすい、最小限で理解しやすい P2P 同期 control plane を提供する。

## 2. 背景

当初は `iroh-wiki-sync` として、LLM Wiki の操作機能と P2P 同期機能を 1 つの実装にまとめていた。

その過程で次のことが分かった。

- LLM Wiki は knowledge workflow であり、read / write / search / ingest / lint / repair などを扱う。
- P2P Sync は transport / replication であり、watch / invite / accept / revoke / conflict / tombstone などを扱う。
- 両者は関心が異なる。
- LLM Wiki の情報構造は今後も変わり得る。
- P2P Sync は LLM Wiki を知らない方が再利用性が高い。

そのため、P2P 同期部分を独立した `p2p-dir-sync` として切り出す。

## 3. 基本方針

### 3.1 中核方針

- 任意ディレクトリを P2P 同期する。
- LLM Wiki の存在を知らない。
- AI agent から安全に操作できる MCP interface を提供する。
- daemon は同期処理を担当し、MCP server は daemon の control plane を提供する。
- plugin は Claude Code / Codex などから daily use できる UX を提供する。

### 3.2 やらないこと

- Syncthing 相当の高機能同期ツールを目指さない。
- LLM Wiki 専用の概念を持たない。
- `entity-registry.json` / `source-map.json` / wiki schema を解釈しない。
- AI agent から任意 path を自由に同期対象追加できる設計にしない。
- 初期 MVP では複数 folder 管理、ignore pattern、file versioning、GUI を持たない。

## 4. 想定ユーザー

- 複数 Mac 間で小さな作業ディレクトリを同期したい開発者。
- Claude Code / Codex などの AI agent から、同期状態や peer 招待を確認したいユーザー。
- LLM Wiki などの上位アプリケーションから、P2P 同期 backend として利用したいユーザー。

## 5. 利用シナリオ

### 5.1 sync-only

任意ディレクトリを 2 台以上の Mac で同期する。

例:

```bash
p2p-dir-sync --watch ~/Documents/notes
```

### 5.2 AI agent からの同期管理

Claude Code / Codex から次のような操作を行う。

- 同期 daemon が起動しているか確認する。
- 自分の invite ticket を発行する。
- 相手から受け取った ticket を accept する。
- 現在の peer 一覧を見る。
- 最近受信した変更を見る。
- 問題がある場合に setup doctor で診断する。

### 5.3 LLM Wiki からの利用

LLM Wiki は `docs/llm-wiki/` を通常の同期対象ディレクトリとして `p2p-dir-sync` に渡す。

`p2p-dir-sync` 側は、そのディレクトリが wiki であることを知らない。

## 6. 機能要件

### 6.1 Daemon

daemon は以下を行う。

- `--watch <dir>` で指定された 1 ディレクトリを監視する。
- file create / update を peer に送信する。
- file delete を Tombstone として peer に送信する。
- peer から受信した変更を local directory に反映する。
- 同時編集時は local file を conflict backup として退避する。
- endpoint key を永続化する。
- allowlist により受信 peer を制御する。
- Unix socket で control API を提供する。

初期 MVP では 1 daemon = 1 watched directory とする。

### 6.2 P2P 同期

P2P 層は Iroh を利用する想定とする。

必要な要素:

- Endpoint
- Gossip
- Blobs
- Router
- invite ticket
- peer allowlist

同期対象は通常 file とする。ディレクトリ構造は再帰的に扱う。

### 6.3 File Watch

初期 MVP では macOS を対象とする。

- default watcher は OS 推奨 backend を使う。
- rename / move の安定性が必要な場合は poll backend を選べるようにする。
- symlink を追跡しない。
- hidden dir や editor temporary file は noise として除外する。

### 6.4 Conflict

同じ path に対して local と remote の変更が衝突する場合、remote を上書きする前に local file を退避する。

例:

```text
foo.md
foo.md.conflict-local-<peer-shortid>
```

初期 MVP では last-writer-wins + conflict backup とする。

CRDT は採用しない。

### 6.5 Invite / Peer 管理

必要な操作:

- invite ticket を発行する。
- ticket を accept して peer を allowlist に追加する。
- peer 一覧を表示する。
- peer を revoke する。

初期状態では 1 人運用向けの open_all mode を許容する。peer を accept した時点で strict mode に移行する。

### 6.6 Pending Log

受信した変更を pending log として記録する。

用途:

- 最近どの peer から何を受信したか確認する。
- 上位アプリケーションが「未確認の同期変更」を表示する。
- LLM Wiki などが git status と組み合わせて変更要約を作る。

`p2p-dir-sync` は git を知らない。git status との結合は上位レイヤで行う。

### 6.7 MCP Server

MCP server は daemon の control plane を提供する。

初期 MVP の tool:

| tool | 目的 |
|---|---|
| `sync.ping` | MCP server の疎通確認 |
| `sync.health-check` | daemon / key / blobs / pending dir / watched dir の確認 |
| `sync.status` | 同期状態の要約 |
| `sync.invite` | invite ticket 発行 |
| `sync.accept-invite` | peer ticket を allowlist に追加 |
| `sync.list-peers` | peer 一覧 |
| `sync.revoke` | peer revoke |
| `sync.list-pending` | 最近の受信変更一覧 |
| `sync.recent-log` | daemon log の最近行 |

初期 MVP では、MCP tool から folder 追加 / 削除は行わない。

### 6.8 Plugin

Claude Code / Codex 向け plugin を提供する。

想定 command:

| command | 目的 |
|---|---|
| `/p2p-dir-sync:setup-doctor` | 全体診断 |
| `/p2p-dir-sync:status` | 同期状態確認 |
| `/p2p-dir-sync:invite` | ticket 発行 |
| `/p2p-dir-sync:accept` | ticket accept |
| `/p2p-dir-sync:peers` | peer 一覧 |
| `/p2p-dir-sync:revoke` | peer revoke |
| `/p2p-dir-sync:pending` | 最近の受信変更 |

plugin は使いやすさのための UX layer であり、同期ロジックは持たない。

## 7. 非機能要件

### 7.1 安全性

- daemon は明示された watch directory のみを触る。
- path は canonicalize して扱う。
- symlink escape を拒否する。
- Unix socket は同一 user のみ接続可能な permission にする。
- MCP tool から同期対象 root を勝手に追加しない。
- destructive に近い操作は slash command 側で明示的に行う。

### 7.2 シンプルさ

- 初期 MVP は 1 watched directory に限定する。
- 設定ファイルは最小限にする。
- GUI は作らない。
- Web UI は作らない。
- 複雑な merge UI は作らない。

### 7.3 観測性

最低限、以下を確認できること。

- daemon が起動しているか。
- watched directory が存在するか。
- endpoint key が存在するか。
- blobs / pending dir が解決できるか。
- peer が何件登録されているか。
- open_all / strict mode のどちらか。
- 最近の daemon log。
- 最近の pending changes。

### 7.4 移植性

初期対象は macOS とする。

Linux / Windows は初期 scope 外。ただし、設計上は systemd / Windows Service へ移植できる余地を残す。

## 8. 設定方針

初期 MVP では CLI 引数で watch directory を明示する。

例:

```bash
p2p-dir-sync --watch /Users/me/Documents/notes
```

将来的に設定ファイルを導入する場合は以下のような構造を想定する。

```toml
[[folders]]
id = "notes"
path = "/Users/me/Documents/notes"

[security]
allow_mode = "strict"
```

ただし、初期 MVP では MCP tool から `folders` を追加・削除しない。

## 9. Architecture

```text
Claude Code / Codex
  |
  | MCP stdio
  v
p2p-dir-sync-mcp
  |
  | Unix socket JSON-RPC
  v
p2p-dir-sync daemon
  |
  | Iroh gossip / blobs
  v
Peers
```

### 責務分離

```text
plugin:
  user-facing command / doctor / skill

mcp server:
  AI agent 向け tool surface

daemon:
  sync engine / peer management / pending log

iroh:
  transport
```

## 10. LLM Wiki との関係

`p2p-dir-sync` は LLM Wiki の下位 backend として使える。

ただし、以下を守る。

- `p2p-dir-sync` は LLM Wiki を知らない。
- `p2p-dir-sync` は wiki schema を読まない。
- `p2p-dir-sync` は `entity-registry.json` を特別扱いしない。
- LLM Wiki 側が必要なら、`sync.list-pending` を MCP / JSON-RPC 経由で呼ぶ。

つまり、LLM Wiki は `p2p-dir-sync` の利用例の 1 つである。

## 11. MVP Scope

MVP で作るもの:

- Rust daemon binary `p2p-dir-sync`
- MCP server binary `p2p-dir-sync-mcp`
- 1 directory watch
- upsert sync
- tombstone sync
- conflict backup
- invite / accept / list-peers / revoke
- pending log
- health-check
- setup-doctor command
- macOS launchd staging
- 2 peer smoke test
- 3 peer e2e test

MVP で作らないもの:

- 複数 folder
- ignore pattern UI
- file versioning
- CRDT
- GUI
- Syncthing compatibility
- cloud relay management UI
- LLM Wiki 専用機能

## 12. 将来拡張

候補:

- 複数 watched directory
- config.toml
- `sync.add-folder` / `sync.remove-folder` ただし要 approval
- ignore pattern
- receive-only / send-only mode
- better conflict report
- QR / URL scheme invite
- invite TTL / 署名
- iroh 1.0 final 追従 (= 既に rc.0 を採用済、final release 後に全 crate を 1.0.x に揃える)
- Linux systemd support
- Windows Service support

## 13. Open Questions

| # | 論点 | 初期方針 |
|---|---|---|
| Q1 | MCP tool から folder 追加を許すか | MVP では許さない |
| Q2 | 複数 folder を初期実装するか | しない |
| Q3 | Syncthing wrapper にするか独自実装にするか | 独自実装 |
| Q4 | CRDT を採用するか | しない |
| Q5 | plugin 名 | `p2p-dir-sync` |
| Q6 | 初期対象 OS | macOS |

## 14. 受け入れ基準

- `cargo build` が通る。
- `cargo test` が通る。
- 2 peer で file create / update / delete が同期される。
- 3 peer e2e で upsert / tombstone / rename / conflict が確認できる。
- `/p2p-dir-sync:setup-doctor` が daemon / MCP / watched dir / peer / log を診断できる。
- LLM Wiki を知らないディレクトリでも同期できる。
- `docs/llm-wiki/` を watch 対象にした場合も、通常ディレクトリとして同期できる。

## 15. 実装フェーズ案

| Phase | 内容 |
|---|---|
| 0 | 要件定義 / 設計 |
| 1 | Cargo project skeleton |
| 2 | sync engine 移植 |
| 3 | daemon JSON-RPC |
| 4 | MCP server |
| 5 | plugin staging |
| 6 | launchd staging |
| 7 | 2 peer smoke |
| 8 | 3 peer e2e |
| 9 | docs / README 整理 |

## 16. 設計上の結論

`p2p-dir-sync` は、AI agent が安全に扱える P2P directory sync control plane として設計する。

LLM Wiki は主用途の 1 つだが、実装上の依存や知識は持たせない。

初期 MVP では、高機能化よりも以下を優先する。

- 明示された 1 ディレクトリだけを同期する。
- 招待と peer 管理を分かりやすくする。
- setup-doctor で壊れた時に説明できる。
- AI agent から危険な path 操作をさせない。
- P2P sync の最小実用 loop を安定させる。
