# p2p-dir-sync 設計書 (design plan v7、2026-05-14)

> [`requirements.md`](requirements.md) で確定した要件を実装に落とすための設計 plan。
> 「何を作るか」(= requirements) と「どう作るか」(= 本 doc) の橋渡し。
>
> 命名: `design.md` (= 実装方針 / 構造 / 公開 API + 移植 mapping)。後で必要に
> なれば `architecture.md` (= 図中心の全体俯瞰) を分離して切り出す。
>
> **履歴**:
> - v0 (2026-05-14): 初版
> - v1 (2026-05-14): user review fix 5 件反映 — (F1 High) gossip topic を **invite-tied secret** 化 + blob ALPN を **allowlist wrap**、「mesh 誰でも join」を撤回 / (F2 High) **多重 daemon 起動防止**を MVP 昇格 (ping check + lock file) / (F3 Medium) `sync.ping` を **MCP liveness 専用** (daemon 不要)、`sync.health-check` を daemon 接続確認に分離 / (F4 Medium) allowlist の **strict empty default** に変更、open_all は `--allow-open-all` 明示 opt-in / (F5 Low) `sync.recent-log` の **lines 上限 / secret 除外** を明示
> - v2 (2026-05-14): user review fix 7 件反映 — (G1 High) **topic_id を peer-local invite_nonce ではなく folder/group secret** に訂正 (= 3 peer chain で mesh 分断する v1 bug を修正、全 peer が同 folder_secret を共有して 1 group 1 mesh) / (G2 High) strict default 下での **mutual allowlist 成立手順を明示** — `sync.allow-peer` RPC + slash command を新規追加、bilateral invite フローを文書化 / (G3 Medium) acceptance gate の `sync.ping` smoke を `sync.health-check` に更新 (= daemon RPC からは ping を完全削除した v1 と整合) / (G4 Medium) InviteTicket を **versioned envelope** `p2psync1-<base32(...)>` 形式に / (G5 Low) §4.3 path tree に `folder-secret.bin` / `daemon.lock` を明示 / (G6 Low) "16 byte entropy" と正確に記述 / (G7 Low) setup-doctor の順序 ping → health-check → status → recent-log を明文化
> - v3 (2026-05-14): user review fix 6 件反映 — (H1 High) **folder_secret を lazy generate** に訂正 (= v2 では起動時必ず生成して founder 化 → 他人の invite を永遠に accept できない bug を修正)、`sync.invite` か `sync.accept-invite` の **最初の呼び出し**で初期化、未初期化 daemon は mesh に居ない (group_initialized=false)。accept 後の有効化は **daemon 再起動が必要** と明示、bilateral flow を 6 step に拡張 / (H2 High) **AllowlistBlobs は accept 側 (= blob serve する側) で動く**ことを明示、mutual allowlist でないと **両方向とも blob fetch が止まる** (= v2 で「片方向 sync」と書いたのは誤り、step 4 必須) / (M1 Medium) `sync.allow-peer` MCP tool 追加で **MCP tool 10 個 / commands 8 個** に更新 / (M2 Medium) PendingEntry schema に **`kind: "upsert" | "tombstone"`** field 追加、Tombstone も pending log に記録可能に / (M3 Medium) HealthInfo を **静的部 + 動的部** に分離、`run_health_check(daemon_state: Option<&DaemonState>)` 引数化 / (L1 Low) SIGKILL 時は Drop が走らない事実を認め、stale socket は **次回起動時の health-check probe + flock** で recover する設計に修正 (= F2 mechanism と整合)
> - v4 (2026-05-14): user review fix 5 件反映 — (I1 High) **Alice 側も `sync.invite` 後に再起動必須** に訂正。v3 では Bob 側だけ再起動する 6 step flow にしていたが、Alice daemon も `folder-secret.bin` 不在状態で起動するので step 1 で folder_secret 生成しても runtime rebuild しない方針上 mesh に居ない → Bob と通信できない。`sync.invite` も `restart_required: true` を返し、bilateral flow を **7 step (Alice invite → Alice restart → Bob accept → Bob restart → Alice allow-peer → mutual)** に拡張 / (I2 Medium) **`mutual: bool` の自動判定を撤回**。local daemon は「相手が自分を allowlist しているか」を直接知れないので MVP では mutual 出さない、代わりに observability で `last_seen_at: Option<i64>` (= 直近通信時刻) を将来追加候補に / (I3 Medium) **D7 を lazy generate 方針に整合**: 「`folder-secret.bin` 不在で start fail」を撤回、未初期化起動は正常、紛失は「既存 group との通信不能」として別問題化 / (I4 Low) v2 由来の「片方向 sync」表現を全箇所「mutual 未成立 / local allowlist に追加するだけ」に訂正 / (I5 Low) pending schema fixture 名 (`pending-entry.v1.upsert.json` / `.tombstone.json`) を dir tree / テスト戦略 / acceptance gate に揃える
> - v5 (2026-05-14): user review fix 5 件反映 — (J1 Medium) **HealthInfoDynamic / sync.status に `gossip_subscribed: bool` + `restart_required: bool` を追加**。「secret はあるが再起動前で gossip 未参加」状態を group_initialized だけでは表現不能だったので、static (file 存在) と dynamic (runtime subscribe 中) を分離 / (J2 Medium) **`last_seen_at` の定義を data-plane 成功時刻に固定**: NeighborUp event time は **含まない** (= mesh 上見えても blob fetch / receive allowlist で落ちる可能性あり、誤診断防止)。「直近に blob fetch / blob serve / Tombstone 受信が成功した時刻」を採用 / (J3 Low) `sync.list-peers` の result に peers field の **JSON example を明記** (peer_id / label / added_at / last_seen_at) / (J4 Low) bilateral flow step 6 忘れ時の **Bob → Alice 方向の失敗理由を訂正**: 主因は Alice 側 receive allowlist が Bob からの gossip Upsert を drop すること (= Bob 側 blob serve は本来 OK) / (J5 Low) lazy generate 後の古い表現残 (folder-secret.bin の "初回起動時 generate" 表現、未初期化 daemon の RPC 一覧に sync.ping が居る誤記) を訂正
> - v6 (2026-05-14): 公式 doc cross-check 完了。Claude Code (`code.claude.com/docs/ja/plugins`) と Codex (`developers.openai.com/codex/plugins/build`) を確認した結果、私の dual-host plugin 設計はほぼ全項目 **公式仕様に整合**: (a) `.claude-plugin/plugin.json` + `.codex-plugin/plugin.json` の sibling 配置は両公式仕様、(b) **`.mcp.json` / `skills/` を plugin root に配置** は両公式共通、**`commands/` は Claude 公式で明示、Codex 公式は skills 中心** (v7 で K3 訂正)、(c) **Codex は `.claude-plugin/marketplace.json` も読める = dual-host plugin 公式サポート**。前回 v3-v5 で「Codex 仕様未掲載 / 実機 0.130+ 依存」と書いた risk note を撤回。残る不確実性は **`commands/<name>.md` の Codex 側挙動** (公式 doc は skills を主に説明) と **MCP `command` field の env var resolution** (両公式 doc 明記なし)。前者は連載 ⑤ M7-4 実機検証で動作確認済として注記、後者は `install.sh` placeholder 置換方式 (= 防御的に正解) を継続
> - v7 (2026-05-14): user review fix 3 件反映 — (K1 Medium) **`.codex-plugin/plugin.json` 内に `skills` / `mcpServers` field を明記**: 公式 Codex manifest が bundled components を `"skills": "./skills/"` / `"mcpServers": "./.mcp.json"` で指す前提なので、§2 dir 構造の plugin.json 例にこれらを追加 / (K2 Medium) **§14 D8 の `last_seen_at` 旧表現を訂正**: 「NeighborUp event time」を含む v4-v5 由来の表現を削除、§3.3 の data-plane 成功時刻のみという定義に整合 / (K3 Low) v6 history の「両公式の共通仕様」表現を訂正: Claude 公式は `commands/` を明記、Codex 公式は `skills/` 中心 (`commands/` 明記なし)、`commands/` の Codex 側挙動は実機検証済として位置付け

## 0. 設計の前提

requirements.md §3 / §16 で確定済の中核方針を再掲:

- **任意ディレクトリの P2P 同期**、LLM Wiki を知らない
- **AI agent 向け control plane**: 1 daemon + 1 MCP server + 1 plugin
- **MVP は 1 watched directory**、複数 folder / config.toml / 自由な path 操作は将来拡張
- **last-writer-wins + conflict backup** (CRDT 不採用)
- **macOS 初期対象**、systemd / Windows Service への移植余地は残す

依存方向:

```
plugin (UX)  →  MCP server (AI agent surface)  →  daemon (sync engine)  →  Iroh
```

逆向きの依存はゼロ。`p2p-dir-sync` は **LLM Wiki / consumer application** を知らない。

## 1. レイヤー構造と責務

### 1.1 4 layer

```text
┌─────────────────────────────────────────────────────┐
│  L4. Plugin (Claude Code / Codex)                    │
│      .claude-plugin/ + .codex-plugin/                │
│      commands/p2p-dir-sync:*.md  (slash commands)    │
│      skills/sync/SKILL.md         (trigger phrase)   │
│      .mcp.json                    (MCP server 起動)  │
└────────────────────┬────────────────────────────────┘
                     │ stdio (MCP protocol)
┌────────────────────▼────────────────────────────────┐
│  L3. MCP server (p2p-dir-sync-mcp binary)            │
│      AI agent 向け 10 tool surface                   │
│      stateless wrapper、daemon に JSON-RPC 中継     │
└────────────────────┬────────────────────────────────┘
                     │ Unix socket (newline-delimited JSON-RPC)
┌────────────────────▼────────────────────────────────┐
│  L2. Daemon (p2p-dir-sync binary)                    │
│      sync engine: watcher / receiver / sender        │
│      state: allowlist / pending log / endpoint key   │
│      Unix socket listener (control plane)            │
└────────────────────┬────────────────────────────────┘
                     │ Iroh API (Endpoint / Gossip / Blobs)
┌────────────────────▼────────────────────────────────┐
│  L1. Iroh (transport)                                 │
│      QUIC + relay / gossip protocol / blob ALPN      │
└─────────────────────────────────────────────────────┘
```

### 1.2 各 layer の責務

requirements.md §9 「責務分離」を実装単位に展開:

| layer | 持つ責務 | 持たない責務 |
|---|---|---|
| L4. plugin | user facing slash command / skill trigger / setup doctor / install script | sync logic / state |
| L3. MCP server | tool schema / argument validation / daemon RPC 呼び出し / error formatting | sync state / 永続化 (= stateless) |
| L2. daemon | watcher / sender / receiver / allowlist / pending log / endpoint key 永続化 / Unix socket | git / wiki schema / agent-specific UX |
| L1. Iroh | QUIC P2P / gossip mesh / blob fetch | daemon 設定 / fs 操作 |

## 2. crate / dir 構造

要件 §6.1 / §6.7 / §6.8 を満たす最小構成:

```
p2p-dir-sync/                        # 独立 repo (jj/git colocated)
├── Cargo.toml
├── README.md
├── docs/
│   ├── requirements.md              # §0
│   ├── design.md                    # 本 doc
│   ├── architecture.md              # (後で分離する候補)
│   ├── operations.md                # 運用手順 (launchd / plugin install / 復旧)
│   └── schema/
│       ├── pending-entry.v1.upsert.json    # golden JSON fixture: Upsert variant (M2 + I5)
│       ├── pending-entry.v1.tombstone.json  # golden JSON fixture: Tombstone variant (M2 + I5)
│       └── sync-update.v2.md        # wire schema 説明
├── src/
│   ├── lib.rs                       # public API + module 宣言
│   ├── runtime.rs                   # Iroh stack 起動 (Endpoint+Gossip+Blobs+Router)
│   ├── state.rs                     # SyncState (pending_written / last_written / 等)
│   ├── send.rs                      # send_file (Upsert broadcast)
│   ├── receive.rs                   # receive_loop / handle_upsert / handle_tombstone
│   ├── conflict.rs                  # compute_conflict_backup_path
│   ├── watcher.rs                   # fsnotify + PollWatcher
│   ├── allowlist.rs                 # peer allowlist (open_all / strict)
│   ├── message.rs                   # wire 形式 (SyncUpdate / Upsert / Tombstone)
│   ├── keystore.rs                  # endpoint.key 永続化
│   ├── paths.rs                     # ~/.local/share/p2p-dir-sync/ 規約
│   ├── pending_log.rs               # 受信 change log (旧 pending_metadata.rs 改名)
│   ├── daemon/                      # Unix socket listener
│   │   ├── mod.rs                   # pub use + integration tests
│   │   ├── state.rs                 # DaemonState + Request/Response
│   │   ├── listener.rs              # bind_listener / accept_loop / ListenerHandle
│   │   ├── dispatch.rs              # sync.* RPC method の dispatch
│   │   └── client.rs                # rpc() (= MCP server / test 用)
│   └── bin/
│       ├── p2p-sync.rs              # daemon binary (= L2)
│       └── p2p-sync-mcp.rs          # MCP server binary (= L3)
├── plugin/                          # L4 staging dir
│   ├── .claude-plugin/marketplace.json + plugin.json
│   ├── .codex-plugin/plugin.json    # K1: skills / mcpServers field で bundled components を指す
│   ├── .mcp.json                    # 1 MCP server (p2p-sync-mcp)
│   ├── skills/sync/SKILL.md
│   ├── commands/                    # 8 slash commands (v3: allow-peer 追加、M1)
│   │   ├── setup-doctor.md
│   │   ├── status.md / invite.md / accept.md / allow-peer.md / peers.md / revoke.md / pending.md
│   ├── scripts/install.sh           # placeholder 置換
│   ├── verify.sh                    # sanity check
│   └── README.md

# K1 (v7、Codex 公式 manifest 例に整合):
# .codex-plugin/plugin.json は bundled components を明示的に指す:
#   {
#     "name": "p2p-dir-sync",
#     "version": "0.1.0",
#     "description": "...",
#     "skills": "./skills/",
#     "mcpServers": "./.mcp.json",
#     "interface": { "displayName": "p2p-dir-sync", ... }
#   }
# `.claude-plugin/plugin.json` は manifest だけ持ち、commands/ skills/ .mcp.json は
# Claude が plugin root から自動 discover (= 公式 doc の規約)。
# commands/ の Codex 側読み込みは公式 doc 明記なし、連載 ⑤ M7-4 実機検証で動作確認済
├── sandbox/
│   ├── scripts/
│   │   ├── 2peer-smoke.sh           # MVP §14
│   │   ├── e2e-3peer.sh             # MVP §14
│   │   └── launchd/                 # plist + wrapper
│   └── README.md
├── examples/
│   └── standalone-watch.rs          # wiki 知識ゼロで動く example
└── tests/                            # integration tests
```

requirements.md §11 (= MVP) と §6.7 (= MCP tool) と §6.8 (= plugin command) に
すべて対応する最小 dir 構造。複数 folder / config.toml は **入れない** (= 拡張時に追加)。

## 3. 公開 API

### 3.1 Library API (`p2p_dir_sync` crate、`use p2p_dir_sync::*`)

```rust
// runtime.rs
pub struct SyncRuntime { ... }
impl SyncRuntime {
    pub async fn build(
        endpoint: Endpoint,
        blobs_dir: &Path,
        allowlist: Arc<AllowList>,
    ) -> Result<Self>;
}

// state.rs
pub struct SyncState { ... }
pub struct PendingTracker { ... }
pub type WriteRegistry = Arc<Mutex<HashMap<PathBuf, Hash>>>;
pub const TOMBSTONE_DEDUP_TTL: Duration;

// send.rs / receive.rs
pub async fn send_file(...) -> Result<()>;
pub async fn receive_loop(...) -> Result<()>;

// watcher.rs
pub struct WatcherConfig { pub max_file_size: u64 }
pub enum WatcherBackend { Recommended, Poll }
pub fn spawn_watcher(...) -> Result<(WatcherHandle, mpsc::UnboundedReceiver<DebouncedEvent>)>;
pub async fn watcher_loop(...) -> Result<()>;

// allowlist.rs
pub struct AllowList { ... }   // open_all / strict mode 内包
pub struct PeerInfo { ... }

// daemon/
pub struct DaemonState { ... }
pub fn spawn_listener(socket_path: PathBuf, state: Arc<DaemonState>) -> Result<ListenerHandle>;
pub async fn rpc(socket: &Path, method: &str, params: Value) -> Result<Value>;

// pending_log.rs
pub struct PendingEntry { ... }    // schema_version, rel_path, source_peer, received_at, blob_hash, bytes
pub fn record_receive(root: &Path, repo_hash: &str, entry: &PendingEntry) -> Result<()>;
pub fn list_pending(root: &Path, repo_hash: &str) -> Result<Vec<PendingEntry>>;

// paths.rs
pub fn default_socket_path() -> Result<PathBuf>;
pub fn default_blobs_dir() -> Result<PathBuf>;
pub fn default_pending_dir() -> Result<PathBuf>;
pub fn default_key_path() -> Result<PathBuf>;
pub fn ensure_dir_700(dir: &Path) -> Result<()>;

// lib.rs (health、v3 で M3 反映 = 静的部 + 動的部に分離 + daemon state 引数化)
pub fn run_health_check(daemon_state: Option<&DaemonState>) -> Result<HealthInfo>;

/// 静的 path 確認 (= path 解決 + file exists、daemon 経由なしで呼べる)。
pub struct HealthInfoStatic {
    pub key_path: PathBuf,
    pub key_exists: bool,
    pub blobs_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub watched_dir: Option<PathBuf>,
    pub watched_dir_exists: bool,
}

/// 動的 daemon state (= daemon が知っている runtime 情報)。
pub struct HealthInfoDynamic {
    pub peer_count: u32,
    pub open_all: bool,
    pub uptime_secs: u64,
    pub group_initialized: bool,         // file 存在ベース (= folder-secret.bin の有無、static)
    pub gossip_subscribed: bool,         // J1: current runtime が gossip topic を subscribe 中か
    pub restart_required: bool,          // J1: group_initialized && !gossip_subscribed (= invite/accept 直後など)
}

/// `run_health_check` の戻り値: 静的部は常に埋まる、動的部は daemon_state ありの時のみ。
pub struct HealthInfo {
    #[serde(flatten)]
    pub static_info: HealthInfoStatic,
    pub dynamic_info: Option<HealthInfoDynamic>,
}
```

`run_health_check(None)` は library / CLI utility から呼べる static-only check (= path 解決のみ)。daemon dispatch (`sync.health-check` RPC) は `run_health_check(Some(&self.state))` を呼んで full info を返す。

外部 (= consumer) は library として直接使う **必要は無い** (= daemon binary + Unix socket RPC で接続する設計)。library を `pub` で出すのは **examples / tests / 将来の embedded use case** のため。

### 3.2 Daemon Unix Socket RPC (= 主要な公開 surface)

socket path: `~/.local/share/p2p-dir-sync/daemon.sock` (mode 0o600)

protocol: newline-delimited JSON (1 request line / 1 response line)

method 一覧 (要件 §6.7 から、v1 で `sync.ping` を MCP 側のみに移動):

| method | params | result | 備考 |
|---|---|---|---|
| `sync.health-check` | `{}` | `HealthInfo` JSON | daemon 接続確認、watched_dir / key / peer_count 含む (= 障害切り分けの primary 指標) |
| `sync.status` | `{}` | `{watched_dir, peer_count, open_all, recent_pending_count, key_exists, uptime_secs, group_initialized, gossip_subscribed, restart_required}` | summary view、軽量。J1 で `gossip_subscribed` + `restart_required` を追加 (= secret 取得直後の「再起動待ち」状態を表現) |
| `sync.invite` | `{}` | `{ticket, restart_required}` | `InviteTicket` (= `EndpointTicket + folder_secret`) を `p2psync1-<base32>` envelope で serialize (G1 + G4)。**未初期化 daemon (= `group_initialized=false`) で呼ばれた場合 folder_secret を新規生成 + persist し、`restart_required: true` を返す** (I1)。既に初期化済なら既存 ticket を返し `restart_required: false` |
| `sync.accept-invite` | `{ticket, label?}` | `{peer_id, label, my_peer_id, restart_required}` | ticket prefix / parse / folder_secret 整合 check 後、inviter を **local allowlist に追加** (= mutual はまだ未成立、I4 反映)。未初期化 daemon なら folder_secret を adopt + persist し `restart_required: true`。response の `my_peer_id` を inviter 側 user に渡して `sync.allow-peer` してもらう (G2) |
| **`sync.allow-peer`** (v2 新規、G2) | `{peer_id, label?}` | `{added: bool, peer_id, label}` | inviter 側で **明示的に counter-peer を local allowlist に追加**。mutual allowlist (= 双方向同期成立) の対称半分。ticket を必要としない (= 既に同 mesh に居る peer の peer_id を持って入力)。allowlist は runtime mutation 可能なので **再起動不要** |
| `sync.list-peers` | `{}` | `{peers: [PeerInfo...], open_all: bool}` | `PeerInfo = {peer_id, label?, added_at, last_seen_at}` (J3 で field 明記)。`last_seen_at` は data-plane 成功時刻 (= J2)、未通信なら `null` |
| `sync.revoke` | `{peer_id}` | `{removed: bool, peer_id}` | best-effort |
| `sync.list-pending` | `{limit?: u32}` | `{entries: [PendingEntry...]}` | 受信 change log (要件 §6.6) |
| `sync.recent-log` | `{lines?: u32 ≤ 100}` | `{lines: [String...]}` | fixed log path (`~/Library/Logs/p2p-dir-sync.log`) の末尾を返す。lines 上限 100、**ticket / folder_secret は `<redacted>` に置換** (F5 + G1 反映) |

すべて **stateless 1 往復** (旧 `iroh-wiki-sync` daemon と同 protocol)。
**`sync.ping` は daemon RPC には存在しない** (= MCP server 内で完結、§3.3 参照)。

### 3.3 MCP tool (`p2p-sync-mcp` binary、L3)

requirements.md §6.7 と 1:1 mapping。**v1 (F3 反映) で `sync.ping` を MCP liveness 専用に切り出し**:

```rust
// sync.ping: MCP server 内で完結、daemon 接続なし (= MCP server 自身の readiness)
#[tool(name = "sync.ping", ...)]
async fn sync_ping(&self) -> String { "pong".to_string() }

// 他は daemon Unix socket に rpc() で 1 RPC を投げる薄い wrapper
#[tool(name = "sync.health-check", ...)]    → daemon "sync.health-check"  // daemon 接続確認
#[tool(name = "sync.status", ...)]          → daemon "sync.status"
#[tool(name = "sync.invite", ...)]          → daemon "sync.invite"
#[tool(name = "sync.accept-invite", ...)]   → daemon "sync.accept-invite"
#[tool(name = "sync.allow-peer", ...)]      → daemon "sync.allow-peer"    // G2 (v2)
#[tool(name = "sync.list-peers", ...)]      → daemon "sync.list-peers"
#[tool(name = "sync.revoke", ...)]          → daemon "sync.revoke"
#[tool(name = "sync.list-pending", ...)]    → daemon "sync.list-pending"
#[tool(name = "sync.recent-log", ...)]      → daemon "sync.recent-log"
```

#### bilateral invite フロー (v4、I1 反映、7 step 化)

strict default 下で 2 peer 間で **双方向同期** を成立させる手順 (= MVP の正規フロー)。v4 で **Alice 側も `sync.invite` 後に再起動必須** であることを明示し、両者の再起動 step を含む 7 step に拡張 (= v3 6 step では Alice が mesh に居ない bug):

```text
[Alice 側]  (daemon 起動済、`folder-secret.bin` は不在の状態 = group_initialized=false)
1. /p2p-dir-sync:invite
   → Alice daemon が folder_secret を新規生成 + persist (= Alice が group founder)
   → response: { ticket: "p2psync1-...", restart_required: true }
   → ticket_A を取得 (Alice の peer_id + folder_secret_A 含む)
2. **Alice: daemon を再起動** (`launchctl kickstart -k gui/$(id -u)/com.user.p2p-dir-sync`)
   → 再起動後、group_initialized=true で gossip subscribe (= topic_id_A) → mesh に居る
3. Alice → Bob に ticket_A を out-of-band で渡す (Slack / iMessage 等)

[Bob 側]  (daemon 起動済、`folder-secret.bin` は不在の状態)
4. /p2p-dir-sync:accept <ticket_A>
   → Bob daemon が folder_secret_A を adopt + persist + Alice の peer_id を Bob allowlist に追加
   → response: { peer_id: Alice_id, my_peer_id: Bob_id, restart_required: true }
5. **Bob: daemon を再起動**
   → 再起動後、group_initialized=true で gossip subscribe (= topic_id_A) → mesh join
   → 出力テンプレに「次に Alice 側で `sync.allow-peer <Bob_id>` を実行してください」を明示

[Alice 側]
6. /p2p-dir-sync:allow-peer <Bob_id> [--label Bob]
   → Alice daemon が Bob_id を Alice allowlist に追加 (= 対称半分、allowlist は runtime mutation 可)
   → Alice 側は再起動不要
7. (mutual 成立、両方向 sync 動作開始)
```

**why 2 回の再起動?** folder_secret の adopt / generate は file system に persist するが、gossip subscribe を反映するには `SyncRuntime::build` の再実行が必要。MVP では runtime swap を実装しない (= シンプル方針)、launchd `KeepAlive=true` で再起動自体は数秒で完了するので UX impact は小さい。連載 ⑥+ で runtime rebuild + zero-downtime accept を検討候補。

#### step 2 / 5 / 6 を忘れると何が起きるか (v4、I1 + I4 反映)

**いずれの step も必須**。AllowlistBlobs wrap は **blob ALPN の accept 側 (= serve する側)** で動くため、mutual allowlist が両方向で揃わないと **両方向とも blob fetch が拒否され、結果として両方向の sync が止まる**:

| 不足 step | 影響 |
|---|---|
| step 2 (Alice 再起動忘れ、I1) | Alice daemon が gossip subscribe してないので、Alice 自身が mesh に居ない → Bob から見ても Alice が見えない (= Bob 側 accept しても無意味) |
| step 5 (Bob 再起動忘れ) | Bob daemon が gossip subscribe してないので、Alice の Upsert を受信できない (mesh に居ない) |
| step 6 (Alice の allow-peer 忘れ、I4 + J4) | Alice 側 allowlist に Bob が居ないので、**両方向止まる**:<br>① Alice → Bob 方向: Bob が Alice から blob fetch しようとして、Alice 側 AllowlistBlobs が Bob からの ALPN connection を reject<br>② Bob → Alice 方向: Bob は blob serve 自体は OK だが (Bob 側 allowlist には Alice 居る)、Alice 側 `receive_loop` が **gossip Upsert を `from_id` allowlist check で drop** する (= Bob からの change そのものが届かない、J4 訂正で主因明確化) |

= **両 daemon の再起動 + mutual allowlist は MVP の必須条件**。setup-doctor で「実際 sync が動いているか」を確認できるように、各 peer の **`last_seen_at: Option<i64>`** を `sync.list-peers` の response に含める。

#### `last_seen_at` の定義 (v5、J2 反映)

`last_seen_at` は **data-plane が実際に成功した時刻** (= Unix epoch 秒) のみを記録する:

- ✅ 含む: 直近 blob fetch 成功 (= 相手から content を受け取れた) / 直近 blob serve 成功 (= 相手に content を送れた) / 直近 Tombstone 受信成功 (= gossip + allowlist filter 通過)
- ❌ 含まない: gossip `NeighborUp` event (= mesh 上見えたが data-plane 未確認)、gossip 受信 try (= allowlist で drop されたかもしれない)

NeighborUp を含めない理由: 同じ topic で mesh に居ても、blob ALPN が AllowlistBlobs で reject されたり、gossip Upsert が receive allowlist で drop されたりして data-plane で落ちる可能性がある。control-plane (NeighborUp) を `last_seen_at` の根拠にすると「mesh で見えているのに sync 動かない」状態を誤って「動作中」と表示してしまう。

`last_seen_at == null` は「accept しただけで一度も data-plane が成立してない」状態。setup-doctor はこれを warning として表示し、user に「相手側 allow-peer 忘れ / 相手 daemon 未再起動 / 単純に書込が無い」のいずれかを示唆する。

**`mutual: bool` を自動で出さない理由** (I2 反映): local daemon は「相手の allowlist 状態」を直接知れない (= remote self-report protocol が無いため)。`last_seen_at` で「通信できているか」を観測する方が user に有用。完全な mutual 自動判定は連載 ⑥+ で gossip self-report を追加する場合に検討。

将来 UX 改善 (= 連載 ⑥+ 候補): inviter daemon に join request を 自動転送する push pattern + 自動 runtime rebuild。MVP では out-of-band + 手動再起動で OK。

**`sync.ping` vs `sync.health-check` の使い分け** (F3 反映、setup-doctor の障害切り分け用):

| tool | daemon 接続 | 何を確認 | 失敗時の意味 |
|---|---|---|---|
| `sync.ping` | 不要 | MCP server が host (Claude/Codex) と stdio で接続できているか | MCP server が起動していない / plugin install 不全 |
| `sync.health-check` | 必要 | daemon socket + key + watched_dir + peers | daemon が落ちている / 設定不全 |

→ setup-doctor は **`sync.ping` 先、`sync.health-check` 後** の順で叩いて段階的に切り分ける。

**MCP tool から folder 追加・削除は無し** (要件 Q1 / §6.7 末尾 / §7.1)。
`--watch <dir>` は daemon 起動時の CLI 引数で固定。

## 4. データ形式 / schema

### 4.1 Wire format: `SyncUpdate` (旧 `WikiUpdate` rename、bytes-on-wire 互換)

```rust
#[serde(rename = "WikiUpdate")]    // bytes-on-wire は v2 互換
pub struct SyncUpdate {
    pub version: u32,              // = 2
    pub body: SyncUpdateBody,
}

#[serde(tag = "kind")]
pub enum SyncUpdateBody {
    Upsert { path: String, hash: Hash, format: BlobFormat, from: PeerInfo },
    Tombstone { path: String, from: PeerInfo },
}
```

Rust 型は `SyncUpdate` に rename (= 命名 hygiene、wiki と無関係)、`#[serde(rename = ...)]` で wire bytes は **旧 `WikiUpdate` と互換**。

### 4.1b Invite ticket (v2 で folder_secret 化、G1 + G4 反映)

旧 v1 の peer-local `invite_nonce` 設計は **3 peer chain で mesh が分断する bug** があった (= A→B→C の invite chain で A の topic と B の topic が異なり、A と C が同 mesh に居ない)。v2 では **folder/group secret** に訂正:

```rust
#[derive(Serialize, Deserialize)]
pub struct InviteTicket {
    pub endpoint: EndpointTicket,   // EndpointId + addr (旧と同じ)
    pub folder_secret: [u8; 16],    // 16 byte entropy、group identity
}
```

- folder_secret は **「同 group 1 個」の identity**。同 mesh に居る全 peer が同じ値を hold する
- daemon 初回起動時 (= `folder-secret.bin` 不在) は **lazy**: 何も生成しない。`group_initialized=false` 状態で起動し、mesh に居ない (= gossip subscribe しない、blob 配信しない、RPC だけ生きている) (H1 反映)
- 初期化 path は **2 通り**、いずれも folder-secret.bin を atomic write して `group_initialized=true` に遷移:
  - (a) `sync.invite` 呼出: 新規 16 byte 生成 → 自分が group founder
  - (b) `sync.accept-invite <ticket>` 呼出: ticket 内 folder_secret を adopt → 既存 group に join
- **すでに初期化済** (= `folder-secret.bin` 存在) の daemon が **異なる** folder_secret の invite を accept しようとした場合は **reject** (= group merge は MVP scope 外、後勝ち上書きすると既存 peer と切断するため危険)
- (a) 後に他人の invite を accept したいケースは **想定外**: `sync.invite` を呼んだ瞬間「自分が group founder」を宣言した、と扱う。仕切り直したいなら `folder-secret.bin` 手動削除 + daemon 再起動

#### Wire format (G4): versioned envelope

旧 `EndpointTicket` (base32 prefix なし) と混同しないよう、新形式は明示 prefix を付ける:

```text
p2psync1-<base32(serde_json::to_string(&InviteTicket))>
```

- prefix `p2psync1-`: schema version 1。将来 v2 で field 追加なら `p2psync2-` に bump
- accept-invite は prefix チェック → version 不一致なら reject、parse 失敗なら reject

### 4.1c Gossip topic_id 導出 (G1 反映)

旧 v1: `TopicId = BLAKE3(self_peer_id ‖ invite_nonce ‖ tag)` → **peer chain で topic 分断 bug**

v2: **folder_secret から全 peer 共通の topic_id を導出**:

```rust
fn derive_topic_id(folder_secret: &[u8; 16]) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"p2p-dir-sync/v1/topic\0");
    hasher.update(folder_secret);
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}
```

- A: folder_secret_A 生成 → topic_X 導出 → A の invite に folder_secret_A を含めて B に渡す
- B: invite を accept → folder_secret_A を adopt → 同じ topic_X 導出 → mesh join
- B が C を invite → folder_secret_A (B が hold する値) を invite に含めて C に渡す
- C: accept → 同 folder_secret_A → 同 topic_X → A/B/C 全員 同 mesh

3 peer chain でも 1 group / 1 mesh が成立する。

**第三者は folder_secret を知らないので topic_id を導出不能 = mesh join 不能** = 任意 dir 同期での private data 流出を防ぐ (要件 §7.1)。

### 4.2 Pending entry (要件 §6.6 / 受信 change log、v3 で kind 追加 = M2)

```json
{
  "schema_version": 1,
  "kind": "upsert",
  "rel_path": "entities/foo.md",
  "received_at": 1715600000,
  "source_peer": "abcd1234...",
  "blob_hash": "blake3:...",
  "bytes": 4096
}
```

Tombstone (= delete) を受信した時:

```json
{
  "schema_version": 1,
  "kind": "tombstone",
  "rel_path": "entities/old.md",
  "received_at": 1715600100,
  "source_peer": "abcd1234..."
}
```

- `schema_version`: drift 対策 (= 上位 consumer = LLM Wiki 等が読む時の互換判定)
- `kind`: `"upsert"` または `"tombstone"` (M2、v3 追加。Tombstone も pending log に記録できないと delete e2e で詰まる)
- `rel_path`: watched_dir 相対
- `source_peer`: 受信元の EndpointId 文字列
- `blob_hash`: BLAKE3 hash (= upsert のみ、tombstone では存在しない)
- `bytes`: blob size (= 同上)

Rust 表現:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingEntry {
    Upsert {
        schema_version: u32,
        rel_path: String,
        received_at: i64,
        source_peer: String,
        blob_hash: String,
        bytes: u64,
    },
    Tombstone {
        schema_version: u32,
        rel_path: String,
        received_at: i64,
        source_peer: String,
    },
}
```

golden fixture: `docs/schema/pending-entry.v1.upsert.json` + `pending-entry.v1.tombstone.json` の 2 example を commit。
将来 LLM Wiki 等の consumer が独立に deserialize する時の整合テストに使う。

### 4.3 永続化 path 規約 (要件 §7.3 観測性 + Q4、v2 で folder-secret.bin / daemon.lock 追加)

```
~/.local/share/p2p-dir-sync/
├── daemon.sock                      # Unix socket (mode 0o600)
├── daemon.lock                      # flock 用 lock file (mode 0o600、F2)
├── folder-secret.bin                # 16 byte folder/group secret (mode 0o600、G1)
├── blobs/                            # iroh-blobs fs store (dir mode 0o700)
├── pending/<repo_hash>/              # 受信 pending log per repo
│   └── <iso-timestamp>-<peer>.json
└── allowlist.json                   # peer allowlist v2 schema

~/.config/p2p-dir-sync/
└── endpoint.key                     # 32-byte Ed25519 (mode 0o600)

~/Library/Logs/
└── p2p-dir-sync.log                  # daemon log (macOS 慣習、`sync.recent-log` の参照先)
```

旧 `iroh-wiki-sync` から **path prefix を全 rename**。両 daemon が並走可能 (= 旧 binary との parallel verify 期間用)。

各 file の責務:
- `daemon.sock`: control plane (= MCP server からの RPC 受付)。Drop 時 unlink、stale 時は起動チェックで unlink + rebind
- `daemon.lock`: 多重 daemon 起動防止の race fence (= F2)。flock 解放 = process exit
- `folder-secret.bin`: 同 group の identity (= G1)。**lazy generate** (= H1)、最初の `sync.invite` で generate (= 自 group founder) または最初の `sync.accept-invite` で adopt (= 既存 group に join)。daemon 起動時には何もしない
- `endpoint.key`: peer 識別 (= EndpointId の元)。破損なら start fail (= auto regen しない、§14 D6)

## 5. ライフサイクル / 主要フロー

### 5.1 daemon 起動 (`p2p-sync --watch <dir>`)

```
1. CLI arg parse                       (clap)
2. watched_dir canonicalize             (paths.rs)
3. 多重起動 check (F2 反映):
    a. socket file 存在?
        Yes → daemon::rpc(socket, "sync.health-check", {}) を 1s timeout で試行
              ├ 応答あり → exit 1 ("daemon already running, PID ...")
              └ timeout / ENOENT / ECONNREFUSED → stale 判定、unlink して続行
        No  → 続行
    b. lock file (~/.local/share/p2p-dir-sync/daemon.lock) を flock(LOCK_EX|LOCK_NB)
        ├ 取得成功 → そのまま hold して main loop に持ち越し
        └ 取得失敗 → exit 1 ("another daemon holds the lock")
4. endpoint.key load / generate         (keystore.rs)
5. **folder_secret load (lazy、H1 反映)**  (keystore.rs)
   ├ folder-secret.bin 存在 → load、group_initialized = true
   └ 不在 → 何もしない、group_initialized = false (= mesh に居ない、`sync.invite` / `sync.accept-invite` で初期化)
6. Endpoint::bind + online              (iroh)
7. blobs_dir prepare 0o700 + load fs    (runtime.rs / paths.rs)
8. allowlist load from allowlist.json   (allowlist.rs)
   ├ file 不在 → strict empty allowlist で起動 (F4 反映)
   └ --allow-open-all flag → open_all=true で起動 (warning banner を log + stderr に出力)
9. SyncRuntime::build                   (blob + Router、AllowlistBlobs wrap)
   ├ group_initialized == true なら gossip も spawn + topic_id = derive_topic_id(folder_secret) で subscribe
   └ group_initialized == false なら gossip subscribe を skip (= 後で初期化時に再起動が必要、H1 反映)
10. spawn_listener (Unix socket、0o600) (daemon/listener.rs)
11. spawn watcher + receive_loop        (watcher.rs + receive.rs、group_initialized 状態に関わらず起動 = RPC は受け付ける)
12. tokio::select! で SIGINT / SIGTERM を待機  → §5.5 へ
```

**未初期化 daemon の挙動**: daemon RPC (= `sync.health-check` / `sync.status` / `sync.invite` / `sync.accept-invite` / `sync.allow-peer` / `sync.list-peers` / `sync.revoke` / `sync.list-pending` / `sync.recent-log`) は応答する (= `sync.ping` は MCP server 側で完結なので daemon RPC list には居ない、J5 反映)。ただし mesh に居ないので peer との sync は動かない。`sync.invite` か `sync.accept-invite` の **最初の呼び出し** で folder-secret.bin を生成 / adopt、ただし **gossip subscribe を反映するには daemon 再起動が必要** (= MVP では runtime rebuild しない方針)。bilateral flow (§3.3) で再起動 step を明示。

この状態は `sync.health-check` の result に `group_initialized=true, gossip_subscribed=false, restart_required=true` で表現される (J1 反映、setup-doctor で「再起動してください」を明示する根拠)。

step 3a / 3b の二重防御で「すでに動いている daemon の socket を奪う」事故を確実に防ぐ (= F2 反映)。
step 3a は通常パス、step 3b は race 中の防御。

### 5.2 file 書込フロー (local → peer)

```
user edit foo.md
  │ fsnotify
  ▼
watcher.rs (debounce 200ms)
  │ should_skip filter
  ▼
state チェック (last_written / pending_written で self-loop 防止)
  │
  ▼
send_file (blob add → WikiUpdate Upsert → gossip broadcast)
```

### 5.3 file 受信フロー (peer → local)

```
gossip Event::Received
  │
  ▼
SyncUpdate::from_bytes  (v2 validate)
  │
  ▼
allowlist filter (from_id contains?)
  │
  ▼
dispatch: Upsert  →  download blob → size guard → conflict backup → atomic write
          Tombstone → remove_file + last_removed mark
  │
  ▼
pending_log.record_receive (entry を JSON で path 配下に append)
```

### 5.4 RPC フロー (MCP → daemon)

```
host (Claude/Codex) → MCP tool 呼び出し
  │ stdio
  ▼
p2p-sync-mcp が #[tool] handler 内で daemon::rpc(socket, "sync.invite", {})
  │ Unix socket newline-delimited JSON
  ▼
daemon listener が accept → dispatch.rs::dispatch("sync.invite") → response
  │ Unix socket
  ▼
p2p-sync-mcp が response を Json<T> で wrap して MCP に返却
```

### 5.5 daemon 停止 / 異常終了

**設計方針**: シンプルに保つ。signal は 2 つだけ、timeout は 1 つの total 値、step ごとの細分化はしない。安全性 (= stale socket / 部分 write を残さない) は実装の不変条件で保証し、CLI flag や設定 file で expose しない。

#### Signal 種別

| signal | source | 期待挙動 |
|---|---|---|
| SIGINT | Ctrl+C | graceful shutdown |
| SIGTERM | launchd `bootout` / `kill <pid>` / `kickstart -k` | graceful shutdown (= SIGINT と同等扱い) |
| SIGKILL | `kill -9` / launchd `ExitTimeOut` 超過 | abort、Drop は走らない。stale socket / lock が残る可能性あり → **次回起動時の F2 mechanism (health-check probe + flock) で recover** (L1 反映)。data 整合性は atomic write の不変条件で保つ |
| (panic) | 内部 panic | 同上 |

#### Graceful shutdown 手順

```text
signal 受信 (SIGINT or SIGTERM)
  │
  ▼ (total budget 10s 以内、超過したら abort)
1. listener: shutdown signal 送信 (= 新規 connection 受付停止、in-flight RPC は最大 3s 待つ)
  │
  ▼
2. watcher: debouncer drop で fsnotify 停止 (= 進行中 debounce event は捨てる、
   atomic write 済 file は次回起動時に reconciliation で peer に再送)
  │
  ▼
3. receive_loop: gossip subscription close (= in-flight download は best-effort、
   pending_written に居る path は次回起動時の reconciliation で復旧)
  │
  ▼
4. endpoint.close().await: peer に QUIC CLOSE frame を 1s で送信 (= ack 待たず)、
   gossip neighbor は自然に NeighborDown を受け取る
  │
  ▼
5. socket file unlink (= 正常経路では listener が remove_file、
   失敗時は ListenerHandle::Drop が fallback で remove_file)
  │
  ▼
6. exit 0
```

total budget = **10 秒固定** (CLI flag 化せず)。budget 超過時は強制 abort + exit 1。step ごとの timeout 内訳は実装 default に任せる。

#### 異常停止時の不変条件 (= panic / SIGKILL でも data は壊れない保証)

| 不変条件 | 実装 |
|---|---|
| stale socket file を残さない | (1) graceful shutdown: listener が自前 remove_file。(2) panic / 早期 return: `ListenerHandle::Drop` impl で remove_file。(3) **SIGKILL では Drop が走らないので stale 残り得る** が、次回起動時の `sync.health-check` probe + `daemon.lock` flock で stale 判定 → unlink して回復 (L1 反映、F2 mechanism と整合) |
| 部分 write file を target に残さない | atomic write = `tempfile_in() + persist()` (= POSIX rename atomic、abort 時は tempfile が孤立するだけ) |
| 不完全な pending log entry を残さない | pending_log も tempfile + rename pattern |
| 破損 allowlist.json を残さない | `add_and_save` / `remove_and_save` で atomic write + write 失敗時 in-memory ロールバック |
| 破損 endpoint.key を残さない | 32 byte 固定、write 完了後しか read しない、起動時に size 検証 (= 失敗で start fail、auto regen はしない) |
| 破損 blobs.db を残さない | iroh-blobs `FsStore` の WAL / atomic flush に委ねる (= library 保証) |

これらは旧 `iroh-wiki-sync` から継承する不変条件。design.md で明文化することで、実装側で **絶対崩さない** 線として扱う。

#### macOS launchd 連携

`sandbox/scripts/launchd/com.user.p2p-dir-sync.plist` に固定値で記述:

```xml
<key>RunAtLoad</key> <true/>
<key>KeepAlive</key> <true/>
<key>ExitTimeOut</key> <integer>15</integer>     <!-- daemon の 10s budget より長い余裕 -->
<key>ThrottleInterval</key> <integer>10</integer> <!-- crash loop 抑止 -->
<key>StandardOutPath</key> <string>~/Library/Logs/p2p-dir-sync.log</string>
<key>StandardErrorPath</key> <string>~/Library/Logs/p2p-dir-sync.log</string>
```

挙動: `launchctl bootout` → SIGTERM → 上記 graceful 手順 → exit 0。15s 超過時は launchd が SIGKILL → 不変条件で安全終了。`KeepAlive=true` で異常終了時 10s 後に自動再起動 (= ThrottleInterval で crash loop 抑止)。

#### MCP server (L3) 側

stateless なので daemon が落ちても MCP server 自身は host (Claude / Codex) が exit するまで生きる。次 RPC で `connect()` が ENOENT を返したら `ErrorData::internal_error("daemon not running")` を host に返すだけ (= MCP server 側に再接続 retry なし、setup-doctor で user に明示)。

## 6. セキュリティ境界 (要件 §7.1)

### 6.1 脅威モデル (= 何から守る、何を守らない)

**前提**: `p2p-dir-sync` は **任意 dir の同期** を謳う以上、watched_dir には **private data が含まれる前提** で設計する (= `~/Documents/notes` 等の個人的内容、wiki content も含む)。

**守る**:
- 別 uid の user / process が daemon の socket / blobs / endpoint.key を読み書きする経路
- **未許可 peer が同期内容を読む経路** (= gossip mesh / blob fetch の両方で防御)
- **未許可 peer が同期内容を書き込む経路** (= 受信側 allowlist フィルタ)
- watched_dir 外の path に書込される経路 (= path traversal / symlink escape)
- 巨大 input / 巨大 file による DoS
- 多重 daemon 起動による socket 奪取
- AI agent (MCP tool 呼び出し) からの destructive / 範囲外操作

**守らない (= 脅威モデル外、シンプル方針で意図的に持たない)**:
- 同 uid の悪意ある process (= sandbox / container 等別 layer で守る話)
- TLS / 暗号化追加 (= Iroh が QUIC で transport 暗号化済、daemon の Unix socket は kernel 内通信)
- bearer token / mTLS 認証 (= Unix socket fs permission で十分)
- peer 改ざん検知 (= 同期内容の正当性は user の peer 信頼に委ねる、署名検証は将来拡張)
- 受信 file 内容の sanitization (= 上位 app が content を実行可能にしない責務、daemon は通常 file 扱い)
- 既に許可した peer が後で悪意ある change を流す経路 (= 信頼境界の問題、revoke で対応)

### 6.2 防御線 (= 実装方針)

シンプル方針: **kernel 機構 (file permission / uid) + Iroh の認可境界 (topic secret + ALPN wrap) + Rust の不変条件 (canonicalize / atomic / validation) で守る**。アプリ層暗号化 / bearer token は追加しない。

| カテゴリ | 防御線 | 実装 |
|---|---|---|
| **fs permission** | endpoint.key の secrecy | `0o600` (user only)、`paths.rs::ensure_dir_700` で親 dir も `0o700` |
|  | blobs / pending / allowlist の secrecy | dir は `0o700`、内 file は default umask (= 同 uid のみ access) |
|  | Unix socket 越権防止 | bind 直後 `chmod 0o600`、kernel で同 uid のみ `connect(2)` 可 |
|  | 多重 daemon 起動防止 (F2 反映) | 起動時に socket 存在 → `sync.health-check` で probe → 応答あれば既存 daemon と判定 exit 1、応答なければ stale → unlink → bind。並行起動 race 対策として lock file (`~/.local/share/p2p-dir-sync/daemon.lock` 0o600、flock) も併用 |
| **path safety** | watched_dir 外への書込防止 | `watched_dir.canonicalize()` で baseline 確定、受信 rel_path は `validate_relative_path` で `..` / absolute / backslash / 空 component 拒否 |
|  | symlink escape 防止 | parent dir を 1 段ずつ `symlink_metadata` で確認、symlink を見つけたら拒否。受信 target が symlink なら overwrite 拒否 |
|  | atomic write 保証 | sibling tempfile + `persist()` rename、`overwrite=false` 時は `persist_noclobber` で AlreadyExists を atomic 検出 |
| **peer 認可 (F1 + G1 反映)** | **gossip mesh 自体への join 制限** | topic_id を **folder/group secret** から導出 (= `BLAKE3("p2p-dir-sync/v1/topic\0" ‖ folder_secret)`、§4.1c)。folder_secret は invite ticket に含まれ、同 group の全 peer が同じ値を hold する = 第三者は mesh に join できない (= 3 peer chain でも 1 mesh が成立) |
|  | **blob ALPN への accept 制限** | `BlobsProtocol` を `AllowlistBlobs` wrapper で `Router::accept` する (= 旧 `AllowlistDocs` パターン)。`conn.remote_id()` を allowlist check、未許可なら `conn.close()` で reject。allowlist 外 peer は blob fetch 不能 |
|  | 不正 peer の gossip 受信遮断 | `receive_loop` の `WikiUpdate` 受信時に `from_id` を allowlist check、不一致なら drop |
|  | 不正 peer の blob fetch 遮断 (Initiator) | 受信した `WikiUpdate.from` の peer からのみ `downloader.download` (= 別 peer から引っ張らない) |
|  | **strict empty default (F4 反映)** | daemon 起動時 allowlist 空 = **何 peer も受信しない**。foreground 開発用 `--allow-open-all` flag で明示 opt-in 可 (= 初回 invite フローでも user が必ず accept を踏む) |
|  | **bilateral allowlist 成立 (G2 + I4 反映)** | strict mode 下では `sync.accept-invite` は **acceptor 側の local allowlist にのみ inviter を追加** (= mutual 未成立)。inviter 側で `sync.allow-peer <acceptor_id>` を明示的に呼び、両側 allowlist に counter-peer が居て初めて両方向 sync が動く (§3.3 7-step フロー)。片側欠けると AllowlistBlobs の reject で両方向止まる |
|  | strict mode の不可逆性 | accept-invite / allow-peer 1 回後は open_all へ戻れない (= bug で全受信に戻る事故防止)。リセットは `allowlist.json` 手動削除 + daemon 再起動 |
| **入力 validation** | RPC 引数 validation | `accept-invite` の ticket は `p2psync1-` prefix + base32 decode + `InviteTicket` deserialize、folder_secret 不在なら reject (G4)。`allow-peer` / `revoke` の peer_id は `EndpointId::from_str` 失敗 → reject |
|  | 巨大 request DoS 防止 | `read_capped_line` で `MAX_REQUEST_BYTES = 1 MiB` cap、超過で error response + 接続 close |
|  | 巨大 file 受信 DoS 防止 | 受信側 size guard、`max_file_size` (default 10 MiB) 超過は download せず skip + warn |
| **MCP surface 制限** | path 自由操作禁止 | `sync.add-folder` 等の tool を **作らない**、`--watch` は daemon CLI のみ |
|  | destructive 操作の明示性 | `revoke` / `accept-invite` は slash command 経由で user 承認、auto execute なし |
| **ログ漏洩防止 (F5 反映)** | secrets を log に出さない | endpoint.key の bytes は出さない、**ticket / folder_secret は出さない** (G1 反映)、`sync.recent-log` は **fixed log path (`~/Library/Logs/p2p-dir-sync.log`) のみ / 最大 100 行 / `<redacted>` で ticket / folder_secret フィールドを置換** |

### 6.3 注意点 (= 知っておくべき limit)

- **gossip mesh は folder-group-tied** (v2 で v1 を訂正): topic_id を **folder_secret から導出** (G1)。同 group の全 peer が同じ folder_secret を hold し、第三者は導出不能 = mesh join 不可。3 peer chain (A→B→C) でも 1 group / 1 mesh が成立する (= v1 の peer-local nonce 設計の bug を修正)
- **ticket は機密扱い**: `p2psync1-` envelope で wire format 明示 (G4)、内部に EndpointTicket + folder_secret を含む。**folder_secret 漏洩 = mesh 全体への join 権限漏洩** に等しいので、ticket 配布経路 (Slack / iMessage 等) は user の責任で confidential を保つ必要がある
- **same-uid の脅威は受け入れる**: 同 uid 内の別 process が socket / blobs / key を読めるが、これは macOS / Linux の通常権限境界。sandbox / SIP / Capability 等は別 layer
- **strict default**: daemon 起動時は何も受信しない (= F4)。最初の peer を入れるには `sync.accept-invite` を user が明示する。foreground 試運転で「とりあえず動かしたい」場合は `--allow-open-all` で起動 (= 警告 banner を log と stderr に出す)

### 6.4 シンプル方針との対応

| 持たない feature | 理由 |
|---|---|
| TLS / mTLS | Iroh QUIC で transport 暗号化済、socket は kernel 内通信 |
| bearer token / API key | Unix socket fs permission で同 uid のみ → 同等の認可 |
| 多要素認証 / WebAuthn | local 専用 control plane、user 自身しか触らない |
| RBAC / fine-grained permission | tool surface 自体が minimal (`sync.invite` 等 10 個)、roleの概念不要 |
| audit log の secure forward | `~/Library/Logs/p2p-dir-sync.log` への普通の append のみ、tamper protection なし (= same-uid threat 外なので不要) |
| 内容暗号化 (at-rest) | watched_dir / blobs の secrecy は user 信頼境界、fs 暗号化 (FileVault) が責務 |

これらは「将来必要になれば追加検討」枠で、初期 MVP では **意図的に持たない**。

## 7. observability (要件 §7.3)

`setup-doctor` slash command は **4 tool を以下の固定順序で叩く** (G7 反映、障害切り分けの段階性を担保):

| step | tool | 目的 | 失敗時の意味 |
|---|---|---|---|
| 1 | `sync.ping` | MCP server liveness (daemon 不要) | MCP server 起動失敗 / plugin install 不全 → daemon の話に進めない |
| 2 | `sync.health-check` | daemon socket 接続 + path / key / watched_dir 解決 | daemon が落ちている / 設定 path 不全 → §4.3 を確認 |
| 3 | `sync.status` | 同期状態 summary (peer 数 / open_all / pending 件数 / uptime) | daemon は健全だが allowlist / sync 状態が想定外 → user 判断 |
| 4 | `sync.recent-log` | daemon log の末尾 (= ticket / nonce は `<redacted>` 置換) | 直前の error / warning を確認 |

出力 view (= user に見せる集約):

```
✓ Step 1 sync.ping            : MCP server reachable
✓ Step 2 sync.health-check    : daemon connected
    socket             : ~/.local/share/p2p-dir-sync/daemon.sock (mode 0o600)
    key                : ~/.config/p2p-dir-sync/endpoint.key (32 bytes, present)
    blobs              : ~/.local/share/p2p-dir-sync/blobs (1.2 MB)
    pending            : ~/.local/share/p2p-dir-sync/pending (3 entries)
    watched            : /Users/me/notes (exists, 42 files)
    group_initialized  : true
    gossip_subscribed  : true                  # J1: false なら restart_required の hint
    restart_required   : false
✓ Step 3 sync.status          : sync running
    peers              : 2 peers (open_all=false)
        - alice (peer_id=abcd1234, added=2026-05-10, last_seen=12s ago)
        - bob   (peer_id=ef567890, added=2026-05-12, last_seen=null)   # J3: data-plane 未成立 (= allow-peer 忘れ疑い)
    pending_recent     : 3 entries
    uptime             : 12h 34m
ℹ Step 4 sync.recent-log (last 5 lines, ticket/folder_secret redacted):
  2026-05-14T10:00:01 INFO peer joined alice <redacted-peer>
  ...
```

daemon log は `~/Library/Logs/p2p-dir-sync.log` (= macOS 慣習)、`sync.recent-log` は最後 N 行 (≤ 100) を返す。各 step が失敗したら以降は **skip** して止まり、user に修復 hint を出す (= 上から順番に直していく対応)。

## 8. 旧 `p2p/iroh-wiki-sync/` からの移植 mapping

requirements.md §15 Phase 2 (= sync engine 移植) の作業内訳。**naming + path prefix を全更新**、wire schema は維持。

| 旧 path | 新 path | 変更 |
|---|---|---|
| `iroh-wiki-sync/src/sync/{runtime,state,send,receive,conflict}.rs` | `p2p-dir-sync/src/{runtime,state,send,receive,conflict}.rs` | module 階層を flat 化 (sync/ → 直下、crate 名で domain 区別)。**runtime.rs に `derive_topic_id(folder_secret)` を新規実装** (G1) |
| `iroh-wiki-sync/src/watcher.rs` | `p2p-dir-sync/src/watcher.rs` | as-is |
| `iroh-wiki-sync/src/allowlist.rs` | `p2p-dir-sync/src/allowlist.rs` | as-is。`open_all default` 削除 → **空 allowlist = strict empty** に変更 (F4) |
| (新規、旧 `allowlist_docs.rs` の patrol を再利用) | `p2p-dir-sync/src/allowlist_blobs.rs` | **新規** (F1): `BlobsProtocol` を `Router::accept` する前に wrap し、`conn.remote_id()` を allowlist check、未許可は `conn.close()` |
| `iroh-wiki-sync/src/message.rs` | `p2p-dir-sync/src/message.rs` | Rust type rename: `WikiUpdate` → `SyncUpdate`、`WikiUpdateBody` → `SyncUpdateBody`。**`#[serde(rename = "...")]` で wire tag は維持**。`WIKI_UPDATE_VERSION` → `SYNC_UPDATE_VERSION` (= 2 のまま)。**`InviteTicket` 型を新規追加** (= `EndpointTicket` + `folder_secret: [u8; 16]`、G1)、wire format は `p2psync1-<base32>` envelope (G4) |
| `iroh-wiki-sync/src/daemon/{state,listener,dispatch,client,mod}.rs` | `p2p-dir-sync/src/daemon/{...}.rs` | dispatch method 名を `wiki.*` → `sync.*` 系に全 rename。新 method `sync.status` / `sync.list-pending` / `sync.recent-log` / **`sync.allow-peer`** (G2) を追加。**`sync.ping` は daemon RPC から削除** (= MCP server 側で完結、F3)。**起動時の多重 daemon check** を listener 前に挿入 (F2) |
| `iroh-wiki-sync/src/keystore.rs` | `p2p-dir-sync/src/keystore.rs` | as-is、key path default を `~/.config/p2p-dir-sync/endpoint.key` に。**`folder-secret.bin` の load/generate を追加** (G1) |
| `iroh-wiki-sync/src/paths.rs` | `p2p-dir-sync/src/paths.rs` | prefix を `~/.local/share/p2p-dir-sync/` / `~/.config/p2p-dir-sync/` に。`daemon.lock` / `folder-secret.bin` 用 path 追加 (G5) |
| `iroh-wiki-sync/src/pending_metadata.rs` | `p2p-dir-sync/src/pending_log.rs` | rename (= 「change log」と中性的な命名)。schema に `schema_version: 1` field 追加 |
| `iroh-wiki-sync/src/main.rs` | `p2p-dir-sync/src/bin/p2p-sync.rs` | wiki 専用 logic はもう無いので機械的。CLI flag `--watch` を required に。**`--allow-open-all` flag 新規追加** (F4)。多重起動 check + lock file (F2) |
| `iroh-wiki-sync/src/bin/wiki-mcp-server.rs` | `p2p-dir-sync/src/bin/p2p-sync-mcp.rs` | tool 名前を `wiki.*` → `sync.*` に rename、wiki io 系 tool (read/write/search/list-files/changes) を **削除** (= llm-wiki 側に残す)。**`sync.ping` を MCP server 内 stateless tool として実装** (= daemon に投げない、F3)。**`sync.allow-peer` tool を新規追加** (G2) |
| `iroh-wiki-sync/src/changes.rs` | (移植しない) | wiki + git 連携なので別 product (llm-wiki) 側に残す |
| `iroh-wiki-sync/src/wiki_io/` | (移植しない) | 同上 |
| `iroh-wiki-sync/src/lib.rs::HealthInfo` | `p2p-dir-sync/src/lib.rs::HealthInfo` | **静的部 (HealthInfoStatic) + 動的部 (HealthInfoDynamic) に分離** (M3、v3)。静的: key_path / key_exists / blobs_dir / pending_dir / watched_dir / watched_dir_exists。動的 (Option): peer_count / open_all / uptime_secs / group_initialized。`run_health_check(daemon_state: Option<&DaemonState>)` 引数化 |

test もこれに合わせて移植。`tests/` 配下の integration test (= daemon round-trip 系 19 件) は path / RPC method 名を更新するだけ。

## 9. テスト戦略

requirements.md §14 受け入れ基準を満たすため:

| layer | test 種別 | 例 |
|---|---|---|
| unit | crate 内 `#[cfg(test)] mod tests` | watcher noise filter / allowlist schema / conflict backup path / send/receive 不変 |
| integration | `tests/*.rs` | daemon listener spawn + RPC round-trip 19 test (旧 `daemon/mod.rs tests` から移植) |
| example | `examples/standalone-watch.rs` | 2 dir + 2 endpoint で 1 file 同期、wiki 知識ゼロを実装で示す |
| smoke | `sandbox/scripts/2peer-smoke.sh` | 2 process 起動 + file create が伝搬する shell test |
| e2e | `sandbox/scripts/e2e-3peer.sh` | T1 Upsert / T2 Tombstone / T3 rename (poll backend) / T4 conflict |
| schema drift | `tests/pending_schema.rs` | `docs/schema/pending-entry.v1.upsert.json` + `pending-entry.v1.tombstone.json` の両 variant を deserialize (M2 + I5、将来 llm-wiki 側 deserialize と整合) |

test 数目標: 旧 `iroh-wiki-sync/` の sync 関連 ~80 + 新規 ~10 = ~90 件。

## 10. 実装フェーズ (要件 §15 を実行手順に展開)

| Phase | 内容 | 成果物 | 工数 |
|---|---|---|---|
| **0** | 要件 (`requirements.md`) + 設計 (`design.md`、本 doc) | doc 完成 | done |
| **1** | Cargo project skeleton: `Cargo.toml` + 空 `src/lib.rs` + 2 bin placeholder + `cargo build` green | workspace 起動可能 | 30min |
| **2** | sync engine 移植 (§8 mapping、message / runtime / state / send / receive / conflict / watcher / allowlist / keystore / paths / pending_log) + unit test ~80 | sync engine 単独 build + test green | 3-4h |
| **3** | daemon JSON-RPC (`daemon/`) + integration test 19 件移植 + `sync.status` / `sync.list-pending` / `sync.recent-log` 新規実装 | daemon binary 動作 | 2-3h |
| **4** | MCP server (`bin/p2p-sync-mcp.rs`) **10 tool 実装** (M1 反映: sync.ping + 9 daemon RPC) | MCP server 起動可、daemon と round-trip | 1-2h |
| **5** | plugin staging (`.claude-plugin/` + `.codex-plugin/` + `.mcp.json` + **8 commands** + skill + install.sh + verify.sh、M1 反映) | plugin install で setup-doctor が green | 2h |
| **6** | launchd staging (`sandbox/scripts/launchd/`、plist + wrapper) | daemon auto-start 確認 | 1h |
| **7** | 2 peer smoke (`sandbox/scripts/2peer-smoke.sh`) | 1 file 同期確認 | 1h |
| **8** | 3 peer e2e (`sandbox/scripts/e2e-3peer.sh`、T1-T4) | 4 test green | 2-3h |
| **9** | docs / README 整理 (operations.md + README.md + plugin/README.md) | doc 完成 | 1-2h |

合計 **13-18h**、requirements.md §15 の 9 phase と整合。

## 11. acceptance gate (= 各 phase 完了条件)

requirements.md §14 を phase 単位に分解:

- Phase 2 完了: `cargo build` + `cargo test` green (lib ~80) + **`pending-entry.v1.upsert.json` + `.tombstone.json` deserialize test green** (M2 + I5)
- Phase 3 完了: daemon binary 起動 + `nc -U <socket>` で `{"method":"sync.health-check"}` → `{"result":{"key_path":...,"watched_dir":...}}` 確認 (G3 反映、`sync.ping` は daemon RPC から削除済なので smoke は health-check で実施)
- Phase 4 完了: `p2p-sync-mcp` initialize round-trip success
- Phase 5 完了: `verify.sh` 10 check 全 ✅ + `claude plugin validate` 通過
- Phase 7 完了: 2 peer smoke で 1 file 同期確認
- Phase 8 完了: 3 peer e2e の T1-T4 全 green
- Phase 9 完了: LLM Wiki を watch 対象にしても **wiki schema 知らず**に通常 dir として動くことを example で示す

## 12. 設計判断 (Q1-Q10 ratify、requirements.md §13 + v1/v2 追加)

| Q | 確定 | 設計反映 |
|---|---|---|
| Q1 MCP から folder 追加禁止 | 採用 | tool に `sync.add-folder` 等を **作らない**、`--watch` は daemon CLI のみ |
| Q2 複数 folder 初期は不要 | 採用 | `WatcherConfig` に `dirs: Vec<PathBuf>` を持たない (= 1 only)、config.toml も作らない |
| Q3 Syncthing wrapper ではなく独自実装 | 採用 | Iroh 直接、外部 process spawn なし |
| Q4 CRDT 不採用 | 採用 | last-writer-wins + conflict backup を維持、`compute_conflict_backup_path` を継承 |
| Q5 plugin 名 `p2p-dir-sync` | 採用 | namespaced slash command は `/p2p-dir-sync:status` 等になる (Claude Code plugin の仕様) |
| Q6 初期対象 macOS | 採用 | launchd staging のみ提供、systemd / Windows Service は dir 構造で余地を残す (= `sandbox/scripts/launchd/` を将来 `systemd/` と並べる)。symlink / fs feature は POSIX 前提だが Linux でも build できるように `#[cfg(unix)]` で囲める箇所は囲む |
| **Q7 gossip mesh / blob の secrecy** (v1 → v2、G1) | **folder/group secret + AllowlistBlobs wrap** で第三者から保護 | `runtime.rs` で topic_id を `BLAKE3("p2p-dir-sync/v1/topic\0" ‖ folder_secret)` 導出 (= 全 peer 共通 topic、3 peer chain でも mesh 不分断)、blob ALPN は `allowlist_blobs.rs` で wrap。InviteTicket wire format = `p2psync1-<base32>` envelope (G4) |
| **Q8 多重 daemon 起動防止** (v1、F2) | **health-check probe + flock lock file** の二重防御を MVP に組込 (G3 でも acceptance gate を health-check に揃え) | 起動時 step 3 で health-check probe + `daemon.lock` flock。race 中の重複起動も exit 1 |
| **Q9 allowlist の初期 mode** (v1、F4) | **strict empty default**、`--allow-open-all` で明示 opt-in | allowlist.json 不在時は空 strict、`--allow-open-all` 時は warning banner と共に open_all |
| **Q10 bilateral allowlist 成立** (v2 → v4、G2 + I1 + I4) | **`sync.allow-peer` RPC + slash command** を MVP に追加、bilateral invite を **7 step** の正規フローとして文書化 (両 daemon 再起動 + Alice の allow-peer) | accept-invite は acceptor 側 local allowlist にのみ追加 (mutual 未成立)、inviter 側で別途 allow-peer を呼んで対称完成。片側欠けると AllowlistBlobs の reject で両方向止まる。UX 自動化 (= join request push + runtime rebuild) は連載 ⑥+ で検討 |

## 13. LLM Wiki / 他 consumer との関係 (要件 §10 を実装に落とす)

- LLM Wiki は **`p2p-dir-sync` の consumer**、本 repo に **knowledge ゼロ**で実装
- LLM Wiki が pending log を読みたければ:
  - (a) `~/.local/share/p2p-dir-sync/pending/<repo_hash>/*.json` を直接 deserialize (= `schema_version: 1` を共有契約として扱う)
  - (b) MCP server 経由で `sync.list-pending` tool を呼ぶ (= cross-MCP server invocation)
  - (c) Unix socket に直接 connect して `sync.list-pending` RPC を投げる (= LLM Wiki MCP server が daemon socket を知っている形)
  - 推奨: **(c)** (= 上位 LLM Wiki が `--sync-socket <path>` 引数で daemon socket を受け取る、source dep ゼロ)
- 本 repo には LLM Wiki 関連の **CLI 引数 / RPC method / コメントを一切持たない** (= 純粋に "watched directory の P2P sync" だけが仕様)

LLM Wiki 側に Phase 4+ で `wiki-mcp` に `--sync-socket <path>` 引数を追加する設計を残しておく。LLM Wiki 側 plan で対応 (= 本 repo 外)。

## 14. 開いた論点 (= 実装段階で詰める、v1 で 3 件 closed)

| # | 論点 | 方針 | v1 status |
|---|---|---|---|
| D1 | `sync.status` の field 細部 | short (peer_id 8 char + label + added_at) で MVP、full は `sync.list-peers` を見る。`status` には `uptime_secs` も含める | **closed** (v1 §3.2 で固定) |
| D2 | `sync.recent-log` の log 取得方法 | fixed log path (`~/Library/Logs/p2p-dir-sync.log`) を tail。**lines 上限 100、ticket / folder_secret は `<redacted>` 置換** (F5 + G1) | **closed** (v1 §3.2 + §6.2 で固定) |
| D3 | `--watch` の受付種別 | dir のみ (`metadata().is_dir()` check)、symlink は canonicalize 後に判定 | open (Phase 2-3 で確定) |
| D4 | watched_dir 不在 / 移動時の挙動 | watcher_loop が NotFound 検出 → daemon は warn loop で待機 (= 自動 restart せず user 介入待ち)、`sync.health-check` で `watched_dir_exists: false` を返す | open (Phase 2-3 で確定) |
| D5 | daemon 多重起動防止 | **ping check + flock lock file** (F2、v1 で確定) | **closed** (v1 §5.1 step 3 で固定) |
| D6 | endpoint.key 破損時の挙動 | start fail (exit 78)、user に再生成手順を示す。auto regen は **しない** (= EndpointId が変わると peer 側 allowlist も無効化されるため危険) | open (Phase 2-3 で確定) |
| D7 | folder-secret.bin の不在 / 紛失時の挙動 (v4、I3 反映で lazy generate と整合) | (a) **初回起動時の不在**: 正常 = `group_initialized=false` で起動、mesh に居ない、`sync.invite` / `sync.accept-invite` を待つ (= H1 + I1 と整合)。(b) **既に初期化済 daemon が次回起動時に file を失っている (= file system 破損 / 誤削除)**: start fail せず group_initialized=false で起動するが、これは「未初期化 daemon」と区別不能 = 旧 group の peer と通信不能。user 介入で再 invite or 全 peer に新 ticket 配布が必要。auto regen は **絶対しない** (= 新 secret になると旧 group と topic 不一致) | open (Phase 2-3 で確定) |
| D8 | mutual allowlist 未成立の検出 (v2 → v5、G2 + I2 + J2 + K2) | local daemon は相手の allowlist 状態を直接知れない (= remote self-report protocol 無し)。代わりに `sync.list-peers` の response に `last_seen_at: Option<i64>` (= **data-plane 成功時刻**: 直近 blob fetch / blob serve / Tombstone 受信成功時、NeighborUp event time は含まない、K2 で §3.3 定義に整合) を含め、setup-doctor で「peer X は accept されているが last_seen_at が null / 古い → mutual allowlist が片側のみ or peer 落ちている疑い」を表示。完全な mutual 自動判定 (= gossip 経由の self-report) は連載 ⑥+ で検討 | open (Phase 3-4 で確定、MVP では last_seen_at 出すだけで OK) |

D3 / D4 / D6 / D7 / D8 は実装中に挙動 fixture を作って確定。

## 15. 次の step

1. **本 design.md の user review** (修正指示があれば v1 で反映)
2. v1 確定後、Phase 1 (Cargo skeleton) 着手
3. Phase 2-3 完了時点で再 review (= daemon 単独動作確認)
4. 以降 phase ごとに review fix を挟みながら進行

並行して LLM Wiki 側 (= 別 repo or workspace) は `--sync-socket` 引数で本 daemon を consume する設計に進める想定。本 repo は LLM Wiki の進行を待たずに Phase 1-9 完走可能 (= 完全独立)。

---

**要約**: `p2p-dir-sync` は 4 layer (Plugin / MCP server / daemon / Iroh) の独立 project。
LLM Wiki を知らず、任意 dir を P2P 同期する control plane を AI agent に提供する。
旧 `iroh-wiki-sync/` から sync engine + daemon RPC + MCP server を **rename + 機能拡張** (`sync.status` / `sync.list-pending` / `sync.recent-log` 追加) して移植。
9 phase / 13-18h で MVP 完成想定。
