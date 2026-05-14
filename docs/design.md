# p2p-dir-sync 設計書

> [`requirements.md`](requirements.md) で確定した要件を実装に落とすための設計 plan。
>
> 改版履歴は git log を参照。

## 0. 設計の前提

- **任意ディレクトリの P2P 同期** が目的。consumer application を知らない
- **AI agent 向け control plane**: 1 daemon + 1 MCP server + 1 plugin
- MVP は 1 watched directory、複数 folder / config.toml / 自由な path 操作は将来拡張
- last-writer-wins + conflict backup (CRDT 不採用)
- macOS 初期対象、systemd / Windows Service への移植余地は残す

依存方向 (= 一方向):

```
plugin (UX)  →  MCP server (AI agent surface)  →  daemon (sync engine)  →  Iroh
```

## 1. レイヤー構造

```text
┌───────────────────────────────────────────────────┐
│  L4. Plugin (Claude Code / Codex)                  │
│      .claude-plugin/ + .codex-plugin/              │
│      commands/ + skills/ + .mcp.json               │
└────────────────────┬──────────────────────────────┘
                     │ stdio (MCP protocol)
┌────────────────────▼──────────────────────────────┐
│  L3. MCP server (p2p-sync-mcp binary)              │
│      AI agent 向け 10 tool surface、stateless      │
└────────────────────┬──────────────────────────────┘
                     │ Unix socket (newline-delimited JSON-RPC)
┌────────────────────▼──────────────────────────────┐
│  L2. Daemon (p2p-sync binary)                      │
│      watcher / receive / send / allowlist /        │
│      pending log / endpoint key                    │
└────────────────────┬──────────────────────────────┘
                     │ Iroh API
┌────────────────────▼──────────────────────────────┐
│  L1. Iroh (QUIC + gossip + blob ALPN)              │
└───────────────────────────────────────────────────┘
```

| layer | 持つ責務 | 持たない責務 |
|---|---|---|
| L4 plugin | slash command / skill trigger / setup-doctor / install script | sync logic / state |
| L3 MCP server | tool schema / argument validation / daemon RPC 中継 / error formatting | sync state (= stateless) |
| L2 daemon | watcher / sender / receiver / allowlist / pending log / endpoint key 永続化 / Unix socket | git / 上位 app schema |
| L1 Iroh | QUIC P2P / gossip mesh / blob fetch | daemon 設定 / fs 操作 |

## 2. dir 構造

```
p2p-dir-sync/                        # 独立 repo (jj/git colocated)
├── Cargo.toml
├── README.md
├── docs/
│   ├── requirements.md
│   ├── design.md                    # 本 doc
│   ├── operations.md                # 運用手順 (launchd / plugin install / 復旧)
│   └── schema/
│       ├── pending-entry.v1.upsert.json    # golden JSON fixture
│       └── pending-entry.v1.tombstone.json
├── src/
│   ├── lib.rs                       # public API + module 宣言
│   ├── runtime.rs                   # Iroh stack 起動 + derive_topic_id
│   ├── state.rs                     # SyncState (pending_written / last_written)
│   ├── send.rs                      # send_file (Upsert broadcast)
│   ├── receive.rs                   # receive_loop / handle_upsert / handle_tombstone
│   ├── conflict.rs                  # compute_conflict_backup_path
│   ├── watcher.rs                   # fsnotify + PollWatcher
│   ├── allowlist.rs                 # peer allowlist (open_all / strict)
│   ├── allowlist_blobs.rs           # blob ALPN allowlist wrap
│   ├── message.rs                   # SyncUpdate / InviteTicket wire format
│   ├── keystore.rs                  # endpoint.key + folder-secret.bin 永続化
│   ├── paths.rs                     # path 規約 (~/.local/share/p2p-dir-sync/)
│   ├── pending_log.rs               # 受信 change log
│   ├── daemon/
│   │   ├── mod.rs                   # pub use + integration tests
│   │   ├── state.rs                 # DaemonState + Request/Response
│   │   ├── listener.rs              # bind_listener / accept_loop / ListenerHandle
│   │   ├── dispatch.rs              # sync.* RPC dispatch
│   │   └── client.rs                # rpc()
│   └── bin/
│       ├── p2p-sync.rs              # daemon binary (= L2)
│       └── p2p-sync-mcp.rs          # MCP server binary (= L3)
├── plugin/                          # L4 staging dir
│   ├── .claude-plugin/marketplace.json + plugin.json
│   ├── .codex-plugin/plugin.json    # skills / mcpServers field で bundled components を指す
│   ├── .mcp.json
│   ├── skills/sync/SKILL.md
│   ├── commands/                    # 8 slash commands
│   │   ├── setup-doctor.md
│   │   └── status.md / invite.md / accept.md / allow-peer.md / peers.md / revoke.md / pending.md
│   ├── scripts/install.sh
│   ├── verify.sh
│   └── README.md
├── sandbox/
│   ├── scripts/
│   │   ├── 2peer-smoke.sh
│   │   ├── e2e-3peer.sh
│   │   └── launchd/                 # plist + wrapper
│   └── README.md
├── examples/
│   └── standalone-watch.rs          # 単独使用例
└── tests/                            # integration tests
```

`.codex-plugin/plugin.json` の例 (公式 Codex manifest 仕様):

```json
{
  "name": "p2p-dir-sync",
  "version": "0.1.0",
  "description": "P2P directory sync",
  "skills": "./skills/",
  "mcpServers": "./.mcp.json",
  "interface": { "displayName": "p2p-dir-sync" }
}
```

`.claude-plugin/plugin.json` は manifest だけ持ち、`commands/` `skills/` `.mcp.json` は Claude 公式が plugin root から自動 discover する。

## 3. 公開 API

### 3.1 Library API (`p2p_dir_sync`)

```rust
// runtime.rs
pub struct SyncRuntime { ... }
impl SyncRuntime {
    pub async fn build(
        endpoint: Endpoint,
        blobs_dir: &Path,
        allowlist: Arc<AllowList>,
        folder_secret: Option<&[u8; 16]>,  // None = gossip subscribe skip
    ) -> Result<Self>;
}

// send.rs / receive.rs
pub async fn send_file(...) -> Result<()>;
pub async fn receive_loop(...) -> Result<()>;

// watcher.rs
pub enum WatcherBackend { Recommended, Poll }
pub fn spawn_watcher(...) -> Result<(WatcherHandle, mpsc::UnboundedReceiver<DebouncedEvent>)>;
pub async fn watcher_loop(...) -> Result<()>;

// allowlist.rs
pub struct AllowList { ... }
pub struct PeerInfo { pub label: Option<String>, pub added_at: i64, pub last_seen_at: Option<i64> }

// daemon/
pub struct DaemonState { ... }
pub fn spawn_listener(socket_path: PathBuf, state: Arc<DaemonState>) -> Result<ListenerHandle>;
pub async fn rpc(socket: &Path, method: &str, params: Value) -> Result<Value>;

// pending_log.rs
pub enum PendingEntry { Upsert {...}, Tombstone {...} }
pub fn record_receive(root: &Path, repo_hash: &str, entry: &PendingEntry) -> Result<()>;
pub fn list_pending(root: &Path, repo_hash: &str) -> Result<Vec<PendingEntry>>;

// paths.rs
pub fn default_socket_path() -> Result<PathBuf>;
pub fn default_blobs_dir() -> Result<PathBuf>;
pub fn default_pending_dir() -> Result<PathBuf>;
pub fn default_key_path() -> Result<PathBuf>;
pub fn default_folder_secret_path() -> Result<PathBuf>;
pub fn ensure_dir_700(dir: &Path) -> Result<()>;

// lib.rs
pub fn run_health_check(daemon_state: Option<&DaemonState>) -> Result<HealthInfo>;

pub struct HealthInfoStatic {
    pub key_path: PathBuf,
    pub key_exists: bool,
    pub blobs_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub watched_dir: Option<PathBuf>,
    pub watched_dir_exists: bool,
}

pub struct HealthInfoDynamic {
    pub peer_count: u32,
    pub open_all: bool,
    pub uptime_secs: u64,
    pub group_initialized: bool,   // folder-secret.bin の有無
    pub gossip_subscribed: bool,   // 現 runtime が topic を subscribe 中か
    pub restart_required: bool,    // group_initialized && !gossip_subscribed
}

pub struct HealthInfo {
    #[serde(flatten)]
    pub static_info: HealthInfoStatic,
    pub dynamic_info: Option<HealthInfoDynamic>,
}
```

library を `pub` で出すのは examples / tests / 将来の embedded use case のため。通常 consumer は daemon binary + Unix socket RPC で接続する。

### 3.2 Daemon Unix Socket RPC

socket path: `~/.local/share/p2p-dir-sync/daemon.sock` (mode 0o600)、newline-delimited JSON。

| method | params | result | 備考 |
|---|---|---|---|
| `sync.health-check` | `{}` | `HealthInfo` | daemon 接続確認。setup-doctor の primary 指標 |
| `sync.status` | `{}` | `{watched_dir, peer_count, open_all, recent_pending_count, key_exists, uptime_secs, group_initialized, gossip_subscribed, restart_required}` | summary view |
| `sync.invite` | `{}` | `{ticket, restart_required}` | 未初期化 daemon なら folder_secret 新規生成 + `restart_required: true`。既初期化なら既存 ticket を返す |
| `sync.accept-invite` | `{ticket, label?}` | `{peer_id, label, my_peer_id, restart_required}` | ticket parse + folder_secret 整合 check → inviter を **local allowlist に追加** (mutual はまだ未成立)。未初期化なら folder_secret adopt + `restart_required: true` |
| `sync.allow-peer` | `{peer_id, label?}` | `{added, peer_id, label}` | inviter 側で counter-peer を **local allowlist に追加** (= mutual の対称半分)。ticket 不要、再起動不要 |
| `sync.list-peers` | `{}` | `{peers: [PeerInfo...], open_all}` | `PeerInfo = {peer_id, label?, added_at, last_seen_at}` |
| `sync.revoke` | `{peer_id}` | `{removed, peer_id}` | best-effort、active connection は次回 handshake で reject |
| `sync.list-pending` | `{limit?}` | `{entries: [PendingEntry...]}` | 受信 change log |
| `sync.recent-log` | `{lines?: u32 ≤ 100}` | `{lines: [String...]}` | `~/Library/Logs/p2p-dir-sync.log` の末尾、ticket / folder_secret は `<redacted>` 置換 |

`sync.ping` は daemon RPC に存在せず、MCP server 内で完結する (§3.3)。

### 3.3 MCP tool (`p2p-sync-mcp` binary)

```rust
// MCP server 自身の readiness (daemon 不要)
#[tool(name = "sync.ping", ...)]
async fn sync_ping(&self) -> String { "pong".to_string() }

// 他は daemon Unix socket に rpc() で 1 RPC を投げる薄い wrapper
#[tool(name = "sync.health-check", ...)]    → daemon "sync.health-check"
#[tool(name = "sync.status", ...)]          → daemon "sync.status"
#[tool(name = "sync.invite", ...)]          → daemon "sync.invite"
#[tool(name = "sync.accept-invite", ...)]   → daemon "sync.accept-invite"
#[tool(name = "sync.allow-peer", ...)]      → daemon "sync.allow-peer"
#[tool(name = "sync.list-peers", ...)]      → daemon "sync.list-peers"
#[tool(name = "sync.revoke", ...)]          → daemon "sync.revoke"
#[tool(name = "sync.list-pending", ...)]    → daemon "sync.list-pending"
#[tool(name = "sync.recent-log", ...)]      → daemon "sync.recent-log"
```

`sync.ping` vs `sync.health-check`:

| tool | daemon 接続 | 失敗時の意味 |
|---|---|---|
| `sync.ping` | 不要 | MCP server 起動失敗 / plugin install 不全 |
| `sync.health-check` | 必要 | daemon が落ちている / 設定不全 |

MCP tool から folder 追加・削除はしない。`--watch <dir>` は daemon 起動時の CLI 引数で固定。

### 3.4 Bilateral invite フロー (= 双方向同期成立の正規手順)

strict default 下では 7 step:

```text
[Alice]  (daemon 起動済、folder-secret.bin 不在)
1. /p2p-dir-sync:invite
   → folder_secret 新規生成 + persist → ticket_A + restart_required: true
2. Alice: daemon 再起動 (launchctl kickstart -k ...)
   → group_initialized=true で gossip subscribe → mesh に居る
3. ticket_A を Bob に out-of-band で渡す

[Bob]  (daemon 起動済、folder-secret.bin 不在)
4. /p2p-dir-sync:accept <ticket_A>
   → folder_secret adopt + persist + Alice を Bob allowlist に追加
   → response.my_peer_id = Bob_id、restart_required: true
5. Bob: daemon 再起動 → mesh join
   → 出力テンプレに「Alice 側で sync.allow-peer <Bob_id> を実行」を明示

[Alice]
6. /p2p-dir-sync:allow-peer <Bob_id> [--label Bob]
   → Bob_id を Alice allowlist に追加 (runtime mutation、再起動不要)
7. mutual 成立、両方向 sync 動作開始
```

**なぜ両 daemon の再起動が必要か**: folder_secret の adopt / generate は file system に persist するが、gossip subscribe を反映するには `SyncRuntime::build` の再実行が必要。MVP では runtime swap を実装しない方針。`KeepAlive=true` で再起動は数秒で完了。

**step を 1 つでも忘れると両方向止まる**:

| 不足 step | 影響 |
|---|---|
| step 2 (Alice 再起動忘れ) | Alice 自身が mesh に居ない → Bob が accept しても無意味 |
| step 5 (Bob 再起動忘れ) | Bob が gossip subscribe してない → Alice の Upsert を受信できない |
| step 6 (Alice の allow-peer 忘れ) | ① Alice → Bob: Alice 側 AllowlistBlobs が Bob の ALPN connection を reject。② Bob → Alice: Alice 側 receive_loop が gossip Upsert を `from_id` allowlist check で drop。両方向止まる |

allow-peer step は optional ではない。`accept-invite` の response テンプレで必ず inviter 側に依頼する hint を出す。

### 3.5 `last_seen_at` の定義 (= mutual 動作確認の proxy 指標)

`sync.list-peers` の `PeerInfo.last_seen_at` は **data-plane が実際に成功した時刻** (Unix epoch 秒) のみを記録:

- ✅ 含む: 直近 blob fetch 成功 / blob serve 成功 / Tombstone 受信成功
- ❌ 含まない: gossip `NeighborUp` event (= mesh 上見えたが data-plane 未確認)

NeighborUp を含めない理由: mesh に居ても blob ALPN が AllowlistBlobs で reject されたり、gossip Upsert が receive allowlist で drop されたりして data-plane で落ちる可能性がある。control-plane 由来だと「mesh で見えているのに sync 動かない」状態を誤って「動作中」と表示してしまう。

`last_seen_at == null` → 「accept しただけで一度も data-plane 成立してない」状態。setup-doctor で warning を出す根拠。

`mutual: bool` の自動判定はしない: local daemon は相手の allowlist 状態を直接知れない (= remote self-report protocol が無い)。`last_seen_at` で「通信できているか」を観測する方が user に有用。

## 4. データ形式

### 4.1 Wire format (SyncUpdate)

```rust
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

### 4.2 InviteTicket

```rust
pub struct InviteTicket {
    pub endpoint: EndpointTicket,   // EndpointId + addr
    pub folder_secret: [u8; 16],    // group identity (= 16 byte entropy)
}
```

wire format = `p2psync1-<base32(serde_json::to_string(&InviteTicket))>`。

- prefix `p2psync1-`: schema version 1。将来 v2 で field 追加なら `p2psync2-` に bump
- accept 時に prefix check → version 不一致なら reject

### 4.3 folder_secret と topic_id

folder_secret は **「同 group 1 個」の identity**。同 mesh に居る全 peer が同じ値を hold する。

- **lazy generate**: daemon 初回起動時 (= `folder-secret.bin` 不在) は何もせず `group_initialized=false` で起動、mesh に居ない (= gossip subscribe しない)。初期化 path は 2 通り:
  - `sync.invite` 呼出: 新規 16 byte 生成 → 自分が group founder
  - `sync.accept-invite <ticket>` 呼出: ticket 内 folder_secret を adopt → 既存 group に join
- 既に初期化済の daemon が **異なる** folder_secret の invite を accept しようとした場合は reject (= group merge は MVP scope 外)。仕切り直しは `folder-secret.bin` 手動削除 + 再起動

topic_id 導出:

```rust
fn derive_topic_id(folder_secret: &[u8; 16]) -> TopicId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"p2p-dir-sync/v1/topic\0");
    hasher.update(folder_secret);
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}
```

3 peer chain (A→B→C) でも全員 同 folder_secret = 同 topic = 1 mesh。第三者は folder_secret を知らないので mesh join 不可。

### 4.4 PendingEntry schema (受信 change log)

Upsert example:

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

Tombstone example:

```json
{
  "schema_version": 1,
  "kind": "tombstone",
  "rel_path": "entities/old.md",
  "received_at": 1715600100,
  "source_peer": "abcd1234..."
}
```

Rust 表現:

```rust
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingEntry {
    Upsert { schema_version: u32, rel_path: String, received_at: i64,
             source_peer: String, blob_hash: String, bytes: u64 },
    Tombstone { schema_version: u32, rel_path: String, received_at: i64,
                source_peer: String },
}
```

golden fixture (`docs/schema/pending-entry.v1.{upsert,tombstone}.json`) は schema drift 防止のため両 variant を commit。

### 4.5 永続化 path 規約

```
~/.local/share/p2p-dir-sync/
├── daemon.sock                # Unix socket (mode 0o600)
├── daemon.lock                # flock 用 lock file (mode 0o600)
├── folder-secret.bin          # 16 byte folder/group secret (mode 0o600、lazy)
├── blobs/                      # iroh-blobs fs store (dir mode 0o700)
├── pending/<repo_hash>/        # 受信 pending log per repo
│   └── <iso-timestamp>-<peer>.json
└── allowlist.json             # peer allowlist v2 schema

~/.config/p2p-dir-sync/
└── endpoint.key               # 32-byte Ed25519 (mode 0o600)

~/Library/Logs/
└── p2p-dir-sync.log           # daemon log (macOS 慣習)
```

## 5. ライフサイクル

### 5.1 daemon 起動 (`p2p-sync --watch <dir>`)

```
 1. CLI arg parse
 2. watched_dir canonicalize
 3. 多重起動 check:
    a. socket 存在? Yes → sync.health-check probe (1s timeout)
       └ 応答あり → exit 1 (already running)
       └ timeout / ENOENT / ECONNREFUSED → stale → unlink → 続行
    b. flock(daemon.lock, LOCK_EX|LOCK_NB)
       └ 失敗 → exit 1 (another daemon holds lock)
 4. endpoint.key load / generate
 5. folder_secret lazy load:
    └ folder-secret.bin 存在 → load、group_initialized=true
    └ 不在 → 何もしない、group_initialized=false
 6. Endpoint::bind + online
 7. blobs_dir prepare 0o700 + load fs
 8. allowlist load (file 不在 → strict empty、--allow-open-all で open_all)
 9. SyncRuntime::build (blob + Router + AllowlistBlobs wrap)
    └ group_initialized なら gossip subscribe (topic = derive_topic_id(folder_secret))
    └ 未初期化なら subscribe skip
10. spawn_listener (Unix socket、0o600)
11. spawn watcher + receive_loop (group_initialized 不問で RPC は応答)
12. tokio::select! で SIGINT / SIGTERM 待機 → §5.4
```

未初期化 daemon の挙動: RPC は応答するが mesh に居ない。`sync.health-check` で `group_initialized=true, gossip_subscribed=false, restart_required=true` のような状態を返すことで「再起動してください」を user に明示する。

### 5.2 file 書込フロー (local → peer)

```
user edit foo.md
  → watcher.rs (debounce 200ms) → should_skip filter
  → state チェック (self-loop 防止: last_written / pending_written)
  → send_file: blob add → SyncUpdate::Upsert → gossip broadcast
```

### 5.3 file 受信フロー (peer → local)

```
gossip Event::Received
  → SyncUpdate::from_bytes (v2 validate)
  → allowlist filter (from_id contains?、drop なら return)
  → dispatch:
       Upsert    → download blob → size guard → conflict backup → atomic write
       Tombstone → remove_file + last_removed mark
  → pending_log.record_receive (entry を JSON で path 配下に append)
```

### 5.4 daemon 停止 / 異常終了

| signal | source | 期待挙動 |
|---|---|---|
| SIGINT | Ctrl+C | graceful shutdown |
| SIGTERM | launchd bootout / kill | 同上 |
| SIGKILL | kill -9 / launchd ExitTimeOut 超過 | abort、Drop は走らない。stale socket / lock は次回起動時 step 3 で recover |
| panic | 内部 panic | Drop で fallback、SIGKILL 同様 |

Graceful shutdown 手順 (total budget 10s 固定):

```text
signal 受信
 1. listener: shutdown signal 送信 (in-flight RPC 最大 3s 待つ)
 2. watcher: debouncer drop で fsnotify 停止
 3. receive_loop: gossip subscription close
 4. endpoint.close().await (peer に CLOSE frame、ack 待たず)
 5. socket file unlink (listener が remove_file、失敗時 Drop で fallback)
 6. exit 0
```

異常停止時の不変条件 (= panic / SIGKILL でも data は壊れない):

| 保証 | 実装 |
|---|---|
| stale socket file | (1) graceful: listener が remove_file。(2) panic / 早期 return: `ListenerHandle::Drop`。(3) SIGKILL では Drop 不発、stale 残るが次回起動の health-check probe + flock で recover |
| 部分 write file | atomic write = tempfile + persist (POSIX rename) |
| 不完全 pending log | pending_log も tempfile + rename |
| 破損 allowlist.json | atomic write + write 失敗時 in-memory ロールバック |
| 破損 endpoint.key | 32 byte 固定、size 検証で start fail (auto regen しない) |
| 破損 blobs.db | iroh-blobs FsStore の WAL に委ねる |

#### launchd 設定

`sandbox/scripts/launchd/com.user.p2p-dir-sync.plist`:

```xml
<key>RunAtLoad</key> <true/>
<key>KeepAlive</key> <true/>
<key>ExitTimeOut</key> <integer>15</integer>      <!-- daemon の 10s budget より長く -->
<key>ThrottleInterval</key> <integer>10</integer>  <!-- crash loop 抑止 -->
<key>StandardOutPath</key> <string>~/Library/Logs/p2p-dir-sync.log</string>
<key>StandardErrorPath</key> <string>~/Library/Logs/p2p-dir-sync.log</string>
```

`launchctl bootout` → SIGTERM → graceful 10s → exit 0。15s 超過時は launchd が SIGKILL → 不変条件で安全終了。`KeepAlive=true` で異常終了時 10s 後に auto restart。

## 6. セキュリティ境界

### 6.1 脅威モデル

watched_dir は **private data が含まれる前提** (= 任意 dir 同期だから)。

**守る**:
- 別 uid の user / process が daemon の socket / blobs / endpoint.key を読み書きする経路
- 未許可 peer が同期内容を読む経路 (gossip mesh + blob fetch の両方)
- 未許可 peer が同期内容を書き込む経路 (受信側 allowlist filter)
- watched_dir 外への書込 (path traversal / symlink escape)
- 巨大 input / 巨大 file による DoS
- 多重 daemon 起動による socket 奪取

**守らない** (シンプル方針で意図的に持たない):
- 同 uid の悪意ある process (= sandbox / container 等別 layer の話)
- 既に許可した peer が後で悪意ある change を流す経路 (= 信頼境界、revoke で対応)
- 受信 file 内容の sanitization (= 上位 app の責務、daemon は通常 file 扱い)

### 6.2 防御線

**kernel 機構 (file permission / uid) + Iroh の認可境界 (topic secret + ALPN wrap) + Rust の不変条件 (canonicalize / atomic / validation) で守る**。アプリ層暗号化 / bearer token は追加しない。

| カテゴリ | 防御線 | 実装 |
|---|---|---|
| fs permission | secrecy | endpoint.key / folder-secret.bin / daemon.lock を 0o600、dir は 0o700、socket は bind 直後 chmod 0o600 |
|  | 多重 daemon 起動防止 | 起動時に socket 存在 → health-check probe → 応答あれば exit 1、なければ stale → unlink。並行 race は `daemon.lock` flock で抑止 |
| path safety | watched_dir 外への書込防止 | `watched_dir.canonicalize()` + `validate_relative_path` で `..` / absolute / backslash / 空 component 拒否 |
|  | symlink escape 防止 | parent dir を 1 段ずつ `symlink_metadata` で確認、symlink を見つけたら拒否 |
|  | atomic write 保証 | sibling tempfile + `persist()` rename、`overwrite=false` 時は `persist_noclobber` |
| peer 認可 | gossip mesh join 制限 | topic_id = `BLAKE3("p2p-dir-sync/v1/topic\0" ‖ folder_secret)`、第三者は導出不能 |
|  | blob ALPN accept 制限 | `BlobsProtocol` を `AllowlistBlobs` wrapper で wrap。`conn.remote_id()` allowlist check、未許可は `conn.close()` |
|  | gossip 受信遮断 | `receive_loop` で `WikiUpdate.from_id` を allowlist check、不一致なら drop |
|  | strict empty default | 起動時 allowlist 空 = 何 peer も受信しない。`--allow-open-all` で明示 opt-in |
|  | bilateral allowlist 必須 | `sync.accept-invite` は acceptor 側のみ追加 → inviter 側で `sync.allow-peer` を呼ぶ。片側欠けで両方向止まる |
|  | strict mode 不可逆性 | accept / allow-peer 1 回後は open_all へ戻れない。リセットは allowlist.json 手動削除 + 再起動 |
| 入力 validation | RPC 引数 | `accept-invite` ticket は `p2psync1-` prefix + base32 decode + InviteTicket deserialize、`allow-peer` / `revoke` の peer_id は `EndpointId::from_str` 失敗 → reject |
|  | DoS 防止 | `MAX_REQUEST_BYTES = 1 MiB` cap、`max_file_size = 10 MiB` で受信 skip + warn |
| MCP surface | path 操作禁止 | `sync.add-folder` 等は作らない、`--watch` は daemon CLI のみ |
| ログ漏洩防止 | secrets を log に出さない | endpoint.key bytes / ticket / folder_secret を出さない。`sync.recent-log` は fixed path + ≤ 100 行 + `<redacted>` 置換 |

### 6.3 注意点

- **ticket は機密扱い**: `p2psync1-` envelope に folder_secret を含む。漏洩 = mesh 全体への join 権限漏洩。配布経路 (Slack / iMessage 等) は user の責任で confidential を保つ
- **same-uid threat は受け入れ**: 同 uid 内の別 process は socket / blobs / key を読める (= 通常の OS 権限境界、sandbox 等は別 layer)
- **strict default**: 起動時は何も受信しない。foreground 試運転で `--allow-open-all` を使う場合は warning banner を log と stderr に出す

### 6.4 持たない feature

| 持たない | 理由 |
|---|---|
| TLS / mTLS | Iroh QUIC で transport 暗号化済、socket は kernel 内通信 |
| bearer token / API key | Unix socket fs permission で同 uid のみ → 同等の認可 |
| RBAC / fine-grained permission | tool surface 自体が minimal (10 個)、role 概念不要 |
| audit log の secure forward | same-uid threat 外なので不要 |
| 内容暗号化 (at-rest) | fs 暗号化 (FileVault) が責務 |

## 7. observability

`setup-doctor` slash command は 4 tool を固定順序で叩く:

| step | tool | 失敗時の意味 |
|---|---|---|
| 1 | `sync.ping` | MCP server 起動失敗 / plugin install 不全 |
| 2 | `sync.health-check` | daemon が落ちている / 設定 path 不全 |
| 3 | `sync.status` | daemon は健全だが allowlist / sync 状態が想定外 |
| 4 | `sync.recent-log` | 直前の error / warning を確認 |

出力例:

```
✓ Step 1 sync.ping            : MCP server reachable
✓ Step 2 sync.health-check    : daemon connected
    socket             : ~/.local/share/p2p-dir-sync/daemon.sock (0o600)
    key                : ~/.config/p2p-dir-sync/endpoint.key (32 bytes)
    watched            : /Users/me/notes (exists, 42 files)
    group_initialized  : true
    gossip_subscribed  : true
    restart_required   : false
✓ Step 3 sync.status          : sync running
    peers              : 2 peers (open_all=false)
        - alice (peer_id=abcd1234, last_seen=12s ago)
        - bob   (peer_id=ef567890, last_seen=null)   # data-plane 未成立
    uptime             : 12h 34m
ℹ Step 4 sync.recent-log (last 5 lines, redacted): ...
```

各 step が失敗したら以降は skip、user に修復 hint を出す。

## 8. テスト戦略

| layer | 種別 | 例 |
|---|---|---|
| unit | crate 内 `#[cfg(test)]` | watcher noise filter / allowlist schema / conflict backup path |
| integration | `tests/*.rs` | daemon listener spawn + RPC round-trip |
| example | `examples/standalone-watch.rs` | 2 dir + 2 endpoint で 1 file 同期 |
| smoke | `sandbox/scripts/2peer-smoke.sh` | 2 process 起動 + file create 伝搬 |
| e2e | `sandbox/scripts/e2e-3peer.sh` | T1 Upsert / T2 Tombstone / T3 rename / T4 conflict |
| schema drift | `tests/pending_schema.rs` | golden JSON (Upsert + Tombstone) を deserialize |

test 数目標: ~90 件 (unit ~80 + integration / schema ~10)。

## 9. 実装フェーズ

| Phase | 内容 | 工数 |
|---|---|---|
| 0 | 要件 + 設計 doc | done |
| 1 | Cargo skeleton (Cargo.toml + lib.rs + 2 bin placeholder + jj init) | 30min |
| 2 | sync engine (message / runtime / state / send / receive / conflict / watcher / allowlist / allowlist_blobs / keystore / paths / pending_log) + unit test ~80 | 3-4h |
| 3 | daemon JSON-RPC (`daemon/`) + integration test + 多重起動 check | 2-3h |
| 4 | MCP server (10 tool 実装) | 1-2h |
| 5 | plugin staging (.claude-plugin + .codex-plugin + 8 commands + skill + install.sh + verify.sh) | 2h |
| 6 | launchd staging (plist + wrapper) | 1h |
| 7 | 2 peer smoke | 1h |
| 8 | 3 peer e2e (T1-T4) | 2-3h |
| 9 | docs / README 整理 | 1-2h |

合計 13-18h。

### acceptance gate

- Phase 2 完了: `cargo build` + `cargo test` green (lib ~80) + golden JSON deserialize test green
- Phase 3 完了: daemon 起動 + `nc -U <socket>` で `sync.health-check` round-trip
- Phase 4 完了: `p2p-sync-mcp` initialize round-trip
- Phase 5 完了: `verify.sh` 全 ✅ + `claude plugin validate` 通過
- Phase 7 完了: 2 peer smoke で 1 file 同期確認
- Phase 8 完了: T1-T4 全 green

## 10. 設計判断 (確定事項)

| # | 確定 |
|---|---|
| Q1 MCP から folder 追加禁止 | tool に `sync.add-folder` 等を作らない、`--watch` は daemon CLI のみ |
| Q2 複数 folder 初期は不要 | 1 daemon = 1 watched dir、config.toml も作らない |
| Q3 独自実装 (Syncthing wrapper にしない) | Iroh 直接、外部 process spawn なし |
| Q4 CRDT 不採用 | last-writer-wins + conflict backup |
| Q5 plugin 名 | `p2p-dir-sync`、namespaced slash command は `/p2p-dir-sync:status` 等 |
| Q6 初期対象 macOS | launchd staging のみ、systemd / Windows Service は dir 構造で余地を残す |
| Q7 peer secrecy | folder_secret 由来 topic + AllowlistBlobs wrap。`p2psync1-<base32>` envelope |
| Q8 多重 daemon 起動防止 | health-check probe + flock の二重防御を MVP に組込 |
| Q9 allowlist 初期 mode | strict empty default、`--allow-open-all` で opt-in |
| Q10 bilateral allowlist 成立 | `sync.allow-peer` RPC + slash command。7-step フローで mutual 成立 |

## 11. 開いた論点 (実装段階で確定)

| # | 論点 | 仮方針 |
|---|---|---|
| D3 | `--watch` の受付種別 | dir のみ (`metadata().is_dir()`)、symlink は canonicalize 後判定 |
| D4 | watched_dir 不在 / 移動時 | watcher_loop が NotFound 検出 → daemon は warn loop で待機、`sync.health-check` で `watched_dir_exists: false` を返す |
| D6 | endpoint.key 破損 | start fail (exit 78)、user に再生成手順を示す。auto regen は **しない** |
| D7 | folder-secret.bin 紛失 (既初期化後) | start fail せず group_initialized=false で起動するが旧 group と通信不能。user 介入で再 invite。auto regen は絶対しない |
| D8 | mutual allowlist 未成立の検出 | `sync.list-peers` の `last_seen_at` で表現、null / 古いなら setup-doctor が warning。完全な mutual 自動判定は連載 ⑥+ で gossip self-report を追加する場合に検討 |

D3 / D4 / D6 / D7 / D8 は実装中に挙動 fixture を作って確定する。
