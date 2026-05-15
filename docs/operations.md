# operations.md — dirhive 運用手順

`dirhive` daemon を実機で長期運用するときの手順集。 **新規 user の install から、 launchd 常駐、 invite/accept、 トラブルシュート、 復旧、 uninstall まで** をこの 1 doc にまとめる。

設計の詳細 (= なぜそうなっているか) は [`design.md`](./design.md) を参照。
AI agent 経由の操作は [`../plugin/README.md`](../plugin/README.md) を参照。

---

## 1. install

### 1.1 binary install

```sh
cd /path/to/dirhive
./plugin/scripts/install.sh
```

何が起きるか:

1. `cargo build --release --bin dirhive --bin dirhive-mcp`
2. `~/.local/bin/dirhive` と `~/.local/bin/dirhive-mcp` を 0755 で配置
3. `~/.local/bin` が `$PATH` に無ければ stderr で warning

`$PATH` に未追加なら shell rc に追加:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

### 1.2 install 後の sanity check

```sh
./plugin/verify.sh
```

5 step 確認 (詳細は `plugin/README.md`):
1. binary on PATH
2. plugin manifest files 揃っている
3. `.mcp.json` の `command` が PATH 上で resolve できる (relative なら warn)
4. daemon socket (= 起動後にだけ存在、 起動前は ⚠)
5. `claude plugin validate` (= `claude` CLI が居れば schema 確認)

### 1.3 plugin を AI agent に登録

**Claude Code**:

```sh
/plugin install /path/to/dirhive/plugin
```

**Codex / 他**: `plugin/.codex-plugin/plugin.json` を agent の plugin 設定で指定。

---

## 2. daemon を起動する

### 2.1 foreground (= 初回試運転)

```sh
dirhive --watch ~/notes
```

起動 log が stderr に流れる。 Ctrl+C で graceful shutdown (= 10s budget)。

env override:
- `DIRHIVE_LOG=info,dirhive=debug` — tracing filter
- `DIRHIVE_STATE_DIR=...` — `~/.local/share/dirhive` の上書き (= test 用)
- `DIRHIVE_CONFIG_DIR=...` — `~/.config/dirhive` の上書き
- `DIRHIVE_LOG_DIR=...` — `~/Library/Logs` の上書き

### 2.2 launchd で常駐させる (macOS user agent)

```sh
./sandbox/scripts/launchd/install-launchd.sh --watch ~/notes
```

何が起きるか:
1. plist template を `__BIN__` / `__WATCH__` / `__HOME__` で実値置換
2. `~/Library/LaunchAgents/com.user.dirhive.plist` に書き出し
3. `launchctl bootstrap gui/$UID ...` で boot
4. `RunAtLoad=true` + `KeepAlive=true` で次回 login / 異常終了でも auto restart

dry-run (= plist 内容だけ確認):

```sh
./sandbox/scripts/launchd/install-launchd.sh --watch ~/notes --dry-run
```

詳細は [`../sandbox/scripts/launchd/README.md`](../sandbox/scripts/launchd/README.md) を参照。

### 2.3 daemon の log を見る

| 出力先 | 中身 |
|---|---|
| `~/Library/Logs/dirhive.log` | daemon 本体の file appender (= `sync.recent-log` が tail する file、 secrets 自動 redact) |
| `~/Library/Logs/dirhive.stdout.log` | launchd の stdout redirect (= 普段空) |
| `~/Library/Logs/dirhive.stderr.log` | launchd の stderr redirect (= cargo build 等の出力もここ) |

AI agent からは `/dirhive:setup-doctor` の step 4 や `sync.recent-log` で覗ける (= secrets redact 済)。

---

## 3. invite / accept (= 2 peer 同期成立まで)

詳細は design.md §3.4 の 7-step。 ここでは AI agent 経由 (= slash command) と CLI 経由 (= RPC 直叩き) の両方の手順を示す。

### 3.1 AI agent 経由

[Alice]
```
/dirhive:invite
→ ticket + restart_required: true
```

→ Alice 側で daemon を再起動 (= 後述 §3.3) → ticket を out-of-band で Bob に渡す

[Bob]
```
/dirhive:accept <ticket>
→ my_peer_id (= Bob 自身の id) + restart_required: true
```

→ Bob 側で daemon を再起動 → Bob は Alice に `my_peer_id` を out-of-band で伝える

[Alice]
```
/dirhive:allow-peer <Bob_id>
```

bilateral allowlist 成立、 sync 開始。

### 3.2 CLI 経由 (= 自動化向け)

`sandbox/scripts/lib/rpc.py` で daemon socket に直接 RPC を投げられる:

```sh
SOCK=~/.local/share/dirhive/daemon.sock

# Alice 側
python3 sandbox/scripts/lib/rpc.py "$SOCK" sync.invite

# Bob 側 (= 別 host)
python3 sandbox/scripts/lib/rpc.py "$SOCK" sync.accept-invite \
    '{"ticket": "dirhive1-...", "label": "alice"}'

# Alice 側
python3 sandbox/scripts/lib/rpc.py "$SOCK" sync.allow-peer \
    '{"peer_id": "...", "label": "bob"}'
```

### 3.3 daemon の restart (= invite/accept の後)

**foreground** で動かしている場合: Ctrl+C → 再度 `dirhive --watch ...`

**launchd 経由**:

```sh
launchctl kickstart -k gui/$UID/com.user.dirhive
```

`-k` = 既存 instance に SIGTERM、 `kickstart` = 即座に再起動。 10s 程度で復帰。

---

## 4. peer の追加 / 削除

### 4.1 既存 group に新しい peer (= 3 peer 目以降) を追加する

新規 peer (= Carol) は **Alice の既存 ticket でも accept できる**。 ticket 自体は folder_secret を含むだけで「 1 度しか使えない 」 性質は無い (= 機密扱いの理由)。

```sh
# Carol: 既存 ticket で accept
python3 sandbox/scripts/lib/rpc.py "$SOCK" sync.accept-invite \
    '{"ticket": "<alice's ticket>", "label": "alice"}'
launchctl kickstart -k gui/$UID/com.user.dirhive   # Carol 再起動

# Alice: Carol を allow
python3 ... rpc.py "$ALICE_SOCK" sync.allow-peer '{"peer_id": "<carol_id>", "label": "carol"}'
```

完全 mesh sync が要るなら、 Bob と Carol も互いを allow-peer する:

```sh
# Bob: Carol を allow
python3 ... rpc.py "$BOB_SOCK"   sync.allow-peer '{"peer_id": "<carol_id>"}'
# Carol: Bob を allow
python3 ... rpc.py "$CAROL_SOCK" sync.allow-peer '{"peer_id": "<bob_id>"}'
```

`sandbox/scripts/3peer-smoke.sh` がこの完全 mesh シナリオを自動化している。

### 4.2 peer を revoke する

```sh
python3 ... rpc.py "$SOCK" sync.revoke '{"peer_id": "<bad_peer_id>"}'
```

best-effort。 既存 connection は次回 handshake で reject される。 強制 disconnect は無いので「 既に転送中の blob 」 は転送が完了する可能性がある。

完全な遮断が必要なら:

```sh
# folder_secret を rotate (= 全 honest peer で再 bootstrap)
rm ~/.local/share/dirhive/folder-secret.bin
launchctl kickstart -k gui/$UID/com.user.dirhive
# → group 解散。 全 peer で 7-step invite を作り直し
```

---

## 5. トラブルシュート

### 5.1 「 起動するけど sync が動かない 」

```sh
/dirhive:setup-doctor   # 4-step probe
# または
python3 ... rpc.py "$SOCK" sync.health-check
python3 ... rpc.py "$SOCK" sync.status
python3 ... rpc.py "$SOCK" sync.list-peers
```

確認ポイント:

| symptom | 意味 | 対処 |
|---|---|---|
| `gossip_subscribed: false` | folder_secret 未生成 or restart 必要 | `sync.invite` or `sync.accept-invite` を呼んでから再起動 |
| `restart_required: true` | folder_secret は持っているが gossip 未 subscribe | daemon を再起動 (= §3.3) |
| `list-peers` 空 | allowlist に誰も居ない | `sync.allow-peer` を呼ぶ、 または `sync.accept-invite` の応答 hint を読み返す |
| `list-peers[].last_seen_at = null` | data-plane 未成立 (= 接続はしたが blob fetch / Tombstone がまだ流れていない) | bilateral allowlist が片側だけになっていないか peer 双方で確認、 file を 1 つ作って propagation を待つ |

### 5.2 「 socket が古いまま残っている (= bind に失敗する) 」

```text
ERROR: another daemon is already listening on .../daemon.sock
```

実際に別 instance が居るか確認:

```sh
launchctl print gui/$UID/com.user.dirhive 2>/dev/null | head -3
ps aux | grep dirhive
```

別 instance が居なければ stale socket:

```sh
rm ~/.local/share/dirhive/daemon.sock
rm ~/.local/share/dirhive/daemon.lock  # flock を保持する file
```

(= 通常の起動では daemon 自身が health-check probe + flock で stale recover するが、 SIGKILL / panic で残骸が残るケースの手当て)

### 5.3 「 endpoint.key を間違えて消した / 破損した 」

`endpoint.key` (= peer の identity) を消すと **新しい EndpointId で再起動** する。 既存 peer 群の allowlist には居なくなるので、 全 peer から再度 `sync.allow-peer` を呼んでもらう必要がある。

```sh
rm ~/.config/dirhive/endpoint.key
launchctl kickstart -k gui/$UID/com.user.dirhive
# → 新規 EndpointId で起動
python3 ... rpc.py "$SOCK" sync.health-check   # 新 EndpointId 確認
# 全 peer に「 私の新 ID は X です、 allow-peer してください 」 を out-of-band で連絡
```

### 5.4 「 受信した file が watch dir 外に書き込まれた / symlink 経由で escape された 」

理論上ありえない (= `resolve_safe_path` で symlink walk check + watched_dir prefix check)。 もし起きたら **bug**。 daemon を停止して `sync.recent-log` で当該 receive event を採取 + issue 報告:

```sh
./sandbox/scripts/launchd/uninstall-launchd.sh
python3 ... rpc.py "$SOCK" sync.recent-log '{"lines": 100}'   # daemon 動いている間に
```

### 5.5 「 同期 file が `*.conflict-local-XXXXXXXX` という名前で増殖している 」

`compute_conflict_backup_path` の動作。 受信 file が local 既存と異なる内容のとき、 **既存 local を backup file に rename して受信内容で上書き** する。 これは「 並行編集で local 変更が失われないように 」 の防御策。

backup file は **watcher の skip filter に入っている** (= `src/watcher.rs` の `path_is_skippable` が name 中に `.conflict-local-` を含む path を skip)。 そのため backup 自体は peer へ broadcast されず、 local 退避物として留まる。

backup file の中身を確認したら、 元 file と手動 merge して 1 本に戻し、 backup file は削除する (= 放置しても害はないが、 混乱防止のため掃除推奨)。

### 5.6 「 daemon が高頻度で restart している (= crash loop) 」

`launchctl print gui/$UID/com.user.dirhive` で `last_exit_code` / `last_exit_status` を見る。 永続的に同じ exit code なら原因あり:

```sh
tail -100 ~/Library/Logs/dirhive.stderr.log
```

代表的な原因:
- watched_dir が消えている / permission がない
- endpoint.key が 32 byte でない (= 破損、 自動 regen はしない、 §5.3 参照)
- folder-secret.bin が 16 byte でない
- `--allow-open-all` を local file 削除しないまま再起動

`ThrottleInterval=10` で 10s sleep してから再起動するので、 デバッグ中はその間に foreground で `dirhive --watch ...` を試して error 内容を見る方が早い。

---

## 6. uninstall

### 6.1 launchd service を止める

```sh
./sandbox/scripts/launchd/uninstall-launchd.sh
```

(= `launchctl bootout` + plist 削除、 daemon は graceful shutdown)

### 6.2 binary を消す

```sh
rm ~/.local/bin/dirhive ~/.local/bin/dirhive-mcp
```

### 6.3 state / log を消す

⚠ folder_secret や endpoint.key も消えるので、 **group 再開はできなくなる**。 完全に綺麗にする場合だけ実行:

```sh
rm -rf ~/.local/share/dirhive   # blobs + allowlist + pending_log + folder_secret
rm -rf ~/.config/dirhive        # endpoint.key
rm -f  ~/Library/Logs/dirhive.{,stdout.,stderr.}log
```

### 6.4 plugin を AI agent から外す

**Claude Code**:

```sh
/plugin uninstall dirhive
```

**Codex / 他**: agent の plugin 設定から該当エントリを削除。

---

## 7. smoke test (= 開発 / regression 検出用)

`sandbox/scripts/` 配下:

| script | 目的 |
|---|---|
| `install-smoke.sh` | 1-peer install → verify → daemon → MCP probe → graceful shutdown |
| `2peer-smoke.sh` | 2-peer install + 7-step bilateral invite + 双方向 file propagation |
| `3peer-smoke.sh` | 3-peer 完全 mesh allowlist + 6 経路 file propagation |

実機 N0 relay 経由なので外向き UDP / TCP が通る環境で実行する。 GUI 環境差を疑った時は `install-smoke.sh` の step 5 (`claude plugin validate`) と step 7 (MCP probe) が特に有効。

```sh
./sandbox/scripts/install-smoke.sh
./sandbox/scripts/2peer-smoke.sh
./sandbox/scripts/3peer-smoke.sh
```

---

## 8. 参考 file 一覧

| path | 役割 |
|---|---|
| `~/.local/bin/dirhive` | daemon binary |
| `~/.local/bin/dirhive-mcp` | MCP server binary (= AI agent 経路) |
| `~/.local/share/dirhive/daemon.sock` | Unix socket (0o600) |
| `~/.local/share/dirhive/daemon.lock` | flock 用 file (0o600、 多重起動防止) |
| `~/.local/share/dirhive/folder-secret.bin` | folder secret (= group identity、 0o600) |
| `~/.local/share/dirhive/allowlist.json` | peer allowlist (0o600) |
| `~/.local/share/dirhive/bootstrap-peers.json` | gossip subscribe 用 bootstrap peer addr (= accept-invite で永続化) |
| `~/.local/share/dirhive/blobs/` | iroh-blobs FsStore (= 同期した file の中身) |
| `~/.local/share/dirhive/pending/<repo_hash>/*.json` | 受信 change log |
| `~/.config/dirhive/endpoint.key` | Ed25519 secret (= peer identity、 0o600) |
| `~/Library/Logs/dirhive.log` | daemon file appender (= secrets redact 済、 `sync.recent-log` の source) |
| `~/Library/Logs/dirhive.{stdout,stderr}.log` | launchd の redirect 先 |
| `~/Library/LaunchAgents/com.user.dirhive.plist` | launchd user agent 定義 |
