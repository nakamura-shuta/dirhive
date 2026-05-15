# launchd sample (macOS)

macOS で `dirhive` daemon を user agent として常駐させるための sample plist と
install / uninstall script。 design.md §5.4 を実装ベースに落とし込んだもの。

## ファイル

| file | 内容 |
|---|---|
| `com.user.dirhive.plist.template` | **reference 用** plist sample (= 主要 key の意味 / 値の docstring 入り)。 install には使わない (= Phase 5 review H1 fix で sed 置換を廃止)。 中身を読みたい人向け |
| `lib/render-plist.py` | `plistlib` で実値版 plist を XML 出力する helper。 path 中の `&` や XML 特殊文字を自動 escape |
| `install-launchd.sh` | `render-plist.py` 経由で plist を生成 → `~/Library/LaunchAgents/` に設置 + `launchctl bootstrap` で boot |
| `uninstall-launchd.sh` | `launchctl bootout` + plist 削除 |

## install / start

事前に `plugin/scripts/install.sh` で `~/.local/bin/dirhive` を入れておくこと。

```sh
# dry-run で plist 内容だけ確認
./install-launchd.sh --watch ~/notes --dry-run

# 実際に install + boot
./install-launchd.sh --watch ~/notes
```

`--bin <PATH>` で binary path を override 可能 (= default は `$HOME/.local/bin/dirhive`)。

install 後の確認:

```sh
launchctl print gui/$UID/com.user.dirhive   # state / pid / last_exit_code
tail -f ~/Library/Logs/dirhive.stderr.log    # tracing log (daemon)
tail -f ~/Library/Logs/dirhive.stdout.log    # launchd の stdout redirect
```

invite / accept-invite で folder_secret を変更した直後は **再起動が必要**:

```sh
launchctl kickstart -k gui/$UID/com.user.dirhive   # SIGTERM → 再起動
```

## stop / uninstall

```sh
./uninstall-launchd.sh
```

(`bootout` → SIGTERM → daemon の 10s graceful budget → exit)

`launchctl bootout` は冪等。 plist が既に消えてても error なし。

## plist 主要 key

| key | 値 | 意味 |
|---|---|---|
| `Label` | `com.user.dirhive` | service 識別子 |
| `ProgramArguments` | `[bin, --watch, dir]` | 実行コマンド |
| `RunAtLoad` | `true` | boot 直後に start |
| `KeepAlive` | `true` | 異常終了時 auto restart |
| `ExitTimeOut` | `15` | SIGTERM 後 15s で SIGKILL (= daemon の 10s graceful budget + 5s 余裕) |
| `ThrottleInterval` | `10` | crash loop 防止 (= 起動失敗時 10s 待って再起動) |
| `StandardOutPath` | `~/Library/Logs/dirhive.stdout.log` | launchd stdout redirect。 daemon 本体の file appender (= `~/Library/Logs/dirhive.log`) とは **別 file** にする (= 重複出力回避) |
| `StandardErrorPath` | `~/Library/Logs/dirhive.stderr.log` | 同上 stderr 用 |
| `EnvironmentVariables.HOME` | `$HOME` | GUI / launchctl bootstrap 経路の薄い env に対する hint |
| `EnvironmentVariables.DIRHIVE_LOG` | `info,dirhive=debug` | tracing filter |

## トラブルシュート

| 症状 | 対処 |
|---|---|
| `launchctl bootstrap ... error: Input/output error` | plist syntax / 絶対 path の問題。 `--dry-run` で内容確認 + `plutil -lint plist_path` で検証 |
| service 起動直後 すぐ exit | daemon log を確認: `tail -50 ~/Library/Logs/dirhive.stderr.log`。 多くは `watched_dir` 不在 / endpoint.key 破損 / 多重起動 lock |
| daemon が起動はしているが peer に届かない | `/dirhive:health-check` で `gossip_subscribed` / `restart_required` を確認。 `restart_required: true` なら `launchctl kickstart -k gui/$UID/com.user.dirhive` |
| service が無限に restart している | `ThrottleInterval=10` で sleep するが、 永続的に失敗する原因 (key 破損 / bind 失敗 等) があれば log を見る。 一旦 uninstall して foreground で `dirhive --watch <dir>` を試す |
| 古い socket / lock 残留 | daemon は起動時 `health-check probe` + `flock` で stale を recover する。 ただし手動で消したいときは `rm ~/.local/share/dirhive/daemon.{sock,lock}` |
