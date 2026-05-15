//! file system watcher (= fsnotify + 200ms debounce、design.md §5.2)。
//!
//! - macOS / Linux native backend (= `RecommendedWatcher`、kqueue / FSEvents / inotify)
//!   を default、test / network FS では `--poll` で `PollWatcher` に切り替え可能
//! - 200ms debounce で「1 回の save が複数 event を生む」を 1 つに纏める
//!   (= notify-debouncer-full の責務、本 module は直接 dedupe しない)
//! - `should_skip` で dotfile / tempfile / debounce 化済 access event を捨てる
//! - watcher thread → tokio runtime への bridging は `mpsc::UnboundedReceiver`
//!   を返す (= subscriber 側で async/await できる)
//!
//! self-loop 防止 (= 受信 → write → watcher で再 broadcast) は本 module では
//! 行わず、`watcher_loop` の caller が `SyncState.last_written` / `last_removed`
//! を見て skip する責務 (= 関心分離)。watcher 自体は「event を出す」までで止める。

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use notify_debouncer_full::notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{
    DebounceEventResult, DebouncedEvent, Debouncer, FileIdMap, new_debouncer,
};
use tokio::sync::mpsc;

/// debounce 窓 (= design.md §5.2)。
pub const DEBOUNCE_DURATION: Duration = Duration::from_millis(200);

/// watcher backend 選択。
///
/// - `Recommended`: OS native (= kqueue / FSEvents / inotify)。production default
/// - `Poll`: notify の `PollWatcher`。test / NFS / shared volume で使う
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WatcherBackend {
    #[default]
    Recommended,
    Poll,
}

/// watcher の hold handle。drop で debouncer が止まる。
///
/// Phase 2 段階では `Recommended` backend のみ実装。`Poll` backend は
/// `new_debouncer_opt::<_, PollWatcher, _>` で同じ shape に組める想定で、
/// 追加 dep なしで Phase 3 で対応予定。
pub struct WatcherHandle {
    #[allow(dead_code)]
    debouncer: Debouncer<notify_debouncer_full::notify::RecommendedWatcher, FileIdMap>,
}

impl std::fmt::Debug for WatcherHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatcherHandle").finish_non_exhaustive()
    }
}

/// `watched_dir` に対して debouncer を spawn し、event を mpsc に流す。
///
/// `backend` 引数は将来 `Poll` 対応用の placeholder。現在 `Poll` を指定すると
/// `unimplemented` error を返す (= silently `Recommended` に fallback しない)。
pub fn spawn_watcher(
    watched_dir: &Path,
    backend: WatcherBackend,
) -> Result<(WatcherHandle, mpsc::UnboundedReceiver<DebouncedEvent>)> {
    if backend == WatcherBackend::Poll {
        anyhow::bail!("WatcherBackend::Poll is not yet implemented (Phase 3 で追加予定)");
    }

    let (tx, rx) = mpsc::unbounded_channel::<DebouncedEvent>();
    // watched_dir を canonicalize して closure に move する。 should_skip 判定は
    // 「watched_dir prefix を strip した relative path」で行う (= watched_dir 自体が
    // `/var/folders/.../.tmpXXX/` のような dot 名でも、 配下 file は通常扱い)。
    let root = watched_dir
        .canonicalize()
        .with_context(|| format!("canonicalize watched_dir {}", watched_dir.display()))?;
    let skip_root = root.clone();

    let handler = move |res: DebounceEventResult| match res {
        Ok(events) => {
            for ev in events {
                if should_skip_under_root(&ev, &skip_root) {
                    continue;
                }
                if tx.send(ev).is_err() {
                    tracing::debug!("watcher receiver closed; dropping event");
                }
            }
        }
        Err(errs) => {
            for e in errs {
                tracing::warn!("notify debouncer error: {e}");
            }
        }
    };

    let mut debouncer = new_debouncer(DEBOUNCE_DURATION, None, handler)
        .context("create notify debouncer")?;
    debouncer
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", root.display()))?;

    Ok((WatcherHandle { debouncer }, rx))
}

/// event を skip すべきか判定 (= broadcast に送らない)。
///
/// - dotfile / dot-dir: `.git/` / `.DS_Store` 等。任意 dir 同期で「内部 metadata
///   が流出して欲しくない」 ものを除外する
/// - tempfile suffix: editor の swap / atomic-rename tempfile (`.swp` / `.swx` /
///   `.tmp` / `~` 末尾)
/// - `EventKind::Access` / `EventKind::Other`: mutation じゃないので broadcast 不要
///
/// 判定は path の各 component を見る (absolute path 前提)。watched_dir 自体に
/// dot を含むケース (例: `/var/folders/.../.tmpXXX/`) を誤判定したい時は
/// [`should_skip_under_root`] を使う。
pub fn should_skip(ev: &DebouncedEvent) -> bool {
    if matches!(ev.event.kind, EventKind::Access(_) | EventKind::Other) {
        return true;
    }
    ev.event.paths.iter().any(|p| path_is_skippable(p))
}

/// `root` で strip した残り (= relative path) で skip 判定する版。
/// watched_dir 自体が dot 名でも内部の通常 file が誤って skip されないようにする。
pub fn should_skip_under_root(ev: &DebouncedEvent, root: &Path) -> bool {
    if matches!(ev.event.kind, EventKind::Access(_) | EventKind::Other) {
        return true;
    }
    ev.event.paths.iter().any(|p| {
        let rel = match p.strip_prefix(root) {
            Ok(r) => r,
            // root 外 path は安全側で skip しない (= caller の strip 判定に任せる)。
            // ただし「絶対 path だが root に含まれない」 ケースは判定対象としては
            // 異常なので、 古い path_is_skippable で fallback 評価する。
            Err(_) => return path_is_skippable(p),
        };
        path_is_skippable(rel)
    })
}

fn path_is_skippable(p: &Path) -> bool {
    for comp in p.components() {
        if let std::path::Component::Normal(s) = comp
            && let Some(name) = s.to_str()
        {
            if name.starts_with('.') {
                return true;
            }
            if name.ends_with(".swp")
                || name.ends_with(".swx")
                || name.ends_with(".tmp")
                || name.ends_with('~')
            {
                return true;
            }
            // conflict backup (= `<orig>.conflict-local-<peer8>`) は受信側が
            // 退避用に作る file (= `compute_conflict_backup_path` in conflict.rs)。
            // 自 watcher が拾って peer に再 broadcast すると mesh 全体で同 file が
            // ピンポン状に増殖する (= 各 peer がさらに別 name で backup を作る)。
            // 抑止するため、 name の途中に `.conflict-local-` を含むものは skip。
            if name.contains(".conflict-local-") {
                return true;
            }
        }
    }
    false
}

/// watcher mpsc を消費する loop。
///
/// **本 loop 自体は send_file を呼ばない**: event を rel_path に正規化して
/// `on_event` callback に渡すだけ。 self-loop 防止 / blob add / SyncUpdate
/// publish は caller (= `daemon` 起動 path) の責務。 関心分離による。
pub async fn watcher_loop<F, Fut>(
    mut rx: mpsc::UnboundedReceiver<DebouncedEvent>,
    watched_dir: PathBuf,
    mut on_event: F,
) -> Result<()>
where
    F: FnMut(DebouncedEvent, PathBuf) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    while let Some(ev) = rx.recv().await {
        let Some(first) = ev.event.paths.first() else {
            continue;
        };
        let rel = match first.strip_prefix(&watched_dir) {
            Ok(r) => r.to_path_buf(),
            Err(_) => {
                tracing::debug!("event path outside watched_dir: {}", first.display());
                continue;
            }
        };
        if rel.as_os_str().is_empty() {
            continue;
        }
        on_event(ev, rel).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify_debouncer_full::notify::{Event, EventKind, event::AccessKind};
    use std::time::Instant;

    fn ev(kind: EventKind, paths: Vec<PathBuf>) -> DebouncedEvent {
        let mut e = Event::new(kind);
        e.paths = paths;
        DebouncedEvent {
            event: e,
            time: Instant::now(),
        }
    }

    fn modify_any() -> EventKind {
        EventKind::Modify(notify_debouncer_full::notify::event::ModifyKind::Any)
    }

    #[test]
    fn debounce_duration_is_200ms() {
        assert_eq!(DEBOUNCE_DURATION, Duration::from_millis(200));
    }

    #[test]
    fn should_skip_access_events() {
        let e = ev(
            EventKind::Access(AccessKind::Any),
            vec![PathBuf::from("/tmp/a.md")],
        );
        assert!(should_skip(&e));
    }

    #[test]
    fn should_skip_other_events() {
        let e = ev(EventKind::Other, vec![PathBuf::from("/tmp/a.md")]);
        assert!(should_skip(&e));
    }

    #[test]
    fn should_skip_dotfile_at_root() {
        let e = ev(modify_any(), vec![PathBuf::from("/tmp/repo/.DS_Store")]);
        assert!(should_skip(&e));
    }

    #[test]
    fn should_skip_dotdir_anywhere_in_path() {
        let e = ev(modify_any(), vec![PathBuf::from("/tmp/repo/.git/index")]);
        assert!(should_skip(&e));
    }

    #[test]
    fn should_skip_swap_and_tmp() {
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/a.swp")]
        )));
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/sub/b.swx")]
        )));
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/c.tmp")]
        )));
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/d.md~")]
        )));
    }

    /// P0 #1 fix: conflict-local backup は watcher で skip して mesh への
    /// 再 broadcast (= ピンポン loop) を抑止する。
    #[test]
    fn should_skip_conflict_local_backup() {
        // 通常 case (= `<orig>.conflict-local-<peer8>`)
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/notes.md.conflict-local-abcd1234")]
        )));
        // nested dir 配下
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/sub/entry.md.conflict-local-deadbeef")]
        )));
        // extension が無い original 名
        assert!(should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/Makefile.conflict-local-feedface")]
        )));
        // pattern 不一致 (= 単に "conflict" だけ含む name は skip しない)
        assert!(!should_skip(&ev(
            modify_any(),
            vec![PathBuf::from("/r/conflict-resolution.md")]
        )));
    }

    #[test]
    fn should_keep_plain_modify() {
        let e = ev(modify_any(), vec![PathBuf::from("/tmp/repo/foo.md")]);
        assert!(!should_skip(&e));
    }

    #[test]
    fn should_keep_create_under_subdir() {
        let e = ev(
            EventKind::Create(notify_debouncer_full::notify::event::CreateKind::File),
            vec![PathBuf::from("/tmp/repo/entities/foo.md")],
        );
        assert!(!should_skip(&e));
    }

    #[test]
    fn should_skip_under_root_ignores_root_dot_components() {
        // watched_dir 自体が `/var/folders/.tmpXXX/` のような dot 名でも、
        // 配下の通常 file (= `hello.md`) は skip されない。
        let root = PathBuf::from("/var/folders/.tmpXYZ");
        let e = ev(modify_any(), vec![PathBuf::from("/var/folders/.tmpXYZ/hello.md")]);
        assert!(!should_skip_under_root(&e, &root));
    }

    #[test]
    fn should_skip_under_root_still_skips_inner_dotfile() {
        // root 配下に dot 名があれば skip 対象 (= 通常通り)。
        let root = PathBuf::from("/tmp/repo");
        let e = ev(modify_any(), vec![PathBuf::from("/tmp/repo/.git/index")]);
        assert!(should_skip_under_root(&e, &root));
    }

    #[test]
    fn watcher_backend_default_is_recommended() {
        assert_eq!(WatcherBackend::default(), WatcherBackend::Recommended);
    }

    #[test]
    fn spawn_watcher_rejects_poll_backend() {
        let tmp = tempfile::TempDir::new().unwrap();
        let e = spawn_watcher(tmp.path(), WatcherBackend::Poll).unwrap_err();
        assert!(format!("{e:#}").contains("Poll"));
    }

    #[tokio::test]
    async fn spawn_watcher_emits_event_for_created_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (handle, mut rx) =
            spawn_watcher(tmp.path(), WatcherBackend::Recommended).unwrap();

        // canonicalize 後の path で event が来る (macOS では /private/... に解決される)。
        let file = tmp.path().join("hello.md");
        std::fs::write(&file, b"hi").unwrap();
        let canonical_file = file.canonicalize().unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        drop(handle);
        let ev = received
            .expect("watcher should emit event within 5s")
            .expect("channel not closed");
        assert!(
            ev.event
                .paths
                .iter()
                .any(|p| p == &file || p == &canonical_file),
            "expected event for {} (or canonical {}), got {:?}",
            file.display(),
            canonical_file.display(),
            ev.event.paths
        );
    }

    #[tokio::test]
    async fn watcher_loop_strips_watched_dir_prefix() {
        let watched = PathBuf::from("/tmp/repo");
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(ev(
            modify_any(),
            vec![PathBuf::from("/tmp/repo/sub/foo.md")],
        ))
        .unwrap();
        drop(tx);

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<PathBuf>::new()));
        let cap2 = captured.clone();
        watcher_loop(rx, watched, move |_ev, rel| {
            let cap2 = cap2.clone();
            async move {
                cap2.lock().unwrap().push(rel);
            }
        })
        .await
        .unwrap();

        assert_eq!(
            captured.lock().unwrap().as_slice(),
            &[PathBuf::from("sub/foo.md")]
        );
    }

    #[tokio::test]
    async fn watcher_loop_skips_path_outside_watched_dir() {
        let watched = PathBuf::from("/tmp/repo");
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(ev(
            modify_any(),
            vec![PathBuf::from("/elsewhere/foo.md")],
        ))
        .unwrap();
        drop(tx);

        let cap = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let cap2 = cap.clone();
        watcher_loop(rx, watched, move |_ev, _rel| {
            let cap2 = cap2.clone();
            async move {
                *cap2.lock().unwrap() += 1;
            }
        })
        .await
        .unwrap();

        assert_eq!(*cap.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn watcher_loop_skips_watched_dir_itself() {
        let watched = PathBuf::from("/tmp/repo");
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(ev(modify_any(), vec![PathBuf::from("/tmp/repo")]))
            .unwrap();
        drop(tx);

        let cap = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let cap2 = cap.clone();
        watcher_loop(rx, watched, move |_ev, _rel| {
            let cap2 = cap2.clone();
            async move {
                *cap2.lock().unwrap() += 1;
            }
        })
        .await
        .unwrap();

        assert_eq!(*cap.lock().unwrap(), 0);
    }
}
