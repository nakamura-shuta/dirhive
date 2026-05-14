//! 永続化 path 規約 (design.md §4.5)。
//!
//! 全 path は `$HOME` 経由で resolve。test override 用に env var を許す:
//! - `P2P_SYNC_STATE_DIR`: state dir prefix (default `~/.local/share/p2p-dir-sync`)
//! - `P2P_SYNC_CONFIG_DIR`: config dir prefix (default `~/.config/p2p-dir-sync`)
//! - `P2P_SYNC_LOG_DIR`: log dir prefix (default `~/Library/Logs`)

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

const STATE_DIR_ENV: &str = "P2P_SYNC_STATE_DIR";
const CONFIG_DIR_ENV: &str = "P2P_SYNC_CONFIG_DIR";
const LOG_DIR_ENV: &str = "P2P_SYNC_LOG_DIR";

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME env var not set"))
}

fn state_dir() -> Result<PathBuf> {
    if let Some(v) = std::env::var_os(STATE_DIR_ENV) {
        return Ok(PathBuf::from(v));
    }
    Ok(home_dir()?.join(".local/share/p2p-dir-sync"))
}

fn config_dir() -> Result<PathBuf> {
    if let Some(v) = std::env::var_os(CONFIG_DIR_ENV) {
        return Ok(PathBuf::from(v));
    }
    Ok(home_dir()?.join(".config/p2p-dir-sync"))
}

fn log_dir() -> Result<PathBuf> {
    if let Some(v) = std::env::var_os(LOG_DIR_ENV) {
        return Ok(PathBuf::from(v));
    }
    Ok(home_dir()?.join("Library/Logs"))
}

pub fn default_socket_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("daemon.sock"))
}

pub fn default_lock_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("daemon.lock"))
}

pub fn default_folder_secret_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("folder-secret.bin"))
}

pub fn default_blobs_dir() -> Result<PathBuf> {
    Ok(state_dir()?.join("blobs"))
}

pub fn default_pending_dir() -> Result<PathBuf> {
    Ok(state_dir()?.join("pending"))
}

pub fn default_allowlist_path() -> Result<PathBuf> {
    Ok(state_dir()?.join("allowlist.json"))
}

pub fn default_key_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("endpoint.key"))
}

pub fn default_log_path() -> Result<PathBuf> {
    Ok(log_dir()?.join("p2p-dir-sync.log"))
}

/// 親 dir を 0o700 で作成。既存なら mode を 0o700 に再強制する (= umask 非依存)。
pub fn ensure_dir_700(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("create_dir_all {}", dir.display()))?;
    let mut perm = std::fs::metadata(dir)
        .with_context(|| format!("metadata {}", dir.display()))?
        .permissions();
    perm.set_mode(0o700);
    std::fs::set_permissions(dir, perm)
        .with_context(|| format!("set_permissions 0o700 {}", dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;

    fn with_state_dir<F: FnOnce(&Path)>(f: F) {
        let tmp = TempDir::new().unwrap();
        // SAFETY: env var を一時的に上書きしてテスト → 直後に remove する。
        // 並行 test 間の干渉は #[serial(state_env)] で防ぐ。
        unsafe {
            std::env::set_var(STATE_DIR_ENV, tmp.path());
        }
        f(tmp.path());
        unsafe {
            std::env::remove_var(STATE_DIR_ENV);
        }
    }

    #[test]
    #[serial(state_env)]
    fn socket_path_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(default_socket_path().unwrap(), state.join("daemon.sock"));
        });
    }

    #[test]
    #[serial(state_env)]
    fn lock_path_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(default_lock_path().unwrap(), state.join("daemon.lock"));
        });
    }

    #[test]
    #[serial(state_env)]
    fn folder_secret_path_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(
                default_folder_secret_path().unwrap(),
                state.join("folder-secret.bin")
            );
        });
    }

    #[test]
    #[serial(state_env)]
    fn blobs_dir_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(default_blobs_dir().unwrap(), state.join("blobs"));
        });
    }

    #[test]
    #[serial(state_env)]
    fn pending_dir_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(default_pending_dir().unwrap(), state.join("pending"));
        });
    }

    #[test]
    #[serial(state_env)]
    fn allowlist_path_under_state_dir() {
        with_state_dir(|state| {
            assert_eq!(
                default_allowlist_path().unwrap(),
                state.join("allowlist.json")
            );
        });
    }

    #[test]
    fn ensure_dir_700_creates_and_sets_mode() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("nested/dir");
        ensure_dir_700(&target).unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn ensure_dir_700_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("d");
        ensure_dir_700(&target).unwrap();
        ensure_dir_700(&target).unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
}
