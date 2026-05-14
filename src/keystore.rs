//! 永続 secret の load / generate (= endpoint.key + folder-secret.bin)。
//!
//! - `endpoint.key`: 32 byte Ed25519 secret。**初回起動時に generate**、以降は load
//! - `folder-secret.bin`: 16 byte folder/group secret。**lazy generate**:
//!   起動時には何もせず、`sync.invite` (新規生成) か `sync.accept-invite` (adopt) で
//!   作る (design.md §4.3、H1 反映)。

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use iroh::SecretKey;
use rand::Rng;

const KEY_BYTES: usize = 32;
const FOLDER_SECRET_BYTES: usize = 16;

/// `key_path` から load。不在なら generate + 0o600 で persist。
pub fn load_or_create_endpoint_key(key_path: &Path) -> Result<SecretKey> {
    match std::fs::read(key_path) {
        Ok(bytes) => {
            if bytes.len() != KEY_BYTES {
                return Err(anyhow!(
                    "{} has invalid size: {} bytes (expected {})",
                    key_path.display(),
                    bytes.len(),
                    KEY_BYTES
                ));
            }
            let arr: [u8; KEY_BYTES] = bytes
                .as_slice()
                .try_into()
                .expect("size already checked");
            Ok(SecretKey::from_bytes(&arr))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            generate_and_persist_key(key_path)
        }
        Err(e) => Err(anyhow!("read {}: {e}", key_path.display())),
    }
}

fn generate_and_persist_key(key_path: &Path) -> Result<SecretKey> {
    if let Some(parent) = key_path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let mut bytes = [0u8; KEY_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    atomic_write_0o600(key_path, &bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}

/// folder-secret.bin を load する。**不在なら `None` を返す** (= lazy generate、
/// daemon 起動時には何もしない)。
pub fn try_load_folder_secret(path: &Path) -> Result<Option<[u8; FOLDER_SECRET_BYTES]>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            if bytes.len() != FOLDER_SECRET_BYTES {
                return Err(anyhow!(
                    "{} has invalid size: {} bytes (expected {})",
                    path.display(),
                    bytes.len(),
                    FOLDER_SECRET_BYTES
                ));
            }
            let arr: [u8; FOLDER_SECRET_BYTES] = bytes
                .as_slice()
                .try_into()
                .expect("size already checked");
            Ok(Some(arr))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow!("read {}: {e}", path.display())),
    }
}

/// folder-secret.bin を新規生成 + persist。`sync.invite` の founder path で呼ぶ。
pub fn generate_and_persist_folder_secret(path: &Path) -> Result<[u8; FOLDER_SECRET_BYTES]> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let mut bytes = [0u8; FOLDER_SECRET_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    atomic_write_0o600(path, &bytes)?;
    Ok(bytes)
}

/// 受け取った folder_secret を persist。`sync.accept-invite` の joiner path で呼ぶ。
pub fn persist_folder_secret(
    path: &Path,
    folder_secret: &[u8; FOLDER_SECRET_BYTES],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    atomic_write_0o600(path, folder_secret)?;
    Ok(())
}

/// 同 dir 内 tempfile + rename で atomic write。mode 0o600 を最終 file に保証。
fn atomic_write_0o600(target: &Path, content: &[u8]) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("target {} has no parent", target.display()))?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".secret.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .with_context(|| format!("tempfile_in {}", parent.display()))?;
    use std::io::Write;
    tmp.as_file_mut()
        .write_all(content)
        .context("write tempfile")?;
    // mode を 0o600 に
    let mut perm = tmp
        .as_file()
        .metadata()
        .context("tempfile metadata")?
        .permissions();
    perm.set_mode(0o600);
    tmp.as_file().set_permissions(perm).context("chmod 0o600")?;
    tmp.persist(target)
        .map_err(|e| anyhow!("persist {}: {}", target.display(), e.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ---------- endpoint.key ----------

    #[test]
    fn endpoint_key_generates_on_first_load() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested/endpoint.key");
        let k1 = load_or_create_endpoint_key(&path).unwrap();
        assert!(path.exists());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        // 2 回目は同じ key を返す (= persist 済を load)
        let k2 = load_or_create_endpoint_key(&path).unwrap();
        assert_eq!(k1.public(), k2.public());
    }

    #[test]
    fn endpoint_key_rejects_invalid_size() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("endpoint.key");
        std::fs::write(&path, vec![0u8; 16]).unwrap();
        let e = load_or_create_endpoint_key(&path).unwrap_err();
        assert!(format!("{e}").contains("invalid size"));
    }

    // ---------- folder-secret.bin ----------

    #[test]
    fn folder_secret_absent_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("folder-secret.bin");
        assert_eq!(try_load_folder_secret(&path).unwrap(), None);
    }

    #[test]
    fn folder_secret_generate_persists_and_loads() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("folder-secret.bin");
        let s1 = generate_and_persist_folder_secret(&path).unwrap();
        assert_eq!(s1.len(), 16);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let s2 = try_load_folder_secret(&path).unwrap().unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn folder_secret_persist_overwrites() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("folder-secret.bin");
        persist_folder_secret(&path, &[0xaa; 16]).unwrap();
        assert_eq!(try_load_folder_secret(&path).unwrap().unwrap(), [0xaa; 16]);
        persist_folder_secret(&path, &[0xbb; 16]).unwrap();
        assert_eq!(try_load_folder_secret(&path).unwrap().unwrap(), [0xbb; 16]);
    }

    #[test]
    fn folder_secret_rejects_invalid_size() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("folder-secret.bin");
        std::fs::write(&path, vec![0u8; 8]).unwrap();
        let e = try_load_folder_secret(&path).unwrap_err();
        assert!(format!("{e}").contains("invalid size"));
    }

    #[test]
    fn generate_creates_unique_secrets() {
        let tmp = TempDir::new().unwrap();
        let p1 = tmp.path().join("a/folder-secret.bin");
        let p2 = tmp.path().join("b/folder-secret.bin");
        let s1 = generate_and_persist_folder_secret(&p1).unwrap();
        let s2 = generate_and_persist_folder_secret(&p2).unwrap();
        assert_ne!(s1, s2, "16 byte entropy で衝突しない");
    }
}
