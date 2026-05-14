//! 同時編集の **conflict backup** ヘルパ。
//!
//! 受信 Upsert が local 既存と異なる内容を持ってきた場合、上書き前に local を
//! `<path>.conflict-local-<peer-shortid>` に rename 退避する。

use std::path::{Path, PathBuf};

use iroh::EndpointId;
use tokio::fs;

/// local file が `incoming_bytes` と異なる内容なら backup path を返す。
///
/// - local file 不在 → `None` (新規作成、conflict なし)
/// - local file == incoming → `None` (already in sync)
/// - local file != incoming → backup path
///
/// symlink 経由の脱出を防ぐため、`out_path` の親 dir 配下に backup を作る。
pub async fn compute_conflict_backup_path(
    out_path: &Path,
    incoming_bytes: &[u8],
    from_id: EndpointId,
) -> Option<PathBuf> {
    let local_bytes = match fs::read(out_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return None,
    };
    if local_bytes == incoming_bytes {
        return None;
    }
    let backup_name = match out_path.file_name() {
        Some(n) => format!(
            "{}.conflict-local-{}",
            n.to_string_lossy(),
            short_peer_id(from_id)
        ),
        None => return None,
    };
    Some(out_path.with_file_name(backup_name))
}

/// peer_id の先頭 8 hex 文字。conflict backup の filename に使う。
pub fn short_peer_id(id: EndpointId) -> String {
    id.to_string().chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_peer_id(byte: u8) -> EndpointId {
        let key = iroh::SecretKey::from_bytes(&[byte; 32]);
        key.public()
    }

    #[test]
    fn short_peer_id_is_8_chars() {
        let id = fixture_peer_id(0xab);
        let s = short_peer_id(id);
        assert_eq!(s.len(), 8);
        assert_eq!(s, id.to_string().chars().take(8).collect::<String>());
    }

    #[tokio::test]
    async fn backup_none_when_local_absent() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("absent.md");
        let r = compute_conflict_backup_path(&path, b"incoming", fixture_peer_id(1)).await;
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn backup_none_when_local_equals_incoming() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("same.md");
        tokio::fs::write(&path, b"hello").await.unwrap();
        let r = compute_conflict_backup_path(&path, b"hello", fixture_peer_id(2)).await;
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn backup_returns_path_when_diff() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested/sla.md");
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        tokio::fs::write(&path, b"local").await.unwrap();
        let peer = fixture_peer_id(0xcd);
        let r = compute_conflict_backup_path(&path, b"incoming", peer)
            .await
            .unwrap();
        let name = r.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with("sla.md.conflict-local-"));
        assert!(name.ends_with(&short_peer_id(peer)));
        assert_eq!(r.parent(), path.parent(), "親 dir 配下に置く");
    }
}
