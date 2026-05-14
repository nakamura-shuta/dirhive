//! gossip subscribe 時に渡す bootstrap peer 一覧 (= Phase 3 review H1)。
//!
//! design v8 では「 invite/accept-invite 経由で対向 peer の EndpointAddr を
//! 知る 」 設計で、 起動時に何の bootstrap 情報も無いと `gossip.subscribe(topic,
//! vec![])` 渡しになり、 mesh discovery が成立しない。
//!
//! 本 module は `bootstrap-peers.json` (= `state_dir/bootstrap-peers.json`) に
//! `Vec<EndpointAddr>` を atomic write で保存し、 daemon 起動時に load して
//! gossip.subscribe へ渡す。
//!
//! 注: bootstrap_peer set は **永続 lower bound** であって、 一度 mesh に
//! 参加すれば gossip 自身が membership を維持するので bootstrap は dead でも
//! 問題ない (= 既存 peer 経由で他 peer を再 discover する)。

use std::path::Path;

use anyhow::{Context, Result};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct BootstrapPeersJson {
    version: u32,
    peers: Vec<EndpointAddr>,
}

/// `path` から bootstrap peer 一覧を load。 不在なら空 Vec。
/// schema_version mismatch / 不正 JSON は error。
pub fn load_bootstrap_peers(path: &Path) -> Result<Vec<EndpointAddr>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let j: BootstrapPeersJson = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", path.display()))?;
            if j.version != SCHEMA_VERSION {
                anyhow::bail!(
                    "bootstrap-peers schema version mismatch: expected {}, got {}",
                    SCHEMA_VERSION,
                    j.version
                );
            }
            Ok(j.peers)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(anyhow::anyhow!("read {}: {e}", path.display())),
    }
}

/// `path` に atomic write (tempfile + rename) で保存。 0o600 で chmod する。
pub fn save_bootstrap_peers(path: &Path, peers: &[EndpointAddr]) -> Result<()> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let parent = path.parent().ok_or_else(|| anyhow::anyhow!("no parent"))?;
    let j = BootstrapPeersJson {
        version: SCHEMA_VERSION,
        peers: peers.to_vec(),
    };
    let bytes = serde_json::to_vec_pretty(&j).context("serialize bootstrap-peers json")?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".bootstrap-peers.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .with_context(|| format!("tempfile_in {}", parent.display()))?;
    use std::io::Write;
    tmp.as_file_mut()
        .write_all(&bytes)
        .context("write tempfile")?;
    use std::os::unix::fs::PermissionsExt;
    let mut perm = tmp.as_file().metadata()?.permissions();
    perm.set_mode(0o600);
    tmp.as_file().set_permissions(perm)?;
    tmp.persist(path)
        .map_err(|e| anyhow::anyhow!("persist {}: {}", path.display(), e.error))?;
    Ok(())
}

/// `path` の bootstrap peer list に 1 件 add する。 同 EndpointId の peer が
/// 既存ならその entry を新 addr で **置換** (= 古い relay-only entry を新 addr
/// に更新)。 新規なら append。
pub fn add_or_replace(path: &Path, new_addr: EndpointAddr) -> Result<()> {
    let mut peers = load_bootstrap_peers(path)?;
    if let Some(existing) = peers.iter_mut().find(|p| p.id == new_addr.id) {
        *existing = new_addr;
    } else {
        peers.push(new_addr);
    }
    save_bootstrap_peers(path, &peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use tempfile::TempDir;

    fn fixture_addr(byte: u8) -> EndpointAddr {
        let id = SecretKey::from_bytes(&[byte; 32]).public();
        EndpointAddr::new(id)
    }

    #[test]
    fn load_missing_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("nope.json");
        assert!(load_bootstrap_peers(&p).unwrap().is_empty());
    }

    #[test]
    fn save_then_load_round_trip() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("bootstrap-peers.json");
        let peers = vec![fixture_addr(1), fixture_addr(2)];
        save_bootstrap_peers(&p, &peers).unwrap();
        let got = load_bootstrap_peers(&p).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, peers[0].id);
        assert_eq!(got[1].id, peers[1].id);
    }

    #[test]
    fn save_file_mode_is_0o600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("bootstrap-peers.json");
        save_bootstrap_peers(&p, &[]).unwrap();
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn add_or_replace_appends_new_peer() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("bootstrap-peers.json");
        add_or_replace(&p, fixture_addr(1)).unwrap();
        add_or_replace(&p, fixture_addr(2)).unwrap();
        let got = load_bootstrap_peers(&p).unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn add_or_replace_updates_existing_id() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("bootstrap-peers.json");
        let id1 = fixture_addr(1);
        add_or_replace(&p, id1.clone()).unwrap();
        // 同 id で再 add → 置換のみ、 list 長は 1
        add_or_replace(&p, id1.clone()).unwrap();
        let got = load_bootstrap_peers(&p).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, id1.id);
    }

    #[test]
    fn load_rejects_wrong_schema_version() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("bad.json");
        std::fs::write(&p, r#"{"version":42,"peers":[]}"#).unwrap();
        let e = load_bootstrap_peers(&p).unwrap_err();
        assert!(format!("{e:#}").contains("schema version mismatch"));
    }
}
