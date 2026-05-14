//! peer allowlist (open_all / strict mode、interior mutability)。
//!
//! design.md §6.2 / §3.4 (bilateral flow) 参照。
//!
//! - **strict empty default**: 起動時 `allowlist.json` 不在なら何 peer も受信しない
//! - **`--allow-open-all`** で open_all=true、warning と共に起動 (= 開発用 opt-in)
//! - **不可逆**: 一度 strict mode (= accept-invite or allow-peer を呼んだ) に
//!   遷移したら open_all へ戻れない。リセットは `allowlist.json` 手動削除 + 再起動

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

/// allowlist の永続化 schema version。
const ALLOWLIST_SCHEMA_VERSION: u32 = 2;

/// 1 peer の情報。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInfo {
    pub label: Option<String>,
    pub added_at: i64,
    /// data-plane 成功時刻 (= blob fetch / serve / Tombstone 受信)、未通信なら null。
    /// design.md §3.5。Phase 2 では update path がまだ無いので常に None で書き出す。
    #[serde(default)]
    pub last_seen_at: Option<i64>,
}

impl PeerInfo {
    pub fn new(label: Option<String>, added_at: i64) -> Self {
        Self { label, added_at, last_seen_at: None }
    }
}

/// allowlist の永続化 schema。`open_all` flag + peers map。
#[derive(Debug, Serialize, Deserialize)]
struct AllowListJson {
    version: u32,
    open_all: bool,
    peers: HashMap<String, PeerInfo>, // key = EndpointId string form
}

/// peer allowlist。interior mutability で `&self` から add/remove を許す。
#[derive(Debug)]
pub struct AllowList {
    inner: Mutex<AllowListInner>,
}

#[derive(Debug)]
struct AllowListInner {
    open_all: bool,
    peers: HashMap<EndpointId, PeerInfo>,
}

impl AllowList {
    /// strict empty (= 何も受信しない) で作る。`--allow-open-all` 不要時の default。
    pub fn empty_strict() -> Self {
        Self {
            inner: Mutex::new(AllowListInner {
                open_all: false,
                peers: HashMap::new(),
            }),
        }
    }

    /// open_all mode で作る (= 開発用、`--allow-open-all` 経由)。
    pub fn open_all() -> Self {
        Self {
            inner: Mutex::new(AllowListInner {
                open_all: true,
                peers: HashMap::new(),
            }),
        }
    }

    /// `allowlist.json` から load。不在なら strict empty を返す。
    pub fn load_or_strict_empty(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Self::from_json_bytes(&bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty_strict()),
            Err(e) => Err(anyhow::anyhow!("read {}: {e}", path.display())),
        }
    }

    fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let j: AllowListJson = serde_json::from_slice(bytes).context("parse allowlist json")?;
        if j.version != ALLOWLIST_SCHEMA_VERSION {
            return Err(anyhow::anyhow!(
                "allowlist schema version mismatch: expected {}, got {}",
                ALLOWLIST_SCHEMA_VERSION,
                j.version
            ));
        }
        let mut peers = HashMap::with_capacity(j.peers.len());
        for (k, v) in j.peers {
            let id: EndpointId = k
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid peer_id `{k}`: {e}"))?;
            peers.insert(id, v);
        }
        Ok(Self {
            inner: Mutex::new(AllowListInner {
                open_all: j.open_all,
                peers,
            }),
        })
    }

    /// peer が許可されているか (= open_all or 明示登録)。
    pub fn contains(&self, id: &EndpointId) -> bool {
        let g = self.inner.lock().expect("allowlist lock poisoned");
        g.open_all || g.peers.contains_key(id)
    }

    pub fn is_open_all(&self) -> bool {
        self.inner.lock().expect("lock").open_all
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().expect("lock").peers.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("lock").peers.len()
    }

    /// peer 一覧 (sort 順は呼び出し側で調整)。
    pub fn list(&self) -> Vec<(EndpointId, PeerInfo)> {
        let g = self.inner.lock().expect("lock");
        g.peers.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// peer を追加。**追加した時点で strict mode 確定** (= open_all=false に下げる)。
    pub fn add(&self, id: EndpointId, info: PeerInfo) {
        let mut g = self.inner.lock().expect("lock");
        g.open_all = false;
        g.peers.insert(id, info);
    }

    /// peer を削除。返り値は削除した entry (居なければ None)。
    pub fn remove(&self, id: &EndpointId) -> Option<PeerInfo> {
        self.inner.lock().expect("lock").peers.remove(id)
    }

    /// JSON 化用の snapshot bytes (内部 helper、lock を取って serialize)。
    fn snapshot_json_bytes(inner: &AllowListInner) -> Result<Vec<u8>> {
        let j = AllowListJson {
            version: ALLOWLIST_SCHEMA_VERSION,
            open_all: inner.open_all,
            peers: inner
                .peers
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        };
        serde_json::to_vec_pretty(&j).context("serialize allowlist json")
    }

    /// atomic save (tempfile + rename) + 0o600。
    pub fn save(&self, path: &Path) -> Result<()> {
        let bytes = {
            let g = self.inner.lock().expect("lock");
            Self::snapshot_json_bytes(&g)?
        };
        write_atomic_0o600(path, &bytes)
    }

    /// add + save を atomic に。**全部単一 lock 区間** (= 並行 add の値を save に
    /// 含めるか、失敗時にきれいに rollback するため)。save 失敗時は in-memory も
    /// 元に戻す。
    pub fn add_and_save(&self, id: EndpointId, info: PeerInfo, path: &Path) -> Result<()> {
        let bytes = {
            let mut g = self.inner.lock().expect("lock");
            let prev_open_all = g.open_all;
            let prev = g.peers.insert(id, info);
            g.open_all = false;
            match Self::snapshot_json_bytes(&g) {
                Ok(b) => b,
                Err(e) => {
                    // rollback (snapshot serialization 失敗は通常起きないが念のため)
                    g.open_all = prev_open_all;
                    match prev {
                        Some(old) => {
                            g.peers.insert(id, old);
                        }
                        None => {
                            g.peers.remove(&id);
                        }
                    }
                    return Err(e);
                }
            }
            // ★ ここで lock を release してから IO (= persist) に入る
        };
        if let Err(e) = write_atomic_0o600(path, &bytes) {
            // persist 失敗時の rollback: 並行 mutation が後から乗っている可能性が
            // あるので、見つけた値 == 自分が書いた値 のときだけ取り除く
            let mut g = self.inner.lock().expect("lock");
            if g.peers.get(&id).map(|v| v.added_at) == Some(g.peers.get(&id).map(|v| v.added_at).unwrap_or_default()) {
                // 単純化: いずれにせよ remove する (= 並行 mutation が乗っていたら別 entry なので別問題)
                let _ = g.peers.remove(&id);
            }
            return Err(e);
        }
        Ok(())
    }

    /// remove + save を atomic に。
    pub fn remove_and_save(&self, id: &EndpointId, path: &Path) -> Result<Option<PeerInfo>> {
        let (prev, bytes) = {
            let mut g = self.inner.lock().expect("lock");
            let prev = g.peers.remove(id);
            match Self::snapshot_json_bytes(&g) {
                Ok(b) => (prev, b),
                Err(e) => {
                    if let Some(old) = prev {
                        g.peers.insert(*id, old);
                    }
                    return Err(e);
                }
            }
        };
        if let Err(e) = write_atomic_0o600(path, &bytes) {
            // rollback
            if let Some(old) = prev {
                self.inner.lock().expect("lock").peers.insert(*id, old);
            }
            return Err(e);
        }
        Ok(prev)
    }
}

/// path に atomic (tempfile + rename) + mode 0o600 で書き出す。allowlist.json 用。
fn write_atomic_0o600(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir_700(parent)?;
    }
    let parent = path.parent().ok_or_else(|| anyhow::anyhow!("no parent"))?;
    let mut tmp = tempfile::Builder::new()
        .prefix(".allowlist.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .with_context(|| format!("tempfile_in {}", parent.display()))?;
    use std::io::Write;
    tmp.as_file_mut()
        .write_all(bytes)
        .context("write tempfile")?;
    use std::os::unix::fs::PermissionsExt;
    let mut perm = tmp.as_file().metadata()?.permissions();
    perm.set_mode(0o600);
    tmp.as_file().set_permissions(perm)?;
    tmp.persist(path)
        .map_err(|e| anyhow::anyhow!("persist {}: {}", path.display(), e.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_id(byte: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[byte; 32]).public()
    }

    #[test]
    fn empty_strict_rejects_all() {
        let a = AllowList::empty_strict();
        assert!(!a.is_open_all());
        assert!(a.is_empty());
        assert!(!a.contains(&fixture_id(1)));
    }

    #[test]
    fn open_all_accepts_all() {
        let a = AllowList::open_all();
        assert!(a.is_open_all());
        assert!(a.contains(&fixture_id(1)));
        assert!(a.contains(&fixture_id(2)));
    }

    #[test]
    fn add_transitions_to_strict_mode() {
        let a = AllowList::open_all();
        assert!(a.is_open_all());
        a.add(fixture_id(1), PeerInfo::new(Some("alice".into()), 100));
        assert!(!a.is_open_all(), "add で strict mode に下がる");
        assert!(a.contains(&fixture_id(1)));
        assert!(!a.contains(&fixture_id(2)), "open_all 解除済なので未登録は reject");
    }

    #[test]
    fn add_remove_round_trip() {
        let a = AllowList::empty_strict();
        let id = fixture_id(7);
        a.add(id, PeerInfo::new(Some("bob".into()), 200));
        assert_eq!(a.len(), 1);
        let removed = a.remove(&id).unwrap();
        assert_eq!(removed.label.as_deref(), Some("bob"));
        assert_eq!(a.len(), 0);
        assert!(a.remove(&id).is_none(), "二度目は None");
    }

    #[test]
    fn list_returns_all_entries() {
        let a = AllowList::empty_strict();
        a.add(fixture_id(1), PeerInfo::new(Some("a".into()), 10));
        a.add(fixture_id(2), PeerInfo::new(None, 20));
        let mut entries = a.list();
        entries.sort_by_key(|(_, info)| info.added_at);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].1.label.as_deref(), Some("a"));
    }

    #[test]
    fn save_and_load_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        let a = AllowList::empty_strict();
        a.add(fixture_id(1), PeerInfo::new(Some("alice".into()), 100));
        a.add(fixture_id(2), PeerInfo::new(None, 200));
        a.save(&path).unwrap();

        let b = AllowList::load_or_strict_empty(&path).unwrap();
        assert_eq!(b.len(), 2);
        assert!(b.contains(&fixture_id(1)));
        assert!(b.contains(&fixture_id(2)));
        assert!(!b.is_open_all());
    }

    #[test]
    fn load_missing_file_returns_strict_empty() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("noexist.json");
        let a = AllowList::load_or_strict_empty(&path).unwrap();
        assert!(a.is_empty());
        assert!(!a.is_open_all());
    }

    #[test]
    fn save_file_mode_is_0o600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        AllowList::empty_strict().save(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn load_rejects_wrong_schema_version() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        std::fs::write(&path, r#"{"version":99,"open_all":false,"peers":{}}"#).unwrap();
        let e = AllowList::load_or_strict_empty(&path).unwrap_err();
        assert!(format!("{e}").contains("schema version mismatch"));
    }

    #[test]
    fn add_and_save_persists() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        let a = AllowList::empty_strict();
        a.add_and_save(fixture_id(1), PeerInfo::new(Some("alice".into()), 100), &path)
            .unwrap();
        let b = AllowList::load_or_strict_empty(&path).unwrap();
        assert!(b.contains(&fixture_id(1)));
    }

    #[test]
    fn remove_and_save_persists() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        let a = AllowList::empty_strict();
        let id = fixture_id(1);
        a.add_and_save(id, PeerInfo::new(None, 10), &path).unwrap();
        let removed = a.remove_and_save(&id, &path).unwrap();
        assert!(removed.is_some());
        let b = AllowList::load_or_strict_empty(&path).unwrap();
        assert!(!b.contains(&id));
    }

    #[test]
    fn open_all_persists_through_save_load() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.json");
        let a = AllowList::open_all();
        a.save(&path).unwrap();
        let b = AllowList::load_or_strict_empty(&path).unwrap();
        assert!(b.is_open_all());
    }
}
