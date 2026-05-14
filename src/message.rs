//! wire format: `SyncUpdate` (gossip 上を流れる) + `InviteTicket` (invite 配布)。
//!
//! design.md §4.1-§4.2 参照。

use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64;
use iroh::{EndpointAddr, EndpointId};
use iroh_blobs::{BlobFormat, Hash};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use serde::{Deserialize, Serialize};

/// 現在の `SyncUpdate` wire schema version。
pub const SYNC_UPDATE_VERSION: u32 = 2;

/// `InviteTicket` envelope の prefix。
pub const INVITE_PREFIX: &str = "p2psync1-";

/// gossip 上で流す change message。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncUpdate {
    pub version: u32,
    pub body: SyncUpdateBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncUpdateBody {
    Upsert {
        path: String,
        hash: Hash,
        format: BlobFormat,
        from: PeerRef,
    },
    Tombstone {
        path: String,
        from: PeerRef,
    },
}

/// gossip message の sender 識別 (= EndpointAddr の最小 form)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerRef {
    pub id: EndpointId,
}

impl PeerRef {
    pub fn from_addr(addr: &EndpointAddr) -> Self {
        Self { id: addr.id }
    }
}

impl SyncUpdate {
    pub fn upsert(path: String, hash: Hash, format: BlobFormat, from: PeerRef) -> Result<Self> {
        validate_relative_path(&path)?;
        Ok(Self {
            version: SYNC_UPDATE_VERSION,
            body: SyncUpdateBody::Upsert { path, hash, format, from },
        })
    }

    pub fn tombstone(path: String, from: PeerRef) -> Result<Self> {
        validate_relative_path(&path)?;
        Ok(Self {
            version: SYNC_UPDATE_VERSION,
            body: SyncUpdateBody::Tombstone { path, from },
        })
    }

    pub fn path(&self) -> &str {
        match &self.body {
            SyncUpdateBody::Upsert { path, .. } => path,
            SyncUpdateBody::Tombstone { path, .. } => path,
        }
    }

    pub fn from(&self) -> &PeerRef {
        match &self.body {
            SyncUpdateBody::Upsert { from, .. } => from,
            SyncUpdateBody::Tombstone { from, .. } => from,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context("serialize SyncUpdate")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let v: Self = serde_json::from_slice(bytes).context("deserialize SyncUpdate")?;
        if v.version != SYNC_UPDATE_VERSION {
            return Err(anyhow!(
                "SyncUpdate version mismatch: expected {}, got {}",
                SYNC_UPDATE_VERSION,
                v.version
            ));
        }
        validate_relative_path(v.path())?;
        Ok(v)
    }
}

/// rel_path を validate。`..` / absolute / backslash / 空 component を拒否。
pub fn validate_relative_path(p: &str) -> Result<()> {
    if p.is_empty() {
        return Err(anyhow!("rel_path is empty"));
    }
    if p.starts_with('/') {
        return Err(anyhow!("absolute path not allowed: {p}"));
    }
    if p.contains('\\') {
        return Err(anyhow!("backslash not allowed in rel_path: {p}"));
    }
    for comp in p.split('/') {
        if comp.is_empty() {
            return Err(anyhow!("empty path component in: {p}"));
        }
        if comp == ".." || comp == "." {
            return Err(anyhow!("`..` / `.` not allowed in rel_path: {p}"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// InviteTicket: EndpointTicket + folder_secret 16 bytes を `p2psync1-<base64url>`
// envelope で wrap する。base64url + no-pad (= URL / messenger 安全)。
// ---------------------------------------------------------------------------

/// invite ticket。peer 間で out-of-band で渡す。folder_secret = group identity。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InviteTicket {
    pub endpoint: EndpointTicket,
    pub folder_secret: [u8; 16],
}

/// JSON serializable な内部 form (= EndpointTicket は文字列に降ろす)。
#[derive(Serialize, Deserialize)]
struct InviteTicketJson {
    endpoint_ticket: String,
    folder_secret: String, // base64url-no-pad encoded [u8; 16]
}

impl InviteTicket {
    pub fn new(endpoint: EndpointTicket, folder_secret: [u8; 16]) -> Self {
        Self { endpoint, folder_secret }
    }

    /// `p2psync1-<base64url>` envelope に encode。
    pub fn encode(&self) -> Result<String> {
        let json = InviteTicketJson {
            endpoint_ticket: Ticket::encode_string(&self.endpoint),
            folder_secret: BASE64.encode(self.folder_secret),
        };
        let bytes = serde_json::to_vec(&json).context("serialize InviteTicket")?;
        Ok(format!("{}{}", INVITE_PREFIX, BASE64.encode(bytes)))
    }

    /// `p2psync1-<base64url>` envelope から decode。
    pub fn decode(s: &str) -> Result<Self> {
        let payload = s
            .strip_prefix(INVITE_PREFIX)
            .ok_or_else(|| anyhow!("missing `{}` prefix", INVITE_PREFIX))?;
        let bytes = BASE64
            .decode(payload)
            .context("base64url decode InviteTicket")?;
        let json: InviteTicketJson =
            serde_json::from_slice(&bytes).context("deserialize InviteTicket json")?;
        let endpoint = EndpointTicket::from_str(&json.endpoint_ticket)
            .map_err(|e| anyhow!("invalid endpoint_ticket: {e}"))?;
        let secret_bytes = BASE64
            .decode(&json.folder_secret)
            .context("base64url decode folder_secret")?;
        if secret_bytes.len() != 16 {
            return Err(anyhow!(
                "folder_secret must be 16 bytes, got {}",
                secret_bytes.len()
            ));
        }
        let mut folder_secret = [0u8; 16];
        folder_secret.copy_from_slice(&secret_bytes);
        Ok(Self { endpoint, folder_secret })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_peer_addr(byte: u8) -> EndpointAddr {
        let id = iroh::SecretKey::from_bytes(&[byte; 32]).public();
        EndpointAddr::new(id)
    }

    fn fixture_hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    // ---------- validate_relative_path ----------

    #[test]
    fn validate_accepts_relative() {
        assert!(validate_relative_path("foo.md").is_ok());
        assert!(validate_relative_path("entities/sla.md").is_ok());
        assert!(validate_relative_path("a/b/c/d.md").is_ok());
    }

    #[test]
    fn validate_rejects_empty() {
        assert!(validate_relative_path("").is_err());
    }

    #[test]
    fn validate_rejects_absolute() {
        assert!(validate_relative_path("/etc/passwd").is_err());
    }

    #[test]
    fn validate_rejects_dotdot() {
        assert!(validate_relative_path("../escape").is_err());
        assert!(validate_relative_path("a/../b").is_err());
    }

    #[test]
    fn validate_rejects_backslash() {
        assert!(validate_relative_path("a\\b").is_err());
    }

    #[test]
    fn validate_rejects_empty_component() {
        assert!(validate_relative_path("a//b").is_err());
    }

    // ---------- SyncUpdate ----------

    #[test]
    fn sync_update_upsert_round_trip() {
        let addr = fixture_peer_addr(1);
        let from = PeerRef::from_addr(&addr);
        let m = SyncUpdate::upsert("entities/sla.md".into(), fixture_hash(0xaa), BlobFormat::Raw, from.clone())
            .unwrap();
        let bytes = m.to_bytes().unwrap();
        let m2 = SyncUpdate::from_bytes(&bytes).unwrap();
        assert_eq!(m, m2);
        assert_eq!(m2.path(), "entities/sla.md");
        assert_eq!(m2.from(), &from);
    }

    #[test]
    fn sync_update_tombstone_round_trip() {
        let addr = fixture_peer_addr(2);
        let from = PeerRef::from_addr(&addr);
        let m = SyncUpdate::tombstone("entities/old.md".into(), from).unwrap();
        let bytes = m.to_bytes().unwrap();
        let m2 = SyncUpdate::from_bytes(&bytes).unwrap();
        assert_eq!(m, m2);
        match m2.body {
            SyncUpdateBody::Tombstone { path, .. } => assert_eq!(path, "entities/old.md"),
            _ => panic!("expected Tombstone"),
        }
    }

    #[test]
    fn sync_update_rejects_invalid_path() {
        let addr = fixture_peer_addr(3);
        let from = PeerRef::from_addr(&addr);
        assert!(SyncUpdate::upsert("../escape".into(), fixture_hash(1), BlobFormat::Raw, from.clone()).is_err());
        assert!(SyncUpdate::tombstone("/abs".into(), from).is_err());
    }

    #[test]
    fn sync_update_rejects_wrong_version() {
        let addr = fixture_peer_addr(4);
        let from = PeerRef::from_addr(&addr);
        let mut m = SyncUpdate::upsert("foo.md".into(), fixture_hash(1), BlobFormat::Raw, from).unwrap();
        m.version = 99;
        let bytes = m.to_bytes().unwrap();
        let e = SyncUpdate::from_bytes(&bytes).unwrap_err();
        assert!(format!("{e}").contains("version mismatch"));
    }

    // ---------- InviteTicket ----------

    fn fixture_endpoint_ticket(byte: u8) -> EndpointTicket {
        let addr = fixture_peer_addr(byte);
        EndpointTicket::new(addr)
    }

    #[test]
    fn invite_ticket_round_trip() {
        let t = InviteTicket::new(fixture_endpoint_ticket(7), [42u8; 16]);
        let encoded = t.encode().unwrap();
        assert!(encoded.starts_with(INVITE_PREFIX));
        let decoded = InviteTicket::decode(&encoded).unwrap();
        assert_eq!(decoded, t);
    }

    #[test]
    fn invite_ticket_rejects_missing_prefix() {
        let t = InviteTicket::new(fixture_endpoint_ticket(8), [0u8; 16]);
        let raw = t.encode().unwrap();
        let stripped = raw.trim_start_matches(INVITE_PREFIX);
        let e = InviteTicket::decode(stripped).unwrap_err();
        assert!(format!("{e}").contains("missing"));
    }

    #[test]
    fn invite_ticket_rejects_invalid_base64() {
        let e = InviteTicket::decode("p2psync1-!!!!!").unwrap_err();
        assert!(format!("{e}").contains("base64url"));
    }
}
