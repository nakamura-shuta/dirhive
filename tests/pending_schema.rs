//! `docs/schema/pending-entry.v1.{upsert,tombstone}.json` を fixture として読み、
//! `PendingEntry` enum で deserialize できることを確認する drift 防止 test。
//! design.md §11 acceptance gate (Phase 2 完了条件)。

use dirhive::pending_log::PendingEntry;

const UPSERT_FIXTURE: &str = include_str!("../docs/schema/pending-entry.v1.upsert.json");
const TOMBSTONE_FIXTURE: &str = include_str!("../docs/schema/pending-entry.v1.tombstone.json");

#[test]
fn upsert_fixture_deserializes() {
    let e: PendingEntry = serde_json::from_str(UPSERT_FIXTURE)
        .expect("upsert fixture must deserialize to PendingEntry");
    match e {
        PendingEntry::Upsert {
            schema_version,
            rel_path,
            received_at,
            source_peer,
            blob_hash,
            bytes,
        } => {
            assert_eq!(schema_version, 1);
            assert_eq!(rel_path, "entities/foo.md");
            assert_eq!(received_at, 1715600000);
            assert_eq!(source_peer, "abcd1234efgh5678ijkl9012mnop3456qrst7890");
            assert_eq!(blob_hash, "blake3:e0a1b2c3d4e5f607");
            assert_eq!(bytes, 4096);
        }
        _ => panic!("expected Upsert"),
    }
}

#[test]
fn tombstone_fixture_deserializes() {
    let e: PendingEntry = serde_json::from_str(TOMBSTONE_FIXTURE)
        .expect("tombstone fixture must deserialize to PendingEntry");
    match e {
        PendingEntry::Tombstone {
            schema_version,
            rel_path,
            received_at,
            source_peer,
        } => {
            assert_eq!(schema_version, 1);
            assert_eq!(rel_path, "entities/old.md");
            assert_eq!(received_at, 1715600100);
            assert_eq!(source_peer, "abcd1234efgh5678ijkl9012mnop3456qrst7890");
        }
        _ => panic!("expected Tombstone"),
    }
}

#[test]
fn upsert_fixture_round_trip() {
    let e: PendingEntry = serde_json::from_str(UPSERT_FIXTURE).unwrap();
    let s = serde_json::to_string(&e).unwrap();
    let e2: PendingEntry = serde_json::from_str(&s).unwrap();
    assert_eq!(e, e2);
}

#[test]
fn tombstone_fixture_round_trip() {
    let e: PendingEntry = serde_json::from_str(TOMBSTONE_FIXTURE).unwrap();
    let s = serde_json::to_string(&e).unwrap();
    let e2: PendingEntry = serde_json::from_str(&s).unwrap();
    assert_eq!(e, e2);
}
