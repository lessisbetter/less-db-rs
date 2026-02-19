//! Tests for SqliteBackend — port of the JS sqlite-adapter integration tests.

use less_db::index::types::{
    ComputedIndex, FieldIndex, IndexDefinition, IndexField, IndexScan, IndexScanType,
    IndexSortOrder, IndexableValue, RangeBound,
};
use less_db::storage::sqlite::SqliteBackend;
use less_db::storage::traits::StorageBackend;
use less_db::types::{PurgeTombstonesOptions, ScanOptions, SerializedRecord};
use serde_json::json;
use std::sync::Arc;

// ============================================================================
// Test helpers
// ============================================================================

/// Build an initialized in-memory `SqliteBackend` with no collections.
fn make_backend() -> SqliteBackend {
    let mut backend = SqliteBackend::open_in_memory().expect("open in-memory DB");
    backend.initialize(&[]).expect("initialize");
    backend
}

/// Create a minimal `SerializedRecord` for testing.
fn make_record(id: &str, collection: &str) -> SerializedRecord {
    SerializedRecord {
        id: id.to_string(),
        collection: collection.to_string(),
        version: 1,
        data: json!({ "name": id }),
        crdt: vec![],
        pending_patches: vec![],
        sequence: -1,
        dirty: false,
        deleted: false,
        deleted_at: None,
        meta: None,
        computed: None,
    }
}

/// Build a `FieldIndex` with a single field.
fn field_index_single(name: &str, field: &str, unique: bool) -> IndexDefinition {
    IndexDefinition::Field(FieldIndex {
        name: name.to_string(),
        fields: vec![IndexField {
            field: field.to_string(),
            order: IndexSortOrder::Asc,
        }],
        unique,
        sparse: false,
    })
}

/// Build an `IndexScan` for an exact match on a `FieldIndex`.
fn exact_field_scan(index: IndexDefinition, value: IndexableValue) -> IndexScan {
    IndexScan {
        scan_type: IndexScanType::Exact,
        index,
        equality_values: Some(vec![value]),
        range_lower: None,
        range_upper: None,
        in_values: None,
        direction: IndexSortOrder::Asc,
    }
}

// ============================================================================
// get_raw
// ============================================================================

#[test]
fn get_raw_returns_none_for_missing_record() {
    let backend = make_backend();
    let result = backend.get_raw("users", "nonexistent").unwrap();
    assert!(result.is_none());
}

// ============================================================================
// put_raw / get_raw round-trip
// ============================================================================

#[test]
fn put_raw_then_get_raw_round_trips() {
    let backend = make_backend();
    let record = SerializedRecord {
        id: "user-1".to_string(),
        collection: "users".to_string(),
        version: 2,
        data: json!({ "name": "Alice", "email": "alice@example.com" }),
        crdt: vec![1, 2, 3],
        pending_patches: vec![4, 5],
        sequence: 42,
        dirty: true,
        deleted: false,
        deleted_at: None,
        meta: Some(json!({ "source": "test" })),
        computed: Some(json!({ "emailLower": "alice@example.com" })),
    };

    backend.put_raw(&record).unwrap();

    let fetched = backend.get_raw("users", "user-1").unwrap().unwrap();
    assert_eq!(fetched.id, "user-1");
    assert_eq!(fetched.collection, "users");
    assert_eq!(fetched.version, 2);
    assert_eq!(fetched.data, json!({ "name": "Alice", "email": "alice@example.com" }));
    assert_eq!(fetched.crdt, vec![1, 2, 3]);
    assert_eq!(fetched.pending_patches, vec![4, 5]);
    assert_eq!(fetched.sequence, 42);
    assert!(fetched.dirty);
    assert!(!fetched.deleted);
    assert_eq!(fetched.meta, Some(json!({ "source": "test" })));
    assert_eq!(
        fetched.computed,
        Some(json!({ "emailLower": "alice@example.com" }))
    );
}

#[test]
fn put_raw_overwrites_existing_record() {
    let backend = make_backend();
    let r1 = make_record("user-1", "users");
    backend.put_raw(&r1).unwrap();

    let r2 = SerializedRecord {
        version: 3,
        data: json!({ "name": "Updated" }),
        ..r1
    };
    backend.put_raw(&r2).unwrap();

    let fetched = backend.get_raw("users", "user-1").unwrap().unwrap();
    assert_eq!(fetched.version, 3);
    assert_eq!(fetched.data["name"], "Updated");
}

// ============================================================================
// scan_raw
// ============================================================================

#[test]
fn scan_raw_returns_all_live_records() {
    let backend = make_backend();
    for i in 0..3 {
        backend.put_raw(&make_record(&format!("r{i}"), "col")).unwrap();
    }
    let result = backend
        .scan_raw("col", &ScanOptions::default())
        .unwrap();
    assert_eq!(result.records.len(), 3);
}

#[test]
fn scan_raw_skips_tombstones_by_default() {
    let backend = make_backend();
    let mut r = make_record("alive", "col");
    backend.put_raw(&r).unwrap();

    r.id = "dead".to_string();
    r.deleted = true;
    backend.put_raw(&r).unwrap();

    let result = backend
        .scan_raw("col", &ScanOptions { include_deleted: false, ..Default::default() })
        .unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].id, "alive");
}

#[test]
fn scan_raw_includes_tombstones_when_requested() {
    let backend = make_backend();
    let mut r = make_record("alive", "col");
    backend.put_raw(&r).unwrap();

    r.id = "dead".to_string();
    r.deleted = true;
    backend.put_raw(&r).unwrap();

    let result = backend
        .scan_raw("col", &ScanOptions { include_deleted: true, ..Default::default() })
        .unwrap();
    assert_eq!(result.records.len(), 2);
}

#[test]
fn scan_raw_respects_limit() {
    let backend = make_backend();
    for i in 0..5 {
        backend.put_raw(&make_record(&format!("r{i}"), "col")).unwrap();
    }
    let result = backend
        .scan_raw("col", &ScanOptions { limit: Some(2), ..Default::default() })
        .unwrap();
    assert_eq!(result.records.len(), 2);
}

#[test]
fn scan_raw_respects_offset() {
    let backend = make_backend();
    for i in 0..5 {
        backend.put_raw(&make_record(&format!("r{i}"), "col")).unwrap();
    }
    let result = backend
        .scan_raw(
            "col",
            &ScanOptions {
                limit: Some(10),
                offset: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(result.records.len(), 2);
}

#[test]
fn scan_raw_only_returns_records_for_requested_collection() {
    let backend = make_backend();
    backend.put_raw(&make_record("a", "col_a")).unwrap();
    backend.put_raw(&make_record("b", "col_b")).unwrap();

    let result = backend
        .scan_raw("col_a", &ScanOptions::default())
        .unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].id, "a");
}

// ============================================================================
// scan_dirty_raw
// ============================================================================

#[test]
fn scan_dirty_raw_returns_only_dirty_records() {
    let backend = make_backend();

    let mut r = make_record("clean", "col");
    r.dirty = false;
    backend.put_raw(&r).unwrap();

    let mut r2 = make_record("dirty", "col");
    r2.dirty = true;
    backend.put_raw(&r2).unwrap();

    let result = backend.scan_dirty_raw("col").unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].id, "dirty");
}

#[test]
fn scan_dirty_raw_returns_empty_when_none_dirty() {
    let backend = make_backend();
    backend.put_raw(&make_record("r", "col")).unwrap();
    let result = backend.scan_dirty_raw("col").unwrap();
    assert!(result.records.is_empty());
}

// ============================================================================
// count_raw
// ============================================================================

#[test]
fn count_raw_counts_live_records() {
    let backend = make_backend();
    for i in 0..4 {
        backend.put_raw(&make_record(&format!("r{i}"), "col")).unwrap();
    }
    // One tombstone — should not be counted
    let mut t = make_record("tomb", "col");
    t.deleted = true;
    backend.put_raw(&t).unwrap();

    assert_eq!(backend.count_raw("col").unwrap(), 4);
}

#[test]
fn count_raw_returns_zero_for_empty_collection() {
    let backend = make_backend();
    assert_eq!(backend.count_raw("empty").unwrap(), 0);
}

// ============================================================================
// batch_put_raw
// ============================================================================

#[test]
fn batch_put_raw_writes_all_records_atomically() {
    let backend = make_backend();
    let records: Vec<SerializedRecord> = (0..5)
        .map(|i| make_record(&format!("r{i}"), "col"))
        .collect();

    backend.batch_put_raw(&records).unwrap();

    assert_eq!(backend.count_raw("col").unwrap(), 5);
}

#[test]
fn batch_put_raw_empty_slice_is_noop() {
    let backend = make_backend();
    backend.batch_put_raw(&[]).unwrap();
    assert_eq!(backend.count_raw("col").unwrap(), 0);
}

// ============================================================================
// get_meta / set_meta
// ============================================================================

#[test]
fn get_meta_returns_none_for_missing_key() {
    let backend = make_backend();
    assert!(backend.get_meta("nonexistent").unwrap().is_none());
}

#[test]
fn set_meta_then_get_meta_round_trips() {
    let backend = make_backend();
    backend.set_meta("last_sequence:users", "42").unwrap();
    let v = backend.get_meta("last_sequence:users").unwrap();
    assert_eq!(v.as_deref(), Some("42"));
}

#[test]
fn set_meta_overwrites_existing_value() {
    let backend = make_backend();
    backend.set_meta("key", "v1").unwrap();
    backend.set_meta("key", "v2").unwrap();
    assert_eq!(backend.get_meta("key").unwrap().as_deref(), Some("v2"));
}

// ============================================================================
// purge_tombstones_raw
// ============================================================================

#[test]
fn purge_tombstones_raw_removes_deleted_records() {
    let backend = make_backend();
    backend.put_raw(&make_record("live", "col")).unwrap();
    let mut t = make_record("tomb", "col");
    t.deleted = true;
    backend.put_raw(&t).unwrap();

    let purged = backend
        .purge_tombstones_raw(
            "col",
            &PurgeTombstonesOptions {
                older_than_seconds: None,
                dry_run: false,
            },
        )
        .unwrap();

    assert_eq!(purged, 1);
    assert_eq!(backend.count_raw("col").unwrap(), 1);
    assert!(backend.get_raw("col", "live").unwrap().is_some());
    assert!(backend.get_raw("col", "tomb").unwrap().is_none());
}

#[test]
fn purge_tombstones_raw_dry_run_does_not_delete() {
    let backend = make_backend();
    let mut t = make_record("tomb", "col");
    t.deleted = true;
    backend.put_raw(&t).unwrap();

    let would_purge = backend
        .purge_tombstones_raw(
            "col",
            &PurgeTombstonesOptions {
                older_than_seconds: None,
                dry_run: true,
            },
        )
        .unwrap();

    assert_eq!(would_purge, 1);
    // Record should still be there
    let result = backend
        .scan_raw("col", &ScanOptions { include_deleted: true, ..Default::default() })
        .unwrap();
    assert_eq!(result.records.len(), 1);
}

#[test]
fn purge_tombstones_raw_older_than_keeps_recent_tombstones() {
    let backend = make_backend();

    // Insert a tombstone with deleted_at = now (should NOT be purged when age filter is large)
    let mut t = SerializedRecord {
        deleted: true,
        // Use a past date for the deleted_at
        deleted_at: Some("2000-01-01T00:00:00Z".to_string()),
        ..make_record("old-tomb", "col")
    };
    backend.put_raw(&t).unwrap();

    // A fresh tombstone — deleted_at is null (should not be matched by date filter)
    t.id = "fresh-tomb".to_string();
    t.deleted_at = None;
    backend.put_raw(&t).unwrap();

    // Purge tombstones older than 1 second — only "old-tomb" matches
    let purged = backend
        .purge_tombstones_raw(
            "col",
            &PurgeTombstonesOptions {
                older_than_seconds: Some(1),
                dry_run: false,
            },
        )
        .unwrap();

    assert_eq!(purged, 1, "only one old tombstone should be purged");
}

// ============================================================================
// check_unique — field index
// ============================================================================

#[test]
fn check_unique_field_succeeds_when_no_conflict() {
    let backend = make_backend();
    backend.put_raw(&make_record("r1", "col")).unwrap();

    let index = field_index_single("idx_name", "name", true);
    // Checking "r2" — no record has name == "r2"
    let result = backend.check_unique("col", &index, &json!({ "name": "r2" }), None, None);
    assert!(result.is_ok());
}

#[test]
fn check_unique_field_returns_error_when_conflict_exists() {
    let backend = make_backend();
    let r = SerializedRecord {
        data: json!({ "name": "Alice" }),
        ..make_record("r1", "col")
    };
    backend.put_raw(&r).unwrap();

    let index = field_index_single("idx_name", "name", true);
    let result = backend.check_unique("col", &index, &json!({ "name": "Alice" }), None, None);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(err_str.contains("idx_name"), "error should mention index: {err_str}");
    assert!(err_str.contains("col"), "error should mention collection: {err_str}");
}

#[test]
fn check_unique_field_excludes_self_when_updating() {
    let backend = make_backend();
    let r = SerializedRecord {
        data: json!({ "name": "Alice" }),
        ..make_record("r1", "col")
    };
    backend.put_raw(&r).unwrap();

    let index = field_index_single("idx_name", "name", true);
    // Updating the same record — should not conflict with itself
    let result = backend.check_unique(
        "col",
        &index,
        &json!({ "name": "Alice" }),
        None,
        Some("r1"),
    );
    assert!(result.is_ok(), "should not conflict with self: {result:?}");
}

#[test]
fn check_unique_field_ignores_deleted_records() {
    let backend = make_backend();
    let r = SerializedRecord {
        data: json!({ "name": "Alice" }),
        deleted: true,
        ..make_record("r1", "col")
    };
    backend.put_raw(&r).unwrap();

    let index = field_index_single("idx_name", "name", true);
    // Deleted record should not cause a conflict
    let result = backend.check_unique("col", &index, &json!({ "name": "Alice" }), None, None);
    assert!(result.is_ok());
}

// ============================================================================
// check_unique — computed index
// ============================================================================

#[test]
fn check_unique_computed_succeeds_when_no_conflict() {
    let backend = make_backend();

    let computed_index = IndexDefinition::Computed(ComputedIndex {
        name: "emailLower".to_string(),
        compute: Arc::new(|_| None),
        unique: true,
        sparse: false,
    });

    let result = backend.check_unique(
        "col",
        &computed_index,
        &json!({}),
        Some(&json!({ "emailLower": "new@example.com" })),
        None,
    );
    assert!(result.is_ok());
}

#[test]
fn check_unique_computed_returns_error_on_conflict() {
    let backend = make_backend();

    let r = SerializedRecord {
        data: json!({}),
        computed: Some(json!({ "emailLower": "alice@example.com" })),
        ..make_record("r1", "col")
    };
    backend.put_raw(&r).unwrap();

    let computed_index = IndexDefinition::Computed(ComputedIndex {
        name: "emailLower".to_string(),
        compute: Arc::new(|_| None),
        unique: true,
        sparse: false,
    });

    let result = backend.check_unique(
        "col",
        &computed_index,
        &json!({}),
        Some(&json!({ "emailLower": "alice@example.com" })),
        None,
    );
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("emailLower"), "error should mention index: {err}");
}

// ============================================================================
// scan_index_raw
// ============================================================================

#[test]
fn scan_index_raw_exact_match_returns_matching_records() {
    let backend = make_backend();

    for (i, name) in ["Alice", "Bob", "Alice"].iter().enumerate() {
        let mut r = make_record(&format!("{i}"), "col");
        r.data = json!({ "name": name });
        backend.put_raw(&r).unwrap();
    }

    let index = field_index_single("idx_name", "name", false);
    let scan = exact_field_scan(index, IndexableValue::String("Alice".to_string()));

    let result = backend.scan_index_raw("col", &scan).unwrap();
    let records = result.unwrap().records;
    assert_eq!(records.len(), 2);
    for r in &records {
        assert_eq!(r.data["name"], "Alice");
    }
}

#[test]
fn scan_index_raw_exact_match_returns_empty_when_no_match() {
    let backend = make_backend();
    let mut r = make_record("r1", "col");
    r.data = json!({ "name": "Bob" });
    backend.put_raw(&r).unwrap();

    let index = field_index_single("idx_name", "name", false);
    let scan = exact_field_scan(index, IndexableValue::String("Zed".to_string()));

    let result = backend.scan_index_raw("col", &scan).unwrap().unwrap();
    assert!(result.records.is_empty());
}

#[test]
fn scan_index_raw_does_not_return_deleted_records() {
    let backend = make_backend();

    let mut r = SerializedRecord {
        data: json!({ "name": "Alice" }),
        deleted: true,
        ..make_record("r1", "col")
    };
    backend.put_raw(&r).unwrap();

    r.id = "r2".to_string();
    r.deleted = false;
    backend.put_raw(&r).unwrap();

    let index = field_index_single("idx_name", "name", false);
    let scan = exact_field_scan(index, IndexableValue::String("Alice".to_string()));

    let result = backend.scan_index_raw("col", &scan).unwrap().unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].id, "r2");
}

// ============================================================================
// count_index_raw
// ============================================================================

#[test]
fn count_index_raw_returns_correct_count() {
    let backend = make_backend();

    for name in ["Alice", "Bob", "Alice", "Alice"] {
        let id = format!("{}-{}", name, uuid::Uuid::new_v4());
        let mut r = make_record(&id, "col");
        r.data = json!({ "name": name });
        backend.put_raw(&r).unwrap();
    }

    let index = field_index_single("idx_name", "name", false);
    let scan = exact_field_scan(index, IndexableValue::String("Alice".to_string()));

    let count = backend.count_index_raw("col", &scan).unwrap().unwrap();
    assert_eq!(count, 3);
}

// ============================================================================
// transaction
// ============================================================================

#[test]
fn transaction_commits_on_success() {
    let backend = make_backend();
    backend
        .transaction(|b| {
            b.put_raw(&make_record("r1", "col"))?;
            b.put_raw(&make_record("r2", "col"))
        })
        .unwrap();

    assert_eq!(backend.count_raw("col").unwrap(), 2);
}

#[test]
fn transaction_rolls_back_on_error() {
    let backend = make_backend();

    let result = backend.transaction(|b| {
        b.put_raw(&make_record("r1", "col"))?;
        Err::<(), _>(less_db::error::LessDbError::Internal("forced failure".to_string()))
    });

    assert!(result.is_err());
    // The record written before the error should have been rolled back.
    assert_eq!(backend.count_raw("col").unwrap(), 0);
}

// ============================================================================
// initialize
// ============================================================================

#[test]
fn initialize_sets_is_initialized() {
    let mut backend = SqliteBackend::open_in_memory().unwrap();
    assert!(!backend.is_initialized());
    backend.initialize(&[]).unwrap();
    assert!(backend.is_initialized());
}

#[test]
fn initialize_is_idempotent() {
    let mut backend = SqliteBackend::open_in_memory().unwrap();
    backend.initialize(&[]).unwrap();
    backend.initialize(&[]).unwrap(); // second call should succeed
    assert!(backend.is_initialized());
}
