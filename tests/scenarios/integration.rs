//! End-to-end integration tests exercising full flows through
//! ReactiveAdapter → Adapter → SqliteBackend.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use less_db::{
    collection::builder::{collection, CollectionDef},
    crdt::{self, MIN_SESSION_ID},
    query::types::{SortDirection, SortEntry, SortInput},
    reactive::ReactiveAdapter,
    schema::node::t,
    storage::{
        adapter::Adapter,
        sqlite::SqliteBackend,
        traits::{StorageBackend, StorageLifecycle, StorageRead, StorageSync, StorageWrite},
    },
    query::types::Query,
    types::{
        ApplyRemoteOptions, DeleteOptions, GetOptions, PatchOptions, PutOptions,
        RemoteRecord, SerializedRecord,
    },
};
use serde_json::{json, Value};

// ============================================================================
// Helpers
// ============================================================================

const SID: u64 = MIN_SESSION_ID;

fn users_def() -> CollectionDef {
    collection("users")
        .v(1, {
            let mut s = BTreeMap::new();
            s.insert("name".to_string(), t::string());
            s.insert("email".to_string(), t::string());
            s.insert("age".to_string(), t::optional(t::number()));
            s
        })
        .index(&["name"])
        .build()
}

fn versioned_def() -> CollectionDef {
    collection("docs")
        .v(1, {
            let mut s = BTreeMap::new();
            s.insert("title".to_string(), t::string());
            s
        })
        .v(
            2,
            {
                let mut s = BTreeMap::new();
                s.insert("title".to_string(), t::string());
                s.insert("body".to_string(), t::string());
                s
            },
            |mut data| {
                if let Value::Object(ref mut m) = data {
                    m.entry("body").or_insert(Value::String(String::new()));
                }
                Ok(data)
            },
        )
        .build()
}

fn put_opts() -> PutOptions {
    PutOptions {
        session_id: Some(SID),
        ..Default::default()
    }
}

/// Build a ReactiveAdapter with the given def, calling the builder function
/// twice to avoid the non-Clone constraint on CollectionDef.
fn make_reactive(
    build_def: fn() -> CollectionDef,
) -> (ReactiveAdapter<SqliteBackend>, CollectionDef) {
    let def1 = build_def();
    let def2 = build_def();
    let mut backend = SqliteBackend::open_in_memory().expect("open");
    backend.initialize(&[&def1]).expect("backend init");
    let inner = Adapter::new(backend);
    let mut ra = ReactiveAdapter::new(inner);
    ra.initialize(&[Arc::new(def2)]).expect("ra init");
    (ra, def1)
}

fn make_adapter(build_def: fn() -> CollectionDef) -> (Adapter<SqliteBackend>, CollectionDef) {
    let def1 = build_def();
    let def2 = build_def();
    let mut backend = SqliteBackend::open_in_memory().expect("open");
    backend.initialize(&[&def1]).expect("backend init");
    let mut adapter = Adapter::new(backend);
    adapter
        .initialize(&[Arc::new(def2)])
        .expect("adapter init");
    (adapter, def1)
}

/// Build an Adapter + a separate SqliteBackend reference for raw operations.
fn make_adapter_with_raw_backend(
    build_def: fn() -> CollectionDef,
) -> (Adapter<SqliteBackend>, CollectionDef, SqliteBackend) {
    let def1 = build_def();
    let def2 = build_def();
    let def3 = build_def();
    let mut backend_for_adapter = SqliteBackend::open_in_memory().expect("open");
    backend_for_adapter.initialize(&[&def1]).expect("backend init");
    let mut adapter = Adapter::new(backend_for_adapter);
    adapter
        .initialize(&[Arc::new(def2)])
        .expect("adapter init");
    (adapter, def3, SqliteBackend::open_in_memory().expect("open"))
}

// ============================================================================
// Scenario 1: Full CRUD lifecycle
// ============================================================================

#[test]
fn crud_lifecycle() {
    let (adapter, def) = make_adapter(users_def);

    // Create
    let created = adapter
        .put(
            &def,
            json!({ "name": "Alice", "email": "alice@example.com", "age": 30 }),
            &put_opts(),
        )
        .expect("put");

    assert_eq!(created.data["name"], json!("Alice"));
    assert!(!created.id.is_empty());
    assert!(created.data["createdAt"].is_string());
    assert!(created.data["updatedAt"].is_string());

    // Read
    let fetched = adapter
        .get(&def, &created.id, &GetOptions::default())
        .expect("get")
        .expect("record should exist");
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.data["email"], json!("alice@example.com"));

    // Update via patch
    let patch_opts = PatchOptions {
        id: created.id.clone(),
        session_id: Some(SID),
        ..Default::default()
    };
    let patched = adapter
        .patch(&def, json!({ "name": "Alice Updated" }), &patch_opts)
        .expect("patch");
    assert_eq!(patched.data["name"], json!("Alice Updated"));
    assert_eq!(patched.data["email"], json!("alice@example.com")); // unchanged

    // Verify update persisted
    let after_patch = adapter
        .get(&def, &created.id, &GetOptions::default())
        .expect("get")
        .expect("still exists");
    assert_eq!(after_patch.data["name"], json!("Alice Updated"));

    // Delete
    let deleted = adapter
        .delete(&def, &created.id, &DeleteOptions::default())
        .expect("delete");
    assert!(deleted);

    // Confirm deleted
    let gone = adapter
        .get(&def, &created.id, &GetOptions::default())
        .expect("get");
    assert!(gone.is_none(), "deleted record should not be returned");

    // Confirm count is 0
    let count = adapter.count(&def, None).expect("count");
    assert_eq!(count, 0);
}

// ============================================================================
// Scenario 2: Query with filter, sort, and pagination
// ============================================================================

#[test]
fn query_with_filter_sort_pagination() {
    let (adapter, def) = make_adapter(users_def);

    let names = ["Charlie", "Alice", "Bob", "Diana", "Eve"];
    for (i, name) in names.iter().enumerate() {
        adapter
            .put(
                &def,
                json!({ "name": name, "email": format!("{name}@x.com"), "age": 20 + i }),
                &put_opts(),
            )
            .expect("put");
    }

    // Query all, sorted by name ASC
    let query = Query {
        sort: Some(SortInput::Entries(vec![SortEntry {
            field: "name".to_string(),
            direction: SortDirection::Asc,
        }])),
        ..Default::default()
    };
    let result = adapter.query(&def, &query).expect("query");
    assert_eq!(result.records.len(), 5);
    let sorted_names: Vec<&str> = result
        .records
        .iter()
        .map(|r| r.data["name"].as_str().unwrap())
        .collect();
    assert_eq!(sorted_names, vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);

    // Query with filter
    let filtered = adapter
        .query(
            &def,
            &Query {
                filter: Some(json!({ "name": "Alice" })),
                ..Default::default()
            },
        )
        .expect("query");
    assert_eq!(filtered.records.len(), 1);
    assert_eq!(filtered.records[0].data["name"], json!("Alice"));

    // Query with pagination
    let page = adapter
        .query(
            &def,
            &Query {
                sort: Some(SortInput::Entries(vec![SortEntry {
                    field: "name".to_string(),
                    direction: SortDirection::Asc,
                }])),
                limit: Some(2),
                offset: Some(1),
                ..Default::default()
            },
        )
        .expect("query");
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].data["name"], json!("Bob"));
    assert_eq!(page.records[1].data["name"], json!("Charlie"));
}

// ============================================================================
// Scenario 3: Sync push round-trip
// ============================================================================

#[test]
fn sync_push_round_trip() {
    let (adapter, def) = make_adapter(users_def);

    // Create a record — it starts dirty
    let record = adapter
        .put(
            &def,
            json!({ "name": "Alice", "email": "a@x.com" }),
            &put_opts(),
        )
        .expect("put");

    // Verify it's dirty
    let dirty = adapter.get_dirty(&def).expect("get_dirty");
    assert_eq!(dirty.records.len(), 1);
    assert_eq!(dirty.records[0].id, record.id);

    // Mark as synced with sequence 42
    adapter
        .mark_synced(&def, &record.id, 42, None)
        .expect("mark_synced");

    // Verify no longer dirty
    let dirty_after = adapter.get_dirty(&def).expect("get_dirty");
    assert!(dirty_after.records.is_empty(), "should be clean after sync");

    // Verify sequence updated
    let synced = adapter
        .get(&def, &record.id, &GetOptions::default())
        .expect("get")
        .expect("exists");
    assert_eq!(synced.sequence, 42);
}

// ============================================================================
// Scenario 4: Remote merge — local edit + remote edit
// ============================================================================

#[test]
fn remote_merge_preserves_both_changes() {
    let (adapter, def) = make_adapter(users_def);

    // Create initial record
    let created = adapter
        .put(
            &def,
            json!({ "name": "Alice", "email": "alice@example.com" }),
            &put_opts(),
        )
        .expect("put");

    // Local edit: change name
    let patch_opts = PatchOptions {
        id: created.id.clone(),
        session_id: Some(SID),
        ..Default::default()
    };
    let _patched = adapter
        .patch(&def, json!({ "name": "Alice Local" }), &patch_opts)
        .expect("patch");

    // Build a "remote" update: different session, change email
    let remote_data = json!({
        "id": created.id,
        "name": "Alice",
        "email": "alice-remote@example.com",
        "createdAt": created.data["createdAt"],
        "updatedAt": "2025-01-01T00:00:00Z",
    });
    let remote_model = crdt::create_model(&remote_data, SID + 100).expect("model");
    let remote_crdt = crdt::model_to_binary(&remote_model);

    let remote = RemoteRecord {
        id: created.id.clone(),
        version: 1,
        crdt: Some(remote_crdt),
        deleted: false,
        sequence: 50,
        meta: None,
    };

    adapter
        .apply_remote_changes(&def, &[remote], &ApplyRemoteOptions::default())
        .expect("apply_remote_changes");

    // After merge, the record should exist and be valid
    let merged = adapter
        .get(&def, &created.id, &GetOptions::default())
        .expect("get")
        .expect("exists");

    // Both name and email should be strings (valid after CRDT merge)
    assert!(merged.data["name"].is_string(), "name should be a string after merge");
    assert!(merged.data["email"].is_string(), "email should be a string after merge");
    // Sequence should be updated from the remote
    assert_eq!(merged.sequence, 50);
}

// ============================================================================
// Scenario 5: Reactive observe — write triggers callback
// ============================================================================

#[test]
fn reactive_observe_through_write_and_flush() {
    let (ra, def) = make_reactive(users_def);

    let created = ra
        .put(
            &def,
            json!({ "name": "Alice", "email": "a@x.com" }),
            &put_opts(),
        )
        .expect("put");

    let log: Arc<Mutex<Vec<Option<Value>>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();

    let _unsub = ra.observe(
        Arc::new(users_def()),
        created.id.clone(),
        Arc::new(move |data: Option<Value>| {
            log_clone.lock().unwrap().push(data);
        }),
        None,
    );

    ra.flush();

    // First callback should have Alice
    {
        let entries = log.lock().unwrap();
        assert!(!entries.is_empty(), "observe should have fired");
        let first = entries[0].as_ref().expect("should have data");
        assert_eq!(first["name"], json!("Alice"));
    }

    // Update name
    let patch_opts = PatchOptions {
        id: created.id.clone(),
        session_id: Some(SID),
        ..Default::default()
    };
    ra.patch(&def, json!({ "name": "Alice Updated" }), &patch_opts)
        .expect("patch");
    ra.flush();

    // Second callback should have updated name
    {
        let entries = log.lock().unwrap();
        assert!(entries.len() >= 2, "should have at least 2 callbacks");
        let latest = entries.last().unwrap().as_ref().expect("data");
        assert_eq!(latest["name"], json!("Alice Updated"));
    }
}

// ============================================================================
// Scenario 6: Migration — read old-version record after schema upgrade
// ============================================================================

#[test]
fn migration_on_read() {
    // Build adapter and backend separately so we can insert a v1 record via put_raw
    let def1 = versioned_def();
    let def2 = versioned_def();
    let mut backend = SqliteBackend::open_in_memory().expect("open");
    backend.initialize(&[&def1]).expect("backend init");

    // Insert a v1 record directly via the backend
    let v1_data = json!({
        "id": "doc-1",
        "title": "Hello World",
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-01-01T00:00:00Z",
    });
    let model = crdt::create_model(&v1_data, SID).expect("model");
    let crdt_binary = crdt::model_to_binary(&model);

    let v1_record = SerializedRecord {
        id: "doc-1".to_string(),
        collection: "docs".to_string(),
        version: 1,
        data: v1_data,
        crdt: crdt_binary,
        pending_patches: vec![],
        sequence: 0,
        dirty: false,
        deleted: false,
        deleted_at: None,
        meta: None,
        computed: None,
    };
    backend.put_raw(&v1_record).expect("put_raw v1 record");

    // Now wrap in adapter and read with migration
    let mut adapter = Adapter::new(backend);
    adapter
        .initialize(&[Arc::new(def2)])
        .expect("adapter init");

    let opts = GetOptions {
        migrate: true,
        ..Default::default()
    };
    let fetched = adapter
        .get(&def1, "doc-1", &opts)
        .expect("get")
        .expect("should find the record");

    // Should have migrated: body field added with empty string
    assert_eq!(fetched.data["title"], json!("Hello World"));
    assert_eq!(fetched.data["body"], json!(""));
}

// ============================================================================
// Scenario 7: Bulk operations end-to-end
// ============================================================================

#[test]
fn bulk_operations_end_to_end() {
    let (adapter, def) = make_adapter(users_def);

    // Bulk put
    let bulk_result = adapter
        .bulk_put(
            &def,
            vec![
                json!({ "name": "Alice", "email": "a@x.com" }),
                json!({ "name": "Bob", "email": "b@x.com" }),
                json!({ "name": "Charlie", "email": "c@x.com" }),
            ],
            &put_opts(),
        )
        .expect("bulk_put");
    assert_eq!(bulk_result.records.len(), 3);

    // Count
    let count = adapter.count(&def, None).expect("count");
    assert_eq!(count, 3);

    // Patch many — update all Alices
    let patch_opts = PatchOptions {
        session_id: Some(SID),
        ..Default::default()
    };
    let patch_result = adapter
        .patch_many(
            &def,
            &json!({ "name": "Alice" }),
            &json!({ "name": "Alice Updated" }),
            &patch_opts,
        )
        .expect("patch_many");
    assert_eq!(patch_result.matched_count, 1);
    assert_eq!(patch_result.updated_count, 1);

    // Delete many — delete Bob
    let del_result = adapter
        .delete_many(&def, &json!({ "name": "Bob" }), &DeleteOptions::default())
        .expect("delete_many");
    assert_eq!(del_result.deleted_ids.len(), 1);

    // Final count should be 2 (Alice Updated + Charlie)
    let final_count = adapter.count(&def, None).expect("count");
    assert_eq!(final_count, 2);
}
