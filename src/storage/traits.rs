/// Storage backend traits for less-db-rs.
///
/// `StorageBackend` is the narrow raw I/O trait implemented by concrete backends
/// (e.g. SQLite, IndexedDB). Higher-level traits that depend on `CollectionDef`
/// will be added once the collection module is implemented.
///
/// TODO: add CollectionDef-based traits once collection module is ready.
use crate::error::Result;
use crate::types::{PurgeTombstonesOptions, RawBatchResult, ScanOptions, SerializedRecord};

/// Low-level storage backend — raw record I/O with no collection semantics.
///
/// Implementors must be `Send + Sync` so they can be shared across threads.
pub trait StorageBackend: Send + Sync {
    /// Fetch a single raw record by collection and id.
    /// Returns `None` if the record does not exist (including tombstones,
    /// depending on backend filtering).
    fn get_raw(&self, collection: &str, id: &str) -> Result<Option<SerializedRecord>>;

    /// Persist (insert or replace) a raw serialized record.
    fn put_raw(&self, record: &SerializedRecord) -> Result<()>;

    /// Scan all records in a collection, respecting `ScanOptions`.
    fn scan_raw(&self, collection: &str, options: &ScanOptions) -> Result<RawBatchResult>;

    /// Scan records that have local unpushed changes (`dirty == true`).
    fn scan_dirty_raw(&self, collection: &str) -> Result<RawBatchResult>;

    /// Count live (non-deleted) records in a collection.
    fn count_raw(&self, collection: &str) -> Result<usize>;

    /// Atomically write multiple records in a single backend transaction.
    fn batch_put_raw(&self, records: &[SerializedRecord]) -> Result<()>;

    /// Remove tombstoned records from storage, optionally filtered by age.
    /// Returns the number of records purged.
    fn purge_tombstones_raw(
        &self,
        collection: &str,
        options: &PurgeTombstonesOptions,
    ) -> Result<usize>;

    /// Read a metadata key-value pair (used for sequence numbers, schema versions, etc.).
    fn get_meta(&self, key: &str) -> Result<Option<String>>;

    /// Write a metadata key-value pair.
    fn set_meta(&self, key: &str, value: &str) -> Result<()>;

    /// Execute a closure inside a backend transaction.
    ///
    /// The closure receives a reference to `self`; implementations should
    /// begin a transaction before calling `f` and commit (or roll back on
    /// error) after it returns.
    fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Self) -> Result<T>;
}
