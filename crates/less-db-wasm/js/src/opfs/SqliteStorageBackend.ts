/**
 * SQLite-backed StorageBackend using @sqlite.org/sqlite-wasm with OPFS.
 *
 * Runs inside a dedicated Web Worker. After async initialization (WASM load +
 * OPFS VFS setup), all operations are fully synchronous — matching the
 * StorageBackend interface contract.
 *
 * SQL schema matches the Rust SqliteBackend (crates/less-db/src/storage/sqlite.rs).
 */

import type {
  StorageBackend,
  SerializedRecord,
  ScanOptions,
  PurgeTombstonesOptions,
  RawBatchResult,
} from "../types.js";

// Static import — bundled with the worker by Vite.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";

// ============================================================================
// Type aliases for @sqlite.org/sqlite-wasm OO1 API
//
// We use `any` at the boundary because the library's complex union types
// don't match our simplified usage. The actual API is well-tested.
// ============================================================================

/* eslint-disable @typescript-eslint/no-explicit-any */
type Sqlite3Db = any;
type Sqlite3Stmt = any;
type OpfsSAHPoolUtil = any;
/* eslint-enable @typescript-eslint/no-explicit-any */

// ============================================================================
// SQL constants
// ============================================================================

const INIT_SQL = `
  PRAGMA journal_mode=DELETE;
  PRAGMA synchronous=NORMAL;

  CREATE TABLE IF NOT EXISTS records (
    id              TEXT NOT NULL,
    collection      TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    data            TEXT NOT NULL DEFAULT '{}',
    crdt            BLOB,
    pending_patches BLOB,
    sequence        INTEGER NOT NULL DEFAULT -1,
    dirty           INTEGER NOT NULL DEFAULT 0,
    deleted         INTEGER NOT NULL DEFAULT 0,
    deleted_at      TEXT,
    meta            TEXT,
    computed        TEXT,
    PRIMARY KEY (collection, id)
  );
  CREATE INDEX IF NOT EXISTS idx_records_collection
    ON records(collection);
  CREATE INDEX IF NOT EXISTS idx_records_dirty
    ON records(collection, dirty);

  CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  INSERT OR IGNORE INTO meta (key, value) VALUES ('schema:version', '1');
`;

const SELECT_COLS =
  "id, collection, version, data, crdt, pending_patches, " +
  "sequence, dirty, deleted, deleted_at, meta, computed";

// ============================================================================
// Helpers
// ============================================================================

function rowToRecord(row: unknown[]): SerializedRecord {
  const [
    id,
    collection,
    version,
    dataStr,
    crdtBlob,
    ppBlob,
    sequence,
    dirty,
    deleted,
    deletedAt,
    metaStr,
    computedStr,
  ] = row as [
    string,
    string,
    number,
    string,
    Uint8Array | null,
    Uint8Array | null,
    number,
    number,
    number,
    string | null,
    string | null,
    string | null,
  ];

  return {
    id,
    collection,
    version,
    data: JSON.parse(dataStr),
    crdt: crdtBlob ? Array.from(crdtBlob) : [],
    pending_patches: ppBlob ? Array.from(ppBlob) : [],
    sequence,
    dirty: dirty !== 0,
    deleted: deleted !== 0,
    deleted_at: deletedAt,
    meta: metaStr ? JSON.parse(metaStr) : null,
    computed: computedStr ? JSON.parse(computedStr) : null,
  };
}

function serializeRecord(record: SerializedRecord): {
  dataStr: string;
  metaStr: string | null;
  computedStr: string | null;
  crdtBlob: Uint8Array;
  ppBlob: Uint8Array;
} {
  return {
    dataStr: JSON.stringify(record.data),
    metaStr: record.meta != null ? JSON.stringify(record.meta) : null,
    computedStr: record.computed != null ? JSON.stringify(record.computed) : null,
    crdtBlob: new Uint8Array(record.crdt),
    ppBlob: new Uint8Array(record.pending_patches),
  };
}

// ============================================================================
// IndexedDbBackend-compatible unique constraint checking types
// (Shape of index objects passed from Rust via WASM)
// ============================================================================

interface FieldCheckIndex {
  type: "field";
  name: string;
  fields: Array<{ field: string; order: string }>;
  unique: boolean;
  sparse: boolean;
}

interface ComputedCheckIndex {
  type: "computed";
  name: string;
  unique: boolean;
  sparse: boolean;
}

type CheckUniqueIndex = FieldCheckIndex | ComputedCheckIndex;

function normalizeValue(val: unknown): unknown {
  if (val instanceof Date) return val.toISOString();
  return val;
}

// ============================================================================
// SqliteStorageBackend
// ============================================================================

export class SqliteStorageBackend implements StorageBackend {
  private db: Sqlite3Db;
  private poolUtil: OpfsSAHPoolUtil;
  private dbPath: string;
  private inTransaction = false;

  private constructor(db: Sqlite3Db, poolUtil: OpfsSAHPoolUtil, dbPath: string) {
    this.db = db;
    this.poolUtil = poolUtil;
    this.dbPath = dbPath;
  }

  /**
   * Create a new SqliteStorageBackend with OPFS persistence.
   *
   * Loads SQLite WASM, sets up the OPFS SAH Pool VFS, opens the database,
   * and initializes the schema. After this, all StorageBackend methods are sync.
   */
  static async create(dbName: string): Promise<SqliteStorageBackend> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const sqlite3 = await (sqlite3InitModule as any)({
      print: () => {},    // suppress logs
      printErr: console.error,
    });

    const poolUtil = await sqlite3.installOpfsSAHPoolVfs({
      initialCapacity: 6,
      directory: `.less-db-${dbName}`,
      name: `less-db-sahpool-${dbName}`,
      clearOnInit: false,
    });

    const dbPath = `/${dbName}.sqlite3`;
    const db = new poolUtil.OpfsSAHPoolDb(dbPath);

    // Initialize schema
    db.exec(INIT_SQL);

    return new SqliteStorageBackend(db, poolUtil, dbPath);
  }

  // ==========================================================================
  // StorageBackend — reads
  // ==========================================================================

  getRaw(collection: string, id: string): SerializedRecord | null {
    const stmt = this.db.prepare(
      `SELECT ${SELECT_COLS} FROM records WHERE collection = ? AND id = ?`,
    );
    try {
      stmt.bind([collection, id]);
      if (stmt.step()) {
        return rowToRecord(stmt.get({ rowMode: "array" }) as unknown[]);
      }
      return null;
    } finally {
      stmt.finalize();
    }
  }

  scanRaw(collection: string, options: ScanOptions): RawBatchResult {
    const includeDeleted = options.include_deleted ?? false;
    const limit = options.limit;
    const offset = options.offset;

    let sql = `SELECT ${SELECT_COLS} FROM records WHERE collection = ?`;
    const binds: unknown[] = [collection];

    if (!includeDeleted) {
      sql += " AND deleted = 0";
    }

    if (limit != null) {
      sql += " LIMIT ?";
      binds.push(limit);
    }
    if (offset != null) {
      if (limit == null) sql += " LIMIT -1";
      sql += " OFFSET ?";
      binds.push(offset);
    }

    const records: SerializedRecord[] = [];
    this.db.exec({
      sql,
      bind: binds,
      rowMode: "array",
      callback: (row: unknown) => records.push(rowToRecord(row as unknown[])),
    });
    return { records };
  }

  scanDirtyRaw(collection: string): RawBatchResult {
    const records: SerializedRecord[] = [];
    this.db.exec({
      sql: `SELECT ${SELECT_COLS} FROM records WHERE collection = ? AND dirty = 1`,
      bind: [collection],
      rowMode: "array",
      callback: (row: unknown) => records.push(rowToRecord(row as unknown[])),
    });
    return { records };
  }

  countRaw(collection: string): number {
    const stmt = this.db.prepare(
      "SELECT COUNT(*) FROM records WHERE collection = ? AND deleted = 0",
    );
    try {
      stmt.bind([collection]);
      stmt.step();
      return stmt.getInt(0);
    } finally {
      stmt.finalize();
    }
  }

  // ==========================================================================
  // StorageBackend — writes
  // ==========================================================================

  putRaw(record: SerializedRecord): void {
    const { dataStr, metaStr, computedStr, crdtBlob, ppBlob } = serializeRecord(record);
    this.db.exec({
      sql: `INSERT OR REPLACE INTO records
            (id, collection, version, data, crdt, pending_patches,
             sequence, dirty, deleted, deleted_at, meta, computed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      bind: [
        record.id,
        record.collection,
        record.version,
        dataStr,
        crdtBlob,
        ppBlob,
        record.sequence,
        record.dirty ? 1 : 0,
        record.deleted ? 1 : 0,
        record.deleted_at,
        metaStr,
        computedStr,
      ],
    });
  }

  batchPutRaw(records: SerializedRecord[]): void {
    if (!this.inTransaction) {
      this.db.exec("BEGIN");
    }
    try {
      for (const record of records) {
        this.putRaw(record);
      }
      if (!this.inTransaction) {
        this.db.exec("COMMIT");
      }
    } catch (e) {
      if (!this.inTransaction) {
        this.db.exec("ROLLBACK");
      }
      throw e;
    }
  }

  purgeTombstonesRaw(collection: string, options: PurgeTombstonesOptions): number {
    const olderThanSeconds = options.older_than_seconds;
    const dryRun = options.dry_run ?? false;

    if (dryRun) {
      const stmt = olderThanSeconds != null
        ? this.db.prepare(
            `SELECT COUNT(*) FROM records WHERE collection = ? AND deleted = 1
             AND deleted_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)`,
          )
        : this.db.prepare(
            "SELECT COUNT(*) FROM records WHERE collection = ? AND deleted = 1",
          );
      try {
        const binds: unknown[] = [collection];
        if (olderThanSeconds != null) binds.push(`-${olderThanSeconds} seconds`);
        stmt.bind(binds);
        stmt.step();
        return stmt.getInt(0);
      } finally {
        stmt.finalize();
      }
    }

    if (olderThanSeconds != null) {
      this.db.exec({
        sql: `DELETE FROM records WHERE collection = ? AND deleted = 1
              AND deleted_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)`,
        bind: [collection, `-${olderThanSeconds} seconds`],
      });
    } else {
      this.db.exec({
        sql: "DELETE FROM records WHERE collection = ? AND deleted = 1",
        bind: [collection],
      });
    }

    // SQLite changes() returns the number of rows modified by the last statement
    const stmt = this.db.prepare("SELECT changes()");
    try {
      stmt.step();
      return stmt.getInt(0);
    } finally {
      stmt.finalize();
    }
  }

  // ==========================================================================
  // StorageBackend — metadata
  // ==========================================================================

  getMeta(key: string): string | null {
    const stmt = this.db.prepare("SELECT value FROM meta WHERE key = ?");
    try {
      stmt.bind([key]);
      if (stmt.step()) {
        return stmt.getString(0);
      }
      return null;
    } finally {
      stmt.finalize();
    }
  }

  setMeta(key: string, value: string): void {
    this.db.exec({
      sql: "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
      bind: [key, value],
    });
  }

  // ==========================================================================
  // StorageBackend — bulk load
  // ==========================================================================

  scanAllRaw(): RawBatchResult {
    const records: SerializedRecord[] = [];
    this.db.exec({
      sql: `SELECT ${SELECT_COLS} FROM records`,
      rowMode: "array",
      callback: (row: unknown) => records.push(rowToRecord(row as unknown[])),
    });
    return { records };
  }

  scanAllMeta(): Array<{ key: string; value: string }> {
    const entries: Array<{ key: string; value: string }> = [];
    this.db.exec({
      sql: "SELECT key, value FROM meta",
      rowMode: "array",
      callback: (row: unknown) => {
        const [key, value] = row as [string, string];
        entries.push({ key, value });
      },
    });
    return entries;
  }

  // ==========================================================================
  // StorageBackend — index methods (return null, Adapter falls back)
  // ==========================================================================

  scanIndexRaw(_collection: string, _scan: unknown): RawBatchResult | null {
    return null;
  }

  countIndexRaw(_collection: string, _scan: unknown): number | null {
    return null;
  }

  checkUnique(
    collection: string,
    index: CheckUniqueIndex,
    data: Record<string, unknown>,
    computed: Record<string, unknown> | null,
    excludeId: string | null,
  ): void {
    if (index.type === "field") {
      this.checkFieldUnique(collection, index, data, excludeId);
    } else if (index.type === "computed") {
      this.checkComputedUnique(collection, index, computed, excludeId);
    }
  }

  // ==========================================================================
  // StorageBackend — transactions
  // ==========================================================================

  beginTransaction(): void {
    this.db.exec("SAVEPOINT sp_adapter");
    this.inTransaction = true;
  }

  commit(): void {
    this.db.exec("RELEASE SAVEPOINT sp_adapter");
    this.inTransaction = false;
  }

  rollback(): void {
    this.db.exec("ROLLBACK TO SAVEPOINT sp_adapter");
    this.inTransaction = false;
  }

  // ==========================================================================
  // Lifecycle
  // ==========================================================================

  /** Close the database connection and release OPFS access handles. */
  close(): void {
    this.db.close();
  }

  /** Delete the OPFS database files. Must call close() first. */
  deleteDatabase(): void {
    this.poolUtil.unlink(this.dbPath);
  }

  /** Get the pool utility for advanced operations. */
  getPoolUtil(): OpfsSAHPoolUtil {
    return this.poolUtil;
  }

  // ==========================================================================
  // Internals — unique constraint checking
  // ==========================================================================

  private checkFieldUnique(
    collection: string,
    index: FieldCheckIndex,
    data: Record<string, unknown>,
    excludeId: string | null,
  ): void {
    const fields = index.fields;
    const newValues = fields.map((f) => normalizeValue(data[f.field]));

    if (index.sparse && newValues.some((v) => v === null || v === undefined)) {
      return;
    }

    // Build SQL query for uniqueness check
    const conditions: string[] = ["collection = ?", "deleted = 0"];
    const binds: unknown[] = [collection];

    for (let i = 0; i < fields.length; i++) {
      const val = newValues[i];
      if (val === null || val === undefined) {
        conditions.push(`json_extract(data, '$.${fields[i].field}') IS NULL`);
      } else {
        conditions.push(`json_extract(data, '$.${fields[i].field}') = ?`);
        binds.push(val);
      }
    }

    if (excludeId) {
      conditions.push("id != ?");
      binds.push(excludeId);
    }

    const sql = `SELECT id FROM records WHERE ${conditions.join(" AND ")} LIMIT 1`;
    const stmt = this.db.prepare(sql);
    try {
      stmt.bind(binds);
      if (stmt.step()) {
        throw new Error(
          `Unique constraint violated on index "${index.name}" in collection "${collection}"`,
        );
      }
    } finally {
      stmt.finalize();
    }
  }

  private checkComputedUnique(
    collection: string,
    index: ComputedCheckIndex,
    computed: Record<string, unknown> | null,
    excludeId: string | null,
  ): void {
    if (!computed) return;
    const newValue = normalizeValue(computed[index.name]);

    if (index.sparse && (newValue === null || newValue === undefined)) {
      return;
    }

    const conditions: string[] = ["collection = ?", "deleted = 0"];
    const binds: unknown[] = [collection];

    if (newValue === null || newValue === undefined) {
      conditions.push(`json_extract(computed, '$.${index.name}') IS NULL`);
    } else {
      conditions.push(`json_extract(computed, '$.${index.name}') = ?`);
      binds.push(newValue);
    }

    if (excludeId) {
      conditions.push("id != ?");
      binds.push(excludeId);
    }

    const sql = `SELECT id FROM records WHERE ${conditions.join(" AND ")} LIMIT 1`;
    const stmt = this.db.prepare(sql);
    try {
      stmt.bind(binds);
      if (stmt.step()) {
        throw new Error(
          `Unique constraint violated on index "${index.name}" in collection "${collection}"`,
        );
      }
    } finally {
      stmt.finalize();
    }
  }
}
