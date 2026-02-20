/**
 * LessDb — main database class wrapping the WASM core.
 *
 * Usage:
 *   const db = new LessDb(myStorageBackend);
 *   db.initialize([users, tasks]);
 *   const record = db.put(users, { name: "Alice", email: "alice@example.com" });
 */

import type {
  StorageBackend,
  SchemaShape,
  CollectionDefHandle,
  CollectionRead,
  CollectionWrite,
  CollectionPatch,
  QueryOptions,
  QueryResult,
  PutOptions,
  GetOptions,
  DeleteOptions,
  ListOptions,
  BatchResult,
  BulkDeleteResult,
  ChangeEvent,
  RemoteRecord,
  ApplyRemoteOptions,
  PushSnapshot,
} from "./types.js";
import { serializeForRust, deserializeFromRust } from "./conversions.js";

// WASM module interface — the actual types come from wasm-pack build
interface WasmDb {
  new (backend: StorageBackend): WasmDb;
  initialize(defs: unknown[]): void;
  put(collection: string, data: unknown, options: unknown): unknown;
  get(collection: string, id: string, options: unknown): unknown;
  patch(collection: string, data: unknown, options: unknown): unknown;
  delete(collection: string, id: string, options: unknown): boolean;
  query(collection: string, query: unknown): { records: unknown[]; total?: number };
  count(collection: string, query: unknown): number;
  getAll(collection: string, options: unknown): unknown[];
  bulkPut(collection: string, records: unknown[], options: unknown): BatchResult<unknown>;
  bulkDelete(collection: string, ids: string[], options: unknown): BulkDeleteResult;
  observe(collection: string, id: string, callback: (data: unknown) => void): () => void;
  observeQuery(collection: string, query: unknown, callback: (result: unknown) => void): () => void;
  onChange(callback: (event: ChangeEvent) => void): () => void;
  getDirty(collection: string): unknown[];
  markSynced(collection: string, id: string, sequence: number, snapshot: unknown): void;
  applyRemoteChanges(collection: string, records: unknown[], options: unknown): unknown;
  getLastSequence(collection: string): number;
  setLastSequence(collection: string, sequence: number): void;
}

export class LessDb {
  private _wasm: WasmDb;
  private _schemas = new Map<string, SchemaShape>();

  constructor(backend: StorageBackend, WasmDbClass: new (backend: StorageBackend) => WasmDb) {
    this._wasm = new WasmDbClass(backend);
  }

  /** Initialize the database with collection definitions. */
  initialize(collections: CollectionDefHandle[]): void {
    for (const col of collections) {
      this._schemas.set(col.name, col.schema);
    }
    this._wasm.initialize(collections.map((c) => c._wasm));
  }

  // ========================================================================
  // CRUD
  // ========================================================================

  /** Insert or replace a record. */
  put<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    data: CollectionWrite<S>,
    options?: PutOptions,
  ): CollectionRead<S> {
    const serialized = serializeForRust(data as Record<string, unknown>);
    const result = this._wasm.put(def.name, serialized, options ?? null) as Record<string, unknown>;
    return deserializeFromRust(result, def.schema) as CollectionRead<S>;
  }

  /** Get a record by id. Returns null if not found. */
  get<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    id: string,
    options?: GetOptions,
  ): CollectionRead<S> | null {
    const result = this._wasm.get(def.name, id, options ?? null) as Record<string, unknown> | null;
    if (result === null) return null;
    return deserializeFromRust(result, def.schema) as CollectionRead<S>;
  }

  /** Partial update a record. */
  patch<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    data: CollectionPatch<S>,
    options?: Omit<PutOptions, "id">,
  ): CollectionRead<S> {
    const { id, ...fields } = data as Record<string, unknown> & { id: string };
    const serialized = serializeForRust(fields);
    const result = this._wasm.patch(
      def.name,
      serialized,
      { ...options, id },
    ) as Record<string, unknown>;
    return deserializeFromRust(result, def.schema) as CollectionRead<S>;
  }

  /** Delete a record. Returns true if the record existed. */
  delete<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    id: string,
    options?: DeleteOptions,
  ): boolean {
    return this._wasm.delete(def.name, id, options ?? null);
  }

  // ========================================================================
  // Query
  // ========================================================================

  /** Query records matching a filter. */
  query<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    query: QueryOptions,
  ): QueryResult<CollectionRead<S>> {
    const serializedFilter = query.filter ? serializeForRust(query.filter) : undefined;
    const result = this._wasm.query(def.name, {
      ...query,
      filter: serializedFilter,
    });
    return {
      records: (result.records as Record<string, unknown>[]).map(
        (r) => deserializeFromRust(r, def.schema) as CollectionRead<S>,
      ),
      total: result.total,
    };
  }

  /** Count records matching a query. */
  count<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    query?: QueryOptions,
  ): number {
    if (!query) return this._wasm.count(def.name, null);
    const serializedFilter = query.filter ? serializeForRust(query.filter) : undefined;
    return this._wasm.count(def.name, { ...query, filter: serializedFilter });
  }

  /** Get all records in a collection. */
  getAll<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    options?: ListOptions,
  ): CollectionRead<S>[] {
    const result = this._wasm.getAll(def.name, options ?? null) as Record<string, unknown>[];
    return result.map((r) => deserializeFromRust(r, def.schema) as CollectionRead<S>);
  }

  // ========================================================================
  // Bulk operations
  // ========================================================================

  /** Bulk insert records. */
  bulkPut<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    records: CollectionWrite<S>[],
    options?: PutOptions,
  ): BatchResult<CollectionRead<S>> {
    const serialized = records.map((r) => serializeForRust(r as Record<string, unknown>));
    const result = this._wasm.bulkPut(def.name, serialized, options ?? null);
    return {
      records: (result.records as Record<string, unknown>[]).map(
        (r) => deserializeFromRust(r, def.schema) as CollectionRead<S>,
      ),
      errors: result.errors,
    };
  }

  /** Bulk delete records by ids. */
  bulkDelete<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    ids: string[],
    options?: DeleteOptions,
  ): BulkDeleteResult {
    return this._wasm.bulkDelete(def.name, ids, options ?? null);
  }

  // ========================================================================
  // Observe (reactive subscriptions)
  // ========================================================================

  /** Observe a single record. Returns an unsubscribe function. */
  observe<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    id: string,
    callback: (record: CollectionRead<S> | null) => void,
  ): () => void {
    return this._wasm.observe(def.name, id, (data) => {
      if (data === null || data === undefined) {
        callback(null);
      } else {
        callback(
          deserializeFromRust(data as Record<string, unknown>, def.schema) as CollectionRead<S>,
        );
      }
    });
  }

  /** Observe a query. Returns an unsubscribe function. */
  observeQuery<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    query: QueryOptions,
    callback: (result: QueryResult<CollectionRead<S>>) => void,
  ): () => void {
    const serializedFilter = query.filter ? serializeForRust(query.filter) : undefined;
    return this._wasm.observeQuery(
      def.name,
      { ...query, filter: serializedFilter },
      (result) => {
        const r = result as { records: Record<string, unknown>[]; total: number };
        callback({
          records: r.records.map(
            (rec) => deserializeFromRust(rec, def.schema) as CollectionRead<S>,
          ),
          total: r.total,
        });
      },
    );
  }

  /** Register a global change listener. Returns an unsubscribe function. */
  onChange(callback: (event: ChangeEvent) => void): () => void {
    return this._wasm.onChange(callback);
  }

  // ========================================================================
  // Sync storage
  // ========================================================================

  /** Get dirty (unsynced) records. */
  getDirty<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
  ): CollectionRead<S>[] {
    const result = this._wasm.getDirty(def.name) as Record<string, unknown>[];
    return result.map((r) => deserializeFromRust(r, def.schema) as CollectionRead<S>);
  }

  /** Mark a record as synced. */
  markSynced<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    id: string,
    sequence: number,
    snapshot?: PushSnapshot,
  ): void {
    this._wasm.markSynced(def.name, id, sequence, snapshot ?? null);
  }

  /** Apply remote changes. */
  applyRemoteChanges<S extends SchemaShape>(
    def: CollectionDefHandle<string, S>,
    records: RemoteRecord[],
    options?: ApplyRemoteOptions,
  ): unknown {
    return this._wasm.applyRemoteChanges(def.name, records, options ?? {});
  }

  /** Get the last sync sequence for a collection. */
  getLastSequence(collection: string): number {
    return this._wasm.getLastSequence(collection);
  }

  /** Set the last sync sequence for a collection. */
  setLastSequence(collection: string, sequence: number): void {
    this._wasm.setLastSequence(collection, sequence);
  }
}
