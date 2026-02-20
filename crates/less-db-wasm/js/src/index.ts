/**
 * less-db-wasm — WASM-powered local-first document store.
 *
 * Usage:
 *   import { init, t, LessDb } from "less-db-wasm";
 *
 *   const { collection, LessDb } = await init();
 *
 *   const users = collection("users")
 *     .v(1, { name: t.string(), email: t.string() })
 *     .index(["email"], { unique: true })
 *     .build();
 *
 *   const db = new LessDb(myBackend);
 *   db.initialize([users]);
 *
 *   const record = db.put(users, { name: "Alice", email: "alice@example.com" });
 */

import type { StorageBackend } from "./types.js";

// Re-export schema builder
export { t } from "./schema.js";

// Re-export types
export type {
  // Schema
  SchemaNode,
  SchemaShape,
  StringSchema,
  TextSchema,
  NumberSchema,
  BooleanSchema,
  DateSchema,
  BytesSchema,
  OptionalSchema,
  ArraySchema,
  RecordSchema,
  ObjectSchema,
  LiteralSchema,
  UnionSchema,
  // Inferred types
  InferRead,
  InferWrite,
  CollectionRead,
  CollectionWrite,
  CollectionPatch,
  // Collection
  CollectionDefHandle,
  // Storage backend
  StorageBackend,
  SerializedRecord,
  RawBatchResult,
  ScanOptions,
  PurgeTombstonesOptions,
  // Query
  QueryOptions,
  QueryResult,
  SortDirection,
  SortEntry,
  // CRUD options
  PutOptions,
  GetOptions,
  PatchOptions,
  DeleteOptions,
  ListOptions,
  // Results
  BatchResult,
  BulkDeleteResult,
  RecordError,
  // Change events
  ChangeEvent,
  // Sync
  RemoteRecord,
  PushSnapshot,
  ApplyRemoteOptions,
  SyncTransport,
  OutboundRecord,
  PushAck,
  PullResult,
  PullFailure,
} from "./types.js";

// Re-export conversions for advanced use
export { serializeForRust, deserializeFromRust } from "./conversions.js";

// Re-export IndexedDB backend
export { IndexedDbBackend } from "./IndexedDbBackend.js";

// Re-export LessDb class
import { LessDb } from "./LessDb.js";
export { LessDb };

// Re-export collection factory
import { createCollectionFactory } from "./collection.js";
export { createCollectionFactory };

/**
 * Initialize the WASM module and return the bound API.
 *
 * This binds the WASM classes to the TypeScript wrappers. Call once at app startup.
 *
 * @param wasmModule - The WASM module exports (from `import * as wasm from "../pkg/less_db_wasm"`)
 *
 * @example
 * ```ts
 * import * as wasm from "../pkg/less_db_wasm";
 * import { createLessDb, t } from "less-db-wasm";
 *
 * const { collection, createDb } = createLessDb(wasm);
 *
 * const users = collection("users")
 *   .v(1, { name: t.string(), email: t.string() })
 *   .build();
 *
 * const db = createDb(myBackend);
 * db.initialize([users]);
 * ```
 */
export function createLessDb(wasmModule: {
  WasmDb: new (backend: unknown) => unknown;
  WasmCollectionBuilder: new (name: string) => unknown;
}) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const collection = createCollectionFactory(wasmModule.WasmCollectionBuilder as any);

  return {
    /** Collection definition builder. */
    collection,
    /** Create a new LessDb instance with a storage backend. */
    createDb: (backend: StorageBackend) =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      new LessDb(backend, wasmModule.WasmDb as any),
  };
}
