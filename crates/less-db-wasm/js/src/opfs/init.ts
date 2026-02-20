/**
 * Worker entry point for the OPFS SQLite backend.
 *
 * Users create a tiny worker file that imports their collections and calls
 * this function:
 *
 * ```ts
 * // my-db-worker.ts
 * import { initOpfsWorker } from "less-db-wasm/worker";
 * import { users } from "./collections.js";
 * initOpfsWorker([users]);
 * ```
 *
 * The worker waits for an "open" request from the main thread with the
 * database name, then initializes WASM + SQLite + OPFS, and starts
 * handling requests.
 */

import type { CollectionDefHandle, CollectionBlueprint } from "../types.js";
import { BLUEPRINT } from "../types.js";
import type { MainToWorkerMessage, WorkerReady, WorkerResponse } from "./types.js";
import { SqliteStorageBackend } from "./SqliteStorageBackend.js";
import { OpfsWorkerHost } from "./OpfsWorkerHost.js";

export function initOpfsWorker(collections: CollectionDefHandle[]): void {
  // We need to listen for an "open" message with the database name.
  // Once received, we initialize everything and switch to the OpfsWorkerHost handler.
  self.onmessage = async (ev: MessageEvent<MainToWorkerMessage>) => {
    const msg = ev.data;

    if (msg.type !== "request" || msg.method !== "open") {
      const response: WorkerResponse = {
        type: "response",
        id: (msg as { id?: number }).id ?? 0,
        error: "Worker not initialized. Send 'open' request first.",
      };
      self.postMessage(response);
      return;
    }

    const requestId = msg.id;
    const dbName = msg.args[0] as string;

    try {
      // Load WASM
      const wasmModule = await import("../../../pkg/less_db_wasm.js");
      const { WasmDb, WasmCollectionBuilder } = wasmModule;

      // Create SQLite backend with OPFS persistence
      const backend = await SqliteStorageBackend.create(dbName);

      // Create WasmDb with the SQLite backend
      const wasm = new WasmDb(backend);

      // Build collection definitions from blueprints (same as LessDb.initialize)
      const wasmDefs: unknown[] = [];

      for (const col of collections) {
        const blueprint = (col as unknown as Record<symbol, CollectionBlueprint>)[BLUEPRINT];
        const builder = new WasmCollectionBuilder(col.name);

        for (const entry of blueprint.versions) {
          if (entry.version === 1) {
            builder.v1(entry.schema);
          } else {
            builder.v(entry.version, entry.schema, entry.migrate!);
          }
        }

        for (const idx of blueprint.indexes) {
          if (idx.type === "field") {
            builder.index(idx.fields, idx.options);
          } else {
            builder.computed(idx.name, idx.compute as (data: unknown) => unknown, idx.options);
          }
        }

        wasmDefs.push(builder.build());
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      wasm.initialize(wasmDefs as any);

      // Switch to the OpfsWorkerHost for all subsequent messages.
      // Pass a close callback to properly shut down SQLite.
      new OpfsWorkerHost(wasm, () => {
        backend.close();
      });

      // Respond to the open request
      const response: WorkerResponse = { type: "response", id: requestId, result: true };
      self.postMessage(response);

      // Signal ready
      const ready: WorkerReady = { type: "ready" };
      self.postMessage(ready);
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      const response: WorkerResponse = { type: "response", id: requestId, error };
      self.postMessage(response);
    }
  };
}
