// Comparison benchmark: less-db-js vs Dexie.
//
// All operations are async (awaiting IDB transactions).
// WASM/OPFS benchmarks are not included here since they require a worker setup.

import { bench, describe } from "vitest";
import Dexie, { type Table } from "dexie";
import { generateUsers, type User } from "./shared.js";

// ---------------------------------------------------------------------------
// JS reference imports (aliased via vitest.bench.config.ts)
// ---------------------------------------------------------------------------
import {
  collection as jsCollection,
  t as jsT,
  IndexedDBAdapter,
} from "@less-platform/db";

// ---------------------------------------------------------------------------
// Collection definitions
// ---------------------------------------------------------------------------
const jsUsers = jsCollection("users")
  .v(1, {
    name: jsT.string(),
    email: jsT.string(),
    age: jsT.number(),
  })
  .index(["name"])
  .index(["age"])
  .build();

// ---------------------------------------------------------------------------
// Dexie types
// ---------------------------------------------------------------------------
interface DexieUser extends User {
  id?: number;
}

// ---------------------------------------------------------------------------
// JS reference lifecycle
// ---------------------------------------------------------------------------
let jsAdapter: IndexedDBAdapter;
let jsDbName: string;
let jsCounter = 0;
let jsInsertedIds: string[] = [];

async function setupJs() {
  jsDbName = `js-bench-${Date.now()}-${jsCounter++}`;
  jsAdapter = new IndexedDBAdapter(jsDbName);
  await jsAdapter.initialize([jsUsers]);
  jsInsertedIds = [];
}

async function teardownJs() {
  await jsAdapter.close();
  const req = indexedDB.deleteDatabase(jsDbName);
  await new Promise<void>((resolve, reject) => {
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

async function jsInsertUsers(count: number): Promise<string[]> {
  const data = generateUsers(count);
  const ids: string[] = [];
  for (const u of data) {
    const result = await jsAdapter.put(jsUsers, u);
    ids.push(result.id);
  }
  return ids;
}

// ---------------------------------------------------------------------------
// Dexie lifecycle
// ---------------------------------------------------------------------------
let dexieDb: Dexie & { users: Table<DexieUser, number> };
let dexieDbName: string;
let dexieCounter = 0;
let dexieInsertedIds: number[] = [];

async function setupDexie() {
  dexieDbName = `dexie-bench-${Date.now()}-${dexieCounter++}`;
  dexieDb = new Dexie(dexieDbName) as Dexie & { users: Table<DexieUser, number> };
  dexieDb.version(1).stores({ users: "++id, name, email, age" });
  await dexieDb.open();
  dexieInsertedIds = [];
}

async function teardownDexie() {
  dexieDb.close();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await Dexie.delete(dexieDbName);
}

// ===========================================================================
// Single operations
// ===========================================================================
describe("single operations", () => {
  // --- put (insert) ---
  bench(
    "js: put",
    async () => {
      await jsAdapter.put(jsUsers, { name: "test", email: "test@example.com", age: 25 });
    },
    { iterations: 50, warmupIterations: 5, setup: setupJs, teardown: teardownJs },
  );

  bench(
    "dexie: add",
    async () => {
      await dexieDb.users.add({ name: "test", email: "test@example.com", age: 25 });
    },
    { iterations: 50, warmupIterations: 5, setup: setupDexie, teardown: teardownDexie },
  );

  // --- get ---
  bench(
    "js: get",
    async () => {
      await jsAdapter.get(jsUsers, jsInsertedIds[0]!);
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupJs();
        jsInsertedIds = await jsInsertUsers(1);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: get",
    async () => {
      await dexieDb.users.get(1);
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.add({ name: "test", email: "test@example.com", age: 25 });
      },
      teardown: teardownDexie,
    },
  );

  // --- put (update) ---
  bench(
    "js: put (update)",
    async () => {
      const existing = await jsAdapter.get(jsUsers, jsInsertedIds[0]!);
      await jsAdapter.put(jsUsers, { ...existing!.data, age: 30 });
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupJs();
        jsInsertedIds = await jsInsertUsers(1);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: put (upsert)",
    async () => {
      await dexieDb.users.put({ id: 1, name: "test", email: "test@example.com", age: 30 });
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.add({ name: "test", email: "test@example.com", age: 25 });
      },
      teardown: teardownDexie,
    },
  );

  // --- patch ---
  bench(
    "js: patch",
    async () => {
      await jsAdapter.patch(jsUsers, { id: jsInsertedIds[0]!, age: 99 });
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupJs();
        jsInsertedIds = await jsInsertUsers(1);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: update (patch)",
    async () => {
      await dexieDb.users.update(1, { age: 99 });
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.add({ name: "test", email: "test@example.com", age: 25 });
      },
      teardown: teardownDexie,
    },
  );

  // --- delete ---
  bench(
    "js: delete",
    async () => {
      await jsAdapter.delete(jsUsers, jsInsertedIds[0]!);
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupJs();
        jsInsertedIds = await jsInsertUsers(1);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: delete",
    async () => {
      await dexieDb.users.delete(1);
    },
    {
      iterations: 50,
      warmupIterations: 5,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.add({ name: "test", email: "test@example.com", age: 25 });
      },
      teardown: teardownDexie,
    },
  );
});

// ===========================================================================
// Bulk operations
// ===========================================================================
describe("bulk operations", () => {
  // --- bulkPut 100 ---
  bench(
    "js: bulkPut 100",
    async () => {
      await jsAdapter.bulkPut(jsUsers, generateUsers(100));
    },
    { iterations: 20, warmupIterations: 2, setup: setupJs, teardown: teardownJs },
  );

  bench(
    "dexie: bulkAdd 100",
    async () => {
      await dexieDb.users.bulkAdd(generateUsers(100));
    },
    { iterations: 20, warmupIterations: 2, setup: setupDexie, teardown: teardownDexie },
  );

  // --- bulkPut 1000 ---
  bench(
    "js: bulkPut 1000",
    async () => {
      await jsAdapter.bulkPut(jsUsers, generateUsers(1000));
    },
    { iterations: 10, warmupIterations: 1, setup: setupJs, teardown: teardownJs },
  );

  bench(
    "dexie: bulkAdd 1000",
    async () => {
      await dexieDb.users.bulkAdd(generateUsers(1000));
    },
    { iterations: 10, warmupIterations: 1, setup: setupDexie, teardown: teardownDexie },
  );

  // --- getAll 100 ---
  bench(
    "js: getAll 100",
    async () => {
      await jsAdapter.getAll(jsUsers);
    },
    {
      iterations: 20,
      warmupIterations: 2,
      setup: async () => {
        await setupJs();
        await jsInsertUsers(100);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: toArray 100",
    async () => {
      await dexieDb.users.toArray();
    },
    {
      iterations: 20,
      warmupIterations: 2,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.bulkAdd(generateUsers(100));
      },
      teardown: teardownDexie,
    },
  );

  // --- getAll 1000 ---
  bench(
    "js: getAll 1000",
    async () => {
      await jsAdapter.getAll(jsUsers);
    },
    {
      iterations: 10,
      warmupIterations: 1,
      setup: async () => {
        await setupJs();
        await jsInsertUsers(1000);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: toArray 1000",
    async () => {
      await dexieDb.users.toArray();
    },
    {
      iterations: 10,
      warmupIterations: 1,
      setup: async () => {
        await setupDexie();
        await dexieDb.users.bulkAdd(generateUsers(1000));
      },
      teardown: teardownDexie,
    },
  );

  // --- bulkDelete 100 ---
  bench(
    "js: bulkDelete 100",
    async () => {
      await jsAdapter.bulkDelete(jsUsers, jsInsertedIds);
    },
    {
      iterations: 20,
      warmupIterations: 2,
      setup: async () => {
        await setupJs();
        jsInsertedIds = await jsInsertUsers(100);
      },
      teardown: teardownJs,
    },
  );

  bench(
    "dexie: bulkDelete 100",
    async () => {
      await dexieDb.users.bulkDelete(dexieInsertedIds);
    },
    {
      iterations: 20,
      warmupIterations: 2,
      setup: async () => {
        await setupDexie();
        dexieInsertedIds = (await dexieDb.users.bulkAdd(
          generateUsers(100),
          { allKeys: true },
        )) as number[];
      },
      teardown: teardownDexie,
    },
  );
});

// ===========================================================================
// Queries (1000 records)
// ===========================================================================
describe("queries (1000 records)", () => {
  const setupJsWith1000 = async () => {
    await setupJs();
    await jsInsertUsers(1000);
  };

  const setupDexieWith1000 = async () => {
    await setupDexie();
    await dexieDb.users.bulkAdd(generateUsers(1000));
  };

  // --- equals (indexed) ---
  bench(
    "js: query equals (indexed)",
    async () => {
      await jsAdapter.query(jsUsers, { filter: { age: 25 } });
    },
    { iterations: 30, warmupIterations: 3, setup: setupJsWith1000, teardown: teardownJs },
  );

  bench(
    "dexie: where equals (indexed)",
    async () => {
      await dexieDb.users.where("age").equals(25).toArray();
    },
    { iterations: 30, warmupIterations: 3, setup: setupDexieWith1000, teardown: teardownDexie },
  );

  // --- range (indexed) ---
  bench(
    "js: query range (indexed)",
    async () => {
      await jsAdapter.query(jsUsers, { filter: { age: { $gte: 20, $lt: 30 } } });
    },
    { iterations: 30, warmupIterations: 3, setup: setupJsWith1000, teardown: teardownJs },
  );

  bench(
    "dexie: where between (indexed)",
    async () => {
      await dexieDb.users.where("age").between(20, 30).toArray();
    },
    { iterations: 30, warmupIterations: 3, setup: setupDexieWith1000, teardown: teardownDexie },
  );

  // --- sort (indexed) ---
  bench(
    "js: query sort (indexed)",
    async () => {
      await jsAdapter.query(jsUsers, { sort: "age" });
    },
    { iterations: 30, warmupIterations: 3, setup: setupJsWith1000, teardown: teardownJs },
  );

  bench(
    "dexie: orderBy (sort)",
    async () => {
      await dexieDb.users.orderBy("age").toArray();
    },
    { iterations: 30, warmupIterations: 3, setup: setupDexieWith1000, teardown: teardownDexie },
  );

  // --- limit 10 ---
  bench(
    "js: query limit 10",
    async () => {
      await jsAdapter.query(jsUsers, { limit: 10 });
    },
    { iterations: 30, warmupIterations: 3, setup: setupJsWith1000, teardown: teardownJs },
  );

  bench(
    "dexie: limit 10",
    async () => {
      await dexieDb.users.limit(10).toArray();
    },
    { iterations: 30, warmupIterations: 3, setup: setupDexieWith1000, teardown: teardownDexie },
  );

  // --- count ---
  bench(
    "js: count",
    async () => {
      await jsAdapter.count(jsUsers);
    },
    { iterations: 30, warmupIterations: 3, setup: setupJsWith1000, teardown: teardownJs },
  );

  bench(
    "dexie: count",
    async () => {
      await dexieDb.users.count();
    },
    { iterations: 30, warmupIterations: 3, setup: setupDexieWith1000, teardown: teardownDexie },
  );
});
