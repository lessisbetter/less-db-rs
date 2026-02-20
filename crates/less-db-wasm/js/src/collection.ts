/**
 * Collection builder — fluent API for defining versioned schemas.
 *
 * Usage:
 *   const users = collection("users")
 *     .v(1, { name: t.string(), email: t.string() })
 *     .index(["email"], { unique: true })
 *     .build();
 */

import type { SchemaShape, CollectionDefHandle } from "./types.js";

// ============================================================================
// WASM types (internal)
// ============================================================================

/** @internal */
export interface WasmCollectionBuilder {
  new (name: string): WasmCollectionBuilder;
  v1(schema: unknown): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  v(version: number, schema: unknown, migrate: (data: any) => any): void;
  index(fields: string[], options: unknown): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  computed(name: string, compute: (data: any) => any, options: unknown): void;
  build(): { readonly name: string; readonly currentVersion: number };
}

// ============================================================================
// Builder option types (exported for consumers)
// ============================================================================

export interface IndexOptions {
  name?: string;
  unique?: boolean;
  sparse?: boolean;
}

export interface ComputedOptions {
  unique?: boolean;
  sparse?: boolean;
}

// ============================================================================
// Builder interfaces (exported for type inference)
// ============================================================================

/** Collection builder before any versions are defined. */
export interface CollectionBuilderNoVersions<TName extends string> {
  /** Define the first version (v1) with its schema. */
  v<S extends SchemaShape>(version: 1, schema: S): CollectionBuilderWithVersions<TName, S>;
}

/** Collection builder after at least one version has been defined. */
export interface CollectionBuilderWithVersions<TName extends string, TSchema extends SchemaShape> {
  /** Add a new schema version with migration function. */
  v<S extends SchemaShape>(
    version: number,
    schema: S,
    migrate: (data: Record<string, unknown>) => Record<string, unknown>,
  ): CollectionBuilderWithVersions<TName, S>;

  /** Define a field index. */
  index(fields: string[], options?: IndexOptions): this;

  /** Define a computed index. */
  computed(
    name: string,
    compute: (data: Record<string, unknown>) => string | number | boolean | null,
    options?: ComputedOptions,
  ): this;

  /** Build the collection definition. */
  build(): CollectionDefHandle<TName, TSchema>;
}

// ============================================================================
// Implementation
// ============================================================================

interface VersionEntry {
  version: number;
  schema: SchemaShape;
  migrate?: (data: Record<string, unknown>) => Record<string, unknown>;
}

type AnyIndexEntry =
  | { type: "field"; fields: string[]; options: IndexOptions }
  | {
      type: "computed";
      name: string;
      compute: (data: Record<string, unknown>) => string | number | boolean | null;
      options: ComputedOptions;
    };

class CollectionBuilderNoVersionsImpl<TName extends string>
  implements CollectionBuilderNoVersions<TName>
{
  #name: TName;
  #wasmBuilderClass: new (name: string) => WasmCollectionBuilder;

  constructor(name: TName, wasmBuilderClass: new (name: string) => WasmCollectionBuilder) {
    this.#name = name;
    this.#wasmBuilderClass = wasmBuilderClass;
  }

  v<S extends SchemaShape>(version: 1, schema: S): CollectionBuilderWithVersions<TName, S> {
    return new CollectionBuilderWithVersionsImpl(
      this.#name,
      schema,
      [{ version: 1, schema }],
      [],
      this.#wasmBuilderClass,
    );
  }
}

class CollectionBuilderWithVersionsImpl<TName extends string, TSchema extends SchemaShape>
  implements CollectionBuilderWithVersions<TName, TSchema>
{
  #name: TName;
  #currentSchema: TSchema;
  #versions: VersionEntry[];
  #indexes: AnyIndexEntry[];
  #wasmBuilderClass: new (name: string) => WasmCollectionBuilder;

  constructor(
    name: TName,
    currentSchema: TSchema,
    versions: VersionEntry[],
    indexes: AnyIndexEntry[],
    wasmBuilderClass: new (name: string) => WasmCollectionBuilder,
  ) {
    this.#name = name;
    this.#currentSchema = currentSchema;
    this.#versions = versions;
    this.#indexes = indexes;
    this.#wasmBuilderClass = wasmBuilderClass;
  }

  v<S extends SchemaShape>(
    version: number,
    schema: S,
    migrate: (data: Record<string, unknown>) => Record<string, unknown>,
  ): CollectionBuilderWithVersions<TName, S> {
    return new CollectionBuilderWithVersionsImpl(
      this.#name,
      schema,
      [...this.#versions, { version, schema, migrate }],
      [],
      this.#wasmBuilderClass,
    );
  }

  index(fields: string[], options: IndexOptions = {}): this {
    this.#indexes.push({ type: "field", fields, options });
    return this;
  }

  computed(
    name: string,
    compute: (data: Record<string, unknown>) => string | number | boolean | null,
    options: ComputedOptions = {},
  ): this {
    this.#indexes.push({ type: "computed", name, compute, options });
    return this;
  }

  build(): CollectionDefHandle<TName, TSchema> {
    const builder = new this.#wasmBuilderClass(this.#name);

    for (const entry of this.#versions) {
      if (entry.version === 1) {
        builder.v1(entry.schema);
      } else {
        builder.v(entry.version, entry.schema, entry.migrate!);
      }
    }

    for (const idx of this.#indexes) {
      if (idx.type === "field") {
        builder.index(idx.fields, idx.options);
      } else {
        builder.computed(idx.name, idx.compute as (data: unknown) => unknown, idx.options);
      }
    }

    const wasmDef = builder.build();

    return {
      name: this.#name,
      currentVersion: wasmDef.currentVersion,
      schema: this.#currentSchema,
      _wasm: wasmDef,
    };
  }
}

// ============================================================================
// Public API
// ============================================================================

/** Create a collection builder factory bound to a WASM module. */
export function createCollectionFactory(
  WasmBuilder: new (name: string) => WasmCollectionBuilder,
): <TName extends string>(name: TName) => CollectionBuilderNoVersions<TName> {
  return function collection<TName extends string>(
    name: TName,
  ): CollectionBuilderNoVersions<TName> {
    return new CollectionBuilderNoVersionsImpl(name, WasmBuilder);
  };
}
