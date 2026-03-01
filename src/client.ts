/**
 * Postgres client with Supabase-js–style API.
 * createClient(options) -> client.from(table, schema?) -> query builder,
 * plus client.sql() for raw queries and client.transaction() for transactions.
 */

import type { Pool, PoolClient } from "pg";
import { normalizeConfig, createPool, DEFAULT_SCHEMA } from "./connection";
import { createError, createErrorFromThrown } from "./errors";
import { safeInvokeOnQuery, safeInvokeOnError } from "./utils/hooks";
import type {
  ClientOptions,
  PublicTableName,
  QueryResponse,
  TableRow,
} from "./types/index";
import { isRetriableError } from "./errors";
import { QueryBuilder } from "./builder/query-builder";

export interface DbClient<
  _DB extends import("./types/index").Database =
    import("./types/index").Database,
> {
  /** Query a table (and optional schema). Returns a chainable builder. Typed when table is a key of default schema. */
  from<K extends PublicTableName<_DB>>(
    table: K,
    schema?: string,
  ): QueryBuilder<TableRow<_DB, K>>;
  from(table: string, schema?: string): QueryBuilder;
  /** Call a Postgres function by name. Optionally pass generic for return type: rpc<ReturnType>(fn, args). */
  rpc<T = Record<string, unknown>>(
    fn: string,
    args?: unknown[],
  ): QueryBuilder<T>;
  /** Run raw SQL with optional params. Returns { data, error }. */
  sql<T = unknown[]>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResponse<T>>;
  /** Alias for sql() for raw query execution. */
  raw<T = unknown[]>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResponse<T>>;
  /**
   * Run multiple operations in a single transaction.
   * Callback receives a client that uses one connection; BEGIN/COMMIT/ROLLBACK are handled.
   * Do not call tx.end(); the connection is released when the callback completes.
   */
  transaction<T>(
    callback: (tx: DbClient<_DB>) => Promise<T>,
  ): Promise<QueryResponse<T>>;
  /**
   * Run a trivial query to verify the pool can get a connection.
   * Useful for readiness probes and startup checks.
   */
  healthCheck(): Promise<QueryResponse<unknown>>;
  /** Close the underlying pool. */
  end(): Promise<void>;
}

/**
 * Creates a Postgres client that works with any provider (Neon, Supabase, self-hosted).
 * Accepts a connection string or config object; supports default schema, pool options, and hooks.
 *
 * @example
 * const db = createClient({
 *   connection: process.env.DATABASE_URL,
 *   schema: "public",
 *   pool: { max: 20 },
 *   hooks: { onQuery: (sql) => console.log(sql) },
 * });
 * const { data, error } = await db.from("users").select().eq("id", 1).single();
 */
export function createClient<
  DB extends import("./types/index").Database =
    import("./types/index").Database,
>(options: ClientOptions<DB>): DbClient<DB> {
  const connection = options.connection;
  const schema = options.schema ?? DEFAULT_SCHEMA;
  const poolOpts = options.pool ?? {};
  const hooks = options.hooks;

  const normalized = normalizeConfig(connection, poolOpts);
  const pool = createPool(normalized);
  const retries = options.retries;
  const ctx = { pool, hooks, retries };

  async function sqlImpl<T = unknown[]>(
    text: string,
    params: unknown[] = [],
    runner?: Pool | PoolClient,
  ): Promise<QueryResponse<T>> {
    const run = runner ?? pool;
    const maxAttempts = retries?.attempts ?? 1;
    const backoffMs = retries?.backoffMs ?? 100;
    const sleep = (ms: number): Promise<void> =>
      new Promise((r) => setTimeout(r, ms));
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        safeInvokeOnQuery(hooks, text, params);
        const result = await run.query(text, params);
        const data = result.rows as T;
        return { data, error: null };
      } catch (err) {
        const dbErr = createErrorFromThrown(
          "QUERY",
          "Unknown query error",
          err,
        );
        if (attempt === maxAttempts - 1 || !isRetriableError(err)) {
          safeInvokeOnError(hooks, dbErr);
          return { data: null, error: dbErr };
        }
        await sleep(backoffMs * Math.pow(2, attempt));
      }
    }
    return {
      data: null,
      error: createErrorFromThrown(
        "QUERY",
        "Max retries exceeded",
        new Error("Max retries exceeded"),
      ),
    };
  }

  const client: DbClient<DB> = {
    from(table: string, schemaOverride?: string): QueryBuilder {
      const effectiveSchema = schemaOverride ?? schema;
      return new QueryBuilder(table, effectiveSchema, ctx);
    },

    rpc<T = Record<string, unknown>>(
      fn: string,
      args: unknown[] = [],
    ): QueryBuilder<T> {
      return new QueryBuilder(fn, schema, ctx).rpc(fn, args) as QueryBuilder<T>;
    },

    sql<T = unknown[]>(
      text: string,
      params?: unknown[],
    ): Promise<QueryResponse<T>> {
      return sqlImpl<T>(text, params ?? []);
    },
    raw<T = unknown[]>(
      text: string,
      params?: unknown[],
    ): Promise<QueryResponse<T>> {
      return sqlImpl<T>(text, params ?? []);
    },

    async transaction<T>(
      callback: (tx: DbClient<DB>) => Promise<T>,
    ): Promise<QueryResponse<T>> {
      let conn: PoolClient;
      try {
        conn = await pool.connect();
      } catch (err) {
        const dbErr = createErrorFromThrown(
          "CONNECTION",
          "Failed to get connection for transaction",
          err,
        );
        safeInvokeOnError(hooks, dbErr);
        return { data: null, error: dbErr };
      }
      const txCtx = { pool, client: conn, hooks };
      const txClient: DbClient<DB> = {
        from(table: string, schemaOverride?: string): QueryBuilder {
          const effectiveSchema = schemaOverride ?? schema;
          return new QueryBuilder(table, effectiveSchema, txCtx);
        },
        rpc<T = Record<string, unknown>>(
          fn: string,
          args: unknown[] = [],
        ): QueryBuilder<T> {
          return new QueryBuilder(fn, schema, txCtx).rpc(
            fn,
            args,
          ) as QueryBuilder<T>;
        },
        sql<T = unknown[]>(
          text: string,
          params?: unknown[],
        ): Promise<QueryResponse<T>> {
          return sqlImpl<T>(text, params ?? [], conn);
        },
        raw<T = unknown[]>(
          text: string,
          params?: unknown[],
        ): Promise<QueryResponse<T>> {
          return sqlImpl<T>(text, params ?? [], conn);
        },
        transaction<Tx>(
          _cb: (tx: DbClient<DB>) => Promise<Tx>,
        ): Promise<QueryResponse<Tx>> {
          return Promise.resolve({
            data: null,
            error: createError(
              "VALIDATION",
              "Nested transactions are not supported; use savepoints in raw SQL if needed",
            ),
          });
        },
        async healthCheck(): Promise<QueryResponse<unknown>> {
          return sqlImpl<unknown[]>("SELECT 1", [], conn);
        },
        async end(): Promise<void> {
          // No-op; connection is released when transaction completes. Do not close the pool.
        },
      };
      try {
        await conn.query("BEGIN");
        const value = await callback(txClient);
        await conn.query("COMMIT");
        return { data: value, error: null };
      } catch (err) {
        try {
          await conn.query("ROLLBACK");
        } catch {
          // Ignore rollback errors
        }
        const dbErr = createError(
          "TRANSACTION",
          err instanceof Error ? err.message : "Transaction failed",
          err,
        );
        safeInvokeOnError(hooks, dbErr);
        return { data: null, error: dbErr };
      } finally {
        conn.release();
      }
    },

    async healthCheck(): Promise<QueryResponse<unknown>> {
      return sqlImpl<unknown[]>("SELECT 1", []);
    },

    async end(): Promise<void> {
      await pool.end();
    },
  };

  return client;
}
