/**
 * Postgres client with Supabase-js–style API.
 * createClient(options) -> client.from(table, schema?) -> query builder,
 * plus client.sql() for raw queries and client.transaction() for transactions.
 */

import type { Pool, PoolClient } from "pg";
import { normalizeConfig, createPool, DEFAULT_SCHEMA } from "./connection";
import { createError, createErrorFromThrown } from "./errors";
import type {
  ClientOptions,
  PublicTableName,
  PublicFunctionName,
  QueryResponse,
  TableRow,
  TableInsert,
  TableUpdate,
  FunctionReturns,
  TransactionOptions,
} from "./types/index";
import { isRetriableError } from "./errors";
import { QueryBuilder } from "./builder/query-builder";
import { RealtimeChannel } from "./realtime";

export interface DbClient<
  _DB extends import("./types/index").Database =
    import("./types/index").Database,
> {
  /** Query a table (and optional schema). Returns a chainable builder. Typed when table is a key of default schema. */
  from<K extends PublicTableName<_DB>>(
    table: K,
    schema?: string,
  ): QueryBuilder<TableRow<_DB, K>, TableInsert<_DB, K>, TableUpdate<_DB, K>>;
  from(table: string, schema?: string): QueryBuilder;
  /** Call a typed Postgres function by name. Return type is inferred from Database.Functions. */
  rpc<FnName extends PublicFunctionName<_DB>>(
    fn: FnName,
    args?: unknown[] | Record<string, unknown>,
  ): QueryBuilder<
    FunctionReturns<_DB, FnName> extends Record<string, unknown>
      ? FunctionReturns<_DB, FnName>
      : Record<string, unknown>
  >;
  /** Call a Postgres function by name. Pass generic for return type: rpc<ReturnType>(fn, args). */
  rpc<T = Record<string, unknown>>(
    fn: string,
    args?: unknown[] | Record<string, unknown>,
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
   * Nested calls use SAVEPOINTs automatically.
   * Do not call tx.end(); the connection is released when the callback completes.
   */
  transaction<T>(
    callback: (tx: DbClient<_DB>) => Promise<T>,
    options?: TransactionOptions,
  ): Promise<QueryResponse<T>>;
  /**
   * Run a trivial query to verify the pool can get a connection.
   * Useful for readiness probes and startup checks.
   */
  healthCheck(): Promise<QueryResponse<unknown>>;
  /**
   * Subscribe to a Postgres LISTEN/NOTIFY channel.
   * Returns a RealtimeChannel. Call .on(handler).subscribe() to start listening.
   */
  channel(name: string): RealtimeChannel;
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
        hooks?.onQuery?.(text, params);
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
          hooks?.onError?.(dbErr);
          return { data: null, error: dbErr };
        }
        await sleep(Math.min(backoffMs * Math.pow(2, attempt), 30_000));
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
      args: unknown[] | Record<string, unknown> = [],
    ): QueryBuilder<T> {
      return QueryBuilder.forRpc<T>(fn, schema, ctx, args);
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
      options?: TransactionOptions,
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
        hooks?.onError?.(dbErr);
        return { data: null, error: dbErr };
      }

      /** Build a nested-transaction-capable tx client that shares `conn`. */
      function makeTxClient(conn: PoolClient): DbClient<DB> {
        const txCtx = { pool, client: conn, hooks };
        const txClient: DbClient<DB> = {
          from(table: string, schemaOverride?: string): QueryBuilder {
            const effectiveSchema = schemaOverride ?? schema;
            return new QueryBuilder(table, effectiveSchema, txCtx);
          },
          rpc<T = Record<string, unknown>>(
            fn: string,
            args: unknown[] | Record<string, unknown> = [],
          ): QueryBuilder<T> {
            return QueryBuilder.forRpc<T>(fn, schema, txCtx, args);
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
          async transaction<Tx>(
            cb: (tx: DbClient<DB>) => Promise<Tx>,
            options?: TransactionOptions,
          ): Promise<QueryResponse<Tx>> {
            // Nested transactions use SAVEPOINTs.
            // isolationLevel is not supported within a savepoint — ignore and warn.
            if (options?.isolationLevel) {
              const msg =
                `[Orbyx] Nested transaction: isolationLevel "${options.isolationLevel}" is ignored — ` +
                `isolation level is set on the outermost transaction only.`;
              if (hooks?.onWarning) {
                hooks.onWarning(msg, { event: "Nested transaction", isolationLevel: options.isolationLevel });
              } else {
                console.warn(msg);
              }
            }
            const spName = `sp_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
            try {
              await conn.query(`SAVEPOINT "${spName}"`);
              const value = await cb(makeTxClient(conn));
              await conn.query(`RELEASE SAVEPOINT "${spName}"`);
              return { data: value, error: null };
            } catch (err) {
              try {
                await conn.query(`ROLLBACK TO SAVEPOINT "${spName}"`);
                await conn.query(`RELEASE SAVEPOINT "${spName}"`);
              } catch {
                // Ignore savepoint cleanup errors
              }
              const dbErr = createError(
                "TRANSACTION",
                err instanceof Error ? err.message : "Nested transaction failed",
                err,
              );
              hooks?.onError?.(dbErr);
              return { data: null, error: dbErr };
            }
          },
          async healthCheck(): Promise<QueryResponse<unknown>> {
            return sqlImpl<unknown[]>("SELECT 1", [], conn);
          },
          channel(name: string): RealtimeChannel {
            return new RealtimeChannel(name, connection);
          },
          async end(): Promise<void> {
            // No-op; connection is released when the outer transaction completes.
          },
        };
        return txClient;
      }

      // Isolation level whitelist — prevent injection from the TypeScript union
      const ISO_MAP: Record<string, string> = {
        "read committed": "READ COMMITTED",
        "repeatable read": "REPEATABLE READ",
        serializable: "SERIALIZABLE",
      };
      let isoSql: string | null = null;
      if (options?.isolationLevel) {
        const mapped = ISO_MAP[options.isolationLevel];
        if (!mapped) {
          const isoErr = createError(
            "VALIDATION",
            `Invalid isolationLevel: "${options.isolationLevel}". Supported values: ${Object.keys(ISO_MAP).join(", ")}`,
          );
          conn.release();
          return { data: null, error: isoErr };
        }
        isoSql = mapped;
      }

      try {
        await conn.query(
          isoSql ? `BEGIN ISOLATION LEVEL ${isoSql}` : "BEGIN",
        );
        const value = await callback(makeTxClient(conn));
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
        hooks?.onError?.(dbErr);
        return { data: null, error: dbErr };
      } finally {
        conn.release();
      }
    },

    async healthCheck(): Promise<QueryResponse<unknown>> {
      return sqlImpl<unknown[]>("SELECT 1", []);
    },

    channel(name: string): RealtimeChannel {
      return new RealtimeChannel(name, connection);
    },

    async end(): Promise<void> {
      await pool.end();
    },
  };

  return client;
}
