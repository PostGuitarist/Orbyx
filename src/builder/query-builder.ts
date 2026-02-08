/**
 * Chainable query builder. Collects table, operation, filters, modifiers;
 * on await/then compiles to SQL, runs via pool, returns { data, error }.
 */

import type { Pool, PoolClient, QueryResult } from "pg";
import { Query as PgQuery } from "pg";
import {
  createError,
  createErrorFromThrown,
  isDbError,
  isRetriableError,
} from "../errors";
import { safeInvokeOnQuery, safeInvokeOnError } from "../utils/hooks";
import type { QueryResponse } from "../types/index";
import {
  validateIdentifier,
  validateLimit,
  validateRange,
} from "../validate";
import {
  compile,
  compileCount,
  compileCountEstimated,
  parseEstimatedCount,
} from "./compile";
import type { BuilderState, FilterOperator } from "./types";
import type { SafetyOptions } from "../types/index";
import { createInitialState } from "./types";
import type { ClientHooks, RetryOptions } from "../types/index";

/** Context for running queries: either pool (default) or a single client (e.g. transaction). */
export interface QueryBuilderContext {
  pool: Pool;
  /** When set (e.g. inside transaction), all queries use this client instead of the pool. */
  client?: PoolClient;
  hooks?: ClientHooks;
  /** When set, transient failures are retried with backoff. */
  retries?: RetryOptions;
  /** Safety limits inherited from client options. */
  safety?: SafetyOptions;
}

/** pg Client internals used to cancel an in-flight query (node-pg does not type these). */
interface PgClientWithCancel {
  processID: number | null;
  secretKey: number | null;
  connection: { cancel: (processID: number, secretKey: number) => void };
}

function cancelPgBackend(client: PoolClient): void {
  const c = client as unknown as PgClientWithCancel;
  if (
    c.processID != null &&
    c.secretKey != null &&
    typeof c.connection?.cancel === "function"
  ) {
    c.connection.cancel(c.processID, c.secretKey);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Row/result type for select and rpc. Defaults to untyped record. */
export class QueryBuilder<TRow = Record<string, unknown>> {
  private state: BuilderState;
  private ctx: QueryBuilderContext;

  constructor(table: string, schema: string, ctx: QueryBuilderContext) {
    validateIdentifier(schema, "schema");
    validateIdentifier(table, "table");
    this.state = createInitialState(table, schema);
    this.ctx = ctx;
  }

  /**
   * For SELECT: set columns to return and optional count. For insert/update/upsert/delete: set RETURNING columns.
   * @param columns - Column list or "*"
   * @param options - Supabase-style: count for select (exact/planned/estimated)
   */
  select(
    columns?: string,
    options?: { count?: "exact" | "planned" | "estimated" },
  ): this {
    if (this.state.operation === "select" || this.state.operation === "rpc") {
      this.state.selectColumns = columns ?? "*";
      if (options?.count) {
        this.state.countOption = options.count;
      }
    } else {
      this.state.returnSelect = columns ?? "*";
    }
    return this;
  }

  insert(values: Record<string, unknown> | Record<string, unknown>[]): this {
    this.state.operation = "insert";
    this.state.insertValues = values;
    return this;
  }

  update(values: Record<string, unknown>): this {
    this.state.operation = "update";
    this.state.updateValues = values;
    return this;
  }

  /**
   * Upsert: insert or on conflict update. Supabase-style options.
   * @param options.onConflict - Column(s) for conflict detection (array or comma-separated string)
   * @param options.ignoreDuplicates - If true, DO NOTHING on conflict; else DO UPDATE
   */
  upsert(
    values: Record<string, unknown> | Record<string, unknown>[],
    options: {
      onConflict: string[] | string;
      ignoreDuplicates?: boolean;
    },
  ): this {
    this.state.operation = "upsert";
    this.state.upsertValues = values;
    this.state.upsertConflictColumns = Array.isArray(options.onConflict)
      ? options.onConflict
      : options.onConflict
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
    this.state.upsertIgnoreDuplicates = options.ignoreDuplicates === true;
    return this;
  }

  delete(): this {
    this.state.operation = "delete";
    return this;
  }

  rpc(fn: string, args: unknown[] = []): this {
    this.state.operation = "rpc";
    this.state.rpcName = fn;
    this.state.rpcArgs = args;
    return this;
  }

  eq(column: string, value: unknown): this {
    this.state.filters.push({ type: "eq", column, value });
    return this;
  }

  neq(column: string, value: unknown): this {
    this.state.filters.push({ type: "neq", column, value });
    return this;
  }

  gt(column: string, value: unknown): this {
    this.state.filters.push({ type: "gt", column, value });
    return this;
  }

  gte(column: string, value: unknown): this {
    this.state.filters.push({ type: "gte", column, value });
    return this;
  }

  lt(column: string, value: unknown): this {
    this.state.filters.push({ type: "lt", column, value });
    return this;
  }

  lte(column: string, value: unknown): this {
    this.state.filters.push({ type: "lte", column, value });
    return this;
  }

  like(column: string, pattern: string): this {
    this.state.filters.push({ type: "like", column, value: pattern });
    return this;
  }

  ilike(column: string, pattern: string): this {
    this.state.filters.push({ type: "ilike", column, value: pattern });
    return this;
  }

  is(column: string, value: null | boolean): this {
    this.state.filters.push({ type: "is", column, value });
    return this;
  }

  in(column: string, values: unknown[]): this {
    this.state.filters.push({ type: "in", column, value: values });
    return this;
  }

  or(
    filters: Array<{
      column: string;
      op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte";
      value: unknown;
    }>,
  ): this {
    this.state.filters.push({
      type: "or",
      orFilters: filters,
    });
    return this;
  }

  match(query: Record<string, unknown>): this {
    this.state.filters.push({ type: "match", matchRecord: query });
    return this;
  }

  notIn(column: string, values: unknown[]): this {
    this.state.filters.push({ type: "notIn", column, value: values });
    return this;
  }

  isDistinct(column: string, value: unknown): this {
    this.state.filters.push({ type: "isDistinct", column, value });
    return this;
  }

  /** Column contains value (array/jsonb/range). Uses @> operator. */
  contains(
    column: string,
    value: string | unknown[] | Record<string, unknown>,
  ): this {
    this.state.filters.push({ type: "contains", column, value });
    return this;
  }

  /** Column is contained by value. Uses <@ operator. */
  containedBy(
    column: string,
    value: string | unknown[] | Record<string, unknown>,
  ): this {
    this.state.filters.push({ type: "containedBy", column, value });
    return this;
  }

  /** Column overlaps value (array/range). Uses && operator. */
  overlaps(column: string, value: string | unknown[]): this {
    this.state.filters.push({ type: "overlaps", column, value });
    return this;
  }

  /** Negate a filter. Supabase-style escape hatch. */
  not(column: string, operator: FilterOperator, value: unknown): this {
    this.state.filters.push({
      type: "not",
      column,
      value,
      notOperator: operator,
    });
    return this;
  }

  /**
   * Full-text search on text/tsvector column. Supabase-style.
   * @param options.config - Text search config (e.g. "english")
   * @param options.type - plain (default), phrase, or websearch
   */
  textSearch(
    column: string,
    query: string,
    options?: { config?: string; type?: "plain" | "phrase" | "websearch" },
  ): this {
    this.state.filters.push({
      type: "textSearch",
      column,
      value: query,
      textSearchConfig: options?.config,
      textSearchType: options?.type,
    });
    return this;
  }

  /** Order by column; chain multiple .order() calls for multiple columns. */
  order(
    column: string,
    options?: { ascending?: boolean; nullsFirst?: boolean },
  ): this {
    this.state.orderBy.push({
      column,
      ascending: options?.ascending !== false,
      nullsFirst: options?.nullsFirst,
    });
    return this;
  }

  /** Set AbortSignal for the request (Supabase-style). */
  abortSignal(signal: AbortSignal): this {
    this.state.abortSignal = signal;
    return this;
  }

  limit(count: number): this {
    validateLimit(count);
    this.state.limitCount = count;
    return this;
  }

  range(from: number, to: number): this {
    validateRange(from, to);
    this.state.rangeFrom = from;
    this.state.rangeTo = to;
    return this;
  }

  single(): this {
    this.state.single = true;
    return this;
  }

  maybeSingle(): this {
    this.state.maybeSingle = true;
    return this;
  }

  /**
   * Explicitly set RETURNING columns after insert/update/upsert/delete (alternative to .select()).
   */
  selectReturn(columns?: string): this {
    this.state.returnSelect = columns ?? "*";
    return this;
  }

  /**
   * Execute the SELECT as a stream of rows. Holds a connection until iteration completes.
   * Only valid for select operations. Use for large result sets to avoid loading all rows into memory.
   */
  async stream(): Promise<QueryResponse<AsyncIterable<TRow>>> {
    if (this.state.operation !== "select") {
      return {
        data: null,
        error: createError(
          "VALIDATION",
          "stream() is only valid for select queries",
        ),
      };
    }
    let text: string;
    let values: unknown[];
    try {
      const compiled = compile(this.state, this.ctx.safety);
      text = compiled.text;
      values = compiled.values;
    } catch (err) {
      const dbErr = isDbError(err)
        ? err
        : createError(
            "VALIDATION",
            err instanceof Error ? err.message : "Invalid query",
            err,
          );
      safeInvokeOnError(this.ctx.hooks, dbErr);
      return { data: null, error: dbErr };
    }
    if (!text) {
      return {
        data: null,
        error: createError(
          "VALIDATION",
          "Invalid query state: missing operation or values",
        ),
      };
    }

    let borrowedClient: PoolClient | null = null;
    try {
      const client = this.ctx.client ?? (await this.ctx.pool.connect());
      if (!this.ctx.client) {
        borrowedClient = client;
      }
      // If statement timeout is configured, ensure we run on a client with SET LOCAL
      if (this.ctx.safety?.statementTimeoutMs != null) {
        const timeout = this.ctx.safety.statementTimeoutMs;
        try {
          await client.query("SET LOCAL statement_timeout = $1", [
            String(timeout),
          ]);
        } catch {
          // ignore; proceed without timeout if not supported
        }
      }
      safeInvokeOnQuery(this.ctx.hooks, text, values);
      const q = client.query(new PgQuery({ text, values }));

      let closed = false;
      const iterable: any = {
        async *[Symbol.asyncIterator](): AsyncGenerator<TRow> {
          try {
            const queue: TRow[] = [];
            let resolveNext: (() => void) | null = null;
            let done = false;
            let err: Error | null = null;
            q.on("row", (row: TRow) => {
              queue.push(row);
              if (resolveNext) {
                const r = resolveNext;
                resolveNext = null;
                r();
              }
            });
            q.on("end", () => {
              done = true;
              if (resolveNext) {
                const r = resolveNext;
                resolveNext = null;
                r();
              }
            });
            q.on("error", (e: Error) => {
              err = e;
              done = true;
              if (resolveNext) {
                const r = resolveNext;
                resolveNext = null;
                r();
              }
            });
            while (!done || queue.length > 0) {
              if (queue.length > 0) {
                yield queue.shift() as TRow;
              } else if (err) {
                throw err;
              } else {
                await new Promise<void>((r) => {
                  resolveNext = r;
                });
              }
            }
          } finally {
            if (borrowedClient) {
              borrowedClient.release();
            }
          }
        },
        close(): void {
          if (closed) return;
          closed = true;
          try {
            // cancel running query and release client
            cancelPgBackend(client as PoolClient);
          } catch {}
          try {
            if (borrowedClient) {
              borrowedClient.release();
            }
          } catch {}
        },
      };
      return { data: iterable, error: null };
    } catch (err) {
      const dbErr = createErrorFromThrown("QUERY", "Unknown query error", err);
      safeInvokeOnError(this.ctx.hooks, dbErr);
      return { data: null, error: dbErr };
    }
  }

  async execute(): Promise<QueryResponse<TRow[]>> {
    let text: string;
    let values: unknown[];
    try {
      const compiled = compile(this.state, this.ctx.safety);
      text = compiled.text;
      values = compiled.values;
    } catch (err) {
      const dbErr = isDbError(err)
        ? err
        : createError(
            "VALIDATION",
            err instanceof Error ? err.message : "Invalid query",
            err,
          );
      safeInvokeOnError(this.ctx.hooks, dbErr);
      return { data: null, error: dbErr };
    }
    if (!text) {
      return {
        data: null,
        error: createError(
          "VALIDATION",
          "Invalid query state: missing operation or values",
        ),
      };
    }

    const maxAttempts = this.ctx.retries?.attempts ?? 1;
    const backoffMs = this.ctx.retries?.backoffMs ?? 100;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      let borrowedClient: PoolClient | null = null;
      try {
        const needCancel = this.state.abortSignal != null;
        if (needCancel && this.ctx.client == null) {
          borrowedClient = await this.ctx.pool.connect();
        }
        const runner = this.ctx.client ?? borrowedClient ?? this.ctx.pool;
        const isClientRunner =
          this.ctx.client != null || borrowedClient != null;

        const runQuery = async (): Promise<QueryResponse<TRow[]>> => {
          // If statement timeout is configured, ensure we run on a client and set LOCAL timeout.
          let effectiveRunner: Pool | PoolClient = runner;
          if (this.ctx.safety?.statementTimeoutMs != null) {
            const timeout = this.ctx.safety.statementTimeoutMs;
            if (effectiveRunner === this.ctx.pool) {
              borrowedClient = await this.ctx.pool.connect();
              effectiveRunner = borrowedClient;
            }
            try {
              await (effectiveRunner as PoolClient).query(
                "SET LOCAL statement_timeout = $1",
                [String(timeout)],
              );
            } catch {
              // ignore if not supported
            }
          }
          safeInvokeOnQuery(this.ctx.hooks, text, values);
          const result = (await (effectiveRunner as PoolClient).query(
            text,
            values,
          )) as QueryResult;
          const rows = (result.rows ?? []) as TRow[];

          const runCount = async (): Promise<number | null> => {
            if (!this.state.countOption || this.state.operation !== "select") {
              return null;
            }
            try {
              const useEstimated =
                this.state.countOption === "estimated" ||
                this.state.countOption === "planned";
              const { text: countText, values: countValues } = useEstimated
                ? compileCountEstimated(this.state, this.ctx.safety)
                : compileCount(this.state, this.ctx.safety);
              if (!countText) return null;
              safeInvokeOnQuery(this.ctx.hooks, countText, countValues);
              const countResult = (await (effectiveRunner as PoolClient).query(
                countText,
                countValues,
              )) as QueryResult;
              const rows = countResult.rows ?? [];
              if (useEstimated) {
                return parseEstimatedCount(rows as unknown[]);
              }
              const countRow = rows[0] as { count: number } | undefined;
              return countRow?.count ?? null;
            } catch {
              return null;
            }
          };

          if (this.state.single) {
            if (rows && Array.isArray(rows) && rows.length > 1) {
              const err = createError(
                "PGRST116",
                "Multiple rows returned for single()",
              );
              safeInvokeOnError(this.ctx.hooks, err);
              return { data: null, error: err };
            }
            const data = (
              Array.isArray(rows) && rows.length === 1 ? rows[0] : rows
            ) as TRow;
            const count = await runCount();
            return {
              data,
              error: null,
              count: count ?? undefined,
            } as QueryResponse<TRow[]>;
          }
          if (this.state.maybeSingle) {
            if (rows && Array.isArray(rows) && rows.length > 1) {
              const err = createError(
                "PGRST116",
                "Multiple rows returned for maybeSingle()",
              );
              safeInvokeOnError(this.ctx.hooks, err);
              return { data: null, error: err };
            }
            const data = (
              Array.isArray(rows) && rows.length === 1
                ? rows[0]
                : Array.isArray(rows) && rows.length === 0
                  ? null
                  : rows
            ) as TRow | TRow[] | null;
            const count = await runCount();
            return {
              data,
              error: null,
              count: count ?? undefined,
            } as QueryResponse<TRow[]>;
          }

          const count = await runCount();
          return { data: rows, error: null, count: count ?? undefined };
        };

        let response: QueryResponse<TRow[]>;
        if (isClientRunner && this.state.abortSignal != null) {
          const signal = this.state.abortSignal;
          const abortPromise = new Promise<QueryResponse<TRow[]>>(
            (_, reject) => {
              if (signal.aborted) {
                cancelPgBackend(runner as PoolClient);
                reject(createError("QUERY", "Query aborted"));
                return;
              }
              signal.addEventListener(
                "abort",
                () => {
                  cancelPgBackend(runner as PoolClient);
                  reject(createError("QUERY", "Query aborted"));
                },
                { once: true },
              );
            },
          );
          response = await Promise.race([runQuery(), abortPromise]);
        } else {
          response = await runQuery();
        }
        return response;
      } catch (err) {
        const dbErr = isDbError(err)
          ? err
          : createErrorFromThrown("QUERY", "Unknown query error", err);
        safeInvokeOnError(this.ctx.hooks, dbErr);
        if (attempt === maxAttempts - 1 || !isRetriableError(err)) {
          return { data: null, error: dbErr };
        }
        await sleep(backoffMs * Math.pow(2, attempt));
      } finally {
        if (borrowedClient != null) {
          borrowedClient.release();
        }
      }
    }
    return { data: null, error: createError("QUERY", "Max retries exceeded") };
  }

  then<TResult1 = QueryResponse<TRow[]>, TResult2 = never>(
    onfulfilled?: (
      value: QueryResponse<TRow[]>,
    ) => TResult1 | PromiseLike<TResult1>,
    onrejected?: (reason: unknown) => TResult2 | PromiseLike<TResult2>,
  ): Promise<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}
