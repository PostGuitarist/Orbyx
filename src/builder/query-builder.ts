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
import type { BuilderState, FilterOperator, NotOperator } from "./types";
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

/**
 * Splits a PostgREST-style OR string at top-level commas (respecting parentheses).
 * e.g. "name.eq.Alice,age.gt.10" → ["name.eq.Alice", "age.gt.10"]
 */
function splitOrSegments(s: string): string[] {
  const result: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === "(") depth++;
    else if (s[i] === ")") { if (depth > 0) depth--; }
    else if (s[i] === "," && depth === 0) {
      result.push(s.slice(start, i).trim());
      start = i + 1;
    }
  }
  result.push(s.slice(start).trim());
  return result.filter(Boolean);
}

/**
 * Parses a PostgREST-style OR filter string into filter objects.
 * Format per segment: "column.operator.value"
 * e.g. "name.eq.Alice,age.gt.10,active.is.true"
 */
function parseOrString(
  orStr: string,
): Array<{ column: string; op: string; value: unknown }> {
  return splitOrSegments(orStr).map((seg) => {
    const firstDot = seg.indexOf(".");
    if (firstDot === -1) throw new Error(`Invalid or() segment: "${seg}"`);
    const column = seg.slice(0, firstDot);
    const rest = seg.slice(firstDot + 1);
    const secondDot = rest.indexOf(".");
    if (secondDot === -1) throw new Error(`Invalid or() segment: "${seg}"`);
    const op = rest.slice(0, secondDot);
    const rawValue = rest.slice(secondDot + 1);
    let value: unknown = rawValue;
    if (rawValue === "null") value = null;
    else if (rawValue === "true") value = true;
    else if (rawValue === "false") value = false;
    else {
      const n = Number(rawValue);
      if (rawValue !== "" && !Number.isNaN(n)) value = n;
    }
    return { column, op, value };
  });
}

/** Allowed operators in PostgREST-style or() filter strings. */
const VALID_OR_OPS = new Set(["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "is"]);

/** Row/result type for select and rpc. Defaults to untyped record. */
export class QueryBuilder<
  TRow = Record<string, unknown>,
  TInsert = Record<string, unknown>,
  TUpdate = Record<string, unknown>,
> {
  private state: BuilderState;
  private ctx: QueryBuilderContext;

  constructor(table: string, schema: string, ctx: QueryBuilderContext) {
    validateIdentifier(schema, "schema");
    validateIdentifier(table, "table");
    this.state = createInitialState(table, schema);
    this.ctx = ctx;
  }

  /**
   * Factory for creating a QueryBuilder in RPC mode.
   * Makes the intent explicit — avoids passing the function name as a table identifier.
   *
   * "__rpc__" is a deliberate sentinel placeholder passed to the constructor as the
   * `table` argument to satisfy the required parameter; `state.table` is never used
   * for RPC queries — `state.rpcName` (set by rpc() below) is what drives compilation.
   */
  static forRpc<T = Record<string, unknown>>(
    fn: string,
    schema: string,
    ctx: QueryBuilderContext,
    args: unknown[] | Record<string, unknown> = [],
  ): QueryBuilder<T> {
    return new QueryBuilder<T, Record<string, unknown>, Record<string, unknown>>(
      "__rpc__",
      schema,
      ctx,
    ).rpc(fn, args) as QueryBuilder<T>;
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

  insert(
    values: TInsert | TInsert[],
  ): QueryBuilder<TRow, TInsert, TUpdate> {
    this.state.operation = "insert";
    this.state.insertValues = values as
      | Record<string, unknown>
      | Record<string, unknown>[];
    return this as QueryBuilder<TRow, TInsert, TUpdate>;
  }

  update(values: TUpdate): QueryBuilder<TRow, TInsert, TUpdate> {
    this.state.operation = "update";
    this.state.updateValues = values as Record<string, unknown>;
    return this as QueryBuilder<TRow, TInsert, TUpdate>;
  }

  /**
   * Upsert: insert or on conflict update. Supabase-style options.
   * @param options.onConflict - Column(s) for conflict detection (array or comma-separated string)
   * @param options.ignoreDuplicates - If true, DO NOTHING on conflict; else DO UPDATE
   */
  upsert(
    values: TInsert | TInsert[],
    options: {
      onConflict: string[] | string;
      ignoreDuplicates?: boolean;
    },
  ): QueryBuilder<TRow, TInsert, TUpdate> {
    this.state.operation = "upsert";
    this.state.upsertValues = values as
      | Record<string, unknown>
      | Record<string, unknown>[];
    this.state.upsertConflictColumns = Array.isArray(options.onConflict)
      ? options.onConflict
      : options.onConflict
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
    this.state.upsertIgnoreDuplicates = options.ignoreDuplicates === true;
    return this as QueryBuilder<TRow, TInsert, TUpdate>;
  }

  delete(): this {
    this.state.operation = "delete";
    return this;
  }

  rpc(fn: string, args: unknown[] | Record<string, unknown> = []): this {
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

  /**
   * OR filter. Accepts either a PostgREST-style filter string
   * (e.g. `'name.eq.Alice,age.gt.10'`) or an array of filter objects.
   */
  or(
    filters:
      | string
      | Array<{ column: string; op: FilterOperator; value: unknown }>,
  ): this {
    if (typeof filters === "string") {
      let parsed: ReturnType<typeof parseOrString>;
      try {
        parsed = parseOrString(filters);
      } catch (err) {
        throw createError(
          "VALIDATION",
          err instanceof Error ? err.message : "Invalid or() filter string",
          err,
        );
      }
      for (const entry of parsed) {
        if (!VALID_OR_OPS.has(entry.op)) {
          throw createError("VALIDATION", `Invalid or() operator: "${entry.op}"`);
        }
      }
      this.state.filters.push({ type: "or", orFilters: parsed });
    } else {
      this.state.filters.push({ type: "or", orFilters: filters });
    }
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

  /** Range column is strictly right of value. Uses >> operator. */
  rangeGt(column: string, range: string): this {
    this.state.filters.push({ type: "rangeGt", column, value: range });
    return this;
  }

  /** Range column is strictly left of value. Uses << operator. */
  rangeLt(column: string, range: string): this {
    this.state.filters.push({ type: "rangeLt", column, value: range });
    return this;
  }

  /** Range column does not extend left of value. Uses &> operator. */
  rangeGte(column: string, range: string): this {
    this.state.filters.push({ type: "rangeGte", column, value: range });
    return this;
  }

  /** Range column does not extend right of value. Uses &< operator. */
  rangeLte(column: string, range: string): this {
    this.state.filters.push({ type: "rangeLte", column, value: range });
    return this;
  }

  /** Range column is adjacent to value. Uses -|- operator. */
  rangeAdjacent(column: string, range: string): this {
    this.state.filters.push({ type: "rangeAdjacent", column, value: range });
    return this;
  }

  /**
   * Generic filter escape hatch. Equivalent to calling the specific operator method.
   * All FilterOperator values are supported.
   */
  filter(column: string, operator: FilterOperator, value: unknown): this {
    this.state.filters.push({ type: operator, column, value });
    return this;
  }

  /** Negate a filter. Supabase-style escape hatch. */
  not(column: string, operator: NotOperator, value: unknown): this {
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

  /** When called, execute() throws the DbError instead of returning { data: null, error }. */
  throwOnError(): this {
    this.state.throwOnError = true;
    return this;
  }

  /**
   * Run a count-only query: returns { data: null, count: N, error: null }.
   * No rows are fetched. Equivalent to Supabase head: true option.
   */
  head(): this {
    this.state.head = true;
    return this;
  }

  /**
   * Execute the query as EXPLAIN (FORMAT JSON) and return the query plan.
   * @param options.analyze - If true, runs EXPLAIN (ANALYZE, FORMAT JSON).
   *
   * **WARNING**: `analyze: true` actually *executes* the query to collect real
   * runtime statistics. Do not use `explain({ analyze: true })` on
   * INSERT/UPDATE/DELETE queries unless you intend to run them — they will
   * modify data.
   */
  async explain(options?: { analyze?: boolean }): Promise<QueryResponse<unknown[]>> {
    let text: string;
    let values: unknown[];
    try {
      const compiled = compile(this.state);
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
      this.ctx.hooks?.onError?.(dbErr);
      return { data: null, error: dbErr };
    }
    if (!text) {
      return {
        data: null,
        error: createError("VALIDATION", "Invalid query state for explain()"),
      };
    }
    const analyzeClause = options?.analyze ? ", ANALYZE" : "";
    const explainText = `EXPLAIN (FORMAT JSON${analyzeClause}) ${text}`;
    const runner = this.ctx.client ?? this.ctx.pool;
    try {
      this.ctx.hooks?.onQuery?.(explainText, values);
      const result = await runner.query(explainText, values);
      return { data: result.rows as unknown[], error: null };
    } catch (err) {
      const dbErr = createErrorFromThrown("QUERY", "explain() failed", err);
      this.ctx.hooks?.onError?.(dbErr);
      return { data: null, error: dbErr };
    }
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
      const compiled = compile(this.state);
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
      this.ctx.hooks?.onError?.(dbErr);
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
      this.ctx.hooks?.onQuery?.(text, values);
      const q = client.query(new PgQuery({ text, values }));

      const iterable: AsyncIterable<TRow> = {
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
      };
      return { data: iterable, error: null };
    } catch (err) {
      // Release the borrowed connection if query setup fails (before the iterable runs)
      if (borrowedClient) {
        borrowedClient.release();
        borrowedClient = null;
      }
      const dbErr = createErrorFromThrown("QUERY", "Unknown query error", err);
      this.ctx.hooks?.onError?.(dbErr);
      return { data: null, error: dbErr };
    }
  }

  async execute(): Promise<QueryResponse<TRow[]>> {
    let text: string;
    let values: unknown[];
    try {
      const compiled = compile(this.state);
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
      this.ctx.hooks?.onError?.(dbErr);
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

    // head() mode: count-only, no rows returned
    if (this.state.head) {
      if (this.state.operation !== "select") {
        const opErr = createError(
          "VALIDATION",
          "head() is only valid for select queries",
        );
        this.ctx.hooks?.onError?.(opErr);
        if (this.state.throwOnError) throw opErr;
        return { data: null, error: opErr };
      }
      let countText: string;
      let countValues: unknown[];
      try {
        const compiled = compileCount(this.state);
        countText = compiled.text;
        countValues = compiled.values;
      } catch (err) {
        const dbErr = isDbError(err)
          ? err
          : createError(
              "VALIDATION",
              err instanceof Error ? err.message : "Invalid query",
              err,
            );
        this.ctx.hooks?.onError?.(dbErr);
        if (this.state.throwOnError) throw dbErr;
        return { data: null, error: dbErr };
      }
      const headRunner = this.ctx.client ?? this.ctx.pool;
      const maxHeadAttempts = this.ctx.retries?.attempts ?? 1;
      const headBackoffMs = this.ctx.retries?.backoffMs ?? 100;
      if (maxHeadAttempts <= 0) {
        const zeroErr = createError("QUERY", "head() called with retries.attempts <= 0");
        this.ctx.hooks?.onError?.(zeroErr);
        if (this.state.throwOnError) throw zeroErr;
        return { data: null, error: zeroErr };
      }
      for (let attempt = 0; attempt < maxHeadAttempts; attempt++) {
        try {
          this.ctx.hooks?.onQuery?.(countText, countValues);
          const result = await headRunner.query(countText, countValues);
          const row = (result.rows[0] ?? {}) as { count: number };
          return { data: null, count: row.count ?? null, error: null };
        } catch (err) {
          const dbErr = createErrorFromThrown("QUERY", "head() count query failed", err);
          this.ctx.hooks?.onError?.(dbErr);
          if (attempt === maxHeadAttempts - 1 || !isRetriableError(err)) {
            if (this.state.throwOnError) throw dbErr;
            return { data: null, error: dbErr };
          }
          await sleep(Math.min(headBackoffMs * Math.pow(2, attempt), 30_000));
        }
      }
      // Unreachable under normal config (the loop always returns or throws above),
      // but satisfies the compiler when maxHeadAttempts > 0.
      /* istanbul ignore next */
      return { data: null, error: createError("QUERY", "head() max retries exceeded") };
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
          this.ctx.hooks?.onQuery?.(text, values);
          const result = (await runner.query(text, values)) as QueryResult;
          const rows = (result.rows ?? []) as TRow[];
          const pgRowCount = result.rowCount ?? undefined;

          const runCount = async (): Promise<number | null> => {
            if (!this.state.countOption || this.state.operation !== "select") {
              return null;
            }
            try {
              const useEstimated =
                this.state.countOption === "estimated" ||
                this.state.countOption === "planned";
              const { text: countText, values: countValues } = useEstimated
                ? compileCountEstimated(this.state)
                : compileCount(this.state);
              if (!countText) return null;
              this.ctx.hooks?.onQuery?.(countText, countValues);
              const countResult = (await runner.query(
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
            if (rows.length === 0) {
              const err = createError("PGRST116", "No rows returned for single()");
              this.ctx.hooks?.onError?.(err);
              if (this.state.throwOnError) throw err;
              return { data: null, error: err };
            }
            if (rows.length > 1) {
              const err = createError(
                "PGRST116",
                "Multiple rows returned for single()",
              );
              this.ctx.hooks?.onError?.(err);
              if (this.state.throwOnError) throw err;
              return { data: null, error: err };
            }
            const count = await runCount();
            return {
              data: rows[0],
              error: null,
              count: count ?? undefined,
              rowCount: pgRowCount,
            } as QueryResponse<TRow[]>;
          }
          if (this.state.maybeSingle) {
            if (rows && Array.isArray(rows) && rows.length > 1) {
              const err = createError(
                "PGRST116",
                "Multiple rows returned for maybeSingle()",
              );
              this.ctx.hooks?.onError?.(err);
              if (this.state.throwOnError) throw err;
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
              rowCount: pgRowCount,
            } as QueryResponse<TRow[]>;
          }

          const count = await runCount();
          return { data: rows, error: null, count: count ?? undefined, rowCount: pgRowCount };
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
        this.ctx.hooks?.onError?.(dbErr);
        if (attempt === maxAttempts - 1 || !isRetriableError(err)) {
          if (this.state.throwOnError) throw dbErr;
          return { data: null, error: dbErr };
        }
        await sleep(Math.min(backoffMs * Math.pow(2, attempt), 30_000));
      } finally {
        if (borrowedClient != null) {
          borrowedClient.release();
        }
      }
    }
    const maxErr = createError("QUERY", "Max retries exceeded");
    if (this.state.throwOnError) throw maxErr;
    return { data: null, error: maxErr };
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
