/**
 * Orbyx: Supabase-js–style Postgres client.
 * Works with any Postgres (Neon, Supabase, self-hosted, etc.).
 */

export { createClient } from "./client";
export type { DbClient } from "./client";
export type {
  QueryResponse,
  ClientOptions,
  ConnectionConfig,
  PoolOptions,
  ClientHooks,
  RetryOptions,
  Database,
  TableDefinition,
  TableRow,
  PublicTableName,
} from "./types/index";
export type { DbError } from "./errors";
export { createError, isDbError, isRetriableError } from "./errors";
export { QueryBuilder } from "./builder/query-builder";
export type { FilterOperator } from "./builder/types";
