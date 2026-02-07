/**
 * Public types for the Postgres wrapper.
 */

import type { DbError } from "../errors";

/** Response shape for all queries: data or error, never both. Supabase-style optional count. */
export interface QueryResponse<T> {
  data: T | null;
  error: DbError | null;
  /** Present when .select(..., { count: 'exact' }) (or planned/estimated) was used. */
  count?: number | null;
}

/**
 * Database type map for typed clients.
 * Schema name -> table name -> { Row, Insert, Update }.
 * Example:
 *   interface Database {
 *     public: {
 *       Tables: {
 *         users: { Row: { id: number; name: string }; Insert: ...; Update: ... };
 *       };
 *     };
 *   }
 */
export interface Database {
  [schema: string]: {
    Tables?: {
      [table: string]: TableDefinition;
    };
  };
}

/** Per-table Row (select), Insert, Update. */
export interface TableDefinition {
  Row: Record<string, unknown>;
  Insert: Record<string, unknown>;
  Update: Record<string, unknown>;
}

type DefaultSchemaTables<DB extends Database> = DB["public"] extends {
  Tables?: infer T;
}
  ? T extends Record<string, TableDefinition>
    ? T
    : Record<string, TableDefinition>
  : Record<string, TableDefinition>;

/** Table names in the default schema "public". Use for typed from(). */
export type PublicTableName<DB extends Database> =
  keyof DefaultSchemaTables<DB>;

/** Helper: Row type for a table in default schema "public". */
export type TableRow<
  DB extends Database,
  TableName extends PublicTableName<DB>,
> = DefaultSchemaTables<DB>[TableName] extends TableDefinition
  ? DefaultSchemaTables<DB>[TableName]["Row"]
  : Record<string, unknown>;

/** Connection config as object (normalized from string or object). */
export interface ConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl?: boolean | { rejectUnauthorized?: boolean };
}

/** Pool options passed to pg.Pool. */
export interface PoolOptions {
  max?: number;
  min?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

/** Hooks for logging/telemetry. */
export interface ClientHooks {
  onQuery?: (sql: string, params: unknown[]) => void;
  onError?: (err: DbError) => void;
}

/** Options for retrying transient failures (e.g. connection errors, deadlock). */
export interface RetryOptions {
  /** Maximum number of attempts (including the first). */
  attempts: number;
  /** Base delay in ms before first retry; doubles each retry. Default 100. */
  backoffMs?: number;
}

/** Full client options (connection + schema + pool + hooks). */
export interface ClientOptions<DB extends Database = Database> {
  /** Connection: DSN string or config object. */
  connection: string | ConnectionConfig;
  /** Default schema for from(). Default "public". */
  schema?: string;
  /** Pool options. */
  pool?: PoolOptions;
  /** Optional hooks. */
  hooks?: ClientHooks;
  /** Optional retries for transient failures (connection, 08xxx, 40xxx). */
  retries?: RetryOptions;
  /** Reserved for generic Database type. */
  _database?: DB;
}
