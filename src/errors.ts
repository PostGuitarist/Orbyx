/**
 * Structured error type returned in query responses.
 * Callers can check `error.code` and `error.message` for handling.
 */
export interface DbError {
  /** Short machine-readable code (e.g. "CONNECTION", "QUERY", "VALIDATION"). */
  code: string;
  /** Human-readable message. */
  message: string;
  /** Optional underlying cause or DB details. */
  details?: unknown;
  /** Postgres SQLSTATE (e.g. "23505" unique violation, "23503" FK violation) when error is from Postgres. */
  pgCode?: string;
}

/**
 * Creates a DbError from a message and optional code/details/pgCode.
 */
export function createError(
  code: string,
  message: string,
  details?: unknown,
  pgCode?: string,
): DbError {
  const err: DbError = { code, message, details };
  if (pgCode != null && pgCode !== "") {
    err.pgCode = pgCode;
  }
  return err;
}

/**
 * Builds a DbError from a thrown value (e.g. from pg). Sets pgCode when the thrown value has a string .code (Postgres SQLSTATE).
 */
export function createErrorFromThrown(
  defaultCode: string,
  fallbackMessage: string,
  err: unknown,
): DbError {
  const message = err instanceof Error ? err.message : fallbackMessage;
  const pgCode =
    typeof (err as { code?: string }).code === "string"
      ? (err as { code: string }).code
      : undefined;
  return createError(defaultCode, message, err, pgCode);
}

/**
 * Type guard: value is DbError.
 */
export function isDbError(value: unknown): value is DbError {
  return (
    typeof value === "object" &&
    value !== null &&
    "code" in value &&
    "message" in value &&
    typeof (value as DbError).code === "string" &&
    typeof (value as DbError).message === "string"
  );
}

/** Node/connection error codes that are typically retriable. */
const RETRIABLE_NODE_CODES = new Set([
  "ECONNRESET",
  "ETIMEDOUT",
  "ECONNREFUSED",
  "ENOTFOUND",
  "EPIPE",
  "EAI_AGAIN",
]);

/**
 * Returns true if the error is typically transient and safe to retry (connection, deadlock, etc.).
 * Postgres: class 08 (connection exception), 40 (transaction rollback).
 */
export function isRetriableError(err: unknown): boolean {
  const code =
    typeof (err as { code?: string }).code === "string"
      ? (err as { code: string }).code
      : "";
  if (code.length >= 2) {
    const cls = code.slice(0, 2);
    if (cls === "08" || cls === "40") return true;
  }
  if (RETRIABLE_NODE_CODES.has(code)) return true;
  return false;
}
