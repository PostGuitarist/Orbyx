/**
 * Validation helpers for security: identifiers and connection/config.
 * Ensures no SQL injection via identifiers and no invalid config.
 */

import { createError } from "./errors";

/** PostgreSQL-safe identifier: unquoted style letters, digits, underscore. */
const IDENTIFIER_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Validates a single identifier (schema, table, column, function name).
 * Rejects empty, nullish, or strings containing anything other than [a-zA-Z0-9_].
 * Use for all user-supplied identifiers before building SQL.
 */
export function validateIdentifier(
  name: string,
  kind: "schema" | "table" | "column" | "function",
): void {
  if (typeof name !== "string" || name.length === 0) {
    throw createError(
      "VALIDATION",
      `Invalid ${kind}: must be a non-empty string`,
    );
  }
  if (name.length > 63) {
    throw createError(
      "VALIDATION",
      `Invalid ${kind}: PostgreSQL identifiers must be ≤63 characters`,
    );
  }
  if (!IDENTIFIER_REGEX.test(name)) {
    throw createError(
      "VALIDATION",
      `Invalid ${kind}: only letters, digits, and underscore allowed (got "${name.slice(0, 20)}${name.length > 20 ? "…" : ""}")`,
    );
  }
}

/**
 * Validates a comma-separated column list (e.g. "id, name" or "*").
 * "*" is allowed; otherwise each token must be a valid identifier.
 */
export function validateColumnList(columns: string): void {
  if (typeof columns !== "string" || columns.length === 0) {
    throw createError(
      "VALIDATION",
      "Invalid columns: must be a non-empty string",
    );
  }
  const trimmed = columns.trim();
  if (trimmed === "*") {
    return;
  }
  const parts = trimmed
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
  for (const p of parts) {
    validateIdentifier(p, "column");
  }
}

/**
 * Validates port number (1–65535).
 */
export function validatePort(port: number): void {
  if (
    typeof port !== "number" ||
    Number.isNaN(port) ||
    port < 1 ||
    port > 65535
  ) {
    throw createError(
      "VALIDATION",
      "Invalid port: must be a number between 1 and 65535",
    );
  }
}

/**
 * Validates limit/range values (non-negative integers).
 */
export function validateLimit(count: number): void {
  if (
    typeof count !== "number" ||
    Number.isNaN(count) ||
    count < 0 ||
    Math.floor(count) !== count
  ) {
    throw createError(
      "VALIDATION",
      "Invalid limit: must be a non-negative integer",
    );
  }
}

export function validateRange(from: number, to: number): void {
  if (
    typeof from !== "number" ||
    Number.isNaN(from) ||
    from < 0 ||
    Math.floor(from) !== from
  ) {
    throw createError(
      "VALIDATION",
      "Invalid range: from must be a non-negative integer",
    );
  }
  if (
    typeof to !== "number" ||
    Number.isNaN(to) ||
    to < 0 ||
    Math.floor(to) !== to
  ) {
    throw createError(
      "VALIDATION",
      "Invalid range: to must be a non-negative integer",
    );
  }
  if (from > to) {
    throw createError("VALIDATION", "Invalid range: from must be ≤ to");
  }
}
