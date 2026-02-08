/**
 * Compiles BuilderState into parameterized SQL and values for pg.
 * All user-derived identifiers are validated and quoted to avoid injection.
 */

import { validateIdentifier, validateColumnList } from "../validate";
import { createError } from "../errors";
import type { SafetyOptions } from "../types/index";
import type { BuilderState } from "./types";

// Default limits to prevent very large parameter lists or IN-lists which could be used
// to cause excessive memory/CPU usage or very long queries.
const DEFAULT_MAX_IN_ELEMENTS = 1000;
const DEFAULT_MAX_TOTAL_PARAMS = 5000;

/** Quote identifier for PostgreSQL (e.g. table, column names). Safe after validateIdentifier. */
function quoteId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** Validates schema and table, then returns quoted schema.table. */
function quotedTableSafe(schema: string, table: string): string {
  validateIdentifier(schema, "schema");
  validateIdentifier(table, "table");
  return [quoteId(schema), quoteId(table)].join(".");
}

/**
 * Builds WHERE clause and appends params to the given array.
 * Returns the WHERE sql and the next param index.
 */
function buildWhere(
  state: BuilderState,
  params: unknown[],
  startIndex: number,
  safety?: SafetyOptions,
): { sql: string; nextIndex: number } {
  if (state.filters.length === 0) {
    return { sql: "", nextIndex: startIndex };
  }
  let idx = startIndex;
  const parts: string[] = [];
  for (const f of state.filters) {
    if (f.type === "or" && f.orFilters && f.orFilters.length > 0) {
      const orParts = f.orFilters.map((o) => {
        validateIdentifier(o.column, "column");
        const col = quoteId(o.column);
        const ph = `$${idx}`;
        idx += 1;
        params.push(o.value);
        const op =
          o.op === "eq"
            ? "="
            : o.op === "neq"
              ? "<>"
              : o.op === "gt"
                ? ">"
                : o.op === "gte"
                  ? ">="
                  : o.op === "lt"
                    ? "<"
                    : o.op === "lte"
                      ? "<="
                      : "=";
        return `${col} ${op} ${ph}`;
      });
      parts.push("(" + orParts.join(" OR ") + ")");
      continue;
    }
    if (
      f.type === "match" &&
      f.matchRecord &&
      Object.keys(f.matchRecord).length > 0
    ) {
      const andParts = Object.entries(f.matchRecord).map(([col, val]) => {
        validateIdentifier(col, "column");
        const ph = `$${idx}`;
        idx += 1;
        params.push(val);
        return `${quoteId(col)} = ${ph}`;
      });
      parts.push("(" + andParts.join(" AND ") + ")");
      continue;
    }
    if (f.column) {
      validateIdentifier(f.column, "column");
    }
    const col = f.column ? quoteId(f.column) : "";
    const ph = `$${idx}`;
    if (f.type === "in" && Array.isArray(f.value)) {
      const maxIn = safety?.maxInElements ?? DEFAULT_MAX_IN_ELEMENTS;
      if (f.value.length > maxIn) {
        throw createError(
          "VALIDATION",
          `IN list too large: limit ${maxIn} elements`,
        );
      }
      const inStart = idx;
      f.value.forEach((v) => {
        params.push(v);
        idx += 1;
      });
      const placeholders = f.value.map((_, i) => `$${inStart + i}`).join(", ");
      parts.push(`${col} IN (${placeholders})`);
      continue;
    }
    if (f.type === "notIn" && Array.isArray(f.value)) {
      const maxIn = safety?.maxInElements ?? DEFAULT_MAX_IN_ELEMENTS;
      if (f.value.length > maxIn) {
        throw createError(
          "VALIDATION",
          `NOT IN list too large: limit ${maxIn} elements`,
        );
      }
      const inStart = idx;
      f.value.forEach((v) => {
        params.push(v);
        idx += 1;
      });
      const placeholders = f.value.map((_, i) => `$${inStart + i}`).join(", ");
      parts.push(`${col} NOT IN (${placeholders})`);
      continue;
    }
    if (f.type === "textSearch") {
      const config = f.textSearchConfig ?? "english";
      const queryVal = f.value;
      const tsStart = idx;
      params.push(config);
      idx += 1;
      params.push(queryVal);
      idx += 1;
      const configPh = `$${tsStart}`;
      const queryPh = `$${tsStart + 1}`;
      const fn =
        f.textSearchType === "phrase"
          ? "phraseto_tsquery"
          : f.textSearchType === "websearch"
            ? "websearch_to_tsquery"
            : "plainto_tsquery";
      parts.push(
        `to_tsvector(${configPh}::regconfig, ${col}) @@ ${fn}(${configPh}::regconfig, ${queryPh})`,
      );
      continue;
    }
    if (f.type === "not" && f.notOperator !== undefined) {
      idx += 1;
      params.push(f.value);
      const notOp =
        f.notOperator === "eq"
          ? "="
          : f.notOperator === "neq"
            ? "<>"
            : f.notOperator === "gt"
              ? ">"
              : f.notOperator === "gte"
                ? ">="
                : f.notOperator === "lt"
                  ? "<"
                  : f.notOperator === "lte"
                    ? "<="
                    : f.notOperator === "like"
                      ? "LIKE"
                      : f.notOperator === "ilike"
                        ? "ILIKE"
                        : f.notOperator === "is"
                          ? "IS"
                          : "=";
      parts.push(`NOT (${col} ${notOp} ${ph})`);
      continue;
    }
    idx += 1;
    params.push(f.value);
    switch (f.type) {
      case "eq":
        parts.push(`${col} = ${ph}`);
        break;
      case "neq":
        parts.push(`${col} <> ${ph}`);
        break;
      case "gt":
        parts.push(`${col} > ${ph}`);
        break;
      case "gte":
        parts.push(`${col} >= ${ph}`);
        break;
      case "lt":
        parts.push(`${col} < ${ph}`);
        break;
      case "lte":
        parts.push(`${col} <= ${ph}`);
        break;
      case "like":
        parts.push(`${col} LIKE ${ph}`);
        break;
      case "ilike":
        parts.push(`${col} ILIKE ${ph}`);
        break;
      case "is":
        parts.push(`${col} IS ${ph}`);
        break;
      case "isDistinct":
        parts.push(`${col} IS DISTINCT FROM ${ph}`);
        break;
      case "contains":
        parts.push(`${col} @> ${ph}`);
        break;
      case "containedBy":
        parts.push(`${col} <@ ${ph}`);
        break;
      case "overlaps":
        parts.push(`${col} && ${ph}`);
        break;
      default:
        break;
    }
  }
  return { sql: " WHERE " + parts.join(" AND "), nextIndex: idx };
}

/**
 * Compiles state into { text, values } for pg.query().
 */
export function compile(state: BuilderState, safety?: SafetyOptions): {
  text: string;
  values: unknown[];
} {
  const values: unknown[] = [];
  let paramIndex = 1;
  const schema = state.schema;
  const table = state.table;
  const fullTable = quotedTableSafe(schema, table);

  if (state.operation === "select") {
    const cols = state.selectColumns ?? "*";
    validateColumnList(cols);
    const colList =
      cols === "*"
        ? "*"
        : cols
            .split(",")
            .map((c) => quoteId(c.trim()))
            .join(", ");
    const { sql: whereSql, nextIndex } = buildWhere(
      state,
      values,
      paramIndex,
      safety,
    );
    paramIndex = nextIndex;
    let text = `SELECT ${colList} FROM ${fullTable}${whereSql}`;
    if (state.orderBy.length > 0) {
      const orderParts = state.orderBy.map((o) => {
        validateIdentifier(o.column, "column");
        const dir = o.ascending ? "ASC" : "DESC";
        const nulls =
          o.nullsFirst === true
            ? " NULLS FIRST"
            : o.nullsFirst === false
              ? " NULLS LAST"
              : "";
        return `${quoteId(o.column)} ${dir}${nulls}`;
      });
      text += " ORDER BY " + orderParts.join(", ");
    }
    if (state.rangeFrom !== null && state.rangeTo !== null) {
      const limit = state.rangeTo - state.rangeFrom + 1;
      const offset = state.rangeFrom;
      text += ` LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
      paramIndex += 2;
      values.push(limit, offset);
    } else if (state.limitCount !== null) {
      text += ` LIMIT $${paramIndex}`;
      paramIndex += 1;
      values.push(state.limitCount);
    } else if (state.single || state.maybeSingle) {
      text += ` LIMIT $${paramIndex}`;
      paramIndex += 1;
      values.push(1);
    }
    return { text, values };
  }

  if (state.operation === "insert" && state.insertValues) {
    const rows = Array.isArray(state.insertValues)
      ? state.insertValues
      : [state.insertValues];
    const keys = Object.keys(rows[0] as Record<string, unknown>);
    // Prevent extremely large bulk inserts that would generate too many params
    const maxTotal = DEFAULT_MAX_TOTAL_PARAMS;
    if (rows.length * keys.length > maxTotal) {
      throw createError(
        "VALIDATION",
        `Insert too large: would create more than ${maxTotal} parameters`,
      );
    }
    keys.forEach((k) => validateIdentifier(k, "column"));
    const cols = keys.map(quoteId).join(", ");
    const rowPlaceholders = rows
      .map((_, i) => {
        const start = paramIndex + i * keys.length;
        return "(" + keys.map((_, j) => `$${start + j}`).join(", ") + ")";
      })
      .join(", ");
    rows.forEach((row) =>
      keys.forEach((k) => values.push((row as Record<string, unknown>)[k])),
    );
    paramIndex += keys.length * rows.length;
    let text = `INSERT INTO ${fullTable} (${cols}) VALUES ${rowPlaceholders}`;
    if (state.returnSelect) {
      validateColumnList(state.returnSelect);
      const retCols =
        state.returnSelect === "*"
          ? "*"
          : state.returnSelect
              .split(",")
              .map((c) => quoteId(c.trim()))
              .join(", ");
      text += ` RETURNING ${retCols}`;
    }
    return { text, values };
  }

  if (state.operation === "update" && state.updateValues) {
    const uv = state.updateValues;
    Object.keys(uv).forEach((k) => validateIdentifier(k, "column"));
    const setParts = Object.keys(uv).map((k) => {
      const ph = `$${paramIndex}`;
      paramIndex += 1;
      values.push(uv[k]);
      return `${quoteId(k)} = ${ph}`;
    });
    const { sql: whereSql, nextIndex } = buildWhere(
      state,
      values,
      paramIndex,
      safety,
    );
    paramIndex = nextIndex;
    let text = `UPDATE ${fullTable} SET ${setParts.join(", ")}${whereSql}`;
    if (state.returnSelect) {
      validateColumnList(state.returnSelect);
      const retCols =
        state.returnSelect === "*"
          ? "*"
          : state.returnSelect
              .split(",")
              .map((c) => quoteId(c.trim()))
              .join(", ");
      text += ` RETURNING ${retCols}`;
    }
    return { text, values };
  }

  if (
    state.operation === "upsert" &&
    state.upsertValues &&
    state.upsertConflictColumns &&
    state.upsertConflictColumns.length > 0
  ) {
    const rows = Array.isArray(state.upsertValues)
      ? state.upsertValues
      : [state.upsertValues];
    const keys = Object.keys(rows[0] as Record<string, unknown>);
    const maxTotal = DEFAULT_MAX_TOTAL_PARAMS;
    if (rows.length * keys.length > maxTotal) {
      throw createError(
        "VALIDATION",
        `Upsert too large: would create more than ${maxTotal} parameters`,
      );
    }
    keys.forEach((k) => validateIdentifier(k, "column"));
    state.upsertConflictColumns.forEach((k) => validateIdentifier(k, "column"));
    const cols = keys.map(quoteId).join(", ");
    const rowPlaceholders = rows
      .map((_, i) => {
        const start = paramIndex + i * keys.length;
        return "(" + keys.map((_, j) => `$${start + j}`).join(", ") + ")";
      })
      .join(", ");
    rows.forEach((row) =>
      keys.forEach((k) => values.push((row as Record<string, unknown>)[k])),
    );
    paramIndex += keys.length * rows.length;
    const onConflict = state.upsertConflictColumns.map(quoteId).join(", ");
    const conflictCols = state.upsertConflictColumns;
    const doConflict = state.upsertIgnoreDuplicates
      ? "DO NOTHING"
      : "DO UPDATE SET " +
        keys
          .filter((k) => conflictCols !== null && !conflictCols.includes(k))
          .map((k) => `${quoteId(k)} = EXCLUDED.${quoteId(k)}`)
          .join(", ");
    let text = `INSERT INTO ${fullTable} (${cols}) VALUES ${rowPlaceholders} ON CONFLICT (${onConflict}) ${doConflict}`;
    if (state.returnSelect) {
      validateColumnList(state.returnSelect);
      const retCols =
        state.returnSelect === "*"
          ? "*"
          : state.returnSelect
              .split(",")
              .map((c) => quoteId(c.trim()))
              .join(", ");
      text += ` RETURNING ${retCols}`;
    }
    return { text, values };
  }

  if (state.operation === "delete") {
    const { sql: whereSql } = buildWhere(state, values, paramIndex, safety);
    let text = `DELETE FROM ${fullTable}${whereSql}`;
    if (state.returnSelect) {
      validateColumnList(state.returnSelect);
      const retCols =
        state.returnSelect === "*"
          ? "*"
          : state.returnSelect
              .split(",")
              .map((c) => quoteId(c.trim()))
              .join(", ");
      text += ` RETURNING ${retCols}`;
    }
    return { text, values };
  }

  if (state.operation === "rpc" && state.rpcName) {
    const name = quotedTableSafe(schema, state.rpcName);
    const args = state.rpcArgs ?? [];
    const placeholders = args.map((_, i) => `$${paramIndex + i}`).join(", ");
    args.forEach((a) => values.push(a));
    const text = `SELECT * FROM ${name}(${placeholders})`;
    return { text, values };
  }

  return { text: "", values: [] };
}

/**
 * Builds a COUNT(*) query from the same table/filters as state. Used when countOption is "exact".
 */
export function compileCount(state: BuilderState, safety?: SafetyOptions): {
  text: string;
  values: unknown[];
} {
  const values: unknown[] = [];
  const paramIndex = 1;
  const fullTable = quotedTableSafe(state.schema, state.table);
  const { sql: whereSql } = buildWhere(state, values, paramIndex, safety);
  const text = `SELECT COUNT(*)::int AS count FROM ${fullTable}${whereSql}`;
  return { text, values };
}

/**
 * Builds EXPLAIN (FORMAT JSON) for the same SELECT (without LIMIT) to get planner row estimate.
 * Used when countOption is "estimated" or "planned". Parse result to get Plan Rows.
 */
export function compileCountEstimated(
  state: BuilderState,
  safety?: SafetyOptions,
): {
  text: string;
  values: unknown[];
} {
  if (state.operation !== "select") {
    return { text: "", values: [] };
  }
  const values: unknown[] = [];
  let paramIndex = 1;
  const fullTable = quotedTableSafe(state.schema, state.table);
  const cols = state.selectColumns ?? "*";
  validateColumnList(cols);
  const colList =
    cols === "*"
      ? "*"
      : cols
          .split(",")
          .map((c) => quoteId(c.trim()))
          .join(", ");
  const { sql: whereSql } = buildWhere(state, values, paramIndex, safety);
  const selectText = `SELECT ${colList} FROM ${fullTable}${whereSql}`;
  const text = `EXPLAIN (FORMAT JSON) ${selectText}`;
  return { text, values };
}

/**
 * Parses EXPLAIN (FORMAT JSON) result row into estimated row count.
 * pg returns a single column; name may be "QUERY PLAN" or similar; value is JSON string.
 */
export function parseEstimatedCount(rows: unknown[]): number | null {
  const row = rows[0] as Record<string, unknown> | undefined;
  if (row == null) return null;
  const raw = row["QUERY PLAN"] ?? row["query plan"];
  const jsonStr = typeof raw === "string" ? raw : null;
  if (jsonStr == null) return null;
  try {
    const arr = JSON.parse(jsonStr) as Array<{
      Plan?: { "Plan Rows"?: number };
    }>;
    const plan = arr[0]?.Plan;
    if (plan == null || typeof plan["Plan Rows"] !== "number") return null;
    return Math.round(plan["Plan Rows"]);
  } catch {
    return null;
  }
}
