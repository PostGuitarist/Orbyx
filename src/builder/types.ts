/**
 * Internal state for the query builder.
 * Used to compile to parameterized SQL and execute.
 */

export type QueryOperation =
  | "select"
  | "insert"
  | "update"
  | "upsert"
  | "delete"
  | "rpc";

/** Operators supported by .not(column, operator, value). */
export type FilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "like"
  | "ilike"
  | "is"
  | "in";

export interface FilterClause {
  type:
    | "eq"
    | "neq"
    | "gt"
    | "gte"
    | "lt"
    | "lte"
    | "like"
    | "ilike"
    | "is"
    | "in"
    | "notIn"
    | "isDistinct"
    | "or"
    | "match"
    | "contains"
    | "containedBy"
    | "overlaps"
    | "not"
    | "textSearch";
  column?: string;
  value?: unknown;
  /** For .or() — raw filter expression string and values. */
  orFilters?: Array<{ column: string; op: string; value: unknown }>;
  /** For .match() — record of column -> value. */
  matchRecord?: Record<string, unknown>;
  /** For .not(column, operator, value). */
  notOperator?: FilterOperator;
  /** For .textSearch() — config and type. */
  textSearchConfig?: string;
  textSearchType?: "plain" | "phrase" | "websearch";
}

export interface BuilderState {
  table: string;
  schema: string;
  operation: QueryOperation;
  selectColumns: string | null;
  insertValues: Record<string, unknown> | Record<string, unknown>[] | null;
  updateValues: Record<string, unknown> | null;
  upsertValues: Record<string, unknown> | Record<string, unknown>[] | null;
  upsertConflictColumns: string[] | null;
  rpcName: string | null;
  rpcArgs: unknown[] | null;
  filters: FilterClause[];
  /** Multiple order columns; each .order() appends. */
  orderBy: Array<{ column: string; ascending: boolean; nullsFirst?: boolean }>;
  limitCount: number | null;
  rangeFrom: number | null;
  rangeTo: number | null;
  single: boolean;
  maybeSingle: boolean;
  returnSelect: string | null;
  /** When set, execute() returns count in response (exact count for select). */
  countOption: "exact" | "planned" | "estimated" | null;
  /** Optional AbortSignal for the query. */
  abortSignal: AbortSignal | null;
  /** For upsert: if true, ON CONFLICT DO NOTHING; else DO UPDATE. */
  upsertIgnoreDuplicates: boolean;
  params: unknown[];
  paramIndex: number;
}

export function createInitialState(
  table: string,
  schema: string,
): BuilderState {
  return {
    table,
    schema,
    operation: "select",
    selectColumns: null,
    insertValues: null,
    updateValues: null,
    upsertValues: null,
    upsertConflictColumns: null,
    rpcName: null,
    rpcArgs: null,
    filters: [],
    orderBy: [],
    limitCount: null,
    rangeFrom: null,
    rangeTo: null,
    single: false,
    maybeSingle: false,
    returnSelect: null,
    countOption: null,
    abortSignal: null,
    upsertIgnoreDuplicates: false,
    params: [],
    paramIndex: 1,
  };
}
