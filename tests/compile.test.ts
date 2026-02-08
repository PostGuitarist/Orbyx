/**
 * Unit tests for query compilation (parameterized SQL and values).
 */

import { compile, compileCount, compileCountEstimated, parseEstimatedCount } from "../src/builder/compile";
import { createInitialState } from "../src/builder/types";
import { isDbError } from "../src/errors";

describe("compile select", () => {
  test("compiles simple select", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    const { text, values } = compile(state);
    expect(text).toContain('SELECT * FROM "public"."users"');
    expect(values.length).toBe(0);
  });

  test("compiles select with eq filter", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "id, name";
    state.filters.push({ type: "eq", column: "id", value: 1 });
    const { text, values } = compile(state);
    expect(text).toContain("WHERE");
    expect(text).toContain('"id"');
    expect(values.length).toBe(1);
    expect(values[0]).toBe(1);
  });

  test("compiles select with order and limit", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    state.orderBy = [{ column: "id", ascending: false }];
    state.limitCount = 10;
    const { text, values } = compile(state);
    expect(text).toContain('ORDER BY "id" DESC');
    expect(text).toContain("LIMIT");
    expect(values.length).toBe(1);
    expect(values[0]).toBe(10);
  });

  test("compiles select with multiple order columns", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    state.orderBy = [
      { column: "name", ascending: true, nullsFirst: true },
      { column: "id", ascending: false },
    ];
    const { text } = compile(state);
    expect(text).toContain('ORDER BY "name" ASC NULLS FIRST, "id" DESC');
  });
});

describe("compile insert", () => {
  test("compiles insert with returning", () => {
    const state = createInitialState("users", "public");
    state.operation = "insert";
    state.insertValues = { name: "Alice", age: 30 };
    state.returnSelect = "*";
    const { text, values } = compile(state);
    expect(text).toContain('INSERT INTO "public"."users"');
    expect(text).toContain("RETURNING");
    expect(values.length).toBe(2);
    expect(values).toEqual(["Alice", 30]);
  });
});

describe("compile update", () => {
  test("compiles update with where", () => {
    const state = createInitialState("users", "public");
    state.operation = "update";
    state.updateValues = { name: "Bob" };
    state.filters.push({ type: "eq", column: "id", value: 1 });
    const { text, values } = compile(state);
    expect(text).toContain('UPDATE "public"."users"');
    expect(text).toContain("WHERE");
    expect(values.length).toBe(2);
    expect(values[0]).toBe("Bob");
    expect(values[1]).toBe(1);
  });
});

describe("compile delete", () => {
  test("compiles delete with where", () => {
    const state = createInitialState("users", "public");
    state.operation = "delete";
    state.filters.push({ type: "eq", column: "id", value: 42 });
    const { text, values } = compile(state);
    expect(text).toContain('DELETE FROM "public"."users"');
    expect(text).toContain("WHERE");
    expect(values.length).toBe(1);
    expect(values[0]).toBe(42);
  });
});

describe("compile filters notIn isDistinct contains", () => {
  test("compiles notIn filter", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    state.filters.push({ type: "notIn", column: "id", value: [1, 2, 3] });
    const { text, values } = compile(state);
    expect(text).toContain("NOT IN");
    expect(values.length).toBe(3);
    expect(values).toEqual([1, 2, 3]);
  });

  test("compiles isDistinct filter", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    state.filters.push({ type: "isDistinct", column: "name", value: null });
    const { text, values } = compile(state);
    expect(text).toContain("IS DISTINCT FROM");
    expect(values.length).toBe(1);
  });

  test("compiles contains filter", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    state.filters.push({ type: "contains", column: "tags", value: ["a", "b"] });
    const { text, values } = compile(state);
    expect(text).toContain("@>");
    expect(values.length).toBe(1);
  });
});

describe("compile validation", () => {
  test("throws on invalid schema", () => {
    const state = createInitialState("users", "public; DROP TABLE users--");
    state.operation = "select";
    state.selectColumns = "*";
    try {
      compile(state);
      throw new Error("Expected compile to throw");
    } catch (err: unknown) {
      expect(isDbError(err)).toBe(true);
      if (isDbError(err)) expect(err.code).toBe("VALIDATION");
    }
  });

  test("throws on invalid table", () => {
    const state = createInitialState("users; DELETE FROM users--", "public");
    state.operation = "select";
    state.selectColumns = "*";
    try {
      compile(state);
      throw new Error("Expected compile to throw");
    } catch (err: unknown) {
      expect(isDbError(err)).toBe(true);
      if (isDbError(err)) expect(err.code).toBe("VALIDATION");
    }
  });
});

describe("compileCount and compileCountEstimated", () => {
  test("compileCount produces COUNT query", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.filters.push({ type: "eq", column: "id", value: 1 });
    const { text, values } = compileCount(state);
    expect(text).toContain("COUNT(*)");
    expect(text).toContain('"public"."users"');
    expect(values.length).toBe(1);
  });

  test("compileCountEstimated produces EXPLAIN (FORMAT JSON) select", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    state.selectColumns = "*";
    const { text, values } = compileCountEstimated(state);
    expect(text.startsWith("EXPLAIN (FORMAT JSON)")).toBe(true);
    expect(text).toContain("SELECT * FROM");
    expect(values.length).toBe(0);
  });

  test("parseEstimatedCount extracts Plan Rows from JSON", () => {
    const row = [{ Plan: { "Plan Rows": 42.5 } }];
    const rows = [{ "QUERY PLAN": JSON.stringify(row) }];
    expect(parseEstimatedCount(rows)).toBe(43);
  });

  test("parseEstimatedCount returns null for empty or invalid", () => {
    expect(parseEstimatedCount([])).toBeNull();
    expect(parseEstimatedCount([{ "QUERY PLAN": "not json" }])).toBeNull();
  });
});

describe("compile rpc", () => {
  test("compiles rpc call", () => {
    const state = createInitialState("get_user", "public");
    state.operation = "rpc";
    state.rpcName = "get_user";
    state.rpcArgs = [1, "active"];
    const { text, values } = compile(state);
    expect(text).toContain('SELECT * FROM "public"."get_user"');
    expect(text).toContain("$1");
    expect(text).toContain("$2");
    expect(values.length).toBe(2);
    expect(values).toEqual([1, "active"]);
  });
});
