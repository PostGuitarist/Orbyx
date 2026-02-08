import { compile } from "../src/builder/compile";
import { createInitialState } from "../src/builder/types";

describe("compile security guards", () => {
  test("throws when IN list exceeds limit", () => {
    const state = createInitialState("users", "public");
    state.operation = "select";
    const many = new Array(1001).fill(1);
    state.filters.push({ type: "in", column: "id", value: many });
    expect(() => compile(state)).toThrow(/IN list too large/);
  });

  test("throws when insert would create too many parameters", () => {
    const state = createInitialState("t", "public");
    state.operation = "insert";
    // create 5001 rows of a single column -> 5001 params > 5000 limit
    const rows = new Array(5001).fill({ a: 1 });
    state.insertValues = rows as any;
    expect(() => compile(state)).toThrow(/Insert too large/);
  });
});
