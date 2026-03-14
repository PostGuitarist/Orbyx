/**
 * Integration tests against a real Postgres. Skip when DATABASE_URL is not set.
 */

import { createClient } from "../src/client";
import dotenv from "dotenv";

dotenv.config();

const DATABASE_URL = process.env.DATABASE_URL ?? "";
const skipIntegration = !DATABASE_URL || DATABASE_URL.length === 0;

// Use Jest's skip inline to avoid assigning describe to a variable.
(skipIntegration ? describe.skip : describe)("integration (requires DATABASE_URL)", () => {
  const SCHEMA = "integration_test";
  const db = createClient({ connection: DATABASE_URL, schema: SCHEMA });
  const table = `orbyx_integration_test_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

  beforeAll(async () => {
    await db.sql(`CREATE SCHEMA IF NOT EXISTS "${SCHEMA}"`);
    await db.sql(
      `CREATE TABLE IF NOT EXISTS "${SCHEMA}"."${table}" (id serial PRIMARY KEY, name text, value int)`
    );
    await db.sql(
      `CREATE OR REPLACE FUNCTION "${SCHEMA}"."add_values"(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$`
    );
  });

  afterAll(async () => {
    // Comment out the await db.sql to intentionally keep test tables and rows for inspection.
    await db.sql(`DROP FUNCTION IF EXISTS "${SCHEMA}"."add_values"(int, int)`);
    await db.sql(`DROP TABLE IF EXISTS "${SCHEMA}"."${table}"`);
    await db.end();
  });

  test("insert and select", async () => {
    const { error: insErr } = await db.from(table).insert({ name: "a", value: 1 }).select();
    expect(insErr).toBeNull();
    const { data, error } = await db.from(table).select().eq("name", "a");
    expect(error).toBeNull();
    expect(Array.isArray(data) && data.length >= 1).toBe(true);
  });

  test("update", async () => {
    const { error } = await db.from(table).update({ value: 2 }).eq("name", "a");
    expect(error).toBeNull();
    const { data } = await db.from(table).select().eq("name", "a").single();
    const row = data as Record<string, unknown> | null;
    expect(row != null && (row as { value: number }).value === 2).toBe(true);
  });

  test("filters and order", async () => {
    await db.from(table).insert([{ name: "b", value: 1 }, { name: "c", value: 3 }]);
    const { data, error } = await db
      .from(table)
      .select("id, name, value")
      .order("value", { ascending: false })
      .order("name", { ascending: true })
      .limit(5);
    expect(error).toBeNull();
    expect(Array.isArray(data) && data.length >= 2).toBe(true);
  });

  test("transaction", async () => {
    const result = await db.transaction(async (tx) => {
      const { data } = await tx.from(table).select().eq("name", "a").single();
      const row = data as Record<string, unknown> | null;
      return (row != null && typeof (row as { id: number }).id === "number")
        ? (row as { id: number }).id
        : 0;
    });
    expect(result.error).toBeNull();
    expect(typeof result.data === "number").toBe(true);
  });

  test("healthCheck", async () => {
    const { data, error } = await db.healthCheck();
    expect(error).toBeNull();
    expect(data != null).toBe(true);
  });

  test("count exact and estimated", async () => {
    const { count: exactCount, error: exactErr } = await db
      .from(table)
      .select("*", { count: "exact" })
      .limit(1);
    expect(exactErr).toBeNull();
    expect(typeof exactCount === "number" && exactCount >= 0).toBe(true);
    const { count: estCount, error: estErr } = await db
      .from(table)
      .select("*", { count: "estimated" })
      .limit(1);
    expect(estErr).toBeNull();
    expect(estCount == null || typeof estCount === "number").toBe(true);
  });

  test("rowCount is returned for insert/update/delete", async () => {
    const { rowCount: insRowCount } = await db
      .from(table)
      .insert({ name: "rowcount_test", value: 99 })
      .select();
    expect(typeof insRowCount === "number" || insRowCount == null).toBe(true);

    const { rowCount: updRowCount } = await db
      .from(table)
      .update({ value: 100 })
      .eq("name", "rowcount_test");
    // pg returns rowCount for UPDATE
    expect(typeof updRowCount === "number").toBe(true);
    expect((updRowCount as number) >= 1).toBe(true);

    const { rowCount: delRowCount } = await db
      .from(table)
      .delete()
      .eq("name", "rowcount_test");
    expect(typeof delRowCount === "number").toBe(true);
    expect((delRowCount as number) >= 1).toBe(true);
  });

  test("throwOnError() throws on errors instead of returning { error }", async () => {
    await expect(
      db
        .from(table)
        .select()
        .eq("non_existent_column_xyz", 1)
        .throwOnError(),
    ).rejects.toThrow();
  });

  test("or() string form (PostgREST-style)", async () => {
    await db.from(table).insert([
      { name: "or_alice", value: 1 },
      { name: "or_bob", value: 2 },
    ]);
    const { data, error } = await db
      .from(table)
      .select()
      .or("name.eq.or_alice,name.eq.or_bob");
    expect(error).toBeNull();
    expect(Array.isArray(data) && data.length >= 2).toBe(true);
  });

  test("is() filter with NULL works correctly", async () => {
    // Insert a row with NULL value field
    await db.sql(
      `INSERT INTO "${SCHEMA}"."${table}" (name, value) VALUES ($1, NULL)`,
      ["null_value_row"],
    );
    const { data, error } = await db
      .from(table)
      .select()
      .is("value", null);
    expect(error).toBeNull();
    expect(Array.isArray(data) && data.length >= 1).toBe(true);
    const allNull = (data as Array<{ value: unknown }>).every(
      (r) => r.value === null,
    );
    expect(allNull).toBe(true);
  });

  test("nested transaction uses savepoints and can rollback independently", async () => {
    const outerRes = await db.transaction(async (tx) => {
      await tx.from(table).insert({ name: "outer_row", value: 10 });

      // Inner transaction intentionally throws — only inner should rollback
      const innerRes = await tx.transaction(async (innerTx) => {
        await innerTx
          .from(table)
          .insert({ name: "inner_row_will_rollback", value: 20 });
        throw new Error("deliberate inner rollback");
      });

      // Inner failed but outer continues
      expect(innerRes.error).not.toBeNull();
      return "outer_ok";
    });

    expect(outerRes.error).toBeNull();
    expect(outerRes.data).toBe("outer_ok");

    // outer_row should exist; inner_row_will_rollback should not
    const { data: outerRows } = await db
      .from(table)
      .select()
      .eq("name", "outer_row");
    expect(Array.isArray(outerRows) && outerRows.length >= 1).toBe(true);

    const { data: innerRows } = await db
      .from(table)
      .select()
      .eq("name", "inner_row_will_rollback");
    expect(Array.isArray(innerRows) && innerRows.length === 0).toBe(true);
  });

  test("transaction with isolation level serializable", async () => {
    const result = await db.transaction(
      async (tx) => {
        const { data } = await tx
          .from(table)
          .select()
          .limit(1);
        return data;
      },
      { isolationLevel: "serializable" },
    );
    expect(result.error).toBeNull();
  });

  test("head() returns count with no rows", async () => {
    // Seed at least one row
    await db.from(table).insert({ name: "Head Test", value: 77 }).select();
    const { data, count, error } = await db.from(table).select().head();
    expect(error).toBeNull();
    expect(data).toBeNull();
    expect(typeof count).toBe("number");
    expect((count as number) >= 1).toBe(true);
  });

  test("head() with eq filter returns filtered count", async () => {
    await db.from(table).insert({ name: "HeadFiltered", value: 99 }).select();
    const { data, count, error } = await db
      .from(table)
      .select()
      .eq("name", "HeadFiltered")
      .head();
    expect(error).toBeNull();
    expect(data).toBeNull();
    expect((count as number) >= 1).toBe(true);
  });

  test("explain() returns query plan", async () => {
    const { data, error } = await db.from(table).select().explain();
    expect(error).toBeNull();
    expect(Array.isArray(data)).toBe(true);
    expect((data as unknown[]).length).toBeGreaterThan(0);
  });

  test("filter() behaves identically to eq()", async () => {
    await db.from(table).insert({ name: "FilterTest", value: 55 }).select();
    const { data: d1, error: e1 } = await db.from(table).select().eq("name", "FilterTest");
    const { data: d2, error: e2 } = await db.from(table).select().filter("name", "eq", "FilterTest");
    expect(e1).toBeNull();
    expect(e2).toBeNull();
    expect(d1).toEqual(d2);
  });

  test("rpc() with named args object", async () => {
    const { data, error } = await db.rpc("add_values", { a: 3, b: 7 });
    expect(error).toBeNull();
    expect(Array.isArray(data)).toBe(true);
    const row = (data as unknown[])[0] as { add_values: number };
    expect(row.add_values).toBe(10);
  });
});
