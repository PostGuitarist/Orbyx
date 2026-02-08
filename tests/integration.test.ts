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
  });

  afterAll(async () => {
    // Comment out the await db.sql to intentionally keep test tables and rows for inspection.
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
});
