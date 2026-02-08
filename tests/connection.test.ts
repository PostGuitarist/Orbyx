/**
 * Unit tests for connection parsing and config normalization.
 * Run with: node --test (Node 18+).
 */

import { isDbError } from "../src/errors";
import { parseConnectionString, normalizeConfig } from "../src/connection";

/** Asserts that fn throws a DbError whose message matches the regex. */
function throwsDbError(fn: () => void, messageRe: RegExp): void {
  try {
    fn();
    throw new Error("Expected function to throw a DbError");
  } catch (err: unknown) {
    expect(isDbError(err)).toBe(true);
    if (err instanceof Error) {
      expect(messageRe.test(err.message)).toBe(true);
    }
  }
}

describe("parseConnectionString", () => {
  test("parses a full DSN", () => {
    const dsn = "postgresql://user:pass@host.example.com:5433/mydb";
    const config = parseConnectionString(dsn);
    expect(config.host).toBe("host.example.com");
    expect(config.port).toBe(5433);
    expect(config.user).toBe("user");
    expect(config.password).toBe("pass");
    expect(config.database).toBe("mydb");
  });

  test("defaults port to 5432", () => {
    const dsn = "postgres://u:p@localhost/db";
    const config = parseConnectionString(dsn);
    expect(config.port).toBe(5432);
  });

  test("decodes URL-encoded password", () => {
    const dsn = "postgres://u:p%40ss%3A@localhost/db";
    const config = parseConnectionString(dsn);
    expect(config.password).toBe("p@ss:");
  });

  test("throws on invalid protocol", () => {
    throwsDbError(() => parseConnectionString("http://localhost/db"), /Invalid protocol/);
  });
});

describe("normalizeConfig", () => {
  test("accepts object config unchanged for required fields", () => {
    const config = {
      host: "h",
      port: 5432,
      user: "u",
      password: "p",
      database: "d",
    };
    const { poolConfig } = normalizeConfig(config as any);
    expect(poolConfig.host).toBe("h");
    expect(poolConfig.port).toBe(5432);
    expect(poolConfig.user).toBe("u");
    expect(poolConfig.database).toBe("d");
  });

  test("accepts connection string", () => {
    const { poolConfig } = normalizeConfig("postgres://u:p@localhost:5432/db");
    expect(poolConfig.host).toBe("localhost");
    expect(poolConfig.database).toBe("db");
  });

  test("merges pool options", () => {
    const { poolConfig } = normalizeConfig("postgres://u:p@localhost/db", {
      max: 10,
      idleTimeoutMillis: 5000,
    } as any);
    expect(poolConfig.max).toBe(10);
    expect(poolConfig.idleTimeoutMillis).toBe(5000);
  });

  test("throws on invalid connection object (empty host)", () => {
    throwsDbError(
      () => normalizeConfig({ host: "", port: 5432, user: "u", password: "p", database: "d" } as any),
      /Invalid connection/
    );
  });

  test("throws on invalid port", () => {
    throwsDbError(
      () => normalizeConfig({ host: "h", port: 0, user: "u", password: "p", database: "d" } as any),
      /Invalid port/
    );
  });
});
