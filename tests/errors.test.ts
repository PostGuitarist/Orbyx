/**
 * Unit tests for error types and helpers.
 */

import { createError, createErrorFromThrown, isDbError, isRetriableError } from "../src/errors";

describe("createError", () => {
  test("returns object with code and message", () => {
    const err = createError("QUERY", "Something failed");
    expect(err.code).toBe("QUERY");
    expect(err.message).toBe("Something failed");
    expect(err.details).toBeUndefined();
    expect(err.pgCode).toBeUndefined();
  });

  test("accepts optional details", () => {
    const cause = new Error("inner");
    const err = createError("CONNECTION", "Failed", cause);
    expect(err.details).toBe(cause);
  });

  test("accepts optional pgCode", () => {
    const err = createError("QUERY", "Duplicate key", undefined, "23505");
    expect(err.pgCode).toBe("23505");
  });
});

describe("createErrorFromThrown", () => {
  test("uses message from Error and sets pgCode when present", () => {
    const pgErr = Object.assign(new Error("duplicate key value"), { code: "23505" });
    const err = createErrorFromThrown("QUERY", "Unknown", pgErr as any);
    expect(err.code).toBe("QUERY");
    expect(err.message).toBe("duplicate key value");
    expect(err.pgCode).toBe("23505");
    expect(err.details).toBe(pgErr);
  });

  test("omits pgCode when thrown value has no code", () => {
    const err = createErrorFromThrown("QUERY", "Unknown", new Error("fail"));
    expect(err.pgCode).toBeUndefined();
  });
});

describe("isRetriableError", () => {
  test("returns true for Postgres class 08 and 40", () => {
    expect(isRetriableError({ code: "08000" } as any)).toBe(true);
    expect(isRetriableError({ code: "40001" } as any)).toBe(true);
  });
  test("returns true for connection error codes", () => {
    expect(isRetriableError({ code: "ECONNRESET" } as any)).toBe(true);
    expect(isRetriableError({ code: "ETIMEDOUT" } as any)).toBe(true);
  });
  test("returns false for non-retriable Postgres code", () => {
    expect(isRetriableError({ code: "23505" } as any)).toBe(false);
  });
});

describe("isDbError", () => {
  test("returns true for valid DbError", () => {
    expect(isDbError(createError("X", "Y"))).toBe(true);
  });

  test("returns false for plain Error", () => {
    expect(isDbError(new Error("x"))).toBe(false);
  });

  test("returns false for null/undefined", () => {
    expect(isDbError(null)).toBe(false);
    expect(isDbError(undefined)).toBe(false);
  });

  test("returns false for object missing code or message", () => {
    expect(isDbError({ code: "X" } as any)).toBe(false);
    expect(isDbError({ message: "Y" } as any)).toBe(false);
  });
});
