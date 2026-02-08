/**
 * Unit tests for validation helpers (identifier, port, limit, range).
 */

import { isDbError } from "../src/errors";
import {
  validateIdentifier,
  validateColumnList,
  validatePort,
  validateLimit,
  validateRange,
} from "../src/validate";

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

describe("validateIdentifier", () => {
  test("accepts valid identifiers", () => {
    expect(() => validateIdentifier("users", "table")).not.toThrow();
    expect(() => validateIdentifier("public", "schema")).not.toThrow();
    expect(() => validateIdentifier("id", "column")).not.toThrow();
    expect(() => validateIdentifier("_private", "column")).not.toThrow();
  });

  test("throws on empty string", () => {
    throwsDbError(() => validateIdentifier("", "table"), /non-empty string/);
  });

  test("throws on invalid characters", () => {
    throwsDbError(() => validateIdentifier("foo; DROP TABLE bar--", "table"), /only letters/);
    throwsDbError(() => validateIdentifier("has-dash", "column"), /only letters/);
    throwsDbError(() => validateIdentifier("has space", "column"), /only letters/);
  });

  test("throws on too long identifier", () => {
    const long = "a".repeat(64);
    throwsDbError(() => validateIdentifier(long, "table"), /63 characters/);
  });
});

describe("validateColumnList", () => {
  test("accepts *", () => {
    expect(() => validateColumnList("*")).not.toThrow();
  });

  test("accepts comma-separated valid columns", () => {
    expect(() => validateColumnList("id, name")).not.toThrow();
    expect(() => validateColumnList("  id ,  name  ")).not.toThrow();
  });

  test("throws on invalid column name in list", () => {
    throwsDbError(() => validateColumnList("id; DROP TABLE users--"), /only letters/);
  });
});

describe("validatePort", () => {
  test("accepts valid port", () => {
    expect(() => validatePort(5432)).not.toThrow();
    expect(() => validatePort(1)).not.toThrow();
    expect(() => validatePort(65535)).not.toThrow();
  });

  test("throws on invalid port", () => {
    throwsDbError(() => validatePort(0), /1 and 65535/);
    throwsDbError(() => validatePort(65536), /1 and 65535/);
    throwsDbError(() => validatePort(Number.NaN), /1 and 65535/);
  });
});

describe("validateLimit", () => {
  test("accepts non-negative integer", () => {
    expect(() => validateLimit(0)).not.toThrow();
    expect(() => validateLimit(100)).not.toThrow();
  });

  test("throws on negative or non-integer", () => {
    throwsDbError(() => validateLimit(-1), /non-negative integer/);
    throwsDbError(() => validateLimit(1.5), /non-negative integer/);
  });
});

describe("validateRange", () => {
  test("accepts valid range", () => {
    expect(() => validateRange(0, 10)).not.toThrow();
    expect(() => validateRange(5, 5)).not.toThrow();
  });

  test("throws when from > to", () => {
    throwsDbError(() => validateRange(10, 5), /from must be ≤ to/);
  });

  test("throws on negative", () => {
    throwsDbError(() => validateRange(-1, 5), /non-negative integer/);
  });
});
