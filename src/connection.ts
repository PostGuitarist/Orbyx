/**
 * Normalizes connection config and creates a pg Pool.
 * Supports connection string or object form; merges pool options.
 */

import { Pool, type PoolConfig } from "pg";
import { createError } from "./errors";
import { validatePort } from "./validate";
import type { ConnectionConfig, PoolOptions } from "./types/index";

/** Result of normalizing client connection input. */
export interface NormalizedConfig {
  poolConfig: PoolConfig;
  poolOptions: PoolOptions;
}

const DEFAULT_PORT = 5432;
const DEFAULT_SCHEMA = "public";

/**
 * Parses a Postgres connection string into a config object.
 * Does not validate connectivity.
 */
export function parseConnectionString(dsn: string): ConnectionConfig {
  try {
    const url = new URL(dsn);
    if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
      throw new Error("Invalid protocol; expected postgres or postgresql");
    }
    const password = url.password ? decodeURIComponent(url.password) : "";
    const pathname = url.pathname.replace(/^\//, "");
    const database = pathname || "postgres";
    const ssl =
      url.searchParams.get("sslmode") === "require" ||
      url.searchParams.get("ssl") === "true";
    const portNum = url.port ? parseInt(url.port, 10) : DEFAULT_PORT;
    const port = Number.isNaN(portNum) ? DEFAULT_PORT : portNum;
    return {
      host: url.hostname,
      port,
      user: decodeURIComponent(url.username || "postgres"),
      password,
      database: decodeURIComponent(database),
      ssl: ssl || undefined,
    };
  } catch (err) {
    throw createError(
      "VALIDATION",
      err instanceof Error ? err.message : "Invalid connection string",
      err,
    );
  }
}

/**
 * Normalizes connection (string or object) and pool options into a single config.
 */
export function normalizeConfig(
  connection: string | ConnectionConfig,
  poolOptions: PoolOptions = {},
): NormalizedConfig {
  const conn: ConnectionConfig =
    typeof connection === "string"
      ? parseConnectionString(connection)
      : connection;

  if (!conn.host || typeof conn.host !== "string" || conn.host.length === 0) {
    throw createError("VALIDATION", "Invalid connection: host is required");
  }
  validatePort(conn.port);

  const poolConfig: PoolConfig = {
    host: conn.host,
    port: conn.port,
    user: conn.user,
    password: conn.password,
    database: conn.database,
    ssl:
      conn.ssl === true
        ? { rejectUnauthorized: true }
        : conn.ssl === false
          ? false
          : conn.ssl,
    max: poolOptions.max,
    min: poolOptions.min,
    idleTimeoutMillis: poolOptions.idleTimeoutMillis,
    connectionTimeoutMillis: poolOptions.connectionTimeoutMillis,
  };

  // Warn in production if SSL is not configured securely; do not fail automatically
  try {
    if (process.env.NODE_ENV === "production") {
      const ssl = poolConfig.ssl as any;
      if (ssl === false || (ssl && ssl.rejectUnauthorized === false)) {
        // eslint-disable-next-line no-console
        console.warn(
          "Orbyx: running in production without secure SSL settings for Postgres. Consider enabling SSL with rejectUnauthorized=true.",
        );
      }
    }
  } catch {
    // ignore env read issues
  }

  return { poolConfig, poolOptions };
}

/**
 * Creates a new pg Pool from normalized config.
 */
export function createPool(normalized: NormalizedConfig): Pool {
  return new Pool(normalized.poolConfig);
}

export { DEFAULT_SCHEMA };
