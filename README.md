# Orbyx

Isomorphic SDK for Databases.

Orbyx provides a Supabase-js–style, isomorphic client for databases. It is designed to work in multiple runtimes (Node and browser) and to offer a consistent, ergonomic API for CRUD, raw SQL, transactions, and streaming. Currently Orbyx ships a Postgres client that speaks the **Postgres wire protocol** (use a connection string or host/port config), but the API and architecture are intended to support additional database backends in the future.

## Compatibility

Orbyx works with **any Postgres-compatible host** over the standard TCP protocol (with optional SSL). That includes:

- **Hosted Postgres**: Neon, Supabase (direct connection), Vercel Postgres, Railway, Render, Fly.io, Crunchy Bridge, Aiven
- **Cloud**: AWS RDS, Azure Database for PostgreSQL, Google Cloud SQL
- **Postgres-compatible**: CockroachDB (Postgres compatibility mode), and other wire-compatible servers
- **Self-hosted**: Any vanilla Postgres (local or on your own infra)

Use the provider’s **Postgres connection string** (or host/port/user/password/database + SSL). No provider-specific SDK is required.

**Not compatible with non-Postgres backends.** Services like **Convex**, **Firebase**, **MongoDB**, or REST-only APIs use different protocols and clients; Orbyx is for Postgres only.

## Install

```bash
npm install orbyx
```

## Quick start

```ts
import { createClient } from "orbyx";

const db = createClient({
  connection: process.env.DATABASE_URL,
  schema: "public",
});

// Select
const { data, error } = await db.from("users").select().eq("id", 1).single();

// Insert with RETURNING
const { data: row } = await db.from("users").insert({ name: "Alice" }).select();

// Update
await db.from("users").update({ name: "Bob" }).eq("id", 1);

// Delete
await db.from("users").delete().eq("id", 1);

// Raw SQL (always use params for user input)
const { data: rows } = await db.sql("SELECT * FROM users WHERE id = $1", [id]);

// Supabase-style: count with select
const { data: list, count } = await db.from("users").select("*", { count: "exact" }).limit(10);

// Upsert with conflict target (string or array)
await db.from("users").upsert({ id: 1, name: "Alice" }, { onConflict: "id" });
await db.from("users").upsert({ id: 2, name: "Bob" }, { onConflict: "id", ignoreDuplicates: true });

// Filters: notIn, contains, textSearch
const { data: found } = await db.from("posts").select().notIn("status", ["draft"]).textSearch("body", "hello world", { config: "english" });
```

## Connection

Use a **connection string** (from your provider’s dashboard) or a **config object**:

```ts
createClient({ connection: "postgresql://user:pass@host:5432/dbname" });

createClient({
  connection: {
    host: "localhost",
    port: 5432,
    user: "user",
    password: "pass",
    database: "dbname",
    ssl: true,  // or { rejectUnauthorized: false } for dev/self-signed certs
  },
});
```

Connection strings can include `?sslmode=require` or `?ssl=true` for TLS. For full control (e.g. custom CA or `rejectUnauthorized`), use the config object.

## Customization

- **Default schema**: `createClient({ connection, schema: "public" })`. Override per query: `db.from("users", "app")`.
- **Pool**: `createClient({ connection, pool: { max: 20, idleTimeoutMillis: 30000 } })`.
- **Hooks**: `createClient({ connection, hooks: { onQuery: (sql, params) => console.log(sql), onError: (err) => console.error(err) } })`.
- **Retries**: `createClient({ connection, retries: { attempts: 3, backoffMs: 100 } })` to retry transient failures (connection errors, Postgres class 08/40). See `isRetriableError` for which errors are retried.
- **Raw SQL**: `db.sql("SELECT * FROM other_schema.foo WHERE id = $1", [id])`.

## Transactions

Run multiple operations in a single connection with automatic BEGIN/COMMIT/ROLLBACK:

```ts
const { data, error } = await db.transaction(async (tx) => {
  await tx.from("users").insert({ name: "Alice" });
  const { data: row } = await tx.from("users").select().eq("name", "Alice").single();
  return row;
});
// Do not call tx.end(); the connection is released when the callback completes.
```

## Health check

For readiness probes or startup checks:

```ts
const { error } = await db.healthCheck();
if (error) console.error("DB unreachable");
```

## Streaming large results

To consume rows one-by-one without loading all into memory (holds a connection until iteration completes):

```ts
const { data: iterable, error } = await db.from("users").select().stream();
if (error) throw new Error(error.message);
for await (const row of iterable) {
  console.log(row);
}
```

## Error handling and Postgres codes

Responses use `{ data, error }`. When the error comes from Postgres, `error.pgCode` is the SQLSTATE (e.g. `"23505"` unique violation, `"23503"` foreign key violation). Use it to branch on constraint failures:

```ts
const { error } = await db.from("users").insert({ id: 1, name: "x" });
if (error?.pgCode === "23505") console.log("Duplicate key");
```

## TypeScript: Database types

For typed tables (Row / Insert / Update), define a `Database` interface and pass it to `createClient`:

```ts
import { createClient, type Database } from "orbyx";

interface Database {
  public: {
    Tables: {
      users: {
        Row: { id: number; name: string };
        Insert: { id?: number; name: string };
        Update: { id?: number; name?: string };
      };
    };
  };
}

const db = createClient<Database>({ connection: "..." });
// Then use db.from("users") and type inference for select/insert/update.
```

You can generate these types from your schema (e.g. with Supabase CLI `supabase gen types typescript` or a custom script) and adapt the shape to the `Database` interface above.

## Security

- **Query builder**: Table, schema, and column names are validated (alphanumeric + underscore only). All values are sent as parameters; never concatenated into SQL.
- **Raw SQL**: `db.sql()` / `db.raw()` execute arbitrary SQL. Always use parameterized queries (use `$1`, `$2`, … and pass user input in `params`). Consider naming raw execution `unsafeRaw` in your own codebase to mark it clearly.
- **Hooks**: `onQuery` receives the SQL and parameters; `onError` receives `DbError`. Orbyx redacts likely secrets (JWTs, long strings, and common secret keys) when invoking `onQuery` and invokes hooks asynchronously to avoid blocking the query flow. Still, avoid logging raw `params` unredacted in production.
- **Safety limits (`ClientOptions.safety`)**: Orbyx exposes `safety` options to protect against large/abusive queries. Example:

```ts
const db = createClient({
  connection: process.env.DATABASE_URL,
  safety: {
    maxInElements: 1000,       // max items allowed in IN/NOT IN lists
    maxTotalParams: 5000,      // max parameters for bulk insert/upsert
    statementTimeoutMs: 5000,  // per-query statement_timeout (SET LOCAL)
  },
});
```

These defaults are conservative; increase them only if you understand the resource impact.

- **Statement timeouts & cancellation**: When `safety.statementTimeoutMs` is set Orbyx attempts `SET LOCAL statement_timeout` on the client before running the query so Postgres will cancel long-running statements. Query cancellation and stream iteration also support a `close()` API on the returned iterable to release the underlying client promptly.

- **Least privilege & credentials**: Always run with a DB role that has the minimum necessary privileges (no superuser). Treat connection strings as secrets and rotate them regularly.

- **SSL/TLS**: Orbyx accepts SSL options from the connection string or config object. In `production` a warning is logged if TLS appears disabled or `rejectUnauthorized` is false. For strict enforcement, validate TLS at deployment time and avoid `rejectUnauthorized: false`.

- **Denial-of-service**: Orbyx enforces IN-list and bulk param limits to reduce the risk of memory/CPU exhaustion from user-supplied arrays. Consider additional rate-limiting in front of the DB for multi-tenant systems.

See [SECURITY.md](SECURITY.md) for more operational advice.

## API summary (Supabase-js–style)

- **Client**: `createClient(options)` → `from(table, schema?)`, `rpc(fn, args?)`, `sql()`, `raw()`, `transaction(callback)`, `healthCheck()`, `end()`.
- **Builder** (from `from()` or `rpc()`):
  - **CRUD**: `.select(columns?, { count?: "exact" | "planned" | "estimated" })`, `.insert()`, `.update()`, `.upsert(values, { onConflict, ignoreDuplicates })`, `.delete()`, `.rpc(fn, args)`.
  - **Filters**: `.eq()`, `.neq()`, `.gt()`, `.gte()`, `.lt()`, `.lte()`, `.like()`, `.ilike()`, `.is()`, `.in()`, `.notIn()`, `.isDistinct()`, `.or()`, `.match()`, `.contains()`, `.containedBy()`, `.overlaps()`, `.not()`, `.textSearch()`.
  - **Modifiers**: `.order(column, options)` (chain multiple `.order()` for multiple columns), `.limit()`, `.range()`, `.single()`, `.maybeSingle()`, `.abortSignal(signal)`, `.stream()` (select only; returns async iterable).
- **Response**: `Promise<{ data: T | null, error: DbError | null, count?: number }>`. Use `.select("*", { count: "exact" })` (or `"planned"` / `"estimated"`) to get `count`; never throws by default. `DbError` may include `pgCode` (Postgres SQLSTATE).

## Differences from Supabase-js

- **Direct Postgres**: Connects to Postgres (Neon, Supabase direct, or self-hosted); no Supabase Realtime or Auth.
- **Transactions**: First-class `db.transaction(callback)`; Supabase-js typically uses RPC or raw SQL for transactions.
- **Streaming**: `.stream()` returns an async iterable of rows for large selects.

## License

GPL v3 (or later). See [LICENSE](LICENSE) and <https://www.gnu.org/licenses/gpl-3.0.html>. You may use, modify, and distribute the software under the terms of the GNU General Public License; derivative works must be released under the same license and you must provide source.
