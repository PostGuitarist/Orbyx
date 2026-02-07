# Security

## Reporting vulnerabilities

If you believe you have found a security issue, please report it responsibly. Do not open a public GitHub issue. Instead, contact the maintainers (e.g. via private email or repository security advisory) so the issue can be addressed before public disclosure.

## Safe usage

### Query builder (recommended)

- **Identifiers**: Table, schema, and column names used in `from()`, filters, and modifiers are validated to contain only `[a-zA-Z0-9_]`. This prevents SQL injection via identifier names.
- **Values**: All filter and insert/update values are passed as parameters to the database driver (`$1`, `$2`, …). User input must only be used as **values**, not as table/column names or raw SQL.

### Raw SQL (`sql()` / `raw()`)

- **Always use parameterized queries.** Pass user-controlled data as the `params` array and use placeholders (`$1`, `$2`, …) in the query string.
- **Do not** interpolate or concatenate user input into the query string. Example of **unsafe** usage:

  ```ts
  // BAD – SQL injection risk
  db.sql(`SELECT * FROM users WHERE id = ${userInput}`);
  ```

- **Safe** usage:

  ```ts
  // GOOD – parameterized
  db.sql("SELECT * FROM users WHERE id = $1", [userInput]);
  ```

### Connection and secrets

- Do not commit connection strings or passwords to version control. Use environment variables or a secrets manager.
- The library does not log connection strings or passwords. Optional `onQuery` hooks receive SQL and parameter **values**; avoid logging parameters that contain secrets.

### Dependencies

- Keep `pg` (and other dependencies) up to date. Run `npm audit` and address reported vulnerabilities.
