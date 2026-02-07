const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error("DATABASE_URL is not set in .env.local");
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL });

async function main(): Promise<void> {
  try {
    const schema = "integration_test";
    const res = await pool.query<{ tablename: string }>(
      `SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename LIKE $2`,
      [schema, "orbyx_integration_test_%"]
    );
    const tables = res.rows.map((r) => r.tablename);
    if (tables.length === 0) {
      console.log("No integration tables found to drop.");
      return;
    }
    for (const t of tables) {
      const q = `DROP TABLE IF EXISTS "${schema}"."${t}" CASCADE`;
      console.log("Dropping", `${schema}.${t}`);
      await pool.query(q);
    }
    console.log("Dropped", tables.length, "tables.");
  } catch (err) {
    console.error("Error cleaning integration tables:", err);
    process.exitCode = 2;
  } finally {
    await pool.end();
  }
}

main();
