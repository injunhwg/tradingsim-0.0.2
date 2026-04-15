const fs = require('node:fs');
const path = require('node:path');

const MIGRATIONS_DIR = path.join(__dirname, '../../migrations');
const MIGRATIONS_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
`;

function listMigrationFiles(migrationsDir = MIGRATIONS_DIR) {
  return fs
    .readdirSync(migrationsDir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith('.sql'))
    .map((entry) => entry.name)
    .sort((left, right) => left.localeCompare(right));
}

function loadMigrations(migrationsDir = MIGRATIONS_DIR) {
  return listMigrationFiles(migrationsDir).map((filename) => ({
    filename,
    sql: fs.readFileSync(path.join(migrationsDir, filename), 'utf8')
  }));
}

async function initializeSchema(db, options = {}) {
  const migrationsDir = options.migrationsDir || MIGRATIONS_DIR;
  const migrations = loadMigrations(migrationsDir);
  const appliedMigrations = [];

  await db.exec(MIGRATIONS_TABLE_SQL);

  await db.withTransaction(async (tx) => {
    const appliedResult = await tx.query(
      `SELECT filename
       FROM schema_migrations`
    );
    const applied = new Set(appliedResult.rows.map((row) => row.filename));

    for (const migration of migrations) {
      if (applied.has(migration.filename)) {
        continue;
      }

      if (typeof tx.exec === 'function') {
        await tx.exec(migration.sql);
      } else {
        await tx.query(migration.sql);
      }

      await tx.query(
        `INSERT INTO schema_migrations (filename)
         VALUES ($1)`,
        [migration.filename]
      );
      appliedMigrations.push(migration.filename);
    }
  });

  return {
    appliedMigrations
  };
}

module.exports = {
  MIGRATIONS_DIR,
  MIGRATIONS_TABLE_SQL,
  listMigrationFiles,
  loadMigrations,
  initializeSchema
};
