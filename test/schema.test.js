const test = require('node:test');
const assert = require('node:assert/strict');

const { createPGliteDatabase } = require('../src');
const { initializeSchema, listMigrationFiles } = require('../src/db/schema');

test('initializeSchema applies tracked migrations exactly once', async () => {
  const db = await createPGliteDatabase();

  try {
    await initializeSchema(db);
    await initializeSchema(db);

    const appliedResult = await db.query(
      `SELECT filename
       FROM schema_migrations
       ORDER BY filename`
    );
    const tableResult = await db.query(
      `SELECT EXISTS (
         SELECT 1
         FROM information_schema.tables
         WHERE table_name = 'market_sessions'
       ) AS present`
    );

    assert.deepEqual(
      appliedResult.rows.map((row) => row.filename),
      listMigrationFiles()
    );
    assert.equal(tableResult.rows[0].present, true);
  } finally {
    await db.close();
  }
});
