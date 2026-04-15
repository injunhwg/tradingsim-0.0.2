const { loadConfig } = require('../src/config');
const { createPgDatabase } = require('../src/db/database');
const { initializeSchema } = require('../src/db/schema');

async function main() {
  const config = loadConfig();
  if (!config.DATABASE_URL) {
    throw new Error('DATABASE_URL is required to run migrations.');
  }

  const db = createPgDatabase(config.DATABASE_URL);

  try {
    const result = await initializeSchema(db);
    console.log(
      JSON.stringify({
        level: 'info',
        message: 'Schema initialized successfully.',
        appliedMigrations: result.appliedMigrations
      })
    );
  } finally {
    await db.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
