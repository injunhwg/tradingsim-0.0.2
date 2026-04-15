const { Pool } = require('pg');
const { PGlite } = require('@electric-sql/pglite');

function normalizeResult(result) {
  if (!result) {
    return { rows: [], rowCount: 0 };
  }

  if (Array.isArray(result.rows)) {
    return {
      rows: result.rows,
      rowCount: typeof result.rowCount === 'number' ? result.rowCount : result.rows.length
    };
  }

  if (Array.isArray(result)) {
    return { rows: result, rowCount: result.length };
  }

  if (Array.isArray(result.records)) {
    return { rows: result.records, rowCount: result.records.length };
  }

  return { rows: [], rowCount: 0 };
}

class QueryExecutor {
  constructor(client) {
    this.client = client;
  }

  async query(text, params = []) {
    const result = await this.client.query(text, params);
    return normalizeResult(result);
  }

  async exec(text) {
    if (typeof this.client.exec === 'function') {
      return this.client.exec(text);
    }

    return this.client.query(text);
  }
}

class PgDatabase {
  constructor(connectionString, options = {}) {
    this.pool = new Pool({
      connectionString,
      max: options.max ?? 10
    });
  }

  async query(text, params = []) {
    const result = await this.pool.query(text, params);
    return normalizeResult(result);
  }

  async exec(text) {
    await this.pool.query(text);
  }

  async withTransaction(handler) {
    const client = await this.pool.connect();
    const executor = new QueryExecutor(client);

    try {
      await client.query('BEGIN');
      const value = await handler(executor);
      await client.query('COMMIT');
      return value;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}

class PGliteDatabase {
  constructor(db) {
    this.db = db;
  }

  async query(text, params = []) {
    const result = await this.db.query(text, params);
    return normalizeResult(result);
  }

  async exec(text) {
    if (typeof this.db.exec === 'function') {
      await this.db.exec(text);
      return;
    }

    await this.db.query(text);
  }

  async withTransaction(handler) {
    const executor = new QueryExecutor(this.db);

    await this.db.query('BEGIN');
    try {
      const value = await handler(executor);
      await this.db.query('COMMIT');
      return value;
    } catch (error) {
      await this.db.query('ROLLBACK');
      throw error;
    }
  }

  async close() {
    if (typeof this.db.close === 'function') {
      await this.db.close();
    }
  }
}

async function createPGliteDatabase(options = {}) {
  const db = options.dataDir ? new PGlite(options.dataDir) : new PGlite();
  return new PGliteDatabase(db);
}

function createPgDatabase(connectionString, options = {}) {
  return new PgDatabase(connectionString, options);
}

module.exports = {
  createPgDatabase,
  createPGliteDatabase
};
