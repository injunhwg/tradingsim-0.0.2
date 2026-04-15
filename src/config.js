const dotenv = require('dotenv');
const {
  DEFAULT_BORROW_INTEREST_BPS,
  DEFAULT_GAME_DURATION_SECONDS,
  DEFAULT_PEEK_PRICE_CENTS,
  DEFAULT_REFERENCE_PRICE_CENTS
} = require('./services/classroomGame');

dotenv.config();

function getOptionalInteger(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) ? parsed : fallback;
}

function loadConfig(overrides = {}) {
  const env = {
    NODE_ENV: process.env.NODE_ENV || 'development',
    HOST: process.env.HOST || '0.0.0.0',
    PORT: getOptionalInteger(process.env.PORT, 3000),
    DATABASE_URL: process.env.DATABASE_URL || '',
    BOOTSTRAP_ADMIN_SECRET: process.env.BOOTSTRAP_ADMIN_SECRET || '',
    DEFAULT_REFERENCE_PRICE_CENTS: getOptionalInteger(process.env.DEFAULT_REFERENCE_PRICE_CENTS, DEFAULT_REFERENCE_PRICE_CENTS),
    RECENT_TRADES_LIMIT: getOptionalInteger(process.env.RECENT_TRADES_LIMIT, 20),
    ANNOUNCEMENT_HISTORY_LIMIT: getOptionalInteger(process.env.ANNOUNCEMENT_HISTORY_LIMIT, 20),
    ANNOUNCEMENT_POLL_INTERVAL_MS: getOptionalInteger(process.env.ANNOUNCEMENT_POLL_INTERVAL_MS, 1000),
    GAME_DURATION_SECONDS: getOptionalInteger(process.env.GAME_DURATION_SECONDS, DEFAULT_GAME_DURATION_SECONDS),
    PEEK_PRICE_CENTS: getOptionalInteger(process.env.PEEK_PRICE_CENTS, DEFAULT_PEEK_PRICE_CENTS),
    BORROW_INTEREST_BPS: getOptionalInteger(process.env.BORROW_INTEREST_BPS, DEFAULT_BORROW_INTEREST_BPS)
  };

  return {
    ...env,
    ...overrides
  };
}

module.exports = {
  loadConfig
};
