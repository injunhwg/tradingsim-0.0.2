CREATE TABLE IF NOT EXISTS market_sessions (
  id SERIAL PRIMARY KEY,
  session_name TEXT NOT NULL DEFAULT 'Classroom Session',
  join_code TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'PAUSED', 'CLOSED')),
  reference_price_cents INTEGER NOT NULL DEFAULT 1000 CHECK (reference_price_cents > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS market_sessions_single_active_idx
  ON market_sessions ((1))
  WHERE status IN ('OPEN', 'PAUSED');

CREATE TABLE IF NOT EXISTS participants (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  external_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'STUDENT' CHECK (role IN ('STUDENT', 'INSTRUCTOR')),
  auth_token TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, external_id)
);

CREATE INDEX IF NOT EXISTS participants_session_role_idx
  ON participants (session_id, role);

CREATE TABLE IF NOT EXISTS accounts (
  participant_id INTEGER PRIMARY KEY REFERENCES participants(id) ON DELETE CASCADE,
  cash_cents INTEGER NOT NULL DEFAULT 20000,
  position_qty INTEGER NOT NULL DEFAULT 5,
  reserved_buy_cents INTEGER NOT NULL DEFAULT 0,
  reserved_sell_qty INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (cash_cents >= -20000),
  CHECK (position_qty >= -5),
  CHECK (reserved_buy_cents >= 0),
  CHECK (reserved_sell_qty >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE RESTRICT,
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  order_type TEXT NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT')),
  limit_price_cents INTEGER,
  original_qty INTEGER NOT NULL CHECK (original_qty > 0),
  remaining_qty INTEGER NOT NULL CHECK (remaining_qty >= 0 AND remaining_qty <= original_qty),
  status TEXT NOT NULL CHECK (status IN ('OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED')),
  rejection_reason TEXT,
  cancel_reason TEXT,
  replaces_order_id INTEGER REFERENCES orders(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (
    (order_type = 'MARKET' AND limit_price_cents IS NULL) OR
    (order_type = 'LIMIT' AND limit_price_cents IS NOT NULL AND limit_price_cents > 0)
  )
);

CREATE INDEX IF NOT EXISTS orders_session_side_status_idx
  ON orders (session_id, side, status, limit_price_cents, id);

CREATE INDEX IF NOT EXISTS orders_participant_status_idx
  ON orders (participant_id, status);

CREATE TABLE IF NOT EXISTS fills (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  buy_order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
  sell_order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
  aggressor_order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
  resting_order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
  price_cents INTEGER NOT NULL CHECK (price_cents > 0),
  qty INTEGER NOT NULL CHECK (qty > 0),
  executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS fills_session_idx
  ON fills (session_id, id);

CREATE TABLE IF NOT EXISTS announcements (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  actor_participant_id INTEGER REFERENCES participants(id) ON DELETE SET NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (LENGTH(TRIM(message)) > 0)
);

CREATE INDEX IF NOT EXISTS announcements_session_idx
  ON announcements (session_id, id DESC);

CREATE TABLE IF NOT EXISTS scheduled_announcements (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  actor_participant_id INTEGER REFERENCES participants(id) ON DELETE SET NULL,
  message TEXT NOT NULL,
  scheduled_for TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED', 'PUBLISHED', 'CANCELLED')),
  published_announcement_id INTEGER REFERENCES announcements(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,
  CHECK (LENGTH(TRIM(message)) > 0)
);

CREATE INDEX IF NOT EXISTS scheduled_announcements_due_idx
  ON scheduled_announcements (status, scheduled_for);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (participant_id, idempotency_key)
);
