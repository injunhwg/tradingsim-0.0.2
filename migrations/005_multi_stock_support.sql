CREATE TABLE IF NOT EXISTS session_stocks (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  stock_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  sort_order INTEGER NOT NULL CHECK (sort_order > 0),
  reference_price_cents INTEGER NOT NULL CHECK (reference_price_cents > 0),
  initial_position_qty INTEGER NOT NULL DEFAULT 5,
  liquidation_value_cents INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, stock_key),
  UNIQUE (session_id, sort_order)
);

CREATE INDEX IF NOT EXISTS session_stocks_session_sort_idx
  ON session_stocks (session_id, sort_order, id);

CREATE TABLE IF NOT EXISTS account_holdings (
  participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  session_stock_id INTEGER NOT NULL REFERENCES session_stocks(id) ON DELETE CASCADE,
  position_qty INTEGER NOT NULL DEFAULT 0,
  reserved_sell_qty INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (participant_id, session_stock_id),
  CHECK (position_qty >= -5),
  CHECK (reserved_sell_qty >= 0)
);

CREATE INDEX IF NOT EXISTS account_holdings_stock_idx
  ON account_holdings (session_stock_id, participant_id);

ALTER TABLE session_cards
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE CASCADE;

ALTER TABLE scheduled_public_info
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE CASCADE;

ALTER TABLE private_peeks
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE CASCADE;

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE CASCADE;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE CASCADE;

ALTER TABLE announcements
  ADD COLUMN IF NOT EXISTS session_stock_id INTEGER REFERENCES session_stocks(id) ON DELETE SET NULL;

ALTER TABLE session_cards
  DROP CONSTRAINT IF EXISTS session_cards_session_id_deck_order_key;

ALTER TABLE scheduled_public_info
  DROP CONSTRAINT IF EXISTS scheduled_public_info_session_id_info_type_sequence_no_key;

CREATE UNIQUE INDEX IF NOT EXISTS session_cards_stock_deck_order_idx
  ON session_cards (session_stock_id, deck_order);

CREATE UNIQUE INDEX IF NOT EXISTS scheduled_public_info_stock_sequence_idx
  ON scheduled_public_info (session_id, session_stock_id, info_type, sequence_no);

CREATE INDEX IF NOT EXISTS session_cards_session_stock_state_idx
  ON session_cards (session_id, session_stock_id, state, deck_order);

CREATE INDEX IF NOT EXISTS scheduled_public_info_stock_due_idx
  ON scheduled_public_info (session_id, session_stock_id, status, scheduled_offset_seconds);

CREATE INDEX IF NOT EXISTS private_peeks_participant_stock_idx
  ON private_peeks (participant_id, session_stock_id, id DESC);

CREATE INDEX IF NOT EXISTS orders_session_stock_side_status_idx
  ON orders (session_id, session_stock_id, side, status, limit_price_cents, id);

CREATE INDEX IF NOT EXISTS fills_session_stock_idx
  ON fills (session_id, session_stock_id, id);

CREATE INDEX IF NOT EXISTS announcements_session_stock_idx
  ON announcements (session_id, session_stock_id, id DESC);
