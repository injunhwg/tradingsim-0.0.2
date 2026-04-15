ALTER TABLE market_sessions
  ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS elapsed_open_seconds INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_duration_seconds INTEGER NOT NULL DEFAULT 1500,
  ADD COLUMN IF NOT EXISTS liquidation_value_cents INTEGER,
  ADD COLUMN IF NOT EXISTS liquidation_revealed_at TIMESTAMPTZ;

ALTER TABLE announcements
  ADD COLUMN IF NOT EXISTS announcement_type TEXT NOT NULL DEFAULT 'MANUAL',
  ADD COLUMN IF NOT EXISTS payload_json JSONB;

CREATE TABLE IF NOT EXISTS session_cards (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  deck_order INTEGER NOT NULL,
  rank TEXT NOT NULL,
  suit TEXT NOT NULL,
  color TEXT NOT NULL CHECK (color IN ('BLACK', 'RED')),
  label TEXT NOT NULL,
  base_value_cents INTEGER NOT NULL,
  contribution_cents INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DECK', 'REMOVED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, deck_order)
);

CREATE INDEX IF NOT EXISTS session_cards_session_state_idx
  ON session_cards (session_id, state, deck_order);

CREATE TABLE IF NOT EXISTS scheduled_public_info (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  info_key TEXT NOT NULL,
  info_type TEXT NOT NULL CHECK (info_type IN ('SAR', 'EPS')),
  sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
  scheduled_offset_seconds INTEGER NOT NULL CHECK (scheduled_offset_seconds >= 0),
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RELEASED', 'CANCELLED')),
  released_at TIMESTAMPTZ,
  announcement_id INTEGER REFERENCES announcements(id) ON DELETE SET NULL,
  payload_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, info_key),
  UNIQUE (session_id, info_type, sequence_no)
);

CREATE INDEX IF NOT EXISTS scheduled_public_info_due_idx
  ON scheduled_public_info (session_id, status, scheduled_offset_seconds);

CREATE TABLE IF NOT EXISTS private_peeks (
  id SERIAL PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES market_sessions(id) ON DELETE CASCADE,
  participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
  card_id INTEGER NOT NULL REFERENCES session_cards(id) ON DELETE RESTRICT,
  price_cents INTEGER NOT NULL DEFAULT 100 CHECK (price_cents >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS private_peeks_participant_idx
  ON private_peeks (participant_id, id DESC);
