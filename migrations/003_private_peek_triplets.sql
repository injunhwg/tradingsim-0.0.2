ALTER TABLE private_peeks
  ADD COLUMN IF NOT EXISTS payload_json JSONB;

ALTER TABLE private_peeks
  ALTER COLUMN card_id DROP NOT NULL;

UPDATE private_peeks pp
SET payload_json = jsonb_build_object(
  'sampleCardIds',
  jsonb_build_array(sc.id),
  'contributionsCents',
  jsonb_build_array(sc.contribution_cents)
)
FROM session_cards sc
WHERE pp.card_id = sc.id
  AND pp.payload_json IS NULL;
