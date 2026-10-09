ALTER TABLE channel_incoming_message_claims ADD COLUMN status TEXT NOT NULL DEFAULT 'processing';
ALTER TABLE channel_incoming_message_claims ADD COLUMN lease_until INTEGER NOT NULL DEFAULT 0;
ALTER TABLE channel_incoming_message_claims ADD COLUMN attempts INTEGER NOT NULL DEFAULT 1;
ALTER TABLE channel_incoming_message_claims ADD COLUMN completed_at INTEGER;
ALTER TABLE channel_incoming_message_claims ADD COLUMN last_error TEXT;

UPDATE channel_incoming_message_claims
SET status = 'processing',
    lease_until = 0,
    attempts = CASE WHEN attempts < 1 THEN 1 ELSE attempts END
WHERE status IS NULL OR status = '';

CREATE INDEX IF NOT EXISTS idx_channel_incoming_message_claims_lease
    ON channel_incoming_message_claims(status, lease_until);
