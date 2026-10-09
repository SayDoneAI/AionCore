ALTER TABLE channel_incoming_message_claims ADD COLUMN claim_token TEXT;

UPDATE channel_incoming_message_claims
SET claim_token = 'legacy-' || owner_user_id || '-' || platform_type || '-' || chat_id || '-' || message_id
WHERE claim_token IS NULL OR claim_token = '';

CREATE INDEX IF NOT EXISTS idx_channel_incoming_message_claims_token
    ON channel_incoming_message_claims(claim_token);
