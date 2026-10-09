CREATE TABLE IF NOT EXISTS channel_incoming_message_claims (
    owner_user_id TEXT NOT NULL,
    platform_type TEXT NOT NULL,
    chat_id       TEXT NOT NULL,
    message_id    TEXT NOT NULL,
    claimed_at    INTEGER NOT NULL,
    PRIMARY KEY (owner_user_id, platform_type, chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_incoming_message_claims_claimed_at
    ON channel_incoming_message_claims(claimed_at);
