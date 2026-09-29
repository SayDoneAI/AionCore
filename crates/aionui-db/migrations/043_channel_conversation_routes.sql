CREATE TABLE IF NOT EXISTS channel_conversation_routes (
    owner_user_id         TEXT    NOT NULL,
    platform_type         TEXT    NOT NULL,
    chat_id               TEXT    NOT NULL,
    message_id            TEXT    NOT NULL,
    conversation_id       TEXT    NOT NULL,
    preview_text          TEXT,
    sent_at               INTEGER NOT NULL,
    delivery_started_at   INTEGER,
    delivery_completed_at INTEGER,
    PRIMARY KEY (owner_user_id, platform_type, chat_id, message_id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_conversation_routes_conversation_sent
    ON channel_conversation_routes(conversation_id, platform_type, chat_id, sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_channel_conversation_routes_preview
    ON channel_conversation_routes(owner_user_id, platform_type, chat_id, preview_text);
