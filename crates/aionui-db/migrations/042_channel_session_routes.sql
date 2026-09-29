CREATE TABLE IF NOT EXISTS channel_session_routes (
    owner_user_id  TEXT    NOT NULL,
    platform_type  TEXT    NOT NULL,
    chat_id        TEXT    NOT NULL,
    channel_user_id TEXT   NOT NULL,
    message_id     TEXT    NOT NULL,
    session_id     TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (owner_user_id, platform_type, chat_id, channel_user_id, message_id),
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_session_routes_session_created
    ON channel_session_routes(session_id, created_at DESC);
