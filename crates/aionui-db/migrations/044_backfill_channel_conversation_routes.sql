-- Restore reply anchors written by the pre-Core Channels implementation.
-- Runtime routing reads only channel_conversation_routes after this one-time
-- backfill; the legacy JSON remains historical conversation metadata.

INSERT OR IGNORE INTO channel_conversation_routes (
    owner_user_id,
    platform_type,
    chat_id,
    message_id,
    conversation_id,
    preview_text,
    sent_at,
    delivery_started_at,
    delivery_completed_at
)
SELECT
    c.user_id,
    lower(json_extract(target.value, '$.platform')),
    trim(json_extract(target.value, '$.chatId')),
    trim(message.value),
    c.id,
    COALESCE(
        json_extract(target.value, '$.previewTexts[' || message.key || ']'),
        json_extract(target.value, '$.lastPreviewText')
    ),
    COALESCE(
        json_extract(target.value, '$.messageTimestamps[' || message.key || ']'),
        json_extract(target.value, '$.updatedAt'),
        c.updated_at
    ),
    json_extract(target.value, '$.deliveryWindows[' || message.key || '].startedAt'),
    json_extract(target.value, '$.deliveryWindows[' || message.key || '].completedAt')
FROM conversations c
JOIN json_each(c.extra, '$.channelRoutes.entries') AS target
JOIN json_each(target.value, '$.messageIds') AS message
WHERE c.user_id IS NOT NULL
  AND trim(c.user_id) != ''
  AND json_type(target.value, '$.platform') = 'text'
  AND trim(json_extract(target.value, '$.platform')) != ''
  AND json_type(target.value, '$.chatId') = 'text'
  AND trim(json_extract(target.value, '$.chatId')) != ''
  AND message.type = 'text'
  AND trim(message.value) != '';

-- Older records stored one unscoped channelRoute. Conversation source and
-- channel_chat_id provide the missing target identity.
INSERT OR IGNORE INTO channel_conversation_routes (
    owner_user_id,
    platform_type,
    chat_id,
    message_id,
    conversation_id,
    preview_text,
    sent_at,
    delivery_started_at,
    delivery_completed_at
)
SELECT
    c.user_id,
    lower(c.source),
    trim(c.channel_chat_id),
    trim(message.value),
    c.id,
    COALESCE(
        json_extract(c.extra, '$.channelRoute.previewTexts[' || message.key || ']'),
        json_extract(c.extra, '$.channelRoute.lastPreviewText')
    ),
    COALESCE(
        json_extract(c.extra, '$.channelRoute.messageTimestamps[' || message.key || ']'),
        json_extract(c.extra, '$.channelRoute.updatedAt'),
        c.updated_at
    ),
    json_extract(c.extra, '$.channelRoute.deliveryWindows[' || message.key || '].startedAt'),
    json_extract(c.extra, '$.channelRoute.deliveryWindows[' || message.key || '].completedAt')
FROM conversations c
JOIN json_each(c.extra, '$.channelRoute.messageIds') AS message
WHERE c.user_id IS NOT NULL
  AND trim(c.user_id) != ''
  AND c.source IN ('weixin', 'lark')
  AND c.channel_chat_id IS NOT NULL
  AND trim(c.channel_chat_id) != ''
  AND message.type = 'text'
  AND trim(message.value) != '';
