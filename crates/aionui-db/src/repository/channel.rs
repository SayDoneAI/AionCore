use aionui_common::TimestampMs;

use crate::error::DbError;
use crate::models::{AssistantSessionRow, AssistantUserRow, ChannelPluginRow, PairingCodeRow};

/// Data access abstraction for channel integration tables.
///
/// Covers four tables: `assistant_plugins`, `assistant_users`,
/// `assistant_sessions`, and `assistant_pairing_codes`.
///
/// Object-safe via `async_trait` to support `Arc<dyn IChannelRepository>`.
#[async_trait::async_trait]
pub trait IChannelRepository: Send + Sync {
    // ── Plugin CRUD ──────────────────────────────────────────────────

    /// Returns all registered plugins for an owner.
    async fn get_all_plugins(&self, owner_user_id: &str) -> Result<Vec<ChannelPluginRow>, DbError>;

    /// Returns a single plugin by id, or `None` if not found.
    async fn get_plugin(&self, owner_user_id: &str, id: &str) -> Result<Option<ChannelPluginRow>, DbError>;

    /// Inserts a new plugin or updates an existing one (by id).
    async fn upsert_plugin(&self, owner_user_id: &str, row: &ChannelPluginRow) -> Result<(), DbError>;

    /// Updates only the `status` and `last_connected` of a plugin.
    async fn update_plugin_status(
        &self,
        owner_user_id: &str,
        id: &str,
        params: &UpdatePluginStatusParams,
    ) -> Result<(), DbError>;

    /// Deletes a plugin by id. Returns `DbError::NotFound` if absent.
    async fn delete_plugin(&self, owner_user_id: &str, id: &str) -> Result<(), DbError>;

    // ── User CRUD ────────────────────────────────────────────────────

    /// Returns all authorized users for an owner.
    async fn get_all_users(&self, owner_user_id: &str) -> Result<Vec<AssistantUserRow>, DbError>;

    /// Finds a user by platform identity. Returns `None` if not found.
    async fn get_user_by_platform(
        &self,
        owner_user_id: &str,
        platform_user_id: &str,
        platform_type: &str,
    ) -> Result<Option<AssistantUserRow>, DbError>;

    /// Creates a new authorized user record.
    async fn create_user(&self, owner_user_id: &str, row: &AssistantUserRow) -> Result<(), DbError>;

    /// Updates `last_active` timestamp for a user.
    async fn update_user_last_active(
        &self,
        owner_user_id: &str,
        id: &str,
        last_active: TimestampMs,
    ) -> Result<(), DbError>;

    /// Deletes a user by id. Returns `DbError::NotFound` if absent.
    /// Associated sessions are cascade-deleted by the database.
    async fn delete_user(&self, owner_user_id: &str, id: &str) -> Result<(), DbError>;

    // ── Session CRUD ─────────────────────────────────────────────────

    /// Returns all sessions for an owner.
    async fn get_all_sessions(&self, owner_user_id: &str) -> Result<Vec<AssistantSessionRow>, DbError>;

    /// Returns a single session by id.
    async fn get_session(&self, owner_user_id: &str, id: &str) -> Result<Option<AssistantSessionRow>, DbError>;

    /// Finds an existing session by user + chat, or creates a new one.
    /// If found, updates `last_activity` and returns the existing row.
    /// If not found, inserts `new_row` and returns it.
    async fn get_or_create_session(
        &self,
        owner_user_id: &str,
        channel_user_id: &str,
        chat_id: &str,
        new_row: &AssistantSessionRow,
    ) -> Result<AssistantSessionRow, DbError>;

    /// Inserts a new session even when the same user and chat already have sessions.
    async fn create_session(
        &self,
        owner_user_id: &str,
        channel_user_id: &str,
        new_row: &AssistantSessionRow,
    ) -> Result<AssistantSessionRow, DbError> {
        let _ = (owner_user_id, channel_user_id, new_row);
        Err(DbError::Init("create_session is not implemented".into()))
    }

    /// Resolves a platform reply target to its exact channel session.
    async fn get_session_by_route(
        &self,
        owner_user_id: &str,
        channel_user_id: &str,
        platform_type: &str,
        chat_id: &str,
        message_id: &str,
    ) -> Result<Option<AssistantSessionRow>, DbError> {
        let _ = (owner_user_id, channel_user_id, platform_type, chat_id, message_id);
        Err(DbError::Init("get_session_by_route is not implemented".into()))
    }

    /// Binds a platform message to a session and keeps recent route history bounded.
    async fn upsert_session_route(
        &self,
        owner_user_id: &str,
        session_id: &str,
        platform_type: &str,
        chat_id: &str,
        message_id: &str,
        created_at: TimestampMs,
    ) -> Result<(), DbError> {
        let _ = (
            owner_user_id,
            session_id,
            platform_type,
            chat_id,
            message_id,
            created_at,
        );
        Err(DbError::Init("upsert_session_route is not implemented".into()))
    }

    // ── Conversation reply routes ───────────────────────────────────

    /// Binds an outgoing platform message to any desktop conversation.
    async fn upsert_conversation_route(
        &self,
        _owner_user_id: &str,
        _params: &UpsertChannelConversationRouteParams<'_>,
    ) -> Result<(), DbError> {
        Err(DbError::Init("upsert_conversation_route is not implemented".into()))
    }

    /// Resolves an exact quoted platform message to its desktop conversation.
    async fn resolve_conversation_route(
        &self,
        _owner_user_id: &str,
        _platform_type: &str,
        _chat_id: &str,
        _message_id: &str,
    ) -> Result<Option<String>, DbError> {
        Err(DbError::Init("resolve_conversation_route is not implemented".into()))
    }

    /// Resolves quoted preview text. When `require_unique` is true, ambiguous
    /// matches intentionally return `None`.
    async fn resolve_conversation_route_by_preview(
        &self,
        _owner_user_id: &str,
        _platform_type: &str,
        _chat_id: &str,
        _quoted_text: &str,
        _require_unique: bool,
    ) -> Result<Option<String>, DbError> {
        Err(DbError::Init(
            "resolve_conversation_route_by_preview is not implemented".into(),
        ))
    }

    /// Resolves a WeChat snowflake timestamp against recorded send/delivery times.
    async fn resolve_conversation_route_by_timestamp(
        &self,
        _owner_user_id: &str,
        _platform_type: &str,
        _chat_id: &str,
        _reply_timestamp: TimestampMs,
        _max_skew_ms: TimestampMs,
    ) -> Result<Option<String>, DbError> {
        Err(DbError::Init(
            "resolve_conversation_route_by_timestamp is not implemented".into(),
        ))
    }

    /// Updates `last_activity` timestamp for a session.
    async fn update_session_activity(
        &self,
        owner_user_id: &str,
        id: &str,
        last_activity: TimestampMs,
    ) -> Result<(), DbError>;

    /// Updates the `conversation_id` of a session.
    async fn update_session_conversation(
        &self,
        owner_user_id: &str,
        id: &str,
        conversation_id: &str,
    ) -> Result<(), DbError>;

    /// Updates the `agent_type` of a session.
    async fn update_session_agent_type(&self, owner_user_id: &str, id: &str, agent_type: &str) -> Result<(), DbError>;

    /// Deletes all sessions belonging to a user.
    async fn delete_sessions_by_user(&self, owner_user_id: &str, channel_user_id: &str) -> Result<(), DbError>;

    /// Deletes the session for a specific user + chat pair.
    async fn delete_session_by_user_chat(
        &self,
        owner_user_id: &str,
        channel_user_id: &str,
        chat_id: &str,
    ) -> Result<(), DbError>;

    // ── Pairing Codes ────────────────────────────────────────────────

    /// Creates a new pairing code record.
    async fn create_pairing(&self, owner_user_id: &str, row: &PairingCodeRow) -> Result<(), DbError>;

    /// Returns all pairing codes with status = 'pending'.
    async fn get_pending_pairings(&self, owner_user_id: &str) -> Result<Vec<PairingCodeRow>, DbError>;

    /// Retrieves a single pairing code, or `None` if not found.
    async fn get_pairing_by_code(&self, owner_user_id: &str, code: &str) -> Result<Option<PairingCodeRow>, DbError>;

    /// Updates the status of a pairing code.
    /// Returns `DbError::NotFound` if the code doesn't exist.
    async fn update_pairing_status(&self, owner_user_id: &str, code: &str, status: &str) -> Result<(), DbError>;

    /// Marks all expired-but-still-pending pairing codes as 'expired'.
    /// `now` is the current timestamp in milliseconds.
    async fn cleanup_expired_pairings(&self, owner_user_id: &str, now: TimestampMs) -> Result<u64, DbError>;
}

/// Parameters for updating plugin runtime status.
#[derive(Debug, Clone, Default)]
pub struct UpdatePluginStatusParams {
    pub status: Option<String>,
    pub last_connected: Option<TimestampMs>,
    pub enabled: Option<bool>,
}

/// Parameters for recording one outgoing channel message as a reply target.
#[derive(Debug, Clone)]
pub struct UpsertChannelConversationRouteParams<'a> {
    pub conversation_id: &'a str,
    pub platform_type: &'a str,
    pub chat_id: &'a str,
    pub message_id: &'a str,
    pub preview_text: Option<&'a str>,
    pub sent_at: TimestampMs,
    pub delivery_started_at: Option<TimestampMs>,
    pub delivery_completed_at: Option<TimestampMs>,
}
