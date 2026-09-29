//! Black-box integration tests for SessionManager and ActionExecutor.
//!
//! Uses real SQLite (in-memory) and mock EventBroadcaster.
//! Covers test-plan items: GS-1, GS-2, PC-1..PC-3, RU-3.

use std::sync::{Arc, Mutex};

use aionui_api_types::WebSocketMessage;
use aionui_common::{generate_id, now_ms};
use aionui_db::models::{AssistantUserRow, ConversationRow};
use aionui_db::{
    IChannelRepository, IConversationRepository, SqliteChannelRepository, SqliteConversationRepository,
    init_database_memory,
};
use aionui_realtime::EventBroadcaster;

use aionui_channel::action::{ActionExecutor, MessageResult};
use aionui_channel::channel_settings::ChannelSettingsService;
use aionui_channel::pairing::PairingService;
use aionui_channel::session::SessionManager;
use aionui_channel::types::{
    ActionBehavior, ActionCategory, ActionContext, MessageContentType, PluginType, UnifiedAction,
    UnifiedIncomingMessage, UnifiedMessageContent, UnifiedUser,
};

// ── Test infrastructure ─────────────────────────────────────────────
const OWNER_ID: &str = "system_default_user";

struct MockBroadcaster {
    events: Mutex<Vec<WebSocketMessage<serde_json::Value>>>,
}

impl MockBroadcaster {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
}

impl EventBroadcaster for MockBroadcaster {
    fn broadcast(&self, event: WebSocketMessage<serde_json::Value>) {
        self.events.lock().unwrap().push(event);
    }
}

async fn setup() -> (
    SessionManager,
    ActionExecutor,
    PairingService,
    Arc<dyn IChannelRepository>,
) {
    let db = init_database_memory().await.unwrap();
    let repo: Arc<dyn IChannelRepository> = Arc::new(SqliteChannelRepository::new(db.pool().clone()));
    let bc: Arc<dyn EventBroadcaster> = Arc::new(MockBroadcaster::new());

    let session_mgr = SessionManager::new(repo.clone());
    let pairing = PairingService::new(repo.clone(), bc);
    let pairing_arc = Arc::new(PairingService::new(repo.clone(), Arc::new(MockBroadcaster::new())));
    let session_mgr_arc = Arc::new(SessionManager::new(repo.clone()));
    let pref_repo: Arc<dyn aionui_db::IClientPreferenceRepository> =
        Arc::new(aionui_db::SqliteClientPreferenceRepository::new(db.pool().clone()));
    let settings = Arc::new(ChannelSettingsService::new(pref_repo));
    let executor = ActionExecutor::new(pairing_arc, session_mgr_arc, settings, Some(OWNER_ID.to_owned()));

    let conversation_repo = SqliteConversationRepository::new(db.pool().clone());
    for (conversation_id, updated_at) in [("desktop-a", 100_i64), ("desktop-b", 200_i64)] {
        conversation_repo
            .create(&ConversationRow {
                id: conversation_id.to_owned(),
                user_id: OWNER_ID.to_owned(),
                name: conversation_id.to_owned(),
                r#type: "acp".to_owned(),
                extra: "{}".to_owned(),
                model: None,
                status: Some("pending".to_owned()),
                source: Some("aionui".to_owned()),
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: updated_at,
                updated_at,
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();
    }

    // Keep db alive
    std::mem::forget(db);
    (session_mgr, executor, pairing, repo)
}

/// Create an assistant_users record (required for FK on sessions).
async fn create_user(repo: &Arc<dyn IChannelRepository>, platform_user_id: &str, platform_type: &str) -> String {
    let user_id = generate_id();
    let row = AssistantUserRow {
        id: user_id.clone(),
        owner_user_id: OWNER_ID.to_owned(),
        platform_user_id: platform_user_id.to_owned(),
        platform_type: platform_type.to_owned(),
        display_name: Some("Test User".into()),
        authorized_at: now_ms(),
        last_active: None,
        session_id: None,
    };
    repo.create_user(OWNER_ID, &row).await.unwrap();
    user_id
}

fn make_text_message(user_id: &str, chat_id: &str, text: &str) -> UnifiedIncomingMessage {
    UnifiedIncomingMessage {
        owner_user_id: None,
        id: format!("msg_{}", now_ms()),
        platform: PluginType::Telegram,
        chat_id: chat_id.into(),
        user: UnifiedUser {
            id: user_id.into(),
            username: None,
            display_name: "Test User".into(),
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type: MessageContentType::Text,
            text: text.into(),
            attachments: None,
        },
        timestamp: now_ms(),
        reply_to_message_id: None,
        action: None,
        raw: None,
    }
}

fn make_reply_message(user_id: &str, chat_id: &str, text: &str, reply_to_message_id: &str) -> UnifiedIncomingMessage {
    UnifiedIncomingMessage {
        reply_to_message_id: Some(reply_to_message_id.into()),
        ..make_text_message(user_id, chat_id, text)
    }
}

fn make_action_message(
    user_id: &str,
    chat_id: &str,
    action_name: &str,
    category: ActionCategory,
) -> UnifiedIncomingMessage {
    UnifiedIncomingMessage {
        owner_user_id: None,
        id: format!("msg_{}", now_ms()),
        platform: PluginType::Telegram,
        chat_id: chat_id.into(),
        user: UnifiedUser {
            id: user_id.into(),
            username: None,
            display_name: "Test User".into(),
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type: MessageContentType::Action,
            text: String::new(),
            attachments: None,
        },
        timestamp: now_ms(),
        reply_to_message_id: None,
        action: Some(UnifiedAction {
            action: action_name.into(),
            category,
            params: None,
            context: ActionContext {
                platform: PluginType::Telegram,
                user_id: user_id.into(),
                chat_id: chat_id.into(),
                message_id: None,
                session_id: None,
            },
        }),
        raw: None,
    }
}

/// Helper: authorize a user via the pairing flow.
async fn authorize_user(pairing: &PairingService, platform_user_id: &str, platform_type: &str) {
    let code = pairing
        .request_pairing(OWNER_ID, platform_user_id, platform_type, Some("Test"))
        .await
        .unwrap();
    pairing.approve_pairing(OWNER_ID, &code).await.unwrap();
}

// ── GS-1: No active sessions returns empty ─────────────────────────

#[tokio::test]
async fn gs1_no_sessions_returns_empty() {
    let (session_mgr, _, _, _) = setup().await;
    let sessions = session_mgr.get_active_sessions(OWNER_ID).await.unwrap();
    assert!(sessions.is_empty());
}

// ── GS-2: Multiple active sessions returned ────────────────────────

#[tokio::test]
async fn gs2_multiple_sessions_returned() {
    let (session_mgr, _, _, repo) = setup().await;

    // Create users first (FK constraint)
    let uid1 = create_user(&repo, "p1", "telegram").await;
    let uid2 = create_user(&repo, "p2", "telegram").await;

    session_mgr
        .get_or_create_session(OWNER_ID, &uid1, "c1", "gemini", None)
        .await
        .unwrap();
    session_mgr
        .get_or_create_session(OWNER_ID, &uid2, "c2", "acp", None)
        .await
        .unwrap();

    let sessions = session_mgr.get_active_sessions(OWNER_ID).await.unwrap();
    assert_eq!(sessions.len(), 2);

    for s in &sessions {
        assert!(!s.id.is_empty());
        assert!(!s.user_id.is_empty());
        assert!(!s.agent_type.is_empty());
        assert!(s.chat_id.is_some());
        assert!(s.created_at > 0);
        assert!(s.last_activity > 0);
    }
}

// ── PC-1: Same user, different chatId → different sessions ─────────

#[tokio::test]
async fn pc1_same_user_different_chat() {
    let (session_mgr, _, _, repo) = setup().await;

    let uid = create_user(&repo, "p1", "telegram").await;

    let s1 = session_mgr
        .get_or_create_session(OWNER_ID, &uid, "chatA", "gemini", None)
        .await
        .unwrap();
    let s2 = session_mgr
        .get_or_create_session(OWNER_ID, &uid, "chatB", "gemini", None)
        .await
        .unwrap();

    assert_ne!(s1.id, s2.id);
    assert_eq!(s1.user_id, uid);
    assert_eq!(s2.user_id, uid);
    assert_eq!(s1.chat_id.as_deref(), Some("chatA"));
    assert_eq!(s2.chat_id.as_deref(), Some("chatB"));
}

// ── PC-2: Different users, same chatId → different sessions ────────

#[tokio::test]
async fn pc2_different_users_same_chat() {
    let (session_mgr, _, _, repo) = setup().await;

    let uid1 = create_user(&repo, "p1", "telegram").await;
    let uid2 = create_user(&repo, "p2", "telegram").await;

    let s1 = session_mgr
        .get_or_create_session(OWNER_ID, &uid1, "chatA", "gemini", None)
        .await
        .unwrap();
    let s2 = session_mgr
        .get_or_create_session(OWNER_ID, &uid2, "chatA", "gemini", None)
        .await
        .unwrap();

    assert_ne!(s1.id, s2.id);
}

// ── PC-3: Same user, same chatId → reuse session ──────────────────

#[tokio::test]
async fn pc3_same_user_same_chat_reuses() {
    let (session_mgr, _, _, repo) = setup().await;

    let uid = create_user(&repo, "p1", "telegram").await;

    let s1 = session_mgr
        .get_or_create_session(OWNER_ID, &uid, "chatA", "gemini", None)
        .await
        .unwrap();
    let s2 = session_mgr
        .get_or_create_session(OWNER_ID, &uid, "chatA", "gemini", None)
        .await
        .unwrap();

    assert_eq!(s1.id, s2.id);
}

// ── RU-3: Revoke user clears sessions ──────────────────────────────

#[tokio::test]
async fn ru3_revoke_clears_sessions() {
    let (session_mgr, _, _, repo) = setup().await;

    let uid1 = create_user(&repo, "p1", "telegram").await;
    let uid2 = create_user(&repo, "p2", "telegram").await;

    session_mgr
        .get_or_create_session(OWNER_ID, &uid1, "c1", "gemini", None)
        .await
        .unwrap();
    session_mgr
        .get_or_create_session(OWNER_ID, &uid1, "c2", "acp", None)
        .await
        .unwrap();
    session_mgr
        .get_or_create_session(OWNER_ID, &uid2, "c1", "gemini", None)
        .await
        .unwrap();

    // Cleanup user1 sessions
    session_mgr.cleanup_user_sessions(OWNER_ID, &uid1).await.unwrap();

    let sessions = repo.get_all_sessions(OWNER_ID).await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].user_id, uid2);
}

// ── ActionExecutor: unauthorized user gets pairing ─────────────────

#[tokio::test]
async fn action_unauthorized_triggers_pairing() {
    let (_, executor, _, _) = setup().await;

    let msg = make_text_message("new_user", "chat1", "Hello");
    let result = executor.handle_incoming_message(&msg).await.unwrap();

    match result {
        MessageResult::Action(resp) => {
            assert_eq!(resp.behavior, ActionBehavior::Send);
            let text = resp.text.unwrap();
            assert!(text.contains("配对码"));
            assert!(resp.buttons.is_some());
        }
        _ => panic!("Expected Action (pairing) for unauthorized user"),
    }
}

// ── ActionExecutor: authorized user dispatches to agent ────────────

#[tokio::test]
async fn action_authorized_plain_text_requires_reply_route() {
    let (_, executor, pairing, _) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;

    let msg = make_text_message("tg_42", "chat1", "Hello AI");
    let result = executor.handle_incoming_message(&msg).await.unwrap();

    match result {
        MessageResult::Action(response) => {
            assert!(response.text.as_deref().is_some_and(|text| text.contains("/new")));
        }
        other => panic!("expected reply-routing instructions, got {other:?}"),
    }
}

// ── ActionExecutor: help.show action ───────────────────────────────

#[tokio::test]
async fn action_help_show() {
    let (_, executor, pairing, _) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;

    let msg = make_action_message("tg_42", "chat1", "help.show", ActionCategory::System);
    let result = executor.handle_incoming_message(&msg).await.unwrap();

    match result {
        MessageResult::Action(resp) => {
            assert!(resp.text.is_some());
            assert!(resp.buttons.is_some());
            let buttons = resp.buttons.unwrap();
            assert!(buttons.len() >= 2);
        }
        _ => panic!("Expected Action result"),
    }
}

// ── ActionExecutor: session.new action ─────────────────────────────

#[tokio::test]
async fn action_session_new() {
    let (_, executor, pairing, _) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;

    let msg = make_text_message("tg_42", "chat1", "/new");
    let result = executor.handle_incoming_message(&msg).await.unwrap();

    match result {
        MessageResult::RoutedAction {
            response: resp,
            session_id,
            ..
        } => {
            let text = resp.text.unwrap();
            assert!(text.contains("新会话已就绪"));
            assert!(text.contains("引用回复这条消息"));
            assert!(!session_id.is_empty());
        }
        _ => panic!("Expected routed action result"),
    }
}

#[tokio::test]
async fn action_session_new_with_prompt_dispatches_prompt_without_ready_message() {
    let (_, executor, pairing, _) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;

    let msg = make_text_message("tg_42", "chat1", "/new hello");
    let result = executor.handle_incoming_message(&msg).await.unwrap();

    match result {
        MessageResult::RoutedAction {
            follow_up_text,
            response,
            session_id,
            ..
        } => {
            assert_eq!(follow_up_text.as_deref(), Some("hello"));
            assert!(response.text.is_none());
            assert!(!session_id.is_empty());
        }
        _ => panic!("Expected routed action result"),
    }
}

// ── ActionExecutor: session.new resets the session (H-2 fix) ─────

#[tokio::test]
async fn action_session_new_preserves_existing_sessions() {
    let (_, executor, pairing, repo) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;

    let first_new = make_action_message("tg_42", "chat1", "session.new", ActionCategory::System);
    let first_result = executor.handle_incoming_message(&first_new).await.unwrap();
    let first_session_id = match first_result {
        MessageResult::RoutedAction { session_id, .. } => session_id,
        _ => panic!("Expected routed action result"),
    };

    let second_new = make_action_message("tg_42", "chat1", "session.new", ActionCategory::System);
    let second_result = executor.handle_incoming_message(&second_new).await.unwrap();
    let second_session_id = match second_result {
        MessageResult::RoutedAction { session_id, .. } => session_id,
        _ => panic!("Expected routed action result"),
    };
    assert_ne!(first_session_id, second_session_id);

    // Each /new route remains independently addressable by replies.
    let all = repo.get_all_sessions(OWNER_ID).await.unwrap();
    let user_sessions: Vec<_> = all.iter().filter(|s| s.chat_id.as_deref() == Some("chat1")).collect();
    assert_eq!(user_sessions.len(), 2);
}

// NOTE: the former `action_agent_select_persists` test was removed. Direct
// `agent.select` channel actions are no longer supported under the
// assistant-first model — the handler now treats them as unknown actions
// (covered by `action::tests::agent_select_is_treated_as_unknown_action`).

// ── ActionExecutor: reply-target session routing ────────────────────

#[tokio::test]
async fn action_replies_route_to_the_exact_desktop_conversation() {
    let (session_mgr, executor, pairing, _repo) = setup().await;

    authorize_user(&pairing, "tg_42", "telegram").await;
    session_mgr
        .register_conversation_route(
            OWNER_ID,
            &aionui_db::UpsertChannelConversationRouteParams {
                conversation_id: "desktop-a",
                platform_type: "telegram",
                chat_id: "chat1",
                message_id: "bot-a",
                preview_text: Some("Answer A"),
                sent_at: 100,
                delivery_started_at: None,
                delivery_completed_at: None,
            },
        )
        .await
        .unwrap();
    session_mgr
        .register_conversation_route(
            OWNER_ID,
            &aionui_db::UpsertChannelConversationRouteParams {
                conversation_id: "desktop-b",
                platform_type: "telegram",
                chat_id: "chat1",
                message_id: "bot-b",
                preview_text: Some("Answer B"),
                sent_at: 200,
                delivery_started_at: None,
                delivery_completed_at: None,
            },
        )
        .await
        .unwrap();

    let reply_a = executor
        .handle_incoming_message(&make_reply_message("tg_42", "chat1", "Continue A", "bot-a"))
        .await
        .unwrap();
    let reply_b = executor
        .handle_incoming_message(&make_reply_message("tg_42", "chat1", "Continue B", "bot-b"))
        .await
        .unwrap();
    let routed_a = match reply_a {
        MessageResult::Dispatched { conversation_id, .. } => conversation_id,
        _ => panic!("Expected reply A to dispatch"),
    };
    let routed_b = match reply_b {
        MessageResult::Dispatched { conversation_id, .. } => conversation_id,
        _ => panic!("Expected reply B to dispatch"),
    };
    assert_eq!(routed_a.as_deref(), Some("desktop-a"));
    assert_eq!(routed_b.as_deref(), Some("desktop-b"));

    assert!(
        session_mgr
            .resolve_conversation_route(OWNER_ID, "telegram", "other-chat", "bot-a")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        session_mgr
            .resolve_conversation_route(OWNER_ID, "lark", "chat1", "bot-a")
            .await
            .unwrap()
            .is_none()
    );

    assert!(
        session_mgr
            .resolve_conversation_route("other-owner", "telegram", "chat1", "bot-a")
            .await
            .unwrap()
            .is_none()
    );

    let unknown = executor
        .handle_incoming_message(&make_reply_message("tg_42", "chat1", "Unknown", "missing"))
        .await
        .unwrap();
    assert!(matches!(unknown, MessageResult::Action(_)));
}

// Note: bind_conversation FK-constrained persistence is tested in
// aionui-db sqlite_channel.rs::update_session_conversation_persists.
// Unit tests for the SessionManager layer are in session.rs.
