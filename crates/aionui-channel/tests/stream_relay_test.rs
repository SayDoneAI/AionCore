use std::sync::Arc;

use aionui_ai_agent::AgentStreamEvent;
use aionui_ai_agent::protocol::events::{
    ErrorEventData, FinishEventData, TextEventData, ThinkingEventData, ToolCallEventData, ToolCallStatus,
};
use aionui_channel::session::SessionManager;
use aionui_channel::stream_relay::{ChannelStreamRelay, MessageRecorder, RelayConfig};
use aionui_channel::types::PluginType;
use aionui_common::{generate_id, now_ms};
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use aionui_db::models::{AssistantUserRow, ConversationRow};
use aionui_db::{
    IChannelRepository, IConversationRepository, SqliteChannelRepository, SqliteConversationRepository,
    init_database_memory,
};
use tokio::sync::broadcast;

fn relay_config(platform: PluginType) -> RelayConfig {
    RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform,
        plugin_id: platform.to_string(),
        chat_id: "chat_1".to_owned(),
        prompt_text: "Test question".to_owned(),
    }
}

async fn wait_for_send_count(recorder: &MessageRecorder, expected: usize) {
    for _ in 0..100 {
        if recorder.snapshot_sends().len() >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "expected at least {expected} sent messages, got {}",
        recorder.snapshot_sends().len()
    );
}

#[tokio::test]
async fn relay_sends_text_immediately_and_marks_agent_stop() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(relay_config(PluginType::Telegram), recorder.clone());
    let relay_task = tokio::spawn(relay.run(event_tx.subscribe()));

    wait_for_send_count(&recorder, 1).await;
    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Hello".to_owned(),
        }))
        .unwrap();
    wait_for_send_count(&recorder, 2).await;

    let sends = recorder.snapshot_sends();
    assert_eq!(sends[0].text.as_deref(), Some("⏳ working"));
    assert_eq!(sends[1].text.as_deref(), Some("Hello\n\n⏳ working"));

    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();
    relay_task.await.unwrap();

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 3);
    assert_eq!(sends[2].text.as_deref(), Some("✅ worked"));
    assert!(sends[2].buttons.is_some());
    assert!(recorder.take_edits().is_empty());
}

#[tokio::test]
async fn relay_uses_the_same_send_contract_for_every_channel() {
    for platform in [
        PluginType::Telegram,
        PluginType::Lark,
        PluginType::Dingtalk,
        PluginType::Weixin,
        PluginType::Wecom,
        PluginType::Slack,
        PluginType::Discord,
    ] {
        let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(16);
        let recorder = Arc::new(MessageRecorder::new());
        let relay = ChannelStreamRelay::new(relay_config(platform), recorder.clone());
        let rx = event_tx.subscribe();

        event_tx
            .send(AgentStreamEvent::Text(TextEventData {
                content: "answer".to_owned(),
            }))
            .unwrap();
        event_tx
            .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
            .unwrap();
        relay.run(rx).await;

        let sends = recorder.take_sends();
        assert_eq!(sends.len(), 3, "unexpected sends for {platform:?}: {sends:?}");
        assert!(sends[1].text.as_deref().is_some_and(|text| text.contains("⏳ working")));
        assert!(sends[2].text.as_deref().is_some_and(|text| text.contains("✅ worked")));
        assert!(recorder.take_edits().is_empty(), "relay edited for {platform:?}");
    }
}

#[tokio::test]
async fn relay_does_not_send_thinking_or_tool_call_events() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(16);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(relay_config(PluginType::Telegram), recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Thinking(ThinkingEventData {
            content: "internal reasoning".to_owned(),
            subject: None,
            duration: None,
            status: None,
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::ToolCall(ToolCallEventData {
            call_id: "call-1".to_owned(),
            name: "read_file".to_owned(),
            args: serde_json::Value::Null,
            status: ToolCallStatus::Running,
            description: None,
            parent_call_id: None,
            input: None,
            output: None,
            files: None,
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0].text.as_deref(), Some("⏳ working"));
    assert_eq!(sends[1].text.as_deref(), Some("✅ worked"));
}

#[tokio::test]
async fn relay_waits_for_agent_stop_after_an_intermediate_finish() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(32);
    let recorder = Arc::new(MessageRecorder::new());
    let runtime_state = Arc::new(ConversationRuntimeStateService::default());
    let claim = runtime_state.try_claim_turn("conversation_1", "turn_1").unwrap();
    let relay =
        ChannelStreamRelay::new(relay_config(PluginType::Telegram), recorder.clone()).with_runtime_state(runtime_state);
    let relay_task = tokio::spawn(relay.run(event_tx.subscribe()));

    wait_for_send_count(&recorder, 1).await;
    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Preparing. ".to_owned(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();
    wait_for_send_count(&recorder, 2).await;
    assert!(
        recorder.snapshot_sends().iter().all(|message| !message
            .text
            .as_deref()
            .unwrap_or_default()
            .contains("✅ worked"))
    );

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Final answer.".to_owned(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();
    wait_for_send_count(&recorder, 3).await;
    assert!(
        recorder.snapshot_sends().iter().all(|message| !message
            .text
            .as_deref()
            .unwrap_or_default()
            .contains("✅ worked"))
    );

    drop(claim);
    relay_task.await.unwrap();

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 4);
    assert_eq!(
        sends.last().and_then(|message| message.text.as_deref()),
        Some("✅ worked")
    );
}

#[tokio::test]
async fn relay_handles_error_without_marking_it_worked() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(16);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(relay_config(PluginType::Telegram), recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "partial".to_owned(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Error(ErrorEventData::legacy("timeout", None)))
        .unwrap();
    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 3);
    assert!(sends[1].text.as_deref().unwrap().contains("partial"));
    assert!(sends[2].text.as_deref().unwrap().contains("timeout"));
    assert!(!sends[2].text.as_deref().unwrap().contains("worked"));
    assert!(sends[2].buttons.is_none());
}

#[tokio::test]
async fn relay_handles_channel_closed_without_marking_it_worked() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(16);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(relay_config(PluginType::Telegram), recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "partial".to_owned(),
        }))
        .unwrap();
    drop(event_tx);
    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 3);
    assert_eq!(sends[0].text.as_deref(), Some("⏳ working"));
    assert!(sends[1].text.as_deref().unwrap().contains("partial"));
    assert!(sends[2].text.as_deref().unwrap().contains("回复流中断"));
    assert!(!sends[2].text.as_deref().unwrap().contains("worked"));
}

#[tokio::test]
async fn relay_registers_final_message_as_a_reply_route() {
    let db = init_database_memory().await.unwrap();
    let repo: Arc<dyn IChannelRepository> = Arc::new(SqliteChannelRepository::new(db.pool().clone()));
    let session_manager = Arc::new(SessionManager::new(Arc::clone(&repo)));
    let owner_user_id = "system_default_user";
    let channel_user_id = generate_id();
    repo.create_user(
        owner_user_id,
        &AssistantUserRow {
            id: channel_user_id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            platform_user_id: "tg_42".to_owned(),
            platform_type: "telegram".to_owned(),
            display_name: Some("Test User".to_owned()),
            authorized_at: now_ms(),
            last_active: None,
            session_id: None,
        },
    )
    .await
    .unwrap();
    session_manager
        .create_session(owner_user_id, &channel_user_id, "chat_1", "codex", None)
        .await
        .unwrap();
    let timestamp = now_ms();
    SqliteConversationRepository::new(db.pool().clone())
        .create(&ConversationRow {
            id: "conversation_1".to_owned(),
            user_id: owner_user_id.to_owned(),
            name: "Desktop chat".to_owned(),
            r#type: "acp".to_owned(),
            extra: "{}".to_owned(),
            model: None,
            status: Some("pending".to_owned()),
            source: Some("aionui".to_owned()),
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at: timestamp,
            updated_at: timestamp,
            project_id: None,
            folder_id: None,
            name_source: None,
        })
        .await
        .unwrap();

    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(16);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::with_session_manager(
        relay_config(PluginType::Telegram),
        recorder,
        Arc::clone(&session_manager),
    );
    let rx = event_tx.subscribe();
    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Hello".to_owned(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();
    relay.run(rx).await;

    let resolved = session_manager
        .resolve_conversation_route(owner_user_id, "telegram", "chat_1", "msg-1")
        .await
        .unwrap()
        .expect("sent message should resolve to its conversation");
    assert_eq!(resolved, "conversation_1");

    let resolved_by_delivery_window = session_manager
        .resolve_conversation_route_by_timestamp(owner_user_id, "telegram", "chat_1", 1, 0)
        .await
        .unwrap()
        .expect("sent message delivery window should resolve to its conversation");
    assert_eq!(resolved_by_delivery_window, "conversation_1");

    let preview: String = sqlx::query_scalar(
        "SELECT preview_text FROM channel_conversation_routes \
         WHERE owner_user_id = ? AND conversation_id = 'conversation_1' AND message_id = 'msg-1'",
    )
    .bind(owner_user_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(preview, "你：Test question\n\nAI：Hello");
}
