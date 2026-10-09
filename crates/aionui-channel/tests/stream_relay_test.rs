use std::sync::Arc;

use aionui_ai_agent::AgentStreamEvent;
use aionui_ai_agent::protocol::events::{
    ErrorEventData, FinishEventData, TextEventData, ToolCallEventData, ToolCallStatus,
};
use aionui_channel::constants::WEIXIN_MESSAGE_LIMIT;
use aionui_channel::session::SessionManager;
use aionui_channel::stream_relay::{ChannelStreamRelay, MessageRecorder, RelayConfig, throttle_ms_for_platform};
use aionui_channel::types::PluginType;
use aionui_common::{generate_id, now_ms};
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use aionui_db::models::{AssistantUserRow, ConversationRow};
use aionui_db::{
    IChannelRepository, IConversationRepository, SqliteChannelRepository, SqliteConversationRepository,
    init_database_memory,
};
use tokio::sync::broadcast;

// ── RelayConfig construction ─────────────────────────────────────

#[test]
fn slack_uses_larger_throttle_than_others() {
    // Slack's chat.update is rate-limited harder, so it gets a wider interval;
    // the other editable platforms must stay at the original 500 ms.
    assert_eq!(throttle_ms_for_platform(PluginType::Slack), 1200);
    assert_eq!(throttle_ms_for_platform(PluginType::Discord), 1000);
    assert_eq!(throttle_ms_for_platform(PluginType::Telegram), 500);
    assert_eq!(throttle_ms_for_platform(PluginType::Lark), 500);
    assert_eq!(throttle_ms_for_platform(PluginType::Dingtalk), 500);
    assert_eq!(throttle_ms_for_platform(PluginType::Weixin), 500);
}

#[test]
fn relay_config_fields() {
    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Telegram,
        plugin_id: "telegram".into(),
        chat_id: "123".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 500,
    };
    assert_eq!(config.throttle_ms, 500);
    assert_eq!(config.plugin_id, "telegram");
}

// ── Full relay run with mock ChannelSender ───────────────────────

#[tokio::test]
async fn relay_sends_thinking_then_final_message() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Telegram,
        plugin_id: "telegram".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10,
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());

    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Hello".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: " World".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert!(!sends.is_empty());
    assert!(sends[0].text.as_deref().unwrap().contains("正在思考"));

    let edits = recorder.take_edits();
    let last = edits.last().unwrap();
    assert!(last.text.as_deref().unwrap().contains("Hello World"));
    assert!(last.buttons.is_some());
    assert!(
        edits
            .iter()
            .any(|message| { message.text.as_deref().is_some_and(|text| text.contains("正在生成")) })
    );
}

#[tokio::test]
async fn relay_registers_sent_message_id_and_delivery_window_as_reply_routes() {
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
    let session = session_manager
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

    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::with_session_manager(
        RelayConfig {
            owner_user_id: owner_user_id.to_owned(),
            session_id: session.id.clone(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Telegram,
            plugin_id: "telegram".to_owned(),
            chat_id: "chat_1".to_owned(),
            prompt_text: "Test question".into(),
            throttle_ms: 10,
        },
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

#[tokio::test]
async fn relay_handles_error_event() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Telegram,
        plugin_id: "telegram".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10,
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "partial".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Error(ErrorEventData::legacy("timeout", None)))
        .unwrap();

    relay.run(rx).await;

    let edits = recorder.take_edits();
    let last = edits.last().unwrap();
    assert!(last.text.as_deref().unwrap().contains("partial"));
    assert!(last.text.as_deref().unwrap().contains("timeout"));
    assert!(last.buttons.is_none());
}

#[tokio::test]
async fn weixin_sends_one_complete_answer_after_tool_call() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Weixin,
        plugin_id: "weixin".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10_000, // large throttle so the mid-stream edit doesn't fire
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Here is the plan:".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::ToolCall(ToolCallEventData {
            call_id: "call-1".into(),
            name: "read_file".into(),
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
        .send(AgentStreamEvent::Text(TextEventData {
            content: " Done.".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1, "WeChat must send only the final answer: {sends:?}");
    assert_eq!(
        sends[0].text.as_deref(),
        Some("**你：** Test question\n\n**AI：** Here is the plan: Done.")
    );
}

#[tokio::test]
async fn weixin_splits_long_answers_and_keeps_buttons_on_the_final_chunk() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let answer = "答".repeat(WEIXIN_MESSAGE_LIMIT);
    let expected = format!("**你：** Test question\n\n**AI：** {answer}");

    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Weixin,
            plugin_id: "weixin".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 10_000,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData { content: answer }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert!(sends.len() > 1, "long WeChat replies must be split: {sends:?}");
    assert!(sends.iter().all(|message| {
        message
            .text
            .as_deref()
            .is_some_and(|text| text.chars().count() <= WEIXIN_MESSAGE_LIMIT)
    }));
    let joined = sends
        .iter()
        .filter_map(|message| message.text.as_deref())
        .collect::<String>();
    assert_eq!(joined, expected);
    assert!(sends[..sends.len() - 1].iter().all(|message| message.buttons.is_none()));
    assert!(sends.last().is_some_and(|message| message.buttons.is_some()));
}

#[tokio::test]
async fn weixin_waits_for_the_whole_turn_after_an_intermediate_finish() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let runtime_state = Arc::new(ConversationRuntimeStateService::default());
    let claim = runtime_state.try_claim_turn("conversation_1", "turn_1").unwrap();
    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Weixin,
            plugin_id: "weixin".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 10_000,
        },
        recorder.clone(),
    )
    .with_runtime_state(runtime_state);
    let rx = event_tx.subscribe();
    let producer_tx = event_tx.clone();
    let relay_task = tokio::spawn(relay.run(rx));

    let producer = tokio::spawn(async move {
        producer_tx
            .send(AgentStreamEvent::Text(TextEventData {
                content: "Preparing. ".into(),
            }))
            .unwrap();
        producer_tx
            .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
            .unwrap();
        tokio::task::yield_now().await;
        producer_tx
            .send(AgentStreamEvent::Text(TextEventData {
                content: "Final answer.".into(),
            }))
            .unwrap();
        producer_tx
            .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
            .unwrap();
        tokio::task::yield_now().await;
        drop(claim);
    });
    producer.await.unwrap();
    relay_task.await.unwrap();

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(
        sends[0].text.as_deref(),
        Some("**你：** Test question\n\n**AI：** Preparing. Final answer.")
    );
}

#[tokio::test]
async fn lark_sends_one_complete_answer_without_intermediate_state() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Lark,
            plugin_id: "lark".into(),
            chat_id: "chat_1".into(),
            prompt_text: "请总结这个文件".into(),
            throttle_ms: 10_000,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "文件已总结".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(
        sends[0].text.as_deref(),
        Some("**你：** 请总结这个文件\n\n**AI：** 文件已总结")
    );
    assert!(recorder.take_edits().is_empty());
}

#[tokio::test]
async fn telegram_does_not_flush_text_before_tool_call() {
    // Editable platforms keep one placeholder and update it in place instead
    // of issuing a new send_message for the buffered text.
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Telegram,
        plugin_id: "telegram".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10_000,
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "Here is the plan:".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::ToolCall(ToolCallEventData {
            call_id: "call-1".into(),
            name: "read_file".into(),
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
    // Only the "Thinking..." placeholder is sent — no flush on non-WeChat.
    assert_eq!(sends.len(), 1, "unexpected extra sends: {:?}", sends);
}

#[tokio::test]
async fn weixin_skips_flush_when_buffer_is_empty() {
    // Tool call before any assistant text should not trigger a blank flush.
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Weixin,
        plugin_id: "weixin".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10_000,
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::ToolCall(ToolCallEventData {
            call_id: "call-1".into(),
            name: "read_file".into(),
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
    // WeChat relay does NOT send Thinking placeholder, and with no buffered
    // text there should be zero sends (no flush needed).
    assert_eq!(sends.len(), 0, "no sends expected for empty buffer: {:?}", sends);
}

#[tokio::test]
async fn lark_skips_flush_when_buffer_is_empty() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Lark,
            plugin_id: "lark".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 10_000,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::ToolCall(ToolCallEventData {
            call_id: "call-1".into(),
            name: "read_file".into(),
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

    assert!(recorder.take_sends().is_empty());
    assert!(recorder.take_edits().is_empty());
}

#[tokio::test]
async fn lark_reports_stream_lag_when_no_text_was_received() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(1);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Lark,
            plugin_id: "lark".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 10_000,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "第一条事件".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "第二条事件".into(),
        }))
        .unwrap();

    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(
        sends[0].text.as_deref(),
        Some("**你：** Test question\n\n**AI：** ❌ 回复流中断，请重试")
    );
    assert!(sends[0].buttons.is_none(), "interrupted replies must not look complete");
}

#[tokio::test]
async fn relay_handles_channel_closed() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());

    let config = RelayConfig {
        owner_user_id: "system_default_user".to_owned(),
        session_id: "session_1".to_owned(),
        conversation_id: "conversation_1".to_owned(),
        platform: PluginType::Telegram,
        plugin_id: "telegram".into(),
        chat_id: "chat_1".into(),
        prompt_text: "Test question".into(),
        throttle_ms: 10,
    };
    let relay = ChannelStreamRelay::new(config, recorder.clone());
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "partial".into(),
        }))
        .unwrap();
    drop(event_tx);

    relay.run(rx).await;

    let edits = recorder.take_edits();
    let last = edits
        .last()
        .expect("an interrupted stream must replace the placeholder");
    assert!(last.text.as_deref().unwrap().contains("partial"));
    assert!(last.text.as_deref().unwrap().contains("回复流中断"));
    assert!(last.buttons.is_none(), "an interrupted reply must not look complete");
}

#[tokio::test]
async fn relay_handles_closed_continuation_without_success_buttons() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let runtime_state = Arc::new(ConversationRuntimeStateService::default());
    let _claim = runtime_state.try_claim_turn("conversation_1", "turn_1").unwrap();
    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Telegram,
            plugin_id: "telegram".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 10,
        },
        recorder.clone(),
    )
    .with_runtime_state(runtime_state);
    let rx = event_tx.subscribe();

    event_tx
        .send(AgentStreamEvent::Text(TextEventData {
            content: "partial before finish".into(),
        }))
        .unwrap();
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();
    drop(event_tx);

    relay.run(rx).await;

    let edits = recorder.take_edits();
    let last = edits
        .last()
        .expect("a closed continuation must replace the placeholder");
    assert!(last.text.as_deref().unwrap().contains("partial before finish"));
    assert!(last.text.as_deref().unwrap().contains("回复流中断"));
    assert!(last.buttons.is_none(), "a closed continuation must not look complete");
}
