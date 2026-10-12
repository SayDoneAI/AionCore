use std::sync::Arc;

use aionui_ai_agent::AgentStreamEvent;
use aionui_ai_agent::protocol::events::{
    ErrorEventData, FinishEventData, TextEventData, ThinkingEventData, ToolCallEventData, ToolCallStatus,
};
use aionui_channel::constants::{
    DINGTALK_MESSAGE_LIMIT, DISCORD_MESSAGE_LIMIT, SLACK_MESSAGE_LIMIT, TELEGRAM_MESSAGE_LIMIT, WEIXIN_MESSAGE_LIMIT,
};
use aionui_channel::error::ChannelError;
use aionui_channel::session::SessionManager;
use aionui_channel::stream_relay::{
    ChannelDeliveryReceipt, ChannelSender, ChannelStreamRelay, MessageRecorder, RelayConfig, throttle_ms_for_platform,
};
use aionui_channel::types::{PluginType, UnifiedOutgoingMessage};
use aionui_common::{generate_id, now_ms};
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use aionui_db::models::{AssistantUserRow, ConversationRow};
use aionui_db::{
    IChannelRepository, IConversationRepository, SqliteChannelRepository, SqliteConversationRepository,
    init_database_memory,
};
use tokio::sync::broadcast;

fn test_config(platform: PluginType) -> RelayConfig {
    RelayConfig {
        owner_user_id: "system_default_user".into(),
        session_id: "session_1".into(),
        conversation_id: "conversation_1".into(),
        platform,
        plugin_id: platform.to_string(),
        chat_id: "chat_1".into(),
        prompt_text: String::new(),
        throttle_ms: 500,
    }
}

async fn run_recorded_events(
    platform: PluginType,
    events: Vec<AgentStreamEvent>,
) -> (Vec<UnifiedOutgoingMessage>, Vec<UnifiedOutgoingMessage>) {
    let (tx, rx) = broadcast::channel(events.len().max(1));
    let recorder = Arc::new(MessageRecorder::new());
    for event in events {
        tx.send(event).unwrap();
    }
    ChannelStreamRelay::new(test_config(platform), recorder.clone())
        .run(rx)
        .await;
    (recorder.take_sends(), recorder.take_edits())
}

fn text_event(text: &str) -> AgentStreamEvent {
    AgentStreamEvent::Text(TextEventData { content: text.into() })
}

fn finish_event() -> AgentStreamEvent {
    AgentStreamEvent::Finish(FinishEventData { session_id: None })
}

#[tokio::test]
async fn rapid_chunks_and_thinking_keep_one_message_on_every_platform() {
    for platform in [
        PluginType::Telegram,
        PluginType::Discord,
        PluginType::Slack,
        PluginType::Dingtalk,
        PluginType::Weixin,
        PluginType::Wecom,
        PluginType::Lark,
    ] {
        let mut events = vec![text_event("First.")];
        events.push(AgentStreamEvent::Thinking(ThinkingEventData {
            content: "internal thinking".into(),
            subject: None,
            duration: None,
            status: None,
        }));
        events.extend(" Second.".chars().map(|ch| text_event(&ch.to_string())));
        events.push(finish_event());
        let (sends, edits) = run_recorded_events(platform, events).await;
        assert_eq!(sends.len(), 1, "platform={platform}, sends={sends:?}");
        let final_message = edits.last().unwrap_or(&sends[0]);
        assert_eq!(final_message.text.as_deref(), Some("First. Second.\n\n✅ worked"));
        assert!(
            final_message
                .buttons
                .as_ref()
                .is_some_and(|buttons| !buttons.is_empty())
        );
    }
}

#[tokio::test]
async fn explicit_segment_break_separates_independent_outputs_on_every_platform() {
    for platform in [
        PluginType::Telegram,
        PluginType::Discord,
        PluginType::Slack,
        PluginType::Dingtalk,
        PluginType::Weixin,
        PluginType::Wecom,
        PluginType::Lark,
    ] {
        let (sends, edits) = run_recorded_events(
            platform,
            vec![
                text_event("Launched."),
                AgentStreamEvent::SegmentBreak,
                text_event("Completed."),
                finish_event(),
            ],
        )
        .await;
        assert_eq!(sends.len(), 2, "platform={platform}, sends={sends:?}");
        let first_output = edits.first().unwrap_or(&sends[0]);
        assert_eq!(first_output.text.as_deref(), Some("Launched."));
        assert!(first_output.buttons.as_ref().is_none_or(Vec::is_empty));
        if platform == PluginType::Dingtalk {
            assert_eq!(
                first_output.buttons,
                Some(Vec::new()),
                "the ended card must be finalized"
            );
        }
        assert_eq!(sends[1].text.as_deref(), Some("Completed.\n\n✅ worked"));
        assert!(sends[1].buttons.as_ref().is_some_and(|buttons| !buttons.is_empty()));
    }
}

#[tokio::test]
async fn finish_after_segment_break_does_not_send_an_empty_reply() {
    for platform in [
        PluginType::Telegram,
        PluginType::Discord,
        PluginType::Slack,
        PluginType::Dingtalk,
        PluginType::Weixin,
        PluginType::Wecom,
        PluginType::Lark,
    ] {
        let (sends, edits) = run_recorded_events(
            platform,
            vec![
                text_event("First output."),
                AgentStreamEvent::SegmentBreak,
                finish_event(),
            ],
        )
        .await;
        assert_eq!(sends.len(), 1, "platform={platform}, sends={sends:?}");

        if matches!(platform, PluginType::Weixin | PluginType::Wecom | PluginType::Lark) {
            assert_eq!(sends[0].text.as_deref(), Some("First output."));
            assert!(edits.is_empty(), "platform={platform}, edits={edits:?}");
        } else {
            assert_eq!(edits.len(), 1, "platform={platform}, edits={edits:?}");
            assert_eq!(edits[0].text.as_deref(), Some("First output."));
        }
    }
}

#[tokio::test]
async fn editable_platforms_preserve_long_unicode_answers_within_the_byte_limit() {
    for (platform, limit) in [
        (PluginType::Telegram, TELEGRAM_MESSAGE_LIMIT),
        (PluginType::Discord, DISCORD_MESSAGE_LIMIT),
        (PluginType::Slack, SLACK_MESSAGE_LIMIT),
        (PluginType::Dingtalk, DINGTALK_MESSAGE_LIMIT),
    ] {
        let answer = "答🌟".repeat(limit);
        let (sends, edits) = run_recorded_events(platform, vec![text_event(&answer), finish_event()]).await;
        let chunks: Vec<_> = edits.iter().chain(sends.iter().skip(1)).collect();
        assert!(chunks.len() > 1, "platform={platform}");
        assert!(
            chunks
                .iter()
                .all(|message| message.text.as_ref().is_some_and(|text| text.len() <= limit))
        );
        let joined: String = chunks.iter().filter_map(|message| message.text.as_deref()).collect();
        assert_eq!(joined, format!("{answer}\n\n✅ worked"));
        assert!(
            chunks[..chunks.len() - 1]
                .iter()
                .all(|message| message.buttons.as_ref().is_none_or(Vec::is_empty))
        );
        assert!(
            chunks
                .last()
                .unwrap()
                .buttons
                .as_ref()
                .is_some_and(|buttons| !buttons.is_empty())
        );
        if platform == PluginType::Dingtalk {
            assert!(
                chunks.iter().all(|message| message.buttons.is_some()),
                "each DingTalk card must be finalized"
            );
        }
    }
}

struct FailingSender {
    recorder: MessageRecorder,
    send_failures: std::sync::atomic::AtomicUsize,
    fail_edits: bool,
}

#[async_trait::async_trait]
impl ChannelSender for FailingSender {
    async fn send_message(
        &self,
        owner: &str,
        plugin: &str,
        chat: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<ChannelDeliveryReceipt, ChannelError> {
        if self
            .send_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(ChannelError::MessageSendFailed("test send failure".into()));
        }
        self.recorder.send_message(owner, plugin, chat, message).await
    }

    async fn edit_message(
        &self,
        owner: &str,
        plugin: &str,
        chat: &str,
        message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        if self.fail_edits {
            return Err(ChannelError::PlatformApi("test edit failure".into()));
        }
        self.recorder
            .edit_message(owner, plugin, chat, message_id, message)
            .await
    }
}

#[tokio::test]
async fn editable_delivery_recovers_from_placeholder_and_final_edit_failures() {
    for (send_failures, fail_edits, expected_sends) in [(1, false, 1), (0, true, 2)] {
        let sender = Arc::new(FailingSender {
            recorder: MessageRecorder::new(),
            send_failures: std::sync::atomic::AtomicUsize::new(send_failures),
            fail_edits,
        });
        let (tx, rx) = broadcast::channel(8);
        tx.send(text_event("Complete answer.")).unwrap();
        tx.send(finish_event()).unwrap();
        ChannelStreamRelay::new(test_config(PluginType::Telegram), sender.clone())
            .run(rx)
            .await;
        let sends = sender.recorder.take_sends();
        assert_eq!(sends.len(), expected_sends);
        assert_eq!(
            sends.last().unwrap().text.as_deref(),
            Some("Complete answer.\n\n✅ worked")
        );
        assert!(sender.recorder.take_edits().is_empty());
    }
}

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
    let relay_task = tokio::spawn(relay.run(rx));
    let streaming_edits = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let edits = recorder.take_edits();
            if !edits.is_empty() {
                break edits;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("streaming text must be edited before Finish, even without another chunk");
    assert_eq!(streaming_edits.len(), 1);
    assert_eq!(streaming_edits[0].text.as_deref(), Some("Hello World\n\n⏳ working"));
    event_tx
        .send(AgentStreamEvent::Finish(FinishEventData { session_id: None }))
        .unwrap();

    relay_task.await.unwrap();

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].text.as_deref(), Some("⏳ working"));

    let edits = recorder.take_edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].text.as_deref(), Some("Hello World\n\n✅ worked"));
    assert!(edits[0].buttons.is_some());
}

#[tokio::test]
async fn slow_single_character_chunks_wait_for_a_meaningful_stream_update() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(
        RelayConfig {
            owner_user_id: "system_default_user".to_owned(),
            session_id: "session_1".to_owned(),
            conversation_id: "conversation_1".to_owned(),
            platform: PluginType::Telegram,
            plugin_id: "telegram".into(),
            chat_id: "chat_1".into(),
            prompt_text: "Test question".into(),
            throttle_ms: 5,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();
    let relay_task = tokio::spawn(relay.run(rx));

    event_tx.send(text_event("a")).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        recorder.take_edits().is_empty(),
        "a slow single-character stream must not emit a one-character working update"
    );

    for character in "bcdefgh".chars() {
        event_tx.send(text_event(&character.to_string())).unwrap();
    }
    let streaming_edits = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let edits = recorder.take_edits();
            if !edits.is_empty() {
                break edits;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the accumulated stream should eventually be visible");
    assert_eq!(streaming_edits.len(), 1);
    let streaming_text = streaming_edits[0].text.as_deref().unwrap();
    assert!(streaming_text.ends_with("\n\n⏳ working"));
    assert!(streaming_text.trim_end_matches("\n\n⏳ working").chars().count() >= 8);

    event_tx.send(finish_event()).unwrap();
    relay_task.await.unwrap();
    let final_edits = recorder.take_edits();
    assert_eq!(final_edits.len(), 1);
    assert_eq!(final_edits[0].text.as_deref(), Some("abcdefgh\n\n✅ worked"));
}

#[tokio::test]
async fn dingtalk_streaming_edits_keep_the_card_open_until_finish() {
    let (event_tx, rx) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let mut config = test_config(PluginType::Dingtalk);
    config.throttle_ms = 5;
    let relay_task = tokio::spawn(ChannelStreamRelay::new(config, recorder.clone()).run(rx));

    for text in ["First fragment", " and another fragment"] {
        event_tx.send(text_event(text)).unwrap();
        let streaming_edits = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let edits = recorder.take_edits();
                if !edits.is_empty() {
                    break edits;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("DingTalk must receive intermediate edits before Finish");
        assert_eq!(streaming_edits.len(), 1);
        assert!(streaming_edits[0].text.as_deref().unwrap().ends_with("\n\n⏳ working"));
        assert!(
            streaming_edits[0].buttons.is_none(),
            "an intermediate edit must not finalize the card"
        );
    }

    event_tx.send(finish_event()).unwrap();
    relay_task.await.unwrap();
    assert_eq!(recorder.take_sends().len(), 1);
    let final_edits = recorder.take_edits();
    assert_eq!(final_edits.len(), 1);
    assert_eq!(
        final_edits[0].text.as_deref(),
        Some("First fragment and another fragment\n\n✅ worked")
    );
    assert!(
        final_edits[0]
            .buttons
            .as_ref()
            .is_some_and(|buttons| !buttons.is_empty())
    );
}

#[tokio::test]
async fn dingtalk_errors_close_the_card_without_success_actions() {
    let (sends, edits) = run_recorded_events(
        PluginType::Dingtalk,
        vec![
            text_event("Partial answer"),
            AgentStreamEvent::Error(ErrorEventData::legacy("provider unavailable", None)),
        ],
    )
    .await;
    assert_eq!(sends.len(), 1);
    assert_eq!(
        edits[0].text.as_deref(),
        Some("Partial answer\n\n❌ provider unavailable")
    );
    assert_eq!(edits[0].buttons, Some(Vec::new()));
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
        Some("**你：** Test question\n\n**AI：** Here is the plan: Done.\n\n✅ worked")
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
            .is_some_and(|text| text.len() <= WEIXIN_MESSAGE_LIMIT)
    }));
    let joined = sends
        .iter()
        .filter_map(|message| message.text.as_deref())
        .collect::<String>();
    assert_eq!(joined, format!("{expected}\n\n✅ worked"));
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
        Some("**你：** Test question\n\n**AI：** Preparing. Final answer.\n\n✅ worked")
    );
}

#[tokio::test]
async fn turn_release_keeps_queued_continuation_on_every_platform() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    for platform in [
        PluginType::Telegram,
        PluginType::Discord,
        PluginType::Slack,
        PluginType::Dingtalk,
        PluginType::Weixin,
        PluginType::Wecom,
        PluginType::Lark,
    ] {
        // Exercise both select orders when delivery and claim release become
        // ready together, after the relay has already begun awaiting them.
        for _ in 0..16 {
            let (tx, rx) = broadcast::channel(64);
            let recorder = Arc::new(MessageRecorder::new());
            let runtime_state = Arc::new(ConversationRuntimeStateService::default());
            let claim = runtime_state.try_claim_turn("conversation_1", "turn_1").unwrap();
            let relay =
                ChannelStreamRelay::new(test_config(platform), recorder.clone()).with_runtime_state(runtime_state);
            tx.send(text_event("Preparing. ")).unwrap();
            tx.send(finish_event()).unwrap();
            let mut running = Box::pin(relay.run(rx));
            poll_fn(|cx| {
                assert!(running.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;

            tx.send(text_event("Final answer.")).unwrap();
            tx.send(finish_event()).unwrap();
            drop(claim);
            running.await;

            let sends = recorder.take_sends();
            let edits = recorder.take_edits();
            assert_eq!(sends.len(), 1, "platform={platform}");
            let final_message = edits.last().unwrap_or(&sends[0]);
            assert_eq!(
                final_message.text.as_deref(),
                Some("Preparing. Final answer.\n\n✅ worked"),
                "platform={platform} must drain events queued before claim release",
            );
            assert!(final_message.buttons.is_some());
        }
    }
}

#[tokio::test]
async fn lark_sends_one_complete_answer_when_the_turn_finishes_within_the_throttle_window() {
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
        Some("**你：** 请总结这个文件\n\n**AI：** 文件已总结\n\n✅ worked")
    );
    assert!(sends[0].buttons.as_ref().is_some_and(|buttons| !buttons.is_empty()));
    assert!(recorder.take_edits().is_empty());
}

#[tokio::test]
async fn lark_streams_into_one_card_after_a_meaningful_fragment() {
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
            throttle_ms: 5,
        },
        recorder.clone(),
    );
    let rx = event_tx.subscribe();
    let relay_task = tokio::spawn(relay.run(rx));

    event_tx.send(text_event("a")).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        recorder.snapshot_sends().is_empty(),
        "Lark must not leave an empty placeholder card"
    );

    for character in "bcdefgh".chars() {
        event_tx.send(text_event(&character.to_string())).unwrap();
    }
    let streaming_message = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let sends = recorder.snapshot_sends();
            if let Some(message) = sends.first() {
                break message.clone();
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a meaningful Lark stream fragment should be sent");
    assert!(streaming_message.text.as_deref().unwrap().ends_with("\n\n⏳ working"));

    event_tx.send(finish_event()).unwrap();
    relay_task.await.unwrap();

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1, "the streamed reply must remain on one Lark card");
    let edits = recorder.take_edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(
        edits[0].text.as_deref(),
        Some("**你：** 请总结这个文件\n\n**AI：** abcdefgh\n\n✅ worked")
    );
    assert!(edits[0].buttons.as_ref().is_some_and(|buttons| !buttons.is_empty()));
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
async fn lark_skips_placeholder_and_flush_when_buffer_is_empty() {
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
async fn editable_platform_completes_tool_only_turn_without_leaving_working_placeholder() {
    let (event_tx, _) = broadcast::channel::<AgentStreamEvent>(64);
    let recorder = Arc::new(MessageRecorder::new());
    let relay = ChannelStreamRelay::new(test_config(PluginType::Telegram), recorder.clone());
    let rx = event_tx.subscribe();

    event_tx.send(finish_event()).unwrap();
    relay.run(rx).await;

    let sends = recorder.take_sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].text.as_deref(), Some("⏳ working"));

    let edits = recorder.take_edits();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].text.as_deref(), Some("✅ worked"));
    assert!(edits[0].buttons.as_ref().is_some_and(|buttons| !buttons.is_empty()));
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
