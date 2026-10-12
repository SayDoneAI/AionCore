use std::sync::Arc;
use std::time::{Duration, Instant};

use aionui_ai_agent::{AgentStreamEvent, AgentStreamReceiver};
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::constants::{
    DINGTALK_MESSAGE_LIMIT, DISCORD_MESSAGE_LIMIT, LARK_MESSAGE_LIMIT, SLACK_MESSAGE_LIMIT, TELEGRAM_MESSAGE_LIMIT,
    WEIXIN_MESSAGE_LIMIT,
};
use crate::error::ChannelError;
use crate::formatter::{format_text_for_platform, truncate_message};
use crate::message_service::{ChannelMessageService, StreamAction};
use crate::session::SessionManager;
use crate::types::{PluginType, UnifiedOutgoingMessage};

// A timer alone still emits one-character edits when a slow model sends text
// more slowly than the platform throttle. Require a small readable fragment
// before exposing an intermediate working update; completion remains immediate.
const MIN_STREAM_UPDATE_CHARS: usize = 8;

/// Configuration for a stream relay session.
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub owner_user_id: String,
    pub session_id: String,
    pub conversation_id: String,
    pub platform: PluginType,
    pub plugin_id: String,
    pub chat_id: String,
    pub prompt_text: String,
    pub throttle_ms: u64,
}

enum ContinuationOutcome {
    Event(Box<AgentStreamEvent>),
    AgentStopped,
    Interrupted,
}

/// Per-platform minimum interval between streaming `edit_message` calls.
///
/// Slack's `chat.update` is rate-limited more aggressively than the other
/// editable platforms, so it uses a larger interval; Telegram/Lark/DingTalk
/// keep the original 500 ms cadence.
pub fn throttle_ms_for_platform(platform: PluginType) -> u64 {
    match platform {
        PluginType::Slack => 1200,
        PluginType::Discord => 1000,
        _ => 500,
    }
}

/// Abstraction for sending/editing messages through a channel plugin.
///
/// Decouples ChannelStreamRelay from ChannelManager for testability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDeliveryReceipt {
    pub message_id: String,
    pub started_at: i64,
    pub completed_at: i64,
}

#[async_trait]
pub trait ChannelSender: Send + Sync {
    async fn send_message(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        chat_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<ChannelDeliveryReceipt, ChannelError>;

    async fn edit_message(
        &self,
        owner_user_id: &str,
        plugin_id: &str,
        chat_id: &str,
        message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError>;
}

/// Relays agent stream events to an IM platform.
///
/// Responsibilities:
/// - Send a "Thinking..." placeholder and update it for editable platforms
/// - Send final message with action buttons on Finish
/// - Send error message on Error
pub struct ChannelStreamRelay {
    config: RelayConfig,
    sender: Arc<dyn ChannelSender>,
    session_manager: Option<Arc<SessionManager>>,
    runtime_state: Option<Arc<ConversationRuntimeStateService>>,
}

impl ChannelStreamRelay {
    pub fn new(config: RelayConfig, sender: Arc<dyn ChannelSender>) -> Self {
        Self {
            config,
            sender,
            session_manager: None,
            runtime_state: None,
        }
    }

    pub fn with_session_manager(
        config: RelayConfig,
        sender: Arc<dyn ChannelSender>,
        session_manager: Arc<SessionManager>,
    ) -> Self {
        Self {
            config,
            sender,
            session_manager: Some(session_manager),
            runtime_state: None,
        }
    }

    pub fn with_runtime_state(mut self, runtime_state: Arc<ConversationRuntimeStateService>) -> Self {
        self.runtime_state = Some(runtime_state);
        self
    }

    async fn next_continuation_event(&self, rx: &mut AgentStreamReceiver) -> ContinuationOutcome {
        let Some(runtime_state) = self.runtime_state.as_ref() else {
            // Standalone tests do not have a runtime claim. Production always
            // supplies runtime state, where this means the active turn ended.
            return ContinuationOutcome::AgentStopped;
        };
        match rx.try_recv() {
            Ok(event) => ContinuationOutcome::Event(Box::new(event)),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                warn!(skipped, "channel stream relay lagged while awaiting continuation");
                ContinuationOutcome::Interrupted
            }
            Err(broadcast::error::TryRecvError::Closed) => ContinuationOutcome::Interrupted,
            Err(broadcast::error::TryRecvError::Empty) => {
                tokio::select! {
                    _ = runtime_state.wait_until_unclaimed(&self.config.conversation_id) => {
                        // The producer queues terminal/continuation events before
                        // releasing its claim. Both branches can become ready
                        // together, so drain that queue before declaring completion.
                        match rx.try_recv() {
                            Ok(event) => ContinuationOutcome::Event(Box::new(event)),
                            Err(broadcast::error::TryRecvError::Empty) => ContinuationOutcome::AgentStopped,
                            Err(broadcast::error::TryRecvError::Closed) => ContinuationOutcome::Interrupted,
                            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                                warn!(skipped, "channel stream relay lagged at turn release");
                                ContinuationOutcome::Interrupted
                            }
                        }
                    },
                    event = rx.recv() => match event {
                        Ok(event) => ContinuationOutcome::Event(Box::new(event)),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "channel stream relay lagged while awaiting continuation");
                            ContinuationOutcome::Interrupted
                        }
                        Err(broadcast::error::RecvError::Closed) => ContinuationOutcome::Interrupted,
                    }
                }
            }
        }
    }

    /// Run the relay loop until the agent stream ends.
    pub async fn run<R>(self, rx: R)
    where
        R: Into<AgentStreamReceiver>,
    {
        let rx = rx.into();
        if is_buffered_platform(self.config.platform) {
            self.run_buffered(rx).await;
        } else {
            self.run_editable(rx).await;
        }
    }

    /// Buffered relay for channels where intermediate messages should not be
    /// exposed as separate user-facing messages.
    async fn run_buffered(self, mut rx: AgentStreamReceiver) {
        let mut text_buffer = String::new();
        let mut pending_event = None;

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) if is_segment_boundary(&event) => {
                    if !text_buffer.trim().is_empty() {
                        let reply = format_remote_reply(&self.config.prompt_text, &text_buffer, self.config.platform);
                        let formatted = format_text_for_platform(&reply, self.config.platform);
                        let preview = self.route_preview(&text_buffer);
                        self.send_buffered_message(&formatted, false, Some(&preview)).await;
                        text_buffer.clear();
                    }
                }
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
                        text_buffer.push_str(&chunk);
                    }
                    Some(StreamAction::Thinking(_)) => {}
                    Some(StreamAction::ToolCall { .. }) => {}
                    Some(StreamAction::Finish) => {
                        match self.next_continuation_event(&mut rx).await {
                            ContinuationOutcome::Event(event) => {
                                pending_event = Some(*event);
                                continue;
                            }
                            ContinuationOutcome::Interrupted => {
                                let error_text = stream_error_text(&text_buffer, "回复流中断，请重试");
                                let reply =
                                    format_remote_reply(&self.config.prompt_text, &error_text, self.config.platform);
                                let formatted = format_text_for_platform(&reply, self.config.platform);
                                let preview = self.route_preview(&error_text);
                                self.send_buffered_message(&formatted, false, Some(&preview)).await;
                                break;
                            }
                            ContinuationOutcome::AgentStopped => {}
                        }
                        if !text_buffer.trim().is_empty() {
                            let reply =
                                format_remote_reply(&self.config.prompt_text, &text_buffer, self.config.platform);
                            let formatted = format_text_for_platform(&reply, self.config.platform);
                            let preview = self.route_preview(&text_buffer);
                            self.send_buffered_message(&formatted, true, Some(&preview)).await;
                        }
                        info!(
                            plugin_id = %self.config.plugin_id,
                            chat_id = %self.config.chat_id,
                            text_len = text_buffer.len(),
                            platform = %self.config.platform,
                            "channel stream relay finished (buffered)"
                        );
                        break;
                    }
                    Some(StreamAction::Error(msg)) => {
                        let error_text = stream_error_text(&text_buffer, &msg);
                        let reply = format_remote_reply(&self.config.prompt_text, &error_text, self.config.platform);
                        let formatted = format_text_for_platform(&reply, self.config.platform);
                        let preview = self.route_preview(&error_text);
                        self.send_buffered_message(&formatted, false, Some(&preview)).await;
                        break;
                    }
                    None => {}
                },
                Err(broadcast::error::RecvError::Closed) => {
                    let error_text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    let reply = format_remote_reply(&self.config.prompt_text, &error_text, self.config.platform);
                    let formatted = format_text_for_platform(&reply, self.config.platform);
                    let preview = self.route_preview(&error_text);
                    self.send_buffered_message(&formatted, false, Some(&preview)).await;
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(
                        lagged = n,
                        platform = %self.config.platform,
                        "channel stream relay lagged (buffered)"
                    );
                    let error_text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    let reply = format_remote_reply(&self.config.prompt_text, &error_text, self.config.platform);
                    let formatted = format_text_for_platform(&reply, self.config.platform);
                    let preview = self.route_preview(&error_text);
                    self.send_buffered_message(&formatted, false, Some(&preview)).await;
                    break;
                }
            }
        }

        debug!(
            plugin_id = %self.config.plugin_id,
            chat_id = %self.config.chat_id,
            platform = %self.config.platform,
            "channel stream relay exited (buffered)"
        );
    }

    /// Keep streaming updates on one message per assistant segment. Only a
    /// completed segment or a size boundary creates another channel message.
    async fn run_editable(self, mut rx: AgentStreamReceiver) {
        let throttle = Duration::from_millis(self.config.throttle_ms.max(1));
        let mut receipt = if self.config.platform == PluginType::Lark {
            None
        } else {
            match self
                .sender
                .send_message(
                    &self.config.owner_user_id,
                    &self.config.plugin_id,
                    &self.config.chat_id,
                    ChannelMessageService::build_thinking_message(),
                )
                .await
            {
                Ok(receipt) => {
                    self.register_message_route(&receipt, None).await;
                    Some(receipt)
                }
                Err(error) => {
                    warn!(error = %error, platform = %self.config.platform,
                        "channel placeholder failed; buffering until segment completion");
                    None
                }
            }
        };
        let mut stream_updates_enabled = receipt.is_some() || self.config.platform == PluginType::Lark;
        let mut text_buffer = String::new();
        let mut last_edit = Instant::now();
        let mut pending_visible_chars = 0;
        let mut pending_event = None;

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => {
                    tokio::select! {
                        event = rx.recv() => event,
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(last_edit + throttle)),
                            if pending_visible_chars >= MIN_STREAM_UPDATE_CHARS && stream_updates_enabled => {
                            let reply = format_remote_reply(&self.config.prompt_text, &text_buffer, self.config.platform);
                            let formatted = format_text_for_platform(&reply, self.config.platform);
                            let limit = message_limit(self.config.platform).saturating_sub("\n\n⏳ working".len());
                            let preview = truncate_message(&formatted, limit);
                            let message = ChannelMessageService::build_streaming_message(&preview);
                            let result = if let Some(active) = receipt.as_ref() {
                                self.sender.edit_message(&self.config.owner_user_id, &self.config.plugin_id,
                                    &self.config.chat_id, &active.message_id, message).await
                            } else {
                                match self.sender.send_message(&self.config.owner_user_id, &self.config.plugin_id,
                                    &self.config.chat_id, message).await {
                                    Ok(sent) => {
                                        self.register_message_route(&sent, None).await;
                                        receipt = Some(sent);
                                        Ok(())
                                    }
                                    Err(error) => Err(error),
                                }
                            };
                            if let Err(error) = result {
                                warn!(error = %error, platform = %self.config.platform,
                                    "channel streaming update failed; buffering until segment completion");
                                stream_updates_enabled = false;
                            }
                            last_edit = Instant::now();
                            pending_visible_chars = 0;
                            continue;
                        }
                    }
                }
            };
            match received {
                Ok(event) if is_segment_boundary(&event) => {
                    if !text_buffer.trim().is_empty() {
                        self.deliver_editable_segment(&text_buffer, receipt.take(), false).await;
                        text_buffer.clear();
                        pending_visible_chars = 0;
                        stream_updates_enabled = true;
                        last_edit = Instant::now();
                    }
                }
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
                        pending_visible_chars += chunk.chars().filter(|character| !character.is_whitespace()).count();
                        text_buffer.push_str(&chunk);
                    }
                    Some(StreamAction::Finish) => {
                        match self.next_continuation_event(&mut rx).await {
                            ContinuationOutcome::Event(event) => {
                                pending_event = Some(*event);
                                continue;
                            }
                            ContinuationOutcome::Interrupted => {
                                let text = stream_error_text(&text_buffer, "回复流中断，请重试");
                                self.deliver_editable_segment(&text, receipt.take(), false).await;
                                break;
                            }
                            ContinuationOutcome::AgentStopped => {}
                        }
                        if !text_buffer.trim().is_empty() || receipt.is_some() {
                            self.deliver_editable_segment(&text_buffer, receipt.take(), true).await;
                        }
                        info!(plugin_id = %self.config.plugin_id, conversation_id = %self.config.conversation_id,
                            platform = %self.config.platform, "channel stream relay finished");
                        break;
                    }
                    Some(StreamAction::Error(reason)) => {
                        let text = stream_error_text(&text_buffer, &reason);
                        self.deliver_editable_segment(&text, receipt.take(), false).await;
                        break;
                    }
                    _ => {}
                },
                Err(error) => {
                    warn!(error = %error, platform = %self.config.platform, "channel reply stream interrupted");
                    let text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    self.deliver_editable_segment(&text, receipt.take(), false).await;
                    break;
                }
            }
        }
    }

    async fn deliver_editable_segment(&self, text: &str, receipt: Option<ChannelDeliveryReceipt>, complete: bool) {
        let reply = format_remote_reply(&self.config.prompt_text, text, self.config.platform);
        let formatted = format_text_for_platform(&reply, self.config.platform);
        let limit = message_limit(self.config.platform);
        let mut chunks = split_text_for_limit(&formatted, limit);
        if complete {
            if chunks.is_empty() {
                chunks.push(String::new());
            }
            reserve_completion_suffix(&mut chunks, limit);
        }
        let preview = self.route_preview(text);
        for (index, chunk) in chunks.iter().enumerate() {
            let is_last = index + 1 == chunks.len();
            let mut message = if complete && is_last {
                ChannelMessageService::build_final_message(chunk)
            } else {
                ChannelMessageService::build_text_message(chunk)
            };
            // DingTalk's card API uses the presence of buttons as its finalize
            // signal. Every call here closes a segment or a terminated stream;
            // intermediate working edits keep buttons absent. Empty buttons
            // also route overflow/replacement sends through the one-shot API.
            if self.config.platform == PluginType::Dingtalk && message.buttons.is_none() {
                message.buttons = Some(Vec::new());
            }
            if index == 0
                && let Some(active) = receipt.as_ref()
            {
                match self
                    .sender
                    .edit_message(
                        &self.config.owner_user_id,
                        &self.config.plugin_id,
                        &self.config.chat_id,
                        &active.message_id,
                        message.clone(),
                    )
                    .await
                {
                    Ok(()) => {
                        self.register_message_route(active, Some(&preview)).await;
                        continue;
                    }
                    Err(error) => warn!(error = %error, platform = %self.config.platform,
                        "channel segment edit failed; sending replacement"),
                }
            }
            match self
                .sender
                .send_message(
                    &self.config.owner_user_id,
                    &self.config.plugin_id,
                    &self.config.chat_id,
                    message,
                )
                .await
            {
                Ok(sent) => self.register_message_route(&sent, Some(&preview)).await,
                Err(error) => {
                    warn!(error = %error, platform = %self.config.platform, chunk_index = index,
                        chunk_count = chunks.len(), "channel segment delivery failed");
                    break;
                }
            }
        }
    }

    async fn send_buffered_message(&self, formatted: &str, with_buttons: bool, preview: Option<&str>) {
        let limit = message_limit(self.config.platform);
        let mut chunks = split_text_for_limit(formatted, limit);
        if with_buttons && chunks.is_empty() {
            chunks.push(String::new());
        }
        if with_buttons {
            reserve_completion_suffix(&mut chunks, limit);
        }
        for (index, chunk) in chunks.iter().enumerate() {
            let is_last = index + 1 == chunks.len();
            let message = if with_buttons && is_last {
                ChannelMessageService::build_final_message(chunk)
            } else {
                ChannelMessageService::build_text_message(chunk)
            };
            match self
                .sender
                .send_message(
                    &self.config.owner_user_id,
                    &self.config.plugin_id,
                    &self.config.chat_id,
                    message,
                )
                .await
            {
                Ok(receipt) => {
                    self.register_message_route(&receipt, if is_last { preview } else { None })
                        .await;
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        chunk_index = index,
                        chunk_count = chunks.len(),
                        platform = %self.config.platform,
                        "failed to send buffered channel reply chunk"
                    );
                    break;
                }
            }
        }
    }

    async fn register_message_route(&self, receipt: &ChannelDeliveryReceipt, preview_text: Option<&str>) {
        let Some(session_manager) = &self.session_manager else {
            return;
        };
        let platform_type = self.config.platform.to_string();
        if let Err(e) = session_manager
            .register_conversation_route(
                &self.config.owner_user_id,
                &aionui_db::UpsertChannelConversationRouteParams {
                    conversation_id: &self.config.conversation_id,
                    platform_type: &platform_type,
                    chat_id: &self.config.chat_id,
                    message_id: &receipt.message_id,
                    preview_text,
                    sent_at: receipt.completed_at,
                    delivery_started_at: Some(receipt.started_at),
                    delivery_completed_at: Some(receipt.completed_at),
                },
            )
            .await
        {
            warn!(error = %e, "failed to register channel reply route");
        }
    }

    fn route_preview(&self, assistant_text: &str) -> String {
        format!(
            "你：{}\n\nAI：{}",
            self.config.prompt_text.trim(),
            assistant_text.trim()
        )
        .chars()
        .take(240)
        .collect()
    }
}

/// Include the original prompt in remote replies where users cannot reliably
/// associate an answer with the originating message from the conversation list.
/// Other platforms keep their existing answer-only message shape.
pub(crate) fn format_remote_reply(prompt_text: &str, assistant_text: &str, platform: PluginType) -> String {
    let prompt = prompt_text.trim();
    let answer = assistant_text.trim();
    if matches!(platform, PluginType::Weixin | PluginType::Wecom | PluginType::Lark) && !prompt.is_empty() {
        format!("**你：** {prompt}\n\n**AI：** {answer}")
    } else {
        answer.to_owned()
    }
}

fn split_text_for_limit(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() || limit == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut remaining = text;
    // Plugins guard the encoded byte length before their platform request.
    // Split at UTF-8 boundaries so no downstream guard truncates the answer.
    while remaining.len() > limit {
        let boundary = remaining
            .char_indices()
            .map(|(index, _)| index)
            .take_while(|index| *index <= limit)
            .last()
            .unwrap_or(0);
        if boundary == 0 {
            return chunks;
        }
        let split_at = remaining[..boundary]
            .rfind('\n')
            .map(|index| index + 1)
            .filter(|index| *index > 0)
            .unwrap_or(boundary);
        chunks.push(remaining[..split_at].to_owned());
        remaining = &remaining[split_at..];
    }
    if !remaining.is_empty() {
        chunks.push(remaining.to_owned());
    }
    chunks
}

/// `build_final_message` appends the completion marker to the last chunk.
/// Move enough content into an additional chunk so that the marker itself is
/// included in the channel's size limit.
fn reserve_completion_suffix(chunks: &mut Vec<String>, limit: usize) {
    const COMPLETION_SUFFIX: &str = "\n\n✅ worked";
    let suffix_len = COMPLETION_SUFFIX.len();
    let Some(last) = chunks.last_mut() else {
        return;
    };
    let max_content_len = limit.saturating_sub(suffix_len);
    let last_len = last.len();
    if last_len <= max_content_len {
        return;
    }

    let split_at = last
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| last_len - *index <= max_content_len)
        .unwrap_or(last_len);
    let tail = last[split_at..].to_owned();
    last.truncate(split_at);
    chunks.push(tail);
}

fn message_limit(platform: PluginType) -> usize {
    match platform {
        PluginType::Telegram => TELEGRAM_MESSAGE_LIMIT,
        PluginType::Discord => DISCORD_MESSAGE_LIMIT,
        PluginType::Slack => SLACK_MESSAGE_LIMIT,
        PluginType::Dingtalk => DINGTALK_MESSAGE_LIMIT,
        PluginType::Lark => LARK_MESSAGE_LIMIT,
        PluginType::Weixin | PluginType::Wecom => WEIXIN_MESSAGE_LIMIT,
    }
}

fn is_segment_boundary(event: &AgentStreamEvent) -> bool {
    matches!(event, AgentStreamEvent::SegmentBreak)
}

fn stream_error_text(text_buffer: &str, reason: &str) -> String {
    let error = format!("\u{274c} {reason}");
    if text_buffer.trim().is_empty() {
        error
    } else {
        format!("{}\n\n{error}", text_buffer.trim_end())
    }
}

/// WeChat cannot edit messages, and WeCom has no runtime plugin yet. Both
/// buffer each assistant segment so streamed deltas do not create extra messages.
fn is_buffered_platform(platform: PluginType) -> bool {
    matches!(platform, PluginType::Weixin | PluginType::Wecom)
}

// ── Test helpers (pub so integration tests can use them) ─────────

/// Records send/edit calls for test assertions.
pub struct MessageRecorder {
    sends: std::sync::Mutex<Vec<UnifiedOutgoingMessage>>,
    edits: std::sync::Mutex<Vec<UnifiedOutgoingMessage>>,
}

impl MessageRecorder {
    pub fn new() -> Self {
        Self {
            sends: std::sync::Mutex::new(Vec::new()),
            edits: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn take_sends(&self) -> Vec<UnifiedOutgoingMessage> {
        std::mem::take(&mut self.sends.lock().unwrap())
    }

    pub fn snapshot_sends(&self) -> Vec<UnifiedOutgoingMessage> {
        self.sends.lock().unwrap().clone()
    }

    pub fn take_edits(&self) -> Vec<UnifiedOutgoingMessage> {
        std::mem::take(&mut self.edits.lock().unwrap())
    }
}

impl Default for MessageRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ChannelSender for MessageRecorder {
    async fn send_message(
        &self,
        _owner_user_id: &str,
        _plugin_id: &str,
        _chat_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<ChannelDeliveryReceipt, ChannelError> {
        self.sends.lock().unwrap().push(message);
        Ok(ChannelDeliveryReceipt {
            message_id: "msg-1".into(),
            started_at: 1,
            completed_at: 2,
        })
    }

    async fn edit_message(
        &self,
        _owner_user_id: &str,
        _plugin_id: &str,
        _chat_id: &str,
        _message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        self.edits.lock().unwrap().push(message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::format_remote_reply;
    use crate::types::PluginType;

    #[test]
    fn remote_reply_uses_markdown_bold_context_labels() {
        assert_eq!(
            format_remote_reply("原始问题", "回答内容", PluginType::Wecom),
            "**你：** 原始问题\n\n**AI：** 回答内容"
        );
    }

    #[test]
    fn remote_reply_omits_context_without_prompt_or_for_other_platforms() {
        assert_eq!(format_remote_reply("   ", "回答内容", PluginType::Weixin), "回答内容");
        assert_eq!(
            format_remote_reply("原始问题", "回答内容", PluginType::Telegram),
            "回答内容"
        );
    }
}
