use std::collections::HashSet;
use std::sync::Arc;

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentStatus {
    Working,
    Worked,
    None,
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
    project_name: Option<String>,
}

impl ChannelStreamRelay {
    pub fn new(config: RelayConfig, sender: Arc<dyn ChannelSender>) -> Self {
        Self {
            config,
            sender,
            session_manager: None,
            runtime_state: None,
            project_name: None,
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
            project_name: None,
        }
    }

    pub fn with_runtime_state(mut self, runtime_state: Arc<ConversationRuntimeStateService>) -> Self {
        self.runtime_state = Some(runtime_state);
        self
    }

    pub fn with_workspace(mut self, workspace: Option<&str>) -> Self {
        self.project_name = workspace_folder_name(workspace);
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

    /// Buffered relay for channels that cannot edit messages. Each visible
    /// tool/thinking boundary closes the text accumulated before it.
    async fn run_buffered(self, mut rx: AgentStreamReceiver) {
        let mut text_buffer = String::new();
        let mut pending_event = None;
        let mut seen_boundary_keys = HashSet::new();

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) if is_new_segment_boundary(&event, &mut seen_boundary_keys) => {
                    if !text_buffer.trim().is_empty() {
                        let preview = self.route_preview(&text_buffer);
                        self.send_buffered_message(&text_buffer, SegmentStatus::Working, Some(&preview))
                            .await;
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
                                let preview = self.route_preview(&error_text);
                                self.send_buffered_message(&error_text, SegmentStatus::None, Some(&preview))
                                    .await;
                                break;
                            }
                            ContinuationOutcome::AgentStopped => {}
                        }
                        if !text_buffer.trim().is_empty() {
                            let preview = self.route_preview(&text_buffer);
                            self.send_buffered_message(&text_buffer, SegmentStatus::Worked, Some(&preview))
                                .await;
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
                        let preview = self.route_preview(&error_text);
                        self.send_buffered_message(&error_text, SegmentStatus::None, Some(&preview))
                            .await;
                        break;
                    }
                    None => {}
                },
                Err(broadcast::error::RecvError::Closed) => {
                    let error_text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    let preview = self.route_preview(&error_text);
                    self.send_buffered_message(&error_text, SegmentStatus::None, Some(&preview))
                        .await;
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(
                        lagged = n,
                        platform = %self.config.platform,
                        "channel stream relay lagged (buffered)"
                    );
                    let error_text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    let preview = self.route_preview(&error_text);
                    self.send_buffered_message(&error_text, SegmentStatus::None, Some(&preview))
                        .await;
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

    /// Keep one message per assistant segment. A visible tool/thinking boundary
    /// closes the accumulated text; individual text deltas never send a message.
    async fn run_editable(self, mut rx: AgentStreamReceiver) {
        let mut receipt = if self.config.platform == PluginType::Lark {
            None
        } else {
            match self
                .sender
                .send_message(
                    &self.config.owner_user_id,
                    &self.config.plugin_id,
                    &self.config.chat_id,
                    build_initial_working_message(
                        &self.config.prompt_text,
                        self.project_name.as_deref(),
                        self.config.platform,
                    ),
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
        let mut text_buffer = String::new();
        let mut pending_event = None;
        let mut seen_boundary_keys = HashSet::new();

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) if is_new_segment_boundary(&event, &mut seen_boundary_keys) => {
                    if !text_buffer.trim().is_empty() {
                        self.deliver_editable_segment(&text_buffer, receipt.take(), SegmentStatus::Working)
                            .await;
                        text_buffer.clear();
                    }
                }
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
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
                                self.deliver_editable_segment(&text, receipt.take(), SegmentStatus::None)
                                    .await;
                                break;
                            }
                            ContinuationOutcome::AgentStopped => {}
                        }
                        if !text_buffer.trim().is_empty() || receipt.is_some() {
                            self.deliver_editable_segment(&text_buffer, receipt.take(), SegmentStatus::Worked)
                                .await;
                        }
                        info!(plugin_id = %self.config.plugin_id, conversation_id = %self.config.conversation_id,
                            platform = %self.config.platform, "channel stream relay finished");
                        break;
                    }
                    Some(StreamAction::Error(reason)) => {
                        let text = stream_error_text(&text_buffer, &reason);
                        self.deliver_editable_segment(&text, receipt.take(), SegmentStatus::None)
                            .await;
                        break;
                    }
                    _ => {}
                },
                Err(error) => {
                    warn!(error = %error, platform = %self.config.platform, "channel reply stream interrupted");
                    let text = stream_error_text(&text_buffer, "回复流中断，请重试");
                    self.deliver_editable_segment(&text, receipt.take(), SegmentStatus::None)
                        .await;
                    break;
                }
            }
        }
    }

    async fn deliver_editable_segment(
        &self,
        text: &str,
        receipt: Option<ChannelDeliveryReceipt>,
        status: SegmentStatus,
    ) {
        let limit = message_limit(self.config.platform);
        let chunks = format_remote_reply_chunks(
            &self.config.prompt_text,
            self.project_name.as_deref(),
            text,
            self.config.platform,
            limit,
            status,
        );
        let preview = self.route_preview(text);
        for (index, chunk) in chunks.iter().enumerate() {
            let is_last = index + 1 == chunks.len();
            let mut message = if is_last {
                build_segment_message(chunk, status)
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

    async fn send_buffered_message(&self, text: &str, status: SegmentStatus, preview: Option<&str>) {
        let limit = message_limit(self.config.platform);
        let chunks = format_remote_reply_chunks(
            &self.config.prompt_text,
            self.project_name.as_deref(),
            text,
            self.config.platform,
            limit,
            status,
        );
        for (index, chunk) in chunks.iter().enumerate() {
            let is_last = index + 1 == chunks.len();
            let message = if is_last {
                build_segment_message(chunk, status)
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
        let project_name = self.project_name.as_deref().unwrap_or("项目");
        format!(
            "{}：{}\n\nAI：{}",
            project_name,
            self.config.prompt_text.trim(),
            assistant_text.trim()
        )
        .chars()
        .take(240)
        .collect()
    }
}

/// Return the final directory component of a persisted workspace path.
pub(crate) fn workspace_folder_name(workspace: Option<&str>) -> Option<String> {
    let path = workspace?.trim().trim_end_matches(['/', '\\']);
    let name = path.rsplit(['/', '\\']).next()?.trim();
    if name.is_empty() || name == "." || name == ".." {
        None
    } else {
        Some(name.to_owned())
    }
}

/// Include the original prompt and project folder in every channel reply so
/// users can associate each step with the originating conversation.
pub(crate) fn format_remote_reply(
    prompt_text: &str,
    project_name: Option<&str>,
    assistant_text: &str,
    _platform: PluginType,
) -> String {
    let prompt = prompt_text.trim();
    let answer = assistant_text.trim();
    if !prompt.is_empty() {
        let project_name = project_name.filter(|name| !name.trim().is_empty()).unwrap_or("项目");
        format!("**{project_name}：** {prompt}\n\n**AI：** {answer}")
    } else {
        answer.to_owned()
    }
}

fn build_initial_working_message(
    prompt_text: &str,
    project_name: Option<&str>,
    platform: PluginType,
) -> UnifiedOutgoingMessage {
    if prompt_text.trim().is_empty() {
        return ChannelMessageService::build_thinking_message();
    }

    let limit = message_limit(platform).saturating_sub("\n\n⏳ working".len());
    let prefix = format_remote_reply_prefix(prompt_text, project_name, platform, limit);
    ChannelMessageService::build_streaming_message(&prefix)
}

fn format_remote_reply_chunks(
    prompt_text: &str,
    project_name: Option<&str>,
    assistant_text: &str,
    platform: PluginType,
    limit: usize,
    status: SegmentStatus,
) -> Vec<String> {
    let answer = format_text_for_platform(assistant_text.trim(), platform);
    let status_suffix_len = status_suffix(status).map_or(0, str::len);
    let answer_budget = if answer.len() <= limit.saturating_sub(status_suffix_len) {
        answer.len()
    } else {
        // Leave room for substantial answer chunks when the context must repeat.
        limit / 2
    };
    let prefix_limit = limit.saturating_sub(status_suffix_len).saturating_sub(answer_budget);
    let prefix = format_remote_reply_prefix(prompt_text, project_name, platform, prefix_limit);
    let answer_limit = limit.saturating_sub(prefix.len()).saturating_sub(status_suffix_len);
    let mut chunks = split_text_for_limit(&answer, answer_limit);
    if chunks.is_empty() && status == SegmentStatus::Worked {
        chunks.push(String::new());
    }

    if prefix.is_empty() {
        chunks
    } else {
        chunks.into_iter().map(|chunk| format!("{prefix}{chunk}")).collect()
    }
}

fn format_remote_reply_prefix(
    prompt_text: &str,
    project_name: Option<&str>,
    platform: PluginType,
    max_bytes: usize,
) -> String {
    let prompt = prompt_text.trim();
    if prompt.is_empty() {
        return String::new();
    }
    let project_name = project_name.filter(|name| !name.trim().is_empty()).unwrap_or("项目");
    let format_prefix =
        |question: &str| format_text_for_platform(&format!("**{project_name}：** {question}\n\n**AI：** "), platform);
    let full_prefix = format_prefix(prompt);
    if full_prefix.len() <= max_bytes {
        return full_prefix;
    }

    let total_chars = prompt.chars().count();
    let mut low = 0;
    let mut high = total_chars;
    while low < high {
        let keep_chars = low + (high - low).div_ceil(2);
        let mut question: String = prompt.chars().take(keep_chars).collect();
        if keep_chars < total_chars {
            question.push_str("...");
        }
        if format_prefix(&question).len() <= max_bytes {
            low = keep_chars;
        } else {
            high = keep_chars - 1;
        }
    }

    let mut question: String = prompt.chars().take(low).collect();
    if low < total_chars {
        question.push_str("...");
    }
    let prefix = format_prefix(&question);
    if prefix.len() <= max_bytes {
        prefix
    } else {
        truncate_message(&prefix, max_bytes)
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

fn build_segment_message(text: &str, status: SegmentStatus) -> UnifiedOutgoingMessage {
    match status {
        SegmentStatus::Working => ChannelMessageService::build_streaming_message(text),
        SegmentStatus::Worked => ChannelMessageService::build_final_message(text),
        SegmentStatus::None => ChannelMessageService::build_text_message(text),
    }
}

fn status_suffix(status: SegmentStatus) -> Option<&'static str> {
    match status {
        SegmentStatus::Working => Some("\n\n⏳ working"),
        SegmentStatus::Worked => Some("\n\n✅ worked"),
        SegmentStatus::None => None,
    }
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
    matches!(
        event,
        AgentStreamEvent::SegmentBreak
            | AgentStreamEvent::ToolCall(_)
            | AgentStreamEvent::AcpToolCall(_)
            | AgentStreamEvent::ToolGroup(_)
    ) || matches!(
        event,
        AgentStreamEvent::Thinking(data)
            if data.status.as_deref() != Some("done") && !data.content.is_empty()
    )
}

fn is_new_segment_boundary(event: &AgentStreamEvent, seen_keys: &mut HashSet<String>) -> bool {
    if !is_segment_boundary(event) {
        return false;
    }
    let Some(key) = segment_boundary_key(event) else {
        return true;
    };
    seen_keys.insert(key)
}

fn segment_boundary_key(event: &AgentStreamEvent) -> Option<String> {
    let (kind, call_id) = match event {
        AgentStreamEvent::ToolCall(data) => ("tool_call", data.call_id.as_str()),
        AgentStreamEvent::AcpToolCall(data) => ("acp_tool_call", data.update.tool_call_id.as_str()),
        AgentStreamEvent::ToolGroup(entries) => (
            "tool_group",
            entries.iter().find(|entry| !entry.call_id.is_empty())?.call_id.as_str(),
        ),
        _ => return None,
    };
    (!call_id.is_empty()).then(|| format!("{kind}:{call_id}"))
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
    use super::{
        SegmentStatus, build_initial_working_message, build_segment_message, format_remote_reply,
        format_remote_reply_chunks, workspace_folder_name,
    };
    use crate::constants::DISCORD_MESSAGE_LIMIT;
    use crate::message_service::ChannelMessageService;
    use crate::types::PluginType;

    #[test]
    fn remote_reply_uses_workspace_folder_as_context_label() {
        assert_eq!(
            format_remote_reply("原始问题", Some("project-a"), "回答内容", PluginType::Wecom),
            "**project-a：** 原始问题\n\n**AI：** 回答内容"
        );
    }

    #[test]
    fn remote_reply_uses_generic_project_label_when_workspace_is_missing() {
        assert_eq!(
            format_remote_reply("原始问题", None, "回答内容", PluginType::Telegram),
            "**项目：** 原始问题\n\n**AI：** 回答内容"
        );
        assert_eq!(
            format_remote_reply("   ", Some("/Users/me/project-a"), "回答内容", PluginType::Weixin),
            "回答内容"
        );
    }

    #[test]
    fn workspace_folder_name_handles_unix_windows_and_root_paths() {
        assert_eq!(
            workspace_folder_name(Some("/Users/me/project-a")),
            Some("project-a".to_owned())
        );
        assert_eq!(
            workspace_folder_name(Some(r"C:\Users\me\project-a")),
            Some("project-a".to_owned())
        );
        assert_eq!(workspace_folder_name(Some("/")), None);
        assert_eq!(workspace_folder_name(Some("  ")), None);
    }

    #[test]
    fn long_question_and_answer_fit_discord_limits_without_losing_answer_text() {
        let question = "question ".repeat(600);
        let answer = "answer ".repeat(500);
        let chunks = format_remote_reply_chunks(
            &question,
            Some("project-a"),
            &answer,
            PluginType::Discord,
            DISCORD_MESSAGE_LIMIT,
            SegmentStatus::Worked,
        );

        assert!(chunks.len() > 1);
        let mut recovered_answer = String::new();
        for (index, chunk) in chunks.iter().enumerate() {
            let message = if index + 1 == chunks.len() {
                build_segment_message(chunk, SegmentStatus::Worked)
            } else {
                ChannelMessageService::build_text_message(chunk)
            };
            let text = message.text.unwrap();
            assert!(text.len() <= DISCORD_MESSAGE_LIMIT);
            assert!(text.starts_with("**project-a：** question"));
            let content = text.split_once("**AI：** ").unwrap().1;
            recovered_answer.push_str(if index + 1 == chunks.len() {
                content.strip_suffix("\n\n✅ worked").unwrap()
            } else {
                content
            });
        }
        assert_eq!(recovered_answer, answer.trim());
    }

    #[test]
    fn initial_working_message_stays_within_platform_limit_for_long_question() {
        let message = build_initial_working_message(&"question ".repeat(600), Some("project-a"), PluginType::Discord);
        let text = message.text.unwrap();

        assert!(text.len() <= DISCORD_MESSAGE_LIMIT);
        assert!(text.starts_with("**project-a：** question"));
        assert!(text.ends_with("\n\n⏳ working"));
    }
}
