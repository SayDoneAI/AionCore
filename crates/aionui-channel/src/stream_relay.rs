use std::sync::Arc;
use std::time::{Duration, Instant};

use aionui_ai_agent::AgentStreamEvent;
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::error::ChannelError;
use crate::formatter::format_text_for_platform;
use crate::message_service::{ChannelMessageService, StreamAction};
use crate::session::SessionManager;
use crate::types::{OutgoingMessageType, PluginType, UnifiedOutgoingMessage};

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
/// - Send "Thinking..." placeholder on start
/// - Accumulate text, throttled editMessage every N ms
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

    async fn next_continuation_event(
        &self,
        rx: &mut broadcast::Receiver<AgentStreamEvent>,
    ) -> Option<AgentStreamEvent> {
        let runtime_state = self.runtime_state.as_ref()?;
        loop {
            match rx.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    warn!(skipped, "channel stream relay lagged while awaiting continuation");
                    continue;
                }
                Err(broadcast::error::TryRecvError::Closed) => return None,
                Err(broadcast::error::TryRecvError::Empty) => {}
            }
            tokio::select! {
                _ = runtime_state.wait_until_unclaimed(&self.config.conversation_id) => return None,
                event = rx.recv() => match event {
                    Ok(event) => return Some(event),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "channel stream relay lagged while awaiting continuation");
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }
    }

    /// Run the relay loop until the agent stream ends.
    pub async fn run(self, rx: broadcast::Receiver<AgentStreamEvent>) {
        if is_weixin_platform(self.config.platform) {
            self.run_weixin(rx).await;
        } else {
            self.run_editable(rx).await;
        }
    }

    /// WeChat-specific relay: no edit support, accumulate text then send once.
    async fn run_weixin(self, mut rx: broadcast::Receiver<AgentStreamEvent>) {
        let mut text_buffer = String::new();
        let mut has_content = false;
        let mut pending_event = None;

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
                        text_buffer.push_str(&chunk);
                        has_content = true;
                    }
                    Some(StreamAction::Thinking(_)) => {}
                    Some(StreamAction::ToolCall { .. }) => {}
                    Some(StreamAction::Finish) => {
                        if let Some(event) = self.next_continuation_event(&mut rx).await {
                            pending_event = Some(event);
                            continue;
                        }
                        if has_content && !text_buffer.trim().is_empty() {
                            let formatted = format_text_for_platform(&text_buffer, self.config.platform);
                            let final_msg = ChannelMessageService::build_final_message(&formatted);
                            if let Ok(receipt) = self
                                .sender
                                .send_message(
                                    &self.config.owner_user_id,
                                    &self.config.plugin_id,
                                    &self.config.chat_id,
                                    final_msg,
                                )
                                .await
                            {
                                let preview = self.route_preview(&formatted);
                                self.register_message_route(&receipt, Some(&preview)).await;
                            }
                        }
                        info!(
                            plugin_id = %self.config.plugin_id,
                            chat_id = %self.config.chat_id,
                            text_len = text_buffer.len(),
                            "channel stream relay finished (weixin)"
                        );
                        break;
                    }
                    Some(StreamAction::Error(msg)) => {
                        let error_msg = UnifiedOutgoingMessage {
                            message_type: OutgoingMessageType::Text,
                            text: Some(format!("\u{274c} {msg}")),
                            parse_mode: None,
                            buttons: None,
                            keyboard: None,
                            image_url: None,
                            file_url: None,
                            file_name: None,
                            media_actions: None,
                            reply_to_message_id: None,
                            silent: None,
                        };
                        if let Ok(receipt) = self
                            .sender
                            .send_message(
                                &self.config.owner_user_id,
                                &self.config.plugin_id,
                                &self.config.chat_id,
                                error_msg,
                            )
                            .await
                        {
                            let preview = self.route_preview(&format!("\u{274c} {msg}"));
                            self.register_message_route(&receipt, Some(&preview)).await;
                        }
                        break;
                    }
                    None => {}
                },
                Err(broadcast::error::RecvError::Closed) => {
                    if has_content && !text_buffer.trim().is_empty() {
                        let formatted = format_text_for_platform(&text_buffer, self.config.platform);
                        let final_msg = ChannelMessageService::build_final_message(&formatted);
                        if let Ok(receipt) = self
                            .sender
                            .send_message(
                                &self.config.owner_user_id,
                                &self.config.plugin_id,
                                &self.config.chat_id,
                                final_msg,
                            )
                            .await
                        {
                            let preview = self.route_preview(&formatted);
                            self.register_message_route(&receipt, Some(&preview)).await;
                        }
                    }
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, "channel stream relay lagged (weixin)");
                }
            }
        }

        debug!(
            plugin_id = %self.config.plugin_id,
            chat_id = %self.config.chat_id,
            "channel stream relay exited (weixin)"
        );
    }

    /// Standard relay for platforms that support edit (Telegram, Lark, DingTalk).
    async fn run_editable(self, mut rx: broadcast::Receiver<AgentStreamEvent>) {
        let throttle = Duration::from_millis(self.config.throttle_ms);

        let thinking_msg = ChannelMessageService::build_thinking_message();
        let thinking_receipt = match self
            .sender
            .send_message(
                &self.config.owner_user_id,
                &self.config.plugin_id,
                &self.config.chat_id,
                thinking_msg,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(e) => {
                error!(error = %e, "failed to send thinking message");
                return;
            }
        };
        self.register_message_route(&thinking_receipt, None).await;
        let thinking_msg_id = thinking_receipt.message_id.clone();

        let mut text_buffer = String::new();
        let mut last_edit = Instant::now() - throttle;
        let mut has_content = false;
        let mut pending_event = None;

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
                        text_buffer.push_str(&chunk);
                        has_content = true;
                        if last_edit.elapsed() >= throttle {
                            let formatted = format_text_for_platform(&text_buffer, self.config.platform);
                            let msg = ChannelMessageService::build_streaming_message(&formatted);
                            let _ = self
                                .sender
                                .edit_message(
                                    &self.config.owner_user_id,
                                    &self.config.plugin_id,
                                    &self.config.chat_id,
                                    &thinking_msg_id,
                                    msg,
                                )
                                .await;
                            last_edit = Instant::now();
                        }
                    }
                    Some(StreamAction::Thinking(_)) => {}
                    Some(StreamAction::ToolCall { name, .. }) => {
                        let msg = ChannelMessageService::build_streaming_message(&format!("\u{23f3} {name}..."));
                        let _ = self
                            .sender
                            .edit_message(
                                &self.config.owner_user_id,
                                &self.config.plugin_id,
                                &self.config.chat_id,
                                &thinking_msg_id,
                                msg,
                            )
                            .await;
                    }
                    Some(StreamAction::Finish) => {
                        if let Some(event) = self.next_continuation_event(&mut rx).await {
                            pending_event = Some(event);
                            continue;
                        }
                        self.send_final_edit(&text_buffer, has_content, &thinking_msg_id).await;
                        if has_content {
                            let formatted = format_text_for_platform(&text_buffer, self.config.platform);
                            let preview = self.route_preview(&formatted);
                            self.register_message_route(&thinking_receipt, Some(&preview)).await;
                        }
                        info!(
                            plugin_id = %self.config.plugin_id,
                            chat_id = %self.config.chat_id,
                            text_len = text_buffer.len(),
                            "channel stream relay finished"
                        );
                        break;
                    }
                    Some(StreamAction::Error(msg)) => {
                        let error_msg = UnifiedOutgoingMessage {
                            message_type: OutgoingMessageType::Text,
                            text: Some(format!("\u{274c} {msg}")),
                            parse_mode: None,
                            buttons: None,
                            keyboard: None,
                            image_url: None,
                            file_url: None,
                            file_name: None,
                            media_actions: None,
                            reply_to_message_id: None,
                            silent: None,
                        };
                        let _ = self
                            .sender
                            .edit_message(
                                &self.config.owner_user_id,
                                &self.config.plugin_id,
                                &self.config.chat_id,
                                &thinking_msg_id,
                                error_msg,
                            )
                            .await;
                        let preview = self.route_preview(&format!("\u{274c} {msg}"));
                        self.register_message_route(&thinking_receipt, Some(&preview)).await;
                        break;
                    }
                    None => {}
                },
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("channel stream relay: broadcast closed without terminal event");
                    self.send_final_edit(&text_buffer, has_content, &thinking_msg_id).await;
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, "channel stream relay lagged");
                }
            }
        }

        debug!(
            plugin_id = %self.config.plugin_id,
            chat_id = %self.config.chat_id,
            "channel stream relay exited"
        );
    }

    async fn send_final_edit(&self, text_buffer: &str, has_content: bool, msg_id: &str) {
        if has_content {
            let formatted = format_text_for_platform(text_buffer, self.config.platform);
            let final_msg = ChannelMessageService::build_final_message(&formatted);
            let _ = self
                .sender
                .edit_message(
                    &self.config.owner_user_id,
                    &self.config.plugin_id,
                    &self.config.chat_id,
                    msg_id,
                    final_msg,
                )
                .await;
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

/// WeChat / WeCom channels cannot edit messages in place. Their relay buffers
/// the whole turn and sends one final answer.
fn is_weixin_platform(platform: PluginType) -> bool {
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
