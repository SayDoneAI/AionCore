use std::sync::Arc;

use aionui_ai_agent::{AgentStreamEvent, AgentStreamReceiver};
use aionui_conversation::runtime_state::ConversationRuntimeStateService;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::error::ChannelError;
use crate::formatter::format_text_for_platform;
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
}

enum ContinuationOutcome {
    Event(Box<AgentStreamEvent>),
    AgentStopped,
    Interrupted,
}

/// Abstraction for sending/editing messages through a channel plugin.
///
/// The relay only sends new messages. Editing remains part of this trait because
/// channel action responses can still target an existing message.
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

/// Relays the meaningful text produced during an agent turn to a channel.
///
/// Every channel follows the same relay contract:
/// - send a working marker immediately;
/// - send each non-empty text event as it arrives with `working`;
/// - send one completion marker with `worked` after the turn finishes.
///
/// Thinking and tool-call events remain internal agent progress and are not
/// exposed as channel messages.
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
                    _ = runtime_state.wait_until_unclaimed(&self.config.conversation_id) => ContinuationOutcome::AgentStopped,
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
        let mut rx = rx.into();

        if self
            .send_outgoing(ChannelMessageService::build_thinking_message(), None)
            .await
            .is_none()
        {
            error!("failed to send channel working marker");
            return;
        }

        let mut text_buffer = String::new();
        let mut pending_event = None;

        loop {
            let received = match pending_event.take() {
                Some(event) => Ok(event),
                None => rx.recv().await,
            };
            match received {
                Ok(event) => match ChannelMessageService::process_stream_event(&event) {
                    Some(StreamAction::AppendText(chunk)) => {
                        if chunk.trim().is_empty() {
                            continue;
                        }
                        text_buffer.push_str(&chunk);
                        let formatted = format_text_for_platform(&chunk, self.config.platform);
                        let message = ChannelMessageService::build_streaming_message(&formatted);
                        self.send_outgoing(message, None).await;
                    }
                    Some(StreamAction::Thinking(_)) | Some(StreamAction::ToolCall { .. }) => {}
                    Some(StreamAction::Finish) => match self.next_continuation_event(&mut rx).await {
                        ContinuationOutcome::Event(event) => {
                            pending_event = Some(*event);
                        }
                        ContinuationOutcome::AgentStopped => {
                            let preview = self.route_preview(&text_buffer);
                            self.send_outgoing(ChannelMessageService::build_final_message(""), Some(&preview))
                                .await;
                            info!(
                                plugin_id = %self.config.plugin_id,
                                chat_id = %self.config.chat_id,
                                text_len = text_buffer.len(),
                                "channel stream relay finished"
                            );
                            break;
                        }
                        ContinuationOutcome::Interrupted => {
                            self.send_error_message("回复流中断，请重试").await;
                            break;
                        }
                    },
                    Some(StreamAction::Error(message)) => {
                        self.send_error_message(&message).await;
                        break;
                    }
                    None => {}
                },
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("channel stream relay: broadcast closed without terminal event");
                    self.send_error_message("回复流中断，请重试").await;
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "channel stream relay lagged");
                    self.send_error_message("回复流中断，请重试").await;
                    break;
                }
            }
        }

        debug!(
            plugin_id = %self.config.plugin_id,
            chat_id = %self.config.chat_id,
            "channel stream relay exited"
        );
    }

    async fn send_error_message(&self, reason: &str) {
        let text = format_text_for_platform(&format!("❌ {reason}"), self.config.platform);
        let preview = self.route_preview(&text);
        self.send_outgoing(ChannelMessageService::build_text_message(&text), Some(&preview))
            .await;
    }

    async fn send_outgoing(
        &self,
        message: UnifiedOutgoingMessage,
        preview_text: Option<&str>,
    ) -> Option<ChannelDeliveryReceipt> {
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
                self.register_message_route(&receipt, preview_text).await;
                Some(receipt)
            }
            Err(error) => {
                warn!(error = %error, "failed to send channel stream message");
                None
            }
        }
    }

    async fn register_message_route(&self, receipt: &ChannelDeliveryReceipt, preview_text: Option<&str>) {
        let Some(session_manager) = &self.session_manager else {
            return;
        };
        let platform_type = self.config.platform.to_string();
        if let Err(error) = session_manager
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
            warn!(error = %error, "failed to register channel reply route");
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
