use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aionui_api_types::WebSocketMessage;
use aionui_db::{IChannelRepository, IConversationRepository, UpsertChannelConversationRouteParams};
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::action::{ActionExecutor, MessageResult};
use crate::formatter::format_text_for_platform;
use crate::message_service::ChannelMessageService;
use crate::session::SessionManager;
use crate::stream_relay::{
    ChannelSender, ChannelStreamRelay, RelayConfig, format_remote_reply, throttle_ms_for_platform,
};
use crate::types::{ActionBehavior, OutgoingMessageType, UnifiedIncomingMessage, UnifiedOutgoingMessage};

/// Orchestrates the full channel message lifecycle.
///
/// Consumes incoming IM messages from `message_rx` and tool confirmation
/// callbacks from `confirm_rx`, driving the pipeline:
/// 1. ActionExecutor routing (auth → action/AI dispatch)
/// 2. For Dispatched: send_to_agent + spawn ChannelStreamRelay
/// 3. For Action: reply via plugin
/// 4. Forward tool confirmations to the agent
pub struct ChannelOrchestrator {
    action_executor: Arc<ActionExecutor>,
    message_service: Arc<ChannelMessageService>,
    session_manager: Arc<SessionManager>,
    channel_repo: Arc<dyn IChannelRepository>,
    sender: Arc<dyn ChannelSender>,
    mirror_service: Arc<ChannelMirrorService>,
    conversation_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

struct IncomingMessageClaim {
    repository: Arc<dyn IChannelRepository>,
    owner_user_id: String,
    platform_type: String,
    chat_id: String,
    message_id: String,
    claim_token: String,
    renewal_task: Option<tokio::task::JoinHandle<()>>,
    completed: bool,
}

impl IncomingMessageClaim {
    fn start_renewal(mut self) -> Self {
        let repository = Arc::clone(&self.repository);
        let owner_user_id = self.owner_user_id.clone();
        let platform_type = self.platform_type.clone();
        let chat_id = self.chat_id.clone();
        let message_id = self.message_id.clone();
        let claim_token = self.claim_token.clone();
        self.renewal_task = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await;
            loop {
                interval.tick().await;
                match repository
                    .renew_incoming_message(&owner_user_id, &platform_type, &chat_id, &message_id, &claim_token)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(message_id = %message_id, "channel message claim was replaced while processing");
                        break;
                    }
                    Err(error) => {
                        warn!(message_id = %message_id, error = %error, "failed to renew channel message claim");
                        break;
                    }
                }
            }
        }));
        self
    }

    async fn complete(mut self) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
        let result = self
            .repository
            .complete_incoming_message(
                &self.owner_user_id,
                &self.platform_type,
                &self.chat_id,
                &self.message_id,
                &self.claim_token,
            )
            .await;
        match result {
            Ok(true) => self.completed = true,
            Ok(false) => {
                warn!(
                    message_id = %self.message_id,
                    "channel message claim was replaced before completion"
                );
                self.completed = true;
            }
            Err(error) => {
                warn!(
                    message_id = %self.message_id,
                    error = %error,
                    "failed to complete channel message claim; releasing it"
                );
                let release_reason = format!("failed to complete channel message claim: {error}");
                self.release(Some(&release_reason)).await;
            }
        }
    }

    async fn release(mut self, error: Option<&str>) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
        match self
            .repository
            .release_incoming_message(
                &self.owner_user_id,
                &self.platform_type,
                &self.chat_id,
                &self.message_id,
                &self.claim_token,
                error,
            )
            .await
        {
            Ok(true) => self.completed = true,
            Ok(false) => {
                warn!(
                    message_id = %self.message_id,
                    "channel message claim was already replaced before release"
                );
                self.completed = true;
            }
            Err(release_error) => {
                warn!(
                    message_id = %self.message_id,
                    error = %release_error,
                    "failed to release channel message claim"
                );
            }
        }
    }
}

impl Drop for IncomingMessageClaim {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let repository = Arc::clone(&self.repository);
        let owner_user_id = self.owner_user_id.clone();
        let platform_type = self.platform_type.clone();
        let chat_id = self.chat_id.clone();
        let message_id = self.message_id.clone();
        let claim_token = self.claim_token.clone();
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
        tokio::spawn(async move {
            match repository
                .release_incoming_message(
                    &owner_user_id,
                    &platform_type,
                    &chat_id,
                    &message_id,
                    &claim_token,
                    Some("channel message processing did not complete"),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    warn!(message_id = %message_id, "channel message claim was already replaced before release")
                }
                Err(error) => {
                    warn!(message_id = %message_id, error = %error, "failed to release channel message claim")
                }
            }
        });
    }
}

async fn settle_incoming_claim(claim: IncomingMessageClaim, result: Result<(), String>) {
    match result {
        Ok(()) => claim.complete().await,
        Err(error) => claim.release(Some(&error)).await,
    }
}

impl ChannelOrchestrator {
    pub fn new(
        action_executor: Arc<ActionExecutor>,
        message_service: Arc<ChannelMessageService>,
        session_manager: Arc<SessionManager>,
        channel_repo: Arc<dyn IChannelRepository>,
        sender: Arc<dyn ChannelSender>,
        mirror_service: Arc<ChannelMirrorService>,
    ) -> Self {
        Self {
            action_executor,
            message_service,
            session_manager,
            channel_repo,
            sender,
            mirror_service,
            conversation_locks: Arc::new(DashMap::new()),
        }
    }

    /// Start the message loop. Runs until both channels close.
    pub async fn run(
        self,
        mut message_rx: mpsc::Receiver<UnifiedIncomingMessage>,
        mut confirm_rx: mpsc::Receiver<(String, String)>,
    ) {
        info!("ChannelOrchestrator started");

        loop {
            tokio::select! {
                Some(msg) = message_rx.recv() => {
                    self.handle_message(msg).await;
                }
                Some((call_id, value)) = confirm_rx.recv() => {
                    handle_confirm(&call_id, &value);
                }
                else => break,
            }
        }

        info!("ChannelOrchestrator stopped (channels closed)");
    }

    async fn handle_message(&self, msg: UnifiedIncomingMessage) {
        let platform = msg.platform;
        let chat_id = msg.chat_id.clone();
        let plugin_id = platform.to_string();
        let text = msg.content.text.clone();
        let platform_user_id = msg.user.id.clone();
        let message_owner_user_id = msg
            .owner_user_id
            .clone()
            .or_else(|| self.action_executor.owner_user_id().map(ToOwned::to_owned));

        let executor = Arc::clone(&self.action_executor);
        let msg_svc = Arc::clone(&self.message_service);
        let session_mgr = Arc::clone(&self.session_manager);
        let sender = Arc::clone(&self.sender);
        let channel_repo = Arc::clone(&self.channel_repo);
        let mirror_service = Arc::clone(&self.mirror_service);
        let conversation_locks = Arc::clone(&self.conversation_locks);

        tokio::spawn(async move {
            let Some(owner_user_id) = message_owner_user_id.as_deref() else {
                error!(message_id = %msg.id, "dropping channel message without owner user");
                return;
            };
            if msg.id.trim().is_empty() {
                error!(platform = %platform, chat_id = %chat_id, "dropping channel message without stable message id");
                return;
            }
            let claim_token = match channel_repo
                .try_claim_incoming_message(owner_user_id, &plugin_id, &chat_id, &msg.id)
                .await
            {
                Ok(Some(claim_token)) => claim_token,
                Ok(None) => {
                    info!(
                        platform = %platform,
                        chat_id = %chat_id,
                        message_id = %msg.id,
                        "channel message ignored: already claimed"
                    );
                    return;
                }
                Err(error) => {
                    error!(
                        platform = %platform,
                        chat_id = %chat_id,
                        message_id = %msg.id,
                        error = %error,
                        "dropping channel message because durable idempotency claim failed"
                    );
                    return;
                }
            };

            let claim = IncomingMessageClaim {
                repository: Arc::clone(&channel_repo),
                owner_user_id: owner_user_id.to_owned(),
                platform_type: plugin_id.clone(),
                chat_id: chat_id.clone(),
                message_id: msg.id.clone(),
                claim_token,
                renewal_task: None,
                completed: false,
            }
            .start_renewal();

            match executor.handle_incoming_message(&msg).await {
                Ok(MessageResult::Action(response)) => {
                    if let Some(owner_user_id) = message_owner_user_id.as_deref()
                        && let Err(error) =
                            send_action_response(&sender, owner_user_id, &plugin_id, &chat_id, &response).await
                    {
                        warn!(
                            message_id = %msg.id,
                            error = %error,
                            "failed to deliver channel action response"
                        );
                        settle_incoming_claim(claim, Err(error.to_string())).await;
                        return;
                    }
                }
                Ok(MessageResult::RoutedAction {
                    response,
                    session_id,
                    conversation_id,
                    follow_up_text,
                }) => {
                    if let Some(owner_user_id) = message_owner_user_id.as_deref() {
                        let session = match session_mgr.get_session_by_id(owner_user_id, &session_id).await {
                            Ok(Some(session)) => session,
                            Ok(None) => {
                                warn!(session_id = %session_id, "routed channel session not found");
                                settle_incoming_claim(claim, Err("routed channel session not found".to_owned())).await;
                                return;
                            }
                            Err(error) => {
                                warn!(session_id = %session_id, error = %error, "failed to load routed channel session");
                                settle_incoming_claim(claim, Err(error.to_string())).await;
                                return;
                            }
                        };
                        let conversation_id = match conversation_id {
                            Some(conversation_id) => conversation_id,
                            None => match msg_svc.ensure_conversation(owner_user_id, &session, platform).await {
                                Ok(conversation_id) => {
                                    if let Err(error) = session_mgr
                                        .bind_conversation(owner_user_id, &session_id, &conversation_id)
                                        .await
                                    {
                                        warn!(error = %error, "failed to bind new channel conversation");
                                        settle_incoming_claim(claim, Err(error.to_string())).await;
                                        return;
                                    }
                                    conversation_id
                                }
                                Err(error) => {
                                    warn!(error = %error, "failed to create new channel conversation");
                                    settle_incoming_claim(claim, Err(error.to_string())).await;
                                    return;
                                }
                            },
                        };
                        if msg
                            .action
                            .as_ref()
                            .is_some_and(|action| action.action == "system.confirm")
                        {
                            let Some(action) = msg.action.as_ref() else {
                                unreachable!("system.confirm action was checked above");
                            };
                            let Some(call_id) = action
                                .params
                                .as_ref()
                                .and_then(|params| params.get("callId"))
                                .filter(|call_id| !call_id.trim().is_empty())
                            else {
                                warn!(message_id = %msg.id, "dropping tool confirmation without call_id");
                                settle_incoming_claim(claim, Err("tool confirmation is missing call_id".to_owned()))
                                    .await;
                                return;
                            };
                            let value = action
                                .params
                                .as_ref()
                                .and_then(|params| params.get("value"))
                                .map(String::as_str)
                                .unwrap_or("allow");
                            if let Err(error) = msg_svc.confirm(owner_user_id, &conversation_id, call_id, value).await {
                                warn!(
                                    message_id = %msg.id,
                                    call_id,
                                    error = %error,
                                    "failed to deliver channel tool confirmation"
                                );
                                settle_incoming_claim(claim, Err(error.to_string())).await;
                                return;
                            }
                        }
                        if let Some(follow_up_text) = follow_up_text {
                            if let Err(error) = handle_dispatched(
                                &msg_svc,
                                &session_mgr,
                                &sender,
                                owner_user_id,
                                &session_id,
                                Some(&conversation_id),
                                &follow_up_text,
                                platform,
                                &plugin_id,
                                &chat_id,
                                &platform_user_id,
                                &mirror_service,
                                &conversation_locks,
                            )
                            .await
                            {
                                warn!(message_id = %msg.id, error = %error, "failed to dispatch routed channel message");
                                settle_incoming_claim(claim, Err(error)).await;
                                return;
                            }
                        } else {
                            let receipt =
                                match send_action_response(&sender, owner_user_id, &plugin_id, &chat_id, &response)
                                    .await
                                {
                                    Ok(receipt) => receipt,
                                    Err(error) => {
                                        warn!(
                                            message_id = %msg.id,
                                            error = %error,
                                            "failed to deliver routed channel action response"
                                        );
                                        settle_incoming_claim(claim, Err(error.to_string())).await;
                                        return;
                                    }
                                };
                            if let Some(receipt) = receipt
                                && let Err(error) = session_mgr
                                    .register_conversation_route(
                                        owner_user_id,
                                        &aionui_db::UpsertChannelConversationRouteParams {
                                            conversation_id: &conversation_id,
                                            platform_type: &platform.to_string(),
                                            chat_id: &chat_id,
                                            message_id: &receipt.message_id,
                                            preview_text: response.text.as_deref(),
                                            sent_at: receipt.completed_at,
                                            delivery_started_at: Some(receipt.started_at),
                                            delivery_completed_at: Some(receipt.completed_at),
                                        },
                                    )
                                    .await
                            {
                                warn!(error = %error, "failed to register channel action conversation route");
                            }
                        }
                    } else {
                        warn!("dropping routed channel action response without owner user");
                    }
                }
                Ok(MessageResult::Dispatched {
                    owner_user_id,
                    session_id,
                    conversation_id,
                }) => {
                    if let Err(error) = handle_dispatched(
                        &msg_svc,
                        &session_mgr,
                        &sender,
                        &owner_user_id,
                        &session_id,
                        conversation_id.as_deref(),
                        &text,
                        platform,
                        &plugin_id,
                        &chat_id,
                        &platform_user_id,
                        &mirror_service,
                        &conversation_locks,
                    )
                    .await
                    {
                        warn!(message_id = %msg.id, error = %error, "failed to dispatch channel message");
                        settle_incoming_claim(claim, Err(error)).await;
                        return;
                    }
                }
                Ok(MessageResult::AlreadyProcessing) => {
                    info!(chat_id = %chat_id, "message ignored: already processing");
                }
                Err(e) => {
                    error!(error = %e, "failed to handle incoming message");
                    settle_incoming_claim(claim, Err(e.to_string())).await;
                    return;
                }
            }
            settle_incoming_claim(claim, Ok(())).await;
        });
    }
}

async fn send_action_response(
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    plugin_id: &str,
    chat_id: &str,
    response: &crate::types::ActionResponse,
) -> Result<Option<crate::stream_relay::ChannelDeliveryReceipt>, crate::error::ChannelError> {
    if let Some(text) = &response.text {
        let platform = crate::types::PluginType::from_str_opt(plugin_id);
        let formatted_text = platform.map_or_else(|| text.clone(), |platform| format_text_for_platform(text, platform));
        let outgoing = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Text,
            text: Some(formatted_text),
            parse_mode: platform.and(response.parse_mode),
            buttons: response.buttons.clone(),
            keyboard: response.keyboard.clone(),
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };
        let started_at = aionui_common::now_ms();

        match response.behavior {
            ActionBehavior::Edit => {
                let Some(edit_id) = response.edit_message_id.as_ref() else {
                    return Err(crate::error::ChannelError::MessageSendFailed(
                        "edit response is missing its target message id".into(),
                    ));
                };
                sender
                    .edit_message(owner_user_id, plugin_id, chat_id, edit_id, outgoing)
                    .await?;
                Ok(Some(crate::stream_relay::ChannelDeliveryReceipt {
                    message_id: edit_id.clone(),
                    started_at,
                    completed_at: aionui_common::now_ms(),
                }))
            }
            _ => sender
                .send_message(owner_user_id, plugin_id, chat_id, outgoing)
                .await
                .map(Some),
        }
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_dispatched(
    msg_svc: &Arc<ChannelMessageService>,
    session_mgr: &Arc<SessionManager>,
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    session_id: &str,
    conversation_id: Option<&str>,
    text: &str,
    platform: crate::types::PluginType,
    plugin_id: &str,
    chat_id: &str,
    platform_user_id: &str,
    mirror_service: &Arc<ChannelMirrorService>,
    conversation_locks: &Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
) -> Result<(), String> {
    let session = match session_mgr.get_session_by_id(owner_user_id, session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            warn!(session_id = %session_id, "session not found after dispatch");
            return Err(format!("session {session_id} not found after dispatch"));
        }
        Err(e) => {
            error!(error = %e, "failed to get session");
            return Err(e.to_string());
        }
    };

    let route_conversation_id = conversation_id
        .map(ToOwned::to_owned)
        .or_else(|| session.conversation_id.clone());
    let lock_key = format!(
        "{owner_user_id}:{}",
        route_conversation_id.as_deref().unwrap_or(session_id)
    );
    let conversation_lock = conversation_locks
        .entry(lock_key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _conversation_guard = conversation_lock.lock().await;

    if let Some(conversation_id) = conversation_id {
        mirror_service.mark_channel_turn(
            owner_user_id,
            conversation_id,
            text,
            ChannelMirrorTargetRef {
                platform,
                chat_id: chat_id.to_owned(),
                platform_user_id: Some(platform_user_id.to_owned()),
            },
        );
    }

    let send_result = match msg_svc.send_to_agent(owner_user_id, &session, text, platform).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to send to agent");
            let err_msg = UnifiedOutgoingMessage {
                message_type: OutgoingMessageType::Text,
                text: Some(format!("\u{274c} 处理失败：{e}")),
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
            let receipt = sender
                .send_message(owner_user_id, plugin_id, chat_id, err_msg)
                .await
                .map_err(|send_error| {
                    format!("agent dispatch failed: {e}; failed to deliver error response: {send_error}")
                })?;
            let route_result = if let Some(conversation_id) = route_conversation_id.as_deref() {
                session_mgr
                    .register_conversation_route(
                        owner_user_id,
                        &aionui_db::UpsertChannelConversationRouteParams {
                            conversation_id,
                            platform_type: &platform.to_string(),
                            chat_id,
                            message_id: &receipt.message_id,
                            preview_text: Some("❌ 处理失败"),
                            sent_at: receipt.completed_at,
                            delivery_started_at: Some(receipt.started_at),
                            delivery_completed_at: Some(receipt.completed_at),
                        },
                    )
                    .await
            } else {
                session_mgr
                    .register_reply_route(
                        owner_user_id,
                        session_id,
                        &platform.to_string(),
                        chat_id,
                        &receipt.message_id,
                    )
                    .await
            };
            if let Err(route_error) = route_result {
                warn!(error = %route_error, "failed to register channel error reply route");
            }
            return Ok(());
        }
    };

    // Bind conversation to session if newly created
    if conversation_id.is_none()
        && let Err(e) = session_mgr
            .bind_conversation(owner_user_id, session_id, &send_result.conversation_id)
            .await
    {
        warn!(error = %e, "failed to bind conversation to session");
    }

    // Spawn stream relay if we got a subscription
    if let Some(rx) = send_result.stream_rx {
        let relay_config = RelayConfig {
            owner_user_id: owner_user_id.to_owned(),
            session_id: session_id.to_owned(),
            conversation_id: send_result.conversation_id.clone(),
            platform,
            plugin_id: plugin_id.to_owned(),
            chat_id: chat_id.to_owned(),
            prompt_text: text.to_owned(),
            throttle_ms: throttle_ms_for_platform(platform),
        };
        let relay = ChannelStreamRelay::with_session_manager(relay_config, Arc::clone(sender), Arc::clone(session_mgr))
            .with_runtime_state(msg_svc.runtime_state());
        relay.run(rx).await;
    } else {
        warn!(
            conversation_id = %send_result.conversation_id,
            "no agent task for stream subscription"
        );
        return Err(format!(
            "agent task missing stream subscription for conversation {}",
            send_result.conversation_id
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ChannelMirrorTargetRef {
    pub platform: crate::types::PluginType,
    pub chat_id: String,
    pub platform_user_id: Option<String>,
}

#[derive(Debug, Default)]
struct MirrorTextSegment {
    msg_id: String,
    text: String,
}

#[derive(Debug, Default)]
struct MirrorTurnState {
    owner_user_id: String,
    latest_user_text: String,
    assistant_text: String,
    assistant_segments: Vec<MirrorTextSegment>,
    excluded_target: Option<ChannelMirrorTargetRef>,
}

/// Mirrors completed desktop conversations to every authorized channel and
/// records each delivered message as a future quoted-reply target.
pub struct ChannelMirrorService {
    channel_repo: Arc<dyn IChannelRepository>,
    conversation_repo: Arc<dyn IConversationRepository>,
    sender: Arc<dyn ChannelSender>,
    turns: Mutex<HashMap<String, MirrorTurnState>>,
}

impl ChannelMirrorService {
    pub fn new(
        channel_repo: Arc<dyn IChannelRepository>,
        conversation_repo: Arc<dyn IConversationRepository>,
        sender: Arc<dyn ChannelSender>,
    ) -> Self {
        Self {
            channel_repo,
            conversation_repo,
            sender,
            turns: Mutex::new(HashMap::new()),
        }
    }

    pub fn mark_channel_turn(
        &self,
        owner_user_id: &str,
        conversation_id: &str,
        text: &str,
        excluded_target: ChannelMirrorTargetRef,
    ) {
        let mut turns = self.turns.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        turns.insert(
            conversation_id.to_owned(),
            MirrorTurnState {
                owner_user_id: owner_user_id.to_owned(),
                latest_user_text: normalize_mirror_text(text),
                assistant_text: String::new(),
                assistant_segments: Vec::new(),
                excluded_target: Some(excluded_target),
            },
        );
    }

    pub async fn run(self: Arc<Self>, mut event_rx: broadcast::Receiver<WebSocketMessage<serde_json::Value>>) {
        loop {
            match event_rx.recv().await {
                Ok(event) => self.handle_event(event).await,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "channel mirror event bus lagged; dropping partial mirror state"
                    );
                    self.turns
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clear();
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn handle_event(&self, event: WebSocketMessage<serde_json::Value>) {
        let conversation_id = event.data.get("conversation_id").and_then(serde_json::Value::as_str);
        let Some(conversation_id) = conversation_id else {
            return;
        };

        if event.name == "message.userCreated" {
            if event.data.get("hidden").and_then(serde_json::Value::as_bool) == Some(true) {
                return;
            }
            // Desktop-created conversations do not always include `user_id` in
            // realtime payloads. Resolve ownership only when a new mirror turn
            // is created; stream chunks must not issue one query per token.
            let owner_user_id = match event.data.get("user_id").and_then(serde_json::Value::as_str) {
                Some(owner_user_id) if !owner_user_id.is_empty() => owner_user_id.to_owned(),
                _ => match self.conversation_repo.owner_user_id(conversation_id).await {
                    Ok(Some(owner_user_id)) => owner_user_id,
                    Ok(None) => return,
                    Err(error) => {
                        warn!(conversation_id = %conversation_id, error = %error, "failed to resolve conversation owner for mirror");
                        return;
                    }
                },
            };
            let text = event
                .data
                .get("content")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let mut turns = self.turns.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous_exclusion = turns
                .get(conversation_id)
                .and_then(|state| state.excluded_target.clone());
            turns.insert(
                conversation_id.to_owned(),
                MirrorTurnState {
                    owner_user_id: owner_user_id.to_owned(),
                    latest_user_text: normalize_mirror_text(text),
                    assistant_text: String::new(),
                    assistant_segments: Vec::new(),
                    excluded_target: previous_exclusion,
                },
            );
            return;
        }

        if event.name == "message.stream" {
            match event.data.get("type").and_then(serde_json::Value::as_str) {
                Some("text" | "content") => {
                    let chunk = event
                        .data
                        .get("data")
                        .and_then(|data| data.get("content"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let replace = event.data.get("replace").and_then(serde_json::Value::as_bool) == Some(true);
                    let msg_id = event
                        .data
                        .get("msg_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let mut turns = self.turns.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(state) = turns.get_mut(conversation_id) {
                        let segment_index = state
                            .assistant_segments
                            .iter()
                            .position(|segment| segment.msg_id == msg_id)
                            .or_else(|| {
                                (replace && !msg_id.is_empty() && state.assistant_segments.len() == 1).then_some(0)
                            });
                        if let Some(index) = segment_index {
                            if replace {
                                state.assistant_segments[index].text = chunk.to_owned();
                            } else {
                                state.assistant_segments[index].text.push_str(chunk);
                            }
                        } else {
                            state.assistant_segments.push(MirrorTextSegment {
                                msg_id: msg_id.to_owned(),
                                text: chunk.to_owned(),
                            });
                        }
                        state.assistant_text = state
                            .assistant_segments
                            .iter()
                            .map(|segment| segment.text.as_str())
                            .collect();
                    }
                }
                // The conversation relay can emit a final `content` replacement
                // after `finish`. Wait for `turn.completed` so the mirrored text
                // matches the final desktop transcript.
                Some("finish") => {}
                Some("error") => {
                    self.turns
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(conversation_id);
                }
                _ => {}
            }
            return;
        }

        if event.name == "turn.completed" {
            let state = self
                .turns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(conversation_id);
            if let Some(state) = state {
                self.mirror_completed_turn(conversation_id, state).await;
            }
        }
    }

    async fn mirror_completed_turn(&self, conversation_id: &str, state: MirrorTurnState) {
        let answer = normalize_mirror_text(&state.assistant_text);
        if answer.is_empty() {
            return;
        }
        let exists = self
            .conversation_repo
            .get(&state.owner_user_id, conversation_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !exists {
            return;
        }

        let plugins = match self.channel_repo.get_all_plugins(&state.owner_user_id).await {
            Ok(plugins) => plugins,
            Err(error) => {
                warn!(error = %error, "failed to list channel plugins for conversation mirror");
                return;
            }
        };
        let users = match self.channel_repo.get_all_users(&state.owner_user_id).await {
            Ok(users) => users,
            Err(error) => {
                warn!(error = %error, "failed to list channel users for conversation mirror");
                return;
            }
        };
        let plugin_by_type: HashMap<String, String> = plugins
            .into_iter()
            .filter(|plugin| plugin.enabled && plugin.status.as_deref() == Some("running"))
            .map(|plugin| (plugin.r#type, plugin.id))
            .collect();

        let mut delivered = std::collections::HashSet::new();
        for user in users {
            let Some(platform) = crate::types::PluginType::from_str_opt(&user.platform_type) else {
                continue;
            };
            if !matches!(
                platform,
                crate::types::PluginType::Lark | crate::types::PluginType::Weixin
            ) {
                continue;
            }
            if is_excluded_mirror_target(platform, &user.platform_user_id, state.excluded_target.as_ref()) {
                continue;
            }
            let Some(plugin_id) = plugin_by_type.get(&user.platform_type) else {
                continue;
            };
            if !delivered.insert((plugin_id.clone(), user.platform_user_id.clone())) {
                continue;
            }
            let mirror_text = format_remote_reply(&state.latest_user_text, &answer, platform);
            let outgoing = UnifiedOutgoingMessage {
                message_type: OutgoingMessageType::Text,
                text: Some(mirror_text.clone()),
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
            match self
                .sender
                .send_message(&state.owner_user_id, plugin_id, &user.platform_user_id, outgoing)
                .await
            {
                Ok(receipt) => {
                    let route_chat_id = mirror_route_chat_id(platform, &user.platform_user_id);
                    let route = UpsertChannelConversationRouteParams {
                        conversation_id,
                        platform_type: &user.platform_type,
                        chat_id: &route_chat_id,
                        message_id: &receipt.message_id,
                        preview_text: Some(&mirror_text),
                        sent_at: receipt.completed_at,
                        delivery_started_at: Some(receipt.started_at),
                        delivery_completed_at: Some(receipt.completed_at),
                    };
                    if let Err(error) = self
                        .channel_repo
                        .upsert_conversation_route(&state.owner_user_id, &route)
                        .await
                    {
                        warn!(error = %error, "failed to register mirrored conversation route");
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        platform = %user.platform_type,
                        "failed to mirror completed conversation to channel"
                    );
                }
            }
        }
    }
}

fn normalize_mirror_text(text: &str) -> String {
    text.replace("\r\n", "\n").trim().to_owned()
}

fn mirror_route_chat_id(platform: crate::types::PluginType, platform_user_id: &str) -> String {
    if platform == crate::types::PluginType::Dingtalk {
        format!("user:{platform_user_id}")
    } else {
        platform_user_id.to_owned()
    }
}

fn is_excluded_mirror_target(
    platform: crate::types::PluginType,
    receive_id: &str,
    excluded_target: Option<&ChannelMirrorTargetRef>,
) -> bool {
    excluded_target.is_some_and(|target| {
        target.platform == platform
            && (target.chat_id == receive_id || target.platform_user_id.as_deref() == Some(receive_id))
    })
}

/// Forward a tool confirmation callback to the active agent.
fn handle_confirm(call_id: &str, value: &str) {
    // Channel conversations use yoloMode which auto-approves everything,
    // so this path is rarely hit. When needed, we can add a
    // call_id→conversation_id lookup via IWorkerTaskManager.
    info!(call_id = %call_id, value = %value, "forwarding tool confirmation");
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_common::now_ms;
    use aionui_db::models::{AssistantUserRow, ChannelPluginRow, ConversationRow};
    use aionui_db::{
        IConversationRepository, SqliteChannelRepository, SqliteConversationRepository, init_database_memory,
    };

    #[derive(Default)]
    struct RecordingSender {
        sends: Mutex<Vec<(String, String, String)>>,
    }

    #[async_trait::async_trait]
    impl ChannelSender for RecordingSender {
        async fn send_message(
            &self,
            _owner_user_id: &str,
            plugin_id: &str,
            chat_id: &str,
            message: UnifiedOutgoingMessage,
        ) -> Result<crate::stream_relay::ChannelDeliveryReceipt, crate::error::ChannelError> {
            self.sends.lock().unwrap().push((
                plugin_id.to_owned(),
                chat_id.to_owned(),
                message.text.unwrap_or_default(),
            ));
            Ok(crate::stream_relay::ChannelDeliveryReceipt {
                message_id: format!("{plugin_id}-{chat_id}"),
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
            _message: UnifiedOutgoingMessage,
        ) -> Result<(), crate::error::ChannelError> {
            Ok(())
        }
    }

    async fn setup_mirror() -> (
        Arc<ChannelMirrorService>,
        Arc<RecordingSender>,
        Arc<dyn IChannelRepository>,
        aionui_db::Database,
    ) {
        let db = init_database_memory().await.unwrap();
        let channel_repo: Arc<dyn IChannelRepository> = Arc::new(SqliteChannelRepository::new(db.pool().clone()));
        let conversation_repo: Arc<dyn IConversationRepository> =
            Arc::new(SqliteConversationRepository::new(db.pool().clone()));
        let now = now_ms();
        conversation_repo
            .create(&ConversationRow {
                id: "desktop-conversation".to_owned(),
                user_id: "system_default_user".to_owned(),
                name: "Desktop conversation".to_owned(),
                r#type: "acp".to_owned(),
                extra: "{}".to_owned(),
                model: None,
                status: Some("pending".to_owned()),
                source: Some("aionui".to_owned()),
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now,
                updated_at: now,
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();
        for platform in ["lark", "weixin"] {
            channel_repo
                .upsert_plugin(
                    "system_default_user",
                    &ChannelPluginRow {
                        id: platform.to_owned(),
                        owner_user_id: "system_default_user".to_owned(),
                        r#type: platform.to_owned(),
                        name: platform.to_owned(),
                        enabled: true,
                        config: "encrypted".to_owned(),
                        status: Some("running".to_owned()),
                        last_connected: Some(now),
                        created_at: now,
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            channel_repo
                .create_user(
                    "system_default_user",
                    &AssistantUserRow {
                        id: format!("{platform}-internal"),
                        owner_user_id: "system_default_user".to_owned(),
                        platform_user_id: format!("{platform}-user"),
                        platform_type: platform.to_owned(),
                        display_name: None,
                        authorized_at: now,
                        last_active: None,
                        session_id: None,
                    },
                )
                .await
                .unwrap();
        }
        let sender = Arc::new(RecordingSender::default());
        let service = Arc::new(ChannelMirrorService::new(
            Arc::clone(&channel_repo),
            conversation_repo,
            sender.clone() as Arc<dyn ChannelSender>,
        ));
        (service, sender, channel_repo, db)
    }

    #[tokio::test]
    async fn failed_incoming_claim_is_released_for_retry() {
        let db = init_database_memory().await.unwrap();
        let repository: Arc<dyn IChannelRepository> = Arc::new(SqliteChannelRepository::new(db.pool().clone()));
        let claim_token = repository
            .try_claim_incoming_message("owner", "lark", "chat", "message")
            .await
            .unwrap()
            .expect("first delivery should claim the message");
        let claim = IncomingMessageClaim {
            repository: Arc::clone(&repository),
            owner_user_id: "owner".to_owned(),
            platform_type: "lark".to_owned(),
            chat_id: "chat".to_owned(),
            message_id: "message".to_owned(),
            claim_token,
            renewal_task: None,
            completed: false,
        };

        settle_incoming_claim(claim, Err("agent failed".to_owned())).await;

        assert!(
            repository
                .try_claim_incoming_message("owner", "lark", "chat", "message")
                .await
                .unwrap()
                .is_some(),
            "a failed delivery must remain retryable"
        );
    }

    fn user_event(content: &str) -> WebSocketMessage<serde_json::Value> {
        WebSocketMessage::new(
            "message.userCreated",
            serde_json::json!({
                "user_id": "system_default_user",
                "conversation_id": "desktop-conversation",
                "content": content,
                "hidden": false,
            }),
        )
    }

    fn desktop_event_without_owner(name: &str, data: serde_json::Value) -> WebSocketMessage<serde_json::Value> {
        WebSocketMessage::new(name, data)
    }

    fn stream_event(kind: &str, content: Option<&str>) -> WebSocketMessage<serde_json::Value> {
        WebSocketMessage::new(
            "message.stream",
            serde_json::json!({
                "user_id": "system_default_user",
                "conversation_id": "desktop-conversation",
                "type": kind,
                "data": content.map_or_else(|| serde_json::json!({}), |text| serde_json::json!({ "content": text })),
            }),
        )
    }

    fn completed_event() -> WebSocketMessage<serde_json::Value> {
        WebSocketMessage::new(
            "turn.completed",
            serde_json::json!({
                "user_id": "system_default_user",
                "conversation_id": "desktop-conversation",
                "status": "finished",
            }),
        )
    }

    #[tokio::test]
    async fn desktop_completion_is_mirrored_and_each_delivery_routes_back_to_the_conversation() {
        let (service, sender, repo, _db) = setup_mirror().await;

        service.handle_event(user_event("Question")).await;
        service.handle_event(stream_event("text", Some("Answer"))).await;
        service.handle_event(stream_event("finish", None)).await;
        service.handle_event(completed_event()).await;

        let sends = sender.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 2);
        assert!(
            sends
                .iter()
                .all(|(_, _, text)| text == "**你：** Question\n\n**AI：** Answer")
        );
        assert_eq!(
            repo.resolve_conversation_route("system_default_user", "lark", "lark-user", "lark-lark-user",)
                .await
                .unwrap()
                .as_deref(),
            Some("desktop-conversation")
        );
        assert_eq!(
            repo.resolve_conversation_route("system_default_user", "weixin", "weixin-user", "weixin-weixin-user",)
                .await
                .unwrap()
                .as_deref(),
            Some("desktop-conversation")
        );
    }

    #[tokio::test]
    async fn desktop_completion_without_realtime_owner_is_mirrored_using_conversation_owner() {
        let (service, sender, _repo, _db) = setup_mirror().await;

        service
            .handle_event(desktop_event_without_owner(
                "message.userCreated",
                serde_json::json!({
                    "conversation_id": "desktop-conversation",
                    "content": "Question",
                    "hidden": false,
                }),
            ))
            .await;
        service
            .handle_event(desktop_event_without_owner(
                "message.stream",
                serde_json::json!({
                    "conversation_id": "desktop-conversation",
                    "type": "text",
                    "data": { "content": "Answer" },
                }),
            ))
            .await;
        service
            .handle_event(desktop_event_without_owner(
                "turn.completed",
                serde_json::json!({ "conversation_id": "desktop-conversation" }),
            ))
            .await;

        assert_eq!(sender.sends.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn channel_completion_is_not_mirrored_back_to_its_source_target() {
        let (service, sender, _repo, _db) = setup_mirror().await;
        service.mark_channel_turn(
            "system_default_user",
            "desktop-conversation",
            "Question",
            ChannelMirrorTargetRef {
                platform: crate::types::PluginType::Lark,
                chat_id: "lark-chat".to_owned(),
                platform_user_id: Some("lark-user".to_owned()),
            },
        );

        service.handle_event(user_event("Question")).await;
        service.handle_event(stream_event("text", Some("Answer"))).await;
        service.handle_event(stream_event("finish", None)).await;
        service.handle_event(completed_event()).await;

        let sends = sender.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, "weixin");
    }

    #[tokio::test]
    async fn final_content_replacement_is_mirrored_after_turn_completion() {
        let (service, sender, _repo, _db) = setup_mirror().await;

        service.handle_event(user_event("Question")).await;
        service.handle_event(stream_event("text", Some("Draft"))).await;
        service.handle_event(stream_event("finish", None)).await;
        assert!(sender.sends.lock().unwrap().is_empty());

        service
            .handle_event(WebSocketMessage::new(
                "message.stream",
                serde_json::json!({
                    "user_id": "system_default_user",
                    "conversation_id": "desktop-conversation",
                    "type": "content",
                    "data": { "content": "Final answer" },
                    "replace": true,
                }),
            ))
            .await;
        service.handle_event(completed_event()).await;

        let sends = sender.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 2);
        assert!(
            sends
                .iter()
                .all(|(_, _, text)| text == "**你：** Question\n\n**AI：** Final answer")
        );
    }

    #[tokio::test]
    async fn multi_segment_final_replacements_keep_the_complete_mirrored_answer() {
        let (service, sender, _repo, _db) = setup_mirror().await;

        service.handle_event(user_event("Question")).await;
        for (msg_id, text) in [("segment-a", "Draft A"), ("segment-b", "Draft B")] {
            service
                .handle_event(WebSocketMessage::new(
                    "message.stream",
                    serde_json::json!({
                        "user_id": "system_default_user",
                        "conversation_id": "desktop-conversation",
                        "msg_id": msg_id,
                        "type": "text",
                        "data": { "content": text },
                    }),
                ))
                .await;
        }
        service.handle_event(stream_event("finish", None)).await;
        for (msg_id, text) in [("segment-a", "Final A"), ("segment-b", "Final B")] {
            service
                .handle_event(WebSocketMessage::new(
                    "message.stream",
                    serde_json::json!({
                        "user_id": "system_default_user",
                        "conversation_id": "desktop-conversation",
                        "msg_id": msg_id,
                        "type": "content",
                        "data": { "content": text },
                        "replace": true,
                    }),
                ))
                .await;
        }
        service.handle_event(completed_event()).await;

        let sends = sender.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 2);
        assert!(
            sends
                .iter()
                .all(|(_, _, text)| text == "**你：** Question\n\n**AI：** Final AFinal B")
        );
    }
}
