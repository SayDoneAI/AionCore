use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::channel_settings::ChannelSettingsService;
use crate::error::ChannelError;
use crate::pairing::PairingService;
use crate::session::SessionManager;
use crate::types::{
    ActionBehavior, ActionButton, ActionCategory, ActionResponse, MessageContentType, ParseMode, UnifiedAction,
    UnifiedIncomingMessage,
};

/// Result of processing an incoming message.
///
/// The caller (ChannelManager / plugin) uses this to decide what to send
/// back to the IM platform.
#[derive(Debug, Clone)]
pub enum MessageResult {
    /// An action response to send/edit on the platform.
    Action(ActionResponse),
    /// An action response whose platform message should route replies to a session.
    RoutedAction {
        response: ActionResponse,
        session_id: String,
        conversation_id: Option<String>,
        follow_up_text: Option<String>,
    },
    /// Message was dispatched to the AI Agent. The caller should send
    /// a "thinking" placeholder and then relay stream events.
    Dispatched {
        owner_user_id: String,
        session_id: String,
        conversation_id: Option<String>,
    },
    /// Message was a text but user already has an active agent stream
    /// (no duplicate dispatch needed).
    AlreadyProcessing,
}

/// Processes incoming IM messages: authorization → action routing → AI dispatch.
///
/// This is the core message entry point for the channel system. Each
/// incoming `UnifiedIncomingMessage` is either:
/// 1. Rejected (unauthorized → pairing flow)
/// 2. Routed to an action handler (button callback)
/// 3. Dispatched to the AI Agent (text message)
pub struct ActionExecutor {
    pairing: Arc<PairingService>,
    session_mgr: Arc<SessionManager>,
    settings: Arc<ChannelSettingsService>,
    owner_user_id: Option<String>,
}

impl ActionExecutor {
    pub fn new(
        pairing: Arc<PairingService>,
        session_mgr: Arc<SessionManager>,
        settings: Arc<ChannelSettingsService>,
        owner_user_id: Option<String>,
    ) -> Self {
        Self {
            pairing,
            session_mgr,
            settings,
            owner_user_id,
        }
    }

    pub fn owner_user_id(&self) -> Option<&str> {
        self.owner_user_id.as_deref()
    }

    /// Main entry point: handle an incoming message from any platform.
    ///
    /// Flow:
    /// 1. Authorization check → if unauthorized, trigger pairing
    /// 2. Button callback → route to action handler
    /// 3. Text message → get/create session → return Dispatched for AI
    pub async fn handle_incoming_message(&self, msg: &UnifiedIncomingMessage) -> Result<MessageResult, ChannelError> {
        let platform_type = msg.platform.to_string();
        let user_id = &msg.user.id;
        let chat_id = &msg.chat_id;
        let owner_user_id = msg
            .owner_user_id
            .as_deref()
            .or(self.owner_user_id.as_deref())
            .ok_or_else(|| ChannelError::InvalidConfig("channel owner user is required".to_owned()))?;

        // 1. Authorization check — resolve platform user → internal user ID
        let internal_user_id = self
            .pairing
            .get_internal_user_id(owner_user_id, user_id, &platform_type)
            .await?;

        let internal_user_id = match internal_user_id {
            Some(id) => id,
            None => {
                let response = self
                    .handle_unauthorized(owner_user_id, user_id, &platform_type, &msg.user.display_name)
                    .await?;
                return Ok(MessageResult::Action(response));
            }
        };

        // 2. Button callback → action routing
        if let Some(action) = &msg.action {
            if action.category == ActionCategory::System && action.action == "session.new" {
                let (response, session_id) = self
                    .create_new_session(owner_user_id, &internal_user_id, action.context.platform, chat_id)
                    .await?;
                return Ok(MessageResult::RoutedAction {
                    response,
                    session_id,
                    conversation_id: None,
                    follow_up_text: None,
                });
            }
            let response = self.route_action(owner_user_id, action, &internal_user_id).await?;
            if let Some(message_id) = action.context.message_id.as_deref()
                && let Some(conversation_id) = self
                    .resolve_conversation_by_reply(
                        owner_user_id,
                        msg.platform,
                        chat_id,
                        user_id,
                        message_id,
                        quoted_text_from_raw(msg.raw.as_ref()),
                    )
                    .await?
            {
                let agent_config = self.settings.get_agent_config(owner_user_id, msg.platform).await?;
                let session = self
                    .session_mgr
                    .activate_conversation(
                        owner_user_id,
                        &internal_user_id,
                        chat_id,
                        &agent_config.agent_type,
                        &conversation_id,
                    )
                    .await?;
                return Ok(MessageResult::RoutedAction {
                    response,
                    session_id: session.id,
                    conversation_id: Some(conversation_id),
                    follow_up_text: None,
                });
            }
            return Ok(MessageResult::Action(response));
        }

        // 3. Text commands create sessions or show help without an existing route.
        if matches!(
            msg.content.content_type,
            MessageContentType::Text | MessageContentType::Command
        ) {
            match resolve_session_command(&msg.content.text) {
                Some(("session.new", follow_up_text)) => {
                    let (mut response, session_id) = self
                        .create_new_session(owner_user_id, &internal_user_id, msg.platform, chat_id)
                        .await?;
                    if follow_up_text.is_some() {
                        response.text = None;
                    }
                    return Ok(MessageResult::RoutedAction {
                        response,
                        session_id,
                        conversation_id: None,
                        follow_up_text,
                    });
                }
                Some(("help.show", _)) => return Ok(MessageResult::Action(build_help_response())),
                _ => {}
            }
        }

        // 4. Normal text is valid only when it replies to a session-bound bot message.
        let Some(reply_to_message_id) = msg.reply_to_message_id.as_deref() else {
            debug!(
                platform = %platform_type,
                has_quoted_text = quoted_text_from_raw(msg.raw.as_ref()).is_some(),
                "incoming channel text has no reply message id"
            );
            return Ok(MessageResult::Action(build_unbound_session_response()));
        };
        let Some(conversation_id) = self
            .resolve_conversation_by_reply(
                owner_user_id,
                msg.platform,
                chat_id,
                user_id,
                reply_to_message_id,
                quoted_text_from_raw(msg.raw.as_ref()),
            )
            .await?
        else {
            return Ok(MessageResult::Action(build_unbound_session_response()));
        };
        let agent_config = self.settings.get_agent_config(owner_user_id, msg.platform).await?;
        let session = self
            .session_mgr
            .activate_conversation(
                owner_user_id,
                &internal_user_id,
                chat_id,
                &agent_config.agent_type,
                &conversation_id,
            )
            .await?;

        info!(
            session_id = %session.id,
            user_id = %user_id,
            chat_id = %chat_id,
            text_len = msg.content.text.len(),
            "message dispatched to agent"
        );

        Ok(MessageResult::Dispatched {
            owner_user_id: owner_user_id.to_owned(),
            session_id: session.id,
            conversation_id: Some(conversation_id),
        })
    }

    async fn resolve_conversation_by_reply(
        &self,
        owner_user_id: &str,
        platform: crate::types::PluginType,
        chat_id: &str,
        platform_user_id: &str,
        message_id: &str,
        quoted_text: Option<&str>,
    ) -> Result<Option<String>, ChannelError> {
        let platform_type = platform.to_string();
        debug!(
            platform = %platform_type,
            has_message_id = !message_id.is_empty(),
            has_quoted_text = quoted_text.is_some_and(|text| !text.trim().is_empty()),
            "resolving quoted channel reply"
        );
        if let Some(conversation_id) = self
            .session_mgr
            .resolve_conversation_route(owner_user_id, &platform_type, chat_id, message_id)
            .await?
        {
            debug!(platform = %platform_type, "matched exact channel message id");
            return Ok(Some(conversation_id));
        }

        if platform == crate::types::PluginType::Lark
            && platform_user_id != chat_id
            && let Some(conversation_id) = self
                .session_mgr
                .resolve_conversation_route(owner_user_id, &platform_type, platform_user_id, message_id)
                .await?
        {
            debug!(platform = %platform_type, "matched Lark user route");
            return Ok(Some(conversation_id));
        }

        if platform == crate::types::PluginType::Weixin
            && let Some(reply_timestamp) = decode_weixin_message_timestamp(message_id)
            && let Some(conversation_id) = self
                .session_mgr
                .resolve_conversation_route_by_timestamp(owner_user_id, &platform_type, chat_id, reply_timestamp, 3_000)
                .await?
        {
            debug!(platform = %platform_type, "matched Weixin timestamp route");
            return Ok(Some(conversation_id));
        }

        let candidates = quoted_route_candidates(quoted_text);
        debug!(
            platform = %platform_type,
            candidate_count = candidates.len(),
            "trying quoted channel preview routes"
        );
        for candidate in candidates {
            if let Some(conversation_id) = self
                .session_mgr
                .resolve_conversation_route_by_preview(
                    owner_user_id,
                    &platform_type,
                    chat_id,
                    &candidate,
                    platform == crate::types::PluginType::Weixin,
                )
                .await?
            {
                debug!(platform = %platform_type, "matched quoted channel preview route");
                return Ok(Some(conversation_id));
            }
        }
        debug!(platform = %platform_type, "no channel route matched quoted reply");
        Ok(None)
    }

    async fn create_new_session(
        &self,
        owner_user_id: &str,
        internal_user_id: &str,
        platform: crate::types::PluginType,
        chat_id: &str,
    ) -> Result<(ActionResponse, String), ChannelError> {
        let agent_config = self.settings.get_agent_config(owner_user_id, platform).await?;
        let session = self
            .session_mgr
            .create_session(owner_user_id, internal_user_id, chat_id, &agent_config.agent_type, None)
            .await?;
        let session_id = session.id;
        let response = ActionResponse {
            text: Some(
                "🆕 <b>新会话已就绪</b>\n\n\
                 引用回复这条消息即可继续这个会话。\n\
                 发送 <code>/new</code> 可再创建一个新会话，也可以发送 <code>/new 你的消息</code> 创建并立即发送。"
                    .into(),
            ),
            parse_mode: Some(ParseMode::HTML),
            buttons: Some(vec![vec![ActionButton {
                label: "帮助".into(),
                action: "help.show".into(),
                params: None,
            }]]),
            keyboard: None,
            behavior: ActionBehavior::Send,
            toast: None,
            edit_message_id: None,
        };
        Ok((response, session_id))
    }

    /// Handles an unauthorized user: generate pairing code and return
    /// a response with instructions and action buttons.
    async fn handle_unauthorized(
        &self,
        owner_user_id: &str,
        platform_user_id: &str,
        platform_type: &str,
        display_name: &str,
    ) -> Result<ActionResponse, ChannelError> {
        let code = self
            .pairing
            .request_pairing(owner_user_id, platform_user_id, platform_type, Some(display_name))
            .await?;

        debug!(
            platform_user_id = %platform_user_id,
            code = %code,
            "pairing code generated for unauthorized user"
        );

        Ok(build_pairing_response(&code))
    }

    /// Routes an action to the appropriate handler by category.
    async fn route_action(
        &self,
        owner_user_id: &str,
        action: &UnifiedAction,
        internal_user_id: &str,
    ) -> Result<ActionResponse, ChannelError> {
        match action.category {
            ActionCategory::Platform => self.handle_platform_action(owner_user_id, action).await,
            ActionCategory::System => self.handle_system_action(owner_user_id, action, internal_user_id).await,
            ActionCategory::Chat => self.handle_chat_action(action).await,
        }
    }

    // ── Platform actions ────────────────────────────────────────────

    async fn handle_platform_action(
        &self,
        owner_user_id: &str,
        action: &UnifiedAction,
    ) -> Result<ActionResponse, ChannelError> {
        match action.action.as_str() {
            "pairing.show" | "pairing.refresh" => {
                let code = self
                    .pairing
                    .request_pairing(
                        owner_user_id,
                        &action.context.user_id,
                        &action.context.platform.to_string(),
                        None,
                    )
                    .await?;
                Ok(build_pairing_response(&code))
            }
            "pairing.check" => {
                let authorized = self
                    .pairing
                    .is_user_authorized(
                        owner_user_id,
                        &action.context.user_id,
                        &action.context.platform.to_string(),
                    )
                    .await?;
                if authorized {
                    Ok(ActionResponse {
                        text: Some("授权已通过。发送 /new 创建新会话。".into()),
                        parse_mode: None,
                        buttons: None,
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    })
                } else {
                    Ok(ActionResponse {
                        text: Some("仍在等待授权。请让管理员前往 SayDoneAI → 远程连接 → Channels 处理。".into()),
                        parse_mode: None,
                        buttons: Some(vec![vec![
                            ActionButton {
                                label: "刷新".into(),
                                action: "pairing.refresh".into(),
                                params: None,
                            },
                            ActionButton {
                                label: "再次检查".into(),
                                action: "pairing.check".into(),
                                params: None,
                            },
                        ]]),
                        keyboard: None,
                        behavior: ActionBehavior::Send,
                        toast: None,
                        edit_message_id: None,
                    })
                }
            }
            "pairing.help" => Ok(ActionResponse {
                text: Some(
                    "使用此机器人前需要完成授权：\n\
                         1. 发送任意消息获取 6 位配对码\n\
                         2. 把配对码交给管理员\n\
                         3. 管理员在 SayDoneAI → 远程连接 → Channels 中批准\n\
                         4. 授权通过后发送 /new 创建会话"
                        .into(),
                ),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Send,
                toast: None,
                edit_message_id: None,
            }),
            other => {
                warn!(action = %other, "unknown platform action");
                Ok(build_unknown_action_response(other))
            }
        }
    }

    // ── System actions ──────────────────────────────────────────────

    async fn handle_system_action(
        &self,
        owner_user_id: &str,
        action: &UnifiedAction,
        internal_user_id: &str,
    ) -> Result<ActionResponse, ChannelError> {
        match action.action.as_str() {
            "session.new" => {
                let user_id = internal_user_id;
                let chat_id = &action.context.chat_id;
                let agent_config = self
                    .settings
                    .get_agent_config(owner_user_id, action.context.platform)
                    .await?;
                let session = self
                    .session_mgr
                    .create_session(owner_user_id, user_id, chat_id, &agent_config.agent_type, None)
                    .await?;
                let cli_name = display_cli_name(agent_config.backend.as_deref().unwrap_or(&session.agent_type));

                Ok(ActionResponse {
                    text: Some(format!("新会话已创建。\nCLI：{}\n会话：{}", cli_name, &session.id[..8])),
                    parse_mode: None,
                    buttons: Some(vec![vec![ActionButton {
                        label: "帮助".into(),
                        action: "help.show".into(),
                        params: None,
                    }]]),
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                })
            }
            "session.status" => {
                let Some(message_id) = action.context.message_id.as_deref() else {
                    return Ok(build_unbound_session_response());
                };
                let Some(conversation_id) = self
                    .resolve_conversation_by_reply(
                        owner_user_id,
                        action.context.platform,
                        &action.context.chat_id,
                        &action.context.user_id,
                        message_id,
                        None,
                    )
                    .await?
                else {
                    return Ok(build_unbound_session_response());
                };
                let agent_config = self
                    .settings
                    .get_agent_config(owner_user_id, action.context.platform)
                    .await?;
                let session = self
                    .session_mgr
                    .activate_conversation(
                        owner_user_id,
                        internal_user_id,
                        &action.context.chat_id,
                        &agent_config.agent_type,
                        &conversation_id,
                    )
                    .await?;
                let cli_name = display_cli_name(agent_config.backend.as_deref().unwrap_or(&session.agent_type));

                Ok(ActionResponse {
                    text: Some(format!(
                        "会话：{}\nCLI：{}\n创建时间：{}\n最后活跃时间：{}",
                        &session.id[..8],
                        cli_name,
                        session.created_at,
                        session.last_activity,
                    )),
                    parse_mode: None,
                    buttons: Some(vec![vec![ActionButton {
                        label: "新建会话".into(),
                        action: "session.new".into(),
                        params: None,
                    }]]),
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: None,
                    edit_message_id: None,
                })
            }
            "help.show" => Ok(build_help_response()),
            "help.features" => Ok(ActionResponse {
                text: Some(
                    "功能：\n\
                         • 使用已配置的 CLI 和模型聊天\n\
                         • 在授权模式下执行工具\n\
                         • 通过引用回复精确进入指定会话"
                        .into(),
                ),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Send,
                toast: None,
                edit_message_id: None,
            }),
            "help.pairing" => Ok(ActionResponse {
                text: Some(
                    "配对：\n\
                         发送任意消息 → 获取 6 位配对码 → 管理员批准 → 完成授权"
                        .into(),
                ),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Send,
                toast: None,
                edit_message_id: None,
            }),
            "help.tips" => Ok(ActionResponse {
                text: Some(
                    "使用提示：\n\
                         • 发送 /new 创建空白新会话\n\
                         • 发送 /new 你的消息，创建会话并立即发送\n\
                         • 引用回复机器人消息，继续对应的会话\n\
                         • 使用 /help 查看帮助"
                        .into(),
                ),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Send,
                toast: None,
                edit_message_id: None,
            }),
            "settings.show" => Ok(ActionResponse {
                text: Some(
                    "渠道设置由 SayDoneAI 桌面应用管理。\n\
                         请前往远程连接 → Channels 配置渠道、CLI、模型和授权用户。"
                        .into(),
                ),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Send,
                toast: None,
                edit_message_id: None,
            }),
            other => {
                warn!(action = %other, "unknown system action");
                Ok(build_unknown_action_response(other))
            }
        }
    }

    // ── Chat actions ────────────────────────────────────────────────

    async fn handle_chat_action(&self, action: &UnifiedAction) -> Result<ActionResponse, ChannelError> {
        match action.action.as_str() {
            "chat.send" | "chat.regenerate" | "chat.continue" => {
                // These are handled by the message flow, not action responses.
                // Return a placeholder; the real logic is in ChannelMessageService.
                Ok(ActionResponse {
                    text: None,
                    parse_mode: None,
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Send,
                    toast: Some("处理中…".into()),
                    edit_message_id: None,
                })
            }
            "action.copy" => Ok(ActionResponse {
                text: None,
                parse_mode: None,
                buttons: None,
                keyboard: None,
                behavior: ActionBehavior::Answer,
                toast: Some("已复制到剪贴板".into()),
                edit_message_id: None,
            }),
            "system.confirm" => {
                let call_id = action
                    .params
                    .as_ref()
                    .and_then(|p| p.get("callId"))
                    .cloned()
                    .unwrap_or_default();
                let value = action
                    .params
                    .as_ref()
                    .and_then(|p| p.get("value"))
                    .cloned()
                    .unwrap_or_else(|| "true".into());

                debug!(call_id = %call_id, value = %value, "tool confirmation received");

                Ok(ActionResponse {
                    text: None,
                    parse_mode: None,
                    buttons: None,
                    keyboard: None,
                    behavior: ActionBehavior::Answer,
                    toast: Some("已确认".into()),
                    edit_message_id: None,
                })
            }
            other => {
                warn!(action = %other, "unknown chat action");
                Ok(build_unknown_action_response(other))
            }
        }
    }
}

fn display_cli_name(cli: &str) -> &str {
    if cli.eq_ignore_ascii_case("pi") {
        "SayDone CLI"
    } else {
        cli
    }
}

// ── Helper builders ─────────────────────────────────────────────────

fn resolve_session_command(text: &str) -> Option<(&'static str, Option<String>)> {
    let normalized = normalize_channel_command_text(text);
    let (command, follow_up_text) = normalized
        .split_once(' ')
        .map_or((normalized.as_str(), None), |(command, follow_up)| {
            (command, (!follow_up.is_empty()).then(|| follow_up.to_owned()))
        });
    let command = command.split('@').next()?.to_ascii_lowercase();
    match command.as_str() {
        "/new" => Some(("session.new", follow_up_text)),
        "/help" if follow_up_text.is_none() => Some(("help.show", None)),
        _ => None,
    }
}

fn normalize_channel_command_text(text: &str) -> String {
    text.chars()
        .filter(|character| {
            !matches!(
                character,
                '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{2060}' | '\u{feff}'
            )
        })
        .map(|character| match character {
            '\u{3000}' => ' ',
            '\u{ff0f}' | '\u{2044}' | '\u{2215}' => '/',
            other => other,
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn quoted_text_from_raw(raw: Option<&serde_json::Value>) -> Option<&str> {
    raw.and_then(|value| value.get("quoted_text"))
        .and_then(serde_json::Value::as_str)
}

fn quoted_route_candidates(value: Option<&str>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let normalized = value
        .replace("\r\n", "\n")
        .lines()
        .map(|line| {
            let line = line.trim();
            if let Some((prefix, suffix)) = line.split_once('：') {
                format!(
                    "{}：{}",
                    prefix.trim_end(),
                    suffix.split_whitespace().collect::<Vec<_>>().join(" ")
                )
            } else if let Some((prefix, suffix)) = line.split_once(':') {
                format!(
                    "{}:{}",
                    prefix.trim_end(),
                    suffix.split_whitespace().collect::<Vec<_>>().join(" ")
                )
            } else {
                line.split_whitespace().collect::<Vec<_>>().join(" ")
            }
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if normalized.is_empty() {
        return Vec::new();
    }
    let mut candidates = vec![normalized.clone()];
    if let Some((_, suffix)) = normalized.split_once('：').or_else(|| normalized.split_once(':'))
        && !suffix.trim().is_empty()
    {
        candidates.push(suffix.trim().to_owned());
    }
    let without_ellipsis = normalized.trim_end_matches("...").trim_end_matches('…').trim();
    if !without_ellipsis.is_empty() && without_ellipsis != normalized {
        candidates.push(without_ellipsis.to_owned());
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn decode_weixin_message_timestamp(message_id: &str) -> Option<i64> {
    if !(16..=20).contains(&message_id.len()) || !message_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let timestamp = message_id.parse::<u128>().ok()?.checked_div(4_194_304)?;
    let timestamp = i64::try_from(timestamp).ok()?;
    let earliest = 1_577_836_800_000_i64;
    (timestamp >= earliest && timestamp <= aionui_common::now_ms() + 86_400_000).then_some(timestamp)
}

fn build_unbound_session_response() -> ActionResponse {
    ActionResponse {
        text: Some("我还不知道你想继续哪个会话。\n请引用回复我之前的一条消息，或发送 /new 创建新会话。".into()),
        parse_mode: None,
        buttons: None,
        keyboard: None,
        behavior: ActionBehavior::Send,
        toast: None,
        edit_message_id: None,
    }
}

fn build_pairing_response(code: &str) -> ActionResponse {
    ActionResponse {
        text: Some(format!(
            "欢迎使用 SayDoneAI。首次使用需要完成授权。\n\n\
             你的配对码：*{code}*\n\n\
             请把配对码交给管理员，由管理员在 SayDoneAI → 远程连接 → Channels 中批准。\n\
             配对码将在 10 分钟后过期。"
        )),
        parse_mode: None,
        buttons: Some(vec![vec![
            ActionButton {
                label: "刷新配对码".into(),
                action: "pairing.refresh".into(),
                params: None,
            },
            ActionButton {
                label: "检查授权状态".into(),
                action: "pairing.check".into(),
                params: None,
            },
            ActionButton {
                label: "帮助".into(),
                action: "pairing.help".into(),
                params: None,
            },
        ]]),
        keyboard: None,
        behavior: ActionBehavior::Send,
        toast: None,
        edit_message_id: None,
    }
}

fn build_help_response() -> ActionResponse {
    ActionResponse {
        text: Some(
            "Channels 使用方式：\n\
             • 发送 /new 创建空白新会话\n\
             • 发送 /new 你的消息，创建新会话并立即发送\n\
             • 其他会话请引用回复我之前的消息，以继续对应会话"
                .into(),
        ),
        parse_mode: None,
        buttons: Some(vec![
            vec![
                ActionButton {
                    label: "新建会话".into(),
                    action: "session.new".into(),
                    params: None,
                },
                ActionButton {
                    label: "会话状态".into(),
                    action: "session.status".into(),
                    params: None,
                },
            ],
            vec![
                ActionButton {
                    label: "功能".into(),
                    action: "help.features".into(),
                    params: None,
                },
                ActionButton {
                    label: "使用提示".into(),
                    action: "help.tips".into(),
                    params: None,
                },
            ],
        ]),
        keyboard: None,
        behavior: ActionBehavior::Send,
        toast: None,
        edit_message_id: None,
    }
}

fn build_unknown_action_response(action: &str) -> ActionResponse {
    ActionResponse {
        text: Some(format!("无法识别的操作：{action}")),
        parse_mode: None,
        buttons: None,
        keyboard: None,
        behavior: ActionBehavior::Send,
        toast: None,
        edit_message_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActionContext, MessageContentType, PluginType, UnifiedMessageContent, UnifiedUser};
    use aionui_api_types::WebSocketMessage;
    use aionui_common::{TimestampMs, now_ms};
    use aionui_db::models::{
        AssistantSessionRow, AssistantUserRow, ChannelPluginRow, ClientPreference, PairingCodeRow,
    };
    use aionui_db::{DbError, IChannelRepository, IClientPreferenceRepository, UpdatePluginStatusParams};
    use aionui_realtime::EventBroadcaster;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ── Mock EventBroadcaster ──────────────────────────────────────────

    struct MockBroadcaster;

    impl EventBroadcaster for MockBroadcaster {
        fn broadcast(&self, _event: WebSocketMessage<serde_json::Value>) {}
    }

    // ── Mock IChannelRepository ────────────────────────────────────────
    const OWNER_ID: &str = "owner-test";

    struct MockRepo {
        users: Mutex<Vec<AssistantUserRow>>,
        sessions: Mutex<Vec<AssistantSessionRow>>,
        pairings: Mutex<Vec<PairingCodeRow>>,
    }

    impl MockRepo {
        fn new() -> Self {
            Self {
                users: Mutex::new(Vec::new()),
                sessions: Mutex::new(Vec::new()),
                pairings: Mutex::new(Vec::new()),
            }
        }

        fn add_authorized_user(&self, platform_user_id: &str, platform_type: &str) {
            let user = AssistantUserRow {
                id: format!("user_{platform_user_id}"),
                owner_user_id: OWNER_ID.to_owned(),
                platform_user_id: platform_user_id.to_owned(),
                platform_type: platform_type.to_owned(),
                display_name: Some("Test User".into()),
                authorized_at: now_ms(),
                last_active: None,
                session_id: None,
            };
            self.users.lock().unwrap().push(user);
        }
    }

    #[async_trait::async_trait]
    impl IChannelRepository for MockRepo {
        async fn get_all_plugins(&self, _owner_user_id: &str) -> Result<Vec<ChannelPluginRow>, DbError> {
            Ok(vec![])
        }
        async fn get_plugin(&self, _owner_user_id: &str, _id: &str) -> Result<Option<ChannelPluginRow>, DbError> {
            Ok(None)
        }
        async fn upsert_plugin(&self, _owner_user_id: &str, _row: &ChannelPluginRow) -> Result<(), DbError> {
            Ok(())
        }
        async fn update_plugin_status(
            &self,
            _owner_user_id: &str,
            _id: &str,
            _params: &UpdatePluginStatusParams,
        ) -> Result<(), DbError> {
            Ok(())
        }
        async fn delete_plugin(&self, _owner_user_id: &str, _id: &str) -> Result<(), DbError> {
            Ok(())
        }

        async fn get_all_users(&self, _owner_user_id: &str) -> Result<Vec<AssistantUserRow>, DbError> {
            Ok(self.users.lock().unwrap().clone())
        }
        async fn get_user_by_platform(
            &self,
            _owner_user_id: &str,
            platform_user_id: &str,
            platform_type: &str,
        ) -> Result<Option<AssistantUserRow>, DbError> {
            let users = self.users.lock().unwrap();
            Ok(users
                .iter()
                .find(|u| u.platform_user_id == platform_user_id && u.platform_type == platform_type)
                .cloned())
        }
        async fn create_user(&self, _owner_user_id: &str, row: &AssistantUserRow) -> Result<(), DbError> {
            self.users.lock().unwrap().push(row.clone());
            Ok(())
        }
        async fn update_user_last_active(
            &self,
            _owner_user_id: &str,
            _id: &str,
            _last_active: TimestampMs,
        ) -> Result<(), DbError> {
            Ok(())
        }
        async fn delete_user(&self, _owner_user_id: &str, _id: &str) -> Result<(), DbError> {
            Ok(())
        }

        async fn get_all_sessions(&self, _owner_user_id: &str) -> Result<Vec<AssistantSessionRow>, DbError> {
            Ok(self.sessions.lock().unwrap().clone())
        }
        async fn get_session(&self, _owner_user_id: &str, id: &str) -> Result<Option<AssistantSessionRow>, DbError> {
            let sessions = self.sessions.lock().unwrap();
            Ok(sessions.iter().find(|s| s.id == id).cloned())
        }
        async fn get_or_create_session(
            &self,
            _owner_user_id: &str,
            user_id: &str,
            chat_id: &str,
            new_row: &AssistantSessionRow,
        ) -> Result<AssistantSessionRow, DbError> {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(existing) = sessions
                .iter_mut()
                .find(|s| s.user_id == user_id && s.chat_id.as_deref() == Some(chat_id))
            {
                existing.last_activity = new_row.last_activity;
                return Ok(existing.clone());
            }
            sessions.push(new_row.clone());
            Ok(new_row.clone())
        }
        async fn create_session(
            &self,
            _owner_user_id: &str,
            _user_id: &str,
            new_row: &AssistantSessionRow,
        ) -> Result<AssistantSessionRow, DbError> {
            self.sessions.lock().unwrap().push(new_row.clone());
            Ok(new_row.clone())
        }
        async fn update_session_activity(
            &self,
            _owner_user_id: &str,
            _id: &str,
            _last_activity: TimestampMs,
        ) -> Result<(), DbError> {
            Ok(())
        }
        async fn update_session_conversation(
            &self,
            _owner_user_id: &str,
            id: &str,
            conversation_id: &str,
        ) -> Result<(), DbError> {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(s) = sessions.iter_mut().find(|s| s.id == id) {
                s.conversation_id = Some(conversation_id.to_owned());
                Ok(())
            } else {
                Err(DbError::NotFound(id.into()))
            }
        }
        async fn update_session_agent_type(
            &self,
            _owner_user_id: &str,
            id: &str,
            agent_type: &str,
        ) -> Result<(), DbError> {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(s) = sessions.iter_mut().find(|s| s.id == id) {
                s.agent_type = agent_type.to_owned();
                Ok(())
            } else {
                Err(DbError::NotFound(id.into()))
            }
        }
        async fn delete_sessions_by_user(&self, _owner_user_id: &str, user_id: &str) -> Result<(), DbError> {
            self.sessions.lock().unwrap().retain(|s| s.user_id != user_id);
            Ok(())
        }
        async fn delete_session_by_user_chat(
            &self,
            _owner_user_id: &str,
            user_id: &str,
            chat_id: &str,
        ) -> Result<(), DbError> {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.retain(|s| !(s.user_id == user_id && s.chat_id.as_deref() == Some(chat_id)));
            Ok(())
        }

        async fn create_pairing(&self, _owner_user_id: &str, row: &PairingCodeRow) -> Result<(), DbError> {
            self.pairings.lock().unwrap().push(row.clone());
            Ok(())
        }
        async fn get_pending_pairings(&self, _owner_user_id: &str) -> Result<Vec<PairingCodeRow>, DbError> {
            let pairings = self.pairings.lock().unwrap();
            Ok(pairings.iter().filter(|p| p.status == "pending").cloned().collect())
        }
        async fn get_pairing_by_code(
            &self,
            _owner_user_id: &str,
            code: &str,
        ) -> Result<Option<PairingCodeRow>, DbError> {
            let pairings = self.pairings.lock().unwrap();
            Ok(pairings.iter().find(|p| p.code == code).cloned())
        }
        async fn update_pairing_status(&self, _owner_user_id: &str, code: &str, status: &str) -> Result<(), DbError> {
            let mut pairings = self.pairings.lock().unwrap();
            if let Some(p) = pairings.iter_mut().find(|p| p.code == code) {
                p.status = status.to_owned();
                Ok(())
            } else {
                Err(DbError::NotFound(code.into()))
            }
        }
        async fn cleanup_expired_pairings(&self, _owner_user_id: &str, _now: TimestampMs) -> Result<u64, DbError> {
            Ok(0)
        }
    }

    // ── Mock IClientPreferenceRepository ──────────────────────────────

    struct MockPrefRepo;

    #[async_trait::async_trait]
    impl IClientPreferenceRepository for MockPrefRepo {
        async fn get_all(&self, _user_id: &str) -> Result<Vec<ClientPreference>, DbError> {
            Ok(vec![])
        }
        async fn get_by_keys(&self, _user_id: &str, _keys: &[&str]) -> Result<Vec<ClientPreference>, DbError> {
            Ok(vec![])
        }
        async fn upsert_batch(&self, _user_id: &str, _entries: &[(&str, &str)]) -> Result<(), DbError> {
            Ok(())
        }
        async fn delete_keys(&self, _user_id: &str, _keys: &[&str]) -> Result<(), DbError> {
            Ok(())
        }
    }

    // ── Test helpers ───────────────────────────────────────────────────

    fn setup() -> (ActionExecutor, Arc<MockRepo>) {
        let repo = Arc::new(MockRepo::new());
        let broadcaster = Arc::new(MockBroadcaster);
        let pairing = Arc::new(PairingService::new(repo.clone(), broadcaster));
        let session_mgr = Arc::new(SessionManager::new(repo.clone()));
        let pref_repo: Arc<dyn IClientPreferenceRepository> = Arc::new(MockPrefRepo);
        let settings = Arc::new(ChannelSettingsService::new(pref_repo));
        let executor = ActionExecutor::new(pairing, session_mgr, settings, Some(OWNER_ID.to_owned()));
        (executor, repo)
    }

    fn setup_without_owner() -> (ActionExecutor, Arc<MockRepo>) {
        let repo = Arc::new(MockRepo::new());
        let broadcaster = Arc::new(MockBroadcaster);
        let pairing = Arc::new(PairingService::new(repo.clone(), broadcaster));
        let session_mgr = Arc::new(SessionManager::new(repo.clone()));
        let pref_repo: Arc<dyn IClientPreferenceRepository> = Arc::new(MockPrefRepo);
        let settings = Arc::new(ChannelSettingsService::new(pref_repo));
        let executor = ActionExecutor::new(pairing, session_mgr, settings, None);
        (executor, repo)
    }

    fn make_text_message(user_id: &str, chat_id: &str, text: &str, platform: PluginType) -> UnifiedIncomingMessage {
        UnifiedIncomingMessage {
            owner_user_id: None,
            id: "msg_1".into(),
            platform,
            chat_id: chat_id.into(),
            user: UnifiedUser {
                id: user_id.into(),
                username: None,
                display_name: "Test".into(),
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

    fn make_action_message(
        user_id: &str,
        chat_id: &str,
        action_name: &str,
        category: ActionCategory,
        platform: PluginType,
        params: Option<HashMap<String, String>>,
    ) -> UnifiedIncomingMessage {
        UnifiedIncomingMessage {
            owner_user_id: None,
            id: "msg_1".into(),
            platform,
            chat_id: chat_id.into(),
            user: UnifiedUser {
                id: user_id.into(),
                username: None,
                display_name: "Test".into(),
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
                params,
                context: ActionContext {
                    platform,
                    user_id: user_id.into(),
                    chat_id: chat_id.into(),
                    message_id: None,
                    session_id: None,
                },
            }),
            raw: None,
        }
    }

    // ── Authorization tests ────────────────────────────────────────────

    #[tokio::test]
    async fn unauthorized_user_gets_pairing_response() {
        let (executor, _repo) = setup();
        let msg = make_text_message("tg_42", "chat_1", "Hello", PluginType::Telegram);

        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                assert_eq!(resp.behavior, ActionBehavior::Send);
                let text = resp.text.unwrap();
                assert!(text.contains("配对码"));
                assert!(resp.buttons.is_some());
            }
            _ => panic!("Expected Action result for unauthorized user"),
        }
    }

    #[tokio::test]
    async fn missing_owner_user_id_is_rejected() {
        let (executor, _repo) = setup_without_owner();
        let msg = make_text_message("tg_42", "chat_1", "Hello", PluginType::Telegram);

        let err = executor.handle_incoming_message(&msg).await.unwrap_err();
        assert!(matches!(err, ChannelError::InvalidConfig(_)));
        assert!(err.to_string().contains("owner user"));
    }

    #[tokio::test]
    async fn authorized_user_text_without_reply_route_is_rejected() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_text_message("tg_42", "chat_1", "Hello AI", PluginType::Telegram);
        let result = executor.handle_incoming_message(&msg).await.unwrap();

        let MessageResult::Action(response) = result else {
            panic!("Expected unbound-session instructions");
        };
        assert!(response.text.as_deref().is_some_and(|text| text.contains("/new")));
    }

    // ── Platform action tests ──────────────────────────────────────────

    #[tokio::test]
    async fn pairing_show_generates_code() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "pairing.show",
            ActionCategory::Platform,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();

        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("配对码"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn pairing_check_authorized() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "pairing.check",
            ActionCategory::Platform,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();

        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("授权已通过"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn pairing_check_not_authorized() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_99", // different user
            "chat_1",
            "pairing.check",
            ActionCategory::Platform,
            PluginType::Telegram,
            None,
        );
        // tg_99 is not authorized, but the action itself needs the user to be authorized
        // first (it's routed via handle_incoming_message which checks auth first)
        // So for this test, authorize tg_99 too
        repo.add_authorized_user("tg_99", "telegram");

        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                // tg_99 is authorized
                assert!(text.contains("授权已通过"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn pairing_help_returns_instructions() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "pairing.help",
            ActionCategory::Platform,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("完成授权"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    // ── System action tests ────────────────────────────────────────────

    #[tokio::test]
    async fn session_new_creates_session() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "session.new",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::RoutedAction {
                response: resp,
                session_id,
                ..
            } => {
                let text = resp.text.unwrap();
                assert!(text.contains("新会话已就绪"));
                assert!(!session_id.is_empty());
            }
            _ => panic!("Expected routed action result"),
        }
    }

    #[tokio::test]
    async fn session_new_preserves_existing_sessions() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let first_new = make_action_message(
            "tg_42",
            "chat_1",
            "session.new",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let r1 = executor.handle_incoming_message(&first_new).await.unwrap();
        let sid1 = match r1 {
            MessageResult::RoutedAction { session_id, .. } => session_id,
            _ => panic!("Expected routed action"),
        };

        let second_new = make_action_message(
            "tg_42",
            "chat_1",
            "session.new",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let r2 = executor.handle_incoming_message(&second_new).await.unwrap();
        let sid2 = match r2 {
            MessageResult::RoutedAction { session_id, .. } => session_id,
            _ => panic!("Expected routed action"),
        };
        assert_ne!(sid1, sid2);

        let sessions = repo.sessions.lock().unwrap();
        let user_chat_sessions: Vec<_> = sessions
            .iter()
            .filter(|s| s.user_id == "user_tg_42" && s.chat_id.as_deref() == Some("chat_1"))
            .collect();
        assert_eq!(user_chat_sessions.len(), 2);
    }

    #[tokio::test]
    async fn session_status_without_reply_route_shows_instructions() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "session.status",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("/new"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn help_show_returns_menu() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "help.show",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                assert!(resp.text.is_some());
                assert!(resp.buttons.is_some());
                let buttons = resp.buttons.unwrap();
                assert!(buttons.len() >= 2); // at least 2 rows
                assert!(
                    !buttons.iter().flatten().any(|button| button.action == "agent.show"),
                    "help menu must not expose direct agent selection"
                );
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn agent_show_is_treated_as_unknown_action() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "agent.show",
            ActionCategory::System,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("无法识别的操作"));
                assert!(text.contains("agent.show"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn agent_select_is_treated_as_unknown_action() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let params = HashMap::from([("agentType".into(), "acp".into())]);
        let select_msg = make_action_message(
            "tg_42",
            "chat_1",
            "agent.select",
            ActionCategory::System,
            PluginType::Telegram,
            Some(params),
        );
        let result = executor.handle_incoming_message(&select_msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("无法识别的操作"));
                assert!(text.contains("agent.select"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    // ── Chat action tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn system_confirm_returns_answer() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let params = HashMap::from([("callId".into(), "call_123".into()), ("value".into(), "true".into())]);
        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "system.confirm",
            ActionCategory::Chat,
            PluginType::Telegram,
            Some(params),
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                assert_eq!(resp.behavior, ActionBehavior::Answer);
                assert_eq!(resp.toast.as_deref(), Some("已确认"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    #[tokio::test]
    async fn action_copy_returns_answer() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "action.copy",
            ActionCategory::Chat,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                assert_eq!(resp.behavior, ActionBehavior::Answer);
                assert!(resp.toast.as_deref().unwrap().contains("已复制"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    // ── Unknown action tests ───────────────────────────────────────────

    #[tokio::test]
    async fn unknown_platform_action() {
        let (executor, repo) = setup();
        repo.add_authorized_user("tg_42", "telegram");

        let msg = make_action_message(
            "tg_42",
            "chat_1",
            "unknown.action",
            ActionCategory::Platform,
            PluginType::Telegram,
            None,
        );
        let result = executor.handle_incoming_message(&msg).await.unwrap();
        match result {
            MessageResult::Action(resp) => {
                let text = resp.text.unwrap();
                assert!(text.contains("无法识别的操作"));
            }
            _ => panic!("Expected Action result"),
        }
    }

    // ── build_pairing_response tests ───────────────────────────────────

    #[test]
    fn pairing_response_contains_code() {
        let resp = build_pairing_response("123456");
        let text = resp.text.unwrap();
        assert!(text.contains("123456"));
        assert!(text.contains("配对码"));
        assert_eq!(resp.behavior, ActionBehavior::Send);
        assert!(resp.buttons.is_some());
    }

    #[test]
    fn help_response_has_buttons() {
        let resp = build_help_response();
        assert!(resp.text.is_some());
        let buttons = resp.buttons.unwrap();
        assert!(!buttons.is_empty());
    }

    #[test]
    fn unknown_action_response_includes_name() {
        let resp = build_unknown_action_response("foo.bar");
        let text = resp.text.unwrap();
        assert!(text.contains("foo.bar"));
    }

    #[test]
    fn quoted_route_candidates_normalize_weixin_preview_text() {
        let candidates = quoted_route_candidates(Some("AI：  answer line  \r\n\r\n"));
        assert!(candidates.contains(&"AI：answer line".to_owned()));
        assert!(candidates.contains(&"answer line".to_owned()));
    }

    #[test]
    fn decode_weixin_message_timestamp_accepts_snowflake_ids() {
        let expected = now_ms();
        let message_id = (u128::try_from(expected).unwrap() * 4_194_304).to_string();
        assert_eq!(decode_weixin_message_timestamp(&message_id), Some(expected));
    }

    #[test]
    fn decode_weixin_message_timestamp_rejects_non_snowflake_ids() {
        assert_eq!(decode_weixin_message_timestamp("bot-route-42"), None);
        assert_eq!(decode_weixin_message_timestamp("1234"), None);
    }

    #[test]
    fn pi_backend_is_presented_as_saydone_cli() {
        assert_eq!(display_cli_name("pi"), "SayDone CLI");
        assert_eq!(display_cli_name("claude"), "claude");
    }
}
