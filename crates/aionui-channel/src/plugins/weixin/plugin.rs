use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::Client;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::constants::{WEIXIN_MAX_BACKOFF, WEIXIN_POLL_TIMEOUT};
use crate::error::ChannelError;
use crate::plugin::{ChannelPlugin, PluginCallbacks, PluginCredentialUpdate};
use crate::types::{
    BotInfo, MessageContentType, PluginConfig, PluginStatus, PluginType, UnifiedIncomingMessage, UnifiedMessageContent,
    UnifiedOutgoingMessage, UnifiedUser,
};

use super::api::WeixinApi;
use super::types::{ITEM_TYPE_TEXT, ITEM_TYPE_VOICE, WeixinRawItem, WeixinRawMessage};

/// Default base URL for the iLink Bot API.
const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
const CONTEXT_TOKENS_CONFIG_KEY: &str = "context_tokens";
const POLL_STATE_CONFIG_KEY: &str = "poll_state";
const POLL_CURSOR_ENTRY_KEY: &str = "get_updates_buf";

/// WeChat (iLink Bot) platform plugin.
///
/// Connects via long-polling (buffer-based `getupdates`), handles text/voice
/// messages. Does not support editing messages (WeChat limitation);
/// `edit_message` sends a new reply instead.
pub struct WeixinPlugin {
    status: PluginStatus,
    bot_info: Option<BotInfo>,
    last_error: Option<String>,
    api: Option<Arc<WeixinApi>>,
    poll_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    context_tokens: Arc<DashMap<String, String>>,
}

impl Default for WeixinPlugin {
    fn default() -> Self {
        Self {
            status: PluginStatus::Created,
            bot_info: None,
            last_error: None,
            api: None,
            poll_handle: None,
            shutdown_tx: None,
            context_tokens: Arc::new(DashMap::new()),
        }
    }
}

impl WeixinPlugin {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ChannelPlugin for WeixinPlugin {
    async fn initialize(&mut self, config: PluginConfig, callbacks: PluginCallbacks) -> Result<(), ChannelError> {
        self.status = PluginStatus::Initializing;
        let initial_poll_cursor = configured_poll_cursor(&config);

        self.context_tokens.clear();
        for (chat_id, token) in configured_context_tokens(&config) {
            self.context_tokens.insert(chat_id, token);
        }

        let bot_token = config
            .credentials
            .bot_token
            .as_deref()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing WeChat bot_token".into());
                ChannelError::InvalidConfig("Missing WeChat bot_token".into())
            })?;

        let account_id = config
            .credentials
            .account_id
            .as_deref()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing WeChat account_id".into());
                ChannelError::InvalidConfig("Missing WeChat account_id".into())
            })?;

        let base_url = config
            .credentials
            .extra
            .get("baseUrl")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_BASE_URL);

        let http_client = Client::builder()
            .timeout(Duration::from_secs(WEIXIN_POLL_TIMEOUT.as_secs() + 10))
            .build()
            .map_err(|e| {
                self.status = PluginStatus::Error;
                self.last_error = Some(format!("HTTP client init failed: {e}"));
                ChannelError::ConnectionFailed(format!("HTTP client init failed: {e}"))
            })?;

        let api = Arc::new(WeixinApi::new(http_client, base_url, bot_token));

        self.bot_info = Some(BotInfo {
            id: account_id.to_string(),
            username: None,
            display_name: format!("WeChat Bot ({account_id})"),
        });

        info!(account_id, "WeChat bot initialized");

        self.api = Some(api);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let api_clone = Arc::clone(self.api.as_ref().expect("api just set"));
        let context_tokens = Arc::clone(&self.context_tokens);
        let credential_update_tx = callbacks.credential_update_tx;
        self.poll_handle = Some(tokio::spawn(poll_loop(
            api_clone,
            callbacks.message_tx,
            shutdown_rx,
            context_tokens,
            credential_update_tx,
            initial_poll_cursor,
        )));

        self.status = PluginStatus::Ready;
        Ok(())
    }

    async fn start(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Starting;
        self.status = PluginStatus::Running;
        info!("WeChat plugin started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Stopping;

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(handle) = self.poll_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        self.api = None;
        self.context_tokens.clear();
        self.status = PluginStatus::Stopped;
        info!("WeChat plugin stopped");
        Ok(())
    }

    async fn send_message(&self, chat_id: &str, message: UnifiedOutgoingMessage) -> Result<String, ChannelError> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("Plugin not initialized".into()))?;

        let text = message.text.as_deref().unwrap_or("").to_string();
        let context_token = self
            .context_tokens
            .get(chat_id)
            .map(|value| value.clone())
            .ok_or_else(|| ChannelError::MessageSendFailed("微信主动发送需要用户先向机器人发送一条消息。".into()))?;

        api.send_message(chat_id, &text, Some(&context_token)).await
    }

    /// WeChat does not support editing messages.
    async fn edit_message(
        &self,
        chat_id: &str,
        _message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        let _ = self.send_message(chat_id, message).await?;
        Ok(())
    }

    fn active_user_count(&self) -> usize {
        0
    }

    fn bot_info(&self) -> Option<&BotInfo> {
        self.bot_info.as_ref()
    }

    fn plugin_type(&self) -> PluginType {
        PluginType::Weixin
    }

    fn status(&self) -> PluginStatus {
        self.status
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

fn configured_context_tokens(config: &PluginConfig) -> Vec<(String, String)> {
    config
        .credentials
        .extra
        .get(CONTEXT_TOKENS_CONFIG_KEY)
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(chat_id, value)| {
            value
                .as_str()
                .filter(|token| !token.is_empty())
                .map(|token| (chat_id.clone(), token.to_owned()))
        })
        .collect()
}

fn configured_poll_cursor(config: &PluginConfig) -> String {
    config
        .credentials
        .extra
        .get(POLL_STATE_CONFIG_KEY)
        .and_then(serde_json::Value::as_object)
        .and_then(|state| state.get(POLL_CURSOR_ENTRY_KEY))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

async fn persist_poll_cursor(
    credential_update_tx: Option<&tokio::sync::mpsc::Sender<PluginCredentialUpdate>>,
    cursor: &str,
) {
    if let Some(tx) = credential_update_tx {
        let _ = tx
            .send(PluginCredentialUpdate {
                map_key: POLL_STATE_CONFIG_KEY.to_owned(),
                entry_key: POLL_CURSOR_ENTRY_KEY.to_owned(),
                value: serde_json::Value::String(cursor.to_owned()),
            })
            .await;
    }
}

// ---------------------------------------------------------------------------
// Long-polling loop (buffer-based protocol)
// ---------------------------------------------------------------------------

async fn poll_loop(
    api: Arc<WeixinApi>,
    message_tx: tokio::sync::mpsc::Sender<UnifiedIncomingMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    context_tokens: Arc<DashMap<String, String>>,
    credential_update_tx: Option<tokio::sync::mpsc::Sender<PluginCredentialUpdate>>,
    initial_poll_cursor: String,
) {
    let mut buf = initial_poll_cursor;
    let mut consecutive_failures: u32 = 0;

    loop {
        if *shutdown_rx.borrow() {
            debug!("WeChat poll loop received shutdown signal");
            break;
        }

        // Reduce each round to success or a failure reason. On success we also
        // advance the buffer and dispatch messages; on API error / transport
        // error we only record the reason.
        let outcome: Result<(), String> = match api.get_updates(&buf).await {
            Ok(resp) => {
                let is_api_error = resp.ret.unwrap_or(0) != 0 || resp.errcode.unwrap_or(0) != 0;
                let cursor_expired = resp.ret == Some(-14) || resp.errcode == Some(-14);
                if cursor_expired {
                    buf.clear();
                    persist_poll_cursor(credential_update_tx.as_ref(), &buf).await;
                    info!("WeChat poll cursor expired; reset persisted cursor");
                    Ok(())
                } else if is_api_error {
                    Err(format!(
                        "getupdates API error ret={:?} errcode={:?}",
                        resp.ret, resp.errcode
                    ))
                } else {
                    if let Some(new_buf) = resp.get_updates_buf
                        && new_buf != buf
                    {
                        buf = new_buf;
                        persist_poll_cursor(credential_update_tx.as_ref(), &buf).await;
                    }
                    for msg in resp.msgs.unwrap_or_default() {
                        handle_message(&msg, &message_tx, &context_tokens, credential_update_tx.as_ref()).await;
                    }
                    Ok(())
                }
            }
            Err(e) => Err(e.to_string()),
        };

        match outcome {
            Ok(()) => {
                if poll_log_action(consecutive_failures, true) == PollLogAction::Recovered {
                    info!(consecutive_failures, "WeChat poll recovered");
                }
                consecutive_failures = 0;
            }
            Err(reason) => {
                let action = poll_log_action(consecutive_failures, false);
                consecutive_failures += 1;
                let delay = backoff_delay(consecutive_failures);
                match action {
                    PollLogAction::StartedFailing => {
                        warn!(error = %reason, "WeChat poll started failing");
                    }
                    PollLogAction::StillFailing => {
                        debug!(
                            error = %reason,
                            consecutive_failures,
                            next_backoff_secs = delay.as_secs(),
                            "WeChat poll still failing"
                        );
                    }
                    // Silent / Recovered are not reachable on the failure branch.
                    _ => {}
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown_rx.changed() => {
                        debug!("WeChat poll loop shutdown during backoff");
                        break;
                    }
                }
            }
        }
    }

    debug!("WeChat poll loop exited");
}

/// Exponential backoff delay for WeChat poll failures: `2^n` seconds,
/// capped at `WEIXIN_MAX_BACKOFF` (10 minutes). `saturating_pow` guards
/// against overflow on long failure streaks.
fn backoff_delay(consecutive_failures: u32) -> Duration {
    let secs = 2u64
        .saturating_pow(consecutive_failures)
        .min(WEIXIN_MAX_BACKOFF.as_secs());
    Duration::from_secs(secs)
}

/// What to log for a single WeChat poll outcome, decided from the failure
/// streak *before* this outcome is applied. Keeps the log-level policy in
/// one pure, testable place; the loop performs the actual logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollLogAction {
    /// Success while healthy — log nothing.
    Silent,
    /// First failure after a healthy streak (0 -> 1) — log once at warn.
    StartedFailing,
    /// Repeated failure while already failing — log at debug only.
    StillFailing,
    /// First success after one or more failures — log once at info.
    Recovered,
}

fn poll_log_action(prev_failures: u32, succeeded: bool) -> PollLogAction {
    match (succeeded, prev_failures) {
        (true, 0) => PollLogAction::Silent,
        (true, _) => PollLogAction::Recovered,
        (false, 0) => PollLogAction::StartedFailing,
        (false, _) => PollLogAction::StillFailing,
    }
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

async fn handle_message(
    msg: &WeixinRawMessage,
    message_tx: &tokio::sync::mpsc::Sender<UnifiedIncomingMessage>,
    context_tokens: &DashMap<String, String>,
    credential_update_tx: Option<&tokio::sync::mpsc::Sender<PluginCredentialUpdate>>,
) {
    let from_user_id = match &msg.from_user_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return,
    };

    // Store context_token for reply use
    if let Some(ctx) = &msg.context_token
        && !ctx.is_empty()
    {
        context_tokens.insert(from_user_id.clone(), ctx.clone());
        if let Some(tx) = credential_update_tx {
            let _ = tx
                .send(PluginCredentialUpdate {
                    map_key: CONTEXT_TOKENS_CONFIG_KEY.to_owned(),
                    entry_key: from_user_id.clone(),
                    value: serde_json::Value::String(ctx.clone()),
                })
                .await;
        }
    }

    let items = msg.item_list.as_deref().unwrap_or_default();
    let (content_type, text, _has_media) = extract_content(items);

    if text.is_empty() {
        return;
    }

    let display_name = if from_user_id.len() > 6 {
        from_user_id[from_user_id.len() - 6..].to_string()
    } else {
        from_user_id.clone()
    };

    let reply_to_message_id = extract_reply_to_message_id(items);
    let quoted_text = extract_quoted_reply_text(items);
    let message_id = msg
        .msg_id
        .clone()
        .or_else(|| msg.context_token.clone())
        .unwrap_or_default();

    let unified = UnifiedIncomingMessage {
        owner_user_id: None,
        id: message_id,
        platform: PluginType::Weixin,
        chat_id: from_user_id.clone(),
        user: UnifiedUser {
            id: from_user_id,
            username: None,
            display_name,
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type,
            text,
            attachments: None,
        },
        timestamp: chrono_now(),
        reply_to_message_id,
        action: None,
        raw: quoted_text.map(|quoted_text| serde_json::json!({ "quoted_text": quoted_text })),
    };

    let _ = message_tx.send(unified).await;
}

/// Extract the message ID referenced by a WeChat quoted reply.
///
/// WeChat places quoted metadata in `item_list[*].ref_msg`; the nested
/// `message_item.msg_id` is the ID of the bot message that was sent earlier
/// and therefore the key needed to resume its channel session.
fn extract_reply_to_message_id(items: &[WeixinRawItem]) -> Option<String> {
    items.iter().find_map(|item| {
        let reference = item.ref_msg.as_ref()?;
        let message_item = reference.message_item.as_ref()?;
        message_item
            .msg_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn extract_quoted_reply_text(items: &[WeixinRawItem]) -> Option<String> {
    items.iter().find_map(|item| {
        let reference = item.ref_msg.as_ref()?;
        reference
            .message_item
            .as_ref()
            .and_then(|message| message.text_item.as_ref())
            .and_then(|text| text.text.as_deref())
            .or(reference.title.as_deref())
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned)
    })
}

/// Extract text content from item_list.
///
/// Returns (content_type, combined_text, has_media_items).
fn extract_content(items: &[WeixinRawItem]) -> (MessageContentType, String, bool) {
    let mut text_parts: Vec<&str> = Vec::new();
    let mut has_media = false;

    for item in items {
        match item.item_type {
            Some(ITEM_TYPE_TEXT) => {
                if let Some(ref ti) = item.text_item
                    && let Some(ref t) = ti.text
                {
                    let trimmed = t.trim();
                    if !trimmed.is_empty() {
                        text_parts.push(trimmed);
                    }
                }
            }
            Some(ITEM_TYPE_VOICE) => {
                if let Some(ref vi) = item.voice_item
                    && let Some(ref t) = vi.text
                {
                    let trimmed = t.trim();
                    if !trimmed.is_empty() {
                        text_parts.push(trimmed);
                    }
                }
            }
            Some(2) | Some(4) => {
                has_media = true;
            }
            _ => {}
        }
    }

    let text = text_parts.join("\n\n");

    let content_type = if text.starts_with('/') {
        MessageContentType::Command
    } else {
        MessageContentType::Text
    };

    (content_type, text, has_media)
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PluginCredentials;
    use std::collections::HashMap;

    // -- backoff_delay ---------------------------------------------------------

    #[test]
    fn backoff_delay_exponential_curve() {
        assert_eq!(backoff_delay(1), Duration::from_secs(2));
        assert_eq!(backoff_delay(2), Duration::from_secs(4));
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
        assert_eq!(backoff_delay(4), Duration::from_secs(16));
        assert_eq!(backoff_delay(5), Duration::from_secs(32));
        assert_eq!(backoff_delay(9), Duration::from_secs(512));
    }

    #[test]
    fn backoff_delay_caps_at_ten_minutes() {
        assert_eq!(backoff_delay(10), Duration::from_secs(600));
        assert_eq!(backoff_delay(20), Duration::from_secs(600));
        // Must not overflow on large counts.
        assert_eq!(backoff_delay(u32::MAX), Duration::from_secs(600));
    }

    // -- poll_log_action -------------------------------------------------------

    #[test]
    fn log_action_silent_when_healthy_success() {
        assert_eq!(poll_log_action(0, true), PollLogAction::Silent);
    }

    #[test]
    fn log_action_started_failing_on_first_failure() {
        assert_eq!(poll_log_action(0, false), PollLogAction::StartedFailing);
    }

    #[test]
    fn log_action_still_failing_on_repeat_failure() {
        assert_eq!(poll_log_action(1, false), PollLogAction::StillFailing);
        assert_eq!(poll_log_action(9, false), PollLogAction::StillFailing);
    }

    #[test]
    fn log_action_recovered_after_failures() {
        assert_eq!(poll_log_action(1, true), PollLogAction::Recovered);
        assert_eq!(poll_log_action(5, true), PollLogAction::Recovered);
    }

    // -- extract_content -------------------------------------------------------

    #[test]
    fn extract_text_only() {
        let items = vec![make_text_item("Hello world")];
        let (ct, text, has_media) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "Hello world");
        assert!(!has_media);
    }

    #[test]
    fn extract_command() {
        let items = vec![make_text_item("/start")];
        let (ct, text, _) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Command);
        assert_eq!(text, "/start");
    }

    #[test]
    fn extract_voice_text() {
        let items = vec![WeixinRawItem {
            item_type: Some(ITEM_TYPE_VOICE),
            voice_item: Some(super::super::types::VoiceItem {
                text: Some("transcribed text".into()),
            }),
            ..Default::default()
        }];
        let (ct, text, _) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "transcribed text");
    }

    #[test]
    fn extract_mixed_text_and_voice() {
        let items = vec![
            make_text_item("Hello"),
            WeixinRawItem {
                item_type: Some(ITEM_TYPE_VOICE),
                voice_item: Some(super::super::types::VoiceItem {
                    text: Some("voice part".into()),
                }),
                ..Default::default()
            },
        ];
        let (_, text, _) = extract_content(&items);
        assert_eq!(text, "Hello\n\nvoice part");
    }

    #[test]
    fn extract_media_items_detected() {
        let items = vec![WeixinRawItem {
            item_type: Some(2),
            image_item: Some(super::super::types::MediaItemData {
                media: Some(super::super::types::MediaEncryptInfo {
                    encrypt_query_param: Some("enc".into()),
                    aes_key: Some("key".into()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }];
        let (_, text, has_media) = extract_content(&items);
        assert!(text.is_empty());
        assert!(has_media);
    }

    #[test]
    fn extract_empty_items() {
        let items: Vec<WeixinRawItem> = vec![];
        let (_, text, has_media) = extract_content(&items);
        assert!(text.is_empty());
        assert!(!has_media);
    }

    #[test]
    fn extract_quoted_reply_message_id() {
        let items = vec![WeixinRawItem {
            ref_msg: Some(super::super::types::WeixinRefMessage {
                message_item: Some(super::super::types::WeixinRefMessageItem {
                    msg_id: Some("  bot-route-42  ".into()),
                    text_item: None,
                }),
                title: None,
            }),
            ..Default::default()
        }];
        assert_eq!(extract_reply_to_message_id(&items).as_deref(), Some("bot-route-42"));
    }

    #[test]
    fn extract_quoted_reply_ignores_blank_ids() {
        let items = vec![WeixinRawItem {
            ref_msg: Some(super::super::types::WeixinRefMessage {
                message_item: Some(super::super::types::WeixinRefMessageItem {
                    msg_id: Some("   ".into()),
                    text_item: None,
                }),
                title: None,
            }),
            ..Default::default()
        }];
        assert_eq!(extract_reply_to_message_id(&items), None);
    }

    #[test]
    fn extract_quoted_reply_text_prefers_nested_message_text() {
        let items = vec![WeixinRawItem {
            ref_msg: Some(super::super::types::WeixinRefMessage {
                message_item: Some(super::super::types::WeixinRefMessageItem {
                    msg_id: Some("bot-route-42".into()),
                    text_item: Some(super::super::types::TextItem {
                        text: Some("  nested answer  ".into()),
                    }),
                }),
                title: Some("fallback title".into()),
            }),
            ..Default::default()
        }];
        assert_eq!(extract_quoted_reply_text(&items).as_deref(), Some("nested answer"));
    }

    #[test]
    fn extract_quoted_reply_text_falls_back_to_reference_title() {
        let items = vec![WeixinRawItem {
            ref_msg: Some(super::super::types::WeixinRefMessage {
                message_item: Some(super::super::types::WeixinRefMessageItem {
                    msg_id: Some("bot-route-42".into()),
                    text_item: None,
                }),
                title: Some("  quoted answer  ".into()),
            }),
            ..Default::default()
        }];
        assert_eq!(extract_quoted_reply_text(&items).as_deref(), Some("quoted answer"));
    }

    // -- WeixinPlugin constructor -----------------------------------------------

    #[test]
    fn new_plugin_initial_state() {
        let plugin = WeixinPlugin::new();
        assert_eq!(plugin.status(), PluginStatus::Created);
        assert!(plugin.bot_info().is_none());
        assert!(plugin.last_error().is_none());
        assert_eq!(plugin.plugin_type(), PluginType::Weixin);
        assert_eq!(plugin.active_user_count(), 0);
    }

    // -- initialize validation --------------------------------------------------

    #[tokio::test]
    async fn initialize_missing_bot_token_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(None, Some("acc_1"));
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
        assert_eq!(plugin.last_error(), Some("Missing WeChat bot_token"));
    }

    #[tokio::test]
    async fn initialize_missing_account_id_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(Some("tok_1"), None);
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
        assert_eq!(plugin.last_error(), Some("Missing WeChat account_id"));
    }

    #[tokio::test]
    async fn initialize_empty_bot_token_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(Some(""), Some("acc_1"));
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
    }

    #[tokio::test]
    async fn inbound_context_token_is_cached_and_emitted_for_encrypted_persistence() {
        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(1);
        let (credential_tx, mut credential_rx) = tokio::sync::mpsc::channel(1);
        let context_tokens = DashMap::new();
        let message = WeixinRawMessage {
            from_user_id: Some("chat-1".into()),
            context_token: Some("context-secret".into()),
            msg_id: Some("message-1".into()),
            item_list: Some(vec![make_text_item("你好")]),
        };

        handle_message(&message, &message_tx, &context_tokens, Some(&credential_tx)).await;

        assert_eq!(
            context_tokens.get("chat-1").map(|value| value.value().clone()),
            Some("context-secret".into())
        );
        let update = credential_rx.recv().await.unwrap();
        assert_eq!(update.map_key, CONTEXT_TOKENS_CONFIG_KEY);
        assert_eq!(update.entry_key, "chat-1");
        assert_eq!(update.value, serde_json::json!("context-secret"));
        let incoming = message_rx.recv().await.unwrap();
        assert!(incoming.raw.is_none());
    }

    #[tokio::test]
    async fn proactive_send_without_context_stops_before_the_http_request() {
        let mut plugin = WeixinPlugin::new();
        plugin.api = Some(Arc::new(WeixinApi::new(Client::new(), "http://127.0.0.1:9", "token")));
        let error = plugin
            .send_message(
                "chat-without-context",
                UnifiedOutgoingMessage {
                    message_type: crate::types::OutgoingMessageType::Text,
                    text: Some("桌面消息".into()),
                    parse_mode: None,
                    buttons: None,
                    keyboard: None,
                    image_url: None,
                    file_url: None,
                    file_name: None,
                    media_actions: None,
                    reply_to_message_id: None,
                    silent: None,
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("需要用户先向机器人发送一条消息"));
    }

    #[test]
    fn persisted_context_tokens_are_restored_from_plugin_config() {
        let mut config = make_config(Some("token"), Some("account"));
        config.credentials.extra.insert(
            CONTEXT_TOKENS_CONFIG_KEY.into(),
            serde_json::json!({ "chat-1": "context-persisted", "empty": "" }),
        );

        assert_eq!(
            configured_context_tokens(&config),
            vec![("chat-1".into(), "context-persisted".into())]
        );
    }

    #[test]
    fn persisted_poll_cursor_is_restored_from_plugin_config() {
        let mut config = make_config(Some("token"), Some("account"));
        config.credentials.extra.insert(
            POLL_STATE_CONFIG_KEY.into(),
            serde_json::json!({ POLL_CURSOR_ENTRY_KEY: "cursor-persisted" }),
        );

        assert_eq!(configured_poll_cursor(&config), "cursor-persisted");
    }

    #[tokio::test]
    async fn poll_cursor_update_uses_encrypted_credential_persistence_channel() {
        let (credential_tx, mut credential_rx) = tokio::sync::mpsc::channel(1);

        persist_poll_cursor(Some(&credential_tx), "cursor-next").await;

        let update = credential_rx.recv().await.unwrap();
        assert_eq!(update.map_key, POLL_STATE_CONFIG_KEY);
        assert_eq!(update.entry_key, POLL_CURSOR_ENTRY_KEY);
        assert_eq!(update.value, serde_json::json!("cursor-next"));
    }

    // -- Test helpers -----------------------------------------------------------

    fn make_text_item(text: &str) -> WeixinRawItem {
        WeixinRawItem {
            item_type: Some(ITEM_TYPE_TEXT),
            text_item: Some(super::super::types::TextItem {
                text: Some(text.into()),
            }),
            ..Default::default()
        }
    }

    fn make_config(bot_token: Option<&str>, account_id: Option<&str>) -> PluginConfig {
        PluginConfig {
            credentials: PluginCredentials {
                token: None,
                app_id: None,
                app_secret: None,
                encrypt_key: None,
                verification_token: None,
                client_id: None,
                client_secret: None,
                account_id: account_id.map(String::from),
                bot_token: bot_token.map(String::from),
                app_token: None,
                extra: HashMap::new(),
            },
            config: None,
        }
    }

    fn make_callbacks() -> PluginCallbacks {
        let (message_tx, _) = tokio::sync::mpsc::channel(16);
        let (confirm_tx, _) = tokio::sync::mpsc::channel(16);
        PluginCallbacks {
            message_tx,
            confirm_tx,
            credential_update_tx: None,
        }
    }
}
