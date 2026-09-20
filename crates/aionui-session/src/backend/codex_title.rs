//! Legacy Codex title-generation state.
//!
//! Production title generation moved to the SayDone client. The state-machine
//! code remains only for compatibility with existing test-support callers; the
//! production backend supplies [`NoTitleIo`] and keeps the latch disarmed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::broadcast;

use crate::adapter::AgentIo;
use crate::backend::types::SessionEnvelope;
use crate::event::SessionEvent;

/// What to generate a title from, and the codex knobs to do it with.
pub(crate) struct TitleRequest {
    /// `User: …\n\nAssistant: …` — the same description shape the claude path
    /// feeds `generate_session_title`. Prompt content: never logged.
    pub description: String,
    /// The session's current model, so the title comes from the same model the
    /// user is already talking to. `None` = let codex pick its default.
    pub model: Option<String>,
}

/// Legacy prompt retained only for compatibility with the test-support exchange.
const TITLE_INSTRUCTIONS: &str = "You generate short titles for chat conversations. \
Reply with ONLY the title: at most 6 words, no quotes, no trailing punctuation, \
in the same language as the conversation.";

/// Hard deadline for the whole exchange. Measured end-to-end (spawn +
/// initialize + one ephemeral turn): 4.5–6.5s, so this is generous headroom for
/// a cold start, not a tuning knob. Its real job is to bound the wait when
/// codex accepts the turn and then goes silent — without it the task would
/// await forever and leak one codex process per affected conversation.
const TITLE_TIMEOUT_SECS: u64 = 60;

/// Run the whole one-shot exchange on `io` and return the title.
///
/// `None` for any failure (spawn/stdio gone, codex rejected a request, no
/// answer, timeout): title generation is best-effort and must never affect the
/// turn path. The caller keeps its latch armed and retries on a later turn.
///
/// The process is force-terminated on EVERY exit path — this is a throwaway
/// session with no reason to outlive the call.
pub(crate) async fn generate_title(io: Box<dyn AgentIo>, req: &TitleRequest) -> Option<String> {
    let timeout = std::time::Duration::from_secs(TITLE_TIMEOUT_SECS);
    let outcome = tokio::time::timeout(timeout, run_exchange(io.as_ref(), req)).await;
    io.terminate().await;
    match outcome {
        Ok(title) => title,
        Err(_) => {
            tracing::warn!(
                timeout_secs = TITLE_TIMEOUT_SECS,
                "codex title generation timed out; conversation keeps its placeholder name for now"
            );
            None
        }
    }
}

async fn run_exchange(io: &dyn AgentIo, req: &TitleRequest) -> Option<String> {
    let (mut stdin, stdout) = io.take_stdio().await?;
    let mut lines = BufReader::new(stdout).lines();

    write_frame(
        &mut stdin,
        &json!({
            "id": 1,
            "method": "initialize",
            "params": { "clientInfo": { "name": "aionui-title", "version": "1" } },
        }),
    )
    .await?;
    read_response(&mut lines, 1).await?;
    write_frame(&mut stdin, &json!({ "method": "initialized", "params": {} })).await?;

    let mut params = json!({
        // Live-verified: an ephemeral thread writes no rollout file and no
        // session_index entry, so this never pollutes `codex resume`.
        "ephemeral": true,
        "baseInstructions": TITLE_INSTRUCTIONS,
    });
    if let Some(model) = &req.model {
        params["model"] = json!(model);
    }
    write_frame(
        &mut stdin,
        &json!({ "id": 2, "method": "thread/start", "params": params }),
    )
    .await?;
    let thread_id = read_response(&mut lines, 2)
        .await?
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)?
        .to_string();

    write_frame(
        &mut stdin,
        &json!({
            "id": 3,
            "method": "turn/start",
            "params": {
                "threadId": thread_id,
                "input": [{ "type": "text", "text": req.description }],
            },
        }),
    )
    .await?;

    read_agent_message(&mut lines).await
}

async fn write_frame(stdin: &mut aionui_process::BoxedStdin, frame: &Value) -> Option<()> {
    let mut line = serde_json::to_vec(frame).ok()?;
    line.push(b'\n');
    stdin.write_all(&line).await.ok()?;
    stdin.flush().await.ok()?;
    Some(())
}

/// Read until the JSON-RPC response for `id`; `None` if the stream ends or
/// codex answered with an `error` instead of a `result`.
async fn read_response<R>(lines: &mut tokio::io::Lines<R>, id: u64) -> Option<Value>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if frame.get("id").and_then(Value::as_u64) == Some(id) {
            return frame.get("result").cloned();
        }
    }
    None
}

/// The title rides the completed `agentMessage` item's `text`.
async fn read_agent_message<R>(lines: &mut tokio::io::Lines<R>) -> Option<String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if frame.get("method").and_then(Value::as_str) != Some("item/completed") {
            continue;
        }
        let item = frame.get("params").and_then(|p| p.get("item"));
        if item.and_then(|i| i.get("type")).and_then(Value::as_str) != Some("agentMessage") {
            continue;
        }
        let text = item
            .and_then(|i| i.get("text"))
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    None
}

/// Where a title run gets its io. The production path no longer opens a source;
/// tests can still inject scripted frames while the legacy state-machine code is
/// retained for compatibility with existing test-support callers.
#[async_trait::async_trait]
pub(crate) trait TitleIoSource: Send + Sync {
    /// A brand-new io for ONE title run, or `None` if it could not be opened.
    async fn open(&self) -> Option<Box<dyn AgentIo>>;
}

/// Max title runs per session (initial try + retries), mirroring the claude
/// latch. Each run is a real codex turn (~10k input tokens), so the cap is what
/// keeps a pathological session from generating titles forever.
const TITLE_MAX_ATTEMPTS: u32 = 3;

/// Ceiling on title runs happening at once, PROCESS-WIDE. Each run is a whole
/// codex process (~10k input tokens, ~5s), so without this a burst — several
/// conversations finishing their first turn together, or a batch of
/// cron-created ones — would spawn one codex per conversation simultaneously.
/// Excess runs queue on the semaphore instead; a queued run still holds its
/// session's in-flight slot, so no session ever doubles up.
const MAX_CONCURRENT_TITLE_RUNS: usize = 2;

static TITLE_SLOTS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(MAX_CONCURRENT_TITLE_RUNS));

/// A source that can never open anything — used wherever a backend is built
/// including production, so the legacy latch is present but inert.
pub(crate) struct NoTitleIo;

#[async_trait::async_trait]
impl TitleIoSource for NoTitleIo {
    async fn open(&self) -> Option<Box<dyn AgentIo>> {
        None
    }
}

/// Legacy first-turn title latch retained for test-support compatibility.
/// Production construction always passes `fresh = false` and [`NoTitleIo`].
pub(crate) struct TitleGen {
    pending: AtomicBool,
    attempts: AtomicU32,
    inflight: AtomicBool,
    /// First user prompt of the first turn. Cloned (not consumed) on fire so a
    /// retry keeps the user part. Prompt content: never logged.
    description: std::sync::Mutex<Option<String>>,
    /// The session's current model, read at fire time so the title comes from
    /// the model the user is actually talking to.
    model: std::sync::Mutex<Option<String>>,
    source: Arc<dyn TitleIoSource>,
}

impl TitleGen {
    pub(crate) fn new(fresh: bool, source: Arc<dyn TitleIoSource>) -> Self {
        Self {
            pending: AtomicBool::new(fresh),
            attempts: AtomicU32::new(0),
            inflight: AtomicBool::new(false),
            description: std::sync::Mutex::new(None),
            model: std::sync::Mutex::new(None),
            source,
        }
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.pending.load(Ordering::SeqCst)
    }

    /// Record the first prompt's text (first non-empty wins), bounded like the
    /// claude path so a huge prompt cannot blow up the description.
    pub(crate) fn record_first_prompt(&self, text: &str) {
        if !self.is_armed() {
            return;
        }
        let mut slot = self.description.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            let bounded: String = text.chars().take(1000).collect();
            if !bounded.is_empty() {
                *slot = Some(bounded);
            }
        }
    }

    /// Track the session's current model so a title run reuses it.
    pub(crate) fn set_model(&self, model: Option<String>) {
        *self.model.lock().unwrap_or_else(|e| e.into_inner()) = model;
    }

    /// Fire a title run for the just-completed successful turn. Detached: the
    /// reader must never block on a title.
    pub(crate) fn fire(
        self: &Arc<Self>,
        session_id: &str,
        result_text: &str,
        event_tx: &broadcast::Sender<SessionEnvelope>,
        turn_gen: u64,
    ) {
        if !self.is_armed() {
            return;
        }
        // Reserve synchronously on the reader thread: one run at a time,
        // bounded total attempts.
        if self.inflight.swap(true, Ordering::SeqCst) {
            return;
        }
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt > TITLE_MAX_ATTEMPTS {
            self.pending.store(false, Ordering::SeqCst);
            self.inflight.store(false, Ordering::SeqCst);
            tracing::warn!(
                session_id,
                max_attempts = TITLE_MAX_ATTEMPTS,
                "codex title generation exhausted retries; conversation keeps its placeholder name"
            );
            return;
        }

        let this = self.clone();
        let session_id = session_id.to_string();
        let event_tx = event_tx.clone();
        let assistant_part: String = result_text.chars().take(1000).collect();
        tokio::spawn(async move {
            let description = this.build_description(&assistant_part);
            let model = this.model.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let description_len = description.chars().count();
            tracing::info!(session_id, attempt, description_len, "codex title generation started");

            // Queue behind the process-wide cap BEFORE opening anything, so a
            // burst of conversations cannot spawn one codex each at once.
            let _slot = TITLE_SLOTS.acquire().await;
            let title = match this.source.open().await {
                Some(io) => generate_title(io, &TitleRequest { description, model }).await,
                None => {
                    tracing::warn!(session_id, "codex title generation could not start a process");
                    None
                }
            };
            this.inflight.store(false, Ordering::SeqCst);

            let Some(title) = title else {
                // Latch stays armed: the next successful turn retries.
                tracing::warn!(session_id, attempt, "codex title generation produced no title");
                return;
            };
            this.pending.store(false, Ordering::SeqCst);
            tracing::info!(
                session_id,
                attempt,
                title_len = title.chars().count(),
                "codex title generation succeeded"
            );
            let _ = event_tx.send(SessionEnvelope {
                session_id,
                turn_gen,
                event: SessionEvent::SessionTitle { title },
            });
        });
    }

    /// `User: …\n\nAssistant: …` — the same shape the claude path proved works
    /// (a bare short prompt makes title generation return nothing useful).
    fn build_description(&self, assistant_part: &str) -> String {
        let user_part = self
            .description
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_default();
        let mut out = String::new();
        if !user_part.is_empty() {
            out.push_str("User: ");
            out.push_str(&user_part);
        }
        if !assistant_part.is_empty() {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str("Assistant: ");
            out.push_str(assistant_part);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeAgentIo;

    /// A source that replays scripted codex frames instead of spawning.
    struct ScriptedSource(std::sync::Mutex<Vec<Vec<u8>>>);

    #[async_trait::async_trait]
    impl TitleIoSource for ScriptedSource {
        async fn open(&self) -> Option<Box<dyn AgentIo>> {
            let bytes = self.0.lock().unwrap().pop()?;
            Some(Box::new(FakeAgentIo::never_exits(bytes)))
        }
    }

    fn scripted_source(runs: Vec<Vec<u8>>) -> Arc<dyn TitleIoSource> {
        // popped from the back, so reverse to get first-run-first ordering.
        Arc::new(ScriptedSource(std::sync::Mutex::new(runs.into_iter().rev().collect())))
    }

    async fn next_title(rx: &mut broadcast::Receiver<SessionEnvelope>) -> Option<String> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(env)) => {
                    if let SessionEvent::SessionTitle { title } = env.event {
                        return Some(title);
                    }
                }
                Ok(Err(_)) => return None,
                Err(_) => continue,
            }
        }
        None
    }

    #[tokio::test]
    async fn a_fresh_latch_emits_the_generated_title() {
        let (tx, mut rx) = broadcast::channel(16);
        let latch = Arc::new(TitleGen::new(true, scripted_source(vec![scripted_codex()])));
        latch.record_first_prompt("my login is broken");

        latch.fire("s-1", "I fixed the redirect.", &tx, 0);

        assert_eq!(next_title(&mut rx).await.as_deref(), Some("Fix login bug"));
    }

    /// The wiring itself: a Fresh codex backend, driven through one SUCCESSFUL
    /// turn on its own connection, must produce a SessionTitle. Proves the
    /// reader's terminal actually reaches the latch.
    #[tokio::test]
    async fn the_backend_titles_a_fresh_session_after_a_successful_turn() {
        let session_frames = format!(
            "{}\n",
            [
                r#"{"jsonrpc":"2.0","method":"thread/started","params":{"thread":{"id":"th1"}}}"#,
                r#"{"jsonrpc":"2.0","method":"turn/started","params":{"threadId":"th1","turn":{"id":"t1"}}}"#,
                r#"{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"th1","turn":{"id":"t1","status":"completed"}}}"#,
            ]
            .join("\n")
        );
        let fake = FakeAgentIo::never_exits(session_frames.into_bytes());
        let latch = Arc::new(TitleGen::new(true, scripted_source(vec![scripted_codex()])));

        let backend = crate::backend::codex_conn::CodexSessionBackend::build_with_io_titled(
            "codex-title-wiring",
            Box::new(fake),
            latch,
        )
        .await;

        use crate::backend::SessionBackend as _;
        use futures_util::StreamExt as _;
        let mut events = backend.events();
        let mut got = None;
        for _ in 0..50 {
            match tokio::time::timeout(std::time::Duration::from_millis(500), events.next()).await {
                Ok(Some(env)) => {
                    if let SessionEvent::SessionTitle { title } = env.event {
                        got = Some(title);
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }
        assert_eq!(got.as_deref(), Some("Fix login bug"));
    }

    #[tokio::test]
    async fn a_resumed_latch_never_fires() {
        let (tx, mut rx) = broadcast::channel(16);
        // fresh=false → an existing conversation; its name is already settled.
        let latch = Arc::new(TitleGen::new(false, scripted_source(vec![scripted_codex()])));
        latch.record_first_prompt("my login is broken");

        latch.fire("s-1", "I fixed the redirect.", &tx, 0);

        assert_eq!(next_title(&mut rx).await, None);
    }

    /// Frame shapes are the captured ones, not invented:
    /// `~/aion/protocols/samples/codex-cli/0.144.6/ephemeral_title_thread_roundtrip.jsonl`
    /// (`thread/start` → `result.thread.id`; the title rides
    /// `item/completed.params.item.text` on a `type:"agentMessage"`).
    fn scripted_codex() -> Vec<u8> {
        concat!(
            r#"{"id":1,"result":{"userAgent":"codex","codexHome":"/tmp","platformOs":"macos"}}"#,
            "\n",
            r#"{"id":2,"result":{"thread":{"id":"th-1","ephemeral":true,"preview":""}}}"#,
            "\n",
            r#"{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg_1","text":"Fix login bug","phase":"final_answer"},"threadId":"th-1","turnId":"t-1"}}"#,
            "\n",
            r#"{"method":"turn/completed","params":{"threadId":"th-1"}}"#,
            "\n",
        )
        .as_bytes()
        .to_vec()
    }

    #[tokio::test]
    async fn returns_the_agent_message_as_the_title() {
        let fake = FakeAgentIo::never_exits(scripted_codex());

        let title = generate_title(
            Box::new(fake),
            &TitleRequest {
                description: "User: my login is broken\n\nAssistant: I fixed the redirect.".to_string(),
                model: Some("gpt-5".to_string()),
            },
        )
        .await;

        assert_eq!(title.as_deref(), Some("Fix login bug"));
    }

    /// The handshake succeeds but the title turn never produces anything, and
    /// the stream stays OPEN (unreleased gated tail) — i.e. codex accepted our
    /// requests and then went silent. Without a deadline this awaits forever,
    /// which would leak a codex process per affected conversation.
    #[tokio::test(start_paused = true)]
    async fn gives_up_when_codex_accepts_the_turn_then_goes_silent() {
        let handshake = concat!(
            r#"{"id":1,"result":{"userAgent":"codex","codexHome":"/tmp"}}"#,
            "\n",
            r#"{"id":2,"result":{"thread":{"id":"th-1","ephemeral":true}}}"#,
            "\n",
        );
        let fake = FakeAgentIo::never_exits(handshake.as_bytes().to_vec()).with_gated_tail(Vec::new());

        let title = generate_title(
            Box::new(fake),
            &TitleRequest {
                description: "User: hi\n\nAssistant: hello".to_string(),
                model: None,
            },
        )
        .await;

        assert!(title.is_none(), "a silent codex must time out, not hang");
    }

    #[tokio::test]
    async fn starts_an_ephemeral_thread_on_the_session_model() {
        // The throwaway thread MUST be ephemeral (no rollout file, never in the
        // user's `codex resume` picker) and MUST reuse the session's model.
        let fake = FakeAgentIo::never_exits(scripted_codex());
        let captured = fake.captured_stdin();

        let _ = generate_title(
            Box::new(fake),
            &TitleRequest {
                description: "User: hi\n\nAssistant: hello".to_string(),
                model: Some("gpt-5.4".to_string()),
            },
        )
        .await;

        // captured_stdin is drained by a spawned task over a 256-byte duplex, and
        // the thread/start frame is larger than that — let the drain catch up
        // before asserting on the wire.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let written = String::from_utf8_lossy(&captured.lock().await.clone()).to_string();
        let start_frame = written
            .lines()
            .find(|l| l.contains(r#""method":"thread/start""#))
            .unwrap_or_else(|| panic!("no thread/start on the wire: {written}"));
        assert!(start_frame.contains(r#""ephemeral":true"#), "frame: {start_frame}");
        assert!(start_frame.contains(r#""model":"gpt-5.4""#), "frame: {start_frame}");
    }
}
