//! antigravity harness: drives google's `agy` cli over its own stream-json
//! print mode — no adapter process in between.
//!
//! wire facts verified live against agy 1.2.2:
//! - prompts go over stdin as `{"event":"user","message":{"content":[{"type":"text","text":…}]}}`,
//!   one turn per line inside a single conversation; closing stdin ends the
//!   process after the running turn.
//! - `-p` consumes the next argument as its prompt, so the empty `-p=` that
//!   switches on print mode must be the last argument.
//! - without `--add-dir <cwd>` the file tools write into agy's own scratch
//!   project instead of the session's repo.
//! - headless mode cannot ask for permission: anything not pre-approved is
//!   auto-denied and listed in `result.denied_actions`, so runs skip
//!   permissions outright and only agy's own deny rules can still block a tool.
//! - the wire reports token counts but no context window, so windows come from
//!   a per-family table.
//! - stdin content blocks are text-only, so image attachments stay path refs
//!   inside the prompt text.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SlashCommand,
    SteeringMode, ToolCall,
};

use crate::{Harness, HarnessError, RunControls, Signal, send_signal, shutdown_child};

const CLI_NAME: &str = "agy";

fn agy_install_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        paths.push(home.join(".local").join("bin").join(CLI_NAME));
    }
    paths.push(PathBuf::from("/opt/homebrew/bin/agy"));
    paths.push(PathBuf::from("/usr/local/bin/agy"));
    paths
}

fn resolve_agy_executable() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("AGY_EXECUTABLE")
        && !path.is_empty()
    {
        return Some(PathBuf::from(path));
    }
    crate::acp::find_on_paths(CLI_NAME, agy_install_paths())
}

pub struct AntigravityHarness {
    executable: Option<PathBuf>,
    kill_grace: Duration,
    models_cache: tokio::sync::OnceCell<Vec<Model>>,
    commands_cache: tokio::sync::OnceCell<Vec<SlashCommand>>,
}

impl Default for AntigravityHarness {
    fn default() -> Self {
        Self {
            executable: None,
            kill_grace: Duration::from_secs(3),
            models_cache: tokio::sync::OnceCell::new(),
            commands_cache: tokio::sync::OnceCell::new(),
        }
    }
}

impl AntigravityHarness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = Some(path.into());
        self
    }

    pub fn with_kill_grace(mut self, kill_grace: Duration) -> Self {
        self.kill_grace = kill_grace;
        self
    }

    fn resolve_executable(&self) -> Result<PathBuf, HarnessError> {
        self.executable
            .clone()
            .or_else(resolve_agy_executable)
            .ok_or_else(|| {
                HarnessError::NotInstalled(
                    "agy (searched PATH, the login shell's PATH, ~/.local/bin, \
                     /opt/homebrew/bin and /usr/local/bin; set AGY_EXECUTABLE to override)"
                        .into(),
                )
            })
    }

    async fn discover_models(&self) -> Result<Vec<Model>, HarnessError> {
        let exe = self.resolve_executable()?;
        let mut cmd = Command::new(&exe);
        crate::compose_child_path(&mut cmd, &exe);
        cmd.arg("models")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(20), cmd.output())
            .await
            .map_err(|_| HarnessError::Protocol("agy models timed out".into()))??;
        Ok(parse_models(&String::from_utf8_lossy(&output.stdout)))
    }

    /// `/skills` is answered by the cli itself without a model turn. the cli's
    /// own built-in commands (`/usage`, `/model`, …) are refused under
    /// `--input-format stream-json`, so only skills are offered. it runs from
    /// home because the command list isn't scoped to a session, so workspace
    /// skills don't appear here but still expand when typed.
    async fn discover_commands(&self) -> Result<Vec<SlashCommand>, HarnessError> {
        let exe = self.resolve_executable()?;
        let mut cmd = Command::new(&exe);
        crate::compose_child_path(&mut cmd, &exe);
        cmd.args([
            "--output-format",
            "json",
            "--print-timeout",
            "1m",
            "-p=/skills",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
        if let Some(home) = std::env::var_os("HOME") {
            cmd.current_dir(home);
        }
        let output = tokio::time::timeout(Duration::from_secs(20), cmd.output())
            .await
            .map_err(|_| HarnessError::Protocol("agy /skills listing timed out".into()))??;
        let listing: Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| HarnessError::Protocol(format!("agy /skills listing: {e}")))?;
        Ok(parse_skill_commands(&listing))
    }
}

fn parse_skill_commands(listing: &Value) -> Vec<SlashCommand> {
    let mut seen = HashSet::new();
    listing
        .pointer("/command/data/skills")
        .and_then(Value::as_array)
        .map(|skills| skills.as_slice())
        .unwrap_or_default()
        .iter()
        .filter_map(|skill| {
            let name = skill
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())?;
            if !seen.insert(name.to_owned()) {
                return None;
            }
            let description = skill
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Some(SlashCommand {
                name: name.to_owned(),
                description: first_sentence(description),
                input_hint: None,
            })
        })
        .collect()
}

/// skill descriptions are model-facing paragraphs; the picker row only has
/// room for the opening sentence.
fn first_sentence(text: &str) -> String {
    let text = text.trim();
    match text.find(". ") {
        Some(end) => text[..=end].to_owned(),
        None => text.to_owned(),
    }
}

fn build_command(exe: &Path, request: &RunRequest, model: Option<&str>) -> Command {
    let mut cmd = Command::new(exe);
    crate::compose_child_path(&mut cmd, exe);
    cmd.args([
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        // the 5m default would kill long agentic turns mid-run
        "--print-timeout",
        "24h",
    ]);
    if !request.cwd.is_empty() {
        cmd.arg("--add-dir").arg(&request.cwd);
        cmd.current_dir(&request.cwd);
    }
    if let Some(model) = model {
        cmd.args(["--model", model]);
    }
    if let Some(resume) = request.resume.as_deref().filter(|r| !r.is_empty()) {
        cmd.args(["--conversation", resume]);
    }
    // zeron has no approval ui and agy's headless mode auto-denies anything it
    // would prompt for, so tools are always approved (parity with claude/codex)
    cmd.arg("--dangerously-skip-permissions");
    cmd.arg("-p=");
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

const EFFORT_VARIANTS: [(&str, &str, ReasoningLevel); 3] = [
    ("-low", " (Low)", ReasoningLevel::Low),
    ("-medium", " (Medium)", ReasoningLevel::Medium),
    ("-high", " (High)", ReasoningLevel::High),
];

/// `agy models` prints `id<TAB>label` rows after a progress line. agy lists
/// every effort as its own model id, so variants fold into one model whose
/// reasoning ladder drives the picker's toggle like the other harnesses.
fn parse_models(listing: &str) -> Vec<Model> {
    let mut models: Vec<Model> = Vec::new();
    for line in listing.lines() {
        let Some((id, label)) = line.split_once('\t') else {
            continue;
        };
        let (id, label) = (id.trim(), label.trim());
        if id.is_empty() || id.contains(char::is_whitespace) {
            continue;
        }
        let variant = EFFORT_VARIANTS
            .iter()
            .find_map(|(id_suffix, label_suffix, level)| {
                let base = id.strip_suffix(id_suffix)?;
                Some((
                    base,
                    label.strip_suffix(label_suffix).unwrap_or(label),
                    *level,
                ))
            });
        let (base_id, base_label, level) = match variant {
            Some((base_id, base_label, level)) => (base_id, base_label, Some(level)),
            None => (id, label, None),
        };
        let index = models
            .iter()
            .position(|model| model.id == base_id)
            .unwrap_or_else(|| {
                models.push(Model {
                    id: base_id.to_owned(),
                    label: base_label.to_owned(),
                    description: None,
                    reasoning_levels: Vec::new(),
                    options: Vec::new(),
                });
                models.len() - 1
            });
        let levels = &mut models[index].reasoning_levels;
        if let Some(level) = level
            && !levels.contains(&level)
        {
            levels.push(level);
            levels.sort();
        }
    }
    models
}

fn static_models() -> Vec<Model> {
    use ReasoningLevel::{High, Low, Medium};
    [
        (
            "gemini-3.8-flash",
            "Gemini 3.8 Flash",
            vec![Low, Medium, High],
        ),
        ("gemini-3.1-pro", "Gemini 3.1 Pro", vec![Low, High]),
        ("claude-sonnet-4-6", "Claude Sonnet 4.6 (Thinking)", vec![]),
    ]
    .into_iter()
    .map(|(id, label, reasoning_levels)| Model {
        id: id.into(),
        label: label.into(),
        description: None,
        reasoning_levels,
        options: Vec::new(),
    })
    .collect()
}

/// ids the catalog doesn't group pass through untouched, so full variant ids
/// saved by chats from before the grouping keep resuming on the same model.
fn variant_model_id(catalog: &[Model], model: &str, reasoning: Option<ReasoningLevel>) -> String {
    let Some(entry) = catalog.iter().find(|entry| entry.id == model) else {
        return model.to_owned();
    };
    let levels = &entry.reasoning_levels;
    let level = reasoning
        .filter(|level| levels.contains(level))
        .or_else(|| {
            [ReasoningLevel::High, ReasoningLevel::Medium]
                .into_iter()
                .find(|level| levels.contains(level))
        })
        .or_else(|| levels.first().copied());
    let suffix = level.and_then(|level| {
        EFFORT_VARIANTS
            .iter()
            .find(|(_, _, variant_level)| *variant_level == level)
            .map(|(id_suffix, _, _)| *id_suffix)
    });
    match suffix {
        Some(suffix) => format!("{model}{suffix}"),
        None => model.to_owned(),
    }
}

#[async_trait]
impl Harness for AntigravityHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Antigravity
    }
    fn display_name(&self) -> &str {
        "Antigravity"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }
    /// effort is baked into agy's model ids (`…-high`, `…-low`).
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[]
    }
    fn installed(&self) -> bool {
        self.executable.is_some() || resolve_agy_executable().is_some()
    }
    fn deterministic_turn_end(&self) -> bool {
        true
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        if let Some(models) = self.models_cache.get() {
            return Ok(models.clone());
        }
        self.resolve_executable()?;
        match self.discover_models().await {
            Ok(models) if !models.is_empty() => {
                let _ = self.models_cache.set(models.clone());
                Ok(models)
            }
            _ => Ok(static_models()),
        }
    }

    async fn commands(&self) -> Result<Vec<SlashCommand>, HarnessError> {
        if let Some(commands) = self.commands_cache.get() {
            return Ok(commands.clone());
        }
        let commands = self.discover_commands().await?;
        if !commands.is_empty() {
            let _ = self.commands_cache.set(commands.clone());
        }
        Ok(commands)
    }

    async fn run(
        &self,
        request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let exe = self.resolve_executable()?;
        let agy_model = match request.model.as_deref().filter(|m| !m.is_empty()) {
            Some(model) => {
                let catalog = self.models().await.unwrap_or_default();
                Some(variant_model_id(&catalog, model, request.reasoning))
            }
            None => None,
        };
        let mut child = build_command(&exe, &request, agy_model.as_deref())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    HarnessError::NotInstalled(exe.display().to_string())
                } else {
                    HarnessError::Io(e)
                }
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Protocol("agy child has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("agy child has no stdout".into()))?;
        let stderr_tail = crate::StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "zeron_harness::antigravity", "stderr: {line}");
                    tail.push(&line);
                }
            });
        }

        let (stdin_tx, stdin_rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(stdin_writer(stdin, stdin_rx));
        let _ = stdin_tx.send(user_message_line(&request.prompt));

        let (event_tx, event_rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(256);
        tokio::spawn(run_session(Session {
            child,
            stdout_lines: BufReader::new(stdout).lines(),
            stdin_tx,
            event_tx,
            controls,
            normalizer: Normalizer::new(request.model.unwrap_or_default(), request.cwd),
            kill_grace: self.kill_grace,
            stderr_tail,
        }));

        Ok(futures::stream::unfold(event_rx, |mut rx| async move {
            rx.recv().await.map(|ev| (ev, rx))
        })
        .boxed())
    }
}

fn user_message_line(prompt: &str) -> String {
    json!({
        "event": "user",
        "message": { "content": [{ "type": "text", "text": prompt }] },
    })
    .to_string()
}

async fn stdin_writer(mut stdin: ChildStdin, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = rx.recv().await {
        let write = async {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        };
        if let Err(e) = write.await {
            tracing::debug!(target: "zeron_harness::antigravity", "stdin write failed (tolerated): {e}");
            return;
        }
    }
    let _ = stdin.shutdown().await;
}

/// once prompt caching kicks in agy moves the cached prefix out of
/// `input_tokens` into `cache_read_tokens` (verified live: 18,580 input became
/// 2,573 input + 16,268 cached on the next step), so occupancy is their sum.
fn context_tokens(step: &Value) -> Option<u64> {
    let usage = step.get("usage")?;
    let input = usage.get("input_tokens").and_then(Value::as_u64)?;
    let cached = usage
        .get("cache_read_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(input + cached)
}

/// the published context sizes of the model families agy serves; an empty or
/// unrecognised model (agy picking its own default) leaves the limit unreported.
fn context_window(model: &str) -> Option<u64> {
    [
        ("gemini-", 1_048_576),
        ("claude-", 200_000),
        ("gpt-oss-", 131_072),
    ]
    .into_iter()
    .find(|(family, _)| model.starts_with(family))
    .map(|(_, window)| window)
}

struct Normalizer {
    model: String,
    cwd: String,
    session_id: Option<String>,
    started: bool,
    assistant_message_id: String,
    seen_tools: HashSet<String>,
}

impl Normalizer {
    fn new(model: String, cwd: String) -> Self {
        Self {
            model,
            cwd,
            session_id: None,
            started: false,
            assistant_message_id: new_message_id(),
            seen_tools: HashSet::new(),
        }
    }

    fn rotate_assistant_message(&mut self) -> (String, String) {
        let previous = std::mem::replace(&mut self.assistant_message_id, new_message_id());
        (previous, self.assistant_message_id.clone())
    }

    fn normalize(&mut self, frame: &Value) -> Vec<AgentEvent> {
        match frame.get("event").and_then(Value::as_str) {
            Some("init") => self.init(frame),
            Some("step_update") => frame
                .get("step_update")
                .map(|step| self.step(step))
                .unwrap_or_default(),
            Some("result") => frame
                .get("result")
                .map(|result| self.result(result))
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn init(&mut self, frame: &Value) -> Vec<AgentEvent> {
        if let Some(id) = frame.get("conversation_id").and_then(Value::as_str) {
            self.session_id = Some(id.to_owned());
        }
        if self.started {
            return Vec::new();
        }
        self.started = true;
        let init = frame.get("init");
        let tools = init
            .and_then(|i| i.get("tools"))
            .and_then(Value::as_array)
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let cwd = init
            .and_then(|i| i.get("cwd"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| self.cwd.clone());
        vec![AgentEvent::SessionStarted {
            harness: HarnessId::Antigravity,
            model: self.model.clone(),
            tools,
            cwd,
            session_id: self.session_id.clone().unwrap_or_default(),
            assistant_message_id: self.assistant_message_id.clone(),
        }]
    }

    fn step(&mut self, step: &Value) -> Vec<AgentEvent> {
        let state = step.get("state").and_then(Value::as_str).unwrap_or("");
        match step.get("step_type").and_then(Value::as_str) {
            Some("agent_response") => {
                let mut events = Vec::new();
                if let Some(text) = step
                    .get("text_delta")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    events.push(AgentEvent::TextDelta { text: text.into() });
                }
                if state == "DONE"
                    && let Some(tokens) = context_tokens(step)
                {
                    events.push(AgentEvent::ContextUsage {
                        tokens: Some(tokens),
                        window: context_window(&self.model),
                    });
                }
                events
            }
            Some("tool") => self.tool_step(step, state),
            _ => Vec::new(),
        }
    }

    fn tool_step(&mut self, step: &Value, state: &str) -> Vec<AgentEvent> {
        let Some(index) = step.get("step_index").and_then(Value::as_u64) else {
            return Vec::new();
        };
        let conversation = step
            .get("conversation_id")
            .and_then(Value::as_str)
            .or(self.session_id.as_deref())
            .unwrap_or("session");
        let id = format!("agy-{conversation}-{index}");
        let mut events = Vec::new();
        if self.seen_tools.insert(id.clone()) {
            events.push(AgentEvent::ToolCall {
                id: id.clone(),
                call: decode_tool(step),
            });
        }
        match state {
            "DONE" => events.push(AgentEvent::ToolResult {
                id,
                is_error: false,
                output: None,
                diff: None,
            }),
            "ERROR" => events.push(AgentEvent::ToolResult {
                id,
                is_error: true,
                output: step
                    .pointer("/tool_info/error/message")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                diff: None,
            }),
            _ => {}
        }
        events
    }

    fn result(&mut self, result: &Value) -> Vec<AgentEvent> {
        if let Some(id) = result.get("conversation_id").and_then(Value::as_str) {
            self.session_id = Some(id.to_owned());
        }
        let mut events = Vec::new();
        let denied: Vec<&str> = result
            .get("denied_actions")
            .and_then(Value::as_array)
            .map(|actions| {
                actions
                    .iter()
                    .filter_map(|action| {
                        action
                            .get("display_name")
                            .or_else(|| action.get("action"))
                            .and_then(Value::as_str)
                    })
                    .collect()
            })
            .unwrap_or_default();
        if !denied.is_empty() {
            events.push(AgentEvent::Error {
                message: format!(
                    "Antigravity denied {}. Check the deny rules under permissions in agy's settings.json.",
                    denied.join(", ")
                ),
            });
        }
        let succeeded = result.get("status").and_then(Value::as_str) == Some("SUCCESS");
        events.push(AgentEvent::Done {
            status: if succeeded {
                DoneStatus::Completed
            } else {
                DoneStatus::Errored
            },
            result: result
                .get("response")
                .and_then(Value::as_str)
                .filter(|response| !response.is_empty())
                .map(str::to_owned),
            error: (!succeeded).then(|| {
                result
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("agy reported a failed turn")
                    .to_owned()
            }),
            session_id: self.session_id.clone(),
        });
        events
    }
}

/// run_command, view_file, grep_search, find_by_name and write_to_file decoded
/// live against agy 1.2.2 (examples/antigravity_probe.rs); the edit and web
/// tool keys are tolerant guesses, and anything that doesn't match degrades to
/// an `Unknown` chip rather than a wrong one.
fn decode_tool(step: &Value) -> ToolCall {
    let name = step
        .get("tool_name")
        .or_else(|| step.pointer("/tool_info/name"))
        .and_then(Value::as_str)
        .unwrap_or("tool")
        .to_owned();
    let params = step
        .pointer("/tool_info/parameters")
        .cloned()
        .unwrap_or(Value::Null);
    let param = |keys: &[&str]| {
        keys.iter()
            .find_map(|key| params.get(*key).and_then(Value::as_str))
            .map(str::to_owned)
    };
    let decoded = match name.as_str() {
        "run_command" => {
            param(&["CommandLine", "Command"]).map(|command| ToolCall::Exec { command })
        }
        "view_file" => {
            param(&["AbsolutePath", "TargetFile", "File"]).map(|path| ToolCall::ReadFile { path })
        }
        "write_to_file" => param(&["TargetFile", "AbsolutePath"]).map(|path| ToolCall::WriteFile {
            path,
            content: None,
        }),
        "replace_file_content" | "multi_replace_file_content" | "sed_file" => {
            param(&["TargetFile", "AbsolutePath"]).map(|path| ToolCall::EditFile {
                path,
                old_string: None,
                new_string: None,
            })
        }
        "grep_search" => param(&["Query", "Pattern"]).map(|pattern| ToolCall::Search {
            pattern,
            path: param(&["SearchPath", "SearchDirectory"]),
        }),
        "find_by_name" => param(&["Pattern", "Query"]).map(|pattern| ToolCall::Glob { pattern }),
        "read_url_content" => {
            param(&["Url", "URL"]).map(|url| ToolCall::WebFetch { url, prompt: None })
        }
        "search_web" => param(&["query", "Query"]).map(|query| ToolCall::WebSearch { query }),
        _ => None,
    };
    decoded.unwrap_or_else(|| ToolCall::Unknown {
        name,
        input: (!params.is_null()).then_some(params),
    })
}

fn new_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

struct Session {
    child: Child,
    stdout_lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stdin_tx: mpsc::UnboundedSender<String>,
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    controls: RunControls,
    normalizer: Normalizer,
    kill_grace: Duration,
    stderr_tail: crate::StderrTail,
}

async fn run_session(session: Session) {
    let Session {
        mut child,
        mut stdout_lines,
        stdin_tx,
        event_tx,
        controls,
        mut normalizer,
        kill_grace,
        stderr_tail,
    } = session;
    let RunControls {
        request_input: _,
        mut steering,
        interrupt,
    } = controls;

    let mut steering_open = true;
    let mut interrupted = false;
    let mut turn_open = true;
    let mut parked = false;
    let mut queued_steers: VecDeque<String> = VecDeque::new();
    let mut escalation: Option<tokio::task::JoinHandle<()>> = None;

    'main: loop {
        tokio::select! {
            line = stdout_lines.next_line() => match line {
                Ok(Some(line)) => {
                    let Ok(frame) = serde_json::from_str::<Value>(line.trim()) else {
                        continue;
                    };
                    for event in normalizer.normalize(&frame) {
                        let event = match event {
                            AgentEvent::Done { result, session_id, .. } if interrupted => AgentEvent::Done {
                                status: DoneStatus::Interrupted,
                                result,
                                error: None,
                                session_id,
                            },
                            other => other,
                        };
                        let is_done = matches!(event, AgentEvent::Done { .. });
                        if event_tx.send(Ok(event)).await.is_err() {
                            break 'main;
                        }
                        if !is_done {
                            continue;
                        }
                        turn_open = false;
                        if interrupted {
                            break 'main;
                        }
                        if let Some(prompt) = queued_steers.pop_front() {
                            turn_open = true;
                            if !start_next_turn(&mut normalizer, &event_tx, &stdin_tx, &prompt).await {
                                break 'main;
                            }
                        } else if !steering_open {
                            break 'main;
                        } else {
                            parked = true;
                        }
                    }
                }
                Ok(None) => break 'main,
                Err(e) => {
                    let _ = event_tx.send(Err(HarnessError::Io(e))).await;
                    break 'main;
                }
            },

            steer = steering.recv(), if steering_open && !interrupted => match steer {
                Some(message) if parked => {
                    parked = false;
                    turn_open = true;
                    if !start_next_turn(&mut normalizer, &event_tx, &stdin_tx, &message.prompt).await {
                        break 'main;
                    }
                }
                Some(message) => queued_steers.push_back(message.prompt),
                None => {
                    steering_open = false;
                    if parked && queued_steers.is_empty() {
                        break 'main;
                    }
                }
            },

            // agy has no interrupt message on its stdin wire, so the process is signalled directly
            _ = interrupt.cancelled(), if !interrupted => {
                interrupted = true;
                if let Some(pid) = child.id() {
                    send_signal(pid, Signal::Term);
                    escalation = Some(tokio::spawn(async move {
                        tokio::time::sleep(kill_grace).await;
                        send_signal(pid, Signal::Kill);
                    }));
                }
            },

            _ = event_tx.closed() => break 'main,
        }
    }

    if turn_open && !event_tx.is_closed() {
        let done = if interrupted {
            AgentEvent::Done {
                status: DoneStatus::Interrupted,
                result: None,
                error: None,
                session_id: normalizer.session_id.clone(),
            }
        } else {
            let status = tokio::time::timeout(Duration::from_millis(500), child.wait())
                .await
                .ok()
                .and_then(Result::ok);
            AgentEvent::Done {
                status: DoneStatus::Errored,
                result: None,
                error: Some(crate::crash_message(CLI_NAME, status, &stderr_tail)),
                session_id: normalizer.session_id.clone(),
            }
        };
        let _ = event_tx.send(Ok(done)).await;
    }

    drop(stdin_tx);
    // closing stdin lets agy persist the conversation and exit on its own before any signal
    let _ = tokio::time::timeout(kill_grace, child.wait()).await;
    shutdown_child(&mut child, kill_grace).await;
    if let Some(handle) = escalation {
        handle.abort();
    }
}

async fn start_next_turn(
    normalizer: &mut Normalizer,
    event_tx: &mpsc::Sender<Result<AgentEvent, HarnessError>>,
    stdin_tx: &mpsc::UnboundedSender<String>,
    prompt: &str,
) -> bool {
    let (previous, next) = normalizer.rotate_assistant_message();
    let steered = AgentEvent::Steered {
        assistant_message_id: Some(previous),
        next_assistant_message_id: Some(next),
    };
    if event_tx.send(Ok(steered)).await.is_err() {
        return false;
    }
    stdin_tx.send(user_message_line(prompt)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skills_listing_maps_to_commands_with_short_descriptions() {
        let listing = json!({
            "status": "SUCCESS",
            "command": {"name": "skills", "data": {"skills": [
                {"name": "generative_ui", "description": "How to render rich widgets inline. Use this skill when you want diagrams.", "builtin": true},
                {"name": "find-skills", "description": "Helps users discover skills", "builtin": false},
                {"name": "find-skills", "description": "duplicate", "builtin": false},
                {"name": "", "description": "no name"}
            ]}}
        });
        let commands = parse_skill_commands(&listing);
        assert_eq!(
            commands,
            [
                SlashCommand {
                    name: "generative_ui".into(),
                    description: "How to render rich widgets inline.".into(),
                    input_hint: None,
                },
                SlashCommand {
                    name: "find-skills".into(),
                    description: "Helps users discover skills".into(),
                    input_hint: None,
                },
            ]
        );
        assert!(parse_skill_commands(&json!({"status": "ERROR"})).is_empty());
    }

    #[test]
    fn groups_effort_variants_and_skips_the_progress_line() {
        let listing = "Fetching available models...\n\
                       gemini-3.8-flash-high\tGemini 3.8 Flash (High)\n\
                       gemini-3.8-flash-medium\tGemini 3.8 Flash (Medium)\n\
                       gemini-3.8-flash-low\tGemini 3.8 Flash (Low)\n\
                       claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)\n\
                       gpt-oss-120b-medium\tGPT-OSS 120B (Medium)\n";
        let models = parse_models(listing);
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            ["gemini-3.8-flash", "claude-sonnet-4-6", "gpt-oss-120b"]
        );
        assert_eq!(models[0].label, "Gemini 3.8 Flash");
        assert_eq!(
            models[0].reasoning_levels,
            [
                ReasoningLevel::Low,
                ReasoningLevel::Medium,
                ReasoningLevel::High
            ]
        );
        assert!(models[1].reasoning_levels.is_empty());
        assert_eq!(models[2].reasoning_levels, [ReasoningLevel::Medium]);
    }

    #[test]
    fn variant_id_follows_the_pick_and_clamps_to_offered_levels() {
        let catalog = static_models();
        let id = |model, reasoning| variant_model_id(&catalog, model, reasoning);
        assert_eq!(
            id("gemini-3.8-flash", Some(ReasoningLevel::Medium)),
            "gemini-3.8-flash-medium"
        );
        assert_eq!(
            id("gemini-3.1-pro", Some(ReasoningLevel::Medium)),
            "gemini-3.1-pro-high"
        );
        assert_eq!(id("gemini-3.1-pro", None), "gemini-3.1-pro-high");
        assert_eq!(
            id("claude-sonnet-4-6", Some(ReasoningLevel::High)),
            "claude-sonnet-4-6"
        );
        assert_eq!(id("gemini-3.8-flash-low", None), "gemini-3.8-flash-low");
    }

    #[test]
    fn decodes_observed_tools_and_falls_back_to_unknown() {
        let run = json!({"tool_name": "run_command", "tool_info": {"parameters": {"CommandLine": "ls -la"}}});
        assert_eq!(
            decode_tool(&run),
            ToolCall::Exec {
                command: "ls -la".into()
            }
        );
        let write = json!({"tool_name": "write_to_file", "tool_info": {"parameters": {"TargetFile": "/w/a.txt"}}});
        assert_eq!(
            decode_tool(&write),
            ToolCall::WriteFile {
                path: "/w/a.txt".into(),
                content: None
            }
        );
        let other =
            json!({"tool_name": "generate_image", "tool_info": {"parameters": {"Prompt": "fox"}}});
        assert!(
            matches!(decode_tool(&other), ToolCall::Unknown { name, .. } if name == "generate_image")
        );
    }

    #[test]
    fn context_usage_carries_the_model_family_window() {
        let step = json!({
            "event": "step_update",
            "step_update": {
                "step_index": 1,
                "state": "DONE",
                "step_type": "agent_response",
                "usage": {"input_tokens": 18171}
            }
        });
        let usage = |model: &str| Normalizer::new(model.into(), "/w".into()).normalize(&step);
        assert_eq!(
            usage("gemini-3.8-flash"),
            [AgentEvent::ContextUsage {
                tokens: Some(18171),
                window: Some(1_048_576)
            }]
        );
        assert_eq!(
            usage("claude-sonnet-4-6"),
            [AgentEvent::ContextUsage {
                tokens: Some(18171),
                window: Some(200_000)
            }]
        );
        assert_eq!(
            usage(""),
            [AgentEvent::ContextUsage {
                tokens: Some(18171),
                window: None
            }]
        );
    }

    #[test]
    fn context_usage_counts_cached_tokens() {
        let mut normalizer = Normalizer::new("gemini-3.8-flash".into(), "/w".into());
        let events = normalizer.normalize(&json!({
            "event": "step_update",
            "step_update": {
                "step_index": 7,
                "state": "DONE",
                "step_type": "agent_response",
                "usage": {"input_tokens": 2573, "cache_read_tokens": 16268, "output_tokens": 127}
            }
        }));
        assert_eq!(
            events,
            [AgentEvent::ContextUsage {
                tokens: Some(18841),
                window: Some(1_048_576)
            }]
        );
    }

    #[test]
    fn denied_actions_surface_before_done() {
        let mut normalizer = Normalizer::new("m".into(), "/w".into());
        let events = normalizer.normalize(&json!({
            "event": "result",
            "result": {
                "conversation_id": "c1",
                "status": "SUCCESS",
                "response": "",
                "denied_actions": [{"action": "command", "display_name": "RunCommand"}]
            }
        }));
        assert!(
            matches!(&events[0], AgentEvent::Error { message } if message.contains("RunCommand"))
        );
        assert_eq!(
            events[1],
            AgentEvent::Done {
                status: DoneStatus::Completed,
                result: None,
                error: None,
                session_id: Some("c1".into()),
            }
        );
    }
}
