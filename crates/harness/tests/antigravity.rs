//! AntigravityHarness integration tests against the fake cli in
//! `tests/fixtures/fake-agy.sh` (no real `agy` binary involved).

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::{mpsc, oneshot};

use zeron_harness::{
    AntigravityHarness, CancellationToken, Harness, HarnessError, RunControls, SteerMessage,
};
use zeron_proto::{AgentEvent, DoneStatus, ReasoningLevel, RunRequest, SandboxLevel, ToolCall};

type EventStream = BoxStream<'static, Result<AgentEvent, HarnessError>>;

fn fixture_path() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake-agy.sh");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755));
    }
    path
}

fn harness() -> AntigravityHarness {
    AntigravityHarness::new()
        .with_executable(fixture_path())
        .with_kill_grace(Duration::from_millis(300))
}

fn request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: Some("gemini-3.8-flash-high".into()),
        reasoning: None,
        model_options: serde_json::Map::new(),
        cwd: std::env::temp_dir().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: false,
        attachments: Vec::new(),
        worktree: None,
        resume: None,
    }
}

fn controls() -> (RunControls, mpsc::Sender<SteerMessage>, CancellationToken) {
    let (steer_tx, steer_rx) = mpsc::channel(8);
    let interrupt = CancellationToken::new();
    let controls = RunControls {
        request_input: Box::new(|_| oneshot::channel().1),
        steering: steer_rx,
        interrupt: interrupt.clone(),
    };
    (controls, steer_tx, interrupt)
}

async fn next_event(stream: &mut EventStream) -> Option<AgentEvent> {
    tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for an event")
        .map(|event| event.expect("harness error"))
}

async fn collect_until_done(stream: &mut EventStream) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    while let Some(event) = next_event(stream).await {
        let is_done = matches!(event, AgentEvent::Done { .. });
        events.push(event);
        if is_done {
            break;
        }
    }
    events
}

async fn collect_to_end(stream: &mut EventStream) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    while let Some(event) = next_event(stream).await {
        events.push(event);
    }
    events
}

async fn run_single_turn(req: RunRequest) -> Vec<AgentEvent> {
    let (controls, _, _) = controls();
    let mut stream = harness().run(req, controls).await.unwrap();
    collect_to_end(&mut stream).await
}

#[tokio::test]
async fn text_turn_streams_and_completes() {
    let events = run_single_turn(request("hello")).await;

    assert!(matches!(
        events.first(),
        Some(AgentEvent::SessionStarted { session_id, model, .. })
            if session_id == "conv-fake" && model == "gemini-3.8-flash-high"
    ));
    assert!(events.contains(&AgentEvent::TextDelta {
        text: "reply 1".into()
    }));
    assert_eq!(
        events.last(),
        Some(&AgentEvent::Done {
            status: DoneStatus::Completed,
            result: Some("reply 1".into()),
            error: None,
            session_id: Some("conv-fake".into()),
        })
    );
}

#[tokio::test]
async fn tool_steps_become_a_call_and_its_result() {
    let events = run_single_turn(request("use a tool")).await;

    let call_id = events
        .iter()
        .find_map(|event| match event {
            AgentEvent::ToolCall { id, call } => {
                assert_eq!(
                    call,
                    &ToolCall::Exec {
                        command: "ls -la".into()
                    }
                );
                Some(id.clone())
            }
            _ => None,
        })
        .expect("tool call");
    let calls = events
        .iter()
        .filter(|event| matches!(event, AgentEvent::ToolCall { .. }))
        .count();
    assert_eq!(calls, 1, "ACTIVE and DONE must fold into one call");
    assert!(events.contains(&AgentEvent::ToolResult {
        id: call_id,
        is_error: false,
        output: None,
        diff: None,
    }));
}

#[tokio::test]
async fn denied_action_fails_the_tool_and_explains_why() {
    let events = run_single_turn(request("deny")).await;

    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolResult { is_error: true, output: Some(output), .. }
            if output.contains("permission check failed")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::Error { message } if message.contains("RunCommand")
    )));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn steer_after_done_runs_the_next_turn_in_the_same_process() {
    let (controls, steer_tx, _interrupt) = controls();
    let mut stream = harness().run(request("hello"), controls).await.unwrap();

    let first = collect_until_done(&mut stream).await;
    assert!(first.contains(&AgentEvent::TextDelta {
        text: "reply 1".into()
    }));

    steer_tx
        .send(SteerMessage {
            prompt: "again".into(),
            message_id: None,
        })
        .await
        .unwrap();
    drop(steer_tx);
    let rest = collect_to_end(&mut stream).await;

    assert!(matches!(rest.first(), Some(AgentEvent::Steered { .. })));
    assert!(
        rest.contains(&AgentEvent::TextDelta {
            text: "reply 2".into()
        }),
        "the fake numbers turns per process, so reply 2 proves the session stayed open"
    );
    assert!(matches!(
        rest.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn resume_passes_the_conversation_id() {
    let mut req = request("hello");
    req.resume = Some("conv-resume".into());
    let events = run_single_turn(req).await;

    assert!(matches!(
        events.first(),
        Some(AgentEvent::SessionStarted { session_id, .. }) if session_id == "conv-resume"
    ));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done { session_id: Some(id), .. }) if id == "conv-resume"
    ));
}

#[tokio::test]
async fn crash_mid_turn_reports_the_stderr_tail() {
    let events = run_single_turn(request("crash")).await;

    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done { status: DoneStatus::Errored, error: Some(error), .. })
            if error.contains("boom: fake agy crashed")
    ));
}

#[tokio::test]
async fn interrupt_ends_the_run_as_interrupted() {
    let (controls, _steer_tx, interrupt) = controls();
    let mut stream = harness().run(request("hang"), controls).await.unwrap();

    assert!(matches!(
        next_event(&mut stream).await,
        Some(AgentEvent::SessionStarted { .. })
    ));
    interrupt.cancel();
    let events = collect_to_end(&mut stream).await;

    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Interrupted,
            ..
        })
    ));
}

#[tokio::test]
async fn effort_variants_group_into_one_model_with_a_reasoning_ladder() {
    let models = harness().models().await.unwrap();
    let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();
    assert_eq!(ids, ["gemini-3.8-flash", "claude-opus-4-6-thinking"]);
    assert_eq!(models[0].label, "Gemini 3.8 Flash");
    assert_eq!(
        models[0].reasoning_levels,
        [ReasoningLevel::Low, ReasoningLevel::High]
    );
    assert!(models[1].reasoning_levels.is_empty());
}

async fn model_arg_for(model: &str, reasoning: Option<ReasoningLevel>) -> String {
    let mut req = request("which model");
    req.model = Some(model.into());
    req.reasoning = reasoning;
    run_single_turn(req)
        .await
        .into_iter()
        .find_map(|event| match event {
            AgentEvent::TextDelta { text } => text.strip_prefix("model ").map(str::to_owned),
            _ => None,
        })
        .expect("fake echoes the --model argument")
}

#[tokio::test]
async fn picked_reasoning_selects_the_matching_model_variant() {
    assert_eq!(
        model_arg_for("gemini-3.8-flash", Some(ReasoningLevel::Low)).await,
        "gemini-3.8-flash-low"
    );
    assert_eq!(
        model_arg_for("gemini-3.8-flash", None).await,
        "gemini-3.8-flash-high",
        "no pick defaults to high, matching the picker"
    );
    assert_eq!(
        model_arg_for("gemini-3.8-flash", Some(ReasoningLevel::Max)).await,
        "gemini-3.8-flash-high",
        "an unoffered level clamps to the default"
    );
    assert_eq!(
        model_arg_for("gemini-3.8-flash-low", None).await,
        "gemini-3.8-flash-low",
        "a full variant id from an older chat passes through"
    );
    assert_eq!(
        model_arg_for("claude-opus-4-6-thinking", Some(ReasoningLevel::High)).await,
        "claude-opus-4-6-thinking"
    );
}
