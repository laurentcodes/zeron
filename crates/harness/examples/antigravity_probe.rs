//! live probe: drive the real agy cli through AntigravityHarness for two
//! turns (the second as a steer) and print the normalized event stream, so
//! tool decoding and turn-boundary steering can be checked against the real
//! wire. needs a logged-in `agy` on PATH.
//!
//!     cargo run -p zeron-harness --example antigravity_probe -- /tmp/probe-agy

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use zeron_harness::{AntigravityHarness, CancellationToken, Harness, RunControls, SteerMessage};
use zeron_proto::{AgentEvent, RunRequest, SandboxLevel};

#[tokio::main]
async fn main() {
    let cwd = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/probe-agy".into());
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(
        format!("{cwd}/notes.md"),
        "# Notes\n\nThe quick brown fox.\n",
    )
    .unwrap();

    let harness = AntigravityHarness::new();
    match harness.models().await {
        Ok(models) => eprintln!(
            "MODELS {:?}",
            models.iter().map(|m| &m.id).collect::<Vec<_>>()
        ),
        Err(e) => eprintln!("MODELS ERR {e}"),
    }

    let (steer_tx, steering) = mpsc::channel::<SteerMessage>(8);
    let controls = RunControls {
        request_input: Box::new(|_| oneshot::channel().1),
        steering,
        interrupt: CancellationToken::new(),
    };
    let request = RunRequest {
        prompt: "In the current directory: view notes.md, grep for the word fox, find files \
                 named *.md, then create probe.txt containing ok. Finally reply with one \
                 short sentence."
            .into(),
        harness: None,
        model: None,
        reasoning: None,
        model_options: serde_json::Map::new(),
        cwd,
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: false,
        attachments: Vec::new(),
        worktree: None,
        resume: None,
    };
    let mut stream = harness.run(request, controls).await.expect("run starts");
    let mut steer_tx = Some(steer_tx);

    loop {
        let event = match tokio::time::timeout(Duration::from_secs(180), stream.next()).await {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(_) => {
                eprintln!("--- timed out waiting for the next event");
                std::process::exit(2);
            }
        };
        match event {
            Ok(AgentEvent::TextDelta { text }) => eprint!("{text}"),
            Ok(AgentEvent::Done {
                status,
                session_id,
                error,
                ..
            }) => {
                eprintln!("\nEV Done({status:?}) session={session_id:?} error={error:?}");
                if let Some(tx) = steer_tx.take() {
                    tx.send(SteerMessage {
                        prompt: "Reply with just the word two.".into(),
                        message_id: None,
                    })
                    .await
                    .unwrap();
                }
            }
            Ok(other) => eprintln!("\nEV {other:?}"),
            Err(e) => eprintln!("\nERR {e}"),
        }
    }
    eprintln!("--- stream ended");
}
