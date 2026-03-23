use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Args)]
pub struct SignalArgs {
    /// Your linked Signal account (E.164, e.g. +15551234567)
    #[arg(long)]
    pub account: String,
    /// The peer to relay with (E.164)
    #[arg(long)]
    pub peer: String,
    /// Path to signal-cli binary
    #[arg(long, default_value = "signal-cli")]
    pub signal_cli: String,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: SignalArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args.api.api_url, args.account, args.peer, args.signal_cli)) {
        eprintln!("[signal bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(
    api_url: String,
    account: String,
    peer: String,
    signal_cli: String,
) -> Result<()> {
    let ver = Command::new(&signal_cli)
        .arg("--version")
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to run {} --version. Install signal-cli and link an account first:\n  {} link -n claude-server",
                signal_cli, signal_cli
            )
        })?;
    if !ver.status.success() {
        anyhow::bail!(
            "{} --version failed: {}",
            signal_cli,
            String::from_utf8_lossy(&ver.stderr)
        );
    }
    eprintln!(
        "[signal bridge] using {} (jsonRpc mode)",
        String::from_utf8_lossy(&ver.stdout).trim()
    );

    // Single daemon process handles both receive and send via JSON-RPC over
    // stdin/stdout. Avoids the file-lock contention that made the old
    // receive-process + spawn-per-send design unable to deliver outbound.
    let mut child = Command::new(&signal_cli)
        .args(["-a", &account, "jsonRpc"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("spawning signal-cli jsonRpc daemon")?;

    let stdin = child.stdin.take().context("no stdin to signal-cli")?;
    let stdout = child.stdout.take().context("no stdout from signal-cli")?;

    let chat_id = format!("signal:{}", peer);
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();

    // signal-cli saves received attachments here; we include the full path in
    // the forwarded message so the agent can attach() them.
    let attach_dir = std::env::var("XDG_DATA_HOME")
        .map(|d| format!("{}/signal-cli/attachments", d))
        .or_else(|_| std::env::var("HOME").map(|h| format!("{}/.local/share/signal-cli/attachments", h)))
        .unwrap_or_else(|_| "~/.local/share/signal-cli/attachments".into());

    // Reader: parse JSON-RPC lines from daemon stdout
    let read_peer = peer.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        eprintln!("[signal bridge] daemon connected, listening for messages");
        while let Ok(Some(line)) = lines.next_line().await {
            let v: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(err) = v.get("error") {
                eprintln!("[signal bridge] jsonrpc error: {}", err);
                continue;
            }
            // Incoming message notification
            if v["method"].as_str() == Some("receive") {
                let env = &v["params"]["envelope"];
                let src = env["sourceNumber"]
                    .as_str()
                    .or_else(|| env["source"].as_str());
                if src != Some(read_peer.as_str()) {
                    continue;
                }
                let dm = &env["dataMessage"];
                let text = dm["message"].as_str().unwrap_or("");
                let attachments: Vec<String> = dm["attachments"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|a| a["id"].as_str())
                    .map(|id| format!("{}/{}", attach_dir, id))
                    .collect();
                let content = match (text.is_empty(), attachments.is_empty()) {
                    (true, true) => continue,
                    (false, true) => text.to_string(),
                    (_, false) => format!("{}\n[attachments: {}]", text, attachments.join(", ")),
                };
                if inbound_tx.send(content).is_err() {
                    return;
                }
            }
        }
        eprintln!("[signal bridge] daemon stdout closed");
    });

    // Writer: serialize send requests to daemon stdin
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<String>();
    let req_id = Arc::new(AtomicU64::new(1));
    let write_peer = peer.clone();
    tokio::spawn(async move {
        let mut stdin = stdin;
        while let Some(content) = outbound_rx.recv().await {
            let id = req_id.fetch_add(1, Ordering::Relaxed);
            let req = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "send",
                "params": { "recipient": [write_peer.as_str()], "message": content }
            });
            let line = format!("{}\n", req);
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                eprintln!("[signal bridge] write to daemon failed: {}", e);
                return;
            }
        }
    });

    // Relay loop: inbound → POST /message, SSE → outbound_tx
    super::relay_loop(&api_url, &chat_id, &peer, inbound_rx, move |content| {
        let tx = outbound_tx.clone();
        async move {
            tx.send(content).context("outbound channel closed")?;
            Ok(())
        }
    })
    .await
}
