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
    /// Optional allowlist: only relay messages from these numbers (E.164).
    /// Omit to accept from anyone who messages your account.
    #[arg(long)]
    pub peer: Vec<String>,
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

/// Minimum gap between degraded-health events posted to the daemon. Errors
/// observed inside the gap are counted and reported in the next alert.
const ALERT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);

fn spawn_stderr_watcher(
    stderr: tokio::process::ChildStderr,
    api_url: String,
    account: String,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/event", api_url);
        // Route to whichever agent spawned this bridge (env injected by
        // ProcessSupervisor); falls back to root if unset.
        let agent = std::env::var("CLAUDE_SERVER_AGENT_NAME").ok();

        let mut lines = BufReader::new(stderr).lines();
        let mut errors_total: u64 = 0;
        let mut last_alert: Option<tokio::time::Instant> = None;

        while let Ok(Some(line)) = lines.next_line().await {
            // Preserve the full signal-cli log stream in our own stderr.
            eprintln!("[signal-cli] {}", line);

            // Java exceptions from envelope handling (NPEs, parse failures).
            // These are the "exit 0 but the message is gone" failures.
            if !line.contains("Exception") {
                continue;
            }
            errors_total += 1;

            let due = last_alert.map_or(true, |t| t.elapsed() >= ALERT_INTERVAL);
            if !due {
                continue;
            }
            last_alert = Some(tokio::time::Instant::now());

            let body = json!({
                "source": "signal-bridge",
                "type": "receive_degraded",
                "priority": 7,
                "agent": agent,
                "data": {
                    "account": account,
                    "error_line": line,
                    "errors_since_bridge_start": errors_total,
                    "hint": "signal-cli logged an exception while processing an envelope; \
                             inbound Signal messages may be silently dropped even though the \
                             process looks healthy. Known cause: sealed-sender getServerGuid \
                             NPE on signal-cli <= 0.14.4.1 — fixed in signal-cli 0.14.5.",
                }
            });
            match client.post(&url).json(&body).send().await {
                Ok(r) if r.status().is_success() => {
                    eprintln!(
                        "[signal bridge] posted receive_degraded event ({} errors so far)",
                        errors_total
                    );
                }
                Ok(r) => eprintln!("[signal bridge] /event returned {}", r.status()),
                Err(e) => eprintln!("[signal bridge] /event post failed: {}", e),
            }
        }
    });
}

async fn run_async(
    api_url: String,
    account: String,
    peer_allowlist: Vec<String>,
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
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning signal-cli jsonRpc daemon")?;

    let stdin = child.stdin.take().context("no stdin to signal-cli")?;
    let stdout = child.stdout.take().context("no stdout from signal-cli")?;
    let stderr = child.stderr.take().context("no stderr from signal-cli")?;

    // Stderr watcher: signal-cli can drop an envelope and exit 0 / keep
    // running with only a logged exception (e.g. the sealed-sender
    // getServerGuid NPE that silently killed all inbound on signal-cli
    // <= 0.14.4.1). Pass every line through to our stderr, but also surface
    // exception lines to the agent as a degraded-health event so a broken
    // receive path is visible instead of silent. Rate-limited: the Signal
    // server redelivers unacked envelopes every few seconds, so one alert
    // per window with a running count, not one per envelope.
    spawn_stderr_watcher(stderr, api_url.clone(), account.clone());

    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();

    // signal-cli saves received attachments here; we include the full path in
    // the forwarded message so the agent can attach() them.
    let attach_dir = std::env::var("XDG_DATA_HOME")
        .map(|d| format!("{}/signal-cli/attachments", d))
        .or_else(|_| std::env::var("HOME").map(|h| format!("{}/.local/share/signal-cli/attachments", h)))
        .unwrap_or_else(|_| "~/.local/share/signal-cli/attachments".into());

    // Reader: parse JSON-RPC lines from daemon stdout
    let allowlist = peer_allowlist.clone();
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
                let Some(src) = env["sourceNumber"].as_str().or_else(|| env["source"].as_str()) else {
                    continue;
                };
                if !allowlist.is_empty() && !allowlist.iter().any(|p| p == src) {
                    continue;
                }
                let dm = &env["dataMessage"];
                let text = dm["message"].as_str().unwrap_or("").to_string();
                let attachments: Vec<String> = dm["attachments"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|a| a["id"].as_str())
                    .map(|id| format!("{}/{}", attach_dir, id))
                    .collect();
                if text.is_empty() && attachments.is_empty() {
                    continue;
                }
                // Signal's message timestamp is the reaction/reply target.
                let message_ref = env["timestamp"].as_i64().map(|t| t.to_string());
                let inbound = super::Inbound {
                    chat_id: format!("signal:{}", src),
                    text, attachments, message_ref,
                };
                if inbound_tx.send(inbound).is_err() {
                    return;
                }
            }
        }
        eprintln!("[signal bridge] daemon stdout closed");
    });

    // Writer: serialize send/sendReaction requests to daemon stdin.
    // Recipient parsed from out.chat_id (e.g. "signal:+15551234567" → "+15551234567").
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<super::Outbound>();
    let req_id = Arc::new(AtomicU64::new(1));
    tokio::spawn(async move {
        let mut stdin = stdin;
        while let Some(out) = outbound_rx.recv().await {
            let Some(recipient) = out.chat_id.strip_prefix("signal:") else {
                eprintln!("[signal bridge] outbound chat_id not signal-prefixed: {}", out.chat_id);
                continue;
            };
            let id = req_id.fetch_add(1, Ordering::Relaxed);
            let req = if let Some(ts) = out.react_to {
                // react_to is the Signal message timestamp; content is the emoji.
                json!({
                    "jsonrpc": "2.0", "id": id, "method": "sendReaction",
                    "params": {
                        "recipient": [recipient],
                        "targetAuthor": recipient,
                        "targetTimestamp": ts.parse::<i64>().unwrap_or(0),
                        "emoji": out.content,
                    }
                })
            } else {
                let mut params = json!({ "recipient": [recipient], "message": out.content });
                if !out.attachments.is_empty() {
                    params["attachments"] = json!(out.attachments);
                }
                json!({ "jsonrpc": "2.0", "id": id, "method": "send", "params": params })
            };
            let line = format!("{}\n", req);
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                eprintln!("[signal bridge] write to daemon failed: {}", e);
                return;
            }
        }
    });

    // Subscribe to signal:* — one bridge handles all Signal peers.
    super::relay_loop(&api_url, "signal:*", &account, inbound_rx, move |out| {
        let tx = outbound_tx.clone();
        async move {
            tx.send(out).context("outbound channel closed")?;
            Ok(())
        }
    })
    .await
}
