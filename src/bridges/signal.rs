use anyhow::{Context, Result};
use clap::Args;
use tokio::io::{AsyncBufReadExt, BufReader};
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
    // Startup check
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
        "[signal bridge] using {}",
        String::from_utf8_lossy(&ver.stdout).trim()
    );

    let chat_id = format!("signal:{}", peer);
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: signal-cli receive → parse JSON → filter by peer → channel
    let recv_account = account.clone();
    let recv_peer = peer.clone();
    let recv_cli = signal_cli.clone();
    tokio::spawn(async move {
        if let Err(e) = receive_loop(&recv_cli, &recv_account, &recv_peer, tx).await {
            eprintln!("[signal bridge] receive loop ended: {:#}", e);
        }
    });

    // Outbound: agent message → signal-cli send
    let send_cli = signal_cli.clone();
    let send_account = account.clone();
    let send_peer = peer.clone();
    super::relay_loop(&api_url, &chat_id, &peer, rx, move |content| {
        let cli = send_cli.clone();
        let acct = send_account.clone();
        let to = send_peer.clone();
        async move {
            let out = Command::new(&cli)
                .args(["-a", &acct, "send", "-m", &content, &to])
                .output()
                .await
                .context("spawning signal-cli send")?;
            if !out.status.success() {
                anyhow::bail!(
                    "signal-cli send failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            Ok(())
        }
    })
    .await
}

async fn receive_loop(
    signal_cli: &str,
    account: &str,
    peer: &str,
    tx: mpsc::UnboundedSender<String>,
) -> Result<()> {
    let mut child = Command::new(signal_cli)
        .args(["-a", account, "-o", "json", "receive", "--timeout", "-1"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning signal-cli receive")?;

    let stdout = child.stdout.take().context("no stdout from signal-cli")?;
    let mut lines = BufReader::new(stdout).lines();

    while let Some(line) = lines.next_line().await? {
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let envelope = match v.get("envelope") {
            Some(e) => e,
            None => continue,
        };
        let source = envelope
            .get("sourceNumber")
            .or_else(|| envelope.get("source"))
            .and_then(|s| s.as_str());
        if source != Some(peer) {
            continue;
        }
        let msg = envelope
            .get("dataMessage")
            .and_then(|d| d.get("message"))
            .and_then(|m| m.as_str());
        if let Some(text) = msg {
            if !text.is_empty() {
                tx.send(text.to_string()).ok();
            }
        }
    }

    let status = child.wait().await?;
    anyhow::bail!("signal-cli receive exited with {}", status);
}
