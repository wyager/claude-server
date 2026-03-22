use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

pub fn run(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return;
    }

    let (api_url, rest) = super::parse_api_url(args);

    let mut account = None;
    let mut peer = None;
    let mut signal_cli = "signal-cli".to_string();
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--account" => {
                account = rest.get(i + 1).cloned();
                i += 2;
            }
            "--peer" => {
                peer = rest.get(i + 1).cloned();
                i += 2;
            }
            "--signal-cli" => {
                if let Some(v) = rest.get(i + 1) {
                    signal_cli = v.clone();
                }
                i += 2;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                print_help();
                std::process::exit(1);
            }
        }
    }

    let account = match account {
        Some(a) => a,
        None => {
            eprintln!("--account is required");
            print_help();
            std::process::exit(1);
        }
    };
    let peer = match peer {
        Some(p) => p,
        None => {
            eprintln!("--peer is required");
            print_help();
            std::process::exit(1);
        }
    };

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(api_url, account, peer, signal_cli)) {
        eprintln!("[signal bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

fn print_help() {
    println!("Usage: claude-server bridge signal --account PHONE --peer PHONE [OPTIONS]");
    println!();
    println!("Relay Signal messages via signal-cli. Requires signal-cli installed and");
    println!("linked to an account (run `signal-cli link` first).");
    println!();
    println!("Options:");
    println!("  --account PHONE    Your linked Signal account (E.164, e.g. +15551234567)");
    println!("  --peer PHONE       The peer to relay with (E.164)");
    println!("  --api-url URL      Claude Server API URL (default: http://127.0.0.1:3000)");
    println!("  --signal-cli PATH  Path to signal-cli binary (default: signal-cli)");
    println!();
    println!("chat_id will be \"signal:<peer>\".");
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
