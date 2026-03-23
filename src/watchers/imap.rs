use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;
use serde_json::json;
use tokio::net::TcpStream;

use tokio::sync::mpsc;

use super::{debounce_loop, Common};

#[derive(Args, Clone)]
pub struct ImapArgs {
    /// IMAP server hostname
    #[arg(long)]
    pub server: String,
    /// Port (default 993 for TLS)
    #[arg(long, default_value_t = 993)]
    pub port: u16,
    #[arg(long)]
    pub user: String,
    #[arg(long)]
    pub password: String,
    #[arg(long, default_value = "INBOX")]
    pub folder: String,
    #[command(flatten)]
    pub common: Common,
}

pub async fn run(args: ImapArgs) -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    let idle_args = args.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = idle_once(&idle_args, &tx).await {
                eprintln!("[watch imap] session error: {:#}, reconnecting in 30s", e);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    });
    debounce_loop(rx, &args.common, "imap").await
}

async fn idle_once(args: &ImapArgs, tx: &mpsc::UnboundedSender<serde_json::Value>) -> Result<()> {
    let tcp = TcpStream::connect((args.server.as_str(), args.port))
        .await
        .context("TCP connect")?;
    let tls = async_native_tls::connect(&args.server, tcp)
        .await
        .context("TLS handshake")?;
    let client = async_imap::Client::new(tls);
    let mut session = client
        .login(&args.user, &args.password)
        .await
        .map_err(|(e, _)| e)
        .context("IMAP login")?;

    let mbox = session.select(&args.folder).await.context("SELECT folder")?;
    let mut last_seen = mbox.exists;
    eprintln!(
        "[watch imap] logged in as {}, {} has {} messages",
        args.user, args.folder, last_seen
    );

    loop {
        // IDLE until server signals activity or we refresh (29min, under the 30min RFC limit)
        let mut idle = session.idle();
        idle.init().await.context("IDLE init")?;
        let (fut, _interrupt) = idle.wait_with_timeout(Duration::from_secs(29 * 60));
        fut.await.context("IDLE wait")?;
        session = idle.done().await.context("IDLE done")?;

        // Re-examine to find new messages
        let mbox = session.examine(&args.folder).await.context("EXAMINE")?;
        let exists = mbox.exists;
        if exists <= last_seen {
            last_seen = exists; // expunges can reduce count
            continue;
        }

        let range = format!("{}:{}", last_seen + 1, exists);
        let mut stream = session
            .fetch(&range, "(UID ENVELOPE)")
            .await
            .context("FETCH")?;
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let env = msg.envelope();
            let from = env
                .and_then(|e| e.from.as_ref())
                .and_then(|f| f.first())
                .map(|a| {
                    format!(
                        "{}@{}",
                        a.mailbox.as_ref().map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default(),
                        a.host.as_ref().map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or_default()
                    )
                })
                .unwrap_or_default();
            let subject = env
                .and_then(|e| e.subject.as_ref())
                .map(|s| String::from_utf8_lossy(s).to_string())
                .unwrap_or_default();
            let uid = msg.uid.unwrap_or(0);

            eprintln!("[watch imap] new mail from {}: {}", from, subject);
            let _ = tx.send(json!({"from": from, "subject": subject, "uid": uid, "folder": args.folder}));
        }
        drop(stream);
        last_seen = exists;
    }
}
