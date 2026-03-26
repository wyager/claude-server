use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;
use lettre::message::{header::ContentType, Attachment, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::Inbound;

#[derive(Args, Clone)]
pub struct EmailArgs {
    /// IMAP server hostname (for receiving)
    #[arg(long)]
    pub imap_server: String,
    #[arg(long, default_value_t = 993)]
    pub imap_port: u16,
    /// SMTP server hostname (for sending)
    #[arg(long)]
    pub smtp_server: String,
    #[arg(long, default_value_t = 587)]
    pub smtp_port: u16,
    #[arg(long)]
    pub user: String,
    #[arg(long)]
    pub password: String,
    /// Only relay mail to/from this address
    #[arg(long)]
    pub peer: String,
    #[arg(long, default_value = "INBOX")]
    pub folder: String,
    /// Directory to save inbound attachments
    #[arg(long, default_value = "/tmp/claude-server-email-attachments")]
    pub attach_dir: String,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: EmailArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args)) {
        eprintln!("[email bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(args: EmailArgs) -> Result<()> {
    std::fs::create_dir_all(&args.attach_dir).ok();
    let chat_id = format!("email:{}", args.peer);
    let (tx, rx) = mpsc::unbounded_channel::<Inbound>();

    // Inbound: IMAP IDLE with reconnect loop
    let imap_args = args.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = imap_loop(&imap_args, &tx).await {
                eprintln!("[email bridge] IMAP error: {:#}, reconnecting in 30s", e);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            if tx.is_closed() { return; }
        }
    });

    // Outbound: SMTP via lettre
    let mailer: AsyncSmtpTransport<Tokio1Executor> =
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&args.smtp_server)?
            .port(args.smtp_port)
            .credentials(Credentials::new(args.user.clone(), args.password.clone()))
            .build();
    let from = args.user.clone();
    let to = args.peer.clone();

    super::relay_loop(&args.api.api_url, &chat_id, &args.peer, rx, move |out: super::Outbound| {
        let (content, attachments) = (out.content, out.attachments);
        let mailer = mailer.clone();
        let from = from.clone();
        let to = to.clone();
        async move {
            let builder = Message::builder()
                .from(from.parse().context("parse from address")?)
                .to(to.parse().context("parse to address")?)
                .subject("Message from Claude");

            let msg = if attachments.is_empty() {
                builder.body(content)?
            } else {
                let mut mp = MultiPart::mixed().singlepart(
                    SinglePart::builder().header(ContentType::TEXT_PLAIN).body(content),
                );
                for path in &attachments {
                    let bytes = tokio::fs::read(path).await.with_context(|| format!("reading {}", path))?;
                    let name = std::path::Path::new(path).file_name()
                        .map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| "file".into());
                    let ct = guess_content_type(path);
                    mp = mp.singlepart(Attachment::new(name).body(bytes, ct.parse().unwrap()));
                }
                builder.multipart(mp)?
            };

            mailer.send(msg).await.context("SMTP send")?;
            Ok(())
        }
    })
    .await
}

async fn imap_loop(args: &EmailArgs, tx: &mpsc::UnboundedSender<Inbound>) -> Result<()> {
    let tcp = TcpStream::connect((args.imap_server.as_str(), args.imap_port)).await
        .context("IMAP TCP connect")?;
    let tls = crate::feedback::rustls_connect(&args.imap_server, tcp).await
        .context("IMAP TLS")?;
    let client = async_imap::Client::new(tls);
    let mut session = client.login(&args.user, &args.password).await
        .map_err(|(e, _)| e).context("IMAP login")?;

    let mbox = session.select(&args.folder).await.context("SELECT")?;
    let mut last_seen = mbox.exists;
    eprintln!("[email bridge] IMAP connected, {} has {} messages", args.folder, last_seen);

    loop {
        let mut idle = session.idle();
        idle.init().await.context("IDLE init")?;
        let (fut, _interrupt) = idle.wait_with_timeout(Duration::from_secs(29 * 60));
        fut.await.context("IDLE wait")?;
        session = idle.done().await.context("IDLE done")?;

        let mbox = session.examine(&args.folder).await.context("EXAMINE")?;
        let exists = mbox.exists;
        if exists <= last_seen {
            last_seen = exists;
            continue;
        }

        let range = format!("{}:{}", last_seen + 1, exists);
        let mut stream = session.fetch(&range, "(UID RFC822)").await.context("FETCH")?;
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let uid = msg.uid.unwrap_or(0);
            let Some(raw) = msg.body() else { continue };
            let parsed = mailparse::parse_mail(raw).context("parse MIME")?;

            // Filter by peer
            let from = parsed.headers.iter()
                .find(|h| h.get_key().eq_ignore_ascii_case("From"))
                .map(|h| h.get_value()).unwrap_or_default();
            if !from.contains(&args.peer) {
                continue;
            }

            let (body, attachments) = extract_parts(&parsed, &args.attach_dir, uid)?;
            eprintln!("[email bridge] mail from {}: {} chars, {} attachments",
                args.peer, body.len(), attachments.len());
            let _ = tx.send(Inbound { text: body, attachments, message_ref: None });
        }
        drop(stream);
        last_seen = exists;
    }
}

fn extract_parts(mail: &mailparse::ParsedMail, attach_dir: &str, uid: u32) -> Result<(String, Vec<String>)> {
    let mut body = String::new();
    let mut attachments = Vec::new();
    walk_parts(mail, attach_dir, uid, &mut body, &mut attachments, &mut 0)?;
    Ok((body.trim().to_string(), attachments))
}

fn walk_parts(
    part: &mailparse::ParsedMail,
    attach_dir: &str,
    uid: u32,
    body: &mut String,
    attachments: &mut Vec<String>,
    part_idx: &mut usize,
) -> Result<()> {
    if part.subparts.is_empty() {
        let ct = part.ctype.mimetype.to_lowercase();
        let disp = part.get_content_disposition();
        if disp.disposition == mailparse::DispositionType::Attachment {
            let name = disp.params.get("filename")
                .cloned()
                .unwrap_or_else(|| format!("part{}", part_idx));
            let path = format!("{}/uid{}-{}", attach_dir, uid, name);
            std::fs::write(&path, part.get_body_raw()?)?;
            attachments.push(path);
        } else if ct.starts_with("text/plain") && body.is_empty() {
            *body = part.get_body()?;
        } else if ct.starts_with("text/html") && body.is_empty() {
            // Crude fallback: strip tags
            let html = part.get_body()?;
            *body = html.replace("<br>", "\n").replace("</p>", "\n\n");
            *body = regex::Regex::new("<[^>]+>").unwrap().replace_all(body, "").to_string();
        }
        *part_idx += 1;
    } else {
        for sub in &part.subparts {
            walk_parts(sub, attach_dir, uid, body, attachments, part_idx)?;
        }
    }
    Ok(())
}

fn guess_content_type(path: &str) -> &'static str {
    match std::path::Path::new(path).extension().and_then(|e| e.to_str()) {
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain",
        _ => "application/octet-stream",
    }
}
