use anyhow::{Context, Result};
use clap::Args;
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Args)]
pub struct TelegramArgs {
    /// Bot token from @BotFather
    #[arg(long)]
    pub token: String,
    /// Numeric chat ID to relay with (DM the bot, then check getUpdates)
    #[arg(long)]
    pub peer: i64,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: TelegramArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args.api.api_url, args.token, args.peer)) {
        eprintln!("[telegram bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(api_url: String, token: String, peer: i64) -> Result<()> {
    let client = reqwest::Client::new();
    let base = format!("https://api.telegram.org/bot{}", token);

    // Validate token
    let me: Value = client
        .get(format!("{}/getMe", base))
        .send()
        .await?
        .json()
        .await
        .context("getMe failed — check your bot token")?;
    let username = me["result"]["username"].as_str().unwrap_or("?");
    eprintln!("[telegram bridge] bot @{} connected, relaying chat {}", username, peer);

    let chat_id = format!("telegram:{}", peer);
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: long-poll getUpdates
    let poll_client = client.clone();
    let poll_base = base.clone();
    tokio::spawn(async move {
        let mut offset = 0i64;
        loop {
            let url = format!("{}/getUpdates?offset={}&timeout=30", poll_base, offset);
            let resp: Value = match poll_client
                .get(&url)
                .timeout(std::time::Duration::from_secs(35))
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("[telegram bridge] parse error: {}", e);
                        continue;
                    }
                },
                Err(e) => {
                    eprintln!("[telegram bridge] getUpdates error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            for update in resp["result"].as_array().into_iter().flatten() {
                if let Some(id) = update["update_id"].as_i64() {
                    offset = id + 1;
                }
                let msg = &update["message"];
                if msg["chat"]["id"].as_i64() == Some(peer) {
                    if let Some(text) = msg["text"].as_str() {
                        if tx.send(text.to_string().into()).is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });

    // Outbound: sendMessage / sendDocument
    let send_base = base.clone();
    super::relay_loop(&api_url, &chat_id, &format!("tg:{}", peer), rx, move |content, attachments| {
        let client = client.clone();
        let base = send_base.clone();
        async move {
            if !content.is_empty() {
                let resp = client
                    .post(format!("{}/sendMessage", base))
                    .json(&json!({"chat_id": peer, "text": content}))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    anyhow::bail!("sendMessage returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
                }
            }
            for path in attachments {
                let bytes = tokio::fs::read(&path).await.with_context(|| format!("reading {}", path))?;
                let name = std::path::Path::new(&path).file_name()
                    .map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| "file".into());
                let is_image = matches!(
                    std::path::Path::new(&path).extension().and_then(|e| e.to_str()),
                    Some("jpg" | "jpeg" | "png" | "gif" | "webp")
                );
                let (method, field) = if is_image { ("sendPhoto", "photo") } else { ("sendDocument", "document") };
                let part = reqwest::multipart::Part::bytes(bytes).file_name(name);
                let form = reqwest::multipart::Form::new()
                    .text("chat_id", peer.to_string())
                    .part(field, part);
                let resp = client.post(format!("{}/{}", base, method)).multipart(form).send().await?;
                if !resp.status().is_success() {
                    anyhow::bail!("{} returned {}: {}", method, resp.status(), resp.text().await.unwrap_or_default());
                }
            }
            Ok(())
        }
    })
    .await
}
