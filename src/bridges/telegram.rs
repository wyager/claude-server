use anyhow::{Context, Result};
use clap::Args;
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Args)]
pub struct TelegramArgs {
    /// Bot token from @BotFather
    #[arg(long)]
    pub token: String,
    /// Optional allowlist of numeric chat IDs. Omit to accept from anyone
    /// who messages the bot.
    #[arg(long)]
    pub peer: Vec<i64>,
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

/// Telegram hard-caps messages at 4096 chars. Split at line boundaries,
/// snapping to char boundaries if a single line exceeds the limit.
fn chunk_for_telegram(content: &str) -> Vec<String> {
    const MAX: usize = 4096;
    if content.len() <= MAX { return vec![content.to_owned()]; }
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in content.split_inclusive('\n') {
        if cur.len() + line.len() > MAX && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        if line.len() > MAX {
            // Single line exceeds limit — hard-split at char boundaries.
            let mut rest = line;
            while rest.len() > MAX {
                let mut cut = MAX;
                while !rest.is_char_boundary(cut) { cut -= 1; }
                out.push(rest[..cut].to_owned());
                rest = &rest[cut..];
            }
            cur.push_str(rest);
        } else {
            cur.push_str(line);
        }
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

async fn run_async(api_url: String, token: String, peer_allowlist: Vec<i64>) -> Result<()> {
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
    eprintln!("[telegram bridge] bot @{} connected", username);

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
                let Some(src) = msg["chat"]["id"].as_i64() else { continue };
                if !peer_allowlist.is_empty() && !peer_allowlist.contains(&src) {
                    continue;
                }
                if let Some(text) = msg["text"].as_str() {
                    let inbound = super::Inbound {
                        chat_id: format!("telegram:{}", src),
                        text: text.to_owned(),
                        attachments: Vec::new(),
                        message_ref: msg["message_id"].as_i64().map(|i| i.to_string()),
                    };
                    if tx.send(inbound).is_err() { return; }
                }
            }
        }
    });

    // Outbound: sendMessage / sendDocument. Subscribes to telegram:* — one
    // bridge handles all chats. Recipient parsed from out.chat_id.
    let send_base = base.clone();
    super::relay_loop(&api_url, "telegram:*", "telegram", rx, move |out: super::Outbound| {
        let client = client.clone();
        let base = send_base.clone();
        async move {
            let recipient: i64 = out.chat_id
                .strip_prefix("telegram:")
                .and_then(|s| s.parse().ok())
                .with_context(|| format!("bad telegram chat_id: {}", out.chat_id))?;
            for chunk in chunk_for_telegram(&out.content) {
                if chunk.is_empty() { continue; }
                let resp = client
                    .post(format!("{}/sendMessage", base))
                    .json(&json!({"chat_id": recipient, "text": chunk}))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    anyhow::bail!("sendMessage returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
                }
            }
            for path in out.attachments {
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
                    .text("chat_id", recipient.to_string())
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

#[cfg(test)]
mod tests {
    use super::chunk_for_telegram;

    #[test]
    fn test_chunk_passthrough() {
        let s = "short message";
        assert_eq!(chunk_for_telegram(s), vec![s.to_owned()]);
    }

    #[test]
    fn test_chunk_line_boundaries() {
        // 3 lines, each ~2000 chars — should split at newlines into 3 chunks
        let line = "x".repeat(2000);
        let s = format!("{}\n{}\n{}", line, line, line);
        let chunks = chunk_for_telegram(&s);
        assert_eq!(chunks.len(), 2); // 2001+2001 > 4096, so first two fit, third alone
        assert!(chunks[0].ends_with('\n'));
        assert!(chunks.iter().all(|c| c.len() <= 4096));
        assert_eq!(chunks.concat(), s);
    }

    #[test]
    fn test_chunk_hard_split_char_boundary() {
        // Single line with multibyte chars exceeding limit — must not split mid-codepoint
        let s = "→".repeat(2000); // 6000 bytes (→ is 3 bytes)
        let chunks = chunk_for_telegram(&s);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(c.len() <= 4096);
            // Every chunk is valid UTF-8 (implicit — &str guarantees) and
            // starts/ends at char boundaries
            assert_eq!(c.chars().count() * 3, c.len()); // all chars are 3-byte →
        }
        assert_eq!(chunks.concat(), s);
    }
}
