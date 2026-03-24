use anyhow::{bail, Context, Result};
use base64::Engine;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::renderer::RenderedContext;
use crate::types::*;

/// Maximum size for a text attachment before we truncate.
/// Keeps the agent from accidentally nuking its context window
/// by attaching a 100MB log file.
const MAX_TEXT_ATTACHMENT_BYTES: usize = 64 * 1024;

/// Sniff media type from extension. Returns Some("image/...") for supported
/// image formats, None for everything else (which we treat as text).
fn sniff_image_media_type(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|e| e.to_str()).map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("png") => Some("image/png"),
        Some("gif") => Some("image/gif"),
        Some("webp") => Some("image/webp"),
        _ => None,
    }
}

/// Resolve an attachment file path into a ContentBlock.
/// Images → base64-encoded image block. Everything else → text block.
/// File-not-found or read errors → text block with an error message
/// (don't fail the turn — the agent gets feedback and moves on).
/// Exponential backoff with cap and ±25% jitter. Base 2s, doubles each attempt.
fn backoff(attempt: u32, cap_secs: u64) -> Duration {
    let base = 2u64.saturating_pow(attempt).min(cap_secs);
    let jitter = (base as f64 * 0.25 * (fastrand_ish() * 2.0 - 1.0)) as i64;
    Duration::from_secs((base as i64 + jitter).max(1) as u64)
}

fn fastrand_ish() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos % 1000) as f64 / 1000.0
}

fn resolve_attachment(att: &Attachment) -> ContentBlock {
    let path = &att.path;
    let display = path.display();

    match sniff_image_media_type(path) {
        Some(media_type) => match std::fs::read(path) {
            Ok(bytes) => ContentBlock::Image {
                source: ImageSource {
                    source_type: "base64".to_string(),
                    media_type: media_type.to_string(),
                    data: base64::engine::general_purpose::STANDARD.encode(&bytes),
                },
            },
            Err(e) => ContentBlock::Text {
                text: format!("[attachment read error: {} — {}]", display, e),
                cache_control: None,
            },
        },
        None => match std::fs::read_to_string(path) {
            Ok(mut text) => {
                if text.len() > MAX_TEXT_ATTACHMENT_BYTES {
                    let orig_len = text.len();
                    text.truncate(MAX_TEXT_ATTACHMENT_BYTES);
                    text.push_str(&format!(
                        "\n[... truncated, {} bytes total]",
                        orig_len
                    ));
                }
                ContentBlock::Text {
                    text: format!("<attachment path=\"{}\">\n{}\n</attachment>", display, text),
                    cache_control: None,
                }
            }
            Err(e) => ContentBlock::Text {
                text: format!("[attachment read error: {} — {}]", display, e),
                cache_control: None,
            },
        },
    }
}

pub struct ApiClient {
    client: reqwest::Client,
    config: Arc<Config>,
    base_system_prompt: String,
}

pub struct ApiTurnResult {
    pub code: String,
    pub thinking: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
}

impl ApiClient {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;

        let base_system_prompt = config.load_system_prompt()?;

        Ok(Self {
            client,
            config,
            base_system_prompt,
        })
    }

    /// Create an API client with a pre-loaded system prompt (used by child agents).
    pub fn new_with_prompt(config: Arc<Config>, system_prompt: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;

        Ok(Self {
            client,
            config,
            base_system_prompt: system_prompt.to_string(),
        })
    }

    pub async fn call(
        &self,
        rendered: &RenderedContext,
        pinned_memory: &[(String, String)],
    ) -> Result<ApiTurnResult> {
        let request = self.build_request(rendered, pinned_memory);
        if let Ok(path) = std::env::var("CLAUDE_SERVER_DUMP_REQUEST") {
            let json = serde_json::to_string(&request).unwrap_or_default();
            if path == "1" {
                eprintln!("=== API REQUEST JSON ===\n{}\n=== END ===",
                    serde_json::to_string_pretty(&request).unwrap_or_default());
            } else {
                let _ = std::fs::write(&path, &json);
                eprintln!("Request dumped to {}", path);
            }
        }
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let send_result = self
                .client
                .post(format!("{}/v1/messages", self.config.api_base_url))
                .header("x-api-key", &self.config.api_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await;

            let (kind, max, wait, detail) = match send_result {
                Ok(resp) if resp.status().is_success() => {
                    let api_response: ApiResponse = resp
                        .json()
                        .await
                        .context("Failed to parse API response")?;
                    return self.extract_code(api_response);
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(Duration::from_secs);
                    let body = resp.text().await.unwrap_or_default();
                    match status {
                        529 => ("overloaded", 20, backoff(attempt, 60), body),
                        429 => ("rate-limited", 8, retry_after.unwrap_or_else(|| backoff(attempt, 60)), body),
                        500..=599 => ("server error", 5, backoff(attempt, 30), body),
                        s => bail!("API returned {}: {}", s, body),
                    }
                }
                Err(e) => ("network", 8, backoff(attempt, 30), e.to_string()),
            };

            if attempt >= max {
                bail!("API {} after {} attempts: {}", kind, attempt, detail);
            }
            eprintln!(
                "[api] {} (attempt {}/{}), retrying in {:?}: {}",
                kind, attempt, max, wait, detail
            );
            tokio::time::sleep(wait).await;
        }
    }

    fn build_system_prompt(&self, pinned_memory: &[(String, String)]) -> String {
        if pinned_memory.is_empty() {
            return self.base_system_prompt.clone();
        }
        let mut prompt = self.base_system_prompt.clone();
        prompt.push_str("\n\n<pinned_memory>\n");
        for (key, content) in pinned_memory {
            prompt.push_str(&format!("## {}\n{}\n\n", key, content));
        }
        prompt.push_str("</pinned_memory>\n");
        prompt
    }

    fn build_request(&self, rendered: &RenderedContext, pinned_memory: &[(String, String)]) -> ApiRequest {
        let system = vec![SystemBlock {
            block_type: "text".to_string(),
            text: self.build_system_prompt(pinned_memory),
            cache_control: Some(CacheControl {
                control_type: "ephemeral".to_string(),
            }),
        }];

        let tools = vec![ToolDefinition {
            name: "execute".to_string(),
            description: "Execute a Python script in the agent environment. You have access to \
                the work queue, memory, timers, event history, and deployment-specific tools. \
                Use this tool to perform all actions."
                .to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Python code to execute"
                    }
                },
                "required": ["code"]
            }),
        }];

        // Block order: [prefix_text, prefix_images..., seg1+cc, seg2+cc, tail, tail_images...].
        // The first cache_control breakpoint (on seg1) caches everything before it
        // — including prefix_text and prefix_images. That's the stable per-role
        // content for templated child agents.
        let prefix_len = rendered.prefix_text.len();
        let cached_len: usize = rendered.cached_segments.iter().map(String::len).sum();
        let tail = &rendered.text[prefix_len + cached_len..];
        let mut blocks: Vec<ContentBlock> = Vec::new();

        if !rendered.prefix_text.is_empty() {
            blocks.push(ContentBlock::Text {
                text: rendered.prefix_text.clone(),
                cache_control: None,
            });
        }
        for att in &rendered.prefix_attachments {
            blocks.push(resolve_attachment(att));
        }
        for seg in &rendered.cached_segments {
            blocks.push(ContentBlock::Text {
                text: seg.clone(),
                cache_control: Some(CacheControl {
                    control_type: "ephemeral".to_string(),
                }),
            });
        }
        blocks.push(ContentBlock::Text {
            text: tail.to_string(),
            cache_control: None,
        });
        for att in &rendered.attachments {
            blocks.push(resolve_attachment(att));
        }
        let content = MessageContent::Blocks(blocks);

        let messages = vec![Message {
            role: "user".to_string(),
            content,
        }];

        ApiRequest {
            model: self.config.model.clone(),
            max_tokens: self.config.max_tokens,
            system,
            tools,
            messages,
            thinking: Some(ThinkingConfig {
                thinking_type: "enabled".to_string(),
                budget_tokens: 10_000,
            }),
        }
    }

    fn extract_code(&self, response: ApiResponse) -> Result<ApiTurnResult> {
        // Extract thinking text if present
        let thinking: Option<String> = {
            let parts: Vec<&str> = response
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Thinking { thinking } => Some(thinking.as_str()),
                    _ => None,
                })
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        };

        // Find the tool_use block with name "execute"
        for block in &response.content {
            if let ContentBlock::ToolUse { name, input, .. } = block {
                if name == "execute" {
                    let code = input
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    return Ok(ApiTurnResult {
                        code,
                        thinking,
                        input_tokens: response.usage.input_tokens,
                        output_tokens: response.usage.output_tokens,
                        cache_creation_tokens: response.usage.cache_creation_input_tokens,
                        cache_read_tokens: response.usage.cache_read_input_tokens,
                    });
                }
            }
        }

        // No tool_use found — Claude chose not to act
        // Extract any text response for logging
        let text: String = response
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");

        bail!(
            "No execute tool_use in response (stop_reason={}). Text: {}",
            response.stop_reason,
            if text.len() > 200 {
                format!("{}...", &text[..200])
            } else {
                text
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sniff_image_media_type() {
        assert_eq!(sniff_image_media_type(Path::new("foo.jpg")), Some("image/jpeg"));
        assert_eq!(sniff_image_media_type(Path::new("foo.JPEG")), Some("image/jpeg"));
        assert_eq!(sniff_image_media_type(Path::new("foo.png")), Some("image/png"));
        assert_eq!(sniff_image_media_type(Path::new("foo.gif")), Some("image/gif"));
        assert_eq!(sniff_image_media_type(Path::new("foo.webp")), Some("image/webp"));
        assert_eq!(sniff_image_media_type(Path::new("foo.txt")), None);
        assert_eq!(sniff_image_media_type(Path::new("foo.json")), None);
        assert_eq!(sniff_image_media_type(Path::new("foo")), None);
    }

    #[test]
    fn test_resolve_attachment_text() {
        let tmp = std::env::temp_dir().join("claude-server-test-text.json");
        std::fs::write(&tmp, r#"{"camera": "front", "confidence": 0.92}"#).unwrap();

        let block = resolve_attachment(&Attachment::new(&tmp));
        match block {
            ContentBlock::Text { text, .. } => {
                assert!(text.contains("<attachment path="));
                assert!(text.contains(r#"{"camera": "front", "confidence": 0.92}"#));
            }
            _ => panic!("Expected Text block, got {:?}", block),
        }
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_resolve_attachment_image() {
        // Minimal valid PNG (1x1 transparent pixel, 67 bytes)
        let png_bytes: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // signature
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
            0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41, // IDAT chunk
            0x54, 0x78, 0x9C, 0x62, 0x00, 0x01, 0x00, 0x00,
            0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, // IEND chunk
            0x42, 0x60, 0x82,
        ];
        let tmp = std::env::temp_dir().join("claude-server-test-img.png");
        std::fs::write(&tmp, png_bytes).unwrap();

        let block = resolve_attachment(&Attachment::new(&tmp));
        match block {
            ContentBlock::Image { source } => {
                assert_eq!(source.source_type, "base64");
                assert_eq!(source.media_type, "image/png");
                // Decode and verify it's the same bytes
                let decoded = base64::engine::general_purpose::STANDARD.decode(&source.data).unwrap();
                assert_eq!(decoded, png_bytes);
            }
            _ => panic!("Expected Image block, got {:?}", block),
        }
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_resolve_attachment_not_found() {
        let block = resolve_attachment(&Attachment::new("/nonexistent/xyz.jpg"));
        match block {
            ContentBlock::Text { text, .. } => {
                assert!(text.contains("[attachment read error"));
                assert!(text.contains("/nonexistent/xyz.jpg"));
            }
            _ => panic!("Expected Text error block, got {:?}", block),
        }
    }
}
