use anyhow::{bail, Context, Result};
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::renderer::RenderedContext;
use crate::types::*;

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
        notes: &[(String, String)],
    ) -> Result<ApiTurnResult> {
        let request = self.build_request(rendered, notes);
        let mut retries = 0;
        let max_retries = 3;

        loop {
            let response = self
                .client
                .post(format!("{}/v1/messages", self.config.api_base_url))
                .header("x-api-key", &self.config.api_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await
                .context("Failed to send API request")?;

            let status = response.status();

            if status.is_success() {
                let api_response: ApiResponse = response
                    .json()
                    .await
                    .context("Failed to parse API response")?;
                return self.extract_code(api_response);
            }

            // Retry on rate limit (429) or overloaded (529)
            if (status.as_u16() == 429 || status.as_u16() == 529) && retries < max_retries {
                retries += 1;
                let body = response.text().await.unwrap_or_default();
                eprintln!(
                    "[api] {} (attempt {}/{}): {}",
                    status, retries, max_retries, body
                );
                let backoff = Duration::from_secs(2u64.pow(retries as u32));
                tokio::time::sleep(backoff).await;
                continue;
            }

            let body = response.text().await.unwrap_or_default();
            bail!("API returned {}: {}", status, body);
        }
    }

    fn build_system_prompt(&self, notes: &[(String, String)]) -> String {
        if notes.is_empty() {
            return self.base_system_prompt.clone();
        }
        let mut prompt = self.base_system_prompt.clone();
        prompt.push_str("\n\n<agent_notes>\n");
        for (section, content) in notes {
            prompt.push_str(&format!("## {}\n{}\n\n", section, content));
        }
        prompt.push_str("</agent_notes>\n");
        prompt
    }

    fn build_request(&self, rendered: &RenderedContext, notes: &[(String, String)]) -> ApiRequest {
        let system = vec![SystemBlock {
            block_type: "text".to_string(),
            text: self.build_system_prompt(notes),
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

        let messages = vec![Message {
            role: "user".to_string(),
            content: MessageContent::Text(rendered.text.clone()),
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
                ContentBlock::Text { text } => Some(text.as_str()),
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
