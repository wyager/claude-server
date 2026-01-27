use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

use crate::types::RenderConfig;

pub struct Config {
    pub model: String,
    pub api_key: String,
    pub api_base_url: String,
    pub max_tokens: u64,
    pub context_window: u64,
    pub db_path: PathBuf,
    pub system_prompt_path: PathBuf,
    pub deployment_context_path: Option<PathBuf>,
    pub listen_addr: SocketAddr,
    /// Compaction triggers when input_tokens exceeds this value
    pub compact_at: u64,
    /// Target token count after compaction
    pub compact_target: u64,
    pub render_config: RenderConfig,
    /// Timeout for Python script execution in seconds
    pub python_timeout_secs: u64,
    /// Cost per million input tokens (USD)
    pub cost_per_m_input: f64,
    /// Cost per million output tokens (USD)
    pub cost_per_m_output: f64,
    /// Cost per million cache read tokens (USD)
    pub cost_per_m_cache_read: f64,
    /// Cost per million cache write tokens (USD)
    pub cost_per_m_cache_write: f64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .context("ANTHROPIC_API_KEY environment variable must be set")?;

        if api_key.is_empty() {
            bail!("ANTHROPIC_API_KEY must not be empty");
        }

        let model = std::env::var("CLAUDE_SERVER_MODEL")
            .unwrap_or_else(|_| "claude-sonnet-4-5-20250929".to_string());

        let api_base_url = std::env::var("CLAUDE_SERVER_API_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

        let max_tokens: u64 = std::env::var("CLAUDE_SERVER_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);

        let context_window: u64 = std::env::var("CLAUDE_SERVER_CONTEXT_WINDOW")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);

        let db_path = std::env::var("CLAUDE_SERVER_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("claude-server.db"));

        let system_prompt_path = std::env::var("CLAUDE_SERVER_SYSTEM_PROMPT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("system_prompt.txt"));

        let deployment_context_path = std::env::var("CLAUDE_SERVER_DEPLOYMENT_CONTEXT")
            .ok()
            .map(PathBuf::from);

        let listen_addr: SocketAddr = std::env::var("CLAUDE_SERVER_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()
            .context("Invalid listen address")?;

        let available = context_window.saturating_sub(max_tokens);
        let compact_at = std::env::var("CLAUDE_SERVER_COMPACT_AT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| (available as f64 * 0.8) as u64);

        let compact_target = std::env::var("CLAUDE_SERVER_COMPACT_TARGET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| (available as f64 * 0.5) as u64);

        let python_timeout_secs: u64 = std::env::var("CLAUDE_SERVER_PYTHON_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let cost_per_m_input: f64 = std::env::var("CLAUDE_SERVER_COST_INPUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3.0);

        let cost_per_m_output: f64 = std::env::var("CLAUDE_SERVER_COST_OUTPUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(15.0);

        let cost_per_m_cache_read: f64 = std::env::var("CLAUDE_SERVER_COST_CACHE_READ")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.30);

        let cost_per_m_cache_write: f64 = std::env::var("CLAUDE_SERVER_COST_CACHE_WRITE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3.75);

        Ok(Self {
            model,
            api_key,
            api_base_url,
            max_tokens,
            context_window,
            db_path,
            system_prompt_path,
            deployment_context_path,
            listen_addr,
            compact_at,
            compact_target,
            render_config: RenderConfig::default(),
            python_timeout_secs,
            cost_per_m_input,
            cost_per_m_output,
            cost_per_m_cache_read,
            cost_per_m_cache_write,
        })
    }

    pub fn load_system_prompt(&self) -> Result<String> {
        std::fs::read_to_string(&self.system_prompt_path)
            .with_context(|| format!("Failed to read system prompt from {:?}", self.system_prompt_path))
    }

    pub fn load_deployment_context(&self) -> Result<String> {
        match &self.deployment_context_path {
            Some(path) => std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read deployment context from {:?}", path)),
            None => Ok(String::new()),
        }
    }
}
