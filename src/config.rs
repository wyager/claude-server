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
    pub compaction_ratio: f64,
    pub compaction_target_ratio: f64,
    pub render_config: RenderConfig,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .context("ANTHROPIC_API_KEY environment variable must be set")?;

        if api_key.is_empty() {
            bail!("ANTHROPIC_API_KEY must not be empty");
        }

        let model = std::env::var("CLAUDE_SERVER_MODEL")
            .unwrap_or_else(|_| "claude-opus-4-5-20251101".to_string());

        let api_base_url = std::env::var("CLAUDE_SERVER_API_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

        let max_tokens = std::env::var("CLAUDE_SERVER_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);

        let context_window = std::env::var("CLAUDE_SERVER_CONTEXT_WINDOW")
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
            compaction_ratio: 0.8,
            compaction_target_ratio: 0.5,
            render_config: RenderConfig::default(),
        })
    }

    pub fn compaction_threshold(&self) -> u64 {
        let available = self.context_window.saturating_sub(self.max_tokens);
        (available as f64 * self.compaction_ratio) as u64
    }

    pub fn compaction_target(&self) -> u64 {
        let available = self.context_window.saturating_sub(self.max_tokens);
        (available as f64 * self.compaction_target_ratio) as u64
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
