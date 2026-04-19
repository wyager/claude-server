use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use crate::types::RenderConfig;

/// How the daemon authenticates to the Claude API.
///
/// `ApiKey` — standard Console API key, sent as `x-api-key`. Production path.
/// `Bearer` — OAuth-style bearer token (e.g. from Claude Code). Dev-only: using
/// subscription tokens outside Claude Code is against Anthropic's ToS. Guarded
/// by a TTY check, a typed acknowledgment, and (when parseable) a JWT exp check.
#[derive(Clone, Debug)]
pub enum AuthCredential {
    ApiKey(String),
    Bearer { token: String },
}

impl AuthCredential {
    pub fn is_bearer(&self) -> bool {
        matches!(self, AuthCredential::Bearer { .. })
    }
}

pub struct Config {
    pub model: String,
    pub auth: AuthCredential,
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
        let auth = Self::auth_from_env()?;

        let model = std::env::var("CLAUDE_SERVER_MODEL")
            .unwrap_or_else(|_| "claude-opus-4-7".to_string());

        let api_base_url = std::env::var("CLAUDE_SERVER_API_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

        let max_tokens: u64 = std::env::var("CLAUDE_SERVER_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);

        let context_window: u64 = std::env::var("CLAUDE_SERVER_CONTEXT_WINDOW")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000_000);

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
            auth,
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

    fn auth_from_env() -> Result<AuthCredential> {
        let api_key = std::env::var("ANTHROPIC_API_KEY").ok().filter(|s| !s.is_empty());
        let bearer = std::env::var("CLAUDE_SERVER_BEARER_TOKEN").ok().filter(|s| !s.is_empty());

        match (api_key, bearer) {
            (Some(_), Some(_)) => {
                bail!("ANTHROPIC_API_KEY and CLAUDE_SERVER_BEARER_TOKEN are mutually exclusive")
            }
            (Some(key), None) => Ok(AuthCredential::ApiKey(key)),
            (None, Some(token)) => {
                enforce_bearer_guardrails(&token)?;
                match parse_jwt_exp(&token) {
                    Some(exp) => {
                        let now = SystemTime::now();
                        if exp <= now {
                            bail!("bearer token is already expired (exp: {:?})", exp);
                        }
                        let remaining = exp.duration_since(now).unwrap_or(Duration::ZERO);
                        eprintln!(
                            "[auth] bearer token expires in {} min",
                            remaining.as_secs() / 60
                        );
                    }
                    None => {
                        eprintln!("[auth] bearer token is opaque (not a JWT) — expiry unknown");
                    }
                }
                Ok(AuthCredential::Bearer { token })
            }
            (None, None) => bail!(
                "Either ANTHROPIC_API_KEY or CLAUDE_SERVER_BEARER_TOKEN must be set"
            ),
        }
    }
}

/// Enforce the three dev-only guardrails on Bearer mode: TTY, acknowledgment,
/// (JWT exp is checked by the caller). Skippable with `CLAUDE_SERVER_AUTH_ACK=1`
/// for scripted test harnesses — the user opted into this by setting the var.
fn enforce_bearer_guardrails(_token: &str) -> Result<()> {
    let ack = std::env::var("CLAUDE_SERVER_AUTH_ACK")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if ack {
        eprintln!("[auth] bearer mode, acknowledgment bypassed via CLAUDE_SERVER_AUTH_ACK=1");
        return Ok(());
    }

    if !std::io::stdin().is_terminal() {
        bail!(
            "bearer mode requires an interactive TTY (stdin is not a terminal). \
             Set CLAUDE_SERVER_AUTH_ACK=1 only if you understand the ToS implications."
        );
    }

    eprintln!();
    eprintln!("========================================================================");
    eprintln!("  BEARER TOKEN AUTH — DEVELOPMENT USE ONLY");
    eprintln!();
    eprintln!("  You are authenticating with a bearer token. If this token came from");
    eprintln!("  a Claude Pro/Max subscription (e.g. via `claude` / Claude Code), using");
    eprintln!("  it from a custom harness is outside Anthropic's sanctioned use and may");
    eprintln!("  violate the Consumer ToS. For production, switch to a Console API key");
    eprintln!("  (ANTHROPIC_API_KEY) billed per token.");
    eprintln!();
    eprintln!("  Type 'I AGREE' (uppercase) and press Enter to continue, or Ctrl-C to abort:");
    eprintln!("========================================================================");

    let mut line = String::new();
    std::io::stdin().read_line(&mut line).context("read acknowledgment")?;
    if line.trim() != "I AGREE" {
        bail!("acknowledgment declined — aborting startup");
    }
    Ok(())
}

/// Best-effort JWT exp parsing. If the token is a JWT (`header.payload.sig`)
/// and the payload contains an `exp` claim, return that as a SystemTime.
/// Claude OAuth tokens are opaque strings, so this will typically return None.
fn parse_jwt_exp(token: &str) -> Option<SystemTime> {
    use base64::Engine;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let exp = json.get("exp")?.as_i64()?;
    if exp < 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::from_secs(exp as u64))
}
