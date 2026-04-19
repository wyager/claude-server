//! Per-model capability lookup via `GET /v1/models/{id}`.
//!
//! Different Claude models support different thinking modes and output
//! controls (adaptive thinking + `output_config.effort` on 4.6+, fixed
//! `budget_tokens` on 4.5 and older, neither on legacy Haiku). Rather than
//! maintain a hardcoded compat table, we query the live capability surface
//! once per model and cache the result for the daemon's lifetime.
//!
//! On a failed lookup (network, 4xx, malformed response) we degrade to the
//! conservative "no thinking, no effort" mode — the request still succeeds,
//! just without extended reasoning. We do not cache failures, so a transient
//! outage doesn't permanently strand a model.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::config::{AuthCredential, Config};

/// How this model wants its `thinking` block shaped (if at all).
#[derive(Clone, Copy, Debug)]
pub enum ThinkingMode {
    /// Model supports neither adaptive nor enabled thinking — omit the field.
    None,
    /// Opus 4.6, Opus 4.7, Sonnet 4.6.
    Adaptive,
    /// Sonnet 4.5, Opus 4.5, and earlier — fixed-budget extended thinking.
    Enabled,
}

#[derive(Clone, Copy, Debug)]
pub struct ModelCapabilities {
    pub thinking: ThinkingMode,
    pub supports_effort: bool,
}

impl ModelCapabilities {
    /// Conservative fallback used when the capability probe fails — send no
    /// thinking and no effort, which every model accepts.
    pub const FALLBACK: Self = Self {
        thinking: ThinkingMode::None,
        supports_effort: false,
    };
}

pub struct ModelCapabilityCache {
    client: reqwest::Client,
    auth: AuthCredential,
    api_base_url: String,
    cache: RwLock<HashMap<String, ModelCapabilities>>,
}

impl ModelCapabilityCache {
    pub fn new(config: &Config) -> Self {
        // Short timeout: the capability probe is on the hot path of the first
        // API call for each new model, and we don't want to stall the whole
        // turn on a slow metadata endpoint.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            auth: config.auth.clone(),
            api_base_url: config.api_base_url.clone(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_arc(config: &Config) -> Arc<Self> {
        Arc::new(Self::new(config))
    }

    /// Return capabilities for `model`, fetching if not cached. Never errors —
    /// on probe failure, returns `FALLBACK` and logs; failures are not cached.
    pub async fn get(&self, model: &str) -> ModelCapabilities {
        if let Some(caps) = self.cache.read().await.get(model).copied() {
            return caps;
        }
        match self.fetch(model).await {
            Ok(caps) => {
                self.cache.write().await.insert(model.to_string(), caps);
                caps
            }
            Err(e) => {
                eprintln!(
                    "[model_capabilities] probe for {} failed ({}); defaulting to no thinking / no effort",
                    model, e
                );
                ModelCapabilities::FALLBACK
            }
        }
    }

    async fn fetch(&self, model: &str) -> anyhow::Result<ModelCapabilities> {
        let url = format!("{}/v1/models/{}", self.api_base_url, model);
        let mut req = self
            .client
            .get(&url)
            .header("anthropic-version", "2023-06-01");
        match &self.auth {
            AuthCredential::ApiKey(k) => {
                req = req.header("x-api-key", k);
            }
            AuthCredential::Bearer { token } => {
                req = req
                    .header("authorization", format!("Bearer {}", token))
                    .header("anthropic-beta", "oauth-2025-04-20");
            }
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {} returned {}: {}", url, status, body);
        }
        let body: serde_json::Value = resp.json().await?;
        Ok(parse_capabilities(&body))
    }
}

/// Parse a `/v1/models/{id}` response into our capability shape.
///
/// Schema (per the API docs as of 2026-04):
/// ```ignore
/// {
///   "capabilities": {
///     "thinking": {"supported": bool,
///                  "types": {"enabled": {"supported": bool},
///                            "adaptive": {"supported": bool}}},
///     "effort": {"supported": bool, ...}
///   }
/// }
/// ```
/// We prefer `adaptive` when both modes are supported (e.g. Opus 4.6, where
/// `enabled` is deprecated but still functional).
fn parse_capabilities(body: &serde_json::Value) -> ModelCapabilities {
    let caps = body.get("capabilities");
    let adaptive = caps
        .and_then(|c| c.pointer("/thinking/types/adaptive/supported"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let enabled = caps
        .and_then(|c| c.pointer("/thinking/types/enabled/supported"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let supports_effort = caps
        .and_then(|c| c.pointer("/effort/supported"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let thinking = if adaptive {
        ThinkingMode::Adaptive
    } else if enabled {
        ThinkingMode::Enabled
    } else {
        ThinkingMode::None
    };
    ModelCapabilities { thinking, supports_effort }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_opus_4_7_shape() {
        let body = json!({
            "capabilities": {
                "thinking": {
                    "supported": true,
                    "types": {
                        "enabled": {"supported": false},
                        "adaptive": {"supported": true}
                    }
                },
                "effort": {"supported": true}
            }
        });
        let caps = parse_capabilities(&body);
        assert!(matches!(caps.thinking, ThinkingMode::Adaptive));
        assert!(caps.supports_effort);
    }

    #[test]
    fn parses_sonnet_4_5_shape() {
        let body = json!({
            "capabilities": {
                "thinking": {
                    "supported": true,
                    "types": {
                        "enabled": {"supported": true},
                        "adaptive": {"supported": false}
                    }
                },
                "effort": {"supported": false}
            }
        });
        let caps = parse_capabilities(&body);
        assert!(matches!(caps.thinking, ThinkingMode::Enabled));
        assert!(!caps.supports_effort);
    }

    #[test]
    fn prefers_adaptive_when_both_supported() {
        // Opus 4.6 shape: both modes supported; we pick adaptive.
        let body = json!({
            "capabilities": {
                "thinking": {
                    "types": {
                        "enabled": {"supported": true},
                        "adaptive": {"supported": true}
                    }
                },
                "effort": {"supported": true}
            }
        });
        let caps = parse_capabilities(&body);
        assert!(matches!(caps.thinking, ThinkingMode::Adaptive));
    }

    #[test]
    fn falls_back_on_missing_fields() {
        let body = json!({"capabilities": {}});
        let caps = parse_capabilities(&body);
        assert!(matches!(caps.thinking, ThinkingMode::None));
        assert!(!caps.supports_effort);
    }
}
