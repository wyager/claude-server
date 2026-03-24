mod fs;
mod imap;
mod mqtt;

use anyhow::Result;
use clap::{Args, Subcommand};
use serde_json::{json, Value};

fn default_api_url() -> String {
    std::env::var("CLAUDE_SERVER_BRIDGE_API")
        .unwrap_or_else(|_| "http://127.0.0.1:3000".into())
}

#[derive(Args, Clone)]
pub struct Common {
    /// Claude Server API URL (env: CLAUDE_SERVER_BRIDGE_API)
    #[arg(long, default_value_t = default_api_url())]
    pub api_url: String,
    /// Work queue priority for generated events
    #[arg(long, default_value_t = 5)]
    pub priority: u8,
    /// Debounce window (ms) — each new event resets this timer
    #[arg(long, default_value_t = 3000)]
    pub debounce_ms: u64,
    /// Force-flush window (ms) — hard cap so a steady stream doesn't stall forever
    #[arg(long, default_value_t = 10000)]
    pub force_ms: u64,
}

#[derive(Subcommand)]
pub enum WatchCmd {
    /// Filesystem events via the notify crate
    Fs(fs::FsArgs),
    /// MQTT subscriber
    Mqtt(mqtt::MqttArgs),
    /// IMAP IDLE — push-based email notifications
    Imap(imap::ImapArgs),
}

pub fn run(cmd: WatchCmd) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let result = match cmd {
        WatchCmd::Fs(a) => rt.block_on(fs::run(a)),
        WatchCmd::Mqtt(a) => rt.block_on(mqtt::run(a)),
        WatchCmd::Imap(a) => rt.block_on(imap::run(a)),
    };
    if let Err(e) = result {
        eprintln!("[watch] error: {:#}", e);
        std::process::exit(1);
    }
}

/// Collect events from `rx`, batch with debounce + force-flush semantics,
/// POST each batch as one ExternalEvent to the daemon.
///
/// Debounce: each incoming event resets the debounce timer. Force: once the
/// first event arrives, a flush happens within `force_ms` regardless of
/// subsequent events. This bounds latency on steady-stream sources.
///
/// Batch payload: `{count: N, events: [...]}`. Agent sees `count` in the
/// preview; full list via `work_queue[i].data["events"]`.
pub async fn debounce_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Value>,
    common: &Common,
    source: &str,
) -> Result<()> {
    use tokio::time::{sleep_until, Duration, Instant};

    let client = reqwest::Client::new();
    let url = format!("{}/event", common.api_url);
    // Route back to whichever agent spawned this watcher (ProcessSupervisor
    // injects the owning agent's name). Falls back to root if unset.
    let agent = std::env::var("CLAUDE_SERVER_AGENT_NAME").ok();
    let debounce = Duration::from_millis(common.debounce_ms);
    let force = Duration::from_millis(common.force_ms);

    let mut pending: Vec<Value> = Vec::new();
    let mut debounce_at: Option<Instant> = None;
    let mut force_at: Option<Instant> = None;

    let far_future = || Instant::now() + Duration::from_secs(86400);

    loop {
        let d = debounce_at.unwrap_or_else(far_future);
        let f = force_at.unwrap_or_else(far_future);

        tokio::select! {
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                if pending.is_empty() {
                    force_at = Some(Instant::now() + force);
                }
                pending.push(ev);
                debounce_at = Some(Instant::now() + debounce);
            }
            _ = sleep_until(d), if debounce_at.is_some() => {}
            _ = sleep_until(f), if force_at.is_some() => {}
        }

        // Flush if a timer fired (pending non-empty and at least one deadline passed)
        let now = Instant::now();
        let should_flush = !pending.is_empty()
            && (debounce_at.map_or(false, |t| now >= t) || force_at.map_or(false, |t| now >= t));
        if should_flush {
            let events = std::mem::take(&mut pending);
            let count = events.len();
            let body = json!({
                "source": source,
                "type": "batch",
                "data": {"count": count, "events": events},
                "priority": common.priority,
                "agent": agent,
            });
            match client.post(&url).json(&body).send().await {
                Ok(r) if r.status().is_success() => {
                    eprintln!("[watch {}] flushed batch of {}", source, count);
                }
                Ok(r) => eprintln!("[watch {}] /event returned {}", source, r.status()),
                Err(e) => eprintln!("[watch {}] post failed: {}", source, e),
            }
            debounce_at = None;
            force_at = None;
        }
    }

    Ok(())
}
