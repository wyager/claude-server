use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use clap::{Args, ValueEnum};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::{debounce_loop, Common};

#[derive(ValueEnum, Clone, Copy, PartialEq)]
pub enum PayloadMode {
    /// Inline payload as UTF-8 text (default; for JSON status topics)
    Text,
    /// Write every payload to --attach-dir as-is, send file path only
    Raw,
    /// Parse {"attachments":[{"name","base64"}],"data":{...}} — decode
    /// attachments to --attach-dir, inline data
    Structured,
}

#[derive(Args)]
pub struct MqttArgs {
    /// Broker address (host:port)
    #[arg(long)]
    pub broker: String,
    /// Topics to subscribe (can repeat; supports +/# wildcards)
    #[arg(long, required = true)]
    pub topic: Vec<String>,
    #[arg(long)]
    pub username: Option<String>,
    #[arg(long)]
    pub password: Option<String>,
    /// Client ID (default: random)
    #[arg(long)]
    pub client_id: Option<String>,
    /// How to handle payloads (see `--help` for mode descriptions)
    #[arg(long, value_enum, default_value_t = PayloadMode::Text)]
    pub payload: PayloadMode,
    /// Directory for raw/structured-mode attachments (created if missing)
    #[arg(long, default_value = "/tmp/claude-mqtt")]
    pub attach_dir: PathBuf,
    /// Max per-message subdirs to retain (oldest deleted when exceeded)
    #[arg(long, default_value_t = 50)]
    pub attach_retain: usize,
    /// Connect via TLS (MQTTS). Typically used with port 8883.
    #[arg(long)]
    pub tls: bool,
    /// Path to a PEM-encoded CA cert to trust instead of the system store.
    /// Only meaningful with --tls. Use for brokers with private-CA certs.
    #[arg(long, value_name = "PATH", requires = "tls")]
    pub ca_file: Option<PathBuf>,
    #[command(flatten)]
    pub common: Common,
}

pub async fn run(args: MqttArgs) -> Result<()> {
    let default_port = if args.tls { 8883 } else { 1883 };
    let (host, port) = args
        .broker
        .rsplit_once(':')
        .map(|(h, p)| (h.to_string(), p.parse().unwrap_or(default_port)))
        .unwrap_or((args.broker.clone(), default_port));

    let client_id = args
        .client_id
        .clone()
        .unwrap_or_else(|| format!("claude-server-{}", std::process::id()));
    let mut opts = MqttOptions::new(client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));
    if let (Some(u), Some(p)) = (&args.username, &args.password) {
        opts.set_credentials(u, p);
    }
    if args.tls {
        opts.set_transport(Transport::Tls(build_tls_config(args.ca_file.as_deref())?));
        eprintln!("[watch mqtt] TLS enabled");
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    for t in &args.topic {
        client
            .subscribe(t, QoS::AtLeastOnce)
            .await
            .with_context(|| format!("subscribing to {}", t))?;
        eprintln!("[watch mqtt] subscribed to {}", t);
    }

    if args.payload != PayloadMode::Text {
        std::fs::create_dir_all(&args.attach_dir)
            .with_context(|| format!("create attach dir {}", args.attach_dir.display()))?;
    }

    let (tx, rx) = mpsc::unbounded_channel();
    let broker = args.broker.clone();
    let mode = args.payload;
    let attach_dir = args.attach_dir.clone();
    let retain_cap = args.attach_retain;
    tokio::spawn(async move {
        let mut retained: VecDeque<PathBuf> = VecDeque::new();
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let ev = match handle_publish(mode, &attach_dir, &p.topic, &p.payload, p.retain, &mut retained, retain_cap) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[watch mqtt] payload handling failed ({}): {}", p.topic, e);
                            continue;
                        }
                    };
                    let _ = tx.send(ev);
                }
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    eprintln!("[watch mqtt] connected to {}", broker);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[watch mqtt] eventloop error: {}, retrying in 5s", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    debounce_loop(rx, &args.common, "mqtt").await
}

/// Build a `TlsConfiguration::Simple` for rumqttc.
///
/// rumqttc 0.24 bundles its own (older) rustls version, so we can't share a
/// `rustls::ClientConfig` with the crate's `rustls` dep. Instead we feed it PEM
/// bytes: the user's CA file if `--ca-file` is set, otherwise all system native
/// roots concatenated into a PEM buffer.
fn build_tls_config(ca_file: Option<&std::path::Path>) -> Result<TlsConfiguration> {
    if let Some(path) = ca_file {
        let ca = std::fs::read(path)
            .with_context(|| format!("read CA file {}", path.display()))?;
        return Ok(TlsConfiguration::Simple { ca, alpn: None, client_auth: None });
    }

    let mut pem = Vec::new();
    let result = rustls_native_certs::load_native_certs();
    if result.certs.is_empty() {
        anyhow::bail!(
            "no system CA certificates loaded ({} errors) — pass --ca-file to supply one",
            result.errors.len()
        );
    }
    for cert in &result.certs {
        pem.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
        let b64 = base64::engine::general_purpose::STANDARD.encode(cert.as_ref());
        // Wrap base64 at 64 chars per PEM convention — strictly optional for
        // parsers but keeps the buffer readable if dumped for debugging.
        for chunk in b64.as_bytes().chunks(64) {
            pem.extend_from_slice(chunk);
            pem.push(b'\n');
        }
        pem.extend_from_slice(b"-----END CERTIFICATE-----\n");
    }
    Ok(TlsConfiguration::Simple { ca: pem, alpn: None, client_auth: None })
}

fn handle_publish(
    mode: PayloadMode,
    attach_dir: &PathBuf,
    topic: &str,
    payload: &[u8],
    retain: bool,
    retained: &mut VecDeque<PathBuf>,
    retain_cap: usize,
) -> Result<Value> {
    match mode {
        PayloadMode::Text => Ok(json!({
            "topic": topic,
            "payload": String::from_utf8_lossy(payload),
            "retain": retain,
        })),
        PayloadMode::Raw => {
            let dir = new_msg_dir(attach_dir, retained, retain_cap)?;
            let path = dir.join(format!("{}.bin", slug(topic)));
            std::fs::write(&path, payload)?;
            Ok(json!({
                "topic": topic,
                "attachments": [path.to_string_lossy()],
                "size": payload.len(),
                "retain": retain,
            }))
        }
        PayloadMode::Structured => {
            let parsed: Value = serde_json::from_slice(payload)
                .context("structured mode: payload is not valid JSON")?;
            let atts = parsed.get("attachments").and_then(|a| a.as_array());
            let mut paths = Vec::new();
            if let Some(atts) = atts.filter(|a| !a.is_empty()) {
                let dir = new_msg_dir(attach_dir, retained, retain_cap)?;
                for (i, a) in atts.iter().enumerate() {
                    let name = a.get("name").and_then(|n| n.as_str())
                        .map(sanitize_name)
                        .unwrap_or_else(|| format!("attachment-{}.bin", i));
                    let b64 = a.get("base64").and_then(|b| b.as_str())
                        .context("structured attachment missing base64")?;
                    let bytes = base64::engine::general_purpose::STANDARD.decode(b64)
                        .context("structured attachment: invalid base64")?;
                    let path = dir.join(name);
                    std::fs::write(&path, bytes)?;
                    paths.push(path.to_string_lossy().into_owned());
                }
            }
            Ok(json!({
                "topic": topic,
                "data": parsed.get("data").cloned().unwrap_or(Value::Null),
                "attachments": paths,
                "retain": retain,
            }))
        }
    }
}

fn new_msg_dir(base: &PathBuf, retained: &mut VecDeque<PathBuf>, cap: usize) -> Result<PathBuf> {
    use rand::Rng;
    let name: String = (0..8).map(|_| format!("{:02x}", rand::thread_rng().gen::<u8>())).collect();
    let dir = base.join(name);
    std::fs::create_dir_all(&dir)?;
    retained.push_back(dir.clone());
    while retained.len() > cap {
        if let Some(old) = retained.pop_front() {
            let _ = std::fs::remove_dir_all(old);
        }
    }
    Ok(dir)
}

fn slug(topic: &str) -> String {
    topic.chars().map(|c| if c.is_alphanumeric() { c } else { '-' }).collect()
}

/// Strip path separators / traversal from publisher-supplied filenames so
/// `name: "../../etc/passwd"` can't escape the per-message dir.
fn sanitize_name(name: &str) -> String {
    let cleaned: String = name.chars()
        .map(|c| if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') { c } else { '_' })
        .collect();
    let cleaned = cleaned.trim_start_matches('.');
    if cleaned.is_empty() { "attachment.bin".into() } else { cleaned.into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_name() {
        assert_eq!(sanitize_name("front-door.jpg"), "front-door.jpg");
        assert_eq!(sanitize_name("../../etc/passwd"), "_.._etc_passwd");
        assert_eq!(sanitize_name("/abs/path.bin"), "_abs_path.bin");
        assert_eq!(sanitize_name("..."), "attachment.bin");
        assert_eq!(sanitize_name("a b.jpg"), "a_b.jpg");
    }

    #[test]
    fn test_structured_parse() {
        let tmp = std::env::temp_dir().join("mqtt-test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let mut retained = VecDeque::new();

        // Structured with attachment
        let payload = serde_json::to_vec(&json!({
            "attachments": [{"name": "img.jpg", "base64": base64::engine::general_purpose::STANDARD.encode(b"fake-jpeg")}],
            "data": {"event": "motion", "zone": "front"}
        })).unwrap();
        let ev = handle_publish(PayloadMode::Structured, &tmp, "cam/front", &payload, false, &mut retained, 10).unwrap();
        assert_eq!(ev["topic"], "cam/front");
        assert_eq!(ev["data"]["event"], "motion");
        let path = ev["attachments"][0].as_str().unwrap();
        assert!(path.ends_with("/img.jpg"));
        assert_eq!(std::fs::read(path).unwrap(), b"fake-jpeg");

        // Retention: exceed cap
        for _ in 0..12 {
            handle_publish(PayloadMode::Raw, &tmp, "t", b"x", false, &mut retained, 10).unwrap();
        }
        assert_eq!(retained.len(), 10);

        std::fs::remove_dir_all(&tmp).ok();
    }
}
