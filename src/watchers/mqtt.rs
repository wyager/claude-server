use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::json;
use tokio::sync::mpsc;

use super::{debounce_loop, Common};

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
    #[command(flatten)]
    pub common: Common,
}

pub async fn run(args: MqttArgs) -> Result<()> {
    let (host, port) = args
        .broker
        .rsplit_once(':')
        .map(|(h, p)| (h.to_string(), p.parse().unwrap_or(1883)))
        .unwrap_or((args.broker.clone(), 1883));

    let client_id = args
        .client_id
        .clone()
        .unwrap_or_else(|| format!("claude-server-{}", std::process::id()));
    let mut opts = MqttOptions::new(client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));
    if let (Some(u), Some(p)) = (&args.username, &args.password) {
        opts.set_credentials(u, p);
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    for t in &args.topic {
        client
            .subscribe(t, QoS::AtLeastOnce)
            .await
            .with_context(|| format!("subscribing to {}", t))?;
        eprintln!("[watch mqtt] subscribed to {}", t);
    }

    let (tx, rx) = mpsc::unbounded_channel();
    let broker = args.broker.clone();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    let _ = tx.send(json!({"topic": p.topic, "payload": payload, "retain": p.retain}));
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
