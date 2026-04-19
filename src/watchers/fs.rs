use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use notify::{Config, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;

use super::{debounce_loop, Common};

#[derive(Args)]
pub struct FsArgs {
    /// Paths to watch (can repeat)
    #[arg(long, required = true)]
    pub path: Vec<PathBuf>,
    /// Watch recursively
    #[arg(long, default_value_t = true)]
    pub recursive: bool,
    /// Use polling backend at this interval instead of native events.
    /// Needed for NFS/SMB/sshfs where inotify/FSEvents don't see remote writes.
    #[arg(long, value_name = "MS")]
    pub poll_interval_ms: Option<u64>,
    #[command(flatten)]
    pub common: Common,
}

pub async fn run(args: FsArgs) -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mode = if args.recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };

    // notify uses its own thread; bridge to tokio via channel
    let cb = move |res: notify::Result<notify::Event>| match res {
        Ok(ev) => {
            let kind = format!("{:?}", ev.kind);
            for p in ev.paths {
                let _ = tx.send(json!({"path": p.to_string_lossy(), "kind": kind}));
            }
        }
        Err(e) => eprintln!("[watch fs] notify error: {}", e),
    };

    let mut watcher: Box<dyn Watcher + Send> = match args.poll_interval_ms {
        Some(ms) => {
            eprintln!("[watch fs] using polling backend (interval {}ms)", ms);
            let cfg = Config::default().with_poll_interval(Duration::from_millis(ms));
            Box::new(PollWatcher::new(cb, cfg).context("creating poll watcher")?)
        }
        None => {
            eprintln!("[watch fs] using native backend");
            Box::new(RecommendedWatcher::new(cb, Config::default()).context("creating fs watcher")?)
        }
    };

    for p in &args.path {
        watcher.watch(p, mode).with_context(|| format!("watching {:?}", p))?;
        eprintln!("[watch fs] watching {:?} ({:?})", p, mode);
    }

    debounce_loop(rx, &args.common, "fs").await
}
