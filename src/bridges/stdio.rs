use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

pub fn run(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("Usage: claude-server bridge stdio [--api-url URL]");
        println!();
        println!("Trivial bridge: reads lines from stdin, prints agent replies to stdout.");
        println!("chat_id is fixed to \"stdio\".");
        return;
    }

    let (api_url, _) = super::parse_api_url(args);

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(api_url)) {
        eprintln!("[stdio bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(api_url: String) -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: stdin lines → channel
    tokio::spawn(async move {
        let stdin = BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // Outbound: agent message → stdout
    super::relay_loop(&api_url, "stdio", "stdio", rx, |content| async move {
        println!("{}", content);
        Ok(())
    })
    .await
}
