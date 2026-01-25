use axum::response::Html;
use axum::routing::get;
use axum::Router;

static CHAT_HTML: &str = include_str!("chat.html");

pub fn run_chat_server(args: &[String]) {
    let mut port: u16 = 8080;
    let mut api_url = "http://127.0.0.1:3000".to_string();

    // Parse --port and --api-url flags
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if let Some(val) = args.get(i + 1) {
                    port = val.parse().unwrap_or_else(|_| {
                        eprintln!("Invalid port: {}", val);
                        std::process::exit(1);
                    });
                    i += 2;
                } else {
                    eprintln!("--port requires a value");
                    std::process::exit(1);
                }
            }
            "--api-url" | "-a" => {
                if let Some(val) = args.get(i + 1) {
                    api_url = val.clone();
                    i += 2;
                } else {
                    eprintln!("--api-url requires a value");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                println!("Usage: claude-server chat [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --port, -p PORT       Port for the chat UI (default: 8080)");
                println!("  --api-url, -a URL     Claude Server API URL (default: http://127.0.0.1:3000)");
                println!("  --help, -h            Show this help");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!("Run 'claude-server chat --help' for usage.");
                std::process::exit(1);
            }
        }
    }

    // Inject API URL into the HTML template
    let html = CHAT_HTML.replace("{{API_URL}}", &api_url);

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        let app = Router::new().route("/", get(move || {
            let html = html.clone();
            async move { Html(html) }
        }));

        let addr = format!("127.0.0.1:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to bind to {}: {}", addr, e);
                std::process::exit(1);
            });

        println!("Chat UI running at http://{}", addr);
        println!("Connecting to Claude Server API at {}", api_url);
        println!();
        println!("Open your browser to http://127.0.0.1:{}", port);

        axum::serve(listener, app).await.unwrap();
    });
}
