use axum::response::Html;
use axum::routing::get;
use axum::Router;
use clap::Args;

static CHAT_HTML: &str = include_str!("chat.html");

#[derive(Args)]
pub struct ChatArgs {
    /// Port for the chat UI
    #[arg(short, long, default_value_t = 8080)]
    pub port: u16,

    /// Claude Server API URL
    #[arg(short = 'a', long, default_value = "http://127.0.0.1:3000")]
    pub api_url: String,
}

pub fn run_chat_server(args: ChatArgs) {
    let html = CHAT_HTML.replace("{{API_URL}}", &args.api_url);
    let port = args.port;
    let api_url = args.api_url;

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
