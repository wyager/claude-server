use std::io::Write;
use std::process::{Command, Stdio};

use clap::Args;

static SOURCE_TARBALL: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/source.tar.gz"));

#[derive(Args)]
pub struct SourceArgs {
    /// Extract into DIR instead of writing tarball to stdout
    #[arg(long, value_name = "DIR")]
    pub extract: Option<String>,
}

pub fn run(args: SourceArgs) {
    match args.extract {
        None => {
            std::io::stdout()
                .write_all(SOURCE_TARBALL)
                .expect("Failed to write tarball to stdout");
        }
        Some(dir) => {
            if let Err(e) = std::fs::create_dir_all(&dir) {
                eprintln!("Failed to create {}: {}", dir, e);
                std::process::exit(1);
            }
            let mut child = Command::new("tar")
                .args(["-xzf", "-", "-C", &dir])
                .stdin(Stdio::piped())
                .spawn()
                .unwrap_or_else(|e| {
                    eprintln!("Failed to spawn tar: {}", e);
                    std::process::exit(1);
                });
            child
                .stdin
                .take()
                .unwrap()
                .write_all(SOURCE_TARBALL)
                .expect("Failed to pipe tarball to tar");
            let status = child.wait().expect("Failed to wait for tar");
            if !status.success() {
                eprintln!("tar exited with {}", status);
                std::process::exit(1);
            }
            println!("Extracted {} bytes to {}", SOURCE_TARBALL.len(), dir);
        }
    }
}
