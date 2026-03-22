use std::io::Write;
use std::process::{Command, Stdio};

static SOURCE_TARBALL: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/source.tar.gz"));

pub fn run(args: &[String]) {
    match args.first().map(String::as_str) {
        Some("--help") | Some("-h") => {
            println!("Usage: claude-server source [--extract DIR]");
            println!();
            println!("Dump the embedded source tarball of this harness.");
            println!();
            println!("Options:");
            println!("  (no args)         Write .tar.gz to stdout");
            println!("  --extract DIR     Extract into DIR (creates it if missing)");
            println!("  --help, -h        Show this help");
        }
        Some("--extract") => {
            let dir = match args.get(1) {
                Some(d) => d,
                None => {
                    eprintln!("--extract requires a directory argument");
                    std::process::exit(1);
                }
            };
            if let Err(e) = std::fs::create_dir_all(dir) {
                eprintln!("Failed to create {}: {}", dir, e);
                std::process::exit(1);
            }
            let mut child = match Command::new("tar")
                .args(["-xzf", "-", "-C", dir])
                .stdin(Stdio::piped())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to spawn tar: {}", e);
                    std::process::exit(1);
                }
            };
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
        None => {
            std::io::stdout()
                .write_all(SOURCE_TARBALL)
                .expect("Failed to write tarball to stdout");
        }
        Some(other) => {
            eprintln!("Unknown argument: {}", other);
            eprintln!("Run 'claude-server source --help' for usage.");
            std::process::exit(1);
        }
    }
}
