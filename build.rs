fn main() {
    // Embed a tarball of the source tree for the `source` subcommand.
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let tarball = format!("{}/source.tar.gz", out_dir);
    let status = std::process::Command::new("git")
        .args(["archive", "HEAD", "--format=tar.gz", "-o", &tarball])
        .status();
    match status {
        Ok(s) if s.success() => {}
        _ => {
            // Not in a git repo or git missing — write empty gzip so include_bytes! still works.
            const EMPTY_GZ: &[u8] = &[
                0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            std::fs::write(&tarball, EMPTY_GZ).unwrap();
            println!("cargo:warning=git archive failed; embedded source tarball is empty");
        }
    }
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
