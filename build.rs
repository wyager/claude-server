fn main() {
    let python = std::env::var("PYO3_PYTHON").unwrap_or_else(|_| "python3".to_string());

    let output = std::process::Command::new(&python)
        .args(["-c", "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"])
        .output()
        .unwrap_or_else(|e| panic!("Failed to run {} to query LIBDIR: {}", python, e));

    if !output.status.success() {
        panic!(
            "Python LIBDIR query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let libdir = String::from_utf8(output.stdout)
        .expect("LIBDIR is not valid utf8")
        .trim()
        .to_string();

    if libdir.is_empty() {
        panic!("Python LIBDIR is empty — is python3 installed with a shared library?");
    }

    // Embed rpath so the binary finds libpython at runtime without DYLD_LIBRARY_PATH
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);

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
