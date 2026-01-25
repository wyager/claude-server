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
}
