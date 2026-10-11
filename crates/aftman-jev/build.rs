//! Capture the toolchain version for the `X-TypeSafe-Runtime` header.
//!
//! The header is meant to identify the client honestly, and the Rust version is part
//! of that. It is not otherwise available at runtime, so it is captured at build time.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        // "rustc 1.99.0 (b9400841c 2026-09-28)" -> "1.99.0"
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=AFTMAN_RUSTC_VERSION={version}");
}
