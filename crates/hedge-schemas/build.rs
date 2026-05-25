//! Build script for `hedge-schemas`.
//!
//! Strategy:
//! - Detect `flatc` on `PATH`.
//! - If present, regenerate `src/generated/<name>_generated.rs` from every
//!   `schemas/*.fbs` file so the committed bindings stay in sync.
//! - If absent, emit a `cargo:warning=` and rely on the committed
//!   `src/generated/*.rs` files. This is the standard pattern for FlatBuffers
//!   workspaces where many CI environments do not ship `flatc`.
//!
//! In either case the crate compiles, because `src/generated/` is
//! version-controlled.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let schemas_dir = manifest_dir.join("schemas");
    let generated_dir = manifest_dir.join("src").join("generated");

    println!("cargo:rerun-if-changed=schemas");
    println!("cargo:rerun-if-changed=build.rs");

    if !schemas_dir.exists() {
        println!(
            "cargo:warning=hedge-schemas: schemas/ directory missing at {}; \
             relying on committed bindings",
            schemas_dir.display()
        );
        return;
    }

    // Re-run if any .fbs file changes.
    if let Ok(entries) = fs::read_dir(&schemas_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("fbs") {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    if !flatc_available() {
        println!(
            "cargo:warning=flatc not found, using committed generated bindings"
        );
        return;
    }

    if let Err(err) = run_flatc(&schemas_dir, &generated_dir) {
        println!(
            "cargo:warning=hedge-schemas: flatc invocation failed ({err}); \
             relying on committed bindings"
        );
    }
}

fn flatc_available() -> bool {
    Command::new("flatc")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

fn run_flatc(schemas_dir: &Path, generated_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(generated_dir)?;

    let mut fbs_files: Vec<PathBuf> = fs::read_dir(schemas_dir)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("fbs"))
        .collect();
    fbs_files.sort();

    if fbs_files.is_empty() {
        println!("cargo:warning=hedge-schemas: no .fbs files in {}", schemas_dir.display());
        return Ok(());
    }

    let mut cmd = Command::new("flatc");
    cmd.arg("--rust")
        .arg("--gen-object-api")
        .arg("-o")
        .arg(generated_dir);
    for fbs in &fbs_files {
        cmd.arg(fbs);
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "flatc exited with status {status}"
        )));
    }
    Ok(())
}
