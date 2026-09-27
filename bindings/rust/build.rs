//! Builds libtinysql.a from the Go sources of the unified C ABI
//! (bindings/c) and links it statically.
//!
//! Set TINYSQL_LIB_DIR to a directory containing a prebuilt `libtinysql.a`
//! (for example, from `go build -buildmode=c-archive`) to skip the Go build,
//! e.g. when cross-compiling or when Go is not installed.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=TINYSQL_LIB_DIR");
    println!("cargo:rerun-if-env-changed=GO");
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    let lib_dir = match env::var_os("TINYSQL_LIB_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => build_with_go(&target_os),
    };
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=tinysql");

    // System libraries required by the Go runtime inside the archive.
    match target_os.as_str() {
        "macos" | "ios" => {
            println!("cargo:rustc-link-lib=framework=CoreFoundation");
            println!("cargo:rustc-link-lib=framework=Security");
            println!("cargo:rustc-link-lib=resolv");
        }
        "windows" => {
            for lib in ["ws2_32", "userenv", "bcrypt", "ntdll", "winmm"] {
                println!("cargo:rustc-link-lib={lib}");
            }
        }
        _ => {
            for lib in ["pthread", "dl", "m"] {
                println!("cargo:rustc-link-lib={lib}");
            }
        }
    }
}

fn build_with_go(target_os: &str) -> PathBuf {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest
        .ancestors()
        .find(|dir| dir.join("go.mod").is_file() && dir.join("bindings/c").is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "tinySQL Go sources not found above {}; set TINYSQL_LIB_DIR to a directory with libtinysql.a",
                manifest.display()
            )
        });
    for path in [
        "go.mod",
        "go.sum",
        "bindings/c",
        "internal",
        "driver",
        "sqlutil",
        "standards",
    ] {
        println!("cargo:rerun-if-changed={}", root.join(path).display());
    }
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "go") {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let archive = out.join("libtinysql.a");
    let go = env::var("GO").unwrap_or_else(|_| "go".to_string());
    let mut command = Command::new(&go);
    command
        .current_dir(&root)
        .args(["build", "-trimpath", "-buildmode=c-archive", "-o"])
        .arg(&archive)
        .arg("./bindings/c")
        .env("CGO_ENABLED", "1");
    if let Some(goos) = go_os(target_os) {
        command.env("GOOS", goos);
    }
    if let Some(goarch) = go_arch(&env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default()) {
        command.env("GOARCH", goarch);
    }
    let status = command.status().unwrap_or_else(|error| {
        panic!("failed to run `{go}` ({error}); install Go or set TINYSQL_LIB_DIR")
    });
    if !status.success() {
        panic!("`{go} build -buildmode=c-archive ./bindings/c` failed with {status}");
    }
    out
}

fn go_os(target_os: &str) -> Option<&'static str> {
    Some(match target_os {
        "linux" => "linux",
        "android" => "android",
        "macos" => "darwin",
        "ios" => "ios",
        "windows" => "windows",
        "freebsd" => "freebsd",
        _ => return None,
    })
}

fn go_arch(target_arch: &str) -> Option<&'static str> {
    Some(match target_arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "arm" => "arm",
        "riscv64" => "riscv64",
        _ => return None,
    })
}
