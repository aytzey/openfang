use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-changed=ui");
    println!("cargo:rerun-if-changed=static/html");
    println!("cargo:rerun-if-changed=static/css");
    println!("cargo:rerun-if-changed=static/vendor");
    println!("cargo:rerun-if-env-changed=BUN_BIN");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing"));
    let ui_dir = manifest_dir.join("ui");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR missing"));
    let bundle_path = out_dir.join("webchat.bundle.js");

    let bun = find_bun().unwrap_or_else(|| {
        panic!(
            "Bun is required to build the embedded webchat UI. Install Bun or set BUN_BIN. \
             Looked in PATH and ~/.bun/bin/bun."
        )
    });

    run_bun(&bun, &ui_dir, &["install", "--save-text-lockfile"], None);
    run_bun(&bun, &ui_dir, &["run", "build:webchat"], Some(&bundle_path));
}

fn find_bun() -> Option<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = env::var_os("BUN_BIN") {
        candidates.push(PathBuf::from(path));
    }
    if let Some(home) = env::var_os("HOME") {
        candidates.push(PathBuf::from(home).join(".bun/bin/bun"));
    }
    candidates.push(PathBuf::from("bun"));

    candidates
        .into_iter()
        .find(|candidate| bun_exists(candidate))
}

fn bun_exists(candidate: &Path) -> bool {
    Command::new(candidate)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run_bun(bun: &Path, ui_dir: &Path, args: &[&str], bundle_path: Option<&Path>) {
    let mut command = Command::new(bun);
    command
        .current_dir(ui_dir)
        .args(args)
        .env("BUN_BIN", bun)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    if let Some(path) = bundle_path {
        command.env("WEBCHAT_OUTFILE", path);
    }

    let status = command.status().unwrap_or_else(|err| {
        panic!(
            "Failed to run Bun command `{}` in {}: {err}",
            args.join(" "),
            ui_dir.display()
        )
    });

    if !status.success() {
        panic!(
            "Bun command `{}` failed in {} with status {}",
            args.join(" "),
            ui_dir.display(),
            status
        );
    }
}
