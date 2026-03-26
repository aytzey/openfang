//! Build automation tasks for the OpenFang workspace.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::{Command, ExitCode};

#[derive(Parser)]
#[command(author, version, about = "Workspace automation commands")]
struct Cli {
    #[command(subcommand)]
    command: XtaskCommand,
}

#[derive(Subcommand)]
enum XtaskCommand {
    /// Run local smoke tests that should stay deterministic and secret-free.
    TestSmoke,
    /// Run the gated live smoke suite that exercises real LLM flows.
    TestLiveSmoke,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        XtaskCommand::TestSmoke => run_group(
            "local smoke",
            &[
                &[
                    "test",
                    "-p",
                    "openfang-api",
                    "--test",
                    "api_integration_test",
                    "--test",
                    "daemon_lifecycle_test",
                    "--test",
                    "load_test",
                    "--",
                    "--nocapture",
                ],
                &[
                    "test",
                    "-p",
                    "openfang-kernel",
                    "--test",
                    "workflow_integration_test",
                    "--",
                    "--nocapture",
                ],
            ],
        ),
        XtaskCommand::TestLiveSmoke => run_group(
            "live smoke",
            &[
                &[
                    "test",
                    "-p",
                    "openfang-api",
                    "--test",
                    "api_integration_test",
                    "test_send_message_with_llm",
                    "--",
                    "--nocapture",
                ],
                &[
                    "test",
                    "-p",
                    "openfang-kernel",
                    "--test",
                    "integration_test",
                    "--",
                    "--nocapture",
                ],
                &[
                    "test",
                    "-p",
                    "openfang-kernel",
                    "--test",
                    "workflow_integration_test",
                    "test_workflow_e2e_with_groq",
                    "--",
                    "--nocapture",
                ],
            ],
        ),
    };

    if let Err(err) = result {
        eprintln!("xtask failed: {err}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}

fn run_group(label: &str, commands: &[&[&str]]) -> Result<(), String> {
    println!("xtask: running {label} suite");
    for args in commands {
        run_cargo(args)?;
    }
    Ok(())
}

fn run_cargo(args: &[&str]) -> Result<(), String> {
    println!("+ cargo {}", args.join(" "));
    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root())
        .status()
        .map_err(|err| format!("failed to spawn cargo {:?}: {err}", args))?;

    if status.success() {
        return Ok(());
    }

    Err(format!("cargo {:?} exited with status {status}", args))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in workspace root child")
        .to_path_buf()
}
