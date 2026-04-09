use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pulsivo_salesman_api::server::{read_daemon_info, run_daemon};
use pulsivo_salesman_kernel::config::{default_config_path, load_config};
use pulsivo_salesman_kernel::PulsivoSalesmanKernel;
use pulsivo_salesman_types::config::KernelConfig;
use std::path::{Path, PathBuf};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "pulsivo-salesman",
    version,
    about = "Launch and bootstrap the Pulsivo Salesman sales daemon."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the sales daemon.
    Start {
        /// Explicit config path. Defaults to ~/.pulsivo-salesman/config.toml.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Override listen address for this process.
        #[arg(long)]
        listen: Option<String>,
    },
    /// Create a default config.toml and data directories.
    Init {
        /// Accepted for compatibility. Init is already non-interactive.
        #[arg(long)]
        quick: bool,
        /// Overwrite an existing config.toml.
        #[arg(long)]
        force: bool,
        /// Explicit config path. Defaults to ~/.pulsivo-salesman/config.toml.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Override the Pulsivo Salesman home directory.
        #[arg(long)]
        home: Option<PathBuf>,
    },
    /// Print the current daemon status from daemon.json.
    Status {
        /// Override the home directory used to locate daemon.json.
        #[arg(long)]
        home: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Start {
        config: None,
        listen: None,
    }) {
        Command::Init {
            quick: _,
            force,
            config,
            home,
        } => {
            init_tracing("info");
            let config_path = config_path_for(config, home.as_deref());
            let config = seed_config(&config_path, home.as_deref());
            bootstrap_config(&config_path, &config, force)?;
        }
        Command::Start { config, listen } => {
            let config_path = config_path_for(config, None);
            ensure_config_exists(&config_path)?;

            let mut config = load_config(Some(&config_path));
            if let Some(listen_addr) = listen {
                config.api_listen = listen_addr;
            }

            init_tracing(&config.log_level);

            let home_dir = config.home_dir.clone();
            let listen_addr = config.api_listen.clone();
            let daemon_info_path = home_dir.join("daemon.json");
            let kernel = PulsivoSalesmanKernel::boot_with_config(config)
                .context("failed to boot Pulsivo Salesman kernel")?;

            run_daemon(kernel, &listen_addr, Some(&daemon_info_path))
                .await
                .map_err(|error| {
                    anyhow::anyhow!("failed to run Pulsivo Salesman daemon: {error}")
                })?;
        }
        Command::Status { home } => {
            let home_dir = home.unwrap_or_else(default_home_dir);
            if let Some(info) = read_daemon_info(&home_dir) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&info)
                        .context("failed to render daemon status as JSON")?
                );
            } else {
                println!("Pulsivo Salesman daemon is not running.");
            }
        }
    }

    Ok(())
}

fn init_tracing(log_level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn default_home_dir() -> PathBuf {
    default_config_path()
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn config_path_for(config: Option<PathBuf>, home: Option<&Path>) -> PathBuf {
    match (config, home) {
        (Some(path), _) => path,
        (None, Some(home_dir)) => home_dir.join("config.toml"),
        (None, None) => default_config_path(),
    }
}

fn seed_config(config_path: &Path, home_override: Option<&Path>) -> KernelConfig {
    let mut config = KernelConfig::default();
    let home_dir = home_override
        .map(Path::to_path_buf)
        .or_else(|| config_path.parent().map(Path::to_path_buf))
        .unwrap_or_else(default_home_dir);

    config.home_dir = home_dir.clone();
    config.data_dir = home_dir.join("data");
    config.api_listen = "127.0.0.1:4200".to_string();
    config
}

fn ensure_config_exists(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        return Ok(());
    }

    let config = seed_config(config_path, None);
    bootstrap_config(config_path, &config, false)?;
    eprintln!("Bootstrapped default config at {}", config_path.display());
    Ok(())
}

fn bootstrap_config(config_path: &Path, config: &KernelConfig, force: bool) -> Result<()> {
    let home_dir = config.home_dir.clone();
    let data_dir = config.data_dir.clone();
    let agents_dir = home_dir.join("agents");

    std::fs::create_dir_all(&home_dir)
        .with_context(|| format!("failed to create {}", home_dir.display()))?;
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;
    std::fs::create_dir_all(&agents_dir)
        .with_context(|| format!("failed to create {}", agents_dir.display()))?;

    if config_path.exists() && !force {
        println!("Config already exists at {}", config_path.display());
        return Ok(());
    }

    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let rendered = format!(
        "# Pulsivo Salesman configuration\n# Generated by `pulsivo-salesman init`\n\n{}",
        toml::to_string_pretty(config).context("failed to serialize config")?
    );
    std::fs::write(config_path, rendered)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    println!("Wrote {}", config_path.display());
    Ok(())
}
