//! Sales-only kernel surface for the PulsivoSalesman daemon.

use crate::config::load_config;
use crate::config_reload::{build_reload_plan, ReloadPlan};
use crate::error::{KernelError, KernelResult};
use crate::registry::AgentRegistry;
use crate::supervisor::Supervisor;

use pulsivo_salesman_memory::MemorySubstrate;
use pulsivo_salesman_runtime::model_catalog::ModelCatalog;
use pulsivo_salesman_types::config::{KernelConfig, WebConfig};

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use tracing::{info, warn};

/// Minimal kernel state required by the sales daemon.
pub struct PulsivoSalesmanKernel {
    /// Live configuration snapshot. Hot-reload swaps this in place.
    pub config: RwLock<KernelConfig>,
    /// Agent registry kept for status/metrics compatibility.
    pub registry: AgentRegistry,
    /// Shared SQLite-backed memory substrate.
    pub memory: Arc<MemorySubstrate>,
    /// Graceful shutdown and health counters.
    pub supervisor: Supervisor,
    /// Provider catalog used by the OAuth/status surface.
    pub model_catalog: RwLock<ModelCatalog>,
    /// Weak self-handle retained for compatibility with the daemon bootstrap.
    self_handle: OnceLock<Weak<PulsivoSalesmanKernel>>,
}

impl PulsivoSalesmanKernel {
    /// Boot the kernel from an optional config path.
    pub fn boot(config_path: Option<&Path>) -> KernelResult<Self> {
        let config = load_config(config_path);
        Self::boot_with_config(config)
    }

    /// Boot the kernel from an explicit configuration.
    pub fn boot_with_config(mut config: KernelConfig) -> KernelResult<Self> {
        config.clamp_bounds();

        for warning in config.validate() {
            warn!("Config: {warning}");
        }

        std::fs::create_dir_all(&config.home_dir)
            .map_err(|e| KernelError::BootFailed(format!("Failed to create home dir: {e}")))?;
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| KernelError::BootFailed(format!("Failed to create data dir: {e}")))?;

        let db_path = config
            .memory
            .sqlite_path
            .clone()
            .unwrap_or_else(|| config.data_dir.join("pulsivo-salesman.db"));
        let memory = Arc::new(
            MemorySubstrate::open(&db_path, config.memory.decay_rate)
                .map_err(|e| KernelError::BootFailed(format!("Memory init failed: {e}")))?,
        );

        let mut model_catalog = ModelCatalog::new();
        model_catalog.detect_auth();

        info!(
            data_dir = %config.data_dir.display(),
            db_path = %db_path.display(),
            available_models = model_catalog.available_models().len(),
            total_models = model_catalog.list_models().len(),
            "Booted sales-only PulsivoSalesman kernel"
        );

        Ok(Self {
            config: RwLock::new(config),
            registry: AgentRegistry::new(),
            memory,
            supervisor: Supervisor::new(),
            model_catalog: RwLock::new(model_catalog),
            self_handle: OnceLock::new(),
        })
    }

    /// Return a cloned configuration snapshot for read-mostly call sites.
    pub fn config_snapshot(&self) -> KernelConfig {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Return the configured PulsivoSalesman home directory.
    pub fn home_dir(&self) -> PathBuf {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .home_dir
            .clone()
    }

    /// Return the configured API key.
    pub fn api_key(&self) -> String {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .api_key
            .clone()
    }

    /// Return the active web tools configuration.
    pub fn web_config(&self) -> WebConfig {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .web
            .clone()
    }

    /// Retain a weak self-handle for compatibility with existing bootstrap code.
    pub fn set_self_handle(self: &Arc<Self>) {
        let _ = self.self_handle.set(Arc::downgrade(self));
    }

    /// Reload config.toml and apply hot-reloadable sales settings in place.
    pub fn reload_config(&self) -> Result<ReloadPlan, String> {
        let current = self.config_snapshot();
        let config_path = current.home_dir.join("config.toml");
        let mut next = load_config(Some(&config_path));
        next.clamp_bounds();

        let plan = build_reload_plan(&current, &next);
        plan.log_summary();

        if !plan.restart_required {
            *self.config.write().unwrap_or_else(|e| e.into_inner()) = next;
            self.model_catalog
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .detect_auth();
        }

        Ok(plan)
    }

    /// Compatibility no-op. The sales daemon no longer boots agent-side background systems.
    pub fn start_background_agents(self: &Arc<Self>) {}

    /// Trigger graceful shutdown.
    pub fn shutdown(&self) {
        self.supervisor.shutdown();
    }
}
