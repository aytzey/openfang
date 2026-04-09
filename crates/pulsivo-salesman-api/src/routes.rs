//! Shared state and core health/status routes for the sales-only API.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use pulsivo_salesman_kernel::PulsivoSalesmanKernel;
use pulsivo_salesman_types::agent::{AgentId, AgentState};
use std::sync::Arc;
use std::time::Instant;

/// Shared application state for the sales daemon.
pub struct AppState {
    pub kernel: Arc<PulsivoSalesmanKernel>,
    pub started_at: Instant,
    /// Notify handle used by the daemon and embedded server for graceful shutdown.
    pub shutdown_notify: Arc<tokio::sync::Notify>,
}

fn health_probe_agent_id() -> AgentId {
    AgentId(uuid::Uuid::from_bytes([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ]))
}

/// GET /api/status — Runtime heartbeat for the sales cockpit.
pub async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.kernel.config_snapshot();
    let agents: Vec<serde_json::Value> = state
        .kernel
        .registry
        .list()
        .into_iter()
        .map(|entry| {
            serde_json::json!({
                "id": entry.id.to_string(),
                "name": entry.name,
                "state": format!("{:?}", entry.state),
                "mode": entry.mode,
                "created_at": entry.created_at.to_rfc3339(),
                "model_provider": entry.manifest.model.provider,
                "model_name": entry.manifest.model.model,
                "profile": entry.manifest.profile,
            })
        })
        .collect();

    Json(serde_json::json!({
        "status": "running",
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "agent_count": agents.len(),
        "default_provider": config.default_model.provider,
        "default_model": config.default_model.model,
        "agents": agents,
    }))
}

/// GET /api/version — Build metadata.
pub async fn version() -> impl IntoResponse {
    Json(serde_json::json!({
        "name": "pulsivo-salesman",
        "version": env!("CARGO_PKG_VERSION"),
        "build_date": option_env!("BUILD_DATE").unwrap_or("dev"),
        "git_sha": option_env!("GIT_SHA").unwrap_or("unknown"),
        "rust_version": option_env!("RUSTC_VERSION").unwrap_or("unknown"),
        "platform": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
    }))
}

/// GET /api/health — Lightweight readiness check.
pub async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_ok = state
        .kernel
        .memory
        .structured_get(health_probe_agent_id(), "__health_check__")
        .is_ok();

    Json(serde_json::json!({
        "status": if db_ok { "ok" } else { "degraded" },
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /api/health/detail — Extended diagnostics.
pub async fn health_detail(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.kernel.config_snapshot();
    let supervisor = state.kernel.supervisor.health();
    let db_ok = state
        .kernel
        .memory
        .structured_get(health_probe_agent_id(), "__health_check__")
        .is_ok();

    Json(serde_json::json!({
        "status": if db_ok { "ok" } else { "degraded" },
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "panic_count": supervisor.panic_count,
        "restart_count": supervisor.restart_count,
        "agent_count": state.kernel.registry.count(),
        "database": if db_ok { "connected" } else { "error" },
        "config_warnings": config.validate(),
    }))
}

/// GET /api/metrics — Minimal Prometheus metrics for the sales daemon.
pub async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.started_at.elapsed().as_secs();
    let agents = state.kernel.registry.list();
    let active = agents
        .iter()
        .filter(|entry| matches!(entry.state, AgentState::Running))
        .count();
    let supervisor = state.kernel.supervisor.health();

    let body = format!(
        "# HELP pulsivo_salesman_uptime_seconds Time since daemon started.\n\
         # TYPE pulsivo_salesman_uptime_seconds gauge\n\
         pulsivo_salesman_uptime_seconds {uptime}\n\n\
         # HELP pulsivo_salesman_agents_active Number of running agents.\n\
         # TYPE pulsivo_salesman_agents_active gauge\n\
         pulsivo_salesman_agents_active {active}\n\
         # HELP pulsivo_salesman_agents_total Total number of registered agents.\n\
         # TYPE pulsivo_salesman_agents_total gauge\n\
         pulsivo_salesman_agents_total {total}\n\n\
         # HELP pulsivo_salesman_panics_total Supervisor panic count.\n\
         # TYPE pulsivo_salesman_panics_total counter\n\
         pulsivo_salesman_panics_total {panic_count}\n\
         # HELP pulsivo_salesman_restarts_total Supervisor restart count.\n\
         # TYPE pulsivo_salesman_restarts_total counter\n\
         pulsivo_salesman_restarts_total {restart_count}\n",
        total = agents.len(),
        panic_count = supervisor.panic_count,
        restart_count = supervisor.restart_count,
    );

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}
