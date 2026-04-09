//! Sales daemon server bootstrap and HTTP router.

use crate::codex_oauth;
use crate::middleware;
use crate::rate_limiter;
use crate::routes::{self, AppState};
use crate::sales;
use crate::webchat;
use axum::routing::{get, patch, post};
use axum::Router;
use pulsivo_salesman_kernel::PulsivoSalesmanKernel;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// Daemon info written to `~/.pulsivo-salesman/daemon.json` so the CLI can find us.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DaemonInfo {
    pub pid: u32,
    pub listen_addr: String,
    pub started_at: String,
    pub version: String,
    pub platform: String,
}

/// Build the sales API router and shared state.
pub async fn build_router(
    kernel: Arc<PulsivoSalesmanKernel>,
    listen_addr: SocketAddr,
) -> (Router<()>, Arc<AppState>) {
    let state = Arc::new(AppState {
        kernel: kernel.clone(),
        started_at: Instant::now(),
        shutdown_notify: Arc::new(tokio::sync::Notify::new()),
    });

    codex_oauth::initialize_codex_auth(&state).await;
    let api_key = state.kernel.api_key();

    let cors = if api_key.is_empty() {
        let port = listen_addr.port();
        let mut origins: Vec<axum::http::HeaderValue> = vec![
            format!("http://{listen_addr}").parse().unwrap(),
            format!("http://localhost:{port}").parse().unwrap(),
        ];
        for dev_port in [3000u16, 8080] {
            if dev_port != port {
                if let Ok(origin) = format!("http://127.0.0.1:{dev_port}").parse() {
                    origins.push(origin);
                }
                if let Ok(origin) = format!("http://localhost:{dev_port}").parse() {
                    origins.push(origin);
                }
            }
        }
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(tower_http::cors::Any)
            .allow_headers(tower_http::cors::Any)
    } else {
        let mut origins: Vec<axum::http::HeaderValue> = vec![
            format!("http://{listen_addr}").parse().unwrap(),
            "http://localhost:4200".parse().unwrap(),
            "http://127.0.0.1:4200".parse().unwrap(),
            "http://localhost:8080".parse().unwrap(),
            "http://127.0.0.1:8080".parse().unwrap(),
        ];
        if listen_addr.port() != 4200 && listen_addr.port() != 8080 {
            if let Ok(origin) = format!("http://localhost:{}", listen_addr.port()).parse() {
                origins.push(origin);
            }
            if let Ok(origin) = format!("http://127.0.0.1:{}", listen_addr.port()).parse() {
                origins.push(origin);
            }
        }
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(tower_http::cors::Any)
            .allow_headers(tower_http::cors::Any)
    };

    let app = Router::new()
        .route("/", get(webchat::webchat_page))
        .route("/logo.png", get(webchat::logo_png))
        .route("/favicon.ico", get(webchat::favicon_ico))
        .route("/api/metrics", get(routes::prometheus_metrics))
        .route("/api/health", get(routes::health))
        .route("/api/health/detail", get(routes::health_detail))
        .route("/api/status", get(routes::status))
        .route("/api/version", get(routes::version))
        .route(
            "/api/auth/codex/start",
            post(codex_oauth::codex_oauth_start),
        )
        .route(
            "/api/auth/codex/callback",
            get(codex_oauth::codex_oauth_callback),
        )
        .route("/auth/callback", get(codex_oauth::codex_oauth_callback))
        .route(
            "/api/auth/codex/paste-code",
            post(codex_oauth::codex_oauth_paste_code),
        )
        .route(
            "/api/auth/codex/import-cli",
            post(codex_oauth::codex_oauth_import_cli),
        )
        .route(
            "/api/auth/codex/status",
            get(codex_oauth::codex_oauth_status),
        )
        .route(
            "/api/auth/codex/logout",
            post(codex_oauth::codex_oauth_logout),
        )
        .route(
            "/api/sales/profile",
            get(sales::get_sales_profile).put(sales::put_sales_profile),
        )
        .route(
            "/api/sales/profile/autofill",
            post(sales::autofill_sales_profile),
        )
        .route(
            "/api/sales/onboarding/status",
            get(sales::get_sales_onboarding_status),
        )
        .route(
            "/api/sales/onboarding/brief",
            post(sales::put_sales_onboarding_brief),
        )
        .route("/api/sales/run", post(sales::run_sales_now))
        .route(
            "/api/sales/jobs/active",
            get(sales::get_active_sales_job_progress),
        )
        .route(
            "/api/sales/jobs/{job_id}/progress",
            get(sales::get_sales_job_progress),
        )
        .route(
            "/api/sales/jobs/{job_id}/retry",
            post(sales::retry_sales_job),
        )
        .route(
            "/api/sales/source-health",
            get(sales::list_sales_source_health),
        )
        .route(
            "/api/sales/policy-proposals",
            get(sales::list_sales_policy_proposals),
        )
        .route(
            "/api/sales/policy-proposals/{id}/approve",
            post(sales::approve_sales_policy_proposal),
        )
        .route(
            "/api/sales/policy-proposals/{id}/reject",
            post(sales::reject_sales_policy_proposal),
        )
        .route("/api/sales/runs", get(sales::list_sales_runs))
        .route("/api/sales/leads", get(sales::list_sales_leads))
        .route("/api/sales/prospects", get(sales::list_sales_prospects))
        .route(
            "/api/sales/accounts/{id}/dossier",
            get(sales::get_sales_account_dossier),
        )
        .route("/api/sales/unsubscribe", get(sales::sales_unsubscribe))
        .route(
            "/api/sales/outcomes/webhook",
            post(sales::sales_outcomes_webhook),
        )
        .route(
            "/api/sales/sequences/advance",
            post(sales::advance_sales_sequences),
        )
        .route(
            "/api/sales/experiments",
            get(sales::list_sales_experiments).post(sales::create_sales_experiment),
        )
        .route(
            "/api/sales/experiments/{id}/results",
            get(sales::get_sales_experiment_results),
        )
        .route(
            "/api/sales/context-factors",
            get(sales::list_sales_context_factors),
        )
        .route(
            "/api/sales/calibration/run",
            post(sales::run_sales_calibration),
        )
        .route("/api/sales/approvals", get(sales::list_sales_approvals))
        .route(
            "/api/sales/approvals/bulk-approve",
            post(sales::bulk_approve_sales_approvals),
        )
        .route(
            "/api/sales/approvals/{id}/edit",
            patch(sales::edit_sales_approval),
        )
        .route(
            "/api/sales/approvals/{id}/approve",
            post(sales::approve_and_send),
        )
        .route(
            "/api/sales/approvals/{id}/reject",
            post(sales::reject_sales_approval),
        )
        .route("/api/sales/deliveries", get(sales::list_sales_deliveries))
        .layer(axum::middleware::from_fn_with_state(
            api_key,
            middleware::auth,
        ));

    let app = app
        .layer(axum::middleware::from_fn_with_state(
            rate_limiter::create_rate_limiter(),
            rate_limiter::gcra_rate_limit,
        ))
        .layer(axum::middleware::from_fn(middleware::security_headers))
        .layer(axum::middleware::from_fn(middleware::request_logging))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state::<()>(state.clone());

    (app, state)
}

/// Start the PulsivoSalesman sales daemon: boot kernel + HTTP API server.
pub async fn run_daemon(
    kernel: PulsivoSalesmanKernel,
    listen_addr: &str,
    daemon_info_path: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = listen_addr.parse()?;

    let kernel = Arc::new(kernel);
    kernel.set_self_handle();
    kernel.start_background_agents();
    sales::spawn_sales_scheduler(kernel.clone());

    {
        let hot_reload_kernel = kernel.clone();
        let config_path = kernel.home_dir().join("config.toml");
        tokio::spawn(async move {
            let mut last_modified = std::fs::metadata(&config_path)
                .and_then(|metadata| metadata.modified())
                .ok();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let current = std::fs::metadata(&config_path)
                    .and_then(|metadata| metadata.modified())
                    .ok();
                if current != last_modified && current.is_some() {
                    last_modified = current;
                    tracing::info!("Config file changed, reloading...");
                    match hot_reload_kernel.reload_config() {
                        Ok(plan) => {
                            if plan.has_changes() {
                                tracing::info!("Config hot-reload applied: {:?}", plan.hot_actions);
                            } else {
                                tracing::debug!("Config hot-reload: no actionable changes");
                            }
                        }
                        Err(error) => tracing::warn!("Config hot-reload failed: {error}"),
                    }
                }
            }
        });
    }

    let (app, state) = build_router(kernel.clone(), addr).await;

    if let Some(info_path) = daemon_info_path {
        if info_path.exists() {
            if let Ok(existing) = std::fs::read_to_string(info_path) {
                if let Ok(info) = serde_json::from_str::<DaemonInfo>(&existing) {
                    if is_process_alive(info.pid) {
                        return Err(format!(
                            "Another daemon (PID {}) is already running at {}",
                            info.pid, info.listen_addr
                        )
                        .into());
                    }
                }
            }
            let _ = std::fs::remove_file(info_path);
        }

        let daemon_info = DaemonInfo {
            pid: std::process::id(),
            listen_addr: addr.to_string(),
            started_at: chrono::Utc::now().to_rfc3339(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            platform: std::env::consts::OS.to_string(),
        };
        if let Ok(json) = serde_json::to_string_pretty(&daemon_info) {
            let _ = std::fs::write(info_path, json);
            restrict_permissions(info_path);
        }
    }

    info!("PulsivoSalesman API server listening on http://{addr}");
    info!("Sales cockpit available at http://{addr}/");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let api_shutdown = state.shutdown_notify.clone();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(api_shutdown))
    .await?;

    if let Some(info_path) = daemon_info_path {
        let _ = std::fs::remove_file(info_path);
    }

    kernel.shutdown();

    info!("PulsivoSalesman daemon stopped");
    Ok(())
}

/// SECURITY: Restrict file permissions to owner-only (0600) on Unix.
#[cfg(unix)]
fn restrict_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) {}

/// Read daemon info from the standard location.
pub fn read_daemon_info(home_dir: &Path) -> Option<DaemonInfo> {
    let info_path = home_dir.join("daemon.json");
    let contents = std::fs::read_to_string(info_path).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Wait for an OS termination signal OR an API shutdown request.
async fn shutdown_signal(api_shutdown: Arc<tokio::sync::Notify>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to listen for SIGINT");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");

        tokio::select! {
            _ = sigint.recv() => info!("Received SIGINT (Ctrl+C), shutting down..."),
            _ = sigterm.recv() => info!("Received SIGTERM, shutting down..."),
            _ = api_shutdown.notified() => info!("Shutdown requested via API, shutting down..."),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("Ctrl+C received, shutting down..."),
            _ = api_shutdown.notified() => info!("Shutdown requested via API, shutting down..."),
        }
    }
}

/// Check if a process with the given PID is still alive.
fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        std::path::Path::new("/proc").join(pid.to_string()).exists()
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, STILL_ACTIVE,
        };

        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle == 0 {
                return false;
            }
            let mut exit_code = 0;
            let ok = GetExitCodeProcess(handle, &mut exit_code) != 0;
            CloseHandle(handle);
            ok && exit_code == STILL_ACTIVE
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Method, Request, StatusCode};
    use pulsivo_salesman_types::config::KernelConfig;
    use tower::ServiceExt;

    fn request_with_loopback(
        method: Method,
        uri: &str,
        body: Body,
        is_json: bool,
    ) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if is_json {
            builder = builder.header("content-type", "application/json");
        }
        let mut request = builder.body(body).expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 8080))));
        request
    }

    fn test_kernel(home_dir: &Path) -> Arc<PulsivoSalesmanKernel> {
        let mut config = KernelConfig::default();
        config.home_dir = home_dir.to_path_buf();
        config.data_dir = home_dir.join("data");
        Arc::new(PulsivoSalesmanKernel::boot_with_config(config).expect("kernel"))
    }

    #[tokio::test]
    async fn router_exposes_sales_unsubscribe_and_admin_routes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let kernel = test_kernel(temp.path());
        sales::SalesEngine::new(&kernel.home_dir())
            .init()
            .expect("sales init");

        let (app, _state) =
            build_router(kernel, "127.0.0.1:4200".parse().expect("listen addr")).await;

        let unsubscribe = app
            .clone()
            .oneshot(request_with_loopback(
                Method::GET,
                "/api/sales/unsubscribe?token=invalid",
                Body::empty(),
                false,
            ))
            .await
            .expect("unsubscribe response");
        assert_eq!(unsubscribe.status(), StatusCode::BAD_REQUEST);

        let proposals = app
            .clone()
            .oneshot(request_with_loopback(
                Method::GET,
                "/api/sales/policy-proposals?limit=1",
                Body::empty(),
                false,
            ))
            .await
            .expect("policy proposals response");
        assert_eq!(proposals.status(), StatusCode::OK);

        let experiments = app
            .clone()
            .oneshot(request_with_loopback(
                Method::GET,
                "/api/sales/experiments",
                Body::empty(),
                false,
            ))
            .await
            .expect("experiments response");
        assert_eq!(experiments.status(), StatusCode::OK);

        let context_factors = app
            .clone()
            .oneshot(request_with_loopback(
                Method::GET,
                "/api/sales/context-factors",
                Body::empty(),
                false,
            ))
            .await
            .expect("context factors response");
        assert_eq!(context_factors.status(), StatusCode::OK);

        let calibration = app
            .clone()
            .oneshot(request_with_loopback(
                Method::POST,
                "/api/sales/calibration/run",
                Body::empty(),
                false,
            ))
            .await
            .expect("calibration response");
        assert_eq!(calibration.status(), StatusCode::OK);

        let outcomes = app
            .clone()
            .oneshot(request_with_loopback(
                Method::POST,
                "/api/sales/outcomes/webhook",
                Body::from(r#"{"delivery_id":"missing","event_type":"unsubscribe","raw_text":""}"#),
                true,
            ))
            .await
            .expect("outcomes response");
        assert_eq!(outcomes.status(), StatusCode::BAD_REQUEST);

        let advance = app
            .oneshot(request_with_loopback(
                Method::POST,
                "/api/sales/sequences/advance",
                Body::empty(),
                false,
            ))
            .await
            .expect("advance response");
        assert_eq!(advance.status(), StatusCode::OK);
    }
}
