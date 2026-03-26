#![allow(dead_code)]

use openfang_api::rate_limiter::KeyedRateLimiter;
use openfang_api::routes::AppState;
use openfang_api::server::{build_router_with_options, RouterBuildOptions};
use openfang_kernel::OpenFangKernel;
use openfang_types::config::{DefaultModelConfig, KernelConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub struct TestModelConfig {
    pub provider: &'static str,
    pub model: &'static str,
    pub api_key_env: &'static str,
}

pub const OLLAMA_TEST_MODEL: TestModelConfig = TestModelConfig {
    provider: "ollama",
    model: "test-model",
    api_key_env: "OLLAMA_API_KEY",
};

pub const GROQ_TEST_MODEL: TestModelConfig = TestModelConfig {
    provider: "groq",
    model: "llama-3.3-70b-versatile",
    api_key_env: "GROQ_API_KEY",
};

pub struct TestServerBuilder {
    model: TestModelConfig,
    api_key: Option<String>,
    rate_limiter: Option<Arc<KeyedRateLimiter>>,
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        Self {
            model: OLLAMA_TEST_MODEL,
            api_key: None,
            rate_limiter: None,
        }
    }
}

impl TestServerBuilder {
    pub fn with_model(mut self, model: TestModelConfig) -> Self {
        self.model = model;
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_rate_limiter(mut self, rate_limiter: Arc<KeyedRateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    pub async fn start(self) -> TestServer {
        let tmp = tempfile::tempdir().expect("Failed to create temp dir");

        let mut config = KernelConfig {
            home_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().join("data"),
            default_model: DefaultModelConfig {
                provider: self.model.provider.to_string(),
                model: self.model.model.to_string(),
                api_key_env: self.model.api_key_env.to_string(),
                base_url: None,
                reasoning_effort: None,
            },
            ..KernelConfig::default()
        };

        if let Some(api_key) = self.api_key {
            config.api_key = api_key;
        }

        let kernel = Arc::new(
            OpenFangKernel::boot_with_config(config).expect("Kernel should boot for test server"),
        );
        kernel.set_self_handle();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind test server");
        let addr = listener.local_addr().expect("listener addr");

        let (app, state) = build_router_with_options(
            kernel,
            addr,
            RouterBuildOptions {
                rate_limiter: self.rate_limiter,
            },
        )
        .await;

        let shutdown_notify = state.shutdown_notify.clone();
        let server_task = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                shutdown_notify.notified().await;
            })
            .await
            .expect("Test HTTP server should run");
        });

        let server = TestServer {
            base_url: format!("http://{addr}"),
            addr,
            state,
            server_task,
            _tmp: tmp,
        };
        server.wait_until_ready().await;
        server
    }
}

pub struct TestServer {
    pub base_url: String,
    pub addr: SocketAddr,
    pub state: Arc<AppState>,
    server_task: tokio::task::JoinHandle<()>,
    _tmp: tempfile::TempDir,
}

impl TestServer {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub fn authed_client(&self, api_key: &str) -> reqwest::Client {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {api_key}")
                .parse()
                .expect("valid auth header"),
        );
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("auth client")
    }

    pub async fn wait_until_ready(&self) {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(5);

        loop {
            match client.get(self.url("/api/health")).send().await {
                Ok(resp) if resp.status().is_success() => return,
                _ if Instant::now() >= deadline => {
                    panic!("Timed out waiting for test server readiness")
                }
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    }

    pub async fn wait_until_stopped(&self) {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(5);

        loop {
            match client.get(self.url("/api/health")).send().await {
                Err(_) => return,
                Ok(resp) if !resp.status().is_success() => return,
                _ if Instant::now() >= deadline => {
                    panic!("Timed out waiting for test server shutdown")
                }
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.state.shutdown_notify.notify_waiters();
        self.server_task.abort();
        self.state.kernel.shutdown();
    }
}

pub fn skip_if_env_missing(env_var: &str, label: &str) -> bool {
    if std::env::var(env_var).is_ok() {
        return false;
    }

    eprintln!("{env_var} not set, skipping {label}");
    true
}
