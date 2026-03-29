//! Real HTTP integration tests for the OpenFang API.
//!
//! These tests boot a real kernel, start a real axum HTTP server on a random
//! port, and hit actual endpoints with reqwest.  No mocking.
//!
//! Tests that require an LLM API call are gated behind GROQ_API_KEY.
//!
//! Run: cargo test -p openfang-api --test api_integration_test -- --nocapture

mod support;

use governor::{clock::DefaultClock, state::keyed::DashMapStateStore, Quota, RateLimiter};
use openfang_types::memory::MemorySource;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use support::{
    skip_if_env_missing, TestServer, TestServerBuilder, GROQ_TEST_MODEL, OLLAMA_TEST_MODEL,
};
use tokio::sync::Mutex;

static CODEX_AUTH_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct EnvSnapshot {
    vars: Vec<(&'static str, Option<String>)>,
}

impl EnvSnapshot {
    fn capture(names: &[&'static str]) -> Self {
        Self {
            vars: names
                .iter()
                .map(|name| (*name, std::env::var(name).ok()))
                .collect(),
        }
    }
}

impl Drop for EnvSnapshot {
    fn drop(&mut self) {
        for (name, value) in &self.vars {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
    }
}

fn codex_cli_auth_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".codex").join("auth.json");
    path.exists().then_some(path)
}

fn skip_if_codex_auth_missing(label: &str) -> bool {
    if codex_cli_auth_path().is_some() {
        return false;
    }
    eprintln!("~/.codex/auth.json not found, skipping {label}");
    true
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

/// Start a test server using ollama as default provider (no API key needed).
/// This lets the kernel boot without any real LLM credentials.
/// Tests that need actual LLM calls should use `start_test_server_with_llm()`.
async fn start_test_server() -> TestServer {
    TestServerBuilder::default()
        .with_model(OLLAMA_TEST_MODEL)
        .start()
        .await
}

/// Start a test server with Groq as the LLM provider (requires GROQ_API_KEY).
async fn start_test_server_with_llm() -> TestServer {
    TestServerBuilder::default()
        .with_model(GROQ_TEST_MODEL)
        .start()
        .await
}

async fn start_test_server_with_auth(api_key: &str) -> TestServer {
    TestServerBuilder::default()
        .with_model(OLLAMA_TEST_MODEL)
        .with_api_key(api_key)
        .start()
        .await
}

/// Manifest that uses ollama (no API key required, won't make real LLM calls).
const TEST_MANIFEST: &str = r#"
name = "test-agent"
version = "0.1.0"
description = "Integration test agent"
author = "test"
module = "builtin:chat"

[model]
provider = "ollama"
model = "test-model"
system_prompt = "You are a test agent. Reply concisely."

[capabilities]
tools = ["file_read"]
memory_read = ["*"]
memory_write = ["self.*"]
"#;

/// Manifest that uses Groq for real LLM tests.
const LLM_MANIFEST: &str = r#"
name = "test-agent"
version = "0.1.0"
description = "Integration test agent"
author = "test"
module = "builtin:chat"

[model]
provider = "groq"
model = "llama-3.3-70b-versatile"
system_prompt = "You are a test agent. Reply concisely."

[capabilities]
tools = ["file_read"]
memory_read = ["*"]
memory_write = ["self.*"]
"#;

const SALES_BRIEF: &str = r#"
Yeni Takim Arkadasiniz: Machinity
Projeleri takip etmek yerine projeleri yoneten bir AI ekip arkadasi.
Toplantidan yonetime, WhatsApp'tan proje panosuna: uctan uca otonom koordinasyon.
Saha operasyonu olan sirketlere odaklaniyoruz: field service, maintenance, installation, construction, facility management.
Machinity toplantiya katilir, aksiyonlari yakalar, gorevleri olusturur, dogru kisilere atar ve WhatsApp uzerinden takip eder.
Kurulum suresi 5 dakikanin altinda. Iletisim: machinity.ai info@machinity.ai
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_endpoint() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/api/health", server.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Middleware injects x-request-id
    assert!(resp.headers().contains_key("x-request-id"));

    let body: serde_json::Value = resp.json().await.unwrap();
    // Public health endpoint returns minimal info (redacted for security)
    assert_eq!(body["status"], "ok");
    assert!(body["version"].is_string());
    // Detailed fields should NOT appear in public health endpoint
    assert!(body["database"].is_null());
    assert!(body["agent_count"].is_null());
}

#[tokio::test]
async fn test_status_endpoint() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/api/status", server.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "running");
    assert_eq!(body["agent_count"], 0);
    assert!(body["uptime_seconds"].is_number());
    assert_eq!(body["default_provider"], "openai-codex");
    assert_eq!(body["default_model"], "gpt-5.3-codex");
    assert_eq!(body["agents"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_spawn_list_kill_agent() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // --- Spawn ---
    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "test-agent");
    let agent_id = body["agent_id"].as_str().unwrap().to_string();
    assert!(!agent_id.is_empty());

    // --- List (1 agent) ---
    let resp = client
        .get(format!("{}/api/agents", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["name"], "test-agent");
    assert_eq!(agents[0]["id"], agent_id);
    assert_eq!(agents[0]["model_provider"], "openai-codex");
    assert_eq!(agents[0]["model_name"], "gpt-5.3-codex");

    // --- Kill ---
    let resp = client
        .delete(format!("{}/api/agents/{}", server.base_url, agent_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "killed");

    // --- List (empty) ---
    let resp = client
        .get(format!("{}/api/agents", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 0);
}

#[tokio::test]
async fn test_agent_session_empty() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Spawn agent
    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_id = body["agent_id"].as_str().unwrap();

    // Session should be empty — no messages sent yet
    let resp = client
        .get(format!(
            "{}/api/agents/{}/session",
            server.base_url, agent_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["message_count"], 0);
    assert_eq!(body["messages"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_memory_search_endpoint_prefers_lookup_matched_memory() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_id = body["agent_id"].as_str().unwrap().to_string();
    let parsed_agent_id: openfang_types::agent::AgentId = agent_id.parse().unwrap();

    let target = "rare rust ownership lifetime pattern";
    server
        .state
        .kernel
        .memory
        .remember_with_embedding(
            parsed_agent_id,
            target,
            MemorySource::Conversation,
            "episodic",
            HashMap::new(),
            None,
        )
        .unwrap();

    for i in 0..120 {
        server
            .state
            .kernel
            .memory
            .remember_with_embedding(
                parsed_agent_id,
                &format!("filler memory {i} unrelated topic"),
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                None,
            )
            .unwrap();
    }

    let resp = client
        .get(format!(
            "{}/api/memory/agents/{}/search",
            server.base_url, agent_id
        ))
        .query(&[("q", target), ("limit", "3")])
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["content"], target);
    assert!(body["strategy"].as_str().unwrap().contains("lookup"));
    assert!(results[0]["score"].as_f64().unwrap() > 0.0);
    assert!(results[0]["gate"].as_f64().unwrap() > 0.0);
    assert!(results[0]["lexical_confidence"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn test_codex_auth_import_status_and_logout_roundtrip() {
    if skip_if_codex_auth_missing("Codex auth import integration test") {
        return;
    }

    let _guard = CODEX_AUTH_TEST_GUARD.lock().await;
    let _env = EnvSnapshot::capture(&["OPENAI_CODEX_ACCESS_TOKEN", "OPENAI_CODEX_ACCOUNT_ID"]);
    std::env::remove_var("OPENAI_CODEX_ACCESS_TOKEN");
    std::env::remove_var("OPENAI_CODEX_ACCOUNT_ID");

    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/auth/codex/import-cli", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "connected");

    let resp = client
        .get(format!("{}/api/auth/codex/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["connected"], true);
    assert_eq!(body["provider"], "openai-codex");

    let resp = client
        .get(format!("{}/api/sales/onboarding/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"]["oauth_connected"], true);

    let resp = client
        .post(format!("{}/api/auth/codex/logout", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "logged_out");

    let resp = client
        .get(format!("{}/api/auth/codex/status", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["connected"], false);
    assert_eq!(body["source"], "logged_out");
}

#[tokio::test]
async fn test_codex_auth_import_enables_real_agent_message() {
    if skip_if_codex_auth_missing("Codex real message integration test") {
        return;
    }

    let _guard = CODEX_AUTH_TEST_GUARD.lock().await;
    let _env = EnvSnapshot::capture(&["OPENAI_CODEX_ACCESS_TOKEN", "OPENAI_CODEX_ACCOUNT_ID"]);
    std::env::remove_var("OPENAI_CODEX_ACCESS_TOKEN");
    std::env::remove_var("OPENAI_CODEX_ACCOUNT_ID");

    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/auth/codex/import-cli", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_id = body["agent_id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!(
            "{}/api/agents/{}/message",
            server.base_url, agent_id
        ))
        .json(&serde_json::json!({
            "message": "Respond with only OPENFANG_AUTH_OK. No punctuation, no extra words."
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let response = body["response"].as_str().unwrap_or_default();
    assert!(!response.trim().is_empty(), "response should not be empty");
    assert!(
        response.contains("OPENFANG_AUTH_OK"),
        "unexpected response: {response}"
    );
    assert!(body["input_tokens"].as_u64().unwrap_or(0) > 0);
    assert!(body["output_tokens"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test]
async fn test_sales_brief_autofill_with_codex_auth() {
    if skip_if_codex_auth_missing("Sales autofill integration test") {
        return;
    }

    let _guard = CODEX_AUTH_TEST_GUARD.lock().await;
    let _env = EnvSnapshot::capture(&["OPENAI_CODEX_ACCESS_TOKEN", "OPENAI_CODEX_ACCOUNT_ID"]);
    std::env::remove_var("OPENAI_CODEX_ACCESS_TOKEN");
    std::env::remove_var("OPENAI_CODEX_ACCOUNT_ID");

    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/auth/codex/import-cli", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .post(format!("{}/api/sales/onboarding/brief", server.base_url))
        .json(&serde_json::json!({
            "brief": SALES_BRIEF,
            "persist": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(matches!(
        body["source"].as_str(),
        Some("llm") | Some("heuristic")
    ));
    assert!(!body["profile"]["product_name"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .is_empty());
    assert!(!body["profile"]["product_description"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .is_empty());
    assert!(!body["profile"]["target_industry"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .is_empty());
    assert_eq!(body["profile"]["target_geo"], "TR");
    assert_eq!(body["profile"]["sender_email"], "info@machinity.ai");
    assert_eq!(body["onboarding"]["has_brief"], true);
    assert_eq!(body["onboarding"]["profile_ready"], true);

    let resp = client
        .get(format!("{}/api/sales/profile", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let profile: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(profile["profile"]["target_geo"], "TR");
    assert_eq!(profile["profile"]["sender_email"], "info@machinity.ai");
}

#[tokio::test]
async fn test_send_message_with_llm() {
    if skip_if_env_missing("GROQ_API_KEY", "LLM integration test") {
        return;
    }

    let server = start_test_server_with_llm().await;
    let client = reqwest::Client::new();

    // Spawn
    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": LLM_MANIFEST}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_id = body["agent_id"].as_str().unwrap().to_string();

    // Send message through the real HTTP endpoint → kernel → Groq LLM
    let resp = client
        .post(format!(
            "{}/api/agents/{}/message",
            server.base_url, agent_id
        ))
        .json(&serde_json::json!({"message": "Say hello in exactly 3 words."}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let response_text = body["response"].as_str().unwrap();
    assert!(
        !response_text.is_empty(),
        "LLM response should not be empty"
    );
    assert!(body["input_tokens"].as_u64().unwrap() > 0);
    assert!(body["output_tokens"].as_u64().unwrap() > 0);

    // Session should now have messages
    let resp = client
        .get(format!(
            "{}/api/agents/{}/session",
            server.base_url, agent_id
        ))
        .send()
        .await
        .unwrap();
    let session: serde_json::Value = resp.json().await.unwrap();
    assert!(session["message_count"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn test_workflow_crud() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Spawn agent for workflow
    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_name = body["name"].as_str().unwrap().to_string();

    // Create workflow
    let resp = client
        .post(format!("{}/api/workflows", server.base_url))
        .json(&serde_json::json!({
            "name": "test-workflow",
            "description": "Integration test workflow",
            "steps": [
                {
                    "name": "step1",
                    "agent_name": agent_name,
                    "prompt": "Echo: {{input}}",
                    "mode": "sequential",
                    "timeout_secs": 30
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let workflow_id = body["workflow_id"].as_str().unwrap().to_string();
    assert!(!workflow_id.is_empty());

    // List workflows
    let resp = client
        .get(format!("{}/api/workflows", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let workflows: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(workflows.len(), 1);
    assert_eq!(workflows[0]["name"], "test-workflow");
    assert_eq!(workflows[0]["steps"], 1);
}

#[tokio::test]
async fn test_trigger_crud() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Spawn agent for trigger
    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": TEST_MANIFEST}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let agent_id = body["agent_id"].as_str().unwrap().to_string();

    // Create trigger (Lifecycle pattern — simplest variant)
    let resp = client
        .post(format!("{}/api/triggers", server.base_url))
        .json(&serde_json::json!({
            "agent_id": agent_id,
            "pattern": "lifecycle",
            "prompt_template": "Handle: {{event}}",
            "max_fires": 5
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let trigger_id = body["trigger_id"].as_str().unwrap().to_string();
    assert_eq!(body["agent_id"], agent_id);

    // List triggers (unfiltered)
    let resp = client
        .get(format!("{}/api/triggers", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let triggers: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0]["agent_id"], agent_id);
    assert_eq!(triggers[0]["enabled"], true);
    assert_eq!(triggers[0]["max_fires"], 5);

    // List triggers (filtered by agent_id)
    let resp = client
        .get(format!(
            "{}/api/triggers?agent_id={}",
            server.base_url, agent_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let triggers: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(triggers.len(), 1);

    // Delete trigger
    let resp = client
        .delete(format!("{}/api/triggers/{}", server.base_url, trigger_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List triggers (should be empty)
    let resp = client
        .get(format!("{}/api/triggers", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let triggers: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(triggers.len(), 0);
}

#[tokio::test]
async fn test_invalid_agent_id_returns_400() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Send message to invalid ID
    let resp = client
        .post(format!("{}/api/agents/not-a-uuid/message", server.base_url))
        .json(&serde_json::json!({"message": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Invalid"));

    // Kill invalid ID
    let resp = client
        .delete(format!("{}/api/agents/not-a-uuid", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Session for invalid ID
    let resp = client
        .get(format!("{}/api/agents/not-a-uuid/session", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn test_kill_nonexistent_agent_returns_404() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let fake_id = uuid::Uuid::new_v4();
    let resp = client
        .delete(format!("{}/api/agents/{}", server.base_url, fake_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_spawn_invalid_manifest_returns_400() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/agents", server.base_url))
        .json(&serde_json::json!({"manifest_toml": "this is {{ not valid toml"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Invalid manifest"));
}

#[tokio::test]
async fn test_request_id_header_is_uuid() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/api/health", server.base_url))
        .send()
        .await
        .unwrap();

    let request_id = resp
        .headers()
        .get("x-request-id")
        .expect("x-request-id header should be present");
    let id_str = request_id.to_str().unwrap();
    assert!(
        uuid::Uuid::parse_str(id_str).is_ok(),
        "x-request-id should be a valid UUID, got: {}",
        id_str
    );
}

#[tokio::test]
async fn test_rate_limit_returns_429_when_budget_is_exhausted() {
    let limiter = RateLimiter::<_, DashMapStateStore<_>, DefaultClock>::keyed(Quota::per_minute(
        std::num::NonZeroU32::new(1).unwrap(),
    ));
    let server = TestServerBuilder::default()
        .with_rate_limiter(std::sync::Arc::new(limiter))
        .start()
        .await;
    let client = reqwest::Client::new();

    let response = client.get(server.url("/api/agents")).send().await.unwrap();
    assert_eq!(response.status(), 429);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "Rate limit exceeded");
}

#[tokio::test]
async fn test_multiple_agents_lifecycle() {
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Spawn 3 agents
    let mut ids = Vec::new();
    for i in 0..3 {
        let manifest = format!(
            r#"
name = "agent-{i}"
version = "0.1.0"
description = "Multi-agent test {i}"
author = "test"
module = "builtin:chat"

[model]
provider = "ollama"
model = "test-model"
system_prompt = "Agent {i}."

[capabilities]
memory_read = ["*"]
memory_write = ["self.*"]
"#
        );

        let resp = client
            .post(format!("{}/api/agents", server.base_url))
            .json(&serde_json::json!({"manifest_toml": manifest}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
        let body: serde_json::Value = resp.json().await.unwrap();
        ids.push(body["agent_id"].as_str().unwrap().to_string());
    }

    // List should show 3
    let resp = client
        .get(format!("{}/api/agents", server.base_url))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 3);

    // Status should agree
    let resp = client
        .get(format!("{}/api/status", server.base_url))
        .send()
        .await
        .unwrap();
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["agent_count"], 3);

    // Kill one
    let resp = client
        .delete(format!("{}/api/agents/{}", server.base_url, ids[1]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List should show 2
    let resp = client
        .get(format!("{}/api/agents", server.base_url))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 2);

    // Kill the rest
    for id in [&ids[0], &ids[2]] {
        client
            .delete(format!("{}/api/agents/{}", server.base_url, id))
            .send()
            .await
            .unwrap();
    }

    // List should be empty
    let resp = client
        .get(format!("{}/api/agents", server.base_url))
        .send()
        .await
        .unwrap();
    let agents: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(agents.len(), 0);
}

#[tokio::test]
async fn test_auth_health_is_public() {
    let server = start_test_server_with_auth("secret-key-123").await;
    let client = reqwest::Client::new();

    // /api/health should be accessible without auth
    let resp = client
        .get(format!("{}/api/health", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_auth_rejects_no_token() {
    let server = start_test_server_with_auth("secret-key-123").await;
    let client = reqwest::Client::new();

    // Protected endpoint without auth header → 401
    // Note: /api/status is public (dashboard needs it), so use a protected endpoint
    let resp = client
        .get(format!("{}/api/commands", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Missing"));
}

#[tokio::test]
async fn test_auth_rejects_wrong_token() {
    let server = start_test_server_with_auth("secret-key-123").await;
    let client = reqwest::Client::new();

    // Wrong bearer token → 401
    // Note: /api/status is public (dashboard needs it), so use a protected endpoint
    let resp = client
        .get(format!("{}/api/commands", server.base_url))
        .header("authorization", "Bearer wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Invalid"));
}

#[tokio::test]
async fn test_auth_accepts_correct_token() {
    let server = start_test_server_with_auth("secret-key-123").await;
    let client = server.authed_client("secret-key-123");

    // Correct bearer token → 200
    let resp = client
        .get(format!("{}/api/commands", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["commands"].as_array().is_some());
}

#[tokio::test]
async fn test_auth_disabled_when_no_key() {
    // Empty API key = auth disabled
    let server = start_test_server().await;
    let client = reqwest::Client::new();

    // Protected endpoint accessible without auth when no key is configured
    let resp = client
        .get(format!("{}/api/commands", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["commands"].as_array().is_some());
}
