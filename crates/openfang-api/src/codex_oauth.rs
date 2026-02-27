//! ChatGPT/Codex OAuth helpers and API endpoints.
//!
//! Supports PKCE login, callback handling, manual code paste fallback,
//! importing existing Codex CLI auth, status checks, and logout.

use crate::routes::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use tracing::warn;

const DEFAULT_AUTH_URL: &str = "https://auth.openai.com/oauth/authorize";
const DEFAULT_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const FALLBACK_TOKEN_URL: &str = "https://auth0.openai.com/oauth/token";
const DEFAULT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEFAULT_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
const DEFAULT_SCOPES: &str = "openid profile email offline_access";
const MAX_PENDING_AGE_SECS: i64 = 15 * 60;

#[derive(Debug, Clone)]
struct PendingPkce {
    verifier: String,
    redirect_uri: String,
    client_id: String,
    created_at: DateTime<Utc>,
}

static PENDING_PKCE: LazyLock<DashMap<String, PendingPkce>> = LazyLock::new(DashMap::new);
struct LoopbackCallbackServer {
    bind_addr: SocketAddr,
    callback_path: String,
    handle: tokio::task::JoinHandle<()>,
}

static LOOPBACK_CALLBACK_TASK: LazyLock<std::sync::Mutex<Option<LoopbackCallbackServer>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));

#[derive(Debug, Clone)]
struct LoopbackCallbackTarget {
    bind_addr: SocketAddr,
    callback_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCodexAuth {
    #[serde(default)]
    pub openai_api_key: Option<String>,
    #[serde(default)]
    pub chatgpt_account_id: Option<String>,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub token_type: String,
    pub scope: String,
    #[serde(default)]
    pub client_id: Option<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub source: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    expires_in: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StartCodexOAuthRequest {
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub redirect_uri: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CodexCallbackQuery {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PasteCodeRequest {
    pub code: String,
    #[serde(default)]
    pub state: Option<String>,
}

fn auth_file(home_dir: &Path) -> PathBuf {
    home_dir.join("auth").join("codex_oauth.json")
}

fn ensure_auth_dir(home_dir: &Path) -> Result<(), String> {
    let dir = home_dir.join("auth");
    std::fs::create_dir_all(&dir).map_err(|e| format!("Failed to create auth dir: {e}"))
}

fn load_stored_auth(home_dir: &Path) -> Result<Option<StoredCodexAuth>, String> {
    let path = auth_file(home_dir);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
    let auth = serde_json::from_str::<StoredCodexAuth>(&raw)
        .map_err(|e| format!("Invalid auth file {}: {e}", path.display()))?;
    Ok(Some(auth))
}

fn save_stored_auth(home_dir: &Path, auth: &StoredCodexAuth) -> Result<(), String> {
    ensure_auth_dir(home_dir)?;
    let path = auth_file(home_dir);
    let json = serde_json::to_string_pretty(auth)
        .map_err(|e| format!("Failed to serialize auth record: {e}"))?;
    std::fs::write(&path, json).map_err(|e| format!("Failed to write {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn apply_codex_auth_to_runtime(state: &AppState, auth: &StoredCodexAuth) {
    std::env::set_var("OPENAI_CODEX_ACCESS_TOKEN", auth.access_token.trim());
    if let Some(account_id) = auth_account_id(auth) {
        std::env::set_var("OPENAI_CODEX_ACCOUNT_ID", account_id);
    } else {
        std::env::remove_var("OPENAI_CODEX_ACCOUNT_ID");
    }
    state
        .kernel
        .model_catalog
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .detect_auth();
}

fn clear_codex_auth_from_runtime(state: &AppState) {
    std::env::remove_var("OPENAI_CODEX_ACCESS_TOKEN");
    std::env::remove_var("OPENAI_CODEX_ACCOUNT_ID");
    state
        .kernel
        .model_catalog
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .detect_auth();
}

fn parse_jwt_payload(jwt: &str) -> Option<serde_json::Value> {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[1])
            .ok()
            .or_else(|| {
                base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE, parts[1]).ok()
            })?;
    serde_json::from_slice::<serde_json::Value>(&payload).ok()
}

fn jwt_client_id_from_id_token(id_token: &str) -> Option<String> {
    let payload = parse_jwt_payload(id_token)?;
    let aud = payload.get("aud")?;
    if let Some(s) = aud.as_str() {
        let out = s.trim();
        if !out.is_empty() {
            return Some(out.to_string());
        }
    }
    if let Some(arr) = aud.as_array() {
        for v in arr {
            if let Some(s) = v.as_str() {
                let out = s.trim();
                if !out.is_empty() {
                    return Some(out.to_string());
                }
            }
        }
    }
    None
}

fn jwt_chatgpt_account_id(token: &str) -> Option<String> {
    let payload = parse_jwt_payload(token)?;
    payload
        .get("https://api.openai.com/auth.chatgpt_account_id")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            payload
                .get("chatgpt_account_id")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            payload
                .get("https://api.openai.com/auth")
                .and_then(|v| v.as_object())
                .and_then(|obj| obj.get("chatgpt_account_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
}

fn auth_account_id(auth: &StoredCodexAuth) -> Option<String> {
    auth.chatgpt_account_id
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| jwt_chatgpt_account_id(&auth.access_token))
        .or_else(|| {
            auth.id_token
                .as_ref()
                .and_then(|id| jwt_chatgpt_account_id(id))
        })
}

fn auth_client_id(auth: &StoredCodexAuth, fallback: &str) -> String {
    auth.client_id
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            auth.id_token
                .as_ref()
                .and_then(|id| jwt_client_id_from_id_token(id))
        })
        .unwrap_or_else(|| fallback.to_string())
}

fn normalize_scope_tokens(scope: &str) -> Vec<String> {
    scope
        .split(|c: char| c.is_whitespace() || c == ',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn jwt_scope_from_token(token: &str) -> Option<String> {
    let payload = parse_jwt_payload(token)?;
    if let Some(scope) = payload.get("scope").and_then(|v| v.as_str()) {
        let scope = scope.trim();
        if !scope.is_empty() {
            return Some(scope.to_string());
        }
    }
    if let Some(arr) = payload.get("scp").and_then(|v| v.as_array()) {
        let parts: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect();
        if !parts.is_empty() {
            return Some(parts.join(" "));
        }
    }
    None
}

fn hydrate_scope_from_tokens(auth: &mut StoredCodexAuth) {
    if !auth.scope.trim().is_empty() {
        return;
    }
    if let Some(scope) = jwt_scope_from_token(&auth.access_token).or_else(|| {
        auth.id_token
            .as_ref()
            .and_then(|id| jwt_scope_from_token(id))
    }) {
        auth.scope = scope;
    }
}

fn missing_org_context_error() -> String {
    "OAuth token is missing organization context (chatgpt_account_id). Reconnect from Sales > Connect OAuth or import ~/.codex/auth.json.".to_string()
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn oauth_callback_html(title: &str, message: &str, success: bool) -> String {
    let title = escape_html(title);
    let message = escape_html(message);
    let action = if success {
        "You can close this tab."
    } else {
        "Return to Sales and retry Connect OAuth."
    };
    let script = if success {
        r#"<script>
(() => {
  try {
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage({ type: "openfang:codex_oauth", status: "connected" }, window.location.origin);
      setTimeout(() => window.close(), 300);
      return;
    }
  } catch (_) {}
  setTimeout(() => { window.location.href = "/#sales"; }, 1200);
})();
</script>"#
    } else {
        ""
    };
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{title}</title><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"></head><body style=\"margin:0;background:#f8fafc;color:#0f172a;font:16px/1.45 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif\"><div style=\"max-width:640px;margin:56px auto;padding:0 20px\"><div style=\"background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:20px 22px;box-shadow:0 8px 24px rgba(15,23,42,.06)\"><h1 style=\"font-size:22px;margin:0 0 10px\">{title}</h1><p style=\"margin:0 0 8px\">{message}</p><p style=\"margin:0;color:#475569;font-size:14px\">{action}</p></div></div>{script}</body></html>"
    )
}

fn cleanup_stale_pkce() {
    let now = Utc::now();
    PENDING_PKCE.retain(|_, v| (now - v.created_at).num_seconds() <= MAX_PENDING_AGE_SECS);
}

fn base64_url_encode(data: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, data)
}

fn generate_pkce_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64_url_encode(&bytes)
}

fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let digest = hasher.finalize();
    base64_url_encode(&digest)
}

fn random_state() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64_url_encode(&bytes)
}

fn infer_client_id_from_codex_auth() -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".codex").join("auth.json");
    let raw = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let id_token = extract_string_by_pointers(
        &v,
        &[
            "/id_token",
            "/idToken",
            "/token/id_token",
            "/tokens/id_token",
        ],
    )?;
    jwt_client_id_from_id_token(&id_token)
}

fn oauth_client_id(req: &StartCodexOAuthRequest) -> String {
    req.client_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("OPENAI_OAUTH_CLIENT_ID").ok())
        .or_else(infer_client_id_from_codex_auth)
        .unwrap_or_else(|| DEFAULT_CLIENT_ID.to_string())
}

fn oauth_redirect_uri(_state: &AppState, req: &StartCodexOAuthRequest) -> String {
    req.redirect_uri
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("OPENAI_OAUTH_REDIRECT_URI").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_REDIRECT_URI.to_string())
}

fn oauth_auth_url() -> String {
    let raw =
        std::env::var("OPENAI_OAUTH_AUTH_URL").unwrap_or_else(|_| DEFAULT_AUTH_URL.to_string());
    if let Ok(mut parsed) = url::Url::parse(raw.trim()) {
        if parsed.host_str() == Some("auth.openai.com") && parsed.path() == "/authorize" {
            parsed.set_path("/oauth/authorize");
            return parsed.to_string();
        }
    }
    raw
}

fn oauth_token_urls() -> Vec<String> {
    if let Ok(url) = std::env::var("OPENAI_OAUTH_TOKEN_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            if let Ok(mut parsed) = url::Url::parse(trimmed) {
                if parsed.host_str() == Some("auth.openai.com") && parsed.path() == "/token" {
                    parsed.set_path("/oauth/token");
                    return vec![parsed.to_string()];
                }
            }
            return vec![trimmed.to_string()];
        }
    }
    vec![
        DEFAULT_TOKEN_URL.to_string(),
        FALLBACK_TOKEN_URL.to_string(),
    ]
}

fn oauth_scopes() -> String {
    let raw = std::env::var("OPENAI_OAUTH_SCOPES").unwrap_or_else(|_| DEFAULT_SCOPES.to_string());
    normalize_scope_tokens(&raw).join(" ")
}

fn oauth_originator() -> String {
    std::env::var("OPENAI_OAUTH_ORIGINATOR").unwrap_or_else(|_| "pi".to_string())
}

fn parse_loopback_callback_target(redirect_uri: &str) -> Option<LoopbackCallbackTarget> {
    let parsed = url::Url::parse(redirect_uri).ok()?;
    if parsed.scheme() != "http" {
        return None;
    }

    let host = parsed.host_str()?.to_ascii_lowercase();
    if host != "localhost" && host != "127.0.0.1" {
        return None;
    }

    let port = parsed.port_or_known_default()?;
    let path = if parsed.path().trim().is_empty() {
        "/auth/callback".to_string()
    } else {
        parsed.path().to_string()
    };
    if !path.starts_with('/') {
        return None;
    }

    Some(LoopbackCallbackTarget {
        bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        callback_path: path,
    })
}

async fn ensure_loopback_callback_listener(
    state: Arc<AppState>,
    redirect_uri: &str,
) -> Result<(), String> {
    let Some(target) = parse_loopback_callback_target(redirect_uri) else {
        return Ok(());
    };

    {
        let task_slot = LOOPBACK_CALLBACK_TASK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(current) = task_slot.as_ref() {
            if current.bind_addr == target.bind_addr
                && current.callback_path == target.callback_path
                && !current.handle.is_finished()
            {
                return Ok(());
            }
        }
    }

    {
        let mut task_slot = LOOPBACK_CALLBACK_TASK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(current) = task_slot.take() {
            current.handle.abort();
        }
    }

    let listener = tokio::net::TcpListener::bind(target.bind_addr)
        .await
        .map_err(|e| {
            format!(
                "Cannot bind OAuth callback listener on {}: {}. Close other login tools using this port and retry.",
                target.bind_addr, e
            )
        })?;

    let mut task_slot = LOOPBACK_CALLBACK_TASK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let callback_path = target.callback_path.clone();
    let bind_addr = target.bind_addr;
    let state_for_server = state.clone();
    let task = tokio::spawn(async move {
        let mut app =
            axum::Router::new().route(&callback_path, axum::routing::get(codex_oauth_callback));
        if callback_path != "/auth/callback" {
            app = app.route("/auth/callback", axum::routing::get(codex_oauth_callback));
        }
        let app = app.with_state(state_for_server);
        if let Err(e) = axum::serve(listener, app).await {
            warn!("Codex OAuth loopback callback server failed on {bind_addr}: {e}");
        }
    });
    *task_slot = Some(LoopbackCallbackServer {
        bind_addr,
        callback_path: target.callback_path,
        handle: task,
    });
    Ok(())
}

fn url_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

async fn exchange_code(
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    client_id: &str,
    source: &str,
) -> Result<StoredCodexAuth, String> {
    let client = reqwest::Client::new();
    let mut errors: Vec<String> = Vec::new();
    let mut token: Option<TokenResponse> = None;

    for token_url in oauth_token_urls() {
        let resp = match client
            .post(&token_url)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("client_id", client_id),
                ("code_verifier", verifier),
            ])
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                errors.push(format!("{token_url}: request failed: {e}"));
                continue;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            errors.push(format!("{token_url}: {status} {body}"));
            continue;
        }

        match resp.json::<TokenResponse>().await {
            Ok(t) => {
                token = Some(t);
                break;
            }
            Err(e) => {
                errors.push(format!("{token_url}: parse failed: {e}"));
                continue;
            }
        }
    }

    let token = token.ok_or_else(|| {
        format!(
            "Token exchange failed on all token endpoints: {}",
            errors.join(" | ")
        )
    })?;

    let issued_at = Utc::now();
    let expires_at = token
        .expires_in
        .map(|secs| issued_at + ChronoDuration::seconds(secs));

    let id_token = token.id_token;
    let account_id = jwt_chatgpt_account_id(&token.access_token)
        .or_else(|| id_token.as_ref().and_then(|id| jwt_chatgpt_account_id(id)));
    let derived_client_id = id_token
        .as_ref()
        .and_then(|id| jwt_client_id_from_id_token(id))
        .or_else(|| Some(client_id.to_string()));

    Ok(StoredCodexAuth {
        openai_api_key: None,
        chatgpt_account_id: account_id,
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        id_token,
        token_type: if token.token_type.is_empty() {
            "Bearer".to_string()
        } else {
            token.token_type
        },
        scope: token.scope,
        client_id: derived_client_id,
        issued_at,
        expires_at,
        source: source.to_string(),
    })
}

async fn refresh_access_token(
    refresh_token: &str,
    client_id: &str,
) -> Result<TokenResponse, String> {
    let client = reqwest::Client::new();
    let mut errors: Vec<String> = Vec::new();

    for token_url in oauth_token_urls() {
        let resp = match client
            .post(&token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", client_id),
            ])
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                errors.push(format!("{token_url}: request failed: {e}"));
                continue;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            errors.push(format!("{token_url}: {status} {body}"));
            continue;
        }

        return resp
            .json::<TokenResponse>()
            .await
            .map_err(|e| format!("Refresh response parse failed: {e}"));
    }

    Err(format!(
        "Refresh failed on all token endpoints: {}",
        errors.join(" | ")
    ))
}

fn update_auth_from_token(auth: &mut StoredCodexAuth, token: TokenResponse, source: &str) {
    let now = Utc::now();
    auth.access_token = token.access_token;
    auth.chatgpt_account_id =
        jwt_chatgpt_account_id(&auth.access_token).or(auth.chatgpt_account_id.clone());
    auth.refresh_token = token.refresh_token.or(auth.refresh_token.clone());
    auth.id_token = token.id_token.or(auth.id_token.clone());
    auth.token_type = if token.token_type.is_empty() {
        auth.token_type.clone()
    } else {
        token.token_type
    };
    if !token.scope.is_empty() {
        auth.scope = token.scope;
    }
    hydrate_scope_from_tokens(auth);
    auth.issued_at = now;
    auth.expires_at = token
        .expires_in
        .map(|secs| now + ChronoDuration::seconds(secs));
    auth.source = source.to_string();
    if let Some(id_token) = auth.id_token.as_ref() {
        auth.client_id = jwt_client_id_from_id_token(id_token).or(auth.client_id.clone());
    }
}

async fn refresh_auth_if_possible(auth: &mut StoredCodexAuth, fallback_client_id: &str) -> bool {
    let Some(refresh) = auth.refresh_token.clone() else {
        return false;
    };
    let client_id = auth_client_id(auth, fallback_client_id);
    match refresh_access_token(&refresh, &client_id).await {
        Ok(token) => {
            update_auth_from_token(auth, token, "refresh_token");
            if auth.client_id.is_none() {
                auth.client_id = Some(client_id);
            }
            true
        }
        Err(_) => false,
    }
}

async fn ensure_access_token_for_auth(
    auth: &mut StoredCodexAuth,
    fallback_client_id: &str,
) -> Result<(), String> {
    if !auth.access_token.trim().is_empty() {
        if auth.chatgpt_account_id.is_none() {
            auth.chatgpt_account_id = auth_account_id(auth);
        }
        hydrate_scope_from_tokens(auth);
        if auth.chatgpt_account_id.is_none() {
            let _ = refresh_auth_if_possible(auth, fallback_client_id).await;
            if auth.chatgpt_account_id.is_none() {
                auth.chatgpt_account_id = auth_account_id(auth);
            }
            hydrate_scope_from_tokens(auth);
            if auth.chatgpt_account_id.is_none() {
                return Err(missing_org_context_error());
            }
        }
        return Ok(());
    }

    if refresh_auth_if_possible(auth, fallback_client_id).await
        && !auth.access_token.trim().is_empty()
    {
        if auth.chatgpt_account_id.is_none() {
            auth.chatgpt_account_id = auth_account_id(auth);
        }
        hydrate_scope_from_tokens(auth);
        if auth.chatgpt_account_id.is_none() {
            return Err(missing_org_context_error());
        }
        return Ok(());
    }

    Err("OAuth access token is missing. Reconnect from Sales > Connect OAuth.".to_string())
}

fn extract_string_by_pointers(v: &serde_json::Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|p| {
        v.pointer(p)
            .and_then(|x| x.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

fn extract_expiry(v: &serde_json::Value) -> Option<DateTime<Utc>> {
    let candidates = [
        "/expires_at",
        "/expiresAt",
        "/token/expires_at",
        "/tokens/expires_at",
    ];
    for p in candidates {
        if let Some(val) = v.pointer(p) {
            if let Some(ts) = val.as_i64() {
                let secs = if ts > 2_000_000_000_000 {
                    ts / 1000
                } else {
                    ts
                };
                if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, 0) {
                    return Some(dt);
                }
            }
            if let Some(s) = val.as_str() {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Some(dt.with_timezone(&Utc));
                }
            }
        }
    }

    let expires_in = v
        .pointer("/expires_in")
        .and_then(|x| x.as_i64())
        .or_else(|| v.pointer("/token/expires_in").and_then(|x| x.as_i64()));
    expires_in.map(|secs| Utc::now() + ChronoDuration::seconds(secs))
}

fn import_codex_cli_auth(_home_dir: &Path) -> Result<StoredCodexAuth, String> {
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("~"));
    let path = home.join(".codex").join("auth.json");
    if !path.exists() {
        return Err(format!("Codex auth file not found: {}", path.display()));
    }

    let raw = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("Invalid JSON in {}: {e}", path.display()))?;

    let access_token = extract_string_by_pointers(
        &v,
        &[
            "/access_token",
            "/accessToken",
            "/token",
            "/token/access_token",
            "/tokens/access_token",
            "/credentials/access_token",
        ],
    )
    .ok_or_else(|| "Could not extract access token from ~/.codex/auth.json".to_string())?;

    let refresh_token = extract_string_by_pointers(
        &v,
        &[
            "/refresh_token",
            "/refreshToken",
            "/token/refresh_token",
            "/tokens/refresh_token",
            "/credentials/refresh_token",
        ],
    );

    let id_token = extract_string_by_pointers(
        &v,
        &[
            "/id_token",
            "/idToken",
            "/token/id_token",
            "/tokens/id_token",
        ],
    );
    let account_id = extract_string_by_pointers(
        &v,
        &[
            "/account_id",
            "/token/account_id",
            "/tokens/account_id",
            "/credentials/account_id",
        ],
    )
    .or_else(|| jwt_chatgpt_account_id(&access_token))
    .or_else(|| id_token.as_ref().and_then(|id| jwt_chatgpt_account_id(id)));

    let auth = StoredCodexAuth {
        openai_api_key: extract_string_by_pointers(&v, &["/OPENAI_API_KEY", "/openai_api_key"]),
        chatgpt_account_id: account_id,
        access_token,
        refresh_token,
        id_token: id_token.clone(),
        token_type: "Bearer".to_string(),
        scope: extract_string_by_pointers(&v, &["/scope", "/token/scope", "/tokens/scope"])
            .unwrap_or_default(),
        client_id: id_token
            .as_ref()
            .and_then(|id| jwt_client_id_from_id_token(id)),
        issued_at: Utc::now(),
        expires_at: extract_expiry(&v),
        source: "codex_cli_import".to_string(),
    };

    Ok(auth)
}

pub async fn codex_oauth_start(
    State(state): State<Arc<AppState>>,
    body: Option<Json<StartCodexOAuthRequest>>,
) -> Response {
    cleanup_stale_pkce();
    let req = body.map(|b| b.0).unwrap_or_default();

    let client_id = oauth_client_id(&req);
    let redirect_uri = oauth_redirect_uri(&state, &req);
    if let Err(e) = ensure_loopback_callback_listener(state.clone(), &redirect_uri).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response();
    }

    let verifier = generate_pkce_verifier();
    let challenge = pkce_challenge(&verifier);
    let state_token = random_state();

    PENDING_PKCE.insert(
        state_token.clone(),
        PendingPkce {
            verifier,
            redirect_uri: redirect_uri.clone(),
            client_id: client_id.clone(),
            created_at: Utc::now(),
        },
    );

    let auth_url = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&id_token_add_organizations=true&codex_cli_simplified_flow=true&originator={}&state={}",
        oauth_auth_url(),
        url_encode(&client_id),
        url_encode(&redirect_uri),
        url_encode(&oauth_scopes()),
        url_encode(&challenge),
        url_encode(&oauth_originator()),
        url_encode(&state_token),
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "auth_url": auth_url,
            "state": state_token,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "instructions": "Open auth_url in your browser. If callback fails, use /api/auth/codex/paste-code with code+state."
        })),
    )
        .into_response()
}

pub async fn codex_oauth_callback(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CodexCallbackQuery>,
) -> impl IntoResponse {
    if let Some(err) = q.error {
        let msg = q
            .error_description
            .unwrap_or_else(|| "OAuth error".to_string());
        return (
            StatusCode::BAD_REQUEST,
            Html(oauth_callback_html(
                "Codex OAuth Error",
                &format!("{err}: {msg}"),
                false,
            )),
        );
    }

    let code = match q.code {
        Some(c) if !c.trim().is_empty() => c,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Html(oauth_callback_html(
                    "Missing code",
                    "No authorization code was provided by the OAuth provider.",
                    false,
                )),
            )
        }
    };

    let state_token = match q.state {
        Some(s) if !s.trim().is_empty() => s,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Html(oauth_callback_html(
                    "Missing state",
                    "No state parameter was provided by the OAuth provider.",
                    false,
                )),
            )
        }
    };

    let pending = match PENDING_PKCE.remove(&state_token) {
        Some((_, p)) => p,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Html(oauth_callback_html(
                    "Invalid state",
                    "State not found or expired. Start OAuth from Sales and try again.",
                    false,
                )),
            )
        }
    };

    match exchange_code(
        &code,
        &pending.verifier,
        &pending.redirect_uri,
        &pending.client_id,
        "pkce_callback",
    )
    .await
    {
        Ok(mut auth) => {
            if let Err(e) = ensure_access_token_for_auth(&mut auth, &pending.client_id).await {
                return (
                    StatusCode::BAD_REQUEST,
                    Html(oauth_callback_html("OAuth Token Error", &e, false)),
                );
            }
            auth.chatgpt_account_id = auth_account_id(&auth);
            if auth.client_id.is_none() {
                auth.client_id = Some(pending.client_id.clone());
            }
            if let Err(e) = save_stored_auth(&state.kernel.config.home_dir, &auth) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(oauth_callback_html("Auth Save Failed", &e, false)),
                );
            }
            apply_codex_auth_to_runtime(&state, &auth);
            (
                StatusCode::OK,
                Html(oauth_callback_html(
                    "Codex OAuth Connected",
                    "Login completed successfully.",
                    true,
                )),
            )
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Html(oauth_callback_html("Token Exchange Failed", &e, false)),
        ),
    }
}

pub async fn codex_oauth_paste_code(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PasteCodeRequest>,
) -> impl IntoResponse {
    cleanup_stale_pkce();

    let pending = if let Some(ref st) = body.state {
        PENDING_PKCE
            .remove(st)
            .map(|(_, p)| p)
            .ok_or_else(|| "Unknown or expired state".to_string())
    } else {
        let mut latest: Option<(String, PendingPkce)> = None;
        for e in PENDING_PKCE.iter() {
            let v = e.value().clone();
            match latest {
                Some((_, ref cur)) if cur.created_at >= v.created_at => {}
                _ => latest = Some((e.key().clone(), v)),
            }
        }
        if let Some((key, pending)) = latest {
            PENDING_PKCE.remove(&key);
            Ok(pending)
        } else {
            Err("No pending PKCE login found".to_string())
        }
    };

    let pending = match pending {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match exchange_code(
        &body.code,
        &pending.verifier,
        &pending.redirect_uri,
        &pending.client_id,
        "pkce_manual_code",
    )
    .await
    {
        Ok(mut auth) => {
            if let Err(e) = ensure_access_token_for_auth(&mut auth, &pending.client_id).await {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                );
            }
            auth.chatgpt_account_id = auth_account_id(&auth);
            if auth.client_id.is_none() {
                auth.client_id = Some(pending.client_id.clone());
            }
            if let Err(e) = save_stored_auth(&state.kernel.config.home_dir, &auth) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e})),
                );
            }
            apply_codex_auth_to_runtime(&state, &auth);
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "connected", "source": auth.source})),
            )
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn codex_oauth_import_cli(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match import_codex_cli_auth(&state.kernel.config.home_dir) {
        Ok(mut auth) => {
            let fallback_client_id = std::env::var("OPENAI_OAUTH_CLIENT_ID")
                .unwrap_or_else(|_| DEFAULT_CLIENT_ID.to_string());
            if let Err(e) = ensure_access_token_for_auth(&mut auth, &fallback_client_id).await {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                );
            }
            auth.chatgpt_account_id = auth_account_id(&auth);
            if auth.client_id.is_none() {
                auth.client_id = Some(fallback_client_id);
            }
            if let Err(e) = save_stored_auth(&state.kernel.config.home_dir, &auth) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e})),
                );
            }
            apply_codex_auth_to_runtime(&state, &auth);
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "connected", "source": auth.source})),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn codex_oauth_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let home = &state.kernel.config.home_dir;
    let fallback_client_id =
        std::env::var("OPENAI_OAUTH_CLIENT_ID").unwrap_or_else(|_| DEFAULT_CLIENT_ID.to_string());

    let mut auth = match load_stored_auth(home) {
        Ok(Some(auth)) => auth,
        Ok(None) => match import_codex_cli_auth(home) {
            Ok(mut auth) => {
                if let Err(e) = ensure_access_token_for_auth(&mut auth, &fallback_client_id).await {
                    clear_codex_auth_from_runtime(&state);
                    return (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "connected": false,
                            "provider": "openai-codex",
                            "model": "gpt-5.3-codex",
                            "reason": e,
                            "source": auth.source
                        })),
                    );
                }
                auth.chatgpt_account_id = auth_account_id(&auth);
                if auth.client_id.is_none() {
                    auth.client_id = Some(fallback_client_id.clone());
                }
                if let Err(_e) = save_stored_auth(home, &auth) {
                    clear_codex_auth_from_runtime(&state);
                }
                auth
            }
            Err(_) => {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "connected": false,
                        "provider": "openai-codex",
                        "model": "gpt-5.3-codex"
                    })),
                );
            }
        },
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let now = Utc::now();
    let should_refresh = auth
        .expires_at
        .map(|exp| exp <= now + ChronoDuration::seconds(60))
        .unwrap_or(false)
        && auth.refresh_token.is_some();

    if should_refresh {
        let _ = refresh_auth_if_possible(&mut auth, &fallback_client_id).await;
    }

    if let Err(e) = ensure_access_token_for_auth(&mut auth, &fallback_client_id).await {
        clear_codex_auth_from_runtime(&state);
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "connected": false,
                "provider": "openai-codex",
                "model": "gpt-5.3-codex",
                "reason": e,
                "source": auth.source,
                "issued_at": auth.issued_at.to_rfc3339(),
                "expires_at": auth.expires_at.map(|d| d.to_rfc3339()),
                "has_refresh_token": auth.refresh_token.is_some(),
            })),
        );
    }

    auth.chatgpt_account_id = auth_account_id(&auth);
    if auth.client_id.is_none() {
        auth.client_id = Some(fallback_client_id);
    }
    if let Err(_e) = save_stored_auth(home, &auth) {
        clear_codex_auth_from_runtime(&state);
    }

    apply_codex_auth_to_runtime(&state, &auth);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "connected": true,
            "provider": "openai-codex",
            "model": "gpt-5.3-codex",
            "source": auth.source,
            "issued_at": auth.issued_at.to_rfc3339(),
            "expires_at": auth.expires_at.map(|d| d.to_rfc3339()),
            "has_refresh_token": auth.refresh_token.is_some(),
        })),
    )
}

pub async fn codex_oauth_logout(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let path = auth_file(&state.kernel.config.home_dir);
    let _ = std::fs::remove_file(&path);
    clear_codex_auth_from_runtime(&state);

    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "logged_out"})),
    )
}
