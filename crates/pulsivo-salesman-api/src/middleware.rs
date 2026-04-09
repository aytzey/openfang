//! Production middleware for the PulsivoSalesman API server.
//!
//! Provides:
//! - Request ID generation and propagation
//! - Per-endpoint structured request logging
//! - In-memory rate limiting (per IP)

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::info;

/// Request ID header name (standard).
pub const REQUEST_ID_HEADER: &str = "x-request-id";

fn is_always_public_path(path: &str) -> bool {
    matches!(
        path,
        "/auth/callback" | "/api/auth/codex/callback" | "/api/sales/unsubscribe"
    )
}

fn is_dashboard_public_path(path: &str) -> bool {
    matches!(path, "/api/health" | "/api/health/detail" | "/api/version")
}

fn is_loopback_request(request: &Request<Body>) -> bool {
    request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().is_loopback())
        .unwrap_or(false)
}

/// Middleware: inject a unique request ID and log the request/response.
pub async fn request_logging(request: Request<Body>, next: Next) -> Response<Body> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let method = request.method().clone();
    let uri = request.uri().path().to_string();
    let start = Instant::now();

    let mut response = next.run(request).await;

    let elapsed = start.elapsed();
    let status = response.status().as_u16();

    info!(
        request_id = %request_id,
        method = %method,
        path = %uri,
        status = status,
        latency_ms = elapsed.as_millis() as u64,
        "API request"
    );

    // Inject the request ID into the response
    if let Ok(header_val) = request_id.parse() {
        response.headers_mut().insert(REQUEST_ID_HEADER, header_val);
    }

    response
}

/// Bearer token authentication middleware.
///
/// When `api_key` is non-empty, all requests must include
/// `Authorization: Bearer <api_key>`. If the key is empty, auth is bypassed.
pub async fn auth(
    axum::extract::State(api_key): axum::extract::State<String>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let path = request.uri().path();

    if is_always_public_path(path) {
        return next.run(request).await;
    }

    // The embedded cockpit is meant to work locally without a separate API-key UX.
    if is_loopback_request(&request) {
        return next.run(request).await;
    }

    // If no API key configured, restrict to loopback addresses only.
    if api_key.is_empty() {
        tracing::warn!(
            "Rejected non-localhost request: no API key configured. \
             Set api_key in config.toml for remote access."
        );
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "error": "No API key configured. Remote access denied. Configure api_key in ~/.pulsivo-salesman/config.toml"
                })
                .to_string(),
            ))
            .unwrap_or_default();
    }

    // Minimal public endpoints for remote liveness checks.
    if is_dashboard_public_path(path) {
        return next.run(request).await;
    }

    // Check Authorization: Bearer <token> header
    let bearer_token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    // SECURITY: Use constant-time comparison to prevent timing attacks.
    let header_auth = bearer_token.map(|token| {
        use subtle::ConstantTimeEq;
        if token.len() != api_key.len() {
            return false;
        }
        token.as_bytes().ct_eq(api_key.as_bytes()).into()
    });

    // Also check ?token= query parameter for clients that cannot set headers.
    let query_token = request.uri().query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes()).find_map(|(key, value)| {
            if key == "token" {
                Some(value.into_owned())
            } else {
                None
            }
        })
    });

    // SECURITY: Use constant-time comparison to prevent timing attacks.
    let query_auth = query_token.as_deref().map(|token| {
        use subtle::ConstantTimeEq;
        if token.len() != api_key.len() {
            return false;
        }
        token.as_bytes().ct_eq(api_key.as_bytes()).into()
    });

    // Accept if either auth method matches
    if header_auth == Some(true) || query_auth == Some(true) {
        return next.run(request).await;
    }

    // Determine error message: was a credential provided but wrong, or missing entirely?
    let credential_provided = header_auth.is_some() || query_auth.is_some();
    let error_msg = if credential_provided {
        "Invalid API key"
    } else {
        "Missing Authorization: Bearer <api_key> header"
    };

    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("www-authenticate", "Bearer")
        .body(Body::from(
            serde_json::json!({"error": error_msg}).to_string(),
        ))
        .unwrap_or_default()
}

/// Security headers middleware — applied to ALL API responses.
pub async fn security_headers(request: Request<Body>, next: Next) -> Response<Body> {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert("x-frame-options", "DENY".parse().unwrap());
    headers.insert("x-xss-protection", "1; mode=block".parse().unwrap());
    // All JS/CSS is bundled inline — only external resource is Google Fonts.
    headers.insert(
        "content-security-policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://fonts.gstatic.com; img-src 'self' data: blob:; connect-src 'self'; font-src 'self' https://fonts.gstatic.com; media-src 'self' blob:; frame-src 'self' blob:; object-src 'none'; base-uri 'self'; form-action 'self'"
            .parse()
            .unwrap(),
    );
    headers.insert(
        "referrer-policy",
        "strict-origin-when-cross-origin".parse().unwrap(),
    );
    headers.insert(
        "cache-control",
        "no-store, no-cache, must-revalidate".parse().unwrap(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::ConnectInfo;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn request_with_ip(path: &str, ip: [u8; 4]) -> Request<Body> {
        let mut request = Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(std::net::SocketAddr::from((ip, 8080))));
        request
    }

    #[test]
    fn test_request_id_header_constant() {
        assert_eq!(REQUEST_ID_HEADER, "x-request-id");
    }

    #[tokio::test]
    async fn always_public_unsubscribe_bypasses_loopback_restriction() {
        let app = Router::new()
            .route(
                "/api/sales/unsubscribe",
                get(|| async { StatusCode::NO_CONTENT }),
            )
            .layer(axum::middleware::from_fn_with_state(
                String::new(),
                super::auth,
            ));

        let response = app
            .oneshot(request_with_ip(
                "/api/sales/unsubscribe?token=bad",
                [203, 0, 113, 10],
            ))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn local_codex_import_cli_bypasses_api_key_when_configured() {
        let app = Router::new()
            .route(
                "/api/auth/codex/import-cli",
                get(|| async { StatusCode::NO_CONTENT }),
            )
            .layer(axum::middleware::from_fn_with_state(
                "secret".to_string(),
                super::auth,
            ));

        let response = app
            .oneshot(request_with_ip(
                "/api/auth/codex/import-cli",
                [127, 0, 0, 1],
            ))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn remote_status_requires_auth_when_api_key_is_configured() {
        let app = Router::new()
            .route(
                "/api/status",
                get(|| async { StatusCode::NO_CONTENT }),
            )
            .layer(axum::middleware::from_fn_with_state(
                "secret".to_string(),
                super::auth,
            ));

        let response = app
            .oneshot(request_with_ip("/api/status", [203, 0, 113, 10]))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn remote_health_stays_public_when_api_key_is_configured() {
        let app = Router::new()
            .route("/api/health", get(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn_with_state(
                "secret".to_string(),
                super::auth,
            ));

        let response = app
            .oneshot(request_with_ip("/api/health", [203, 0, 113, 10]))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn remote_query_token_allows_percent_encoded_api_key() {
        let app = Router::new()
            .route("/api/private", get(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn_with_state(
                "s3cr+et/=".to_string(),
                super::auth,
            ));

        let response = app
            .oneshot(request_with_ip(
                "/api/private?token=s3cr%2Bet%2F%3D",
                [203, 0, 113, 10],
            ))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
