//! Cost-aware rate limiting for the sales cockpit API.
//!
//! Lightweight health/status calls stay cheap while sales execution and approval
//! actions consume more of the shared per-IP token budget.

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use governor::{clock::DefaultClock, state::keyed::DashMapStateStore, Quota, RateLimiter};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

pub fn operation_cost(method: &str, path: &str) -> NonZeroU32 {
    match (method, path) {
        (_, "/api/health") => NonZeroU32::new(1).unwrap(),
        (_, "/api/health/detail") => NonZeroU32::new(2).unwrap(),
        ("GET", "/api/status") => NonZeroU32::new(1).unwrap(),
        ("GET", "/api/version") => NonZeroU32::new(1).unwrap(),
        ("GET", "/api/metrics") => NonZeroU32::new(2).unwrap(),
        ("GET", "/api/auth/codex/status") => NonZeroU32::new(1).unwrap(),
        ("POST", "/api/auth/codex/start") => NonZeroU32::new(5).unwrap(),
        ("POST", "/api/auth/codex/paste-code") => NonZeroU32::new(8).unwrap(),
        ("POST", "/api/auth/codex/import-cli") => NonZeroU32::new(8).unwrap(),
        ("POST", "/api/auth/codex/logout") => NonZeroU32::new(5).unwrap(),
        ("GET", "/api/sales/profile") => NonZeroU32::new(2).unwrap(),
        ("PUT", "/api/sales/profile") => NonZeroU32::new(8).unwrap(),
        ("POST", "/api/sales/profile/autofill") => NonZeroU32::new(25).unwrap(),
        ("GET", "/api/sales/onboarding/status") => NonZeroU32::new(2).unwrap(),
        ("POST", "/api/sales/onboarding/brief") => NonZeroU32::new(15).unwrap(),
        ("POST", "/api/sales/run") => NonZeroU32::new(60).unwrap(),
        ("GET", "/api/sales/jobs/active") => NonZeroU32::new(3).unwrap(),
        ("GET", p) if p.starts_with("/api/sales/jobs/") && p.ends_with("/progress") => {
            NonZeroU32::new(3).unwrap()
        }
        ("POST", p) if p.starts_with("/api/sales/jobs/") && p.ends_with("/retry") => {
            NonZeroU32::new(40).unwrap()
        }
        ("GET", "/api/sales/source-health") => NonZeroU32::new(4).unwrap(),
        ("GET", "/api/sales/policy-proposals") => NonZeroU32::new(4).unwrap(),
        ("POST", p) if p.starts_with("/api/sales/policy-proposals/") && p.ends_with("/approve") => {
            NonZeroU32::new(12).unwrap()
        }
        ("POST", p) if p.starts_with("/api/sales/policy-proposals/") && p.ends_with("/reject") => {
            NonZeroU32::new(10).unwrap()
        }
        ("GET", "/api/sales/runs") => NonZeroU32::new(4).unwrap(),
        ("GET", "/api/sales/leads") => NonZeroU32::new(5).unwrap(),
        ("GET", "/api/sales/prospects") => NonZeroU32::new(5).unwrap(),
        ("GET", p) if p.starts_with("/api/sales/accounts/") && p.ends_with("/dossier") => {
            NonZeroU32::new(6).unwrap()
        }
        ("GET", "/api/sales/unsubscribe") => NonZeroU32::new(1).unwrap(),
        ("POST", "/api/sales/outcomes/webhook") => NonZeroU32::new(10).unwrap(),
        ("POST", "/api/sales/sequences/advance") => NonZeroU32::new(10).unwrap(),
        ("GET", "/api/sales/experiments") => NonZeroU32::new(4).unwrap(),
        ("POST", "/api/sales/experiments") => NonZeroU32::new(12).unwrap(),
        ("GET", p) if p.starts_with("/api/sales/experiments/") && p.ends_with("/results") => {
            NonZeroU32::new(5).unwrap()
        }
        ("GET", "/api/sales/context-factors") => NonZeroU32::new(4).unwrap(),
        ("POST", "/api/sales/calibration/run") => NonZeroU32::new(12).unwrap(),
        ("GET", "/api/sales/approvals") => NonZeroU32::new(5).unwrap(),
        ("POST", "/api/sales/approvals/bulk-approve") => NonZeroU32::new(30).unwrap(),
        ("PATCH", p) if p.starts_with("/api/sales/approvals/") && p.ends_with("/edit") => {
            NonZeroU32::new(15).unwrap()
        }
        ("POST", p) if p.starts_with("/api/sales/approvals/") && p.ends_with("/approve") => {
            NonZeroU32::new(25).unwrap()
        }
        ("POST", p) if p.starts_with("/api/sales/approvals/") && p.ends_with("/reject") => {
            NonZeroU32::new(12).unwrap()
        }
        ("GET", "/api/sales/deliveries") => NonZeroU32::new(5).unwrap(),
        _ => NonZeroU32::new(5).unwrap(),
    }
}

pub type KeyedRateLimiter = RateLimiter<IpAddr, DashMapStateStore<IpAddr>, DefaultClock>;

/// 500 tokens per minute per IP.
pub fn create_rate_limiter() -> Arc<KeyedRateLimiter> {
    Arc::new(RateLimiter::keyed(Quota::per_minute(
        NonZeroU32::new(500).unwrap(),
    )))
}

/// GCRA rate limiting middleware.
///
/// Extracts the client IP from `ConnectInfo`, computes the cost for the
/// requested operation, and checks the GCRA limiter. Returns 429 if the
/// client has exhausted its token budget.
pub async fn gcra_rate_limit(
    axum::extract::State(limiter): axum::extract::State<Arc<KeyedRateLimiter>>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let ip = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::from([127, 0, 0, 1]));

    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let cost = operation_cost(&method, &path);

    if limiter.check_key_n(&ip, cost).is_err() {
        tracing::warn!(ip = %ip, cost = cost.get(), path = %path, "GCRA rate limit exceeded");
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("content-type", "application/json")
            .header("retry-after", "60")
            .body(Body::from(
                serde_json::json!({"error": "Rate limit exceeded"}).to_string(),
            ))
            .unwrap_or_default();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_costs() {
        assert_eq!(operation_cost("GET", "/api/health").get(), 1);
        assert_eq!(operation_cost("GET", "/api/status").get(), 1);
        assert_eq!(operation_cost("GET", "/api/auth/codex/status").get(), 1);
        assert_eq!(operation_cost("PUT", "/api/sales/profile").get(), 8);
        assert_eq!(
            operation_cost("POST", "/api/sales/profile/autofill").get(),
            25
        );
        assert_eq!(operation_cost("POST", "/api/sales/run").get(), 60);
        assert_eq!(
            operation_cost("GET", "/api/sales/jobs/123/progress").get(),
            3
        );
        assert_eq!(
            operation_cost("POST", "/api/sales/jobs/123/retry").get(),
            40
        );
        assert_eq!(
            operation_cost("GET", "/api/sales/accounts/example.com/dossier").get(),
            6
        );
        assert_eq!(
            operation_cost("GET", "/api/sales/policy-proposals").get(),
            4
        );
        assert_eq!(operation_cost("GET", "/api/sales/unsubscribe").get(), 1);
        assert_eq!(
            operation_cost("POST", "/api/sales/outcomes/webhook").get(),
            10
        );
        assert_eq!(
            operation_cost("POST", "/api/sales/sequences/advance").get(),
            10
        );
        assert_eq!(operation_cost("POST", "/api/sales/experiments").get(), 12);
        assert_eq!(operation_cost("GET", "/api/sales/context-factors").get(), 4);
        assert_eq!(operation_cost("POST", "/api/sales/calibration/run").get(), 12);
        assert_eq!(
            operation_cost("POST", "/api/sales/approvals/bulk-approve").get(),
            30
        );
        assert_eq!(
            operation_cost("PATCH", "/api/sales/approvals/a1/edit").get(),
            15
        );
    }
}
