//! Embedded WebChat UI served as static HTML.
//!
//! The production dashboard is assembled at compile time from HTML fragments,
//! CSS assets, vendor libraries, and a Bun-built application bundle. This
//! preserves single-binary deployment while keeping the source tree manageable.

use axum::http::header;
use axum::response::IntoResponse;

/// Compile-time ETag based on the crate version.
const ETAG: &str = concat!("\"pulsivo-salesman-", env!("CARGO_PKG_VERSION"), "\"");

/// Embedded logo PNG for single-binary deployment.
const LOGO_PNG: &[u8] = include_bytes!("../static/pulsivo-salesman-logo.png");

/// Embedded favicon ICO for browser tabs.
const FAVICON_ICO: &[u8] = include_bytes!("../static/favicon.ico");

/// GET /logo.png — Serve the PulsivoSalesman logo.
pub async fn logo_png() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400, immutable"),
        ],
        LOGO_PNG,
    )
}

/// GET /favicon.ico — Serve the PulsivoSalesman favicon.
pub async fn favicon_ico() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/x-icon"),
            (header::CACHE_CONTROL, "public, max-age=86400, immutable"),
        ],
        FAVICON_ICO,
    )
}

/// GET / — Serve the PulsivoSalesman Dashboard single-page application.
pub async fn webchat_page() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::ETAG, ETAG),
            (
                header::CACHE_CONTROL,
                "public, max-age=3600, must-revalidate",
            ),
        ],
        WEBCHAT_HTML,
    )
}

/// The embedded HTML/CSS/JS for the PulsivoSalesman Dashboard.
///
/// Vendor libraries are bundled locally — no CDN dependency. Alpine.js is
/// included LAST because it immediately processes x-data directives on load.
const WEBCHAT_HTML: &str = concat!(
    include_str!("../static/index_head.html"),
    "<style>\n",
    include_str!("../static/css/theme.css"),
    "\n",
    include_str!("../static/css/layout.css"),
    "\n",
    include_str!("../static/css/components.css"),
    "\n",
    include_str!("../static/vendor/github-dark.min.css"),
    "\n</style>\n",
    include_str!("../static/html/body_open.html"),
    include_str!("../static/html/sales_onboarding.html"),
    include_str!("../static/html/sales_command.html"),
    include_str!("../static/html/sales_market_b2c.html"),
    include_str!("../static/html/sales_ops.html"),
    include_str!("../static/html/sales_runs.html"),
    include_str!("../static/html/sales_profiles_shared.html"),
    include_str!("../static/html/sales_profiles_b2b.html"),
    include_str!("../static/html/sales_profiles_b2c.html"),
    include_str!("../static/html/sales_approvals.html"),
    include_str!("../static/html/sales_leads.html"),
    include_str!("../static/html/sales_run_profiles.html"),
    include_str!("../static/html/sales_deliveries.html"),
    include_str!("../static/html/body_close.html"),
    "<script>\n",
    include_str!("../static/vendor/marked.min.js"),
    "\n</script>\n",
    "<script>\n",
    include_str!("../static/vendor/highlight.min.js"),
    "\n</script>\n",
    "<script>\n",
    include_str!(concat!(env!("OUT_DIR"), "/webchat.bundle.js")),
    "\n</script>\n",
    "<script>\n",
    include_str!("../static/vendor/alpine.min.js"),
    "\n</script>\n",
    "</body></html>"
);
