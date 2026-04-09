# Pulsivo Salesman — Repo Notes

## Project Overview
Pulsivo Salesman is a Rust sales daemon plus browser cockpit.

- Config: `~/.pulsivo-salesman/config.toml`
- Default API: `http://127.0.0.1:4200`
- Binary: `cargo run -p pulsivo-salesman -- start`
- Active HTTP surface: health/status/version plus `/api/sales/*`

## Build & Verify Workflow
After meaningful changes, prefer these checks:

```bash
cargo build --bin pulsivo-salesman
cargo check --workspace
cargo test --workspace
cargo xtask test-smoke
```

Use narrower commands when the changed area is obviously isolated, but do not leave route wiring or launcher changes unverified.

## Live Smoke Notes

The daemon entrypoint is now the first-party `pulsivo-salesman` binary:

```bash
cargo run -p pulsivo-salesman -- init --quick
GROQ_API_KEY=<key> cargo run -p pulsivo-salesman -- start
```

Basic endpoint checks:

```bash
curl -s http://127.0.0.1:4200/api/health
curl -s http://127.0.0.1:4200/api/status
curl -s http://127.0.0.1:4200/api/sales/profile?segment=b2c
curl -s http://127.0.0.1:4200/api/sales/runs?segment=b2c&limit=5
```

Write-path checks:

```bash
curl -s -X POST 'http://127.0.0.1:4200/api/sales/onboarding/brief?segment=b2c' \
  -H 'Content-Type: application/json' \
  -d '{"brief":"Pulsivo helps local service businesses turn buyer signals into outbound sales actions.","persist":true}'
```

## Architecture Notes

- `crates/pulsivo-salesman/src/main.rs` is intentionally thin. Keep bootstrap logic there, not product logic.
- `crates/pulsivo-salesman-api/src/server.rs` is the route assembly point.
- `crates/pulsivo-salesman-api/src/sales/http.rs` is the live sales HTTP contract.
- `crates/pulsivo-salesman-kernel/src/kernel.rs` owns config, memory, registry, and model catalog bootstrap.
- New routes are only real when they are both implemented and registered in `server.rs`.
- If config shape changes, update `KernelConfig`, its `Default` impl, and any validation/hot-reload logic together.

## Common Gotchas

- The repo is sales-only now. Do not reintroduce legacy chat, workflow, or general agent-loop assumptions by accident.
- The official SDKs are expected to mirror `/api/sales/*`; if you add or remove sales routes, update both SDKs.
- The WhatsApp gateway is an inbox/outbound bridge now, not an agent chat adapter.
