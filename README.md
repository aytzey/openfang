# Pulsivo Salesman

Pulsivo Salesman is the trimmed, sales-only descendant of the original OpenFang codebase.

This repository is no longer a general-purpose agent operating system. The active product surface is a sales operator stack built around:

- B2B and B2C prospect discovery
- deterministic account and contact enrichment
- approval-gated outbound drafting
- an embedded browser cockpit served by the API daemon
- a shared memory substrate for structured, session, semantic, and graph-shaped state

This README was written against the current codebase shape and a fresh graph snapshot in `graphify-out/`. It is intentionally opinionated about what is alive, what is compatibility residue, and what should be treated as packaging backlog.

## Current Status

The repo is best understood as a library-first sales daemon workspace with an embedded UI.

- The core live path is `kernel -> api -> sales engine -> memory/runtime`.
- The main business logic lives in `crates/pulsivo-salesman-api/src/sales/`.
- The browser UI is built with Bun and embedded into the API crate at compile time.
- The repo still contains some packaging or compatibility artifacts that assume a broader product shape than the current sales-only runtime actually exposes.

The graph-derived highest-leverage abstractions in the current repo are:

1. `SalesEngine`
2. `MemorySubstrate`
3. `AgentRegistry`
4. `CodexDriver`
5. `ModelCatalog`

Those five nodes are the fastest way to orient yourself in the code.

## Graph-Derived Map

A fresh graph was rebuilt after removing `.clawdbot_ref`. Current snapshot:

- `99` source files detected in the active repo
- `2330` graph nodes
- `4186` graph edges
- `62` communities
- code-first structural extraction only, so token cost was `0`

The most important communities are:

- `Sales Engine Core`: persistence, orchestration, run lifecycle
- `Sales Enrichment`: deterministic enrichment and ranking logic
- `Prospect Dossiers`: prospect profile assembly and account summaries
- `Onboarding Flow`: brief parsing, profile normalization, first-run readiness
- `B2C Discovery`: social/local market discovery logic
- `Sales API Surface`: HTTP handlers, middleware, status, approvals, deliveries
- `Codex OAuth Flow`: browser PKCE flow and stored CLI auth import
- `Kernel Config` and `Config Reload`: runtime configuration and hot-reload decisions
- `Memory Substrate`, `Structured Memory`, `Session Memory`, `Semantic Memory`: persistent state backbone
- `LLM Driver Mesh`, `Codex Driver`, `OpenAI Driver`, `Gemini Driver`, `Anthropic Driver`, `Copilot Driver`: outbound model/runtime layer
- `JavaScript SDK` and `Python Client SDK`: sales-only client libraries for the live HTTP surface

Graph artifacts generated from this pass:

- `graphify-out/graph.html`
- `graphify-out/graph.json`
- `graphify-out/GRAPH_REPORT.md`

## What This Repo Actually Does

At a high level, Pulsivo Salesman turns a short operator brief into a sales workflow:

1. Normalize and persist a sales profile.
2. Gather public-source candidate accounts.
3. Enrich accounts and contacts with deterministic rules and web search.
4. Build prospect dossiers and score them.
5. Generate approval-ready outbound drafts.
6. Let an operator review, edit, approve, or reject drafts.
7. Send through supported outbound channels.
8. Persist outcomes into memory and sales state for reuse.

The product currently serves two adjacent operating modes:

- `B2B`: account-first prospecting and outreach
- `B2C`: social/local discovery and buyer-priority queues

The important thing is that both modes share the same underlying engine, storage, and UI shell.

## Architecture

### Runtime shape

```text
Browser UI
  -> Axum HTTP server
    -> sales HTTP handlers
      -> SalesEngine
        -> sales.db
        -> Kernel
          -> MemorySubstrate
            -> structured/session/semantic/knowledge stores
          -> ModelCatalog
          -> config snapshot + reload plan
        -> runtime web tools
          -> web search
          -> web fetch
        -> runtime LLM drivers
          -> Codex / OpenAI / Gemini / Anthropic / Copilot / fallback
```

### Embedded UI path

```text
ui/src/*.js
  -> Bun build:webchat
    -> webchat.bundle.js
      -> included by build.rs
        -> embedded into pulsivo-salesman-api
          -> served at /
```

### Persistence split

There are two separate state systems by design:

- `sales.db`
  Stores operational sales state: runs, profiles, leads, approvals, deliveries, accounts, contacts, signals, theses, queues.
- `pulsivo-salesman.db`
  Stores shared memory substrate state: structured KV, sessions, semantic search material, knowledge graph entities/relations, usage/consolidation data.

This split matters. The sales engine is operational and workflow-driven. The memory substrate is long-lived and cross-cutting.

## Workspace Layout

### `crates/pulsivo-salesman-api`

Purpose: HTTP server, embedded cockpit, OAuth surface, all sales business logic.

Key files:

- `src/server.rs`
  Router assembly, middleware stack, scheduler bootstrap, daemon lifecycle.
- `src/routes.rs`
  Health, status, version, metrics.
- `src/codex_oauth.rs`
  PKCE flow, callback handling, token storage, CLI auth import.
- `src/sales.rs`
  Aggregates all sales modules.
- `src/sales/engine.rs`
  The operational heart of the system.
- `src/sales/http.rs`
  Public sales handlers exposed by the router.
- `src/sales/enrichment.rs`
  Deterministic enrichment, domain health, query construction, scoring helpers.
- `src/sales/prospects.rs`
  Dossier building and prospect profile shaping.
- `src/sales/onboarding.rs`
  Brief ingestion, profile draft normalization, readiness checks.
- `src/sales/discovery.rs`
  B2C-oriented discovery logic.
- `src/sales/directories.rs`
  Directory scrapers and public-source collection.
- `src/sales/llm.rs`
  LLM-assisted sales tasks layered on top of deterministic flow.
- `src/webchat.rs`
  Compile-time HTML/CSS/JS assembly.
- `build.rs`
  Forces Bun install/build and embeds the bundle.

### `crates/pulsivo-salesman-kernel`

Purpose: minimal kernel surface for the sales daemon.

It is intentionally smaller than the original OpenFang kernel. The kernel now owns:

- config loading
- config hot-reload planning
- agent registry retained for status compatibility
- supervisor health and shutdown state
- model catalog
- access to the memory substrate

Important files:

- `src/kernel.rs`
- `src/config.rs`
- `src/config_reload.rs`
- `src/registry.rs`
- `src/supervisor.rs`

### `crates/pulsivo-salesman-runtime`

Purpose: runtime primitives used by the sales engine.

Live capabilities:

- model drivers
- model/provider registry
- web search
- web fetch
- content parsing
- response caching

This crate used to be much broader. It is now a supporting runtime rather than a full agent loop runtime.

Important files:

- `src/llm_driver.rs`
- `src/model_catalog.rs`
- `src/web_search.rs`
- `src/web_fetch.rs`
- `src/web_content.rs`
- `src/web_cache.rs`
- `src/drivers/*.rs`

### `crates/pulsivo-salesman-memory`

Purpose: long-lived storage substrate.

Backends exposed through one substrate:

- structured state
- session state
- semantic lookup/search
- knowledge entities/relations
- usage tracking
- consolidation

Important files:

- `src/substrate.rs`
- `src/structured.rs`
- `src/session.rs`
- `src/semantic.rs`
- `src/knowledge.rs`
- `src/usage.rs`
- `src/consolidation.rs`
- `src/migration.rs`

### `crates/pulsivo-salesman-types`

Purpose: all shared types and config models used across crates.

Includes:

- agent metadata
- config schema
- events
- messages
- model catalog types
- taint labels
- manifest signing
- memory-facing types

### `xtask`

Purpose: deterministic smoke and verification helpers.

Current commands:

- `cargo run -p xtask -- test-smoke`
- `cargo run -p xtask -- test-live-smoke`

### `sdk/javascript`

Purpose: JavaScript/TypeScript client package.
The current package now targets the live sales-only HTTP surface: profile management, onboarding, runs, prospects, approvals, deliveries, and operational health endpoints.

### `sdk/python`

Purpose: Python client package for the same sales-only HTTP surface.

### `packages/whatsapp-gateway`

Purpose: Baileys-based WhatsApp Web bridge.
The current package is aligned to the trimmed repo: it handles QR login, outbound send, and inbound inbox capture without assuming the removed legacy chat surface.

## HTTP Surface

The router is assembled in `crates/pulsivo-salesman-api/src/server.rs`.

### Public assets

- `GET /`
- `GET /logo.png`
- `GET /favicon.ico`

### Operational endpoints

- `GET /api/metrics`
- `GET /api/health`
- `GET /api/health/detail`
- `GET /api/status`
- `GET /api/version`

### Codex OAuth endpoints

- `POST /api/auth/codex/start`
- `GET /api/auth/codex/callback`
- `GET /auth/callback`
- `POST /api/auth/codex/paste-code`
- `POST /api/auth/codex/import-cli`
- `GET /api/auth/codex/status`
- `POST /api/auth/codex/logout`

### Sales endpoints

- `GET /api/sales/profile`
- `PUT /api/sales/profile`
- `POST /api/sales/profile/autofill`
- `GET /api/sales/onboarding/status`
- `POST /api/sales/onboarding/brief`
- `POST /api/sales/run`
- `GET /api/sales/jobs/active`
- `GET /api/sales/jobs/{job_id}/progress`
- `POST /api/sales/jobs/{job_id}/retry`
- `GET /api/sales/source-health`
- `GET /api/sales/runs`
- `GET /api/sales/leads`
- `GET /api/sales/prospects`
- `GET /api/sales/accounts/{id}/dossier`
- `GET /api/sales/approvals`
- `POST /api/sales/approvals/bulk-approve`
- `PATCH /api/sales/approvals/{id}/edit`
- `POST /api/sales/approvals/{id}/approve`
- `POST /api/sales/approvals/{id}/reject`
- `GET /api/sales/deliveries`

## Sales Flow in Detail

### 1. Onboarding and profile creation

The operator provides a short product/ICP brief or edits a profile directly.

The onboarding layer:

- normalizes loose numeric/string input
- validates required fields
- clamps daily quotas and schedule settings
- tracks readiness by segment
- checks whether Codex OAuth is connected

This work lives primarily in `sales/onboarding.rs`.

### 2. Discovery

Discovery differs slightly by segment:

- `B2B`
  starts from industry/geo targeting and public-source account gathering
- `B2C`
  emphasizes social discovery, local market pockets, and buyer-priority shaping

Directory scraping and source collection live in:

- `sales/directories.rs`
- `sales/discovery.rs`

### 3. Enrichment

Enrichment is where most of the repo-specific differentiation lives.

The engine does not simply ask an LLM to guess. It first uses deterministic logic:

- domain normalization
- reachability heuristics
- contact identity shaping
- signal extraction from search/fetch results
- source-specific scoring
- company/account canonicalization

Then LLMs are used as assistants, not the only truth source.

This layering is split across:

- `sales/enrichment.rs`
- `sales/strategy.rs`
- `sales/llm.rs`

### 4. Prospect dossier construction

The repo distinguishes raw leads from reusable prospect dossiers.

A dossier is closer to an account-level sales reasoning object:

- company/account identity
- available contacts and contact methods
- signals and why-now evidence
- role hypotheses
- outreach angle
- pain points
- summary/thesis

This logic sits mainly in `sales/prospects.rs`.

### 5. Approval drafting

The system creates approval records rather than sending everything immediately.

That gives the operator control over:

- message payload
- channel choice
- last-mile edits
- approval/reject decisions

Approval and delivery lifecycle state is persisted in `sales.db`.

### 6. Delivery

Current live outbound story is approval-gated sending with email and operator-assisted messaging patterns.

The repo still contains broader messaging/package artifacts, but the active HTTP product is the sales approval and delivery surface described above.

## B2B vs B2C

The repo supports both, but they are not mirror images.

### B2B

B2B is account-centric:

- define ICP
- search company/account space
- enrich accounts and people
- build dossiers
- produce approval-ready outreach

### B2C

B2C is market-centric:

- identify local/social demand pockets
- cluster likely buyer audiences
- surface high-intent queues
- build market thesis and offer angles
- produce operator-facing market views in the UI

The B2C UI/community work shows up clearly in:

- `ui/src/pages/sales/b2c.js`
- `static/html/sales_market_b2c.html`

## Data Model

### Sales operational database

`SalesEngine` initializes and owns `sales.db`.

Tables include:

- `sales_profile`
- `sales_profiles_v2`
- `sales_runs`
- `leads`
- `approvals`
- `deliveries`
- `sales_onboarding`
- `sales_onboarding_v2`
- `discovered_domains`
- `prospect_profiles`
- `artifacts`
- `evidence`
- `accounts`
- `account_aliases`
- `domains`
- `contacts`
- `contact_methods`
- `buyer_roles`
- `signals`
- `signal_rationales`
- `score_snapshots`
- `research_queue`
- `account_theses`
- `activation_queue`

This schema tells you the product direction: the repo is moving toward account intelligence and operator review, not raw one-off lead blasting.

### Memory substrate

The memory crate keeps a second layer of durable state:

- structured KV and operational side state
- canonical sessions and compaction
- semantic lookup tokens/grams
- graph entities and relations
- usage and consolidation

If you are tracing cross-cutting state, `MemorySubstrate` is the hub.

## Configuration

Core config schema is defined in `crates/pulsivo-salesman-types/src/config.rs`.

Important top-level fields:

- `home_dir`
- `data_dir`
- `log_level`
- `api_listen`
- `api_key`
- `default_model`
- `memory`
- `web`
- `channels`
- `reload`
- `mode`
- `language`
- `include`

Default home layout:

```text
~/.pulsivo-salesman/
  config.toml
  daemon.json
  auth/
    codex_oauth.json
  data/
    pulsivo-salesman.db
  sales.db
```

### Web/search config

Supported search providers:

- Brave
- Tavily
- Perplexity
- DuckDuckGo
- Auto fallback

### Email config

The active channel config is intentionally small:

- SMTP host
- SMTP port
- username
- password environment variable

### Hot reload

Config reload can be:

- `off`
- `restart`
- `hot`
- `hybrid`

The reload planner explicitly decides which fields require restart versus in-place update.

## UI Composition

The browser surface is embedded into the API crate.

Important characteristics:

- no CDN dependency
- vendor JS and CSS are vendored locally
- Bun builds the app shell into a single bundle
- build-time embedding keeps deployment single-binary friendly

The UI is assembled from fragments:

- onboarding
- command
- B2C market
- runs
- profile views
- approvals
- leads
- delivery history

The current client-side architecture is plain JavaScript rather than a heavyweight frontend framework runtime.

## Security and Access Model

### API auth

If `api_key` is empty:

- localhost access is effectively open for convenience

If `api_key` is non-empty:

- requests must include `Authorization: Bearer <api_key>`

The middleware is constant-time for token comparison and intentionally rejects remote access when no key is configured.

### OAuth

Codex OAuth is handled by:

- browser PKCE start
- callback completion
- manual paste-code fallback
- optional import from existing `~/.codex/auth.json`
- local persistence into `auth/codex_oauth.json`

### Web safety

The runtime web fetch layer includes SSRF checks and response size/time limits. Search and fetch are not unconstrained raw network pipes.

## Development Workflow

### Baseline checks

```bash
bun run build:webchat --cwd crates/pulsivo-salesman-api/ui
cargo check
cargo run -p xtask -- test-smoke
```

### Graph inspection

The current graph artifacts live in:

```text
graphify-out/graph.html
graphify-out/graph.json
graphify-out/GRAPH_REPORT.md
```

If you are new to the codebase, start with the graph, not a blind grep pass.

### What to read first

If you want the shortest path to understanding:

1. `crates/pulsivo-salesman-api/src/server.rs`
2. `crates/pulsivo-salesman-api/src/sales.rs`
3. `crates/pulsivo-salesman-api/src/sales/engine.rs`
4. `crates/pulsivo-salesman-api/src/sales/enrichment.rs`
5. `crates/pulsivo-salesman-api/src/sales/prospects.rs`
6. `crates/pulsivo-salesman-kernel/src/kernel.rs`
7. `crates/pulsivo-salesman-memory/src/substrate.rs`
8. `crates/pulsivo-salesman-runtime/src/model_catalog.rs`

## Current Caveats and Honest Notes

This section is deliberate. The repo contains real implementation, but a few boundaries are still worth stating explicitly.

### 1. The first-party launcher now exists and is intentionally thin

The workspace now includes a dedicated `pulsivo-salesman` binary crate that:

- boots `PulsivoSalesmanKernel`
- auto-bootstraps `~/.pulsivo-salesman/config.toml` when missing
- calls `pulsivo-salesman-api::server::run_daemon`

This keeps the daemon entrypoint explicit without reintroducing the removed legacy CLI/TUI layers.

### 2. Packaging is aligned to that launcher

The following files now point at the real binary and the real default port:

- `Dockerfile`
- `deploy/pulsivo-salesman-entrypoint.sh`
- `deploy/pulsivo-salesman.service`
- `scripts/install.sh`
- `scripts/install.ps1`

### 3. The SDKs now track the live sales router

Both SDK trees target the current `/api/sales/*` contract instead of the removed agent/session/workflow API.

### 4. The WhatsApp gateway is now self-consistent

`packages/whatsapp-gateway/index.js` no longer assumes the removed legacy chat route. It now exposes:

- QR login flow
- outbound send
- inbound inbox capture for operator-side processing

### 5. The graph is structural, not semantic

For this refresh the corpus was code-only, so the graph is AST-heavy and does not include an LLM semantic pass.

That means:

- it is excellent for architecture and file-level clustering
- it is weaker for rationale extraction and cross-document explanation
- it found no surprising cross-file semantic bridges because most connections are structural and localized

## Non-Goals of This Repo State

This repo is not currently trying to be:

- a general-purpose multi-channel agent OS
- a workflow orchestration platform
- a broad plugin marketplace runtime
- a desktop/TUI product

Those surfaces were intentionally removed or reduced.

## Recommended Next Steps

If you are continuing development, the highest-value follow-ups are:

1. expand the first-party CLI beyond `init`, `start`, and `status` only if the workflow genuinely needs it
2. decide whether the WhatsApp gateway should stay inbox/outbound-only or gain a formal sales reply ingestion contract
3. keep regenerating `graphify-out/` whenever the repo shape changes materially

## Bottom Line

Pulsivo Salesman is currently a focused sales system with a real engine, real persistence, real UI, and a much narrower scope than the codebase it came from.

If you read the repo through that lens, it becomes coherent quickly:

- `api` owns the product surface
- `sales` owns the workflow
- `kernel` owns runtime state and config
- `memory` owns durable substrate
- `runtime` owns model and web primitives
- everything else is either support code or packaging residue waiting to be brought back in line
