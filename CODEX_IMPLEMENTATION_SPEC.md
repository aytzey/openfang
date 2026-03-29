# OpenFang Signal-to-Meeting Engine — Codex Implementation Spec

> **Purpose:** Single-file, self-contained implementation specification.
> An AI coding agent (Codex) should be able to read ONLY this file and
> implement the entire system correctly, phase by phase.
>
> **Reference:** `OPENFANG_MEGA_DOC_TR.md` contains full analysis, rationale,
> and architectural discussion. This file contains ONLY actionable instructions.
>
> **Language:** Rust (backend), Alpine.js (frontend), SQLite (data), SQL (DDL)
> **Date:** 2026-03-27

---

## TABLE OF CONTENTS

- [0. CODEBASE MAP](#0-codebase-map)
- [1. ARCHITECTURAL RULES](#1-architectural-rules)
- [2. CURRENT STATE SNAPSHOT](#2-current-state-snapshot)
- [3. TARGET DATA MODEL](#3-target-data-model)
- [4. PHASE 0 — Stop the Bleeding (14 tasks)](#4-phase-0--stop-the-bleeding)
- [5. PHASE 1 — Truth + Decision Layer (14 tasks)](#5-phase-1--truth--decision-layer)
- [6. PHASE 2 — Activation + Learning (17 tasks)](#6-phase-2--activation--learning)
- [7. PHASE 3 — UI/UX Overhaul (9 tasks)](#7-phase-3--uiux-overhaul)
- [8. PHASE 4 — Scale (7 tasks)](#8-phase-4--scale)
- [9. VERIFICATION PROTOCOL](#9-verification-protocol)

---

## 0. CODEBASE MAP

```
/programs/openfang/
├── crates/
│   ├── openfang-api/
│   │   ├── src/
│   │   │   ├── sales.rs          # 14,466 lines — Sales/Prospecting engine (ALL pipeline logic)
│   │   │   ├── codex_oauth.rs    # 1,335 lines — Codex OAuth2 PKCE flow
│   │   │   ├── server.rs         # Route registration + AppState
│   │   │   └── routes.rs         # Non-sales API routes
│   │   └── static/
│   │       ├── index_body.html   # 508 lines — Dashboard SPA shell
│   │       └── js/pages/
│   │           └── sales.js      # 611 lines — Sales UI (Alpine.js)
│   ├── openfang-kernel/src/
│   │   └── kernel.rs             # ~5,000 lines — Kernel orchestration
│   ├── openfang-runtime/src/
│   │   └── drivers/codex.rs      # Codex LLM driver
│   ├── openfang-types/src/       # Shared types
│   ├── openfang-memory/src/      # SQLite + semantic search
│   └── openfang-wire/src/        # OFP mesh protocol
├── xtask/                        # Build automation
├── CLAUDE.md                     # Agent instructions
├── OPENFANG_MEGA_DOC_TR.md       # Full analysis reference
└── Cargo.toml                    # Workspace root
```

**Key facts:**
- Config: `~/.openfang/config.toml`
- Sales DB: `~/.openfang/sales.db` (SQLite)
- Default API: `http://127.0.0.1:4200`
- LLM: `gpt-5.3-codex` via OpenAI Codex OAuth (`openai-codex` provider)
- CLI start command: `target/release/openfang.exe start`

---

## 1. ARCHITECTURAL RULES

### MUST DO

1. **Single normalization gateway** — ALL data (LLM, web search, directory scraper, site HTML) passes through the same normalize+verify+classify functions before storage.
2. **Relational source-of-truth** — Canonical Relational Core in SQLite. Suppression, idempotency, approval, retry, audit, queue state, experiment assignment, and delivery events are deterministic records.
3. **Evidence-bound messaging** — Message engine REFUSES to generate copy without an evidence bundle + thesis. Every claim in a message maps to an `evidence_id`.
4. **5-axis scoring** — FitScore, IntentScore, ReachabilityScore, DeliverabilityRisk, ComplianceRisk. Operator sees each axis separately, not a single number.
5. **Send Gate routing** — Block / Research / Nurture / Ready-to-Activate based on axis thresholds.
6. **Discovery ≠ Activation** — `daily_target` limits SENDING only, never discovery. Discovery reservoir fills continuously.
7. **Thesis before lead** — Before creating a lead, create an `account_thesis`: why this account, why now, buyer committee, evidence refs, do-not-say list, recommended channel.
8. **Versioned policies** — Every prompt, scoring formula, and sequence template is versioned. No free-form mutation.
9. **Suppression is permanent for opt-outs** — Turkish Law 6563: once recipient opts out, PERMANENT suppress. No retry ever.
10. **Field-level confidence** — Every data field carries a confidence score (0.0–1.0) and extraction source.
11. **Stage checkpointing** — Pipeline runs as async job with per-stage DB checkpoints. On failure, retry only the failed stage.
12. **New routes must be registered in `server.rs`** AND implemented in the appropriate route file.
13. **Config fields need:** struct field + `#[serde(default)]` + Default impl entry.

### MUST NOT DO

1. **No LinkedIn browser automation** — Remove ALL Playwright/DOM-click LinkedIn code. LinkedIn = partner API OR operator-assist task only.
2. **No single-number scoring** — Never collapse 5 axes into one score for send decisions.
3. **No LLM without evidence** — LLM is optional/low-confidence for discovery (0.4), but main tool for synthesis ONLY after evidence bundle + thesis are built.
4. **No discovery cutting** — Never stop discovery early because `daily_target` is reached.
5. **No sending from main domain** — Always use sending subdomain(s).
6. **No SMTP VRFY as primary** — RFC 5321 allows servers to disable it. Use syntax + MX + domain health instead.
7. **No personality inference** — Do not infer personality from digital footprint. Ground-truth is too low.
8. **No Kuzu in production** — Archived Oct 2025. Use Neo4j/FalkorDB for graph, or defer graph to Phase 2.
9. **No Graph as transactional SoT** — Graph is analytical projection only. Relational core is SoT.
10. **No free agent autonomy for sales** — Use typed workflow, not autonomous agent loop.

---

## 2. CURRENT STATE SNAPSHOT

### 2.1 Current Rust Structs (in `sales.rs`)

```rust
// --- User Configuration ---
pub struct SalesProfile {
    pub product_name: String,
    pub product_description: String,
    pub target_industry: String,
    pub target_geo: String,             // BUG: Rust default "US", JS default "TR"
    pub sender_name: String,
    pub sender_email: String,
    pub sender_linkedin: Option<String>,
    pub target_title_policy: String,    // "ceo_then_founder" | "ceo_only"
    pub daily_target: u32,              // default: 20
    pub daily_send_cap: u32,            // default: 20
    pub schedule_hour_local: u8,        // default: 9
    pub timezone_mode: String,          // "local"
}

// --- Internal Pipeline ---
struct DomainCandidate {
    domain: String,
    score: i32,
    evidence: Vec<String>,
    matched_keywords: Vec<String>,
    source_links: Vec<String>,
}

// --- Prospect Profile (stored as JSON blob) ---
pub struct SalesProspectProfile {
    pub id: String,                         // company_domain
    pub run_id: String,
    pub company: String,
    pub website: String,
    pub company_domain: String,
    pub fit_score: i32,                     // 0-100 (BUG: nearly all 100)
    pub profile_status: String,             // contact_ready|contact_identified|company_only
    pub summary: String,
    pub matched_signals: Vec<String>,
    pub primary_contact_name: Option<String>,
    pub primary_contact_title: Option<String>,
    pub primary_email: Option<String>,
    pub primary_linkedin_url: Option<String>,
    pub company_linkedin_url: Option<String>,
    pub osint_links: Vec<String>,           // max 6
    pub contact_count: u32,
    pub source_count: u32,
    pub buyer_roles: Vec<String>,           // hardcoded templates
    pub pain_points: Vec<String>,           // hardcoded templates
    pub trigger_events: Vec<String>,        // hardcoded templates
    pub recommended_channel: String,
    pub outreach_angle: String,
    pub research_status: String,            // "heuristic" | "llm_enriched"
    pub research_confidence: f32,
}

// --- Lead ---
pub struct SalesLead {
    pub id: String,                     // UUID
    pub run_id: String,
    pub company: String,
    pub website: String,
    pub company_domain: String,
    pub contact_name: String,           // "Leadership Team" if unknown
    pub contact_title: String,
    pub linkedin_url: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,          // BUG: always None (hardcoded)
    pub reasons: Vec<String>,           // 3-4 template sentences
    pub email_subject: String,
    pub email_body: String,
    pub linkedin_message: String,
    pub score: i32,                     // (lead_score + candidate.score).min(100)
    pub status: String,
}
```

### 2.2 Current DB Schema (8 tables in `~/.openfang/sales.db`)

```sql
sales_profile       (id INTEGER PK CHECK(id=1), json TEXT, updated_at TEXT)
sales_runs          (id TEXT PK, status, started_at, completed_at, discovered, inserted, approvals_queued, error)
leads               (id TEXT PK, run_id, company, website, company_domain, contact_name, contact_title,
                     linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message,
                     score, status, created_at, UNIQUE(company_domain, contact_name, contact_title))
approvals           (id TEXT PK, lead_id, channel, payload_json, status, created_at, decided_at)
deliveries          (id TEXT PK, approval_id, channel, recipient, status, error, sent_at)
sales_onboarding    (id INTEGER PK CHECK(id=1), brief_text, updated_at)
discovered_domains  (domain TEXT PK, first_seen_run_id, first_seen_at)
prospect_profiles   (company_domain TEXT PK, run_id, json TEXT, created_at, updated_at)
```

### 2.3 Current Pipeline Call Chain

```
run_generation(profile)
  ├── [Stage 1] llm_build_lead_query_plan()          // 4s timeout
  │   └── fallback: heuristic_lead_query_plan()
  ├── [Stage 2] tokio::join! {                        // 3 parallel streams
  │     llm_generate_company_candidates()             // 10s, max 12
  │     discover_via_web_search()                     // 8s, 10 queries
  │     fetch_free_discovery_candidates()             // 3.2s, 8 TR directories
  │   }
  ├── [Stage 3] merge_all_discovery_sources()         // domain-based dedup
  ├── [Stage 4] llm_validate_candidate_relevance()    // 6s, batch 40
  ├── [Stage 5] filter + sort by score
  ├── [Stage 6] seed_prospect_profiles_for_run()      // 18s LLM enrichment
  └── [Stage 7] Lead Generation Loop:
        for candidate in candidates:
          ├── web_contact_search()
          ├── direct_site_html_enrichment()
          ├── normalize (name, email, linkedin)
          ├── create SalesLead { phone: None }        // BUG-05
          ├── insert_lead() (UNIQUE check)
          └── queue_approvals_for_lead()
```

### 2.4 Current Constants

| Constant | Value |
|----------|-------|
| `SALES_LLM_MODEL` | `gpt-5.3-codex` |
| `MIN_DOMAIN_RELEVANCE_SCORE` | 5 |
| `MAX_DISCOVERY_QUERIES` | 10 |
| `MAX_DIRECT_ENRICH_ATTEMPTS` | 12 |
| `MAX_WEB_CONTACT_SEARCH_ATTEMPTS` | 12 |
| `DIRECT_ENRICH_TIMEOUT_MS` | 3500 |
| `SITE_PAGE_FETCH_TIMEOUT_MS` | 1600 |
| `MAX_FREE_DIRECTORY_CANDIDATES` | 30 |
| `SALES_RUN_REQUEST_TIMEOUT_SECS` | 240 |
| `SALES_SEARCH_BATCH_CONCURRENCY` | 3 |
| `SALES_OSINT_PROFILE_CONCURRENCY` | 4 |
| `PROSPECT_LLM_ENRICH_TIMEOUT_SECS` | 18 |
| `MAX_OSINT_LINKS_PER_PROSPECT` | 6 |

### 2.5 Current API Endpoints (Sales)

| Method | Path | Handler |
|--------|------|---------|
| GET | `/api/sales/profile` | get_sales_profile |
| PUT | `/api/sales/profile` | put_sales_profile |
| POST | `/api/sales/profile/autofill` | autofill_sales_profile |
| GET | `/api/sales/onboarding/status` | get_sales_onboarding_status |
| POST | `/api/sales/onboarding/brief` | put_sales_onboarding_brief |
| POST | `/api/sales/run` | run_sales_now |
| GET | `/api/sales/runs` | list_sales_runs |
| GET | `/api/sales/leads` | list_sales_leads |
| GET | `/api/sales/prospects` | list_sales_prospects |
| GET | `/api/sales/approvals` | list_sales_approvals |
| POST | `/api/sales/approvals/{id}/approve` | approve_and_send |
| POST | `/api/sales/approvals/{id}/reject` | reject_sales_approval |
| GET | `/api/sales/deliveries` | list_sales_deliveries |

### 2.6 Known Bugs (12)

| ID | Severity | Summary | Root Cause | Fix Location |
|----|----------|---------|------------|--------------|
| BUG-01 | CRITICAL | gmail.com accepted as prospect domain | `is_consumer_email_domain()` not called on company domains from directory scrapers | Each `fetch_*_directory_candidates()` function |
| BUG-02 | CRITICAL | ~25 info@ emails became leads | Directory-sourced emails bypass `email_is_generic_role_mailbox()` filter | Lead creation path — apply `normalize_actionable_outreach_email()` to ALL emails |
| BUG-03 | HIGH | "Baskan'in Mesaji" extracted as person name | `contact_name_is_placeholder()` missing Turkish entries | Add: "baskanin mesaji", "genel mudurun mesaji", "hakkimizda", "vizyonumuz", "misyonumuz", "iletisim", "kariyer", "basin" |
| BUG-04 | HIGH | All fit_scores = 100 (no differentiation) | `(60 + 20 + candidate.score).min(100)` — info@ counts as +20 | Replace with 0-1000 weighted scoring (see Phase 1) |
| BUG-05 | HIGH | Phone numbers extracted then lost | `phone: None` hardcoded at lead creation (~line 2315) | Change to `phone: candidate_phone.clone()` + add `phone: Option<String>` to `FreeDiscoveryCandidate` |
| BUG-06 | MEDIUM | Duplicate approval records for same email | `approvals` table has no UNIQUE constraint | Add check: same channel + recipient + pending before creating approval |
| BUG-07 | HIGH | LinkedIn enrichment 0% for Turkish companies | Search query `site:linkedin.com/company "[Turkish Name]"` returns nothing | Implement 4-layer LinkedIn search fallback |
| BUG-08 | MEDIUM | Email templates generic, no LLM | `build_sales_email_body()` uses fixed template | Replace with 2-stage LLM message generation (Phase 2) |
| BUG-09 | MEDIUM | ICP signals identical for all prospects | `matched_keywords` empty → falls back to `profile.target_industry` | Source-based signal differentiation |
| BUG-10 | MEDIUM | "No contact" but "contact_ready" status | info@ email counts as "actionable" | New status: "email_only" for generic-email-only profiles |
| BUG-11 | LOW | Pain points identical for all companies | `build_prospect_pain_points()` uses hardcoded templates | LLM-based company-specific pain point generation (Phase 2) |
| BUG-12 | LOW | First run timeout | 240s API timeout insufficient | Convert to async job + progress endpoint (Phase 0) |

---

## 3. TARGET DATA MODEL

### 3.1 Canonical Relational Core (NEW tables — add to `sales.db`)

```sql
-- ============================================================
-- EVIDENCE PLANE
-- ============================================================

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,       -- 'directory_tmb' | 'directory_eud' | 'web_search' | 'llm_generation' | 'site_html' | 'osint_search'
    source_id TEXT,
    raw_content TEXT,
    parse_status TEXT NOT NULL DEFAULT 'ok',  -- 'ok' | 'partial' | 'failed'
    parser_health REAL DEFAULT 1.0,
    freshness TEXT,                  -- ISO8601 timestamp
    legal_mode TEXT DEFAULT 'public',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS evidence (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    field_type TEXT NOT NULL,        -- 'company_name' | 'domain' | 'contact_name' | 'email' | 'phone' | 'linkedin' | 'title' | 'signal'
    field_value TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.5,
    extraction_method TEXT,          -- 'regex' | 'llm' | 'html_parse' | 'directory_listing'
    verified_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- ACCOUNT / CONTACT CORE
-- ============================================================

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    display_name TEXT,
    legal_name TEXT,
    sector TEXT,
    geo TEXT,
    employee_estimate INTEGER,
    website TEXT,
    tier TEXT DEFAULT 'standard',    -- 'a_tier' | 'standard' | 'basic'
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS account_aliases (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    alias_name TEXT NOT NULL,
    alias_type TEXT NOT NULL          -- 'trade_name' | 'brand' | 'abbreviation' | 'transliteration'
);

CREATE TABLE IF NOT EXISTS domains (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    domain TEXT NOT NULL UNIQUE,
    is_primary INTEGER DEFAULT 0,
    verified INTEGER DEFAULT 0,
    mx_valid INTEGER,
    checked_at TEXT
);

CREATE TABLE IF NOT EXISTS contacts (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    full_name TEXT,
    title TEXT,
    seniority TEXT,                  -- 'c_level' | 'vp' | 'director' | 'manager' | 'unknown'
    department TEXT,
    name_confidence REAL DEFAULT 0.5,
    title_confidence REAL DEFAULT 0.5,
    is_decision_maker INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contact_methods (
    id TEXT PRIMARY KEY,
    contact_id TEXT NOT NULL REFERENCES contacts(id),
    channel_type TEXT NOT NULL,      -- 'email' | 'phone' | 'linkedin'
    value TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    verified_at TEXT,
    classification TEXT,             -- 'personal' | 'generic' | 'role' | 'consumer' | 'invalid'
    suppressed INTEGER DEFAULT 0,
    UNIQUE(contact_id, channel_type, value)
);

CREATE TABLE IF NOT EXISTS buyer_roles (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    contact_id TEXT REFERENCES contacts(id),
    role_type TEXT NOT NULL,         -- 'decision_maker' | 'champion' | 'influencer' | 'blocker' | 'user'
    inferred_from TEXT               -- evidence_id
);

-- ============================================================
-- SIGNALS + SCORING
-- ============================================================

CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    signal_type TEXT NOT NULL,       -- 'directory_membership' | 'job_posting' | 'news' | 'site_content' | 'tech_stack' | 'growth' | 'tender'
    text TEXT NOT NULL,
    source TEXT,
    observed_at TEXT,
    confidence REAL DEFAULT 0.5,
    effect_horizon TEXT,             -- 'immediate' | 'campaign_window' | 'structural'
    expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS signal_rationales (
    id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES signals(id),
    account_id TEXT NOT NULL REFERENCES accounts(id),
    why_it_matters TEXT NOT NULL,
    expected_effect TEXT,
    evidence_ids TEXT,               -- JSON array of evidence IDs
    confidence REAL DEFAULT 0.5,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    validated_at TEXT,
    validation_result TEXT           -- 'validated' | 'false_positive' | 'inconclusive'
);

CREATE TABLE IF NOT EXISTS score_snapshots (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    fit_score REAL NOT NULL DEFAULT 0.0,
    intent_score REAL NOT NULL DEFAULT 0.0,
    reachability_score REAL NOT NULL DEFAULT 0.0,
    deliverability_risk REAL NOT NULL DEFAULT 0.0,
    compliance_risk REAL NOT NULL DEFAULT 0.0,
    activation_priority REAL,
    computed_at TEXT NOT NULL DEFAULT (datetime('now')),
    scoring_version TEXT NOT NULL DEFAULT 'v1'
);

-- ============================================================
-- RESEARCH + THESIS
-- ============================================================

CREATE TABLE IF NOT EXISTS research_queue (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    priority INTEGER DEFAULT 0,
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'in_progress' | 'completed' | 'failed'
    assigned_at TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS account_theses (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    why_this_account TEXT NOT NULL,
    why_now TEXT,
    buyer_committee_json TEXT,       -- JSON: [{role, name, confidence}]
    evidence_ids TEXT,               -- JSON array
    do_not_say TEXT,                 -- JSON array
    recommended_channel TEXT,
    recommended_pain TEXT,
    thesis_confidence REAL DEFAULT 0.0,
    thesis_status TEXT NOT NULL DEFAULT 'draft', -- 'draft' | 'ready' | 'needs_research' | 'blocked'
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- ACTIVATION + SEQUENCES
-- ============================================================

CREATE TABLE IF NOT EXISTS activation_queue (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    contact_id TEXT REFERENCES contacts(id),
    thesis_id TEXT REFERENCES account_theses(id),
    priority INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'activated' | 'skipped'
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sequence_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    steps_json TEXT NOT NULL,        -- JSON: [{step, channel, delay_days, template}]
    icp_id TEXT,
    persona_id TEXT,
    version INTEGER DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sequence_instances (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL REFERENCES sequence_templates(id),
    account_id TEXT NOT NULL REFERENCES accounts(id),
    contact_id TEXT NOT NULL REFERENCES contacts(id),
    thesis_id TEXT REFERENCES account_theses(id),
    current_step INTEGER DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'active', -- 'active' | 'paused' | 'completed' | 'cancelled'
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS touches (
    id TEXT PRIMARY KEY,
    sequence_instance_id TEXT REFERENCES sequence_instances(id),
    step INTEGER NOT NULL,
    channel TEXT NOT NULL,            -- 'email' | 'phone_task' | 'linkedin_assist'
    message_payload TEXT NOT NULL,    -- JSON: {subject, body, linkedin_copy}
    claims_json TEXT,                 -- JSON array of claim strings
    evidence_ids TEXT,                -- JSON array
    variant_id TEXT,
    risk_flags TEXT,                  -- JSON array
    sent_at TEXT,
    mailbox_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- OUTCOMES + LEARNING
-- ============================================================

CREATE TABLE IF NOT EXISTS outcomes (
    id TEXT PRIMARY KEY,
    touch_id TEXT NOT NULL REFERENCES touches(id),
    outcome_type TEXT NOT NULL,
    -- outcome_type enum: hard_bounce | soft_bounce | no_reply | open | click |
    --   auto_reply | unsubscribe | wrong_person | forwarded | referral |
    --   interested | not_now | meeting_booked | closed_won | closed_lost
    raw_text TEXT,
    classified_at TEXT NOT NULL DEFAULT (datetime('now')),
    classifier_confidence REAL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS outcome_attribution_snapshots (
    id TEXT PRIMARY KEY,
    touch_id TEXT NOT NULL REFERENCES touches(id),
    account_id TEXT NOT NULL REFERENCES accounts(id),
    snapshot_at TEXT NOT NULL DEFAULT (datetime('now')),
    score_at_touch_json TEXT,        -- JSON: {fit, intent, reach, deliv_risk, comp_risk}
    active_signal_ids TEXT,          -- JSON array
    unused_signal_ids TEXT,          -- JSON array
    thesis_id TEXT,
    sequence_variant TEXT,
    message_variant TEXT,
    channel TEXT,
    mailbox_id TEXT,
    contextual_factors_json TEXT
);

CREATE TABLE IF NOT EXISTS missed_signal_reviews (
    id TEXT PRIMARY KEY,
    outcome_id TEXT NOT NULL REFERENCES outcomes(id),
    snapshot_id TEXT NOT NULL REFERENCES outcome_attribution_snapshots(id),
    reviewed_at TEXT NOT NULL DEFAULT (datetime('now')),
    validated_signals TEXT,          -- JSON array
    false_positive_signals TEXT,     -- JSON array
    missed_signals TEXT,             -- JSON array
    timing_mistakes TEXT,            -- JSON array
    persona_mismatch TEXT,
    channel_mismatch TEXT,
    reviewer_type TEXT DEFAULT 'auto' -- 'auto' | 'operator'
);

CREATE TABLE IF NOT EXISTS retrieval_rule_versions (
    id TEXT PRIMARY KEY,
    rule_type TEXT NOT NULL,
    -- rule_type enum: query_expansion | source_priority | signal_weight |
    --   pause_rule | asset_matching | buyer_role_priority | timing_rule
    rule_key TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT NOT NULL,
    proposal_source TEXT,
    backtest_result_json TEXT,
    holdout_result_json TEXT,
    status TEXT NOT NULL DEFAULT 'proposed', -- 'proposed' | 'testing' | 'approved' | 'active' | 'retired'
    approved_by TEXT,
    activated_at TEXT,
    version INTEGER DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contextual_factors (
    id TEXT PRIMARY KEY,
    factor_type TEXT NOT NULL,       -- 'holiday' | 'season' | 'regulation' | 'budget_quarter' | 'industry_event'
    factor_key TEXT NOT NULL,
    factor_value TEXT,
    effective_from TEXT,
    effective_until TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS exploration_log (
    id TEXT PRIMARY KEY,
    touch_id TEXT REFERENCES touches(id),
    account_id TEXT NOT NULL REFERENCES accounts(id),
    exploration_reason TEXT NOT NULL,
    exploration_type TEXT,           -- 'new_source' | 'unusual_signal' | 'new_segment' | 'new_variant'
    outcome_id TEXT,
    learned_pattern TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- SUPPRESSION + COMPLIANCE
-- ============================================================

CREATE TABLE IF NOT EXISTS suppressions (
    id TEXT PRIMARY KEY,
    contact_method_value TEXT NOT NULL, -- email address or phone number
    reason TEXT NOT NULL,               -- 'opt_out' | 'hard_bounce' | 'complaint' | 'manual' | 'legal'
    source_outcome_id TEXT,
    suppressed_at TEXT NOT NULL DEFAULT (datetime('now')),
    permanent INTEGER NOT NULL DEFAULT 0  -- 1 = permanent (opt-out per Law 6563)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_suppressions_value ON suppressions(contact_method_value);

-- ============================================================
-- EXPERIMENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    hypothesis TEXT,
    variant_a TEXT,
    variant_b TEXT,
    status TEXT NOT NULL DEFAULT 'active', -- 'active' | 'paused' | 'completed'
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS experiment_assignments (
    id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    sequence_instance_id TEXT,
    variant TEXT NOT NULL             -- 'a' | 'b'
);

-- ============================================================
-- OPERATIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS source_health (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL UNIQUE, -- 'directory_tmb' | 'directory_eud' | ... | 'web_search' | 'llm'
    precision REAL,
    freshness TEXT,
    parser_health REAL DEFAULT 1.0,
    legal_mode TEXT DEFAULT 'public',
    historical_reply_yield REAL,
    last_checked_at TEXT,
    auto_skip INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_runs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,           -- 'discovery' | 'research' | 'activation' | 'delivery'
    status TEXT NOT NULL DEFAULT 'running', -- 'running' | 'completed' | 'failed' | 'cancelled'
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS job_stages (
    id TEXT PRIMARY KEY,
    job_run_id TEXT NOT NULL REFERENCES job_runs(id),
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'running' | 'completed' | 'failed' | 'skipped'
    checkpoint_data TEXT,            -- JSON: stage-specific state
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================
-- STRATEGY (ICP Control Plane)
-- ============================================================

CREATE TABLE IF NOT EXISTS icp_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sector_rules TEXT,               -- JSON
    geo_rules TEXT,                   -- JSON
    size_rules TEXT,                  -- JSON
    negative_rules TEXT,             -- JSON: excluded sectors, domains, roles
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    icp_id TEXT NOT NULL REFERENCES icp_definitions(id),
    name TEXT NOT NULL,
    criteria_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS personas (
    id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL REFERENCES segments(id),
    role_type TEXT NOT NULL,
    pain_angles TEXT,                -- JSON array
    message_strategy TEXT
);

CREATE TABLE IF NOT EXISTS sender_policies (
    id TEXT PRIMARY KEY,
    icp_id TEXT REFERENCES icp_definitions(id),
    mailbox_pool TEXT,               -- JSON array of mailbox configs
    daily_cap INTEGER DEFAULT 20,
    subdomain TEXT,
    warm_state TEXT DEFAULT 'cold'   -- 'cold' | 'warming' | 'warm' | 'hot'
);

-- ============================================================
-- LEGACY (kept during transition)
-- ============================================================
-- sales_profile, sales_runs, leads, approvals, deliveries,
-- sales_onboarding, discovered_domains, prospect_profiles
-- These remain functional. New tables run in parallel.
-- Migration: Phase 1, Task 15.
```

### 3.2 Migration Strategy

Legacy tables remain functional during transition. New tables run in parallel:
- `prospect_profiles` + `leads` → `accounts` + `contacts` + `contact_methods`
- `discovered_domains` → `domains`
- `sales_profile` → `icp_definitions` + `sender_policies`
- `approvals` + `deliveries` stay, `touches` + `outcomes` run alongside

---

## 4. PHASE 0 — Stop the Bleeding

> **Goal:** Fix critical data quality bugs, add safety rails, enable async jobs.
> **Timeline guidance:** 0-7 days. All tasks are independent unless noted.

### TASK-01: Normalize + Verify + Classify Gateway

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None
**Action:** Create a module of gateway functions that ALL data sources must pass through. Every directory scraper, web search result, LLM output, and site HTML extraction must call these before storage.

```rust
// --- Add to sales.rs (or extract to sales/normalize.rs) ---

/// Master gateway: normalize all fields of a raw candidate.
/// Call this on every DomainCandidate before any further processing.
pub fn normalize_candidate_gateway(candidate: &mut DomainCandidate) -> bool {
    // Returns false if candidate should be rejected
    if !is_valid_company_domain(&candidate.domain) {
        return false;
    }
    candidate.domain = normalize_domain(&candidate.domain);
    // Normalize contact hints if present
    true
}

/// Domain validation — rejects consumer, blocked, government, and malformed domains
pub fn is_valid_company_domain(domain: &str) -> bool {
    let d = domain.trim().to_lowercase();
    !d.is_empty()
        && d.contains('.')
        && d.len() > 4
        && !is_consumer_email_domain(&d)
        && !is_blocked_company_domain(&d)
        && !d.ends_with(".gov.tr")
        && !d.ends_with(".edu.tr")
        && !d.ends_with(".mil.tr")
}

/// Email classification: personal | generic | role | consumer | invalid
pub fn classify_email(email: &str, company_domain: &str) -> &'static str {
    let e = email.trim().to_lowercase();
    if !email_syntax_valid(&e) { return "invalid"; }
    let (local, domain) = match e.split_once('@') {
        Some(parts) => parts,
        None => return "invalid",
    };
    if is_consumer_email_domain(domain) { return "consumer"; }
    if email_is_generic_role_mailbox(&e) { return "generic"; }
    // Role mailboxes: sales@, hr@, support@, etc.
    let role_prefixes = ["sales", "hr", "support", "billing", "accounting",
                         "marketing", "pr", "legal", "procurement", "satin", "satinalma"];
    if role_prefixes.iter().any(|p| local == *p) { return "role"; }
    // If local part looks like a name (contains dot or >3 chars, not a dept)
    "personal"
}

/// Phone normalization to E.164 format
pub fn normalize_phone(raw: &str) -> Option<String> {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 10 { return None; }
    if digits.starts_with("90") && digits.len() == 12 {
        Some(format!("+{}", digits))
    } else if digits.starts_with("0") && digits.len() == 11 {
        Some(format!("+90{}", &digits[1..]))
    } else if digits.len() == 10 {
        Some(format!("+90{}", digits))
    } else {
        Some(format!("+{}", digits)) // best effort
    }
}

/// Turkish + English placeholder name detection
pub fn is_placeholder_name(name: &str) -> bool {
    let n = name.trim().to_lowercase();
    let n = n.replace(['ı','İ','ş','Ş','ç','Ç','ö','Ö','ü','Ü','ğ','Ğ'], |c: char| {
        match c {
            'ı' | 'İ' => "i", 'ş' | 'Ş' => "s", 'ç' | 'Ç' => "c",
            'ö' | 'Ö' => "o", 'ü' | 'Ü' => "u", 'ğ' | 'Ğ' => "g",
            _ => "",
        }.to_string().chars().next().unwrap_or(c)
    }); // simplified — use proper transliteration
    let placeholders = [
        // English
        "unknown", "leadership", "leadership team", "management",
        "management team", "executive team", "executive committee",
        "board of directors", "n/a", "not available", "undisclosed",
        // Turkish
        "baskanin mesaji", "genel mudurun mesaji", "hakkimizda",
        "vizyonumuz", "misyonumuz", "iletisim", "kariyer", "basin",
        "ust yonetim", "yonetim ekibi", "yonetim takimi",
        "yonetim kurulu", "icra kurulu", "kurumsal",
        // Section headings (BUG-03)
        "anasayfa", "hakkinda", "referanslar", "projeler", "haberler",
        "duyurular", "galeri", "urunler", "hizmetler",
    ];
    placeholders.iter().any(|p| n == *p || n.contains(p))
}
```

**Verify:** Write a unit test with these cases:
- `is_valid_company_domain("gmail.com")` → `false`
- `is_valid_company_domain("machinity.com")` → `true`
- `classify_email("info@acme.com", "acme.com")` → `"generic"`
- `classify_email("ali.vural@acme.com", "acme.com")` → `"personal"`
- `is_placeholder_name("Başkan'ın Mesajı")` → `true`
- `normalize_phone("0530 851 89 61")` → `Some("+905308518961")`

---

### TASK-02: Consumer Domain Gate on ALL Code Paths

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-01
**Action:** Add `is_valid_company_domain()` call at the output of EVERY directory scraper function. Specifically, wrap the domain extraction in each of these 8 functions:

```rust
// In each fetch_*_directory_candidates() function, after extracting domain:
// BEFORE (example from fetch_platformder_directory_candidates):
candidates.push(DomainCandidate { domain: extracted_domain, ... });

// AFTER:
if is_valid_company_domain(&extracted_domain) {
    candidates.push(DomainCandidate { domain: extracted_domain, ... });
}
```

Apply to: `fetch_tmb_directory_candidates`, `fetch_eud_directory_candidates`, `fetch_asmud_directory_candidates`, `fetch_platformder_directory_candidates`, `fetch_mib_directory_candidates`, `fetch_imder_directory_candidates`, `fetch_isder_directory_candidates`, `fetch_thbb_directory_candidates`.

Also add the gate to `llm_generate_company_candidates()` and `discover_via_web_search()` output processing.

**Verify:** Run a pipeline with known gmail.com entry in Platformder. It should be filtered out. Zero consumer domains in output.

---

### TASK-03: Turkish Placeholder Name List

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-01
**Action:** Replace the existing `contact_name_is_placeholder()` function body with a call to the new `is_placeholder_name()` from TASK-01. The new function already includes Turkish entries.

```rust
// Replace old function:
fn contact_name_is_placeholder(name: Option<&str>) -> bool {
    match name {
        None => true,
        Some(n) => is_placeholder_name(n),
    }
}
```

**Verify:** `contact_name_is_placeholder(Some("Başkan'ın Mesajı"))` → `true`

---

### TASK-04: Phone Number Pass-Through

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-01
**Action:**

Step 1: Add `phone: Option<String>` field to `FreeDiscoveryCandidate` (or equivalent internal struct) and to `DomainCandidate`:

```rust
struct DomainCandidate {
    domain: String,
    score: i32,
    evidence: Vec<String>,
    matched_keywords: Vec<String>,
    source_links: Vec<String>,
    phone: Option<String>,  // NEW
}
```

Step 2: In each directory scraper that extracts phone numbers, populate this field (apply `normalize_phone()` from TASK-01).

Step 3: At lead creation (~line 2315 in sales.rs), change:
```rust
// BEFORE:
phone: None,

// AFTER:
phone: candidate.phone.as_ref().and_then(|p| normalize_phone(p)),
```

**Verify:** Run pipeline against a directory entry with a known phone number. Lead should have `phone` populated.

---

### TASK-05: Idempotency Key for Approvals

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None
**Action:** Before creating an approval record, check for existing pending approval with same channel + recipient:

```rust
// Before INSERT INTO approvals:
fn approval_already_pending(db: &Connection, channel: &str, recipient: &str) -> Result<bool> {
    let count: i32 = db.query_row(
        "SELECT COUNT(*) FROM approvals WHERE channel = ?1 AND status = 'pending'
         AND json_extract(payload_json, '$.to') = ?2",
        params![channel, recipient],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}
```

Also add partial unique index to prevent race conditions:
```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_approvals_pending_recipient
    ON approvals(channel, json_extract(payload_json, '$.to'))
    WHERE status = 'pending';
```

**Verify:** Queue same email twice → second attempt returns existing approval, not duplicate.

---

### TASK-06: Remove LinkedIn Browser Automation

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None
**Action:**

Step 1: Find and remove all Playwright/browser automation code for LinkedIn sending. Search for: `send_linkedin`, `playwright`, `browser`, `page.goto("linkedin`)`, `DOM`, `click`.

Step 2: Replace `send_linkedin()` with operator-assist task creation:

```rust
async fn send_linkedin(
    &self, _state: &AppState, profile_url: &str, message: &str
) -> Result<(), String> {
    // Create operator task instead of automating
    // Store as a task in deliveries with status = 'operator_pending'
    self.db_execute(
        "INSERT INTO deliveries (id, approval_id, channel, recipient, status, sent_at)
         VALUES (?1, ?2, 'linkedin_assist', ?3, 'operator_pending', ?4)",
        // ...
    );
    Ok(())
}
```

Step 3: In the dashboard, show LinkedIn tasks as "Manual Action Required" items.

**Verify:** `cargo clippy` — no references to playwright/browser automation remain. LinkedIn deliveries show as `operator_pending`.

---

### TASK-07: Async Job + Stage Checkpointing

**Files:** `crates/openfang-api/src/sales.rs`, `crates/openfang-api/src/server.rs`
**Depends on:** None
**Action:**

Step 1: Define pipeline stages:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PipelineStage {
    QueryPlanning,
    Discovery,
    Merging,
    Validation,
    Filtering,
    Enrichment,
    LeadGeneration,
}
```

Step 2: Refactor `run_generation()` to write checkpoints to `job_runs` + `job_stages` tables after each stage.

Step 3: Change `run_sales_now` endpoint to:
- Create `job_runs` record with status `running`
- Spawn `run_generation()` as `tokio::spawn` background task
- Return immediately with `{ "job_id": "...", "status": "running" }`

Step 4: Add progress endpoint:
```
GET /api/sales/jobs/{job_id}/progress
→ { "job_id": "...", "status": "running", "current_stage": "Discovery",
    "stages": [{"name": "QueryPlanning", "status": "completed"}, ...] }
```

Step 5: Register new route in `server.rs`.

Step 6: On failure, mark only the failed stage. Add retry endpoint:
```
POST /api/sales/jobs/{job_id}/retry
→ Resumes from the last failed stage using checkpoint_data
```

**Verify:** Start a run → immediately get job_id. Poll progress → see stages advance. Kill daemon mid-run → restart → retry resumes from checkpoint.

---

### TASK-08: Suppression Table + Unsubscribe Ledger

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None (uses DDL from Section 3)
**Action:**

Step 1: Create `suppressions` table (DDL in Section 3).

Step 2: Add suppression check before every delivery:

```rust
fn is_suppressed(db: &Connection, contact_value: &str) -> Result<bool> {
    let count: i32 = db.query_row(
        "SELECT COUNT(*) FROM suppressions WHERE contact_method_value = ?1",
        params![contact_value],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}
```

Step 3: In `approve_and_send()`, check suppression before sending:
```rust
if is_suppressed(&db, &recipient)? {
    // Update approval status to 'blocked_suppressed'
    return Err("Recipient is suppressed".into());
}
```

Step 4: On bounce/unsubscribe outcome, add to suppressions:
```rust
fn suppress_contact(db: &Connection, value: &str, reason: &str, permanent: bool) -> Result<()> {
    db.execute(
        "INSERT OR IGNORE INTO suppressions (id, contact_method_value, reason, permanent)
         VALUES (?1, ?2, ?3, ?4)",
        params![uuid(), value, reason, permanent as i32],
    )?;
    Ok(())
}
```

**Verify:** Suppress an email → try to approve delivery to that email → blocked. Unsubscribe → permanent=1 in DB.

---

### TASK-09: Field-Level Confidence

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-01
**Action:** Add confidence tracking to prospect profile data. When extracting data from different sources, tag each field with confidence:

```rust
/// Source-based confidence defaults
fn source_confidence(source: &str) -> f64 {
    match source {
        "directory_listing" => 0.9,  // Verified membership
        "site_html" => 0.8,         // From company's own site
        "web_search" => 0.6,        // From search results
        "llm_generation" => 0.4,    // LLM-generated
        "llm_enrichment" => 0.5,    // LLM-enriched from evidence
        _ => 0.3,
    }
}
```

Store in `evidence` table (DDL in Section 3). Each extracted field gets an evidence record with confidence.

**Verify:** After enrichment, query `evidence` table → each field has a confidence score. Directory data = 0.9, LLM data = 0.4.

---

### TASK-10: Fix target_geo Default

**Files:** `crates/openfang-api/src/sales.rs`, `crates/openfang-api/static/js/pages/sales.js`
**Depends on:** None
**Action:**

Rust side — change default:
```rust
// In SalesProfile Default impl or serde default:
// BEFORE:
target_geo: "US".to_string(),
// AFTER:
target_geo: String::new(),  // No default — force user to set
```

JS side — remove hardcoded default:
```javascript
// BEFORE (if exists):
target_geo: 'TR',
// AFTER:
target_geo: '',  // Match Rust — no default
```

Add validation in `run_generation()`:
```rust
if profile.target_geo.is_empty() {
    return Err("target_geo must be set before running".into());
}
```

**Verify:** New profile without geo → run fails with clear error. Set geo to "TR" → run succeeds.

---

### TASK-11: Scraper Health Check

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None (uses DDL from Section 3)
**Action:**

Step 1: Create `source_health` table (DDL in Section 3).

Step 2: After each scraper run, record results:
```rust
fn update_source_health(db: &Connection, source_type: &str, count: usize) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let health = if count == 0 { 0.0 } else { 1.0 };
    db.execute(
        "INSERT INTO source_health (id, source_type, parser_health, last_checked_at, auto_skip)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(source_type) DO UPDATE SET
           parser_health = ?3, last_checked_at = ?4,
           auto_skip = CASE WHEN ?3 = 0.0 THEN 1 ELSE 0 END",
        params![uuid(), source_type, health, now, if count == 0 { 1 } else { 0 }],
    )?;
    Ok(())
}
```

Step 3: Before running a scraper, check if auto_skip:
```rust
fn should_skip_source(db: &Connection, source_type: &str) -> Result<bool> {
    // Skip if last 3 runs returned 0 results
    let auto_skip: i32 = db.query_row(
        "SELECT COALESCE(auto_skip, 0) FROM source_health WHERE source_type = ?1",
        params![source_type],
        |r| r.get(0),
    ).unwrap_or(0);
    Ok(auto_skip == 1)
}
```

Step 4: Add API endpoint `GET /api/sales/source-health` to expose health status.

**Verify:** Scraper returns 0 → source_health.auto_skip = 1. Next run skips it. API shows health.

---

### TASK-12: Bounce Shield (Email Validation)

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-08 (suppressions)
**Action:**

Implement pre-send email validation:

```rust
/// Bounce Shield: multi-layer email validation before sending
async fn validate_email_for_sending(email: &str, db: &Connection) -> EmailValidation {
    let mut result = EmailValidation {
        email: email.to_string(),
        syntax_valid: false,
        mx_valid: false,
        domain_health: 0.0,
        suppressed: false,
        classification: "unknown",
        safe_to_send: false,
    };

    // Layer 1: Syntax (RFC 5322)
    result.syntax_valid = email_syntax_valid(email);
    if !result.syntax_valid { return result; }

    // Layer 2: Suppression check
    result.suppressed = is_suppressed(db, email).unwrap_or(false);
    if result.suppressed { return result; }

    // Layer 3: MX record check
    let domain = email.split('@').nth(1).unwrap_or("");
    result.mx_valid = check_mx_record(domain).await;
    if !result.mx_valid { return result; }

    // Layer 4: Domain health (age, SPF/DKIM, blacklist)
    result.domain_health = assess_domain_health(domain).await;

    // Layer 5: Historical bounce check
    let bounce_count: i32 = db.query_row(
        "SELECT COUNT(*) FROM suppressions WHERE contact_method_value LIKE '%' || ?1
         AND reason = 'hard_bounce'",
        params![domain],
        |r| r.get(0),
    ).unwrap_or(0);

    // Layer 6: Classification
    result.classification = classify_email(email, domain);

    // Decision
    result.safe_to_send = result.syntax_valid
        && result.mx_valid
        && !result.suppressed
        && result.domain_health > 0.3
        && bounce_count < 3;

    result
}

async fn check_mx_record(domain: &str) -> bool {
    // Use trust-dns-resolver or hickory-dns
    use hickory_resolver::TokioAsyncResolver;
    let resolver = TokioAsyncResolver::tokio_from_system_conf().ok();
    match resolver {
        Some(r) => r.mx_lookup(domain).await.is_ok(),
        None => true, // fail-open if resolver unavailable
    }
}
```

Step 2: Call `validate_email_for_sending()` in `approve_and_send()` before SMTP send. If not safe, redirect to LinkedIn operator-assist.

**Verify:** Send to nonexistent-domain.xyz → blocked (no MX). Send to suppressed email → blocked. Send to valid email → passes.

---

### TASK-13: Sending Subdomain Infrastructure

**Files:** `crates/openfang-api/src/sales.rs`, config
**Depends on:** None
**Action:**

Step 1: Add mailbox pool config to SalesProfile:
```rust
pub struct SenderConfig {
    pub mailboxes: Vec<MailboxConfig>,
}

pub struct MailboxConfig {
    pub email: String,           // e.g., "outreach1@send.machinity.com"
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_user: String,
    pub smtp_pass_env: String,   // env var name containing password
    pub daily_cap: u32,          // per-mailbox daily limit
    pub warm_state: String,      // cold | warming | warm | hot
    pub sends_today: u32,        // runtime counter
}
```

Step 2: In `send_email()`, select mailbox with lowest sends_today that hasn't hit daily_cap:
```rust
fn select_mailbox(mailboxes: &mut [MailboxConfig]) -> Option<&mut MailboxConfig> {
    mailboxes.iter_mut()
        .filter(|m| m.sends_today < m.daily_cap && m.warm_state != "cold")
        .min_by_key(|m| m.sends_today)
}
```

Step 3: Reject sending from main brand domain. Validate subdomain pattern.

**Verify:** Configure 2 mailboxes with cap=10 each. Send 15 emails. First 10 go to mailbox A, next 5 to mailbox B. Mailbox A at cap → automatically routes to B.

---

### TASK-14: SPF + DKIM + DMARC + One-Click Unsubscribe

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-13
**Action:**

Step 1: Add RFC 8058 one-click unsubscribe header to all outgoing emails:
```rust
// In send_email(), add headers:
let unsubscribe_url = format!(
    "https://{}/api/sales/unsubscribe?token={}",
    config.domain, generate_unsubscribe_token(recipient)
);
message_builder = message_builder
    .header(("List-Unsubscribe", format!("<{}>", unsubscribe_url)))
    .header(("List-Unsubscribe-Post", "List-Unsubscribe=One-Click"));
```

Step 2: Add unsubscribe endpoint:
```
GET /api/sales/unsubscribe?token=XXX
→ Verify token, add to suppressions(permanent=1), show confirmation page
```

Step 3: Register route in `server.rs`.

Step 4: Document that sending subdomains MUST have SPF + DKIM + DMARC configured at DNS level (this is a deployment requirement, not code).

**Verify:** Email contains `List-Unsubscribe` header. Click unsubscribe link → email added to suppressions with permanent=1.

---

## 5. PHASE 1 — Truth + Decision Layer

> **Goal:** Canonical data model, 5-axis scoring, send gate, research workbench.
> **Timeline guidance:** 7-21 days. Depends on Phase 0 completion.

### TASK-15: Canonical Relational Core Migration

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** All Phase 0 tasks
**Action:**

Step 1: Create ALL tables from Section 3 DDL in `init()`.

Step 2: Write migration function that reads existing data:
```rust
async fn migrate_to_canonical_core(db: &Connection) -> Result<()> {
    // 1. For each prospect_profile JSON blob:
    //    - Create accounts record
    //    - Create domains record (company_domain)
    //    - Create contacts record (primary_contact_*)
    //    - Create contact_methods records (email, linkedin, phone)
    //    - Create evidence records from osint_links

    // 2. For each lead:
    //    - Link to account via company_domain → domains → accounts
    //    - Create/update contact if not exists

    // 3. sales_profile → icp_definitions + sender_policies
    //    (keep sales_profile for backward compat)

    // 4. Mark migration complete (add migration_version to config)
    Ok(())
}
```

Step 3: Run migration on first boot if `migration_version < 2`.

**Verify:** After migration: `SELECT COUNT(*) FROM accounts` = `SELECT COUNT(DISTINCT company_domain) FROM prospect_profiles`. All contact methods have classification. All domains verified via `is_valid_company_domain()`.

---

### TASK-16: Discovery / Activation Separation

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15, TASK-07
**Action:**

Refactor pipeline into 3 queues:

```rust
// Discovery fills the reservoir (NEVER limited by daily_target)
async fn run_discovery(profile: &SalesProfile, job_id: &str) -> Result<Vec<String>> {
    // Stages 1-5 of current pipeline
    // Returns: Vec<account_id>
    // Saves all discovered accounts to `accounts` + `domains` tables
    // Does NOT create leads or approvals
}

// Research enriches accounts that need more data
async fn run_research(account_ids: Vec<String>, job_id: &str) -> Result<()> {
    // For each account in research_queue:
    //   - Site HTML enrichment (multi-page)
    //   - OSINT search enrichment
    //   - LLM profile enrichment
    //   - Update contact_methods, evidence, signals
    //   - Compute 5-axis scores
    //   - Create thesis if ready
}

// Activation selects best accounts for today's sending
async fn run_activation(daily_target: u32, job_id: &str) -> Result<()> {
    // SELECT from accounts WHERE thesis_status = 'ready'
    //   AND send_gate_decision = 'activate'
    //   ORDER BY activation_priority DESC
    //   LIMIT daily_target
    // For each: create sequence_instance, generate message, queue for approval
}
```

**Verify:** Run discovery with daily_target=5. Should discover 30+ accounts but only activate 5 for sending.

---

### TASK-17: 5-Axis Scoring Engine

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15
**Action:**

```rust
pub struct FiveAxisScore {
    pub fit_score: f64,            // 0.0-1.0
    pub intent_score: f64,         // 0.0-1.0
    pub reachability_score: f64,   // 0.0-1.0
    pub deliverability_risk: f64,  // 0.0-1.0 (higher = worse)
    pub compliance_risk: f64,      // 0.0-1.0 (higher = worse)
}

pub fn compute_five_axis_score(
    account_id: &str, db: &Connection
) -> Result<FiveAxisScore> {
    // FitScore: sector_match*0.3 + size_match*0.2 + geo_match*0.2
    //           + site_content_match*0.15 + directory_membership*0.15
    let fit = compute_fit_score(account_id, db)?;

    // IntentScore: new_facility_signal*0.3 + tender_signal*0.3
    //              + growth_signal*0.2 + web_activity*0.2
    let intent = compute_intent_score(account_id, db)?;

    // ReachabilityScore: personal_email*0.35 + linkedin*0.25 + phone*0.2
    //                    + real_name*0.1 + verified_title*0.1
    let reach = compute_reachability_score(account_id, db)?;

    // DeliverabilityRisk: bounce_history*0.3 + domain_reputation*0.3
    //                     + generic_email*0.2 + sender_health*0.2
    let deliv = compute_deliverability_risk(account_id, db)?;

    // ComplianceRisk: suppression_list*0.4 + opt_out_history*0.3 + kvkk_risk*0.3
    let comp = compute_compliance_risk(account_id, db)?;

    // Save snapshot
    save_score_snapshot(db, account_id, &FiveAxisScore {
        fit_score: fit, intent_score: intent,
        reachability_score: reach,
        deliverability_risk: deliv, compliance_risk: comp,
    })?;

    Ok(FiveAxisScore { fit_score: fit, intent_score: intent,
        reachability_score: reach, deliverability_risk: deliv,
        compliance_risk: comp })
}
```

**Verify:** Account with directory membership + personal email + recent signal → high fit, high reach. Account with only info@ + no signal → low reach, low intent.

---

### TASK-18: Send Gate Logic

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-17
**Action:**

```rust
#[derive(Debug, Clone, Serialize)]
pub enum SendGateDecision {
    Block { reason: String },
    Research { missing: Vec<String> },
    Nurture { reason: String },
    Activate,
}

pub fn send_gate(score: &FiveAxisScore) -> SendGateDecision {
    // Block: high risk
    if score.deliverability_risk > 0.7 {
        return SendGateDecision::Block {
            reason: "Deliverability risk too high".into()
        };
    }
    if score.compliance_risk > 0.5 {
        return SendGateDecision::Block {
            reason: "Compliance risk too high".into()
        };
    }

    // Research: can't reach the person
    if score.reachability_score < 0.3 {
        return SendGateDecision::Research {
            missing: vec!["Need personal email or LinkedIn profile".into()]
        };
    }

    // Nurture: bad timing
    if score.intent_score < 0.2 {
        return SendGateDecision::Nurture {
            reason: "No active intent signals detected".into()
        };
    }

    // Activate: good to go
    if score.fit_score > 0.5 && score.reachability_score > 0.4 {
        return SendGateDecision::Activate;
    }

    // Default: research more
    SendGateDecision::Research {
        missing: vec!["Need more data to make decision".into()]
    }
}
```

**Verify:** High-risk account → Block. No contact info → Research. No intent → Nurture. Good account → Activate.

---

### TASK-19: Outcome Classification Ingest

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15, TASK-08
**Action:**

```rust
pub fn classify_outcome(raw_event: &str, event_type: &str) -> OutcomeRecord {
    let outcome_type = match event_type {
        "bounce_hard" => "hard_bounce",
        "bounce_soft" => "soft_bounce",
        "open" => "open",
        "click" => "click",
        "reply" => classify_reply_content(raw_event),
        "unsubscribe" => "unsubscribe",
        _ => "no_reply",
    };

    // Side effects based on outcome
    match outcome_type {
        "hard_bounce" => { /* suppress_contact(permanent=false), update deliverability_risk */ },
        "unsubscribe" => { /* suppress_contact(permanent=true) per Law 6563 */ },
        "wrong_person" => { /* update contact record, flag for research */ },
        "meeting_booked" => { /* mark sequence as success, full attribution */ },
        _ => {},
    }

    OutcomeRecord { outcome_type, raw_text: raw_event.to_string(), .. }
}

fn classify_reply_content(text: &str) -> &'static str {
    let t = text.to_lowercase();
    if t.contains("toplanti") || t.contains("meeting") || t.contains("goruselim") {
        "meeting_booked"
    } else if t.contains("ilginc") || t.contains("interested") || t.contains("merak") {
        "interested"
    } else if t.contains("simdi degil") || t.contains("not now") || t.contains("sonra") {
        "not_now"
    } else if t.contains("yanlis") || t.contains("wrong") || t.contains("hatali") {
        "wrong_person"
    } else if t.contains("cikar") || t.contains("unsubscribe") || t.contains("gonderme") {
        "unsubscribe"
    } else {
        "interested" // default positive for replies
    }
}
```

Add API endpoint for webhook-based outcome ingestion:
```
POST /api/sales/outcomes/webhook
→ { "delivery_id": "...", "event_type": "bounce_hard", "raw_text": "..." }
```

**Verify:** Hard bounce → suppression created. Unsubscribe → permanent suppression. Reply with "toplanti" → meeting_booked.

---

### TASK-19b: Signal Effect Horizon Classifier

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15
**Action:**

```rust
pub fn classify_signal_horizon(signal_type: &str, text: &str) -> (&'static str, Option<String>) {
    // Returns (horizon, expires_at)
    let (horizon, days) = match signal_type {
        "tender" | "crisis" | "urgent_hire" => ("immediate", 21),
        "new_department" | "digitalization" | "new_location" => ("campaign_window", 90),
        "erp_migration" | "merger" | "regulation_pressure" => ("structural", 365),
        "job_posting" => {
            if text.contains("acil") || text.contains("urgent") {
                ("immediate", 21)
            } else {
                ("campaign_window", 60)
            }
        },
        "directory_membership" => ("structural", 365),
        _ => ("campaign_window", 90),
    };

    let expires = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::days(days))
        .map(|d| d.to_rfc3339());

    (horizon, expires)
}
```

**Verify:** Tender signal → "immediate", expires in 21 days. Directory membership → "structural", 365 days.

---

### TASK-19c: Signal Rationale Records

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-19b
**Action:** When creating a signal, also create a rationale record explaining WHY it matters:

```rust
fn create_signal_with_rationale(
    db: &Connection,
    account_id: &str,
    signal_type: &str,
    text: &str,
    source: &str,
    evidence_ids: &[String],
) -> Result<String> {
    let signal_id = uuid();
    let (horizon, expires_at) = classify_signal_horizon(signal_type, text);

    // Insert signal
    db.execute(
        "INSERT INTO signals (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'), ?6, ?7, ?8)",
        params![signal_id, account_id, signal_type, text, source,
                source_confidence(source), horizon, expires_at],
    )?;

    // Insert rationale
    let why = generate_signal_rationale(signal_type, text);
    db.execute(
        "INSERT INTO signal_rationales (id, signal_id, account_id, why_it_matters, expected_effect, evidence_ids, confidence)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![uuid(), signal_id, account_id, why, "meeting_probability_up",
                serde_json::to_string(evidence_ids)?, source_confidence(source)],
    )?;

    Ok(signal_id)
}
```

**Verify:** Each signal in DB has a corresponding rationale record.

---

### TASK-20: Sender Pool + Mailbox Policy

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-13 (Phase 0), TASK-15
**Action:** Create `sender_policies` records from config. Track per-mailbox daily sends. Implement warm-up schedule (start at 5/day, increase by 5/day until target cap).

**Verify:** New mailbox starts with effective cap = 5. After 3 days = 20. Never exceeds configured cap.

---

### TASK-21: Evidence Provenance + Source Hierarchy

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15
**Action:** Every data extraction creates `artifacts` + `evidence` records. When conflicting data exists (e.g., two different emails for same contact), resolve using confidence hierarchy:

```
Directory membership = 0.9
Site HTML = 0.8
Web search = 0.6
LLM enrichment = 0.5
LLM generation = 0.4
```

**Verify:** Two sources report different company names → higher-confidence source wins in `accounts.canonical_name`.

---

### TASK-22: Research Workbench / Thesis Engine

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15, TASK-17
**Action:**

Before creating any lead/activation, create account thesis:

```rust
async fn build_account_thesis(
    account_id: &str, db: &Connection, llm: &LlmClient
) -> Result<String> {
    let account = load_account(db, account_id)?;
    let signals = load_signals(db, account_id)?;
    let contacts = load_contacts(db, account_id)?;
    let evidence = load_evidence_for_account(db, account_id)?;

    // LLM-assisted thesis generation
    let prompt = format!(
        "You are a B2B sales strategist. Given the following account data, create a thesis.\n\
         Account: {}\nSignals: {:?}\nContacts: {:?}\nEvidence: {:?}\n\n\
         Output JSON:\n\
         {{\"why_this_account\": \"...\", \"why_now\": \"...\",\n\
           \"buyer_committee\": [{{\"role\": \"...\", \"name\": \"...\", \"confidence\": 0.0}}],\n\
           \"do_not_say\": [\"...\"], \"recommended_channel\": \"email|phone_task|linkedin_assist\",\n\
           \"recommended_pain\": \"...\", \"thesis_confidence\": 0.0}}",
        account.canonical_name,
        signals.iter().map(|s| &s.text).collect::<Vec<_>>(),
        contacts.iter().map(|c| &c.full_name).collect::<Vec<_>>(),
        evidence.iter().take(5).map(|e| &e.field_value).collect::<Vec<_>>(),
    );

    let response = llm.complete(&prompt, 1200, 0.2).await?;
    let thesis: ThesisJson = serde_json::from_str(&response)?;

    let thesis_id = uuid();
    db.execute(
        "INSERT INTO account_theses (id, account_id, why_this_account, why_now,
         buyer_committee_json, evidence_ids, do_not_say, recommended_channel,
         recommended_pain, thesis_confidence, thesis_status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![thesis_id, account_id, thesis.why_this_account, thesis.why_now,
                serde_json::to_string(&thesis.buyer_committee)?,
                serde_json::to_string(&evidence.iter().map(|e| &e.id).collect::<Vec<_>>())?,
                serde_json::to_string(&thesis.do_not_say)?,
                thesis.recommended_channel, thesis.recommended_pain,
                thesis.thesis_confidence,
                if thesis.thesis_confidence > 0.5 { "ready" } else { "needs_research" }],
    )?;

    Ok(thesis_id)
}
```

**Verify:** Account with 3+ signals and verified contact → thesis_status="ready". Account with only LLM data → "needs_research".

---

### TASK-23: Tier-Based Discovery Model

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-17
**Action:**

After scoring, assign tier:
```rust
fn assign_tier(score: &FiveAxisScore) -> &'static str {
    if score.fit_score > 0.8 && score.intent_score > 0.5 {
        "a_tier"     // Full enrichment: all sources, multi-page, LLM research
    } else if score.fit_score > 0.5 {
        "standard"   // Standard enrichment
    } else {
        "basic"      // Minimal: just verify domain + basic contact
    }
}
```

A-tier accounts get: full multi-page site crawl, multi-query OSINT, LLM deep research, phone verification, value asset eligibility.

**Verify:** High-fit+intent account → a_tier with full enrichment. Low-fit → basic with minimal.

---

### TASK-24: 4-Layer LinkedIn Search

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15
**Action:**

```rust
async fn find_linkedin_url(
    company_name: &str, domain: &str, search: &SearchClient
) -> Option<String> {
    // Layer 1: Domain match
    let q1 = format!("site:linkedin.com/company/ \"{}\"", domain);
    if let Some(url) = search_and_extract_linkedin(&q1, search).await {
        return Some(url);
    }

    // Layer 2: Transliterated name
    let transliterated = transliterate_turkish(company_name);
    let q2 = format!("site:linkedin.com/company/ \"{}\"", transliterated);
    if let Some(url) = search_and_extract_linkedin(&q2, search).await {
        return Some(url);
    }

    // Layer 3: Name + CEO search
    let q3 = format!("\"{}\" linkedin CEO", company_name);
    if let Some(url) = search_and_extract_linkedin(&q3, search).await {
        return Some(url);
    }

    // Layer 4: Turkish LinkedIn
    let q4 = format!("site:tr.linkedin.com \"{}\"", domain);
    search_and_extract_linkedin(&q4, search).await
}

fn transliterate_turkish(s: &str) -> String {
    s.chars().map(|c| match c {
        'ı' => 'i', 'İ' => 'I', 'ş' | 'Ş' => 's',
        'ç' | 'Ç' => 'c', 'ö' | 'Ö' => 'o',
        'ü' | 'Ü' => 'u', 'ğ' | 'Ğ' => 'g',
        _ => c,
    }).collect()
}
```

**Verify:** Turkish company with LinkedIn page → found in 1-4 layers. Target: 30%+ LinkedIn discovery rate (up from 0%).

---

### TASK-25: Email Pattern Guesser

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-12 (bounce shield)
**Action:**

```rust
async fn guess_personal_email(
    first_name: &str, last_name: &str, domain: &str
) -> Option<String> {
    let patterns = [
        format!("{}.{}@{}", first_name.to_lowercase(), last_name.to_lowercase(), domain),
        format!("{}{}@{}", first_name.chars().next().unwrap_or('x'), last_name.to_lowercase(), domain),
        format!("{}@{}", first_name.to_lowercase(), domain),
        format!("{}.{}@{}", first_name.chars().next().unwrap_or('x'), last_name.to_lowercase(), domain),
        format!("{}{}@{}", first_name.to_lowercase(), last_name.to_lowercase(), domain),
    ];

    for email in &patterns {
        // Validate with bounce shield (syntax + MX only, no send)
        if email_syntax_valid(email) && check_mx_record(domain).await {
            return Some(email.clone());
            // Note: confidence = 0.3 for guessed emails
            // Selective SMTP verification only for catch-all domains
        }
    }
    None
}
```

**Verify:** Known contact "Ali Vural" at "acme.com.tr" → suggests "ali.vural@acme.com.tr" with confidence 0.3.

---

### TASK-26: Turkish Site Page Expansion

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** None
**Action:** Expand site HTML enrichment to fetch additional Turkish-language pages:

```rust
const TURKISH_EXTRA_PAGES: &[&str] = &[
    "/yonetim", "/ekibimiz", "/yonetim-kurulu",
    "/iletisim", "/bize-ulasin",
    "/referanslarimiz", "/projelerimiz",
    "/haberler", "/duyurular",
    "/hakkimizda", "/kurumsal",
];
// Also try: /en/management, /en/team, /en/contact (English versions)
```

Add sitemap.xml parsing: if `/sitemap.xml` exists, extract relevant URLs matching patterns like `/team`, `/about`, `/management`.

**Verify:** Site with `/yonetim` page → fetched and parsed for contacts. More contact data extracted than before.

---

### TASK-27: Job Posting Intent Signals

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-15, TASK-19b
**Action:**

Add job posting search as intent signal source:
```rust
async fn search_job_posting_signals(
    account_name: &str, domain: &str, search: &SearchClient
) -> Vec<Signal> {
    let queries = vec![
        format!("site:kariyer.net \"{}\"", account_name),
        format!("site:linkedin.com/jobs \"{}\"", account_name),
        format!("\"{}\" \"saha\" OR \"operasyon\" OR \"field\" iş ilanı", account_name),
    ];

    let mut signals = Vec::new();
    for q in queries {
        if let Ok(results) = search.search(&q, 3).await {
            for r in results {
                if r.title.to_lowercase().contains("operasyon")
                    || r.title.to_lowercase().contains("saha")
                    || r.title.to_lowercase().contains("field")
                {
                    signals.push(Signal {
                        signal_type: "job_posting".into(),
                        text: r.title.clone(),
                        source: r.url.clone(),
                        effect_horizon: "campaign_window".into(),
                        confidence: 0.7,
                    });
                }
            }
        }
    }
    signals
}
```

**Verify:** Company hiring "Saha Operasyon Yöneticisi" → intent signal created with campaign_window horizon.

---

### TASK-28: Tech Stack Detection

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-26
**Action:**

During site HTML enrichment, detect tech stack indicators:
```rust
fn detect_tech_stack(html: &str, headers: &HashMap<String, String>) -> Vec<String> {
    let mut stack = Vec::new();

    // Check common patterns
    let detections = [
        ("SAP", vec!["sap.com", "sap-ui", "sapui5"]),
        ("Salesforce", vec!["salesforce.com", "force.com", "pardot"]),
        ("HubSpot", vec!["hubspot.com", "hs-scripts", "hbspt"]),
        ("Microsoft Dynamics", vec!["dynamics.com", "d365"]),
        ("Oracle", vec!["oracle.com", "eloqua"]),
    ];

    let html_lower = html.to_lowercase();
    for (name, indicators) in &detections {
        if indicators.iter().any(|i| html_lower.contains(i)) {
            stack.push(name.to_string());
        }
    }

    // Check X-Powered-By and Server headers
    if let Some(powered_by) = headers.get("x-powered-by") {
        stack.push(powered_by.clone());
    }

    stack
}
```

**Verify:** Site using SAP → "SAP" in tech stack. Can inform competitive positioning.

---

## 6. PHASE 2 — Activation + Learning

> **Goal:** LLM message generation, sequences, outcome tracking, learning loop.
> **Timeline guidance:** 21-45 days.

### TASK-29: 2-Stage LLM Message Generation

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-22 (thesis engine)
**Action:**

```rust
/// Stage 1: Strategy selection
async fn generate_message_strategy(
    thesis: &AccountThesis, persona: &Contact, db: &Connection
) -> Result<MessageStrategy> {
    // Determine: pain angle, trigger ref, CTA type, tone, language
    let language = if thesis.recommended_channel == "email"
        && account_geo_is_turkey(db, &thesis.account_id)? { "tr" } else { "en" };

    Ok(MessageStrategy {
        pain_angle: thesis.recommended_pain.clone(),
        trigger_evidence: load_best_evidence(db, &thesis.account_id)?,
        cta_type: "soft_ask".into(), // "istersen kisa analiz gondereyim"
        tone: "professional_warm".into(),
        language: language.into(),
    })
}

/// Stage 2: Copy generation (LLM, temp 0.4-0.6)
async fn generate_message_copy(
    strategy: &MessageStrategy,
    thesis: &AccountThesis,
    persona: &Contact,
    evidence_bundle: &[Evidence],
    llm: &LlmClient,
) -> Result<MessageCopy> {
    if evidence_bundle.is_empty() {
        return Err("REFUSED: No evidence bundle provided. Message engine requires evidence.".into());
    }

    let prompt = format!(
        "You are writing a B2B outreach email. Language: {}.\n\n\
         Rules:\n\
         - Max 120 words\n\
         - First sentence: company-specific hook using evidence\n\
         - One pain point + one solution connection\n\
         - Soft CTA: '{}'\n\
         - No generic openings ('I hope this finds you well')\n\
         - No product name in subject line\n\
         - Every claim must map to provided evidence\n\n\
         Account: {}\nPerson: {} ({})\nPain: {}\nEvidence: {:?}\n\
         Thesis: {}\n\nDo NOT say: {:?}\n\n\
         Output JSON: {{\"subject\": \"...\", \"body\": \"...\", \
         \"linkedin_copy\": \"...\", \"claims\": [\"...\"], \
         \"evidence_ids\": [\"...\"]}}",
        strategy.language,
        match strategy.cta_type.as_str() {
            "soft_ask" if strategy.language == "tr" =>
                "Uygunsa size 2 sayfalik kisa bir operasyon analizi paylasabilirim.",
            "soft_ask" =>
                "Happy to share a brief 2-page operational analysis if helpful.",
            _ => "Would you be open to a brief conversation?",
        },
        thesis.why_this_account,
        persona.full_name.as_deref().unwrap_or(""),
        persona.title.as_deref().unwrap_or(""),
        strategy.pain_angle,
        evidence_bundle.iter().map(|e| format!("[{}] {}", e.id, e.field_value)).collect::<Vec<_>>(),
        thesis.why_now.as_deref().unwrap_or(""),
        thesis.do_not_say,
    );

    let response = llm.complete(&prompt, 1200, 0.5).await?;
    let copy: MessageCopy = serde_json::from_str(&response)?;

    // Validate: all evidence_ids must exist in provided bundle
    for eid in &copy.evidence_ids {
        if !evidence_bundle.iter().any(|e| &e.id == eid) {
            return Err(format!("LLM referenced non-existent evidence: {}", eid).into());
        }
    }

    Ok(copy)
}
```

**Verify:** Account with thesis + evidence → personalized email. Account without evidence → REFUSED error. All claims map to real evidence IDs.

---

### TASK-30: Sequence Planner

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-29, TASK-15
**Action:**

Create default 5-step sequence template:

```rust
fn create_default_sequence_template(db: &Connection) -> Result<String> {
    let template_id = uuid();
    let steps = serde_json::json!([
        {"step": 1, "channel": "email", "delay_days": 0, "type": "initial_outreach",
         "description": "Short evidence-based email with soft CTA"},
        {"step": 2, "channel": "email", "delay_days": 3, "type": "value_content",
         "description": "Teardown / case study / relevant insight"},
        {"step": 3, "channel": "email", "delay_days": 5, "type": "followup",
         "description": "Reference previous email, add new angle"},
        {"step": 4, "channel": "linkedin_assist", "delay_days": 3, "type": "channel_switch",
         "description": "LinkedIn connection request (operator manual task)"},
        {"step": 5, "channel": "email", "delay_days": 5, "type": "closing",
         "description": "Final polite closing email"},
    ]);

    db.execute(
        "INSERT INTO sequence_templates (id, name, steps_json, version)
         VALUES (?1, 'default_5step', ?2, 1)",
        params![template_id, steps.to_string()],
    )?;

    Ok(template_id)
}
```

Sequence advancement: after each outcome, check if sequence should advance, pause, or cancel.

**Verify:** Create sequence instance → step 1 email sent. After 3 days with no reply → step 2 queued. Meeting booked at step 2 → sequence marked complete.

---

### TASK-31: Outcome Attribution Snapshot

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-19, TASK-17
**Action:** At every touch send, capture full snapshot of account state:

```rust
fn capture_attribution_snapshot(
    db: &Connection, touch_id: &str, account_id: &str
) -> Result<()> {
    let score = load_latest_score(db, account_id)?;
    let signals = load_active_signals(db, account_id)?;
    let active_ids: Vec<String> = signals.iter()
        .filter(|s| s.used_in_thesis).map(|s| s.id.clone()).collect();
    let unused_ids: Vec<String> = signals.iter()
        .filter(|s| !s.used_in_thesis).map(|s| s.id.clone()).collect();

    db.execute(
        "INSERT INTO outcome_attribution_snapshots
         (id, touch_id, account_id, score_at_touch_json,
          active_signal_ids, unused_signal_ids, thesis_id,
          sequence_variant, message_variant, channel)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![uuid(), touch_id, account_id,
                serde_json::to_string(&score)?,
                serde_json::to_string(&active_ids)?,
                serde_json::to_string(&unused_ids)?,
                /* thesis_id, variant, etc. */],
    )?;
    Ok(())
}
```

**Verify:** After sending email, `outcome_attribution_snapshots` has record with all 5 axis scores and signal lists.

---

### TASK-32: Missed Signal Analyzer

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-31
**Action:** After positive outcome (reply, meeting), analyze what signals were used vs missed:

```rust
async fn analyze_missed_signals(
    db: &Connection, outcome_id: &str
) -> Result<MissedSignalReview> {
    let outcome = load_outcome(db, outcome_id)?;
    let snapshot = load_snapshot_for_touch(db, &outcome.touch_id)?;
    let current_signals = load_all_signals(db, &snapshot.account_id)?;

    let review = MissedSignalReview {
        validated_signals: snapshot.active_signal_ids.clone(),
        false_positive_signals: vec![], // Signals used but outcome was negative
        missed_signals: current_signals.iter()
            .filter(|s| !snapshot.active_signal_ids.contains(&s.id)
                     && !snapshot.unused_signal_ids.contains(&s.id))
            .map(|s| s.id.clone()).collect(), // New since touch
        timing_mistakes: vec![], // TODO: analyze if timing was off
        persona_mismatch: None,
        channel_mismatch: None,
    };

    // Save review
    db.execute(
        "INSERT INTO missed_signal_reviews (...) VALUES (...)",
        // ... params
    )?;

    Ok(review)
}
```

**Verify:** Meeting booked → review shows which signals contributed and which were missed.

---

### TASK-33: Rule Proposal Sandbox

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-32
**Action:** Create system for proposing policy changes that require operator approval:

Store proposals in `retrieval_rule_versions` with status flow: `proposed → testing → approved → active`.

Evaluator creates proposals but CANNOT activate them without operator approval.

Add endpoints:
```
GET  /api/sales/policy/proposals     → list pending proposals
POST /api/sales/policy/proposals/{id}/approve → activate proposal
POST /api/sales/policy/proposals/{id}/reject  → reject proposal
```

**Verify:** Missed signal analysis suggests increasing signal weight → proposal created with status "proposed". Operator approves → status "active".

---

### TASK-34: Calibration + Exploration Manager

**Files:** `crates/openfang-api/src/sales.rs`
**Depends on:** TASK-16 (activation queue)
**Action:**

When selecting accounts for activation:
```rust
fn select_for_activation(db: &Connection, daily_target: u32) -> Result<Vec<String>> {
    let exploit_count = (daily_target as f64 * 0.87).ceil() as u32;
    let explore_count = daily_target - exploit_count;

    // Exploitation: highest activation_priority
    let mut selected = db.prepare(
        "SELECT account_id FROM activation_queue
         WHERE status = 'pending'
         ORDER BY priority DESC LIMIT ?1"
    )?.query_map(params![exploit_count], |r| r.get(0))?
       .collect::<Result<Vec<String>, _>>()?;

    // Exploration: interesting but not top-scoring
    let explored = db.prepare(
        "SELECT a.id FROM accounts a
         JOIN score_snapshots s ON s.account_id = a.id
         WHERE s.fit_score BETWEEN 0.3 AND 0.7
         AND a.id NOT IN (SELECT account_id FROM exploration_log WHERE created_at > datetime('now', '-30 days'))
         ORDER BY RANDOM() LIMIT ?1"
    )?.query_map(params![explore_count], |r| r.get(0))?
       .collect::<Result<Vec<String>, _>>()?;

    for acc_id in &explored {
        db.execute(
            "INSERT INTO exploration_log (id, account_id, exploration_reason, exploration_type)
             VALUES (?1, ?2, 'scheduled_exploration', 'mid_score_random')",
            params![uuid(), acc_id],
        )?;
    }

    selected.extend(explored);
    Ok(selected)
}
```

**Verify:** 20 daily target → 17-18 exploitation + 2-3 exploration. Exploration accounts logged.

---

### TASK-35 through TASK-45: (Remaining Phase 2)

| Task | Summary | Key Detail |
|------|---------|------------|
| TASK-35 | Context Warehouse | `contextual_factors` table: holidays, seasons, regulation dates, budget quarters. Query during message timing. |
| TASK-36 | Outcome-based score calibration | After N outcomes, adjust scoring weights. Signal rationale → validated/false_positive. |
| TASK-37 | Experiment registry | `experiments` + `experiment_assignments` tables. A/B: 2 subject line variants per sequence. |
| TASK-38 | Source pack health metrics | `source_health` per-source precision, freshness, reply yield. Alert on degradation. |
| TASK-39 | Dossier builder | Visual thesis view: company thesis + why-now + buyer committee + top 3 pains + top 3 proofs + do-not-say. API endpoint: `GET /api/sales/accounts/{id}/dossier` |
| TASK-40 | LLM hallucination check | After LLM generates company: HEAD request (200=real), WHOIS domain age. LLM score = 0.4 vs directory = 0.9. |
| TASK-41 | Graph projection (Graphiti) | Event-driven projection from relational core to Graphiti (Python). MCP server mode. Temporal relations, community detection, buyer committee reasoning. |
| TASK-42 | Multi-agent swarm | 3 LLM agents: Researcher (DOM→Markdown→JSON, gpt-4o-mini), Writer (messages, claude-sonnet), Compliance (spam check, length, tone, similarity — if fail, return to Writer). |
| TASK-43 | Dynamic value assets (A-tier) | PDF micro-report only for: A-tier + verified trigger + low risk + operator approval. CTA: "istersen kisa analiz gondereyim". |
| TASK-44 | Tone learning (RLHF) | Operator message edits → store (original, edited, context) triples in SemanticStore. Writer agent uses RAG to mimic operator's tone. |
| TASK-45 | Omnichannel state machine | Day 1: LinkedIn visit (operator-assist) → Day 2: Email → Day 5: If opened no reply → LinkedIn connect → Day 8: Follow-up → Day 12: Closing. Event-driven transitions. |

---

## 7. PHASE 3 — UI/UX Overhaul

> **Timeline guidance:** Days 30-50, parallel with Phase 2.
> **Files:** `static/index_body.html`, `static/js/pages/sales.js`

| Task | Summary | Key Detail |
|------|---------|------------|
| TASK-46 | Tab-based navigation | 4 tabs: Command Center / Profiles / Approval Queue / Delivery. Alpine.js `x-show` + URL hash routing. |
| TASK-47 | Dashboard metrics | Top cards: 5-axis score summary, north star metrics (positive_reply_rate, meeting_rate). Live polling. |
| TASK-48 | Score breakdown component | Horizontal bar chart showing 5 axes per account. Color-coded: green (good), yellow (medium), red (risk). |
| TASK-49 | Bulk approve/reject | Checkbox column + "Approve all Ready" button. Batch API: `POST /api/sales/approvals/bulk-approve` with `{ids: [...]}`. |
| TASK-50 | Inline message editing | Clickable textarea in approval card. On save: `PATCH /api/sales/approvals/{id}/edit` with `{edited_payload: {...}}`. |
| TASK-51 | info@ warning badge | Yellow badge: "Generic email — low reply probability" on any approval with generic/role email classification. |
| TASK-52 | Context-aware action suggestions | Per-account: contact_ready+high → "Send email now". email_only → "Search LinkedIn". company_only → "Research needed". |
| TASK-53 | Quick approval mode (Tinder-style) | Right arrow = Approve, Left = Reject, Up = Rewrite. Keyboard shortcuts. 100 leads in 3 minutes. |
| TASK-54 | Icebreaker editor | Editable first-sentence field (LLM-generated personalized hook). Rest of message fixed. |

---

## 8. PHASE 4 — Scale

> **Timeline guidance:** Days 60-90.

| Task | Summary | Key Detail |
|------|---------|------------|
| TASK-55 | First-party intent + reverse IP | Site visits, demo forms, webinar signups. Reverse IP: identify visitor company, boost IntentScore. |
| TASK-56 | ICP control plane (full UI) | Multiple ICPs, sub-segments, negative rules, persona map, sender policy. CRUD endpoints + dashboard UI. |
| TASK-57 | CRM sync (HubSpot/Pipedrive) | Webhook-based bidirectional sync. Account + contact + activity push. Deal stage pull. |
| TASK-58 | Canonical account dedup | Name similarity (Levenshtein + Jaro-Winkler) + site identity + contact signal + source verification. Merge UI. |
| TASK-59 | Compliance dashboard | KVKK status, suppression management, retention periods, purpose logging, opt-out history timeline. |
| TASK-60 | Operator auto-approve segments | auto-block (high risk), research-needed (generic), manual-review (high value + risk), auto-send (evidence-bound + low risk). |
| TASK-61 | LLM-assisted scraper extraction | HTML → LLM → structured data (instead of regex). Resilient to HTML changes. Confidence-tagged output. |

---

## 9. VERIFICATION PROTOCOL

### After Each Phase

```bash
# 1. Build
cargo build --workspace --lib

# 2. Tests
cargo test --workspace

# 3. Lint
cargo clippy --workspace --all-targets -- -D warnings

# 4. Smoke test
cargo xtask test-smoke
```

### Phase 0 Verification Checklist

- [ ] `is_valid_company_domain("gmail.com")` returns false
- [ ] No consumer domains in pipeline output
- [ ] Turkish placeholders detected: "Başkan'ın Mesajı" → true
- [ ] Phone numbers flow through to leads
- [ ] No duplicate approvals for same recipient
- [ ] No LinkedIn browser automation code remains
- [ ] Pipeline runs as async job, returns job_id immediately
- [ ] Suppression blocks delivery to opted-out recipients
- [ ] Each field has confidence score
- [ ] Empty target_geo → clear error
- [ ] Scraper returning 0 results → auto-skip next run
- [ ] Invalid MX → email blocked
- [ ] Emails go through subdomain, not main domain
- [ ] Unsubscribe header present in all emails

### Phase 1 Verification Checklist

- [ ] All new tables created successfully
- [ ] Migration from legacy tables preserves all data
- [ ] Discovery finds 30+ accounts even with daily_target=5
- [ ] 5-axis scores computed for each account
- [ ] Send Gate correctly routes: Block/Research/Nurture/Activate
- [ ] Outcomes classified and side-effects applied
- [ ] Signals have effect horizons and rationales
- [ ] Thesis created before any activation
- [ ] LinkedIn found for ≥30% of Turkish companies
- [ ] Email patterns guessed with MX verification
- [ ] Turkish site pages enriched
- [ ] Job postings detected as intent signals

### North Star Metrics

| Metric | Current | Phase 0 Target | Phase 1 | Phase 2-3 | Phase 4 |
|--------|---------|----------------|---------|-----------|---------|
| Correct person rate | ? | ? | 50%+ | 70%+ | 80%+ |
| Positive reply rate | ? | ? | ? | 3-5% | 5-8% |
| Meeting booked rate | ? | ? | ? | ~1% | 2%+ |
| Bounce rate | ? | <1% | <0.5% | <0.3% | <0.1% |
| Invalid domain rate | 3% | 0% | 0% | 0% | 0% |
| info@ lead rate | 80% | 0% | 0% | 0% | 0% |
| LinkedIn discovery | 0% | 0% | 30%+ | 30%+ | 35%+ |
| Personal email rate | 0% | 0% | 20%+ | 20%+ | 25%+ |
| Phone discovery | 0% | 50%+ | 50%+ | 50%+ | 50%+ |
| Message personalization | 0% | 0% | 0% | 100% LLM | 100% + A/B |

---

## END OF SPEC

**Priority order:** Fix operational truth first (Phase 0) → Build evidence layer (Phase 1) → Enable learning (Phase 2) → Polish UI (Phase 3) → Scale (Phase 4).

**The 4 mechanisms that make this a customer magnet:**
1. **Right-time signal** — "Why can this account act NOW?"
2. **Real evidence** — "What data supports this claim?"
3. **Low-friction value** — Not "book a demo" but "want a brief analysis?"
4. **Outcome-learning engine** — "Who replied, who didn't, why?"
