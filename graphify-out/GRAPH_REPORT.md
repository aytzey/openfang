# Graph Report - crates/pulsivo-salesman-api (vendor JS excluded)  (2026-04-09)

## Corpus Check
- 33 files · ~87,514 words
- Verdict: corpus is large enough that graph structure adds value.

## Corpus Breakdown
- Code: 33 files
- Docs: 0 files
- Papers: 0 files
- Images: 1 files

## Summary
- 918 nodes · 1625 edges · 28 communities detected
- Extraction: 55% EXTRACTED · 45% INFERRED · 0% AMBIGUOUS · INFERRED: 731 edges (avg confidence: 0.5)
- Token cost: 0 input · 0 output

## Topology
- Connected components: 26
- Largest component: 160 nodes
- Average degree: 3.54
- Density: 0.0039

## God Nodes (most connected - your core abstractions)
1. `SalesEngine` - 98 edges
2. `build_prospect_profiles()` - 15 edges
3. `codex_oauth_status()` - 13 edges
4. `extract_contact_from_company_site_html()` - 13 edges
5. `dedupe_strings()` - 13 edges
6. `expected_source_counts_for_profile()` - 13 edges
7. `llm_candidate_relevance_prompt_context()` - 13 edges
8. `build_candidate_prospect_profiles()` - 12 edges
9. `seed_contains_any()` - 12 edges
10. `ensure_access_token_for_auth()` - 11 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Hotspot Files
- `crates/pulsivo-salesman-api/src/sales/enrichment.rs` — 95 real nodes · 353 cumulative connections
- `crates/pulsivo-salesman-api/src/codex_oauth.rs` — 67 real nodes · 276 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/prospects.rs` — 54 real nodes · 238 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/strategy.rs` — 38 real nodes · 162 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/shared.rs` — 59 real nodes · 104 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/onboarding.rs` — 36 real nodes · 100 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/engine.rs` — 1 real nodes · 98 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/directories.rs` — 41 real nodes · 85 cumulative connections
- `crates/pulsivo-salesman-api/src/sales/discovery.rs` — 21 real nodes · 69 cumulative connections
- `crates/pulsivo-salesman-api/ui/src/core/api.js` — 16 real nodes · 44 cumulative connections

## Hotspot Directories
- `crates` — 492 real nodes · 1685 cumulative connections

## Bridge Nodes
- `SalesEngine` — betweenness 0.0085 · `crates/pulsivo-salesman-api/src/sales/engine.rs`
- `InstagramScraperAdapter` — betweenness 0.0012 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `TikTokScraperAdapter` — betweenness 0.0012 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `ListUnsubscribeHeader` — betweenness 0.0006 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `SalesSegment` — betweenness 0.0006 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `ListUnsubscribePostHeader` — betweenness 0.0006 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `EnvGuard` — betweenness 0.0005 · `crates/pulsivo-salesman-api/src/codex_oauth.rs`
- `PipelineStage` — betweenness 0.0004 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `SenderConfig` — betweenness 0.0004 · `crates/pulsivo-salesman-api/src/sales/shared.rs`
- `MailboxConfig` — betweenness 0.0004 · `crates/pulsivo-salesman-api/src/sales/shared.rs`

## Community Bridges
- `codex_oauth` ↔ `http + middleware` — 1 cross-community edges
- `http + middleware` ↔ `routes + state` — 1 cross-community edges

## Communities

### Community 0 - "tests"
Cohesion: 0.01
Nodes (0): 

### Community 1 - "enrichment"
Cohesion: 0.03
Nodes (95): absolutize_source_link(), assess_domain_health(), assign_experiment_variant(), best_site_contact_enrichment(), build_company_osint_queries(), calibrate_scoring_from_outcomes(), canonical_contact_key(), canonicalize_osint_url() (+87 more)

### Community 2 - "engine"
Cohesion: 0.06
Nodes (1): SalesEngine

### Community 3 - "shared"
Cohesion: 0.03
Nodes (59): ActivationLeadCandidate, B2cDiscoveryCandidate, CandidateCheckpoint, CanonicalAccountSync, default_daily_send_cap(), default_daily_target(), default_mailbox_daily_cap(), default_mailbox_pool_from_profile() (+51 more)

### Community 4 - "codex_oauth"
Cohesion: 0.07
Nodes (67): apply_codex_auth_to_runtime(), auth_account_id(), auth_client_id(), auth_file(), auth_is_expired_or_expiring_soon(), base64_url_encode(), cleanup_stale_pkce(), clear_codex_auth_from_runtime() (+59 more)

### Community 5 - "http + middleware"
Cohesion: 0.04
Nodes (24): approve_and_send(), bulk_approve_sales_approvals(), OptionalRow, always_public_unsubscribe_bypasses_loopback_restriction(), auth(), is_always_public_path(), is_dashboard_public_path(), is_loopback_request() (+16 more)

### Community 6 - "prospects"
Cohesion: 0.09
Nodes (54): account_reachability_penalty(), apply_source_contact_hint(), build_candidate_prospect_profiles(), build_prospect_buyer_roles(), build_prospect_outreach_angle(), build_prospect_pain_points(), build_prospect_profiles(), build_prospect_summary() (+46 more)

### Community 7 - "strategy"
Cohesion: 0.11
Nodes (38): best_search_contact_enrichment(), best_search_contact_evidence(), build_company_search_aliases(), build_sales_email_body(), build_sales_email_subject(), build_sales_linkedin_message(), collect_osint_links_from_search_outputs(), expected_source_counts_for_profile() (+30 more)

### Community 8 - "onboarding"
Cohesion: 0.08
Nodes (36): apply_better_site_identity_candidate(), apply_brief_to_profile(), apply_search_contact_enrichment(), apply_search_osint_to_profile(), apply_site_contact_enrichment(), apply_site_osint_to_profile(), brief_summary(), build_onboarding_status() (+28 more)

### Community 9 - "directories"
Cohesion: 0.07
Nodes (41): fetch_ared_member_candidates(), fetch_asmud_member_candidates(), fetch_eder_member_candidates(), fetch_emsad_member_candidates(), fetch_eud_member_candidates(), fetch_ida_member_candidates(), fetch_imder_member_candidates(), fetch_isder_member_candidates() (+33 more)

### Community 10 - "discovery"
Cohesion: 0.15
Nodes (21): adaptive_discovery_focus_terms(), adaptive_discovery_retry_threshold(), apply_b2c_local_market_search_output(), b2c_candidate_from_search_entry(), b2c_geo_search_terms(), b2c_local_market_markers(), b2c_relevance_terms(), build_adaptive_discovery_queries() (+13 more)

### Community 11 - "api"
Cohesion: 0.15
Nodes (16): del(), dismissToast(), error(), friendlyError(), get(), getContainer(), headers(), info() (+8 more)

### Community 12 - "llm"
Cohesion: 0.14
Nodes (16): apply_cached_prospect_memory(), apply_cached_prospect_research(), base64_url_decode(), build_sales_llm_driver(), llm_autofill_profile(), llm_build_lead_query_plan(), llm_enrich_prospect_profiles(), llm_generate_company_candidates() (+8 more)

### Community 13 - "b2c"
Cohesion: 0.36
Nodes (8): asArray(), cleanSignal(), extractSignalValue(), firstSentence(), nonEmpty(), platformKeyFromUrl(), topKeywords(), truncateText()

### Community 14 - "routes + state"
Cohesion: 0.27
Nodes (7): AppState, health(), health_detail(), health_probe_agent_id(), prometheus_metrics(), createSalesState(), defaultExperimentForm()

### Community 15 - "build"
Cohesion: 0.7
Nodes (4): bun_exists(), find_bun(), main(), run_bun()

### Community 16 - "ops"
Cohesion: 0.67
Nodes (2): nonEmpty(), truncateText()

### Community 17 - "index"
Cohesion: 1.0
Nodes (2): applyMixin(), createSalesPage()

### Community 18 - "http"
Cohesion: 1.0
Nodes (1): Result<T, rusqlite::Error>

### Community 19 - "app-shell"
Cohesion: 1.0
Nodes (0): 

### Community 20 - "lib"
Cohesion: 1.0
Nodes (0): 

### Community 21 - "engine"
Cohesion: 1.0
Nodes (0): 

### Community 22 - "approvals"
Cohesion: 1.0
Nodes (0): 

### Community 23 - "data"
Cohesion: 1.0
Nodes (0): 

### Community 24 - "formatters"
Cohesion: 1.0
Nodes (0): 

### Community 25 - "jobs"
Cohesion: 1.0
Nodes (0): 

### Community 26 - "oauth"
Cohesion: 1.0
Nodes (0): 

### Community 27 - "webchat-entry"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **67 isolated node(s):** `PendingPkce`, `LoopbackCallbackServer`, `LoopbackCallbackTarget`, `StoredCodexAuth`, `TokenResponse` (+62 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `http`** (2 nodes): `Result<T, rusqlite::Error>`, `.optional()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `app-shell`** (2 nodes): `app-shell.js`, `createApp()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `lib`** (1 nodes): `lib.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `engine`** (1 nodes): `engine.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `approvals`** (1 nodes): `approvals.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `data`** (1 nodes): `data.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `formatters`** (1 nodes): `formatters.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `jobs`** (1 nodes): `jobs.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `oauth`** (1 nodes): `oauth.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `webchat-entry`** (1 nodes): `webchat-entry.js`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Are the 14 inferred relationships involving `build_prospect_profiles()` (e.g. with `prospect_contact_score()` and `dedupe_strings()`) actually correct?**
  _`build_prospect_profiles()` has 14 INFERRED edges - model-reasoned connections that need verification._
- **Are the 12 inferred relationships involving `codex_oauth_status()` (e.g. with `has_codex_cli_auth()` and `logout_marker_exists()`) actually correct?**
  _`codex_oauth_status()` has 12 INFERRED edges - model-reasoned connections that need verification._
- **Are the 12 inferred relationships involving `extract_contact_from_company_site_html()` (e.g. with `best_site_contact_enrichment()` and `strip_html_tags()`) actually correct?**
  _`extract_contact_from_company_site_html()` has 12 INFERRED edges - model-reasoned connections that need verification._
- **Are the 12 inferred relationships involving `dedupe_strings()` (e.g. with `build_prospect_profiles()` and `build_candidate_prospect_profiles()`) actually correct?**
  _`dedupe_strings()` has 12 INFERRED edges - model-reasoned connections that need verification._
- **What connects `PendingPkce`, `LoopbackCallbackServer`, `LoopbackCallbackTarget` to the rest of the system?**
  _67 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `tests` be split into smaller, more focused modules?**
  _Cohesion score 0.01 - nodes in this community are weakly interconnected._
- **Should `enrichment` be split into smaller, more focused modules?**
  _Cohesion score 0.03 - nodes in this community are weakly interconnected._