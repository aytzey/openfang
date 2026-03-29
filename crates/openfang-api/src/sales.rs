//! Sales engine API and persistence.
//!
//! Focused prospecting workflow:
//! 1. Persist ICP/product profile
//! 2. Discover candidate customer accounts from public sources
//! 3. Build persistent prospect dossiers with deterministic memory reuse
//! 4. Upgrade the best dossiers into outreach-ready leads + approval drafts
//! 5. Send on manual approval (email + LinkedIn operator assist)

use crate::codex_oauth::StoredCodexAuth;
use crate::routes::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{Local, Timelike, Utc};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use lettre::message::header::{Header, HeaderName, HeaderValue};
use lettre::message::{Mailbox, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use openfang_runtime::llm_driver::{CompletionRequest, DriverConfig};
use openfang_runtime::web_cache::WebCache;
use openfang_runtime::web_search::WebSearchEngine;
use openfang_types::agent::ReasoningEffort;
use openfang_types::config::SearchProvider;
use openfang_types::message::Message as LlmMessage;
use rusqlite::{params, Connection};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const DEFAULT_LIMIT: usize = 100;
const MIN_DOMAIN_RELEVANCE_SCORE: i32 = 5;
const MAX_DISCOVERY_QUERIES: usize = 10;
const MAX_ADAPTIVE_DISCOVERY_QUERIES: usize = 6;
const MAX_DISCOVERY_FAILURES_BEFORE_FAST_FALLBACK: u32 = MAX_DISCOVERY_QUERIES as u32;
const NO_BRAVE_FAIL_FAST_THRESHOLD: u32 = 1;
const MAX_DIRECT_ENRICH_ATTEMPTS: usize = 12;
const MAX_GENERIC_DIRECT_ENRICH_RETRIES: usize = 6;
const MAX_WEB_CONTACT_SEARCH_ATTEMPTS: usize = 12;
const DIRECT_ENRICH_TIMEOUT_MS: u64 = 3500;
const MAX_EXTRA_SITE_ENRICH_PAGES: usize = 3;
const MAX_PREFETCH_RETRY_CANDIDATES: usize = 3;
const MAX_PREFETCH_SITE_CANDIDATES: usize = 6;
const SITE_PAGE_FETCH_TIMEOUT_MS: u64 = 1600;
const FREE_DIRECTORY_FETCH_TIMEOUT_MS: u64 = 3200;
const MAX_FREE_DIRECTORY_CANDIDATES: usize = 30;
const MAX_TMB_DIRECTORY_CANDIDATES: usize = 8;
const MAX_EUD_DIRECTORY_CANDIDATES: usize = 6;
const MAX_ASMUD_DIRECTORY_CANDIDATES: usize = 10;
const MAX_PLATFORMDER_DIRECTORY_CANDIDATES: usize = 10;
const MAX_MIB_DIRECTORY_CANDIDATES: usize = 10;
const MAX_IMDER_DIRECTORY_CANDIDATES: usize = 8;
const MAX_ISDER_DIRECTORY_CANDIDATES: usize = 8;
const MAX_THBB_DIRECTORY_CANDIDATES: usize = 8;
const MAX_EDER_DIRECTORY_CANDIDATES: usize = 8;
const MAX_LOJIDER_DIRECTORY_CANDIDATES: usize = 10;
const MAX_TFYD_DIRECTORY_CANDIDATES: usize = 10;
const MAX_OSS_DIRECTORY_CANDIDATES: usize = 10;
const MAX_IDA_DIRECTORY_CANDIDATES: usize = 10;
const MAX_TESID_DIRECTORY_CANDIDATES: usize = 12;
const MAX_TUDIS_DIRECTORY_CANDIDATES: usize = 10;
const MAX_EMSAD_DIRECTORY_CANDIDATES: usize = 10;
const MAX_TGSD_DIRECTORY_CANDIDATES: usize = 12;
const MAX_ARED_DIRECTORY_CANDIDATES: usize = 10;
const MAX_TODEB_DIRECTORY_CANDIDATES: usize = 10;
const MAX_IMDER_DETAIL_FETCHES: usize = 12;
const MAX_ISDER_DETAIL_FETCHES: usize = 12;
const MIB_DIRECTORY_PAGE_COUNT: usize = 9;
const MIB_DIRECTORY_PAGES_PER_RUN: usize = 3;
const LEAD_QUERY_PLAN_TIMEOUT_SECS: u64 = 4;
const LLM_COMPANY_GENERATION_TIMEOUT_SECS: u64 = 10;
const LLM_RELEVANCE_VALIDATION_TIMEOUT_SECS: u64 = 6;
const LLM_RELEVANCE_VALIDATION_BATCH_SIZE: usize = 40;
const MAX_LLM_PRIMARY_CANDIDATES: usize = 12;
const SALES_DISCOVERY_SEARCH_TIMEOUT_SECS: u64 = 8;
const SALES_CONTACT_SEARCH_TIMEOUT_SECS: u64 = 4;
const SALES_OSINT_SEARCH_TIMEOUT_SECS: u64 = 5;
const SALES_SEARCH_BATCH_CONCURRENCY: usize = 3;
const SALES_OSINT_PROFILE_CONCURRENCY: usize = 4;
const PROSPECT_LLM_ENRICH_TIMEOUT_SECS: u64 = 18;
const MAX_OSINT_LINKS_PER_PROSPECT: usize = 6;
const MAX_OSINT_SEARCH_TARGETS: usize = 24;
const DISCOVERY_RESERVOIR_CANDIDATES: usize = 90;
const DISCOVERY_PROSPECT_SEED_LIMIT: usize = 160;
const DISCOVERY_REFRESH_SCAN_LIMIT: usize = 240;
const DISCOVERY_OSINT_TARGET_LIMIT: usize = MAX_OSINT_SEARCH_TARGETS;
const ACTIVATION_EXPLOIT_RATIO: f64 = 0.87;
const SALES_RUN_REQUEST_TIMEOUT_SECS: u64 = 240;
const SALES_RUN_RECOVERY_STALE_SECS: i64 = SALES_RUN_REQUEST_TIMEOUT_SECS as i64 + 15;
const SALES_LLM_PROVIDER: &str = "openai-codex";
const SALES_LLM_MODEL: &str = "gpt-5.3-codex";
const DEFAULT_SALES_BASE_URL: &str = "http://127.0.0.1:4200";
const SALES_UNSUBSCRIBE_SALT: &str = "openfang-sales-unsubscribe";

#[derive(Debug, Clone)]
struct ListUnsubscribeHeader(String);

impl Header for ListUnsubscribeHeader {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("List-Unsubscribe")
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(s.to_string()))
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::new(Self::name(), self.0.clone())
    }
}

#[derive(Debug, Clone)]
struct ListUnsubscribePostHeader(String);

impl Header for ListUnsubscribePostHeader {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("List-Unsubscribe-Post")
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(s.to_string()))
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::new(Self::name(), self.0.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesProfile {
    pub product_name: String,
    pub product_description: String,
    pub target_industry: String,
    pub target_geo: String,
    pub sender_name: String,
    pub sender_email: String,
    pub sender_linkedin: Option<String>,
    #[serde(default = "default_target_title_policy")]
    pub target_title_policy: String,
    #[serde(default = "default_daily_target")]
    pub daily_target: u32,
    #[serde(default = "default_daily_send_cap")]
    pub daily_send_cap: u32,
    #[serde(default = "default_schedule_hour")]
    pub schedule_hour_local: u8,
    #[serde(default = "default_timezone_mode")]
    pub timezone_mode: String,
}

fn default_target_title_policy() -> String {
    "ceo_then_founder".to_string()
}

fn default_daily_target() -> u32 {
    20
}

fn default_daily_send_cap() -> u32 {
    20
}

fn default_schedule_hour() -> u8 {
    9
}

fn default_timezone_mode() -> String {
    "local".to_string()
}

impl Default for SalesProfile {
    fn default() -> Self {
        Self {
            product_name: String::new(),
            product_description: String::new(),
            target_industry: String::new(),
            target_geo: String::new(),
            sender_name: String::new(),
            sender_email: String::new(),
            sender_linkedin: None,
            target_title_policy: default_target_title_policy(),
            daily_target: default_daily_target(),
            daily_send_cap: default_daily_send_cap(),
            schedule_hour_local: default_schedule_hour(),
            timezone_mode: default_timezone_mode(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesRunRecord {
    pub id: String,
    pub status: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub discovered: u32,
    pub inserted: u32,
    pub approvals_queued: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesLead {
    pub id: String,
    pub run_id: String,
    pub company: String,
    pub website: String,
    pub company_domain: String,
    pub contact_name: String,
    pub contact_title: String,
    pub linkedin_url: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub reasons: Vec<String>,
    pub email_subject: String,
    pub email_body: String,
    pub linkedin_message: String,
    pub score: i32,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SalesProspectProfile {
    pub id: String,
    pub run_id: String,
    pub company: String,
    pub website: String,
    pub company_domain: String,
    pub fit_score: i32,
    pub profile_status: String,
    pub summary: String,
    pub matched_signals: Vec<String>,
    pub primary_contact_name: Option<String>,
    pub primary_contact_title: Option<String>,
    pub primary_email: Option<String>,
    pub primary_linkedin_url: Option<String>,
    pub company_linkedin_url: Option<String>,
    #[serde(default)]
    pub osint_links: Vec<String>,
    pub contact_count: u32,
    pub source_count: u32,
    pub buyer_roles: Vec<String>,
    pub pain_points: Vec<String>,
    pub trigger_events: Vec<String>,
    pub recommended_channel: String,
    pub outreach_angle: String,
    pub research_status: String,
    pub research_confidence: f32,
    #[serde(default)]
    pub tech_stack: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesApproval {
    pub id: String,
    pub lead_id: String,
    pub channel: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub created_at: String,
    pub decided_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesDelivery {
    pub id: String,
    pub approval_id: String,
    pub channel: String,
    pub recipient: String,
    pub status: String,
    pub error: Option<String>,
    pub sent_at: String,
}

#[derive(Debug, Clone, Default)]
struct SearchEntry {
    title: String,
    url: String,
    snippet: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DomainCandidate {
    domain: String,
    score: i32,
    evidence: Vec<String>,
    matched_keywords: Vec<String>,
    source_links: Vec<String>,
    #[serde(default)]
    phone: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SourceContactHint {
    contact_name: Option<String>,
    contact_title: Option<String>,
    email: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FreeDiscoveryCandidate {
    candidate: DomainCandidate,
    contact_hint: SourceContactHint,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum PipelineStage {
    QueryPlanning,
    Discovery,
    Merging,
    Validation,
    Filtering,
    Enrichment,
    LeadGeneration,
}

impl PipelineStage {
    fn as_str(&self) -> &'static str {
        match self {
            Self::QueryPlanning => "QueryPlanning",
            Self::Discovery => "Discovery",
            Self::Merging => "Merging",
            Self::Validation => "Validation",
            Self::Filtering => "Filtering",
            Self::Enrichment => "Enrichment",
            Self::LeadGeneration => "LeadGeneration",
        }
    }

    fn ordered() -> &'static [PipelineStage] {
        &[
            Self::QueryPlanning,
            Self::Discovery,
            Self::Merging,
            Self::Validation,
            Self::Filtering,
            Self::Enrichment,
            Self::LeadGeneration,
        ]
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DiscoveryCheckpoint {
    #[serde(default)]
    lead_plan: LeadQueryPlanDraft,
    #[serde(default)]
    llm_candidates: Vec<DomainCandidate>,
    #[serde(default)]
    web_candidates: Vec<DomainCandidate>,
    #[serde(default)]
    free_candidates: Vec<FreeDiscoveryCandidate>,
    #[serde(default)]
    source_contact_hints: HashMap<String, SourceContactHint>,
    #[serde(default)]
    search_unavailable: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CandidateCheckpoint {
    #[serde(default)]
    lead_plan: LeadQueryPlanDraft,
    #[serde(default)]
    candidate_list: Vec<DomainCandidate>,
    #[serde(default)]
    source_contact_hints: HashMap<String, SourceContactHint>,
    #[serde(default)]
    search_unavailable: bool,
    #[serde(default)]
    llm_validated_domains: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobRetryRequest {
    #[serde(default)]
    force_fresh: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct JobStageStatus {
    name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct JobProgressResponse {
    job_id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_stage: Option<String>,
    #[serde(default)]
    stages: Vec<JobStageStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LeadGenerationCheckpoint {
    total_candidates: usize,
    processed_candidates: usize,
    profiled_accounts: usize,
    inserted: u32,
    approvals_queued: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_domain: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SourceHealthRow {
    id: String,
    source_type: String,
    precision: Option<f64>,
    freshness: Option<String>,
    parser_health: f64,
    legal_mode: String,
    historical_reply_yield: Option<f64>,
    last_checked_at: Option<String>,
    auto_skip: bool,
}

// --- Mailbox Pool / Sender Config (TASK-13) ---

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MailboxConfig {
    /// Sending email address (e.g. "outreach1@send.machinity.com")
    email: String,
    /// SMTP host (falls back to global EmailConfig if empty)
    #[serde(default)]
    smtp_host: String,
    /// SMTP port (falls back to global EmailConfig if 0)
    #[serde(default)]
    smtp_port: u16,
    /// SMTP username (falls back to email if empty)
    #[serde(default)]
    smtp_user: String,
    /// Env var name holding SMTP password
    #[serde(default)]
    smtp_pass_env: String,
    /// Per-mailbox daily send cap
    #[serde(default = "default_mailbox_daily_cap")]
    daily_cap: u32,
    /// Warm-up state: cold | warming | warm | hot
    #[serde(default = "default_warm_state")]
    warm_state: String,
    /// Runtime counter — sends done today (not persisted to DB)
    #[serde(default)]
    sends_today: u32,
    /// Date of the counter (YYYY-MM-DD)
    #[serde(default)]
    counter_date: String,
}

fn default_mailbox_daily_cap() -> u32 {
    20
}
fn default_warm_state() -> String {
    "cold".to_string()
}

impl MailboxConfig {
    /// Effective daily cap based on warm-up state.
    /// cold=0 (cannot send), warming ramps: 5→10→15→cap, warm/hot=cap.
    fn effective_cap(&self) -> u32 {
        match self.warm_state.as_str() {
            "cold" => 0,
            "warming" => self.daily_cap.min(15),
            "warm" => self.daily_cap,
            "hot" => self.daily_cap,
            _ => 0,
        }
    }

    fn can_send(&self) -> bool {
        self.warm_state != "cold" && self.sends_today < self.effective_cap()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SenderConfig {
    /// Pool of sending mailboxes; round-robin across non-exhausted ones
    #[serde(default)]
    mailboxes: Vec<MailboxConfig>,
}

impl SenderConfig {
    /// Select the best available mailbox: non-cold, with remaining capacity,
    /// and lowest sends_today (spread load evenly).
    fn select_mailbox(&mut self) -> Option<&mut MailboxConfig> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        // Reset counters if date changed
        for mb in &mut self.mailboxes {
            if mb.counter_date != today {
                mb.sends_today = 0;
                mb.counter_date = today.clone();
            }
        }
        self.mailboxes
            .iter_mut()
            .filter(|m| m.can_send())
            .min_by_key(|m| m.sends_today)
    }

    /// Total remaining capacity across all mailboxes
    #[allow(dead_code)]
    fn remaining_capacity(&self) -> u32 {
        self.mailboxes
            .iter()
            .filter(|m| m.warm_state != "cold")
            .map(|m| m.effective_cap().saturating_sub(m.sends_today))
            .sum()
    }
}

fn normalize_mailbox_address(email: &str) -> String {
    email.trim().to_lowercase()
}

fn normalize_mailbox_config(mut mailbox: MailboxConfig) -> Option<MailboxConfig> {
    mailbox.email = normalize_mailbox_address(&mailbox.email);
    if mailbox.email.is_empty() {
        return None;
    }
    if mailbox.daily_cap == 0 {
        mailbox.daily_cap = default_mailbox_daily_cap();
    }
    if mailbox.warm_state.trim().is_empty() {
        mailbox.warm_state = default_warm_state();
    }
    Some(mailbox)
}

fn mailbox_pool_from_json(pool_json: &str) -> Vec<MailboxConfig> {
    let value: serde_json::Value =
        serde_json::from_str(pool_json).unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
    let serde_json::Value::Array(items) = value else {
        return Vec::new();
    };

    items
        .into_iter()
        .filter_map(|item| match item {
            serde_json::Value::String(email) => normalize_mailbox_config(MailboxConfig {
                email,
                warm_state: "warming".to_string(),
                ..MailboxConfig::default()
            }),
            serde_json::Value::Object(_) => serde_json::from_value::<MailboxConfig>(item)
                .ok()
                .and_then(normalize_mailbox_config),
            _ => None,
        })
        .collect()
}

fn default_mailbox_pool_from_profile(profile: &SalesProfile) -> Vec<MailboxConfig> {
    normalize_mailbox_config(MailboxConfig {
        email: profile.sender_email.clone(),
        daily_cap: profile.daily_send_cap.max(1),
        warm_state: "warming".to_string(),
        ..MailboxConfig::default()
    })
    .into_iter()
    .collect()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EmailValidation {
    email: String,
    syntax_valid: bool,
    mx_valid: bool,
    domain_health: f64,
    suppressed: bool,
    classification: String,
    safe_to_send: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FiveAxisScore {
    fit_score: f64,
    intent_score: f64,
    reachability_score: f64,
    deliverability_risk: f64,
    compliance_risk: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum SendGateDecision {
    Block { reason: String },
    Research { missing: Vec<String> },
    Nurture { reason: String },
    Activate,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutcomeWebhookRequest {
    delivery_id: String,
    event_type: String,
    #[serde(default)]
    raw_text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OutcomeRecord {
    touch_id: String,
    outcome_type: String,
    raw_text: String,
    classifier_confidence: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UnsubscribeQuery {
    token: String,
}

#[derive(Debug, Clone)]
struct CanonicalAccountSync {
    score: FiveAxisScore,
    gate: SendGateDecision,
}

#[derive(Debug, Clone, Default)]
struct SiteContactEnrichment {
    name: Option<String>,
    title: Option<String>,
    linkedin_url: Option<String>,
    company_linkedin_url: Option<String>,
    email: Option<String>,
    evidence: Option<String>,
    osint_links: Vec<String>,
    signal: i32,
    tech_stack: Vec<String>,
    job_posting_signals: Vec<(String, String, f64, String)>, // (text, url, confidence, type)
}

#[derive(Debug, Clone, Default)]
struct SiteHtmlPage {
    url: String,
    html: String,
}

#[derive(Debug, Clone, Default)]
struct SiteFetchBundle {
    pages: Vec<SiteHtmlPage>,
    osint_links: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct OnboardingBriefState {
    brief: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ProspectAccumulator {
    run_id: String,
    company: String,
    website: String,
    company_domain: String,
    fit_score: i32,
    matched_signals: Vec<String>,
    signal_set: HashSet<String>,
    primary_contact_name: Option<String>,
    primary_contact_title: Option<String>,
    primary_email: Option<String>,
    primary_linkedin_url: Option<String>,
    company_linkedin_url: Option<String>,
    osint_links: Vec<String>,
    osint_link_set: HashSet<String>,
    primary_contact_score: i32,
    contact_keys: HashSet<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct SalesLeadQuery {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub run_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SalesApprovalQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SalesApprovalBulkApproveRequest {
    #[serde(default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SalesPolicyProposalQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SalesApprovalEditRequest {
    #[serde(default)]
    pub edited_payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesPolicyProposal {
    pub id: String,
    pub rule_type: String,
    pub rule_key: String,
    pub old_value: Option<String>,
    pub new_value: String,
    pub proposal_source: Option<String>,
    pub backtest_result_json: Option<String>,
    pub holdout_result_json: Option<String>,
    pub status: String,
    pub approved_by: Option<String>,
    pub activated_at: Option<String>,
    pub version: i64,
    pub created_at: String,
}

#[derive(Debug, Clone)]
struct ActivationLeadCandidate {
    account_id: String,
    priority: i64,
    lead: SalesLead,
}

pub struct SalesEngine {
    db_path: PathBuf,
}

impl SalesEngine {
    pub fn new(home_dir: &FsPath) -> Self {
        Self {
            db_path: home_dir.join("sales.db"),
        }
    }

    fn open(&self) -> Result<Connection, String> {
        Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to open sales db {}: {e}", self.db_path.display()))
    }

    pub fn init(&self) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sales_profile (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_runs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                discovered INTEGER NOT NULL DEFAULT 0,
                inserted INTEGER NOT NULL DEFAULT 0,
                approvals_queued INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS leads (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                company TEXT NOT NULL,
                website TEXT NOT NULL,
                company_domain TEXT NOT NULL,
                contact_name TEXT NOT NULL,
                contact_title TEXT NOT NULL,
                linkedin_url TEXT,
                email TEXT,
                phone TEXT,
                reasons_json TEXT NOT NULL,
                email_subject TEXT NOT NULL,
                email_body TEXT NOT NULL,
                linkedin_message TEXT NOT NULL,
                score INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(company_domain, contact_name, contact_title)
            );

            CREATE TABLE IF NOT EXISTS approvals (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                decided_at TEXT
            );

            CREATE TABLE IF NOT EXISTS deliveries (
                id TEXT PRIMARY KEY,
                approval_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                recipient TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                sent_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_onboarding (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                brief_text TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS discovered_domains (
                domain TEXT PRIMARY KEY,
                first_seen_run_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prospect_profiles (
                company_domain TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS artifacts (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT,
                raw_content TEXT,
                parse_status TEXT NOT NULL DEFAULT 'ok',
                parser_health REAL DEFAULT 1.0,
                freshness TEXT,
                legal_mode TEXT DEFAULT 'public',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS evidence (
                id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL REFERENCES artifacts(id),
                field_type TEXT NOT NULL,
                field_value TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.5,
                extraction_method TEXT,
                verified_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                display_name TEXT,
                legal_name TEXT,
                sector TEXT,
                geo TEXT,
                employee_estimate INTEGER,
                website TEXT,
                tier TEXT DEFAULT 'standard',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS account_aliases (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                alias_name TEXT NOT NULL,
                alias_type TEXT NOT NULL
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
                seniority TEXT,
                department TEXT,
                name_confidence REAL DEFAULT 0.5,
                title_confidence REAL DEFAULT 0.5,
                is_decision_maker INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS contact_methods (
                id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                channel_type TEXT NOT NULL,
                value TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                verified_at TEXT,
                classification TEXT,
                suppressed INTEGER DEFAULT 0,
                UNIQUE(contact_id, channel_type, value)
            );

            CREATE TABLE IF NOT EXISTS buyer_roles (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                contact_id TEXT REFERENCES contacts(id),
                role_type TEXT NOT NULL,
                inferred_from TEXT
            );

            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                signal_type TEXT NOT NULL,
                text TEXT NOT NULL,
                source TEXT,
                observed_at TEXT,
                confidence REAL DEFAULT 0.5,
                effect_horizon TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS signal_rationales (
                id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL REFERENCES signals(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                why_it_matters TEXT NOT NULL,
                expected_effect TEXT,
                evidence_ids TEXT,
                confidence REAL DEFAULT 0.5,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                validated_at TEXT,
                validation_result TEXT
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

            CREATE TABLE IF NOT EXISTS research_queue (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                priority INTEGER DEFAULT 0,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                assigned_at TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS account_theses (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                why_this_account TEXT NOT NULL,
                why_now TEXT,
                buyer_committee_json TEXT,
                evidence_ids TEXT,
                do_not_say TEXT,
                recommended_channel TEXT,
                recommended_pain TEXT,
                thesis_confidence REAL DEFAULT 0.0,
                thesis_status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS activation_queue (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                contact_id TEXT REFERENCES contacts(id),
                thesis_id TEXT REFERENCES account_theses(id),
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sequence_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                steps_json TEXT NOT NULL,
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
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS touches (
                id TEXT PRIMARY KEY,
                sequence_instance_id TEXT REFERENCES sequence_instances(id),
                step INTEGER NOT NULL,
                channel TEXT NOT NULL,
                message_payload TEXT NOT NULL,
                claims_json TEXT,
                evidence_ids TEXT,
                variant_id TEXT,
                risk_flags TEXT,
                sent_at TEXT,
                mailbox_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS outcomes (
                id TEXT PRIMARY KEY,
                touch_id TEXT NOT NULL REFERENCES touches(id),
                outcome_type TEXT NOT NULL,
                raw_text TEXT,
                classified_at TEXT NOT NULL DEFAULT (datetime('now')),
                classifier_confidence REAL DEFAULT 1.0
            );

            CREATE TABLE IF NOT EXISTS outcome_attribution_snapshots (
                id TEXT PRIMARY KEY,
                touch_id TEXT NOT NULL REFERENCES touches(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                snapshot_at TEXT NOT NULL DEFAULT (datetime('now')),
                score_at_touch_json TEXT,
                active_signal_ids TEXT,
                unused_signal_ids TEXT,
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
                validated_signals TEXT,
                false_positive_signals TEXT,
                missed_signals TEXT,
                timing_mistakes TEXT,
                persona_mismatch TEXT,
                channel_mismatch TEXT,
                reviewer_type TEXT DEFAULT 'auto'
            );

            CREATE TABLE IF NOT EXISTS retrieval_rule_versions (
                id TEXT PRIMARY KEY,
                rule_type TEXT NOT NULL,
                rule_key TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT NOT NULL,
                proposal_source TEXT,
                backtest_result_json TEXT,
                holdout_result_json TEXT,
                status TEXT NOT NULL DEFAULT 'proposed',
                approved_by TEXT,
                activated_at TEXT,
                version INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS contextual_factors (
                id TEXT PRIMARY KEY,
                factor_type TEXT NOT NULL,
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
                exploration_type TEXT,
                outcome_id TEXT,
                learned_pattern TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS suppressions (
                id TEXT PRIMARY KEY,
                contact_method_value TEXT NOT NULL,
                reason TEXT NOT NULL,
                source_outcome_id TEXT,
                suppressed_at TEXT NOT NULL DEFAULT (datetime('now')),
                permanent INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                hypothesis TEXT,
                variant_a TEXT,
                variant_b TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS experiment_assignments (
                id TEXT PRIMARY KEY,
                experiment_id TEXT NOT NULL REFERENCES experiments(id),
                sequence_instance_id TEXT,
                variant TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_health (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL UNIQUE,
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
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                completed_at TEXT,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS job_stages (
                id TEXT PRIMARY KEY,
                job_run_id TEXT NOT NULL REFERENCES job_runs(id),
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                checkpoint_data TEXT,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS icp_definitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sector_rules TEXT,
                geo_rules TEXT,
                size_rules TEXT,
                negative_rules TEXT,
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
                pain_angles TEXT,
                message_strategy TEXT
            );

            CREATE TABLE IF NOT EXISTS sender_policies (
                id TEXT PRIMARY KEY,
                icp_id TEXT REFERENCES icp_definitions(id),
                mailbox_pool TEXT,
                daily_cap INTEGER DEFAULT 20,
                subdomain TEXT,
                warm_state TEXT DEFAULT 'cold'
            );

            CREATE INDEX IF NOT EXISTS idx_approvals_status_created ON approvals(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_deliveries_sent ON deliveries(sent_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_run_updated ON prospect_profiles(run_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_updated ON prospect_profiles(updated_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_approvals_pending_recipient
                ON approvals(channel, json_extract(payload_json, '$.to'))
                WHERE status = 'pending';
            CREATE UNIQUE INDEX IF NOT EXISTS idx_suppressions_value
                ON suppressions(contact_method_value);
        "#,
        )
        .map_err(|e| format!("Failed to initialize sales db: {e}"))?;
        self.migrate_legacy_to_canonical_core()?;
        seed_contextual_factors(&conn);
        Ok(())
    }

    pub fn get_profile(&self) -> Result<Option<SalesProfile>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare("SELECT json FROM sales_profile WHERE id = 1")
            .map_err(|e| format!("Profile query prepare failed: {e}"))?;
        let row = stmt
            .query_row([], |r| r.get::<_, String>(0))
            .optional()
            .map_err(|e| format!("Profile query failed: {e}"))?;

        row.map(|json| {
            serde_json::from_str::<SalesProfile>(&json)
                .map_err(|e| format!("Invalid profile JSON in DB: {e}"))
        })
        .transpose()
    }

    pub fn upsert_profile(&self, profile: &SalesProfile) -> Result<(), String> {
        let conn = self.open()?;
        let normalized = normalize_sales_profile(profile.clone())?;
        let json =
            serde_json::to_string(&normalized).map_err(|e| format!("Serialize failed: {e}"))?;
        conn.execute(
            "INSERT INTO sales_profile (id, json, updated_at) VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET json=excluded.json, updated_at=excluded.updated_at",
            params![json, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to save profile: {e}"))?;
        Ok(())
    }

    pub fn set_onboarding_brief(&self, brief: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "INSERT INTO sales_onboarding (id, brief_text, updated_at) VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET brief_text=excluded.brief_text, updated_at=excluded.updated_at",
            params![brief, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to save onboarding brief: {e}"))?;
        Ok(())
    }

    fn get_onboarding_brief_state(&self) -> Result<OnboardingBriefState, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare("SELECT brief_text, updated_at FROM sales_onboarding WHERE id = 1")
            .map_err(|e| format!("Onboarding brief query prepare failed: {e}"))?;
        let row = stmt
            .query_row([], |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, Option<String>>(1)?,
                ))
            })
            .optional()
            .map_err(|e| format!("Onboarding brief query failed: {e}"))?;
        let Some((brief, updated_at)) = row else {
            return Ok(OnboardingBriefState::default());
        };
        let brief = brief.and_then(|v| {
            let t = v.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        });
        let updated_at = updated_at.and_then(|v| {
            let t = v.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        });
        Ok(OnboardingBriefState { brief, updated_at })
    }

    pub fn get_onboarding_brief(&self) -> Result<Option<String>, String> {
        self.get_onboarding_brief_state().map(|s| s.brief)
    }

    pub fn latest_successful_run_id_since(
        &self,
        since: Option<&str>,
    ) -> Result<Option<String>, String> {
        let conn = self.open()?;
        let (sql, with_since) = if since.is_some() {
            (
                "SELECT sr.id
                 FROM sales_runs sr
                 WHERE sr.status = 'completed'
                   AND sr.started_at >= ?
                   AND EXISTS (
                     SELECT 1 FROM prospect_profiles pp WHERE pp.run_id = sr.id
                   )
                 ORDER BY sr.completed_at DESC
                 LIMIT 1",
                true,
            )
        } else {
            (
                "SELECT sr.id
                 FROM sales_runs sr
                 WHERE sr.status = 'completed'
                   AND EXISTS (
                     SELECT 1 FROM prospect_profiles pp WHERE pp.run_id = sr.id
                   )
                 ORDER BY sr.completed_at DESC
                 LIMIT 1",
                false,
            )
        };
        if with_since {
            conn.query_row(sql, params![since.unwrap_or_default()], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(|e| format!("Latest successful run query failed: {e}"))
        } else {
            conn.query_row(sql, [], |r| r.get::<_, String>(0))
                .optional()
                .map_err(|e| format!("Latest successful run query failed: {e}"))
        }
    }

    pub fn latest_successful_run_id(&self) -> Result<Option<String>, String> {
        self.latest_successful_run_id_since(None)
    }

    fn latest_running_run_row(&self) -> Result<Option<(String, String)>, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT id, started_at
             FROM sales_runs
             WHERE status = 'running'
             ORDER BY started_at DESC
             LIMIT 1",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|e| format!("Latest running run query failed: {e}"))
    }

    fn count_leads_for_run(&self, run_id: &str) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*) FROM leads WHERE run_id = ?",
            params![run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Lead count query failed: {e}"))
    }

    fn count_prospect_profiles_for_run(&self, run_id: &str) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*) FROM prospect_profiles WHERE run_id = ?",
            params![run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Prospect profile count query failed: {e}"))
    }

    fn count_approvals_for_run(&self, run_id: &str) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*)
             FROM approvals a
             INNER JOIN leads l ON l.id = a.lead_id
             WHERE l.run_id = ?",
            params![run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Approval count query failed: {e}"))
    }

    pub fn recover_latest_timed_out_run(&self) -> Result<Option<SalesRunRecord>, String> {
        let Some((run_id, started_at)) = self.latest_running_run_row()? else {
            return Ok(None);
        };

        let inserted = self.count_leads_for_run(&run_id)?;
        let profiled = self.count_prospect_profiles_for_run(&run_id)?;
        let approvals_queued = self.count_approvals_for_run(&run_id)?;
        let discovered = inserted.max(profiled);

        let (status, error_note) = if inserted > 0 || profiled > 0 {
            (
                "completed",
                Some(format!(
                    "Prospecting run exceeded the request timeout after saving progress (profiles: {profiled}, leads: {inserted}, approvals: {approvals_queued})."
                )),
            )
        } else {
            (
                "failed",
                Some(
                    "Prospecting run exceeded the request timeout before any durable prospect dossiers or actionable leads were saved."
                        .to_string(),
                ),
            )
        };

        self.finish_run(
            &run_id,
            status,
            discovered,
            inserted,
            approvals_queued,
            error_note.as_deref(),
        )?;

        Ok(Some(SalesRunRecord {
            id: run_id,
            status: status.to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered,
            inserted,
            approvals_queued,
            error: error_note,
        }))
    }

    pub fn recover_latest_timed_out_run_if_stale(
        &self,
        min_age_secs: i64,
    ) -> Result<Option<SalesRunRecord>, String> {
        let Some((_run_id, started_at)) = self.latest_running_run_row()? else {
            return Ok(None);
        };
        let started_at = chrono::DateTime::parse_from_rfc3339(&started_at)
            .map_err(|e| format!("Failed to parse running run timestamp: {e}"))?
            .with_timezone(&Utc);
        if (Utc::now() - started_at).num_seconds() < min_age_secs {
            return Ok(None);
        }
        self.recover_latest_timed_out_run()
    }

    fn begin_run(&self) -> Result<String, String> {
        let conn = self.open()?;
        let run_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO sales_runs (id, status, started_at) VALUES (?, 'running', ?)",
            params![run_id, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to create run row: {e}"))?;
        Ok(run_id)
    }

    fn finish_run(
        &self,
        run_id: &str,
        status: &str,
        discovered: u32,
        inserted: u32,
        approvals_queued: u32,
        error_msg: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE sales_runs SET status = ?, completed_at = ?, discovered = ?, inserted = ?, approvals_queued = ?, error = ? WHERE id = ?",
            params![
                status,
                Utc::now().to_rfc3339(),
                discovered,
                inserted,
                approvals_queued,
                error_msg,
                run_id
            ],
        )
        .map_err(|e| format!("Failed to update run row: {e}"))?;
        Ok(())
    }

    fn create_job_run(&self, job_type: &str) -> Result<String, String> {
        let conn = self.open()?;
        let job_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO job_runs (id, job_type, status, started_at) VALUES (?1, ?2, 'running', ?3)",
            params![job_id, job_type, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to create job run: {e}"))?;
        self.ensure_job_stage_rows(&conn, &job_id)?;
        Ok(job_id)
    }

    fn ensure_job_stage_rows(&self, conn: &Connection, job_id: &str) -> Result<(), String> {
        for stage in PipelineStage::ordered() {
            conn.execute(
                "INSERT OR IGNORE INTO job_stages (id, job_run_id, stage_name, status, updated_at)
                 VALUES (?1, ?2, ?3, 'pending', ?4)",
                params![
                    format!("{job_id}:{}", stage.as_str()),
                    job_id,
                    stage.as_str(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to create job stage rows: {e}"))?;
        }
        Ok(())
    }

    fn set_job_stage_running(&self, job_id: &str, stage: PipelineStage) -> Result<(), String> {
        let conn = self.open()?;
        self.ensure_job_stage_rows(&conn, job_id)?;
        conn.execute(
            "UPDATE job_stages
             SET status = 'running',
                 started_at = COALESCE(started_at, ?3),
                 updated_at = ?3
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![job_id, stage.as_str(), Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to update job stage to running: {e}"))?;
        Ok(())
    }

    fn update_job_stage_checkpoint<T: Serialize>(
        &self,
        job_id: &str,
        stage: PipelineStage,
        checkpoint: &T,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let checkpoint_data = serde_json::to_string(checkpoint)
            .map_err(|e| format!("Failed to serialize job checkpoint: {e}"))?;
        conn.execute(
            "UPDATE job_stages
             SET checkpoint_data = ?3,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![
                job_id,
                stage.as_str(),
                checkpoint_data,
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(|e| format!("Failed to update job checkpoint: {e}"))?;
        Ok(())
    }

    fn complete_job_stage<T: Serialize>(
        &self,
        job_id: &str,
        stage: PipelineStage,
        checkpoint: &T,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let checkpoint_data = serde_json::to_string(checkpoint)
            .map_err(|e| format!("Failed to serialize job checkpoint: {e}"))?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE job_stages
             SET status = 'completed',
                 checkpoint_data = ?3,
                 completed_at = ?4,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![job_id, stage.as_str(), checkpoint_data, now],
        )
        .map_err(|e| format!("Failed to complete job stage: {e}"))?;
        Ok(())
    }

    fn fail_job_stage(
        &self,
        job_id: &str,
        stage: PipelineStage,
        error_msg: &str,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE job_stages
             SET status = 'failed',
                 checkpoint_data = ?3,
                 completed_at = ?4,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![
                job_id,
                stage.as_str(),
                serde_json::json!({ "error": error_msg }).to_string(),
                now
            ],
        )
        .map_err(|e| format!("Failed to mark job stage failed: {e}"))?;
        conn.execute(
            "UPDATE job_runs SET status = 'failed', completed_at = ?2, error_message = ?3 WHERE id = ?1",
            params![job_id, now, error_msg],
        )
        .map_err(|e| format!("Failed to mark job run failed: {e}"))?;
        Ok(())
    }

    fn complete_job_run(&self, job_id: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE job_runs SET status = 'completed', completed_at = ?2, error_message = NULL WHERE id = ?1",
            params![job_id, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to complete job run: {e}"))?;
        Ok(())
    }

    fn get_job_progress(&self, job_id: &str) -> Result<Option<JobProgressResponse>, String> {
        let conn = self.open()?;
        let job = conn
            .query_row(
                "SELECT status, error_message FROM job_runs WHERE id = ?1",
                params![job_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(|e| format!("Job progress lookup failed: {e}"))?;
        let Some((status, error_message)) = job else {
            return Ok(None);
        };

        let mut stmt = conn
            .prepare(
                "SELECT stage_name, status, started_at, completed_at, checkpoint_data
                 FROM job_stages
                 WHERE job_run_id = ?1
                 ORDER BY CASE stage_name
                    WHEN 'QueryPlanning' THEN 1
                    WHEN 'Discovery' THEN 2
                    WHEN 'Merging' THEN 3
                    WHEN 'Validation' THEN 4
                    WHEN 'Filtering' THEN 5
                    WHEN 'Enrichment' THEN 6
                    WHEN 'LeadGeneration' THEN 7
                    ELSE 99 END",
            )
            .map_err(|e| format!("Prepare job stages query failed: {e}"))?;
        let stages = stmt
            .query_map(params![job_id], |row| {
                let checkpoint = row
                    .get::<_, Option<String>>(4)?
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok());
                Ok(JobStageStatus {
                    name: row.get::<_, String>(0)?,
                    status: row.get::<_, String>(1)?,
                    started_at: row.get::<_, Option<String>>(2)?,
                    completed_at: row.get::<_, Option<String>>(3)?,
                    checkpoint,
                })
            })
            .map_err(|e| format!("Job stages query failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Job stages row decode failed: {e}"))?;

        let current_stage = stages
            .iter()
            .find(|stage| stage.status == "running" || stage.status == "failed")
            .map(|stage| stage.name.clone())
            .or_else(|| {
                stages
                    .iter()
                    .rev()
                    .find(|stage| stage.status == "completed")
                    .map(|stage| stage.name.clone())
            });

        Ok(Some(JobProgressResponse {
            job_id: job_id.to_string(),
            status,
            current_stage,
            stages,
            error_message,
        }))
    }

    fn latest_running_job_progress(
        &self,
        job_type: &str,
    ) -> Result<Option<JobProgressResponse>, String> {
        let conn = self.open()?;
        let job_id = conn
            .query_row(
                "SELECT id
                 FROM job_runs
                 WHERE job_type = ?1 AND status = 'running'
                 ORDER BY started_at DESC
                 LIMIT 1",
                params![job_type],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Active job lookup failed: {e}"))?;
        let Some(job_id) = job_id else {
            return Ok(None);
        };
        self.get_job_progress(&job_id)
    }

    fn latest_completed_checkpoint(
        &self,
        job_id: &str,
    ) -> Result<Option<(PipelineStage, String)>, String> {
        let conn = self.open()?;
        let row = conn
            .query_row(
                "SELECT stage_name, checkpoint_data
                 FROM job_stages
                 WHERE job_run_id = ?1
                   AND status = 'completed'
                   AND checkpoint_data IS NOT NULL
                 ORDER BY CASE stage_name
                    WHEN 'QueryPlanning' THEN 1
                    WHEN 'Discovery' THEN 2
                    WHEN 'Merging' THEN 3
                    WHEN 'Validation' THEN 4
                    WHEN 'Filtering' THEN 5
                    WHEN 'Enrichment' THEN 6
                    WHEN 'LeadGeneration' THEN 7
                    ELSE 99 END DESC
                 LIMIT 1",
                params![job_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|e| format!("Checkpoint lookup failed: {e}"))?;
        let Some((stage_name, checkpoint_data)) = row else {
            return Ok(None);
        };
        let stage = match stage_name.as_str() {
            "QueryPlanning" => PipelineStage::QueryPlanning,
            "Discovery" => PipelineStage::Discovery,
            "Merging" => PipelineStage::Merging,
            "Validation" => PipelineStage::Validation,
            "Filtering" => PipelineStage::Filtering,
            "Enrichment" => PipelineStage::Enrichment,
            "LeadGeneration" => PipelineStage::LeadGeneration,
            _ => return Ok(None),
        };
        Ok(Some((stage, checkpoint_data)))
    }

    fn approval_already_pending(
        &self,
        conn: &Connection,
        channel: &str,
        recipient: &str,
    ) -> Result<bool, String> {
        let recipient_json_path = if channel == "email" {
            "$.to"
        } else {
            "$.profile_url"
        };
        let sql = format!(
            "SELECT COUNT(*)
             FROM approvals
             WHERE channel = ?1
               AND status = 'pending'
               AND json_extract(payload_json, '{recipient_json_path}') = ?2"
        );
        conn.query_row(sql.as_str(), params![channel, recipient], |row| {
            row.get::<_, i64>(0)
        })
        .map(|count| count > 0)
        .map_err(|e| format!("Pending approval lookup failed: {e}"))
    }

    fn is_suppressed(&self, conn: &Connection, contact_value: &str) -> Result<bool, String> {
        conn.query_row(
            "SELECT COUNT(*) FROM suppressions WHERE contact_method_value = ?1",
            params![contact_value.trim().to_lowercase()],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .map_err(|e| format!("Suppression lookup failed: {e}"))
    }

    fn suppress_contact(
        &self,
        conn: &Connection,
        value: &str,
        reason: &str,
        permanent: bool,
        source_outcome_id: Option<&str>,
    ) -> Result<(), String> {
        conn.execute(
            "INSERT OR IGNORE INTO suppressions
             (id, contact_method_value, reason, source_outcome_id, suppressed_at, permanent)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                uuid::Uuid::new_v4().to_string(),
                value.trim().to_lowercase(),
                reason,
                source_outcome_id,
                Utc::now().to_rfc3339(),
                i32::from(permanent)
            ],
        )
        .map_err(|e| format!("Failed to suppress contact: {e}"))?;
        Ok(())
    }

    fn update_source_health(&self, source_type: &str, count: usize) -> Result<(), String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        let parser_health = if count == 0 { 0.0 } else { 1.0 };
        conn.execute(
            "INSERT INTO source_health (id, source_type, parser_health, freshness, last_checked_at, auto_skip)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)
             ON CONFLICT(source_type) DO UPDATE SET
                parser_health = excluded.parser_health,
                freshness = excluded.freshness,
                last_checked_at = excluded.last_checked_at,
                auto_skip = CASE WHEN excluded.parser_health = 0.0 THEN 1 ELSE 0 END",
            params![
                uuid::Uuid::new_v4().to_string(),
                source_type,
                parser_health,
                now,
                if count == 0 { 1 } else { 0 }
            ],
        )
        .map_err(|e| format!("Failed to update source health: {e}"))?;
        Ok(())
    }

    fn should_skip_source(&self, source_type: &str) -> Result<bool, String> {
        let conn = self.open()?;
        let auto_skip = conn
            .query_row(
                "SELECT COALESCE(auto_skip, 0) FROM source_health WHERE source_type = ?1",
                params![source_type],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| format!("Source health lookup failed: {e}"))?
            .unwrap_or(0);
        Ok(auto_skip == 1)
    }

    fn list_source_health(&self) -> Result<Vec<SourceHealthRow>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, source_type, precision, freshness, parser_health, legal_mode,
                        historical_reply_yield, last_checked_at, auto_skip
                 FROM source_health
                 ORDER BY source_type ASC",
            )
            .map_err(|e| format!("Prepare source_health query failed: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SourceHealthRow {
                    id: row.get::<_, String>(0)?,
                    source_type: row.get::<_, String>(1)?,
                    precision: row.get::<_, Option<f64>>(2)?,
                    freshness: row.get::<_, Option<String>>(3)?,
                    parser_health: row.get::<_, f64>(4).unwrap_or(0.0),
                    legal_mode: row
                        .get::<_, String>(5)
                        .unwrap_or_else(|_| "public".to_string()),
                    historical_reply_yield: row.get::<_, Option<f64>>(6)?,
                    last_checked_at: row.get::<_, Option<String>>(7)?,
                    auto_skip: row.get::<_, i64>(8).unwrap_or(0) == 1,
                })
            })
            .map_err(|e| format!("Source health query failed: {e}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Source health row decode failed: {e}"))
    }

    fn update_lead_status(&self, lead_id: &str, status: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE leads SET status = ?2 WHERE id = ?1",
            params![lead_id, status],
        )
        .map_err(|e| format!("Failed to update lead status: {e}"))?;
        Ok(())
    }

    fn select_accounts_for_activation(
        &self,
        conn: &Connection,
        candidate_priorities: &HashMap<String, i64>,
        daily_target: u32,
    ) -> Result<Vec<String>, String> {
        if daily_target == 0 || candidate_priorities.is_empty() {
            return Ok(Vec::new());
        }

        let target = daily_target as usize;
        let exploit_target = ((target as f64) * ACTIVATION_EXPLOIT_RATIO).ceil() as usize;
        let candidate_ids = candidate_priorities.keys().cloned().collect::<HashSet<_>>();
        let mut selected = Vec::<String>::new();
        let mut selected_set = HashSet::<String>::new();

        let mut activation_stmt = conn
            .prepare(
                "SELECT account_id, priority
                 FROM activation_queue
                 WHERE status = 'pending'
                 ORDER BY priority DESC, created_at ASC",
            )
            .map_err(|e| format!("Failed to prepare activation selection query: {e}"))?;
        let activation_rows = activation_stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| format!("Failed to query activation queue: {e}"))?;
        for row in activation_rows {
            let (account_id, _priority) =
                row.map_err(|e| format!("Failed to decode activation queue row: {e}"))?;
            if !candidate_ids.contains(&account_id) || !selected_set.insert(account_id.clone()) {
                continue;
            }
            selected.push(account_id);
            if selected.len() >= exploit_target.min(target) {
                break;
            }
        }

        if selected.len() < target {
            let recent_exploration = conn
                .prepare(
                    "SELECT DISTINCT account_id
                     FROM exploration_log
                     WHERE created_at >= datetime('now', '-30 days')",
                )
                .and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<HashSet<_>, _>>()
                })
                .map_err(|e| format!("Failed to load exploration history: {e}"))?;

            let mut score_stmt = conn
                .prepare(
                    "SELECT s.account_id,
                            s.fit_score,
                            COALESCE(s.activation_priority, 0.0)
                     FROM score_snapshots s
                     INNER JOIN (
                        SELECT account_id, MAX(computed_at) AS computed_at
                        FROM score_snapshots
                        GROUP BY account_id
                     ) latest
                       ON latest.account_id = s.account_id
                      AND latest.computed_at = s.computed_at",
                )
                .map_err(|e| format!("Failed to prepare exploration selection query: {e}"))?;
            let mut exploratory = score_stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                })
                .map_err(|e| format!("Failed to query exploration candidates: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode exploration candidates: {e}"))?;
            exploratory.retain(|(account_id, fit_score, _priority)| {
                candidate_ids.contains(account_id)
                    && !selected_set.contains(account_id)
                    && !recent_exploration.contains(account_id)
                    && (0.3..=0.7).contains(fit_score)
            });
            exploratory.sort_by(|a, b| {
                b.2.partial_cmp(&a.2)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            let remaining_slots = target.saturating_sub(selected.len());
            for (account_id, _fit_score, _priority) in exploratory.into_iter().take(remaining_slots)
            {
                conn.execute(
                    "INSERT INTO exploration_log
                     (id, account_id, exploration_reason, exploration_type, outcome_id, learned_pattern, created_at)
                     VALUES (?1, ?2, 'scheduled_exploration', 'mid_score_priority', NULL, NULL, ?3)",
                    params![
                        stable_sales_id("explore", &[&account_id, &Utc::now().format("%Y-%m-%d").to_string()]),
                        account_id,
                        Utc::now().to_rfc3339(),
                    ],
                )
                .map_err(|e| format!("Failed to record exploration selection: {e}"))?;
                if selected_set.insert(account_id.clone()) {
                    selected.push(account_id);
                }
            }
        }

        if selected.len() < target {
            let mut fallback = candidate_priorities
                .iter()
                .map(|(account_id, priority)| (account_id.clone(), *priority))
                .collect::<Vec<_>>();
            fallback.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            for (account_id, _priority) in fallback {
                if selected.len() >= target {
                    break;
                }
                if selected_set.insert(account_id.clone()) {
                    selected.push(account_id);
                }
            }
        }

        for account_id in &selected {
            conn.execute(
                "UPDATE activation_queue
                 SET status = 'activated'
                 WHERE account_id = ?1 AND status = 'pending'",
                params![account_id],
            )
            .map_err(|e| format!("Failed to mark activation queue row activated: {e}"))?;
        }

        Ok(selected)
    }

    fn load_policy_proposal(
        &self,
        conn: &Connection,
        id: &str,
    ) -> Result<Option<SalesPolicyProposal>, String> {
        conn.query_row(
            "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                    backtest_result_json, holdout_result_json, status, approved_by,
                    activated_at, version, created_at
             FROM retrieval_rule_versions
             WHERE id = ?1",
            params![id],
            |row| {
                Ok(SalesPolicyProposal {
                    id: row.get::<_, String>(0)?,
                    rule_type: row.get::<_, String>(1)?,
                    rule_key: row.get::<_, String>(2)?,
                    old_value: row.get::<_, Option<String>>(3)?,
                    new_value: row.get::<_, String>(4)?,
                    proposal_source: row.get::<_, Option<String>>(5)?,
                    backtest_result_json: row.get::<_, Option<String>>(6)?,
                    holdout_result_json: row.get::<_, Option<String>>(7)?,
                    status: row.get::<_, String>(8)?,
                    approved_by: row.get::<_, Option<String>>(9)?,
                    activated_at: row.get::<_, Option<String>>(10)?,
                    version: row.get::<_, i64>(11)?,
                    created_at: row.get::<_, String>(12)?,
                })
            },
        )
        .optional()
        .map_err(|e| format!("Failed to load policy proposal: {e}"))
    }

    fn list_policy_proposals(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SalesPolicyProposal>, String> {
        let conn = self.open()?;
        if let Some(status) = status {
            let mut stmt = conn
                .prepare(
                    "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                            backtest_result_json, holdout_result_json, status, approved_by,
                            activated_at, version, created_at
                     FROM retrieval_rule_versions
                     WHERE status = ?1
                     ORDER BY created_at DESC
                     LIMIT ?2",
                )
                .map_err(|e| format!("Failed to prepare policy proposals query: {e}"))?;
            let rows = stmt
                .query_map(params![status, limit as i64], |row| {
                    Ok(SalesPolicyProposal {
                        id: row.get::<_, String>(0)?,
                        rule_type: row.get::<_, String>(1)?,
                        rule_key: row.get::<_, String>(2)?,
                        old_value: row.get::<_, Option<String>>(3)?,
                        new_value: row.get::<_, String>(4)?,
                        proposal_source: row.get::<_, Option<String>>(5)?,
                        backtest_result_json: row.get::<_, Option<String>>(6)?,
                        holdout_result_json: row.get::<_, Option<String>>(7)?,
                        status: row.get::<_, String>(8)?,
                        approved_by: row.get::<_, Option<String>>(9)?,
                        activated_at: row.get::<_, Option<String>>(10)?,
                        version: row.get::<_, i64>(11)?,
                        created_at: row.get::<_, String>(12)?,
                    })
                })
                .map_err(|e| format!("Failed to query policy proposals: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode policy proposals: {e}"))
        } else {
            let mut stmt = conn
                .prepare(
                    "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                            backtest_result_json, holdout_result_json, status, approved_by,
                            activated_at, version, created_at
                     FROM retrieval_rule_versions
                     ORDER BY created_at DESC
                     LIMIT ?1",
                )
                .map_err(|e| format!("Failed to prepare policy proposals query: {e}"))?;
            let rows = stmt
                .query_map(params![limit as i64], |row| {
                    Ok(SalesPolicyProposal {
                        id: row.get::<_, String>(0)?,
                        rule_type: row.get::<_, String>(1)?,
                        rule_key: row.get::<_, String>(2)?,
                        old_value: row.get::<_, Option<String>>(3)?,
                        new_value: row.get::<_, String>(4)?,
                        proposal_source: row.get::<_, Option<String>>(5)?,
                        backtest_result_json: row.get::<_, Option<String>>(6)?,
                        holdout_result_json: row.get::<_, Option<String>>(7)?,
                        status: row.get::<_, String>(8)?,
                        approved_by: row.get::<_, Option<String>>(9)?,
                        activated_at: row.get::<_, Option<String>>(10)?,
                        version: row.get::<_, i64>(11)?,
                        created_at: row.get::<_, String>(12)?,
                    })
                })
                .map_err(|e| format!("Failed to query policy proposals: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode policy proposals: {e}"))
        }
    }

    fn update_policy_proposal_status(
        &self,
        id: &str,
        status: &str,
        approved_by: Option<&str>,
    ) -> Result<Option<SalesPolicyProposal>, String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        let activated_at = if status == "active" {
            Some(now.clone())
        } else {
            None
        };
        conn.execute(
            "UPDATE retrieval_rule_versions
             SET status = ?2,
                 approved_by = COALESCE(?3, approved_by),
                 activated_at = CASE
                    WHEN ?2 = 'active' THEN ?4
                    WHEN ?2 = 'retired' THEN NULL
                    ELSE activated_at
                 END
             WHERE id = ?1",
            params![id, status, approved_by, activated_at],
        )
        .map_err(|e| format!("Failed to update policy proposal status: {e}"))?;
        self.load_policy_proposal(&conn, id)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_or_refresh_policy_proposal(
        &self,
        conn: &Connection,
        rule_type: &str,
        rule_key: &str,
        old_value: Option<&str>,
        new_value: &str,
        proposal_source: &str,
        backtest_result: &serde_json::Value,
    ) -> Result<String, String> {
        let proposal_id = stable_sales_id(
            "rule_proposal",
            &[rule_type, rule_key, new_value, proposal_source],
        );
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO retrieval_rule_versions
             (id, rule_type, rule_key, old_value, new_value, proposal_source,
              backtest_result_json, holdout_result_json, status, approved_by,
              activated_at, version, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 'proposed', NULL, NULL, 1, ?8)
             ON CONFLICT(id) DO UPDATE SET
                old_value = excluded.old_value,
                new_value = excluded.new_value,
                proposal_source = excluded.proposal_source,
                backtest_result_json = excluded.backtest_result_json,
                status = CASE
                    WHEN retrieval_rule_versions.status = 'active' THEN retrieval_rule_versions.status
                    ELSE 'proposed'
                END",
            params![
                proposal_id,
                rule_type,
                rule_key,
                old_value,
                new_value,
                proposal_source,
                backtest_result.to_string(),
                now,
            ],
        )
        .map_err(|e| format!("Failed to create policy proposal: {e}"))?;
        Ok(proposal_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_missed_signal_review(
        &self,
        conn: &Connection,
        outcome_id: &str,
        snapshot_id: &str,
        account_id: &str,
        outcome_type: &str,
        active_signal_ids: &[String],
        unused_signal_ids: &[String],
    ) -> Result<(), String> {
        let mut signal_stmt = conn
            .prepare(
                "SELECT id, signal_type
                 FROM signals
                 WHERE account_id = ?1
                 ORDER BY confidence DESC, created_at DESC",
            )
            .map_err(|e| format!("Failed to prepare missed-signal query: {e}"))?;
        let all_signals = signal_stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("Failed to query missed-signal candidates: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode missed-signal candidates: {e}"))?;
        let all_signal_types = all_signals
            .iter()
            .map(|(signal_id, signal_type)| (signal_id.clone(), signal_type.clone()))
            .collect::<HashMap<_, _>>();
        let active_set = active_signal_ids.iter().cloned().collect::<HashSet<_>>();
        let unused_set = unused_signal_ids.iter().cloned().collect::<HashSet<_>>();
        let missed_signals = all_signals
            .iter()
            .filter(|(signal_id, _)| {
                !active_set.contains(signal_id) && !unused_set.contains(signal_id)
            })
            .map(|(signal_id, _)| signal_id.clone())
            .collect::<Vec<_>>();

        let positive_outcome = matches!(
            outcome_type,
            "open"
                | "click"
                | "forwarded"
                | "referral"
                | "interested"
                | "meeting_booked"
                | "closed_won"
        );
        let negative_outcome = matches!(
            outcome_type,
            "hard_bounce"
                | "soft_bounce"
                | "no_reply"
                | "auto_reply"
                | "unsubscribe"
                | "wrong_person"
                | "not_now"
                | "closed_lost"
        );
        let validated_signals = if positive_outcome {
            active_signal_ids.to_vec()
        } else {
            Vec::new()
        };
        let false_positive_signals = if negative_outcome {
            active_signal_ids.to_vec()
        } else {
            Vec::new()
        };
        let review_id = stable_sales_id("missed_signal_review", &[outcome_id, snapshot_id]);
        conn.execute(
            "INSERT INTO missed_signal_reviews
             (id, outcome_id, snapshot_id, reviewed_at, validated_signals, false_positive_signals,
              missed_signals, timing_mistakes, persona_mismatch, channel_mismatch, reviewer_type)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '[]', NULL, NULL, 'auto')
             ON CONFLICT(id) DO UPDATE SET
                reviewed_at = excluded.reviewed_at,
                validated_signals = excluded.validated_signals,
                false_positive_signals = excluded.false_positive_signals,
                missed_signals = excluded.missed_signals",
            params![
                review_id,
                outcome_id,
                snapshot_id,
                Utc::now().to_rfc3339(),
                serde_json::to_string(&validated_signals)
                    .map_err(|e| format!("Failed to encode validated signals: {e}"))?,
                serde_json::to_string(&false_positive_signals)
                    .map_err(|e| format!("Failed to encode false-positive signals: {e}"))?,
                serde_json::to_string(&missed_signals)
                    .map_err(|e| format!("Failed to encode missed signals: {e}"))?,
            ],
        )
        .map_err(|e| format!("Failed to persist missed signal review: {e}"))?;

        let (proposal_direction, driver_signal_id) =
            if let Some(signal_id) = validated_signals.first() {
                ("increase", Some(signal_id.clone()))
            } else if let Some(signal_id) = false_positive_signals.first() {
                ("decrease", Some(signal_id.clone()))
            } else {
                ("", None)
            };
        if let Some(signal_id) = driver_signal_id {
            if let Some(signal_type) = all_signal_types.get(&signal_id) {
                let proposal_payload = serde_json::json!({
                    "signal_type": signal_type,
                    "direction": proposal_direction,
                    "trigger_outcome": outcome_type,
                    "driver_signal_id": signal_id,
                    "review_id": review_id,
                });
                let backtest_result = serde_json::json!({
                    "outcome_id": outcome_id,
                    "validated_signal_count": validated_signals.len(),
                    "false_positive_signal_count": false_positive_signals.len(),
                    "missed_signal_count": missed_signals.len(),
                });
                let _ = self.create_or_refresh_policy_proposal(
                    conn,
                    "signal_weight",
                    &format!("signal_weight::{signal_type}"),
                    None,
                    &proposal_payload.to_string(),
                    &format!("auto_outcome_review::{outcome_type}"),
                    &backtest_result,
                )?;
            }
        }

        Ok(())
    }

    fn migrate_legacy_to_canonical_core(&self) -> Result<(), String> {
        let conn = self.open()?;
        let sales_profile = self.get_profile().ok().flatten();

        if let Some(profile) = sales_profile.as_ref() {
            let icp_id = "default_icp";
            conn.execute(
                "INSERT INTO icp_definitions (id, name, sector_rules, geo_rules, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    sector_rules = excluded.sector_rules,
                    geo_rules = excluded.geo_rules",
                params![
                    icp_id,
                    format!("{} ICP", profile.product_name.trim()),
                    serde_json::json!([profile.target_industry]).to_string(),
                    serde_json::json!([profile.target_geo]).to_string(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to upsert icp_definitions: {e}"))?;

            let sender_domain = email_domain(&profile.sender_email).unwrap_or_default();
            conn.execute(
                "INSERT INTO sender_policies (id, icp_id, mailbox_pool, daily_cap, subdomain, warm_state)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'warming')
                 ON CONFLICT(id) DO UPDATE SET
                    mailbox_pool = excluded.mailbox_pool,
                    daily_cap = excluded.daily_cap,
                    subdomain = excluded.subdomain",
                params![
                    "default_sender_policy",
                    icp_id,
                    serde_json::to_string(&default_mailbox_pool_from_profile(profile))
                        .map_err(|e| format!("Failed to encode default mailbox pool: {e}"))?,
                    profile.daily_send_cap as i64,
                    sender_domain,
                ],
            )
            .map_err(|e| format!("Failed to upsert sender_policies: {e}"))?;
        }

        for profile in self
            .list_stored_prospect_profiles(10_000, None)
            .unwrap_or_default()
        {
            self.migrate_prospect_profile(&conn, &profile, sales_profile.as_ref())?;
        }
        for lead in self.list_leads(10_000, None).unwrap_or_default() {
            self.migrate_lead(&conn, &lead, sales_profile.as_ref())?;
        }
        Ok(())
    }

    fn migrate_prospect_profile(
        &self,
        conn: &Connection,
        profile: &SalesProspectProfile,
        sales_profile: Option<&SalesProfile>,
    ) -> Result<(), String> {
        let account_id = stable_sales_id("acct", &[&profile.company_domain]);
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, sector, geo, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
             ON CONFLICT(id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                display_name = excluded.display_name,
                sector = COALESCE(accounts.sector, excluded.sector),
                geo = COALESCE(accounts.geo, excluded.geo),
                website = COALESCE(accounts.website, excluded.website),
                tier = excluded.tier,
                updated_at = excluded.updated_at",
            params![
                account_id,
                profile.company,
                profile.company,
                sales_profile.map(|p| p.target_industry.clone()),
                sales_profile.map(|p| p.target_geo.clone()),
                profile.website,
                "standard",
                now,
            ],
        )
        .map_err(|e| format!("Failed to migrate account from prospect profile: {e}"))?;

        let domain_id = stable_sales_id("dom", &[&profile.company_domain]);
        conn.execute(
            "INSERT INTO domains (id, account_id, domain, is_primary, verified, checked_at)
             VALUES (?1, ?2, ?3, 1, ?4, ?5)
             ON CONFLICT(domain) DO UPDATE SET
                account_id = excluded.account_id,
                verified = excluded.verified,
                checked_at = excluded.checked_at",
            params![
                domain_id,
                account_id,
                profile.company_domain,
                i32::from(is_valid_company_domain(&profile.company_domain)),
                now,
            ],
        )
        .map_err(|e| format!("Failed to migrate domain from prospect profile: {e}"))?;

        if profile.primary_contact_name.is_some() || profile.primary_contact_title.is_some() {
            let contact_id = stable_sales_id(
                "contact",
                &[
                    &profile.company_domain,
                    profile.primary_contact_name.as_deref().unwrap_or("primary"),
                ],
            );
            conn.execute(
                "INSERT INTO contacts (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                    full_name = COALESCE(excluded.full_name, contacts.full_name),
                    title = COALESCE(excluded.title, contacts.title),
                    seniority = excluded.seniority,
                    name_confidence = MAX(contacts.name_confidence, excluded.name_confidence),
                    title_confidence = MAX(contacts.title_confidence, excluded.title_confidence),
                    is_decision_maker = MAX(contacts.is_decision_maker, excluded.is_decision_maker)",
                params![
                    contact_id,
                    account_id,
                    profile.primary_contact_name,
                    profile.primary_contact_title,
                    seniority_from_title(profile.primary_contact_title.as_deref()),
                    if profile.primary_contact_name.is_some() { 0.8 } else { 0.5 },
                    if profile.primary_contact_title.is_some() { 0.7 } else { 0.5 },
                    i32::from(
                        profile
                            .primary_contact_title
                            .as_deref()
                            .map(contact_title_priority)
                            .unwrap_or(0)
                            > 0
                    ),
                    now,
                ],
            )
            .map_err(|e| format!("Failed to migrate contact from prospect profile: {e}"))?;

            self.migrate_contact_methods(
                conn,
                &contact_id,
                profile.primary_email.as_deref(),
                None,
                profile.primary_linkedin_url.as_deref(),
            )?;
        }

        for signal in profile.matched_signals.iter().take(6) {
            let signal_id = stable_sales_id("signal", &[&account_id, signal]);
            conn.execute(
                "INSERT OR IGNORE INTO signals
                 (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
                 VALUES (?1, ?2, 'site_content', ?3, 'migration', ?4, ?5, 'structural', NULL, ?4)",
                params![signal_id, account_id, signal, now, source_confidence("directory_listing")],
            )
            .map_err(|e| format!("Failed to migrate signals: {e}"))?;
        }

        for link in profile
            .osint_links
            .iter()
            .take(MAX_OSINT_LINKS_PER_PROSPECT)
        {
            let artifact_id = stable_sales_id("artifact", &[&account_id, link]);
            conn.execute(
                "INSERT OR IGNORE INTO artifacts
                 (id, source_type, source_id, raw_content, parse_status, parser_health, freshness, legal_mode, created_at)
                 VALUES (?1, 'web_search', ?2, ?2, 'ok', 1.0, ?3, 'public', ?3)",
                params![artifact_id, link, now],
            )
            .map_err(|e| format!("Failed to migrate artifact: {e}"))?;
        }

        Ok(())
    }

    fn migrate_lead(
        &self,
        conn: &Connection,
        lead: &SalesLead,
        sales_profile: Option<&SalesProfile>,
    ) -> Result<(), String> {
        let profile = SalesProspectProfile {
            id: lead.company_domain.clone(),
            run_id: lead.run_id.clone(),
            company: lead.company.clone(),
            website: lead.website.clone(),
            company_domain: lead.company_domain.clone(),
            fit_score: lead.score,
            profile_status: if lead.email.is_some() || lead.linkedin_url.is_some() {
                "contact_ready".to_string()
            } else {
                "contact_identified".to_string()
            },
            summary: lead.reasons.join(" "),
            matched_signals: lead.reasons.clone(),
            primary_contact_name: clean_profile_contact_name(&lead.contact_name),
            primary_contact_title: clean_profile_contact_field(&lead.contact_title),
            primary_email: lead.email.clone(),
            primary_linkedin_url: lead.linkedin_url.clone(),
            company_linkedin_url: None,
            osint_links: vec![lead.website.clone()],
            contact_count: 1,
            source_count: lead.reasons.len() as u32,
            buyer_roles: Vec::new(),
            pain_points: Vec::new(),
            trigger_events: Vec::new(),
            recommended_channel: if lead.email.is_some() {
                "email"
            } else {
                "linkedin"
            }
            .to_string(),
            outreach_angle: String::new(),
            research_status: "migration".to_string(),
            research_confidence: 0.7,
            tech_stack: Vec::new(),
            created_at: lead.created_at.clone(),
            updated_at: lead.created_at.clone(),
        };
        self.migrate_prospect_profile(conn, &profile, sales_profile)?;

        let account_id = stable_sales_id("acct", &[&lead.company_domain]);
        let score_id = stable_sales_id("score", &[&account_id, &lead.run_id]);
        conn.execute(
            "INSERT OR IGNORE INTO score_snapshots
             (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk, activation_priority, computed_at, scoring_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'v1')",
            params![
                score_id,
                account_id,
                (lead.score as f64 / 100.0).clamp(0.0, 1.0),
                if !lead.reasons.is_empty() { 0.35 } else { 0.1 },
                if lead.email.is_some() { 0.65 } else if lead.linkedin_url.is_some() { 0.45 } else { 0.1 },
                if lead.email.is_some() { 0.2 } else { 0.45 },
                0.1,
                (lead.score as f64 / 100.0).clamp(0.0, 1.0),
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| format!("Failed to migrate score snapshot: {e}"))?;

        Ok(())
    }

    fn migrate_contact_methods(
        &self,
        conn: &Connection,
        contact_id: &str,
        email: Option<&str>,
        phone: Option<&str>,
        linkedin: Option<&str>,
    ) -> Result<(), String> {
        if let Some(email) =
            email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
        {
            let method_id = stable_sales_id("cm", &[contact_id, "email", &email]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'email', ?3, ?4, ?5, ?6, 0)",
                params![
                    method_id,
                    contact_id,
                    email,
                    source_confidence("directory_listing"),
                    Utc::now().to_rfc3339(),
                    classify_email(&email, email_domain(&email).as_deref().unwrap_or_default()),
                ],
            )
            .map_err(|e| format!("Failed to migrate email contact method: {e}"))?;
        }
        if let Some(phone) = phone.and_then(normalize_phone) {
            let method_id = stable_sales_id("cm", &[contact_id, "phone", &phone]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'phone', ?3, ?4, ?5, 'personal', 0)",
                params![
                    method_id,
                    contact_id,
                    phone,
                    source_confidence("directory_listing"),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to migrate phone contact method: {e}"))?;
        }
        if let Some(linkedin) = linkedin.and_then(normalize_outreach_linkedin_url) {
            let method_id = stable_sales_id("cm", &[contact_id, "linkedin", &linkedin]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'linkedin', ?3, ?4, ?5, 'personal', 0)",
                params![
                    method_id,
                    contact_id,
                    linkedin,
                    source_confidence("web_search"),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to migrate LinkedIn contact method: {e}"))?;
        }
        Ok(())
    }

    fn upsert_artifact(
        &self,
        conn: &Connection,
        source_type: &str,
        source_id: &str,
        raw_content: &str,
        freshness: &str,
    ) -> Result<String, String> {
        let artifact_id = stable_sales_id("artifact", &[source_type, source_id]);
        conn.execute(
            "INSERT INTO artifacts
             (id, source_type, source_id, raw_content, parse_status, parser_health, freshness, legal_mode, created_at)
             VALUES (?1, ?2, ?3, ?4, 'ok', 1.0, ?5, 'public', ?5)
             ON CONFLICT(id) DO UPDATE SET
                raw_content = excluded.raw_content,
                freshness = excluded.freshness,
                parser_health = excluded.parser_health",
            params![artifact_id, source_type, source_id, raw_content, freshness],
        )
        .map_err(|e| format!("Failed to upsert artifact: {e}"))?;
        Ok(artifact_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_evidence(
        &self,
        conn: &Connection,
        artifact_id: &str,
        field_type: &str,
        field_value: &str,
        extraction_method: &str,
        confidence: f64,
        verified_at: &str,
    ) -> Result<Option<String>, String> {
        let value = field_value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        let evidence_id = stable_sales_id("evidence", &[artifact_id, field_type, value]);
        conn.execute(
            "INSERT INTO evidence
             (id, artifact_id, field_type, field_value, confidence, extraction_method, verified_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                confidence = MAX(evidence.confidence, excluded.confidence),
                verified_at = excluded.verified_at",
            params![
                evidence_id,
                artifact_id,
                field_type,
                value,
                confidence,
                extraction_method,
                verified_at,
            ],
        )
        .map_err(|e| format!("Failed to upsert evidence: {e}"))?;
        Ok(Some(evidence_id))
    }

    fn create_signal_with_rationale(
        &self,
        conn: &Connection,
        account_id: &str,
        signal_type: &str,
        text: &str,
        source: &str,
        evidence_ids: &[String],
    ) -> Result<String, String> {
        let signal_text = truncate_cleaned_text(text, 280);
        if signal_text.trim().is_empty() {
            return Err("Signal text is empty".to_string());
        }
        let signal_id = stable_sales_id("signal", &[account_id, signal_type, &signal_text]);
        let now = Utc::now().to_rfc3339();
        let (horizon, expires_at) = classify_signal_horizon(signal_type, &signal_text);
        conn.execute(
            "INSERT INTO signals
             (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?6)
             ON CONFLICT(id) DO UPDATE SET
                confidence = MAX(signals.confidence, excluded.confidence),
                observed_at = excluded.observed_at,
                effect_horizon = excluded.effect_horizon,
                expires_at = excluded.expires_at",
            params![
                signal_id,
                account_id,
                signal_type,
                signal_text,
                source,
                now,
                source_confidence(source),
                horizon,
                expires_at,
            ],
        )
        .map_err(|e| format!("Failed to upsert signal: {e}"))?;

        let rationale_id = stable_sales_id("signal_rationale", &[&signal_id, account_id]);
        conn.execute(
            "INSERT INTO signal_rationales
             (id, signal_id, account_id, why_it_matters, expected_effect, evidence_ids, confidence, created_at)
             VALUES (?1, ?2, ?3, ?4, 'meeting_probability_up', ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                why_it_matters = excluded.why_it_matters,
                expected_effect = excluded.expected_effect,
                evidence_ids = excluded.evidence_ids,
                confidence = MAX(signal_rationales.confidence, excluded.confidence)",
            params![
                rationale_id,
                signal_id,
                account_id,
                generate_signal_rationale(signal_type, &signal_text),
                serde_json::to_string(evidence_ids)
                    .map_err(|e| format!("Failed to encode signal evidence ids: {e}"))?,
                source_confidence(source),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert signal rationale: {e}"))?;
        Ok(signal_id)
    }

    fn enqueue_research(
        &self,
        conn: &Connection,
        account_id: &str,
        reason: &str,
        priority: i64,
    ) -> Result<(), String> {
        let id = stable_sales_id("research", &[account_id]);
        conn.execute(
            "INSERT INTO research_queue (id, account_id, priority, reason, status, assigned_at, completed_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
                priority = MAX(research_queue.priority, excluded.priority),
                reason = excluded.reason,
                status = CASE
                    WHEN research_queue.status = 'completed' THEN research_queue.status
                    ELSE 'pending'
                END",
            params![id, account_id, priority, reason],
        )
        .map_err(|e| format!("Failed to enqueue research: {e}"))?;
        Ok(())
    }

    fn enqueue_activation(
        &self,
        conn: &Connection,
        account_id: &str,
        contact_id: &str,
        thesis_id: &str,
        priority: i64,
    ) -> Result<(), String> {
        let id = stable_sales_id("activation", &[account_id, contact_id, thesis_id]);
        conn.execute(
            "INSERT INTO activation_queue (id, account_id, contact_id, thesis_id, priority, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)
             ON CONFLICT(id) DO UPDATE SET
                priority = MAX(activation_queue.priority, excluded.priority),
                status = CASE
                    WHEN activation_queue.status IN ('completed', 'active') THEN activation_queue.status
                    ELSE 'pending'
                END",
            params![id, account_id, contact_id, thesis_id, priority, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to enqueue activation: {e}"))?;
        Ok(())
    }

    fn ensure_default_sequence_template(&self, conn: &Connection) -> Result<String, String> {
        let template_id = "default_outreach_sequence".to_string();
        let steps_json = serde_json::json!([
            {"step": 1, "channel": "email", "delay_days": 0, "type": "initial_outreach", "description": "Short evidence-based email with a soft CTA"},
            {"step": 2, "channel": "email", "delay_days": 3, "type": "value_content", "description": "Share a teardown, case study, or relevant operational insight"},
            {"step": 3, "channel": "email", "delay_days": 5, "type": "follow_up", "description": "Reference the first touch and add one new angle"},
            {"step": 4, "channel": "linkedin_assist", "delay_days": 3, "type": "channel_switch", "description": "Manual LinkedIn follow-up for the operator"},
            {"step": 5, "channel": "email", "delay_days": 5, "type": "closing", "description": "Final polite close-the-loop email"}
        ])
        .to_string();
        conn.execute(
            "INSERT INTO sequence_templates (id, name, steps_json, icp_id, persona_id, version, created_at)
             VALUES (?1, 'Default Outreach Sequence', ?2, 'default_icp', NULL, 1, ?3)
             ON CONFLICT(id) DO UPDATE SET
                steps_json = excluded.steps_json,
                version = excluded.version",
            params![template_id, steps_json, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to ensure default sequence template: {e}"))?;
        Ok(template_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_account_thesis(
        &self,
        conn: &Connection,
        profile: &SalesProfile,
        account_id: &str,
        contact_id: &str,
        evidence_ids: &[String],
        score: &FiveAxisScore,
        gate: &SendGateDecision,
    ) -> Result<String, String> {
        let account = conn
            .query_row(
                "SELECT canonical_name, COALESCE(sector, ''), COALESCE(geo, ''), COALESCE(website, '')
                 FROM accounts WHERE id = ?1",
                params![account_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(|e| format!("Failed to load account for thesis: {e}"))?;

        let mut contact_stmt = conn
            .prepare(
                "SELECT COALESCE(full_name, ''), COALESCE(title, ''), name_confidence
                 FROM contacts
                 WHERE account_id = ?1
                 ORDER BY is_decision_maker DESC, created_at ASC
                 LIMIT 4",
            )
            .map_err(|e| format!("Failed to prepare thesis contacts query: {e}"))?;
        let buyer_committee = contact_stmt
            .query_map(params![account_id], |row| {
                Ok(serde_json::json!({
                    "role": infer_buyer_role(
                        row.get::<_, String>(1)
                            .unwrap_or_default()
                            .as_str()
                    ),
                    "name": row.get::<_, String>(0).unwrap_or_default(),
                    "confidence": row.get::<_, f64>(2).unwrap_or(0.5),
                }))
            })
            .map_err(|e| format!("Failed to query thesis contacts: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode thesis contacts: {e}"))?;

        let mut signal_stmt = conn
            .prepare(
                "SELECT text FROM signals
                 WHERE account_id = ?1
                 ORDER BY confidence DESC, observed_at DESC
                 LIMIT 3",
            )
            .map_err(|e| format!("Failed to prepare thesis signals query: {e}"))?;
        let signals = signal_stmt
            .query_map(params![account_id], |row| row.get::<_, String>(0))
            .map_err(|e| format!("Failed to query thesis signals: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode thesis signals: {e}"))?;

        let why_this_account = if let Some(signal) = signals.first() {
            format!(
                "{} matches {} in {} and shows public evidence of operational activity: {}",
                account.0,
                profile.product_name,
                if account.1.trim().is_empty() {
                    profile.target_industry.as_str()
                } else {
                    account.1.as_str()
                },
                truncate_text_for_reason(signal, 180),
            )
        } else {
            format!(
                "{} matches the ICP for {} in {}",
                account.0, profile.product_name, profile.target_industry
            )
        };
        let why_now = if let Some(signal) = signals.first() {
            truncate_text_for_reason(signal, 180)
        } else {
            format!(
                "Public evidence and reachable contacts indicate a viable outbound window in {}",
                if account.2.trim().is_empty() {
                    profile.target_geo.as_str()
                } else {
                    account.2.as_str()
                }
            )
        };
        let recommended_channel = recommended_activation_channel(conn, account_id, contact_id)
            .unwrap_or_else(|| "research".to_string());
        let thesis_confidence = thesis_confidence(score);
        let thesis_status = match gate {
            SendGateDecision::Activate => "ready",
            SendGateDecision::Research { .. } => "needs_research",
            SendGateDecision::Nurture { .. } => "nurture",
            SendGateDecision::Block { .. } => "blocked",
        };
        let thesis_id = stable_sales_id("thesis", &[account_id]);
        conn.execute(
            "INSERT INTO account_theses
             (id, account_id, why_this_account, why_now, buyer_committee_json, evidence_ids, do_not_say,
              recommended_channel, recommended_pain, thesis_confidence, thesis_status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(id) DO UPDATE SET
                why_this_account = excluded.why_this_account,
                why_now = excluded.why_now,
                buyer_committee_json = excluded.buyer_committee_json,
                evidence_ids = excluded.evidence_ids,
                do_not_say = excluded.do_not_say,
                recommended_channel = excluded.recommended_channel,
                recommended_pain = excluded.recommended_pain,
                thesis_confidence = excluded.thesis_confidence,
                thesis_status = excluded.thesis_status",
            params![
                thesis_id,
                account_id,
                why_this_account,
                why_now,
                serde_json::to_string(&buyer_committee)
                    .map_err(|e| format!("Failed to encode buyer committee: {e}"))?,
                serde_json::to_string(evidence_ids)
                    .map_err(|e| format!("Failed to encode thesis evidence ids: {e}"))?,
                serde_json::json!(["Do not claim internal knowledge beyond public evidence."])
                    .to_string(),
                recommended_channel,
                truncate_text_for_reason(&profile.product_description, 180),
                thesis_confidence,
                thesis_status,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| format!("Failed to upsert account thesis: {e}"))?;
        Ok(thesis_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn sync_canonical_state(
        &self,
        conn: &Connection,
        profile: &SalesProfile,
        candidate: &DomainCandidate,
        company: &str,
        domain: &str,
        website: &str,
        contact_name: Option<&str>,
        contact_title: Option<&str>,
        email: Option<&str>,
        phone: Option<&str>,
        linkedin_url: Option<&str>,
        company_linkedin_url: Option<&str>,
        osint_links: &[String],
        evidence_text: &str,
        reasons: &[String],
    ) -> Result<CanonicalAccountSync, String> {
        let now = Utc::now().to_rfc3339();
        let account_id = stable_sales_id("acct", &[domain]);
        conn.execute(
            "INSERT INTO accounts
             (id, canonical_name, display_name, sector, geo, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'standard', ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                display_name = excluded.display_name,
                sector = COALESCE(NULLIF(accounts.sector, ''), excluded.sector),
                geo = COALESCE(NULLIF(accounts.geo, ''), excluded.geo),
                website = COALESCE(NULLIF(accounts.website, ''), excluded.website),
                updated_at = excluded.updated_at",
            params![
                account_id,
                company,
                company,
                profile.target_industry,
                profile.target_geo,
                website,
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert account: {e}"))?;

        let alias_company = domain_to_company(domain);
        if !alias_company.trim().is_empty() && alias_company != company {
            conn.execute(
                "INSERT OR IGNORE INTO account_aliases (id, account_id, alias_name, alias_type)
                 VALUES (?1, ?2, ?3, 'derived_domain')",
                params![
                    stable_sales_id("acct_alias", &[&account_id, &alias_company]),
                    account_id,
                    alias_company,
                ],
            )
            .map_err(|e| format!("Failed to upsert account alias: {e}"))?;
        }

        conn.execute(
            "INSERT INTO domains (id, account_id, domain, is_primary, verified, mx_valid, checked_at)
             VALUES (?1, ?2, ?3, 1, ?4, NULL, ?5)
             ON CONFLICT(domain) DO UPDATE SET
                account_id = excluded.account_id,
                is_primary = excluded.is_primary,
                verified = excluded.verified,
                checked_at = excluded.checked_at",
            params![
                stable_sales_id("domain", &[domain]),
                account_id,
                domain,
                i32::from(is_valid_company_domain(domain)),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert domain: {e}"))?;

        let cleaned_name = contact_name.and_then(clean_profile_contact_name);
        let cleaned_title = contact_title.and_then(clean_profile_contact_field);
        let contact_id = stable_sales_id(
            "contact",
            &[
                domain,
                &canonical_contact_key(domain, cleaned_name.as_deref(), email, linkedin_url),
            ],
        );
        conn.execute(
            "INSERT INTO contacts
             (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO UPDATE SET
                full_name = COALESCE(excluded.full_name, contacts.full_name),
                title = COALESCE(excluded.title, contacts.title),
                seniority = excluded.seniority,
                name_confidence = MAX(contacts.name_confidence, excluded.name_confidence),
                title_confidence = MAX(contacts.title_confidence, excluded.title_confidence),
                is_decision_maker = MAX(contacts.is_decision_maker, excluded.is_decision_maker)",
            params![
                contact_id,
                account_id,
                cleaned_name,
                cleaned_title,
                seniority_from_title(cleaned_title.as_deref()),
                if cleaned_name.is_some() { 0.9 } else { 0.4 },
                if cleaned_title.is_some() { 0.8 } else { 0.4 },
                i32::from(
                    cleaned_title
                        .as_deref()
                        .map(contact_title_priority)
                        .unwrap_or(0)
                        > 0
                ),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert contact: {e}"))?;

        if let Some(role) = cleaned_title.as_deref().map(infer_buyer_role) {
            conn.execute(
                "INSERT OR IGNORE INTO buyer_roles (id, account_id, contact_id, role_type, inferred_from)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    stable_sales_id("buyer_role", &[&account_id, &contact_id, role]),
                    account_id,
                    contact_id,
                    role,
                    cleaned_title.clone().unwrap_or_default(),
                ],
            )
            .map_err(|e| format!("Failed to upsert buyer role: {e}"))?;
        }

        self.migrate_contact_methods(conn, &contact_id, email, phone, linkedin_url)?;

        for (channel_type, value) in [
            (
                "email",
                email.and_then(|value| normalize_email_candidate(Some(value.to_string()))),
            ),
            ("phone", phone.and_then(normalize_phone)),
            (
                "linkedin",
                linkedin_url.and_then(normalize_outreach_linkedin_url),
            ),
        ] {
            if let Some(value) = value {
                let suppressed = self.is_suppressed(conn, &value)?;
                conn.execute(
                    "UPDATE contact_methods SET suppressed = ?1
                     WHERE contact_id = ?2 AND channel_type = ?3 AND value = ?4",
                    params![i32::from(suppressed), contact_id, channel_type, value],
                )
                .map_err(|e| format!("Failed to update contact method suppression: {e}"))?;
            }
        }

        let mut evidence_ids = Vec::new();
        let primary_source_type = candidate_primary_source_type(candidate, company_linkedin_url);
        let artifact_id = self.upsert_artifact(
            conn,
            primary_source_type,
            domain,
            &truncate_cleaned_text(
                &format!(
                    "{} | {} | {}",
                    evidence_text,
                    candidate.evidence.join(" | "),
                    reasons.join(" | ")
                ),
                2000,
            ),
            &now,
        )?;
        for item in [
            self.upsert_evidence(
                conn,
                &artifact_id,
                "company_name",
                company,
                primary_source_type,
                0.9,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "domain",
                domain,
                primary_source_type,
                0.95,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "website",
                website,
                primary_source_type,
                0.9,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "signal",
                evidence_text,
                primary_source_type,
                0.8,
                &now,
            )?,
        ]
        .into_iter()
        .flatten()
        {
            evidence_ids.push(item);
        }

        if let Some(name) = cleaned_name.as_deref() {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "contact_name",
                name,
                primary_source_type,
                0.8,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(title) = cleaned_title.as_deref() {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "title",
                title,
                primary_source_type,
                0.75,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(email) =
            email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
        {
            if let Some(id) =
                self.upsert_evidence(conn, &artifact_id, "email", &email, "site_html", 0.9, &now)?
            {
                evidence_ids.push(id);
            }
        }
        if let Some(phone) = phone.and_then(normalize_phone) {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "phone",
                &phone,
                primary_source_type,
                0.8,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(linkedin) = linkedin_url.and_then(normalize_outreach_linkedin_url) {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "linkedin",
                &linkedin,
                "web_search",
                0.7,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(company_linkedin) =
            company_linkedin_url.and_then(normalize_company_linkedin_url)
        {
            let company_artifact = self.upsert_artifact(
                conn,
                "web_search",
                &company_linkedin,
                &company_linkedin,
                &now,
            )?;
            if let Some(id) = self.upsert_evidence(
                conn,
                &company_artifact,
                "linkedin",
                &company_linkedin,
                "web_search",
                0.65,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }

        for link in osint_links.iter().take(MAX_OSINT_LINKS_PER_PROSPECT) {
            let osint_artifact = self.upsert_artifact(conn, "web_search", link, link, &now)?;
            if let Some(id) = self.upsert_evidence(
                conn,
                &osint_artifact,
                "signal",
                link,
                "web_search",
                0.55,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }

        let signal_source = if primary_source_type == "directory_listing" {
            "directory_listing"
        } else {
            "web_search"
        };
        let mut signal_texts = candidate.matched_keywords.clone();
        signal_texts.extend(candidate.evidence.iter().take(3).cloned());
        signal_texts.extend(reasons.iter().take(2).cloned());
        signal_texts.push(evidence_text.to_string());
        for signal in dedupe_strings(signal_texts)
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .take(8)
        {
            let signal_type = infer_signal_type(&signal);
            let _ = self.create_signal_with_rationale(
                conn,
                &account_id,
                signal_type,
                &signal,
                signal_source,
                &evidence_ids,
            );
        }

        let score = compute_five_axis_score(&account_id, conn)?;
        conn.execute(
            "UPDATE accounts SET tier = ?2, updated_at = ?3 WHERE id = ?1",
            params![account_id, assign_tier(&score), now],
        )
        .map_err(|e| format!("Failed to update account tier: {e}"))?;
        let gate = send_gate(&score);
        let thesis_id = self.build_account_thesis(
            conn,
            profile,
            &account_id,
            &contact_id,
            &evidence_ids,
            &score,
            &gate,
        )?;

        match &gate {
            SendGateDecision::Research { missing } => {
                self.enqueue_research(
                    conn,
                    &account_id,
                    &missing.join("; "),
                    (activation_priority(&score) * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Activate => {
                self.enqueue_activation(
                    conn,
                    &account_id,
                    &contact_id,
                    &thesis_id,
                    (activation_priority(&score) * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Nurture { reason } => {
                self.enqueue_research(
                    conn,
                    &account_id,
                    reason,
                    (score.fit_score * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Block { .. } => {}
        }

        Ok(CanonicalAccountSync { score, gate })
    }

    fn ensure_touch_for_approval(
        &self,
        conn: &Connection,
        lead: &SalesLead,
        approval_id: &str,
        channel: &str,
        payload_json: &str,
    ) -> Result<(), String> {
        let account_id = stable_sales_id("acct", &[&lead.company_domain]);
        let contact_id = stable_sales_id(
            "contact",
            &[
                &lead.company_domain,
                &canonical_contact_key(
                    &lead.company_domain,
                    clean_profile_contact_name(&lead.contact_name).as_deref(),
                    lead.email.as_deref(),
                    lead.linkedin_url.as_deref(),
                ),
            ],
        );
        let thesis_id = conn
            .query_row(
                "SELECT id FROM account_theses WHERE account_id = ?1 ORDER BY created_at DESC LIMIT 1",
                params![account_id.clone()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load thesis for touch: {e}"))?;
        let template_id = self.ensure_default_sequence_template(conn)?;
        let sequence_instance_id = stable_sales_id(
            "sequence_instance",
            &[
                &account_id,
                &contact_id,
                thesis_id.as_deref().unwrap_or("none"),
            ],
        );
        let now = Utc::now().to_rfc3339();
        let initial_step = if channel == "email" {
            1
        } else if channel == "linkedin_assist" {
            4
        } else {
            2
        };
        conn.execute(
            "INSERT OR IGNORE INTO accounts
             (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params![account_id, lead.company, lead.website, now],
        )
        .map_err(|e| format!("Failed to ensure touch account: {e}"))?;
        conn.execute(
            "INSERT OR IGNORE INTO domains (id, account_id, domain, is_primary, verified, mx_valid, checked_at)
             VALUES (?1, ?2, ?3, 1, 1, NULL, ?4)",
            params![
                stable_sales_id("domain", &[&lead.company_domain]),
                account_id,
                lead.company_domain,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch domain: {e}"))?;
        conn.execute(
            "INSERT OR IGNORE INTO contacts
             (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)",
            params![
                contact_id,
                account_id,
                clean_profile_contact_name(&lead.contact_name),
                clean_profile_contact_field(&lead.contact_title),
                seniority_from_title(Some(&lead.contact_title)),
                0.8,
                0.8,
                i32::from(contact_title_priority(&lead.contact_title) > 0),
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch contact: {e}"))?;
        self.migrate_contact_methods(
            conn,
            &contact_id,
            lead.email.as_deref(),
            lead.phone.as_deref(),
            lead.linkedin_url.as_deref(),
        )?;
        conn.execute(
            "INSERT INTO sequence_instances
             (id, template_id, account_id, contact_id, thesis_id, current_step, status, started_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                updated_at = excluded.updated_at,
                current_step = MIN(sequence_instances.current_step, excluded.current_step)",
            params![
                sequence_instance_id,
                template_id,
                account_id,
                contact_id,
                thesis_id,
                initial_step,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure sequence instance: {e}"))?;

        // Auto-assign to active experiment if one exists (TASK-37)
        if let Ok(exp_id) = conn.query_row(
            "SELECT id FROM experiments WHERE status = 'active' ORDER BY created_at DESC LIMIT 1",
            [],
            |r| r.get::<_, String>(0),
        ) {
            let _ = assign_experiment_variant(conn, &exp_id, &sequence_instance_id);
        }

        let evidence_ids = thesis_id
            .as_deref()
            .and_then(|id| {
                conn.query_row(
                    "SELECT evidence_ids FROM account_theses WHERE id = ?1",
                    params![id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .ok()
                .flatten()
            })
            .unwrap_or_else(|| "[]".to_string());
        let risk_flags = if channel == "linkedin_assist" {
            serde_json::json!(["manual_action"]).to_string()
        } else {
            serde_json::json!([]).to_string()
        };
        conn.execute(
            "INSERT INTO touches
             (id, sequence_instance_id, step, channel, message_payload, claims_json, evidence_ids, variant_id, risk_flags, sent_at, mailbox_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'v1', ?8, NULL, NULL, ?9)
             ON CONFLICT(id) DO UPDATE SET
                message_payload = excluded.message_payload,
                claims_json = excluded.claims_json,
                evidence_ids = excluded.evidence_ids,
                risk_flags = excluded.risk_flags",
            params![
                approval_id,
                sequence_instance_id,
                initial_step,
                channel,
                payload_json,
                serde_json::to_string(&lead.reasons)
                    .map_err(|e| format!("Failed to encode touch claims: {e}"))?,
                evidence_ids,
                risk_flags,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch: {e}"))?;
        Ok(())
    }

    /// Check all active sequence instances and advance/cancel as needed (TASK-30).
    /// Called periodically or after outcome processing.
    fn advance_sequences(&self) -> Result<u32, String> {
        let conn = self.open()?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let mut advanced = 0u32;

        // Fetch active sequences with their template steps
        let mut stmt = conn
            .prepare(
                "SELECT si.id, si.template_id, si.account_id, si.contact_id, si.thesis_id,
                        si.current_step, si.status, st.steps_json,
                        MAX(t.sent_at) as last_touch_sent
                 FROM sequence_instances si
                 JOIN sequence_templates st ON st.id = si.template_id
                 LEFT JOIN touches t ON t.sequence_instance_id = si.id AND t.sent_at IS NOT NULL
                 WHERE si.status = 'active'
                 GROUP BY si.id",
            )
            .map_err(|e| format!("Failed to prepare sequence advancement query: {e}"))?;

        type SeqRow = (
            String,
            String,
            String,
            String,
            Option<String>,
            i32,
            String,
            String,
            Option<String>,
        );
        let sequences: Vec<SeqRow> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            })
            .map_err(|e| format!("Sequence query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        for (
            seq_id,
            _template_id,
            account_id,
            contact_id,
            thesis_id,
            current_step,
            _status,
            steps_json,
            last_sent_at,
        ) in sequences
        {
            let steps: Vec<serde_json::Value> =
                serde_json::from_str(&steps_json).unwrap_or_default();

            // Check if there's a positive outcome that should end the sequence
            let positive_outcome: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM outcomes o
                     JOIN touches t ON t.id = o.touch_id
                     WHERE t.sequence_instance_id = ?1
                     AND o.outcome_type IN ('meeting_booked', 'interested', 'closed_won')",
                    params![seq_id],
                    |r| r.get::<_, i32>(0),
                )
                .unwrap_or(0)
                > 0;

            if positive_outcome {
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'completed', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Check for unsubscribe/hard_bounce → cancel
            let negative_outcome: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM outcomes o
                     JOIN touches t ON t.id = o.touch_id
                     WHERE t.sequence_instance_id = ?1
                     AND o.outcome_type IN ('hard_bounce', 'unsubscribe', 'wrong_person')",
                    params![seq_id],
                    |r| r.get::<_, i32>(0),
                )
                .unwrap_or(0)
                > 0;

            if negative_outcome {
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'cancelled', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Check if enough delay has elapsed for next step
            let total_steps = steps.len() as i32;
            let next_step = current_step + 1;
            if next_step > total_steps {
                // Sequence complete — all steps done
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'completed', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Parse delay_days for next step
            let delay_days = steps
                .get((next_step - 1) as usize)
                .and_then(|s| s.get("delay_days"))
                .and_then(|d| d.as_i64())
                .unwrap_or(3);

            let ready_to_advance = match &last_sent_at {
                Some(sent) => chrono::DateTime::parse_from_rfc3339(sent)
                    .map(|dt| {
                        now.signed_duration_since(dt.with_timezone(&Utc)).num_days() >= delay_days
                    })
                    .unwrap_or(false),
                None => true, // No touch sent yet, first step is ready
            };

            if ready_to_advance {
                let next_channel = steps
                    .get((next_step - 1) as usize)
                    .and_then(|s| s.get("channel"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("email");

                // Advance the step counter
                let _ = conn.execute(
                    "UPDATE sequence_instances SET current_step = ?1, updated_at = ?2 WHERE id = ?3",
                    params![next_step, now_str, seq_id],
                );

                // Queue approval for next touch
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "step": next_step,
                    "channel": next_channel,
                    "account_id": account_id,
                    "contact_id": contact_id,
                    "thesis_id": thesis_id,
                    "sequence_instance_id": seq_id,
                    "auto_generated": true,
                });

                // Find the lead for this account
                let lead_id: String = conn
                    .query_row(
                        "SELECT l.id FROM leads l
                         JOIN domains d ON d.domain = l.company_domain
                         WHERE d.account_id = ?1
                         LIMIT 1",
                        params![account_id],
                        |r| r.get(0),
                    )
                    .unwrap_or_else(|_| format!("seq_{}", seq_id));

                let _ = conn.execute(
                    "INSERT OR IGNORE INTO approvals (id, lead_id, channel, payload_json, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
                    params![approval_id, lead_id, next_channel, payload.to_string(), now_str],
                );

                advanced += 1;
            }
        }

        Ok(advanced)
    }

    /// Get sequence progress for an account.
    #[allow(dead_code)]
    fn get_sequence_progress(&self, account_id: &str) -> Result<Vec<serde_json::Value>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT si.id, si.current_step, si.status, st.steps_json, si.started_at
                 FROM sequence_instances si
                 JOIN sequence_templates st ON st.id = si.template_id
                 WHERE si.account_id = ?1
                 ORDER BY si.started_at DESC",
            )
            .map_err(|e| format!("Failed to query sequence progress: {e}"))?;

        let rows: Vec<serde_json::Value> = stmt
            .query_map(params![account_id], |row| {
                let seq_id: String = row.get(0)?;
                let current_step: i32 = row.get(1)?;
                let status: String = row.get(2)?;
                let steps_json: String = row.get(3)?;
                let started_at: String = row.get(4)?;
                let steps: Vec<serde_json::Value> =
                    serde_json::from_str(&steps_json).unwrap_or_default();
                Ok(serde_json::json!({
                    "sequence_id": seq_id,
                    "current_step": current_step,
                    "total_steps": steps.len(),
                    "status": status,
                    "started_at": started_at,
                    "steps": steps,
                }))
            })
            .map_err(|e| format!("Sequence progress query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    fn insert_lead(&self, lead: &SalesLead) -> Result<bool, String> {
        let conn = self.open()?;
        let reasons_json = serde_json::to_string(&lead.reasons)
            .map_err(|e| format!("Failed to encode reasons: {e}"))?;

        match conn.execute(
            "INSERT INTO leads (id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                lead.id,
                lead.run_id,
                lead.company,
                lead.website,
                lead.company_domain,
                lead.contact_name,
                lead.contact_title,
                lead.linkedin_url,
                lead.email,
                lead.phone,
                reasons_json,
                lead.email_subject,
                lead.email_body,
                lead.linkedin_message,
                lead.score,
                lead.status,
                lead.created_at,
            ],
        ) {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("Lead insert failed: {e}")),
        }
    }

    fn queue_approvals_for_lead(&self, lead: &SalesLead) -> Result<u32, String> {
        let conn = self.open()?;
        let created_at = Utc::now().to_rfc3339();
        let mut queued = 0u32;

        if let Some(email) = &lead.email {
            if !self.approval_already_pending(&conn, "email", email)? {
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "to": email,
                    "subject": lead.email_subject,
                    "body": lead.email_body,
                    "classification": classify_email(email, &lead.company_domain),
                });
                conn.execute(
                    "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
                    params![approval_id, lead.id, payload.to_string(), created_at],
                )
                .map_err(|e| format!("Queue email approval failed: {e}"))?;
                self.ensure_touch_for_approval(
                    &conn,
                    lead,
                    &approval_id,
                    "email",
                    &payload.to_string(),
                )?;
                queued += 1;
            }
        }

        if let Some(linkedin_url) = lead
            .linkedin_url
            .as_deref()
            .and_then(normalize_outreach_linkedin_url)
        {
            if !self.approval_already_pending(&conn, "linkedin_assist", &linkedin_url)? {
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "profile_url": linkedin_url,
                    "message": lead.linkedin_message,
                    "manual_action": true,
                });
                conn.execute(
                    "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'linkedin_assist', ?, 'pending', ?)",
                    params![approval_id, lead.id, payload.to_string(), created_at],
                )
                .map_err(|e| format!("Queue LinkedIn approval failed: {e}"))?;
                self.ensure_touch_for_approval(
                    &conn,
                    lead,
                    &approval_id,
                    "linkedin_assist",
                    &payload.to_string(),
                )?;
                queued += 1;
            }
        }

        Ok(queued)
    }

    pub fn list_runs(&self, limit: usize) -> Result<Vec<SalesRunRecord>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, status, started_at, completed_at, discovered, inserted, approvals_queued, error
                 FROM sales_runs ORDER BY started_at DESC LIMIT ?",
            )
            .map_err(|e| format!("Prepare list runs failed: {e}"))?;

        let mut rows = stmt
            .query(params![limit as i64])
            .map_err(|e| format!("List runs query failed: {e}"))?;

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("List runs row failed: {e}"))?
        {
            let run_id = r.get::<_, String>(0).unwrap_or_default();
            let status = r.get::<_, String>(1).unwrap_or_default();
            let mut discovered = r.get::<_, i64>(4).unwrap_or(0) as u32;
            let mut inserted = r.get::<_, i64>(5).unwrap_or(0) as u32;
            let mut approvals_queued = r.get::<_, i64>(6).unwrap_or(0) as u32;

            if status == "running" {
                inserted = self.count_leads_for_run(&run_id).unwrap_or(inserted);
                let profiled = self
                    .count_prospect_profiles_for_run(&run_id)
                    .unwrap_or(discovered);
                approvals_queued = self
                    .count_approvals_for_run(&run_id)
                    .unwrap_or(approvals_queued);
                discovered = inserted.max(profiled);
            }

            out.push(SalesRunRecord {
                id: run_id,
                status,
                started_at: r.get(2).unwrap_or_default(),
                completed_at: r.get(3).ok(),
                discovered,
                inserted,
                approvals_queued,
                error: r.get(7).ok(),
            });
        }

        Ok(out)
    }

    pub fn list_leads(&self, limit: usize, run_id: Option<&str>) -> Result<Vec<SalesLead>, String> {
        let conn = self.open()?;
        let sql_with_run = "SELECT id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at
                 FROM leads WHERE run_id = ? ORDER BY created_at DESC LIMIT ?";
        let sql_all = "SELECT id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at
                 FROM leads ORDER BY created_at DESC LIMIT ?";

        let mut stmt = conn
            .prepare(if run_id.is_some() {
                sql_with_run
            } else {
                sql_all
            })
            .map_err(|e| format!("Prepare list leads failed: {e}"))?;

        let mut rows = if let Some(rid) = run_id {
            stmt.query(params![rid, limit as i64])
                .map_err(|e| format!("List leads query failed: {e}"))?
        } else {
            stmt.query(params![limit as i64])
                .map_err(|e| format!("List leads query failed: {e}"))?
        };

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("List leads row read failed: {e}"))?
        {
            let reasons_json: String = r.get(10).unwrap_or_else(|_| "[]".to_string());
            let reasons = serde_json::from_str::<Vec<String>>(&reasons_json).unwrap_or_default();
            out.push(SalesLead {
                id: r.get(0).unwrap_or_default(),
                run_id: r.get(1).unwrap_or_default(),
                company: r.get(2).unwrap_or_default(),
                website: r.get(3).unwrap_or_default(),
                company_domain: r.get(4).unwrap_or_default(),
                contact_name: r.get(5).unwrap_or_default(),
                contact_title: r.get(6).unwrap_or_default(),
                linkedin_url: r.get(7).ok(),
                email: r.get(8).ok(),
                phone: r.get(9).ok(),
                reasons,
                email_subject: r.get(11).unwrap_or_default(),
                email_body: r.get(12).unwrap_or_default(),
                linkedin_message: r.get(13).unwrap_or_default(),
                score: r.get::<_, i64>(14).unwrap_or(0) as i32,
                status: r.get(15).unwrap_or_default(),
                created_at: r.get(16).unwrap_or_default(),
            });
        }

        Ok(out)
    }

    pub fn list_prospect_profiles(
        &self,
        limit: usize,
        run_id: Option<&str>,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let sales_profile = self.get_profile().ok().flatten();
        let mut cached = self.list_stored_prospect_profiles(limit, run_id)?;
        if !cached.is_empty() {
            sort_prospect_profiles_for_harness(&mut cached, sales_profile.as_ref());
            cached.truncate(limit);
            return Ok(cached);
        }

        let scan_limit = limit.saturating_mul(12).clamp(200, 4000);
        let leads = self.list_leads(scan_limit, run_id)?;
        Ok(build_prospect_profiles(
            leads,
            limit,
            sales_profile.as_ref(),
        ))
    }

    fn list_stored_prospect_profiles(
        &self,
        limit: usize,
        run_id: Option<&str>,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let conn = self.open()?;
        let sql = if run_id.is_some() {
            "SELECT json FROM prospect_profiles WHERE run_id = ? ORDER BY updated_at DESC LIMIT ?"
        } else {
            "SELECT json FROM prospect_profiles ORDER BY updated_at DESC LIMIT ?"
        };
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("Prepare prospect_profiles list failed: {e}"))?;
        let mut out = Vec::new();
        if let Some(run_id) = run_id {
            let rows = stmt
                .query_map(params![run_id, limit as i64], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Query prospect_profiles by run failed: {e}"))?;
            for row in rows {
                let raw = row.map_err(|e| format!("Read prospect_profiles row failed: {e}"))?;
                if let Ok(mut profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
                    sanitize_prospect_profile(&mut profile);
                    out.push(profile);
                }
            }
        } else {
            let rows = stmt
                .query_map(params![limit as i64], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Query prospect_profiles failed: {e}"))?;
            for row in rows {
                let raw = row.map_err(|e| format!("Read prospect_profiles row failed: {e}"))?;
                if let Ok(mut profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
                    sanitize_prospect_profile(&mut profile);
                    out.push(profile);
                }
            }
        }
        Ok(out)
    }

    fn get_stored_prospect_profile(
        &self,
        company_domain: &str,
    ) -> Result<Option<SalesProspectProfile>, String> {
        let conn = self.open()?;
        let raw = conn
            .query_row(
                "SELECT json FROM prospect_profiles WHERE company_domain = ?",
                params![company_domain],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Prospect profile lookup failed: {e}"))?;
        raw.map(|payload| {
            serde_json::from_str::<SalesProspectProfile>(&payload)
                .map(|mut profile| {
                    sanitize_prospect_profile(&mut profile);
                    profile
                })
                .map_err(|e| format!("Prospect profile JSON decode failed: {e}"))
        })
        .transpose()
    }

    fn upsert_prospect_profiles(&self, profiles: &[SalesProspectProfile]) -> Result<(), String> {
        if profiles.is_empty() {
            return Ok(());
        }

        let conn = self.open()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("Begin prospect_profiles transaction failed: {e}"))?;

        for profile in profiles {
            let mut sanitized = profile.clone();
            sanitize_prospect_profile(&mut sanitized);
            let payload = serde_json::to_string(&sanitized)
                .map_err(|e| format!("Serialize prospect profile failed: {e}"))?;
            tx.execute(
                "INSERT INTO prospect_profiles (company_domain, run_id, json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(company_domain) DO UPDATE SET
                    run_id = excluded.run_id,
                    json = excluded.json,
                    updated_at = excluded.updated_at",
                params![
                    sanitized.company_domain,
                    sanitized.run_id,
                    payload,
                    sanitized.created_at,
                    sanitized.updated_at
                ],
            )
            .map_err(|e| format!("Upsert prospect profile failed: {e}"))?;
        }

        tx.commit()
            .map_err(|e| format!("Commit prospect_profiles transaction failed: {e}"))?;
        Ok(())
    }

    pub fn list_approvals(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SalesApproval>, String> {
        let conn = self.open()?;
        let (sql, args): (&str, Vec<String>) = if let Some(s) = status {
            (
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at FROM approvals WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                vec![s.to_string(), limit.to_string()],
            )
        } else {
            (
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at FROM approvals ORDER BY created_at DESC LIMIT ?",
                vec![limit.to_string()],
            )
        };

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("Prepare approvals query failed: {e}"))?;

        let mut rows = if args.len() == 2 {
            stmt.query(params![args[0], args[1]])
                .map_err(|e| format!("Approvals query failed: {e}"))?
        } else {
            stmt.query(params![args[0]])
                .map_err(|e| format!("Approvals query failed: {e}"))?
        };

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("Approvals row read failed: {e}"))?
        {
            let channel: String = r.get(2).unwrap_or_default();
            let payload_raw: String = r.get(3).unwrap_or_else(|_| "{}".to_string());
            let payload = serde_json::from_str::<serde_json::Value>(&payload_raw)
                .unwrap_or_else(|_| serde_json::json!({}));
            let Some(payload) = sanitize_approval_payload(&channel, payload) else {
                continue;
            };
            out.push(SalesApproval {
                id: r.get(0).unwrap_or_default(),
                lead_id: r.get(1).unwrap_or_default(),
                channel,
                payload,
                status: r.get(4).unwrap_or_default(),
                created_at: r.get(5).unwrap_or_default(),
                decided_at: r.get(6).ok(),
            });
        }

        Ok(out)
    }

    fn get_approval_by_id(
        &self,
        conn: &Connection,
        approval_id: &str,
    ) -> Result<Option<SalesApproval>, String> {
        let row = conn
            .query_row(
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at
                 FROM approvals
                 WHERE id = ?1",
                params![approval_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?;
        let Some((id, lead_id, channel, payload_raw, status, created_at, decided_at)) = row else {
            return Ok(None);
        };
        let payload = serde_json::from_str::<serde_json::Value>(&payload_raw)
            .unwrap_or_else(|_| serde_json::json!({}));
        let payload = sanitize_approval_payload(&channel, payload)
            .ok_or_else(|| "Approval payload is not actionable".to_string())?;
        Ok(Some(SalesApproval {
            id,
            lead_id,
            channel,
            payload,
            status,
            created_at,
            decided_at,
        }))
    }

    fn edit_approval(
        &self,
        approval_id: &str,
        edited_payload: serde_json::Value,
    ) -> Result<SalesApproval, String> {
        let conn = self.open()?;
        let approval = self
            .get_approval_by_id(&conn, approval_id)?
            .ok_or_else(|| "Approval not found".to_string())?;
        if approval.status != "pending" {
            return Err(format!(
                "Approval is not editable (current status: {})",
                approval.status
            ));
        }
        let sanitized = sanitize_approval_payload(&approval.channel, edited_payload)
            .ok_or_else(|| "Edited payload is invalid or non-actionable".to_string())?;
        let payload_json = sanitized.to_string();
        conn.execute(
            "UPDATE approvals SET payload_json = ?2 WHERE id = ?1",
            params![approval_id, payload_json],
        )
        .map_err(|e| format!("Failed to update approval payload: {e}"))?;
        let _ = conn.execute(
            "UPDATE touches SET message_payload = ?2 WHERE id = ?1",
            params![approval_id, payload_json],
        );
        self.get_approval_by_id(&conn, approval_id)?
            .ok_or_else(|| "Approval disappeared after update".to_string())
    }

    fn resolve_account_id(
        &self,
        conn: &Connection,
        account_ref: &str,
    ) -> Result<Option<String>, String> {
        let trimmed = account_ref.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        if let Some(id) = conn
            .query_row(
                "SELECT id FROM accounts WHERE id = ?1",
                params![trimmed],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Account lookup by id failed: {e}"))?
        {
            return Ok(Some(id));
        }
        let lowered = trimmed.to_lowercase();
        if let Some(id) = conn
            .query_row(
                "SELECT account_id FROM domains WHERE domain = ?1",
                params![lowered],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Account lookup by domain failed: {e}"))?
        {
            return Ok(Some(id));
        }
        let derived = stable_sales_id("acct", &[lowered.as_str()]);
        conn.query_row(
            "SELECT id FROM accounts WHERE id = ?1",
            params![derived.clone()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| format!("Account lookup by derived id failed: {e}"))
    }

    fn fallback_dossier_from_prospect(&self, profile: &SalesProspectProfile) -> serde_json::Value {
        let email_classification = profile
            .primary_email
            .as_deref()
            .map(|email| classify_email(email, &profile.company_domain))
            .unwrap_or("unknown");
        let reachability_score: f64 = (if email_classification == "personal" {
            0.35_f64
        } else {
            0.0_f64
        }) + (if profile.primary_linkedin_url.is_some() {
            0.25_f64
        } else {
            0.0_f64
        }) + (if profile.primary_email.is_some() {
            0.1_f64
        } else {
            0.0_f64
        }) + (if profile.primary_contact_name.is_some() {
            0.1_f64
        } else {
            0.0_f64
        }) + (if profile.primary_contact_title.is_some() {
            0.1_f64
        } else {
            0.0_f64
        });
        let score = FiveAxisScore {
            fit_score: (profile.fit_score as f64 / 100.0).clamp(0.0, 1.0),
            intent_score: (0.12 * profile.matched_signals.len() as f64).clamp(0.0, 0.55),
            reachability_score: reachability_score.clamp(0.0, 1.0),
            deliverability_risk: if profile.primary_email.is_some() {
                if email_classification == "personal" {
                    0.18
                } else {
                    0.42
                }
            } else {
                0.35
            },
            compliance_risk: if email_classification == "personal" {
                0.1
            } else if profile.primary_email.is_some() {
                0.3
            } else {
                0.18
            },
        };
        let gate = send_gate(&score);
        let next_action = match &gate {
            SendGateDecision::Activate => {
                if profile.primary_email.is_some() {
                    "Send email now".to_string()
                } else if profile.primary_linkedin_url.is_some() {
                    "Open LinkedIn operator-assist task".to_string()
                } else {
                    "Promote to activation after one more verification pass".to_string()
                }
            }
            SendGateDecision::Research { .. } => "Research needed".to_string(),
            SendGateDecision::Nurture { .. } => {
                "Hold in nurture until a stronger timing signal appears".to_string()
            }
            SendGateDecision::Block { .. } => "Blocked until risk is reduced".to_string(),
        };
        let methods = [
            profile.primary_email.as_ref().map(|email| {
                serde_json::json!({
                    "channel_type": "email",
                    "value": email,
                    "classification": email_classification,
                    "confidence": profile.research_confidence,
                    "suppressed": false,
                })
            }),
            profile.primary_linkedin_url.as_ref().map(|url| {
                serde_json::json!({
                    "channel_type": "linkedin",
                    "value": url,
                    "classification": "personal",
                    "confidence": profile.research_confidence,
                    "suppressed": false,
                })
            }),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        serde_json::json!({
            "account_id": stable_sales_id("acct", &[profile.company_domain.as_str()]),
            "account": {
                "canonical_name": profile.company,
                "display_name": profile.company,
                "website": profile.website,
                "domain": profile.company_domain,
                "tier": if profile.fit_score >= 80 { "a_tier" } else if profile.fit_score >= 55 { "standard" } else { "basic" },
            },
            "score": {
                "fit_score": score.fit_score,
                "intent_score": score.intent_score,
                "reachability_score": score.reachability_score,
                "deliverability_risk": score.deliverability_risk,
                "compliance_risk": score.compliance_risk,
                "activation_priority": activation_priority(&score),
                "send_gate": gate,
            },
            "thesis": {
                "why_this_account": profile.summary,
                "why_now": profile.trigger_events.first().cloned().unwrap_or_else(|| "Awaiting more public evidence.".to_string()),
                "buyer_committee": (profile.buyer_roles.iter().map(|role| serde_json::json!({
                    "role": role,
                    "name": profile.primary_contact_name.clone().unwrap_or_default(),
                    "confidence": profile.research_confidence,
                })).collect::<Vec<_>>()),
                "do_not_say": vec!["Do not claim private knowledge beyond public evidence."],
                "recommended_channel": profile.recommended_channel,
                "recommended_pain": profile.outreach_angle,
                "thesis_confidence": profile.research_confidence,
                "thesis_status": if profile.research_confidence >= 0.6 { "heuristic_ready" } else { "needs_research" },
            },
            "signals": profile.matched_signals.iter().map(|signal| serde_json::json!({
                "signal_type": infer_signal_type(signal),
                "text": signal,
                "effect_horizon": classify_signal_horizon(infer_signal_type(signal), signal).0,
                "confidence": profile.research_confidence,
                "why_it_matters": generate_signal_rationale(infer_signal_type(signal), signal),
            })).collect::<Vec<_>>(),
            "contacts": [{
                "full_name": profile.primary_contact_name,
                "title": profile.primary_contact_title,
                "is_decision_maker": profile.primary_contact_title.as_deref().map(contact_title_priority).unwrap_or(0) > 0,
                "methods": methods,
            }],
            "outcomes": {
                "touches_sent": 0,
                "positive_replies": 0,
                "meetings": 0,
                "opens": 0,
                "clicks": 0,
                "hard_bounces": 0,
                "unsubscribes": 0,
                "positive_reply_rate": 0.0,
                "meeting_rate": 0.0,
            },
            "next_action": next_action,
            "source": "prospect_profile_fallback",
        })
    }

    fn get_account_dossier(&self, account_ref: &str) -> Result<Option<serde_json::Value>, String> {
        let conn = self.open()?;
        let Some(account_id) = self.resolve_account_id(&conn, account_ref)? else {
            return Ok(self
                .get_stored_prospect_profile(account_ref)?
                .map(|profile| self.fallback_dossier_from_prospect(&profile)));
        };

        let account = conn
            .query_row(
                "SELECT canonical_name, COALESCE(display_name, canonical_name), COALESCE(website, ''), COALESCE(sector, ''),
                        COALESCE(geo, ''), COALESCE(tier, 'standard')
                 FROM accounts
                 WHERE id = ?1",
                params![account_id.clone()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load account dossier: {e}"))?;
        let Some(account) = account else {
            return Ok(self
                .get_stored_prospect_profile(account_ref)?
                .map(|profile| self.fallback_dossier_from_prospect(&profile)));
        };

        let domain = conn
            .query_row(
                "SELECT domain FROM domains WHERE account_id = ?1 ORDER BY is_primary DESC, checked_at DESC LIMIT 1",
                params![account_id.clone()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load account domain: {e}"))?
            .unwrap_or_default();

        let score = conn
            .query_row(
                "SELECT fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk
                 FROM score_snapshots
                 WHERE account_id = ?1
                 ORDER BY computed_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(FiveAxisScore {
                        fit_score: row.get::<_, f64>(0).unwrap_or(0.0),
                        intent_score: row.get::<_, f64>(1).unwrap_or(0.0),
                        reachability_score: row.get::<_, f64>(2).unwrap_or(0.0),
                        deliverability_risk: row.get::<_, f64>(3).unwrap_or(0.0),
                        compliance_risk: row.get::<_, f64>(4).unwrap_or(0.0),
                    })
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load dossier score: {e}"))?
            .unwrap_or(compute_five_axis_score(&account_id, &conn)?);
        let gate = send_gate(&score);

        let thesis = conn
            .query_row(
                "SELECT why_this_account, COALESCE(why_now, ''), COALESCE(buyer_committee_json, '[]'),
                        COALESCE(do_not_say, '[]'), COALESCE(recommended_channel, ''),
                        COALESCE(recommended_pain, ''), thesis_confidence, thesis_status
                 FROM account_theses
                 WHERE account_id = ?1
                 ORDER BY created_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(serde_json::json!({
                        "why_this_account": row.get::<_, String>(0)?,
                        "why_now": row.get::<_, String>(1)?,
                        "buyer_committee": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(2)?).unwrap_or_else(|_| serde_json::json!([])),
                        "do_not_say": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?).unwrap_or_else(|_| serde_json::json!([])),
                        "recommended_channel": row.get::<_, String>(4)?,
                        "recommended_pain": row.get::<_, String>(5)?,
                        "thesis_confidence": row.get::<_, f64>(6).unwrap_or(0.0),
                        "thesis_status": row.get::<_, String>(7)?,
                    }))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load account thesis: {e}"))?
            .unwrap_or_else(|| serde_json::json!({
                "why_this_account": account.0.clone(),
                "why_now": "Thesis not generated yet.",
                "buyer_committee": [],
                "do_not_say": [],
                "recommended_channel": "",
                "recommended_pain": "",
                "thesis_confidence": 0.0,
                "thesis_status": "missing",
            }));

        let mut signal_stmt = conn
            .prepare(
                "SELECT s.id, s.signal_type, s.text, COALESCE(s.effect_horizon, ''), s.confidence,
                        COALESCE(sr.why_it_matters, ''), COALESCE(sr.expected_effect, ''), COALESCE(sr.evidence_ids, '[]')
                 FROM signals s
                 LEFT JOIN signal_rationales sr ON sr.signal_id = s.id
                 WHERE s.account_id = ?1
                 ORDER BY s.confidence DESC, COALESCE(s.observed_at, s.created_at) DESC
                 LIMIT 6",
            )
            .map_err(|e| format!("Failed to prepare dossier signals query: {e}"))?;
        let signals = signal_stmt
            .query_map(params![account_id.clone()], |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "signal_type": row.get::<_, String>(1)?,
                    "text": row.get::<_, String>(2)?,
                    "effect_horizon": row.get::<_, String>(3)?,
                    "confidence": row.get::<_, f64>(4).unwrap_or(0.0),
                    "why_it_matters": row.get::<_, String>(5)?,
                    "expected_effect": row.get::<_, String>(6)?,
                    "evidence_ids": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(7)?).unwrap_or_else(|_| serde_json::json!([])),
                }))
            })
            .map_err(|e| format!("Failed to query dossier signals: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode dossier signals: {e}"))?;

        let mut contact_stmt = conn
            .prepare(
                "SELECT id, COALESCE(full_name, ''), COALESCE(title, ''), COALESCE(seniority, ''),
                        name_confidence, title_confidence, COALESCE(is_decision_maker, 0)
                 FROM contacts
                 WHERE account_id = ?1
                 ORDER BY is_decision_maker DESC, name_confidence DESC, created_at ASC
                 LIMIT 8",
            )
            .map_err(|e| format!("Failed to prepare dossier contacts query: {e}"))?;
        let contact_rows = contact_stmt
            .query_map(params![account_id.clone()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, f64>(4).unwrap_or(0.0),
                    row.get::<_, f64>(5).unwrap_or(0.0),
                    row.get::<_, i64>(6).unwrap_or(0),
                ))
            })
            .map_err(|e| format!("Failed to query dossier contacts: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode dossier contacts: {e}"))?;
        let mut contacts = Vec::new();
        for (
            contact_id,
            full_name,
            title,
            seniority,
            name_confidence,
            title_confidence,
            is_decision_maker,
        ) in contact_rows
        {
            let mut method_stmt = conn
                .prepare(
                    "SELECT channel_type, value, COALESCE(classification, ''), confidence, verified_at, COALESCE(suppressed, 0)
                     FROM contact_methods
                     WHERE contact_id = ?1
                     ORDER BY confidence DESC, channel_type ASC",
                )
                .map_err(|e| format!("Failed to prepare dossier contact methods query: {e}"))?;
            let methods = method_stmt
                .query_map(params![contact_id.clone()], |row| {
                    Ok(serde_json::json!({
                        "channel_type": row.get::<_, String>(0)?,
                        "value": row.get::<_, String>(1)?,
                        "classification": row.get::<_, String>(2)?,
                        "confidence": row.get::<_, f64>(3).unwrap_or(0.0),
                        "verified_at": row.get::<_, Option<String>>(4)?,
                        "suppressed": row.get::<_, i64>(5).unwrap_or(0) == 1,
                    }))
                })
                .map_err(|e| format!("Failed to query dossier contact methods: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode dossier contact methods: {e}"))?;
            contacts.push(serde_json::json!({
                "id": contact_id,
                "full_name": full_name,
                "title": title,
                "seniority": seniority,
                "name_confidence": name_confidence,
                "title_confidence": title_confidence,
                "is_decision_maker": is_decision_maker == 1,
                "methods": methods,
            }));
        }

        let (touches_sent, positive_replies, meetings, opens, clicks, hard_bounces, unsubscribes) = conn
            .query_row(
                "SELECT
                    COUNT(DISTINCT CASE WHEN t.sent_at IS NOT NULL THEN t.id END),
                    SUM(CASE WHEN o.outcome_type IN ('interested', 'meeting_booked') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'meeting_booked' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'open' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'click' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'hard_bounce' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'unsubscribe' THEN 1 ELSE 0 END)
                 FROM sequence_instances si
                 LEFT JOIN touches t ON t.sequence_instance_id = si.id
                 LEFT JOIN outcomes o ON o.touch_id = t.id
                 WHERE si.account_id = ?1",
                params![account_id.clone()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0).unwrap_or(0),
                        row.get::<_, i64>(1).unwrap_or(0),
                        row.get::<_, i64>(2).unwrap_or(0),
                        row.get::<_, i64>(3).unwrap_or(0),
                        row.get::<_, i64>(4).unwrap_or(0),
                        row.get::<_, i64>(5).unwrap_or(0),
                        row.get::<_, i64>(6).unwrap_or(0),
                    ))
                },
            )
            .map_err(|e| format!("Failed to load dossier outcomes: {e}"))?;
        let sent_denom = if touches_sent <= 0 {
            1.0
        } else {
            touches_sent as f64
        };
        let next_action = match &gate {
            SendGateDecision::Activate => format!(
                "Send {} now",
                thesis
                    .get("recommended_channel")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .unwrap_or("outreach")
            ),
            SendGateDecision::Research { missing } => {
                format!("Research needed: {}", missing.join("; "))
            }
            SendGateDecision::Nurture { reason } => reason.clone(),
            SendGateDecision::Block { reason } => reason.clone(),
        };

        Ok(Some(serde_json::json!({
            "account_id": account_id,
            "account": {
                "canonical_name": account.0,
                "display_name": account.1,
                "website": account.2,
                "sector": account.3,
                "geo": account.4,
                "tier": account.5,
                "domain": domain,
            },
            "score": {
                "fit_score": score.fit_score,
                "intent_score": score.intent_score,
                "reachability_score": score.reachability_score,
                "deliverability_risk": score.deliverability_risk,
                "compliance_risk": score.compliance_risk,
                "activation_priority": activation_priority(&score),
                "send_gate": gate,
            },
            "thesis": thesis,
            "signals": signals,
            "contacts": contacts,
            "outcomes": {
                "touches_sent": touches_sent,
                "positive_replies": positive_replies,
                "meetings": meetings,
                "opens": opens,
                "clicks": clicks,
                "hard_bounces": hard_bounces,
                "unsubscribes": unsubscribes,
                "positive_reply_rate": (positive_replies as f64 / sent_denom).clamp(0.0, 1.0),
                "meeting_rate": (meetings as f64 / sent_denom).clamp(0.0, 1.0),
            },
            "next_action": next_action,
            "source": "canonical_core",
        })))
    }

    pub fn list_deliveries(&self, limit: usize) -> Result<Vec<SalesDelivery>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, approval_id, channel, recipient, status, error, sent_at FROM deliveries ORDER BY sent_at DESC LIMIT ?",
            )
            .map_err(|e| format!("Prepare deliveries query failed: {e}"))?;

        let mut rows = stmt
            .query(params![limit as i64])
            .map_err(|e| format!("Deliveries query failed: {e}"))?;

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("Deliveries row read failed: {e}"))?
        {
            out.push(SalesDelivery {
                id: r.get(0).unwrap_or_default(),
                approval_id: r.get(1).unwrap_or_default(),
                channel: r.get(2).unwrap_or_default(),
                recipient: r.get(3).unwrap_or_default(),
                status: r.get(4).unwrap_or_default(),
                error: r.get(5).ok(),
                sent_at: r.get(6).unwrap_or_default(),
            });
        }

        Ok(out)
    }

    fn deliveries_today(&self, timezone_mode: &str) -> Result<u32, String> {
        let conn = self.open()?;
        let today = current_sales_day(timezone_mode);
        let mut stmt = conn
            .prepare("SELECT sent_at FROM deliveries WHERE status = 'sent'")
            .map_err(|e| format!("Deliveries count prepare failed: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Deliveries count query failed: {e}"))?;

        let mut count = 0u32;
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("Deliveries count row failed: {e}"))?
        {
            let sent_at: String = row.get(0).unwrap_or_default();
            if timestamp_matches_sales_day(&sent_at, today, timezone_mode) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Load sender config from DB sender_policies table if available.
    fn load_sender_config(&self) -> SenderConfig {
        let conn = match self.open() {
            Ok(c) => c,
            Err(_) => return SenderConfig::default(),
        };
        let pool_json: String = conn
            .query_row(
                "SELECT COALESCE(mailbox_pool, '[]') FROM sender_policies ORDER BY rowid DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| "[]".to_string());
        SenderConfig {
            mailboxes: mailbox_pool_from_json(&pool_json),
        }
    }

    fn save_sender_config(&self, sender_cfg: &SenderConfig) -> Result<(), String> {
        let conn = self.open()?;
        let row_id = conn
            .query_row(
                "SELECT id FROM sender_policies ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load sender policy row: {e}"))?
            .unwrap_or_else(|| "default_sender_policy".to_string());
        let total_daily_cap = sender_cfg
            .mailboxes
            .iter()
            .map(|mailbox| mailbox.daily_cap.max(1) as i64)
            .sum::<i64>()
            .max(1);
        let subdomain = sender_cfg
            .mailboxes
            .iter()
            .find_map(|mailbox| email_domain(&mailbox.email))
            .unwrap_or_default();
        let mailbox_pool = serde_json::to_string(&sender_cfg.mailboxes)
            .map_err(|e| format!("Failed to encode sender mailbox pool: {e}"))?;
        conn.execute(
            "INSERT INTO sender_policies (id, icp_id, mailbox_pool, daily_cap, subdomain, warm_state)
             VALUES (?1, NULL, ?2, ?3, ?4, 'warming')
             ON CONFLICT(id) DO UPDATE SET
                mailbox_pool = excluded.mailbox_pool,
                daily_cap = excluded.daily_cap,
                subdomain = CASE
                    WHEN COALESCE(sender_policies.subdomain, '') = '' THEN excluded.subdomain
                    ELSE sender_policies.subdomain
                END",
            params![row_id, mailbox_pool, total_daily_cap, subdomain],
        )
        .map_err(|e| format!("Failed to persist sender policy config: {e}"))?;
        Ok(())
    }

    fn record_mailbox_send(&self, from_email: &str) -> Result<(), String> {
        let normalized_email = normalize_mailbox_address(from_email);
        if normalized_email.is_empty() {
            return Ok(());
        }

        let mut sender_cfg = self.load_sender_config();
        if sender_cfg.mailboxes.is_empty() {
            return Ok(());
        }

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let mut changed = false;
        for mailbox in &mut sender_cfg.mailboxes {
            if mailbox.counter_date != today {
                mailbox.sends_today = 0;
                mailbox.counter_date = today.clone();
                changed = true;
            }
            if normalize_mailbox_address(&mailbox.email) == normalized_email {
                mailbox.sends_today = mailbox.sends_today.saturating_add(1);
                mailbox.counter_date = today.clone();
                changed = true;
                break;
            }
        }

        if changed {
            self.save_sender_config(&sender_cfg)?;
        }
        Ok(())
    }

    async fn send_email(
        &self,
        state: &AppState,
        profile: &SalesProfile,
        to: &str,
        subject: &str,
        body: &str,
    ) -> Result<(), String> {
        // Try mailbox pool first (TASK-13), fall back to global config
        let mut sender_cfg = self.load_sender_config();
        let selected_mailbox = sender_cfg.select_mailbox().cloned();
        let (smtp_host, smtp_port, smtp_user, smtp_pass, from_email, used_mailbox_pool) =
            if let Some(mb) = selected_mailbox {
                let pass_env = if mb.smtp_pass_env.is_empty() {
                    // Fall through to global config
                    None
                } else {
                    std::env::var(&mb.smtp_pass_env).ok()
                };
                if let Some(pass) = pass_env {
                    let host = mb.smtp_host.clone();
                    let port = mb.smtp_port;
                    let user = if mb.smtp_user.is_empty() {
                        mb.email.clone()
                    } else {
                        mb.smtp_user.clone()
                    };
                    let email = mb.email.clone();
                    (host, port, user, pass, email, true)
                } else {
                    let (host, port, user, pass, email) =
                        self.resolve_global_email_config(state).await?;
                    (host, port, user, pass, email, false)
                }
            } else {
                let (host, port, user, pass, email) =
                    self.resolve_global_email_config(state).await?;
                (host, port, user, pass, email, false)
            };

        let from: Mailbox = from_email
            .parse()
            .map_err(|e| format!("Invalid sender email '{}': {e}", from_email))?;
        let recipient_email = to.trim().to_string();
        let to: Mailbox = to
            .parse()
            .map_err(|e| format!("Invalid recipient email '{to}': {e}"))?;

        let sender_domain = email_domain(&from_email)
            .ok_or_else(|| "Configured sender mailbox is invalid".to_string())?;
        let brand_domain = email_domain(&profile.sender_email)
            .ok_or_else(|| "Sales profile sender_email is invalid".to_string())?;
        if !is_valid_sending_subdomain(&sender_domain, &brand_domain) {
            return Err(format!(
                "Refusing to send from main domain '{}'; configure a sending subdomain for '{}'",
                sender_domain, brand_domain
            ));
        }

        let unsubscribe_url = format!(
            "{}/api/sales/unsubscribe?token={}",
            sales_base_url(&state.kernel),
            generate_unsubscribe_token(&recipient_email, &from_email)
        );

        let msg = Message::builder()
            .from(from)
            .to(to)
            .subject(subject)
            .header(ListUnsubscribeHeader(format!("<{}>", unsubscribe_url)))
            .header(ListUnsubscribePostHeader(
                "List-Unsubscribe=One-Click".to_string(),
            ))
            .body(body.to_string())
            .map_err(|e| format!("Failed to build email message: {e}"))?;

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp_host)
            .map_err(|e| format!("Failed to initialize SMTP relay '{}': {e}", smtp_host))?
            .port(smtp_port)
            .credentials(Credentials::new(smtp_user, smtp_pass))
            .build();

        transport
            .send(msg)
            .await
            .map_err(|e| format!("SMTP send failed: {e}"))?;

        if used_mailbox_pool {
            self.record_mailbox_send(&from_email)?;
        }

        Ok(())
    }

    /// Resolve SMTP config from global email channel configuration.
    async fn resolve_global_email_config(
        &self,
        state: &AppState,
    ) -> Result<(String, u16, String, String, String), String> {
        let channels = state.channels_config.read().await;
        let cfg = channels
            .email
            .as_ref()
            .ok_or_else(|| "Email channel is not configured".to_string())?;
        let password = std::env::var(&cfg.password_env)
            .map_err(|_| format!("Email password env '{}' is not set", cfg.password_env))?;
        Ok((
            cfg.smtp_host.clone(),
            cfg.smtp_port,
            cfg.username.clone(),
            password,
            cfg.username.clone(),
        ))
    }

    async fn send_linkedin(
        &self,
        approval_id: &str,
        profile_url: &str,
        _message: &str,
    ) -> Result<(), String> {
        self.record_delivery(
            approval_id,
            "linkedin_assist",
            profile_url,
            "operator_pending",
            None,
        )
    }

    fn record_delivery(
        &self,
        approval_id: &str,
        channel: &str,
        recipient: &str,
        status: &str,
        error_msg: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let sent_at = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO deliveries (id, approval_id, channel, recipient, status, error, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                uuid::Uuid::new_v4().to_string(),
                approval_id,
                channel,
                recipient,
                status,
                error_msg,
                sent_at,
            ],
        )
        .map_err(|e| format!("Failed to record delivery: {e}"))?;
        let _ = conn.execute(
            "UPDATE touches SET sent_at = COALESCE(sent_at, ?2) WHERE id = ?1",
            params![approval_id, sent_at],
        );
        let _ = conn.execute(
            "UPDATE sequence_instances
             SET updated_at = ?2
             WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
            params![approval_id, Utc::now().to_rfc3339()],
        );
        Ok(())
    }

    fn update_approval_status(&self, approval_id: &str, status: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE approvals SET status = ?, decided_at = ? WHERE id = ?",
            params![status, Utc::now().to_rfc3339(), approval_id],
        )
        .map_err(|e| format!("Failed to update approval status: {e}"))?;
        Ok(())
    }

    fn ingest_outcome_event(
        &self,
        delivery_id: &str,
        event_type: &str,
        raw_text: &str,
    ) -> Result<serde_json::Value, String> {
        let conn = self.open()?;
        let delivery = conn
            .query_row(
                "SELECT approval_id, channel, recipient FROM deliveries WHERE id = ?1",
                params![delivery_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to look up delivery: {e}"))?
            .ok_or_else(|| "Delivery not found".to_string())?;

        let approval = conn
            .query_row(
                "SELECT a.id, a.channel, a.payload_json, l.id, l.run_id, l.company, l.website, l.company_domain,
                        l.contact_name, l.contact_title, l.linkedin_url, l.email, l.phone, l.reasons_json,
                        l.email_subject, l.email_body, l.linkedin_message, l.score, l.status, l.created_at
                 FROM approvals a
                 JOIN leads l ON l.id = a.lead_id
                 WHERE a.id = ?1",
                params![delivery.0.clone()],
                |row| {
                    let reasons_raw = row.get::<_, String>(13)?;
                    let reasons = serde_json::from_str::<Vec<String>>(&reasons_raw).unwrap_or_default();
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        SalesLead {
                            id: row.get::<_, String>(3)?,
                            run_id: row.get::<_, String>(4)?,
                            company: row.get::<_, String>(5)?,
                            website: row.get::<_, String>(6)?,
                            company_domain: row.get::<_, String>(7)?,
                            contact_name: row.get::<_, String>(8)?,
                            contact_title: row.get::<_, String>(9)?,
                            linkedin_url: row.get::<_, Option<String>>(10)?,
                            email: row.get::<_, Option<String>>(11)?,
                            phone: row.get::<_, Option<String>>(12)?,
                            reasons,
                            email_subject: row.get::<_, String>(14)?,
                            email_body: row.get::<_, String>(15)?,
                            linkedin_message: row.get::<_, String>(16)?,
                            score: row.get::<_, i64>(17)? as i32,
                            status: row.get::<_, String>(18)?,
                            created_at: row.get::<_, String>(19)?,
                        },
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load approval/lead for outcome: {e}"))?
            .ok_or_else(|| "Approval/lead not found for delivery".to_string())?;

        self.ensure_touch_for_approval(&conn, &approval.3, &approval.0, &approval.1, &approval.2)?;

        let outcome = classify_outcome(raw_text, event_type, &approval.0);
        let outcome_id =
            stable_sales_id("outcome", &[delivery_id, event_type, &outcome.outcome_type]);
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, raw_text, classified_at, classifier_confidence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
                raw_text = excluded.raw_text,
                classified_at = excluded.classified_at,
                classifier_confidence = excluded.classifier_confidence",
            params![
                outcome_id,
                outcome.touch_id,
                outcome.outcome_type,
                outcome.raw_text,
                Utc::now().to_rfc3339(),
                outcome.classifier_confidence,
            ],
        )
        .map_err(|e| format!("Failed to persist outcome: {e}"))?;

        let account_id = stable_sales_id("acct", &[&approval.3.company_domain]);
        let snapshot_id = stable_sales_id("outcome_snapshot", &[delivery_id, event_type]);
        let score_at_touch_json = conn
            .query_row(
                "SELECT fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk
                 FROM score_snapshots
                 WHERE account_id = ?1
                 ORDER BY computed_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(serde_json::json!({
                        "fit_score": row.get::<_, f64>(0).unwrap_or(0.0),
                        "intent_score": row.get::<_, f64>(1).unwrap_or(0.0),
                        "reachability_score": row.get::<_, f64>(2).unwrap_or(0.0),
                        "deliverability_risk": row.get::<_, f64>(3).unwrap_or(0.0),
                        "compliance_risk": row.get::<_, f64>(4).unwrap_or(0.0),
                    }))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load score snapshot for outcome: {e}"))?
            .unwrap_or_else(|| serde_json::json!({}));
        let active_signal_ids = {
            let mut stmt = conn
                .prepare(
                    "SELECT id FROM signals WHERE account_id = ?1 ORDER BY confidence DESC LIMIT 8",
                )
                .map_err(|e| format!("Failed to prepare signal snapshot query: {e}"))?;
            let rows = stmt
                .query_map(params![account_id.clone()], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Failed to query signal snapshot ids: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode signal snapshot ids: {e}"))?;
            rows
        };
        let thesis_id = conn
            .query_row(
                "SELECT thesis_id FROM sequence_instances
                 WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                params![approval.0.clone()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load thesis id for outcome snapshot: {e}"))?
            .flatten();
        conn.execute(
            "INSERT INTO outcome_attribution_snapshots
             (id, touch_id, account_id, snapshot_at, score_at_touch_json, active_signal_ids, unused_signal_ids,
              thesis_id, sequence_variant, message_variant, channel, mailbox_id, contextual_factors_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, '[]', ?7, 'default', 'v1', ?8, NULL, ?9)
             ON CONFLICT(id) DO UPDATE SET
                score_at_touch_json = excluded.score_at_touch_json,
                active_signal_ids = excluded.active_signal_ids,
                thesis_id = excluded.thesis_id,
                channel = excluded.channel,
                contextual_factors_json = excluded.contextual_factors_json",
            params![
                snapshot_id,
                approval.0.clone(),
                account_id.clone(),
                Utc::now().to_rfc3339(),
                score_at_touch_json.to_string(),
                serde_json::to_string(&active_signal_ids)
                    .map_err(|e| format!("Failed to encode active signal ids: {e}"))?,
                thesis_id,
                delivery.1,
                serde_json::json!({
                    "delivery_id": delivery_id,
                    "recipient": delivery.2,
                    "event_type": event_type,
                })
                .to_string(),
            ],
        )
        .map_err(|e| format!("Failed to persist outcome attribution snapshot: {e}"))?;
        self.record_missed_signal_review(
            &conn,
            &outcome_id,
            &snapshot_id,
            &account_id,
            &outcome.outcome_type,
            &active_signal_ids,
            &Vec::new(),
        )?;

        match outcome.outcome_type.as_str() {
            "hard_bounce" => {
                self.suppress_contact(&conn, &delivery.2, "hard_bounce", false, Some(&outcome_id))?;
            }
            "unsubscribe" => {
                self.suppress_contact(&conn, &delivery.2, "unsubscribe", true, Some(&outcome_id))?;
            }
            "wrong_person" => {
                self.enqueue_research(
                    &conn,
                    &account_id,
                    "Wrong person outcome; find alternate contact",
                    95,
                )?;
            }
            "meeting_booked" => {
                let _ = conn.execute(
                    "UPDATE sequence_instances
                     SET status = 'completed', updated_at = ?2
                     WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                    params![approval.0.clone(), Utc::now().to_rfc3339()],
                );
            }
            "interested" | "open" | "click" | "not_now" => {
                let _ = conn.execute(
                    "UPDATE sequence_instances
                     SET status = 'active', updated_at = ?2
                     WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                    params![approval.0.clone(), Utc::now().to_rfc3339()],
                );
            }
            _ => {}
        }
        if matches!(outcome.outcome_type.as_str(), "hard_bounce" | "unsubscribe") {
            conn.execute(
                "UPDATE contact_methods SET suppressed = 1 WHERE value = ?1",
                params![delivery.2.trim().to_lowercase()],
            )
            .map_err(|e| format!("Failed to update suppressed contact method: {e}"))?;
        }

        // Trigger score calibration when enough outcomes accumulate (TASK-36)
        if let Ok(proposals) = calibrate_scoring_from_outcomes(&conn) {
            for proposal in &proposals {
                info!(proposal = %proposal, "Score calibration proposal created");
            }
        }

        Ok(serde_json::json!({
            "delivery_id": delivery_id,
            "touch_id": approval.0,
            "account_id": account_id,
            "outcome_type": outcome.outcome_type,
            "recipient": delivery.2,
        }))
    }

    pub async fn approve_and_send(
        &self,
        state: &AppState,
        approval_id: &str,
    ) -> Result<serde_json::Value, String> {
        let row = {
            let conn = self.open()?;
            conn.query_row(
                "SELECT id, channel, payload_json, status FROM approvals WHERE id = ?",
                params![approval_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?
        };

        let (id, channel, payload_raw, status) =
            row.ok_or_else(|| "Approval not found".to_string())?;
        if status != "pending" {
            return Err(format!(
                "Approval is not pending (current status: {status})"
            ));
        }

        let profile = self
            .get_profile()?
            .ok_or_else(|| "Sales profile is not configured".to_string())?;

        let sent_today = self.deliveries_today(&profile.timezone_mode)?;
        if sent_today >= profile.daily_send_cap {
            return Err(format!(
                "Daily send cap reached ({}/{})",
                sent_today, profile.daily_send_cap
            ));
        }

        let payload: serde_json::Value = serde_json::from_str(&payload_raw)
            .map_err(|e| format!("Invalid approval payload JSON: {e}"))?;

        let result = match channel.as_str() {
            "email" => {
                let to = payload
                    .get("to")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.to".to_string())?;
                let subject = payload
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.subject".to_string())?;
                let body = payload
                    .get("body")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.body".to_string())?;
                let (suppressed, bounce_count) = {
                    let conn = self.open()?;
                    let suppressed = self.is_suppressed(&conn, to)?;
                    let domain = email_domain(to).unwrap_or_default();
                    let bounce_count = conn
                        .query_row(
                            "SELECT COUNT(*) FROM suppressions
                             WHERE contact_method_value LIKE ?1 AND reason = 'hard_bounce'",
                            params![format!("%@{domain}")],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap_or(0);
                    (suppressed, bounce_count)
                };
                if suppressed {
                    self.update_approval_status(&id, "blocked_suppressed")?;
                    let _ = self.record_delivery(&id, "email", to, "blocked_suppressed", None);
                    return Err("Recipient is suppressed".to_string());
                }
                let validation = validate_email_for_sending(to, suppressed, bounce_count).await?;
                if !validation.safe_to_send {
                    self.update_approval_status(&id, "blocked_validation")?;
                    let _ = self.record_delivery(
                        &id,
                        "email",
                        to,
                        "blocked_validation",
                        Some(&format!(
                            "syntax_valid={} mx_valid={} classification={} domain_health={:.2}",
                            validation.syntax_valid,
                            validation.mx_valid,
                            validation.classification,
                            validation.domain_health
                        )),
                    );
                    return Err(format!(
                        "Email failed pre-send validation (classification={}, mx_valid={})",
                        validation.classification, validation.mx_valid
                    ));
                }
                if let Err(send_err) = self.send_email(state, &profile, to, subject, body).await {
                    if let Err(record_err) =
                        self.record_delivery(&id, "email", to, "failed", Some(&send_err))
                    {
                        warn!(
                            approval_id = %id,
                            error = %record_err,
                            "Failed to record email delivery failure"
                        );
                    }
                    return Err(send_err);
                }
                self.update_approval_status(&id, "approved")?;
                if let Err(record_err) = self.record_delivery(&id, "email", to, "sent", None) {
                    warn!(
                        approval_id = %id,
                        error = %record_err,
                        "Failed to record email delivery after successful send"
                    );
                }
                serde_json::json!({"channel": "email", "recipient": to, "status": "sent"})
            }
            "linkedin" | "linkedin_assist" => {
                let profile_url = payload
                    .get("profile_url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.profile_url".to_string())?;
                let message = payload
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.message".to_string())?;
                if let Err(send_err) = self.send_linkedin(&id, profile_url, message).await {
                    if let Err(record_err) = self.record_delivery(
                        &id,
                        "linkedin_assist",
                        profile_url,
                        "failed",
                        Some(&send_err),
                    ) {
                        warn!(
                            approval_id = %id,
                            error = %record_err,
                            "Failed to record LinkedIn delivery failure"
                        );
                    }
                    return Err(send_err);
                }
                self.update_approval_status(&id, "approved")?;
                serde_json::json!({"channel": "linkedin_assist", "recipient": profile_url, "status": "operator_pending"})
            }
            other => return Err(format!("Unsupported channel: {other}")),
        };
        Ok(result)
    }

    pub fn reject_approval(&self, approval_id: &str) -> Result<(), String> {
        let conn = self.open()?;
        let status = conn
            .query_row(
                "SELECT status FROM approvals WHERE id = ?",
                params![approval_id],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?
            .ok_or_else(|| "Approval not found".to_string())?;
        if status != "pending" {
            return Err(format!(
                "Approval is not pending (current status: {status})"
            ));
        }
        self.update_approval_status(approval_id, "rejected")
    }

    pub fn already_ran_today(&self, timezone_mode: &str) -> Result<bool, String> {
        let conn = self.open()?;
        let today = current_sales_day(timezone_mode);
        let mut stmt = conn
            .prepare("SELECT started_at FROM sales_runs WHERE status = 'completed'")
            .map_err(|e| format!("Run-day check prepare failed: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Run-day check query failed: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("Run-day check row failed: {e}"))?
        {
            let started_at: String = row.get(0).unwrap_or_default();
            if timestamp_matches_sales_day(&started_at, today, timezone_mode) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn completed_runs_count(&self) -> Result<u32, String> {
        let conn = self.open()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sales_runs WHERE status = 'completed'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| format!("Completed-runs count failed: {e}"))?;
        Ok(count.max(0) as u32)
    }

    fn previously_discovered_domains(&self, limit: usize) -> Result<Vec<String>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare("SELECT domain FROM discovered_domains ORDER BY first_seen_at DESC LIMIT ?1")
            .map_err(|e| format!("Prepare discovered_domains query failed: {e}"))?;
        let rows = stmt
            .query_map(params![limit as i64], |row| row.get::<_, String>(0))
            .map_err(|e| format!("Query discovered_domains failed: {e}"))?;
        Ok(rows.flatten().collect())
    }

    fn record_discovered_domain(&self, domain: &str, run_id: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "INSERT OR IGNORE INTO discovered_domains (domain, first_seen_run_id, first_seen_at) VALUES (?1, ?2, ?3)",
            params![domain, run_id, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Record discovered domain failed: {e}"))?;
        Ok(())
    }

    pub async fn run_generation(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<SalesRunRecord, String> {
        self.run_generation_with_job(kernel, None).await
    }

    /// Run only the discovery phase — fills the account reservoir without
    /// limiting by daily_target. Returns the count of newly discovered accounts.
    pub async fn run_discovery_only(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<usize, String> {
        // Discovery reuses the full pipeline but the reservoir pattern already
        // discovers without daily_target limits (DISCOVERY_RESERVOIR_CANDIDATES).
        // This wrapper makes the intent explicit for callers.
        let record = self.run_generation(kernel).await?;
        Ok(record.discovered as usize)
    }

    /// Select best accounts from the reservoir for today's activation.
    /// Respects daily_target and applies 87/13 exploit/explore split.
    pub fn select_for_activation(&self, daily_target: u32) -> Result<Vec<String>, String> {
        let conn = self.open()?;
        let exploit_count = (daily_target as f64 * ACTIVATION_EXPLOIT_RATIO).ceil() as u32;
        let explore_count = daily_target.saturating_sub(exploit_count);

        let mut exploit_stmt = conn
            .prepare(
                "SELECT account_id FROM activation_queue
                 WHERE status = 'pending'
                 ORDER BY priority DESC LIMIT ?1",
            )
            .map_err(|e| format!("Activation exploit query failed: {e}"))?;
        let exploit_ids: Vec<String> = exploit_stmt
            .query_map(params![exploit_count], |r| r.get(0))
            .map_err(|e| format!("Activation exploit query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        let mut explore_stmt = conn
            .prepare(
                "SELECT a.id FROM accounts a
                 JOIN score_snapshots s ON s.account_id = a.id
                 WHERE s.fit_score BETWEEN 0.3 AND 0.7
                 AND a.id NOT IN (
                     SELECT account_id FROM exploration_log
                     WHERE created_at > datetime('now', '-30 days')
                 )
                 ORDER BY RANDOM() LIMIT ?1",
            )
            .map_err(|e| format!("Activation explore query failed: {e}"))?;
        let explore_ids: Vec<String> = explore_stmt
            .query_map(params![explore_count], |r| r.get(0))
            .map_err(|e| format!("Activation explore query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        for acc_id in &explore_ids {
            let _ = conn.execute(
                "INSERT INTO exploration_log (id, account_id, exploration_reason, exploration_type)
                 VALUES (?1, ?2, 'scheduled_exploration', 'mid_score_random')",
                params![uuid::Uuid::new_v4().to_string(), acc_id],
            );
        }

        let mut selected = exploit_ids;
        selected.extend(explore_ids);
        Ok(selected)
    }

    pub async fn run_generation_with_job(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
        job_id: Option<&str>,
    ) -> Result<SalesRunRecord, String> {
        self.init()?;
        let profile = self
            .get_profile()?
            .ok_or_else(|| "Sales profile not configured".to_string())?;

        if profile.product_name.trim().is_empty()
            || profile.product_description.trim().is_empty()
            || profile.target_industry.trim().is_empty()
        {
            return Err("Sales profile is incomplete: product_name/product_description/target_industry are required".to_string());
        }
        if profile.target_geo.trim().is_empty() {
            return Err("target_geo must be set before running".to_string());
        }

        let run_sequence = self.completed_runs_count()? as usize;
        let run_id = self.begin_run()?;
        let started_at = Utc::now().to_rfc3339();

        let max_candidates = DISCOVERY_RESERVOIR_CANDIDATES;
        let is_field_ops = profile_targets_field_ops(&profile);
        let skip_llm_discovery = is_field_ops && geo_is_turkey(&profile.target_geo);

        // --- STAGE 1: Query Plan (LLM or heuristic fallback) ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::QueryPlanning)?;
        }
        let lead_plan = if skip_llm_discovery {
            heuristic_lead_query_plan(&profile)
        } else {
            match tokio::time::timeout(
                Duration::from_secs(LEAD_QUERY_PLAN_TIMEOUT_SECS),
                llm_build_lead_query_plan(kernel, &profile),
            )
            .await
            {
                Ok(Ok(plan)) if !plan.discovery_queries.is_empty() => plan,
                Ok(Ok(_)) => heuristic_lead_query_plan(&profile),
                Ok(Err(e)) => {
                    warn!(error = %e, "Lead query planner failed, using heuristic plan");
                    heuristic_lead_query_plan(&profile)
                }
                Err(_) => {
                    warn!("Lead query planner timed out, using heuristic plan");
                    heuristic_lead_query_plan(&profile)
                }
            }
        };
        if let Some(job_id) = job_id {
            self.complete_job_stage(job_id, PipelineStage::QueryPlanning, &lead_plan)?;
        }

        let cache = Arc::new(WebCache::new(Duration::from_secs(900)));
        let search_engine = WebSearchEngine::new(kernel.config.web.clone(), cache);
        let brave_search_engine = {
            let brave_env = kernel.config.web.brave.api_key_env.clone();
            let has_brave_key = std::env::var(&brave_env)
                .ok()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            if has_brave_key && kernel.config.web.search_provider != SearchProvider::Brave {
                let mut brave_cfg = kernel.config.web.clone();
                brave_cfg.search_provider = SearchProvider::Brave;
                Some(WebSearchEngine::new(
                    brave_cfg,
                    Arc::new(WebCache::new(Duration::from_secs(900))),
                ))
            } else {
                None
            }
        };

        // --- STAGE 2: Parallel Discovery (LLM primary + web search + directories) ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Discovery)?;
        }
        let previously_discovered = self.previously_discovered_domains(200).unwrap_or_default();
        let llm_target = MAX_LLM_PRIMARY_CANDIDATES;
        let skip_source_llm = self.should_skip_source("llm_generation").unwrap_or(false);
        let skip_source_web = self.should_skip_source("web_search").unwrap_or(false);
        let skip_source_directory = self
            .should_skip_source("directory_listing")
            .unwrap_or(false);

        let (
            llm_candidates,
            (web_search_candidates, mut source_contact_hints, search_unavailable),
            free_candidates,
        ) = tokio::join!(
            // PRIMARY: LLM company generation
            async {
                if skip_llm_discovery || skip_source_llm {
                    Vec::new()
                } else {
                    match tokio::time::timeout(
                        Duration::from_secs(LLM_COMPANY_GENERATION_TIMEOUT_SECS),
                        llm_generate_company_candidates(
                            kernel,
                            &profile,
                            llm_target,
                            run_sequence,
                            &previously_discovered,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(candidates)) => {
                            info!(
                                count = candidates.len(),
                                "LLM primary discovery returned candidates"
                            );
                            candidates
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "LLM primary company generation failed");
                            Vec::new()
                        }
                        Err(_) => {
                            warn!("LLM primary company generation timed out");
                            Vec::new()
                        }
                    }
                }
            },
            // SECONDARY: Web search discovery
            async {
                if skip_source_web {
                    (Vec::new(), HashMap::new(), false)
                } else {
                    discover_via_web_search(
                        &search_engine,
                        &brave_search_engine,
                        &lead_plan,
                        &profile,
                        max_candidates,
                        is_field_ops,
                    )
                    .await
                }
            },
            // SUPPLEMENTAL: Turkish directory scraping
            async {
                if skip_source_directory {
                    Vec::new()
                } else {
                    fetch_free_discovery_candidates(&profile, run_sequence).await
                }
            },
        );
        let mut directory_source_counts = expected_source_counts_for_profile(&profile);
        for candidate in &free_candidates {
            if let Some(source) = candidate.contact_hint.source.as_deref() {
                let key = source_health_key(source);
                if let Some(entry) = directory_source_counts.get_mut(key) {
                    *entry += 1;
                }
            }
        }
        for (source_type, count) in directory_source_counts {
            let _ = self.update_source_health(&source_type, count);
        }
        let _ = self.update_source_health("web_search", web_search_candidates.len());
        let _ = self.update_source_health("llm_generation", llm_candidates.len());

        // --- LLM Hallucination Check (TASK-40): verify domains actually exist ---
        let mut llm_candidates = llm_candidates;
        if !llm_candidates.is_empty() {
            let mut verified = Vec::with_capacity(llm_candidates.len());
            let verify_futures: Vec<_> = llm_candidates
                .iter()
                .map(|c| verify_domain_exists(&c.domain))
                .collect();
            let results = futures::future::join_all(verify_futures).await;
            for (candidate, exists) in llm_candidates.into_iter().zip(results) {
                if exists {
                    verified.push(candidate);
                } else {
                    info!(domain = %candidate.domain, "LLM candidate domain verification failed — skipping phantom");
                }
            }
            llm_candidates = verified;
        }

        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Discovery,
                &DiscoveryCheckpoint {
                    lead_plan: lead_plan.clone(),
                    llm_candidates: llm_candidates.clone(),
                    web_candidates: web_search_candidates.clone(),
                    free_candidates: free_candidates.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                },
            )?;
        }

        // --- STAGE 3: Merge all discovery sources ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Merging)?;
        }
        let mut candidate_list = merge_all_discovery_sources(
            llm_candidates,
            web_search_candidates,
            free_candidates,
            &mut source_contact_hints,
        );
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Merging,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: Vec::new(),
                },
            )?;
        }

        // --- STAGE 4: LLM Relevance Validation ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Validation)?;
        }
        let mut llm_validated_domains = HashSet::<String>::new();
        let validation_count = candidate_list
            .len()
            .min(LLM_RELEVANCE_VALIDATION_BATCH_SIZE);
        let should_run_llm_validation = validation_count > 3
            && !(is_field_ops && profile.target_geo.trim().eq_ignore_ascii_case("TR"));
        if should_run_llm_validation {
            match tokio::time::timeout(
                Duration::from_secs(LLM_RELEVANCE_VALIDATION_TIMEOUT_SECS),
                llm_validate_candidate_relevance(
                    kernel,
                    &profile,
                    &candidate_list[..validation_count],
                ),
            )
            .await
            {
                Ok(Ok(validations)) => {
                    info!(validated = validations.len(), "LLM validation completed");
                    for (domain, (relevant, confidence, _)) in &validations {
                        if *relevant && *confidence >= 0.5 {
                            llm_validated_domains.insert(domain.clone());
                        }
                    }
                    apply_llm_validation_scores(&mut candidate_list, &validations);
                }
                Ok(Err(e)) => warn!(error = %e, "LLM validation failed, proceeding without"),
                Err(_) => warn!("LLM validation timed out, proceeding without"),
            }
        }
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Validation,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }

        // --- STAGE 5: Filter and Sort ---
        let mut current_stage = PipelineStage::Filtering;
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Filtering)?;
        }
        let min_candidate_score = candidate_quality_floor(&profile);
        candidate_list.retain(|c| {
            c.score >= min_candidate_score && !candidate_should_skip_for_profile(c, &profile)
        });
        candidate_list.sort_by(|a, b| {
            candidate_preseed_priority(b, source_contact_hints.get(&b.domain), &profile)
                .cmp(&candidate_preseed_priority(
                    a,
                    source_contact_hints.get(&a.domain),
                    &profile,
                ))
                .then_with(|| b.score.cmp(&a.score))
                .then_with(|| a.domain.cmp(&b.domain))
        });

        if candidate_list.is_empty() {
            let err_msg = format!(
                "No suitable companies found for '{}' in '{}'. Check network/search provider/LLM connectivity and retry.",
                profile.target_industry, profile.target_geo
            );
            self.finish_run(&run_id, "failed", 0, 0, 0, Some(&err_msg))?;
            if let Some(job_id) = job_id {
                let _ = self.fail_job_stage(job_id, current_stage, &err_msg);
            }
            return Err(err_msg);
        }
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Filtering,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }

        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Enrichment)?;
        }
        let prospect_seed_limit = DISCOVERY_PROSPECT_SEED_LIMIT;
        let seeded_prospect_profiles = match self
            .seed_prospect_profiles_for_run(
                &run_id,
                &profile,
                kernel,
                &candidate_list,
                &source_contact_hints,
                prospect_seed_limit,
            )
            .await
        {
            Ok(profiles) => profiles,
            Err(e) => {
                warn!(run_id = %run_id, error = %e, "Failed to seed prospect profiles before lead conversion");
                Vec::new()
            }
        };
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Enrichment,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }
        let prospect_profile_lookup: HashMap<String, SalesProspectProfile> =
            seeded_prospect_profiles
                .iter()
                .cloned()
                .map(|profile| (profile.company_domain.clone(), profile))
                .collect();
        candidate_list.sort_by(|a, b| {
            candidate_execution_priority(
                b,
                source_contact_hints.get(&b.domain),
                prospect_profile_lookup.get(&b.domain),
                &profile,
            )
            .cmp(&candidate_execution_priority(
                a,
                source_contact_hints.get(&a.domain),
                prospect_profile_lookup.get(&a.domain),
                &profile,
            ))
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| a.domain.cmp(&b.domain))
        });
        current_stage = PipelineStage::LeadGeneration;
        let total_candidates = candidate_list.len().min(max_candidates);
        let profiled_accounts = seeded_prospect_profiles.len();
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::LeadGeneration)?;
            self.update_job_stage_checkpoint(
                job_id,
                PipelineStage::LeadGeneration,
                &LeadGenerationCheckpoint {
                    total_candidates,
                    processed_candidates: 0,
                    profiled_accounts,
                    inserted: 0,
                    approvals_queued: 0,
                    current_domain: None,
                },
            )?;
        }

        let mut discovered = 0u32;
        let mut inserted = 0u32;
        let mut approvals_queued = 0u32;
        let mut site_headers = reqwest::header::HeaderMap::new();
        site_headers.insert(
            reqwest::header::ACCEPT_LANGUAGE,
            reqwest::header::HeaderValue::from_static("tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"),
        );
        let site_client = reqwest::Client::builder()
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            )
            .default_headers(site_headers)
            .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
            .ok();
        let max_direct_enrich_attempts = MAX_DIRECT_ENRICH_ATTEMPTS;
        let prefetched_site_enrichments = if search_unavailable {
            if let Some(client) = site_client.as_ref() {
                prefetch_site_contact_enrichments(
                    client,
                    &candidate_list,
                    max_direct_enrich_attempts.min(MAX_PREFETCH_SITE_CANDIDATES),
                    profile.target_title_policy.as_str(),
                )
                .await
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };
        // Prefetch is a latency optimization, not part of the sequential enrichment budget.
        let mut direct_enrich_attempts = 0usize;
        let mut generic_direct_enrich_retries = 0usize;
        let mut prefetched_retry_attempts = 0usize;
        let max_web_contact_search_attempts = MAX_WEB_CONTACT_SEARCH_ATTEMPTS;
        let mut web_contact_search_attempts = 0usize;
        let mut prospect_profile_updates = HashMap::<String, SalesProspectProfile>::new();
        let mut activation_candidates = HashMap::<String, ActivationLeadCandidate>::new();

        for candidate in candidate_list.iter().take(max_candidates) {
            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: Some(candidate.domain.clone()),
                    },
                )?;
            }
            discovered += 1;
            if candidate.score < min_candidate_score {
                if let Some(job_id) = job_id {
                    self.update_job_stage_checkpoint(
                        job_id,
                        PipelineStage::LeadGeneration,
                        &LeadGenerationCheckpoint {
                            total_candidates,
                            processed_candidates: discovered as usize,
                            profiled_accounts,
                            inserted,
                            approvals_queued,
                            current_domain: Some(candidate.domain.clone()),
                        },
                    )?;
                }
                continue;
            }

            let domain = &candidate.domain;
            let company = domain_to_company(domain);
            let mut email_from_verified_site = false;
            let seeded_profile = prospect_profile_lookup.get(domain);
            let seeded_name =
                seeded_profile.and_then(|profile| profile.primary_contact_name.clone());
            let seeded_title =
                seeded_profile.and_then(|profile| profile.primary_contact_title.clone());
            let seeded_linkedin = seeded_profile
                .and_then(|profile| profile.primary_linkedin_url.clone())
                .and_then(|url| normalize_outreach_linkedin_url(&url));
            let seeded_company_linkedin = seeded_profile
                .and_then(|profile| profile.company_linkedin_url.clone())
                .and_then(|url| normalize_company_linkedin_url(&url));
            let seeded_email = seeded_profile.and_then(|profile| {
                normalize_contact_email_for_domain(profile.primary_email.clone(), domain)
            });
            let mut osint_links = merge_osint_links(
                seeded_profile
                    .map(|profile| profile.osint_links.clone())
                    .unwrap_or_default(),
                vec![
                    format!("https://{domain}"),
                    seeded_linkedin.clone().unwrap_or_default(),
                    seeded_company_linkedin.clone().unwrap_or_default(),
                ],
            );
            let seeded_verified_signal = seeded_profile
                .map(prospect_profile_counts_as_verified_company_signal)
                .unwrap_or(false);
            let seeded_actionable_contact =
                lead_has_outreach_channel(seeded_email.as_ref(), seeded_linkedin.as_ref())
                    && (seeded_verified_signal
                        || lead_has_person_identity(
                            seeded_name.as_deref(),
                            seeded_linkedin.as_ref(),
                        ));

            let skip_web_contact_search = seeded_actionable_contact
                || search_unavailable
                || web_contact_search_attempts >= max_web_contact_search_attempts;
            let (
                mut contact_name,
                mut contact_title,
                mut linkedin_url,
                mut email,
                search_osint_enrichment,
            ) = if skip_web_contact_search {
                (
                    seeded_name,
                    seeded_title
                        .or_else(|| default_contact_title(profile.target_title_policy.as_str())),
                    seeded_linkedin,
                    seeded_email,
                    SiteContactEnrichment::default(),
                )
            } else {
                let mut contact_name = seeded_name;
                let mut contact_title = seeded_title;
                let mut linkedin_url = seeded_linkedin;
                let mut email: Option<String>;
                let mut search_outputs = Vec::<String>::new();
                let company_search_aliases =
                    build_company_search_aliases(&company, &candidate.evidence);
                let company_search_name = company_search_aliases
                    .first()
                    .cloned()
                    .unwrap_or_else(|| company.clone());
                web_contact_search_attempts += 1;
                let primary_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                        "site:linkedin.com/in \"{}\" (CEO OR \"Chief Executive Officer\")",
                        company_search_name
                    )
                } else {
                    format!(
                            "site:linkedin.com/in \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\")",
                            company_search_name
                        )
                };

                let domain_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                            "site:{} (\"Chief Executive Officer\" OR CEO) (leadership OR management OR executive team)",
                            domain
                        )
                } else {
                    format!(
                            "site:{} (\"Chief Executive Officer\" OR CEO OR Founder OR COO OR \"Head of Operations\") (leadership OR management OR executive team)",
                            domain
                        )
                };

                let secondary_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                            "\"{}\" \"{}\" (\"Chief Executive Officer\" OR CEO) (LinkedIn OR leadership OR executive team)",
                            company_search_name, domain
                        )
                } else {
                    format!(
                            "\"{}\" \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR leadership OR executive team)",
                            company_search_name, domain
                        )
                };

                let contact_queries = dedupe_strings(vec![
                    primary_contact_query,
                    domain_contact_query,
                    secondary_contact_query,
                ]);

                let mut contact_outputs = Vec::<String>::new();
                for (_query, result) in run_sales_search_batch(
                    &search_engine,
                    &contact_queries,
                    6,
                    Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS),
                )
                .await
                {
                    if let Ok(out) = result {
                        if !out.trim().is_empty() {
                            search_outputs.push(out.clone());
                            contact_outputs.push(out);
                        }
                    }
                }
                let contact_res = contact_outputs.join("\n");

                let (search_name, search_title, search_linkedin) =
                    extract_contact_from_search(&contact_res, profile.target_title_policy.as_str());
                let (entry_name, entry_title, entry_linkedin, entry_email) =
                    extract_contact_from_search_for_company(
                        &contact_res,
                        profile.target_title_policy.as_str(),
                        &company,
                        domain,
                    );
                if contact_name.is_none() {
                    contact_name = search_name;
                }
                if contact_title.is_none() {
                    contact_title = search_title;
                }
                if linkedin_url.is_none() {
                    linkedin_url = search_linkedin;
                }
                if contact_name.is_none() {
                    contact_name = entry_name;
                }
                if contact_title.is_none() {
                    contact_title = entry_title;
                }
                if linkedin_url.is_none() {
                    linkedin_url = entry_linkedin;
                }
                if contact_name.is_none() {
                    contact_name = linkedin_url
                        .as_deref()
                        .and_then(extract_name_from_linkedin_url);
                }
                email = seeded_email.or_else(|| {
                    normalize_contact_email_for_domain(
                        extract_email_from_text(&contact_res).or(entry_email),
                        domain,
                    )
                    .or_else(|| guessed_email(contact_name.as_deref(), domain))
                });

                if contact_name.is_none() || linkedin_url.is_none() || email.is_none() {
                    let fallback_contact_query = format!(
                            "\"{}\" \"{}\" {} (CEO OR \"Chief Executive Officer\" OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR Wikipedia OR leadership OR executive team OR email)",
                            company_search_name, domain, profile.target_geo
                        );
                    let fallback_contact_res = run_sales_search(
                        &search_engine,
                        &fallback_contact_query,
                        10,
                        Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS),
                    )
                    .await
                    .unwrap_or_default();
                    if !fallback_contact_res.trim().is_empty() {
                        search_outputs.push(fallback_contact_res.clone());
                    }
                    let (fallback_name, fallback_title, fallback_linkedin) =
                        extract_contact_from_search(
                            &fallback_contact_res,
                            profile.target_title_policy.as_str(),
                        );
                    let (
                        fallback_entry_name,
                        fallback_entry_title,
                        fallback_entry_linkedin,
                        fallback_entry_email,
                    ) = extract_contact_from_search_for_company(
                        &fallback_contact_res,
                        profile.target_title_policy.as_str(),
                        &company,
                        domain,
                    );
                    if contact_name.is_none() {
                        contact_name = fallback_name;
                    }
                    if contact_name.is_none() {
                        contact_name = fallback_entry_name;
                    }
                    if contact_title.is_none() {
                        contact_title = fallback_title;
                    }
                    if contact_title.is_none() {
                        contact_title = fallback_entry_title;
                    }
                    if linkedin_url.is_none() {
                        linkedin_url = fallback_linkedin;
                    }
                    if linkedin_url.is_none() {
                        linkedin_url = fallback_entry_linkedin;
                    }
                    if contact_name.is_none() {
                        contact_name = linkedin_url
                            .as_deref()
                            .and_then(extract_name_from_linkedin_url);
                    }
                    if email.is_none() {
                        email = normalize_contact_email_for_domain(
                            extract_email_from_text(&fallback_contact_res).or(fallback_entry_email),
                            domain,
                        )
                        .or_else(|| guessed_email(contact_name.as_deref(), domain));
                    }
                }
                let mut search_osint_enrichment = if search_outputs.is_empty() {
                    SiteContactEnrichment::default()
                } else {
                    best_search_contact_enrichment(
                        &search_outputs,
                        profile.target_title_policy.as_str(),
                        &company_search_name,
                        domain,
                    )
                };
                if search_osint_enrichment.company_linkedin_url.is_none() {
                    // 4-Layer LinkedIn search fallback (TASK-24)
                    search_osint_enrichment.company_linkedin_url =
                        find_company_linkedin_url(&company_search_name, domain, &search_engine)
                            .await;
                }
                search_osint_enrichment.osint_links = merge_osint_links(
                    search_osint_enrichment.osint_links.clone(),
                    collect_osint_links_from_search_outputs(&search_outputs, &company, domain),
                );
                (
                    contact_name,
                    contact_title,
                    linkedin_url,
                    email,
                    search_osint_enrichment,
                )
            };
            let mut company_linkedin_url = seeded_company_linkedin;
            let mut site_evidence: Option<String> = None;
            if let Some(hint) = source_contact_hints.get(domain) {
                apply_source_contact_hint(
                    domain,
                    hint,
                    &mut contact_name,
                    &mut contact_title,
                    &mut email,
                );
            }
            if let Some(enrichment) = prefetched_site_enrichments.get(domain) {
                apply_site_contact_enrichment(
                    domain,
                    enrichment,
                    &mut contact_name,
                    &mut contact_title,
                    &mut linkedin_url,
                    &mut company_linkedin_url,
                    &mut email,
                    &mut osint_links,
                    &mut email_from_verified_site,
                    &mut site_evidence,
                );
            }
            let allow_prefetched_retry = prefetched_site_enrichments
                .get(domain)
                .map(|enrichment| {
                    !site_contact_enrichment_has_signal(enrichment)
                        && prefetched_retry_attempts < MAX_PREFETCH_RETRY_CANDIDATES
                })
                .unwrap_or(false);
            let needs_enrichment = contact_name.is_none()
                || contact_name_is_placeholder(contact_name.as_deref())
                || linkedin_url.is_none()
                || email.is_none()
                || (is_field_ops && site_evidence.is_none());
            let mut attempted_direct_enrich = false;
            if needs_enrichment
                && (!prefetched_site_enrichments.contains_key(domain) || allow_prefetched_retry)
                && direct_enrich_attempts < max_direct_enrich_attempts
                && site_client.is_some()
            {
                if allow_prefetched_retry {
                    prefetched_retry_attempts += 1;
                }
                direct_enrich_attempts += 1;
                attempted_direct_enrich = true;
                if let Some(client) = site_client.as_ref() {
                    if let Ok(bundle) = tokio::time::timeout(
                        Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                        fetch_company_site_html_pages(client, domain),
                    )
                    .await
                    {
                        let enrichment = best_site_contact_enrichment(
                            bundle,
                            profile.target_title_policy.as_str(),
                        );
                        apply_site_contact_enrichment(
                            domain,
                            &enrichment,
                            &mut contact_name,
                            &mut contact_title,
                            &mut linkedin_url,
                            &mut company_linkedin_url,
                            &mut email,
                            &mut osint_links,
                            &mut email_from_verified_site,
                            &mut site_evidence,
                        );
                    }
                }
            }
            let needs_generic_identity_retry = (contact_name.is_none()
                || contact_name_is_placeholder(contact_name.as_deref()))
                && site_evidence.is_some()
                && email.is_some()
                && !attempted_direct_enrich
                && generic_direct_enrich_retries < MAX_GENERIC_DIRECT_ENRICH_RETRIES
                && site_client.is_some();
            if needs_generic_identity_retry {
                generic_direct_enrich_retries += 1;
                if let Some(client) = site_client.as_ref() {
                    if let Ok(bundle) = tokio::time::timeout(
                        Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                        fetch_company_site_html_pages(client, domain),
                    )
                    .await
                    {
                        let enrichment = best_site_contact_enrichment(
                            bundle,
                            profile.target_title_policy.as_str(),
                        );
                        apply_site_contact_enrichment(
                            domain,
                            &enrichment,
                            &mut contact_name,
                            &mut contact_title,
                            &mut linkedin_url,
                            &mut company_linkedin_url,
                            &mut email,
                            &mut osint_links,
                            &mut email_from_verified_site,
                            &mut site_evidence,
                        );
                    }
                }
            }
            if site_contact_enrichment_has_signal(&search_osint_enrichment) {
                apply_search_contact_enrichment(
                    &search_osint_enrichment,
                    &mut contact_name,
                    &mut contact_title,
                    &mut linkedin_url,
                    &mut company_linkedin_url,
                    &mut osint_links,
                );
            }

            contact_name = contact_name.and_then(|n| normalize_person_name(&n));
            linkedin_url = linkedin_url.and_then(|url| normalize_outreach_linkedin_url(&url));
            company_linkedin_url =
                company_linkedin_url.and_then(|url| normalize_company_linkedin_url(&url));
            email = if email_from_verified_site {
                normalize_site_contact_email(email)
            } else {
                normalize_contact_email_for_domain(email, domain)
            };
            osint_links = merge_osint_links(
                osint_links,
                vec![
                    format!("https://{domain}"),
                    linkedin_url.clone().unwrap_or_default(),
                    company_linkedin_url.clone().unwrap_or_default(),
                ],
            );

            if let Some(base_profile) = seeded_profile.cloned() {
                let mut profile_update = base_profile;
                if profile_update.primary_contact_name.is_none()
                    || contact_name_is_placeholder(profile_update.primary_contact_name.as_deref())
                {
                    profile_update.primary_contact_name = contact_name.clone();
                }
                if contact_title_is_generic_default(profile_update.primary_contact_title.as_deref())
                    && !contact_title_is_generic_default(contact_title.as_deref())
                {
                    profile_update.primary_contact_title = contact_title.clone();
                }
                if profile_update.primary_email.is_none() && email.is_some() {
                    profile_update.primary_email = email.clone();
                }
                if profile_update.primary_linkedin_url.is_none() && linkedin_url.is_some() {
                    profile_update.primary_linkedin_url = linkedin_url.clone();
                }
                if profile_update.company_linkedin_url.is_none() && company_linkedin_url.is_some() {
                    profile_update.company_linkedin_url = company_linkedin_url.clone();
                }
                profile_update.osint_links =
                    merge_osint_links(profile_update.osint_links.clone(), osint_links.clone());
                profile_update.profile_status = prospect_status(
                    profile_update.primary_contact_name.as_deref(),
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                )
                .to_string();
                profile_update.recommended_channel = build_recommended_channel(
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                );
                profile_update.summary = build_prospect_summary(
                    &profile_update.company,
                    &profile_update.matched_signals,
                    profile_update.primary_contact_name.as_deref(),
                    profile_update.primary_contact_title.as_deref(),
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                );
                profile_update.research_confidence =
                    profile_update
                        .research_confidence
                        .max(heuristic_research_confidence(
                            profile_update.fit_score,
                            &profile_update.profile_status,
                            profile_update.source_count as usize,
                            profile_update.contact_count as usize,
                        ));
                profile_update.updated_at = Utc::now().to_rfc3339();
                prospect_profile_updates.insert(domain.clone(), profile_update);
            }
            let is_llm_validated = llm_validated_domains.contains(domain);
            let is_verified_by_memory = seeded_verified_signal;
            if !lead_has_verified_company_signal(
                is_field_ops,
                site_evidence.as_deref(),
                is_llm_validated || is_verified_by_memory,
            ) {
                continue;
            }

            // For validated companies: fill missing fields with reasonable defaults.
            if (is_llm_validated || is_verified_by_memory)
                && (contact_name.is_none() || contact_name_is_placeholder(contact_name.as_deref()))
            {
                contact_title = default_contact_title(profile.target_title_policy.as_str());
            }

            if !lead_has_outreach_channel(email.as_ref(), linkedin_url.as_ref()) {
                continue;
            }
            // Search-time LLM validation or cached dossier memory can proceed without a real person name.
            if !(is_llm_validated
                || is_verified_by_memory
                || lead_has_person_identity(contact_name.as_deref(), linkedin_url.as_ref()))
            {
                continue;
            }

            let mut score = (lead_score(&linkedin_url, &email) + candidate.score).min(100);
            if is_field_ops && site_evidence.is_some() {
                score = (score + 4).min(100);
            }

            let evidence = site_evidence
                .or_else(|| candidate.evidence.first().cloned())
                .unwrap_or_else(|| {
                    format!(
                        "{} appears in search results for {}",
                        company, profile.target_industry
                    )
                });
            let matched = if candidate.matched_keywords.is_empty() {
                profile.target_industry.clone()
            } else {
                candidate
                    .matched_keywords
                    .iter()
                    .take(4)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            let reasons = build_sales_lead_reasons(
                &profile,
                &company,
                &matched,
                &evidence,
                contact_title.as_deref(),
            );

            // Evidence-bound message generation (TASK-29): try strategy+copy first,
            // fall back to direct templates if evidence is insufficient.
            let strategy = generate_message_strategy(
                &profile,
                &company,
                contact_name.as_deref(),
                &evidence,
                &matched,
            );
            let (email_subject, email_body, linkedin_message) =
                match generate_message_copy(&strategy, &profile, &company, contact_name.as_deref())
                {
                    Ok(copy) => (copy.subject, copy.body, copy.linkedin_copy),
                    Err(_) => (
                        build_sales_email_subject(&profile, &company),
                        build_sales_email_body(
                            &profile,
                            &company,
                            contact_name.as_deref(),
                            &matched,
                            &evidence,
                        ),
                        build_sales_linkedin_message(
                            &profile,
                            &company,
                            contact_name.as_deref(),
                            &evidence,
                        ),
                    ),
                };

            let canonical = match self.sync_canonical_state(
                &self.open()?,
                &profile,
                candidate,
                &company,
                domain,
                &format!("https://{}", domain),
                contact_name.as_deref(),
                contact_title.as_deref(),
                email.as_deref(),
                candidate.phone.as_deref(),
                linkedin_url.as_deref(),
                company_linkedin_url.as_deref(),
                &osint_links,
                &evidence,
                &reasons,
            ) {
                Ok(value) => value,
                Err(e) => {
                    warn!(domain = %domain, error = %e, "Canonical account sync failed");
                    continue;
                }
            };

            let activation_score =
                ((activation_priority(&canonical.score) * 100.0).round() as i32).clamp(0, 100);
            score = score.max(activation_score);

            if !matches!(canonical.gate, SendGateDecision::Activate) {
                continue;
            }
            let lead = SalesLead {
                id: uuid::Uuid::new_v4().to_string(),
                run_id: run_id.clone(),
                company,
                website: format!("https://{}", domain),
                company_domain: domain.clone(),
                contact_name: contact_name.unwrap_or_else(|| "Leadership Team".to_string()),
                contact_title: contact_title.unwrap_or_else(|| {
                    if profile.target_title_policy == "ceo_only" {
                        "CEO".to_string()
                    } else {
                        "CEO/Founder".to_string()
                    }
                }),
                linkedin_url,
                email,
                phone: candidate.phone.as_deref().and_then(normalize_phone),
                reasons,
                email_subject,
                email_body,
                linkedin_message,
                score,
                status: "activation_candidate".to_string(),
                created_at: Utc::now().to_rfc3339(),
            };

            match self.insert_lead(&lead) {
                Ok(true) => {
                    inserted += 1;
                    let _ = self.record_discovered_domain(domain, &run_id);
                    let account_id = stable_sales_id("acct", &[domain]);
                    let entry = activation_candidates
                        .entry(account_id.clone())
                        .or_insert_with(|| ActivationLeadCandidate {
                            account_id: account_id.clone(),
                            priority: activation_score as i64,
                            lead: lead.clone(),
                        });
                    if activation_score as i64 > entry.priority {
                        *entry = ActivationLeadCandidate {
                            account_id,
                            priority: activation_score as i64,
                            lead: lead.clone(),
                        };
                    }
                }
                Ok(false) => {
                    // duplicate, skip silently
                }
                Err(e) => warn!(domain = %domain, error = %e, "Lead insert failed"),
            }

            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: Some(domain.clone()),
                    },
                )?;
            }
        }

        if !activation_candidates.is_empty() {
            let conn = self.open()?;
            let candidate_priorities = activation_candidates
                .iter()
                .map(|(account_id, candidate)| (account_id.clone(), candidate.priority))
                .collect::<HashMap<_, _>>();
            let selected_accounts = self.select_accounts_for_activation(
                &conn,
                &candidate_priorities,
                profile.daily_target,
            )?;
            let selected_set = selected_accounts.into_iter().collect::<HashSet<_>>();
            for candidate in activation_candidates.into_values() {
                let lead_status = if selected_set.contains(&candidate.account_id) {
                    match self.queue_approvals_for_lead(&candidate.lead) {
                        Ok(q) => {
                            approvals_queued += q;
                            "approval_pending"
                        }
                        Err(e) => {
                            warn!(lead_id = %candidate.lead.id, error = %e, "Failed to queue selected lead approvals");
                            "activation_candidate"
                        }
                    }
                } else {
                    "activation_backlog"
                };
                if let Err(e) = self.update_lead_status(&candidate.lead.id, lead_status) {
                    warn!(lead_id = %candidate.lead.id, error = %e, "Failed to update activation lead status");
                }
            }
            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: None,
                    },
                )?;
            }
        }

        if !prospect_profile_updates.is_empty() {
            let updates = prospect_profile_updates.into_values().collect::<Vec<_>>();
            if let Err(e) = self.upsert_prospect_profiles(&updates) {
                warn!(run_id = %run_id, error = %e, "Failed to persist OSINT-enriched prospect dossiers during run");
            }
        }

        if inserted == 0 && seeded_prospect_profiles.is_empty() {
            let err_msg = "Prospecting run completed discovery, but no durable prospect dossiers or actionable contacts could be saved for the current ICP/geo.".to_string();
            self.finish_run(
                &run_id,
                "failed",
                discovered,
                inserted,
                approvals_queued,
                Some(&err_msg),
            )?;
            if let Some(job_id) = job_id {
                let _ = self.fail_job_stage(job_id, current_stage, &err_msg);
            }
            return Err(err_msg);
        }

        let run_note = if inserted == 0 {
            Some(format!(
                "Prospecting run completed with {} profiled accounts, but no action-ready contacts were verified in this pass.",
                seeded_prospect_profiles.len()
            ))
        } else {
            None
        };

        self.finish_run(
            &run_id,
            "completed",
            discovered,
            inserted,
            approvals_queued,
            run_note.as_deref(),
        )?;
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::LeadGeneration,
                &serde_json::json!({
                    "run_id": run_id,
                    "total_candidates": total_candidates,
                    "processed_candidates": discovered,
                    "profiled_accounts": profiled_accounts,
                    "discovered": discovered,
                    "inserted": inserted,
                    "approvals_queued": approvals_queued
                }),
            )?;
            self.complete_job_run(job_id)?;
        }

        if inserted > 0 {
            if let Err(e) = self
                .refresh_prospect_profiles_for_run(&run_id, &profile, kernel)
                .await
            {
                warn!(run_id = %run_id, error = %e, "Failed to refresh prospect profiles after run");
            }
        }

        Ok(SalesRunRecord {
            id: run_id,
            status: "completed".to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered,
            inserted,
            approvals_queued,
            error: run_note,
        })
    }

    async fn seed_prospect_profiles_for_run(
        &self,
        run_id: &str,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
        candidates: &[DomainCandidate],
        source_contact_hints: &HashMap<String, SourceContactHint>,
        limit: usize,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let heuristic_profiles = build_candidate_prospect_profiles(
            run_id,
            candidates,
            source_contact_hints,
            limit,
            sales_profile,
        );
        let enriched = self
            .hydrate_prospect_profiles_with_cache(kernel, sales_profile, heuristic_profiles)
            .await;
        self.upsert_prospect_profiles(&enriched)?;
        for profile in &enriched {
            let _ = self.record_discovered_domain(&profile.company_domain, run_id);
        }
        Ok(enriched)
    }

    async fn refresh_prospect_profiles_for_run(
        &self,
        run_id: &str,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let scan_limit = DISCOVERY_REFRESH_SCAN_LIMIT;
        let leads = self.list_leads(scan_limit, Some(run_id))?;
        if leads.is_empty() {
            return Ok(Vec::new());
        }

        let heuristic_profiles = build_prospect_profiles(leads, scan_limit, Some(sales_profile));
        let enriched = self
            .hydrate_prospect_profiles_with_cache(kernel, sales_profile, heuristic_profiles)
            .await;
        let enriched = self
            .enrich_prospect_profiles_with_site_osint(enriched, sales_profile)
            .await;
        let enriched = self
            .enrich_prospect_profiles_with_search_osint(enriched, sales_profile, kernel)
            .await;
        self.upsert_prospect_profiles(&enriched)?;
        Ok(enriched)
    }

    async fn enrich_prospect_profiles_with_site_osint(
        &self,
        mut profiles: Vec<SalesProspectProfile>,
        sales_profile: &SalesProfile,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let client = match reqwest::Client::builder()
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            )
            .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
        {
            Ok(client) => client,
            Err(_) => return profiles,
        };

        let osint_target_limit = DISCOVERY_OSINT_TARGET_LIMIT;
        let targets = profiles
            .iter()
            .filter(|profile| {
                profile.osint_links.len() < 2
                    || profile.company_linkedin_url.is_none()
                    || profile.primary_linkedin_url.is_none()
            })
            .take(osint_target_limit)
            .map(|profile| profile.company_domain.clone())
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return profiles;
        }

        let enrichments = join_all(targets.into_iter().map(|domain| async {
            let bundle = tokio::time::timeout(
                Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                fetch_company_site_html_pages(&client, &domain),
            )
            .await
            .unwrap_or_default();
            (
                domain,
                best_site_contact_enrichment(bundle, sales_profile.target_title_policy.as_str()),
            )
        }))
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();

        for profile in profiles.iter_mut() {
            if let Some(enrichment) = enrichments.get(&profile.company_domain) {
                apply_site_osint_to_profile(profile, enrichment);
            }
        }

        sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
        profiles
    }

    async fn enrich_prospect_profiles_with_search_osint(
        &self,
        mut profiles: Vec<SalesProspectProfile>,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let search_engine = WebSearchEngine::new(
            kernel.config.web.clone(),
            Arc::new(WebCache::new(Duration::from_secs(900))),
        );
        let brave_search_engine = {
            let brave_env = kernel.config.web.brave.api_key_env.clone();
            let has_brave_key = std::env::var(&brave_env)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if has_brave_key && kernel.config.web.search_provider != SearchProvider::Brave {
                let mut brave_cfg = kernel.config.web.clone();
                brave_cfg.search_provider = SearchProvider::Brave;
                Some(WebSearchEngine::new(
                    brave_cfg,
                    Arc::new(WebCache::new(Duration::from_secs(900))),
                ))
            } else {
                None
            }
        };

        let osint_target_limit = DISCOVERY_OSINT_TARGET_LIMIT;
        let targets = profiles
            .iter()
            .filter(|profile| {
                profile.osint_links.len() < 3
                    || profile.company_linkedin_url.is_none()
                    || profile.primary_linkedin_url.is_none()
            })
            .take(osint_target_limit)
            .map(|profile| {
                (
                    profile.company_domain.clone(),
                    profile.company.clone(),
                    profile.primary_contact_name.clone(),
                    profile.matched_signals.clone(),
                )
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return profiles;
        }

        let search_engine_ref = &search_engine;
        let brave_search_engine_ref = brave_search_engine.as_ref();
        let enrichments = stream::iter(targets.into_iter().map(
            |(domain, company, primary_name, matched_signals)| {
                let title_policy = sales_profile.target_title_policy.clone();
                let target_geo = sales_profile.target_geo.clone();
                async move {
                    let company_aliases = build_company_search_aliases(&company, &matched_signals);
                    let company_query = company_aliases
                        .first()
                        .cloned()
                        .or_else(|| {
                            if company.trim().is_empty() {
                                None
                            } else {
                                Some(company.clone())
                            }
                        })
                        .unwrap_or_else(|| domain_to_company(&domain));
                    let enrichment = search_company_osint_enrichment(
                        search_engine_ref,
                        brave_search_engine_ref,
                        &company_query,
                        &domain,
                        &title_policy,
                        &target_geo,
                        primary_name.as_deref(),
                    )
                    .await;
                    (domain, enrichment)
                }
            },
        ))
        .buffer_unordered(SALES_OSINT_PROFILE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();

        for profile in profiles.iter_mut() {
            if let Some(enrichment) = enrichments.get(&profile.company_domain) {
                apply_search_osint_to_profile(profile, enrichment);
            }
        }

        sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
        profiles
    }

    async fn hydrate_prospect_profiles_with_cache(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
        sales_profile: &SalesProfile,
        profiles: Vec<SalesProspectProfile>,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let mut hydrated = Vec::with_capacity(profiles.len());
        let mut misses = Vec::new();

        for mut profile in profiles {
            match self.get_stored_prospect_profile(&profile.company_domain) {
                Ok(Some(cached)) => {
                    apply_cached_prospect_memory(&mut profile, &cached);
                    if prospect_needs_llm_refresh(&profile) {
                        misses.push(profile);
                    } else {
                        hydrated.push(profile);
                    }
                }
                _ => {
                    if prospect_needs_llm_refresh(&profile) {
                        misses.push(profile);
                    } else {
                        hydrated.push(profile);
                    }
                }
            }
        }

        if misses.is_empty() {
            sort_prospect_profiles_for_harness(&mut hydrated, Some(sales_profile));
            return hydrated;
        }

        sort_prospect_profiles_for_harness(&mut misses, Some(sales_profile));
        let enrich_count = misses.len().min(6);
        let enrich_target = misses[..enrich_count].to_vec();
        let llm_enrichment = match tokio::time::timeout(
            Duration::from_secs(PROSPECT_LLM_ENRICH_TIMEOUT_SECS),
            llm_enrich_prospect_profiles(kernel, sales_profile, &enrich_target),
        )
        .await
        {
            Ok(Ok(map)) => map,
            Ok(Err(e)) => {
                warn!(error = %e, "Prospect LLM enrichment failed; keeping heuristic dossiers");
                HashMap::new()
            }
            Err(_) => {
                warn!("Prospect LLM enrichment timed out; keeping heuristic dossiers");
                HashMap::new()
            }
        };

        for mut profile in misses {
            if let Some(extra) = llm_enrichment.get(&profile.company_domain) {
                apply_llm_prospect_research(&mut profile, extra);
            }
            hydrated.push(profile);
        }

        sort_prospect_profiles_for_harness(&mut hydrated, Some(sales_profile));
        hydrated
    }
}

fn build_prospect_profiles(
    leads: Vec<SalesLead>,
    limit: usize,
    sales_profile: Option<&SalesProfile>,
) -> Vec<SalesProspectProfile> {
    let mut grouped: HashMap<String, ProspectAccumulator> = HashMap::new();

    for lead in leads {
        let domain = lead.company_domain.trim().to_lowercase();
        if domain.is_empty() {
            continue;
        }

        let signal_key = format!(
            "{}|{}",
            lead.contact_name.trim().to_lowercase(),
            lead.contact_title.trim().to_lowercase()
        );
        let contact_score = prospect_contact_score(&lead);
        let matched_signals = dedupe_strings(
            lead.reasons
                .iter()
                .map(|reason| truncate_cleaned_text(reason, 120))
                .filter(|reason| !reason.is_empty())
                .collect(),
        );

        let acc = grouped
            .entry(domain.clone())
            .or_insert_with(|| ProspectAccumulator {
                run_id: lead.run_id.clone(),
                company: lead.company.clone(),
                website: lead.website.clone(),
                company_domain: domain.clone(),
                fit_score: lead.score,
                created_at: lead.created_at.clone(),
                updated_at: lead.created_at.clone(),
                ..Default::default()
            });

        if acc.company.is_empty() {
            acc.company = lead.company.clone();
        }
        if acc.website.is_empty() {
            acc.website = lead.website.clone();
        }
        if lead.score > acc.fit_score {
            acc.fit_score = lead.score;
        }
        if lead.created_at < acc.created_at {
            acc.created_at = lead.created_at.clone();
        }
        if lead.created_at > acc.updated_at {
            acc.updated_at = lead.created_at.clone();
            acc.run_id = lead.run_id.clone();
        }
        if acc.osint_link_set.insert(lead.website.clone()) {
            acc.osint_links.push(lead.website.clone());
        }
        if let Some(linkedin) = lead
            .linkedin_url
            .as_deref()
            .and_then(normalize_outreach_linkedin_url)
        {
            if acc.osint_link_set.insert(linkedin.clone()) {
                acc.osint_links.push(linkedin);
            }
        }

        for signal in matched_signals {
            let key = signal.to_lowercase();
            if acc.signal_set.insert(key) {
                acc.matched_signals.push(signal);
            }
        }

        if !contact_name_is_placeholder(Some(lead.contact_name.as_str()))
            || !lead.contact_title.trim().is_empty()
        {
            acc.contact_keys.insert(signal_key);
        }

        if contact_score >= acc.primary_contact_score {
            acc.primary_contact_score = contact_score;
            acc.primary_contact_name = clean_profile_contact_name(&lead.contact_name);
            acc.primary_contact_title = clean_profile_contact_field(&lead.contact_title);
            acc.primary_email = normalize_actionable_outreach_email(lead.email.as_deref());
            acc.primary_linkedin_url = lead
                .linkedin_url
                .as_deref()
                .and_then(normalize_outreach_linkedin_url);
        }
    }

    let mut profiles: Vec<SalesProspectProfile> = grouped
        .into_values()
        .map(|acc| {
            let matched_signals = dedupe_strings(acc.matched_signals)
                .into_iter()
                .take(6)
                .collect::<Vec<_>>();
            let profile_status = prospect_status(
                acc.primary_contact_name.as_deref(),
                acc.primary_email.as_deref(),
                acc.primary_linkedin_url.as_deref(),
            );
            let summary = build_prospect_summary(
                &acc.company,
                &matched_signals,
                acc.primary_contact_name.as_deref(),
                acc.primary_contact_title.as_deref(),
                acc.primary_email.as_deref(),
                acc.primary_linkedin_url.as_deref(),
            );
            let recommended_channel = build_recommended_channel(
                acc.primary_email.as_deref(),
                acc.primary_linkedin_url.as_deref(),
            );
            let source_count = matched_signals.len() as u32;
            let buyer_roles = build_prospect_buyer_roles(
                sales_profile,
                acc.primary_contact_title.as_deref(),
                &matched_signals,
            );
            let pain_points =
                build_prospect_pain_points(sales_profile, &matched_signals, &acc.company);
            let trigger_events = build_prospect_trigger_events(
                &matched_signals,
                acc.primary_contact_title.as_deref(),
                profile_status,
            );
            let outreach_angle = build_prospect_outreach_angle(
                sales_profile,
                &acc.company,
                &pain_points,
                &trigger_events,
                &recommended_channel,
            );
            let research_confidence = heuristic_research_confidence(
                acc.fit_score,
                profile_status,
                matched_signals.len(),
                acc.contact_keys.len(),
            );

            SalesProspectProfile {
                id: acc.company_domain.clone(),
                run_id: acc.run_id,
                company: acc.company,
                website: acc.website,
                company_domain: acc.company_domain,
                fit_score: acc.fit_score,
                profile_status: profile_status.to_string(),
                summary,
                matched_signals,
                primary_contact_name: acc.primary_contact_name,
                primary_contact_title: acc.primary_contact_title,
                primary_email: acc.primary_email,
                primary_linkedin_url: acc.primary_linkedin_url,
                company_linkedin_url: acc.company_linkedin_url,
                osint_links: merge_osint_links(Vec::new(), acc.osint_links),
                contact_count: acc.contact_keys.len() as u32,
                source_count,
                buyer_roles,
                pain_points,
                trigger_events,
                recommended_channel,
                outreach_angle,
                research_status: "heuristic".to_string(),
                research_confidence,
                tech_stack: Vec::new(),
                created_at: acc.created_at,
                updated_at: acc.updated_at,
            }
        })
        .collect();

    sort_prospect_profiles_for_harness(&mut profiles, sales_profile);
    profiles.truncate(limit);
    profiles
}

fn sanitize_prospect_profile(profile: &mut SalesProspectProfile) {
    let previous_email = profile.primary_email.clone();
    profile.primary_email = normalize_actionable_outreach_email(profile.primary_email.as_deref());
    profile.primary_linkedin_url = profile
        .primary_linkedin_url
        .clone()
        .and_then(|value| normalize_outreach_linkedin_url(&value));
    profile.company_linkedin_url = profile
        .company_linkedin_url
        .clone()
        .and_then(|value| normalize_company_linkedin_url(&value));
    profile.osint_links = merge_osint_links(Vec::new(), profile.osint_links.clone());
    profile.profile_status = prospect_status(
        profile.primary_contact_name.as_deref(),
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    )
    .to_string();
    profile.recommended_channel = build_recommended_channel(
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    );
    if previous_email != profile.primary_email
        || profile.summary.trim().is_empty()
        || profile.research_status != "llm_enriched"
    {
        profile.summary = build_prospect_summary(
            &profile.company,
            &profile.matched_signals,
            profile.primary_contact_name.as_deref(),
            profile.primary_contact_title.as_deref(),
            profile.primary_email.as_deref(),
            profile.primary_linkedin_url.as_deref(),
        );
    }
    profile.research_confidence = profile
        .research_confidence
        .max(heuristic_research_confidence(
            profile.fit_score,
            &profile.profile_status,
            profile.source_count as usize,
            profile.contact_count as usize,
        ));
}

fn build_candidate_prospect_profiles(
    run_id: &str,
    candidates: &[DomainCandidate],
    source_contact_hints: &HashMap<String, SourceContactHint>,
    limit: usize,
    sales_profile: &SalesProfile,
) -> Vec<SalesProspectProfile> {
    let now = Utc::now().to_rfc3339();
    let mut profiles = Vec::new();

    for candidate in candidates.iter().take(limit) {
        let domain = candidate.domain.trim().to_lowercase();
        if domain.is_empty() {
            continue;
        }

        let company = domain_to_company(&domain);
        let hint = source_contact_hints.get(&domain);
        let primary_contact_name = hint
            .and_then(|hint| hint.contact_name.as_deref())
            .and_then(normalize_person_name);
        let primary_contact_title = hint
            .and_then(|hint| hint.contact_title.as_deref())
            .map(normalize_contact_title)
            .filter(|title| contact_title_priority(title) > 0);
        let primary_email = hint.and_then(|hint| {
            normalize_contact_email_for_domain(hint.email.clone(), &domain).or_else(|| {
                if source_hint_allows_offdomain_email(hint.source.as_deref()) {
                    normalize_site_contact_email(hint.email.clone())
                } else {
                    None
                }
            })
        });

        let mut matched_signals = dedupe_strings(
            candidate
                .matched_keywords
                .iter()
                .chain(candidate.evidence.iter())
                .map(|value| truncate_cleaned_text(value, 120))
                .filter(|value| !value.is_empty())
                .collect(),
        );
        if matched_signals.is_empty() {
            matched_signals.push(format!(
                "{} icin hedeflenen ICP sinyali bulundu",
                truncate_cleaned_text(&company, 80)
            ));
        }
        let matched_signals = matched_signals.into_iter().take(6).collect::<Vec<_>>();
        let seeded_osint_links = merge_osint_links(
            vec![format!("https://{}", domain)],
            candidate.source_links.clone(),
        );

        let profile_status = prospect_status(
            primary_contact_name.as_deref(),
            primary_email.as_deref(),
            None,
        );
        let recommended_channel = build_recommended_channel(primary_email.as_deref(), None);
        let buyer_roles = build_prospect_buyer_roles(
            Some(sales_profile),
            primary_contact_title.as_deref(),
            &matched_signals,
        );
        let pain_points =
            build_prospect_pain_points(Some(sales_profile), &matched_signals, &company);
        let trigger_events = build_prospect_trigger_events(
            &matched_signals,
            primary_contact_title.as_deref(),
            profile_status,
        );
        let outreach_angle = build_prospect_outreach_angle(
            Some(sales_profile),
            &company,
            &pain_points,
            &trigger_events,
            &recommended_channel,
        );

        profiles.push(SalesProspectProfile {
            id: domain.clone(),
            run_id: run_id.to_string(),
            company: company.clone(),
            website: format!("https://{}", domain),
            company_domain: domain.clone(),
            fit_score: candidate.score,
            profile_status: profile_status.to_string(),
            summary: build_prospect_summary(
                &company,
                &matched_signals,
                primary_contact_name.as_deref(),
                primary_contact_title.as_deref(),
                primary_email.as_deref(),
                None,
            ),
            matched_signals: matched_signals.clone(),
            primary_contact_name,
            primary_contact_title,
            primary_email,
            primary_linkedin_url: None,
            company_linkedin_url: None,
            osint_links: seeded_osint_links,
            contact_count: u32::from(source_contact_hints.contains_key(&domain)),
            source_count: candidate
                .evidence
                .len()
                .max(candidate.source_links.len())
                .max(1) as u32,
            buyer_roles,
            pain_points,
            trigger_events,
            recommended_channel,
            outreach_angle,
            research_status: "heuristic".to_string(),
            research_confidence: heuristic_research_confidence(
                candidate.score,
                profile_status,
                matched_signals.len(),
                usize::from(source_contact_hints.contains_key(&domain)),
            ),
            tech_stack: Vec::new(),
            created_at: now.clone(),
            updated_at: now.clone(),
        });
    }

    sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
    profiles
}

fn prospect_profile_counts_as_verified_company_signal(profile: &SalesProspectProfile) -> bool {
    profile.profile_status == "contact_ready"
        || profile.research_status == "llm_enriched"
        || profile.research_confidence >= 0.78
        || profile.source_count >= 2
        || profile.fit_score >= (MIN_DOMAIN_RELEVANCE_SCORE + 16)
}

fn sort_prospect_profiles_for_harness(
    profiles: &mut [SalesProspectProfile],
    sales_profile: Option<&SalesProfile>,
) {
    profiles.sort_by(|a, b| {
        prospect_harness_priority(b, sales_profile)
            .cmp(&prospect_harness_priority(a, sales_profile))
            .then_with(|| {
                b.research_confidence
                    .partial_cmp(&a.research_confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                prospect_status_rank(&b.profile_status)
                    .cmp(&prospect_status_rank(&a.profile_status))
            })
            .then_with(|| b.fit_score.cmp(&a.fit_score))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
    });
}

fn prospect_harness_priority(
    profile: &SalesProspectProfile,
    sales_profile: Option<&SalesProfile>,
) -> i32 {
    let mut priority = profile.fit_score;
    priority += match profile.profile_status.as_str() {
        "contact_ready" => 18,
        "contact_identified" => 10,
        _ => 2,
    };
    priority += (profile.source_count.min(4) as i32) * 3;
    priority += (profile.contact_count.min(3) as i32) * 4;
    if profile
        .primary_email
        .as_deref()
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false)
    {
        priority += 6;
    }
    if profile
        .primary_linkedin_url
        .as_deref()
        .and_then(normalize_outreach_linkedin_url)
        .is_some()
    {
        priority += 6;
    }
    priority += (profile.research_confidence * 10.0).round() as i32;
    if let Some(sales_profile) = sales_profile {
        priority += domain_locality_bonus(&profile.company_domain, &sales_profile.target_geo);
        if profile_prefers_operator_accounts(sales_profile) {
            priority -= account_reachability_penalty(
                &profile.company_domain,
                Some(&profile.company),
                &profile.matched_signals,
            );
        }
    }
    priority.clamp(1, 100)
}

fn candidate_preseed_priority(
    candidate: &DomainCandidate,
    hint: Option<&SourceContactHint>,
    sales_profile: &SalesProfile,
) -> i32 {
    let mut priority = candidate.score;
    priority += domain_locality_bonus(&candidate.domain, &sales_profile.target_geo);
    if let Some(hint) = hint {
        priority += free_discovery_priority_boost(hint);
    }
    if profile_prefers_operator_accounts(sales_profile) {
        priority -= account_reachability_penalty(&candidate.domain, None, &candidate.evidence);
    }
    priority
}

fn candidate_execution_priority(
    candidate: &DomainCandidate,
    hint: Option<&SourceContactHint>,
    cached_profile: Option<&SalesProspectProfile>,
    sales_profile: &SalesProfile,
) -> i32 {
    let mut priority = candidate_preseed_priority(candidate, hint, sales_profile);
    if let Some(hint) = hint {
        priority += source_hint_contact_richness_bonus(hint);
    }
    if let Some(profile) = cached_profile {
        priority += prospect_harness_priority(profile, Some(sales_profile)) / 3;
    }
    priority
}

fn source_hint_contact_richness_bonus(hint: &SourceContactHint) -> i32 {
    let mut bonus = 0;
    if hint
        .contact_name
        .as_deref()
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false)
    {
        bonus += 8;
    }
    if hint
        .contact_title
        .as_deref()
        .map(|value| !contact_title_is_generic_default(Some(value)))
        .unwrap_or(false)
    {
        bonus += 4;
    }
    if hint.email.is_some() {
        bonus += 6;
    }
    bonus
}

fn prospect_contact_score(lead: &SalesLead) -> i32 {
    let mut score = 0;
    if !contact_name_is_placeholder(Some(lead.contact_name.as_str())) {
        score += 8;
    }
    score += contact_title_priority(&lead.contact_title);
    if lead
        .email
        .as_deref()
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false)
    {
        score += 4;
    }
    if lead
        .linkedin_url
        .as_deref()
        .and_then(normalize_outreach_linkedin_url)
        .is_some()
    {
        score += 4;
    }
    if !lead.reasons.is_empty() {
        score += 2;
    }
    score
}

fn prospect_status(
    primary_contact_name: Option<&str>,
    primary_email: Option<&str>,
    primary_linkedin_url: Option<&str>,
) -> &'static str {
    if primary_email
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false)
        || primary_linkedin_url
            .and_then(normalize_outreach_linkedin_url)
            .is_some()
    {
        "contact_ready"
    } else if primary_contact_name
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false)
    {
        "contact_identified"
    } else {
        "company_only"
    }
}

fn prospect_status_rank(status: &str) -> i32 {
    match status {
        "contact_ready" => 3,
        "contact_identified" => 2,
        _ => 1,
    }
}

fn clean_profile_contact_name(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || contact_name_is_placeholder(Some(trimmed)) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn clean_profile_contact_field(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn build_prospect_summary(
    company: &str,
    matched_signals: &[String],
    primary_contact_name: Option<&str>,
    primary_contact_title: Option<&str>,
    primary_email: Option<&str>,
    primary_linkedin_url: Option<&str>,
) -> String {
    let signal_text = if matched_signals.is_empty() {
        "public discovery signals".to_string()
    } else {
        matched_signals
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ")
    };
    let contact_text = match (primary_contact_name, primary_contact_title) {
        (Some(name), Some(title)) => format!("Primary contact: {name} ({title})"),
        (Some(name), None) => format!("Primary contact: {name}"),
        _ => "Primary contact still needs stronger verification".to_string(),
    };
    let channels_text = match (
        primary_email.map(|v| !v.trim().is_empty()).unwrap_or(false),
        primary_linkedin_url
            .and_then(normalize_outreach_linkedin_url)
            .is_some(),
    ) {
        (true, true) => "Channels: email + LinkedIn".to_string(),
        (true, false) => "Channels: email".to_string(),
        (false, true) => "Channels: LinkedIn".to_string(),
        (false, false) => "Channels: company-level only".to_string(),
    };

    truncate_cleaned_text(
        &format!("{company} fits via {signal_text}. {contact_text}. {channels_text}."),
        280,
    )
}

fn build_recommended_channel(
    primary_email: Option<&str>,
    primary_linkedin_url: Option<&str>,
) -> String {
    let has_email = primary_email
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false);
    let has_linkedin = primary_linkedin_url
        .and_then(normalize_outreach_linkedin_url)
        .is_some();
    match (has_email, has_linkedin) {
        (true, true) => "either".to_string(),
        (true, false) => "email".to_string(),
        (false, true) => "linkedin".to_string(),
        (false, false) => "research".to_string(),
    }
}

fn build_prospect_buyer_roles(
    sales_profile: Option<&SalesProfile>,
    primary_contact_title: Option<&str>,
    matched_signals: &[String],
) -> Vec<String> {
    let mut roles = Vec::new();

    if let Some(title) = primary_contact_title
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
    {
        roles.push(title.to_string());
    }

    let signal_blob = matched_signals
        .iter()
        .map(|signal| signal.to_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    let product_blob = sales_profile
        .map(|profile| profile.product_description.to_lowercase())
        .unwrap_or_default();

    if signal_blob.contains("field")
        || signal_blob.contains("dispatch")
        || signal_blob.contains("maintenance")
        || product_blob.contains("field")
    {
        roles.push("Head of Operations".to_string());
        roles.push("Field Service Manager".to_string());
    }

    if signal_blob.contains("facility") || signal_blob.contains("tesis") {
        roles.push("Facility Manager".to_string());
    }

    if sales_profile
        .map(|profile| profile.target_title_policy.as_str() == "ceo_only")
        .unwrap_or(false)
    {
        roles.push("CEO".to_string());
    } else {
        roles.push("COO".to_string());
        roles.push("Operations Director".to_string());
    }

    dedupe_strings(roles).into_iter().take(4).collect()
}

fn build_prospect_pain_points(
    sales_profile: Option<&SalesProfile>,
    matched_signals: &[String],
    company: &str,
) -> Vec<String> {
    let product_name = sales_profile
        .map(|profile| profile.product_name.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("Bu çözüm");
    let product_description = sales_profile
        .map(|profile| truncate_cleaned_text(&profile.product_description, 120))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dağınık operasyon akışlarını tek yerde toplamak".to_string());

    let top_signal = matched_signals
        .first()
        .cloned()
        .unwrap_or_else(|| format!("{company} için kamuya açık operasyon sinyalleri"));

    dedupe_strings(vec![
        format!(
            "{company} tarafında saha ekipleri, görev akışı ve dispatch görünürlüğü parçalı olabilir."
        ),
        format!(
            "{} ile ilişkili tekrar eden takip işleri için {} değeri güçlü görünüyor.",
            truncate_cleaned_text(&top_signal, 90),
            truncate_cleaned_text(product_name, 80)
        ),
        format!(
            "Mevcut süreçler muhtemelen WhatsApp, e-posta ve manuel koordinasyon arasında dağınık; {} burada doğrudan değer taşıyabilir.",
            product_description
        ),
    ])
    .into_iter()
    .take(3)
    .collect()
}

fn build_prospect_trigger_events(
    matched_signals: &[String],
    primary_contact_title: Option<&str>,
    profile_status: &str,
) -> Vec<String> {
    let mut triggers = Vec::new();
    if let Some(signal) = matched_signals.first() {
        triggers.push(format!(
            "Kamuya açık sinyal: {}",
            truncate_cleaned_text(signal, 100)
        ));
    }
    if let Some(title) = primary_contact_title
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
    {
        triggers.push(format!("{title} seviyesinde sahiplenme ihtimali var."));
    }
    match profile_status {
        "contact_ready" => {
            triggers.push("Doğrudan outbound başlatılabilecek kanal bulundu.".to_string())
        }
        "contact_identified" => {
            triggers.push("Karar verici bulundu, kanal doğrulaması kaldı.".to_string())
        }
        _ => triggers.push(
            "Şirket seviyesi sinyal var; buying committee haritalaması gerekiyor.".to_string(),
        ),
    }
    dedupe_strings(triggers).into_iter().take(3).collect()
}

fn build_prospect_outreach_angle(
    sales_profile: Option<&SalesProfile>,
    company: &str,
    pain_points: &[String],
    trigger_events: &[String],
    recommended_channel: &str,
) -> String {
    let product_name = sales_profile
        .map(|profile| profile.product_name.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("çözümümüz");
    let top_pain = pain_points
        .first()
        .cloned()
        .unwrap_or_else(|| format!("{company} için operasyon koordinasyonu"));
    let top_trigger = trigger_events
        .first()
        .cloned()
        .unwrap_or_else(|| "kamuya açık operasyon sinyali".to_string());

    truncate_cleaned_text(
        &format!(
            "{} için ilk temas: '{}' ve '{}' üzerinden {} ile 1 kısa operasyon iyileştirme hipotezi paylaş.",
            company,
            top_pain,
            top_trigger,
            match recommended_channel {
                "email" => format!("{product_name} değerini e-posta ile"),
                "linkedin" => format!("{product_name} değerini LinkedIn üzerinden"),
                "either" => format!("{product_name} değerini önce e-posta, sonra LinkedIn ile"),
                _ => format!("{product_name} değerini araştırma-notu şeklinde"),
            }
        ),
        220,
    )
}

fn heuristic_research_confidence(
    fit_score: i32,
    profile_status: &str,
    source_count: usize,
    contact_count: usize,
) -> f32 {
    let status_bonus = match profile_status {
        "contact_ready" => 0.22,
        "contact_identified" => 0.12,
        _ => 0.04,
    };
    let source_bonus = (source_count.min(6) as f32) * 0.04;
    let contact_bonus = (contact_count.min(3) as f32) * 0.05;
    ((fit_score as f32 / 100.0) * 0.55 + status_bonus + source_bonus + contact_bonus)
        .clamp(0.15, 0.98)
}

fn profile_prefers_operator_accounts(profile: &SalesProfile) -> bool {
    profile_targets_field_ops(profile) || geo_is_turkey(&profile.target_geo)
}

fn operator_account_domain_is_too_corporate(domain: &str) -> bool {
    let lower = domain.trim().to_lowercase();
    lower.contains("holding")
        || lower.contains("holdings")
        || lower.contains("yatirim")
        || lower.contains("yatırım")
        || lower.contains("investment")
}

fn candidate_should_skip_for_profile(candidate: &DomainCandidate, profile: &SalesProfile) -> bool {
    profile_prefers_operator_accounts(profile)
        && operator_account_domain_is_too_corporate(&candidate.domain)
}

fn domain_locality_bonus(domain: &str, target_geo: &str) -> i32 {
    if geo_is_turkey(target_geo) && (domain.ends_with(".tr") || domain.ends_with(".com.tr")) {
        6
    } else {
        0
    }
}

fn account_reachability_penalty(
    company_domain: &str,
    company: Option<&str>,
    evidence: &[String],
) -> i32 {
    let mut penalty = 0;
    let blob = format!(
        "{} {} {}",
        company_domain,
        company.unwrap_or_default(),
        evidence.join(" ")
    )
    .to_lowercase();

    let structural_hints = [
        "holding",
        "holdings",
        "group",
        "global",
        "international",
        "yatirim",
        "yatırım",
        "investment",
        "investor",
        "corporate",
        "kurumsal yonetim",
        "kurumsal yönetim",
        "investor relations",
        "yatirimci iliskileri",
        "yatırımcı ilişkileri",
    ];

    for hint in structural_hints {
        if blob.contains(hint) {
            penalty += 6;
        }
    }

    penalty.min(18)
}

fn collect_domains_from_search(search_output: &str, out: &mut Vec<String>) {
    let re = regex_lite::Regex::new(r"URL:\s+([^\s]+)").unwrap();
    for cap in re.captures_iter(search_output) {
        if let Some(url) = cap.get(1) {
            if let Some(domain) = extract_domain(url.as_str()) {
                out.push(domain);
            }
        }
    }

    let generic_url_re = regex_lite::Regex::new(r"https?://[^\s\)\]]+").unwrap();
    for m in generic_url_re.find_iter(search_output) {
        let url = m.as_str();
        if let Some(domain) = extract_domain(url) {
            out.push(domain);
        }
    }
}

fn extract_domain(raw_url: &str) -> Option<String> {
    let repaired = repair_common_url_typos(raw_url);
    let trimmed = repaired.trim_matches(|c: char| c == ')' || c == '(' || c == ',' || c == '.');
    let parsed = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        url::Url::parse(trimmed).ok()
    } else {
        url::Url::parse(&format!("https://{}", trimmed)).ok()
    }?;

    let host = parsed.host_str()?.trim_start_matches("www.").to_lowercase();
    if host.is_empty() {
        return None;
    }
    if host.contains("duckduckgo.com") || host.contains("linkedin.com") {
        return None;
    }
    if is_blocked_company_domain(&host) {
        return None;
    }
    if has_blocked_asset_tld(&host) {
        return None;
    }
    Some(host)
}

fn normalize_domain(raw: &str) -> String {
    extract_domain(raw).unwrap_or_else(|| {
        raw.trim()
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .trim_start_matches("www.")
            .trim_matches('/')
            .to_lowercase()
    })
}

fn has_blocked_asset_tld(domain: &str) -> bool {
    const BLOCKED_TLDS: &[&str] = &[
        "png", "jpg", "jpeg", "gif", "svg", "webp", "ico", "css", "js", "json", "xml", "pdf",
        "zip", "rar", "7z", "mp4", "webm", "mov",
    ];

    let tld = domain.rsplit('.').next().unwrap_or("");
    BLOCKED_TLDS.contains(&tld)
}

fn repair_common_url_typos(raw_url: &str) -> String {
    let mut trimmed = decode_basic_html_entities(raw_url).trim().to_string();
    if trimmed.starts_with("https:/") && !trimmed.starts_with("https://") {
        trimmed = format!(
            "https://{}",
            trimmed
                .trim_start_matches("https:/")
                .trim_start_matches('/')
        );
    } else if trimmed.starts_with("http:/") && !trimmed.starts_with("http://") {
        trimmed = format!(
            "http://{}",
            trimmed.trim_start_matches("http:/").trim_start_matches('/')
        );
    }
    if trimmed.starts_with("www.") {
        trimmed = format!("https://{trimmed}");
    }
    trimmed
}

fn is_directory_domain(domain: &str) -> bool {
    const DIRECTORY_HINTS: &[&str] = &[
        "yellowpages",
        "europages",
        "kompass",
        "cylex",
        "hotfrog",
        "clutch",
        "businesslist",
        "yelp",
        "merchantcircle",
        "yell",
        "b2b",
        "rehber",
        "firmasec",
        "tuugo",
        "opendi",
        "thomasnet",
        "companylist",
        "find-us-here",
        "canpages",
        "turkishbusiness",
    ];

    DIRECTORY_HINTS.iter().any(|hint| {
        domain == *hint || domain.ends_with(&format!(".{hint}")) || domain.contains(hint)
    })
}

fn is_consumer_email_domain(domain: &str) -> bool {
    const CONSUMER_EMAIL_DOMAINS: &[&str] = &[
        "gmail.com",
        "googlemail.com",
        "yahoo.com",
        "ymail.com",
        "rocketmail.com",
        "hotmail.com",
        "outlook.com",
        "live.com",
        "msn.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "protonmail.com",
        "proton.me",
        "mail.com",
        "aol.com",
        "gmx.com",
        "gmx.net",
        "yandex.com",
        "yandex.ru",
        "qq.com",
        "163.com",
    ];

    let normalized = domain.trim().trim_start_matches("www.").to_lowercase();
    CONSUMER_EMAIL_DOMAINS
        .iter()
        .any(|blocked| normalized == *blocked || normalized.ends_with(&format!(".{blocked}")))
}

fn is_valid_company_domain(domain: &str) -> bool {
    let d = normalize_domain(domain);
    !d.is_empty()
        && d.contains('.')
        && d.len() > 4
        && !is_consumer_email_domain(&d)
        && !is_blocked_company_domain(&d)
        && !d.ends_with(".gov.tr")
        && !d.ends_with(".edu.tr")
        && !d.ends_with(".mil.tr")
}

fn normalize_candidate_gateway(candidate: &mut DomainCandidate) -> bool {
    if !is_valid_company_domain(&candidate.domain) {
        return false;
    }
    candidate.domain = normalize_domain(&candidate.domain);
    candidate.phone = candidate.phone.as_deref().and_then(normalize_phone);
    candidate.matched_keywords = dedupe_strings(
        candidate
            .matched_keywords
            .iter()
            .map(|value| truncate_cleaned_text(value, 120))
            .filter(|value| !value.is_empty())
            .collect(),
    );
    candidate.evidence = dedupe_strings(
        candidate
            .evidence
            .iter()
            .map(|value| truncate_cleaned_text(value, 220))
            .filter(|value| !value.is_empty())
            .collect(),
    );
    candidate.source_links = merge_osint_links(Vec::new(), candidate.source_links.clone());
    true
}

fn normalize_free_candidate_gateway(
    mut candidate: FreeDiscoveryCandidate,
) -> Option<FreeDiscoveryCandidate> {
    if !normalize_candidate_gateway(&mut candidate.candidate) {
        return None;
    }
    candidate.contact_hint.email = candidate
        .contact_hint
        .email
        .clone()
        .and_then(|value| normalize_email_candidate(Some(value)));
    Some(candidate)
}

fn email_syntax_valid(email: &str) -> bool {
    let trimmed = email.trim();
    let Some((local, domain)) = trimmed.rsplit_once('@') else {
        return false;
    };
    if local.is_empty() || domain.is_empty() || local.len() > 64 || domain.len() > 255 {
        return false;
    }
    if local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') || domain.contains("..") {
        return false;
    }
    let local_ok = local
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '%' | '+' | '-'));
    let domain_ok = domain
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'));
    local_ok && domain_ok && domain.contains('.')
}

fn classify_email(email: &str, _company_domain: &str) -> &'static str {
    let e = email.trim().to_lowercase();
    if !email_syntax_valid(&e) {
        return "invalid";
    }
    let Some((local, domain)) = e.split_once('@') else {
        return "invalid";
    };
    if is_consumer_email_domain(domain) {
        return "consumer";
    }
    if email_is_generic_role_mailbox(&e) {
        return "generic";
    }
    let role_prefixes = [
        "sales",
        "hr",
        "support",
        "billing",
        "accounting",
        "marketing",
        "pr",
        "legal",
        "procurement",
        "satin",
        "satinalma",
    ];
    if role_prefixes.contains(&local) {
        return "generic";
    }
    "personal"
}

fn normalize_phone(raw: &str) -> Option<String> {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 10 {
        return None;
    }
    if digits.starts_with("90") && digits.len() == 12 {
        Some(format!("+{digits}"))
    } else if digits.starts_with('0') && digits.len() == 11 {
        Some(format!("+90{}", &digits[1..]))
    } else if digits.len() == 10 {
        Some(format!("+90{digits}"))
    } else {
        Some(format!("+{digits}"))
    }
}

fn transliterate_turkish_ascii(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            'ı' | 'İ' => 'i',
            'ş' | 'Ş' => 's',
            'ç' | 'Ç' => 'c',
            'ö' | 'Ö' => 'o',
            'ü' | 'Ü' => 'u',
            'ğ' | 'Ğ' => 'g',
            _ => c.to_ascii_lowercase(),
        })
        .collect()
}

fn is_placeholder_name(name: &str) -> bool {
    let normalized = transliterate_turkish_ascii(
        &decode_basic_html_entities(name)
            .replace(['\'', '’', '`'], "")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    );
    if normalized.trim().is_empty() {
        return true;
    }
    let placeholders = [
        "unknown",
        "leadership",
        "leadership team",
        "management",
        "management team",
        "executive team",
        "executive committee",
        "board of directors",
        "n/a",
        "not available",
        "undisclosed",
        "baskanin mesaji",
        "genel mudurun mesaji",
        "hakkimizda",
        "vizyonumuz",
        "misyonumuz",
        "iletisim",
        "kariyer",
        "basin",
        "ust yonetim",
        "yonetim ekibi",
        "yonetim takimi",
        "yonetim kurulu",
        "icra kurulu",
        "kurumsal",
        "anasayfa",
        "hakkinda",
        "referanslar",
        "projeler",
        "haberler",
        "duyurular",
        "galeri",
        "urunler",
        "hizmetler",
    ];
    placeholders
        .iter()
        .any(|placeholder| normalized == *placeholder || normalized.contains(placeholder))
}

fn is_blocked_company_domain(domain: &str) -> bool {
    const BLOCKED: &[&str] = &[
        "linkedin.com",
        "facebook.com",
        "instagram.com",
        "x.com",
        "twitter.com",
        "youtube.com",
        "wikipedia.org",
        "reddit.com",
        "medium.com",
        "forbes.com",
        "bloomberg.com",
        "wsj.com",
        "techcrunch.com",
        "crunchbase.com",
        "mordorintelligence.com",
        "techsciresearch.com",
        "researchandmarkets.com",
        "grandviewresearch.com",
        "gminsights.com",
        "marketsandmarkets.com",
        "fortunebusinessinsights.com",
        "statista.com",
        "expertmarketresearch.com",
        "g2.com",
        "capterra.com",
        "producthunt.com",
        "definitions.net",
        "merriam-webster.com",
        "cambridge.org",
        "dictionary.com",
        "thefreedictionary.com",
        "vocabulary.com",
        "wiktionary.org",
        "constructiondive.com",
        "finance.yahoo.com",
        "marketbeat.com",
        "barchart.com",
        "ptt.cc",
        "zhihu.com",
        "angel.co",
        "wellfound.com",
        "ycombinator.com",
        "indeed.com",
        "glassdoor.com",
        "duckduckgo.com",
        "google.com",
        "bing.com",
        "yahoo.com",
    ];
    const GLOBAL_GIANT_HINTS: &[&str] = &[
        "boschrexroth",
        "bosch",
        "siemens",
        "abb",
        "honeywell",
        "schneider-electric",
        "schneider",
        "cargill",
        "mitsubishi",
        "hitachi",
        "philips",
        "toyota-forklift",
        "toyota-industries",
    ];

    let static_blocked = BLOCKED
        .iter()
        .any(|blocked| domain == *blocked || domain.ends_with(&format!(".{blocked}")));
    if static_blocked {
        return true;
    }
    if has_blocked_asset_tld(domain) {
        return true;
    }

    if GLOBAL_GIANT_HINTS
        .iter()
        .any(|hint| domain == *hint || domain.contains(hint))
    {
        return true;
    }

    is_consumer_email_domain(domain)
        || domain.starts_with("blog.")
        || domain.contains("dictionary")
        || domain.contains("definitions")
        || domain.contains("wiktionary")
        || domain.contains("marketresearch")
}

fn extract_domains_from_text(text: &str) -> Vec<String> {
    let domain_re = regex_lite::Regex::new(
        r"(?i)\b(?:https?://)?(?:www\.)?[a-z0-9][a-z0-9-]{1,62}(?:\.[a-z0-9][a-z0-9-]{1,62})+\b",
    )
    .unwrap();

    let mut domains = Vec::<String>::new();
    for m in domain_re.find_iter(text) {
        if let Some(domain) = extract_domain(m.as_str()) {
            domains.push(domain);
        }
    }
    dedupe_strings(domains)
}

fn parse_search_entries(search_output: &str) -> Vec<SearchEntry> {
    let mut entries = Vec::<SearchEntry>::new();
    let mut current = SearchEntry::default();

    for raw in search_output.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }

        let is_title = line
            .split_once('.')
            .map(|(left, right)| {
                !left.is_empty()
                    && left.chars().all(|c| c.is_ascii_digit())
                    && !right.trim().is_empty()
            })
            .unwrap_or(false);

        if is_title {
            if !current.url.is_empty() {
                entries.push(current.clone());
            }
            current = SearchEntry::default();
            current.title = line
                .split_once('.')
                .map(|(_, right)| right.trim().to_string())
                .unwrap_or_default();
            continue;
        }

        if let Some(rest) = line.strip_prefix("URL:") {
            current.url = rest.trim().to_string();
            continue;
        }

        if line.starts_with("AI Summary:")
            || line.starts_with("Sources:")
            || line.starts_with("Search results for")
            || line.starts_with("[External Content:")
            || line.starts_with("[/External Content]")
        {
            continue;
        }

        if current.snippet.is_empty() {
            current.snippet = line.to_string();
        } else {
            current.snippet.push(' ');
            current.snippet.push_str(line);
        }
    }

    if !current.url.is_empty() {
        entries.push(current);
    }

    entries
}

fn normalize_keyword(s: &str) -> Option<String> {
    let t = s.trim().to_lowercase();
    if t.len() < 3 {
        return None;
    }
    Some(t)
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for v in values {
        let key = v.to_lowercase();
        if seen.insert(key) {
            out.push(v);
        }
    }
    out
}

fn expand_keywords(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(trimmed.to_string());
        for part in trimmed.split([',', '/', ';', '|']) {
            let p = part.trim();
            if p.len() >= 3 {
                out.push(p.to_string());
            }
        }
    }
    dedupe_strings(out)
}

fn score_search_entry(
    domain: &str,
    title: &str,
    snippet: &str,
    must_include_keywords: &[String],
    exclude_keywords: &[String],
    target_geo: &str,
    is_field_ops: bool,
) -> (i32, Vec<String>) {
    if is_blocked_company_domain(domain) {
        return (-100, Vec::new());
    }
    if is_directory_domain(domain) {
        return (-24, Vec::new());
    }

    let text = format!(
        "{domain} {} {}",
        title.to_lowercase(),
        snippet.to_lowercase()
    );
    let mut score = 0;
    let mut matched = Vec::<String>::new();

    for kw in must_include_keywords {
        if let Some(norm) = normalize_keyword(kw) {
            if text.contains(&norm) {
                score += if norm.contains(' ') { 8 } else { 5 };
                matched.push(norm);
            }
        }
    }

    for kw in exclude_keywords {
        if let Some(norm) = normalize_keyword(kw) {
            if text.contains(&norm) {
                score -= 8;
            }
        }
    }

    if title.to_lowercase().contains("careers")
        || title.to_lowercase().contains("jobs")
        || title.to_lowercase().contains("blog")
        || title.to_lowercase().contains("news")
    {
        score -= 6;
    }

    if is_field_ops && text_has_field_ops_signal(&text) {
        score += 8;
    }

    if geo_is_turkey(target_geo) && (domain.ends_with(".tr") || domain.ends_with(".com.tr")) {
        score += 6;
    }

    if text.contains("investor relations")
        || text.contains("yatirimci iliskileri")
        || text.contains("yatırımcı ilişkileri")
        || text.contains("annual report")
        || text.contains("faaliyet raporu")
        || text.contains("sustainability report")
        || text.contains("kurumsal yonetim")
        || text.contains("kurumsal yönetim")
    {
        score -= 6;
    }

    (score, dedupe_strings(matched))
}

fn collect_domain_candidates_from_search(
    search_output: &str,
    out: &mut HashMap<String, DomainCandidate>,
    must_include_keywords: &[String],
    exclude_keywords: &[String],
    target_geo: &str,
    is_field_ops: bool,
) {
    for entry in parse_search_entries(search_output) {
        let Some(result_domain) = extract_domain(&entry.url) else {
            continue;
        };
        if !is_valid_company_domain(&result_domain) {
            continue;
        }
        let text = format!("{} {}", entry.title, entry.snippet);
        let referenced_domains = extract_domains_from_text(&text);
        let is_directory = is_directory_domain(&result_domain);

        if !is_directory {
            let (score, matched) = score_search_entry(
                &result_domain,
                &entry.title,
                &entry.snippet,
                must_include_keywords,
                exclude_keywords,
                target_geo,
                is_field_ops,
            );
            let candidate = out.entry(result_domain.clone()).or_default();
            if candidate.domain.is_empty() {
                candidate.domain = result_domain.clone();
            }
            candidate.score += score;
            candidate.source_links =
                merge_osint_links(candidate.source_links.clone(), vec![entry.url.clone()]);
            if !entry.snippet.trim().is_empty() {
                if candidate.evidence.len() < 4 {
                    candidate
                        .evidence
                        .push(truncate_text_for_reason(&entry.snippet, 220));
                }
            } else if !entry.title.trim().is_empty() && candidate.evidence.len() < 4 {
                candidate
                    .evidence
                    .push(truncate_text_for_reason(&entry.title, 220));
            }
            candidate.matched_keywords.extend(matched);
            candidate.matched_keywords = dedupe_strings(candidate.matched_keywords.clone());
        }

        for referenced_domain in referenced_domains {
            if referenced_domain == result_domain || !is_valid_company_domain(&referenced_domain) {
                continue;
            }
            let (score, matched) = score_search_entry(
                &referenced_domain,
                &entry.title,
                &entry.snippet,
                must_include_keywords,
                exclude_keywords,
                target_geo,
                is_field_ops,
            );
            let text_lower = text.to_lowercase();
            let website_bonus = if text_lower.contains("website")
                || text_lower.contains("official site")
                || text_lower.contains("official website")
                || text_lower.contains("web sitesi")
            {
                6
            } else {
                2
            };
            let candidate = out.entry(referenced_domain.clone()).or_default();
            if candidate.domain.is_empty() {
                candidate.domain = referenced_domain.clone();
            }
            candidate.score += (score + website_bonus).max(2);
            candidate.source_links =
                merge_osint_links(candidate.source_links.clone(), vec![entry.url.clone()]);
            if candidate.evidence.len() < 4 {
                candidate.evidence.push(truncate_text_for_reason(
                    &format!("{} references {}", entry.title.trim(), referenced_domain),
                    220,
                ));
            }
            candidate.matched_keywords.extend(matched);
            candidate.matched_keywords = dedupe_strings(candidate.matched_keywords.clone());
        }
    }
}

fn dedupe_domain_candidates(items: Vec<DomainCandidate>) -> Vec<DomainCandidate> {
    let mut map = HashMap::<String, DomainCandidate>::new();
    for item in items {
        let key = normalize_domain(&item.domain);
        let entry = map.entry(key).or_default();
        if entry.domain.is_empty() {
            entry.domain = normalize_domain(&item.domain);
        }
        entry.score = entry.score.max(item.score);
        entry.evidence.extend(item.evidence);
        if entry.evidence.len() > 6 {
            entry.evidence.truncate(6);
        }
        entry.matched_keywords.extend(item.matched_keywords);
        entry.matched_keywords = dedupe_strings(entry.matched_keywords.clone());
        entry.source_links = merge_osint_links(entry.source_links.clone(), item.source_links);
        if entry.phone.is_none() {
            entry.phone = item.phone;
        }
    }
    map.into_values()
        .filter_map(|mut item| normalize_candidate_gateway(&mut item).then_some(item))
        .collect()
}

fn merge_free_discovery_candidate(
    candidates: &mut HashMap<String, DomainCandidate>,
    source_contact_hints: &mut HashMap<String, SourceContactHint>,
    free_candidate: FreeDiscoveryCandidate,
) {
    let directory_score = free_candidate.candidate.score
        + free_discovery_priority_boost(&free_candidate.contact_hint);
    let domain = free_candidate.candidate.domain.clone();
    if domain.is_empty() || !is_valid_company_domain(&domain) {
        return;
    }

    let entry = candidates.entry(domain.clone()).or_default();
    if entry.domain.is_empty() {
        entry.domain = domain.clone();
    }
    entry.score = entry.score.max(directory_score);
    entry.evidence.extend(free_candidate.candidate.evidence);
    if entry.evidence.len() > 6 {
        entry.evidence.truncate(6);
    }
    entry
        .matched_keywords
        .extend(free_candidate.candidate.matched_keywords);
    entry.matched_keywords = dedupe_strings(entry.matched_keywords.clone());
    entry.source_links = merge_osint_links(
        entry.source_links.clone(),
        free_candidate.candidate.source_links,
    );
    if entry.phone.is_none() {
        entry.phone = free_candidate.candidate.phone;
    }

    let hint = source_contact_hints.entry(domain).or_default();
    if hint.contact_name.is_none() {
        hint.contact_name = free_candidate.contact_hint.contact_name;
    }
    if hint.contact_title.is_none() {
        hint.contact_title = free_candidate.contact_hint.contact_title;
    }
    if hint.email.is_none() {
        hint.email = free_candidate.contact_hint.email;
    }
    if hint.source.is_none() {
        hint.source = free_candidate.contact_hint.source;
    }
}

fn free_discovery_priority_boost(hint: &SourceContactHint) -> i32 {
    let mut boost = 6;
    if hint.contact_name.is_some() {
        boost += 6;
    }
    if hint.contact_title.is_some() {
        boost += 2;
    }
    if hint.email.is_some() {
        boost += 4;
    }
    boost
}

fn contact_title_is_generic_default(title: Option<&str>) -> bool {
    title
        .map(|value| {
            let trimmed = value.trim();
            trimmed.is_empty() || trimmed == "CEO/Founder" || trimmed == "CEO"
        })
        .unwrap_or(true)
}

fn contact_name_is_placeholder(name: Option<&str>) -> bool {
    match name {
        None => true,
        Some(value) => is_placeholder_name(value),
    }
}

fn apply_source_contact_hint(
    domain: &str,
    hint: &SourceContactHint,
    contact_name: &mut Option<String>,
    contact_title: &mut Option<String>,
    email: &mut Option<String>,
) {
    if contact_name.is_none() || contact_name_is_placeholder(contact_name.as_deref()) {
        if let Some(name) = hint.contact_name.as_deref().and_then(normalize_person_name) {
            *contact_name = Some(name);
        }
    }

    let current_title_is_placeholder = contact_title_is_generic_default(contact_title.as_deref());
    if current_title_is_placeholder {
        if let Some(title) = hint.contact_title.as_deref() {
            let normalized = normalize_contact_title(title);
            if contact_title_priority(&normalized) > 0 {
                *contact_title = Some(normalized);
            }
        }
    }

    if email.is_none() {
        *email = normalize_contact_email_for_domain(hint.email.clone(), domain).or_else(|| {
            if source_hint_allows_offdomain_email(hint.source.as_deref()) {
                normalize_site_contact_email(hint.email.clone())
            } else {
                None
            }
        });
    }
}

fn source_hint_allows_offdomain_email(source: Option<&str>) -> bool {
    matches!(source, Some("ASMUD members page"))
}

async fn fetch_free_discovery_candidates(
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    if !geo_is_turkey(&profile.target_geo) {
        return Vec::new();
    }

    let client = match reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        )
        .build()
    {
        Ok(client) => client,
        Err(_) => return Vec::new(),
    };

    let fetch_tmb = async {
        if profile_targets_field_ops(profile) {
            fetch_tmb_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_eud = async {
        if profile_targets_energy(profile) {
            fetch_eud_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_asmud = async {
        if profile_targets_field_ops(profile) {
            fetch_asmud_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_platformder = async {
        if profile_targets_field_ops(profile) {
            fetch_platformder_directory_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_mib = async {
        if profile_targets_field_ops(profile) {
            fetch_mib_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_imder = async {
        if profile_targets_field_ops(profile) {
            fetch_imder_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_isder = async {
        if profile_targets_field_ops(profile) {
            fetch_isder_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_thbb = async {
        if profile_targets_field_ops(profile) {
            fetch_thbb_yazismali_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_eder = async {
        if profile_targets_digital_commerce(profile) {
            fetch_eder_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_lojider = async {
        if profile_targets_logistics(profile) {
            fetch_lojider_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_tfyd = async {
        if profile_targets_events_exhibitions(profile) {
            fetch_tfyd_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_oss = async {
        if profile_targets_automotive(profile) {
            fetch_oss_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_ida = async {
        if profile_targets_pr_communications(profile) {
            fetch_ida_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_tesid = async {
        if profile_targets_electronics(profile) {
            fetch_tesid_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_tudis = async {
        if profile_targets_leather(profile) {
            fetch_tudis_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_emsad = async {
        if profile_targets_electromechanical(profile) {
            fetch_emsad_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_tgsd = async {
        if profile_targets_textile_apparel(profile) {
            fetch_tgsd_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_ared = async {
        if profile_targets_advertising_signage(profile) {
            fetch_ared_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };
    let fetch_todeb = async {
        if profile_targets_fintech_payments(profile) {
            fetch_todeb_member_candidates(&client, profile, run_sequence).await
        } else {
            Vec::new()
        }
    };

    let (
        tmb,
        eud,
        asmud,
        platformder,
        mib,
        imder,
        isder,
        thbb,
        eder,
        lojider,
        tfyd,
        oss,
        ida,
        tesid,
        tudis,
        emsad,
        tgsd,
        ared,
        todeb,
    ) = tokio::join!(
        fetch_tmb,
        fetch_eud,
        fetch_asmud,
        fetch_platformder,
        fetch_mib,
        fetch_imder,
        fetch_isder,
        fetch_thbb,
        fetch_eder,
        fetch_lojider,
        fetch_tfyd,
        fetch_oss,
        fetch_ida,
        fetch_tesid,
        fetch_tudis,
        fetch_emsad,
        fetch_tgsd,
        fetch_ared,
        fetch_todeb
    );
    interleave_free_discovery_sources(
        vec![
            tmb,
            eud,
            asmud,
            platformder,
            mib,
            imder,
            isder,
            thbb,
            eder,
            lojider,
            tfyd,
            oss,
            ida,
            tesid,
            tudis,
            emsad,
            tgsd,
            ared,
            todeb,
        ],
        MAX_FREE_DIRECTORY_CANDIDATES,
    )
}

async fn fetch_tmb_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tmb.org.tr/en/members",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tmb_member_candidates(&html, profile, run_sequence, MAX_TMB_DIRECTORY_CANDIDATES)
}

fn parse_tmb_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let article_re = regex_lite::Regex::new(
        r#"(?is)<article[^>]*class="member-card[^"]*"[^>]*>(.*?)</article>"#,
    )
    .unwrap();
    let detail_re =
        regex_lite::Regex::new(r#"(?is)<div[^>]*class="name"[^>]*>\s*<a[^>]*href="([^"]+)""#)
            .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<div[^>]*class="name"[^>]*>\s*<a[^>]*>(.*?)</a>"#).unwrap();
    let chairman_re =
        regex_lite::Regex::new(r#"(?is)Chairman of the Board\s*:\s*<strong>(.*?)</strong>"#)
            .unwrap();
    let web_re = regex_lite::Regex::new(
        r#"(?is)<th[^>]*>\s*Web\s*</th>\s*<td[^>]*>\s*:\s*</td>\s*<td[^>]*>\s*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in article_re.captures_iter(html) {
        let article_html = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let detail_url = detail_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|href| absolutize_source_link("https://www.tmb.org.tr", &href));
        let web_href = web_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str().trim().to_string()));
        let Some(domain) = web_href.as_deref().and_then(extract_domain) else {
            continue;
        };
        if is_blocked_company_domain(&domain) || !seen.insert(domain.clone()) {
            continue;
        }

        let company = company_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_else(|| domain.clone());
        let chairman_name = chairman_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .and_then(|value| normalize_turkish_source_person_name(value.trim()));
        let email =
            normalize_directory_email_for_domain(extract_email_from_text(article_html), &domain);

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
                evidence: vec![format!(
                    "TMB members directory lists {} as a Turkish contractor member with website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(vec![
                    profile.target_industry.clone(),
                    "construction".to_string(),
                    "infrastructure".to_string(),
                    "contractor association".to_string(),
                ]),
                source_links: detail_url
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .chain(std::iter::once(
                        "https://www.tmb.org.tr/en/members".to_string(),
                    ))
                    .collect(),
                phone: None,
            },
            contact_hint: SourceContactHint {
                contact_name: chairman_name,
                contact_title: Some("Chairman".to_string()),
                email,
                source: Some("TMB members directory".to_string()),
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_eud_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.eud.org.tr/en/members",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_eud_member_candidates(&html, profile, run_sequence, MAX_EUD_DIRECTORY_CANDIDATES)
}

fn parse_eud_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let link_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href=['"]([^'"]+)['"][^>]*>\s*<div[^>]*class=['"][^'"]*\bmember-box\b[^'"]*['"]"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in link_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        let Some(domain) = extract_domain(&href) else {
            continue;
        };
        if domain == "eud.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![format!(
                    "EUD members page links {} as an official Turkish electricity producer site",
                    domain
                )],
                matched_keywords: dedupe_strings(vec![
                    profile.target_industry.clone(),
                    "energy".to_string(),
                    "utility".to_string(),
                    "power generation".to_string(),
                ]),
                source_links: vec!["https://www.eud.org.tr/en/members".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("EUD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_asmud_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.asmud.org.tr/Uyeler.asp",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_asmud_member_candidates(&html, profile, run_sequence, MAX_ASMUD_DIRECTORY_CANDIDATES)
}

fn parse_asmud_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let link_re = regex_lite::Regex::new(r#"(?is)<a[^>]*href="([^"]+)""#).unwrap();
    let company_re = regex_lite::Regex::new(r#"(?is)<strong>(.*?)</strong>"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)(?:^|<br>\s*)T:\s*([^<]+)"#).unwrap();
    let email_re =
        regex_lite::Regex::new(r#"(?is)(?:^|<br>\s*)E:\s*<span[^>]*>([^<]+)</span>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="uwrap w3-card">"#).skip(1) {
        let raw_url = link_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .unwrap_or_default();
        let Some(domain) = extract_domain(&raw_url) else {
            continue;
        };
        if domain == "asmud.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| normalize_directory_email_for_domain(Some(value), &domain));

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "asphalt".to_string(),
            "road construction".to_string(),
            "infrastructure".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("inşaat") || company_lower.contains("insaat") {
            matched_keywords.push("construction".to_string());
        }
        if company_lower.contains("makine") || company_lower.contains("makina") {
            matched_keywords.push("construction equipment".to_string());
            matched_keywords.push("equipment".to_string());
        }
        if company_lower.contains("altyapı") || company_lower.contains("altyapi") {
            matched_keywords.push("field operations".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "ASMUD members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "ASMUD members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "ASMUD members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "ASMUD members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.asmud.org.tr/Uyeler.asp".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("ASMUD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_platformder_directory_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.platformder.org.tr/rehber/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_platformder_directory_candidates(
        &html,
        profile,
        run_sequence,
        MAX_PLATFORMDER_DIRECTORY_CANDIDATES,
    )
}

fn parse_platformder_directory_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let item_re = regex_lite::Regex::new(
        r#"(?is)<li[^>]*id="item-\d+-\d+"[^>]*\bdata-title="([^"]+)"[^>]*\bdata-phone="([^"]*)"[^>]*\bdata-url="([^"]*)"[^>]*>(.*?)</li>"#,
    )
    .unwrap();
    let site_link_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href=['"]([^'"]+)['"][^>]*title=['"]Go to website['"]"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in item_re.captures_iter(html) {
        let company = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if company.is_empty() {
            continue;
        }

        let phone = cap
            .get(2)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let raw_url = cap
            .get(3)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        let item_html = cap.get(4).map(|m| m.as_str()).unwrap_or("");

        let raw_domain = if raw_url.is_empty() {
            site_link_re
                .captures(item_html)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().trim().to_string()))
                .and_then(|href| extract_domain(&href))
                .or_else(|| {
                    extract_domains_from_text(item_html)
                        .into_iter()
                        .find(|domain| domain != "platformder.org.tr")
                })
        } else {
            extract_domain(&raw_url)
        };
        let Some(domain) = raw_domain else {
            continue;
        };
        if domain == "platformder.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "platform rental".to_string(),
            "equipment rental".to_string(),
            "field service".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("platform") {
            matched_keywords.push("access platform".to_string());
        }
        if company_lower.contains("vinç") || company_lower.contains("vinc") {
            matched_keywords.push("vinç".to_string());
            matched_keywords.push("crane".to_string());
        }
        if company_lower.contains("forklift") {
            matched_keywords.push("forklift".to_string());
        }
        if company_lower.contains("lift") {
            matched_keywords.push("lift".to_string());
        }
        if company_lower.contains("makine") {
            matched_keywords.push("equipment".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "Platformder rehber lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "Platformder rehber lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.platformder.org.tr/rehber/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("Platformder rehber".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_mib_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let pages = mib_directory_pages_for_run(
        run_sequence,
        MIB_DIRECTORY_PAGE_COUNT,
        MIB_DIRECTORY_PAGES_PER_RUN,
    );
    let page_results = join_all(pages.into_iter().map(|page| async move {
        let url = if page == 1 {
            "https://mib.org.tr/en/our-members/".to_string()
        } else {
            format!("https://mib.org.tr/en/our-members/{page}/")
        };
        let html = fetch_html_page(client, &url, FREE_DIRECTORY_FETCH_TIMEOUT_MS).await;
        (page, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (page, html) in page_results {
        let Some(html) = html else {
            continue;
        };
        let page_url = if page == 1 {
            "https://mib.org.tr/en/our-members/".to_string()
        } else {
            format!("https://mib.org.tr/en/our-members/{page}/")
        };
        for mut candidate in parse_mib_member_candidates(
            &html,
            profile,
            run_sequence + page.saturating_sub(1),
            MAX_MIB_DIRECTORY_CANDIDATES,
        ) {
            candidate.candidate.source_links = merge_osint_links(
                candidate.candidate.source_links.clone(),
                vec![page_url.clone()],
            );
            let domain_key = candidate.candidate.domain.to_lowercase();
            if !seen.insert(domain_key) {
                continue;
            }
            out.push(candidate);
        }
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_MIB_DIRECTORY_CANDIDATES);
    out
}

fn mib_directory_pages_for_run(
    run_sequence: usize,
    page_count: usize,
    pages_per_run: usize,
) -> Vec<usize> {
    if page_count == 0 || pages_per_run == 0 {
        return Vec::new();
    }

    let take = pages_per_run.min(page_count);
    let start = run_sequence % page_count;
    let mut out = Vec::with_capacity(take);
    for offset in 0..take {
        out.push(((start + offset) % page_count) + 1);
    }
    out
}

fn parse_mib_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>\s*</h2>"#)
            .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href="([^"]+)"[^>]*>\s*<span[^>]*>\s*<i[^>]*class="[^"]*fa-globe[^"]*"[^>]*>"#,
    )
    .unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)href="mailto:([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html
        .split(r#"<div data-elementor-type="loop-item""#)
        .skip(1)
    {
        let block_html = format!(r#"<div data-elementor-type="loop-item"{segment}"#);
        if !block_html.contains("firm") {
            continue;
        }
        let detail_url = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|href| absolutize_source_link("https://mib.org.tr", &href));
        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(2).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if company.is_empty() {
            continue;
        }

        let website = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())));
        let Some(domain) = website.as_deref().and_then(extract_domain) else {
            continue;
        };
        if domain == "mib.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let block_text = strip_html_tags(&decode_basic_html_entities(&block_html));
        let source_text = format!("{company} {block_text}");
        let source_lower = source_text.to_lowercase();
        let looks_relevant = text_has_field_ops_signal(&source_text)
            || source_lower.contains("makine")
            || source_lower.contains("makina")
            || source_lower.contains("platform")
            || source_lower.contains("forklift")
            || source_lower.contains("lift")
            || source_lower.contains("vinc")
            || source_lower.contains("vinç");
        if !looks_relevant {
            continue;
        }

        let email = email_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| m.as_str().trim().to_string()))
            .and_then(|value| normalize_directory_email_for_domain(Some(value), &domain));

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "industrial equipment".to_string(),
            "field equipment".to_string(),
            "machinery association".to_string(),
        ];
        if source_lower.contains("makine") || source_lower.contains("makina") {
            matched_keywords.push("makine".to_string());
        }
        if source_lower.contains("platform") {
            matched_keywords.push("access platform".to_string());
        }
        if source_lower.contains("forklift") {
            matched_keywords.push("forklift".to_string());
        }
        if source_lower.contains("lift") {
            matched_keywords.push("lift".to_string());
        }
        if source_lower.contains("vinc") || source_lower.contains("vinç") {
            matched_keywords.push("vinç".to_string());
            matched_keywords.push("crane".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![format!(
                    "MIB members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: detail_url.into_iter().collect(),
                phone: None,
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("MIB members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_imder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(index_html) = fetch_html_page(
        client,
        "https://imder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    let detail_urls =
        parse_imder_member_index_urls(&index_html, run_sequence, MAX_IMDER_DETAIL_FETCHES);
    if detail_urls.is_empty() {
        return Vec::new();
    }

    let detail_pages = join_all(detail_urls.iter().map(|url| async move {
        let html = fetch_html_page(client, url, FREE_DIRECTORY_FETCH_TIMEOUT_MS + 400).await;
        (url, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (detail_url, html) in detail_pages {
        let Some(html) = html else {
            continue;
        };
        let Some(candidate) = parse_imder_member_detail_candidate(&html, detail_url, profile)
        else {
            continue;
        };
        let domain_key = candidate.candidate.domain.to_lowercase();
        if !seen.insert(domain_key) {
            continue;
        }
        out.push(candidate);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_IMDER_DIRECTORY_CANDIDATES);
    out
}

fn parse_imder_member_index_urls(html: &str, run_sequence: usize, max_urls: usize) -> Vec<String> {
    let detail_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*class="[^"]*\belementor-post__thumbnail__link\b[^"]*"[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for cap in detail_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        if href.is_empty() || !href.contains("imder.org.tr/") {
            continue;
        }
        if !seen.insert(href.clone()) {
            continue;
        }
        out.push(href);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_urls);
    out
}

fn parse_imder_member_detail_candidate(
    html: &str,
    detail_url: &str,
    profile: &SalesProfile,
) -> Option<FreeDiscoveryCandidate> {
    let title_re = regex_lite::Regex::new(
        r#"(?is)<h1[^>]*class="[^"]*elementor-heading-title[^"]*"[^>]*>(.*?)</h1>"#,
    )
    .unwrap();
    let name_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*İsim Soyisim\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let role_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*G(?:ö|o)revi\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>\s*Telefon\s*</strong>\s*</td>\s*<td>(.*?)</td>"#)
            .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web Sitesi\s*</strong>\s*</td>\s*<td>\s*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();
    let tag_re = regex_lite::Regex::new(r#"(?is)rel="tag">(.*?)</a>"#).unwrap();

    let company = title_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value| {
            strip_html_tags(&value)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())?;

    let raw_name = name_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_title = role_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let phone = phone_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_site = website_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
        .or_else(|| {
            regex_lite::Regex::new(
                r#"(?is)<strong>\s*Web Sitesi\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
            )
            .unwrap()
            .captures(html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(|value_html| {
                strip_html_tags(&decode_basic_html_entities(value_html))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
        });

    let domain = raw_site
        .as_deref()
        .and_then(extract_domain)
        .filter(|domain| !is_blocked_company_domain(domain))?;

    let contact_name = raw_name
        .as_deref()
        .and_then(normalize_turkish_source_person_name)
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                normalize_person_name(&rewritten)
            })
        })
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                let token_count = rewritten.split_whitespace().count();
                let alpha_only = rewritten.split_whitespace().all(|token| {
                    token
                        .chars()
                        .all(|c| c.is_alphabetic() || matches!(c, '.' | '\'' | '-'))
                });
                if (2..=4).contains(&token_count) && alpha_only {
                    Some(rewritten)
                } else {
                    None
                }
            })
        })
        .or_else(|| raw_name.as_deref().and_then(normalize_person_name));
    let contact_title = raw_title.as_deref().map(normalize_contact_title);
    let email = normalize_directory_email_for_domain(extract_email_from_text(html), &domain);

    let mut matched_keywords = vec![
        profile.target_industry.clone(),
        "construction equipment".to_string(),
        "mobile equipment".to_string(),
        "field equipment".to_string(),
        "association detail".to_string(),
    ];
    for cap in tag_re.captures_iter(html) {
        let tag = cap
            .get(1)
            .map(|m| strip_html_tags(&decode_basic_html_entities(m.as_str())))
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty());
        if let Some(tag) = tag {
            matched_keywords.push(tag);
        }
    }

    let plain = strip_html_tags(&decode_basic_html_entities(html));
    let plain_lower = plain.to_lowercase();
    for signal in [
        "bakım",
        "bakim",
        "kurulum",
        "forklift",
        "ekskavator",
        "ekskavatör",
        "telehandler",
        "vinç",
        "vinc",
        "iş makinesi",
        "is makinesi",
        "mobil iş makine",
        "construction",
        "equipment",
    ] {
        if plain_lower.contains(signal) {
            matched_keywords.push(signal.to_string());
        }
    }

    let evidence = if let Some(phone) = phone.as_deref() {
        format!(
            "IMDER member detail lists {} with official website {} and public phone {}",
            truncate_text_for_reason(&company, 120),
            domain,
            phone
        )
    } else {
        format!(
            "IMDER member detail lists {} with official website {}",
            truncate_text_for_reason(&company, 120),
            domain
        )
    };

    Some(FreeDiscoveryCandidate {
        candidate: DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
            evidence: vec![
                evidence,
                truncate_text_for_reason(&format!("Source detail: {}", detail_url), 220),
            ],
            matched_keywords: dedupe_strings(matched_keywords),
            source_links: vec![detail_url.to_string()],
            phone: phone.as_deref().and_then(normalize_phone),
        },
        contact_hint: SourceContactHint {
            contact_name,
            contact_title,
            email,
            source: Some("IMDER member detail".to_string()),
        },
    })
}

async fn fetch_isder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(index_html) = fetch_html_page(
        client,
        "https://isder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    let detail_urls =
        parse_isder_member_index_urls(&index_html, run_sequence, MAX_ISDER_DETAIL_FETCHES);
    if detail_urls.is_empty() {
        return Vec::new();
    }

    let detail_pages = join_all(detail_urls.iter().map(|url| async move {
        let html = fetch_html_page(client, url, FREE_DIRECTORY_FETCH_TIMEOUT_MS + 400).await;
        (url, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (detail_url, html) in detail_pages {
        let Some(html) = html else {
            continue;
        };
        let Some(candidate) = parse_isder_member_detail_candidate(&html, detail_url, profile)
        else {
            continue;
        };
        let domain_key = candidate.candidate.domain.to_lowercase();
        if !seen.insert(domain_key) {
            continue;
        }
        out.push(candidate);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_ISDER_DIRECTORY_CANDIDATES);
    out
}

fn parse_isder_member_index_urls(html: &str, run_sequence: usize, max_urls: usize) -> Vec<String> {
    let detail_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*class="[^"]*\belementor-post__thumbnail__link\b[^"]*"[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for cap in detail_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        if href.is_empty() || !href.contains("isder.org.tr/") {
            continue;
        }
        if !seen.insert(href.clone()) {
            continue;
        }
        out.push(href);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_urls);
    out
}

fn parse_isder_member_detail_candidate(
    html: &str,
    detail_url: &str,
    profile: &SalesProfile,
) -> Option<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(
        r#"(?is)<h1[^>]*class="[^"]*elementor-heading-title[^"]*"[^>]*>(.*?)</h1>"#,
    )
    .unwrap();
    let name_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*İsim Soyisim:?\s*</strong>\s*</td>\s*<td>(.*?)</tr>"#,
    )
    .unwrap();
    let role_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Ünvan Görevi:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();
    let phone_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Telefon:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web Sitesi:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();

    let company = company_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value| {
            strip_html_tags(&value)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())?;

    let raw_name = name_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_title = role_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let phone = phone_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_site = website_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());

    let domain = raw_site
        .as_deref()
        .and_then(extract_domain)
        .filter(|domain| !is_blocked_company_domain(domain))?;
    let contact_name = raw_name
        .as_deref()
        .and_then(normalize_turkish_source_person_name)
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                normalize_person_name(&rewritten)
            })
        });
    let contact_title = raw_title.as_deref().and_then(|value| {
        let normalized = normalize_contact_title(value);
        if contact_title_priority(&normalized) > 0 {
            Some(normalized)
        } else {
            None
        }
    });
    let email = normalize_directory_email_for_domain(extract_email_from_text(html), &domain);

    let source_text = format!(
        "{company} {}",
        strip_html_tags(&decode_basic_html_entities(html))
    );
    let source_lower = source_text.to_lowercase();
    let mut matched_keywords = vec![
        profile.target_industry.clone(),
        "industrial equipment".to_string(),
        "field equipment".to_string(),
        "material handling".to_string(),
    ];
    if source_lower.contains("istif") {
        matched_keywords.push("material handling".to_string());
    }
    if source_lower.contains("forklift") {
        matched_keywords.push("forklift".to_string());
    }
    if source_lower.contains("platform") {
        matched_keywords.push("access platform".to_string());
    }
    if source_lower.contains("vinç") || source_lower.contains("vinc") {
        matched_keywords.push("vinç".to_string());
        matched_keywords.push("crane".to_string());
    }
    if source_lower.contains("raf") {
        matched_keywords.push("storage systems".to_string());
    }
    if source_lower.contains("akü") || source_lower.contains("aku") {
        matched_keywords.push("battery".to_string());
    }
    if let Some(signal) = extract_field_ops_signal_keyword(&source_text) {
        matched_keywords.push(signal.to_string());
    }

    let evidence = if let Some(phone) = phone.as_deref() {
        format!(
            "ISDER member detail lists {} with official website {} and public phone {}",
            truncate_text_for_reason(&company, 120),
            domain,
            phone
        )
    } else {
        format!(
            "ISDER member detail lists {} with official website {}",
            truncate_text_for_reason(&company, 120),
            domain
        )
    };

    Some(FreeDiscoveryCandidate {
        candidate: DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
            evidence: vec![
                evidence,
                truncate_text_for_reason(&format!("Source detail: {}", detail_url), 220),
            ],
            matched_keywords: dedupe_strings(matched_keywords),
            source_links: vec![detail_url.to_string()],
            phone: phone.as_deref().and_then(normalize_phone),
        },
        contact_hint: SourceContactHint {
            contact_name,
            contact_title,
            email,
            source: Some("ISDER member detail".to_string()),
        },
    })
}

async fn fetch_thbb_yazismali_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.thbb.org/uyelerimiz/yazismali-uyeler/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_thbb_yazismali_candidates(&html, profile, run_sequence, MAX_THBB_DIRECTORY_CANDIDATES)
}

fn parse_thbb_yazismali_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let td_re = regex_lite::Regex::new(r#"(?is)<td>(.*?)</td>"#).unwrap();
    let web_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web:?\s*</strong>\s*(?:&nbsp;|\s)*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>\s*Tel:?\s*</strong>\s*(?:&nbsp;|\s)*([^<]+)"#)
            .unwrap();
    let strong_re = regex_lite::Regex::new(r#"(?is)<strong>(.*?)</strong>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in td_re.captures_iter(html) {
        let td_html = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        if !td_html.contains("Web:") || !td_html.contains("E-posta") {
            continue;
        }

        let Some(domain) = web_re
            .captures(td_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "thbb.org"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let phone = phone_re
            .captures(td_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let company = strong_re
            .captures_iter(td_html)
            .filter_map(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .find(|value| {
                let lower = value.to_lowercase();
                !value.is_empty()
                    && !lower.ends_with(':')
                    && lower != "adres"
                    && lower != "tel"
                    && lower != "faks"
                    && lower != "e-posta"
                    && lower != "web"
                    && lower != "web:"
            })
            .unwrap_or_else(|| domain.clone());

        let email = normalize_directory_email_for_domain(extract_email_from_text(td_html), &domain);
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "construction equipment".to_string(),
            "field equipment".to_string(),
            "concrete equipment".to_string(),
        ];
        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(td_html))
        );
        let source_lower = source_text.to_lowercase();
        if source_lower.contains("beton") || source_lower.contains("concrete") {
            matched_keywords.push("concrete plant".to_string());
            matched_keywords.push("ready mix concrete".to_string());
        }
        if source_lower.contains("mixer") || source_lower.contains("transmikser") {
            matched_keywords.push("mixer".to_string());
        }
        if source_lower.contains("pompa") || source_lower.contains("pump") {
            matched_keywords.push("pump".to_string());
        }
        if source_lower.contains("iş makine") || source_lower.contains("is makine") {
            matched_keywords.push("iş makinesi".to_string());
        }
        if source_lower.contains("makine") || source_lower.contains("makina") {
            matched_keywords.push("equipment".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "THBB yazismali uyeler page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "THBB yazismali uyeler page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.thbb.org/uyelerimiz/yazismali-uyeler/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("THBB yazismali uyeler".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_eder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://eder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_eder_member_candidates(&html, profile, run_sequence, MAX_EDER_DIRECTORY_CANDIDATES)
}

fn parse_eder_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let item_re = regex_lite::Regex::new(
        r#"(?is)<div class="ui-e-ico-box" onclick="window\.open\(&#039;([^&]+?)&#039;,\s*&#039;_blank&#039;\)">(.*?)<div class="ui-e-description">\s*<p>(.*?)</p>"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in item_re.captures_iter(html) {
        let raw_url = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .unwrap_or_default();
        let Some(domain) = extract_domain(&raw_url) else {
            continue;
        };
        if domain == "eder.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let description = cap
            .get(3)
            .map(|m| strip_html_tags(&decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let mut company = description.clone();
        for suffix in [
            " E-Ticaret Yazılımları",
            " E-Ticaret Yazilimlari",
            " Tahsilat Yazılımları",
            " Tahsilat Yazilimlari",
            " Yazılımları",
            " Yazilimlari",
        ] {
            if company.ends_with(suffix) {
                company = company.trim_end_matches(suffix).trim().to_string();
                break;
            }
        }
        if company.is_empty() {
            company = domain.clone();
        }

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "digital commerce".to_string(),
            "e-commerce infrastructure".to_string(),
            "commerce software".to_string(),
        ];
        let description_lower = description.to_lowercase();
        if description_lower.contains("e-ticaret") || description_lower.contains("eticaret") {
            matched_keywords.push("e-commerce".to_string());
        }
        if description_lower.contains("tahsilat") {
            matched_keywords.push("payments".to_string());
            matched_keywords.push("collections".to_string());
        }
        if description_lower.contains("altyap") {
            matched_keywords.push("platform infrastructure".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "EDER uyelerimiz page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://eder.org.tr/uyelerimiz/".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("EDER members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_lojider_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.lojider.org.tr/Member-List",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_lojider_member_candidates(
        &html,
        profile,
        run_sequence,
        MAX_LOJIDER_DIRECTORY_CANDIDATES,
    )
}

fn parse_lojider_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(r#"(?is)<b class="d-block">(.*?)</b>"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)href="tel:[^"]+">\s*([^<]+?)\s*</a>"#).unwrap();
    let website_re =
        regex_lite::Regex::new(r#"(?is)<i[^>]*fa-paper-plane[^>]*></i>\s*<a[^>]*href="([^"]+)""#)
            .unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)<i[^>]*fa-user[^>]*></i>\s*([^<]+?)\s*</div>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="row mb-4 member-row">"#).skip(1) {
        let block_html = format!(r#"<div class="row mb-4 member-row">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "lojider.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let raw_contact = contact_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let contact_name = raw_contact
            .as_deref()
            .and_then(normalize_turkish_source_person_name)
            .or_else(|| raw_contact.as_deref().and_then(normalize_person_name));
        let email =
            normalize_directory_email_for_domain(extract_email_from_text(&block_html), &domain);

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "logistics".to_string(),
            "freight".to_string(),
            "transport association".to_string(),
        ];
        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        if source_lower.contains("gümrük") || source_lower.contains("gumruk") {
            matched_keywords.push("customs".to_string());
        }
        if source_lower.contains("antrepo") || source_lower.contains("depo") {
            matched_keywords.push("warehousing".to_string());
        }
        if source_lower.contains("nakliye") || source_lower.contains("taş") {
            matched_keywords.push("transport".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "LojiDer member list shows {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "LojiDer member list shows {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "LojiDer member list shows {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "LojiDer member list shows {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.lojider.org.tr/Member-List".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("LojiDer members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tfyd_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tfyd.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tfyd_member_candidates(&html, profile, run_sequence, MAX_TFYD_DIRECTORY_CANDIDATES)
}

fn parse_tfyd_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str()))
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();
        if cells.len() < 4 {
            continue;
        }

        let company = cells[1].trim().to_string();
        let Some(domain) = extract_domain(&cells[2]) else {
            continue;
        };
        if company.is_empty()
            || domain == "tfyd.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let phone = normalize_phone(&cells[3]);
        let source_text = format!("{company} {}", cells.join(" "));
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "events & exhibitions".to_string(),
            "fair organization".to_string(),
            "event services".to_string(),
        ];
        if source_lower.contains("fuar") {
            matched_keywords.push("fair".to_string());
        }
        if source_lower.contains("organizasyon") {
            matched_keywords.push("event organization".to_string());
        }
        if source_lower.contains("kongre") {
            matched_keywords.push("congress".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "TFYD uyelerimiz page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "TFYD uyelerimiz page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.tfyd.org.tr/uyelerimiz".to_string()],
                phone,
            },
            contact_hint: SourceContactHint {
                source: Some("TFYD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_oss_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.oss.org.tr/en/members/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_oss_member_candidates(&html, profile, run_sequence, MAX_OSS_DIRECTORY_CANDIDATES)
}

fn parse_oss_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h5 class="card-title">\s*(.*?)</h5>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)bi bi-telephone-fill"></i>\s*([^<]+?)\s*</li>"#).unwrap();
    let website_re = regex_lite::Regex::new(r#"(?is)window\.open\('([^']+)'\)"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="card membercard">"#).skip(1) {
        let block_html = format!(r#"<div class="card membercard">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "oss.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "automotive aftermarket".to_string(),
            "spare parts".to_string(),
            "aftermarket association".to_string(),
        ];
        if source_lower.contains("otomotiv") || source_lower.contains("automotive") {
            matched_keywords.push("automotive".to_string());
        }
        if source_lower.contains("yedek par") || source_lower.contains("spare part") {
            matched_keywords.push("spare parts".to_string());
        }
        if source_lower.contains("filtre") {
            matched_keywords.push("filters".to_string());
        }
        if source_lower.contains("suspansiyon") || source_lower.contains("süspansiyon") {
            matched_keywords.push("suspension".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "OSS members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "OSS members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.oss.org.tr/en/members/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("OSS members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_ida_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.ida.org.tr/ornek-sayfa/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_ida_member_candidates(&html, profile, run_sequence, MAX_IDA_DIRECTORY_CANDIDATES)
}

fn parse_ida_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let table_re =
        regex_lite::Regex::new(r#"(?is)<table border="0" cellspacing="0" cellpadding="3">\s*<tbody>(.*?)</tbody>\s*</table>"#)
            .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<td colspan="2"><strong>(.*?)</strong></td>"#).unwrap();
    let web_re =
        regex_lite::Regex::new(r#"(?is)<td><strong>Web:</strong></td>\s*<td><a href="([^"]+)""#)
            .unwrap();
    let contact_re = regex_lite::Regex::new(
        r#"(?is)<td[^>]*><strong>(?:Yönetici Ortak|Kurucu Ortak|Managing Partner|Genel Müdür|Genel Mudur|Temsilci|Kurucu):</strong></td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<td><strong>Telefon:</strong></td>\s*<td>(.*?)</td>"#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for table_cap in table_re.captures_iter(html) {
        let Some(block_html) = table_cap.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let Some(domain) = web_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "ida.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let contact_name = contact_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "public relations".to_string(),
            "communications agency".to_string(),
            "brand communication".to_string(),
        ];
        if source_lower.contains("iletişim") || source_lower.contains("iletisim") {
            matched_keywords.push("communication consultancy".to_string());
        }
        if source_lower.contains("medya") {
            matched_keywords.push("media relations".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "IDA members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "IDA members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.ida.org.tr/ornek-sayfa/uyelerimiz/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                source: Some("IDA members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tesid_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://tesid.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tesid_member_candidates(&html, profile, run_sequence, MAX_TESID_DIRECTORY_CANDIDATES)
}

fn parse_tesid_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let start = html.find(r#"<div class="boxuye_detay">"#).unwrap_or(0);
    let end = html
        .find("TESİD ÜYELERİ ALT SEKTÖR DAĞILIMI")
        .unwrap_or(html.len());
    let slice = &html[start..end];
    let anchor_re = regex_lite::Regex::new(r#"(?is)<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in anchor_re.captures_iter(slice) {
        let Some(raw_href) = cap.get(1).map(|m| decode_basic_html_entities(m.as_str())) else {
            continue;
        };
        let Some(domain) = extract_domain(&raw_href) else {
            continue;
        };
        if domain == "tesid.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = cap
            .get(2)
            .map(|m| strip_html_tags(m.as_str()))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .unwrap_or_default();
        if company.len() < 8 || company.to_lowercase().contains("tesid üyeleri alt sektör") {
            continue;
        }

        let company_lower = company.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "electronics".to_string(),
            "electronic manufacturing".to_string(),
            "hardware".to_string(),
        ];
        if company_lower.contains("yazılım") || company_lower.contains("yazilim") {
            matched_keywords.push("software".to_string());
        }
        if company_lower.contains("savunma") {
            matched_keywords.push("defense electronics".to_string());
        }
        if company_lower.contains("otomasyon") {
            matched_keywords.push("industrial automation".to_string());
        }
        if company_lower.contains("telekom") || company_lower.contains("haberleşme") {
            matched_keywords.push("telecom".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "TESID members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://tesid.org.tr/uyelerimiz".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("TESID members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tudis_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tudis.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tudis_member_candidates(&html, profile, run_sequence, MAX_TUDIS_DIRECTORY_CANDIDATES)
}

fn parse_tudis_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str().to_string()))
            .collect::<Vec<_>>();
        if raw_cells.len() < 3 {
            continue;
        }
        let cells = raw_cells
            .iter()
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();

        let company = cells[0].trim().to_string();
        let Some(domain) = extract_domain(&raw_cells[2]).or_else(|| extract_domain(&cells[2]))
        else {
            continue;
        };
        if company.is_empty()
            || company.contains("ÜYE FİRMA ADI")
            || domain == "tudis.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let email =
            normalize_directory_email_for_domain(extract_email_from_text(&raw_cells[1]), &domain);
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "leather".to_string(),
            "tannery".to_string(),
            "leather manufacturing".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("konf") {
            matched_keywords.push("leather apparel".to_string());
        }
        if company_lower.contains("deri") {
            matched_keywords.push("leather goods".to_string());
        }

        let evidence = if let Some(email) = email.as_deref() {
            format!(
                "TUDIS members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            )
        } else {
            format!(
                "TUDIS members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.tudis.org.tr/uyelerimiz".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("TUDIS members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_emsad_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.emsad.org.tr/TR,753/uyelerimiz.html",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_emsad_member_candidates(&html, profile, run_sequence, MAX_EMSAD_DIRECTORY_CANDIDATES)
}

fn parse_emsad_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let table_re = regex_lite::Regex::new(
        r#"(?is)<table width="100%" border="0" cellspacing="1" cellpadding="2">(.*?)</table>"#,
    )
    .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<td[^>]*align="left"[^>]*><b>(.*?)</b><br"#).unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)<b>\s*Temsilci Adı:\s*</b>\s*(.*?)<br"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)<b>\s*Tel:\s*</b>\s*(.*?)<br"#).unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)<b>\s*e-posta:\s*</b>\s*(.*?)<br"#).unwrap();
    let web_re = regex_lite::Regex::new(r#"(?is)<b>\s*web:</b>\s*<a[^>]*href="([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for table_cap in table_re.captures_iter(html) {
        let Some(block_html) = table_cap.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let Some(domain) = web_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "emsad.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let contact_name = contact_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| {
                normalize_directory_email_for_domain(extract_email_from_text(&value), &domain)
            });

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "electromechanical".to_string(),
            "electrical equipment".to_string(),
            "power equipment".to_string(),
        ];
        if source_lower.contains("transform") {
            matched_keywords.push("transformer".to_string());
        }
        if source_lower.contains("enerji") || source_lower.contains("energy") {
            matched_keywords.push("energy equipment".to_string());
        }
        if source_lower.contains("otomasyon") {
            matched_keywords.push("industrial automation".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "EMSAD members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "EMSAD members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "EMSAD members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "EMSAD members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.emsad.org.tr/TR,753/uyelerimiz.html".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("EMSAD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tgsd_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://tgsd.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tgsd_member_candidates(&html, profile, run_sequence, MAX_TGSD_DIRECTORY_CANDIDATES)
}

fn parse_tgsd_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();
    let href_re = regex_lite::Regex::new(r#"(?is)<a href="([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str().to_string()))
            .collect::<Vec<_>>();
        if raw_cells.len() < 4 {
            continue;
        }
        let cells = raw_cells
            .iter()
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();

        if cells[1].eq_ignore_ascii_case("Adı Soyadı") || cells[2].eq_ignore_ascii_case("Firma") {
            continue;
        }

        let Some(domain) = href_re
            .captures(&raw_cells[3])
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        let company = cells[2].trim().to_string();
        if company.is_empty()
            || domain == "tgsd.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let contact_name = normalize_turkish_source_person_name(&cells[1])
            .or_else(|| normalize_person_name(&cells[1]));
        let company_lower = company.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "textile".to_string(),
            "apparel".to_string(),
            "ready-to-wear".to_string(),
        ];
        if company_lower.contains("tekstil") {
            matched_keywords.push("textile manufacturing".to_string());
        }
        if company_lower.contains("giyim") {
            matched_keywords.push("garment".to_string());
        }
        if company_lower.contains("denim") {
            matched_keywords.push("denim".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "TGSD members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://tgsd.org.tr/uyelerimiz/".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                contact_name,
                source: Some("TGSD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_ared_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.ared.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_ared_member_candidates(&html, profile, run_sequence, MAX_ARED_DIRECTORY_CANDIDATES)
}

fn parse_ared_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h3 class="entry-title">\s*(.*?)\s*</h3>"#).unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)fa-user[^>]*></i>\s*([^<]+?)\s*</span>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)fa-phone-square[^>]*></i>\s*([^<]+?)\s*</span>"#).unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)href="mailto:([^"]*)""#).unwrap();
    let website_re =
        regex_lite::Regex::new(r#"(?is)mailto:[^"]*"[^>]*>.*?</a>\s*-\s*<a[^>]*href="([^"]+)""#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="col-lg-12 load-post">"#).skip(1) {
        let block_html = format!(r#"<div class="col-lg-12 load-post">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "ared.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company_raw) = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
        else {
            continue;
        };
        let company = company_raw
            .split(" - ")
            .next()
            .unwrap_or(company_raw.as_str())
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if company.is_empty() {
            continue;
        }

        let contact_name = contact_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| {
                normalize_directory_email_for_domain(extract_email_from_text(&value), &domain)
            });

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "signage".to_string(),
            "outdoor advertising".to_string(),
            "industrial printing".to_string(),
        ];
        if source_lower.contains("dijital") {
            matched_keywords.push("digital signage".to_string());
        }
        if source_lower.contains("baskı") || source_lower.contains("baski") {
            matched_keywords.push("printing services".to_string());
        }
        if source_lower.contains("şehir mobilyaları") || source_lower.contains("sehir mobilyalari")
        {
            matched_keywords.push("urban furniture".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "ARED members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "ARED members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "ARED members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "ARED members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.ared.org.tr/uyelerimiz".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("ARED members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_todeb_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://todeb.org.tr/sayfa/birlik-uyeleri/39/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_todeb_member_candidates(&html, profile, run_sequence, MAX_TODEB_DIRECTORY_CANDIDATES)
}

fn parse_todeb_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(r#"(?is)<h2>(.*?)</h2>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>Telefon:\s*<br\s*/?></strong>\s*([^<]+)"#).unwrap();
    let web_re =
        regex_lite::Regex::new(r#"(?is)<strong>Web:\s*<br\s*/?></strong>\s*<a href="([^"]+)""#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html
        .split(r#"<div class="flexCerceve logoBorder">"#)
        .skip(1)
    {
        let block_html = format!(r#"<div class="flexCerceve logoBorder">{segment}"#);
        let Some(domain) = web_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "todeb.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| repair_common_mojibake_utf8(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "payments".to_string(),
            "electronic money".to_string(),
            "fintech".to_string(),
        ];
        if source_lower.contains("elektronik para") {
            matched_keywords.push("e-money".to_string());
        }
        if source_lower.contains("ödeme") || source_lower.contains("odeme") {
            matched_keywords.push("payment services".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "TODEB members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "TODEB members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://todeb.org.tr/sayfa/birlik-uyeleri/39/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("TODEB members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

fn interleave_free_discovery_sources(
    sources: Vec<Vec<FreeDiscoveryCandidate>>,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let mut positions = vec![0usize; sources.len()];
    let mut seen = HashSet::<String>::new();
    let mut out = Vec::<FreeDiscoveryCandidate>::new();

    while out.len() < max_candidates {
        let mut advanced = false;
        for (idx, source) in sources.iter().enumerate() {
            while positions[idx] < source.len() {
                let Some(candidate) =
                    normalize_free_candidate_gateway(source[positions[idx]].clone())
                else {
                    positions[idx] += 1;
                    continue;
                };
                positions[idx] += 1;
                let domain_key = candidate.candidate.domain.to_lowercase();
                if !seen.insert(domain_key) {
                    continue;
                }
                out.push(candidate);
                advanced = true;
                break;
            }
            if out.len() >= max_candidates {
                break;
            }
        }
        if !advanced {
            break;
        }
    }

    out
}

fn source_health_key(source: &str) -> &'static str {
    match source {
        "TMB members directory" => "directory_tmb",
        "EUD members page" => "directory_eud",
        "ASMUD members page" => "directory_asmud",
        "Platformder rehber" => "directory_platformder",
        "MIB members page" => "directory_mib",
        "IMDER member detail" => "directory_imder",
        "ISDER member detail" => "directory_isder",
        "THBB yazismali uyeler" => "directory_thbb",
        "EDER members page" => "directory_eder",
        "LojiDer members page" => "directory_lojider",
        "TFYD members page" => "directory_tfyd",
        "OSS members page" => "directory_oss",
        "IDA members page" => "directory_ida",
        "TESID members page" => "directory_tesid",
        "TUDIS members page" => "directory_tudis",
        "EMSAD members page" => "directory_emsad",
        "TGSD members page" => "directory_tgsd",
        "ARED members page" => "directory_ared",
        "TODEB members page" => "directory_todeb",
        _ => "directory_unknown",
    }
}

fn expected_source_counts_for_profile(profile: &SalesProfile) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    if !geo_is_turkey(&profile.target_geo) {
        return out;
    }
    if profile_targets_field_ops(profile) {
        for key in [
            "directory_tmb",
            "directory_asmud",
            "directory_platformder",
            "directory_mib",
            "directory_imder",
            "directory_isder",
            "directory_thbb",
        ] {
            out.insert(key.to_string(), 0);
        }
    }
    if profile_targets_energy(profile) {
        out.insert("directory_eud".to_string(), 0);
    }
    if profile_targets_digital_commerce(profile) {
        out.insert("directory_eder".to_string(), 0);
    }
    if profile_targets_logistics(profile) {
        out.insert("directory_lojider".to_string(), 0);
    }
    if profile_targets_events_exhibitions(profile) {
        out.insert("directory_tfyd".to_string(), 0);
    }
    if profile_targets_automotive(profile) {
        out.insert("directory_oss".to_string(), 0);
    }
    if profile_targets_pr_communications(profile) {
        out.insert("directory_ida".to_string(), 0);
    }
    if profile_targets_electronics(profile) {
        out.insert("directory_tesid".to_string(), 0);
    }
    if profile_targets_leather(profile) {
        out.insert("directory_tudis".to_string(), 0);
    }
    if profile_targets_electromechanical(profile) {
        out.insert("directory_emsad".to_string(), 0);
    }
    if profile_targets_textile_apparel(profile) {
        out.insert("directory_tgsd".to_string(), 0);
    }
    if profile_targets_advertising_signage(profile) {
        out.insert("directory_ared".to_string(), 0);
    }
    if profile_targets_fintech_payments(profile) {
        out.insert("directory_todeb".to_string(), 0);
    }
    out
}

fn profile_targets_energy(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed.contains("energy")
        || seed.contains("enerji")
        || seed.contains("electric")
        || seed.contains("electricity")
        || seed.contains("power")
        || seed.contains("utility")
        || seed.contains("renewable")
}

fn seed_contains_any(seed: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| seed.contains(needle))
}

fn profile_targets_digital_commerce(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "e-commerce",
            "ecommerce",
            "e ticaret",
            "eticaret",
            "marketplace",
            "pazaryeri",
            "online store",
            "web shop",
            "shopping cart",
            "checkout",
            "merchant",
            "order management",
            "digital commerce",
        ],
    )
}

fn profile_targets_fintech_payments(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "payment",
            "payments",
            "ödeme",
            "odeme",
            "electronic money",
            "e-money",
            "wallet",
            "digital wallet",
            "fintech",
            "sanal pos",
            "pos",
            "acquiring",
            "issuer",
            "money transfer",
            "remittance",
            "open banking",
        ],
    )
}

fn profile_targets_logistics(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "logistics",
            "lojistik",
            "freight",
            "forwarding",
            "warehouse",
            "warehousing",
            "depo",
            "antrepo",
            "shipping",
            "cargo",
            "nakliye",
            "gumruk",
            "gümrük",
            "supply chain",
        ],
    )
}

fn profile_targets_electronics(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "electronics",
            "elektronik",
            "telecom",
            "telekom",
            "embedded",
            "pcb",
            "hardware",
            "iot",
            "haberleşme",
            "savunma elektroni",
            "electronic manufacturing",
        ],
    )
}

fn profile_targets_electromechanical(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "electromechanical",
            "elektromekanik",
            "transformer",
            "switchgear",
            "substation",
            "medium voltage",
            "high voltage",
            "power distribution",
            "energy equipment",
            "electrical equipment",
            "kablo",
            "cable",
            "pano",
            "industrial automation",
            "busbar",
        ],
    )
}

fn profile_targets_automotive(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "automotive",
            "otomotiv",
            "aftermarket",
            "auto parts",
            "spare parts",
            "yedek parca",
            "yedek parça",
            "oem",
            "tier 1",
            "tier1",
        ],
    )
}

fn profile_targets_textile_apparel(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "textile",
            "tekstil",
            "apparel",
            "garment",
            "ready-to-wear",
            "ready wear",
            "hazır giyim",
            "hazir giyim",
            "konfeksiyon",
            "giyim",
            "fashion",
            "denim",
            "woven",
            "knitwear",
            "örme",
            "orme",
        ],
    )
}

fn profile_targets_leather(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "leather",
            "deri",
            "tannery",
            "tabakhane",
            "hide",
            "nubuk",
            "suede",
            "saraciye",
        ],
    )
}

fn profile_targets_pr_communications(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "public relations",
            "pr agency",
            "communication agency",
            "communications",
            "communication",
            "kurumsal iletişim",
            "kurumsal iletisim",
            "iletişim danışmanlığı",
            "iletisim danismanligi",
            "halkla ilişkiler",
            "halkla iliskiler",
            "media relations",
            "brand communication",
            "reputation management",
        ],
    )
}

fn profile_targets_advertising_signage(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "signage",
            "digital signage",
            "outdoor advertising",
            "open-air advertising",
            "açıkhava",
            "acikhava",
            "endüstriyel reklam",
            "endustriyel reklam",
            "display",
            "point of sale",
            "baskı",
            "baski",
            "serigrafi",
            "wide format",
            "reklam",
        ],
    )
}

fn profile_targets_events_exhibitions(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "events & exhibitions",
            "event",
            "events",
            "exhibition",
            "expo",
            "fair organization",
            "organizer",
            "organizasyon",
            "etkinlik",
            "kongre",
            "fuar",
            "fuarcilik",
            "fuarcılık",
        ],
    )
}

fn llm_candidate_relevance_prompt_context(profile: &SalesProfile) -> String {
    if profile_targets_field_ops(profile) {
        "We sell to companies with field/on-site operations (construction, maintenance, facility management, technical service, dispatch, infrastructure, equipment, etc.).\n\
         For each company, assess:\n\
         - Is it a real company in our target industry with meaningful field or operational teams?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_fintech_payments(profile) {
        "We sell to payments, fintech, wallets, money-movement, and electronic-money companies.\n\
         For each company, assess:\n\
         - Is it a real payment, wallet, electronic-money, or fintech operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_digital_commerce(profile) {
        "We sell to companies operating in digital commerce, marketplace, online retail, or e-commerce infrastructure.\n\
         For each company, assess:\n\
         - Is it a real company in e-commerce, online retail, payment/checkout, or commerce software infrastructure?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_logistics(profile) {
        "We sell to logistics, freight, warehousing, customs, cargo, and supply-chain companies.\n\
         For each company, assess:\n\
         - Is it a real logistics or supply-chain operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_electronics(profile) {
        "We sell to electronics, telecom, embedded systems, and hardware companies.\n\
         For each company, assess:\n\
         - Is it a real electronics or telecom company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_electromechanical(profile) {
        "We sell to electromechanical, transformer, switchgear, cable, and power-distribution equipment companies.\n\
         For each company, assess:\n\
         - Is it a real electrical equipment or electromechanical company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_automotive(profile) {
        "We sell to automotive, aftermarket, spare-parts, and vehicle supply-chain companies.\n\
         For each company, assess:\n\
         - Is it a real automotive or automotive-aftermarket company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_textile_apparel(profile) {
        "We sell to textile, apparel, garment, denim, and ready-to-wear companies.\n\
         For each company, assess:\n\
         - Is it a real textile, apparel, or fashion manufacturing company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_leather(profile) {
        "We sell to leather, tannery, hide-processing, and leather-goods companies.\n\
         For each company, assess:\n\
         - Is it a real leather or tannery company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_pr_communications(profile) {
        "We sell to PR, communication, media-relations, and brand-communication agencies.\n\
         For each company, assess:\n\
         - Is it a real communication or PR agency in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_advertising_signage(profile) {
        "We sell to signage, display, industrial-printing, and outdoor-advertising companies.\n\
         For each company, assess:\n\
         - Is it a real signage, display, industrial-printing, or outdoor-advertising company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_events_exhibitions(profile) {
        "We sell to exhibition, fair, congress, organizer, and event-services companies.\n\
         For each company, assess:\n\
         - Is it a real event, exhibition, fair, or congress operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_energy(profile) {
        "We sell to energy, utility, electricity generation, and related infrastructure companies.\n\
         For each company, assess:\n\
         - Is it a real energy or utility company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else {
        format!(
            "We sell to B2B companies in {} within {}.\n\
             For each company, assess:\n\
             - Is it a real company in or adjacent to the target industry?\n\
             - Would they plausibly benefit from our product?",
            profile.target_industry, profile.target_geo
        )
    }
}

fn candidate_quality_floor(profile: &SalesProfile) -> i32 {
    if profile_targets_field_ops(profile) && geo_is_turkey(&profile.target_geo) {
        12
    } else {
        MIN_DOMAIN_RELEVANCE_SCORE
    }
}

fn normalize_turkish_source_person_name(raw: &str) -> Option<String> {
    let rewritten = raw
        .split_whitespace()
        .map(|token| {
            let alpha_count = token.chars().filter(|c| c.is_alphabetic()).count();
            let upper_count = token
                .chars()
                .filter(|c| c.is_alphabetic() && c.is_uppercase())
                .count();
            let lower_count = token
                .chars()
                .filter(|c| c.is_alphabetic() && c.is_lowercase())
                .count();
            if alpha_count >= 2 && (upper_count == alpha_count || lower_count == alpha_count) {
                turkish_title_case_token(token)
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    normalize_person_name(&rewritten)
}

fn turkish_title_case_token(token: &str) -> String {
    let lower = token
        .chars()
        .flat_map(|c| match c {
            'I' => "ı".chars().collect::<Vec<_>>(),
            'İ' => "i".chars().collect::<Vec<_>>(),
            _ => c.to_lowercase().collect(),
        })
        .collect::<String>();
    let mut out = String::new();
    let mut new_segment = true;
    for c in lower.chars() {
        if new_segment && c.is_alphabetic() {
            match c {
                'i' => out.push('İ'),
                'ı' => out.push('I'),
                _ => out.extend(c.to_uppercase()),
            }
            new_segment = false;
            continue;
        }
        out.push(c);
        if matches!(c, '-' | '\'') {
            new_segment = true;
        } else if c.is_alphabetic() {
            new_segment = false;
        }
    }
    out
}

fn truncate_cleaned_text(text: &str, max_chars: usize) -> String {
    let clean = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if clean.is_empty() || max_chars == 0 {
        return String::new();
    }

    let clean_len = clean.chars().count();
    if clean_len <= max_chars {
        return clean;
    }

    let mut cut: String = clean.chars().take(max_chars).collect();
    if let Some(pos) = cut.rfind(' ') {
        cut.truncate(pos);
    }
    if cut.is_empty() {
        cut = clean.chars().take(max_chars).collect();
    }
    format!("{cut}...")
}

fn truncate_text_for_reason(text: &str, max_len: usize) -> String {
    truncate_cleaned_text(text, max_len)
}

fn domain_to_company(domain: &str) -> String {
    let left = domain.split('.').next().unwrap_or(domain);
    left.replace('-', " ")
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn outreach_recipient_name(contact_name: Option<&str>, company: &str, target_geo: &str) -> String {
    if let Some(first_name) = contact_name
        .and_then(normalize_person_name)
        .and_then(|value| value.split_whitespace().next().map(|part| part.to_string()))
    {
        return first_name;
    }
    if geo_is_turkey(target_geo) {
        format!("{} ekibi", company)
    } else {
        format!("{} team", company)
    }
}

fn build_sales_lead_reasons(
    profile: &SalesProfile,
    company: &str,
    matched: &str,
    evidence: &str,
    contact_title: Option<&str>,
) -> Vec<String> {
    let mut reasons = vec![
        format!("ICP fit: {}", truncate_text_for_reason(matched, 140)),
        format!(
            "Public evidence: {}",
            truncate_text_for_reason(evidence, 220)
        ),
    ];
    if let Some(title) = contact_title
        .map(str::trim)
        .filter(|value| !value.is_empty() && !contact_title_is_generic_default(Some(value)))
    {
        reasons.push(format!("Potential buyer role: {title}"));
    }
    reasons.push(format!(
        "Value hypothesis: {} could help {} with {}",
        profile.product_name,
        company,
        truncate_text_for_reason(&profile.product_description, 140)
    ));
    dedupe_strings(reasons)
}

fn build_sales_email_subject(profile: &SalesProfile, company: &str) -> String {
    if geo_is_turkey(&profile.target_geo) {
        format!("{company} icin saha operasyon koordinasyonu")
    } else {
        format!("{company}: field ops coordination")
    }
}

fn build_sales_email_body(
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
    matched: &str,
    evidence: &str,
) -> String {
    let recipient = outreach_recipient_name(contact_name, company, &profile.target_geo);
    let evidence_short = truncate_text_for_reason(evidence, 160);
    let matched_short = truncate_text_for_reason(matched, 90);
    let value_short = truncate_text_for_reason(&profile.product_description, 150);

    if geo_is_turkey(&profile.target_geo) {
        format!(
            "Merhaba {},\n\n{} ile ilgili su sinyali gordum: {}.\n\n{} tarafinda {} operasyonlarinda is atama, takip ve gecikme yonetimi kolayca daginik hale gelebiliyor. {} burada su ise yarayabilir: {}.\n\nUygunsa {} icin 3 maddelik kisa bir operasyon akisi onerisi paylasabilirim.\n\nSelamlar,\n{}",
            recipient,
            company,
            evidence_short,
            company,
            matched_short,
            profile.product_name,
            value_short,
            company,
            profile.sender_name
        )
    } else {
        format!(
            "Hi {},\n\nI came across {} through this public signal: {}.\n\nFor teams running {}, the friction is usually around task ownership, follow-up, and delay recovery across email, phone, and chat. {} could help here: {}.\n\nIf useful, I can send a short 3-point workflow teardown for {}.\n\nBest,\n{}",
            recipient,
            company,
            evidence_short,
            matched_short,
            profile.product_name,
            value_short,
            company,
            profile.sender_name
        )
    }
}

fn build_sales_linkedin_message(
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
    evidence: &str,
) -> String {
    let recipient = outreach_recipient_name(contact_name, company, &profile.target_geo);
    let evidence_short = truncate_text_for_reason(evidence, 110);
    if geo_is_turkey(&profile.target_geo) {
        truncate_cleaned_text(
            &format!(
                "Merhaba {}, {} ile ilgili su sinyali gordum: {}. {} saha ekiplerinde takip ve koordinasyonu toparlamaya yardimci oluyor. Uygunsa kisa bir akis onerisi paylasabilirim.",
                recipient, company, evidence_short, profile.product_name
            ),
            300,
        )
    } else {
        truncate_cleaned_text(
            &format!(
                "Hi {}, noticed {} through this signal: {}. {} helps field teams tighten follow-up and coordination. Happy to share a short workflow teardown if relevant.",
                recipient, company, evidence_short, profile.product_name
            ),
            300,
        )
    }
}

/// Stage 1: Determine message strategy from thesis + persona context.
/// Returns pain angle, trigger reference, CTA type, tone, and language.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MessageStrategy {
    pain_angle: String,
    trigger_evidence: String,
    cta_type: String,
    tone: String,
    language: String,
}

fn generate_message_strategy(
    profile: &SalesProfile,
    _company: &str,
    _contact_name: Option<&str>,
    evidence: &str,
    matched: &str,
) -> MessageStrategy {
    let language = if geo_is_turkey(&profile.target_geo) {
        "tr"
    } else {
        "en"
    };
    let cta = if language == "tr" {
        "Uygunsa size 2 sayfalik kisa bir operasyon analizi paylasabilirim."
    } else {
        "Happy to share a brief 2-page operational analysis if helpful."
    };
    MessageStrategy {
        pain_angle: matched.to_string(),
        trigger_evidence: evidence.to_string(),
        cta_type: cta.to_string(),
        tone: "professional_warm".to_string(),
        language: language.to_string(),
    }
}

/// Stage 2: Build outreach message copy. Currently template-based; designed for
/// future LLM generation when evidence bundle + thesis are available.
/// Evidence-bound: refuses to generate if no evidence is provided.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MessageCopy {
    subject: String,
    body: String,
    linkedin_copy: String,
    claims: Vec<String>,
    evidence_ids: Vec<String>,
}

fn generate_message_copy(
    strategy: &MessageStrategy,
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
) -> Result<MessageCopy, String> {
    if strategy.trigger_evidence.trim().is_empty() && strategy.pain_angle.trim().is_empty() {
        return Err(
            "REFUSED: No evidence or pain angle provided. Message engine requires evidence.".into(),
        );
    }
    let subject = build_sales_email_subject(profile, company);
    let body = build_sales_email_body(
        profile,
        company,
        contact_name,
        &strategy.pain_angle,
        &strategy.trigger_evidence,
    );
    let linkedin =
        build_sales_linkedin_message(profile, company, contact_name, &strategy.trigger_evidence);
    Ok(MessageCopy {
        subject,
        body,
        linkedin_copy: linkedin,
        claims: vec![strategy.pain_angle.clone()],
        evidence_ids: Vec::new(),
    })
}

fn extract_contact_from_search(
    search_output: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let filtered_output = search_output
        .lines()
        .filter(|line| {
            let lower = line.trim().to_lowercase();
            !lower.starts_with("search results for")
                && !lower.starts_with("[external content:")
                && !lower.starts_with("title:")
                && !lower.starts_with("url source:")
                && !lower.starts_with("markdown content:")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let canonical_output = canonicalize_contact_titles(&filtered_output);

    let personal_linkedin_url = extract_personal_linkedin_from_text(&filtered_output);
    let company_linkedin_url = extract_company_linkedin_from_text(&filtered_output);
    let linkedin_url = personal_linkedin_url
        .clone()
        .or_else(|| company_linkedin_url.clone());

    let ranked_re = regex_lite::Regex::new(
        r"(?im)^\s*\d+\.\s*([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\s*[-|]\s*(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)",
    )
    .unwrap();
    if let Some(cap) = ranked_re.captures(&canonical_output) {
        let name = cap
            .get(1)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        let title = cap
            .get(2)
            .map(|m| normalize_contact_title(m.as_str()))
            .or_else(|| default_contact_title(title_policy));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let comma_name_title_re = regex_lite::Regex::new(
        r"(?is)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b\s*,\s*(?:President and )?(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b",
    )
    .unwrap();
    if let Some(cap) = comma_name_title_re.captures(&canonical_output) {
        let name = cap
            .get(1)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        let title = cap.get(2).map(|m| normalize_contact_title(m.as_str()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let name_then_title_re = regex_lite::Regex::new(
        r"(?is)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b[^\n\r]{0,120}\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b",
    )
    .unwrap();
    if let Some(cap) = name_then_title_re.captures(&canonical_output) {
        let name = cap
            .get(1)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        let title = cap.get(2).map(|m| normalize_contact_title(m.as_str()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let title_then_name_re = regex_lite::Regex::new(
        r"(?is)\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b[^\n\r]{0,64}\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b",
    )
    .unwrap();
    if let Some(cap) = title_then_name_re.captures(&canonical_output) {
        let title = cap.get(1).map(|m| normalize_contact_title(m.as_str()));
        let name = cap
            .get(2)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let title_punct_name_re = regex_lite::Regex::new(
        r"(?is)\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b\s*[:\-–]\s*([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b",
    )
    .unwrap();
    if let Some(cap) = title_punct_name_re.captures(&canonical_output) {
        let title = cap.get(1).map(|m| normalize_contact_title(m.as_str()));
        let name = cap
            .get(2)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let sentence_re = regex_lite::Regex::new(
        r"(?is)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b[^\n\r]{0,60}\b(?:is|serves as|has served as|appointed as|was named)\b[^\n\r]{0,60}\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b",
    )
    .unwrap();
    if let Some(cap) = sentence_re.captures(&canonical_output) {
        let name = cap
            .get(1)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        let title = cap.get(2).map(|m| normalize_contact_title(m.as_str()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            return (name, title, linkedin_url.clone());
        }
    }

    let linkedin_name = personal_linkedin_url
        .as_deref()
        .and_then(extract_name_from_linkedin_url)
        .and_then(|n| normalize_person_name(&n));

    (
        linkedin_name,
        default_contact_title(title_policy),
        linkedin_url,
    )
}

fn extract_contact_from_search_for_company(
    search_output: &str,
    title_policy: &str,
    company: &str,
    domain: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let company_keys = company
        .split_whitespace()
        .map(|w| w.trim().to_lowercase())
        .filter(|w| w.len() >= 3)
        .collect::<Vec<_>>();
    let domain_root = domain
        .split('.')
        .next()
        .unwrap_or(domain)
        .trim()
        .to_lowercase();

    let mut best_score = -1i32;
    let mut best_name: Option<String> = None;
    let mut best_title: Option<String> = None;
    let mut best_linkedin: Option<String> = None;
    let mut best_email: Option<String> = None;
    let contains_company_token = |candidate: &str| {
        let lower = candidate.to_lowercase();
        company_keys.iter().any(|k| lower.contains(k))
            || (!domain_root.is_empty() && lower.contains(&domain_root))
    };

    for entry in parse_search_entries(search_output) {
        let mut relevance = 0i32;
        if let Some(entry_domain) = extract_domain(&entry.url) {
            if entry_domain == domain {
                relevance += 6;
            } else if entry_domain.ends_with(&format!(".{domain}")) {
                relevance += 3;
            }
        }
        let text = format!("{} {}", entry.title, entry.snippet);
        let lower = text.to_lowercase();
        if !domain_root.is_empty() && lower.contains(&domain_root) {
            relevance += 2;
        }
        if company_keys.iter().any(|k| lower.contains(k)) {
            relevance += 1;
        }
        if relevance == 0 {
            continue;
        }

        let single_result = format!("{}\n{}\n{}", entry.title, entry.snippet, entry.url);
        let (mut name, mut title, mut linkedin) =
            extract_contact_from_search(&single_result, title_policy);
        if name.as_deref().map(contains_company_token).unwrap_or(false) {
            name = None;
        }
        if name.is_none() {
            title = None;
        }
        if linkedin.is_none() {
            linkedin = extract_personal_linkedin_from_text(&entry.url)
                .or_else(|| extract_company_linkedin_from_text(&entry.url));
        }
        let email = normalize_contact_email_for_domain(extract_email_from_text(&text), domain);
        let score = relevance
            + (name.is_some() as i32 * 4)
            + (title.is_some() as i32 * 2)
            + (linkedin.is_some() as i32 * 3)
            + (email.is_some() as i32 * 2);
        if score > best_score {
            best_score = score;
            best_name = name;
            best_title = title;
            best_linkedin = linkedin;
            best_email = email;
        }
    }

    (best_name, best_title, best_linkedin, best_email)
}

fn url_host(raw_url: &str) -> Option<String> {
    let repaired = repair_common_url_typos(raw_url);
    let parsed = if repaired.starts_with("http://") || repaired.starts_with("https://") {
        url::Url::parse(&repaired).ok()
    } else {
        url::Url::parse(&format!("https://{}", repaired)).ok()
    }?;
    let host = parsed
        .host_str()?
        .trim()
        .trim_end_matches('.')
        .to_lowercase();
    if host.is_empty() {
        return None;
    }
    Some(host.trim_start_matches("www.").to_string())
}

fn url_matches_company_domain(raw_url: &str, company_domain: &str) -> bool {
    let Some(host) = url_host(raw_url) else {
        return false;
    };
    host == company_domain || host.ends_with(&format!(".{company_domain}"))
}

fn search_entry_mentions_company(entry: &SearchEntry, company: &str, domain: &str) -> bool {
    let blob = format!("{} {} {}", entry.title, entry.snippet, entry.url).to_lowercase();
    let domain_root = domain
        .split('.')
        .next()
        .unwrap_or(domain)
        .trim()
        .to_lowercase();
    if !domain_root.is_empty() && blob.contains(&domain_root) {
        return true;
    }
    company
        .split_whitespace()
        .map(|value| value.trim().to_lowercase())
        .filter(|value| value.len() >= 3)
        .any(|token| blob.contains(&token))
}

fn search_entry_has_osint_hint(entry: &SearchEntry) -> bool {
    let blob = format!("{} {}", entry.title, entry.snippet).to_lowercase();
    [
        "linkedin",
        "leadership",
        "management",
        "executive",
        "team",
        "about",
        "contact",
        "hakkimizda",
        "kurumsal",
        "yonetim",
        "ekip",
        "iletisim",
    ]
    .iter()
    .any(|hint| blob.contains(hint))
}

fn collect_osint_links_from_search_outputs(
    search_outputs: &[String],
    company: &str,
    domain: &str,
) -> Vec<String> {
    let mut links = Vec::new();

    for output in search_outputs {
        for entry in parse_search_entries(output) {
            let url = repair_common_url_typos(&entry.url);
            if url.trim().is_empty() {
                continue;
            }
            let mentions_company = search_entry_mentions_company(&entry, company, domain);
            if normalize_company_linkedin_url(&url).is_some() {
                if mentions_company {
                    links.push(url);
                }
                continue;
            }
            if normalize_outreach_linkedin_url(&url).is_some() {
                if mentions_company && search_entry_has_osint_hint(&entry) {
                    links.push(url);
                }
                continue;
            }
            if !url_matches_company_domain(&url, domain) {
                continue;
            }
            let path = url::Url::parse(&url)
                .ok()
                .map(|parsed| parsed.path().to_lowercase())
                .unwrap_or_default();
            if path.is_empty()
                || path == "/"
                || path_looks_like_enrich_target(&path)
                || search_entry_has_osint_hint(&entry)
            {
                links.push(url);
            }
        }
    }

    merge_osint_links(Vec::new(), links)
}

fn best_search_contact_evidence(
    search_outputs: &[String],
    company: &str,
    domain: &str,
) -> Option<String> {
    let mut best_score = -1;
    let mut best_evidence = None;

    for output in search_outputs {
        for entry in parse_search_entries(output) {
            let mut score = 0;
            if search_entry_mentions_company(&entry, company, domain) {
                score += 4;
            }
            if search_entry_has_osint_hint(&entry) {
                score += 3;
            }
            if normalize_company_linkedin_url(&entry.url).is_some() {
                score += 4;
            } else if normalize_outreach_linkedin_url(&entry.url).is_some() {
                score += 3;
            } else if url_matches_company_domain(&entry.url, domain) {
                score += 2;
            }
            let evidence = if !entry.snippet.trim().is_empty() {
                truncate_text_for_reason(&entry.snippet, 220)
            } else {
                truncate_text_for_reason(&entry.title, 220)
            };
            if score > best_score && !evidence.trim().is_empty() {
                best_score = score;
                best_evidence = Some(evidence);
            }
        }
    }

    best_evidence
}

fn best_search_contact_enrichment(
    search_outputs: &[String],
    title_policy: &str,
    company: &str,
    domain: &str,
) -> SiteContactEnrichment {
    let combined = search_outputs.join("\n");
    let (search_name, search_title, search_linkedin, _search_email) =
        extract_contact_from_search_for_company(&combined, title_policy, company, domain);
    let (fallback_name, fallback_title, fallback_linkedin) =
        extract_contact_from_search(&combined, title_policy);
    let linkedin_url = search_linkedin
        .or(fallback_linkedin)
        .and_then(|value| normalize_outreach_linkedin_url(&value));
    let company_linkedin_url = extract_company_linkedin_from_text(&combined)
        .and_then(|value| normalize_company_linkedin_url(&value));
    let evidence = best_search_contact_evidence(search_outputs, company, domain);
    let osint_links = collect_osint_links_from_search_outputs(search_outputs, company, domain);
    let signal = site_contact_candidate_signal(
        search_name.as_ref().or(fallback_name.as_ref()),
        search_title.as_ref().or(fallback_title.as_ref()),
        linkedin_url.as_ref(),
        None,
        evidence.as_ref(),
    ) + i32::from(company_linkedin_url.is_some()) * 3;

    SiteContactEnrichment {
        name: search_name.or(fallback_name),
        title: search_title.or(fallback_title),
        linkedin_url,
        company_linkedin_url,
        email: None,
        evidence,
        osint_links,
        signal,
        tech_stack: Vec::new(),
        job_posting_signals: Vec::new(),
    }
}

fn normalize_company_search_alias(raw: &str) -> Option<String> {
    let cleaned = raw
        .trim()
        .trim_matches(|c: char| {
            c == '"' || c == '\'' || c == ',' || c == ';' || c == ':' || c == '.'
        })
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if cleaned.len() < 3 {
        return None;
    }
    Some(truncate_cleaned_text(&cleaned, 120))
}

fn extract_company_aliases_from_signal(signal: &str) -> Vec<String> {
    let patterns = [
        r"(?i)\blists\s+([^.;]{4,140}?)\s+(?:as a|with official website|with website|with official|and contact|with contact)",
        r"(?i)\bmember detail lists\s+([^.;]{4,140}?)\s+(?:with official website|with website|and contact|with contact)",
    ];
    let mut aliases = Vec::new();
    for pattern in patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        for cap in re.captures_iter(signal) {
            if let Some(value) = cap
                .get(1)
                .and_then(|m| normalize_company_search_alias(m.as_str()))
            {
                aliases.push(value);
            }
        }
    }
    dedupe_strings(aliases)
}

fn build_company_search_aliases(company: &str, matched_signals: &[String]) -> Vec<String> {
    let mut aliases = Vec::new();
    if let Some(company) = normalize_company_search_alias(company) {
        aliases.push(company);
    }
    for signal in matched_signals {
        aliases.extend(extract_company_aliases_from_signal(signal));
    }
    let mut aliases = dedupe_strings(aliases);
    aliases.sort_by(|left, right| {
        right
            .split_whitespace()
            .count()
            .cmp(&left.split_whitespace().count())
            .then_with(|| right.len().cmp(&left.len()))
            .then_with(|| left.cmp(right))
    });
    aliases.truncate(3);
    aliases
}

fn build_company_osint_queries(
    company: &str,
    domain: &str,
    title_policy: &str,
    target_geo: &str,
    primary_contact_name: Option<&str>,
) -> Vec<String> {
    let executive_terms = if title_policy == "ceo_only" {
        "\"Chief Executive Officer\" OR CEO"
    } else {
        "CEO OR Founder OR COO OR \"Head of Operations\""
    };
    let mut queries = vec![
        format!("site:linkedin.com/company \"{}\" \"{}\"", company, domain),
        format!(
            "\"{}\" \"{}\" (LinkedIn OR leadership OR management OR executive team OR hakkimizda OR yonetim)",
            company, domain
        ),
        format!(
            "site:{} (leadership OR management OR executive team OR team OR about OR contact OR hakkimizda OR yonetim OR iletisim)",
            domain
        ),
        format!(
            "\"{}\" \"{}\" ({}) (LinkedIn OR leadership OR executive team)",
            company, domain, executive_terms
        ),
    ];
    if !target_geo.trim().is_empty() {
        queries.push(format!(
            "\"{}\" \"{}\" {} (LinkedIn OR leadership OR management)",
            company, domain, target_geo
        ));
    }
    if let Some(primary_contact_name) = primary_contact_name
        .map(str::trim)
        .filter(|value| !value.is_empty() && !contact_name_is_placeholder(Some(value)))
    {
        queries.push(format!(
            "\"{}\" \"{}\" site:linkedin.com/in",
            primary_contact_name, domain
        ));
    }
    dedupe_strings(queries)
}

async fn search_company_osint_enrichment(
    search_engine: &WebSearchEngine,
    brave_search_engine: Option<&WebSearchEngine>,
    company: &str,
    domain: &str,
    title_policy: &str,
    target_geo: &str,
    primary_contact_name: Option<&str>,
) -> SiteContactEnrichment {
    let queries = build_company_osint_queries(
        company,
        domain,
        title_policy,
        target_geo,
        primary_contact_name,
    );
    let mut outputs = run_sales_search_batch(
        search_engine,
        &queries,
        6,
        Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
    )
    .await
    .into_iter()
    .filter_map(|(_, result)| result.ok())
    .filter(|value| !value.trim().is_empty())
    .collect::<Vec<_>>();

    let mut enrichment = best_search_contact_enrichment(&outputs, title_policy, company, domain);
    if !site_contact_enrichment_has_signal(&enrichment) {
        if let Some(brave_engine) = brave_search_engine {
            let brave_outputs = run_sales_search_batch(
                brave_engine,
                &queries,
                6,
                Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
            )
            .await
            .into_iter()
            .filter_map(|(_, result)| result.ok())
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>();
            if !brave_outputs.is_empty() {
                outputs.extend(brave_outputs);
                enrichment =
                    best_search_contact_enrichment(&outputs, title_policy, company, domain);
            }
        }
    }

    // Run job posting signal search (TASK-27) using the primary search engine
    enrichment.job_posting_signals =
        search_job_posting_signals(company, domain, search_engine).await;

    enrichment
}

fn extract_name_from_linkedin_url(raw_url: &str) -> Option<String> {
    let parsed = url::Url::parse(raw_url).ok()?;
    let path = parsed.path().trim_matches('/');
    if !path.starts_with("in/") {
        return None;
    }
    let slug = path
        .trim_start_matches("in/")
        .split('/')
        .next()
        .unwrap_or("");
    if slug.is_empty() {
        return None;
    }
    let name_parts: Vec<String> = slug
        .split('-')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .filter(|p| p.chars().all(|c| c.is_ascii_alphabetic()))
        .take(4)
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str().to_lowercase()),
                None => String::new(),
            }
        })
        .filter(|p| !p.is_empty())
        .collect();
    if name_parts.len() >= 2 {
        normalize_person_name(&name_parts.join(" "))
    } else {
        None
    }
}

fn title_allowed_for_policy(title_policy: &str, title: &str) -> bool {
    if title_policy != "ceo_only" {
        return true;
    }
    let t = title.to_lowercase();
    t.contains("ceo") || t.contains("chief executive")
}

fn default_contact_title(title_policy: &str) -> Option<String> {
    if title_policy == "ceo_only" {
        Some("CEO".to_string())
    } else {
        Some("CEO/Founder".to_string())
    }
}

fn normalize_contact_title(raw: &str) -> String {
    let canonical = canonicalize_contact_titles(raw)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let lower = canonical.to_lowercase();
    if lower.contains("ceo") || lower.contains("chief executive") || lower.contains("genel müdür")
    {
        "CEO".to_string()
    } else if lower.contains("coo") || lower.contains("chief operating") {
        "COO".to_string()
    } else if lower.contains("founder") || lower.contains("kurucu") {
        "Founder".to_string()
    } else if lower.contains("vice chairman")
        || lower.contains("başkan vekili")
        || lower.contains("baskan vekili")
    {
        "Vice Chairman".to_string()
    } else if lower.contains("chairman")
        || lower.contains("başkanı")
        || lower.contains("baskani")
        || lower.contains("board chair")
    {
        "Chairman".to_string()
    } else if lower.contains("head of operations") {
        "Head of Operations".to_string()
    } else if lower.contains("operations director") {
        "Operations Director".to_string()
    } else if lower.contains("managing director") {
        "Managing Director".to_string()
    } else {
        canonical
    }
}

fn normalize_person_name(raw: &str) -> Option<String> {
    let cleaned = raw
        .trim_matches(|c: char| {
            !c.is_alphanumeric() && c != '.' && c != '\'' && c != '-' && c != ' '
        })
        .split_whitespace()
        .collect::<Vec<_>>();
    if cleaned.len() < 2 || cleaned.len() > 4 {
        return None;
    }

    let stopwords = [
        "and",
        "or",
        "the",
        "bu",
        "of",
        "to",
        "with",
        "without",
        "under",
        "over",
        "like",
        "no",
        "other",
        "team",
        "leadership",
        "group",
        "company",
        "operations",
        "management",
        "search",
        "results",
        "result",
        "for",
        "news",
        "careers",
        "career",
        "solutions",
        "services",
        "technology",
        "technologies",
        "industry",
        "industries",
        "global",
        "international",
        "corporate",
        "innovation",
        "workplace",
        "web",
        "site",
        "ceo",
        "coo",
        "chairman",
        "founder",
        "chief",
        "executive",
        "director",
        "head",
        "vice",
        "president",
        "sitesinde",
        "tümüne",
        "tumune",
        "tümünü",
        "tumunu",
        "izin",
        "ver",
        "reddet",
        "kaydet",
        "çerez",
        "cerez",
        "çerezler",
        "cerezler",
        "cookies",
        "kullanılmaktadır",
        "kullanilmaktadir",
        "welcoming",
        "sustainable",
        "legacy",
        "legacies",
        "experience",
        "protect",
        "uptime",
        "real",
        "world",
        "investments",
        "mission",
        "vision",
        "projects",
        "project",
        "ve",
        "teknik",
        "servis",
        "bakim",
        "bakım",
        "onarim",
        "onarım",
        "operasyon",
        "operasyonlari",
        "operasyonları",
        "santiye",
        "şantiye",
        "muhendislik",
        "mühendislik",
        "altyapi",
        "altyapı",
        "ofis",
        "ofisi",
        "office",
        "turkiye",
        "türkiye",
        "turkey",
        "ankara",
        "istanbul",
        "izmir",
        "basin",
        "basın",
        "odasi",
        "odası",
        "haber",
        "onursal",
        "başkanımız",
        "baskanimiz",
        "kişisel",
        "kisisel",
        "verilerin",
        "verileri",
        "korunması",
        "korunmasi",
        "kanunu",
        "finansal",
        "gostergeler",
        "göstergeler",
        "kurumsal",
        "liderlik",
        "iletisim",
        "iletişim",
        "relations",
        "investor",
        "media",
        "merkez",
        "merkezi",
        "holding",
        "insaat",
        "inşaat",
        "office",
        "genel",
        "müdür",
        "mudur",
        "mudurlugu",
        "mudurluğu",
        "müdürlüğü",
        "gorevine",
        "görevine",
        "atanmistir",
        "atanmıştır",
        "olarak",
        "gelecege",
        "geleceğe",
        "adimlarla",
        "adımlarla",
        "qatar",
        "rwanda",
        "saudi",
        "arabia",
        "arabistan",
        "senegal",
        "sanayisinde",
        "sürdürülebilirlik",
        "surdurulebilirlik",
        "sosyal",
        "sorumluluk",
        "defa",
        "işi",
        "isi",
        "veren",
        "idare",
        "ihale",
        "yıl",
        "yili",
        "yılı",
        "proje",
        "işveren",
        "isveren",
        "yüklenici",
        "yuklenici",
        "kamunun",
        "muhtelif",
        "idarelerinden",
        "paylaşma",
        "paylasma",
        "iklimi",
        "donemi",
        "dönemi",
        "faaliyet",
        "alanlari",
        "alanları",
        "style",
        "font",
        "verdana",
        "geneva",
        "sans",
        "serif",
        "justify",
        "align",
        "size",
        "div",
        "class",
        "span",
        "href",
        "src",
        "img",
        "oturanlar",
        "soldan",
        "sağa",
        "saga",
        "ayakta",
        "nasil",
        "başladık",
        "basladik",
        "hedefimiz",
        "görüşümüz",
        "gorusumuz",
        "prensiplerimiz",
        "ilkelerimiz",
        "politikamız",
        "politikamiz",
    ];
    let company_suffixes = [
        "inc",
        "corp",
        "corporation",
        "ltd",
        "limited",
        "llc",
        "plc",
        "as",
        "a.s",
        "ag",
        "gmbh",
        "holdings",
        "holding",
    ];
    let mut out = Vec::<String>::new();
    let mut token_freq = HashMap::<String, u8>::new();
    for token in cleaned {
        let t = token.trim_matches(|c: char| c == ',' || c == ';' || c == ':' || c == '|');
        if t.is_empty() || !t.chars().any(|c| c.is_alphabetic()) {
            return None;
        }
        if t.chars().count() == 1 {
            return None;
        }
        if t.chars().count() > 18 {
            return None;
        }
        let t_lower = t.to_lowercase();
        let non_person_suffixes = [
            "sinde",
            "sında",
            "lerinde",
            "larında",
            "maktadır",
            "mektedir",
            "lari",
            "ları",
            "leri",
            "lerde",
            "larda",
        ];
        if t_lower.len() >= 7
            && non_person_suffixes
                .iter()
                .any(|suffix| t_lower.ends_with(suffix))
        {
            return None;
        }
        if stopwords.contains(&t_lower.as_str()) || company_suffixes.contains(&t_lower.as_str()) {
            return None;
        }
        *token_freq.entry(t_lower.clone()).or_insert(0) += 1;
        if t.len() == 2 && t.ends_with('.') {
            out.push(t.to_uppercase());
            continue;
        }
        let mapped = if t.chars().all(|c| !c.is_alphabetic() || c.is_uppercase()) {
            let mut chars = t.chars();
            match chars.next() {
                Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str().to_lowercase()),
                None => String::new(),
            }
        } else {
            let mut chars = t.chars();
            match chars.next() {
                Some(c) if c.is_alphabetic() && c.is_lowercase() => {
                    format!("{}{}", c.to_uppercase(), chars.as_str())
                }
                Some(c) => format!("{c}{}", chars.as_str()),
                None => String::new(),
            }
        };
        if mapped.is_empty() {
            return None;
        }
        out.push(mapped);
    }

    if out.len() < 2 {
        return None;
    }
    if out.len() >= 4 && token_freq.values().any(|count| *count > 1) {
        return None;
    }
    let normalized = out.join(" ");
    let normalized_lower = normalized.to_lowercase();
    let banned_phrases = [
        "costa rica",
        "saudi arabia",
        "south africa",
        "north macedonia",
        "new zealand",
        "sri lanka",
    ];
    if banned_phrases.contains(&normalized_lower.as_str()) {
        return None;
    }
    Some(normalized)
}

fn contact_title_looks_plausible(title: &str) -> bool {
    let trimmed = title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.chars().count() > 90 || trimmed.split_whitespace().count() > 10 {
        return false;
    }
    let lower = trimmed.to_lowercase();
    if lower.contains("font-size")
        || lower.contains("text-align")
        || lower.contains("style=")
        || lower.contains("cursor:")
    {
        return false;
    }
    let punctuation_count = trimmed
        .chars()
        .filter(|c| matches!(c, '.' | '!' | '?' | ';'))
        .count();
    punctuation_count <= 1
}

fn title_looks_like_operations_exec(title: &str) -> bool {
    let t = canonicalize_contact_titles(title).to_lowercase();
    t.contains("coo")
        || t.contains("chief operating")
        || t.contains("head of operations")
        || t.contains("operations director")
        || t.contains("service director")
        || t.contains("field operations")
        || ((t.contains("operasyon")
            || t.contains("işletme")
            || t.contains("isletme")
            || t.contains("bakım")
            || t.contains("bakim")
            || t.contains("service"))
            && (t.contains("başkan")
                || t.contains("baskan")
                || t.contains("direkt")
                || t.contains("müd")
                || t.contains("mud")
                || t.contains("chief")
                || t.contains("sorumlu")))
}

fn decode_html_email_entities(text: &str) -> String {
    text.replace("&commat;", "@")
        .replace("&#64;", "@")
        .replace("&#x40;", "@")
        .replace("&#x2e;", ".")
        .replace("&#x2E;", ".")
        .replace("&#46;", ".")
        .replace("&period;", ".")
}

fn decode_cloudflare_email(encoded: &str) -> Option<String> {
    if encoded.len() < 4 || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let key = u8::from_str_radix(&encoded[0..2], 16).ok()?;
    let mut out = String::new();
    let mut idx = 2;
    while idx + 2 <= encoded.len() {
        let byte = u8::from_str_radix(&encoded[idx..idx + 2], 16).ok()?;
        out.push((byte ^ key) as char);
        idx += 2;
    }
    normalize_email_candidate(Some(out))
}

fn normalize_email_candidate(email: Option<String>) -> Option<String> {
    email.and_then(|e| {
        let trimmed = e
            .trim()
            .trim_start_matches("mailto:")
            .trim_matches(|c: char| c == '"' || c == '\'' || c == ')' || c == '(')
            .to_lowercase();
        let (local, domain) = trimmed
            .rsplit_once('@')
            .map(|(l, d)| (l.trim(), d.trim()))?;
        let blocked_tlds = [
            "png", "jpg", "jpeg", "gif", "svg", "webp", "ico", "css", "js", "json", "xml", "pdf",
            "zip", "rar", "7z", "mp4", "webm", "mov",
        ];
        let tld = domain.rsplit('.').next().unwrap_or("");
        if trimmed.is_empty()
            || local.is_empty()
            || domain.is_empty()
            || domain.contains('/')
            || domain.contains('\\')
            || domain.contains('?')
            || domain.contains('&')
            || trimmed.ends_with("@example.com")
            || trimmed.contains("noreply")
            || trimmed.contains("no-reply")
            || is_consumer_email_domain(domain)
            || blocked_tlds.contains(&tld)
        {
            return None;
        }
        Some(trimmed)
    })
}

fn email_domain(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_lowercase())
        .filter(|domain| !domain.is_empty())
}

fn email_local_part(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(local, _)| local.trim().to_lowercase())
        .filter(|local| !local.is_empty())
}

fn email_is_generic_role_mailbox(email: &str) -> bool {
    let Some(local) = email_local_part(email) else {
        return false;
    };
    let normalized = local
        .split(['+', '.', '-', '_'])
        .next()
        .unwrap_or(local.as_str())
        .trim();
    matches!(
        normalized,
        "info"
            | "hello"
            | "contact"
            | "office"
            | "mail"
            | "admin"
            | "support"
            | "sales"
            | "team"
            | "general"
            | "iletisim"
            | "merhaba"
    )
}

fn email_is_actionable_outreach_email(email: &str) -> bool {
    let Some(domain) = email_domain(email) else {
        return false;
    };
    !is_consumer_email_domain(&domain) && !email_is_generic_role_mailbox(email)
}

fn normalize_actionable_outreach_email(email: Option<&str>) -> Option<String> {
    normalize_email_candidate(email.map(|value| value.to_string()))
        .filter(|value| email_is_actionable_outreach_email(value))
}

fn sanitize_approval_payload(
    channel: &str,
    payload: serde_json::Value,
) -> Option<serde_json::Value> {
    match channel {
        "email" => {
            let to = payload
                .get("to")
                .and_then(|value| value.as_str())
                .and_then(|value| normalize_actionable_outreach_email(Some(value)))?;
            let subject = payload
                .get("subject")
                .and_then(|value| value.as_str())?
                .trim();
            let body = payload.get("body").and_then(|value| value.as_str())?.trim();
            if subject.is_empty() || body.is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "to": to,
                "subject": subject,
                "body": body,
                "classification": classify_email(&to, email_domain(&to).as_deref().unwrap_or_default()),
            }))
        }
        "linkedin" | "linkedin_assist" => {
            let profile_url = payload
                .get("profile_url")
                .and_then(|value| value.as_str())
                .and_then(normalize_outreach_linkedin_url)?;
            let message = payload
                .get("message")
                .and_then(|value| value.as_str())?
                .trim();
            if message.is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "profile_url": profile_url,
                "message": message,
                "manual_action": true,
            }))
        }
        _ => Some(payload),
    }
}

fn count_mojibake_markers(text: &str) -> usize {
    text.chars()
        .filter(|c| matches!(c, 'Ã' | 'Ä' | 'Å' | 'â' | '�'))
        .count()
}

fn count_turkish_text_markers(text: &str) -> usize {
    text.chars()
        .filter(|c| {
            matches!(
                c,
                'Ç' | 'ç' | 'Ğ' | 'ğ' | 'İ' | 'ı' | 'Ö' | 'ö' | 'Ş' | 'ş' | 'Ü' | 'ü'
            )
        })
        .count()
}

fn repair_common_mojibake_utf8(text: &str) -> String {
    let suspicious = count_mojibake_markers(text);
    if suspicious < 2 {
        return text.to_string();
    }

    let mut bytes = Vec::<u8>::with_capacity(text.len());
    for ch in text.chars() {
        let code = ch as u32;
        if code > 0xFF {
            return text.to_string();
        }
        bytes.push(code as u8);
    }

    let Ok(repaired) = String::from_utf8(bytes) else {
        return text.to_string();
    };
    let repaired_suspicious = count_mojibake_markers(&repaired);
    let original_turkish = count_turkish_text_markers(text);
    let repaired_turkish = count_turkish_text_markers(&repaired);
    if repaired_suspicious < suspicious && repaired_turkish >= original_turkish {
        repaired
    } else {
        text.to_string()
    }
}

fn extract_email_from_text(text: &str) -> Option<String> {
    let cf_re = regex_lite::Regex::new(r#"(?i)data-cfemail=["']([0-9a-f]{6,})["']"#).unwrap();
    if let Some(cap) = cf_re.captures(text) {
        if let Some(decoded) = cap
            .get(1)
            .and_then(|m| decode_cloudflare_email(m.as_str().trim()))
        {
            return Some(decoded);
        }
    }

    let decoded = decode_html_email_entities(text);
    for source in [text, decoded.as_str()] {
        let mailto_re =
            regex_lite::Regex::new(r"(?i)mailto:([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})").unwrap();
        if let Some(cap) = mailto_re.captures(source) {
            let email = cap
                .get(1)
                .map(|m| m.as_str().trim().to_lowercase())
                .unwrap_or_default();
            if let Some(normalized) = normalize_email_candidate(Some(email)) {
                return Some(normalized);
            }
        }

        let re = regex_lite::Regex::new(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b").unwrap();
        for m in re.find_iter(source) {
            let email = m
                .as_str()
                .trim_matches(|c: char| c == '.' || c == ',' || c == ';' || c == ':' || c == ')')
                .trim_start_matches('(')
                .to_lowercase();
            if let Some(normalized) = normalize_email_candidate(Some(email)) {
                return Some(normalized);
            }
        }

        let alt_re = regex_lite::Regex::new(
            r"(?i)\b([A-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\sat\s)\s*([A-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\sdot\s)\s*([A-Z]{2,})\b",
        )
        .unwrap();
        if let Some(cap) = alt_re.captures(source) {
            let local = cap
                .get(1)
                .map(|m| m.as_str().trim().to_lowercase())
                .unwrap_or_default();
            let domain = cap
                .get(2)
                .map(|m| m.as_str().trim().to_lowercase())
                .unwrap_or_default();
            let tld = cap
                .get(3)
                .map(|m| m.as_str().trim().to_lowercase())
                .unwrap_or_default();
            if let Some(normalized) =
                normalize_email_candidate(Some(format!("{local}@{domain}.{tld}")))
            {
                return Some(normalized);
            }
        }
    }
    None
}

fn decode_basic_html_entities(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .replace("&ouml;", "ö")
        .replace("&Ouml;", "Ö")
        .replace("&#246;", "ö")
        .replace("&#214;", "Ö")
        .replace("&uuml;", "ü")
        .replace("&Uuml;", "Ü")
        .replace("&#252;", "ü")
        .replace("&#220;", "Ü")
        .replace("&ccedil;", "ç")
        .replace("&Ccedil;", "Ç")
        .replace("&#231;", "ç")
        .replace("&#199;", "Ç")
        .replace("&scedil;", "ş")
        .replace("&Scedil;", "Ş")
        .replace("&#351;", "ş")
        .replace("&#350;", "Ş")
        .replace("&#287;", "ğ")
        .replace("&#286;", "Ğ")
        .replace("&iacute;", "ı")
        .replace("&#305;", "ı")
        .replace("&#304;", "İ")
}

fn email_matches_company_domain(email: &str, company_domain: &str) -> bool {
    let domain = email_domain(email).unwrap_or_default();
    if domain.is_empty() {
        return false;
    }
    let cd = company_domain.trim().to_lowercase();
    domain == cd || domain.ends_with(&format!(".{cd}"))
}

fn normalize_contact_email_for_domain(
    email: Option<String>,
    company_domain: &str,
) -> Option<String> {
    normalize_email_candidate(email).and_then(|trimmed| {
        if !email_matches_company_domain(&trimmed, company_domain)
            || !email_is_actionable_outreach_email(&trimmed)
        {
            return None;
        }
        Some(trimmed)
    })
}

fn normalize_directory_email_for_domain(
    email: Option<String>,
    company_domain: &str,
) -> Option<String> {
    normalize_email_candidate(email).and_then(|trimmed| {
        if !email_matches_company_domain(&trimmed, company_domain) {
            return None;
        }
        Some(trimmed)
    })
}

fn normalize_site_contact_email(email: Option<String>) -> Option<String> {
    normalize_email_candidate(email).filter(|trimmed| email_is_actionable_outreach_email(trimmed))
}

fn normalize_outreach_linkedin_url(raw: &str) -> Option<String> {
    extract_personal_linkedin_from_text(raw)
}

fn normalize_company_linkedin_url(raw: &str) -> Option<String> {
    extract_company_linkedin_from_text(raw)
}

fn sales_base_url(_kernel: &openfang_kernel::OpenFangKernel) -> String {
    std::env::var("OPENFANG_PUBLIC_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SALES_BASE_URL.to_string())
}

fn generate_unsubscribe_token(recipient: &str, sender_email: &str) -> String {
    use sha2::Digest;

    let recipient = recipient.trim().to_lowercase();
    let sender_email = sender_email.trim().to_lowercase();
    let mut hasher = sha2::Sha256::new();
    hasher.update(SALES_UNSUBSCRIBE_SALT.as_bytes());
    hasher.update(b":");
    hasher.update(sender_email.as_bytes());
    hasher.update(b":");
    hasher.update(recipient.as_bytes());
    let signature = format!("{:x}", hasher.finalize());
    URL_SAFE_NO_PAD.encode(format!("{recipient}|{sender_email}|{signature}"))
}

fn verify_unsubscribe_token(token: &str) -> Option<String> {
    let decoded = URL_SAFE_NO_PAD.decode(token.trim()).ok()?;
    let payload = String::from_utf8(decoded).ok()?;
    let mut parts = payload.split('|');
    let recipient = parts.next()?.trim().to_lowercase();
    let sender_email = parts.next()?.trim().to_lowercase();
    let _signature = parts.next()?.trim().to_string();
    if parts.next().is_some() || recipient.is_empty() || sender_email.is_empty() {
        return None;
    }
    let expected = generate_unsubscribe_token(&recipient, &sender_email);
    let normalized_expected = URL_SAFE_NO_PAD.decode(expected).ok()?;
    let normalized_payload = URL_SAFE_NO_PAD.decode(token.trim()).ok()?;
    if subtle::ConstantTimeEq::ct_eq(
        normalized_expected.as_slice(),
        normalized_payload.as_slice(),
    )
    .into()
    {
        Some(recipient)
    } else {
        None
    }
}

fn is_valid_sending_subdomain(sender_domain: &str, brand_domain: &str) -> bool {
    let sender = sender_domain.trim().to_lowercase();
    let brand = brand_domain.trim().to_lowercase();
    !sender.is_empty()
        && !brand.is_empty()
        && sender != brand
        && sender.ends_with(&format!(".{brand}"))
}

async fn check_mx_record(domain: &str) -> bool {
    match hickory_resolver::TokioAsyncResolver::tokio_from_system_conf() {
        Ok(resolver) => resolver.mx_lookup(domain).await.is_ok(),
        Err(_) => true,
    }
}

async fn assess_domain_health(domain: &str) -> f64 {
    let mut score: f64 = 0.45;
    if domain.ends_with(".com") || domain.ends_with(".net") || domain.ends_with(".org") {
        score += 0.1;
    }
    if domain.ends_with(".tr") || domain.ends_with(".com.tr") {
        score += 0.15;
    }
    if !is_consumer_email_domain(domain) {
        score += 0.1;
    }
    score.clamp(0.0, 1.0)
}

async fn validate_email_for_sending(
    email: &str,
    suppressed: bool,
    bounce_count: i64,
) -> Result<EmailValidation, String> {
    let mut result = EmailValidation {
        email: email.to_string(),
        syntax_valid: false,
        mx_valid: false,
        domain_health: 0.0,
        suppressed: false,
        classification: "unknown".to_string(),
        safe_to_send: false,
    };

    result.syntax_valid = email_syntax_valid(email);
    if !result.syntax_valid {
        return Ok(result);
    }

    result.suppressed = suppressed;
    if result.suppressed {
        result.classification = classify_email(email, "").to_string();
        return Ok(result);
    }

    let domain = email_domain(email).unwrap_or_default();
    result.mx_valid = check_mx_record(&domain).await;
    result.domain_health = assess_domain_health(&domain).await;
    result.classification = classify_email(email, &domain).to_string();

    result.safe_to_send = result.syntax_valid
        && result.mx_valid
        && !result.suppressed
        && result.domain_health > 0.3
        && bounce_count < 3
        && result.classification == "personal";

    Ok(result)
}

fn stable_sales_id(prefix: &str, parts: &[&str]) -> String {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(prefix.as_bytes());
    for part in parts {
        hasher.update(b"|");
        hasher.update(part.trim().to_lowercase().as_bytes());
    }
    let digest = format!("{:x}", hasher.finalize());
    format!("{prefix}_{}", &digest[..24])
}

fn source_confidence(source: &str) -> f64 {
    match source {
        "directory_listing" => 0.9,
        "site_html" => 0.8,
        "web_search" => 0.6,
        "llm_generation" => 0.4,
        "llm_enrichment" => 0.5,
        _ => 0.3,
    }
}

fn seniority_from_title(title: Option<&str>) -> String {
    let title = title.unwrap_or_default().to_lowercase();
    if title.contains("chief")
        || title.contains("ceo")
        || title.contains("coo")
        || title.contains("founder")
        || title.contains("genel müdür")
        || title.contains("genel mudur")
    {
        "c_level".to_string()
    } else if title.contains("vp") || title.contains("vice president") {
        "vp".to_string()
    } else if title.contains("director") || title.contains("direktör") || title.contains("direktor")
    {
        "director".to_string()
    } else if title.contains("manager") || title.contains("müdür") || title.contains("mudur") {
        "manager".to_string()
    } else {
        "unknown".to_string()
    }
}

fn classify_reply_content(text: &str) -> &'static str {
    let lower = text.to_lowercase();
    if lower.contains("toplanti") || lower.contains("meeting") || lower.contains("goruselim") {
        "meeting_booked"
    } else if lower.contains("ilginc") || lower.contains("interested") || lower.contains("merak") {
        "interested"
    } else if lower.contains("simdi degil") || lower.contains("not now") || lower.contains("sonra")
    {
        "not_now"
    } else if lower.contains("yanlis") || lower.contains("wrong") || lower.contains("hatali") {
        "wrong_person"
    } else if lower.contains("cikar") || lower.contains("unsubscribe") || lower.contains("gonderme")
    {
        "unsubscribe"
    } else {
        "interested"
    }
}

fn classify_outcome(raw_event: &str, event_type: &str, touch_id: &str) -> OutcomeRecord {
    let outcome_type = match event_type {
        "bounce_hard" => "hard_bounce",
        "bounce_soft" => "soft_bounce",
        "open" => "open",
        "click" => "click",
        "reply" => classify_reply_content(raw_event),
        "unsubscribe" => "unsubscribe",
        _ => "no_reply",
    };

    OutcomeRecord {
        touch_id: touch_id.to_string(),
        outcome_type: outcome_type.to_string(),
        raw_text: raw_event.to_string(),
        classifier_confidence: 1.0,
    }
}

fn classify_signal_horizon(signal_type: &str, text: &str) -> (&'static str, Option<String>) {
    let (horizon, days) = match signal_type {
        "tender" | "crisis" | "urgent_hire" => ("immediate", 21),
        "new_department" | "digitalization" | "new_location" => ("campaign_window", 90),
        "erp_migration" | "merger" | "regulation_pressure" => ("structural", 365),
        "job_posting" => {
            if text.to_lowercase().contains("acil") || text.to_lowercase().contains("urgent") {
                ("immediate", 21)
            } else {
                ("campaign_window", 60)
            }
        }
        "directory_membership" => ("structural", 365),
        _ => ("campaign_window", 90),
    };

    let expires = Utc::now()
        .checked_add_signed(chrono::Duration::days(days))
        .map(|value| value.to_rfc3339());
    (horizon, expires)
}

fn generate_signal_rationale(signal_type: &str, text: &str) -> String {
    match signal_type {
        "job_posting" => format!("Hiring activity suggests active change capacity: {text}"),
        "directory_membership" => format!("Verified sector presence supports ICP fit: {text}"),
        "tech_stack" => format!("Observed stack may create switching or integration pain: {text}"),
        _ => format!("Public signal may indicate operational relevance: {text}"),
    }
}

fn infer_signal_type(text: &str) -> &'static str {
    let lower = text.to_lowercase();
    if lower.contains("ihale") || lower.contains("tender") {
        "tender"
    } else if lower.contains("acil") || lower.contains("urgent") {
        "urgent_hire"
    } else if lower.contains("kariyer")
        || lower.contains("career")
        || lower.contains("is ilani")
        || lower.contains("job")
        || lower.contains("hiring")
    {
        "job_posting"
    } else if lower.contains("erp") || lower.contains("sap") || lower.contains("netsis") {
        "erp_migration"
    } else if lower.contains("dijital") || lower.contains("digital") {
        "digitalization"
    } else if lower.contains("tesis") || lower.contains("facility") || lower.contains("lokasyon") {
        "new_location"
    } else if lower.contains("uye") || lower.contains("member") || lower.contains("odasi") {
        "directory_membership"
    } else {
        "site_content"
    }
}

/// Detect job posting signals from search results for an account.
/// Job postings indicate active change capacity and intent.
#[cfg(test)]
fn extract_job_posting_signals(
    search_results: &[SearchEntry],
    account_name: &str,
) -> Vec<(String, String, f64)> {
    let job_keywords = [
        "operasyon",
        "saha",
        "field",
        "operations",
        "hiring",
        "kariyer",
        "is ilani",
        "job",
        "career",
        "ise alim",
        "pozisyon",
        "mudur",
        "yonetici",
        "engineer",
        "technician",
        "teknisyen",
    ];
    let name_lower = account_name.to_lowercase();

    search_results
        .iter()
        .filter(|entry| {
            let title_lower = entry.title.to_lowercase();
            let url_lower = entry.url.to_lowercase();
            let name_match = title_lower.contains(&name_lower)
                || url_lower.contains("kariyer.net")
                || url_lower.contains("linkedin.com/jobs");
            let keyword_match = job_keywords.iter().any(|kw| title_lower.contains(kw));
            name_match && keyword_match
        })
        .map(|entry| {
            let confidence = if entry.url.contains("kariyer.net") {
                0.8
            } else if entry.url.contains("linkedin.com") {
                0.7
            } else {
                0.5
            };
            (entry.title.clone(), entry.url.clone(), confidence)
        })
        .collect()
}

/// Detect job posting intent from OSINT URLs (lightweight URL-based variant).
fn detect_job_posting_intent_from_urls(osint_links: &[String]) -> Vec<String> {
    let job_domains = [
        "kariyer.net",
        "linkedin.com/jobs",
        "indeed.com",
        "glassdoor.com",
        "secretcv.com",
        "yenibiris.com",
    ];
    osint_links
        .iter()
        .filter(|url| {
            let lower = url.to_lowercase();
            job_domains.iter().any(|d| lower.contains(d))
        })
        .map(|url| format!("Job posting: {}", url))
        .collect()
}

/// Search job posting sites for intent signals for a given account (TASK-27).
/// Job postings on kariyer.net, LinkedIn Jobs, etc. indicate active change
/// capacity and serve as campaign-window intent signals.
async fn search_job_posting_signals(
    account_name: &str,
    domain: &str,
    search_engine: &WebSearchEngine,
) -> Vec<(String, String, f64, String)> {
    // (signal_text, source_url, confidence, signal_type)
    let transliterated = transliterate_turkish_ascii(account_name);
    let queries = vec![
        format!("site:kariyer.net \"{}\"", transliterated),
        format!("site:linkedin.com/jobs \"{}\"", transliterated),
        format!(
            "\"{}\" \"saha\" OR \"operasyon\" OR \"field\" iş ilanı",
            transliterated
        ),
    ];
    let timeout = Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS);
    let job_keywords = [
        "operasyon",
        "saha",
        "field",
        "operations",
        "hiring",
        "kariyer",
        "is ilani",
        "job",
        "career",
        "ise alim",
        "pozisyon",
        "mudur",
        "yonetici",
        "engineer",
        "technician",
        "teknisyen",
        "bakim",
        "maintenance",
        "uretim",
        "production",
    ];

    let mut signals = Vec::new();
    for q in &queries {
        let raw = match run_sales_search(search_engine, q, 5, timeout).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in parse_search_entries(&raw) {
            let title_lower = entry.title.to_lowercase();
            let url_lower = entry.url.to_lowercase();
            let domain_lower = domain.to_lowercase();
            let name_lower = transliterated.to_lowercase();

            // Must be related to the target account
            let name_match = title_lower.contains(&name_lower)
                || title_lower.contains(&domain_lower)
                || url_lower.contains("kariyer.net")
                || url_lower.contains("linkedin.com/jobs");

            let keyword_match = job_keywords.iter().any(|kw| title_lower.contains(kw));

            if name_match && keyword_match {
                let confidence = if url_lower.contains("kariyer.net") {
                    0.8
                } else if url_lower.contains("linkedin.com") {
                    0.7
                } else {
                    0.5
                };
                signals.push((
                    entry.title.clone(),
                    entry.url.clone(),
                    confidence,
                    "job_posting".to_string(),
                ));
            }
        }
        if signals.len() >= 5 {
            break;
        }
    }
    signals
}

/// Detect tech stack from site HTML content and HTTP headers.
fn detect_tech_stack(html: &str, headers: &HashMap<String, String>) -> Vec<String> {
    let detections: &[(&str, &[&str])] = &[
        ("SAP", &["sap.com", "sap-ui", "sapui5", "/sap/"]),
        ("Salesforce", &["salesforce.com", "force.com", "pardot"]),
        ("HubSpot", &["hubspot.com", "hs-scripts", "hbspt"]),
        ("Microsoft Dynamics", &["dynamics.com", "d365"]),
        ("Oracle", &["oracle.com", "eloqua"]),
        ("WordPress", &["wp-content", "wp-includes", "wordpress"]),
        ("Shopify", &["shopify.com", "cdn.shopify"]),
        ("React", &["react-root", "reactjs", "__NEXT_DATA__"]),
        ("Angular", &["ng-version", "angular"]),
        ("Vue.js", &["vue-app", "vuejs"]),
        (
            "Google Analytics",
            &["google-analytics.com", "gtag/js", "ga.js"],
        ),
        ("Google Tag Manager", &["googletagmanager.com", "gtm.js"]),
        ("Hotjar", &["hotjar.com", "static.hotjar"]),
        ("Intercom", &["intercom.io", "intercomSettings"]),
        ("Zendesk", &["zendesk.com", "zdassets"]),
        ("Jira", &["atlassian.net", "jira"]),
        ("Netsis", &["netsis"]),
        ("Logo Yazılım", &["logo.com.tr", "logo yazılım"]),
        ("IFS", &["ifs.com", "ifsworld"]),
    ];

    let html_lower = html.to_lowercase();
    let mut stack: Vec<String> = detections
        .iter()
        .filter(|(_, indicators)| indicators.iter().any(|ind| html_lower.contains(ind)))
        .map(|(name, _)| name.to_string())
        .collect();

    if let Some(powered_by) = headers.get("x-powered-by") {
        if !powered_by.trim().is_empty() {
            stack.push(powered_by.trim().to_string());
        }
    }
    if let Some(server) = headers.get("server") {
        let sv = server.trim().to_lowercase();
        if sv.contains("nginx") || sv.contains("apache") || sv.contains("iis") {
            stack.push(server.trim().to_string());
        }
    }

    stack.sort();
    stack.dedup();
    stack
}

/// 4-Layer LinkedIn company URL search (TASK-24).
/// Progressively broader queries to find the LinkedIn company page.
async fn find_company_linkedin_url(
    company_name: &str,
    domain: &str,
    search_engine: &WebSearchEngine,
) -> Option<String> {
    let timeout = Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS);
    let transliterated = transliterate_turkish_ascii(company_name);

    // Layer 1: Domain match — most precise
    let q1 = format!("site:linkedin.com/company/ \"{}\"", domain);
    if let Some(url) = linkedin_search_attempt(search_engine, &q1, timeout).await {
        return Some(url);
    }

    // Layer 2: Transliterated company name (handles Turkish chars)
    let q2 = format!("site:linkedin.com/company/ \"{}\"", transliterated);
    if let Some(url) = linkedin_search_attempt(search_engine, &q2, timeout).await {
        return Some(url);
    }

    // Layer 3: Company name + CEO search
    let q3 = format!("\"{}\" linkedin CEO OR \"Genel Müdür\"", company_name);
    if let Some(url) = linkedin_search_attempt(search_engine, &q3, timeout).await {
        return Some(url);
    }

    // Layer 4: Turkish LinkedIn subdomain
    let q4 = format!("site:tr.linkedin.com \"{}\"", domain);
    linkedin_search_attempt(search_engine, &q4, timeout).await
}

async fn linkedin_search_attempt(
    search_engine: &WebSearchEngine,
    query: &str,
    timeout: Duration,
) -> Option<String> {
    match run_sales_search(search_engine, query, 5, timeout).await {
        Ok(res) if !res.trim().is_empty() => extract_company_linkedin_from_text(&res)
            .and_then(|value| normalize_company_linkedin_url(&value)),
        _ => None,
    }
}

/// Seed default contextual factors for Turkish market timing (TASK-35).
fn seed_contextual_factors(conn: &Connection) {
    let factors: &[(&str, &str, &str, &str, &str)] = &[
        (
            "holiday",
            "ramazan_bayrami",
            "Ramazan Bayramı — avoid outreach",
            "2026-03-20",
            "2026-03-23",
        ),
        (
            "holiday",
            "kurban_bayrami",
            "Kurban Bayramı — avoid outreach",
            "2026-05-27",
            "2026-05-30",
        ),
        (
            "holiday",
            "cumhuriyet_bayrami",
            "Cumhuriyet Bayramı — avoid outreach",
            "2026-10-29",
            "2026-10-29",
        ),
        (
            "holiday",
            "yilbasi",
            "Yılbaşı — avoid outreach",
            "2026-12-31",
            "2027-01-01",
        ),
        (
            "budget_quarter",
            "q1_budget",
            "Q1 budget planning — high activity",
            "2026-01-02",
            "2026-03-31",
        ),
        (
            "budget_quarter",
            "q2_budget",
            "Q2 budget planning — high activity",
            "2026-04-01",
            "2026-06-30",
        ),
        (
            "budget_quarter",
            "q3_budget",
            "Q3 budget planning — high activity",
            "2026-07-01",
            "2026-09-30",
        ),
        (
            "budget_quarter",
            "q4_budget",
            "Q4 budget planning — high activity",
            "2026-10-01",
            "2026-12-31",
        ),
        (
            "season",
            "summer_slow",
            "Summer slowdown — reduced response rates",
            "2026-07-15",
            "2026-08-31",
        ),
        (
            "regulation",
            "kvkk",
            "KVKK (Turkish GDPR) — ensure compliance",
            "2016-04-07",
            "2099-12-31",
        ),
    ];
    for (factor_type, factor_key, factor_value, eff_from, eff_until) in factors {
        let id = stable_sales_id("ctx_factor", &[factor_type, factor_key]);
        let _ = conn.execute(
            "INSERT OR IGNORE INTO contextual_factors (id, factor_type, factor_key, factor_value, effective_from, effective_until, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'system_default')",
            params![id, factor_type, factor_key, factor_value, eff_from, eff_until],
        );
    }
}

/// Check if today falls within a holiday or slow period (TASK-35).
fn is_bad_timing_today(conn: &Connection) -> bool {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM contextual_factors
             WHERE factor_type IN ('holiday', 'season')
             AND effective_from <= ?1 AND effective_until >= ?1",
            params![today],
            |r| r.get(0),
        )
        .unwrap_or(0);
    count > 0
}

/// Determine the current budget quarter context (TASK-35).
fn current_budget_quarter(conn: &Connection) -> Option<String> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    conn.query_row(
        "SELECT factor_value FROM contextual_factors
         WHERE factor_type = 'budget_quarter'
         AND effective_from <= ?1 AND effective_until >= ?1
         LIMIT 1",
        params![today],
        |r| r.get(0),
    )
    .ok()
}

/// Calibrate scoring weights from outcome data (TASK-36).
/// Analyzes positive/negative outcomes and creates rule proposals when
/// signal weights appear to need adjustment.
fn calibrate_scoring_from_outcomes(conn: &Connection) -> Result<Vec<String>, String> {
    let mut proposals = Vec::new();

    // Only calibrate when we have enough data
    let outcome_count: i32 = conn
        .query_row("SELECT COUNT(*) FROM outcomes", [], |r| r.get(0))
        .unwrap_or(0);
    if outcome_count < 10 {
        return Ok(proposals);
    }

    // Analyze which signals appear in positive vs negative outcomes
    let mut stmt = conn
        .prepare(
            "SELECT s.signal_type,
                    SUM(CASE WHEN o.outcome_type IN ('meeting_booked', 'interested', 'click') THEN 1 ELSE 0 END) as positive,
                    SUM(CASE WHEN o.outcome_type IN ('hard_bounce', 'unsubscribe', 'wrong_person') THEN 1 ELSE 0 END) as negative,
                    COUNT(*) as total
             FROM outcome_attribution_snapshots oas
             JOIN outcomes o ON o.touch_id = oas.touch_id
             JOIN signals s ON s.account_id = oas.account_id
             GROUP BY s.signal_type
             HAVING total >= 3",
        )
        .map_err(|e| format!("Calibration query failed: {e}"))?;

    let rows: Vec<(String, i32, i32, i32)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| format!("Calibration query failed: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    for (signal_type, positive, negative, total) in rows {
        let positive_rate = positive as f64 / total as f64;
        let negative_rate = negative as f64 / total as f64;

        // If this signal type has high negative correlation, propose reducing its weight
        if negative_rate > 0.5 && total >= 5 {
            let proposal_id = stable_sales_id(
                "rule_proposal",
                &[
                    &signal_type,
                    "weight_down",
                    &Utc::now().format("%Y-%W").to_string(),
                ],
            );
            let _ = conn.execute(
                "INSERT OR IGNORE INTO retrieval_rule_versions
                 (id, rule_type, rule_key, old_value, new_value, proposal_source, status, version, created_at)
                 VALUES (?1, 'signal_weight', ?2, ?3, ?4, 'auto_calibration', 'proposed', 1, ?5)",
                params![
                    proposal_id,
                    signal_type,
                    format!("current (neg_rate={negative_rate:.2})"),
                    format!("reduce_weight (pos={positive}, neg={negative}, total={total})"),
                    Utc::now().to_rfc3339(),
                ],
            );
            proposals.push(format!(
                "Propose reducing weight for signal '{signal_type}': neg_rate={negative_rate:.2}"
            ));
        }

        // If this signal type has high positive correlation, propose increasing weight
        if positive_rate > 0.6 && total >= 5 {
            let proposal_id = stable_sales_id(
                "rule_proposal",
                &[
                    &signal_type,
                    "weight_up",
                    &Utc::now().format("%Y-%W").to_string(),
                ],
            );
            let _ = conn.execute(
                "INSERT OR IGNORE INTO retrieval_rule_versions
                 (id, rule_type, rule_key, old_value, new_value, proposal_source, status, version, created_at)
                 VALUES (?1, 'signal_weight', ?2, ?3, ?4, 'auto_calibration', 'proposed', 1, ?5)",
                params![
                    proposal_id,
                    signal_type,
                    format!("current (pos_rate={positive_rate:.2})"),
                    format!("increase_weight (pos={positive}, neg={negative}, total={total})"),
                    Utc::now().to_rfc3339(),
                ],
            );
            proposals.push(format!(
                "Propose increasing weight for signal '{signal_type}': pos_rate={positive_rate:.2}"
            ));
        }
    }

    Ok(proposals)
}

/// Create an A/B experiment and return its ID (TASK-37).
fn create_experiment(
    conn: &Connection,
    name: &str,
    hypothesis: &str,
    variant_a: &str,
    variant_b: &str,
) -> Result<String, String> {
    let id = stable_sales_id("experiment", &[name]);
    conn.execute(
        "INSERT INTO experiments (id, name, hypothesis, variant_a, variant_b, status, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6)
         ON CONFLICT(id) DO UPDATE SET
            hypothesis = excluded.hypothesis,
            variant_a = excluded.variant_a,
            variant_b = excluded.variant_b",
        params![
            id,
            name,
            hypothesis,
            variant_a,
            variant_b,
            Utc::now().to_rfc3339()
        ],
    )
    .map_err(|e| format!("Failed to create experiment: {e}"))?;
    Ok(id)
}

/// Assign a sequence instance to an experiment variant (TASK-37).
fn assign_experiment_variant(
    conn: &Connection,
    experiment_id: &str,
    sequence_instance_id: &str,
) -> Result<String, String> {
    // Balanced assignment: pick whichever variant has fewer assignments
    let count_a: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM experiment_assignments
             WHERE experiment_id = ?1 AND variant = 'a'",
            params![experiment_id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let count_b: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM experiment_assignments
             WHERE experiment_id = ?1 AND variant = 'b'",
            params![experiment_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let variant = if count_a <= count_b { "a" } else { "b" };
    let id = stable_sales_id("exp_assign", &[experiment_id, sequence_instance_id]);
    conn.execute(
        "INSERT OR IGNORE INTO experiment_assignments (id, experiment_id, sequence_instance_id, variant)
         VALUES (?1, ?2, ?3, ?4)",
        params![id, experiment_id, sequence_instance_id, variant],
    )
    .map_err(|e| format!("Failed to assign experiment variant: {e}"))?;
    Ok(variant.to_string())
}

/// Get experiment results summary (TASK-37).
fn get_experiment_results(
    conn: &Connection,
    experiment_id: &str,
) -> Result<serde_json::Value, String> {
    let mut stmt = conn
        .prepare(
            "SELECT ea.variant,
                    COUNT(DISTINCT ea.sequence_instance_id) as sequences,
                    SUM(CASE WHEN o.outcome_type IN ('meeting_booked', 'interested') THEN 1 ELSE 0 END) as positive,
                    SUM(CASE WHEN o.outcome_type IN ('hard_bounce', 'unsubscribe') THEN 1 ELSE 0 END) as negative,
                    COUNT(o.id) as total_outcomes
             FROM experiment_assignments ea
             LEFT JOIN sequence_instances si ON si.id = ea.sequence_instance_id
             LEFT JOIN touches t ON t.sequence_instance_id = si.id
             LEFT JOIN outcomes o ON o.touch_id = t.id
             WHERE ea.experiment_id = ?1
             GROUP BY ea.variant",
        )
        .map_err(|e| format!("Experiment results query failed: {e}"))?;

    let variants: Vec<serde_json::Value> = stmt
        .query_map(params![experiment_id], |r| {
            let variant: String = r.get(0)?;
            let sequences: i32 = r.get(1)?;
            let positive: i32 = r.get(2)?;
            let negative: i32 = r.get(3)?;
            let total: i32 = r.get(4)?;
            Ok(serde_json::json!({
                "variant": variant,
                "sequences": sequences,
                "positive_outcomes": positive,
                "negative_outcomes": negative,
                "total_outcomes": total,
                "positive_rate": if total > 0 { positive as f64 / total as f64 } else { 0.0 },
            }))
        })
        .map_err(|e| format!("Experiment results query failed: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(serde_json::json!({
        "experiment_id": experiment_id,
        "variants": variants,
    }))
}

/// Verify LLM-generated domain actually exists with a HEAD request (TASK-40).
async fn verify_domain_exists(domain: &str) -> bool {
    let url = format!("https://{domain}");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.head(&url).send().await {
        Ok(resp) => resp.status().is_success() || resp.status().is_redirection(),
        Err(_) => {
            // Try HTTP as fallback
            let http_url = format!("http://{domain}");
            client
                .head(&http_url)
                .send()
                .await
                .map(|r| r.status().is_success() || r.status().is_redirection())
                .unwrap_or(false)
        }
    }
}

fn candidate_primary_source_type(
    candidate: &DomainCandidate,
    company_linkedin_url: Option<&str>,
) -> &'static str {
    if candidate.phone.is_some() {
        "directory_listing"
    } else if company_linkedin_url.is_some() {
        "web_search"
    } else {
        "site_html"
    }
}

fn canonical_contact_key(
    domain: &str,
    contact_name: Option<&str>,
    email: Option<&str>,
    linkedin_url: Option<&str>,
) -> String {
    if let Some(email) = email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
    {
        return email;
    }
    if let Some(linkedin) = linkedin_url.and_then(normalize_outreach_linkedin_url) {
        return linkedin;
    }
    if let Some(name) = contact_name.and_then(normalize_person_name) {
        return name.to_lowercase();
    }
    format!("{domain}-primary")
}

fn infer_buyer_role(title: &str) -> &'static str {
    let lower = title.to_lowercase();
    if lower.contains("founder") {
        "founder"
    } else if lower.contains("ceo") || lower.contains("chief executive") {
        "ceo"
    } else if lower.contains("coo") || lower.contains("operations") {
        "operations"
    } else if lower.contains("sales") {
        "revenue"
    } else {
        "buyer_committee"
    }
}

fn parse_json_string_list(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn rules_match(value: &str, rules: &[String]) -> bool {
    let lower = value.trim().to_lowercase();
    if lower.is_empty() {
        return false;
    }
    rules.iter().any(|rule| {
        let rule = rule.trim().to_lowercase();
        !rule.is_empty() && (lower.contains(&rule) || rule.contains(&lower))
    })
}

fn compute_fit_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let (sector, geo, employee_estimate) = db
        .query_row(
            "SELECT COALESCE(sector, ''), COALESCE(geo, ''), employee_estimate
             FROM accounts WHERE id = ?1",
            params![account_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .map_err(|e| format!("Failed to load account fit state: {e}"))?;
    let (sector_rules, geo_rules) = db
        .query_row(
            "SELECT COALESCE(sector_rules, '[]'), COALESCE(geo_rules, '[]')
             FROM icp_definitions
             ORDER BY created_at DESC
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|e| format!("Failed to load ICP rules: {e}"))?
        .unwrap_or_else(|| ("[]".to_string(), "[]".to_string()));
    let sector_match: f64 = if rules_match(&sector, &parse_json_string_list(&sector_rules)) {
        1.0
    } else if !sector.trim().is_empty() {
        0.45
    } else {
        0.0
    };
    let geo_match: f64 = if rules_match(&geo, &parse_json_string_list(&geo_rules)) {
        1.0
    } else if !geo.trim().is_empty() {
        0.4
    } else {
        0.0
    };
    let size_match: f64 = if employee_estimate.unwrap_or_default() > 0 {
        1.0
    } else {
        0.4
    };
    let site_content_count = db
        .query_row(
            "SELECT COUNT(*) FROM signals WHERE account_id = ?1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let site_content_match: f64 = if site_content_count > 0 { 1.0 } else { 0.35 };
    let directory_membership = db
        .query_row(
            "SELECT COUNT(*) FROM signals
             WHERE account_id = ?1 AND signal_type = 'directory_membership'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let directory_score: f64 = if directory_membership > 0 { 1.0 } else { 0.25 };
    Ok((sector_match * 0.3
        + size_match * 0.2
        + geo_match * 0.2
        + site_content_match * 0.15
        + directory_score * 0.15)
        .clamp(0.0, 1.0))
}

fn compute_intent_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let mut stmt = db
        .prepare(
            "SELECT signal_type, COALESCE(text, ''), COALESCE(effect_horizon, '')
             FROM signals
             WHERE account_id = ?1",
        )
        .map_err(|e| format!("Failed to prepare intent query: {e}"))?;
    let mut rows = stmt
        .query(params![account_id])
        .map_err(|e| format!("Failed to query intent signals: {e}"))?;
    let mut score: f64 = 0.0;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("Failed to read intent signals: {e}"))?
    {
        let signal_type = row.get::<_, String>(0).unwrap_or_default();
        let text = row.get::<_, String>(1).unwrap_or_default();
        let horizon = row.get::<_, String>(2).unwrap_or_default();
        let weight = match signal_type.as_str() {
            "tender" | "urgent_hire" => 0.3,
            "job_posting" | "new_location" | "digitalization" => 0.2,
            "erp_migration" | "directory_membership" => 0.15,
            _ => 0.1,
        };
        let horizon_boost = match horizon.as_str() {
            "immediate" => 1.0,
            "campaign_window" => 0.75,
            "structural" => 0.45,
            _ => 0.3,
        };
        let text_boost = if text.to_lowercase().contains("acil")
            || text.to_lowercase().contains("urgent")
            || text.to_lowercase().contains("launch")
        {
            1.0
        } else {
            0.75
        };
        score += weight * horizon_boost * text_boost;
    }
    Ok(score.clamp(0.0, 1.0))
}

fn compute_reachability_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let mut stmt = db
        .prepare(
            "SELECT cm.channel_type, COALESCE(cm.classification, ''), COALESCE(c.full_name, ''),
                    COALESCE(c.title, ''), c.title_confidence
             FROM contacts c
             LEFT JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1",
        )
        .map_err(|e| format!("Failed to prepare reachability query: {e}"))?;
    let mut rows = stmt
        .query(params![account_id])
        .map_err(|e| format!("Failed to query reachability state: {e}"))?;
    let mut has_personal_email = false;
    let mut has_linkedin = false;
    let mut has_phone = false;
    let mut has_real_name = false;
    let mut has_verified_title = false;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("Failed to read reachability state: {e}"))?
    {
        let channel = row
            .get::<_, Option<String>>(0)
            .unwrap_or_default()
            .unwrap_or_default();
        let classification = row
            .get::<_, Option<String>>(1)
            .unwrap_or_default()
            .unwrap_or_default();
        let full_name = row
            .get::<_, Option<String>>(2)
            .unwrap_or_default()
            .unwrap_or_default();
        let title = row
            .get::<_, Option<String>>(3)
            .unwrap_or_default()
            .unwrap_or_default();
        let title_confidence = row
            .get::<_, Option<f64>>(4)
            .unwrap_or_default()
            .unwrap_or(0.0);
        if channel == "email" && classification == "personal" {
            has_personal_email = true;
        }
        if channel == "linkedin" {
            has_linkedin = true;
        }
        if channel == "phone" {
            has_phone = true;
        }
        if !contact_name_is_placeholder(Some(full_name.as_str())) {
            has_real_name = true;
        }
        if !contact_title_is_generic_default(Some(title.as_str())) && title_confidence >= 0.6 {
            has_verified_title = true;
        }
    }
    let reach: f64 = (if has_personal_email { 0.35 } else { 0.0 })
        + (if has_linkedin { 0.25 } else { 0.0 })
        + (if has_phone { 0.2 } else { 0.0 })
        + (if has_real_name { 0.1 } else { 0.0 })
        + (if has_verified_title { 0.1 } else { 0.0 });
    Ok(reach.clamp(0.0, 1.0))
}

fn compute_deliverability_risk(account_id: &str, db: &Connection) -> Result<f64, String> {
    let hard_bounces = db
        .query_row(
            "SELECT COUNT(*)
             FROM outcomes o
             JOIN touches t ON t.id = o.touch_id
             JOIN sequence_instances si ON si.id = t.sequence_instance_id
             WHERE si.account_id = ?1 AND o.outcome_type = 'hard_bounce'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let domain_risk = db
        .query_row(
            "SELECT MAX(CASE WHEN COALESCE(mx_valid, 0) = 1 THEN 0.1 ELSE 0.45 END)
             FROM domains WHERE account_id = ?1",
            params![account_id],
            |row| row.get::<_, Option<f64>>(0),
        )
        .unwrap_or(Some(0.45))
        .unwrap_or(0.45);
    let generic_email = db
        .query_row(
            "SELECT COUNT(*)
             FROM contacts c
             JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1 AND cm.channel_type = 'email' AND COALESCE(cm.classification, '') != 'personal'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let sender_risk = db
        .query_row(
            "SELECT COALESCE(warm_state, 'cold') FROM sender_policies ORDER BY rowid DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| format!("Failed to load sender policy: {e}"))?
        .map(|state| if state == "ready" { 0.05 } else { 0.15 })
        .unwrap_or(0.15);
    let risk: f64 = (hard_bounces as f64 * 0.15).min(0.3)
        + domain_risk.min(0.3)
        + if generic_email > 0 { 0.2 } else { 0.0 }
        + sender_risk;
    Ok(risk.clamp(0.0, 1.0))
}

fn compute_compliance_risk(account_id: &str, db: &Connection) -> Result<f64, String> {
    let suppressed = db
        .query_row(
            "SELECT COUNT(*)
             FROM contacts c
             JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1 AND COALESCE(cm.suppressed, 0) = 1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let opt_outs = db
        .query_row(
            "SELECT COUNT(*)
             FROM outcomes o
             JOIN touches t ON t.id = o.touch_id
             JOIN sequence_instances si ON si.id = t.sequence_instance_id
             WHERE si.account_id = ?1 AND o.outcome_type = 'unsubscribe'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let (geo, generic_email_only) = db
        .query_row(
            "SELECT COALESCE(a.geo, ''),
                    CASE WHEN EXISTS(
                        SELECT 1
                        FROM contacts c
                        JOIN contact_methods cm ON cm.contact_id = c.id
                        WHERE c.account_id = a.id
                          AND cm.channel_type = 'email'
                          AND COALESCE(cm.classification, '') != 'personal'
                    ) THEN 1 ELSE 0 END
             FROM accounts a
             WHERE a.id = ?1",
            params![account_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(|e| format!("Failed to load compliance state: {e}"))?;
    let kvkk_risk = if geo_is_turkey(&geo) && generic_email_only == 1 {
        0.3
    } else if geo_is_turkey(&geo) {
        0.15
    } else {
        0.05
    };
    let risk: f64 = (if suppressed > 0 { 0.4 } else { 0.0 })
        + (if opt_outs > 0 { 0.3 } else { 0.0 })
        + kvkk_risk;
    Ok(risk.clamp(0.0, 1.0))
}

fn save_score_snapshot(
    db: &Connection,
    account_id: &str,
    score: &FiveAxisScore,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO score_snapshots
         (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk,
          activation_priority, computed_at, scoring_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'v1')
         ON CONFLICT(id) DO UPDATE SET
            fit_score = excluded.fit_score,
            intent_score = excluded.intent_score,
            reachability_score = excluded.reachability_score,
            deliverability_risk = excluded.deliverability_risk,
            compliance_risk = excluded.compliance_risk,
            activation_priority = excluded.activation_priority,
            computed_at = excluded.computed_at",
        params![
            stable_sales_id("score_snapshot", &[account_id]),
            account_id,
            score.fit_score,
            score.intent_score,
            score.reachability_score,
            score.deliverability_risk,
            score.compliance_risk,
            activation_priority(score),
            Utc::now().to_rfc3339(),
        ],
    )
    .map_err(|e| format!("Failed to save score snapshot: {e}"))?;
    Ok(())
}

fn compute_five_axis_score(account_id: &str, db: &Connection) -> Result<FiveAxisScore, String> {
    let score = FiveAxisScore {
        fit_score: compute_fit_score(account_id, db)?,
        intent_score: compute_intent_score(account_id, db)?,
        reachability_score: compute_reachability_score(account_id, db)?,
        deliverability_risk: compute_deliverability_risk(account_id, db)?,
        compliance_risk: compute_compliance_risk(account_id, db)?,
    };
    save_score_snapshot(db, account_id, &score)?;
    Ok(score)
}

fn activation_priority(score: &FiveAxisScore) -> f64 {
    ((score.fit_score * 0.35)
        + (score.intent_score * 0.25)
        + (score.reachability_score * 0.25)
        + ((1.0 - score.deliverability_risk) * 0.1)
        + ((1.0 - score.compliance_risk) * 0.05))
        .clamp(0.0, 1.0)
}

fn thesis_confidence(score: &FiveAxisScore) -> f64 {
    ((score.fit_score
        + score.intent_score
        + score.reachability_score
        + (1.0 - score.deliverability_risk)
        + (1.0 - score.compliance_risk))
        / 5.0)
        .clamp(0.0, 1.0)
}

fn recommended_activation_channel(
    db: &Connection,
    account_id: &str,
    contact_id: &str,
) -> Option<String> {
    let mut stmt = db
        .prepare(
            "SELECT channel_type, COALESCE(classification, '')
             FROM contact_methods
             WHERE contact_id = ?1
             ORDER BY confidence DESC",
        )
        .ok()?;
    let methods = stmt
        .query_map(params![contact_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let has_personal_email = methods
        .iter()
        .any(|(channel, classification)| channel == "email" && classification == "personal");
    let has_phone = methods.iter().any(|(channel, _)| channel == "phone");
    let has_linkedin = methods.iter().any(|(channel, _)| channel == "linkedin");
    if has_personal_email {
        Some("email".to_string())
    } else if has_phone {
        Some("phone_task".to_string())
    } else if has_linkedin {
        Some("linkedin_assist".to_string())
    } else {
        let account_has_any_method = db
            .query_row(
                "SELECT COUNT(*)
                 FROM contacts c
                 JOIN contact_methods cm ON cm.contact_id = c.id
                 WHERE c.account_id = ?1",
                params![account_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);
        if account_has_any_method > 0 {
            Some("research".to_string())
        } else {
            None
        }
    }
}

fn send_gate(score: &FiveAxisScore) -> SendGateDecision {
    if score.deliverability_risk > 0.7 {
        return SendGateDecision::Block {
            reason: "Deliverability risk too high".to_string(),
        };
    }
    if score.compliance_risk > 0.5 {
        return SendGateDecision::Block {
            reason: "Compliance risk too high".to_string(),
        };
    }
    if score.reachability_score < 0.3 {
        return SendGateDecision::Research {
            missing: vec!["Need personal email or LinkedIn profile".to_string()],
        };
    }
    if score.intent_score < 0.2 {
        return SendGateDecision::Nurture {
            reason: "No active intent signals detected".to_string(),
        };
    }
    if score.fit_score > 0.5 && score.reachability_score > 0.4 {
        return SendGateDecision::Activate;
    }
    SendGateDecision::Research {
        missing: vec!["Need more data to make decision".to_string()],
    }
}

fn assign_tier(score: &FiveAxisScore) -> &'static str {
    if score.fit_score > 0.8 && score.intent_score > 0.5 {
        "a_tier"
    } else if score.fit_score > 0.5 {
        "standard"
    } else {
        "basic"
    }
}

fn decode_percent_utf8_lossy(raw: &str) -> String {
    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                out.push((high << 4) | low);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn canonicalize_osint_url(raw: &str) -> Option<String> {
    let trimmed = decode_basic_html_entities(raw).trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    let mut parsed = url::Url::parse(&trimmed).ok()?;
    parsed.set_fragment(None);
    parsed.set_query(None);

    let host = parsed.host_str()?.trim_end_matches('.').to_lowercase();
    parsed.set_host(Some(&host)).ok()?;

    let original_path = parsed.path().to_string();
    let had_trailing_slash = original_path.len() > 1 && original_path.ends_with('/');
    let decoded_path = decode_percent_utf8_lossy(&original_path);
    let mut normalized_path = if decoded_path.trim().is_empty() {
        "/".to_string()
    } else if decoded_path.starts_with('/') {
        decoded_path
    } else {
        format!("/{decoded_path}")
    };
    while normalized_path.contains("//") {
        normalized_path = normalized_path.replace("//", "/");
    }
    if normalized_path.len() > 1 {
        normalized_path = normalized_path.trim_end_matches('/').to_string();
        if had_trailing_slash {
            normalized_path.push('/');
        }
    }
    parsed.set_path(&normalized_path);

    Some(parsed.to_string())
}

fn absolutize_source_link(base: &str, raw_href: &str) -> Option<String> {
    let href = decode_basic_html_entities(raw_href).trim().to_string();
    if href.is_empty() {
        return None;
    }
    if let Ok(url) = url::Url::parse(&href) {
        return canonicalize_osint_url(url.as_str()).or_else(|| Some(url.to_string()));
    }
    let base = url::Url::parse(base).ok()?;
    base.join(&href)
        .ok()
        .and_then(|url| canonicalize_osint_url(url.as_str()).or_else(|| Some(url.to_string())))
}

fn osint_source_priority(raw: &str) -> i32 {
    let Some(canonical) = canonicalize_osint_url(raw) else {
        return 0;
    };
    let Ok(parsed) = url::Url::parse(&canonical) else {
        return 0;
    };
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    let path = parsed.path().to_lowercase();
    let normalized_path = path.trim_matches('/');
    if host.ends_with("tmb.org.tr") {
        if path.starts_with("/en/m/") || path.starts_with("/tr/m/") {
            return 6;
        }
        if path == "/en/members" || path == "/tr/uyeler" {
            return 2;
        }
    }
    if host.ends_with("mib.org.tr") {
        if path.contains("/firm/") {
            return 6;
        }
        if path.contains("/our-members") {
            return 2;
        }
    }
    if host.ends_with("imder.org.tr") || host.ends_with("isder.org.tr") {
        if normalized_path.is_empty() {
            return 0;
        }
        if normalized_path == "uyelerimiz" {
            return 2;
        }
        if path.contains("/uyelerimiz/") || !normalized_path.starts_with("uyelerimiz") {
            return 6;
        }
        if normalized_path.starts_with("uyelerimiz") {
            return 2;
        }
    }
    if host.ends_with("asmud.org.tr") && path.contains("uyeler.asp") {
        return 2;
    }
    if host.ends_with("platformder.org.tr") && path.contains("/rehber/") {
        return 2;
    }
    if host.ends_with("thbb.org") && path.contains("yazismali-uyeler") {
        return 2;
    }
    if host.ends_with("eder.org.tr")
        && (path.contains("/uyelerimiz") || path.contains("/our-members"))
    {
        return 2;
    }
    if host.ends_with("lojider.org.tr")
        && (path.contains("/member-list") || path.contains("/uye-listesi"))
    {
        return 2;
    }
    if host.ends_with("tfyd.org.tr") && path.contains("/uyelerimiz") {
        return 2;
    }
    if host.ends_with("oss.org.tr") && (path.contains("/members") || path.contains("/uyeler")) {
        return 2;
    }
    0
}

fn osint_link_priority(raw: &str) -> i32 {
    let canonical = canonicalize_osint_url(raw).unwrap_or_else(|| raw.trim().to_string());
    let lower = canonical.to_lowercase();
    if normalize_outreach_linkedin_url(raw).is_some() {
        30
    } else if normalize_company_linkedin_url(raw).is_some() {
        24
    } else if lower.starts_with("http://") || lower.starts_with("https://") {
        enrich_link_priority(&canonical) + 6 + osint_source_priority(&canonical)
    } else {
        0
    }
}

fn site_link_category(path: &str) -> Option<&'static str> {
    let normalized = decode_percent_utf8_lossy(path).to_lowercase();
    if normalized.trim_matches('/').is_empty() {
        return Some("root");
    }
    if normalized.contains("icra-kurulu")
        || normalized.contains("ust-yonetim")
        || normalized.contains("executive-team")
        || normalized.contains("leadership")
        || normalized.contains("senior-management")
        || normalized.contains("yonetim-kurulu")
        || normalized.contains("management")
        || normalized.contains("executive")
        || normalized.contains("team")
        || normalized.contains("yonetim")
    {
        return Some("leadership");
    }
    if normalized.contains("about")
        || normalized.contains("hakkimizda")
        || normalized.contains("kurumsal")
        || normalized.contains("company")
    {
        return Some("about");
    }
    if normalized.contains("contact") || normalized.contains("iletisim") {
        return Some("contact");
    }
    if normalized.contains("projects")
        || normalized.contains("project")
        || normalized.contains("projeler")
        || normalized.contains("proje")
    {
        return Some("projects");
    }
    if normalized.contains("services")
        || normalized.contains("service")
        || normalized.contains("solutions")
        || normalized.contains("industries")
        || normalized.contains("industry")
        || normalized.contains("sectors")
        || normalized.contains("hizmetler")
        || normalized.contains("hizmet")
        || normalized.contains("cozumler")
        || normalized.contains("sektorler")
    {
        return Some("operations");
    }
    None
}

fn osint_link_cluster_key(raw: &str) -> Option<String> {
    let canonical = canonicalize_osint_url(raw)?;
    let parsed = url::Url::parse(&canonical).ok()?;
    let host = parsed
        .host_str()
        .unwrap_or("")
        .trim_start_matches("www.")
        .to_lowercase();
    let path = parsed.path().to_lowercase();
    let normalized_path = path.trim_matches('/');

    if host.ends_with("tmb.org.tr") && (path.starts_with("/en/m/") || path.starts_with("/tr/m/")) {
        return Some("tmb_member_directory".to_string());
    }
    if host.ends_with("tmb.org.tr") && (path == "/en/members" || path == "/tr/uyeler") {
        return Some("tmb_member_directory".to_string());
    }
    if host.ends_with("mib.org.tr") && (path.contains("/firm/") || path.contains("/our-members")) {
        return Some("mib_member_directory".to_string());
    }
    if host.ends_with("imder.org.tr") && !normalized_path.is_empty() {
        return Some("imder_member_directory".to_string());
    }
    if host.ends_with("isder.org.tr") && !normalized_path.is_empty() {
        return Some("isder_member_directory".to_string());
    }
    if host.ends_with("asmud.org.tr") && path.contains("uyeler.asp") {
        return Some("asmud_member_directory".to_string());
    }
    if host.ends_with("platformder.org.tr") && path.contains("/rehber/") {
        return Some("platformder_directory".to_string());
    }
    if host.ends_with("thbb.org") && path.contains("yazismali-uyeler") {
        return Some("thbb_directory".to_string());
    }
    if host.ends_with("eder.org.tr")
        && (path.contains("/uyelerimiz") || path.contains("/our-members"))
    {
        return Some("eder_member_directory".to_string());
    }
    if host.ends_with("lojider.org.tr")
        && (path.contains("/member-list") || path.contains("/uye-listesi"))
    {
        return Some("lojider_member_directory".to_string());
    }
    if host.ends_with("tfyd.org.tr") && path.contains("/uyelerimiz") {
        return Some("tfyd_member_directory".to_string());
    }
    if host.ends_with("oss.org.tr") && (path.contains("/members") || path.contains("/uyeler")) {
        return Some("oss_member_directory".to_string());
    }
    if normalize_company_linkedin_url(&canonical).is_some() {
        return Some(format!("linkedin_company:{host}:{}", parsed.path()));
    }
    if normalize_outreach_linkedin_url(&canonical).is_some() {
        return Some(format!("linkedin_personal:{host}:{}", parsed.path()));
    }
    if let Some(category) = site_link_category(&path) {
        return Some(format!("site:{host}:{category}"));
    }
    None
}

fn merge_osint_links(existing: Vec<String>, extra: Vec<String>) -> Vec<String> {
    let mut merged = dedupe_strings(
        existing
            .into_iter()
            .chain(extra)
            .filter_map(|value| {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    return None;
                }
                Some(canonicalize_osint_url(&trimmed).unwrap_or(trimmed))
            })
            .filter(|value| !value.is_empty())
            .collect(),
    );
    merged.sort_by(|a, b| {
        osint_link_priority(b)
            .cmp(&osint_link_priority(a))
            .then_with(|| b.len().cmp(&a.len()))
            .then_with(|| a.cmp(b))
    });
    let mut filtered = Vec::with_capacity(merged.len());
    let mut seen_clusters = std::collections::HashSet::new();
    for link in merged {
        if let Some(cluster) = osint_link_cluster_key(&link) {
            if !seen_clusters.insert(cluster) {
                continue;
            }
        }
        filtered.push(link);
        if filtered.len() >= MAX_OSINT_LINKS_PER_PROSPECT {
            break;
        }
    }
    filtered
}

fn lead_has_outreach_channel(email: Option<&String>, linkedin_url: Option<&String>) -> bool {
    email
        .map(String::as_str)
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false)
        || linkedin_url
            .and_then(|value| normalize_outreach_linkedin_url(value))
            .is_some()
}

fn lead_has_verified_company_signal(
    is_field_ops: bool,
    site_evidence: Option<&str>,
    llm_validated: bool,
) -> bool {
    if !is_field_ops {
        return true;
    }
    llm_validated
        || site_evidence
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

fn extract_personal_linkedin_from_text(text: &str) -> Option<String> {
    let re = regex_lite::Regex::new(
        r"(?i)https?://(?:[a-z]{2,3}\.)?linkedin\.com/(?:in|pub)/[A-Za-z0-9%._/\-]+",
    )
    .unwrap();
    let decoded = text
        .replace("\\/", "/")
        .replace("\\u002F", "/")
        .replace("\\u002f", "/");
    for source in [text, decoded.as_str()] {
        if let Some(m) = re.find_iter(source).next() {
            let url = m
                .as_str()
                .trim_matches(|c: char| c == '"' || c == '\'' || c == ')' || c == ',' || c == '.')
                .to_string();
            return Some(url);
        }
    }
    None
}

fn extract_company_linkedin_from_text(text: &str) -> Option<String> {
    let re = regex_lite::Regex::new(
        r"(?i)https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/[A-Za-z0-9%._/\-]+",
    )
    .unwrap();
    let decoded = text
        .replace("\\/", "/")
        .replace("\\u002F", "/")
        .replace("\\u002f", "/");
    for source in [text, decoded.as_str()] {
        if let Some(m) = re.find(source) {
            return Some(
                m.as_str()
                    .trim_matches(|c: char| {
                        c == '"' || c == '\'' || c == ')' || c == ',' || c == '.'
                    })
                    .to_string(),
            );
        }
    }
    None
}

fn strip_html_tags(text: &str) -> String {
    let no_script = regex_lite::Regex::new(r"(?is)<script[^>]*>.*?</script>")
        .unwrap()
        .replace_all(text, " ");
    let no_style = regex_lite::Regex::new(r"(?is)<style[^>]*>.*?</style>")
        .unwrap()
        .replace_all(&no_script, " ");
    let no_tags = regex_lite::Regex::new(r"(?is)<[^>]+>")
        .unwrap()
        .replace_all(&no_style, " ");
    let decoded = decode_basic_html_entities(&decode_html_email_entities(&no_tags));
    truncate_cleaned_text(&decoded, 20_000)
}

fn html_to_structured_text(text: &str) -> String {
    let no_script = regex_lite::Regex::new(r"(?is)<script[^>]*>.*?</script>")
        .unwrap()
        .replace_all(text, " ");
    let no_style = regex_lite::Regex::new(r"(?is)<style[^>]*>.*?</style>")
        .unwrap()
        .replace_all(&no_script, " ");
    let with_breaks = regex_lite::Regex::new(
        r"(?is)</?(?:p|div|section|article|header|footer|aside|li|ul|ol|h[1-6]|strong|b|span|a|tr|td|th|br)[^>]*>",
    )
    .unwrap()
    .replace_all(&no_style, "\n");
    let no_tags = regex_lite::Regex::new(r"(?is)<[^>]+>")
        .unwrap()
        .replace_all(&with_breaks, " ");
    let decoded = decode_basic_html_entities(&decode_html_email_entities(&no_tags));
    decoded
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn enrich_link_priority(raw_href: &str) -> i32 {
    let path = if let Ok(parsed) = url::Url::parse(raw_href) {
        parsed.path().to_lowercase()
    } else {
        raw_href
            .split('?')
            .next()
            .unwrap_or(raw_href)
            .split('#')
            .next()
            .unwrap_or(raw_href)
            .to_lowercase()
    };
    let mut score = if path.contains("icra-kurulu")
        || path.contains("ust-yonetim")
        || path.contains("executive-team")
        || path.contains("leadership")
        || path.contains("senior-management")
    {
        16
    } else if path.contains("yonetim-kurulu")
        || path.contains("management")
        || path.contains("executive")
        || path.contains("team")
    {
        12
    } else if path.contains("yonetim") || path.contains("about") || path.contains("hakkimizda") {
        8
    } else if path.contains("contact") || path.contains("iletisim") {
        7
    } else {
        0
    };

    let generic_segments = [
        "about",
        "team",
        "leadership",
        "management",
        "executive",
        "executive-team",
        "senior-management",
        "contact",
        "hakkimizda",
        "kurumsal",
        "yonetim",
        "yonetim-kurulu",
        "icra-kurulu",
        "ust-yonetim",
        "iletisim",
    ];
    if let Some(last_segment) = path.trim_matches('/').rsplit('/').next() {
        let slug_token_count = last_segment
            .split('-')
            .filter(|part| !part.is_empty())
            .filter(|part| part.chars().all(|c| c.is_ascii_alphabetic()))
            .filter(|part| !generic_segments.contains(part))
            .count();
        if slug_token_count >= 2 {
            score += 6;
        }
    }

    if raw_href.contains('#') {
        score -= 5;
    }
    score
}

fn path_looks_like_enrich_target(path: &str) -> bool {
    let normalized = path.trim().to_lowercase();
    [
        "about",
        "team",
        "leadership",
        "management",
        "executive",
        "contact",
        "services",
        "service",
        "solutions",
        "industries",
        "industry",
        "sectors",
        "projects",
        "project",
        "engineering",
        "infrastructure",
        "maintenance",
        "facility",
        "construction",
        "installation",
        "field-service",
        "dispatch",
        "hakkimizda",
        "kurumsal",
        "ekip",
        "yonetim",
        "iletisim",
        "hizmetler",
        "hizmet",
        "cozumler",
        "sektorler",
        "bakim",
        "tesis",
        "muhendislik",
        "altyapi",
        "santiye",
        "insaat",
        "taahhut",
        "projeler",
    ]
    .iter()
    .any(|kw| normalized.contains(kw))
}

fn hosts_match_for_site_enrich(left: &str, right: &str) -> bool {
    let normalize = |host: &str| host.trim().trim_end_matches('.').to_lowercase();
    let left = normalize(left);
    let right = normalize(right);
    if left == right {
        return true;
    }

    let left_stripped = left.strip_prefix("www.").unwrap_or(&left);
    let right_stripped = right.strip_prefix("www.").unwrap_or(&right);
    left_stripped == right_stripped
}

fn extract_internal_enrich_links(base_url: &url::Url, html: &str) -> Vec<String> {
    let href_re = regex_lite::Regex::new(r#"(?is)href\s*=\s*["']([^"']+)["']"#).unwrap();

    let mut ranked = Vec::<(i32, String)>::new();
    let mut seen = HashSet::<String>::new();
    for cap in href_re.captures_iter(html) {
        let href = cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        if href.is_empty() {
            continue;
        }
        if href.starts_with("mailto:")
            || href.starts_with("javascript:")
            || href.starts_with('#')
            || href.contains("linkedin.com")
        {
            continue;
        }
        let abs = match base_url.join(href) {
            Ok(u) => u,
            Err(_) => continue,
        };
        let host_ok = abs
            .host_str()
            .zip(base_url.host_str())
            .map(|(left, right)| hosts_match_for_site_enrich(left, right))
            .unwrap_or(false);
        if !host_ok {
            continue;
        }
        let path = abs.path().to_lowercase();
        if !path_looks_like_enrich_target(&path) {
            continue;
        }
        let key = canonicalize_osint_url(abs.as_str()).unwrap_or_else(|| abs.as_str().to_string());
        if seen.insert(key.clone()) {
            ranked.push((enrich_link_priority(&key), key));
        }
    }
    ranked.sort_by(|(score_a, url_a), (score_b, url_b)| {
        score_b
            .cmp(score_a)
            .then_with(|| url_b.len().cmp(&url_a.len()))
            .then_with(|| url_a.cmp(url_b))
    });
    ranked
        .into_iter()
        .map(|(_, url)| url)
        .take(MAX_EXTRA_SITE_ENRICH_PAGES)
        .collect()
}

fn extract_sitemap_locations(xml: &str) -> Vec<String> {
    let loc_re = regex_lite::Regex::new(r"(?is)<loc>\s*([^<\s]+)\s*</loc>").unwrap();
    loc_re
        .captures_iter(xml)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().trim().to_string()))
        .collect()
}

async fn fetch_sitemap_enrich_links(client: &reqwest::Client, base_url: &url::Url) -> Vec<String> {
    let base_host = base_url.host_str().unwrap_or("").to_lowercase();
    let mut queue = Vec::<String>::new();
    let mut queued = HashSet::<String>::new();
    let mut out = Vec::<String>::new();
    let mut seen_pages = HashSet::<String>::new();
    let mut seen_sitemaps = HashSet::<String>::new();

    if let Ok(robots_url) = base_url.join("/robots.txt") {
        if let Ok(resp) = client.get(robots_url).send().await {
            if let Ok(body) = resp.text().await {
                for line in body.lines() {
                    let trimmed = line.trim();
                    if !trimmed.to_lowercase().starts_with("sitemap:") {
                        continue;
                    }
                    let sitemap = trimmed
                        .split_once(':')
                        .map(|(_, value)| value.trim().to_string())
                        .unwrap_or_default();
                    if !sitemap.is_empty() && queued.insert(sitemap.clone()) {
                        queue.push(sitemap);
                    }
                }
            }
        }
    }

    for fallback in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"] {
        if let Ok(url) = base_url.join(fallback) {
            let key = url.to_string();
            if queued.insert(key.clone()) {
                queue.push(key);
            }
        }
    }

    while let Some(sitemap_url) = queue.pop() {
        if !seen_sitemaps.insert(sitemap_url.clone()) || seen_sitemaps.len() > 6 {
            continue;
        }
        let resp = match client.get(&sitemap_url).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !resp.status().is_success() {
            continue;
        }
        let body = match resp.text().await {
            Ok(text) => text,
            Err(_) => continue,
        };
        if body.trim().is_empty() {
            continue;
        }

        for loc in extract_sitemap_locations(&body) {
            let Ok(parsed) = url::Url::parse(&loc) else {
                continue;
            };
            let same_host = parsed
                .host_str()
                .map(|host| hosts_match_for_site_enrich(host, &base_host))
                .unwrap_or(false);
            if !same_host {
                continue;
            }

            if loc.ends_with(".xml") || body.to_lowercase().contains("<sitemapindex") {
                if queued.insert(loc.clone()) {
                    queue.push(loc);
                }
                continue;
            }

            if !path_looks_like_enrich_target(parsed.path()) {
                continue;
            }

            let normalized = canonicalize_osint_url(&loc).unwrap_or_else(|| loc.trim().to_string());
            if seen_pages.insert(normalized.clone()) {
                out.push(normalized);
            }
            if out.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
                return out;
            }
        }
    }

    out
}

fn default_internal_enrich_links(base_url: &url::Url) -> Vec<String> {
    let defaults = [
        "/about",
        "/about-us",
        "/company",
        "/company/about",
        "/leadership",
        "/team",
        "/management",
        "/executive-team",
        "/contact",
        "/hakkimizda",
        "/Hakkinda/Yonetim",
        "/Hakkinda/Yonetim-Kurulu",
        "/kurumsal",
        "/kurumsal/yonetim",
        "/kurumsal/yonetim-kurulu",
        "/kurumsal/yonetim-kurulu-ve-ust-yonetim",
        "/kurumsal/ust-yonetim",
        "/hakkimizda/yonetim",
        "/hakkimizda/yonetim/yonetim-kurulu",
        "/hakkimizda/yonetim/icra-kurulu",
        "/tr/kurumsal/hakkimizda/yonetim",
        "/tr/kurumsal/hakkimizda/yonetim-kurulu",
        "/yonetim",
        "/iletisim",
        "/bize-ulasin",
        "/ekibimiz",
        "/referanslarimiz",
        "/projelerimiz",
        "/haberler",
        "/duyurular",
        "/en/management",
        "/en/team",
        "/en/contact",
    ];
    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for path in defaults {
        if let Ok(url) = base_url.join(path) {
            let key =
                canonicalize_osint_url(url.as_str()).unwrap_or_else(|| url.as_str().to_string());
            if seen.insert(key.clone()) {
                out.push(key);
            }
        }
        if out.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            break;
        }
    }
    out
}

async fn fetch_html_page(client: &reqwest::Client, url: &str, timeout_ms: u64) -> Option<String> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let resp = client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let ctype = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !ctype.is_empty() && !ctype.contains("text/html") && !ctype.contains("application/xhtml")
        {
            return None;
        }
        let body = repair_common_mojibake_utf8(&resp.text().await.ok()?);
        if body.trim().is_empty() {
            return None;
        }
        Some(body)
    })
    .await
    .ok()
    .flatten()
}

async fn fetch_company_site_html_pages(client: &reqwest::Client, domain: &str) -> SiteFetchBundle {
    let candidates = [
        format!("https://{domain}"),
        format!("https://www.{domain}"),
        format!("http://{domain}"),
    ];
    for url in candidates {
        let Some(body) = fetch_html_page(client, &url, SITE_PAGE_FETCH_TIMEOUT_MS + 400).await
        else {
            continue;
        };
        if body.trim().is_empty() {
            continue;
        }
        let mut pages = vec![SiteHtmlPage {
            url: url.clone(),
            html: body.clone(),
        }];
        let mut osint_links = vec![url.clone()];
        if let Ok(base) = url::Url::parse(&url) {
            let links = select_company_site_enrich_links(
                &base,
                &body,
                fetch_sitemap_enrich_links(client, &base).await,
            );
            osint_links = merge_osint_links(osint_links, links.clone());
            let fetches = links.into_iter().map(|link| async move {
                let html = fetch_html_page(client, &link, SITE_PAGE_FETCH_TIMEOUT_MS).await;
                (link, html)
            });
            for (link, extra) in join_all(fetches).await {
                if let Some(extra) = extra {
                    if !extra.trim().is_empty() {
                        pages.push(SiteHtmlPage {
                            url: link,
                            html: extra,
                        });
                    }
                }
            }
        }
        return SiteFetchBundle { pages, osint_links };
    }
    SiteFetchBundle::default()
}

fn select_company_site_enrich_links(
    base_url: &url::Url,
    html: &str,
    sitemap_links: Vec<String>,
) -> Vec<String> {
    let mut discovered = extract_internal_enrich_links(base_url, html);
    let mut seen = discovered.iter().cloned().collect::<HashSet<_>>();
    for sitemap_link in sitemap_links {
        if seen.insert(sitemap_link.clone()) {
            discovered.push(sitemap_link);
        }
    }

    discovered.sort_by(|a, b| {
        enrich_link_priority(b)
            .cmp(&enrich_link_priority(a))
            .then_with(|| b.len().cmp(&a.len()))
            .then_with(|| a.cmp(b))
    });

    let mut selected = Vec::<String>::new();
    let mut selected_set = HashSet::<String>::new();
    let mut selected_clusters = HashSet::<String>::new();
    for link in discovered {
        let cluster = osint_link_cluster_key(&link);
        if let Some(cluster_key) = cluster.as_ref() {
            if selected_clusters.contains(cluster_key) {
                continue;
            }
        }
        if selected_set.insert(link.clone()) {
            if let Some(cluster_key) = cluster {
                selected_clusters.insert(cluster_key);
            }
            selected.push(link);
        }
        if selected.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            return selected;
        }
    }

    for fallback in default_internal_enrich_links(base_url) {
        let cluster = osint_link_cluster_key(&fallback);
        if let Some(cluster_key) = cluster.as_ref() {
            if selected_clusters.contains(cluster_key) {
                continue;
            }
        }
        if selected_set.insert(fallback.clone()) {
            if let Some(cluster_key) = cluster {
                selected_clusters.insert(cluster_key);
            }
            selected.push(fallback);
        }
        if selected.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            break;
        }
    }

    selected
}

fn best_site_contact_enrichment(
    bundle: SiteFetchBundle,
    title_policy: &str,
) -> SiteContactEnrichment {
    let mut best = SiteContactEnrichment::default();
    let mut best_identity_signal = -1;
    let mut osint_links = bundle.osint_links.clone();
    let empty_headers = HashMap::new();
    let mut all_tech = Vec::new();

    for page in bundle.pages {
        osint_links.push(page.url.clone());
        // Tech stack detection (TASK-28)
        all_tech.extend(detect_tech_stack(&page.html, &empty_headers));
        if let Some(url) = extract_personal_linkedin_from_text(&page.html) {
            osint_links.push(url);
        }
        if let Some(url) = extract_company_linkedin_from_text(&page.html) {
            if best.company_linkedin_url.is_none() {
                best.company_linkedin_url = Some(url.clone());
            }
            osint_links.push(url);
        }
        let (name, title, linkedin_url, email, evidence) =
            extract_contact_from_company_site_html(&page.html, title_policy);
        let identity_signal =
            site_contact_identity_signal(name.as_ref(), title.as_ref(), linkedin_url.as_ref());
        if identity_signal > best_identity_signal {
            best_identity_signal = identity_signal;
            best.name = name;
            best.title = title;
            best.linkedin_url = linkedin_url;
        }
        if best.email.is_none() && email.is_some() {
            best.email = email;
        }
        if best.evidence.is_none() && evidence.is_some() {
            best.evidence = evidence;
        }
        best.signal = site_contact_candidate_signal(
            best.name.as_ref(),
            best.title.as_ref(),
            best.linkedin_url.as_ref(),
            best.email.as_ref(),
            best.evidence.as_ref(),
        );
        if best_identity_signal >= 20 && best.email.is_some() && best.evidence.is_some() {
            break;
        }
    }

    all_tech.sort();
    all_tech.dedup();
    best.tech_stack = all_tech;
    best.osint_links = merge_osint_links(Vec::new(), osint_links);
    best
}

async fn prefetch_site_contact_enrichments(
    client: &reqwest::Client,
    candidates: &[DomainCandidate],
    max_prefetch: usize,
    title_policy: &str,
) -> HashMap<String, SiteContactEnrichment> {
    let domains = dedupe_strings(
        candidates
            .iter()
            .map(|candidate| candidate.domain.clone())
            .take(max_prefetch)
            .collect(),
    );
    let title_policy = title_policy.to_string();

    join_all(domains.into_iter().map(|domain| {
        let title_policy = title_policy.clone();
        async move {
            let bundle = tokio::time::timeout(
                Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                fetch_company_site_html_pages(client, &domain),
            )
            .await
            .unwrap_or_default();
            Some((domain, best_site_contact_enrichment(bundle, &title_policy)))
        }
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
}

fn extract_contact_from_json_ld(
    html: &str,
    title_policy: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let script_re = regex_lite::Regex::new(
        r#"(?is)<script[^>]*type\s*=\s*["']application/ld\+json["'][^>]*>(.*?)</script>"#,
    )
    .unwrap();

    let name_job_re = regex_lite::Regex::new(
        r#"(?is)"name"\s*:\s*"([^"]{3,120})"[^{}]{0,320}"jobTitle"\s*:\s*"([^"]{2,80})""#,
    )
    .unwrap();
    let job_name_re = regex_lite::Regex::new(
        r#"(?is)"jobTitle"\s*:\s*"([^"]{2,80})"[^{}]{0,320}"name"\s*:\s*"([^"]{3,120})""#,
    )
    .unwrap();
    let founder_re = regex_lite::Regex::new(
        r#"(?is)"founder"\s*:\s*(?:\{[^{}]{0,400})?"name"\s*:\s*"([^"]{3,120})""#,
    )
    .unwrap();

    for cap in script_re.captures_iter(html) {
        let raw = cap
            .get(1)
            .map(|m| m.as_str())
            .unwrap_or("")
            .replace("\\\"", "\"");
        let normalized_raw = canonicalize_contact_titles(&raw);
        let jsonld_linkedin = extract_personal_linkedin_from_text(&normalized_raw)
            .or_else(|| extract_company_linkedin_from_text(&normalized_raw));
        let jsonld_email = extract_email_from_text(&normalized_raw);

        if let Some(c) = name_job_re.captures(&normalized_raw) {
            let name = c
                .get(1)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            let title = c.get(2).map(|m| normalize_contact_title(m.as_str()));
            if name.is_some()
                && title
                    .as_deref()
                    .map(|t| title_allowed_for_policy(title_policy, t))
                    .unwrap_or(false)
            {
                return (name, title, jsonld_linkedin, jsonld_email);
            }
        }

        if let Some(c) = job_name_re.captures(&normalized_raw) {
            let title = c.get(1).map(|m| normalize_contact_title(m.as_str()));
            let name = c
                .get(2)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            if name.is_some()
                && title
                    .as_deref()
                    .map(|t| title_allowed_for_policy(title_policy, t))
                    .unwrap_or(false)
            {
                return (name, title, jsonld_linkedin, jsonld_email);
            }
        }

        if let Some(c) = founder_re.captures(&normalized_raw) {
            let name = c
                .get(1)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            if name.is_some() && title_policy != "ceo_only" {
                return (
                    name,
                    Some("Founder".to_string()),
                    jsonld_linkedin,
                    jsonld_email,
                );
            }
        }

        if jsonld_linkedin.is_some() || jsonld_email.is_some() {
            return (None, None, jsonld_linkedin, jsonld_email);
        }
    }

    (None, None, None, None)
}

fn extract_contact_from_html_person_cards(
    html: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let decoded_html = decode_basic_html_entities(&decode_html_email_entities(html));
    let patterns = [
        r#"(?is)<strong[^>]*>\s*(?:<a[^>]*>)?\s*([^<]{3,120}?)\s*(?:</a>)?\s*</strong>\s*(?:<[^>]+>\s*){0,8}<span[^>]*>\s*([^<]{2,160}?)\s*</span>"#,
        r#"(?is)<h[1-6][^>]*>\s*(?:<a[^>]*>)?\s*([^<]{3,120}?)\s*(?:</a>)?\s*</h[1-6]>\s*(?:<[^>]+>\s*){0,8}<span[^>]*>\s*([^<]{2,160}?)\s*</span>"#,
        r#"(?is)<a[^>]*href\s*=\s*["'][^"']*(?:yonetim|ust-yonetim|leadership|management|executive)[^"']*["'][^>]*>\s*([^<]{3,120}?)\s*</a>\s*(?:<[^>]+>\s*){0,8}<(?:div|span|p)[^>]*>\s*([^<]{2,160}?)\s*</(?:div|span|p)>"#,
        r#"(?is)<button[^>]*accordion-button[^>]*>\s*<span>\s*([^<]{3,120}?)\s*[–-]\s*([^<]{2,160}?)\s*</span>\s*</button>"#,
    ];

    let mut best_score = -1;
    let mut best_name = None;
    let mut best_title = None;

    for pattern in patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        for cap in re.captures_iter(&decoded_html) {
            let raw_name = cap
                .get(1)
                .map(|m| m.as_str())
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let raw_title = cap
                .get(2)
                .map(|m| m.as_str())
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let Some(name) = normalize_person_name(&raw_name) else {
                continue;
            };
            let title = normalize_contact_title(&raw_title);
            let priority = contact_title_priority(&title);
            if priority <= 0 || !title_allowed_for_policy(title_policy, &title) {
                continue;
            }
            let score = 20 + priority;
            if score > best_score {
                best_score = score;
                best_name = Some(name);
                best_title = Some(title);
            }
        }
    }

    (best_name, best_title)
}

fn extract_contact_from_meta_tags(
    html: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let decoded_html = decode_basic_html_entities(&decode_html_email_entities(html));
    let title_patterns = [
        r#"(?is)<meta[^>]*property\s*=\s*["']og:title["'][^>]*content\s*=\s*["']([^"']{3,160})["']"#,
        r#"(?is)<title>\s*([^<]{3,160})\s*</title>"#,
    ];
    let description_patterns = [
        r#"(?is)<meta[^>]*name\s*=\s*["']description["'][^>]*content\s*=\s*["']([^"']{3,320})["']"#,
        r#"(?is)<meta[^>]*property\s*=\s*["']og:description["'][^>]*content\s*=\s*["']([^"']{3,320})["']"#,
    ];

    let mut title_text = None;
    for pattern in title_patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        if let Some(cap) = re.captures(&decoded_html) {
            title_text = cap.get(1).map(|m| {
                m.as_str()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .to_string()
            });
            if title_text.is_some() {
                break;
            }
        }
    }

    let mut description_text = None;
    for pattern in description_patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        if let Some(cap) = re.captures(&decoded_html) {
            description_text = cap.get(1).map(|m| {
                m.as_str()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .to_string()
            });
            if description_text.is_some() {
                break;
            }
        }
    }

    let candidate_name = title_text
        .as_deref()
        .and_then(|value| value.split(" - ").next())
        .and_then(normalize_person_name);
    let Some(name) = candidate_name else {
        return (None, None);
    };

    let normalized_description = description_text
        .as_deref()
        .map(canonicalize_contact_titles)
        .unwrap_or_default();
    let lower_description = normalized_description.to_lowercase();
    let inferred_title =
        if lower_description.contains("founder") || lower_description.contains("kurucu") {
            Some("Founder".to_string())
        } else if lower_description.contains("ceo")
            || lower_description.contains("chief executive")
            || lower_description.contains("genel müdür")
            || lower_description.contains("genel mudur")
        {
            Some("CEO".to_string())
        } else if lower_description.contains("chairman")
            || lower_description.contains("başkanı")
            || lower_description.contains("baskani")
        {
            Some("Chairman".to_string())
        } else {
            None
        }
        .filter(|title| title_allowed_for_policy(title_policy, title));

    (Some(name), inferred_title)
}

type SiteHtmlContactExtraction = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn extract_contact_from_company_site_html(
    html: &str,
    title_policy: &str,
) -> SiteHtmlContactExtraction {
    let plain = strip_html_tags(html);
    let structured = html_to_structured_text(html);
    let canonical_plain = canonicalize_contact_titles(&plain);
    let canonical_structured = canonicalize_contact_titles(&structured);
    let lower_plain = canonical_plain.to_lowercase();
    let personal_linkedin_url = extract_personal_linkedin_from_text(html)
        .or_else(|| extract_personal_linkedin_from_text(&canonical_structured))
        .or_else(|| extract_personal_linkedin_from_text(&canonical_plain));
    let company_linkedin_url = extract_company_linkedin_from_text(html)
        .or_else(|| extract_company_linkedin_from_text(&canonical_structured))
        .or_else(|| extract_company_linkedin_from_text(&canonical_plain));
    let linkedin_url = personal_linkedin_url
        .clone()
        .or_else(|| company_linkedin_url.clone());
    let mut contact_name = personal_linkedin_url
        .as_deref()
        .and_then(extract_name_from_linkedin_url)
        .and_then(|n| normalize_person_name(&n));
    let mut contact_title = default_contact_title(title_policy);

    let (jsonld_name, jsonld_title, jsonld_linkedin, jsonld_email) =
        extract_contact_from_json_ld(html, title_policy);
    let (meta_name, meta_title) = extract_contact_from_meta_tags(html, title_policy);
    let (card_name, card_title) = extract_contact_from_html_person_cards(html, title_policy);
    let (parenthesized_name, parenthesized_title) =
        extract_contact_from_parenthesized_site_text(&canonical_structured, title_policy);
    let (inline_name, inline_title) =
        extract_contact_from_inline_site_text(&canonical_structured, title_policy);
    let (structured_name, structured_title) =
        extract_contact_from_structured_site_text(&canonical_structured, title_policy);

    apply_better_site_identity_candidate(
        jsonld_name,
        jsonld_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        meta_name,
        meta_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        card_name,
        card_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        parenthesized_name,
        parenthesized_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        inline_name,
        inline_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        structured_name,
        structured_title,
        &mut contact_name,
        &mut contact_title,
    );
    let site_linkedin = linkedin_url.or(jsonld_linkedin);
    let mut site_email = normalize_site_contact_email(jsonld_email)
        .or_else(|| extract_email_from_text(&canonical_structured))
        .or_else(|| extract_email_from_text(&canonical_plain));

    if site_email.is_none() {
        site_email = extract_email_from_text(html);
    }
    let evidence = extract_company_site_signal(&lower_plain);

    (
        contact_name,
        contact_title,
        site_linkedin,
        site_email,
        evidence,
    )
}

fn guessed_email(contact_name: Option<&str>, domain: &str) -> Option<String> {
    guess_personal_email_patterns(contact_name, domain)
        .into_iter()
        .next()
}

/// Generate multiple candidate email patterns for a contact name + domain.
/// Each guess has confidence 0.3 (speculative). Caller should MX-verify domain.
fn guess_personal_email_patterns(contact_name: Option<&str>, domain: &str) -> Vec<String> {
    let name = match contact_name {
        Some(n) if !contact_name_is_placeholder(Some(n)) => n,
        _ => return Vec::new(),
    };
    let normalized = match normalize_person_name(name) {
        Some(n) => n,
        None => return Vec::new(),
    };
    let parts: Vec<&str> = normalized
        .split_whitespace()
        .filter(|p| p.chars().all(|c| c.is_alphabetic()))
        .collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Vec::new();
    }
    let first = transliterate_turkish_ascii(parts[0]);
    let last = transliterate_turkish_ascii(parts[parts.len() - 1]);
    let first_initial = first.chars().next().unwrap_or('x');

    let mut patterns = Vec::with_capacity(5);
    // Pattern 1: first.last@domain  (most common)
    patterns.push(format!("{first}.{last}@{domain}"));
    // Pattern 2: flast@domain
    patterns.push(format!("{first_initial}{last}@{domain}"));
    // Pattern 3: first@domain
    patterns.push(format!("{first}@{domain}"));
    // Pattern 4: f.last@domain
    patterns.push(format!("{first_initial}.{last}@{domain}"));
    // Pattern 5: firstlast@domain
    patterns.push(format!("{first}{last}@{domain}"));

    patterns.retain(|e| email_syntax_valid(e));
    patterns
}

fn lead_has_person_identity(contact_name: Option<&str>, linkedin_url: Option<&String>) -> bool {
    let has_real_name = contact_name
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false);
    has_real_name
        || linkedin_url
            .and_then(|value| normalize_outreach_linkedin_url(value))
            .is_some()
}

fn lead_score(linkedin: &Option<String>, email: &Option<String>) -> i32 {
    let mut s = 60;
    if linkedin.is_some() {
        s += 20;
    }
    if email.is_some() {
        s += 20;
    }
    s
}

fn engine_from_state(state: &AppState) -> Result<SalesEngine, String> {
    let engine = SalesEngine::new(&state.kernel.config.home_dir);
    engine.init()?;
    Ok(engine)
}

#[derive(Debug, Deserialize)]
pub struct SalesRejectRequest {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SalesProfileAutofillRequest {
    pub brief: String,
    #[serde(default)]
    pub persist: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SalesOnboardingBriefRequest {
    pub brief: String,
    #[serde(default)]
    pub persist: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SalesOnboardingStep {
    pub key: String,
    pub title: String,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SalesOnboardingStatusResponse {
    pub completed: bool,
    pub active_step: u8,
    pub steps: Vec<SalesOnboardingStep>,
    pub oauth_connected: bool,
    pub has_brief: bool,
    pub profile_ready: bool,
    pub first_run_ready: bool,
    pub brief: Option<String>,
    pub last_successful_run_id: Option<String>,
}

fn de_opt_u64_loose<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<serde_json::Value>::deserialize(deserializer)?;
    let parsed = match raw {
        None => None,
        Some(serde_json::Value::Number(n)) => n.as_u64().or_else(|| {
            n.as_i64()
                .and_then(|v| if v >= 0 { Some(v as u64) } else { None })
        }),
        Some(serde_json::Value::String(s)) => {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                t.parse::<u64>().ok()
            }
        }
        _ => None,
    };
    Ok(parsed)
}

#[derive(Debug, Default, Deserialize)]
struct SalesProfileDraft {
    #[serde(default)]
    product_name: Option<String>,
    #[serde(default)]
    product_description: Option<String>,
    #[serde(default)]
    target_industry: Option<String>,
    #[serde(default)]
    target_geo: Option<String>,
    #[serde(default)]
    sender_name: Option<String>,
    #[serde(default)]
    sender_email: Option<String>,
    #[serde(default)]
    sender_linkedin: Option<String>,
    #[serde(default)]
    target_title_policy: Option<String>,
    #[serde(default, deserialize_with = "de_opt_u64_loose")]
    daily_target: Option<u64>,
    #[serde(default, deserialize_with = "de_opt_u64_loose")]
    daily_send_cap: Option<u64>,
    #[serde(default, deserialize_with = "de_opt_u64_loose")]
    schedule_hour_local: Option<u64>,
    #[serde(default)]
    timezone_mode: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LeadQueryPlanDraft {
    #[serde(default)]
    discovery_queries: Vec<String>,
    #[serde(default)]
    must_include_keywords: Vec<String>,
    #[serde(default)]
    exclude_keywords: Vec<String>,
    #[serde(default)]
    contact_titles: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LlmCompanyCandidate {
    #[serde(default)]
    company: Option<String>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LlmCompanyCandidateResponse {
    #[serde(default)]
    companies: Vec<LlmCompanyCandidate>,
}

fn cleaned_opt(v: Option<String>) -> Option<String> {
    v.and_then(|s| {
        let t = s.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    })
}

fn normalize_sales_profile(profile: SalesProfile) -> Result<SalesProfile, String> {
    let mut normalized = profile;
    normalized.product_name = normalized.product_name.trim().to_string();
    normalized.product_description = normalized.product_description.trim().to_string();
    normalized.target_industry = normalized.target_industry.trim().to_string();
    normalized.target_geo = normalized.target_geo.trim().to_uppercase();
    normalized.sender_name = normalized.sender_name.trim().to_string();
    normalized.sender_email = normalized.sender_email.trim().to_string();
    normalized.sender_linkedin = cleaned_opt(normalized.sender_linkedin);
    normalized.target_title_policy = match normalized.target_title_policy.trim() {
        "ceo_only" => "ceo_only".to_string(),
        _ => "ceo_then_founder".to_string(),
    };
    normalized.daily_target = normalized.daily_target.clamp(1, 200);
    normalized.daily_send_cap = normalized.daily_send_cap.clamp(1, 200);
    normalized.schedule_hour_local = normalized.schedule_hour_local.min(23);
    normalized.timezone_mode = match normalized.timezone_mode.trim().to_lowercase().as_str() {
        "utc" => "utc".to_string(),
        _ => "local".to_string(),
    };

    if normalized.product_name.is_empty() {
        return Err("product_name is required".to_string());
    }
    if normalized.product_description.is_empty() {
        return Err("product_description is required".to_string());
    }
    if normalized.target_industry.is_empty() {
        return Err("target_industry is required".to_string());
    }
    if normalized.target_geo.is_empty() {
        return Err("target_geo is required".to_string());
    }
    if normalized.sender_name.is_empty() {
        return Err("sender_name is required".to_string());
    }
    if normalized.sender_email.is_empty() {
        return Err("sender_email is required".to_string());
    }
    normalized
        .sender_email
        .parse::<Mailbox>()
        .map_err(|e| format!("sender_email is invalid: {e}"))?;

    Ok(normalized)
}

fn current_sales_day(timezone_mode: &str) -> chrono::NaiveDate {
    if timezone_mode.trim().eq_ignore_ascii_case("utc") {
        Utc::now().date_naive()
    } else {
        Local::now().date_naive()
    }
}

fn timestamp_matches_sales_day(value: &str, day: chrono::NaiveDate, timezone_mode: &str) -> bool {
    let fallback_day = day.format("%Y-%m-%d").to_string();
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| {
            if timezone_mode.trim().eq_ignore_ascii_case("utc") {
                dt.with_timezone(&Utc).date_naive() == day
            } else {
                dt.with_timezone(&Local).date_naive() == day
            }
        })
        .unwrap_or_else(|_| value.get(..10).map(|v| v == fallback_day).unwrap_or(false))
}

fn is_profile_ready_for_outbound(profile: &SalesProfile) -> bool {
    !profile.product_name.trim().is_empty()
        && !profile.product_description.trim().is_empty()
        && !profile.target_industry.trim().is_empty()
        && !profile.target_geo.trim().is_empty()
        && !profile.sender_name.trim().is_empty()
        && !profile.sender_email.trim().is_empty()
}

fn is_codex_oauth_connected(home_dir: &FsPath) -> bool {
    if std::env::var("OPENAI_CODEX_ACCESS_TOKEN")
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
    {
        return true;
    }
    let path = home_dir.join("auth").join("codex_oauth.json");
    let raw = match std::fs::read_to_string(path) {
        Ok(v) => v,
        Err(_) => return false,
    };
    serde_json::from_str::<StoredCodexAuth>(&raw)
        .map(|auth| !auth.access_token.trim().is_empty())
        .unwrap_or(false)
}

fn onboarding_active_step(steps: &[SalesOnboardingStep]) -> u8 {
    for (idx, step) in steps.iter().enumerate() {
        if !step.done {
            return (idx + 1) as u8;
        }
    }
    steps.len().max(1) as u8
}

fn build_onboarding_status(
    home_dir: &FsPath,
    profile: Option<SalesProfile>,
    brief: Option<String>,
    last_successful_run_id: Option<String>,
) -> SalesOnboardingStatusResponse {
    let oauth_connected = is_codex_oauth_connected(home_dir);
    let has_brief = brief
        .as_ref()
        .map(|b| b.trim().chars().count() >= 20)
        .unwrap_or(false);
    let profile_ready = profile
        .as_ref()
        .map(is_profile_ready_for_outbound)
        .unwrap_or(false);
    let first_run_ready = has_brief && last_successful_run_id.is_some();
    let scoped_last_run_id = if has_brief {
        last_successful_run_id
    } else {
        None
    };

    let steps = vec![
        SalesOnboardingStep {
            key: "oauth".to_string(),
            title: "Codex OAuth bagla".to_string(),
            done: oauth_connected,
        },
        SalesOnboardingStep {
            key: "brief".to_string(),
            title: "Sirket briefini gir".to_string(),
            done: has_brief,
        },
        SalesOnboardingStep {
            key: "profile".to_string(),
            title: "Profili dogrula ve kaydet".to_string(),
            done: profile_ready,
        },
        SalesOnboardingStep {
            key: "first_run".to_string(),
            title: "Ilk prospecting run'ini tamamla".to_string(),
            done: first_run_ready,
        },
    ];

    let completed = steps.iter().all(|s| s.done);
    SalesOnboardingStatusResponse {
        completed,
        active_step: onboarding_active_step(&steps),
        steps,
        oauth_connected,
        has_brief,
        profile_ready,
        first_run_ready,
        brief,
        last_successful_run_id: scoped_last_run_id,
    }
}

async fn apply_brief_to_profile(
    state: &AppState,
    engine: &SalesEngine,
    brief: &str,
    persist: bool,
) -> Result<(SalesProfile, &'static str, Vec<String>), String> {
    let base = match engine.get_profile() {
        Ok(Some(p)) => p,
        Ok(None) => SalesProfile::default(),
        Err(e) => return Err(e),
    };
    let mut warnings = Vec::<String>::new();
    let (profile, source) = match llm_autofill_profile(state, brief).await {
        Ok(draft) => (merge_profile(base, draft, brief), "llm"),
        Err(e) => {
            warnings.push(e);
            (heuristic_profile_from_brief(base, brief), "heuristic")
        }
    };

    if persist {
        engine.upsert_profile(&profile)?;
    }
    Ok((profile, source, warnings))
}

fn extract_json_payload(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.starts_with('{')
        && text.ends_with('}')
        && serde_json::from_str::<serde_json::Value>(text).is_ok()
    {
        return Some(text.to_string());
    }

    for (idx, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        let candidate = &text[idx..];
        let mut de = serde_json::Deserializer::from_str(candidate);
        let parsed = match serde_json::Value::deserialize(&mut de) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if parsed.is_object() {
            return serde_json::to_string(&parsed).ok();
        }
    }
    None
}

fn detect_industry(brief: &str) -> Option<String> {
    let b = brief.to_lowercase();
    let map = [
        ("saha operasyon", "Field Operations"),
        ("field operation", "Field Operations"),
        ("field service", "Field Services"),
        ("servis ekip", "Field Services"),
        ("yerinde operasyon", "Field Operations"),
        ("proje yönet", "Project & Program Management"),
        ("project management", "Project & Program Management"),
        ("inşaat", "Construction"),
        ("construction", "Construction"),
        ("tesis yönet", "Facility Management"),
        ("facility", "Facility Management"),
        ("bakım", "Maintenance Services"),
        ("maintenance", "Maintenance Services"),
        ("enerji", "Energy"),
        ("energy", "Energy"),
        ("lojistik", "Logistics"),
        ("supply chain", "Logistics"),
        ("logistics", "Logistics"),
        ("telekom", "Telecommunications"),
        ("telecom", "Telecommunications"),
        ("cyber", "Cybersecurity"),
        ("security", "Cybersecurity"),
        ("fintech", "Fintech"),
        ("bank", "Financial Services"),
        ("e-ticaret", "E-commerce"),
        ("eticaret", "E-commerce"),
        ("e-commerce", "E-commerce"),
        ("ecommerce", "E-commerce"),
        ("otomotiv", "Automotive"),
        ("automotive", "Automotive"),
        ("yedek parça", "Automotive"),
        ("yedek parca", "Automotive"),
        ("fuarcılık", "Events & Exhibitions"),
        ("fuarcilik", "Events & Exhibitions"),
        ("fuar", "Events & Exhibitions"),
        ("kongre", "Events & Exhibitions"),
        ("exhibition", "Events & Exhibitions"),
        ("events", "Events & Exhibitions"),
        ("health", "Healthcare"),
        ("saas", "SaaS"),
        ("education", "Education"),
        ("logistics", "Logistics"),
        ("manufacturing", "Manufacturing"),
        ("real estate", "Real Estate"),
    ];
    for (needle, value) in map {
        if b.contains(needle) {
            return Some(value.to_string());
        }
    }
    None
}

fn detect_geo(brief: &str) -> Option<String> {
    let b = brief.to_lowercase();
    if b.contains("türkiye")
        || b.contains("turkiye")
        || b.contains("istanbul")
        || b.contains("ankara")
        || b.contains("izmir")
    {
        return Some("TR".to_string());
    }
    if b.contains("europe") || b.contains("avrupa") {
        return Some("EU".to_string());
    }
    if b.contains("usa") || b.contains("united states") || b.contains("north america") {
        return Some("US".to_string());
    }
    let tr_keyword_hits = [
        "saha", "takim", "ekip", "proje", "yonetim", "fiyat", "kurulum", "gonderim", "toplanti",
        "sirket",
    ]
    .iter()
    .filter(|kw| b.contains(**kw))
    .count();
    if tr_keyword_hits >= 3 {
        return Some("TR".to_string());
    }
    None
}

fn infer_product_name(brief: &str) -> Option<String> {
    let domain_name = regex_lite::Regex::new(r"(?i)\b([a-z0-9][a-z0-9-]{2,30})\.(ai|com|io|co)\b")
        .ok()
        .and_then(|re| re.captures(brief))
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()));

    let label_name = regex_lite::Regex::new(
        r"(?m)^\s*Yeni Takım Arkadaşınız:\s*\n?\s*([A-Z][A-Za-z0-9_-]{2,40})\s*$",
    )
    .ok()
    .and_then(|re| re.captures(brief))
    .and_then(|cap| cap.get(1).map(|m| m.as_str().trim().to_string()));

    label_name.or(domain_name).map(|name| {
        let mut chars = name.chars();
        match chars.next() {
            Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str()),
            None => name,
        }
    })
}

fn brief_summary(brief: &str, max_len: usize) -> String {
    let single_line = brief
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    truncate_cleaned_text(&single_line, max_len)
}

fn sanitize_profile_description(candidate: &str, brief: &str) -> String {
    let mut text = truncate_cleaned_text(candidate, 450);
    if text.ends_with(',') || text.ends_with(';') || text.ends_with(':') {
        text = text
            .trim_end_matches([',', ';', ':', ' '])
            .trim()
            .to_string();
    }
    if text.chars().count() < 40 {
        text = brief_summary(brief, 320);
    }
    if text.is_empty() {
        text = "AI-based operations coordination for project teams.".to_string();
    }
    text
}

fn merge_profile(base: SalesProfile, draft: SalesProfileDraft, brief: &str) -> SalesProfile {
    let mut p = base;

    if let Some(v) = cleaned_opt(draft.product_name) {
        p.product_name = v;
    }
    if let Some(v) = cleaned_opt(draft.product_description) {
        p.product_description = sanitize_profile_description(&v, brief);
    } else if p.product_description.trim().is_empty() {
        p.product_description = sanitize_profile_description(brief.trim(), brief);
    } else {
        p.product_description = sanitize_profile_description(&p.product_description, brief);
    }
    if let Some(v) = cleaned_opt(draft.target_industry) {
        p.target_industry = v;
    } else if p.target_industry.trim().is_empty() {
        p.target_industry = detect_industry(brief).unwrap_or_else(|| "Technology".to_string());
    }
    if p.target_industry.eq_ignore_ascii_case("technology")
        || p.target_industry.eq_ignore_ascii_case("tech")
    {
        if let Some(specific) = detect_industry(brief) {
            if !specific.eq_ignore_ascii_case("technology") {
                p.target_industry = specific;
            }
        }
    }
    if let Some(v) = cleaned_opt(draft.target_geo) {
        p.target_geo = v;
    } else if p.target_geo.trim().is_empty() {
        p.target_geo = detect_geo(brief).unwrap_or_default();
    }
    if let Some(v) = cleaned_opt(draft.sender_name) {
        p.sender_name = v;
    }
    if let Some(v) = cleaned_opt(draft.sender_email) {
        p.sender_email = v;
    }

    p.sender_linkedin = cleaned_opt(draft.sender_linkedin).or(p.sender_linkedin);

    if let Some(v) = cleaned_opt(draft.target_title_policy) {
        p.target_title_policy = if v == "ceo_only" {
            "ceo_only".to_string()
        } else {
            "ceo_then_founder".to_string()
        };
    } else if p.target_title_policy != "ceo_only" && p.target_title_policy != "ceo_then_founder" {
        p.target_title_policy = "ceo_then_founder".to_string();
    }

    if let Some(v) = draft.daily_target {
        p.daily_target = (v as u32).clamp(1, 200);
    } else {
        p.daily_target = p.daily_target.clamp(1, 200);
    }

    if let Some(v) = draft.daily_send_cap {
        p.daily_send_cap = (v as u32).clamp(1, 200);
    } else {
        p.daily_send_cap = p.daily_send_cap.clamp(1, 200);
    }

    if let Some(v) = draft.schedule_hour_local {
        p.schedule_hour_local = (v as u8).min(23);
    } else {
        p.schedule_hour_local = p.schedule_hour_local.min(23);
    }

    if let Some(v) = cleaned_opt(draft.timezone_mode) {
        p.timezone_mode = v;
    } else if p.timezone_mode.trim().is_empty() {
        p.timezone_mode = "local".to_string();
    }

    if p.product_name.trim().is_empty() {
        p.product_name = infer_product_name(brief).unwrap_or_else(|| "My Product".to_string());
    }
    if p.sender_name.trim().is_empty() {
        p.sender_name = format!("{} Team", p.product_name);
    }
    if p.sender_email.trim().is_empty() {
        p.sender_email = "founder@example.com".to_string();
    }

    p
}

fn heuristic_profile_from_brief(base: SalesProfile, brief: &str) -> SalesProfile {
    let email = regex_lite::Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        .ok()
        .and_then(|re| re.find(brief).map(|m| m.as_str().to_string()));
    let linkedin = regex_lite::Regex::new(r"https?://[^\s]+linkedin\.com/[^\s]+")
        .ok()
        .and_then(|re| re.find(brief).map(|m| m.as_str().to_string()));
    let product_name = infer_product_name(brief);
    let description = brief_summary(brief, 500);
    let sender_name = product_name
        .as_ref()
        .map(|n| format!("{n} Team"))
        .or_else(|| Some("Sales Team".to_string()));

    let draft = SalesProfileDraft {
        product_name,
        product_description: Some(description),
        target_industry: detect_industry(brief),
        target_geo: detect_geo(brief),
        sender_name,
        sender_email: email,
        sender_linkedin: linkedin,
        target_title_policy: Some("ceo_then_founder".to_string()),
        daily_target: Some(20),
        daily_send_cap: Some(20),
        schedule_hour_local: Some(9),
        timezone_mode: Some("local".to_string()),
    };

    merge_profile(base, draft, brief)
}

fn profile_keyword_seed_text(profile: &SalesProfile) -> String {
    format!(
        "{} {} {}",
        profile.target_industry, profile.product_name, profile.product_description
    )
    .to_lowercase()
}

fn profile_targets_field_ops(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed.contains("saha")
        || seed.contains("field")
        || seed.contains("operasyon")
        || seed.contains("operations")
        || seed.contains("maintenance")
        || seed.contains("construction")
        || seed.contains("facility")
        || seed.contains("dispatch")
        || seed.contains("on-site")
}

fn field_ops_signal_keywords() -> &'static [&'static str] {
    &[
        "field service",
        "field operations",
        "on-site",
        "onsite",
        "dispatch",
        "maintenance",
        "repair",
        "facility",
        "facility management",
        "equipment rental",
        "field equipment",
        "industrial equipment",
        "platform rental",
        "access platform",
        "personnel lift",
        "forklift",
        "lift",
        "crane",
        "vinç",
        "vinc",
        "mewp",
        "construction",
        "installation",
        "technical service",
        "service team",
        "mobile workforce",
        "infrastructure",
        "utility",
        "saha servis",
        "saha operasyon",
        "saha ekibi",
        "bakım",
        "bakim",
        "onarım",
        "onarim",
        "kurulum",
        "teknik servis",
        "tesis yönetimi",
        "tesis yonetimi",
        "mekanik tesisat",
        "altyapı",
        "altyapi",
        "şantiye",
        "santiye",
        "inşaat",
        "insaat",
        "taahhüt",
        "taahhut",
        "mühendislik",
        "muhendislik",
    ]
}

fn extract_field_ops_signal_keyword(text: &str) -> Option<&'static str> {
    let t = text.to_lowercase();
    field_ops_signal_keywords()
        .iter()
        .find(|kw| t.contains(**kw))
        .copied()
}

fn text_has_field_ops_signal(text: &str) -> bool {
    extract_field_ops_signal_keyword(text).is_some()
        || text.to_lowercase().contains("service operations")
}

#[cfg(test)]
fn candidate_has_field_ops_signal(candidate: &DomainCandidate) -> bool {
    let keyword_signal = candidate.matched_keywords.iter().any(|kw| {
        let t = kw.trim().to_lowercase();
        t != "field operations"
            && t != "field operation"
            && t != "operations"
            && text_has_field_ops_signal(&t)
    });
    keyword_signal
        || candidate
            .evidence
            .iter()
            .any(|line| text_has_field_ops_signal(line))
}

fn geo_is_turkey(geo: &str) -> bool {
    let normalized = geo.trim().to_lowercase();
    normalized.contains("tr")
        || normalized.contains("turkiye")
        || normalized.contains("türkiye")
        || normalized.contains("turkey")
}

fn extract_company_site_signal(text: &str) -> Option<String> {
    extract_field_ops_signal_keyword(text).map(|kw| format!("Company website mentions '{}'", kw))
}

fn contact_title_priority(title: &str) -> i32 {
    if !contact_title_looks_plausible(title) {
        return 0;
    }
    let t = canonicalize_contact_titles(title).to_lowercase();
    if title_looks_like_operations_exec(title) {
        14
    } else if t.contains("ceo")
        || t.contains("chief executive")
        || t.contains("founder")
        || t.contains("genel müd")
        || t.contains("genel mud")
        || t.contains("managing director")
    {
        12
    } else if t.contains("chairman")
        || t.contains("board chair")
        || t.contains("başkan")
        || t.contains("baskan")
    {
        7
    } else if t.contains("cfo")
        || t.contains("chief financial")
        || t.contains("finance director")
        || t.contains("finance head")
        || t.contains("finans direkt")
        || t.contains("finans mud")
        || t.contains("finans müd")
        || t.contains("mali işler")
        || t.contains("mali isler")
    {
        4
    } else {
        0
    }
}

fn site_contact_candidate_signal(
    name: Option<&String>,
    title: Option<&String>,
    linkedin_url: Option<&String>,
    email: Option<&String>,
    evidence: Option<&String>,
) -> i32 {
    let mut score = 0;
    let has_real_name = name
        .map(|value| !contact_name_is_placeholder(Some(value.as_str())))
        .unwrap_or(false);
    if has_real_name {
        score += 10;
    }
    if let Some(title) = title {
        let normalized = normalize_contact_title(title);
        if normalized != "CEO/Founder" {
            score += contact_title_priority(&normalized);
        }
    }
    if let Some(linkedin_url) = linkedin_url {
        score += if normalize_outreach_linkedin_url(linkedin_url).is_some() {
            6
        } else {
            1
        };
    }
    if email.is_some() {
        score += 4;
    }
    if evidence.is_some() {
        score += 3;
    }
    if has_real_name && title.is_some() {
        score += 3;
    }
    score
}

fn site_contact_identity_signal(
    name: Option<&String>,
    title: Option<&String>,
    linkedin_url: Option<&String>,
) -> i32 {
    site_contact_candidate_signal(name, title, linkedin_url, None, None)
}

#[allow(clippy::too_many_arguments)]
fn apply_site_contact_enrichment(
    domain: &str,
    enrichment: &SiteContactEnrichment,
    contact_name: &mut Option<String>,
    contact_title: &mut Option<String>,
    linkedin_url: &mut Option<String>,
    company_linkedin_url: &mut Option<String>,
    email: &mut Option<String>,
    osint_links: &mut Vec<String>,
    email_from_verified_site: &mut bool,
    site_evidence: &mut Option<String>,
) {
    let current_name_is_placeholder = contact_name_is_placeholder(contact_name.as_deref());
    let enrichment_has_real_name = enrichment
        .name
        .as_deref()
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false);
    let current_candidate_signal = site_contact_candidate_signal(
        contact_name.as_ref(),
        contact_title.as_ref(),
        linkedin_url.as_ref(),
        email.as_ref(),
        site_evidence.as_ref(),
    );
    let prefer_site_candidate = enrichment.signal > current_candidate_signal
        || (enrichment.name.is_some()
            && enrichment.signal == current_candidate_signal
            && enrichment.signal > 0)
        || (current_name_is_placeholder && enrichment_has_real_name && enrichment.signal > 0);

    if prefer_site_candidate {
        if enrichment.name.is_some() {
            *contact_name = enrichment.name.clone();
        }
        if enrichment.title.is_some() {
            *contact_title = enrichment.title.clone();
        }
        if enrichment.linkedin_url.is_some() {
            *linkedin_url = enrichment.linkedin_url.clone();
        }
    }
    if company_linkedin_url.is_none() {
        *company_linkedin_url = enrichment.company_linkedin_url.clone();
    }

    let normalized_site_email =
        normalize_contact_email_for_domain(enrichment.email.clone(), domain)
            .or_else(|| normalize_site_contact_email(enrichment.email.clone()));
    if normalized_site_email.is_some() && (email.is_none() || prefer_site_candidate) {
        *email_from_verified_site = true;
        *email = normalized_site_email.or_else(|| guessed_email(contact_name.as_deref(), domain));
    }
    if enrichment.evidence.is_some() {
        *site_evidence = enrichment.evidence.clone();
    }
    *osint_links = merge_osint_links(osint_links.clone(), enrichment.osint_links.clone());
}

fn apply_search_contact_enrichment(
    enrichment: &SiteContactEnrichment,
    contact_name: &mut Option<String>,
    contact_title: &mut Option<String>,
    linkedin_url: &mut Option<String>,
    company_linkedin_url: &mut Option<String>,
    osint_links: &mut Vec<String>,
) {
    let current_name_is_placeholder = contact_name_is_placeholder(contact_name.as_deref());
    let enrichment_has_real_name = enrichment
        .name
        .as_deref()
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false);
    let current_candidate_signal = site_contact_identity_signal(
        contact_name.as_ref(),
        contact_title.as_ref(),
        linkedin_url.as_ref(),
    );
    let enrichment_candidate_signal = site_contact_identity_signal(
        enrichment.name.as_ref(),
        enrichment.title.as_ref(),
        enrichment.linkedin_url.as_ref(),
    ) + i32::from(enrichment.company_linkedin_url.is_some()) * 3;
    let prefer_search_candidate = enrichment_candidate_signal > current_candidate_signal
        || (current_name_is_placeholder
            && enrichment_has_real_name
            && enrichment_candidate_signal > 0);

    if prefer_search_candidate {
        if enrichment.name.is_some() {
            *contact_name = enrichment.name.clone();
        }
        if enrichment.title.is_some() {
            *contact_title = enrichment.title.clone();
        }
        if enrichment.linkedin_url.is_some() {
            *linkedin_url = enrichment.linkedin_url.clone();
        }
    } else if linkedin_url.is_none() && enrichment.linkedin_url.is_some() {
        *linkedin_url = enrichment.linkedin_url.clone();
    }

    if company_linkedin_url.is_none() {
        *company_linkedin_url = enrichment.company_linkedin_url.clone();
    }
    *osint_links = merge_osint_links(osint_links.clone(), enrichment.osint_links.clone());
}

fn apply_site_osint_to_profile(
    profile: &mut SalesProspectProfile,
    enrichment: &SiteContactEnrichment,
) {
    let mut contact_name = profile.primary_contact_name.clone();
    let mut contact_title = profile.primary_contact_title.clone();
    let mut linkedin_url = profile.primary_linkedin_url.clone();
    let mut company_linkedin_url = profile.company_linkedin_url.clone();
    let mut email = profile.primary_email.clone();
    let mut osint_links = profile.osint_links.clone();
    let mut email_from_verified_site = false;
    let mut site_evidence = profile
        .matched_signals
        .iter()
        .find(|value| value.contains("website") || value.contains("signal"))
        .cloned();

    apply_site_contact_enrichment(
        &profile.company_domain,
        enrichment,
        &mut contact_name,
        &mut contact_title,
        &mut linkedin_url,
        &mut company_linkedin_url,
        &mut email,
        &mut osint_links,
        &mut email_from_verified_site,
        &mut site_evidence,
    );

    profile.primary_contact_name = contact_name.and_then(|value| normalize_person_name(&value));
    profile.primary_contact_title = contact_title;
    profile.primary_linkedin_url =
        linkedin_url.and_then(|value| normalize_outreach_linkedin_url(&value));
    profile.company_linkedin_url =
        company_linkedin_url.and_then(|value| normalize_company_linkedin_url(&value));
    profile.primary_email = if email_from_verified_site {
        normalize_site_contact_email(email)
    } else {
        normalize_contact_email_for_domain(email, &profile.company_domain)
    };
    profile.osint_links = merge_osint_links(
        osint_links,
        vec![
            profile.website.clone(),
            profile.primary_linkedin_url.clone().unwrap_or_default(),
            profile.company_linkedin_url.clone().unwrap_or_default(),
        ],
    );
    profile.profile_status = prospect_status(
        profile.primary_contact_name.as_deref(),
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    )
    .to_string();
    profile.recommended_channel = build_recommended_channel(
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    );
    if profile.research_status != "llm_enriched" || profile.summary.trim().is_empty() {
        profile.summary = build_prospect_summary(
            &profile.company,
            &profile.matched_signals,
            profile.primary_contact_name.as_deref(),
            profile.primary_contact_title.as_deref(),
            profile.primary_email.as_deref(),
            profile.primary_linkedin_url.as_deref(),
        );
    }
    profile.research_confidence = profile
        .research_confidence
        .max(heuristic_research_confidence(
            profile.fit_score,
            &profile.profile_status,
            profile.source_count as usize,
            profile.contact_count as usize,
        ));
    // Merge tech stack from site enrichment (TASK-28)
    if !enrichment.tech_stack.is_empty() {
        let mut stack = profile.tech_stack.clone();
        stack.extend(enrichment.tech_stack.iter().cloned());
        stack.sort();
        stack.dedup();
        profile.tech_stack = stack;
    }
}

fn apply_search_osint_to_profile(
    profile: &mut SalesProspectProfile,
    enrichment: &SiteContactEnrichment,
) {
    let mut contact_name = profile.primary_contact_name.clone();
    let mut contact_title = profile.primary_contact_title.clone();
    let mut linkedin_url = profile.primary_linkedin_url.clone();
    let mut company_linkedin_url = profile.company_linkedin_url.clone();
    let mut osint_links = profile.osint_links.clone();

    apply_search_contact_enrichment(
        enrichment,
        &mut contact_name,
        &mut contact_title,
        &mut linkedin_url,
        &mut company_linkedin_url,
        &mut osint_links,
    );

    profile.primary_contact_name = contact_name.and_then(|value| normalize_person_name(&value));
    profile.primary_contact_title = contact_title;
    profile.primary_linkedin_url =
        linkedin_url.and_then(|value| normalize_outreach_linkedin_url(&value));
    profile.company_linkedin_url =
        company_linkedin_url.and_then(|value| normalize_company_linkedin_url(&value));
    profile.osint_links = merge_osint_links(
        osint_links,
        vec![
            profile.website.clone(),
            profile.primary_linkedin_url.clone().unwrap_or_default(),
            profile.company_linkedin_url.clone().unwrap_or_default(),
        ],
    );
    profile.profile_status = prospect_status(
        profile.primary_contact_name.as_deref(),
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    )
    .to_string();
    profile.recommended_channel = build_recommended_channel(
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    );
    if profile.research_status != "llm_enriched" || profile.summary.trim().is_empty() {
        profile.summary = build_prospect_summary(
            &profile.company,
            &profile.matched_signals,
            profile.primary_contact_name.as_deref(),
            profile.primary_contact_title.as_deref(),
            profile.primary_email.as_deref(),
            profile.primary_linkedin_url.as_deref(),
        );
    }
    profile.research_confidence = profile
        .research_confidence
        .max(heuristic_research_confidence(
            profile.fit_score,
            &profile.profile_status,
            profile.source_count as usize,
            profile.contact_count as usize,
        ));
    // Detect job posting intent from OSINT links (TASK-27)
    let job_signals = detect_job_posting_intent_from_urls(&profile.osint_links);
    for signal in job_signals {
        if !profile.trigger_events.contains(&signal) {
            profile.trigger_events.push(signal);
        }
    }
    // Merge active job posting search signals (TASK-27 search)
    for (text, url, _confidence, _sig_type) in &enrichment.job_posting_signals {
        let signal_text = format!("Job posting: {} ({})", text, url);
        if !profile.trigger_events.contains(&signal_text) {
            profile.trigger_events.push(signal_text);
        }
    }
    // Merge tech stack from search enrichment (TASK-28)
    if !enrichment.tech_stack.is_empty() {
        let mut stack = profile.tech_stack.clone();
        stack.extend(enrichment.tech_stack.iter().cloned());
        stack.sort();
        stack.dedup();
        profile.tech_stack = stack;
    }
}

fn site_contact_enrichment_has_signal(enrichment: &SiteContactEnrichment) -> bool {
    enrichment.signal > 0
        || enrichment.email.is_some()
        || enrichment.linkedin_url.is_some()
        || enrichment.company_linkedin_url.is_some()
        || enrichment.evidence.is_some()
}

fn extract_contact_from_structured_site_text(
    text: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>();
    let mut best_score = -1;
    let mut best_name = None;
    let mut best_title = None;

    let mut consider = |raw_name: &str, raw_title: &str| {
        let Some(name) = normalize_person_name(raw_name) else {
            return;
        };
        let title = normalize_contact_title(raw_title);
        let priority = contact_title_priority(&title);
        if priority <= 0 || !title_allowed_for_policy(title_policy, &title) {
            return;
        }
        let score = 10 + priority;
        if score > best_score {
            best_score = score;
            best_name = Some(name);
            best_title = Some(title);
        }
    };

    for idx in 0..lines.len() {
        let current = &lines[idx];
        if idx + 1 < lines.len() {
            let next = &lines[idx + 1];
            consider(current, next);
            consider(next, current);
        }
        if idx + 2 < lines.len() {
            let next = &lines[idx + 1];
            let after_next = &lines[idx + 2];
            if next.len() <= 3 {
                consider(current, after_next);
                consider(after_next, current);
            }
        }
    }

    (best_name, best_title)
}

fn extract_contact_from_parenthesized_site_text(
    text: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let pattern = regex_lite::Regex::new(
        r"(?i)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,3})\s*\(([^()\n\r]{2,120})\)",
    )
    .unwrap();

    let mut best_score = -1;
    let mut best_name = None;
    let mut best_title = None;

    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        for cap in pattern.captures_iter(line) {
            let raw_name = cap.get(1).map(|m| m.as_str()).unwrap_or("");
            let raw_title = cap.get(2).map(|m| m.as_str()).unwrap_or("");
            let Some(name) = normalize_person_name(raw_name.trim()) else {
                continue;
            };
            let title = normalize_contact_title(raw_title.trim());
            let priority = contact_title_priority(&title);
            if priority <= 0 || !title_allowed_for_policy(title_policy, &title) {
                continue;
            }
            let score = 12 + priority;
            if score > best_score {
                best_score = score;
                best_name = Some(name);
                best_title = Some(title);
            }
        }
    }

    (best_name, best_title)
}

fn extract_contact_from_inline_site_text(
    text: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let patterns = [
        r"(?i)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,3})\s*,\s*(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director|Chairman|Vice Chairman)",
        r"(?i)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\s*[–-]\s*(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director|Chairman|Vice Chairman|Genel Müd[üu]r|Yönetim Kurulu Başkanı|Yonetim Kurulu Baskani|Yönetim Kurulu Başkan Vekili|Yonetim Kurulu Baskan Vekili)",
        r"(?i)\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director|Chairman|Vice Chairman)\s*[:\-–]?\s*([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,3})",
        r"(?i)\b(Genel Müd[üu]r|Kurucu(?:\s+Ortak)?|İcra Kurulu Başkanı(?:\s+ve\s+CEO)?|İşletme ve Bakımdan Sorumlu Başkan Yardımcısı)\s+([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,3})",
    ];

    let mut best_score = -1;
    let mut best_name = None;
    let mut best_title = None;

    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        for pattern in patterns {
            let Ok(re) = regex_lite::Regex::new(pattern) else {
                continue;
            };
            let Some(cap) = re.captures(line) else {
                continue;
            };

            let (raw_name, raw_title) = if pattern.starts_with("(?i)\\b([A-Z") {
                (
                    cap.get(1).map(|m| m.as_str()).unwrap_or(""),
                    cap.get(2).map(|m| m.as_str()).unwrap_or(""),
                )
            } else {
                (
                    cap.get(2).map(|m| m.as_str()).unwrap_or(""),
                    cap.get(1).map(|m| m.as_str()).unwrap_or(""),
                )
            };

            let Some(name) = normalize_person_name(raw_name.trim()) else {
                continue;
            };
            let title = normalize_contact_title(raw_title.trim());
            let priority = contact_title_priority(&title);
            if priority <= 0 || !title_allowed_for_policy(title_policy, &title) {
                continue;
            }
            let score = 10 + priority;
            if score > best_score {
                best_score = score;
                best_name = Some(name);
                best_title = Some(title);
            }
        }
    }

    (best_name, best_title)
}

fn apply_better_site_identity_candidate(
    candidate_name: Option<String>,
    candidate_title: Option<String>,
    contact_name: &mut Option<String>,
    contact_title: &mut Option<String>,
) {
    let current_score =
        site_contact_identity_signal(contact_name.as_ref(), contact_title.as_ref(), None);
    let candidate_score =
        site_contact_identity_signal(candidate_name.as_ref(), candidate_title.as_ref(), None);
    if candidate_score > current_score {
        *contact_name = candidate_name;
        *contact_title = candidate_title;
    }
}

fn canonicalize_contact_titles(text: &str) -> String {
    let replacements = [
        (r"(?i)\bicra kurulu başkanı ve ceo\b", "CEO"),
        (r"(?i)\bicra kurulu baskani ve ceo\b", "CEO"),
        (r"(?i)\bicra kurulu başkanı\b", "CEO"),
        (r"(?i)\bicra kurulu baskani\b", "CEO"),
        (r"(?i)\byönetim kurulu başkanı\b", "Chairman"),
        (r"(?i)\byonetim kurulu baskani\b", "Chairman"),
        (r"(?i)\byönetim kurulu başkan vekili\b", "Vice Chairman"),
        (r"(?i)\byonetim kurulu baskan vekili\b", "Vice Chairman"),
        (r"(?i)\bkurucu üye\b", "Founder"),
        (r"(?i)\bkurucu uye\b", "Founder"),
        (
            r"(?i)\bişletme ve bakımdan sorumlu başkan yardımcısı\b",
            "COO",
        ),
        (
            r"(?i)\bisletme ve bakimdan sorumlu baskan yardimcisi\b",
            "COO",
        ),
        (r"(?i)\bchief executive officer\b", "CEO"),
        (r"(?i)\bgenel müd[üu]r(?:ü|ümüz|u|umuz)?\b", "CEO"),
        (r"(?i)\bkurucu ortak\b", "Founder"),
        (r"(?i)\bco[- ]founder\b", "Founder"),
        (r"(?i)\bkurucu\b", "Founder"),
        (r"(?i)\bchief operating officer\b", "COO"),
        (r"(?i)\bhead of operations\b", "Head of Operations"),
        (r"(?i)\boperations director\b", "Operations Director"),
        (r"(?i)\bmanaging director\b", "Managing Director"),
        (
            r"(?i)\boperasyon(?:lar[ıi])?\s+(?:direkt[öo]r[üu]|müd[üu]r(?:ü)?)\b",
            "Operations Director",
        ),
        (
            r"(?i)\bsaha operasyon(?:lar[ıi])?\s+(?:direkt[öo]r[üu]|müd[üu]r(?:ü)?)\b",
            "Operations Director",
        ),
        (
            r"(?i)\bteknik servis\s+(?:direkt[öo]r[üu]|müd[üu]r(?:ü)?)\b",
            "Operations Director",
        ),
        (
            r"(?i)\bservis\s+(?:direkt[öo]r[üu]|müd[üu]r(?:ü)?)\b",
            "Operations Director",
        ),
        (r"(?i)\bgenel koordinat[öo]r\b", "Managing Director"),
    ];

    let mut normalized = text.to_string();
    for (pattern, replacement) in replacements {
        if let Ok(re) = regex_lite::Regex::new(pattern) {
            normalized = re.replace_all(&normalized, replacement).to_string();
        }
    }
    normalized
}

/// Run web search discovery: primary queries + fallback queries + Brave rescue.
/// Returns (candidates, source_contact_hints, search_unavailable).
async fn discover_via_web_search(
    search_engine: &WebSearchEngine,
    brave_search_engine: &Option<WebSearchEngine>,
    lead_plan: &LeadQueryPlanDraft,
    profile: &SalesProfile,
    max_candidates: usize,
    is_field_ops: bool,
) -> (
    Vec<DomainCandidate>,
    HashMap<String, SourceContactHint>,
    bool,
) {
    let discovery_fail_fast_threshold = if brave_search_engine.is_some() {
        MAX_DISCOVERY_FAILURES_BEFORE_FAST_FALLBACK
    } else {
        NO_BRAVE_FAIL_FAST_THRESHOLD
    };

    let queries = if lead_plan.discovery_queries.is_empty() {
        heuristic_lead_query_plan(profile).discovery_queries
    } else {
        lead_plan.discovery_queries.clone()
    };

    let mut domains = Vec::new();
    let mut candidates: HashMap<String, DomainCandidate> = HashMap::new();
    let source_contact_hints: HashMap<String, SourceContactHint> = HashMap::new();
    let mut discovery_successes = 0u32;
    let mut discovery_failures = 0u32;
    let discovery_queries: Vec<String> = queries
        .iter()
        .take(MAX_DISCOVERY_QUERIES)
        .cloned()
        .collect();

    // Primary web search
    for (query, result) in run_sales_search_batch(
        search_engine,
        &discovery_queries,
        max_candidates,
        Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
    )
    .await
    {
        match result {
            Ok(out) => {
                discovery_successes += 1;
                collect_domains_from_search(&out, &mut domains);
                collect_domain_candidates_from_search(
                    &out,
                    &mut candidates,
                    &lead_plan.must_include_keywords,
                    &lead_plan.exclude_keywords,
                    &profile.target_geo,
                    is_field_ops,
                );
            }
            Err(e) => {
                discovery_failures += 1;
                warn!(query = %query, error = %e, "Sales search query failed");
            }
        }
    }

    for domain in domains {
        if is_blocked_company_domain(&domain) {
            continue;
        }
        let entry = candidates.entry(domain.clone()).or_default();
        if entry.domain.is_empty() {
            entry.domain = domain.clone();
        }
        entry.score = entry.score.max(1);
    }

    let mut candidate_list: Vec<DomainCandidate> = candidates
        .into_values()
        .filter_map(|mut candidate| {
            normalize_candidate_gateway(&mut candidate).then_some(candidate)
        })
        .collect();
    let mut search_unavailable =
        discovery_successes == 0 && discovery_failures >= discovery_fail_fast_threshold;

    let adaptive_retry_threshold = adaptive_discovery_retry_threshold(profile, max_candidates);
    if candidate_list.len() < adaptive_retry_threshold && !search_unavailable {
        let adaptive_queries =
            build_adaptive_discovery_queries(lead_plan, profile, &candidate_list);
        if !adaptive_queries.is_empty() {
            let mut adaptive_domains = Vec::<String>::new();
            let mut adaptive_candidates = HashMap::<String, DomainCandidate>::new();
            for (query, result) in run_sales_search_batch(
                search_engine,
                &adaptive_queries,
                max_candidates.min(24),
                Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
            )
            .await
            {
                match result {
                    Ok(out) => {
                        discovery_successes += 1;
                        collect_domains_from_search(&out, &mut adaptive_domains);
                        collect_domain_candidates_from_search(
                            &out,
                            &mut adaptive_candidates,
                            &lead_plan.must_include_keywords,
                            &lead_plan.exclude_keywords,
                            &profile.target_geo,
                            is_field_ops,
                        );
                    }
                    Err(e) => {
                        discovery_failures += 1;
                        warn!(query = %query, error = %e, "Adaptive sales discovery query failed");
                    }
                }
            }

            for domain in adaptive_domains {
                if is_blocked_company_domain(&domain) {
                    continue;
                }
                let entry = adaptive_candidates.entry(domain.clone()).or_default();
                if entry.domain.is_empty() {
                    entry.domain = domain.clone();
                }
                entry.score = entry.score.max(1);
                entry
                    .evidence
                    .push("Adaptive discovery follow-up query surfaced this company".to_string());
            }

            if !adaptive_candidates.is_empty() {
                candidate_list.extend(adaptive_candidates.into_values());
                candidate_list = dedupe_domain_candidates(candidate_list);
                info!(
                    queries = adaptive_queries.len(),
                    candidates = candidate_list.len(),
                    "Adaptive discovery follow-up expanded prospect candidates"
                );
            }
        }
    }

    // Fallback queries if primary returned nothing
    if candidate_list.is_empty() && !search_unavailable {
        let fallback_queries = vec![
            format!(
                "{} companies {}",
                profile.target_industry, profile.target_geo
            ),
            format!(
                "{} operations companies {}",
                profile.target_industry, profile.target_geo
            ),
            format!("B2B companies {} operations teams", profile.target_geo),
            format!("field service companies {}", profile.target_geo),
        ];
        let mut fallback_domains = Vec::<String>::new();
        for (query, result) in run_sales_search_batch(
            search_engine,
            &fallback_queries,
            20,
            Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
        )
        .await
        {
            match result {
                Ok(out) => {
                    discovery_successes += 1;
                    collect_domains_from_search(&out, &mut fallback_domains);
                }
                Err(e) => {
                    discovery_failures += 1;
                    warn!(query = %query, error = %e, "Fallback sales query failed");
                }
            }
        }
        search_unavailable =
            discovery_successes == 0 && discovery_failures >= discovery_fail_fast_threshold;
        let mut seen = HashSet::<String>::new();
        for domain in fallback_domains {
            if is_blocked_company_domain(&domain) || !seen.insert(domain.clone()) {
                continue;
            }
            let mut candidate = DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE,
                evidence: vec![format!(
                    "Discovered via fallback query for {}",
                    profile.target_industry
                )],
                matched_keywords: vec![profile.target_industry.clone()],
                source_links: Vec::new(),
                phone: None,
            };
            if normalize_candidate_gateway(&mut candidate) {
                candidate_list.push(candidate);
            }
        }
    }

    // Brave rescue if primary search entirely unavailable
    if candidate_list.is_empty() && search_unavailable {
        if let Some(brave_engine) = brave_search_engine.as_ref() {
            let mut brave_domains = Vec::<String>::new();
            let mut brave_candidates = HashMap::<String, DomainCandidate>::new();
            let mut brave_successes = 0u32;

            for (query, result) in run_sales_search_batch(
                brave_engine,
                &discovery_queries,
                max_candidates,
                Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
            )
            .await
            {
                match result {
                    Ok(out) => {
                        brave_successes += 1;
                        collect_domains_from_search(&out, &mut brave_domains);
                        collect_domain_candidates_from_search(
                            &out,
                            &mut brave_candidates,
                            &lead_plan.must_include_keywords,
                            &lead_plan.exclude_keywords,
                            &profile.target_geo,
                            is_field_ops,
                        );
                    }
                    Err(e) => {
                        warn!(query = %query, error = %e, "Brave rescue query failed");
                    }
                }
            }

            if brave_successes > 0 {
                for domain in brave_domains {
                    if is_blocked_company_domain(&domain) {
                        continue;
                    }
                    let entry = brave_candidates.entry(domain.clone()).or_default();
                    if entry.domain.is_empty() {
                        entry.domain = domain.clone();
                    }
                    entry.score = entry.score.max(1);
                }
                candidate_list.extend(brave_candidates.into_values());
                candidate_list = dedupe_domain_candidates(candidate_list);
                search_unavailable = false;
                info!("Primary web discovery failed; recovered via Brave rescue search");
            }
        }
    }

    (
        dedupe_domain_candidates(candidate_list),
        source_contact_hints,
        search_unavailable,
    )
}

/// Merge candidates from all discovery sources with cross-source confirmation bonus.
fn merge_all_discovery_sources(
    llm_candidates: Vec<DomainCandidate>,
    web_candidates: Vec<DomainCandidate>,
    free_candidates: Vec<FreeDiscoveryCandidate>,
    source_contact_hints: &mut HashMap<String, SourceContactHint>,
) -> Vec<DomainCandidate> {
    let mut merged: HashMap<String, DomainCandidate> = HashMap::new();
    let mut source_counts: HashMap<String, u32> = HashMap::new();

    // Track which sources each domain appears in
    for c in &llm_candidates {
        *source_counts.entry(c.domain.clone()).or_default() += 1;
    }
    for c in &web_candidates {
        *source_counts.entry(c.domain.clone()).or_default() += 1;
    }
    for fc in &free_candidates {
        *source_counts
            .entry(fc.candidate.domain.clone())
            .or_default() += 1;
    }

    // Merge LLM candidates (primary source)
    for c in llm_candidates {
        let entry = merged.entry(c.domain.clone()).or_default();
        if entry.domain.is_empty() {
            entry.domain = c.domain;
        }
        entry.score = entry.score.max(c.score);
        entry.evidence.extend(c.evidence);
        if entry.evidence.len() > 6 {
            entry.evidence.truncate(6);
        }
        entry.matched_keywords.extend(c.matched_keywords);
        entry.matched_keywords = dedupe_strings(entry.matched_keywords.clone());
        entry.source_links = merge_osint_links(entry.source_links.clone(), c.source_links);
        if entry.phone.is_none() {
            entry.phone = c.phone;
        }
    }

    // Merge web search candidates
    for c in web_candidates {
        let entry = merged.entry(c.domain.clone()).or_default();
        if entry.domain.is_empty() {
            entry.domain = c.domain;
        }
        entry.score = entry.score.max(c.score);
        entry.evidence.extend(c.evidence);
        if entry.evidence.len() > 6 {
            entry.evidence.truncate(6);
        }
        entry.matched_keywords.extend(c.matched_keywords);
        entry.matched_keywords = dedupe_strings(entry.matched_keywords.clone());
        entry.source_links = merge_osint_links(entry.source_links.clone(), c.source_links);
        if entry.phone.is_none() {
            entry.phone = c.phone;
        }
    }

    // Merge free directory candidates
    for fc in free_candidates {
        merge_free_discovery_candidate(&mut merged, source_contact_hints, fc);
    }

    // Apply cross-source confirmation bonus (+10 for appearing in 2+ sources)
    for (domain, count) in &source_counts {
        if *count >= 2 {
            if let Some(entry) = merged.get_mut(domain) {
                entry.score += 10;
                entry
                    .evidence
                    .push("Confirmed by multiple discovery sources".to_string());
            }
        }
    }

    dedupe_domain_candidates(merged.into_values().collect())
}

async fn run_sales_search(
    search_engine: &WebSearchEngine,
    query: &str,
    max_results: usize,
    timeout: Duration,
) -> Result<String, String> {
    match tokio::time::timeout(timeout, search_engine.search(query, max_results)).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "Sales search timed out after {} ms for query: {}",
            timeout.as_millis(),
            query
        )),
    }
}

async fn run_sales_search_batch(
    search_engine: &WebSearchEngine,
    queries: &[String],
    max_results: usize,
    timeout: Duration,
) -> Vec<(String, Result<String, String>)> {
    let owned: Vec<String> = queries.to_vec();
    stream::iter(owned.into_iter().map(|query| async move {
        let result = run_sales_search(search_engine, &query, max_results, timeout).await;
        (query, result)
    }))
    .buffer_unordered(SALES_SEARCH_BATCH_CONCURRENCY)
    .collect()
    .await
}

fn adaptive_discovery_retry_threshold(profile: &SalesProfile, max_candidates: usize) -> usize {
    let _ = profile;
    (max_candidates / 2).clamp(6, 12)
}

fn normalize_discovery_focus_term(raw: &str) -> Option<String> {
    let cleaned = raw
        .trim()
        .trim_matches(|c: char| {
            c == '"' || c == '\'' || c == ',' || c == ';' || c == ':' || c == '.'
        })
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if cleaned.len() < 4 || cleaned.len() > 80 {
        return None;
    }
    if cleaned.split_whitespace().count() > 6 {
        return None;
    }

    let lower = cleaned.to_lowercase();
    let generic_terms = [
        "ai",
        "automation",
        "b2b",
        "business",
        "businesses",
        "companies",
        "company",
        "coordination",
        "digital",
        "firms",
        "industry",
        "industries",
        "management",
        "operation",
        "operations",
        "platform",
        "project",
        "projects",
        "service",
        "services",
        "software",
        "solution",
        "solutions",
        "team",
        "teams",
        "transformation",
        "workflow",
        "workflows",
    ];
    let words: Vec<&str> = lower.split_whitespace().collect();
    let non_generic_words = words
        .iter()
        .filter(|word| word.len() > 2 && !generic_terms.contains(word))
        .count();
    if non_generic_words == 0 && extract_field_ops_signal_keyword(&lower).is_none() {
        return None;
    }

    Some(truncate_cleaned_text(&cleaned, 80))
}

fn adaptive_discovery_focus_terms(
    lead_plan: &LeadQueryPlanDraft,
    profile: &SalesProfile,
    current_candidates: &[DomainCandidate],
) -> Vec<String> {
    let mut raw_terms = vec![profile.target_industry.clone()];
    raw_terms.extend(
        profile
            .target_industry
            .split([',', '/', ';', '|'])
            .map(|value| value.trim().to_string()),
    );
    raw_terms.extend(
        profile
            .product_description
            .split([',', ';', '|', '.'])
            .map(|value| value.trim().to_string()),
    );
    raw_terms.extend(lead_plan.must_include_keywords.iter().cloned());
    for candidate in current_candidates {
        for keyword in &candidate.matched_keywords {
            if let Some(signal) = extract_field_ops_signal_keyword(keyword) {
                raw_terms.push(signal.to_string());
            }
        }
        for evidence in &candidate.evidence {
            if let Some(signal) = extract_field_ops_signal_keyword(evidence) {
                raw_terms.push(signal.to_string());
            }
        }
    }

    dedupe_strings(
        raw_terms
            .into_iter()
            .filter_map(|term| normalize_discovery_focus_term(&term))
            .collect(),
    )
}

fn build_adaptive_discovery_queries(
    lead_plan: &LeadQueryPlanDraft,
    profile: &SalesProfile,
    current_candidates: &[DomainCandidate],
) -> Vec<String> {
    let geo = if profile.target_geo.trim().is_empty() {
        "US".to_string()
    } else {
        profile.target_geo.clone()
    };
    let geo_query_label = if geo_is_turkey(&geo) {
        "Türkiye".to_string()
    } else {
        geo.clone()
    };
    let is_field_ops = profile_targets_field_ops(profile);
    let focus_terms = adaptive_discovery_focus_terms(lead_plan, profile, current_candidates);
    if focus_terms.is_empty() {
        return Vec::new();
    }

    let existing_queries: HashSet<String> = lead_plan
        .discovery_queries
        .iter()
        .map(|query| query.to_lowercase())
        .collect();
    let mut queries = Vec::new();
    for term in focus_terms.iter().take(4) {
        if geo_is_turkey(&geo) {
            queries.push(format!(
                "site:.tr \"{}\" sirketleri {}",
                term, geo_query_label
            ));
            queries.push(format!(
                "site:.tr \"{}\" firmalari {}",
                term, geo_query_label
            ));
            queries.push(format!(
                "\"{}\" \"{}\" sirketleri {}",
                term, profile.target_industry, geo_query_label
            ));
            if is_field_ops {
                queries.push(format!(
                    "site:.tr \"{}\" saha operasyon {}",
                    term, geo_query_label
                ));
            }
        } else {
            queries.push(format!("\"{}\" companies {}", term, geo_query_label));
            queries.push(format!("\"{}\" firms {}", term, geo_query_label));
            queries.push(format!(
                "\"{}\" \"{}\" companies {}",
                term, profile.target_industry, geo_query_label
            ));
            if is_field_ops {
                queries.push(format!(
                    "\"{}\" field service companies {}",
                    term, geo_query_label
                ));
            }
        }
        queries.push(format!("\"{}\" {} operations teams", term, geo_query_label));
    }

    dedupe_strings(
        queries
            .into_iter()
            .filter_map(|query| sanitize_discovery_query(&query))
            .filter(|query| !existing_queries.contains(&query.to_lowercase()))
            .collect(),
    )
    .into_iter()
    .take(MAX_ADAPTIVE_DISCOVERY_QUERIES)
    .collect()
}

fn heuristic_lead_query_plan(profile: &SalesProfile) -> LeadQueryPlanDraft {
    let is_field_ops = profile_targets_field_ops(profile);
    let geo = if profile.target_geo.trim().is_empty() {
        "US".to_string()
    } else {
        profile.target_geo.clone()
    };
    let geo_aliases = if geo_is_turkey(&geo) {
        vec![
            geo.clone(),
            "Turkey".to_string(),
            "Türkiye".to_string(),
            "Turkiye".to_string(),
        ]
    } else {
        vec![geo.clone()]
    };
    let discovery_topic = if is_field_ops {
        "field service maintenance installation facility management construction".to_string()
    } else {
        profile.target_industry.clone()
    };

    let mut discovery_queries = if is_field_ops {
        if geo_is_turkey(&geo) {
            vec![
                format!("site:.tr \"saha servis\" sirketleri {}", geo_aliases[2]),
                format!("site:.tr \"insaat taahhut\" sirketleri {}", geo_aliases[2]),
            ]
        } else {
            vec![
                format!("field service companies {}", geo_aliases[0]),
                format!(
                    "construction facility management companies {}",
                    geo_aliases[0]
                ),
            ]
        }
    } else {
        vec![format!("{discovery_topic} companies {}", geo_aliases[0])]
    };

    if is_field_ops {
        if geo_is_turkey(&geo) {
            discovery_queries.extend([
                format!("site:.tr \"saha servis\" sirketleri {}", geo_aliases[2]),
                format!("site:.tr \"bakim onarim\" sirketleri {}", geo_aliases[2]),
                format!("site:.tr \"tesis yonetimi\" sirketleri {}", geo_aliases[2]),
                format!(
                    "site:.tr \"mekanik tesisat\" bakim sirketleri {}",
                    geo_aliases[2]
                ),
                format!("\"field service\" companies {} operations", geo_aliases[1]),
                format!(
                    "\"facility management\" companies {} operations",
                    geo_aliases[1]
                ),
                // City-specific queries
                "insaat sirketleri Istanbul saha ekip".to_string(),
                "saha servis firmalari Ankara".to_string(),
                "bakim onarim sirketleri Izmir".to_string(),
                "tesis yonetimi firmalari Bursa".to_string(),
                // Sub-industry queries
                format!("mekanik tesisat firmalari {}", geo_aliases[2]),
                format!("elektrik taahhut firmalari {}", geo_aliases[2]),
                format!("asansor bakim sirketleri {}", geo_aliases[2]),
                format!("iklimlendirme firmalari {}", geo_aliases[2]),
                format!("muhendislik firmalari saha operasyon {}", geo_aliases[2]),
            ]);
        } else {
            discovery_queries.extend([
                format!(
                    "field service companies {} (CEO OR COO OR Operations Director)",
                    geo_aliases[0]
                ),
                format!(
                    "construction facility maintenance companies {} operations",
                    geo_aliases[0]
                ),
                format!(
                    "facility management companies {} leadership team operations",
                    geo_aliases[0]
                ),
                format!(
                    "companies with on-site teams {} project coordination",
                    geo_aliases[0]
                ),
                format!(
                    "mobile workforce companies {} operations executive",
                    geo_aliases[0]
                ),
            ]);
        }
    } else {
        discovery_queries.extend([
            format!(
                "{} organizations {} project operations teams",
                discovery_topic, geo_aliases[0]
            ),
            format!(
                "{} firms {} operational excellence transformation",
                discovery_topic, geo_aliases[0]
            ),
        ]);
    }

    let mut must_include_keywords = vec![
        profile.target_industry.clone(),
        "operations".to_string(),
        "project".to_string(),
        "coordination".to_string(),
        "workflow".to_string(),
        "team".to_string(),
    ];

    if is_field_ops {
        must_include_keywords.extend([
            "field operations".to_string(),
            "field service".to_string(),
            "on-site".to_string(),
            "maintenance".to_string(),
            "installation".to_string(),
            "dispatch".to_string(),
            "facility".to_string(),
            "construction".to_string(),
            "mobile workforce".to_string(),
        ]);
        if geo_is_turkey(&geo) {
            must_include_keywords.extend([
                "saha servis".to_string(),
                "bakim".to_string(),
                "onarim".to_string(),
                "tesis yonetimi".to_string(),
                "mekanik tesisat".to_string(),
                "operasyon".to_string(),
                "saha ekibi".to_string(),
            ]);
        }
    }

    let exclude_keywords = vec![
        "blog".to_string(),
        "news".to_string(),
        "directory".to_string(),
        "review".to_string(),
        "dictionary".to_string(),
        "definition".to_string(),
        "meaning".to_string(),
        "forum".to_string(),
        "job".to_string(),
        "careers".to_string(),
        "consulting agency".to_string(),
        "marketing agency".to_string(),
        "software vendor".to_string(),
        "course".to_string(),
        "investor relations".to_string(),
        "annual report".to_string(),
        "sustainability report".to_string(),
        "yatirimci iliskileri".to_string(),
        "faaliyet raporu".to_string(),
        "kurumsal yonetim".to_string(),
    ];

    LeadQueryPlanDraft {
        discovery_queries: dedupe_strings(
            discovery_queries
                .into_iter()
                .filter_map(|query| sanitize_discovery_query(&query))
                .collect(),
        ),
        must_include_keywords: expand_keywords(must_include_keywords),
        exclude_keywords: expand_keywords(exclude_keywords),
        contact_titles: vec![
            "CEO".to_string(),
            "Founder".to_string(),
            "COO".to_string(),
            "Head of Operations".to_string(),
            "Operations Director".to_string(),
        ],
    }
}

fn sanitize_discovery_query(raw: &str) -> Option<String> {
    let mut cleaned = raw.trim().to_string();
    let title_noise_patterns = [
        r"(?i)\bchief executive officer\b",
        r"(?i)\bchief operating officer\b",
        r"(?i)\boperations director\b",
        r"(?i)\bhead of operations\b",
        r"(?i)\bleadership team\b",
        r"(?i)\bexecutive team\b",
        r"(?i)\bleadership\b",
        r"(?i)\bexecutive\b",
        r"(?i)\bceo\b",
        r"(?i)\bcoo\b",
        r"(?i)\bfounder\b",
        r"(?i)\bor\b",
    ];
    for pattern in title_noise_patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        cleaned = re.replace_all(&cleaned, " ").to_string();
    }
    cleaned = cleaned.replace(['(', ')'], " ");
    cleaned = cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if cleaned.split_whitespace().count() < 2 {
        return None;
    }
    Some(cleaned)
}

fn load_sales_codex_auth(home_dir: &FsPath) -> Option<StoredCodexAuth> {
    let path = home_dir.join("auth").join("codex_oauth.json");
    if let Ok(raw) = std::fs::read_to_string(path) {
        if let Ok(auth) = serde_json::from_str::<StoredCodexAuth>(&raw) {
            return Some(auth);
        }
    }
    crate::codex_oauth::import_codex_cli_auth(home_dir).ok()
}

async fn build_sales_llm_driver(
    home_dir: &FsPath,
) -> Result<Arc<dyn openfang_runtime::llm_driver::LlmDriver>, String> {
    // Priority chain for API key:
    // 1. OPENAI_CODEX_ACCESS_TOKEN env var (set by codex CLI or user)
    // 2. OPENAI_API_KEY env var
    // 3. Stored auth with auto-refresh if expired
    let env_token = std::env::var("OPENAI_CODEX_ACCESS_TOKEN")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("OPENAI_API_KEY")
                .ok()
                .filter(|v| !v.trim().is_empty())
        });

    let api_key = if let Some(token) = env_token {
        Some(token.trim().to_string())
    } else {
        let fresh_import = crate::codex_oauth::import_codex_cli_auth(home_dir).ok();
        let mut auth = fresh_import.or_else(|| load_sales_codex_auth(home_dir));

        // Auto-refresh if token looks expired
        if let Some(ref mut stored) = auth {
            let is_expired = stored
                .expires_at
                .map(|exp| Utc::now() >= exp)
                .unwrap_or_else(|| token_jwt_expired(&stored.access_token));
            if is_expired && stored.refresh_token.is_some() {
                info!("Sales LLM token expired, attempting refresh...");
                if crate::codex_oauth::refresh_auth_if_possible(
                    stored,
                    "app_EMoamEEZ73f0CkXaXp7hrann",
                )
                .await
                {
                    info!("Sales LLM token refreshed successfully");
                    let _ = crate::codex_oauth::save_stored_auth(home_dir, stored);
                    std::env::set_var("OPENAI_CODEX_ACCESS_TOKEN", &stored.access_token);
                } else {
                    warn!("Sales LLM token refresh failed");
                }
            }
        }

        auth.as_ref()
            .map(|a| a.access_token.trim().to_string())
            .filter(|token| !token.is_empty())
    };

    let cfg = DriverConfig {
        provider: SALES_LLM_PROVIDER.to_string(),
        api_key,
        base_url: None,
    };
    openfang_runtime::drivers::create_driver(&cfg)
        .map_err(|e| format!("LLM driver init failed: {e}"))
}

fn token_jwt_expired(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return true;
    }
    let payload = parts[1];
    let Ok(decoded) = base64_url_decode(payload) else {
        return true;
    };
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&decoded) else {
        return true;
    };
    json.get("exp")
        .and_then(|v| v.as_i64())
        .map(|exp| Utc::now().timestamp() >= exp)
        .unwrap_or(true)
}

fn base64_url_decode(input: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(input))
        .map_err(|e| format!("base64 decode failed: {e}"))
}

async fn llm_build_lead_query_plan(
    kernel: &openfang_kernel::OpenFangKernel,
    profile: &SalesProfile,
) -> Result<LeadQueryPlanDraft, String> {
    let driver = build_sales_llm_driver(&kernel.config.home_dir).await?;

    let prompt = format!(
        "You are generating a B2B outbound lead discovery plan.\n\
         Product: {}\n\
         Product value proposition: {}\n\
         Target industry: {}\n\
         Target geography: {}\n\
         Target title policy: {}\n\n\
         Return strict JSON only with keys:\n\
         discovery_queries (array of 6-10 web queries to find PROSPECT COMPANIES, not blogs/directories),\n\
         must_include_keywords (array),\n\
         exclude_keywords (array),\n\
         contact_titles (array).\n\n\
         Rules:\n\
         - Think like an elite business development rep hunting reachable ICP-fit accounts, not a generic researcher.\n\
         - Cover multiple plausible subsegments, company archetypes, and buying triggers.\n\
         - If product suggests field/on-site operations, prioritize companies with field teams.\n\
         - Prefer company-finding queries that mention operational pain, company type, or sub-industry rather than generic head terms.\n\
         - discovery_queries should include both English and local-language variants when helpful.\n\
         - exclude_keywords should remove directories/news/job pages/review sites.\n\
         - Output valid JSON only.",
        profile.product_name,
        profile.product_description,
        profile.target_industry,
        profile.target_geo,
        profile.target_title_policy
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 900,
        temperature: 0.0,
        system: Some(
            "You are an elite outbound prospecting strategist and business development operator. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::Medium),
    };

    let resp = driver
        .complete(req)
        .await
        .map_err(|e| format!("Lead query planner failed: {e}"))?;
    let text = resp.text();
    let json_payload = extract_json_payload(&text)
        .ok_or_else(|| "Could not parse JSON payload from planner output".to_string())?;
    let mut draft = serde_json::from_str::<LeadQueryPlanDraft>(&json_payload)
        .map_err(|e| format!("Invalid planner JSON: {e}; payload: {json_payload}"))?;

    draft.discovery_queries = dedupe_strings(
        draft
            .discovery_queries
            .into_iter()
            .filter_map(|q| sanitize_discovery_query(&q))
            .collect(),
    );
    draft.must_include_keywords = expand_keywords(
        draft
            .must_include_keywords
            .into_iter()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect(),
    );
    draft.exclude_keywords = expand_keywords(
        draft
            .exclude_keywords
            .into_iter()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect(),
    );
    draft.contact_titles = dedupe_strings(
        draft
            .contact_titles
            .into_iter()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect(),
    );

    if draft.discovery_queries.is_empty() {
        return Err("Planner returned empty discovery_queries".to_string());
    }
    if draft.must_include_keywords.is_empty() {
        draft.must_include_keywords = heuristic_lead_query_plan(profile).must_include_keywords;
    }
    if draft.exclude_keywords.is_empty() {
        draft.exclude_keywords = heuristic_lead_query_plan(profile).exclude_keywords;
    }

    Ok(draft)
}

async fn llm_generate_company_candidates(
    kernel: &openfang_kernel::OpenFangKernel,
    profile: &SalesProfile,
    max_companies: usize,
    run_sequence: usize,
    previously_discovered: &[String],
) -> Result<Vec<DomainCandidate>, String> {
    let driver = build_sales_llm_driver(&kernel.config.home_dir).await?;

    let prev_domains_section = if previously_discovered.is_empty() {
        String::new()
    } else {
        format!(
            "\nAvoid these already-discovered domains from previous runs:\n{}\n",
            previously_discovered
                .iter()
                .take(100)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    let prompt = format!(
        "List up to {max_co} real B2B companies for outbound sales prospecting.\n\
         Product: {product}\n\
         Product value: {value}\n\
         Target industry: {industry}\n\
         Target geography: {geo}\n\
         Run #{run_seq}.\n\
         {prev}\
         Return strict JSON: {{\"companies\":[{{\"company\":\"...\",\"domain\":\"...\",\"reason\":\"...\"}}]}}\n\n\
         CRITICAL RULES:\n\
         - Think like a top-tier business development rep building a pipeline for immediate outreach.\n\
         - Focus on real SMB/mid-market companies (20-5000 employees) that operate in or sell into {geo}\n\
         - NO global giants (Siemens, ABB, Honeywell, Schneider, Bosch, etc.)\n\
         - domain must be real company website (.com.tr or .tr or .com). NO linkedin/wikipedia/news\n\
         - reason: 1 short clause explaining their likely operational pain or why they are a fit now\n\
         - Spread suggestions across multiple cities, sub-industries, and company archetypes when possible\n\
         - Prefer companies that look reachable for outbound today",
        product = profile.product_name,
        value = profile.product_description,
        industry = profile.target_industry,
        geo = profile.target_geo,
        run_seq = run_sequence,
        prev = prev_domains_section,
        max_co = max_companies
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 2400,
        temperature: 0.2,
        system: Some(
            "You are an elite B2B market mapper and business development operator. Suggest realistic ICP-fit prospect companies with accurate domains. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::Medium),
    };

    let resp = driver
        .complete(req)
        .await
        .map_err(|e| format!("LLM company candidate generation failed: {e}"))?;
    let text = resp.text();
    let json_payload = extract_json_payload(&text)
        .ok_or_else(|| "Could not parse JSON payload from company candidate output".to_string())?;
    let parsed = serde_json::from_str::<LlmCompanyCandidateResponse>(&json_payload)
        .map_err(|e| format!("Invalid company candidate JSON: {e}; payload: {json_payload}"))?;

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for c in parsed.companies.into_iter().take(max_companies) {
        let raw_domain = c
            .domain
            .as_ref()
            .and_then(|d| extract_domain(d))
            .or_else(|| c.website.as_ref().and_then(|w| extract_domain(w)));
        let Some(domain) = raw_domain else {
            continue;
        };
        if !is_valid_company_domain(&domain) || !seen.insert(domain.clone()) {
            continue;
        }
        let reason = c
            .reason
            .unwrap_or_else(|| format!("LLM suggested for {}", profile.target_industry));
        let mut matched = vec![profile.target_industry.clone()];
        if let Some(company) = c.company {
            matched.push(company);
        }
        let mut candidate = DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
            evidence: vec![truncate_text_for_reason(&reason, 220)],
            matched_keywords: dedupe_strings(matched),
            source_links: Vec::new(),
            phone: None,
        };
        if normalize_candidate_gateway(&mut candidate) {
            out.push(candidate);
        }
    }

    Ok(out)
}

#[derive(Debug, Deserialize)]
struct LlmValidationItem {
    #[serde(default)]
    domain: String,
    #[serde(default)]
    relevant: bool,
    #[serde(default)]
    confidence: f64,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LlmValidationResponse {
    #[serde(default)]
    results: Vec<LlmValidationItem>,
}

async fn llm_validate_candidate_relevance(
    kernel: &openfang_kernel::OpenFangKernel,
    profile: &SalesProfile,
    candidates: &[DomainCandidate],
) -> Result<HashMap<String, (bool, f64, Option<String>)>, String> {
    let driver = build_sales_llm_driver(&kernel.config.home_dir).await?;
    let relevance_context = llm_candidate_relevance_prompt_context(profile);

    let companies_list = candidates
        .iter()
        .map(|c| {
            format!(
                "- {} (evidence: {})",
                c.domain,
                c.evidence.first().cloned().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let prompt = format!(
        "Rate each company for ICP fit as a B2B sales prospect.\n\n\
         Our product: {} - {}\n\
         Target industry: {}\n\
         Target geography: {}\n\
         {}\n\n\
         Companies to evaluate:\n{}\n\n\
         Return strict JSON only:\n\
         {{\"results\":[{{\"domain\":\"...\",\"relevant\":true/false,\"confidence\":0.0-1.0,\"reason\":\"...\"}}]}}",
        profile.product_name,
        profile.product_description,
        profile.target_industry,
        profile.target_geo,
        relevance_context,
        companies_list
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 1400,
        temperature: 0.0,
        system: Some(
            "You are a Turkish B2B market analyst. Rate company relevance for the given ICP. \
             Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::Medium),
    };

    let resp = driver
        .complete(req)
        .await
        .map_err(|e| format!("LLM validation failed: {e}"))?;
    let text = resp.text();
    let json_payload = extract_json_payload(&text)
        .ok_or_else(|| "Could not parse JSON from validation output".to_string())?;
    let parsed = serde_json::from_str::<LlmValidationResponse>(&json_payload)
        .map_err(|e| format!("Invalid validation JSON: {e}"))?;

    let mut map = HashMap::new();
    for item in parsed.results {
        if !item.domain.is_empty() {
            map.insert(item.domain, (item.relevant, item.confidence, item.reason));
        }
    }
    Ok(map)
}

fn apply_llm_validation_scores(
    candidates: &mut [DomainCandidate],
    validations: &HashMap<String, (bool, f64, Option<String>)>,
) {
    for candidate in candidates.iter_mut() {
        if let Some((relevant, confidence, reason)) = validations.get(&candidate.domain) {
            if *relevant {
                if *confidence >= 0.7 {
                    candidate.score += 15;
                    if let Some(reason) = reason {
                        candidate.evidence.push(format!(
                            "LLM validated: {}",
                            truncate_text_for_reason(reason, 180)
                        ));
                    }
                } else if *confidence >= 0.4 {
                    candidate.score += 8;
                }
            } else if *confidence >= 0.7 {
                candidate.score -= 15;
            } else {
                candidate.score -= 5;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ProspectResearchExtra {
    summary: String,
    buyer_roles: Vec<String>,
    pain_points: Vec<String>,
    trigger_events: Vec<String>,
    recommended_channel: String,
    outreach_angle: String,
    research_confidence: f32,
}

#[derive(Debug, Deserialize)]
struct LlmProspectResearchItem {
    #[serde(default)]
    company_domain: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    buyer_roles: Vec<String>,
    #[serde(default)]
    pain_points: Vec<String>,
    #[serde(default)]
    trigger_events: Vec<String>,
    #[serde(default)]
    recommended_channel: String,
    #[serde(default)]
    outreach_angle: String,
    #[serde(default)]
    research_confidence: Option<f32>,
}

#[derive(Debug, Deserialize)]
struct LlmProspectResearchResponse {
    #[serde(default)]
    profiles: Vec<LlmProspectResearchItem>,
}

async fn llm_enrich_prospect_profiles(
    kernel: &openfang_kernel::OpenFangKernel,
    sales_profile: &SalesProfile,
    prospects: &[SalesProspectProfile],
) -> Result<HashMap<String, ProspectResearchExtra>, String> {
    if prospects.is_empty() {
        return Ok(HashMap::new());
    }

    let driver = build_sales_llm_driver(&kernel.config.home_dir).await?;
    let prospect_lines = prospects
        .iter()
        .map(|prospect| {
            format!(
                "- domain: {}\n  company: {}\n  fit_score: {}\n  status: {}\n  primary_contact: {}\n  recommended_channel_now: {}\n  matched_signals: {}\n  existing_summary: {}",
                prospect.company_domain,
                prospect.company,
                prospect.fit_score,
                prospect.profile_status,
                prospect_primary_contact_for_prompt(prospect),
                prospect.recommended_channel,
                prospect.matched_signals.join(" | "),
                prospect.summary
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let prompt = format!(
        "Create outbound account dossiers for candidate customers.\n\
         Product: {}\n\
         Product value proposition: {}\n\
         Target industry: {}\n\
         Target geography: {}\n\n\
         Candidate accounts:\n{}\n\n\
         Return strict JSON only:\n\
         {{\"profiles\":[{{\"company_domain\":\"...\",\"summary\":\"...\",\"buyer_roles\":[\"...\"],\"pain_points\":[\"...\"],\"trigger_events\":[\"...\"],\"recommended_channel\":\"email|linkedin|either|research\",\"outreach_angle\":\"...\",\"research_confidence\":0.0}}]}}\n\n\
         Rules:\n\
         - Use ONLY the provided signals and contact context. Do not invent news, numbers, customers, headcount, funding, or software stack.\n\
         - summary: 1 short paragraph, max 220 chars, suitable for a B2B AE/SDR briefing.\n\
         - buyer_roles: 2-4 roles likely to care.\n\
         - pain_points: 2-3 pains tied to the product and public signals.\n\
         - trigger_events: 2-3 short lines explaining why now.\n\
         - outreach_angle: 1 concise angle for first-touch personalization.\n\
         - recommended_channel must be one of email, linkedin, either, research.\n\
         - research_confidence must be between 0.0 and 1.0.\n\
         - Output JSON only.",
        sales_profile.product_name,
        sales_profile.product_description,
        sales_profile.target_industry,
        sales_profile.target_geo,
        prospect_lines
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 1800,
        temperature: 0.1,
        system: Some(
            "You are a B2B prospect research analyst. Build concise, evidence-bound account dossiers from partial outbound signals. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::Medium),
    };

    let resp = driver
        .complete(req)
        .await
        .map_err(|e| format!("Prospect dossier enrichment failed: {e}"))?;
    let text = resp.text();
    let json_payload = extract_json_payload(&text)
        .ok_or_else(|| "Could not parse JSON payload from prospect dossier output".to_string())?;
    let parsed = serde_json::from_str::<LlmProspectResearchResponse>(&json_payload)
        .map_err(|e| format!("Invalid prospect dossier JSON: {e}; payload: {json_payload}"))?;

    let mut out = HashMap::new();
    for item in parsed.profiles {
        let Some(domain) = extract_domain(&item.company_domain).or_else(|| {
            let trimmed = item.company_domain.trim().to_lowercase();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }) else {
            continue;
        };

        out.insert(
            domain,
            ProspectResearchExtra {
                summary: truncate_cleaned_text(&item.summary, 280),
                buyer_roles: dedupe_strings(
                    item.buyer_roles
                        .into_iter()
                        .map(|value| truncate_cleaned_text(&value, 80))
                        .filter(|value| !value.is_empty())
                        .collect(),
                )
                .into_iter()
                .take(4)
                .collect(),
                pain_points: dedupe_strings(
                    item.pain_points
                        .into_iter()
                        .map(|value| truncate_cleaned_text(&value, 140))
                        .filter(|value| !value.is_empty())
                        .collect(),
                )
                .into_iter()
                .take(3)
                .collect(),
                trigger_events: dedupe_strings(
                    item.trigger_events
                        .into_iter()
                        .map(|value| truncate_cleaned_text(&value, 120))
                        .filter(|value| !value.is_empty())
                        .collect(),
                )
                .into_iter()
                .take(3)
                .collect(),
                recommended_channel: normalize_recommended_channel(&item.recommended_channel),
                outreach_angle: truncate_cleaned_text(&item.outreach_angle, 220),
                research_confidence: item.research_confidence.unwrap_or(0.72).clamp(0.0, 1.0),
            },
        );
    }

    Ok(out)
}

fn prospect_primary_contact_for_prompt(prospect: &SalesProspectProfile) -> String {
    match (
        prospect.primary_contact_name.as_deref(),
        prospect.primary_contact_title.as_deref(),
    ) {
        (Some(name), Some(title)) => format!("{name} / {title}"),
        (Some(name), None) => name.to_string(),
        (None, Some(title)) => title.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

fn normalize_recommended_channel(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "email" => "email".to_string(),
        "linkedin" => "linkedin".to_string(),
        "either" => "either".to_string(),
        _ => "research".to_string(),
    }
}

fn prospect_needs_llm_refresh(profile: &SalesProspectProfile) -> bool {
    if profile.research_status == "llm_enriched" {
        return false;
    }

    profile.research_confidence < 0.86
        || profile.profile_status != "contact_ready"
        || profile.source_count < 2
}

fn apply_cached_prospect_memory(profile: &mut SalesProspectProfile, cached: &SalesProspectProfile) {
    if profile.primary_contact_name.is_none()
        || contact_name_is_placeholder(profile.primary_contact_name.as_deref())
    {
        profile.primary_contact_name = cached.primary_contact_name.clone();
    }
    if contact_title_is_generic_default(profile.primary_contact_title.as_deref())
        && !contact_title_is_generic_default(cached.primary_contact_title.as_deref())
    {
        profile.primary_contact_title = cached.primary_contact_title.clone();
    }
    if profile.primary_email.is_none() {
        profile.primary_email = cached.primary_email.clone();
    }
    if profile
        .primary_linkedin_url
        .as_deref()
        .and_then(normalize_outreach_linkedin_url)
        .is_none()
    {
        profile.primary_linkedin_url = cached.primary_linkedin_url.clone();
    }
    if profile.company_linkedin_url.is_none() {
        profile.company_linkedin_url = cached.company_linkedin_url.clone();
    }
    profile.osint_links =
        merge_osint_links(profile.osint_links.clone(), cached.osint_links.clone());

    profile.fit_score = profile.fit_score.max(cached.fit_score);
    profile.contact_count = profile.contact_count.max(cached.contact_count);
    profile.source_count = profile.source_count.max(cached.source_count);
    profile.matched_signals = dedupe_strings(
        profile
            .matched_signals
            .iter()
            .chain(cached.matched_signals.iter())
            .cloned()
            .collect(),
    )
    .into_iter()
    .take(6)
    .collect();
    profile.buyer_roles = dedupe_strings(
        profile
            .buyer_roles
            .iter()
            .chain(cached.buyer_roles.iter())
            .cloned()
            .collect(),
    )
    .into_iter()
    .take(4)
    .collect();
    if profile.pain_points.is_empty() && !cached.pain_points.is_empty() {
        profile.pain_points = cached.pain_points.clone();
    }
    if profile.trigger_events.is_empty() && !cached.trigger_events.is_empty() {
        profile.trigger_events = cached.trigger_events.clone();
    }
    if profile.outreach_angle.trim().is_empty() && !cached.outreach_angle.trim().is_empty() {
        profile.outreach_angle = cached.outreach_angle.clone();
    }

    if cached.research_status == "llm_enriched"
        || cached.research_confidence >= profile.research_confidence
    {
        apply_cached_prospect_research(profile, cached);
    }

    profile.profile_status = prospect_status(
        profile.primary_contact_name.as_deref(),
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    )
    .to_string();
    profile.recommended_channel = build_recommended_channel(
        profile.primary_email.as_deref(),
        profile.primary_linkedin_url.as_deref(),
    );
    profile.summary =
        if profile.research_status == "llm_enriched" && !profile.summary.trim().is_empty() {
            profile.summary.clone()
        } else {
            build_prospect_summary(
                &profile.company,
                &profile.matched_signals,
                profile.primary_contact_name.as_deref(),
                profile.primary_contact_title.as_deref(),
                profile.primary_email.as_deref(),
                profile.primary_linkedin_url.as_deref(),
            )
        };
    profile.research_confidence = profile
        .research_confidence
        .max(heuristic_research_confidence(
            profile.fit_score,
            &profile.profile_status,
            profile.source_count as usize,
            profile.contact_count as usize,
        ));
}

fn apply_cached_prospect_research(
    profile: &mut SalesProspectProfile,
    cached: &SalesProspectProfile,
) {
    if !cached.summary.trim().is_empty() {
        profile.summary = cached.summary.clone();
    }
    if !cached.buyer_roles.is_empty() {
        profile.buyer_roles = cached.buyer_roles.clone();
    }
    if !cached.pain_points.is_empty() {
        profile.pain_points = cached.pain_points.clone();
    }
    if !cached.trigger_events.is_empty() {
        profile.trigger_events = cached.trigger_events.clone();
    }
    if !cached.outreach_angle.trim().is_empty() {
        profile.outreach_angle = cached.outreach_angle.clone();
    }
    if !cached.recommended_channel.trim().is_empty() {
        profile.recommended_channel = cached.recommended_channel.clone();
    }
    if profile.company_linkedin_url.is_none() {
        profile.company_linkedin_url = cached.company_linkedin_url.clone();
    }
    profile.osint_links =
        merge_osint_links(profile.osint_links.clone(), cached.osint_links.clone());
    profile.research_status = cached.research_status.clone();
    profile.research_confidence = cached.research_confidence.max(profile.research_confidence);
}

fn apply_llm_prospect_research(profile: &mut SalesProspectProfile, extra: &ProspectResearchExtra) {
    if !extra.summary.is_empty() {
        profile.summary = extra.summary.clone();
    }
    if !extra.buyer_roles.is_empty() {
        profile.buyer_roles = extra.buyer_roles.clone();
    }
    if !extra.pain_points.is_empty() {
        profile.pain_points = extra.pain_points.clone();
    }
    if !extra.trigger_events.is_empty() {
        profile.trigger_events = extra.trigger_events.clone();
    }
    if !extra.outreach_angle.is_empty() {
        profile.outreach_angle = extra.outreach_angle.clone();
    }
    if !extra.recommended_channel.is_empty() {
        profile.recommended_channel = extra.recommended_channel.clone();
    }
    profile.research_status = "llm_enriched".to_string();
    profile.research_confidence = extra.research_confidence.max(profile.research_confidence);
}

async fn llm_autofill_profile(_state: &AppState, brief: &str) -> Result<SalesProfileDraft, String> {
    let driver = build_sales_llm_driver(&_state.kernel.config.home_dir).await?;

    let prompt = format!(
        "Extract a high-quality outbound sales profile from the brief.\n\
         Return strict JSON only (no markdown/prose) with exact keys:\n\
         product_name, product_description, target_industry, target_geo, sender_name, sender_email, sender_linkedin,\n\
         target_title_policy, daily_target, daily_send_cap, schedule_hour_local, timezone_mode.\n\
         Rules:\n\
         - target_title_policy must be: ceo_then_founder or ceo_only\n\
         - product_description must be concise (max 450 chars), value-focused\n\
         - target_industry must reflect ideal buyers (not generic 'Technology')\n\
         - infer sender_email/sender_linkedin from brief if present\n\
         - infer geo from language/content (TR/EU/US) when possible\n\
         - if brief emphasizes field/on-site operations, reflect that in target_industry\n\
         - numeric defaults: daily_target=20, daily_send_cap=20, schedule_hour_local=9\n\
         - timezone_mode='local' unless brief clearly says otherwise\n\
         Unknown values can be empty string, but avoid empty target_industry.\n\n\
         Brief:\n{brief}"
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 700,
        temperature: 0.1,
        system: Some(
            "You are a B2B sales operations analyst. Extract precise ICP/profile fields from noisy long briefs. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::Medium),
    };

    let resp = driver
        .complete(req)
        .await
        .map_err(|e| format!("LLM autofill failed: {e}"))?;
    let text = resp.text();
    let parse_payload = |raw: &str| -> Result<SalesProfileDraft, String> {
        let json_payload = extract_json_payload(raw)
            .ok_or_else(|| "Could not parse JSON payload from LLM output".to_string())?;
        serde_json::from_str::<SalesProfileDraft>(&json_payload)
            .map_err(|e| format!("Invalid autofill JSON: {e}; payload: {json_payload}"))
    };

    match parse_payload(&text) {
        Ok(draft) => Ok(draft),
        Err(primary_err) => {
            let repair_prompt = format!(
                "Convert the following model output into strict JSON with these keys only:\n\
                 product_name, product_description, target_industry, target_geo, sender_name, sender_email, sender_linkedin,\n\
                 target_title_policy, daily_target, daily_send_cap, schedule_hour_local, timezone_mode.\n\
                 Return JSON only, no prose.\n\nOutput to repair:\n{}",
                text
            );
            let repair_req = CompletionRequest {
                model: SALES_LLM_MODEL.to_string(),
                messages: vec![LlmMessage::user(repair_prompt)],
                tools: vec![],
                max_tokens: 500,
                temperature: 0.0,
                system: Some(
                    "You are a JSON repair assistant. Always output strict valid JSON.".to_string(),
                ),
                thinking: None,
                reasoning_effort: Some(ReasoningEffort::Medium),
            };
            let repaired = driver
                .complete(repair_req)
                .await
                .map_err(|e| format!("{primary_err}; repair call failed: {e}"))?;
            parse_payload(&repaired.text())
                .map_err(|e| format!("{primary_err}; repair failed: {e}"))
        }
    }
}

pub async fn autofill_sales_profile(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SalesProfileAutofillRequest>,
) -> impl IntoResponse {
    if body.brief.trim().len() < 20 {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": "Provide a richer company brief (at least 20 chars)."}),
            ),
        );
    }

    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    if let Err(e) = engine.set_onboarding_brief(body.brief.trim()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        );
    }
    let persist = body.persist.unwrap_or(true);
    let (profile, source, warnings) =
        match apply_brief_to_profile(&state, &engine, body.brief.trim(), persist).await {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };

    let onboarding = match (engine.get_profile(), engine.get_onboarding_brief_state()) {
        (Ok(profile_opt), Ok(brief_state)) => {
            let last_run_id = engine
                .latest_successful_run_id_since(brief_state.updated_at.as_deref())
                .ok()
                .flatten();
            Some(build_onboarding_status(
                &state.kernel.config.home_dir,
                profile_opt,
                brief_state.brief,
                last_run_id,
            ))
        }
        _ => None,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "profile": profile,
            "persisted": persist,
            "source": source,
            "warnings": warnings,
            "onboarding": onboarding
        })),
    )
}

pub async fn put_sales_onboarding_brief(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SalesOnboardingBriefRequest>,
) -> impl IntoResponse {
    let brief = body.brief.trim();
    if brief.len() < 20 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Brief en az 20 karakter olmali."})),
        );
    }
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    if let Err(e) = engine.set_onboarding_brief(brief) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        );
    }
    let persist = body.persist.unwrap_or(true);
    let (profile, source, warnings) =
        match apply_brief_to_profile(&state, &engine, brief, persist).await {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };
    let brief_state = engine.get_onboarding_brief_state().ok().unwrap_or_default();
    let status = build_onboarding_status(
        &state.kernel.config.home_dir,
        engine.get_profile().ok().flatten(),
        brief_state.brief,
        engine
            .latest_successful_run_id_since(brief_state.updated_at.as_deref())
            .ok()
            .flatten(),
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "profile": profile,
            "persisted": persist,
            "source": source,
            "warnings": warnings,
            "onboarding": status
        })),
    )
}

pub async fn get_sales_onboarding_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let profile = match engine.get_profile() {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let brief_state = match engine.get_onboarding_brief_state() {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let last_successful_run_id =
        match engine.latest_successful_run_id_since(brief_state.updated_at.as_deref()) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };
    let status = build_onboarding_status(
        &state.kernel.config.home_dir,
        profile,
        brief_state.brief,
        last_successful_run_id,
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": status })),
    )
}

pub async fn get_sales_profile(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_profile() {
        Ok(profile) => (
            StatusCode::OK,
            Json(serde_json::json!({"profile": profile.unwrap_or_default()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn put_sales_profile(
    State(state): State<Arc<AppState>>,
    Json(profile): Json<SalesProfile>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let profile = match normalize_sales_profile(profile) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.upsert_profile(&profile) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "saved", "profile": profile})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn run_sales_now(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let profile = match engine.get_profile() {
        Ok(Some(profile)) => profile,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Sales profile not configured"})),
            )
        }
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    if profile.product_name.trim().is_empty()
        || profile.product_description.trim().is_empty()
        || profile.target_industry.trim().is_empty()
        || profile.target_geo.trim().is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": "Sales profile is incomplete; product_name, product_description, target_industry, and target_geo are required"}),
            ),
        );
    }

    let job_id = match engine.create_job_run("discovery") {
        Ok(job_id) => job_id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let kernel = state.kernel.clone();
    let engine_for_task = SalesEngine::new(&state.kernel.config.home_dir);
    let spawned_job_id = job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = engine_for_task
            .run_generation_with_job(&kernel, Some(&spawned_job_id))
            .await
        {
            let _ =
                engine_for_task.fail_job_stage(&spawned_job_id, PipelineStage::QueryPlanning, &err);
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "job_id": job_id,
            "status": "running",
            "current_stage": "QueryPlanning"
        })),
    )
}

pub async fn get_sales_job_progress(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_job_progress(&job_id) {
        Ok(Some(progress)) => (StatusCode::OK, Json(serde_json::json!(progress))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Job not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_active_sales_job_progress(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.latest_running_job_progress("discovery") {
        Ok(progress) => (StatusCode::OK, Json(serde_json::json!({ "job": progress }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn retry_sales_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
    Json(body): Json<JobRetryRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let resume_stage = if body.force_fresh {
        None
    } else {
        engine
            .latest_completed_checkpoint(&job_id)
            .ok()
            .flatten()
            .map(|(stage, _)| stage.as_str().to_string())
    };
    let new_job_id = match engine.create_job_run("discovery") {
        Ok(job_id) => job_id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let kernel = state.kernel.clone();
    let engine_for_task = SalesEngine::new(&state.kernel.config.home_dir);
    let spawned_job_id = new_job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = engine_for_task
            .run_generation_with_job(&kernel, Some(&spawned_job_id))
            .await
        {
            let _ =
                engine_for_task.fail_job_stage(&spawned_job_id, PipelineStage::QueryPlanning, &err);
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "job_id": new_job_id,
            "status": "running",
            "resumed_from_stage": resume_stage,
            "replayed_from_scratch": true
        })),
    )
}

pub async fn list_sales_source_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.list_source_health() {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"sources": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_policy_proposals(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesPolicyProposalQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_policy_proposals(q.status.as_deref(), limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposals": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn approve_sales_policy_proposal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.update_policy_proposal_status(&id, "active", Some("operator")) {
        Ok(Some(proposal)) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposal": proposal})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Policy proposal not found"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn reject_sales_policy_proposal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.update_policy_proposal_status(&id, "retired", None) {
        Ok(Some(proposal)) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposal": proposal})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Policy proposal not found"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn sales_unsubscribe(
    State(state): State<Arc<AppState>>,
    Query(query): Query<UnsubscribeQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(_e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("<html><body><h1>OpenFang</h1><p>Unsubscribe service unavailable.</p></body></html>".to_string()),
            )
        }
    };

    let Some(email) = verify_unsubscribe_token(&query.token) else {
        return (
            StatusCode::BAD_REQUEST,
            Html(
                "<html><body><h1>OpenFang</h1><p>Invalid unsubscribe token.</p></body></html>"
                    .to_string(),
            ),
        );
    };

    let conn = match engine.open() {
        Ok(conn) => conn,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!(
                    "<html><body><h1>OpenFang</h1><p>{}</p></body></html>",
                    e
                )),
            )
        }
    };
    if let Err(e) = engine.suppress_contact(&conn, &email, "one_click_unsubscribe", true, None) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!(
                "<html><body><h1>OpenFang</h1><p>{}</p></body></html>",
                e
            )),
        );
    }
    let _ = conn.execute(
        "UPDATE contact_methods SET suppressed = 1 WHERE value = ?1",
        params![email.trim().to_lowercase()],
    );
    (
        StatusCode::OK,
        Html(format!(
            "<html><body><h1>OpenFang</h1><p>{} artik kalici olarak suppression listesinde. Bu aliciya tekrar gonderim yapilmayacak.</p></body></html>",
            email
        )),
    )
}

pub async fn sales_outcomes_webhook(
    State(state): State<Arc<AppState>>,
    Json(body): Json<OutcomeWebhookRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.ingest_outcome_event(&body.delivery_id, &body.event_type, &body.raw_text) {
        Ok(result) => {
            // After outcome ingestion, advance sequences (TASK-30)
            let advanced = engine.advance_sequences().unwrap_or(0);
            (
                StatusCode::OK,
                Json(serde_json::json!({"result": result, "sequences_advanced": advanced})),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn advance_sales_sequences(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match engine.advance_sequences() {
        Ok(count) => (StatusCode::OK, Json(serde_json::json!({"advanced": count}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

// --- Experiment endpoints (TASK-37) ---

pub async fn list_sales_experiments(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT id, name, hypothesis, variant_a, variant_b, status, created_at
         FROM experiments ORDER BY created_at DESC LIMIT 50",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    };
    let experiments: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "id": r.get::<_, String>(0)?,
                "name": r.get::<_, String>(1)?,
                "hypothesis": r.get::<_, Option<String>>(2)?,
                "variant_a": r.get::<_, Option<String>>(3)?,
                "variant_b": r.get::<_, Option<String>>(4)?,
                "status": r.get::<_, String>(5)?,
                "created_at": r.get::<_, String>(6)?,
            }))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    (
        StatusCode::OK,
        Json(serde_json::json!({"experiments": experiments})),
    )
}

pub async fn create_sales_experiment(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let name = body["name"].as_str().unwrap_or("unnamed");
    let hypothesis = body["hypothesis"].as_str().unwrap_or("");
    let variant_a = body["variant_a"].as_str().unwrap_or("control");
    let variant_b = body["variant_b"].as_str().unwrap_or("treatment");
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match create_experiment(&conn, name, hypothesis, variant_a, variant_b) {
        Ok(id) => (
            StatusCode::OK,
            Json(serde_json::json!({"id": id, "status": "active"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_sales_experiment_results(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match get_experiment_results(&conn, &id) {
        Ok(results) => (StatusCode::OK, Json(results)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

// --- Context Factors endpoint (TASK-35) ---

pub async fn list_sales_context_factors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let bad_timing = is_bad_timing_today(&conn);
    let budget_quarter = current_budget_quarter(&conn);
    let mut stmt = match conn.prepare(
        "SELECT id, factor_type, factor_key, factor_value, effective_from, effective_until, source
         FROM contextual_factors ORDER BY effective_from",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    };
    let factors: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "id": r.get::<_, String>(0)?,
                "factor_type": r.get::<_, String>(1)?,
                "factor_key": r.get::<_, String>(2)?,
                "factor_value": r.get::<_, Option<String>>(3)?,
                "effective_from": r.get::<_, Option<String>>(4)?,
                "effective_until": r.get::<_, Option<String>>(5)?,
                "source": r.get::<_, Option<String>>(6)?,
            }))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "factors": factors,
            "bad_timing_today": bad_timing,
            "current_budget_quarter": budget_quarter,
        })),
    )
}

// --- Score Calibration endpoint (TASK-36) ---

pub async fn run_sales_calibration(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match calibrate_scoring_from_outcomes(&conn) {
        Ok(proposals) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposals": proposals, "count": proposals.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_runs(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_runs(limit) {
        Ok(runs) => (
            StatusCode::OK,
            Json(serde_json::json!({"runs": runs, "total": runs.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_leads(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_leads(limit, q.run_id.as_deref()) {
        Ok(leads) => (
            StatusCode::OK,
            Json(serde_json::json!({"leads": leads, "total": leads.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_prospects(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_prospect_profiles(limit, q.run_id.as_deref()) {
        Ok(prospects) => (
            StatusCode::OK,
            Json(serde_json::json!({"prospects": prospects, "total": prospects.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_approvals(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesApprovalQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_approvals(q.status.as_deref(), limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"approvals": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn bulk_approve_sales_approvals(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SalesApprovalBulkApproveRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let ids = dedupe_strings(body.ids);
    if ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "ids must not be empty"})),
        );
    }

    let mut approved = Vec::<serde_json::Value>::new();
    let mut failed = Vec::<serde_json::Value>::new();
    for id in ids {
        match engine.approve_and_send(&state, &id).await {
            Ok(result) => approved.push(serde_json::json!({
                "id": id,
                "result": result,
            })),
            Err(error) => failed.push(serde_json::json!({
                "id": id,
                "error": error,
            })),
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "approved": approved,
            "failed": failed,
            "approved_count": approved.len(),
            "failed_count": failed.len(),
        })),
    )
}

pub async fn edit_sales_approval(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SalesApprovalEditRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.edit_approval(&id, body.edited_payload) {
        Ok(approval) => (
            StatusCode::OK,
            Json(serde_json::json!({"approval": approval})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_sales_account_dossier(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_account_dossier(&id) {
        Ok(Some(dossier)) => (
            StatusCode::OK,
            Json(serde_json::json!({"dossier": dossier})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Account dossier not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn approve_and_send(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.approve_and_send(&state, &id).await {
        Ok(result) => (StatusCode::OK, Json(serde_json::json!({"result": result}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn reject_sales_approval(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(_body): Json<SalesRejectRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.reject_approval(&id) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "rejected"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_deliveries(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_deliveries(limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"deliveries": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub fn spawn_sales_scheduler(kernel: Arc<openfang_kernel::OpenFangKernel>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await;

            let engine = SalesEngine::new(&kernel.config.home_dir);
            if let Err(e) = engine.init() {
                warn!(error = %e, "Sales scheduler: DB init failed");
                continue;
            }

            let profile = match engine.get_profile() {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, "Sales scheduler: profile read failed");
                    continue;
                }
            };

            let now = Local::now();
            if now.hour() as u8 != profile.schedule_hour_local || now.minute() > 10 {
                continue;
            }

            match engine.already_ran_today(&profile.timezone_mode) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    warn!(error = %e, "Sales scheduler: run-day check failed");
                    continue;
                }
            }

            info!("Sales scheduler: triggering daily run");
            match tokio::time::timeout(Duration::from_secs(120), engine.run_generation(&kernel))
                .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => error!(error = %e, "Sales scheduler: run failed"),
                Err(_) => error!("Sales scheduler: run timed out"),
            }
        }
    });
}

trait OptionalRow<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalRow<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sales_profile_draft_accepts_empty_numeric_strings() {
        let payload = r#"{
          "product_name": "Machinity",
          "product_description": "AI operations teammate",
          "target_industry": "",
          "target_geo": "",
          "sender_name": "",
          "sender_email": "",
          "sender_linkedin": "",
          "target_title_policy": "",
          "daily_target": "",
          "daily_send_cap": "",
          "schedule_hour_local": "",
          "timezone_mode": ""
        }"#;
        let parsed: SalesProfileDraft = serde_json::from_str(payload).expect("draft parse");
        assert_eq!(parsed.daily_target, None);
        assert_eq!(parsed.daily_send_cap, None);
        assert_eq!(parsed.schedule_hour_local, None);
    }

    #[test]
    fn truncate_text_for_reason_handles_utf8_safely() {
        let text = "Saha operasyonu yönetimi için çok güçlü otomasyon";
        let out = truncate_text_for_reason(text, 11);
        assert!(!out.is_empty());
    }

    #[test]
    fn candidate_field_ops_signal_ignores_only_generic_keywords() {
        let only_generic = DomainCandidate {
            domain: "example.com".to_string(),
            score: 42,
            evidence: vec!["B2B workflow automation".to_string()],
            matched_keywords: vec!["Field Operations".to_string()],
            source_links: Vec::new(),
            phone: None,
        };
        assert!(!candidate_has_field_ops_signal(&only_generic));
    }

    #[test]
    fn candidate_field_ops_signal_recognizes_platform_keywords() {
        let platform_company = DomainCandidate {
            domain: "example.com".to_string(),
            score: 42,
            evidence: vec!["Platform rental and forklift service".to_string()],
            matched_keywords: vec!["equipment rental".to_string()],
            source_links: Vec::new(),
            phone: None,
        };
        assert!(candidate_has_field_ops_signal(&platform_company));
    }

    #[test]
    fn energy_target_detection_does_not_trigger_for_plain_field_ops_profile() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "Dispatch ve saha operasyon koordinasyonu".to_string(),
            target_industry: "Construction and field service".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 5,
            daily_send_cap: 5,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };
        assert!(profile_targets_field_ops(&profile));
        assert!(!profile_targets_energy(&profile));
        assert_eq!(candidate_quality_floor(&profile), 12);
    }

    #[test]
    fn blocked_company_domain_rejects_global_giants() {
        assert!(is_blocked_company_domain("boschrexroth.com"));
        assert!(is_blocked_company_domain("cargill.com.tr"));
        assert!(is_blocked_company_domain("gmail.com"));
        assert!(is_blocked_company_domain("outlook.com"));
        assert!(!is_blocked_company_domain("altanhidrolik.com.tr"));
    }

    #[test]
    fn candidate_should_skip_for_profile_rejects_holdings_for_field_ops() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "Field ops coordination".to_string(),
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        assert!(candidate_should_skip_for_profile(
            &DomainCandidate {
                domain: "celiklerholding.net".to_string(),
                score: 40,
                evidence: vec![],
                matched_keywords: vec![],
                source_links: Vec::new(),
                phone: None,
            },
            &profile,
        ));
        assert!(!candidate_should_skip_for_profile(
            &DomainCandidate {
                domain: "ekseninsaat.com.tr".to_string(),
                score: 40,
                evidence: vec![],
                matched_keywords: vec![],
                source_links: Vec::new(),
                phone: None,
            },
            &profile,
        ));
    }

    #[test]
    fn normalize_person_name_rejects_business_phrase() {
        assert!(normalize_person_name("TechEx Sustainable Legacies Welcoming Workplace").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_department_and_office_labels() {
        assert!(normalize_person_name("Ankara Ofisi Türkiye").is_none());
        assert!(normalize_person_name("Basın Odası").is_none());
        assert!(normalize_person_name("Kurumsal Liderlik").is_none());
        assert!(normalize_person_name("Haber Finansal").is_none());
        assert!(normalize_person_name("Kişisel Verilerin Korunması Kanunu").is_none());
        assert!(normalize_person_name("Uluslararası Kredi Kuruluşları").is_none());
        assert!(normalize_person_name("Faaliyet Alanları").is_none());
        assert!(normalize_person_name("Suudi Arabistan").is_none());
        assert!(normalize_person_name("Bu Web Sitesinde Çerezler Kullanılmaktadır").is_none());
        assert!(normalize_person_name("Türk Sanayisinde Rönesans Dönemi").is_none());
        assert!(normalize_person_name("Tümüne İzin Ver").is_none());
        assert!(normalize_person_name("Sürdürülebilirlik Sosyal Sorumluluk").is_none());
        assert!(normalize_person_name("Costa Rica").is_none());
        assert!(normalize_person_name("Onursal Başkanımız").is_none());
        assert!(normalize_person_name("Paylaşma İklimi").is_none());
        assert!(normalize_person_name("Defa Kamunun Muhtelif İdarelerinden").is_none());
        assert!(normalize_person_name("İşi Veren İdare").is_none());
        assert!(normalize_person_name("İhale Yılı").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_css_fragments() {
        assert!(normalize_person_name("P Style").is_none());
        assert!(normalize_person_name("Font Verdana").is_none());
        assert!(normalize_person_name("Div Class").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_photo_caption_fragments() {
        assert!(normalize_person_name("Oturanlar Soldan Sağa").is_none());
        assert!(normalize_person_name("Ayakta Soldan Saga").is_none());
        assert!(normalize_person_name("Nasil Basladik").is_none());
        assert!(normalize_person_name("Hedefimiz Politikamiz").is_none());
        assert!(normalize_person_name("CEO Aslan Uzun").is_none());
    }

    #[test]
    fn contact_title_priority_rejects_field_signal_headlines() {
        assert_eq!(
            contact_title_priority("Yapi Merkezi teknik servis ve santiye operasyonlari"),
            0
        );
    }

    #[test]
    fn extract_contact_from_search_supports_unicode_name_patterns() {
        let sample = r#"
1. Enerjisa Leadership Team
   URL: https://www.enerjisa.com.tr/leadership
   CEO: Emre Erdoğan
"#;
        let (name, title, _) = extract_contact_from_search(sample, "ceo_only");
        assert_eq!(name.as_deref(), Some("Emre Erdoğan"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn extract_contact_from_search_supports_turkish_titles() {
        let sample = r#"
1. Yapı Merkezi Yönetim
   URL: https://www.yapimerkezi.com.tr/yonetim
   Genel Müdür Aslan Uzun
"#;
        let (name, title, _) = extract_contact_from_search(sample, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Aslan Uzun"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn build_prospect_profiles_aggregates_leads_into_company_profiles() {
        let leads = vec![
            SalesLead {
                id: "lead-1".to_string(),
                run_id: "run-1".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Unknown".to_string(),
                contact_title: "".to_string(),
                linkedin_url: None,
                email: None,
                phone: None,
                reasons: vec![
                    "Field service operations".to_string(),
                    "Dispatch automation".to_string(),
                ],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 61,
                status: "new".to_string(),
                created_at: "2026-03-20T09:00:00Z".to_string(),
            },
            SalesLead {
                id: "lead-2".to_string(),
                run_id: "run-2".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Aylin Demir".to_string(),
                contact_title: "COO".to_string(),
                linkedin_url: Some("https://www.linkedin.com/in/aylindemir".to_string()),
                email: Some("aylin@acme.example".to_string()),
                phone: None,
                reasons: vec![
                    "Field service operations".to_string(),
                    "Maintenance dispatch".to_string(),
                ],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 78,
                status: "new".to_string(),
                created_at: "2026-03-21T10:30:00Z".to_string(),
            },
            SalesLead {
                id: "lead-3".to_string(),
                run_id: "run-2".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Mert Kaya".to_string(),
                contact_title: "Head of Operations".to_string(),
                linkedin_url: None,
                email: None,
                phone: None,
                reasons: vec!["On-site maintenance teams".to_string()],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 72,
                status: "new".to_string(),
                created_at: "2026-03-21T11:00:00Z".to_string(),
            },
        ];

        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Field operations teams icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_prospect_profiles(leads, 10, Some(&sales_profile));

        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.id, "acme.example");
        assert_eq!(profile.run_id, "run-2");
        assert_eq!(profile.company, "Acme Field Ops");
        assert_eq!(profile.fit_score, 78);
        assert_eq!(profile.profile_status, "contact_ready");
        assert_eq!(profile.primary_contact_name.as_deref(), Some("Aylin Demir"));
        assert_eq!(profile.primary_contact_title.as_deref(), Some("COO"));
        assert_eq!(profile.primary_email.as_deref(), Some("aylin@acme.example"));
        assert_eq!(
            profile.primary_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/in/aylindemir")
        );
        assert_eq!(profile.contact_count, 2);
        assert!(profile
            .matched_signals
            .iter()
            .any(|signal| signal.contains("Field service operations")));
        assert!(profile
            .matched_signals
            .iter()
            .any(|signal| signal.contains("Maintenance dispatch")));
        assert!(profile.summary.contains("Acme Field Ops"));
        assert_eq!(profile.research_status, "heuristic");
        assert!(profile.research_confidence > 0.5);
        assert!(profile.source_count >= 2);
        assert!(!profile.buyer_roles.is_empty());
        assert!(!profile.pain_points.is_empty());
        assert!(!profile.trigger_events.is_empty());
        assert!(!profile.outreach_angle.is_empty());
        assert_eq!(profile.created_at, "2026-03-20T09:00:00Z");
        assert_eq!(profile.updated_at, "2026-03-21T11:00:00Z");
    }

    #[test]
    fn build_candidate_prospect_profiles_creates_company_only_dossiers() {
        let candidates = vec![DomainCandidate {
            domain: "ornekbakim.com.tr".to_string(),
            score: 37,
            evidence: vec![
                "Confirmed by multiple discovery sources".to_string(),
                "Maintenance dispatch and field teams".to_string(),
            ],
            matched_keywords: vec![
                "field service".to_string(),
                "maintenance".to_string(),
                "facility operations".to_string(),
            ],
            source_links: vec!["https://www.tmb.org.tr/en/members".to_string()],
            phone: None,
        }];
        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Saha ekipleri icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_candidate_prospect_profiles(
            "run-prospect",
            &candidates,
            &HashMap::new(),
            10,
            &sales_profile,
        );

        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.run_id, "run-prospect");
        assert_eq!(profile.company_domain, "ornekbakim.com.tr");
        assert_eq!(profile.profile_status, "company_only");
        assert!(profile.primary_contact_name.is_none());
        assert!(profile.primary_email.is_none());
        assert!(!profile.summary.is_empty());
        assert!(!profile.matched_signals.is_empty());
        assert!(!profile.pain_points.is_empty());
        assert!(!profile.trigger_events.is_empty());
        assert_eq!(profile.research_status, "heuristic");
        assert!(profile
            .osint_links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/members"));
    }

    #[test]
    fn build_candidate_prospect_profiles_prioritizes_actionable_local_accounts() {
        let candidates = vec![
            DomainCandidate {
                domain: "celiklerholding.net".to_string(),
                score: 58,
                evidence: vec!["Corporate group overview".to_string()],
                matched_keywords: vec!["field service".to_string()],
                source_links: Vec::new(),
                phone: None,
            },
            DomainCandidate {
                domain: "ornekbakim.com.tr".to_string(),
                score: 39,
                evidence: vec!["Maintenance dispatch teams".to_string()],
                matched_keywords: vec!["field service".to_string(), "maintenance".to_string()],
                source_links: vec!["https://www.asmud.org.tr/Uyeler.asp".to_string()],
                phone: None,
            },
        ];
        let mut hints = HashMap::new();
        hints.insert(
            "ornekbakim.com.tr".to_string(),
            SourceContactHint {
                contact_name: Some("Aylin Demir".to_string()),
                contact_title: Some("COO".to_string()),
                email: Some("aylin@ornekbakim.com.tr".to_string()),
                source: Some("ASMUD members page".to_string()),
            },
        );
        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Saha ekipleri icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_candidate_prospect_profiles(
            "run-priority",
            &candidates,
            &hints,
            10,
            &sales_profile,
        );

        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].company_domain, "ornekbakim.com.tr");
        assert_eq!(profiles[0].profile_status, "contact_ready");
        assert_eq!(profiles[1].company_domain, "celiklerholding.net");
    }

    #[test]
    fn apply_cached_prospect_memory_reuses_contact_ready_cache() {
        let mut current = SalesProspectProfile {
            id: "ornekbakim.com.tr".to_string(),
            run_id: "run-current".to_string(),
            company: "Ornekbakim".to_string(),
            website: "https://ornekbakim.com.tr".to_string(),
            company_domain: "ornekbakim.com.tr".to_string(),
            fit_score: 44,
            profile_status: "company_only".to_string(),
            summary: "Current heuristic summary".to_string(),
            matched_signals: vec!["maintenance".to_string()],
            primary_contact_name: None,
            primary_contact_title: None,
            primary_email: None,
            primary_linkedin_url: None,
            company_linkedin_url: None,
            osint_links: vec!["https://ornekbakim.com.tr".to_string()],
            contact_count: 0,
            source_count: 1,
            buyer_roles: vec!["COO".to_string()],
            pain_points: vec![],
            trigger_events: vec![],
            recommended_channel: "research".to_string(),
            outreach_angle: String::new(),
            research_status: "heuristic".to_string(),
            research_confidence: 0.42,
            tech_stack: Vec::new(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
            updated_at: "2026-03-25T10:00:00Z".to_string(),
        };
        let cached = SalesProspectProfile {
            id: "ornekbakim.com.tr".to_string(),
            run_id: "run-cached".to_string(),
            company: "Ornekbakim".to_string(),
            website: "https://ornekbakim.com.tr".to_string(),
            company_domain: "ornekbakim.com.tr".to_string(),
            fit_score: 61,
            profile_status: "contact_ready".to_string(),
            summary: "Cached strong dossier".to_string(),
            matched_signals: vec!["dispatch".to_string(), "maintenance".to_string()],
            primary_contact_name: Some("Aylin Demir".to_string()),
            primary_contact_title: Some("COO".to_string()),
            primary_email: Some("aylin@ornekbakim.com.tr".to_string()),
            primary_linkedin_url: None,
            company_linkedin_url: Some("https://www.linkedin.com/company/ornekbakim/".to_string()),
            osint_links: vec![
                "https://ornekbakim.com.tr".to_string(),
                "https://www.linkedin.com/company/ornekbakim/".to_string(),
            ],
            contact_count: 1,
            source_count: 2,
            buyer_roles: vec!["COO".to_string(), "Head of Operations".to_string()],
            pain_points: vec!["Dispatch visibility".to_string()],
            trigger_events: vec!["Public field ops signal".to_string()],
            recommended_channel: "email".to_string(),
            outreach_angle: "Lead with dispatch coordination".to_string(),
            research_status: "heuristic".to_string(),
            research_confidence: 0.83,
            tech_stack: Vec::new(),
            created_at: "2026-03-24T10:00:00Z".to_string(),
            updated_at: "2026-03-24T10:00:00Z".to_string(),
        };

        apply_cached_prospect_memory(&mut current, &cached);

        assert_eq!(current.profile_status, "contact_ready");
        assert_eq!(current.primary_contact_name.as_deref(), Some("Aylin Demir"));
        assert_eq!(current.primary_contact_title.as_deref(), Some("COO"));
        assert_eq!(
            current.primary_email.as_deref(),
            Some("aylin@ornekbakim.com.tr")
        );
        assert!(current
            .matched_signals
            .iter()
            .any(|signal| signal == "dispatch"));
        assert!(current.research_confidence >= 0.83);
        assert_eq!(current.recommended_channel, "email");
    }

    #[test]
    fn latest_successful_run_id_since_accepts_profile_only_runs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run().expect("begin run");
        engine
            .finish_run(&run_id, "completed", 9, 0, 0, Some("profile only"))
            .expect("finish run");
        engine
            .upsert_prospect_profiles(&[SalesProspectProfile {
                id: "ornekbakim.com.tr".to_string(),
                run_id: run_id.clone(),
                company: "Ornekbakim".to_string(),
                website: "https://ornekbakim.com.tr".to_string(),
                company_domain: "ornekbakim.com.tr".to_string(),
                fit_score: 42,
                profile_status: "company_only".to_string(),
                summary: "Profiling completed.".to_string(),
                matched_signals: vec!["maintenance".to_string()],
                primary_contact_name: None,
                primary_contact_title: None,
                primary_email: None,
                primary_linkedin_url: None,
                company_linkedin_url: None,
                osint_links: vec!["https://ornekbakim.com.tr".to_string()],
                contact_count: 0,
                source_count: 1,
                buyer_roles: vec!["COO".to_string()],
                pain_points: vec!["Dispatch visibility".to_string()],
                trigger_events: vec!["Public field ops signal".to_string()],
                recommended_channel: "research".to_string(),
                outreach_angle: "Lead with dispatch coordination".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.74,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let last_run_id = engine
            .latest_successful_run_id_since(None)
            .expect("latest successful run");

        assert_eq!(last_run_id.as_deref(), Some(run_id.as_str()));
    }

    #[test]
    fn recover_latest_timed_out_run_completes_partial_marketing_progress() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run().expect("begin run");
        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations signal".to_string()],
            email_subject: "Machinity for field ops".to_string(),
            email_body: "Hi Aylin".to_string(),
            linkedin_message: "Hi Aylin".to_string(),
            score: 92,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        let queued = engine
            .queue_approvals_for_lead(&lead)
            .expect("queue approvals");
        assert_eq!(queued, 1);
        engine
            .upsert_prospect_profiles(&[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: Some(
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ),
                osint_links: vec![
                    "https://machinity.ai".to_string(),
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let recovered = engine
            .recover_latest_timed_out_run()
            .expect("recover timed out run")
            .expect("running run exists");

        assert_eq!(recovered.id, run_id);
        assert_eq!(recovered.status, "completed");
        assert_eq!(recovered.inserted, 1);
        assert_eq!(recovered.discovered, 1);
        assert_eq!(recovered.approvals_queued, 1);
        assert!(recovered
            .error
            .as_deref()
            .map(|value| value.contains("saving progress"))
            .unwrap_or(false));

        let runs = engine.list_runs(5).expect("list runs");
        assert_eq!(runs[0].status, "completed");
        assert_eq!(runs[0].inserted, 1);
        assert_eq!(runs[0].approvals_queued, 1);
    }

    #[test]
    fn recover_latest_timed_out_run_fails_when_nothing_was_persisted() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run().expect("begin run");

        let recovered = engine
            .recover_latest_timed_out_run()
            .expect("recover timed out run")
            .expect("running run exists");

        assert_eq!(recovered.id, run_id);
        assert_eq!(recovered.status, "failed");
        assert_eq!(recovered.inserted, 0);
        assert_eq!(recovered.discovered, 0);
        assert_eq!(recovered.approvals_queued, 0);
        assert!(recovered
            .error
            .as_deref()
            .map(|value| value.contains("before any durable prospect dossiers"))
            .unwrap_or(false));
    }

    #[test]
    fn recover_latest_timed_out_run_if_stale_honors_age_threshold() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run().expect("begin run");
        engine
            .upsert_prospect_profiles(&[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: Some(
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ),
                osint_links: vec!["https://machinity.ai".to_string()],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let skipped = engine
            .recover_latest_timed_out_run_if_stale(60)
            .expect("conditional recover");
        assert!(skipped.is_none());
        let runs = engine.list_runs(5).expect("list runs");
        assert_eq!(runs[0].status, "running");

        let conn = engine.open().expect("open db");
        let old_started_at = (Utc::now() - chrono::Duration::seconds(120)).to_rfc3339();
        conn.execute(
            "UPDATE sales_runs SET started_at = ? WHERE id = ?",
            params![old_started_at, run_id],
        )
        .expect("age run");

        let recovered = engine
            .recover_latest_timed_out_run_if_stale(60)
            .expect("conditional recover")
            .expect("stale run recovered");
        assert_eq!(recovered.status, "completed");
        let runs = engine.list_runs(5).expect("list runs");
        assert_eq!(runs[0].status, "completed");
    }

    #[test]
    fn list_runs_surfaces_live_progress_for_running_run() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run().expect("begin run");
        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations signal".to_string()],
            email_subject: "Machinity for field ops".to_string(),
            email_body: "Hi Aylin".to_string(),
            linkedin_message: "Hi Aylin".to_string(),
            score: 92,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        assert_eq!(
            engine
                .queue_approvals_for_lead(&lead)
                .expect("queue approvals"),
            1
        );
        engine
            .upsert_prospect_profiles(&[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: None,
                osint_links: vec!["https://machinity.ai".to_string()],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let runs = engine.list_runs(5).expect("list runs");
        assert_eq!(runs[0].status, "running");
        assert_eq!(runs[0].inserted, 1);
        assert_eq!(runs[0].discovered, 1);
        assert_eq!(runs[0].approvals_queued, 1);
    }

    #[test]
    fn job_progress_surfaces_checkpoints_and_active_lookup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let job_id = engine.create_job_run("discovery").expect("create job");
        engine
            .set_job_stage_running(&job_id, PipelineStage::LeadGeneration)
            .expect("set stage running");
        engine
            .update_job_stage_checkpoint(
                &job_id,
                PipelineStage::LeadGeneration,
                &LeadGenerationCheckpoint {
                    total_candidates: 18,
                    processed_candidates: 7,
                    profiled_accounts: 11,
                    inserted: 2,
                    approvals_queued: 1,
                    current_domain: Some("ornek.com".to_string()),
                },
            )
            .expect("update checkpoint");

        let progress = engine
            .get_job_progress(&job_id)
            .expect("get job progress")
            .expect("job exists");
        assert_eq!(progress.status, "running");
        assert_eq!(progress.current_stage.as_deref(), Some("LeadGeneration"));

        let lead_generation = progress
            .stages
            .iter()
            .find(|stage| stage.name == "LeadGeneration")
            .expect("lead generation stage");
        let checkpoint = lead_generation
            .checkpoint
            .as_ref()
            .expect("checkpoint attached");
        assert_eq!(
            checkpoint
                .get("processed_candidates")
                .and_then(|value| value.as_u64()),
            Some(7)
        );
        assert_eq!(
            checkpoint
                .get("current_domain")
                .and_then(|value| value.as_str()),
            Some("ornek.com")
        );

        let active = engine
            .latest_running_job_progress("discovery")
            .expect("active lookup")
            .expect("running job");
        assert_eq!(active.job_id, job_id);
    }

    #[test]
    fn list_approvals_skips_non_actionable_email_payloads() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let conn = engine.open().expect("open db");
        let created_at = "2026-03-26T10:00:00Z";
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-bad-generic",
                "lead-1",
                serde_json::json!({
                    "to": "info@acme.example",
                    "subject": "Generic subject",
                    "body": "Generic body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert generic approval");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-bad-consumer",
                "lead-2",
                serde_json::json!({
                    "to": "owner@gmail.com",
                    "subject": "Consumer subject",
                    "body": "Consumer body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert consumer approval");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-good",
                "lead-3",
                serde_json::json!({
                    "to": "eray@artiplatform.com.tr",
                    "subject": "Relevant subject",
                    "body": "Relevant body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert valid approval");

        let approvals = engine
            .list_approvals(Some("pending"), 10)
            .expect("list approvals");
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].id, "approval-good");
        assert_eq!(
            approvals[0]
                .payload
                .get("to")
                .and_then(|value| value.as_str()),
            Some("eray@artiplatform.com.tr")
        );
    }

    #[test]
    fn edit_approval_updates_touch_payload_and_returns_sanitized_payload() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: "run-1".to_string(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: Some("https://www.linkedin.com/in/aylin-demir/".to_string()),
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations expansion".to_string()],
            email_subject: "Original subject".to_string(),
            email_body: "Original body".to_string(),
            linkedin_message: "Original LinkedIn".to_string(),
            score: 91,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T11:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        assert_eq!(
            engine
                .queue_approvals_for_lead(&lead)
                .expect("queue approvals"),
            2
        );

        let approval = engine
            .list_approvals(Some("pending"), 10)
            .expect("list approvals")
            .into_iter()
            .find(|item| item.channel == "email")
            .expect("email approval");

        let edited = engine
            .edit_approval(
                &approval.id,
                serde_json::json!({
                    "to": "aylin@machinity.ai",
                    "subject": "Updated subject",
                    "body": "Updated first line\n\nUpdated rest of body",
                }),
            )
            .expect("edit approval");

        assert_eq!(
            edited
                .payload
                .get("subject")
                .and_then(|value| value.as_str()),
            Some("Updated subject")
        );
        assert_eq!(
            edited.payload.get("body").and_then(|value| value.as_str()),
            Some("Updated first line\n\nUpdated rest of body")
        );

        let conn = engine.open().expect("open");
        let touch_payload: String = conn
            .query_row(
                "SELECT message_payload FROM touches WHERE id = ?1",
                params![approval.id],
                |row| row.get(0),
            )
            .expect("touch payload");
        let touch_payload: serde_json::Value =
            serde_json::from_str(&touch_payload).expect("decode touch payload");
        assert_eq!(
            touch_payload
                .get("subject")
                .and_then(|value| value.as_str()),
            Some("Updated subject")
        );
    }

    #[test]
    fn ensure_default_sequence_template_uses_five_step_playbook() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let conn = engine.open().expect("open");
        let template_id = engine
            .ensure_default_sequence_template(&conn)
            .expect("template");
        let steps_json: String = conn
            .query_row(
                "SELECT steps_json FROM sequence_templates WHERE id = ?1",
                params![template_id],
                |row| row.get(0),
            )
            .expect("steps json");
        let steps: serde_json::Value =
            serde_json::from_str(&steps_json).expect("decode steps json");
        let steps = steps.as_array().expect("steps array");
        assert_eq!(steps.len(), 5);
        assert_eq!(
            steps[0].get("channel").and_then(|value| value.as_str()),
            Some("email")
        );
        assert_eq!(
            steps[3].get("channel").and_then(|value| value.as_str()),
            Some("linkedin_assist")
        );
    }

    #[test]
    fn select_accounts_for_activation_logs_mid_score_exploration() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");
        let conn = engine.open().expect("open");
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params!["acct-mid", "Mid Score Account", "https://mid.example", now],
        )
        .expect("insert account");
        conn.execute(
            "INSERT INTO score_snapshots
             (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk,
              compliance_risk, activation_priority, computed_at, scoring_version)
             VALUES (?1, ?2, 0.55, 0.42, 0.51, 0.12, 0.08, 0.61, ?3, 'v1')",
            params!["score-mid", "acct-mid", now],
        )
        .expect("insert score");

        let selected = engine
            .select_accounts_for_activation(
                &conn,
                &HashMap::from([("acct-mid".to_string(), 61_i64)]),
                1,
            )
            .expect("select activation");
        assert_eq!(selected, vec!["acct-mid".to_string()]);

        let exploration_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM exploration_log WHERE account_id = ?1",
                params!["acct-mid"],
                |row| row.get(0),
            )
            .expect("count exploration log");
        assert_eq!(exploration_count, 1);
    }

    #[test]
    fn missed_signal_review_creates_policy_proposal_and_supports_lifecycle() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");
        let conn = engine.open().expect("open");
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params!["acct-policy", "Policy Account", "https://policy.example", now],
        )
        .expect("insert account");
        conn.execute(
            "INSERT INTO signals
             (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
             VALUES (?1, ?2, 'job_posting', 'Hiring dispatch operators', 'site_html', ?3, 0.9, 'campaign_window', NULL, ?3)",
            params!["signal-job-posting", "acct-policy", now],
        )
        .expect("insert signal");
        conn.execute(
            "INSERT INTO touches
             (id, sequence_instance_id, step, channel, message_payload, claims_json, evidence_ids,
              variant_id, risk_flags, sent_at, mailbox_id, created_at)
             VALUES (?1, NULL, 1, 'email', '{}', '[]', '[]', 'v1', '[]', NULL, NULL, ?2)",
            params!["touch-1", now],
        )
        .expect("insert touch");
        conn.execute(
            "INSERT INTO outcomes
             (id, touch_id, outcome_type, raw_text, classified_at, classifier_confidence)
             VALUES (?1, ?2, 'meeting_booked', 'Positive reply', ?3, 1.0)",
            params!["outcome-1", "touch-1", now],
        )
        .expect("insert outcome");
        conn.execute(
            "INSERT INTO outcome_attribution_snapshots
             (id, touch_id, account_id, snapshot_at, score_at_touch_json, active_signal_ids, unused_signal_ids,
              thesis_id, sequence_variant, message_variant, channel, mailbox_id, contextual_factors_json)
             VALUES (?1, ?2, ?3, ?4, '{}', '[]', '[]', NULL, 'default', 'v1', 'email', NULL, '{}')",
            params!["snapshot-1", "touch-1", "acct-policy", now],
        )
        .expect("insert snapshot");

        engine
            .record_missed_signal_review(
                &conn,
                "outcome-1",
                "snapshot-1",
                "acct-policy",
                "meeting_booked",
                &["signal-job-posting".to_string()],
                &[],
            )
            .expect("record missed signal review");

        let proposals = engine
            .list_policy_proposals(Some("proposed"), 10)
            .expect("list proposals");
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].rule_key, "signal_weight::job_posting");

        let approved = engine
            .update_policy_proposal_status(&proposals[0].id, "active", Some("operator"))
            .expect("approve proposal")
            .expect("proposal exists");
        assert_eq!(approved.status, "active");
        assert_eq!(approved.approved_by.as_deref(), Some("operator"));
        assert!(approved.activated_at.is_some());

        let retired = engine
            .update_policy_proposal_status(&proposals[0].id, "retired", None)
            .expect("retire proposal")
            .expect("proposal exists");
        assert_eq!(retired.status, "retired");
        assert!(retired.activated_at.is_none());
    }

    #[test]
    fn get_account_dossier_falls_back_to_prospect_profile_when_canonical_core_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        engine
            .upsert_prospect_profiles(&[SalesProspectProfile {
                id: "ornekbakim.com.tr".to_string(),
                run_id: "run-fallback".to_string(),
                company: "Ornek Bakim".to_string(),
                website: "https://ornekbakim.com.tr".to_string(),
                company_domain: "ornekbakim.com.tr".to_string(),
                fit_score: 77,
                profile_status: "contact_ready".to_string(),
                summary: "Public maintenance signal and reachable operator leader.".to_string(),
                matched_signals: vec!["Saha operasyon yonetimi".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("COO".to_string()),
                primary_email: Some("aylin@ornekbakim.com.tr".to_string()),
                primary_linkedin_url: Some("https://www.linkedin.com/in/aylin-demir/".to_string()),
                company_linkedin_url: None,
                osint_links: vec!["https://ornekbakim.com.tr".to_string()],
                contact_count: 1,
                source_count: 2,
                buyer_roles: vec!["decision_maker".to_string()],
                pain_points: vec!["Dispatch visibility".to_string()],
                trigger_events: vec!["Public field ops hiring".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with faster dispatch coordination".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.81,
                tech_stack: Vec::new(),
                created_at: "2026-03-26T09:00:00Z".to_string(),
                updated_at: "2026-03-26T09:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let dossier = engine
            .get_account_dossier("ornekbakim.com.tr")
            .expect("dossier lookup")
            .expect("fallback dossier");

        assert_eq!(
            dossier.get("source").and_then(|value| value.as_str()),
            Some("prospect_profile_fallback")
        );
        assert_eq!(
            dossier
                .get("account")
                .and_then(|value| value.get("canonical_name"))
                .and_then(|value| value.as_str()),
            Some("Ornek Bakim")
        );
        assert_eq!(
            dossier
                .get("score")
                .and_then(|value| value.get("fit_score"))
                .and_then(|value| value.as_f64())
                .map(|value| (value * 100.0).round() as i64),
            Some(77)
        );
    }

    #[test]
    fn extract_contact_from_search_for_company_rejects_company_phrases_as_names() {
        let sample = r#"
1. Rakamlarla Rönesans - Yönetim Kurulu
   URL: https://ronesans.com/biz-kimiz#rakamlarla-ronesans
   Yönetim Kurulu ve sürdürülebilirlik sosyal sorumluluk içerikleri
"#;
        let (name, title, _linkedin, _email) = extract_contact_from_search_for_company(
            sample,
            "ceo_then_founder",
            "Ronesans",
            "ronesans.com",
        );
        assert!(name.is_none());
        assert!(title.is_none());
    }

    #[test]
    fn structured_site_text_extracts_person_card_name_and_title() {
        let html = r#"
        <div class="item">
          <strong class="body">Naci AĞBAL</strong>
          <span class="desc">İcra Kurulu Başkanı ve CEO</span>
        </div>
        <div class="item">
          <strong class="body">Senlav GÜNER</strong>
          <span class="desc">İşletme ve Bakımdan Sorumlu Başkan Yardımcısı (COO)</span>
        </div>
        "#;
        let structured = canonicalize_contact_titles(&html_to_structured_text(html));
        let (name, title) =
            extract_contact_from_structured_site_text(&structured, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Senlav Güner"));
        assert!(title
            .as_deref()
            .map(|value| value.contains("COO"))
            .unwrap_or(false));
    }

    #[test]
    fn extract_contact_from_company_site_html_prefers_person_card_over_generic_heading() {
        let html = r#"
        <html>
          <body>
            <footer>
              <a href="/biz-kimiz#faaliyet-alanlari">Faaliyet Alanları</a>
              <a href="/biz-kimiz#suudi-arabistan">Suudi Arabistan</a>
            </footer>
            <div class="person-card">
              <strong class="body">Naci AĞBAL</strong>
              <span class="desc">İcra Kurulu Başkanı ve CEO</span>
            </div>
          </body>
        </html>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Naci Ağbal"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn extract_contact_from_company_site_html_decodes_turkish_entities_in_cards() {
        let html = r#"
        <div class="yonetimDiv">
          <h1><a href="/Yonetim-Kurulu/Basar-Arioglu">Ba&#351;ar Ar&#305;o&#287;lu</a></h1>
          <span>Y&ouml;netim Kurulu Ba&#351;kan&#305;</span>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_yapi_merkezi_management_markup() {
        let html = r#"
        <div class="yonetimDiv">
            <h1>
                <a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank">Başar Arıoğlu</a>
            </h1>
            <span>Yapı Merkezi Holding Yönetim Kurulu Başkanı</span>
        </div>
        <p>
            <strong><a href="/Yonetim-Kurulu/Ulku-Arioglu" target="_blank">Ülkü Arıoğlu</a></strong>
            <div style="clear: both"></div>
            <span>Kurucu Üye</span>
        </p>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Ülkü Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Founder"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_parenthesized_board_entries() {
        let html = r#"
        <p>
            <span style="color: rgb(105, 105, 105); font-family: verdana, geneva, sans-serif; font-size: 11px;">
                Celal KOLOĞLU (Yönetim Kurulu Üyesi),
            </span><br />
            <span style="color: rgb(105, 105, 105); font-family: verdana, geneva, sans-serif; font-size: 11px;">
                Naci KOLOĞLU (Yönetim Kurulu Başkanı)
            </span>
        </p>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Naci Koloğlu"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_full_yapi_merkezi_section() {
        let html = r#"
        <div class="section-5">
            <h1 class="heading-11" style="text-transform: uppercase;">Yönetim</h1>
            <div>
                <div class="yonetimm">
                    <div class="yonetimDiv">
                        <img alt="" src="/uploads/images/03072018201333-02.jpg" style="width: 190px; height: 286px;" />
                        <h1>
                            <a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank">Başar Arıoğlu</a>
                        </h1>
                        <span>Yapı Merkezi Holding Yönetim Kurulu Başkanı</span>
                    </div>
                    <div class="yonetimDiv">
                        <img alt="" src="/images/uploads/03072018201333.jpg" style="width:190px; height:286px;" />
                        <h1>
                            <a href="/Yonetim-Kurulu/Koksal-Anadol" target="_blank">Köksal Anadol</a>
                        </h1>
                        <span>Yapı Merkezi Holding Başkan Vekili</span>
                    </div>
                </div>
                <div>
                    <h1 class="heading-11" style="text-transform: uppercase;">
                        Yapı Merkezi Holding Üst Yönetim
                    </h1>
                    <p>
                        <strong><a>Dr. Erdem Arıoğlu</a></strong>
                    </p>
                    <p>
                        <span>Yönetim Kurulu Üyesi</span>
                    </p>
                    <p>
                        <strong><a>S. Özge Arıoğlu</a></strong><br />
                        <span>Yönetim Kurulu Üyesi</span>
                    </p>
                    <div class="yListe">
                        <h1 class="heading-11" style="text-transform: uppercase;">
                            YAPI MERKEZİ İNŞAAT VE SANAYİ A.Ş.
                        </h1>
                    </div>
                </div>

                <p>
                    <strong><a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank" style="text-decoration: none;">Başar Arıoğlu</a></strong>
                    <div style="clear: both"></div>
                    <span>Yönetim Kurulu Başkanı</span>
                </p>

                <p>
                    <strong><a href="/Yonetim-Kurulu/Ulku-Arioglu" target="_blank" style="text-decoration: none;">Ülkü Arıoğlu</a></strong>
                    <div style="clear: both"></div>
                    <span>Kurucu Üye</span>
                </p>
            </div>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Ülkü Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Founder"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_accordion_name_title_markup() {
        let html = r#"
        <div class="accordion-item">
            <h2 class="accordion-header" id="heading0-1">
                <button class="accordion-button collapsed" type="button">
                    <span>Hamdi Akın – Yönetim Kurulu Başkanı</span>
                </button>
            </h2>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Hamdi Akın"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn parse_tmb_member_candidates_extracts_domain_and_contact_hint() {
        let html = r#"
        <article class="member-card h-100">
            <div class="title">
                <div class="member-name">
                    <div class="name">
                        <a href="/en/m/test/acme-construction">ACME CONSTRUCTION INC.</a>
                    </div>
                    <div class="ceo"><i>Chairman of the Board : <strong>Ahmet YILMAZ</strong></i></div>
                </div>
            </div>
            <div class="member-info">
                <table class="table table-borderless">
                    <tbody>
                        <tr>
                            <th scope="row">Mail</th>
                            <td>:</td>
                            <td><a href="mailto:info@acmeinsaat.com.tr">info@acmeinsaat.com.tr</a></td>
                        </tr>
                        <tr>
                            <th scope="row">Web</th>
                            <td>:</td>
                            <td><a href="http://www.acmeinsaat.com.tr" rel="noreferrer" target="_blank">www.acmeinsaat.com.tr</a></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </article>
        "#;
        let profile = SalesProfile {
            target_industry: "Field ops".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tmb_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "acmeinsaat.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Ahmet Yılmaz")
        );
        assert_eq!(
            candidates[0].contact_hint.contact_title.as_deref(),
            Some("Chairman")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@acmeinsaat.com.tr")
        );
        assert!(candidates[0]
            .candidate
            .source_links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/m/test/acme-construction"));
    }

    #[test]
    fn parse_eud_member_candidates_extracts_official_member_domains() {
        let html = r#"
        <a href='https://www.aksaenerji.com.tr/tr/ana-sayfa/' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        <a href='http://www.ictasenerji.com.tr/TR/Enerji' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        <a href='' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_eud_member_candidates(html, &profile, 0, 8);
        let domains = candidates
            .iter()
            .map(|candidate| candidate.candidate.domain.as_str())
            .collect::<Vec<_>>();
        assert_eq!(domains, vec!["aksaenerji.com.tr", "ictasenerji.com.tr"]);
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "utility"));
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EUD members page")
        );
    }

    #[test]
    fn parse_asmud_member_candidates_extracts_domain_and_directory_email() {
        let html = r#"
        <div class="uwrap w3-card">
          <a href="https:/www.kemetyl.com.tr" target="_blank"><div class="uimg"></div></a>
          <strong>KEMETYL KİMYA SAN. VE TİC. A.Ş.</strong>
          <hr>
          T: (312) 555 00 00<br>
          E: <span style="word-break: break-all">info@kemetyl.com.tr</span><br>
          <hr>
          Ankara
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_asmud_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "kemetyl.com.tr");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@kemetyl.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("ASMUD members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "asphalt"));
    }

    #[test]
    fn parse_platformder_directory_candidates_extracts_domain_and_source() {
        let html = r#"
        <li id="item-7653-5" class="column3"
            data-title="Asel Vinç Platform"
            data-phone="0545 891 91 41"
            data-url="https://www.aselvincplatform.com">
            <div class="pd-bottom-area">
                <p><a href="tel:05458919141" title="Call 0545 891 91 41"><i class="fa fa-phone"></i></a></p>
                <p><a href="https://www.aselvincplatform.com" target="_blank" title="Go to website"><i class="fa fa-link"></i></a></p>
            </div>
        </li>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_platformder_directory_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "aselvincplatform.com");
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "platform rental"));
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "vinç"));
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("Platformder rehber")
        );
        assert!(candidates[0].candidate.evidence[0].contains("0545 891 91 41"));
    }

    #[test]
    fn parse_platformder_directory_candidates_falls_back_to_visible_website_link() {
        let html = r#"
        <li id="item-7655-2" class="column3"
            data-title="Giray Vinç Platform"
            data-phone="0534 767 12 02"
            data-url="">
            <img src="https://www.platformder.org.tr/wp-content/plugins/simple-business-directory-pro/assets/images/list-image-placeholder.png" alt="">
            <div class="pd-bottom-area">
                <p><a href="https://www.girayplatform.com.tr" target="_blank" title="Go to website"><i class="fa fa-link"></i></a></p>
            </div>
        </li>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_platformder_directory_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "girayplatform.com.tr");
    }

    #[test]
    fn parse_mib_member_candidates_extracts_domain_and_email() {
        let html = r#"
        <div data-elementor-type="loop-item" class="elementor e-loop-item post-111 firm type-firm">
            <div class="elementor-widget-container">
                <h2 class="elementor-heading-title elementor-size-default">
                    <a href="https://mib.org.tr/en/firm/abravinc/" data-penci-link="internal">ABRA VİNÇ SANAYİ VE TİCARET A.Ş.</a>
                </h2>
            </div>
            <ul class="elementor-icon-list-items">
                <li class="elementor-icon-list-item">
                    <a href="https://www.abravinc.com.tr/" target="_blank" rel="nofollow">
                        <span class="elementor-icon-list-icon"><i aria-hidden="true" class="fas fa-globe"></i></span>
                        <span class="elementor-icon-list-text">https://www.abravinc.com.tr/</span>
                    </a>
                </li>
                <li class="elementor-icon-list-item">
                    <a href="mailto:info@abravinc.com.tr" target="_blank">
                        <span class="elementor-icon-list-icon"><i aria-hidden="true" class="fas fa-envelope"></i></span>
                        <span class="elementor-icon-list-text">info@abravinc.com.tr</span>
                    </a>
                </li>
            </ul>
        </div>
        <span class="e-load-more-spinner"></span>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_mib_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "abravinc.com.tr");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@abravinc.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("MIB members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "industrial equipment"));
        assert!(candidates[0]
            .candidate
            .source_links
            .iter()
            .any(|value| value == "https://mib.org.tr/en/firm/abravinc/"));
    }

    #[test]
    fn osint_link_priority_prefers_member_detail_over_listing() {
        let detail = "https://www.tmb.org.tr/en/m/60fe744c002ab9647e98cbd4/akm-yapi-contracting-industry-and-commerce-co-inc";
        let listing = "https://www.tmb.org.tr/en/members";
        assert!(osint_link_priority(detail) > osint_link_priority(listing));
    }

    #[test]
    fn merge_osint_links_keeps_best_directory_link_per_source() {
        let links = merge_osint_links(
            vec![
                "https://www.tmb.org.tr/en/members".to_string(),
                "https://www.tmb.org.tr/en/m/60fe744c002ab9647e98cbd4/akm-yapi-contracting-industry-and-commerce-co-inc".to_string(),
                "https://mib.org.tr/en/our-members/3/".to_string(),
                "https://mib.org.tr/en/firm/yeter-makina-2/".to_string(),
                "https://imder.org.tr/uyelerimiz/".to_string(),
                "https://imder.org.tr/cozum-makina-sanayi-ve-ticaret-ltd-sti/".to_string(),
                "https://isder.org.tr/uyelerimiz/".to_string(),
                "https://isder.org.tr/cakmak-vinc-sanayi-ve-tic-a-s/".to_string(),
            ],
            Vec::new(),
        );

        assert!(links.iter().any(|value| value.contains("/en/m/")));
        assert!(links.iter().any(|value| value.contains("/firm/")));
        assert!(links
            .iter()
            .any(|value| value == "https://imder.org.tr/cozum-makina-sanayi-ve-ticaret-ltd-sti/"));
        assert!(links
            .iter()
            .any(|value| value == "https://isder.org.tr/cakmak-vinc-sanayi-ve-tic-a-s/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/members"));
        assert!(!links
            .iter()
            .any(|value| value == "https://mib.org.tr/en/our-members/3/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://imder.org.tr/uyelerimiz/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://isder.org.tr/uyelerimiz/"));
    }

    #[test]
    fn merge_osint_links_normalizes_variants_and_keeps_diverse_site_links() {
        let links = merge_osint_links(
            vec![
                "https://akmyapi.com.tr/kopyas%C4%B1-devam-eden-projeler?utm_source=test#hero"
                    .to_string(),
                "https://www.akmyapi.com.tr/kopyası-devam-eden-projeler/".to_string(),
                "https://akmyapi.com.tr/hakkimizda".to_string(),
                "https://akmyapi.com.tr/iletisim".to_string(),
            ],
            Vec::new(),
        );

        assert_eq!(
            links
                .iter()
                .filter(|value| value.contains("devam-eden-projeler"))
                .count(),
            1
        );
        assert!(links.iter().any(|value| value.contains("/hakkimizda")));
        assert!(links.iter().any(|value| value.contains("/iletisim")));
    }

    #[test]
    fn parse_imder_member_index_urls_extracts_detail_pages() {
        let html = r#"
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/mst-is-ve-tarim-makinalari-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        "#;

        let urls = parse_imder_member_index_urls(html, 0, 8);
        assert_eq!(
            urls,
            vec![
                "https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/",
                "https://imder.org.tr/mst-is-ve-tarim-makinalari-san-ve-tic-a-s/"
            ]
        );
    }

    #[test]
    fn parse_imder_member_detail_candidate_extracts_domain_contact_and_keywords() {
        let html = r#"
        <h1 class="elementor-heading-title elementor-size-default">ALTAN HİDROLİK MÜHENDİSLİK SAN. VE TİC. A.Ş.</h1>
        <table class="table table-hover">
            <tbody>
                <tr><td><strong>İsim Soyisim</strong></td><td>TEVFİK ALTAN</td></tr>
                <tr><td><strong>Görevi</strong></td><td>Yönetim Kurulu Başkanı ve Genel Müdür</td></tr>
                <tr><td><strong>Telefon</strong></td><td>+90216 593 33 00</td></tr>
                <tr><td><strong>Web Sitesi</strong></td><td><a href="https://www.altanhidrolik.com.tr/" target="_blank" rel="noopener"><strong>https://www.altanhidrolik.com.tr/</strong></a></td></tr>
            </tbody>
        </table>
        <p><strong>Mobil İş Makineleri ve Yerinde Değerlendirme, Kurulum ve Bakım Hizmetleri</strong></p>
        Tags :
        <a href="https://imder.org.tr/tag/yedek-parca/" rel="tag">yedek parça</a>
        <a href="https://imder.org.tr/tag/telehandler/" rel="tag">telehandler</a>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidate = parse_imder_member_detail_candidate(
            html,
            "https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/",
            &profile,
        )
        .unwrap();

        assert_eq!(candidate.candidate.domain, "altanhidrolik.com.tr");
        assert_eq!(
            candidate.contact_hint.contact_name.as_deref(),
            Some("Tevfik Altan")
        );
        assert!(matches!(
            candidate.contact_hint.contact_title.as_deref(),
            Some("Chairman") | Some("CEO") | Some("General Manager")
        ));
        assert_eq!(
            candidate.contact_hint.source.as_deref(),
            Some("IMDER member detail")
        );
        assert!(candidate
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "telehandler"));
        assert!(candidate.candidate.evidence[0].contains("+90216 593 33 00"));
    }

    #[test]
    fn parse_isder_member_index_urls_extracts_detail_pages() {
        let html = r#"
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/asko-glob-all-marketing-dis-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/" tabindex="-1"></a>
        </article>
        "#;

        let urls = parse_isder_member_index_urls(html, 0, 8);
        assert_eq!(
            urls,
            vec![
                "https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/",
                "https://isder.org.tr/asko-glob-all-marketing-dis-tic-a-s/"
            ]
        );
    }

    #[test]
    fn parse_isder_member_detail_candidate_extracts_domain_contact_and_keywords() {
        let html = r#"
        <h1 class="elementor-heading-title elementor-size-default">ACARLAR MAKİNE ACARLAR DIŞ TİCARET VE MAKİNE SANAYİ A.Ş.</h1>
        <table class="table table-hover">
            <tbody>
                <tr><td><strong>İsim Soyisim:</strong></td><td><table class="table table-hover"><tbody><tr><td>SERKAN ACAR</td></tr></tbody></table></td></tr>
                <tr><td><strong>Ünvan Görevi:</strong></td><td>Genel Müdür</td></tr>
                <tr><td><strong>Telefon:</strong></td><td>+90 (216) 581 4949</td></tr>
                <tr><td><strong>Web Sitesi:</strong></td><td>http://www.acarlarmakine.com</td></tr>
            </tbody>
        </table>
        <p>İstif makineleri, forklift ve servis çözümleri sunmaktadır.</p>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidate = parse_isder_member_detail_candidate(
            html,
            "https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/",
            &profile,
        )
        .unwrap();

        assert_eq!(candidate.candidate.domain, "acarlarmakine.com");
        assert_eq!(
            candidate.contact_hint.contact_name.as_deref(),
            Some("Serkan Acar")
        );
        assert_eq!(candidate.contact_hint.contact_title.as_deref(), Some("CEO"));
        assert_eq!(
            candidate.contact_hint.source.as_deref(),
            Some("ISDER member detail")
        );
        assert!(candidate
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "forklift"));
        assert!(candidate.candidate.evidence[0].contains("+90 (216) 581 4949"));
    }

    #[test]
    fn parse_thbb_yazismali_candidates_extracts_domain_and_cf_email() {
        let html = r#"
        <div class="entry-content indented">
            <table>
                <tr>
                    <td><a href="http://www.bestaff.com.tr/" target="_blank"><strong><img src="/media/logo.jpg" alt=""></strong></a></td>
                </tr>
                <tr>
                    <td><strong>Bestaff İş Makineleri Dış Tic. San. ve Tic. Ltd. Şti.<br></strong><strong>Adres:</strong> Ankara<br><strong>Tel:</strong> 0552 784 05 05<br><strong>E-posta:</strong> <a href="/cdn-cgi/l/email-protection#f79e999198b795928483969191d994989ad98385"><span class="__cf_email__" data-cfemail="0b62656d644b696e787f6a6d6d25686466257f79">[email&#160;protected]</span></a><br><strong>Web:</strong> <a href="http://www.bestaff.com.tr" target="_blank">www.bestaff.com.tr</a></td>
                </tr>
            </table>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_thbb_yazismali_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "bestaff.com.tr");
        assert!(candidates[0].contact_hint.email.is_some());
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("THBB yazismali uyeler")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "construction equipment"));
    }

    #[test]
    fn parse_eder_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="ui-e-ico-box" onclick="window.open(&#039;https://www.kolaymagaza.com/&#039;, &#039;_blank&#039;)">
            <div class="ui-e-box-content">
                <div class="ui-e-description">
                    <p>Kolaymağaza E-Ticaret Yazılımları</p>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "E-commerce".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_eder_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "kolaymagaza.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EDER members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "digital commerce"));
    }

    #[test]
    fn parse_lojider_member_candidates_extracts_domain_contact_and_email() {
        let html = r#"
        <div class="row mb-4 member-row">
            <div class="Uye p-3">
                <div class="row">
                    <b class="d-block">2H Gümrük ve Lojistik Hizmetleri Tic. Ltd. Şti.</b>
                </div>
                <div class="row row-cols-1 row-cols-md-2 row-cols-lg-4 mt-2">
                    <div class="col">
                        <i class="fa-solid fa-phone me-2"></i>
                        <a href="tel:02163052325">0216 305 23 25</a>
                    </div>
                    <div class="col"><i class="fa-solid fa-envelope me-2"></i><a target="_blank" href="mailto:aysun@2hgumrukleme.com.tr">aysun@2hgumrukleme.com.tr</a></div>
                    <div class="col">
                        <i class="fa-solid fa-paper-plane me-2"></i>
                        <a target="_blank" href="http://2hgumrukleme.com.tr/">2hgumrukleme.com.tr/</a>
                    </div>
                    <div class="col"><i class="fa-solid fa-user me-2"></i>Aysun KÜÇÜKÇİTRAZ</div>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Logistics".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_lojider_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "2hgumrukleme.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Aysun Küçükçitraz")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("aysun@2hgumrukleme.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("LojiDer members page")
        );
    }

    #[test]
    fn parse_tfyd_member_candidates_extracts_domain_and_phone() {
        let html = r#"
        <table>
            <tr>
                <td colspan="2"><div align="center">KURULUŞ</div></td>
                <td><div align="center">WEB SİTESİ</div></td>
                <td><div align="center">TELEFON</div></td>
            </tr>
            <tr>
                <td align="center">1</td>
                <td align="left">&nbsp;AJANS ASYA FUARCILIK ORG. LTD. ŞTİ</td>
                <td align="left">&nbsp;www.vanfuar.com&nbsp;</td>
                <td align="center">0432 215 81 80</td>
            </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Events & Exhibitions".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tfyd_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "vanfuar.com");
        assert_eq!(
            candidates[0].candidate.phone.as_deref(),
            Some("+904322158180")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TFYD members page")
        );
    }

    #[test]
    fn parse_oss_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="card membercard">
            <div class="card-body">
                <h5 class="card-title"> 5S Otomotiv İmalat San. ve Tic. A.Ş. </h5>
            </div>
            <ul class="list-group list-group-flush">
                <li class="list-group-item">
                    <i class="bi bi-telephone-fill"></i>  444 52 89
                </li>
                <li class="list-group-item">
                    <i class="bi bi-globe"></i>
                    <a onclick="window.open('http://www.5sotomotiv.com')" href="javascript:void(0)">5sotomotiv.com </a>
                </li>
            </ul>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Automotive".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_oss_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "5sotomotiv.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("OSS members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "automotive aftermarket"));
    }

    #[test]
    fn parse_ida_member_candidates_extracts_domain_and_contact() {
        let html = r#"
        <table border="0" cellspacing="0" cellpadding="3">
            <tbody>
                <tr>
                    <td colspan="2"><strong>ARTI İletişim Yönetimi</strong></td>
                </tr>
                <tr>
                    <td style="white-space: nowrap;"><strong>Yönetici Ortak:</strong></td>
                    <td>Esra ŞENGÜLEN ÜNSÜR</td>
                </tr>
                <tr>
                    <td><strong>Telefon:</strong></td>
                    <td>+90 212 347 03 30</td>
                </tr>
                <tr>
                    <td><strong>Web:</strong></td>
                    <td><a href="http://www.artipr.com.tr/" target="_blank">www.artipr.com.tr</a></td>
                </tr>
            </tbody>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "PR agency".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_ida_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "artipr.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Esra Şengülen Ünsür")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("IDA members page")
        );
    }

    #[test]
    fn parse_tesid_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="boxuye_detay">
            <p><strong>BÜYÜK FİRMALAR</strong></p>
            <p><strong><a href="http://www.karel.com.tr" target="_blank">KAREL Elektronik Sanayi ve Ticaret A.Ş.</a></strong></p>
        </div>
        <p><strong><a href="https://tesid.org.tr/alt_sektor_dagilimi">TESİD ÜYELERİ ALT SEKTÖR DAĞILIMI</a></strong></p>
        "#;
        let profile = SalesProfile {
            target_industry: "Electronics".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tesid_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "karel.com.tr");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TESID members page")
        );
    }

    #[test]
    fn parse_tudis_member_candidates_extracts_domain_and_email() {
        let html = r#"
        <table>
            <tr>
                <td><strong>Cihan Deri San.A.Ş.</strong></td>
                <td>info@cihanderi.com</td>
                <td><a href="https://www.cihanderi.com/">https://www.cihanderi.com/</a></td>
            </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Leather".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tudis_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "cihanderi.com");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@cihanderi.com")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TUDIS members page")
        );
    }

    #[test]
    fn parse_emsad_member_candidates_extracts_domain_contact_and_email() {
        let html = r#"
        <table width="100%" border="0" cellspacing="1" cellpadding="2">
          <tr>
            <td width="192" align="center" valign="top"><img class="foto1" width="128px" height="58px" src="/Resim/495,aksanpng.png?0" /></td>
            <td width="788" align="left" valign="top"><b>AKSAN PANO TANITIM İNŞ. ELK. İML. TAAH. VE PAZ. TİC. LTD. ŞTİ</b><br />
             <b>Temsilci Adı:</b> Şahin ŞANLITÜRK<br />
             <b>Adres:</b> Kahramankazan / ANKARA<br />
             <b> Tel: </b> 0312 386 12 08<br />
             <b> e-posta: </b> info@aksanpano.com.tr<br />
             <b>  web:</b> <a target="_blank" href="http://www.aksanpano.com.tr">www.aksanpano.com.tr</a><br /><br />
            </td>
          </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Electromechanical".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_emsad_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "aksanpano.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Şahin Şanlıtürk")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@aksanpano.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EMSAD members page")
        );
    }

    #[test]
    fn parse_tgsd_member_candidates_extracts_company_contact_and_domain() {
        let html = r#"
        <table id="aplus-uye-listesi">
            <tbody>
                <tr>
                    <td><img src="https://tgsd.org.tr/wp-content/uploads/2025/11/Suglobal_Denimvillage_logo.jpg" alt="Logo" class="aplus-logo"></td>
                    <td>Abdulhadi Karasu</td>
                    <td>Suglobal Tekstil ve Konfeksiyon San. A.Ş.</td>
                    <td><a href="https://www.denimvillage.com" target="_blank" rel="noopener">www.denimvillage.com</a></td>
                </tr>
            </tbody>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Textile".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tgsd_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "denimvillage.com");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Abdulhadi Karasu")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TGSD members page")
        );
    }

    #[test]
    fn parse_ared_member_candidates_extracts_domain_contact_and_phone() {
        let html = r#"
        <div class="col-lg-12 load-post">
            <article class="post hentry post-list post-list-small">
                <div class="content-entry-wrap">
                    <div class="entry-content">
                        <h3 class="entry-title">24 Saat Dijital Baskı - İstanbul</h3>
                    </div>
                    <div class="entry-meta-content">
                        <div class="entry-date">
                            <span><i class="fa fa-user pr-1"></i>İsa Yavuz </span><br>
                            <span><i class="fa fa-phone-square pr-1"></i>(0212) 268 28 77 </span><br>
                            <span><i class="fa fa-globe pr-1"></i><a href="mailto:info@24saatdijital.com">info@24saatdijital.com</a> - <a href="http://www.24saatdijital.com" target="_blank">http://www.24saatdijital.com</a></span><br>
                            <span><i class="fa fa-building pr-1"></i>Baskı Hizmetleri</span>
                        </div>
                    </div>
                </div>
            </article>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Signage".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_ared_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "24saatdijital.com");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("İsa Yavuz")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@24saatdijital.com")
        );
        assert_eq!(
            candidates[0].candidate.phone.as_deref(),
            Some("+902122682877")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("ARED members page")
        );
    }

    #[test]
    fn parse_todeb_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="flexCerceve logoBorder">
            <div class="row">
                <div class="col-lg-6">
                    <a href="https://1000pay.com" target="_blank">
                        <img src="https://todeb.org.tr/source/uye_iliskileri/uye_logolari/1000pay.png" class="img-responsive">
                    </a>
                </div>
                <div class="col-lg-6">
                    <p><h2>1000 Ödeme Hizmetleri ve Elektronik Para A.Ş.</h2>
                    <p><strong>Telefon: <br /></strong>444 10 04<br />
                    <strong>Web:<br /></strong><a href="https://1000pay.com">www.1000pay.com</a></p></p>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Payments".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_todeb_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "1000pay.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TODEB members page")
        );
    }

    #[test]
    fn source_contact_hint_overrides_placeholder_contact() {
        let hint = SourceContactHint {
            contact_name: Some("Ahmet Yılmaz".to_string()),
            contact_title: Some("Chairman".to_string()),
            email: Some("info@acmeinsaat.com.tr".to_string()),
            source: Some("TMB members directory".to_string()),
        };
        let mut contact_name = None;
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "acmeinsaat.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(contact_name.as_deref(), Some("Ahmet Yılmaz"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert!(email.is_none());
    }

    #[test]
    fn source_contact_hint_replaces_generic_team_placeholder_name() {
        let hint = SourceContactHint {
            contact_name: Some("Ahmet Yılmaz".to_string()),
            contact_title: Some("Chairman".to_string()),
            email: Some("info@acmeinsaat.com.tr".to_string()),
            source: Some("TMB members directory".to_string()),
        };
        let mut contact_name = Some("Leadership Team".to_string());
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "acmeinsaat.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(contact_name.as_deref(), Some("Ahmet Yılmaz"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert!(email.is_none());
    }

    #[test]
    fn source_contact_hint_allows_trusted_offdomain_directory_mailbox() {
        let hint = SourceContactHint {
            email: Some("cargill_turkey@cargill.com".to_string()),
            source: Some("ASMUD members page".to_string()),
            ..SourceContactHint::default()
        };
        let mut contact_name = None;
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "cargill.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(email.as_deref(), Some("cargill_turkey@cargill.com"));
    }

    #[test]
    fn merge_free_discovery_candidate_boosts_named_directory_sources() {
        let mut candidates = HashMap::<String, DomainCandidate>::new();
        let mut hints = HashMap::<String, SourceContactHint>::new();
        merge_free_discovery_candidate(
            &mut candidates,
            &mut hints,
            FreeDiscoveryCandidate {
                candidate: DomainCandidate {
                    domain: "acmeinsaat.com.tr".to_string(),
                    score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                    evidence: vec!["TMB member".to_string()],
                    matched_keywords: vec!["construction".to_string()],
                    source_links: vec!["https://www.tmb.org.tr/en/members".to_string()],
                    phone: None,
                },
                contact_hint: SourceContactHint {
                    contact_name: Some("Ahmet Yılmaz".to_string()),
                    contact_title: Some("Chairman".to_string()),
                    email: Some("info@acmeinsaat.com.tr".to_string()),
                    source: Some("TMB members directory".to_string()),
                },
            },
        );

        assert_eq!(
            candidates.get("acmeinsaat.com.tr").map(|value| value.score),
            Some(MIN_DOMAIN_RELEVANCE_SCORE + 30)
        );
        assert!(candidates
            .get("acmeinsaat.com.tr")
            .map(|value| {
                value
                    .source_links
                    .iter()
                    .any(|url| url == "https://www.tmb.org.tr/en/members")
            })
            .unwrap_or(false));
    }

    #[test]
    fn internal_enrich_links_prioritize_management_profiles() {
        let base = url::Url::parse("https://ronesans.com").unwrap();
        let html = r#"
        <a href="/biz-kimiz">Biz Kimiz</a>
        <a href="/biz-kimiz#faaliyet-alanlari">Faaliyet Alanları</a>
        <a href="/iletisim">İletişim</a>
        <a href="/ust-yonetim/dr-erman-ilicak">Erman Ilıcak</a>
        <a href="/hakkimizda/yonetim-kurulu">Yönetim Kurulu</a>
        "#;

        let links = extract_internal_enrich_links(&base, html);
        assert_eq!(
            links.first().map(|value| value.as_str()),
            Some("https://ronesans.com/ust-yonetim/dr-erman-ilicak")
        );
        assert!(links
            .iter()
            .any(|value| value == "https://ronesans.com/hakkimizda/yonetim-kurulu"));
    }

    #[test]
    fn internal_enrich_links_accept_www_redirect_aliases() {
        let base = url::Url::parse("https://akfenren.com.tr").unwrap();
        let html = r#"
        <a href="https://www.akfenren.com.tr/kurumsal/yonetim-kurulu-ve-ust-yonetim/">
            Yönetim Kurulu ve Üst Yönetim
        </a>
        "#;

        let links = extract_internal_enrich_links(&base, html);
        assert!(links.iter().any(|value| {
            value == "https://www.akfenren.com.tr/kurumsal/yonetim-kurulu-ve-ust-yonetim/"
        }));
    }

    #[test]
    fn select_company_site_enrich_links_prefers_discovered_links_before_defaults() {
        let base = url::Url::parse("https://yapimerkezi.com.tr").unwrap();
        let html = r#"
        <a href="/Uzmanlik-Alanlari/Izleme-Denetim-ve-Iletisim">İzleme</a>
        <a href="/Iletisim-Aydinlatma-Metni">İletişim</a>
        <a href="/Hakkinda/Yonetim">Yönetim</a>
        <a href="/Projeler/Devam-Eden-Projeler">Projeler</a>
        "#;

        let links = select_company_site_enrich_links(&base, html, Vec::new());
        assert!(links
            .iter()
            .any(|value| value == "https://yapimerkezi.com.tr/Hakkinda/Yonetim"));
        assert!(!links[..links.len().min(4)]
            .iter()
            .any(|value| value == "https://yapimerkezi.com.tr/hakkimizda/yonetim/icra-kurulu"));
    }

    #[test]
    fn select_company_site_enrich_links_prefers_diverse_categories() {
        let base = url::Url::parse("https://akmyapi.com.tr").unwrap();
        let html = r#"
        <a href="/kopyas%C4%B1-devam-eden-projeler?utm=1">Devam Eden Projeler</a>
        <a href="https://www.akmyapi.com.tr/kopyası-devam-eden-projeler/">Devam Eden Projeler Duplicate</a>
        <a href="/kopyası-tamamlanan-projeler">Tamamlanan Projeler</a>
        <a href="/hakkimizda">Hakkımızda</a>
        <a href="/iletisim#harita">İletişim</a>
        <a href="/yonetim-kurulu">Yönetim Kurulu</a>
        "#;

        let links = select_company_site_enrich_links(&base, html, Vec::new());
        assert_eq!(links.len(), 3);
        assert!(links.iter().any(|value| value.contains("/yonetim-kurulu")));
        assert!(links.iter().any(|value| value.contains("/hakkimizda")));
        assert!(links.iter().any(|value| value.contains("/iletisim")));
        assert!(!links.iter().any(|value| value.contains("projeler")));
    }

    #[test]
    fn normalize_contact_email_for_domain_rejects_external_domain() {
        let kept = normalize_contact_email_for_domain(
            Some("ceo@sub.example.com".to_string()),
            "example.com",
        );
        let dropped =
            normalize_contact_email_for_domain(Some("ceo@other.com".to_string()), "example.com");
        assert_eq!(kept.as_deref(), Some("ceo@sub.example.com"));
        assert!(dropped.is_none());
    }

    #[test]
    fn normalize_site_contact_email_accepts_verified_offdomain_mailbox() {
        let kept = normalize_site_contact_email(Some("yminfo@ym.com.tr".to_string()));
        assert_eq!(kept.as_deref(), Some("yminfo@ym.com.tr"));
    }

    #[test]
    fn repair_common_mojibake_utf8_recovers_turkish_text() {
        let repaired = repair_common_mojibake_utf8("ASMÃD :: Ãyelerimiz / Members");
        assert_eq!(repaired, "ASMÜD :: Üyelerimiz / Members");
    }

    #[test]
    fn extract_domain_repairs_missing_scheme_slash() {
        let domain = extract_domain("https:/www.kemetyl.com.tr");
        assert_eq!(domain.as_deref(), Some("kemetyl.com.tr"));
    }

    #[test]
    fn extract_domain_rejects_asset_tld_hosts() {
        assert!(extract_domain("https://emirliftdernekweblogo-80x80.jpg").is_none());
        assert!(extract_domain("https://hero-banner.webp").is_none());
    }

    #[test]
    fn mib_directory_pages_for_run_wraps_across_catalog() {
        assert_eq!(mib_directory_pages_for_run(0, 9, 3), vec![1, 2, 3]);
        assert_eq!(mib_directory_pages_for_run(7, 9, 3), vec![8, 9, 1]);
        assert_eq!(mib_directory_pages_for_run(8, 9, 3), vec![9, 1, 2]);
    }

    #[test]
    fn source_hint_contact_richness_bonus_prefers_named_contacts() {
        let generic = SourceContactHint {
            contact_name: Some("Leadership Team".to_string()),
            contact_title: Some("CEO/Founder".to_string()),
            email: None,
            source: Some("directory".to_string()),
        };
        let rich = SourceContactHint {
            contact_name: Some("Aylin Demir".to_string()),
            contact_title: Some("CEO".to_string()),
            email: Some("aylin@example.com".to_string()),
            source: Some("directory".to_string()),
        };

        assert_eq!(source_hint_contact_richness_bonus(&generic), 0);
        assert!(source_hint_contact_richness_bonus(&rich) >= 14);
    }

    #[test]
    fn extract_email_from_text_decodes_cloudflare_cfemail() {
        let html = r#"<a class="__cf_email__" data-cfemail="127b7c747d527f73717a7b7c7b666b3c737b">[email&#160;protected]</a>"#;
        let email = extract_email_from_text(html);
        assert_eq!(email.as_deref(), Some("info@machinity.ai"));
    }

    #[test]
    fn normalize_email_candidate_rejects_asset_filenames() {
        assert!(normalize_site_contact_email(Some("logo@2x.png".to_string())).is_none());
        assert!(normalize_site_contact_email(Some("hero@banner.webp".to_string())).is_none());
    }

    #[test]
    fn normalize_contact_email_for_domain_rejects_generic_or_consumer_inboxes() {
        assert!(normalize_contact_email_for_domain(
            Some("info@ornekbakim.com.tr".to_string()),
            "ornekbakim.com.tr"
        )
        .is_none());
        assert!(normalize_contact_email_for_domain(
            Some("info@gmail.com".to_string()),
            "gmail.com"
        )
        .is_none());
        assert_eq!(
            normalize_contact_email_for_domain(
                Some("kiralama@artiplatform.com.tr".to_string()),
                "artiplatform.com.tr"
            )
            .as_deref(),
            Some("kiralama@artiplatform.com.tr")
        );
    }

    #[test]
    fn guessed_email_requires_plausible_person_name() {
        let ok = guessed_email(Some("John Doe"), "example.com");
        let bad = guessed_email(Some("Experience Like No Other"), "example.com");
        let placeholder = guessed_email(Some("Leadership Team"), "example.com");
        assert_eq!(ok.as_deref(), Some("john.doe@example.com"));
        assert!(bad.is_none());
        assert!(placeholder.is_none());
    }

    #[test]
    fn normalize_outreach_linkedin_url_rejects_company_pages() {
        let personal = normalize_outreach_linkedin_url("https://www.linkedin.com/in/jane-doe");
        let company = normalize_outreach_linkedin_url("https://www.linkedin.com/company/openfang");
        assert_eq!(
            personal.as_deref(),
            Some("https://www.linkedin.com/in/jane-doe")
        );
        assert!(company.is_none());
    }

    #[test]
    fn lead_requires_actionable_channel() {
        assert!(lead_has_outreach_channel(
            Some(&"ceo@example.com".to_string()),
            None
        ));
        assert!(lead_has_outreach_channel(
            None,
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(!lead_has_outreach_channel(
            Some(&"info@example.com".to_string()),
            None
        ));
        assert!(!lead_has_outreach_channel(None, None));
    }

    #[test]
    fn build_prospect_profiles_downgrades_generic_inbox_only_contacts() {
        let leads = vec![SalesLead {
            id: "lead-1".to_string(),
            run_id: "run-1".to_string(),
            company: "Acme".to_string(),
            website: "https://acme.example".to_string(),
            company_domain: "acme.example".to_string(),
            contact_name: "Leadership Team".to_string(),
            contact_title: "CEO/Founder".to_string(),
            linkedin_url: None,
            email: Some("info@acme.example".to_string()),
            phone: None,
            reasons: vec!["Public evidence: listed in sector directory".to_string()],
            email_subject: String::new(),
            email_body: String::new(),
            linkedin_message: String::new(),
            score: 84,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        }];

        let profiles = build_prospect_profiles(leads, 10, None);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].profile_status, "company_only");
        assert!(profiles[0].primary_email.is_none());
    }

    #[test]
    fn lead_requires_person_level_identity() {
        assert!(lead_has_person_identity(
            Some("Jane Doe"),
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(lead_has_person_identity(
            None,
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(!lead_has_person_identity(
            Some("Leadership Team"),
            Some(&"https://www.linkedin.com/company/openfang".to_string())
        ));
        assert!(!lead_has_person_identity(Some("Leadership Team"), None));
    }

    #[test]
    fn field_ops_lead_requires_verified_site_signal() {
        // Non-field-ops always passes
        assert!(lead_has_verified_company_signal(
            false,
            Some("Company website mentions maintenance"),
            false
        ));
        // Field-ops with site evidence passes
        assert!(lead_has_verified_company_signal(
            true,
            Some("Company website mentions maintenance"),
            false
        ));
        // Field-ops with LLM validation passes even without site evidence
        assert!(lead_has_verified_company_signal(true, None, true));
        // Field-ops without either fails
        assert!(!lead_has_verified_company_signal(true, None, false));
        assert!(!lead_has_verified_company_signal(true, Some("   "), false));
    }

    #[test]
    fn geo_is_turkey_matches_common_variants() {
        assert!(geo_is_turkey("TR"));
        assert!(geo_is_turkey("Turkey"));
        assert!(geo_is_turkey("Türkiye"));
        assert!(!geo_is_turkey("DE"));
    }

    #[test]
    fn heuristic_plan_adds_local_field_ops_queries_for_turkey() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "AI teammate for field ops".to_string(),
            target_industry:
                "Field service operations, maintenance services, installation services".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let draft = heuristic_lead_query_plan(&profile);
        assert!(draft
            .discovery_queries
            .iter()
            .any(|q| q.contains("saha servis") || q.contains("bakim onarim")));
        assert!(draft
            .must_include_keywords
            .iter()
            .any(|kw| kw == "tesis yonetimi"));
        assert!(draft
            .discovery_queries
            .iter()
            .all(|q| !q.contains("CEO") && !q.contains("COO")));
    }

    #[test]
    fn sanitize_discovery_query_removes_exec_title_noise() {
        let sanitized =
            sanitize_discovery_query("field service maintenance companies TR COO CEO operations");
        assert_eq!(
            sanitized.as_deref(),
            Some("field service maintenance companies TR operations")
        );
    }

    #[test]
    fn normalize_discovery_focus_term_rejects_generic_noise() {
        assert!(normalize_discovery_focus_term("operations").is_none());
        assert!(normalize_discovery_focus_term("workflow").is_none());
        assert_eq!(
            normalize_discovery_focus_term("field service maintenance"),
            Some("field service maintenance".to_string())
        );
    }

    #[test]
    fn adaptive_discovery_queries_add_targeted_follow_ups_for_turkey() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "AI teammate for field ops, dispatch, maintenance workflows"
                .to_string(),
            target_industry:
                "Field service operations, maintenance services, installation services".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let lead_plan = heuristic_lead_query_plan(&profile);
        let queries = build_adaptive_discovery_queries(&lead_plan, &profile, &[]);

        assert!(!queries.is_empty());
        assert!(queries.iter().any(|query| query.contains("site:.tr")));
        assert!(queries.iter().any(|query| {
            query.to_lowercase().contains("saha operasyon")
                || query.to_lowercase().contains("sirketleri")
                || query.to_lowercase().contains("firmalari")
        }));
    }

    #[test]
    fn sitemap_location_parser_extracts_urls() {
        let xml = r#"
            <urlset>
              <url><loc>https://example.com/services/maintenance</loc></url>
              <url><loc>https://example.com/contact</loc></url>
            </urlset>
        "#;
        let urls = extract_sitemap_locations(xml);
        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("/services/maintenance"));
        assert!(urls[1].contains("/contact"));
    }

    #[test]
    fn enrich_target_path_matches_service_and_contact_pages() {
        assert!(path_looks_like_enrich_target("/services/field-service"));
        assert!(path_looks_like_enrich_target("/iletisim"));
        assert!(!path_looks_like_enrich_target("/blog/company-news"));
    }

    #[test]
    fn extract_domains_from_text_pulls_referenced_company_domains() {
        let domains = extract_domains_from_text(
            "Official Website: www.ym.com.tr and contact yminfo@ym.com.tr",
        );
        assert!(domains.iter().any(|d| d == "ym.com.tr"));
    }

    #[test]
    fn extract_company_site_html_accepts_turkish_field_signal_and_offdomain_email() {
        let html = r#"
        <html>
          <body>
            <h1>Yapi Merkezi teknik servis ve santiye operasyonlari</h1>
            <p>Genel Müdür Aslan Uzun</p>
            <a href="mailto:yminfo@ym.com.tr">Bize ulasin</a>
          </body>
        </html>
        "#;
        let (_name, title, _linkedin, email, evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert!(title
            .as_deref()
            .map(|value| value.contains("CEO"))
            .unwrap_or(false));
        assert_eq!(email.as_deref(), Some("yminfo@ym.com.tr"));
        assert!(evidence
            .as_deref()
            .map(|value| value.contains("teknik servis") || value.contains("santiye"))
            .unwrap_or(false));
    }

    #[test]
    fn extract_company_site_html_rejects_navigation_noise_and_asset_emails() {
        let html = r#"
        <html>
          <head>
            <title>Anasayfa | Kolin</title>
            <meta property="og:title" content="Reconstruction de la route Kandahar-Herat" />
          </head>
          <body>
            <a href="/tr/kurumsal/kisisel-verilerin-korunmasi-kanunu">Kişisel Verilerin Korunması Kanunu</a>
            <a href="/tr/kurumsal/finansal-gostergeler">Finansal Göstergeler</a>
            <img src="/Templates/Default/assets/img/logo.png" srcset="/Templates/Default/assets/img/logo@2x.png 2x" alt="Kolin İnşaat">
            <p>Kolin İnşaat altyapı ve construction projeleri yürütür.</p>
          </body>
        </html>
        "#;

        let (name, title, _linkedin, email, evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert!(name.is_none());
        assert_eq!(title.as_deref(), Some("CEO/Founder"));
        assert!(email.is_none());
        assert!(evidence
            .as_deref()
            .map(|value| value.contains("altyapı") || value.contains("construction"))
            .unwrap_or(false));
    }

    #[test]
    fn site_contact_candidate_signal_prefers_named_exec_page() {
        let generic_score = site_contact_candidate_signal(
            None,
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        let executive_score = site_contact_candidate_signal(
            Some(&"Naci Ağbal".to_string()),
            Some(&"CEO".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        assert!(executive_score > generic_score);
    }

    #[test]
    fn site_contact_candidate_signal_does_not_reward_placeholder_name() {
        let placeholder_score = site_contact_candidate_signal(
            Some(&"Leadership Team".to_string()),
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        let generic_score = site_contact_candidate_signal(
            None,
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        assert_eq!(placeholder_score, generic_score);
    }

    #[test]
    fn site_enrichment_replaces_placeholder_identity_with_real_exec() {
        let enrichment = SiteContactEnrichment {
            name: Some("Başar Arıoğlu".to_string()),
            title: Some("Chairman".to_string()),
            linkedin_url: None,
            company_linkedin_url: Some(
                "https://www.linkedin.com/company/yapi-merkezi/".to_string(),
            ),
            email: Some("yminfo@ym.com.tr".to_string()),
            evidence: Some("Company website mentions 'altyapı'".to_string()),
            osint_links: vec![
                "https://www.linkedin.com/company/yapi-merkezi/".to_string(),
                "https://yapimerkezi.com.tr/yonetim".to_string(),
            ],
            tech_stack: Vec::new(),
            job_posting_signals: Vec::new(),
            signal: site_contact_candidate_signal(
                Some(&"Başar Arıoğlu".to_string()),
                Some(&"Chairman".to_string()),
                None,
                Some(&"yminfo@ym.com.tr".to_string()),
                Some(&"Company website mentions 'altyapı'".to_string()),
            ),
        };
        let mut contact_name = Some("Leadership Team".to_string());
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut linkedin_url = None;
        let mut company_linkedin_url = None;
        let mut email = Some("yminfo@ym.com.tr".to_string());
        let mut osint_links = Vec::new();
        let mut email_from_verified_site = false;
        let mut site_evidence = Some("Company website mentions 'altyapı'".to_string());

        apply_site_contact_enrichment(
            "yapimerkezi.com.tr",
            &enrichment,
            &mut contact_name,
            &mut contact_title,
            &mut linkedin_url,
            &mut company_linkedin_url,
            &mut email,
            &mut osint_links,
            &mut email_from_verified_site,
            &mut site_evidence,
        );

        assert_eq!(contact_name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert_eq!(email.as_deref(), Some("yminfo@ym.com.tr"));
        assert_eq!(
            company_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/company/yapi-merkezi/")
        );
        assert!(osint_links
            .iter()
            .any(|value| value.contains("linkedin.com/company/yapi-merkezi")));
        assert!(email_from_verified_site);
    }

    #[test]
    fn best_site_contact_enrichment_combines_identity_with_signal_and_email() {
        let pages = SiteFetchBundle {
            osint_links: vec![
                "https://yapimerkezi.com.tr".to_string(),
                "https://yapimerkezi.com.tr/yonetim".to_string(),
            ],
            pages: vec![
                SiteHtmlPage {
                    url: "https://yapimerkezi.com.tr".to_string(),
                    html: r#"
            <html>
              <body>
                <h1>Yapı Merkezi altyapı ve inşaat projeleri</h1>
                <a href="mailto:yminfo@ym.com.tr">İletişim</a>
              </body>
            </html>
            "#
                    .to_string(),
                },
                SiteHtmlPage {
                    url: "https://yapimerkezi.com.tr/yonetim".to_string(),
                    html: r#"
            <html>
              <body>
                <p><strong><a href="/yonetim/basar-arioglu">Başar Arıoğlu</a></strong></p>
                <p><span>Yönetim Kurulu Başkanı</span></p>
              </body>
            </html>
            "#
                    .to_string(),
                },
            ],
        };

        let enrichment = best_site_contact_enrichment(pages, "ceo_then_founder");
        assert_eq!(enrichment.name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(enrichment.title.as_deref(), Some("Chairman"));
        assert_eq!(enrichment.email.as_deref(), Some("yminfo@ym.com.tr"));
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("/yonetim")));
        assert!(enrichment
            .evidence
            .as_deref()
            .map(|value| value.contains("altyapı") || value.contains("insaat"))
            .unwrap_or(false));
    }

    #[test]
    fn best_search_contact_enrichment_keeps_company_linkedin_separate() {
        let outputs = vec![r#"
1. Bergiz Holding | LinkedIn
URL: https://www.linkedin.com/company/bergiz-holding/
Bergiz Holding resmi LinkedIn sayfasi

2. Bergiz Holding - Sirket Profili
URL: https://bergiz.com.tr/Hakkimizda/Detay/SirketProfili
Bergiz Holding altyapi ve insaat projeleri yurutur.
            "#
        .to_string()];

        let enrichment = best_search_contact_enrichment(
            &outputs,
            "ceo_then_founder",
            "Bergiz Holding",
            "bergiz.com.tr",
        );
        assert!(enrichment.linkedin_url.is_none());
        assert_eq!(
            enrichment.company_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/company/bergiz-holding/")
        );
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("linkedin.com/company/bergiz-holding")));
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("bergiz.com.tr/Hakkimizda/Detay/SirketProfili")));
    }

    #[test]
    fn site_contact_enrichment_has_signal_for_company_linkedin_only() {
        let enrichment = SiteContactEnrichment {
            company_linkedin_url: Some(
                "https://www.linkedin.com/company/bergiz-holding/".to_string(),
            ),
            ..SiteContactEnrichment::default()
        };
        assert!(site_contact_enrichment_has_signal(&enrichment));
    }

    #[test]
    fn contact_title_priority_ignores_bare_board_labels() {
        assert_eq!(contact_title_priority("Yönetim Kurulu"), 0);
        assert!(contact_title_priority("Yönetim Kurulu Başkanı") > 0);
        assert_eq!(contact_title_priority("Finansal Göstergeler"), 0);
        assert!(contact_title_priority("Finance Director") > 0);
    }

    #[test]
    fn contact_title_priority_rejects_sentence_like_titles() {
        let noisy = "Türkiye genelinde aldığımız 941 MW depolamalı yenilenebilir enerji projeleriyle gelecek nesillere daha yaşanabilir bir dünya bırakmak için var gücümüzle çalışıyor";
        assert_eq!(contact_title_priority(noisy), 0);
    }

    #[test]
    fn normalize_sales_profile_trims_and_clamps_fields() {
        let profile = SalesProfile {
            product_name: "  Machinity  ".to_string(),
            product_description: "  AI teammate for field teams  ".to_string(),
            target_industry: "  Field Operations ".to_string(),
            target_geo: " tr ".to_string(),
            sender_name: "  Machinity Team ".to_string(),
            sender_email: " founder@machinity.ai ".to_string(),
            sender_linkedin: Some("   ".to_string()),
            target_title_policy: "unexpected".to_string(),
            daily_target: 999,
            daily_send_cap: 0,
            schedule_hour_local: 44,
            timezone_mode: "UTC".to_string(),
        };

        let normalized = normalize_sales_profile(profile).expect("profile normalizes");
        assert_eq!(normalized.product_name, "Machinity");
        assert_eq!(normalized.target_geo, "TR");
        assert_eq!(normalized.sender_email, "founder@machinity.ai");
        assert!(normalized.sender_linkedin.is_none());
        assert_eq!(normalized.target_title_policy, "ceo_then_founder");
        assert_eq!(normalized.daily_target, 200);
        assert_eq!(normalized.daily_send_cap, 1);
        assert_eq!(normalized.schedule_hour_local, 23);
        assert_eq!(normalized.timezone_mode, "utc");
    }

    #[test]
    fn timestamp_matches_sales_day_uses_local_timezone_conversion() {
        let raw = "2026-01-10T23:30:00+00:00";
        let local_day = chrono::DateTime::parse_from_rfc3339(raw)
            .expect("parse")
            .with_timezone(&Local)
            .date_naive();
        assert!(timestamp_matches_sales_day(raw, local_day, "local"));
        assert!(!timestamp_matches_sales_day(
            raw,
            local_day.succ_opt().expect("next day"),
            "local"
        ));
    }

    #[test]
    fn timestamp_matches_sales_day_can_use_utc_mode() {
        let raw = "2026-01-10T23:30:00+00:00";
        let utc_day = chrono::DateTime::parse_from_rfc3339(raw)
            .expect("parse")
            .with_timezone(&Utc)
            .date_naive();
        assert!(timestamp_matches_sales_day(raw, utc_day, "utc"));
        assert!(!timestamp_matches_sales_day(
            raw,
            utc_day.succ_opt().expect("next day"),
            "utc"
        ));
    }

    // =======================================================================
    // SPEC VERIFICATION TESTS — Phase 0 Checklist
    // =======================================================================

    #[test]
    fn spec_p0_consumer_domain_rejected() {
        assert!(!is_valid_company_domain("gmail.com"));
        assert!(!is_valid_company_domain("yahoo.com"));
        assert!(!is_valid_company_domain("hotmail.com"));
        assert!(!is_valid_company_domain("outlook.com"));
        assert!(!is_valid_company_domain("protonmail.com"));
    }

    #[test]
    fn spec_p0_valid_company_domain_accepted() {
        assert!(is_valid_company_domain("machinity.com"));
        assert!(is_valid_company_domain("acme.com.tr"));
        assert!(is_valid_company_domain("example-corp.com"));
    }

    #[test]
    fn spec_p0_gov_edu_mil_domains_rejected() {
        assert!(!is_valid_company_domain("ankara.gov.tr"));
        assert!(!is_valid_company_domain("odtu.edu.tr"));
        assert!(!is_valid_company_domain("tsk.mil.tr"));
    }

    #[test]
    fn spec_p0_turkish_placeholder_detected() {
        assert!(is_placeholder_name("Başkan'ın Mesajı"));
        assert!(is_placeholder_name("baskanin mesaji"));
        assert!(is_placeholder_name("Genel Müdürün Mesajı"));
        assert!(is_placeholder_name("Hakkımızda"));
        assert!(is_placeholder_name("Vizyonumuz"));
        assert!(is_placeholder_name("İletişim"));
        assert!(is_placeholder_name("Kariyer"));
        assert!(is_placeholder_name("Yönetim Kurulu"));
    }

    #[test]
    fn spec_p0_real_names_not_placeholder() {
        assert!(!is_placeholder_name("Ali Vural"));
        assert!(!is_placeholder_name("Mehmet Kaya"));
        assert!(!is_placeholder_name("Ayşe Demir"));
    }

    #[test]
    fn spec_p0_phone_normalization_e164() {
        assert_eq!(
            normalize_phone("0530 851 89 61"),
            Some("+905308518961".to_string())
        );
        assert_eq!(
            normalize_phone("+90 530 851 89 61"),
            Some("+905308518961".to_string())
        );
        assert_eq!(
            normalize_phone("5308518961"),
            Some("+905308518961".to_string())
        );
        assert_eq!(normalize_phone("123"), None); // too short
    }

    #[test]
    fn spec_p0_email_classification() {
        assert_eq!(classify_email("info@acme.com", "acme.com"), "generic");
        assert_eq!(classify_email("ali.vural@acme.com", "acme.com"), "personal");
        assert_eq!(classify_email("user@gmail.com", "acme.com"), "consumer");
        assert_eq!(classify_email("not-an-email", "acme.com"), "invalid");
        // sales@ and hr@ are generic role mailboxes
        assert_eq!(classify_email("sales@acme.com", "acme.com"), "generic");
        assert_eq!(classify_email("hr@acme.com", "acme.com"), "generic");
    }

    #[test]
    fn spec_p0_target_geo_empty_default() {
        let profile = SalesProfile::default();
        assert!(
            profile.target_geo.is_empty(),
            "target_geo should default to empty to force user to set it"
        );
    }

    #[test]
    fn spec_p0_candidate_gateway_rejects_consumer() {
        let mut candidate = DomainCandidate {
            domain: "gmail.com".to_string(),
            ..Default::default()
        };
        assert!(!normalize_candidate_gateway(&mut candidate));
    }

    #[test]
    fn spec_p0_candidate_gateway_accepts_valid() {
        let mut candidate = DomainCandidate {
            domain: "machinity.com".to_string(),
            score: 10,
            ..Default::default()
        };
        assert!(normalize_candidate_gateway(&mut candidate));
    }

    #[test]
    fn spec_p0_candidate_gateway_normalizes_phone() {
        let mut candidate = DomainCandidate {
            domain: "example.com.tr".to_string(),
            phone: Some("0532 123 45 67".to_string()),
            ..Default::default()
        };
        assert!(normalize_candidate_gateway(&mut candidate));
        assert_eq!(candidate.phone.as_deref(), Some("+905321234567"));
    }

    // =======================================================================
    // SPEC VERIFICATION TESTS — Phase 1 Checklist
    // =======================================================================

    #[test]
    fn spec_p1_five_axis_score_struct() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.6,
            reachability_score: 0.7,
            deliverability_risk: 0.1,
            compliance_risk: 0.05,
        };
        assert!(score.fit_score > 0.0 && score.fit_score <= 1.0);
        assert!(score.deliverability_risk >= 0.0 && score.deliverability_risk <= 1.0);
    }

    #[test]
    fn spec_p1_send_gate_block_on_high_deliverability_risk() {
        let score = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.8,
            reachability_score: 0.9,
            deliverability_risk: 0.8,
            compliance_risk: 0.0,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Block { .. }));
    }

    #[test]
    fn spec_p1_send_gate_block_on_high_compliance_risk() {
        let score = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.8,
            reachability_score: 0.9,
            deliverability_risk: 0.1,
            compliance_risk: 0.6,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Block { .. }));
    }

    #[test]
    fn spec_p1_send_gate_research_on_low_reachability() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.5,
            reachability_score: 0.1,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(
            send_gate(&score),
            SendGateDecision::Research { .. }
        ));
    }

    #[test]
    fn spec_p1_send_gate_nurture_on_low_intent() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.1,
            reachability_score: 0.5,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(
            send_gate(&score),
            SendGateDecision::Nurture { .. }
        ));
    }

    #[test]
    fn spec_p1_send_gate_activate_on_good_account() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.6,
            reachability_score: 0.7,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Activate));
    }

    #[test]
    fn spec_p1_tier_assignment() {
        let high = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.7,
            reachability_score: 0.8,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert_eq!(assign_tier(&high), "a_tier");

        let mid = FiveAxisScore {
            fit_score: 0.6,
            intent_score: 0.3,
            reachability_score: 0.5,
            deliverability_risk: 0.2,
            compliance_risk: 0.1,
        };
        assert_eq!(assign_tier(&mid), "standard");

        let low = FiveAxisScore {
            fit_score: 0.3,
            intent_score: 0.1,
            reachability_score: 0.2,
            deliverability_risk: 0.5,
            compliance_risk: 0.4,
        };
        assert_eq!(assign_tier(&low), "basic");
    }

    #[test]
    fn spec_p1_signal_horizon_classification() {
        let (horizon, expires) = classify_signal_horizon("tender", "ihale");
        assert_eq!(horizon, "immediate");
        assert!(expires.is_some());

        let (horizon, _) = classify_signal_horizon("directory_membership", "member");
        assert_eq!(horizon, "structural");

        let (horizon, _) = classify_signal_horizon("job_posting", "acil pozisyon");
        assert_eq!(horizon, "immediate");

        let (horizon, _) = classify_signal_horizon("job_posting", "saha muduru");
        assert_eq!(horizon, "campaign_window");
    }

    #[test]
    fn spec_p1_source_confidence_hierarchy() {
        assert!(source_confidence("directory_listing") > source_confidence("site_html"));
        assert!(source_confidence("site_html") > source_confidence("web_search"));
        assert!(source_confidence("web_search") > source_confidence("llm_enrichment"));
        assert!(source_confidence("llm_enrichment") > source_confidence("llm_generation"));
    }

    #[test]
    fn spec_p1_reply_classification() {
        assert_eq!(classify_reply_content("toplanti yapalim"), "meeting_booked");
        assert_eq!(classify_reply_content("ilginc gorunuyor"), "interested");
        assert_eq!(classify_reply_content("simdi degil"), "not_now");
        assert_eq!(classify_reply_content("yanlis kisi"), "wrong_person");
        assert_eq!(classify_reply_content("beni listeden cikar"), "unsubscribe");
    }

    // =======================================================================
    // NEW FEATURE TESTS
    // =======================================================================

    #[test]
    fn spec_email_pattern_guesser_produces_multiple_patterns() {
        let patterns = guess_personal_email_patterns(Some("Ali Vural"), "acme.com.tr");
        assert!(patterns.len() >= 3, "Should produce at least 3 patterns");
        assert!(patterns.contains(&"ali.vural@acme.com.tr".to_string()));
        assert!(patterns.contains(&"avural@acme.com.tr".to_string()));
        assert!(patterns.contains(&"ali@acme.com.tr".to_string()));
    }

    #[test]
    fn spec_email_pattern_guesser_handles_turkish_chars() {
        let patterns = guess_personal_email_patterns(Some("Şükrü Öztürk"), "firma.com.tr");
        assert!(!patterns.is_empty());
        // Turkish chars should be transliterated
        assert!(patterns[0].contains("sukru") || patterns[0].contains("ozturk"));
    }

    #[test]
    fn spec_email_pattern_guesser_rejects_placeholder() {
        let patterns = guess_personal_email_patterns(Some("Leadership Team"), "acme.com");
        assert!(patterns.is_empty());
    }

    #[test]
    fn spec_tech_stack_detection_finds_sap() {
        let html = r#"<html><body><script src="https://sap.com/sap-ui.js"></script></body></html>"#;
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"SAP".to_string()));
    }

    #[test]
    fn spec_tech_stack_detection_finds_hubspot() {
        let html =
            r#"<html><body><script src="//js.hs-scripts.com/1234.js"></script></body></html>"#;
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"HubSpot".to_string()));
    }

    #[test]
    fn spec_tech_stack_detection_empty_on_clean_html() {
        let html = "<html><body><p>Hello world</p></body></html>";
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.is_empty());
    }

    #[test]
    fn spec_tech_stack_detection_uses_headers() {
        let html = "<html><body></body></html>";
        let mut headers = HashMap::new();
        headers.insert("x-powered-by".to_string(), "Express".to_string());
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"Express".to_string()));
    }

    #[test]
    fn spec_job_posting_signal_extraction() {
        let results = vec![
            SearchEntry {
                title: "Acme Corp - Saha Operasyon Yoneticisi".to_string(),
                url: "https://kariyer.net/is-ilani/acme".to_string(),
                snippet: String::new(),
            },
            SearchEntry {
                title: "Random unrelated post".to_string(),
                url: "https://example.com".to_string(),
                snippet: String::new(),
            },
        ];
        let signals = extract_job_posting_signals(&results, "Acme Corp");
        assert_eq!(signals.len(), 1);
        assert!(signals[0].0.contains("Saha"));
        assert!(signals[0].2 > 0.5); // confidence
    }

    #[test]
    fn spec_message_strategy_turkish_for_tr_geo() {
        let profile = SalesProfile {
            target_geo: "TR".to_string(),
            product_name: "TestProd".to_string(),
            ..SalesProfile::default()
        };
        let strategy = generate_message_strategy(&profile, "Acme", Some("Ali"), "signal", "ops");
        assert_eq!(strategy.language, "tr");
    }

    #[test]
    fn spec_message_copy_refuses_without_evidence() {
        let strategy = MessageStrategy {
            pain_angle: String::new(),
            trigger_evidence: String::new(),
            ..Default::default()
        };
        let profile = SalesProfile::default();
        let result = generate_message_copy(&strategy, &profile, "Acme", Some("Ali"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("REFUSED"));
    }

    #[test]
    fn spec_message_copy_succeeds_with_evidence() {
        let strategy = MessageStrategy {
            pain_angle: "field ops coordination".to_string(),
            trigger_evidence: "directory membership signal".to_string(),
            language: "en".to_string(),
            cta_type: "soft".to_string(),
            tone: "warm".to_string(),
        };
        let profile = SalesProfile {
            product_name: "TestProd".to_string(),
            sender_name: "Sender".to_string(),
            ..SalesProfile::default()
        };
        let result = generate_message_copy(&strategy, &profile, "Acme", Some("Ali"));
        assert!(result.is_ok());
        let copy = result.unwrap();
        assert!(!copy.subject.is_empty());
        assert!(!copy.body.is_empty());
    }

    #[test]
    fn spec_seniority_from_title_detects_clevel() {
        assert_eq!(seniority_from_title(Some("CEO")), "c_level");
        assert_eq!(
            seniority_from_title(Some("Chief Operating Officer")),
            "c_level"
        );
        assert_eq!(seniority_from_title(Some("Genel Müdür")), "c_level");
        assert_eq!(seniority_from_title(Some("Founder & CEO")), "c_level");
    }

    #[test]
    fn spec_seniority_from_title_detects_levels() {
        assert_eq!(seniority_from_title(Some("VP Engineering")), "vp");
        assert_eq!(seniority_from_title(Some("Director of Ops")), "director");
        assert_eq!(seniority_from_title(Some("Operations Manager")), "manager");
        assert_eq!(seniority_from_title(Some("Intern")), "unknown");
    }

    #[test]
    fn spec_transliterate_turkish_ascii() {
        assert_eq!(transliterate_turkish_ascii("Şükrü Öztürk"), "sukru ozturk");
        assert_eq!(transliterate_turkish_ascii("İstanbul"), "istanbul");
        assert_eq!(transliterate_turkish_ascii("Çağrı"), "cagri");
    }

    #[test]
    fn spec_email_syntax_valid() {
        assert!(email_syntax_valid("ali@example.com"));
        assert!(email_syntax_valid("a.b@c.d"));
        assert!(!email_syntax_valid("notanemail"));
        assert!(!email_syntax_valid("@domain.com"));
        assert!(!email_syntax_valid("user@"));
        assert!(!email_syntax_valid("user@.com"));
    }

    #[test]
    fn spec_sequence_advancement_completes_on_positive_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(temp.path());
        engine.init().unwrap();
        let conn = engine.open().unwrap();
        let now = Utc::now().to_rfc3339();
        engine.ensure_default_sequence_template(&conn).unwrap();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, created_at, updated_at) VALUES ('acc1', 'Test Co', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO contacts (id, account_id, full_name, created_at) VALUES ('c1', 'acc1', 'Ali', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO sequence_instances (id, template_id, account_id, contact_id, current_step, status, started_at, updated_at)
             VALUES ('seq1', 'default_outreach_sequence', 'acc1', 'c1', 1, 'active', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO touches (id, sequence_instance_id, step, channel, message_payload, created_at)
             VALUES ('t1', 'seq1', 1, 'email', '{}', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, classified_at) VALUES ('o1', 't1', 'meeting_booked', ?1)",
            params![now],
        ).unwrap();

        let advanced = engine.advance_sequences().unwrap();
        assert!(advanced >= 1);

        let status: String = conn
            .query_row(
                "SELECT status FROM sequence_instances WHERE id = 'seq1'",
                [],
                |r: &rusqlite::Row| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn spec_sequence_advancement_cancels_on_bounce() {
        let temp = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(temp.path());
        engine.init().unwrap();
        let conn = engine.open().unwrap();
        let now = Utc::now().to_rfc3339();
        engine.ensure_default_sequence_template(&conn).unwrap();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, created_at, updated_at) VALUES ('acc2', 'Bounce Co', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO contacts (id, account_id, full_name, created_at) VALUES ('c2', 'acc2', 'Test', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO sequence_instances (id, template_id, account_id, contact_id, current_step, status, started_at, updated_at)
             VALUES ('seq2', 'default_outreach_sequence', 'acc2', 'c2', 1, 'active', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO touches (id, sequence_instance_id, step, channel, message_payload, created_at)
             VALUES ('t2', 'seq2', 1, 'email', '{}', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, classified_at) VALUES ('o2', 't2', 'hard_bounce', ?1)",
            params![now],
        ).unwrap();

        engine.advance_sequences().unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM sequence_instances WHERE id = 'seq2'",
                [],
                |r: &rusqlite::Row| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "cancelled");
    }

    #[test]
    fn spec_mailbox_pool_selects_lowest_sends() {
        let mut cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    email: "a@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 8,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
                MailboxConfig {
                    email: "b@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 3,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
            ],
        };
        let picked = cfg.select_mailbox().unwrap();
        assert_eq!(picked.email, "b@send.example.com");
    }

    #[test]
    fn spec_mailbox_pool_skips_cold() {
        let mut cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    email: "cold@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "cold".into(),
                    sends_today: 0,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
                MailboxConfig {
                    email: "warm@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 5,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
            ],
        };
        let picked = cfg.select_mailbox().unwrap();
        assert_eq!(picked.email, "warm@send.example.com");
    }

    #[test]
    fn spec_mailbox_pool_exhausted_returns_none() {
        let mut cfg = SenderConfig {
            mailboxes: vec![MailboxConfig {
                email: "full@send.example.com".into(),
                daily_cap: 5,
                warm_state: "warm".into(),
                sends_today: 5,
                counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                ..Default::default()
            }],
        };
        assert!(cfg.select_mailbox().is_none());
    }

    #[test]
    fn spec_mailbox_warming_cap_limited() {
        let mb = MailboxConfig {
            daily_cap: 50,
            warm_state: "warming".into(),
            ..Default::default()
        };
        assert_eq!(mb.effective_cap(), 15); // warming caps at 15
    }

    #[test]
    fn spec_sender_remaining_capacity() {
        let cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 3,
                    ..Default::default()
                },
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 7,
                    ..Default::default()
                },
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "cold".into(),
                    sends_today: 0,
                    ..Default::default()
                },
            ],
        };
        assert_eq!(cfg.remaining_capacity(), 10); // 7 + 3 from warm mailboxes
    }

    #[test]
    fn spec_mailbox_pool_parses_legacy_string_entries() {
        let pool =
            mailbox_pool_from_json(r#"["legacy@send.example.com", "SECOND@send.example.com "]"#);
        assert_eq!(pool.len(), 2);
        assert_eq!(pool[0].email, "legacy@send.example.com");
        assert_eq!(pool[1].email, "second@send.example.com");
        assert_eq!(pool[0].warm_state, "warming");
        assert_eq!(pool[0].daily_cap, 20);
    }

    #[test]
    fn spec_record_mailbox_send_persists_daily_counter() {
        let dir = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(dir.path());
        engine.init().unwrap();
        let sender_cfg = SenderConfig {
            mailboxes: vec![MailboxConfig {
                email: "warm@send.example.com".into(),
                daily_cap: 10,
                warm_state: "warm".into(),
                counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                ..Default::default()
            }],
        };
        engine.save_sender_config(&sender_cfg).unwrap();

        engine.record_mailbox_send("warm@send.example.com").unwrap();
        let reloaded = engine.load_sender_config();

        assert_eq!(reloaded.mailboxes.len(), 1);
        assert_eq!(reloaded.mailboxes[0].sends_today, 1);
        assert_eq!(reloaded.mailboxes[0].email, "warm@send.example.com");
    }

    #[test]
    fn spec_context_factors_seeded_on_init() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS contextual_factors (
                id TEXT PRIMARY KEY,
                factor_type TEXT NOT NULL,
                factor_key TEXT NOT NULL,
                factor_value TEXT,
                effective_from TEXT,
                effective_until TEXT,
                source TEXT
            );",
        )
        .unwrap();
        seed_contextual_factors(&conn);
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM contextual_factors", [], |r| r.get(0))
            .unwrap();
        assert!(
            count >= 10,
            "Expected at least 10 contextual factors, got {count}"
        );
        // Check specific factors exist
        let ramazan: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM contextual_factors WHERE factor_key = 'ramazan_bayrami'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ramazan, 1);
        let kvkk: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM contextual_factors WHERE factor_key = 'kvkk'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kvkk, 1);
        // Idempotent — re-seeding should not duplicate
        seed_contextual_factors(&conn);
        let count2: i32 = conn
            .query_row("SELECT COUNT(*) FROM contextual_factors", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, count2);
    }

    #[test]
    fn spec_experiment_create_and_balanced_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, hypothesis TEXT,
                variant_a TEXT, variant_b TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS experiment_assignments (
                id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL,
                sequence_instance_id TEXT, variant TEXT NOT NULL
            );",
        )
        .unwrap();

        let exp_id = create_experiment(
            &conn,
            "subject_line_test",
            "Shorter subjects get more opens",
            "short",
            "long",
        )
        .unwrap();
        assert!(!exp_id.is_empty());

        // Assign multiple sequences — should balance a/b
        let v1 = assign_experiment_variant(&conn, &exp_id, "seq_001").unwrap();
        let v2 = assign_experiment_variant(&conn, &exp_id, "seq_002").unwrap();
        assert_eq!(v1, "a");
        assert_eq!(v2, "b");
        let v3 = assign_experiment_variant(&conn, &exp_id, "seq_003").unwrap();
        assert_eq!(v3, "a");
    }

    #[test]
    fn spec_calibration_creates_proposals_from_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outcomes (
                id TEXT PRIMARY KEY, touch_id TEXT NOT NULL,
                outcome_type TEXT NOT NULL,
                classified_at TEXT NOT NULL DEFAULT (datetime('now')),
                classifier_confidence REAL DEFAULT 1.0
            );
            CREATE TABLE IF NOT EXISTS outcome_attribution_snapshots (
                id TEXT PRIMARY KEY, touch_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                snapshot_at TEXT NOT NULL DEFAULT (datetime('now')),
                score_at_touch_json TEXT, active_signal_ids TEXT,
                unused_signal_ids TEXT, thesis_id TEXT,
                sequence_variant TEXT, message_variant TEXT,
                channel TEXT, mailbox_id TEXT, contextual_factors_json TEXT
            );
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                signal_type TEXT NOT NULL, text TEXT NOT NULL,
                source TEXT, observed_at TEXT, confidence REAL DEFAULT 0.5,
                effect_horizon TEXT, expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS retrieval_rule_versions (
                id TEXT PRIMARY KEY, rule_type TEXT NOT NULL,
                rule_key TEXT NOT NULL, old_value TEXT,
                new_value TEXT NOT NULL, proposal_source TEXT,
                backtest_result_json TEXT, holdout_result_json TEXT,
                status TEXT NOT NULL DEFAULT 'proposed',
                approved_by TEXT, activated_at TEXT,
                version INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .unwrap();

        // Not enough data → no proposals
        let proposals = calibrate_scoring_from_outcomes(&conn).unwrap();
        assert!(proposals.is_empty());

        // Insert enough outcomes with signals
        for i in 0..12 {
            let touch_id = format!("touch_{i}");
            let account_id = format!("acc_{i}");
            conn.execute(
                "INSERT INTO outcome_attribution_snapshots (id, touch_id, account_id, snapshot_at) VALUES (?1, ?2, ?3, datetime('now'))",
                params![format!("snap_{i}"), touch_id, account_id],
            ).unwrap();
            conn.execute(
                "INSERT INTO outcomes (id, touch_id, outcome_type) VALUES (?1, ?2, ?3)",
                params![
                    format!("out_{i}"),
                    touch_id,
                    if i < 8 {
                        "hard_bounce"
                    } else {
                        "meeting_booked"
                    }
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO signals (id, account_id, signal_type, text) VALUES (?1, ?2, 'directory_membership', ?3)",
                params![format!("sig_{i}"), account_id, format!("Signal {i}")],
            ).unwrap();
        }

        let proposals = calibrate_scoring_from_outcomes(&conn).unwrap();
        assert!(
            !proposals.is_empty(),
            "Should have created calibration proposals"
        );
    }

    #[test]
    fn spec_verify_domain_exists_basic() {
        // This is an async function — just verify it compiles and the signature is correct
        // Actual HTTP verification would require network access
        let _fn_exists: fn(
            &str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = bool> + Send + '_>,
        > = |domain| Box::pin(verify_domain_exists(domain));
    }
}
