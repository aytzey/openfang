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
use pulsivo_salesman_runtime::llm_driver::{CompletionRequest, DriverConfig};
use pulsivo_salesman_runtime::web_cache::WebCache;
use pulsivo_salesman_runtime::web_search::WebSearchEngine;
use pulsivo_salesman_types::agent::ReasoningEffort;
use pulsivo_salesman_types::config::SearchProvider;
use pulsivo_salesman_types::message::Message as LlmMessage;
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
const SALES_UNSUBSCRIBE_SALT: &str = "pulsivo-salesman-sales-unsubscribe";
const SALES_SEGMENT_B2B: &str = "b2b";
const SALES_SEGMENT_B2C: &str = "b2c";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SalesSegment {
    B2B,
    B2C,
}

impl SalesSegment {
    fn as_str(&self) -> &'static str {
        match self {
            Self::B2B => SALES_SEGMENT_B2B,
            Self::B2C => SALES_SEGMENT_B2C,
        }
    }

    fn from_optional(value: Option<&str>) -> Self {
        match value
            .unwrap_or(SALES_SEGMENT_B2B)
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            SALES_SEGMENT_B2C => Self::B2C,
            _ => Self::B2B,
        }
    }

    fn is_b2c(&self) -> bool {
        matches!(self, Self::B2C)
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct B2cDiscoveryCandidate {
    key: String,
    platform: String,
    handle: String,
    display_name: String,
    profile_url: String,
    score: i32,
    evidence: Vec<String>,
    matched_signals: Vec<String>,
    bio_hint: Option<String>,
    locality_hint: Option<String>,
    local_market_signals: Vec<String>,
}

trait SocialScraperAdapter {
    fn platform(&self) -> &'static str;
    fn normalize_profile_url(&self, raw: &str) -> Option<String>;
    fn profile_key(&self, url: &str) -> Option<String>;
    fn extract_handle(&self, url: &str) -> Option<String>;
    fn extract_display_name(&self, title: &str, handle: &str) -> String;
    fn extract_bio_hint(&self, html: &str) -> Option<String>;
}

#[derive(Debug, Clone, Copy)]
struct InstagramScraperAdapter;

#[derive(Debug, Clone, Copy)]
struct TikTokScraperAdapter;

static INSTAGRAM_SCRAPER_ADAPTER: InstagramScraperAdapter = InstagramScraperAdapter;
static TIKTOK_SCRAPER_ADAPTER: TikTokScraperAdapter = TikTokScraperAdapter;

impl SocialScraperAdapter for InstagramScraperAdapter {
    fn platform(&self) -> &'static str {
        "instagram"
    }

    fn normalize_profile_url(&self, raw: &str) -> Option<String> {
        normalize_instagram_profile_url(raw)
    }

    fn profile_key(&self, url: &str) -> Option<String> {
        self.extract_handle(url).map(|handle| format!("ig::{handle}"))
    }

    fn extract_handle(&self, url: &str) -> Option<String> {
        normalize_instagram_profile_url(url).and_then(|value| {
            url::Url::parse(&value)
                .ok()
                .and_then(|parsed| {
                    parsed
                        .path()
                        .trim_matches('/')
                        .split('/')
                        .next()
                        .map(|part| part.trim().to_string())
                })
                .filter(|handle| !handle.is_empty())
        })
    }

    fn extract_display_name(&self, title: &str, handle: &str) -> String {
        let cleaned = title
            .split(['|', '-', '•'])
            .next()
            .unwrap_or(title)
            .replace("(@", " (")
            .trim()
            .trim_matches('"')
            .to_string();
        if cleaned.is_empty() || cleaned.eq_ignore_ascii_case("instagram") {
            handle.to_string()
        } else {
            cleaned
        }
    }

    fn extract_bio_hint(&self, html: &str) -> Option<String> {
        extract_meta_description(html)
    }
}

impl SocialScraperAdapter for TikTokScraperAdapter {
    fn platform(&self) -> &'static str {
        "tiktok"
    }

    fn normalize_profile_url(&self, raw: &str) -> Option<String> {
        normalize_tiktok_profile_url(raw)
    }

    fn profile_key(&self, url: &str) -> Option<String> {
        self.extract_handle(url).map(|handle| format!("tt::{handle}"))
    }

    fn extract_handle(&self, url: &str) -> Option<String> {
        normalize_tiktok_profile_url(url).and_then(|value| {
            url::Url::parse(&value)
                .ok()
                .and_then(|parsed| {
                    parsed
                        .path()
                        .trim_matches('/')
                        .split('/')
                        .next()
                        .map(|part| part.trim().to_string())
                })
                .filter(|handle| !handle.is_empty())
        })
    }

    fn extract_display_name(&self, title: &str, handle: &str) -> String {
        let cleaned = title
            .split(['|', '-', '•'])
            .next()
            .unwrap_or(title)
            .trim()
            .trim_matches('"')
            .to_string();
        if cleaned.is_empty() || cleaned.eq_ignore_ascii_case("tiktok") {
            handle.to_string()
        } else {
            cleaned
        }
    }

    fn extract_bio_hint(&self, html: &str) -> Option<String> {
        extract_meta_description(html)
    }
}

fn social_scraper_adapters() -> [&'static dyn SocialScraperAdapter; 2] {
    [&INSTAGRAM_SCRAPER_ADAPTER, &TIKTOK_SCRAPER_ADAPTER]
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
    #[serde(default)]
    pub segment: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SalesApprovalQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SalesSegmentQuery {
    #[serde(default)]
    pub segment: Option<String>,
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
