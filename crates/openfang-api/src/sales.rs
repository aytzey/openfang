//! Sales engine API and persistence.
//!
//! Focused prospecting workflow:
//! 1. Persist ICP/product profile
//! 2. Discover candidate customer accounts from public sources
//! 3. Build persistent prospect dossiers with deterministic memory reuse
//! 4. Upgrade the best dossiers into outreach-ready leads + approval drafts
//! 5. Send on manual approval (email + LinkedIn browser automation)

use crate::codex_oauth::StoredCodexAuth;
use crate::routes::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{Local, Timelike, Utc};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use lettre::message::{Mailbox, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use openfang_runtime::browser::BrowserCommand;
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
const SALES_RUN_REQUEST_TIMEOUT_SECS: u64 = 240;
const SALES_RUN_RECOVERY_STALE_SECS: i64 = SALES_RUN_REQUEST_TIMEOUT_SECS as i64 + 15;
const SALES_LLM_PROVIDER: &str = "openai-codex";
const SALES_LLM_MODEL: &str = "gpt-5.3-codex";

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
            target_geo: "US".to_string(),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default)]
struct DomainCandidate {
    domain: String,
    score: i32,
    evidence: Vec<String>,
    matched_keywords: Vec<String>,
    source_links: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct SourceContactHint {
    contact_name: Option<String>,
    contact_title: Option<String>,
    email: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct FreeDiscoveryCandidate {
    candidate: DomainCandidate,
    contact_hint: SourceContactHint,
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

            CREATE INDEX IF NOT EXISTS idx_approvals_status_created ON approvals(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_deliveries_sent ON deliveries(sent_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_run_updated ON prospect_profiles(run_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_updated ON prospect_profiles(updated_at DESC);
        "#,
        )
        .map_err(|e| format!("Failed to initialize sales db: {e}"))?;
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
            let payload = serde_json::json!({
                "to": email,
                "subject": lead.email_subject,
                "body": lead.email_body,
            });
            conn.execute(
                "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
                params![uuid::Uuid::new_v4().to_string(), lead.id, payload.to_string(), created_at],
            )
            .map_err(|e| format!("Queue email approval failed: {e}"))?;
            queued += 1;
        }

        if let Some(linkedin_url) = lead
            .linkedin_url
            .as_deref()
            .and_then(normalize_outreach_linkedin_url)
        {
            let payload = serde_json::json!({
                "profile_url": linkedin_url,
                "message": lead.linkedin_message,
            });
            conn.execute(
                "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'linkedin', ?, 'pending', ?)",
                params![uuid::Uuid::new_v4().to_string(), lead.id, payload.to_string(), created_at],
            )
            .map_err(|e| format!("Queue LinkedIn approval failed: {e}"))?;
            queued += 1;
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
                if let Ok(profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
                    out.push(profile);
                }
            }
        } else {
            let rows = stmt
                .query_map(params![limit as i64], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Query prospect_profiles failed: {e}"))?;
            for row in rows {
                let raw = row.map_err(|e| format!("Read prospect_profiles row failed: {e}"))?;
                if let Ok(profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
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
            let payload = serde_json::to_string(profile)
                .map_err(|e| format!("Serialize prospect profile failed: {e}"))?;
            tx.execute(
                "INSERT INTO prospect_profiles (company_domain, run_id, json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(company_domain) DO UPDATE SET
                    run_id = excluded.run_id,
                    json = excluded.json,
                    updated_at = excluded.updated_at",
                params![
                    profile.company_domain,
                    profile.run_id,
                    payload,
                    profile.created_at,
                    profile.updated_at
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
            let payload_raw: String = r.get(3).unwrap_or_else(|_| "{}".to_string());
            let payload = serde_json::from_str::<serde_json::Value>(&payload_raw)
                .unwrap_or_else(|_| serde_json::json!({}));
            out.push(SalesApproval {
                id: r.get(0).unwrap_or_default(),
                lead_id: r.get(1).unwrap_or_default(),
                channel: r.get(2).unwrap_or_default(),
                payload,
                status: r.get(4).unwrap_or_default(),
                created_at: r.get(5).unwrap_or_default(),
                decided_at: r.get(6).ok(),
            });
        }

        Ok(out)
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

    async fn send_email(
        &self,
        state: &AppState,
        to: &str,
        subject: &str,
        body: &str,
    ) -> Result<(), String> {
        let channels = state.channels_config.read().await;
        let cfg = channels
            .email
            .as_ref()
            .ok_or_else(|| "Email channel is not configured".to_string())?;
        let password = std::env::var(&cfg.password_env)
            .map_err(|_| format!("Email password env '{}' is not set", cfg.password_env))?;

        let from: Mailbox = cfg
            .username
            .parse()
            .map_err(|e| format!("Invalid sender email '{}': {e}", cfg.username))?;
        let to: Mailbox = to
            .parse()
            .map_err(|e| format!("Invalid recipient email '{to}': {e}"))?;

        let msg = Message::builder()
            .from(from)
            .to(to)
            .subject(subject)
            .body(body.to_string())
            .map_err(|e| format!("Failed to build email message: {e}"))?;

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.smtp_host)
            .map_err(|e| format!("Failed to initialize SMTP relay '{}': {e}", cfg.smtp_host))?
            .port(cfg.smtp_port)
            .credentials(Credentials::new(cfg.username.clone(), password))
            .build();

        transport
            .send(msg)
            .await
            .map_err(|e| format!("SMTP send failed: {e}"))?;

        Ok(())
    }

    async fn send_linkedin(
        &self,
        state: &AppState,
        profile_url: &str,
        message: &str,
    ) -> Result<(), String> {
        let agent_id = "sales_linkedin";
        state
            .kernel
            .browser_ctx
            .send_command(
                agent_id,
                BrowserCommand::Navigate {
                    url: profile_url.to_string(),
                },
            )
            .await
            .map_err(|e| format!("LinkedIn navigate failed: {e}"))?;

        // Playwright bridge click() supports text fallback if selector click fails.
        state
            .kernel
            .browser_ctx
            .send_command(
                agent_id,
                BrowserCommand::Click {
                    selector: "Message".to_string(),
                },
            )
            .await
            .map_err(|e| format!("LinkedIn 'Message' click failed: {e}"))?;

        state
            .kernel
            .browser_ctx
            .send_command(
                agent_id,
                BrowserCommand::Type {
                    selector: "div.msg-form__contenteditable[contenteditable='true']".to_string(),
                    text: message.to_string(),
                },
            )
            .await
            .map_err(|e| format!("LinkedIn message typing failed: {e}"))?;

        state
            .kernel
            .browser_ctx
            .send_command(
                agent_id,
                BrowserCommand::Click {
                    selector: "button.msg-form__send-button".to_string(),
                },
            )
            .await
            .map_err(|e| format!("LinkedIn send click failed: {e}"))?;

        Ok(())
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
        conn.execute(
            "INSERT INTO deliveries (id, approval_id, channel, recipient, status, error, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                uuid::Uuid::new_v4().to_string(),
                approval_id,
                channel,
                recipient,
                status,
                error_msg,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| format!("Failed to record delivery: {e}"))?;
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

    pub async fn approve_and_send(
        &self,
        state: &AppState,
        approval_id: &str,
    ) -> Result<serde_json::Value, String> {
        let conn = self.open()?;
        let row = conn
            .query_row(
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
            .map_err(|e| format!("Approval lookup failed: {e}"))?;

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
                if let Err(send_err) = self.send_email(state, to, subject, body).await {
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
            "linkedin" => {
                let profile_url = payload
                    .get("profile_url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.profile_url".to_string())?;
                let message = payload
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.message".to_string())?;
                if let Err(send_err) = self.send_linkedin(state, profile_url, message).await {
                    if let Err(record_err) = self.record_delivery(
                        &id,
                        "linkedin",
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
                if let Err(record_err) =
                    self.record_delivery(&id, "linkedin", profile_url, "sent", None)
                {
                    warn!(
                        approval_id = %id,
                        error = %record_err,
                        "Failed to record LinkedIn delivery after successful send"
                    );
                }
                serde_json::json!({"channel": "linkedin", "recipient": profile_url, "status": "sent"})
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

        let run_sequence = self.completed_runs_count()? as usize;
        let run_id = self.begin_run()?;
        let started_at = Utc::now().to_rfc3339();

        let max_candidates = (profile.daily_target as usize).saturating_mul(3).max(40);
        let is_field_ops = profile_targets_field_ops(&profile);
        let skip_llm_discovery = is_field_ops && geo_is_turkey(&profile.target_geo);

        // --- STAGE 1: Query Plan (LLM or heuristic fallback) ---
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
        let previously_discovered = self.previously_discovered_domains(200).unwrap_or_default();
        let llm_target = ((profile.daily_target as usize).saturating_add(4))
            .clamp(6, MAX_LLM_PRIMARY_CANDIDATES);

        let (
            llm_candidates,
            (web_search_candidates, mut source_contact_hints, search_unavailable),
            free_candidates,
        ) = tokio::join!(
            // PRIMARY: LLM company generation
            async {
                if skip_llm_discovery {
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
            discover_via_web_search(
                &search_engine,
                &brave_search_engine,
                &lead_plan,
                &profile,
                max_candidates,
                is_field_ops,
            ),
            // SUPPLEMENTAL: Turkish directory scraping
            fetch_free_discovery_candidates(&profile, run_sequence),
        );

        // --- STAGE 3: Merge all discovery sources ---
        let mut candidate_list = merge_all_discovery_sources(
            llm_candidates,
            web_search_candidates,
            free_candidates,
            &mut source_contact_hints,
        );

        // --- STAGE 4: LLM Relevance Validation ---
        let mut llm_validated_domains = HashSet::<String>::new();
        let validation_count = candidate_list.len().min(
            (profile.daily_target as usize)
                .saturating_mul(2)
                .clamp(8, LLM_RELEVANCE_VALIDATION_BATCH_SIZE),
        );
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

        // --- STAGE 5: Filter and Sort ---
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
            return Err(err_msg);
        }

        let prospect_seed_limit = (profile.daily_target as usize)
            .saturating_mul(5)
            .clamp(30, 160);
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
        let max_direct_enrich_attempts =
            (profile.daily_target as usize).clamp(MAX_DIRECT_ENRICH_ATTEMPTS, 16);
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
        let max_web_contact_search_attempts =
            (profile.daily_target as usize).clamp(4, MAX_WEB_CONTACT_SEARCH_ATTEMPTS);
        let mut web_contact_search_attempts = 0usize;
        let mut prospect_profile_updates = HashMap::<String, SalesProspectProfile>::new();

        for candidate in candidate_list.iter().take(max_candidates) {
            if inserted >= profile.daily_target {
                break;
            }

            discovered += 1;
            if candidate.score < min_candidate_score {
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
                let default_title = if profile.target_title_policy == "ceo_only" {
                    Some("CEO".to_string())
                } else {
                    Some("CEO/Founder".to_string())
                };
                // Use info@domain as fallback email so the lead has an outreach channel
                let fallback_email = Some(format!("info@{domain}"));
                (
                    seeded_name,
                    seeded_title.or(default_title),
                    seeded_linkedin,
                    seeded_email.or(fallback_email),
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
                    let company_linkedin_query = format!(
                        "site:linkedin.com/company \"{}\" \"{}\"",
                        company_search_name, domain
                    );
                    if let Ok(company_linkedin_res) = run_sales_search(
                        &search_engine,
                        &company_linkedin_query,
                        6,
                        Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS),
                    )
                    .await
                    {
                        if !company_linkedin_res.trim().is_empty() {
                            search_outputs.push(company_linkedin_res.clone());
                        }
                        search_osint_enrichment.company_linkedin_url =
                            extract_company_linkedin_from_text(&company_linkedin_res)
                                .and_then(|value| normalize_company_linkedin_url(&value));
                    }
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
            if is_llm_validated || is_verified_by_memory {
                if email.is_none() && linkedin_url.is_none() {
                    email = Some(format!("info@{domain}"));
                }
                if contact_name.is_none() || contact_name_is_placeholder(contact_name.as_deref()) {
                    contact_title = Some(if profile.target_title_policy == "ceo_only" {
                        "CEO".to_string()
                    } else {
                        "CEO/Founder".to_string()
                    });
                }
            }

            if !lead_has_outreach_channel(email.as_ref(), linkedin_url.as_ref()) {
                continue;
            }
            // Search-time LLM validation or cached dossier memory can proceed without a real person name.
            if !(is_llm_validated || is_verified_by_memory)
                && !lead_has_person_identity(contact_name.as_deref(), linkedin_url.as_ref())
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

            let reasons = vec![
                format!(
                    "{} matched ICP keywords: {}",
                    company, matched
                ),
                format!(
                    "Observed public signal: {}",
                    truncate_text_for_reason(&evidence, 220)
                ),
                format!(
                    "{} is a decision-maker role that typically owns operations/process adoption priorities.",
                    contact_title
                        .clone()
                        .unwrap_or_else(|| "Leadership".to_string())
                ),
                format!(
                    "{} helps teams with: {}",
                    profile.product_name,
                    truncate_text_for_reason(&profile.product_description, 220)
                ),
            ];

            let recipient_name = contact_name.clone().unwrap_or_else(|| "there".to_string());
            let email_subject = format!(
                "{} for {} operations coordination",
                profile.product_name, company
            );
            let email_body = format!(
                "Hi {},\n\nI came across {} and noticed this signal: {}.\n\n{} could likely help your team by {}.\n\nIf helpful, I can share a short plan specifically for your operation model in {}.\n\nBest,\n{}",
                recipient_name,
                company,
                truncate_text_for_reason(&evidence, 180),
                profile.product_name,
                truncate_text_for_reason(&profile.product_description, 220),
                profile.target_industry,
                profile.sender_name
            );
            let linkedin_message = format!(
                "Hi {}, saw {} and a signal around {}. {} could be relevant for your {} workflows. Open to a quick exchange?",
                recipient_name,
                company,
                truncate_text_for_reason(&matched, 80),
                profile.product_name,
                profile.target_industry
            );

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
                phone: None,
                reasons,
                email_subject,
                email_body,
                linkedin_message,
                score,
                status: "draft_ready".to_string(),
                created_at: Utc::now().to_rfc3339(),
            };

            match self.insert_lead(&lead) {
                Ok(true) => {
                    inserted += 1;
                    let _ = self.record_discovered_domain(domain, &run_id);
                    match self.queue_approvals_for_lead(&lead) {
                        Ok(q) => approvals_queued += q,
                        Err(e) => {
                            warn!(lead_id = %lead.id, error = %e, "Failed to queue lead approvals")
                        }
                    }
                }
                Ok(false) => {
                    // duplicate, skip silently
                }
                Err(e) => warn!(domain = %domain, error = %e, "Lead insert failed"),
            }
        }

        if !prospect_profile_updates.is_empty() {
            let updates = prospect_profile_updates.into_values().collect::<Vec<_>>();
            if let Err(e) = self.upsert_prospect_profiles(&updates) {
                warn!(run_id = %run_id, error = %e, "Failed to persist OSINT-enriched prospect dossiers during run");
            }
        }

        if inserted == 0 && seeded_prospect_profiles.is_empty() {
            let err_msg = format!(
                "Prospecting run completed discovery, but no durable prospect dossiers or actionable contacts could be saved for the current ICP/geo."
            );
            self.finish_run(
                &run_id,
                "failed",
                discovered,
                inserted,
                approvals_queued,
                Some(&err_msg),
            )?;
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
        let scan_limit = (sales_profile.daily_target as usize)
            .saturating_mul(6)
            .clamp(60, 400);
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

        let osint_target_limit = (sales_profile.daily_target as usize)
            .saturating_mul(2)
            .clamp(12, MAX_OSINT_SEARCH_TARGETS);
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

        let osint_target_limit = (sales_profile.daily_target as usize)
            .saturating_mul(2)
            .clamp(12, MAX_OSINT_SEARCH_TARGETS);
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
            acc.primary_email =
                clean_profile_contact_field(lead.email.as_deref().unwrap_or_default());
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
                created_at: acc.created_at,
                updated_at: acc.updated_at,
            }
        })
        .collect();

    sort_prospect_profiles_for_harness(&mut profiles, sales_profile);
    profiles.truncate(limit);
    profiles
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
    if profile.primary_email.is_some() {
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
        .map(|v| !v.trim().is_empty())
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
        .map(|value| !value.trim().is_empty())
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
        .map(|value| !value.trim().is_empty())
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
    if has_blocked_asset_tld(&host) {
        return None;
    }
    Some(host)
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

    domain.starts_with("blog.")
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
        if is_blocked_company_domain(&result_domain) {
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
            } else if !entry.title.trim().is_empty() {
                if candidate.evidence.len() < 4 {
                    candidate
                        .evidence
                        .push(truncate_text_for_reason(&entry.title, 220));
                }
            }
            candidate.matched_keywords.extend(matched);
            candidate.matched_keywords = dedupe_strings(candidate.matched_keywords.clone());
        }

        for referenced_domain in referenced_domains {
            if referenced_domain == result_domain || is_blocked_company_domain(&referenced_domain) {
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
        let key = item.domain.to_lowercase();
        let entry = map.entry(key).or_default();
        if entry.domain.is_empty() {
            entry.domain = item.domain.clone();
        }
        entry.score = entry.score.max(item.score);
        entry.evidence.extend(item.evidence);
        if entry.evidence.len() > 6 {
            entry.evidence.truncate(6);
        }
        entry.matched_keywords.extend(item.matched_keywords);
        entry.matched_keywords = dedupe_strings(entry.matched_keywords.clone());
        entry.source_links = merge_osint_links(entry.source_links.clone(), item.source_links);
    }
    map.into_values().collect()
}

fn merge_free_discovery_candidate(
    candidates: &mut HashMap<String, DomainCandidate>,
    source_contact_hints: &mut HashMap<String, SourceContactHint>,
    free_candidate: FreeDiscoveryCandidate,
) {
    let directory_score = free_candidate.candidate.score
        + free_discovery_priority_boost(&free_candidate.contact_hint);
    let domain = free_candidate.candidate.domain.clone();
    if domain.is_empty() || is_blocked_company_domain(&domain) {
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
    let Some(name) = name else {
        return false;
    };
    let normalized = decode_basic_html_entities(name)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase();
    if normalized.is_empty() {
        return false;
    }

    let placeholder_terms = [
        "unknown",
        "leadership",
        "leadership team",
        "management",
        "management team",
        "executive team",
        "executive committee",
        "board of directors",
        "ust yonetim",
        "üst yönetim",
        "yonetim ekibi",
        "yönetim ekibi",
        "yonetim takimi",
        "yönetim takımı",
        "yonetim kurulu",
        "yönetim kurulu",
        "icra kurulu",
        "i̇cra kurulu",
    ];

    placeholder_terms.iter().any(|term| normalized == *term)
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

    let (tmb, eud, asmud, platformder, mib, imder, isder, thbb) = tokio::join!(
        fetch_tmb,
        fetch_eud,
        fetch_asmud,
        fetch_platformder,
        fetch_mib,
        fetch_imder,
        fetch_isder,
        fetch_thbb
    );
    interleave_free_discovery_sources(
        vec![tmb, eud, asmud, platformder, mib, imder, isder, thbb],
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
            normalize_contact_email_for_domain(extract_email_from_text(article_html), &domain);

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
            .and_then(|value| normalize_site_contact_email(Some(value)));

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
            .and_then(|value| normalize_contact_email_for_domain(Some(value), &domain));

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
                if token_count >= 2 && token_count <= 4 && alpha_only {
                    Some(rewritten)
                } else {
                    None
                }
            })
        })
        .or_else(|| raw_name.as_deref().and_then(normalize_person_name));
    let contact_title = raw_title.as_deref().map(normalize_contact_title);
    let email = normalize_contact_email_for_domain(extract_email_from_text(html), &domain);

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
    let email = normalize_contact_email_for_domain(extract_email_from_text(html), &domain);

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

        let email = normalize_contact_email_for_domain(extract_email_from_text(td_html), &domain);
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
                let candidate = source[positions[idx]].clone();
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
    if encoded.len() < 4 || encoded.len() % 2 != 0 {
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
        let Some((local, domain)) = trimmed.rsplit_once('@').map(|(l, d)| (l.trim(), d.trim()))
        else {
            return None;
        };
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
            || blocked_tlds.contains(&tld)
        {
            return None;
        }
        Some(trimmed)
    })
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
    let domain = email
        .rsplit_once('@')
        .map(|(_, d)| d.trim().to_lowercase())
        .unwrap_or_default();
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
        if !email_matches_company_domain(&trimmed, company_domain) {
            return None;
        }
        Some(trimmed)
    })
}

fn normalize_site_contact_email(email: Option<String>) -> Option<String> {
    normalize_email_candidate(email)
}

fn normalize_outreach_linkedin_url(raw: &str) -> Option<String> {
    extract_personal_linkedin_from_text(raw)
}

fn normalize_company_linkedin_url(raw: &str) -> Option<String> {
    extract_company_linkedin_from_text(raw)
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

    let decoded_path = decode_percent_utf8_lossy(parsed.path());
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
    email.is_some() || linkedin_url.is_some()
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
        for m in re.find_iter(source) {
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

    for page in bundle.pages {
        osint_links.push(page.url.clone());
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

fn extract_contact_from_company_site_html(
    html: &str,
    title_policy: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
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
    let name = contact_name?;
    if contact_name_is_placeholder(Some(name)) {
        return None;
    }
    let normalized = normalize_person_name(name)?;
    let parts: Vec<&str> = normalized
        .split_whitespace()
        .filter(|p| p.chars().all(|c| c.is_ascii_alphabetic()))
        .collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }
    let first = parts[0].to_lowercase();
    let last = parts[parts.len() - 1].to_lowercase();
    Some(format!("{}.{}@{}", first, last, domain))
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

#[derive(Debug, Default, Deserialize)]
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
        ("lojistik", "Logistics"),
        ("logistics", "Logistics"),
        ("telekom", "Telecommunications"),
        ("telecom", "Telecommunications"),
        ("cyber", "Cybersecurity"),
        ("security", "Cybersecurity"),
        ("fintech", "Fintech"),
        ("bank", "Financial Services"),
        ("e-commerce", "E-commerce"),
        ("ecommerce", "E-commerce"),
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
        p.target_geo = detect_geo(brief).unwrap_or_else(|| "US".to_string());
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
        target_geo: detect_geo(brief).or_else(|| Some("US".to_string())),
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

    let mut candidate_list: Vec<DomainCandidate> = candidates.into_values().collect();
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
            candidate_list.push(DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE,
                evidence: vec![format!(
                    "Discovered via fallback query for {}",
                    profile.target_industry
                )],
                matched_keywords: vec![profile.target_industry.clone()],
                source_links: Vec::new(),
            });
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

    (candidate_list, source_contact_hints, search_unavailable)
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

    merged.into_values().collect()
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
    stream::iter(queries.iter().cloned().map(|query| async move {
        let result = run_sales_search(search_engine, &query, max_results, timeout).await;
        (query, result)
    }))
    .buffer_unordered(SALES_SEARCH_BATCH_CONCURRENCY)
    .collect()
    .await
}

fn adaptive_discovery_retry_threshold(profile: &SalesProfile, max_candidates: usize) -> usize {
    ((profile.daily_target as usize).saturating_add(1) / 2).clamp(6, max_candidates.clamp(6, 12))
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
        if is_blocked_company_domain(&domain) || !seen.insert(domain.clone()) {
            continue;
        }
        let reason = c
            .reason
            .unwrap_or_else(|| format!("LLM suggested for {}", profile.target_industry));
        let mut matched = vec![profile.target_industry.clone()];
        if let Some(company) = c.company {
            matched.push(company);
        }
        out.push(DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
            evidence: vec![truncate_text_for_reason(&reason, 220)],
            matched_keywords: dedupe_strings(matched),
            source_links: Vec::new(),
        });
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
         We sell to companies with field/on-site operations (construction, maintenance, facility management, etc.)\n\n\
         Companies to evaluate:\n{}\n\n\
         For each company, assess:\n\
         - Is it a real company in our target industry with field operations teams?\n\
         - Would they benefit from our product?\n\n\
         Return strict JSON only:\n\
         {{\"results\":[{{\"domain\":\"...\",\"relevant\":true/false,\"confidence\":0.0-1.0,\"reason\":\"...\"}}]}}",
        profile.product_name,
        profile.product_description,
        profile.target_industry,
        profile.target_geo,
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

    match tokio::time::timeout(
        Duration::from_secs(SALES_RUN_REQUEST_TIMEOUT_SECS),
        engine.run_generation(&state.kernel),
    )
    .await
    {
        Ok(Ok(run)) => (StatusCode::OK, Json(serde_json::json!({"run": run}))),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
        Err(_) => match engine.recover_latest_timed_out_run() {
            Ok(Some(run)) if run.status == "completed" => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "run": run,
                    "warning": "Prospecting run exceeded the request timeout, but saved progress was recovered."
                })),
            ),
            Ok(Some(_run)) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Prospecting run timed out while profiling candidate accounts. No durable marketing output could be recovered."
                })),
            ),
            Ok(None) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Prospecting run timed out while profiling candidate accounts. No durable marketing output could be recovered."
                })),
            ),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Prospecting run timed out and recovery failed: {e}")
                })),
            ),
        },
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
            },
            DomainCandidate {
                domain: "ornekbakim.com.tr".to_string(),
                score: 39,
                evidence: vec!["Maintenance dispatch teams".to_string()],
                matched_keywords: vec!["field service".to_string(), "maintenance".to_string()],
                source_links: vec!["https://www.asmud.org.tr/Uyeler.asp".to_string()],
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
        assert_eq!(email.as_deref(), Some("info@acmeinsaat.com.tr"));
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
        assert_eq!(email.as_deref(), Some("info@acmeinsaat.com.tr"));
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
        assert!(!lead_has_outreach_channel(None, None));
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
}
