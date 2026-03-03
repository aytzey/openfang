//! Sales engine API and persistence.
//!
//! Focused outbound workflow:
//! 1. Persist ICP/product profile
//! 2. Generate daily leads from public web search
//! 3. Build value hypotheses + outreach drafts
//! 4. Queue per-message approvals
//! 5. Send on manual approval (email + LinkedIn browser automation)

use crate::codex_oauth::StoredCodexAuth;
use crate::routes::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{Local, Timelike, Utc};
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
const MIN_DOMAIN_RELEVANCE_SCORE: i32 = 10;
const MAX_DISCOVERY_QUERIES: usize = 6;
const MAX_DISCOVERY_FAILURES_BEFORE_FAST_FALLBACK: u32 = MAX_DISCOVERY_QUERIES as u32;
const NO_BRAVE_FAIL_FAST_THRESHOLD: u32 = 1;
const MAX_DIRECT_ENRICH_ATTEMPTS: usize = 8;
const DIRECT_ENRICH_TIMEOUT_MS: u64 = 3500;
const MAX_EXTRA_SITE_ENRICH_PAGES: usize = 5;
const LEAD_QUERY_PLAN_TIMEOUT_SECS: u64 = 14;
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
}

#[derive(Debug, Clone, Default)]
struct OnboardingBriefState {
    brief: Option<String>,
    updated_at: Option<String>,
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

            CREATE INDEX IF NOT EXISTS idx_approvals_status_created ON approvals(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_deliveries_sent ON deliveries(sent_at DESC);
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
        let json = serde_json::to_string(profile).map_err(|e| format!("Serialize failed: {e}"))?;
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
                "SELECT id FROM sales_runs WHERE status = 'completed' AND inserted > 0 AND started_at >= ? ORDER BY completed_at DESC LIMIT 1",
                true,
            )
        } else {
            (
                "SELECT id FROM sales_runs WHERE status = 'completed' AND inserted > 0 ORDER BY completed_at DESC LIMIT 1",
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

        if let Some(linkedin_url) = &lead.linkedin_url {
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
            out.push(SalesRunRecord {
                id: r.get(0).unwrap_or_default(),
                status: r.get(1).unwrap_or_default(),
                started_at: r.get(2).unwrap_or_default(),
                completed_at: r.get(3).ok(),
                discovered: r.get::<_, i64>(4).unwrap_or(0) as u32,
                inserted: r.get::<_, i64>(5).unwrap_or(0) as u32,
                approvals_queued: r.get::<_, i64>(6).unwrap_or(0) as u32,
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

    fn deliveries_today(&self) -> Result<u32, String> {
        let conn = self.open()?;
        let today = Local::now().format("%Y-%m-%d").to_string();
        let count = conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'sent' AND substr(sent_at, 1, 10) = ?",
                params![today],
                |r| r.get::<_, i64>(0),
            )
            .map_err(|e| format!("Deliveries count failed: {e}"))?;
        Ok(count as u32)
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

        let sent_today = self.deliveries_today()?;
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
                self.send_email(state, to, subject, body).await?;
                self.record_delivery(&id, "email", to, "sent", None)?;
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
                self.send_linkedin(state, profile_url, message).await?;
                self.record_delivery(&id, "linkedin", profile_url, "sent", None)?;
                serde_json::json!({"channel": "linkedin", "recipient": profile_url, "status": "sent"})
            }
            other => return Err(format!("Unsupported channel: {other}")),
        };

        self.update_approval_status(&id, "approved")?;
        Ok(result)
    }

    pub fn reject_approval(&self, approval_id: &str) -> Result<(), String> {
        self.update_approval_status(approval_id, "rejected")
    }

    pub fn already_ran_today(&self) -> Result<bool, String> {
        let conn = self.open()?;
        let today = Local::now().format("%Y-%m-%d").to_string();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sales_runs WHERE status = 'completed' AND substr(started_at, 1, 10) = ?",
                params![today],
                |r| r.get(0),
            )
            .map_err(|e| format!("Run-day check failed: {e}"))?;
        Ok(count > 0)
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
        let mut llm_fallback_attempted = false;

        let max_candidates = (profile.daily_target as usize).saturating_mul(4).max(30);
        let lead_plan = match tokio::time::timeout(
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
        };
        let queries = if lead_plan.discovery_queries.is_empty() {
            heuristic_lead_query_plan(&profile).discovery_queries
        } else {
            lead_plan.discovery_queries.clone()
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
        let discovery_fail_fast_threshold = if brave_search_engine.is_some() {
            MAX_DISCOVERY_FAILURES_BEFORE_FAST_FALLBACK
        } else {
            NO_BRAVE_FAIL_FAST_THRESHOLD
        };
        let is_field_ops = profile_targets_field_ops(&profile);
        let strict_min_score = if is_field_ops {
            MIN_DOMAIN_RELEVANCE_SCORE + 8
        } else {
            MIN_DOMAIN_RELEVANCE_SCORE + 4
        };

        let mut domains = Vec::new();
        let mut candidates: HashMap<String, DomainCandidate> = HashMap::new();
        let mut discovery_successes = 0u32;
        let mut discovery_failures = 0u32;
        for q in queries.iter().take(MAX_DISCOVERY_QUERIES) {
            match search_engine.search(q, max_candidates).await {
                Ok(out) => {
                    discovery_successes += 1;
                    collect_domains_from_search(&out, &mut domains);
                    collect_domain_candidates_from_search(
                        &out,
                        &mut candidates,
                        &lead_plan.must_include_keywords,
                        &lead_plan.exclude_keywords,
                    );
                }
                Err(e) => {
                    discovery_failures += 1;
                    warn!(query = %q, error = %e, "Sales search query failed");
                    if discovery_successes == 0
                        && discovery_failures >= discovery_fail_fast_threshold
                    {
                        break;
                    }
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

        if candidate_list.is_empty() {
            if !search_unavailable {
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
                for q in fallback_queries {
                    match search_engine.search(&q, 20).await {
                        Ok(out) => {
                            discovery_successes += 1;
                            collect_domains_from_search(&out, &mut fallback_domains)
                        }
                        Err(e) => {
                            discovery_failures += 1;
                            warn!(query = %q, error = %e, "Fallback sales query failed");
                            if discovery_successes == 0
                                && discovery_failures >= discovery_fail_fast_threshold
                            {
                                break;
                            }
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
                    });
                }
            }

            if candidate_list.is_empty() && search_unavailable {
                if let Some(brave_engine) = brave_search_engine.as_ref() {
                    let mut brave_domains = Vec::<String>::new();
                    let mut brave_candidates = HashMap::<String, DomainCandidate>::new();
                    let mut brave_successes = 0u32;

                    for q in queries.iter().take(MAX_DISCOVERY_QUERIES) {
                        match brave_engine.search(q, max_candidates).await {
                            Ok(out) => {
                                brave_successes += 1;
                                collect_domains_from_search(&out, &mut brave_domains);
                                collect_domain_candidates_from_search(
                                    &out,
                                    &mut brave_candidates,
                                    &lead_plan.must_include_keywords,
                                    &lead_plan.exclude_keywords,
                                );
                            }
                            Err(e) => {
                                warn!(query = %q, error = %e, "Brave rescue query failed");
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
                        discovery_successes += brave_successes;
                        search_unavailable = false;
                        info!("Primary web discovery failed; recovered via Brave rescue search");
                    }
                }
            }

            if candidate_list.is_empty() && !search_unavailable {
                llm_fallback_attempted = true;
                match tokio::time::timeout(
                    Duration::from_secs(4),
                    llm_generate_company_candidates(
                        kernel,
                        &profile,
                        profile.daily_target as usize,
                    ),
                )
                .await
                {
                    Ok(Ok(mut llm_candidates)) => candidate_list.append(&mut llm_candidates),
                    Ok(Err(e)) => warn!(error = %e, "LLM company fallback generation failed"),
                    Err(_) => warn!("LLM company fallback generation timed out"),
                }
            }

            if candidate_list.is_empty() {
                let mut matched_keywords = vec![profile.target_industry.clone()];
                if is_field_ops {
                    matched_keywords.push("on-site teams".to_string());
                    matched_keywords.push("field service".to_string());
                }
                let fallback_reason = if search_unavailable {
                    format!(
                        "Deterministic fallback while web search is unavailable for {}",
                        profile.target_industry
                    )
                } else {
                    format!(
                        "Deterministic fallback after filtering low-signal web results for {}",
                        profile.target_industry
                    )
                };
                for domain in deterministic_seed_domains_for_profile(&profile, run_sequence) {
                    candidate_list.push(DomainCandidate {
                        domain: domain.clone(),
                        score: strict_min_score,
                        evidence: vec![fallback_reason.clone()],
                        matched_keywords: dedupe_strings(matched_keywords.clone()),
                    });
                }
            }
        }

        let candidate_pool = candidate_list.clone();
        candidate_list.retain(|c| {
            c.score >= strict_min_score && (!is_field_ops || candidate_has_field_ops_signal(c))
        });

        if candidate_list.is_empty() {
            candidate_list = candidate_pool
                .into_iter()
                .filter(|c| {
                    c.score >= MIN_DOMAIN_RELEVANCE_SCORE
                        && (!is_field_ops || candidate_has_relaxed_field_ops_signal(c))
                })
                .collect();
        }

        if candidate_list.is_empty() {
            let mut matched_keywords = vec![profile.target_industry.clone()];
            if is_field_ops {
                matched_keywords.push("on-site teams".to_string());
                matched_keywords.push("field service".to_string());
            }
            for domain in deterministic_seed_domains_for_profile(&profile, run_sequence) {
                candidate_list.push(DomainCandidate {
                    domain: domain.clone(),
                    score: strict_min_score,
                    evidence: vec![format!(
                        "Deterministic fallback after post-filter candidate exhaustion for {}",
                        profile.target_industry
                    )],
                    matched_keywords: dedupe_strings(matched_keywords.clone()),
                });
            }
        }

        if !llm_fallback_attempted
            && discovery_successes == 0
            && candidate_list.len() < (profile.daily_target as usize / 2).max(5)
        {
            match tokio::time::timeout(
                Duration::from_secs(4),
                llm_generate_company_candidates(
                    kernel,
                    &profile,
                    (profile.daily_target as usize).max(12),
                ),
            )
            .await
            {
                Ok(Ok(llm_candidates)) => {
                    let mut seen = candidate_list
                        .iter()
                        .map(|c| c.domain.clone())
                        .collect::<HashSet<_>>();
                    for c in llm_candidates {
                        if !seen.insert(c.domain.clone()) {
                            continue;
                        }
                        if c.score >= strict_min_score
                            && (!is_field_ops || candidate_has_field_ops_signal(&c))
                        {
                            candidate_list.push(c);
                        }
                    }
                }
                Ok(Err(e)) => warn!(error = %e, "LLM company augmentation failed"),
                Err(_) => warn!("LLM company augmentation timed out"),
            }
        }

        candidate_list.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.domain.cmp(&b.domain)));

        if candidate_list.len() < max_candidates {
            let mut seen = candidate_list
                .iter()
                .map(|c| c.domain.clone())
                .collect::<HashSet<_>>();
            let mut matched_keywords = vec![profile.target_industry.clone()];
            if is_field_ops {
                matched_keywords.push("on-site teams".to_string());
                matched_keywords.push("field service".to_string());
            }
            for domain in deterministic_seed_domains_for_profile(&profile, run_sequence) {
                if !seen.insert(domain.clone()) {
                    continue;
                }
                candidate_list.push(DomainCandidate {
                    domain: domain.clone(),
                    score: strict_min_score,
                    evidence: vec![format!(
                        "Deterministic supplementation to keep lead pipeline full for {}",
                        profile.target_industry
                    )],
                    matched_keywords: dedupe_strings(matched_keywords.clone()),
                });
                if candidate_list.len() >= max_candidates {
                    break;
                }
            }
        }

        if candidate_list.is_empty() && discovery_successes == 0 {
            let err_msg = format!(
                "Lead discovery failed: all web discovery queries failed ({} errors). Check network/search provider and retry.",
                discovery_failures
            );
            self.finish_run(&run_id, "failed", 0, 0, 0, Some(&err_msg))?;
            return Err(err_msg);
        }

        let mut discovered = 0u32;
        let mut inserted = 0u32;
        let mut approvals_queued = 0u32;
        let site_client = reqwest::Client::builder()
            .user_agent("OpenFangSalesBot/1.0 (+https://openfang.ai)")
            .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
            .ok();
        let max_direct_enrich_attempts =
            (profile.daily_target as usize).clamp(MAX_DIRECT_ENRICH_ATTEMPTS, 40);
        let mut direct_enrich_attempts = 0usize;

        for candidate in candidate_list.iter().take(max_candidates) {
            if inserted >= profile.daily_target {
                break;
            }

            discovered += 1;
            if candidate.score < MIN_DOMAIN_RELEVANCE_SCORE {
                continue;
            }

            let domain = &candidate.domain;
            let company = domain_to_company(domain);

            let (mut contact_name, mut contact_title, mut linkedin_url, mut email) =
                if search_unavailable {
                    let default_title = if profile.target_title_policy == "ceo_only" {
                        Some("CEO".to_string())
                    } else {
                        Some("CEO/Founder".to_string())
                    };
                    (None, default_title, None, None)
                } else {
                    let primary_contact_query = if profile.target_title_policy == "ceo_only" {
                        format!(
                            "site:linkedin.com/in \"{}\" (CEO OR \"Chief Executive Officer\")",
                            company
                        )
                    } else {
                        format!(
                            "site:linkedin.com/in \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\")",
                            company
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
                            company, domain
                        )
                    } else {
                        format!(
                            "\"{}\" \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR leadership OR executive team)",
                            company, domain
                        )
                    };

                    let contact_queries = dedupe_strings(vec![
                        primary_contact_query,
                        domain_contact_query,
                        secondary_contact_query,
                    ]);

                    let mut contact_outputs = Vec::<String>::new();
                    for query in contact_queries {
                        if let Ok(out) = search_engine.search(&query, 6).await {
                            if !out.trim().is_empty() {
                                contact_outputs.push(out);
                                let merged = contact_outputs.join("\n");
                                let (found_name, found_title, found_linkedin) =
                                    extract_contact_from_search(
                                        &merged,
                                        profile.target_title_policy.as_str(),
                                    );
                                if found_name.is_some()
                                    && (found_linkedin.is_some() || found_title.is_some())
                                {
                                    break;
                                }
                            }
                        }
                    }
                    let contact_res = contact_outputs.join("\n");

                    let (mut contact_name, mut contact_title, mut linkedin_url) =
                        extract_contact_from_search(
                            &contact_res,
                            profile.target_title_policy.as_str(),
                        );
                    let (entry_name, entry_title, entry_linkedin, entry_email) =
                        extract_contact_from_search_for_company(
                            &contact_res,
                            profile.target_title_policy.as_str(),
                            &company,
                            domain,
                        );
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
                    let mut email = normalize_contact_email_for_domain(
                        extract_email_from_text(&contact_res).or(entry_email),
                        domain,
                    )
                    .or_else(|| guessed_email(contact_name.as_deref(), domain));

                    if contact_name.is_none() || linkedin_url.is_none() || email.is_none() {
                        let fallback_contact_query = format!(
                            "\"{}\" \"{}\" {} (CEO OR \"Chief Executive Officer\" OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR Wikipedia OR leadership OR executive team OR email)",
                            company, domain, profile.target_geo
                        );
                        let fallback_contact_res = search_engine
                            .search(&fallback_contact_query, 10)
                            .await
                            .unwrap_or_default();
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
                                extract_email_from_text(&fallback_contact_res)
                                    .or(fallback_entry_email),
                                domain,
                            )
                            .or_else(|| guessed_email(contact_name.as_deref(), domain));
                        }
                    }
                    if linkedin_url.is_none() {
                        let company_linkedin_query =
                            format!("site:linkedin.com/company \"{}\" \"{}\"", company, domain);
                        if let Ok(company_linkedin_res) =
                            search_engine.search(&company_linkedin_query, 6).await
                        {
                            linkedin_url =
                                extract_company_linkedin_from_text(&company_linkedin_res);
                        }
                    }
                    (contact_name, contact_title, linkedin_url, email)
                };
            let mut site_evidence: Option<String> = None;
            let needs_enrichment =
                contact_name.is_none() || linkedin_url.is_none() || email.is_none();
            if needs_enrichment
                && direct_enrich_attempts < max_direct_enrich_attempts
                && site_client.is_some()
            {
                direct_enrich_attempts += 1;
                if let Some(client) = site_client.as_ref() {
                    if let Ok(pages) = tokio::time::timeout(
                        Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                        fetch_company_site_html_pages(client, domain),
                    )
                    .await
                    {
                        let mut best_page_signal = 0u8;
                        let mut best_site_name = None;
                        let mut best_site_title = None;
                        let mut best_site_linkedin = None;
                        let mut best_site_email = None;
                        let mut best_site_evidence = None;

                        for html in pages {
                            let (s_name, s_title, s_linkedin, s_email, s_evidence) =
                                extract_contact_from_company_site_html(
                                    &html,
                                    profile.target_title_policy.as_str(),
                                );
                            let signal = (s_name.is_some() as u8)
                                + (s_linkedin.is_some() as u8)
                                + (s_email.is_some() as u8)
                                + (s_evidence.is_some() as u8);
                            if signal > best_page_signal {
                                best_page_signal = signal;
                                best_site_name = s_name;
                                best_site_title = s_title;
                                best_site_linkedin = s_linkedin;
                                best_site_email = s_email;
                                best_site_evidence = s_evidence;
                            }
                            if best_page_signal >= 3 {
                                break;
                            }
                        }

                        if contact_name.is_none() {
                            contact_name = best_site_name;
                        }
                        if contact_title.is_none() {
                            contact_title = best_site_title;
                        }
                        if linkedin_url.is_none() {
                            linkedin_url = best_site_linkedin;
                        }
                        if email.is_none() {
                            email = normalize_contact_email_for_domain(best_site_email, domain)
                                .or_else(|| guessed_email(contact_name.as_deref(), domain));
                        }
                        site_evidence = best_site_evidence;
                    }
                }
            }

            contact_name = contact_name.and_then(|n| normalize_person_name(&n));
            email = normalize_contact_email_for_domain(email, domain);

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

        self.finish_run(
            &run_id,
            "completed",
            discovered,
            inserted,
            approvals_queued,
            None,
        )?;

        Ok(SalesRunRecord {
            id: run_id,
            status: "completed".to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered,
            inserted,
            approvals_queued,
            error: None,
        })
    }
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
    let trimmed = raw_url.trim_matches(|c: char| c == ')' || c == '(' || c == ',' || c == '.');
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
    Some(host)
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

    let static_blocked = BLOCKED
        .iter()
        .any(|blocked| domain == *blocked || domain.ends_with(&format!(".{blocked}")));
    if static_blocked {
        return true;
    }

    domain.starts_with("blog.")
        || domain.contains("dictionary")
        || domain.contains("definitions")
        || domain.contains("wiktionary")
        || domain.contains("marketresearch")
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
) -> (i32, Vec<String>) {
    if is_blocked_company_domain(domain) {
        return (-100, Vec::new());
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
                score += if norm.contains(' ') { 12 } else { 8 };
                matched.push(norm);
            }
        }
    }

    for kw in exclude_keywords {
        if let Some(norm) = normalize_keyword(kw) {
            if text.contains(&norm) {
                score -= 14;
            }
        }
    }

    if title.to_lowercase().contains("careers")
        || title.to_lowercase().contains("jobs")
        || title.to_lowercase().contains("blog")
        || title.to_lowercase().contains("news")
    {
        score -= 8;
    }

    (score, dedupe_strings(matched))
}

fn collect_domain_candidates_from_search(
    search_output: &str,
    out: &mut HashMap<String, DomainCandidate>,
    must_include_keywords: &[String],
    exclude_keywords: &[String],
) {
    for entry in parse_search_entries(search_output) {
        let Some(domain) = extract_domain(&entry.url) else {
            continue;
        };
        if is_blocked_company_domain(&domain) {
            continue;
        }
        let (score, matched) = score_search_entry(
            &domain,
            &entry.title,
            &entry.snippet,
            must_include_keywords,
            exclude_keywords,
        );
        let candidate = out.entry(domain.clone()).or_default();
        if candidate.domain.is_empty() {
            candidate.domain = domain.clone();
        }
        candidate.score += score;
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
    }
    map.into_values().collect()
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

    let personal_linkedin_url = extract_personal_linkedin_from_text(&filtered_output);
    let company_linkedin_url = extract_company_linkedin_from_text(&filtered_output);
    let linkedin_url = personal_linkedin_url
        .clone()
        .or_else(|| company_linkedin_url.clone());

    let ranked_re = regex_lite::Regex::new(
        r"(?im)^\s*\d+\.\s*([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\s*[-|]\s*(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)",
    )
    .unwrap();
    if let Some(cap) = ranked_re.captures(&filtered_output) {
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
    if let Some(cap) = comma_name_title_re.captures(&filtered_output) {
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
    if let Some(cap) = name_then_title_re.captures(&filtered_output) {
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
    if let Some(cap) = title_then_name_re.captures(&filtered_output) {
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
    if let Some(cap) = title_punct_name_re.captures(&filtered_output) {
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
    if let Some(cap) = sentence_re.captures(&filtered_output) {
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
        let (name, title, mut linkedin) = extract_contact_from_search(&single_result, title_policy);
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
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_person_name(raw: &str) -> Option<String> {
    let cleaned = raw
        .trim_matches(|c: char| {
            !c.is_alphanumeric() && c != '.' && c != '\'' && c != '-' && c != ' '
        })
        .split_whitespace()
        .collect::<Vec<_>>();
    if cleaned.len() < 2 || cleaned.len() > 5 {
        return None;
    }

    let stopwords = [
        "and",
        "or",
        "the",
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
        "office",
        "genel",
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
        "senegal",
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
        if t.chars().count() > 18 {
            return None;
        }
        let t_lower = t.to_lowercase();
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
    Some(out.join(" "))
}

fn extract_email_from_text(text: &str) -> Option<String> {
    let mailto_re =
        regex_lite::Regex::new(r"(?i)mailto:([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})").unwrap();
    if let Some(cap) = mailto_re.captures(text) {
        let email = cap
            .get(1)
            .map(|m| m.as_str().trim().to_lowercase())
            .unwrap_or_default();
        if !email.is_empty() && !email.ends_with("@example.com") {
            return Some(email);
        }
    }

    let re = regex_lite::Regex::new(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b").unwrap();
    for m in re.find_iter(text) {
        let email = m
            .as_str()
            .trim_matches(|c: char| c == '.' || c == ',' || c == ';' || c == ':' || c == ')')
            .trim_start_matches('(')
            .to_lowercase();
        if email.is_empty()
            || email.ends_with("@example.com")
            || email.contains("noreply")
            || email.contains("no-reply")
        {
            continue;
        }
        return Some(email);
    }
    let alt_re = regex_lite::Regex::new(
        r"(?i)\b([A-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\sat\s)\s*([A-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\sdot\s)\s*([A-Z]{2,})\b",
    )
    .unwrap();
    if let Some(cap) = alt_re.captures(text) {
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
        if !local.is_empty() && !domain.is_empty() && !tld.is_empty() {
            return Some(format!("{local}@{domain}.{tld}"));
        }
    }
    None
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
    email.and_then(|e| {
        let trimmed = e.trim().to_lowercase();
        if trimmed.is_empty() || !email_matches_company_domain(&trimmed, company_domain) {
            return None;
        }
        Some(trimmed)
    })
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
    truncate_cleaned_text(&no_tags, 20_000)
}

fn extract_internal_enrich_links(base_url: &url::Url, html: &str) -> Vec<String> {
    let href_re = regex_lite::Regex::new(r#"(?is)href\s*=\s*["']([^"']+)["']"#).unwrap();
    let keywords = [
        "about",
        "team",
        "leadership",
        "management",
        "executive",
        "contact",
        "hakkimizda",
        "kurumsal",
        "ekip",
        "yonetim",
    ];

    let mut out = Vec::<String>::new();
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
        let host_ok = abs.host_str() == base_url.host_str();
        if !host_ok {
            continue;
        }
        let path = abs.path().to_lowercase();
        if !keywords.iter().any(|kw| path.contains(kw)) {
            continue;
        }
        let key = abs.as_str().to_string();
        if seen.insert(key.clone()) {
            out.push(key);
        }
        if out.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            break;
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
        "/kurumsal",
        "/kurumsal/yonetim",
        "/yonetim",
        "/iletisim",
    ];
    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for path in defaults {
        if let Ok(url) = base_url.join(path) {
            let key = url.as_str().to_string();
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

async fn fetch_company_site_html_pages(client: &reqwest::Client, domain: &str) -> Vec<String> {
    let candidates = [
        format!("https://{domain}"),
        format!("https://www.{domain}"),
        format!("http://{domain}"),
    ];
    for url in candidates {
        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !resp.status().is_success() {
            continue;
        }
        let ctype = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !ctype.is_empty() && !ctype.contains("text/html") && !ctype.contains("application/xhtml")
        {
            continue;
        }
        let body = match resp.text().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        if body.trim().is_empty() {
            continue;
        }
        let mut pages = vec![body.clone()];
        if let Ok(base) = url::Url::parse(&url) {
            let mut links = extract_internal_enrich_links(&base, &body);
            let mut seen = links.iter().cloned().collect::<HashSet<_>>();
            for fallback in default_internal_enrich_links(&base) {
                if seen.insert(fallback.clone()) {
                    links.push(fallback);
                }
                if links.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
                    break;
                }
            }

            for link in links.into_iter().take(MAX_EXTRA_SITE_ENRICH_PAGES) {
                if let Ok(r) = client.get(&link).send().await {
                    if !r.status().is_success() {
                        continue;
                    }
                    if let Ok(extra) = r.text().await {
                        if !extra.trim().is_empty() {
                            pages.push(extra);
                        }
                    }
                }
            }
        }
        return pages;
    }
    Vec::new()
}

fn extract_contact_from_json_ld(
    html: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
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

        if let Some(c) = name_job_re.captures(&raw) {
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
                return (name, title);
            }
        }

        if let Some(c) = job_name_re.captures(&raw) {
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
                return (name, title);
            }
        }

        if let Some(c) = founder_re.captures(&raw) {
            let name = c
                .get(1)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            if name.is_some() && title_policy != "ceo_only" {
                return (name, Some("Founder".to_string()));
            }
        }
    }

    (None, None)
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
    let lower_plain = plain.to_lowercase();
    let personal_linkedin_url = extract_personal_linkedin_from_text(html)
        .or_else(|| extract_personal_linkedin_from_text(&plain));
    let company_linkedin_url = extract_company_linkedin_from_text(html)
        .or_else(|| extract_company_linkedin_from_text(&plain));
    let linkedin_url = personal_linkedin_url
        .clone()
        .or_else(|| company_linkedin_url.clone());
    let mut contact_name = personal_linkedin_url
        .as_deref()
        .and_then(extract_name_from_linkedin_url)
        .and_then(|n| normalize_person_name(&n));
    let mut contact_title = default_contact_title(title_policy);

    let (jsonld_name, jsonld_title) = extract_contact_from_json_ld(html, title_policy);
    if jsonld_name.is_some() {
        contact_name = jsonld_name;
    }
    if jsonld_title.is_some() {
        contact_title = jsonld_title;
    }

    let name_then_title_re = regex_lite::Regex::new(
        r"(?is)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b[^\n\r]{0,140}\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b",
    )
    .unwrap();
    if let Some(cap) = name_then_title_re.captures(&plain) {
        let name = cap
            .get(1)
            .and_then(|m| normalize_person_name(m.as_str().trim()));
        let title = cap.get(2).map(|m| normalize_contact_title(m.as_str()));
        if title
            .as_deref()
            .map(|t| title_allowed_for_policy(title_policy, t))
            .unwrap_or(false)
        {
            if name.is_some() {
                contact_name = name;
            }
            contact_title = title;
        }
    } else {
        let comma_name_title_re = regex_lite::Regex::new(
            r"(?is)\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b\s*,\s*(?:President and )?(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b",
        )
        .unwrap();
        if let Some(cap) = comma_name_title_re.captures(&plain) {
            let name = cap
                .get(1)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            let title = cap.get(2).map(|m| normalize_contact_title(m.as_str()));
            if title
                .as_deref()
                .map(|t| title_allowed_for_policy(title_policy, t))
                .unwrap_or(false)
            {
                if name.is_some() {
                    contact_name = name;
                }
                contact_title = title;
            }
        }

        let title_then_name_re = regex_lite::Regex::new(
            r"(?is)\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b[^\n\r]{0,80}\b([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b",
        )
        .unwrap();
        if let Some(cap) = title_then_name_re.captures(&plain) {
            let title = cap.get(1).map(|m| normalize_contact_title(m.as_str()));
            let name = cap
                .get(2)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            if title
                .as_deref()
                .map(|t| title_allowed_for_policy(title_policy, t))
                .unwrap_or(false)
            {
                if name.is_some() {
                    contact_name = name;
                }
                contact_title = title;
            }
        } else {
            let title_punct_name_re = regex_lite::Regex::new(
                r"(?is)\b(CEO|Chief Executive Officer|Founder|Co[- ]Founder|Managing Director|COO|Chief Operating Officer|Head of Operations|Operations Director)\b\s*[:\-–]\s*([A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*(?:\s+[A-ZÀ-ÖØ-ÞİĞŞÇÜÖÂÊÎÔÛ][A-Za-zÀ-ÖØ-öø-ÿİıĞğŞşÇçÜüÖöÂâÊêÎîÔôÛû\.'\-]*){1,4})\b",
            )
            .unwrap();
            if let Some(cap) = title_punct_name_re.captures(&plain) {
                let title = cap.get(1).map(|m| normalize_contact_title(m.as_str()));
                let name = cap
                    .get(2)
                    .and_then(|m| normalize_person_name(m.as_str().trim()));
                if title
                    .as_deref()
                    .map(|t| title_allowed_for_policy(title_policy, t))
                    .unwrap_or(false)
                {
                    if name.is_some() {
                        contact_name = name;
                    }
                    contact_title = title;
                }
            }
        }
    }

    let email = extract_email_from_text(&plain).or_else(|| extract_email_from_text(html));
    let evidence = [
        "field service",
        "on-site",
        "onsite",
        "dispatch",
        "maintenance",
        "installation",
        "facility",
        "construction",
        "operations",
    ]
    .iter()
    .find_map(|kw| {
        if lower_plain.contains(kw) {
            Some(format!("Company website mentions '{}'", kw))
        } else {
            None
        }
    });

    (contact_name, contact_title, linkedin_url, email, evidence)
}

fn guessed_email(contact_name: Option<&str>, domain: &str) -> Option<String> {
    let name = contact_name?;
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
            title: "Ilk lead uretimini tamamla".to_string(),
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

fn text_has_field_ops_signal(text: &str) -> bool {
    let t = text.to_lowercase();
    t.contains("field")
        || t.contains("saha")
        || t.contains("on-site")
        || t.contains("onsite")
        || t.contains("dispatch")
        || t.contains("maintenance")
        || t.contains("facility")
        || t.contains("construction")
        || t.contains("installation")
        || t.contains("service team")
        || t.contains("mobile workforce")
}

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

fn candidate_has_relaxed_field_ops_signal(candidate: &DomainCandidate) -> bool {
    candidate_has_field_ops_signal(candidate)
        || candidate.matched_keywords.iter().any(|kw| {
            let t = kw.to_lowercase();
            t.contains("operations")
                || t.contains("operasyon")
                || t.contains("maintenance")
                || t.contains("facility")
                || t.contains("construction")
                || t.contains("field")
        })
}

fn deterministic_seed_domains_for_profile(
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<String> {
    let geo = profile.target_geo.to_lowercase();
    let is_field_ops = profile_targets_field_ops(profile);
    let mut seeds = Vec::<String>::new();

    if is_field_ops {
        seeds.extend(
            [
                "johnsoncontrols.com",
                "schneider-electric.com",
                "siemens.com",
                "abb.com",
                "honeywell.com",
                "emerson.com",
                "carrier.com",
                "trane.com",
                "otis.com",
                "kone.com",
                "schindler.com",
                "dormakaba.com",
                "sodexo.com",
                "issworld.com",
                "veolia.com",
                "bilfinger.com",
                "cbre.com",
                "jll.com",
                "cushmanwakefield.com",
                "mitie.com",
                "serco.com",
                "fluor.com",
                "bechtel.com",
                "kbr.com",
                "vinci.com",
                "ferrovial.com",
                "skanska.com",
                "strabag.com",
                "bouygues.com",
                "eiffage.com",
                "hochtief.com",
                "balfourbeatty.com",
                "wsp.com",
                "stantec.com",
                "aecom.com",
                "jacobs.com",
                "emcor.net",
                "comfortsystemsusa.com",
                "prim.com",
                "myrgroup.com",
                "mastec.com",
                "quanta.com",
                "dycomind.com",
                "apigroupinc.com",
                "g4s.com",
                "securitas.com",
                "tk-elevator.com",
            ]
            .iter()
            .map(|d| d.to_string()),
        );

        if geo.contains("tr")
            || geo.contains("turkiye")
            || geo.contains("türkiye")
            || geo.contains("turkey")
        {
            seeds.extend(
                [
                    "enerjisa.com.tr",
                    "aksaenerji.com.tr",
                    "tekfen.com.tr",
                    "limak.com.tr",
                    "koc.com.tr",
                    "enka.com",
                    "ronesans.com",
                    "yapimerkezi.com.tr",
                    "dogusinsaat.com.tr",
                    "kalyoninsaat.com.tr",
                    "gama.com.tr",
                    "icictaas.com.tr",
                    "kolin.com.tr",
                    "nurol.com.tr",
                    "stfa.com",
                    "yukselinsaat.com.tr",
                ]
                .iter()
                .map(|d| d.to_string()),
            );
        } else if geo.contains("eu") || geo.contains("europe") || geo.contains("avrupa") {
            seeds.extend(
                [
                    "vinci.com",
                    "ferrovial.com",
                    "skanska.com",
                    "strabag.com",
                    "bouygues-construction.com",
                    "eiffage.com",
                    "hochtief.com",
                    "mitie.com",
                    "serco.com",
                    "wates.co.uk",
                    "macegroup.com",
                    "interserve.com",
                    "spiie.com",
                ]
                .iter()
                .map(|d| d.to_string()),
            );
        } else {
            seeds.extend(
                [
                    "quanta.com",
                    "mastec.com",
                    "dycomind.com",
                    "aecom.com",
                    "jacobs.com",
                    "emcor.net",
                    "comfortsystemsusa.com",
                    "prim.com",
                    "myrgroup.com",
                    "apigroupinc.com",
                    "unisys.com",
                    "steris.com",
                    "eosits.com",
                ]
                .iter()
                .map(|d| d.to_string()),
            );
        }
    } else {
        seeds.extend(
            [
                "ge.com",
                "3m.com",
                "bosch.com",
                "siemens.com",
                "honeywell.com",
                "emerson.com",
                "abb.com",
                "schneider-electric.com",
            ]
            .iter()
            .map(|d| d.to_string()),
        );
    }

    let mut unique = dedupe_strings(
        seeds
            .into_iter()
            .filter(|d| !is_blocked_company_domain(d))
            .filter(|d| d.split('.').count() >= 2)
            .collect(),
    );
    if !unique.is_empty() {
        let offset = run_sequence % unique.len();
        unique.rotate_left(offset);
    }
    unique
}

fn heuristic_lead_query_plan(profile: &SalesProfile) -> LeadQueryPlanDraft {
    let is_field_ops = profile_targets_field_ops(profile);
    let geo = if profile.target_geo.trim().is_empty() {
        "US".to_string()
    } else {
        profile.target_geo.clone()
    };
    let discovery_topic = if is_field_ops {
        "field service maintenance installation facility management construction".to_string()
    } else {
        profile.target_industry.clone()
    };

    let mut discovery_queries = vec![
        format!("{discovery_topic} companies {geo} COO CEO operations"),
        format!(
            "{} organizations {} project operations teams",
            discovery_topic, geo
        ),
        format!(
            "{} firms {} operational excellence transformation",
            discovery_topic, geo
        ),
    ];

    if is_field_ops {
        discovery_queries.extend([
            format!(
                "field service companies {} (CEO OR COO OR Operations Director)",
                geo
            ),
            format!(
                "construction facility maintenance companies {} operations",
                geo
            ),
            format!(
                "facility management companies {} leadership team operations",
                geo
            ),
            format!("companies with on-site teams {} project coordination", geo),
            format!("mobile workforce companies {} operations executive", geo),
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
    ];

    LeadQueryPlanDraft {
        discovery_queries: dedupe_strings(discovery_queries),
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

fn build_sales_llm_driver() -> Result<Arc<dyn openfang_runtime::llm_driver::LlmDriver>, String> {
    let cfg = DriverConfig {
        provider: SALES_LLM_PROVIDER.to_string(),
        api_key: None,
        base_url: None,
    };
    openfang_runtime::drivers::create_driver(&cfg)
        .map_err(|e| format!("LLM driver init failed: {e}"))
}

async fn llm_build_lead_query_plan(
    _kernel: &openfang_kernel::OpenFangKernel,
    profile: &SalesProfile,
) -> Result<LeadQueryPlanDraft, String> {
    let driver = build_sales_llm_driver()?;

    let prompt = format!(
        "You are generating a B2B outbound lead discovery plan.\n\
         Product: {}\n\
         Product value proposition: {}\n\
         Target industry: {}\n\
         Target geography: {}\n\
         Target title policy: {}\n\n\
         Return strict JSON only with keys:\n\
         discovery_queries (array of 8-14 web queries to find PROSPECT COMPANIES, not blogs/directories),\n\
         must_include_keywords (array),\n\
         exclude_keywords (array),\n\
         contact_titles (array).\n\n\
         Rules:\n\
         - If product suggests field/on-site operations, prioritize companies with field teams.\n\
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
        max_tokens: 1200,
        temperature: 0.0,
        system: Some(
            "You are a precise outbound prospecting strategist. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::High),
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
            .map(|q| q.trim().to_string())
            .filter(|q| !q.is_empty())
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
    _kernel: &openfang_kernel::OpenFangKernel,
    profile: &SalesProfile,
    max_companies: usize,
) -> Result<Vec<DomainCandidate>, String> {
    let driver = build_sales_llm_driver()?;

    let is_field_ops = profile_targets_field_ops(profile);
    let prompt = format!(
        "List real B2B prospect companies for outbound.\n\
         Product: {}\n\
         Product value: {}\n\
         Target industry: {}\n\
         Geo: {}\n\
         Need: prioritize companies with field/on-site operations when relevant.\n\
         Return strict JSON only with shape:\n\
         {{\"companies\":[{{\"company\":\"...\",\"domain\":\"...\",\"reason\":\"...\"}}]}}\n\
         Constraints:\n\
         - {} companies max\n\
         - domain must be company website domain (no linkedin/wikipedia/news/directories)\n\
         - reason must be short and concrete.\n\
         - if field/on-site operations are relevant, reason must explicitly mention field operations context (e.g. field service, on-site teams, dispatch, maintenance, installation).",
        profile.product_name,
        profile.product_description,
        profile.target_industry,
        profile.target_geo,
        max_companies
    );

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 1400,
        temperature: 0.1,
        system: Some(
            "You are a B2B outbound researcher. Output strict valid JSON only.".to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::High),
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
            .unwrap_or_else(|| format!("Suggested for {}", profile.target_industry));
        if is_field_ops && !text_has_field_ops_signal(&reason) {
            continue;
        }
        let mut matched = vec![profile.target_industry.clone()];
        if let Some(company) = c.company {
            matched.push(company);
        }
        out.push(DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 8,
            evidence: vec![truncate_text_for_reason(&reason, 220)],
            matched_keywords: dedupe_strings(matched),
        });
    }

    Ok(out)
}

async fn llm_autofill_profile(_state: &AppState, brief: &str) -> Result<SalesProfileDraft, String> {
    let driver = build_sales_llm_driver()?;

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
        max_tokens: 900,
        temperature: 0.1,
        system: Some(
            "You are a B2B sales operations analyst. Extract precise ICP/profile fields from noisy long briefs. Output strict valid JSON only."
                .to_string(),
        ),
        thinking: None,
        reasoning_effort: Some(ReasoningEffort::High),
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
                max_tokens: 700,
                temperature: 0.0,
                system: Some(
                    "You are a JSON repair assistant. Always output strict valid JSON.".to_string(),
                ),
                thinking: None,
                reasoning_effort: Some(ReasoningEffort::High),
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

    match engine.upsert_profile(&profile) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "saved"}))),
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
        Duration::from_secs(240),
        engine.run_generation(&state.kernel),
    )
    .await
    {
        Ok(Ok(run)) => (StatusCode::OK, Json(serde_json::json!({"run": run}))),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
        Err(_) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Sales run timed out while collecting leads. Check network/search availability and retry."
            })),
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

            match engine.already_ran_today() {
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
        };
        assert!(!candidate_has_field_ops_signal(&only_generic));
    }

    #[test]
    fn normalize_person_name_rejects_business_phrase() {
        assert!(normalize_person_name("TechEx Sustainable Legacies Welcoming Workplace").is_none());
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
    fn guessed_email_requires_plausible_person_name() {
        let ok = guessed_email(Some("John Doe"), "example.com");
        let bad = guessed_email(Some("Experience Like No Other"), "example.com");
        assert_eq!(ok.as_deref(), Some("john.doe@example.com"));
        assert!(bad.is_none());
    }
}
