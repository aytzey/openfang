//! Request/response types for the OpenFang API.

use serde::{Deserialize, Serialize};

/// Request to spawn an agent from a TOML manifest string.
#[derive(Debug, Deserialize)]
pub struct SpawnRequest {
    /// Agent manifest as TOML string.
    pub manifest_toml: String,
    /// Optional Ed25519 signed manifest envelope (JSON).
    /// When present, the signature is verified before spawning.
    #[serde(default)]
    pub signed_manifest: Option<String>,
}

/// Response after spawning an agent.
#[derive(Debug, Serialize)]
pub struct SpawnResponse {
    pub agent_id: String,
    pub name: String,
}

/// A file attachment reference (from a prior upload).
#[derive(Debug, Clone, Deserialize)]
pub struct AttachmentRef {
    pub file_id: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub content_type: String,
}

/// Request to send a message to an agent.
#[derive(Debug, Deserialize)]
pub struct MessageRequest {
    pub message: String,
    /// Optional file attachments (uploaded via /upload endpoint).
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

/// Response from sending a message.
#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub response: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub iterations: u32,
}

fn default_memory_search_limit() -> usize {
    5
}

/// Query parameters for semantic memory search.
#[derive(Debug, Deserialize)]
pub struct MemorySearchQuery {
    #[serde(rename = "q")]
    pub query: String,
    #[serde(default = "default_memory_search_limit")]
    pub limit: usize,
    #[serde(default)]
    pub scope: Option<String>,
}

/// A single semantic memory hit returned by the API.
#[derive(Debug, Serialize)]
pub struct MemorySearchHit {
    pub id: String,
    pub content: String,
    pub scope: String,
    pub source: openfang_types::memory::MemorySource,
    pub confidence: f32,
    pub access_count: u64,
    pub score: f32,
    pub gate: f32,
    pub lexical_confidence: f32,
    pub semantic_score: f32,
    pub lexical_hits: u32,
}

/// Response payload for semantic memory search.
#[derive(Debug, Serialize)]
pub struct MemorySearchResponse {
    pub query: String,
    pub strategy: String,
    pub results: Vec<MemorySearchHit>,
}

/// Request to install a skill from the marketplace.
#[derive(Debug, Deserialize)]
pub struct SkillInstallRequest {
    pub name: String,
}

/// Request to uninstall a skill.
#[derive(Debug, Deserialize)]
pub struct SkillUninstallRequest {
    pub name: String,
}

/// Request to update an agent's manifest.
#[derive(Debug, Deserialize)]
pub struct AgentUpdateRequest {
    pub manifest_toml: String,
}

/// Request to change an agent's operational mode.
#[derive(Debug, Deserialize)]
pub struct SetModeRequest {
    pub mode: openfang_types::agent::AgentMode,
}

/// Request to run a migration.
#[derive(Debug, Deserialize)]
pub struct MigrateRequest {
    pub source: String,
    pub source_dir: String,
    pub target_dir: String,
    #[serde(default)]
    pub dry_run: bool,
}

/// Request to scan a directory for migration.
#[derive(Debug, Deserialize)]
pub struct MigrateScanRequest {
    pub path: String,
}

/// Request to install a skill from ClawHub.
#[derive(Debug, Deserialize)]
pub struct ClawHubInstallRequest {
    /// ClawHub skill slug (e.g., "github-helper").
    pub slug: String,
}
