//! Semantic memory store with hybrid lookup support.
//!
//! Recall uses a two-stage path:
//! 1. Deterministic hashed n-gram lookup to fetch candidates cheaply.
//! 2. Context-aware gating that fuses lexical priors with semantic similarity.
//!
//! This keeps memory recall fast and robust even when the store grows, while
//! still using embeddings when they are available.

use crate::lookup::{
    canonical_tokens, extract_hashed_ngrams, token_overlap_score, total_slot_weight,
};
use chrono::Utc;
use openfang_types::agent::AgentId;
use openfang_types::error::{OpenFangError, OpenFangResult};
use openfang_types::memory::{MemoryFilter, MemoryFragment, MemoryId, MemorySource};
use rusqlite::{types::Value, Connection};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Semantic store backed by SQLite with optional vector search.
#[derive(Clone)]
pub struct SemanticStore {
    conn: Arc<Mutex<Connection>>,
}

/// Ranked semantic recall result with score breakdown.
#[derive(Debug, Clone, Serialize)]
pub struct ScoredMemoryMatch {
    pub fragment: MemoryFragment,
    pub score: f32,
    pub gate: f32,
    pub lexical_confidence: f32,
    pub semantic_score: f32,
    pub lexical_hits: u32,
}

impl SemanticStore {
    /// Create a new semantic store wrapping the given connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Store a new memory fragment (without embedding).
    pub fn remember(
        &self,
        agent_id: AgentId,
        content: &str,
        source: MemorySource,
        scope: &str,
        metadata: HashMap<String, serde_json::Value>,
    ) -> OpenFangResult<MemoryId> {
        self.remember_with_embedding(agent_id, content, source, scope, metadata, None)
    }

    /// Store a new memory fragment with an optional embedding vector.
    pub fn remember_with_embedding(
        &self,
        agent_id: AgentId,
        content: &str,
        source: MemorySource,
        scope: &str,
        metadata: HashMap<String, serde_json::Value>,
        embedding: Option<&[f32]>,
    ) -> OpenFangResult<MemoryId> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| OpenFangError::Internal(e.to_string()))?;
        let id = MemoryId::new();
        let now = Utc::now().to_rfc3339();
        let source_str = serde_json::to_string(&source)
            .map_err(|e| OpenFangError::Serialization(e.to_string()))?;
        let meta_str = serde_json::to_string(&metadata)
            .map_err(|e| OpenFangError::Serialization(e.to_string()))?;
        let embedding_bytes: Option<Vec<u8>> = embedding.map(embedding_to_bytes);

        conn.execute(
            "INSERT INTO memories (id, agent_id, content, source, scope, confidence, metadata, created_at, accessed_at, access_count, deleted, embedding)
             VALUES (?1, ?2, ?3, ?4, ?5, 1.0, ?6, ?7, ?7, 0, 0, ?8)",
            rusqlite::params![
                id.0.to_string(),
                agent_id.0.to_string(),
                content,
                source_str,
                scope,
                meta_str,
                now,
                embedding_bytes,
            ],
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

        index_memory_lookup_rows(&conn, &id.0.to_string(), &agent_id.0.to_string(), content)?;
        Ok(id)
    }

    /// Search for memories using text matching (fallback, no embeddings).
    pub fn recall(
        &self,
        query: &str,
        limit: usize,
        filter: Option<MemoryFilter>,
    ) -> OpenFangResult<Vec<MemoryFragment>> {
        self.recall_with_embedding(query, limit, filter, None)
    }

    /// Search for memories using vector similarity when a query embedding is provided,
    /// falling back to LIKE matching otherwise.
    pub fn recall_with_embedding(
        &self,
        query: &str,
        limit: usize,
        filter: Option<MemoryFilter>,
        query_embedding: Option<&[f32]>,
    ) -> OpenFangResult<Vec<MemoryFragment>> {
        Ok(self
            .recall_scored_with_embedding(query, limit, filter, query_embedding)?
            .into_iter()
            .map(|hit| hit.fragment)
            .collect())
    }

    /// Search for memories and return score details for observability/debugging.
    pub fn recall_scored_with_embedding(
        &self,
        query: &str,
        limit: usize,
        filter: Option<MemoryFilter>,
        query_embedding: Option<&[f32]>,
    ) -> OpenFangResult<Vec<ScoredMemoryMatch>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| OpenFangError::Internal(e.to_string()))?;

        let query_slots = extract_hashed_ngrams(query);
        let query_tokens = canonical_tokens(query);
        let query_slot_weight = total_slot_weight(&query_slots).max(1.0);
        let candidate_limit = (limit * 8).max(40);
        let lexical_limit = ((candidate_limit as f32) * 0.75).round() as usize;

        let mut candidate_signals = HashMap::<String, CandidateSignal>::new();

        for candidate in lookup_candidates(
            &conn,
            &query_slots,
            lexical_limit.max(limit),
            filter.as_ref(),
        )? {
            candidate_signals.insert(
                candidate.id.clone(),
                CandidateSignal {
                    lexical_weight: candidate.lexical_weight,
                    lexical_hits: candidate.lexical_hits,
                },
            );
        }

        if candidate_signals.len() < candidate_limit {
            let recent_candidates = recent_candidates(
                &conn,
                candidate_limit - candidate_signals.len(),
                filter.as_ref(),
                query_embedding.is_some(),
            )?;
            for id in recent_candidates {
                candidate_signals.entry(id).or_default();
            }
        }

        if query_embedding.is_some() && candidate_signals.len() < candidate_limit {
            let recent_candidates = recent_candidates(
                &conn,
                candidate_limit - candidate_signals.len(),
                filter.as_ref(),
                false,
            )?;
            for id in recent_candidates {
                candidate_signals.entry(id).or_default();
            }
        }

        if candidate_signals.is_empty() && !query.is_empty() {
            let fallback = text_match_candidates(&conn, query, candidate_limit, filter.as_ref())?;
            for id in fallback {
                candidate_signals.entry(id).or_default();
            }
        }

        if candidate_signals.is_empty() {
            return Ok(Vec::new());
        }

        let candidate_ids: Vec<String> = candidate_signals.keys().cloned().collect();
        let mut scored: Vec<ScoredMemoryMatch> = fetch_fragments_by_ids(&conn, &candidate_ids)?
            .into_iter()
            .map(|fragment| {
                let signal = candidate_signals
                    .get(&fragment.id.0.to_string())
                    .cloned()
                    .unwrap_or_default();

                let lexical_confidence =
                    (signal.lexical_weight / query_slot_weight).clamp(0.0, 1.0);
                let semantic_score = semantic_agreement(query_embedding, &query_tokens, &fragment);
                let gate = sigmoid(4.0 * semantic_score + 2.0 * lexical_confidence - 2.0);
                let access_boost =
                    ((fragment.access_count as f32 + 1.0).ln() / 4.0).clamp(0.0, 1.0);
                let recency_boost = recency_boost(fragment.accessed_at);
                let score = gate * (0.55 * semantic_score + 0.45 * lexical_confidence)
                    + 0.05 * access_boost
                    + 0.05 * recency_boost
                    + 0.01 * signal.lexical_hits as f32;

                ScoredMemoryMatch {
                    fragment,
                    score,
                    gate,
                    lexical_confidence,
                    semantic_score,
                    lexical_hits: signal.lexical_hits,
                }
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let hits: Vec<ScoredMemoryMatch> = scored.into_iter().take(limit).collect();

        // Update access counts for returned memories
        for hit in &hits {
            let _ = conn.execute(
                "UPDATE memories SET access_count = access_count + 1, accessed_at = ?1 WHERE id = ?2",
                rusqlite::params![Utc::now().to_rfc3339(), hit.fragment.id.0.to_string()],
            );
        }

        Ok(hits)
    }

    /// Soft-delete a memory fragment.
    pub fn forget(&self, id: MemoryId) -> OpenFangResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| OpenFangError::Internal(e.to_string()))?;
        conn.execute(
            "DELETE FROM memory_lookup_index WHERE memory_id = ?1",
            rusqlite::params![id.0.to_string()],
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
        conn.execute(
            "UPDATE memories SET deleted = 1 WHERE id = ?1",
            rusqlite::params![id.0.to_string()],
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
        Ok(())
    }

    /// Update the embedding for an existing memory.
    pub fn update_embedding(&self, id: MemoryId, embedding: &[f32]) -> OpenFangResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| OpenFangError::Internal(e.to_string()))?;
        let bytes = embedding_to_bytes(embedding);
        conn.execute(
            "UPDATE memories SET embedding = ?1 WHERE id = ?2",
            rusqlite::params![bytes, id.0.to_string()],
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct CandidateSignal {
    lexical_weight: f32,
    lexical_hits: u32,
}

#[derive(Debug, Clone)]
struct LookupCandidate {
    id: String,
    lexical_weight: f32,
    lexical_hits: u32,
}

fn lookup_candidates(
    conn: &Connection,
    query_slots: &[crate::lookup::HashedNgram],
    limit: usize,
    filter: Option<&MemoryFilter>,
) -> OpenFangResult<Vec<LookupCandidate>> {
    if query_slots.is_empty() {
        return Ok(Vec::new());
    }

    let values_sql = std::iter::repeat_n("(?, ?, ?, ?)", query_slots.len())
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = format!(
        "WITH lookup(ngram_hash, ngram_order, hash_head, weight) AS (VALUES {values_sql})
         SELECT m.id, SUM(lookup.weight) AS lexical_weight, COUNT(*) AS lexical_hits
         FROM lookup
         JOIN memory_lookup_index idx
           ON idx.ngram_hash = lookup.ngram_hash
          AND idx.ngram_order = lookup.ngram_order
          AND idx.hash_head = lookup.hash_head
         JOIN memories m ON m.id = idx.memory_id
         WHERE m.deleted = 0"
    );

    let mut params = Vec::<Value>::new();
    for slot in query_slots {
        params.push(Value::Integer(slot.hash));
        params.push(Value::Integer(slot.order as i64));
        params.push(Value::Integer(slot.head as i64));
        params.push(Value::Real(slot.weight as f64));
    }
    append_filter_clauses(&mut sql, &mut params, filter)?;

    sql.push_str(
        " GROUP BY m.id ORDER BY lexical_weight DESC, lexical_hits DESC, MAX(m.accessed_at) DESC",
    );
    sql.push_str(&format!(" LIMIT {}", limit.max(1)));

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Ok(LookupCandidate {
                id: row.get(0)?,
                lexical_weight: row.get::<_, f64>(1)? as f32,
                lexical_hits: row.get::<_, i64>(2)? as u32,
            })
        })
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(|e| OpenFangError::Memory(e.to_string()))?);
    }

    debug!(
        hits = candidates.len(),
        slots = query_slots.len(),
        "Deterministic lexical lookup produced candidates"
    );

    Ok(candidates)
}

fn recent_candidates(
    conn: &Connection,
    limit: usize,
    filter: Option<&MemoryFilter>,
    require_embedding: bool,
) -> OpenFangResult<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut sql = String::from("SELECT m.id FROM memories m WHERE m.deleted = 0");
    let mut params = Vec::<Value>::new();
    if require_embedding {
        sql.push_str(" AND m.embedding IS NOT NULL");
    }
    append_filter_clauses(&mut sql, &mut params, filter)?;
    sql.push_str(" ORDER BY accessed_at DESC, access_count DESC");
    sql.push_str(&format!(" LIMIT {}", limit));

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(0))
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(|e| OpenFangError::Memory(e.to_string()))?);
    }
    Ok(ids)
}

fn text_match_candidates(
    conn: &Connection,
    query: &str,
    limit: usize,
    filter: Option<&MemoryFilter>,
) -> OpenFangResult<Vec<String>> {
    let mut sql =
        String::from("SELECT m.id FROM memories m WHERE m.deleted = 0 AND m.content LIKE ?");
    let mut params = vec![Value::Text(format!("%{query}%"))];
    append_filter_clauses(&mut sql, &mut params, filter)?;
    sql.push_str(" ORDER BY accessed_at DESC, access_count DESC");
    sql.push_str(&format!(" LIMIT {}", limit.max(1)));

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(0))
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(|e| OpenFangError::Memory(e.to_string()))?);
    }
    Ok(ids)
}

fn fetch_fragments_by_ids(
    conn: &Connection,
    ids: &[String],
) -> OpenFangResult<Vec<MemoryFragment>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat_n("?", ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id, agent_id, content, source, scope, confidence, metadata, created_at, accessed_at, access_count, embedding
         FROM memories
         WHERE deleted = 0 AND id IN ({placeholders})"
    );

    let params: Vec<Value> = ids.iter().cloned().map(Value::Text).collect();
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(params.iter()),
            parse_memory_fragment,
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

    let mut fragments = Vec::new();
    for row in rows {
        fragments.push(row.map_err(|e| OpenFangError::Memory(e.to_string()))?);
    }
    Ok(fragments)
}

fn parse_memory_fragment(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryFragment> {
    let id_str: String = row.get(0)?;
    let agent_str: String = row.get(1)?;
    let content: String = row.get(2)?;
    let source_str: String = row.get(3)?;
    let scope: String = row.get(4)?;
    let confidence: f64 = row.get(5)?;
    let meta_str: String = row.get(6)?;
    let created_str: String = row.get(7)?;
    let accessed_str: String = row.get(8)?;
    let access_count: i64 = row.get(9)?;
    let embedding_bytes: Option<Vec<u8>> = row.get(10)?;

    let id = uuid::Uuid::parse_str(&id_str).map(MemoryId).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let agent_id = uuid::Uuid::parse_str(&agent_str)
        .map(openfang_types::agent::AgentId)
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?;
    let source: MemorySource = serde_json::from_str(&source_str).unwrap_or(MemorySource::System);
    let metadata: HashMap<String, serde_json::Value> =
        serde_json::from_str(&meta_str).unwrap_or_default();
    let created_at = chrono::DateTime::parse_from_rfc3339(&created_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let accessed_at = chrono::DateTime::parse_from_rfc3339(&accessed_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let embedding = embedding_bytes.as_deref().map(embedding_from_bytes);

    Ok(MemoryFragment {
        id,
        agent_id,
        content,
        embedding,
        metadata,
        source,
        confidence: confidence as f32,
        created_at,
        accessed_at,
        access_count: access_count as u64,
        scope,
    })
}

fn append_filter_clauses(
    sql: &mut String,
    params: &mut Vec<Value>,
    filter: Option<&MemoryFilter>,
) -> OpenFangResult<()> {
    if let Some(filter) = filter {
        if let Some(agent_id) = filter.agent_id {
            sql.push_str(" AND m.agent_id = ?");
            params.push(Value::Text(agent_id.0.to_string()));
        }
        if let Some(scope) = &filter.scope {
            sql.push_str(" AND m.scope = ?");
            params.push(Value::Text(scope.clone()));
        }
        if let Some(min_conf) = filter.min_confidence {
            sql.push_str(" AND m.confidence >= ?");
            params.push(Value::Real(min_conf as f64));
        }
        if let Some(source) = &filter.source {
            let source_str = serde_json::to_string(source)
                .map_err(|e| OpenFangError::Serialization(e.to_string()))?;
            sql.push_str(" AND m.source = ?");
            params.push(Value::Text(source_str));
        }
        if let Some(after) = filter.after {
            sql.push_str(" AND m.created_at >= ?");
            params.push(Value::Text(after.to_rfc3339()));
        }
        if let Some(before) = filter.before {
            sql.push_str(" AND m.created_at <= ?");
            params.push(Value::Text(before.to_rfc3339()));
        }
    }
    Ok(())
}

fn index_memory_lookup_rows(
    conn: &Connection,
    memory_id: &str,
    agent_id: &str,
    content: &str,
) -> OpenFangResult<()> {
    let mut stmt = conn
        .prepare(
            "INSERT OR IGNORE INTO memory_lookup_index
             (memory_id, agent_id, ngram_hash, ngram_order, hash_head)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;

    for slot in extract_hashed_ngrams(content) {
        stmt.execute(rusqlite::params![
            memory_id,
            agent_id,
            slot.hash,
            slot.order as i64,
            slot.head as i64,
        ])
        .map_err(|e| OpenFangError::Memory(e.to_string()))?;
    }

    Ok(())
}

fn semantic_agreement(
    query_embedding: Option<&[f32]>,
    query_tokens: &[String],
    fragment: &MemoryFragment,
) -> f32 {
    if let (Some(query), Some(embedding)) = (query_embedding, fragment.embedding.as_deref()) {
        return cosine_similarity(query, embedding).clamp(0.0, 1.0);
    }

    token_overlap_score(query_tokens, &fragment.content)
}

fn recency_boost(accessed_at: chrono::DateTime<Utc>) -> f32 {
    let age_hours = (Utc::now() - accessed_at).num_hours().max(0) as f32;
    (1.0 / (1.0 + age_hours / (24.0 * 14.0))).clamp(0.0, 1.0)
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

/// Compute cosine similarity between two vectors.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < f32::EPSILON {
        0.0
    } else {
        dot / denom
    }
}

/// Serialize embedding to bytes for SQLite BLOB storage.
fn embedding_to_bytes(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for &val in embedding {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

/// Deserialize embedding from bytes.
fn embedding_from_bytes(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::run_migrations;

    fn setup() -> SemanticStore {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        SemanticStore::new(Arc::new(Mutex::new(conn)))
    }

    #[test]
    fn test_remember_and_recall() {
        let store = setup();
        let agent_id = AgentId::new();
        store
            .remember(
                agent_id,
                "The user likes Rust programming",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();
        let results = store.recall("Rust", 10, None).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].content.contains("Rust"));
    }

    #[test]
    fn test_recall_with_filter() {
        let store = setup();
        let agent_id = AgentId::new();
        store
            .remember(
                agent_id,
                "Memory A",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();
        store
            .remember(
                AgentId::new(),
                "Memory B",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();
        let filter = MemoryFilter::agent(agent_id);
        let results = store.recall("Memory", 10, Some(filter)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "Memory A");
    }

    #[test]
    fn test_recall_normalizes_unicode_and_case() {
        let store = setup();
        let agent_id = AgentId::new();
        store
            .remember(
                agent_id,
                "Ａlexander THE Great could tame Bucephalus",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();

        let results = store
            .recall(
                "alexander the great",
                5,
                Some(MemoryFilter::agent(agent_id)),
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].content.contains("Bucephalus"));
    }

    #[test]
    fn test_forget() {
        let store = setup();
        let agent_id = AgentId::new();
        let id = store
            .remember(
                agent_id,
                "To forget",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();
        store.forget(id).unwrap();
        let results = store.recall("To forget", 10, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_remember_with_embedding() {
        let store = setup();
        let agent_id = AgentId::new();
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        let id = store
            .remember_with_embedding(
                agent_id,
                "Rust is great",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&embedding),
            )
            .unwrap();
        assert_ne!(id.0.to_string(), "");
    }

    #[test]
    fn test_vector_recall_ranking() {
        let store = setup();
        let agent_id = AgentId::new();

        // Store 3 memories with embeddings pointing in different directions
        let emb_rust = vec![0.9, 0.1, 0.0, 0.0]; // "Rust" direction
        let emb_python = vec![0.0, 0.0, 0.9, 0.1]; // "Python" direction
        let emb_mixed = vec![0.5, 0.5, 0.0, 0.0]; // mixed

        store
            .remember_with_embedding(
                agent_id,
                "Rust is a systems language",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&emb_rust),
            )
            .unwrap();
        store
            .remember_with_embedding(
                agent_id,
                "Python is interpreted",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&emb_python),
            )
            .unwrap();
        store
            .remember_with_embedding(
                agent_id,
                "Both are popular",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&emb_mixed),
            )
            .unwrap();

        // Query with a "Rust"-like embedding
        let query_emb = vec![0.85, 0.15, 0.0, 0.0];
        let results = store
            .recall_with_embedding("", 3, None, Some(&query_emb))
            .unwrap();

        assert_eq!(results.len(), 3);
        // Rust memory should be first (highest cosine similarity)
        assert!(results[0].content.contains("Rust"));
        // Python memory should be last (lowest similarity)
        assert!(results[2].content.contains("Python"));
    }

    #[test]
    fn test_lookup_retrieves_older_embedded_memory_beyond_recent_window() {
        let store = setup();
        let agent_id = AgentId::new();
        let target_embedding = vec![1.0, 0.0, 0.0, 0.0];
        let target_content = "rare rust ownership lifetime pattern";

        store
            .remember_with_embedding(
                agent_id,
                target_content,
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&target_embedding),
            )
            .unwrap();

        for i in 0..150 {
            let filler_embedding = if i % 2 == 0 {
                vec![0.0, 1.0, 0.0, 0.0]
            } else {
                vec![0.0, 0.0, 1.0, 0.0]
            };
            store
                .remember_with_embedding(
                    agent_id,
                    &format!("filler memory {i} unrelated topic"),
                    MemorySource::Conversation,
                    "episodic",
                    HashMap::new(),
                    Some(&filler_embedding),
                )
                .unwrap();
        }

        let results = store
            .recall_with_embedding(
                target_content,
                5,
                Some(MemoryFilter::agent(agent_id)),
                Some(&target_embedding),
            )
            .unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].content, target_content);
    }

    #[test]
    fn test_update_embedding() {
        let store = setup();
        let agent_id = AgentId::new();
        let id = store
            .remember(
                agent_id,
                "No embedding yet",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();

        // Update with embedding
        let emb = vec![1.0, 0.0, 0.0];
        store.update_embedding(id, &emb).unwrap();

        // Verify the embedding is stored by doing vector recall
        let query_emb = vec![1.0, 0.0, 0.0];
        let results = store
            .recall_with_embedding("", 10, None, Some(&query_emb))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].embedding.is_some());
        assert_eq!(results[0].embedding.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_mixed_embedded_and_non_embedded() {
        let store = setup();
        let agent_id = AgentId::new();

        // One memory with embedding, one without
        store
            .remember_with_embedding(
                agent_id,
                "Has embedding",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
                Some(&[1.0, 0.0]),
            )
            .unwrap();
        store
            .remember(
                agent_id,
                "No embedding",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();

        // Vector recall should rank embedded memory higher
        let results = store
            .recall_with_embedding("", 10, None, Some(&[1.0, 0.0]))
            .unwrap();
        assert_eq!(results.len(), 2);
        // Embedded memory should rank first
        assert_eq!(results[0].content, "Has embedding");
    }

    #[test]
    fn test_recall_scored_with_embedding_exposes_score_breakdown() {
        let store = setup();
        let agent_id = AgentId::new();

        store
            .remember(
                agent_id,
                "Alexander the Great defeated Darius",
                MemorySource::Conversation,
                "episodic",
                HashMap::new(),
            )
            .unwrap();

        let hits = store
            .recall_scored_with_embedding(
                "alexander the great",
                3,
                Some(MemoryFilter::agent(agent_id)),
                None,
            )
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].fragment.content,
            "Alexander the Great defeated Darius"
        );
        assert!(hits[0].score > 0.0);
        assert!(hits[0].gate > 0.0);
        assert!(hits[0].lexical_confidence > 0.0);
        assert!(hits[0].lexical_hits >= 1);
    }
}
