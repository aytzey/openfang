//! Deterministic lexical lookup primitives inspired by conditional memory.
//!
//! This module canonicalizes text into a compressed token space and indexes
//! 2/3-gram hashes with multiple stable hash heads. The resulting lookup slots
//! give the memory substrate a cheap, deterministic candidate-generation path
//! before higher-cost semantic scoring.

use std::collections::HashSet;
use unicode_normalization::UnicodeNormalization;

/// N-gram orders used by the lookup layer.
pub const LOOKUP_NGRAM_ORDERS: [usize; 2] = [2, 3];

/// Seeds for the multi-head stable hash.
const HASH_SEEDS: [u64; 2] = [0xcbf2_9ce4_8422_2325, 0x9e37_79b1_85eb_ca87];

/// A single hashed lookup slot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HashedNgram {
    pub hash: i64,
    pub order: usize,
    pub head: usize,
    pub weight: f32,
}

/// Normalize text into a canonical token space:
/// - NFKC normalization
/// - lowercasing
/// - punctuation/whitespace collapse
pub fn canonical_tokens(input: &str) -> Vec<String> {
    let normalized = input.nfkc().collect::<String>().to_lowercase();
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in normalized.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Extract a deduplicated set of multi-head hashed n-grams for lookup.
pub fn extract_hashed_ngrams(input: &str) -> Vec<HashedNgram> {
    let tokens = canonical_tokens(input);
    extract_hashed_ngrams_from_tokens(&tokens)
}

/// Total query-side weight across slots. Used to normalize lexical confidence.
pub fn total_slot_weight(slots: &[HashedNgram]) -> f32 {
    slots.iter().map(|slot| slot.weight).sum()
}

/// Cheap token overlap used as a fallback gating signal when embeddings are absent.
pub fn token_overlap_score(query_tokens: &[String], candidate_text: &str) -> f32 {
    if query_tokens.is_empty() {
        return 0.0;
    }

    let query: HashSet<&str> = query_tokens.iter().map(|token| token.as_str()).collect();
    let candidate: HashSet<String> = canonical_tokens(candidate_text).into_iter().collect();
    if candidate.is_empty() {
        return 0.0;
    }

    let overlap = query
        .iter()
        .filter(|token| candidate.contains(**token))
        .count();

    overlap as f32 / query.len() as f32
}

fn extract_hashed_ngrams_from_tokens(tokens: &[String]) -> Vec<HashedNgram> {
    let mut slots = Vec::new();
    let mut seen = HashSet::new();

    for order in LOOKUP_NGRAM_ORDERS {
        if tokens.len() < order {
            continue;
        }

        for window in tokens.windows(order) {
            let joined = window.join("\u{001f}");
            for (head, seed) in HASH_SEEDS.iter().enumerate() {
                let hash = stable_hash(&joined, *seed);
                if seen.insert((hash, order, head)) {
                    slots.push(HashedNgram {
                        hash,
                        order,
                        head,
                        weight: order_weight(order),
                    });
                }
            }
        }
    }

    slots
}

fn stable_hash(value: &str, seed: u64) -> i64 {
    let mut hash = seed;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
        hash ^= hash >> 32;
    }
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

fn order_weight(order: usize) -> f32 {
    match order {
        3 => 3.0,
        2 => 1.5,
        _ => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_tokens_nfkc_and_lowercase() {
        let tokens = canonical_tokens("Ａlexander\tTHE Great");
        assert_eq!(tokens, vec!["alexander", "the", "great"]);
    }

    #[test]
    fn test_extract_hashed_ngrams_for_2_and_3_grams() {
        let slots = extract_hashed_ngrams("alexander the great");
        assert_eq!(slots.len(), 6);
        assert!(slots.iter().any(|slot| slot.order == 2));
        assert!(slots.iter().any(|slot| slot.order == 3));
    }

    #[test]
    fn test_token_overlap_score() {
        let query = canonical_tokens("rust ownership borrowing");
        let score = token_overlap_score(&query, "Borrowing rules in Rust are strict");
        assert!(score > 0.5);
    }
}
