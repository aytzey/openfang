//! Sales engine API and persistence.
//!
//! Focused prospecting workflow:
//! 1. Persist ICP/product profile
//! 2. Discover candidate customer accounts from public sources
//! 3. Build persistent prospect dossiers with deterministic memory reuse
//! 4. Upgrade the best dossiers into outreach-ready leads + approval drafts
//! 5. Send on manual approval (email + LinkedIn operator assist)

include!("sales/shared.rs");
include!("sales/engine.rs");
include!("sales/prospects.rs");
include!("sales/directories.rs");
include!("sales/strategy.rs");
include!("sales/enrichment.rs");
include!("sales/onboarding.rs");
include!("sales/discovery.rs");
include!("sales/llm.rs");
include!("sales/http.rs");
include!("sales/tests.rs");
