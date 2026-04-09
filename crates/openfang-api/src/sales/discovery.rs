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

fn b2c_geo_search_terms(target_geo: &str) -> Vec<String> {
    let geo = target_geo.trim();
    if geo.is_empty() {
        return vec!["local".to_string()];
    }
    if geo_is_turkey(geo) {
        return vec![
            "Türkiye".to_string(),
            "Istanbul".to_string(),
            "Ankara".to_string(),
            "Izmir".to_string(),
        ];
    }
    dedupe_strings(vec![geo.to_string(), geo.to_uppercase()])
}

fn b2c_relevance_terms(profile: &SalesProfile) -> Vec<String> {
    dedupe_strings(
        profile
            .target_industry
            .split([',', '/', ';', '|'])
            .chain(profile.product_name.split([',', '/', ';', '|']))
            .chain(profile.product_description.split([',', ';', '|', '.']))
            .map(|value| value.trim().to_lowercase())
            .filter(|value| value.len() >= 3)
            .collect(),
    )
}

fn b2c_local_market_markers() -> &'static [&'static str] {
    &[
        "local",
        "same day",
        "pickup",
        "delivery",
        "neighborhood",
        "boutique",
        "store",
        "shop",
        "mahalle",
        "yerel",
        "teslimat",
        "magaza",
        "butik",
        "istanbul",
        "ankara",
        "izmir",
        "kadikoy",
        "besiktas",
        "sisli",
        "beyoglu",
    ]
}

fn extract_b2c_locality_hint(blob: &str, target_geo: &str) -> Option<String> {
    let lower = blob.to_lowercase();
    for marker in b2c_local_market_markers() {
        if lower.contains(marker) {
            return Some(marker.to_string());
        }
    }
    if !target_geo.trim().is_empty() && lower.contains(&target_geo.trim().to_lowercase()) {
        return Some(target_geo.trim().to_string());
    }
    None
}

fn extract_b2c_local_market_signals(blob: &str) -> Vec<String> {
    let lower = blob.to_lowercase();
    let mut signals = Vec::new();
    for marker in b2c_local_market_markers() {
        if lower.contains(marker) {
            signals.push(format!("Local market signal: {marker}"));
        }
    }
    dedupe_strings(signals)
}

fn heuristic_b2c_query_plan(profile: &SalesProfile) -> LeadQueryPlanDraft {
    let geo_terms = b2c_geo_search_terms(&profile.target_geo);
    let geo = geo_terms
        .first()
        .cloned()
        .unwrap_or_else(|| "local".to_string());
    let niche = if profile.target_industry.trim().is_empty() {
        "local consumer".to_string()
    } else {
        profile.target_industry.clone()
    };
    let product = if profile.product_name.trim().is_empty() {
        "consumer brand".to_string()
    } else {
        profile.product_name.clone()
    };

    LeadQueryPlanDraft {
        discovery_queries: dedupe_strings(vec![
            format!("site:instagram.com \"{niche}\" \"{geo}\""),
            format!("site:tiktok.com/@ \"{niche}\" \"{geo}\""),
            format!("site:instagram.com \"{product}\" \"{geo}\""),
            format!("site:tiktok.com/@ \"{product}\" \"{geo}\""),
            format!("\"{niche}\" instagram {geo}"),
            format!("\"{niche}\" tiktok {geo}"),
        ]),
        must_include_keywords: expand_keywords(vec![
            niche,
            product,
            "instagram".to_string(),
            "tiktok".to_string(),
            "local".to_string(),
        ]),
        exclude_keywords: expand_keywords(vec![
            "agency".to_string(),
            "consulting".to_string(),
            "jobs".to_string(),
            "news".to_string(),
            "directory".to_string(),
        ]),
        contact_titles: vec!["creator".to_string(), "shop owner".to_string()],
    }
}

fn b2c_candidate_from_search_entry(
    entry: &SearchEntry,
    profile: &SalesProfile,
) -> Option<B2cDiscoveryCandidate> {
    let blob = format!("{} {} {}", entry.title, entry.snippet, entry.url);
    let lower = blob.to_lowercase();
    let relevance_terms = b2c_relevance_terms(profile);

    for adapter in social_scraper_adapters() {
        let profile_url = adapter.normalize_profile_url(&entry.url)?;
        let handle = adapter.extract_handle(&profile_url)?;
        let key = adapter.profile_key(&profile_url)?;
        let keyword_hits = relevance_terms
            .iter()
            .filter(|term| lower.contains(term.as_str()))
            .count() as i32;
        let locality_hint = extract_b2c_locality_hint(&lower, &profile.target_geo);
        let mut signals = vec![format!("{} public profile", adapter.platform())];
        if keyword_hits > 0 {
            signals.push(format!(
                "Consumer niche match: {}",
                relevance_terms
                    .iter()
                    .filter(|term| lower.contains(term.as_str()))
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(locality) = locality_hint.as_ref() {
            signals.push(format!("Locality hint: {locality}"));
        }
        let local_market_signals = extract_b2c_local_market_signals(&lower);
        signals.extend(local_market_signals.clone());
        let score = 45 + keyword_hits * 6 + (locality_hint.is_some() as i32) * 10;

        let display_name = adapter.extract_display_name(&entry.title, &handle);
        return Some(B2cDiscoveryCandidate {
            key,
            platform: adapter.platform().to_string(),
            handle,
            display_name,
            profile_url,
            score: score.min(100),
            evidence: vec![truncate_text_for_reason(&blob, 220)],
            matched_signals: dedupe_strings(signals),
            bio_hint: if entry.snippet.trim().is_empty() {
                None
            } else {
                Some(truncate_cleaned_text(&entry.snippet, 180))
            },
            locality_hint,
            local_market_signals,
        });
    }
    None
}

fn merge_b2c_candidate(current: &mut B2cDiscoveryCandidate, incoming: B2cDiscoveryCandidate) {
    current.score = current.score.max(incoming.score);
    current.evidence = dedupe_strings(
        current
            .evidence
            .iter()
            .chain(incoming.evidence.iter())
            .cloned()
            .collect(),
    )
    .into_iter()
    .take(4)
    .collect();
    current.matched_signals = dedupe_strings(
        current
            .matched_signals
            .iter()
            .chain(incoming.matched_signals.iter())
            .cloned()
            .collect(),
    );
    current.local_market_signals = dedupe_strings(
        current
            .local_market_signals
            .iter()
            .chain(incoming.local_market_signals.iter())
            .cloned()
            .collect(),
    );
    if current.bio_hint.is_none() {
        current.bio_hint = incoming.bio_hint;
    }
    if current.locality_hint.is_none() {
        current.locality_hint = incoming.locality_hint;
    }
    if current.display_name.trim().is_empty() || current.display_name == current.handle {
        current.display_name = incoming.display_name;
    }
}

fn b2c_candidates_to_checkpoint(candidates: &[B2cDiscoveryCandidate]) -> Vec<DomainCandidate> {
    candidates
        .iter()
        .map(|candidate| DomainCandidate {
            domain: candidate.key.clone(),
            score: candidate.score,
            evidence: candidate.evidence.clone(),
            matched_keywords: candidate.matched_signals.clone(),
            source_links: vec![candidate.profile_url.clone()],
            phone: None,
        })
        .collect()
}

async fn discover_b2c_social_candidates(
    search_engine: &WebSearchEngine,
    brave_search_engine: Option<&WebSearchEngine>,
    profile: &SalesProfile,
    lead_plan: &LeadQueryPlanDraft,
    max_candidates: usize,
) -> Vec<B2cDiscoveryCandidate> {
    let queries = if lead_plan.discovery_queries.is_empty() {
        heuristic_b2c_query_plan(profile).discovery_queries
    } else {
        lead_plan.discovery_queries.clone()
    };
    let mut merged = HashMap::<String, B2cDiscoveryCandidate>::new();
    let mut outputs = run_sales_search_batch(
        search_engine,
        &queries,
        8,
        Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
    )
    .await;
    if outputs.iter().all(|(_, result)| result.is_err()) {
        if let Some(brave) = brave_search_engine {
            outputs = run_sales_search_batch(
                brave,
                &queries,
                8,
                Duration::from_secs(SALES_DISCOVERY_SEARCH_TIMEOUT_SECS),
            )
            .await;
        }
    }

    for (_, result) in outputs {
        let Ok(output) = result else {
            continue;
        };
        for entry in parse_search_entries(&output) {
            let Some(candidate) = b2c_candidate_from_search_entry(&entry, profile) else {
                continue;
            };
            if let Some(existing) = merged.get_mut(&candidate.key) {
                merge_b2c_candidate(existing, candidate);
            } else {
                merged.insert(candidate.key.clone(), candidate);
            }
        }
    }

    let mut candidates = merged.into_values().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.key.cmp(&right.key))
    });
    candidates.truncate(max_candidates);
    candidates
}

fn build_b2c_local_market_queries(
    candidate: &B2cDiscoveryCandidate,
    profile: &SalesProfile,
) -> Vec<String> {
    let query_name = if candidate.display_name.trim().is_empty() {
        candidate.handle.clone()
    } else {
        candidate.display_name.clone()
    };
    dedupe_strings(vec![
        format!("\"{query_name}\" \"{}\" local", profile.target_geo),
        format!("\"{query_name}\" \"{}\" delivery", profile.target_geo),
        format!("\"{query_name}\" \"{}\" boutique", profile.target_geo),
    ])
}

fn apply_b2c_local_market_search_output(
    candidate: &mut B2cDiscoveryCandidate,
    output: &str,
    target_geo: &str,
) {
    let blob = output.to_lowercase();
    let signals = extract_b2c_local_market_signals(&blob);
    if !signals.is_empty() {
        candidate.local_market_signals = dedupe_strings(
            candidate
                .local_market_signals
                .iter()
                .chain(signals.iter())
                .cloned()
                .collect(),
        );
        candidate.score = (candidate.score + 8).min(100);
    }
    if candidate.locality_hint.is_none() {
        candidate.locality_hint = extract_b2c_locality_hint(&blob, target_geo);
    }
}

async fn enrich_b2c_candidates_with_local_market(
    search_engine: &WebSearchEngine,
    brave_search_engine: Option<&WebSearchEngine>,
    profile: &SalesProfile,
    mut candidates: Vec<B2cDiscoveryCandidate>,
) -> Vec<B2cDiscoveryCandidate> {
    if candidates.is_empty() {
        return candidates;
    }
    let client = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        )
        .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
        .build()
        .ok();

    for candidate in candidates.iter_mut().take(12) {
        if let Some(client) = client.as_ref() {
            if let Some(html) =
                fetch_html_page(client, &candidate.profile_url, SITE_PAGE_FETCH_TIMEOUT_MS).await
            {
                for adapter in social_scraper_adapters() {
                    if adapter.normalize_profile_url(&candidate.profile_url).is_some() {
                        if candidate.bio_hint.is_none() {
                            candidate.bio_hint = adapter.extract_bio_hint(&html);
                        }
                        break;
                    }
                }
            }
        }

        let queries = build_b2c_local_market_queries(candidate, profile);
        let mut outputs = run_sales_search_batch(
            search_engine,
            &queries,
            4,
            Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
        )
        .await
        .into_iter()
        .filter_map(|(_, result)| result.ok())
        .collect::<Vec<_>>();
        if outputs.is_empty() {
            if let Some(brave) = brave_search_engine {
                outputs = run_sales_search_batch(
                    brave,
                    &queries,
                    4,
                    Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
                )
                .await
                .into_iter()
                .filter_map(|(_, result)| result.ok())
                .collect::<Vec<_>>();
            }
        }
        for output in outputs {
            apply_b2c_local_market_search_output(candidate, &output, &profile.target_geo);
        }
        candidate.matched_signals = dedupe_strings(
            candidate
                .matched_signals
                .iter()
                .chain(candidate.local_market_signals.iter())
                .cloned()
                .collect(),
        );
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.key.cmp(&right.key))
    });
    candidates
}

fn build_b2c_prospect_profiles(
    run_id: &str,
    sales_profile: &SalesProfile,
    candidates: Vec<B2cDiscoveryCandidate>,
) -> Vec<SalesProspectProfile> {
    let now = Utc::now().to_rfc3339();
    let mut profiles = candidates
        .into_iter()
        .map(|candidate| {
            let mut matched_signals = candidate.matched_signals.clone();
            if let Some(locality) = candidate.locality_hint.as_ref() {
                matched_signals.push(format!("Local market focus: {locality}"));
            }
            let summary = truncate_cleaned_text(
                &format!(
                    "{} discovered via {}. {} {}",
                    if candidate.display_name.trim().is_empty() {
                        candidate.handle.clone()
                    } else {
                        candidate.display_name.clone()
                    },
                    candidate.platform,
                    candidate
                        .bio_hint
                        .clone()
                        .unwrap_or_else(|| "Public social profile with consumer-interest signals.".to_string()),
                    if candidate.local_market_signals.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "Local signals: {}.",
                            candidate.local_market_signals.join("; ")
                        )
                    }
                ),
                260,
            );

            let profile_url = candidate.profile_url.clone();
            SalesProspectProfile {
                id: candidate.key.clone(),
                run_id: run_id.to_string(),
                company: if candidate.display_name.trim().is_empty() {
                    candidate.handle.clone()
                } else {
                    candidate.display_name.clone()
                },
                website: profile_url.clone(),
                company_domain: candidate.key.clone(),
                fit_score: candidate.score,
                profile_status: "contact_ready".to_string(),
                summary,
                matched_signals: dedupe_strings(matched_signals),
                primary_contact_name: Some(if candidate.display_name.trim().is_empty() {
                    candidate.handle.clone()
                } else {
                    candidate.display_name
                }),
                primary_contact_title: Some(format!("{} profile", candidate.platform)),
                primary_email: None,
                primary_linkedin_url: Some(profile_url.clone()),
                company_linkedin_url: None,
                osint_links: vec![profile_url],
                contact_count: 1,
                source_count: 1 + candidate.local_market_signals.len() as u32,
                buyer_roles: vec!["Individual consumer".to_string(), "Social shopper".to_string()],
                pain_points: vec![
                    format!("Needs stronger visibility for {}", sales_profile.product_name),
                    "Responds to local relevance, convenience, and trend signals".to_string(),
                ],
                trigger_events: if candidate.local_market_signals.is_empty() {
                    vec!["Public social activity suggests active consumer interest".to_string()]
                } else {
                    candidate.local_market_signals
                },
                recommended_channel: "social_dm".to_string(),
                outreach_angle: "Use the visible niche, profile bio, and local-market cues for first-touch personalization.".to_string(),
                research_status: "social_enriched".to_string(),
                research_confidence: 0.62,
                tech_stack: Vec::new(),
                created_at: now.clone(),
                updated_at: now.clone(),
            }
        })
        .collect::<Vec<_>>();
    sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
    profiles
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
