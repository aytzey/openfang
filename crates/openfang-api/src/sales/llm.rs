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

async fn llm_autofill_profile(
    _state: &AppState,
    brief: &str,
    segment: SalesSegment,
) -> Result<SalesProfileDraft, String> {
    let driver = build_sales_llm_driver(&_state.kernel.config.home_dir).await?;

    let prompt = if segment.is_b2c() {
        format!(
            "Extract a high-quality B2C lead discovery profile from the brief.\n\
             Return strict JSON only (no markdown/prose) with exact keys:\n\
             product_name, product_description, target_industry, target_geo, sender_name, sender_email, sender_linkedin,\n\
             target_title_policy, daily_target, daily_send_cap, schedule_hour_local, timezone_mode.\n\
             Rules:\n\
             - target_industry must describe the consumer niche or local market (for example Fashion & Apparel, Beauty & Personal Care, Local Consumer)\n\
             - emphasize public social discovery on Instagram/TikTok and local market relevance when the brief suggests B2C\n\
             - product_description must be concise (max 450 chars), value-focused, consumer-oriented\n\
             - infer sender_email/sender_linkedin from brief if present\n\
             - infer geo from language/content (TR/EU/US) when possible\n\
             - target_title_policy can stay ceo_then_founder when unknown\n\
             - numeric defaults: daily_target=40, daily_send_cap=5, schedule_hour_local=10\n\
             - timezone_mode='local' unless brief clearly says otherwise\n\
             Unknown values can be empty string, but avoid empty target_industry.\n\n\
             Brief:\n{brief}"
        )
    } else {
        format!(
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
        )
    };

    let req = CompletionRequest {
        model: SALES_LLM_MODEL.to_string(),
        messages: vec![LlmMessage::user(prompt)],
        tools: vec![],
        max_tokens: 700,
        temperature: 0.1,
        system: Some(if segment.is_b2c() {
            "You are a B2C growth analyst. Extract precise consumer niche and local-market targeting fields from noisy briefs. Output strict valid JSON only.".to_string()
        } else {
            "You are a B2B sales operations analyst. Extract precise ICP/profile fields from noisy long briefs. Output strict valid JSON only.".to_string()
        }),
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

