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
        .and_then(|value| normalize_public_profile_url(&value));
    profile.company_linkedin_url = profile
        .company_linkedin_url
        .clone()
        .and_then(|value| {
            normalize_company_linkedin_url(&value).or_else(|| normalize_public_profile_url(&value))
        });
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
        || primary_linkedin_url.and_then(normalize_public_profile_url).is_some()
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
        public_profile_channel(primary_linkedin_url).is_some(),
    ) {
        (true, true) => match public_profile_channel(primary_linkedin_url) {
            Some("social_dm") => "Channels: email + social DM".to_string(),
            _ => "Channels: email + LinkedIn".to_string(),
        },
        (true, false) => "Channels: email".to_string(),
        (false, true) => match public_profile_channel(primary_linkedin_url) {
            Some("social_dm") => "Channels: social DM".to_string(),
            _ => "Channels: LinkedIn".to_string(),
        },
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
    let public_channel = public_profile_channel(primary_linkedin_url);
    match (has_email, public_channel) {
        (true, Some(_)) => "either".to_string(),
        (true, None) => "email".to_string(),
        (false, Some(channel)) => channel.to_string(),
        (false, None) => "research".to_string(),
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

