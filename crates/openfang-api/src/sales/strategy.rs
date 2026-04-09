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
                let Some(candidate) =
                    normalize_free_candidate_gateway(source[positions[idx]].clone())
                else {
                    positions[idx] += 1;
                    continue;
                };
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

fn source_health_key(source: &str) -> &'static str {
    match source {
        "TMB members directory" => "directory_tmb",
        "EUD members page" => "directory_eud",
        "ASMUD members page" => "directory_asmud",
        "Platformder rehber" => "directory_platformder",
        "MIB members page" => "directory_mib",
        "IMDER member detail" => "directory_imder",
        "ISDER member detail" => "directory_isder",
        "THBB yazismali uyeler" => "directory_thbb",
        "EDER members page" => "directory_eder",
        "LojiDer members page" => "directory_lojider",
        "TFYD members page" => "directory_tfyd",
        "OSS members page" => "directory_oss",
        "IDA members page" => "directory_ida",
        "TESID members page" => "directory_tesid",
        "TUDIS members page" => "directory_tudis",
        "EMSAD members page" => "directory_emsad",
        "TGSD members page" => "directory_tgsd",
        "ARED members page" => "directory_ared",
        "TODEB members page" => "directory_todeb",
        _ => "directory_unknown",
    }
}

fn expected_source_counts_for_profile(profile: &SalesProfile) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    if !geo_is_turkey(&profile.target_geo) {
        return out;
    }
    if profile_targets_field_ops(profile) {
        for key in [
            "directory_tmb",
            "directory_asmud",
            "directory_platformder",
            "directory_mib",
            "directory_imder",
            "directory_isder",
            "directory_thbb",
        ] {
            out.insert(key.to_string(), 0);
        }
    }
    if profile_targets_energy(profile) {
        out.insert("directory_eud".to_string(), 0);
    }
    if profile_targets_digital_commerce(profile) {
        out.insert("directory_eder".to_string(), 0);
    }
    if profile_targets_logistics(profile) {
        out.insert("directory_lojider".to_string(), 0);
    }
    if profile_targets_events_exhibitions(profile) {
        out.insert("directory_tfyd".to_string(), 0);
    }
    if profile_targets_automotive(profile) {
        out.insert("directory_oss".to_string(), 0);
    }
    if profile_targets_pr_communications(profile) {
        out.insert("directory_ida".to_string(), 0);
    }
    if profile_targets_electronics(profile) {
        out.insert("directory_tesid".to_string(), 0);
    }
    if profile_targets_leather(profile) {
        out.insert("directory_tudis".to_string(), 0);
    }
    if profile_targets_electromechanical(profile) {
        out.insert("directory_emsad".to_string(), 0);
    }
    if profile_targets_textile_apparel(profile) {
        out.insert("directory_tgsd".to_string(), 0);
    }
    if profile_targets_advertising_signage(profile) {
        out.insert("directory_ared".to_string(), 0);
    }
    if profile_targets_fintech_payments(profile) {
        out.insert("directory_todeb".to_string(), 0);
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

fn seed_contains_any(seed: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| seed.contains(needle))
}

fn profile_targets_digital_commerce(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "e-commerce",
            "ecommerce",
            "e ticaret",
            "eticaret",
            "marketplace",
            "pazaryeri",
            "online store",
            "web shop",
            "shopping cart",
            "checkout",
            "merchant",
            "order management",
            "digital commerce",
        ],
    )
}

fn profile_targets_fintech_payments(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "payment",
            "payments",
            "ödeme",
            "odeme",
            "electronic money",
            "e-money",
            "wallet",
            "digital wallet",
            "fintech",
            "sanal pos",
            "pos",
            "acquiring",
            "issuer",
            "money transfer",
            "remittance",
            "open banking",
        ],
    )
}

fn profile_targets_logistics(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "logistics",
            "lojistik",
            "freight",
            "forwarding",
            "warehouse",
            "warehousing",
            "depo",
            "antrepo",
            "shipping",
            "cargo",
            "nakliye",
            "gumruk",
            "gümrük",
            "supply chain",
        ],
    )
}

fn profile_targets_electronics(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "electronics",
            "elektronik",
            "telecom",
            "telekom",
            "embedded",
            "pcb",
            "hardware",
            "iot",
            "haberleşme",
            "savunma elektroni",
            "electronic manufacturing",
        ],
    )
}

fn profile_targets_electromechanical(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "electromechanical",
            "elektromekanik",
            "transformer",
            "switchgear",
            "substation",
            "medium voltage",
            "high voltage",
            "power distribution",
            "energy equipment",
            "electrical equipment",
            "kablo",
            "cable",
            "pano",
            "industrial automation",
            "busbar",
        ],
    )
}

fn profile_targets_automotive(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "automotive",
            "otomotiv",
            "aftermarket",
            "auto parts",
            "spare parts",
            "yedek parca",
            "yedek parça",
            "oem",
            "tier 1",
            "tier1",
        ],
    )
}

fn profile_targets_textile_apparel(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "textile",
            "tekstil",
            "apparel",
            "garment",
            "ready-to-wear",
            "ready wear",
            "hazır giyim",
            "hazir giyim",
            "konfeksiyon",
            "giyim",
            "fashion",
            "denim",
            "woven",
            "knitwear",
            "örme",
            "orme",
        ],
    )
}

fn profile_targets_leather(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "leather",
            "deri",
            "tannery",
            "tabakhane",
            "hide",
            "nubuk",
            "suede",
            "saraciye",
        ],
    )
}

fn profile_targets_pr_communications(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "public relations",
            "pr agency",
            "communication agency",
            "communications",
            "communication",
            "kurumsal iletişim",
            "kurumsal iletisim",
            "iletişim danışmanlığı",
            "iletisim danismanligi",
            "halkla ilişkiler",
            "halkla iliskiler",
            "media relations",
            "brand communication",
            "reputation management",
        ],
    )
}

fn profile_targets_advertising_signage(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "signage",
            "digital signage",
            "outdoor advertising",
            "open-air advertising",
            "açıkhava",
            "acikhava",
            "endüstriyel reklam",
            "endustriyel reklam",
            "display",
            "point of sale",
            "baskı",
            "baski",
            "serigrafi",
            "wide format",
            "reklam",
        ],
    )
}

fn profile_targets_events_exhibitions(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed_contains_any(
        &seed,
        &[
            "events & exhibitions",
            "event",
            "events",
            "exhibition",
            "expo",
            "fair organization",
            "organizer",
            "organizasyon",
            "etkinlik",
            "kongre",
            "fuar",
            "fuarcilik",
            "fuarcılık",
        ],
    )
}

fn llm_candidate_relevance_prompt_context(profile: &SalesProfile) -> String {
    if profile_targets_field_ops(profile) {
        "We sell to companies with field/on-site operations (construction, maintenance, facility management, technical service, dispatch, infrastructure, equipment, etc.).\n\
         For each company, assess:\n\
         - Is it a real company in our target industry with meaningful field or operational teams?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_fintech_payments(profile) {
        "We sell to payments, fintech, wallets, money-movement, and electronic-money companies.\n\
         For each company, assess:\n\
         - Is it a real payment, wallet, electronic-money, or fintech operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_digital_commerce(profile) {
        "We sell to companies operating in digital commerce, marketplace, online retail, or e-commerce infrastructure.\n\
         For each company, assess:\n\
         - Is it a real company in e-commerce, online retail, payment/checkout, or commerce software infrastructure?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_logistics(profile) {
        "We sell to logistics, freight, warehousing, customs, cargo, and supply-chain companies.\n\
         For each company, assess:\n\
         - Is it a real logistics or supply-chain operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_electronics(profile) {
        "We sell to electronics, telecom, embedded systems, and hardware companies.\n\
         For each company, assess:\n\
         - Is it a real electronics or telecom company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_electromechanical(profile) {
        "We sell to electromechanical, transformer, switchgear, cable, and power-distribution equipment companies.\n\
         For each company, assess:\n\
         - Is it a real electrical equipment or electromechanical company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_automotive(profile) {
        "We sell to automotive, aftermarket, spare-parts, and vehicle supply-chain companies.\n\
         For each company, assess:\n\
         - Is it a real automotive or automotive-aftermarket company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_textile_apparel(profile) {
        "We sell to textile, apparel, garment, denim, and ready-to-wear companies.\n\
         For each company, assess:\n\
         - Is it a real textile, apparel, or fashion manufacturing company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_leather(profile) {
        "We sell to leather, tannery, hide-processing, and leather-goods companies.\n\
         For each company, assess:\n\
         - Is it a real leather or tannery company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_pr_communications(profile) {
        "We sell to PR, communication, media-relations, and brand-communication agencies.\n\
         For each company, assess:\n\
         - Is it a real communication or PR agency in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_advertising_signage(profile) {
        "We sell to signage, display, industrial-printing, and outdoor-advertising companies.\n\
         For each company, assess:\n\
         - Is it a real signage, display, industrial-printing, or outdoor-advertising company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_events_exhibitions(profile) {
        "We sell to exhibition, fair, congress, organizer, and event-services companies.\n\
         For each company, assess:\n\
         - Is it a real event, exhibition, fair, or congress operator/vendor in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else if profile_targets_energy(profile) {
        "We sell to energy, utility, electricity generation, and related infrastructure companies.\n\
         For each company, assess:\n\
         - Is it a real energy or utility company in the target geography?\n\
         - Would they plausibly benefit from our product?"
            .to_string()
    } else {
        format!(
            "We sell to B2B companies in {} within {}.\n\
             For each company, assess:\n\
             - Is it a real company in or adjacent to the target industry?\n\
             - Would they plausibly benefit from our product?",
            profile.target_industry, profile.target_geo
        )
    }
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

fn outreach_recipient_name(contact_name: Option<&str>, company: &str, target_geo: &str) -> String {
    if let Some(first_name) = contact_name
        .and_then(normalize_person_name)
        .and_then(|value| value.split_whitespace().next().map(|part| part.to_string()))
    {
        return first_name;
    }
    if geo_is_turkey(target_geo) {
        format!("{} ekibi", company)
    } else {
        format!("{} team", company)
    }
}

fn build_sales_lead_reasons(
    profile: &SalesProfile,
    company: &str,
    matched: &str,
    evidence: &str,
    contact_title: Option<&str>,
) -> Vec<String> {
    let mut reasons = vec![
        format!("ICP fit: {}", truncate_text_for_reason(matched, 140)),
        format!(
            "Public evidence: {}",
            truncate_text_for_reason(evidence, 220)
        ),
    ];
    if let Some(title) = contact_title
        .map(str::trim)
        .filter(|value| !value.is_empty() && !contact_title_is_generic_default(Some(value)))
    {
        reasons.push(format!("Potential buyer role: {title}"));
    }
    reasons.push(format!(
        "Value hypothesis: {} could help {} with {}",
        profile.product_name,
        company,
        truncate_text_for_reason(&profile.product_description, 140)
    ));
    dedupe_strings(reasons)
}

fn build_sales_email_subject(profile: &SalesProfile, company: &str) -> String {
    if geo_is_turkey(&profile.target_geo) {
        format!("{company} icin saha operasyon koordinasyonu")
    } else {
        format!("{company}: field ops coordination")
    }
}

fn build_sales_email_body(
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
    matched: &str,
    evidence: &str,
) -> String {
    let recipient = outreach_recipient_name(contact_name, company, &profile.target_geo);
    let evidence_short = truncate_text_for_reason(evidence, 160);
    let matched_short = truncate_text_for_reason(matched, 90);
    let value_short = truncate_text_for_reason(&profile.product_description, 150);

    if geo_is_turkey(&profile.target_geo) {
        format!(
            "Merhaba {},\n\n{} ile ilgili su sinyali gordum: {}.\n\n{} tarafinda {} operasyonlarinda is atama, takip ve gecikme yonetimi kolayca daginik hale gelebiliyor. {} burada su ise yarayabilir: {}.\n\nUygunsa {} icin 3 maddelik kisa bir operasyon akisi onerisi paylasabilirim.\n\nSelamlar,\n{}",
            recipient,
            company,
            evidence_short,
            company,
            matched_short,
            profile.product_name,
            value_short,
            company,
            profile.sender_name
        )
    } else {
        format!(
            "Hi {},\n\nI came across {} through this public signal: {}.\n\nFor teams running {}, the friction is usually around task ownership, follow-up, and delay recovery across email, phone, and chat. {} could help here: {}.\n\nIf useful, I can send a short 3-point workflow teardown for {}.\n\nBest,\n{}",
            recipient,
            company,
            evidence_short,
            matched_short,
            profile.product_name,
            value_short,
            company,
            profile.sender_name
        )
    }
}

fn build_sales_linkedin_message(
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
    evidence: &str,
) -> String {
    let recipient = outreach_recipient_name(contact_name, company, &profile.target_geo);
    let evidence_short = truncate_text_for_reason(evidence, 110);
    if geo_is_turkey(&profile.target_geo) {
        truncate_cleaned_text(
            &format!(
                "Merhaba {}, {} ile ilgili su sinyali gordum: {}. {} saha ekiplerinde takip ve koordinasyonu toparlamaya yardimci oluyor. Uygunsa kisa bir akis onerisi paylasabilirim.",
                recipient, company, evidence_short, profile.product_name
            ),
            300,
        )
    } else {
        truncate_cleaned_text(
            &format!(
                "Hi {}, noticed {} through this signal: {}. {} helps field teams tighten follow-up and coordination. Happy to share a short workflow teardown if relevant.",
                recipient, company, evidence_short, profile.product_name
            ),
            300,
        )
    }
}

/// Stage 1: Determine message strategy from thesis + persona context.
/// Returns pain angle, trigger reference, CTA type, tone, and language.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MessageStrategy {
    pain_angle: String,
    trigger_evidence: String,
    cta_type: String,
    tone: String,
    language: String,
}

fn generate_message_strategy(
    profile: &SalesProfile,
    _company: &str,
    _contact_name: Option<&str>,
    evidence: &str,
    matched: &str,
) -> MessageStrategy {
    let language = if geo_is_turkey(&profile.target_geo) {
        "tr"
    } else {
        "en"
    };
    let cta = if language == "tr" {
        "Uygunsa size 2 sayfalik kisa bir operasyon analizi paylasabilirim."
    } else {
        "Happy to share a brief 2-page operational analysis if helpful."
    };
    MessageStrategy {
        pain_angle: matched.to_string(),
        trigger_evidence: evidence.to_string(),
        cta_type: cta.to_string(),
        tone: "professional_warm".to_string(),
        language: language.to_string(),
    }
}

/// Stage 2: Build outreach message copy. Currently template-based; designed for
/// future LLM generation when evidence bundle + thesis are available.
/// Evidence-bound: refuses to generate if no evidence is provided.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MessageCopy {
    subject: String,
    body: String,
    linkedin_copy: String,
    claims: Vec<String>,
    evidence_ids: Vec<String>,
}

fn generate_message_copy(
    strategy: &MessageStrategy,
    profile: &SalesProfile,
    company: &str,
    contact_name: Option<&str>,
) -> Result<MessageCopy, String> {
    if strategy.trigger_evidence.trim().is_empty() && strategy.pain_angle.trim().is_empty() {
        return Err(
            "REFUSED: No evidence or pain angle provided. Message engine requires evidence.".into(),
        );
    }
    let subject = build_sales_email_subject(profile, company);
    let body = build_sales_email_body(
        profile,
        company,
        contact_name,
        &strategy.pain_angle,
        &strategy.trigger_evidence,
    );
    let linkedin =
        build_sales_linkedin_message(profile, company, contact_name, &strategy.trigger_evidence);
    Ok(MessageCopy {
        subject,
        body,
        linkedin_copy: linkedin,
        claims: vec![strategy.pain_angle.clone()],
        evidence_ids: Vec::new(),
    })
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
        tech_stack: Vec::new(),
        job_posting_signals: Vec::new(),
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

