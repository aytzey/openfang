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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    segment: SalesSegment,
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
            title: if segment.is_b2c() {
                "Marka / niche briefini gir".to_string()
            } else {
                "Sirket briefini gir".to_string()
            },
            done: has_brief,
        },
        SalesOnboardingStep {
            key: "profile".to_string(),
            title: if segment.is_b2c() {
                "B2C ICP profilini dogrula ve kaydet".to_string()
            } else {
                "Profili dogrula ve kaydet".to_string()
            },
            done: profile_ready,
        },
        SalesOnboardingStep {
            key: "first_run".to_string(),
            title: if segment.is_b2c() {
                "Ilk social discovery run'ini tamamla".to_string()
            } else {
                "Ilk prospecting run'ini tamamla".to_string()
            },
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
    segment: SalesSegment,
    brief: &str,
    persist: bool,
) -> Result<(SalesProfile, &'static str, Vec<String>), String> {
    let base = match engine.get_profile(segment) {
        Ok(Some(p)) => p,
        Ok(None) => SalesProfile::default(),
        Err(e) => return Err(e),
    };
    let mut warnings = Vec::<String>::new();
    let (profile, source) = match llm_autofill_profile(state, brief, segment).await {
        Ok(draft) => (merge_profile(base, draft, brief, segment), "llm"),
        Err(e) => {
            warnings.push(e);
            (heuristic_profile_from_brief(base, brief, segment), "heuristic")
        }
    };

    if persist {
        engine.upsert_profile(segment, &profile)?;
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

fn detect_industry(brief: &str, segment: SalesSegment) -> Option<String> {
    let b = brief.to_lowercase();
    let consumer_map = [
        ("giyim", "Fashion & Apparel"),
        ("moda", "Fashion & Apparel"),
        ("fashion", "Fashion & Apparel"),
        ("beauty", "Beauty & Personal Care"),
        ("kozmetik", "Beauty & Personal Care"),
        ("skincare", "Beauty & Personal Care"),
        ("makyaj", "Beauty & Personal Care"),
        ("fitness", "Fitness & Wellness"),
        ("wellness", "Fitness & Wellness"),
        ("spor salonu", "Fitness & Wellness"),
        ("gym", "Fitness & Wellness"),
        ("cafe", "Food & Beverage"),
        ("coffee", "Food & Beverage"),
        ("restaurant", "Food & Beverage"),
        ("restoran", "Food & Beverage"),
        ("food", "Food & Beverage"),
        ("anne", "Parenting & Family"),
        ("bebek", "Parenting & Family"),
        ("parent", "Parenting & Family"),
        ("ev dekorasyon", "Home & Lifestyle"),
        ("home decor", "Home & Lifestyle"),
        ("lifestyle", "Home & Lifestyle"),
        ("takı", "Accessories & Jewelry"),
        ("taki", "Accessories & Jewelry"),
        ("jewelry", "Accessories & Jewelry"),
        ("travel", "Travel & Experiences"),
        ("seyahat", "Travel & Experiences"),
        ("local", "Local Consumer"),
        ("mahalle", "Local Consumer"),
    ];
    if segment.is_b2c() {
        for (needle, value) in consumer_map {
            if b.contains(needle) {
                return Some(value.to_string());
            }
        }
    }
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
        ("enerji", "Energy"),
        ("energy", "Energy"),
        ("lojistik", "Logistics"),
        ("supply chain", "Logistics"),
        ("logistics", "Logistics"),
        ("telekom", "Telecommunications"),
        ("telecom", "Telecommunications"),
        ("cyber", "Cybersecurity"),
        ("security", "Cybersecurity"),
        ("fintech", "Fintech"),
        ("bank", "Financial Services"),
        ("e-ticaret", "E-commerce"),
        ("eticaret", "E-commerce"),
        ("e-commerce", "E-commerce"),
        ("ecommerce", "E-commerce"),
        ("otomotiv", "Automotive"),
        ("automotive", "Automotive"),
        ("yedek parça", "Automotive"),
        ("yedek parca", "Automotive"),
        ("fuarcılık", "Events & Exhibitions"),
        ("fuarcilik", "Events & Exhibitions"),
        ("fuar", "Events & Exhibitions"),
        ("kongre", "Events & Exhibitions"),
        ("exhibition", "Events & Exhibitions"),
        ("events", "Events & Exhibitions"),
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
    if segment.is_b2c() {
        return Some("Local Consumer".to_string());
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

fn merge_profile(
    base: SalesProfile,
    draft: SalesProfileDraft,
    brief: &str,
    segment: SalesSegment,
) -> SalesProfile {
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
        p.target_industry = detect_industry(brief, segment).unwrap_or_else(|| {
            if segment.is_b2c() {
                "Local Consumer".to_string()
            } else {
                "Technology".to_string()
            }
        });
    }
    if p.target_industry.eq_ignore_ascii_case("technology")
        || p.target_industry.eq_ignore_ascii_case("tech")
    {
        if let Some(specific) = detect_industry(brief, segment) {
            if !specific.eq_ignore_ascii_case("technology") {
                p.target_industry = specific;
            }
        }
    }
    if let Some(v) = cleaned_opt(draft.target_geo) {
        p.target_geo = v;
    } else if p.target_geo.trim().is_empty() {
        p.target_geo = detect_geo(brief).unwrap_or_default();
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

fn heuristic_profile_from_brief(
    base: SalesProfile,
    brief: &str,
    segment: SalesSegment,
) -> SalesProfile {
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
        target_industry: detect_industry(brief, segment),
        target_geo: detect_geo(brief),
        sender_name,
        sender_email: email,
        sender_linkedin: linkedin,
        target_title_policy: Some("ceo_then_founder".to_string()),
        daily_target: Some(if segment.is_b2c() { 40 } else { 20 }),
        daily_send_cap: Some(if segment.is_b2c() { 5 } else { 20 }),
        schedule_hour_local: Some(if segment.is_b2c() { 10 } else { 9 }),
        timezone_mode: Some("local".to_string()),
    };

    merge_profile(base, draft, brief, segment)
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

#[allow(clippy::too_many_arguments)]
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
    // Merge tech stack from site enrichment (TASK-28)
    if !enrichment.tech_stack.is_empty() {
        let mut stack = profile.tech_stack.clone();
        stack.extend(enrichment.tech_stack.iter().cloned());
        stack.sort();
        stack.dedup();
        profile.tech_stack = stack;
    }
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
    // Detect job posting intent from OSINT links (TASK-27)
    let job_signals = detect_job_posting_intent_from_urls(&profile.osint_links);
    for signal in job_signals {
        if !profile.trigger_events.contains(&signal) {
            profile.trigger_events.push(signal);
        }
    }
    // Merge active job posting search signals (TASK-27 search)
    for (text, url, _confidence, _sig_type) in &enrichment.job_posting_signals {
        let signal_text = format!("Job posting: {} ({})", text, url);
        if !profile.trigger_events.contains(&signal_text) {
            profile.trigger_events.push(signal_text);
        }
    }
    // Merge tech stack from search enrichment (TASK-28)
    if !enrichment.tech_stack.is_empty() {
        let mut stack = profile.tech_stack.clone();
        stack.extend(enrichment.tech_stack.iter().cloned());
        stack.sort();
        stack.dedup();
        profile.tech_stack = stack;
    }
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
