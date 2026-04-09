fn build_company_osint_queries(
    company: &str,
    domain: &str,
    title_policy: &str,
    target_geo: &str,
    primary_contact_name: Option<&str>,
) -> Vec<String> {
    let executive_terms = if title_policy == "ceo_only" {
        "\"Chief Executive Officer\" OR CEO"
    } else {
        "CEO OR Founder OR COO OR \"Head of Operations\""
    };
    let mut queries = vec![
        format!("site:linkedin.com/company \"{}\" \"{}\"", company, domain),
        format!(
            "\"{}\" \"{}\" (LinkedIn OR leadership OR management OR executive team OR hakkimizda OR yonetim)",
            company, domain
        ),
        format!(
            "site:{} (leadership OR management OR executive team OR team OR about OR contact OR hakkimizda OR yonetim OR iletisim)",
            domain
        ),
        format!(
            "\"{}\" \"{}\" ({}) (LinkedIn OR leadership OR executive team)",
            company, domain, executive_terms
        ),
    ];
    if !target_geo.trim().is_empty() {
        queries.push(format!(
            "\"{}\" \"{}\" {} (LinkedIn OR leadership OR management)",
            company, domain, target_geo
        ));
    }
    if let Some(primary_contact_name) = primary_contact_name
        .map(str::trim)
        .filter(|value| !value.is_empty() && !contact_name_is_placeholder(Some(value)))
    {
        queries.push(format!(
            "\"{}\" \"{}\" site:linkedin.com/in",
            primary_contact_name, domain
        ));
    }
    dedupe_strings(queries)
}

async fn search_company_osint_enrichment(
    search_engine: &WebSearchEngine,
    brave_search_engine: Option<&WebSearchEngine>,
    company: &str,
    domain: &str,
    title_policy: &str,
    target_geo: &str,
    primary_contact_name: Option<&str>,
) -> SiteContactEnrichment {
    let queries = build_company_osint_queries(
        company,
        domain,
        title_policy,
        target_geo,
        primary_contact_name,
    );
    let mut outputs = run_sales_search_batch(
        search_engine,
        &queries,
        6,
        Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
    )
    .await
    .into_iter()
    .filter_map(|(_, result)| result.ok())
    .filter(|value| !value.trim().is_empty())
    .collect::<Vec<_>>();

    let mut enrichment = best_search_contact_enrichment(&outputs, title_policy, company, domain);
    if !site_contact_enrichment_has_signal(&enrichment) {
        if let Some(brave_engine) = brave_search_engine {
            let brave_outputs = run_sales_search_batch(
                brave_engine,
                &queries,
                6,
                Duration::from_secs(SALES_OSINT_SEARCH_TIMEOUT_SECS),
            )
            .await
            .into_iter()
            .filter_map(|(_, result)| result.ok())
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>();
            if !brave_outputs.is_empty() {
                outputs.extend(brave_outputs);
                enrichment =
                    best_search_contact_enrichment(&outputs, title_policy, company, domain);
            }
        }
    }

    // Run job posting signal search (TASK-27) using the primary search engine
    enrichment.job_posting_signals =
        search_job_posting_signals(company, domain, search_engine).await;

    enrichment
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
    let canonical = canonicalize_contact_titles(raw)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let lower = canonical.to_lowercase();
    if lower.contains("ceo") || lower.contains("chief executive") || lower.contains("genel müdür")
    {
        "CEO".to_string()
    } else if lower.contains("coo") || lower.contains("chief operating") {
        "COO".to_string()
    } else if lower.contains("founder") || lower.contains("kurucu") {
        "Founder".to_string()
    } else if lower.contains("vice chairman")
        || lower.contains("başkan vekili")
        || lower.contains("baskan vekili")
    {
        "Vice Chairman".to_string()
    } else if lower.contains("chairman")
        || lower.contains("başkanı")
        || lower.contains("baskani")
        || lower.contains("board chair")
    {
        "Chairman".to_string()
    } else if lower.contains("head of operations") {
        "Head of Operations".to_string()
    } else if lower.contains("operations director") {
        "Operations Director".to_string()
    } else if lower.contains("managing director") {
        "Managing Director".to_string()
    } else {
        canonical
    }
}

fn normalize_person_name(raw: &str) -> Option<String> {
    let cleaned = raw
        .trim_matches(|c: char| {
            !c.is_alphanumeric() && c != '.' && c != '\'' && c != '-' && c != ' '
        })
        .split_whitespace()
        .collect::<Vec<_>>();
    if cleaned.len() < 2 || cleaned.len() > 4 {
        return None;
    }

    let stopwords = [
        "and",
        "or",
        "the",
        "bu",
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
        "web",
        "site",
        "ceo",
        "coo",
        "chairman",
        "founder",
        "chief",
        "executive",
        "director",
        "head",
        "vice",
        "president",
        "sitesinde",
        "tümüne",
        "tumune",
        "tümünü",
        "tumunu",
        "izin",
        "ver",
        "reddet",
        "kaydet",
        "çerez",
        "cerez",
        "çerezler",
        "cerezler",
        "cookies",
        "kullanılmaktadır",
        "kullanilmaktadir",
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
        "ve",
        "teknik",
        "servis",
        "bakim",
        "bakım",
        "onarim",
        "onarım",
        "operasyon",
        "operasyonlari",
        "operasyonları",
        "santiye",
        "şantiye",
        "muhendislik",
        "mühendislik",
        "altyapi",
        "altyapı",
        "ofis",
        "ofisi",
        "office",
        "turkiye",
        "türkiye",
        "turkey",
        "ankara",
        "istanbul",
        "izmir",
        "basin",
        "basın",
        "odasi",
        "odası",
        "haber",
        "onursal",
        "başkanımız",
        "baskanimiz",
        "kişisel",
        "kisisel",
        "verilerin",
        "verileri",
        "korunması",
        "korunmasi",
        "kanunu",
        "finansal",
        "gostergeler",
        "göstergeler",
        "kurumsal",
        "liderlik",
        "iletisim",
        "iletişim",
        "relations",
        "investor",
        "media",
        "merkez",
        "merkezi",
        "holding",
        "insaat",
        "inşaat",
        "office",
        "genel",
        "müdür",
        "mudur",
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
        "arabistan",
        "senegal",
        "sanayisinde",
        "sürdürülebilirlik",
        "surdurulebilirlik",
        "sosyal",
        "sorumluluk",
        "defa",
        "işi",
        "isi",
        "veren",
        "idare",
        "ihale",
        "yıl",
        "yili",
        "yılı",
        "proje",
        "işveren",
        "isveren",
        "yüklenici",
        "yuklenici",
        "kamunun",
        "muhtelif",
        "idarelerinden",
        "paylaşma",
        "paylasma",
        "iklimi",
        "donemi",
        "dönemi",
        "faaliyet",
        "alanlari",
        "alanları",
        "style",
        "font",
        "verdana",
        "geneva",
        "sans",
        "serif",
        "justify",
        "align",
        "size",
        "div",
        "class",
        "span",
        "href",
        "src",
        "img",
        "oturanlar",
        "soldan",
        "sağa",
        "saga",
        "ayakta",
        "nasil",
        "başladık",
        "basladik",
        "hedefimiz",
        "görüşümüz",
        "gorusumuz",
        "prensiplerimiz",
        "ilkelerimiz",
        "politikamız",
        "politikamiz",
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
        if t.chars().count() == 1 {
            return None;
        }
        if t.chars().count() > 18 {
            return None;
        }
        let t_lower = t.to_lowercase();
        let non_person_suffixes = [
            "sinde",
            "sında",
            "lerinde",
            "larında",
            "maktadır",
            "mektedir",
            "lari",
            "ları",
            "leri",
            "lerde",
            "larda",
        ];
        if t_lower.len() >= 7
            && non_person_suffixes
                .iter()
                .any(|suffix| t_lower.ends_with(suffix))
        {
            return None;
        }
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
    let normalized = out.join(" ");
    let normalized_lower = normalized.to_lowercase();
    let banned_phrases = [
        "costa rica",
        "saudi arabia",
        "south africa",
        "north macedonia",
        "new zealand",
        "sri lanka",
    ];
    if banned_phrases.contains(&normalized_lower.as_str()) {
        return None;
    }
    Some(normalized)
}

fn contact_title_looks_plausible(title: &str) -> bool {
    let trimmed = title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.chars().count() > 90 || trimmed.split_whitespace().count() > 10 {
        return false;
    }
    let lower = trimmed.to_lowercase();
    if lower.contains("font-size")
        || lower.contains("text-align")
        || lower.contains("style=")
        || lower.contains("cursor:")
    {
        return false;
    }
    let punctuation_count = trimmed
        .chars()
        .filter(|c| matches!(c, '.' | '!' | '?' | ';'))
        .count();
    punctuation_count <= 1
}

fn title_looks_like_operations_exec(title: &str) -> bool {
    let t = canonicalize_contact_titles(title).to_lowercase();
    t.contains("coo")
        || t.contains("chief operating")
        || t.contains("head of operations")
        || t.contains("operations director")
        || t.contains("service director")
        || t.contains("field operations")
        || ((t.contains("operasyon")
            || t.contains("işletme")
            || t.contains("isletme")
            || t.contains("bakım")
            || t.contains("bakim")
            || t.contains("service"))
            && (t.contains("başkan")
                || t.contains("baskan")
                || t.contains("direkt")
                || t.contains("müd")
                || t.contains("mud")
                || t.contains("chief")
                || t.contains("sorumlu")))
}

fn decode_html_email_entities(text: &str) -> String {
    text.replace("&commat;", "@")
        .replace("&#64;", "@")
        .replace("&#x40;", "@")
        .replace("&#x2e;", ".")
        .replace("&#x2E;", ".")
        .replace("&#46;", ".")
        .replace("&period;", ".")
}

fn decode_cloudflare_email(encoded: &str) -> Option<String> {
    if encoded.len() < 4 || encoded.len() % 2 != 0 {
        return None;
    }
    let key = u8::from_str_radix(&encoded[0..2], 16).ok()?;
    let mut out = String::new();
    let mut idx = 2;
    while idx + 2 <= encoded.len() {
        let byte = u8::from_str_radix(&encoded[idx..idx + 2], 16).ok()?;
        out.push((byte ^ key) as char);
        idx += 2;
    }
    normalize_email_candidate(Some(out))
}

fn normalize_email_candidate(email: Option<String>) -> Option<String> {
    email.and_then(|e| {
        let trimmed = e
            .trim()
            .trim_start_matches("mailto:")
            .trim_matches(|c: char| c == '"' || c == '\'' || c == ')' || c == '(')
            .to_lowercase();
        let (local, domain) = trimmed
            .rsplit_once('@')
            .map(|(l, d)| (l.trim(), d.trim()))?;
        let blocked_tlds = [
            "png", "jpg", "jpeg", "gif", "svg", "webp", "ico", "css", "js", "json", "xml", "pdf",
            "zip", "rar", "7z", "mp4", "webm", "mov",
        ];
        let tld = domain.rsplit('.').next().unwrap_or("");
        if trimmed.is_empty()
            || local.is_empty()
            || domain.is_empty()
            || domain.contains('/')
            || domain.contains('\\')
            || domain.contains('?')
            || domain.contains('&')
            || trimmed.ends_with("@example.com")
            || trimmed.contains("noreply")
            || trimmed.contains("no-reply")
            || is_consumer_email_domain(domain)
            || blocked_tlds.contains(&tld)
        {
            return None;
        }
        Some(trimmed)
    })
}

fn email_domain(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_lowercase())
        .filter(|domain| !domain.is_empty())
}

fn email_local_part(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(local, _)| local.trim().to_lowercase())
        .filter(|local| !local.is_empty())
}

fn email_is_generic_role_mailbox(email: &str) -> bool {
    let Some(local) = email_local_part(email) else {
        return false;
    };
    let normalized = local
        .split(['+', '.', '-', '_'])
        .next()
        .unwrap_or(local.as_str())
        .trim();
    matches!(
        normalized,
        "info"
            | "hello"
            | "contact"
            | "office"
            | "mail"
            | "admin"
            | "support"
            | "sales"
            | "team"
            | "general"
            | "iletisim"
            | "merhaba"
    )
}

fn email_is_actionable_outreach_email(email: &str) -> bool {
    let Some(domain) = email_domain(email) else {
        return false;
    };
    !is_consumer_email_domain(&domain) && !email_is_generic_role_mailbox(email)
}

fn normalize_actionable_outreach_email(email: Option<&str>) -> Option<String> {
    normalize_email_candidate(email.map(|value| value.to_string()))
        .filter(|value| email_is_actionable_outreach_email(value))
}

fn sanitize_approval_payload(
    channel: &str,
    payload: serde_json::Value,
) -> Option<serde_json::Value> {
    match channel {
        "email" => {
            let to = payload
                .get("to")
                .and_then(|value| value.as_str())
                .and_then(|value| normalize_actionable_outreach_email(Some(value)))?;
            let subject = payload
                .get("subject")
                .and_then(|value| value.as_str())?
                .trim();
            let body = payload.get("body").and_then(|value| value.as_str())?.trim();
            if subject.is_empty() || body.is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "to": to,
                "subject": subject,
                "body": body,
                "classification": classify_email(&to, email_domain(&to).as_deref().unwrap_or_default()),
            }))
        }
        "linkedin" | "linkedin_assist" => {
            let profile_url = payload
                .get("profile_url")
                .and_then(|value| value.as_str())
                .and_then(normalize_outreach_linkedin_url)?;
            let message = payload
                .get("message")
                .and_then(|value| value.as_str())?
                .trim();
            if message.is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "profile_url": profile_url,
                "message": message,
                "manual_action": true,
            }))
        }
        _ => Some(payload),
    }
}

fn count_mojibake_markers(text: &str) -> usize {
    text.chars()
        .filter(|c| matches!(c, 'Ã' | 'Ä' | 'Å' | 'â' | '�'))
        .count()
}

fn count_turkish_text_markers(text: &str) -> usize {
    text.chars()
        .filter(|c| {
            matches!(
                c,
                'Ç' | 'ç' | 'Ğ' | 'ğ' | 'İ' | 'ı' | 'Ö' | 'ö' | 'Ş' | 'ş' | 'Ü' | 'ü'
            )
        })
        .count()
}

fn repair_common_mojibake_utf8(text: &str) -> String {
    let suspicious = count_mojibake_markers(text);
    if suspicious < 2 {
        return text.to_string();
    }

    let mut bytes = Vec::<u8>::with_capacity(text.len());
    for ch in text.chars() {
        let code = ch as u32;
        if code > 0xFF {
            return text.to_string();
        }
        bytes.push(code as u8);
    }

    let Ok(repaired) = String::from_utf8(bytes) else {
        return text.to_string();
    };
    let repaired_suspicious = count_mojibake_markers(&repaired);
    let original_turkish = count_turkish_text_markers(text);
    let repaired_turkish = count_turkish_text_markers(&repaired);
    if repaired_suspicious < suspicious && repaired_turkish >= original_turkish {
        repaired
    } else {
        text.to_string()
    }
}

fn extract_email_from_text(text: &str) -> Option<String> {
    let cf_re = regex_lite::Regex::new(r#"(?i)data-cfemail=["']([0-9a-f]{6,})["']"#).unwrap();
    if let Some(cap) = cf_re.captures(text) {
        if let Some(decoded) = cap
            .get(1)
            .and_then(|m| decode_cloudflare_email(m.as_str().trim()))
        {
            return Some(decoded);
        }
    }

    let decoded = decode_html_email_entities(text);
    for source in [text, decoded.as_str()] {
        let mailto_re =
            regex_lite::Regex::new(r"(?i)mailto:([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})").unwrap();
        if let Some(cap) = mailto_re.captures(source) {
            let email = cap
                .get(1)
                .map(|m| m.as_str().trim().to_lowercase())
                .unwrap_or_default();
            if let Some(normalized) = normalize_email_candidate(Some(email)) {
                return Some(normalized);
            }
        }

        let re = regex_lite::Regex::new(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b").unwrap();
        for m in re.find_iter(source) {
            let email = m
                .as_str()
                .trim_matches(|c: char| c == '.' || c == ',' || c == ';' || c == ':' || c == ')')
                .trim_start_matches('(')
                .to_lowercase();
            if let Some(normalized) = normalize_email_candidate(Some(email)) {
                return Some(normalized);
            }
        }

        let alt_re = regex_lite::Regex::new(
            r"(?i)\b([A-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\sat\s)\s*([A-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\sdot\s)\s*([A-Z]{2,})\b",
        )
        .unwrap();
        if let Some(cap) = alt_re.captures(source) {
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
            if let Some(normalized) =
                normalize_email_candidate(Some(format!("{local}@{domain}.{tld}")))
            {
                return Some(normalized);
            }
        }
    }
    None
}

fn decode_basic_html_entities(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .replace("&ouml;", "ö")
        .replace("&Ouml;", "Ö")
        .replace("&#246;", "ö")
        .replace("&#214;", "Ö")
        .replace("&uuml;", "ü")
        .replace("&Uuml;", "Ü")
        .replace("&#252;", "ü")
        .replace("&#220;", "Ü")
        .replace("&ccedil;", "ç")
        .replace("&Ccedil;", "Ç")
        .replace("&#231;", "ç")
        .replace("&#199;", "Ç")
        .replace("&scedil;", "ş")
        .replace("&Scedil;", "Ş")
        .replace("&#351;", "ş")
        .replace("&#350;", "Ş")
        .replace("&#287;", "ğ")
        .replace("&#286;", "Ğ")
        .replace("&iacute;", "ı")
        .replace("&#305;", "ı")
        .replace("&#304;", "İ")
}

fn email_matches_company_domain(email: &str, company_domain: &str) -> bool {
    let domain = email_domain(email).unwrap_or_default();
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
    normalize_email_candidate(email).and_then(|trimmed| {
        if !email_matches_company_domain(&trimmed, company_domain)
            || !email_is_actionable_outreach_email(&trimmed)
        {
            return None;
        }
        Some(trimmed)
    })
}

fn normalize_directory_email_for_domain(
    email: Option<String>,
    company_domain: &str,
) -> Option<String> {
    normalize_email_candidate(email).and_then(|trimmed| {
        if !email_matches_company_domain(&trimmed, company_domain) {
            return None;
        }
        Some(trimmed)
    })
}

fn normalize_site_contact_email(email: Option<String>) -> Option<String> {
    normalize_email_candidate(email).filter(|trimmed| email_is_actionable_outreach_email(trimmed))
}

fn normalize_outreach_linkedin_url(raw: &str) -> Option<String> {
    extract_personal_linkedin_from_text(raw)
}

fn normalize_company_linkedin_url(raw: &str) -> Option<String> {
    extract_company_linkedin_from_text(raw)
}

fn extract_meta_description(html: &str) -> Option<String> {
    let patterns = [
        r#"(?is)<meta[^>]*property\s*=\s*["']og:description["'][^>]*content\s*=\s*["']([^"']{8,400})["']"#,
        r#"(?is)<meta[^>]*name\s*=\s*["']description["'][^>]*content\s*=\s*["']([^"']{8,400})["']"#,
    ];
    for pattern in patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        if let Some(cap) = re.captures(html) {
            if let Some(value) = cap.get(1) {
                let text = truncate_cleaned_text(
                    &decode_basic_html_entities(value.as_str().trim()),
                    220,
                );
                if !text.is_empty() {
                    return Some(text);
                }
            }
        }
    }
    None
}

fn normalize_instagram_profile_url(raw: &str) -> Option<String> {
    let url = repair_common_url_typos(raw).trim().to_string();
    let parsed = url::Url::parse(&url).ok()?;
    let host = parsed.host_str()?.trim().to_ascii_lowercase();
    if host != "instagram.com" && host != "www.instagram.com" {
        return None;
    }
    let handle = parsed.path().trim_matches('/').split('/').next()?.trim();
    if handle.is_empty()
        || handle.eq_ignore_ascii_case("p")
        || handle.eq_ignore_ascii_case("reel")
        || handle.eq_ignore_ascii_case("explore")
    {
        return None;
    }
    Some(format!("https://www.instagram.com/{handle}/"))
}

fn normalize_tiktok_profile_url(raw: &str) -> Option<String> {
    let url = repair_common_url_typos(raw).trim().to_string();
    let parsed = url::Url::parse(&url).ok()?;
    let host = parsed.host_str()?.trim().to_ascii_lowercase();
    if host != "tiktok.com" && host != "www.tiktok.com" {
        return None;
    }
    let path = parsed.path().trim_matches('/');
    let first = path.split('/').next()?.trim();
    if !first.starts_with('@') || first.len() < 3 {
        return None;
    }
    Some(format!("https://www.tiktok.com/{first}"))
}

fn normalize_public_profile_url(raw: &str) -> Option<String> {
    normalize_outreach_linkedin_url(raw)
        .or_else(|| normalize_instagram_profile_url(raw))
        .or_else(|| normalize_tiktok_profile_url(raw))
}

fn public_profile_channel(raw: Option<&str>) -> Option<&'static str> {
    let value = raw?;
    if normalize_outreach_linkedin_url(value).is_some() {
        Some("linkedin")
    } else if normalize_instagram_profile_url(value).is_some()
        || normalize_tiktok_profile_url(value).is_some()
    {
        Some("social_dm")
    } else {
        None
    }
}

fn sales_base_url(_kernel: &pulsivo_salesman_kernel::PulsivoSalesmanKernel) -> String {
    std::env::var("PULSIVO_SALESMAN_PUBLIC_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SALES_BASE_URL.to_string())
}

fn generate_unsubscribe_token(recipient: &str, sender_email: &str) -> String {
    use sha2::Digest;

    let recipient = recipient.trim().to_lowercase();
    let sender_email = sender_email.trim().to_lowercase();
    let mut hasher = sha2::Sha256::new();
    hasher.update(SALES_UNSUBSCRIBE_SALT.as_bytes());
    hasher.update(b":");
    hasher.update(sender_email.as_bytes());
    hasher.update(b":");
    hasher.update(recipient.as_bytes());
    let signature = format!("{:x}", hasher.finalize());
    URL_SAFE_NO_PAD.encode(format!("{recipient}|{sender_email}|{signature}"))
}

fn verify_unsubscribe_token(token: &str) -> Option<String> {
    let decoded = URL_SAFE_NO_PAD.decode(token.trim()).ok()?;
    let payload = String::from_utf8(decoded).ok()?;
    let mut parts = payload.split('|');
    let recipient = parts.next()?.trim().to_lowercase();
    let sender_email = parts.next()?.trim().to_lowercase();
    let _signature = parts.next()?.trim().to_string();
    if parts.next().is_some() || recipient.is_empty() || sender_email.is_empty() {
        return None;
    }
    let expected = generate_unsubscribe_token(&recipient, &sender_email);
    let normalized_expected = URL_SAFE_NO_PAD.decode(expected).ok()?;
    let normalized_payload = URL_SAFE_NO_PAD.decode(token.trim()).ok()?;
    if subtle::ConstantTimeEq::ct_eq(
        normalized_expected.as_slice(),
        normalized_payload.as_slice(),
    )
    .into()
    {
        Some(recipient)
    } else {
        None
    }
}

fn is_valid_sending_subdomain(sender_domain: &str, brand_domain: &str) -> bool {
    let sender = sender_domain.trim().to_lowercase();
    let brand = brand_domain.trim().to_lowercase();
    !sender.is_empty()
        && !brand.is_empty()
        && sender != brand
        && sender.ends_with(&format!(".{brand}"))
}

async fn check_mx_record(domain: &str) -> bool {
    match hickory_resolver::TokioAsyncResolver::tokio_from_system_conf() {
        Ok(resolver) => resolver.mx_lookup(domain).await.is_ok(),
        Err(_) => true,
    }
}

async fn assess_domain_health(domain: &str) -> f64 {
    let mut score: f64 = 0.45;
    if domain.ends_with(".com") || domain.ends_with(".net") || domain.ends_with(".org") {
        score += 0.1;
    }
    if domain.ends_with(".tr") || domain.ends_with(".com.tr") {
        score += 0.15;
    }
    if !is_consumer_email_domain(domain) {
        score += 0.1;
    }
    score.clamp(0.0, 1.0)
}

async fn validate_email_for_sending(
    email: &str,
    suppressed: bool,
    bounce_count: i64,
) -> Result<EmailValidation, String> {
    let mut result = EmailValidation {
        email: email.to_string(),
        syntax_valid: false,
        mx_valid: false,
        domain_health: 0.0,
        suppressed: false,
        classification: "unknown".to_string(),
        safe_to_send: false,
    };

    result.syntax_valid = email_syntax_valid(email);
    if !result.syntax_valid {
        return Ok(result);
    }

    result.suppressed = suppressed;
    if result.suppressed {
        result.classification = classify_email(email, "").to_string();
        return Ok(result);
    }

    let domain = email_domain(email).unwrap_or_default();
    result.mx_valid = check_mx_record(&domain).await;
    result.domain_health = assess_domain_health(&domain).await;
    result.classification = classify_email(email, &domain).to_string();

    result.safe_to_send = result.syntax_valid
        && result.mx_valid
        && !result.suppressed
        && result.domain_health > 0.3
        && bounce_count < 3
        && result.classification == "personal";

    Ok(result)
}

fn stable_sales_id(prefix: &str, parts: &[&str]) -> String {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(prefix.as_bytes());
    for part in parts {
        hasher.update(b"|");
        hasher.update(part.trim().to_lowercase().as_bytes());
    }
    let digest = format!("{:x}", hasher.finalize());
    format!("{prefix}_{}", &digest[..24])
}

fn source_confidence(source: &str) -> f64 {
    match source {
        "directory_listing" => 0.9,
        "site_html" => 0.8,
        "web_search" => 0.6,
        "llm_generation" => 0.4,
        "llm_enrichment" => 0.5,
        _ => 0.3,
    }
}

fn seniority_from_title(title: Option<&str>) -> String {
    let title = title.unwrap_or_default().to_lowercase();
    if title.contains("chief")
        || title.contains("ceo")
        || title.contains("coo")
        || title.contains("founder")
        || title.contains("genel müdür")
        || title.contains("genel mudur")
    {
        "c_level".to_string()
    } else if title.contains("vp") || title.contains("vice president") {
        "vp".to_string()
    } else if title.contains("director") || title.contains("direktör") || title.contains("direktor")
    {
        "director".to_string()
    } else if title.contains("manager") || title.contains("müdür") || title.contains("mudur") {
        "manager".to_string()
    } else {
        "unknown".to_string()
    }
}

fn classify_reply_content(text: &str) -> &'static str {
    let lower = text.to_lowercase();
    if lower.contains("toplanti") || lower.contains("meeting") || lower.contains("goruselim") {
        "meeting_booked"
    } else if lower.contains("ilginc") || lower.contains("interested") || lower.contains("merak") {
        "interested"
    } else if lower.contains("simdi degil") || lower.contains("not now") || lower.contains("sonra")
    {
        "not_now"
    } else if lower.contains("yanlis") || lower.contains("wrong") || lower.contains("hatali") {
        "wrong_person"
    } else if lower.contains("cikar") || lower.contains("unsubscribe") || lower.contains("gonderme")
    {
        "unsubscribe"
    } else {
        "interested"
    }
}

fn classify_outcome(raw_event: &str, event_type: &str, touch_id: &str) -> OutcomeRecord {
    let outcome_type = match event_type {
        "bounce_hard" => "hard_bounce",
        "bounce_soft" => "soft_bounce",
        "open" => "open",
        "click" => "click",
        "reply" => classify_reply_content(raw_event),
        "unsubscribe" => "unsubscribe",
        _ => "no_reply",
    };

    OutcomeRecord {
        touch_id: touch_id.to_string(),
        outcome_type: outcome_type.to_string(),
        raw_text: raw_event.to_string(),
        classifier_confidence: 1.0,
    }
}

fn classify_signal_horizon(signal_type: &str, text: &str) -> (&'static str, Option<String>) {
    let (horizon, days) = match signal_type {
        "tender" | "crisis" | "urgent_hire" => ("immediate", 21),
        "new_department" | "digitalization" | "new_location" => ("campaign_window", 90),
        "erp_migration" | "merger" | "regulation_pressure" => ("structural", 365),
        "job_posting" => {
            if text.to_lowercase().contains("acil") || text.to_lowercase().contains("urgent") {
                ("immediate", 21)
            } else {
                ("campaign_window", 60)
            }
        }
        "directory_membership" => ("structural", 365),
        _ => ("campaign_window", 90),
    };

    let expires = Utc::now()
        .checked_add_signed(chrono::Duration::days(days))
        .map(|value| value.to_rfc3339());
    (horizon, expires)
}

fn generate_signal_rationale(signal_type: &str, text: &str) -> String {
    match signal_type {
        "job_posting" => format!("Hiring activity suggests active change capacity: {text}"),
        "directory_membership" => format!("Verified sector presence supports ICP fit: {text}"),
        "tech_stack" => format!("Observed stack may create switching or integration pain: {text}"),
        _ => format!("Public signal may indicate operational relevance: {text}"),
    }
}

fn infer_signal_type(text: &str) -> &'static str {
    let lower = text.to_lowercase();
    if lower.contains("ihale") || lower.contains("tender") {
        "tender"
    } else if lower.contains("acil") || lower.contains("urgent") {
        "urgent_hire"
    } else if lower.contains("kariyer")
        || lower.contains("career")
        || lower.contains("is ilani")
        || lower.contains("job")
        || lower.contains("hiring")
    {
        "job_posting"
    } else if lower.contains("erp") || lower.contains("sap") || lower.contains("netsis") {
        "erp_migration"
    } else if lower.contains("dijital") || lower.contains("digital") {
        "digitalization"
    } else if lower.contains("tesis") || lower.contains("facility") || lower.contains("lokasyon") {
        "new_location"
    } else if lower.contains("uye") || lower.contains("member") || lower.contains("odasi") {
        "directory_membership"
    } else {
        "site_content"
    }
}

/// Detect job posting signals from search results for an account.
/// Job postings indicate active change capacity and intent.
#[cfg(test)]
fn extract_job_posting_signals(
    search_results: &[SearchEntry],
    account_name: &str,
) -> Vec<(String, String, f64)> {
    let job_keywords = [
        "operasyon",
        "saha",
        "field",
        "operations",
        "hiring",
        "kariyer",
        "is ilani",
        "job",
        "career",
        "ise alim",
        "pozisyon",
        "mudur",
        "yonetici",
        "engineer",
        "technician",
        "teknisyen",
    ];
    let name_lower = account_name.to_lowercase();

    search_results
        .iter()
        .filter(|entry| {
            let title_lower = entry.title.to_lowercase();
            let url_lower = entry.url.to_lowercase();
            let name_match = title_lower.contains(&name_lower)
                || url_lower.contains("kariyer.net")
                || url_lower.contains("linkedin.com/jobs");
            let keyword_match = job_keywords.iter().any(|kw| title_lower.contains(kw));
            name_match && keyword_match
        })
        .map(|entry| {
            let confidence = if entry.url.contains("kariyer.net") {
                0.8
            } else if entry.url.contains("linkedin.com") {
                0.7
            } else {
                0.5
            };
            (entry.title.clone(), entry.url.clone(), confidence)
        })
        .collect()
}

/// Detect job posting intent from OSINT URLs (lightweight URL-based variant).
fn detect_job_posting_intent_from_urls(osint_links: &[String]) -> Vec<String> {
    let job_domains = [
        "kariyer.net",
        "linkedin.com/jobs",
        "indeed.com",
        "glassdoor.com",
        "secretcv.com",
        "yenibiris.com",
    ];
    osint_links
        .iter()
        .filter(|url| {
            let lower = url.to_lowercase();
            job_domains.iter().any(|d| lower.contains(d))
        })
        .map(|url| format!("Job posting: {}", url))
        .collect()
}

/// Search job posting sites for intent signals for a given account (TASK-27).
/// Job postings on kariyer.net, LinkedIn Jobs, etc. indicate active change
/// capacity and serve as campaign-window intent signals.
async fn search_job_posting_signals(
    account_name: &str,
    domain: &str,
    search_engine: &WebSearchEngine,
) -> Vec<(String, String, f64, String)> {
    // (signal_text, source_url, confidence, signal_type)
    let transliterated = transliterate_turkish_ascii(account_name);
    let queries = vec![
        format!("site:kariyer.net \"{}\"", transliterated),
        format!("site:linkedin.com/jobs \"{}\"", transliterated),
        format!(
            "\"{}\" \"saha\" OR \"operasyon\" OR \"field\" iş ilanı",
            transliterated
        ),
    ];
    let timeout = Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS);
    let job_keywords = [
        "operasyon",
        "saha",
        "field",
        "operations",
        "hiring",
        "kariyer",
        "is ilani",
        "job",
        "career",
        "ise alim",
        "pozisyon",
        "mudur",
        "yonetici",
        "engineer",
        "technician",
        "teknisyen",
        "bakim",
        "maintenance",
        "uretim",
        "production",
    ];

    let mut signals = Vec::new();
    for q in &queries {
        let raw = match run_sales_search(search_engine, q, 5, timeout).await {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in parse_search_entries(&raw) {
            let title_lower = entry.title.to_lowercase();
            let url_lower = entry.url.to_lowercase();
            let domain_lower = domain.to_lowercase();
            let name_lower = transliterated.to_lowercase();

            // Must be related to the target account
            let name_match = title_lower.contains(&name_lower)
                || title_lower.contains(&domain_lower)
                || url_lower.contains("kariyer.net")
                || url_lower.contains("linkedin.com/jobs");

            let keyword_match = job_keywords.iter().any(|kw| title_lower.contains(kw));

            if name_match && keyword_match {
                let confidence = if url_lower.contains("kariyer.net") {
                    0.8
                } else if url_lower.contains("linkedin.com") {
                    0.7
                } else {
                    0.5
                };
                signals.push((
                    entry.title.clone(),
                    entry.url.clone(),
                    confidence,
                    "job_posting".to_string(),
                ));
            }
        }
        if signals.len() >= 5 {
            break;
        }
    }
    signals
}

/// Detect tech stack from site HTML content and HTTP headers.
fn detect_tech_stack(html: &str, headers: &HashMap<String, String>) -> Vec<String> {
    let detections: &[(&str, &[&str])] = &[
        ("SAP", &["sap.com", "sap-ui", "sapui5", "/sap/"]),
        ("Salesforce", &["salesforce.com", "force.com", "pardot"]),
        ("HubSpot", &["hubspot.com", "hs-scripts", "hbspt"]),
        ("Microsoft Dynamics", &["dynamics.com", "d365"]),
        ("Oracle", &["oracle.com", "eloqua"]),
        ("WordPress", &["wp-content", "wp-includes", "wordpress"]),
        ("Shopify", &["shopify.com", "cdn.shopify"]),
        ("React", &["react-root", "reactjs", "__NEXT_DATA__"]),
        ("Angular", &["ng-version", "angular"]),
        ("Vue.js", &["vue-app", "vuejs"]),
        (
            "Google Analytics",
            &["google-analytics.com", "gtag/js", "ga.js"],
        ),
        ("Google Tag Manager", &["googletagmanager.com", "gtm.js"]),
        ("Hotjar", &["hotjar.com", "static.hotjar"]),
        ("Intercom", &["intercom.io", "intercomSettings"]),
        ("Zendesk", &["zendesk.com", "zdassets"]),
        ("Jira", &["atlassian.net", "jira"]),
        ("Netsis", &["netsis"]),
        ("Logo Yazılım", &["logo.com.tr", "logo yazılım"]),
        ("IFS", &["ifs.com", "ifsworld"]),
    ];

    let html_lower = html.to_lowercase();
    let mut stack: Vec<String> = detections
        .iter()
        .filter(|(_, indicators)| indicators.iter().any(|ind| html_lower.contains(ind)))
        .map(|(name, _)| name.to_string())
        .collect();

    if let Some(powered_by) = headers.get("x-powered-by") {
        if !powered_by.trim().is_empty() {
            stack.push(powered_by.trim().to_string());
        }
    }
    if let Some(server) = headers.get("server") {
        let sv = server.trim().to_lowercase();
        if sv.contains("nginx") || sv.contains("apache") || sv.contains("iis") {
            stack.push(server.trim().to_string());
        }
    }

    stack.sort();
    stack.dedup();
    stack
}

/// 4-Layer LinkedIn company URL search (TASK-24).
/// Progressively broader queries to find the LinkedIn company page.
async fn find_company_linkedin_url(
    company_name: &str,
    domain: &str,
    search_engine: &WebSearchEngine,
) -> Option<String> {
    let timeout = Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS);
    let transliterated = transliterate_turkish_ascii(company_name);

    // Layer 1: Domain match — most precise
    let q1 = format!("site:linkedin.com/company/ \"{}\"", domain);
    if let Some(url) = linkedin_search_attempt(search_engine, &q1, timeout).await {
        return Some(url);
    }

    // Layer 2: Transliterated company name (handles Turkish chars)
    let q2 = format!("site:linkedin.com/company/ \"{}\"", transliterated);
    if let Some(url) = linkedin_search_attempt(search_engine, &q2, timeout).await {
        return Some(url);
    }

    // Layer 3: Company name + CEO search
    let q3 = format!("\"{}\" linkedin CEO OR \"Genel Müdür\"", company_name);
    if let Some(url) = linkedin_search_attempt(search_engine, &q3, timeout).await {
        return Some(url);
    }

    // Layer 4: Turkish LinkedIn subdomain
    let q4 = format!("site:tr.linkedin.com \"{}\"", domain);
    linkedin_search_attempt(search_engine, &q4, timeout).await
}

async fn linkedin_search_attempt(
    search_engine: &WebSearchEngine,
    query: &str,
    timeout: Duration,
) -> Option<String> {
    match run_sales_search(search_engine, query, 5, timeout).await {
        Ok(res) if !res.trim().is_empty() => extract_company_linkedin_from_text(&res)
            .and_then(|value| normalize_company_linkedin_url(&value)),
        _ => None,
    }
}

/// Seed default contextual factors for Turkish market timing (TASK-35).
fn seed_contextual_factors(conn: &Connection) {
    let factors: &[(&str, &str, &str, &str, &str)] = &[
        (
            "holiday",
            "ramazan_bayrami",
            "Ramazan Bayramı — avoid outreach",
            "2026-03-20",
            "2026-03-23",
        ),
        (
            "holiday",
            "kurban_bayrami",
            "Kurban Bayramı — avoid outreach",
            "2026-05-27",
            "2026-05-30",
        ),
        (
            "holiday",
            "cumhuriyet_bayrami",
            "Cumhuriyet Bayramı — avoid outreach",
            "2026-10-29",
            "2026-10-29",
        ),
        (
            "holiday",
            "yilbasi",
            "Yılbaşı — avoid outreach",
            "2026-12-31",
            "2027-01-01",
        ),
        (
            "budget_quarter",
            "q1_budget",
            "Q1 budget planning — high activity",
            "2026-01-02",
            "2026-03-31",
        ),
        (
            "budget_quarter",
            "q2_budget",
            "Q2 budget planning — high activity",
            "2026-04-01",
            "2026-06-30",
        ),
        (
            "budget_quarter",
            "q3_budget",
            "Q3 budget planning — high activity",
            "2026-07-01",
            "2026-09-30",
        ),
        (
            "budget_quarter",
            "q4_budget",
            "Q4 budget planning — high activity",
            "2026-10-01",
            "2026-12-31",
        ),
        (
            "season",
            "summer_slow",
            "Summer slowdown — reduced response rates",
            "2026-07-15",
            "2026-08-31",
        ),
        (
            "regulation",
            "kvkk",
            "KVKK (Turkish GDPR) — ensure compliance",
            "2016-04-07",
            "2099-12-31",
        ),
    ];
    for (factor_type, factor_key, factor_value, eff_from, eff_until) in factors {
        let id = stable_sales_id("ctx_factor", &[factor_type, factor_key]);
        let _ = conn.execute(
            "INSERT OR IGNORE INTO contextual_factors (id, factor_type, factor_key, factor_value, effective_from, effective_until, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'system_default')",
            params![id, factor_type, factor_key, factor_value, eff_from, eff_until],
        );
    }
}

/// Check if today falls within a holiday or slow period (TASK-35).
fn is_bad_timing_today(conn: &Connection) -> bool {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let count: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM contextual_factors
             WHERE factor_type IN ('holiday', 'season')
             AND effective_from <= ?1 AND effective_until >= ?1",
            params![today],
            |r| r.get(0),
        )
        .unwrap_or(0);
    count > 0
}

/// Determine the current budget quarter context (TASK-35).
fn current_budget_quarter(conn: &Connection) -> Option<String> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    conn.query_row(
        "SELECT factor_value FROM contextual_factors
         WHERE factor_type = 'budget_quarter'
         AND effective_from <= ?1 AND effective_until >= ?1
         LIMIT 1",
        params![today],
        |r| r.get(0),
    )
    .ok()
}

/// Calibrate scoring weights from outcome data (TASK-36).
/// Analyzes positive/negative outcomes and creates rule proposals when
/// signal weights appear to need adjustment.
fn calibrate_scoring_from_outcomes(conn: &Connection) -> Result<Vec<String>, String> {
    let mut proposals = Vec::new();

    // Only calibrate when we have enough data
    let outcome_count: i32 = conn
        .query_row("SELECT COUNT(*) FROM outcomes", [], |r| r.get(0))
        .unwrap_or(0);
    if outcome_count < 10 {
        return Ok(proposals);
    }

    // Analyze which signals appear in positive vs negative outcomes
    let mut stmt = conn
        .prepare(
            "SELECT s.signal_type,
                    SUM(CASE WHEN o.outcome_type IN ('meeting_booked', 'interested', 'click') THEN 1 ELSE 0 END) as positive,
                    SUM(CASE WHEN o.outcome_type IN ('hard_bounce', 'unsubscribe', 'wrong_person') THEN 1 ELSE 0 END) as negative,
                    COUNT(*) as total
             FROM outcome_attribution_snapshots oas
             JOIN outcomes o ON o.touch_id = oas.touch_id
             JOIN signals s ON s.account_id = oas.account_id
             GROUP BY s.signal_type
             HAVING total >= 3",
        )
        .map_err(|e| format!("Calibration query failed: {e}"))?;

    let rows: Vec<(String, i32, i32, i32)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| format!("Calibration query failed: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    for (signal_type, positive, negative, total) in rows {
        let positive_rate = positive as f64 / total as f64;
        let negative_rate = negative as f64 / total as f64;

        // If this signal type has high negative correlation, propose reducing its weight
        if negative_rate > 0.5 && total >= 5 {
            let proposal_id = stable_sales_id(
                "rule_proposal",
                &[
                    &signal_type,
                    "weight_down",
                    &Utc::now().format("%Y-%W").to_string(),
                ],
            );
            let _ = conn.execute(
                "INSERT OR IGNORE INTO retrieval_rule_versions
                 (id, rule_type, rule_key, old_value, new_value, proposal_source, status, version, created_at)
                 VALUES (?1, 'signal_weight', ?2, ?3, ?4, 'auto_calibration', 'proposed', 1, ?5)",
                params![
                    proposal_id,
                    signal_type,
                    format!("current (neg_rate={negative_rate:.2})"),
                    format!("reduce_weight (pos={positive}, neg={negative}, total={total})"),
                    Utc::now().to_rfc3339(),
                ],
            );
            proposals.push(format!(
                "Propose reducing weight for signal '{signal_type}': neg_rate={negative_rate:.2}"
            ));
        }

        // If this signal type has high positive correlation, propose increasing weight
        if positive_rate > 0.6 && total >= 5 {
            let proposal_id = stable_sales_id(
                "rule_proposal",
                &[
                    &signal_type,
                    "weight_up",
                    &Utc::now().format("%Y-%W").to_string(),
                ],
            );
            let _ = conn.execute(
                "INSERT OR IGNORE INTO retrieval_rule_versions
                 (id, rule_type, rule_key, old_value, new_value, proposal_source, status, version, created_at)
                 VALUES (?1, 'signal_weight', ?2, ?3, ?4, 'auto_calibration', 'proposed', 1, ?5)",
                params![
                    proposal_id,
                    signal_type,
                    format!("current (pos_rate={positive_rate:.2})"),
                    format!("increase_weight (pos={positive}, neg={negative}, total={total})"),
                    Utc::now().to_rfc3339(),
                ],
            );
            proposals.push(format!(
                "Propose increasing weight for signal '{signal_type}': pos_rate={positive_rate:.2}"
            ));
        }
    }

    Ok(proposals)
}

/// Create an A/B experiment and return its ID (TASK-37).
fn create_experiment(
    conn: &Connection,
    name: &str,
    hypothesis: &str,
    variant_a: &str,
    variant_b: &str,
) -> Result<String, String> {
    let id = stable_sales_id("experiment", &[name]);
    conn.execute(
        "INSERT INTO experiments (id, name, hypothesis, variant_a, variant_b, status, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6)
         ON CONFLICT(id) DO UPDATE SET
            hypothesis = excluded.hypothesis,
            variant_a = excluded.variant_a,
            variant_b = excluded.variant_b",
        params![
            id,
            name,
            hypothesis,
            variant_a,
            variant_b,
            Utc::now().to_rfc3339()
        ],
    )
    .map_err(|e| format!("Failed to create experiment: {e}"))?;
    Ok(id)
}

/// Assign a sequence instance to an experiment variant (TASK-37).
fn assign_experiment_variant(
    conn: &Connection,
    experiment_id: &str,
    sequence_instance_id: &str,
) -> Result<String, String> {
    // Balanced assignment: pick whichever variant has fewer assignments
    let count_a: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM experiment_assignments
             WHERE experiment_id = ?1 AND variant = 'a'",
            params![experiment_id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let count_b: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM experiment_assignments
             WHERE experiment_id = ?1 AND variant = 'b'",
            params![experiment_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let variant = if count_a <= count_b { "a" } else { "b" };
    let id = stable_sales_id("exp_assign", &[experiment_id, sequence_instance_id]);
    conn.execute(
        "INSERT OR IGNORE INTO experiment_assignments (id, experiment_id, sequence_instance_id, variant)
         VALUES (?1, ?2, ?3, ?4)",
        params![id, experiment_id, sequence_instance_id, variant],
    )
    .map_err(|e| format!("Failed to assign experiment variant: {e}"))?;
    Ok(variant.to_string())
}

/// Get experiment results summary (TASK-37).
fn get_experiment_results(
    conn: &Connection,
    experiment_id: &str,
) -> Result<serde_json::Value, String> {
    let mut stmt = conn
        .prepare(
            "SELECT ea.variant,
                    COUNT(DISTINCT ea.sequence_instance_id) as sequences,
                    SUM(CASE WHEN o.outcome_type IN ('meeting_booked', 'interested') THEN 1 ELSE 0 END) as positive,
                    SUM(CASE WHEN o.outcome_type IN ('hard_bounce', 'unsubscribe') THEN 1 ELSE 0 END) as negative,
                    COUNT(o.id) as total_outcomes
             FROM experiment_assignments ea
             LEFT JOIN sequence_instances si ON si.id = ea.sequence_instance_id
             LEFT JOIN touches t ON t.sequence_instance_id = si.id
             LEFT JOIN outcomes o ON o.touch_id = t.id
             WHERE ea.experiment_id = ?1
             GROUP BY ea.variant",
        )
        .map_err(|e| format!("Experiment results query failed: {e}"))?;

    let variants: Vec<serde_json::Value> = stmt
        .query_map(params![experiment_id], |r| {
            let variant: String = r.get(0)?;
            let sequences: i32 = r.get(1)?;
            let positive: i32 = r.get(2)?;
            let negative: i32 = r.get(3)?;
            let total: i32 = r.get(4)?;
            Ok(serde_json::json!({
                "variant": variant,
                "sequences": sequences,
                "positive_outcomes": positive,
                "negative_outcomes": negative,
                "total_outcomes": total,
                "positive_rate": if total > 0 { positive as f64 / total as f64 } else { 0.0 },
            }))
        })
        .map_err(|e| format!("Experiment results query failed: {e}"))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(serde_json::json!({
        "experiment_id": experiment_id,
        "variants": variants,
    }))
}

/// Verify LLM-generated domain actually exists with a HEAD request (TASK-40).
async fn verify_domain_exists(domain: &str) -> bool {
    let url = format!("https://{domain}");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.head(&url).send().await {
        Ok(resp) => resp.status().is_success() || resp.status().is_redirection(),
        Err(_) => {
            // Try HTTP as fallback
            let http_url = format!("http://{domain}");
            client
                .head(&http_url)
                .send()
                .await
                .map(|r| r.status().is_success() || r.status().is_redirection())
                .unwrap_or(false)
        }
    }
}

fn candidate_primary_source_type(
    candidate: &DomainCandidate,
    company_linkedin_url: Option<&str>,
) -> &'static str {
    if candidate.phone.is_some() {
        "directory_listing"
    } else if company_linkedin_url.is_some() {
        "web_search"
    } else {
        "site_html"
    }
}

fn canonical_contact_key(
    domain: &str,
    contact_name: Option<&str>,
    email: Option<&str>,
    linkedin_url: Option<&str>,
) -> String {
    if let Some(email) = email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
    {
        return email;
    }
    if let Some(linkedin) = linkedin_url.and_then(normalize_outreach_linkedin_url) {
        return linkedin;
    }
    if let Some(name) = contact_name.and_then(normalize_person_name) {
        return name.to_lowercase();
    }
    format!("{domain}-primary")
}

fn infer_buyer_role(title: &str) -> &'static str {
    let lower = title.to_lowercase();
    if lower.contains("founder") {
        "founder"
    } else if lower.contains("ceo") || lower.contains("chief executive") {
        "ceo"
    } else if lower.contains("coo") || lower.contains("operations") {
        "operations"
    } else if lower.contains("sales") {
        "revenue"
    } else {
        "buyer_committee"
    }
}

fn parse_json_string_list(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn rules_match(value: &str, rules: &[String]) -> bool {
    let lower = value.trim().to_lowercase();
    if lower.is_empty() {
        return false;
    }
    rules.iter().any(|rule| {
        let rule = rule.trim().to_lowercase();
        !rule.is_empty() && (lower.contains(&rule) || rule.contains(&lower))
    })
}

fn compute_fit_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let (sector, geo, employee_estimate) = db
        .query_row(
            "SELECT COALESCE(sector, ''), COALESCE(geo, ''), employee_estimate
             FROM accounts WHERE id = ?1",
            params![account_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .map_err(|e| format!("Failed to load account fit state: {e}"))?;
    let (sector_rules, geo_rules) = db
        .query_row(
            "SELECT COALESCE(sector_rules, '[]'), COALESCE(geo_rules, '[]')
             FROM icp_definitions
             ORDER BY created_at DESC
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|e| format!("Failed to load ICP rules: {e}"))?
        .unwrap_or_else(|| ("[]".to_string(), "[]".to_string()));
    let sector_match: f64 = if rules_match(&sector, &parse_json_string_list(&sector_rules)) {
        1.0
    } else if !sector.trim().is_empty() {
        0.45
    } else {
        0.0
    };
    let geo_match: f64 = if rules_match(&geo, &parse_json_string_list(&geo_rules)) {
        1.0
    } else if !geo.trim().is_empty() {
        0.4
    } else {
        0.0
    };
    let size_match: f64 = if employee_estimate.unwrap_or_default() > 0 {
        1.0
    } else {
        0.4
    };
    let site_content_count = db
        .query_row(
            "SELECT COUNT(*) FROM signals WHERE account_id = ?1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let site_content_match: f64 = if site_content_count > 0 { 1.0 } else { 0.35 };
    let directory_membership = db
        .query_row(
            "SELECT COUNT(*) FROM signals
             WHERE account_id = ?1 AND signal_type = 'directory_membership'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let directory_score: f64 = if directory_membership > 0 { 1.0 } else { 0.25 };
    Ok((sector_match * 0.3
        + size_match * 0.2
        + geo_match * 0.2
        + site_content_match * 0.15
        + directory_score * 0.15)
        .clamp(0.0, 1.0))
}

fn compute_intent_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let mut stmt = db
        .prepare(
            "SELECT signal_type, COALESCE(text, ''), COALESCE(effect_horizon, '')
             FROM signals
             WHERE account_id = ?1",
        )
        .map_err(|e| format!("Failed to prepare intent query: {e}"))?;
    let mut rows = stmt
        .query(params![account_id])
        .map_err(|e| format!("Failed to query intent signals: {e}"))?;
    let mut score: f64 = 0.0;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("Failed to read intent signals: {e}"))?
    {
        let signal_type = row.get::<_, String>(0).unwrap_or_default();
        let text = row.get::<_, String>(1).unwrap_or_default();
        let horizon = row.get::<_, String>(2).unwrap_or_default();
        let weight = match signal_type.as_str() {
            "tender" | "urgent_hire" => 0.3,
            "job_posting" | "new_location" | "digitalization" => 0.2,
            "erp_migration" | "directory_membership" => 0.15,
            _ => 0.1,
        };
        let horizon_boost = match horizon.as_str() {
            "immediate" => 1.0,
            "campaign_window" => 0.75,
            "structural" => 0.45,
            _ => 0.3,
        };
        let text_boost = if text.to_lowercase().contains("acil")
            || text.to_lowercase().contains("urgent")
            || text.to_lowercase().contains("launch")
        {
            1.0
        } else {
            0.75
        };
        score += weight * horizon_boost * text_boost;
    }
    Ok(score.clamp(0.0, 1.0))
}

fn compute_reachability_score(account_id: &str, db: &Connection) -> Result<f64, String> {
    let mut stmt = db
        .prepare(
            "SELECT cm.channel_type, COALESCE(cm.classification, ''), COALESCE(c.full_name, ''),
                    COALESCE(c.title, ''), c.title_confidence
             FROM contacts c
             LEFT JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1",
        )
        .map_err(|e| format!("Failed to prepare reachability query: {e}"))?;
    let mut rows = stmt
        .query(params![account_id])
        .map_err(|e| format!("Failed to query reachability state: {e}"))?;
    let mut has_personal_email = false;
    let mut has_linkedin = false;
    let mut has_phone = false;
    let mut has_real_name = false;
    let mut has_verified_title = false;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("Failed to read reachability state: {e}"))?
    {
        let channel = row
            .get::<_, Option<String>>(0)
            .unwrap_or_default()
            .unwrap_or_default();
        let classification = row
            .get::<_, Option<String>>(1)
            .unwrap_or_default()
            .unwrap_or_default();
        let full_name = row
            .get::<_, Option<String>>(2)
            .unwrap_or_default()
            .unwrap_or_default();
        let title = row
            .get::<_, Option<String>>(3)
            .unwrap_or_default()
            .unwrap_or_default();
        let title_confidence = row
            .get::<_, Option<f64>>(4)
            .unwrap_or_default()
            .unwrap_or(0.0);
        if channel == "email" && classification == "personal" {
            has_personal_email = true;
        }
        if channel == "linkedin" {
            has_linkedin = true;
        }
        if channel == "phone" {
            has_phone = true;
        }
        if !contact_name_is_placeholder(Some(full_name.as_str())) {
            has_real_name = true;
        }
        if !contact_title_is_generic_default(Some(title.as_str())) && title_confidence >= 0.6 {
            has_verified_title = true;
        }
    }
    let reach: f64 = (if has_personal_email { 0.35 } else { 0.0 })
        + (if has_linkedin { 0.25 } else { 0.0 })
        + (if has_phone { 0.2 } else { 0.0 })
        + (if has_real_name { 0.1 } else { 0.0 })
        + (if has_verified_title { 0.1 } else { 0.0 });
    Ok(reach.clamp(0.0, 1.0))
}

fn compute_deliverability_risk(account_id: &str, db: &Connection) -> Result<f64, String> {
    let hard_bounces = db
        .query_row(
            "SELECT COUNT(*)
             FROM outcomes o
             JOIN touches t ON t.id = o.touch_id
             JOIN sequence_instances si ON si.id = t.sequence_instance_id
             WHERE si.account_id = ?1 AND o.outcome_type = 'hard_bounce'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let domain_risk = db
        .query_row(
            "SELECT MAX(CASE WHEN COALESCE(mx_valid, 0) = 1 THEN 0.1 ELSE 0.45 END)
             FROM domains WHERE account_id = ?1",
            params![account_id],
            |row| row.get::<_, Option<f64>>(0),
        )
        .unwrap_or(Some(0.45))
        .unwrap_or(0.45);
    let generic_email = db
        .query_row(
            "SELECT COUNT(*)
             FROM contacts c
             JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1 AND cm.channel_type = 'email' AND COALESCE(cm.classification, '') != 'personal'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let sender_risk = db
        .query_row(
            "SELECT COALESCE(warm_state, 'cold') FROM sender_policies ORDER BY rowid DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| format!("Failed to load sender policy: {e}"))?
        .map(|state| if state == "ready" { 0.05 } else { 0.15 })
        .unwrap_or(0.15);
    let risk: f64 = (hard_bounces as f64 * 0.15).min(0.3)
        + domain_risk.min(0.3)
        + if generic_email > 0 { 0.2 } else { 0.0 }
        + sender_risk;
    Ok(risk.clamp(0.0, 1.0))
}

fn compute_compliance_risk(account_id: &str, db: &Connection) -> Result<f64, String> {
    let suppressed = db
        .query_row(
            "SELECT COUNT(*)
             FROM contacts c
             JOIN contact_methods cm ON cm.contact_id = c.id
             WHERE c.account_id = ?1 AND COALESCE(cm.suppressed, 0) = 1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let opt_outs = db
        .query_row(
            "SELECT COUNT(*)
             FROM outcomes o
             JOIN touches t ON t.id = o.touch_id
             JOIN sequence_instances si ON si.id = t.sequence_instance_id
             WHERE si.account_id = ?1 AND o.outcome_type = 'unsubscribe'",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    let (geo, generic_email_only) = db
        .query_row(
            "SELECT COALESCE(a.geo, ''),
                    CASE WHEN EXISTS(
                        SELECT 1
                        FROM contacts c
                        JOIN contact_methods cm ON cm.contact_id = c.id
                        WHERE c.account_id = a.id
                          AND cm.channel_type = 'email'
                          AND COALESCE(cm.classification, '') != 'personal'
                    ) THEN 1 ELSE 0 END
             FROM accounts a
             WHERE a.id = ?1",
            params![account_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(|e| format!("Failed to load compliance state: {e}"))?;
    let kvkk_risk = if geo_is_turkey(&geo) && generic_email_only == 1 {
        0.3
    } else if geo_is_turkey(&geo) {
        0.15
    } else {
        0.05
    };
    let risk: f64 = (if suppressed > 0 { 0.4 } else { 0.0 })
        + (if opt_outs > 0 { 0.3 } else { 0.0 })
        + kvkk_risk;
    Ok(risk.clamp(0.0, 1.0))
}

fn save_score_snapshot(
    db: &Connection,
    account_id: &str,
    score: &FiveAxisScore,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO score_snapshots
         (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk,
          activation_priority, computed_at, scoring_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'v1')
         ON CONFLICT(id) DO UPDATE SET
            fit_score = excluded.fit_score,
            intent_score = excluded.intent_score,
            reachability_score = excluded.reachability_score,
            deliverability_risk = excluded.deliverability_risk,
            compliance_risk = excluded.compliance_risk,
            activation_priority = excluded.activation_priority,
            computed_at = excluded.computed_at",
        params![
            stable_sales_id("score_snapshot", &[account_id]),
            account_id,
            score.fit_score,
            score.intent_score,
            score.reachability_score,
            score.deliverability_risk,
            score.compliance_risk,
            activation_priority(score),
            Utc::now().to_rfc3339(),
        ],
    )
    .map_err(|e| format!("Failed to save score snapshot: {e}"))?;
    Ok(())
}

fn compute_five_axis_score(account_id: &str, db: &Connection) -> Result<FiveAxisScore, String> {
    let score = FiveAxisScore {
        fit_score: compute_fit_score(account_id, db)?,
        intent_score: compute_intent_score(account_id, db)?,
        reachability_score: compute_reachability_score(account_id, db)?,
        deliverability_risk: compute_deliverability_risk(account_id, db)?,
        compliance_risk: compute_compliance_risk(account_id, db)?,
    };
    save_score_snapshot(db, account_id, &score)?;
    Ok(score)
}

fn activation_priority(score: &FiveAxisScore) -> f64 {
    ((score.fit_score * 0.35)
        + (score.intent_score * 0.25)
        + (score.reachability_score * 0.25)
        + ((1.0 - score.deliverability_risk) * 0.1)
        + ((1.0 - score.compliance_risk) * 0.05))
        .clamp(0.0, 1.0)
}

fn thesis_confidence(score: &FiveAxisScore) -> f64 {
    ((score.fit_score
        + score.intent_score
        + score.reachability_score
        + (1.0 - score.deliverability_risk)
        + (1.0 - score.compliance_risk))
        / 5.0)
        .clamp(0.0, 1.0)
}

fn recommended_activation_channel(
    db: &Connection,
    account_id: &str,
    contact_id: &str,
) -> Option<String> {
    let mut stmt = db
        .prepare(
            "SELECT channel_type, COALESCE(classification, '')
             FROM contact_methods
             WHERE contact_id = ?1
             ORDER BY confidence DESC",
        )
        .ok()?;
    let methods = stmt
        .query_map(params![contact_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let has_personal_email = methods
        .iter()
        .any(|(channel, classification)| channel == "email" && classification == "personal");
    let has_phone = methods.iter().any(|(channel, _)| channel == "phone");
    let has_linkedin = methods.iter().any(|(channel, _)| channel == "linkedin");
    if has_personal_email {
        Some("email".to_string())
    } else if has_phone {
        Some("phone_task".to_string())
    } else if has_linkedin {
        Some("linkedin_assist".to_string())
    } else {
        let account_has_any_method = db
            .query_row(
                "SELECT COUNT(*)
                 FROM contacts c
                 JOIN contact_methods cm ON cm.contact_id = c.id
                 WHERE c.account_id = ?1",
                params![account_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);
        if account_has_any_method > 0 {
            Some("research".to_string())
        } else {
            None
        }
    }
}

fn send_gate(score: &FiveAxisScore) -> SendGateDecision {
    if score.deliverability_risk > 0.7 {
        return SendGateDecision::Block {
            reason: "Deliverability risk too high".to_string(),
        };
    }
    if score.compliance_risk > 0.5 {
        return SendGateDecision::Block {
            reason: "Compliance risk too high".to_string(),
        };
    }
    if score.reachability_score < 0.3 {
        return SendGateDecision::Research {
            missing: vec!["Need personal email or LinkedIn profile".to_string()],
        };
    }
    if score.intent_score < 0.2 {
        return SendGateDecision::Nurture {
            reason: "No active intent signals detected".to_string(),
        };
    }
    if score.fit_score > 0.5 && score.reachability_score > 0.4 {
        return SendGateDecision::Activate;
    }
    SendGateDecision::Research {
        missing: vec!["Need more data to make decision".to_string()],
    }
}

fn assign_tier(score: &FiveAxisScore) -> &'static str {
    if score.fit_score > 0.8 && score.intent_score > 0.5 {
        "a_tier"
    } else if score.fit_score > 0.5 {
        "standard"
    } else {
        "basic"
    }
}

fn decode_percent_utf8_lossy(raw: &str) -> String {
    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                out.push((high << 4) | low);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn canonicalize_osint_url(raw: &str) -> Option<String> {
    let trimmed = decode_basic_html_entities(raw).trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    let mut parsed = url::Url::parse(&trimmed).ok()?;
    parsed.set_fragment(None);
    parsed.set_query(None);

    let host = parsed.host_str()?.trim_end_matches('.').to_lowercase();
    parsed.set_host(Some(&host)).ok()?;

    let original_path = parsed.path().to_string();
    let had_trailing_slash = original_path.len() > 1 && original_path.ends_with('/');
    let decoded_path = decode_percent_utf8_lossy(&original_path);
    let mut normalized_path = if decoded_path.trim().is_empty() {
        "/".to_string()
    } else if decoded_path.starts_with('/') {
        decoded_path
    } else {
        format!("/{decoded_path}")
    };
    while normalized_path.contains("//") {
        normalized_path = normalized_path.replace("//", "/");
    }
    if normalized_path.len() > 1 {
        normalized_path = normalized_path.trim_end_matches('/').to_string();
        if had_trailing_slash {
            normalized_path.push('/');
        }
    }
    parsed.set_path(&normalized_path);

    Some(parsed.to_string())
}

fn absolutize_source_link(base: &str, raw_href: &str) -> Option<String> {
    let href = decode_basic_html_entities(raw_href).trim().to_string();
    if href.is_empty() {
        return None;
    }
    if let Ok(url) = url::Url::parse(&href) {
        return canonicalize_osint_url(url.as_str()).or_else(|| Some(url.to_string()));
    }
    let base = url::Url::parse(base).ok()?;
    base.join(&href)
        .ok()
        .and_then(|url| canonicalize_osint_url(url.as_str()).or_else(|| Some(url.to_string())))
}

fn osint_source_priority(raw: &str) -> i32 {
    let Some(canonical) = canonicalize_osint_url(raw) else {
        return 0;
    };
    let Ok(parsed) = url::Url::parse(&canonical) else {
        return 0;
    };
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    let path = parsed.path().to_lowercase();
    let normalized_path = path.trim_matches('/');
    if host.ends_with("tmb.org.tr") {
        if path.starts_with("/en/m/") || path.starts_with("/tr/m/") {
            return 6;
        }
        if path == "/en/members" || path == "/tr/uyeler" {
            return 2;
        }
    }
    if host.ends_with("mib.org.tr") {
        if path.contains("/firm/") {
            return 6;
        }
        if path.contains("/our-members") {
            return 2;
        }
    }
    if host.ends_with("imder.org.tr") || host.ends_with("isder.org.tr") {
        if normalized_path.is_empty() {
            return 0;
        }
        if normalized_path == "uyelerimiz" {
            return 2;
        }
        if path.contains("/uyelerimiz/") || !normalized_path.starts_with("uyelerimiz") {
            return 6;
        }
        if normalized_path.starts_with("uyelerimiz") {
            return 2;
        }
    }
    if host.ends_with("asmud.org.tr") && path.contains("uyeler.asp") {
        return 2;
    }
    if host.ends_with("platformder.org.tr") && path.contains("/rehber/") {
        return 2;
    }
    if host.ends_with("thbb.org") && path.contains("yazismali-uyeler") {
        return 2;
    }
    if host.ends_with("eder.org.tr")
        && (path.contains("/uyelerimiz") || path.contains("/our-members"))
    {
        return 2;
    }
    if host.ends_with("lojider.org.tr")
        && (path.contains("/member-list") || path.contains("/uye-listesi"))
    {
        return 2;
    }
    if host.ends_with("tfyd.org.tr") && path.contains("/uyelerimiz") {
        return 2;
    }
    if host.ends_with("oss.org.tr") && (path.contains("/members") || path.contains("/uyeler")) {
        return 2;
    }
    0
}

fn osint_link_priority(raw: &str) -> i32 {
    let canonical = canonicalize_osint_url(raw).unwrap_or_else(|| raw.trim().to_string());
    let lower = canonical.to_lowercase();
    if normalize_outreach_linkedin_url(raw).is_some() {
        30
    } else if normalize_company_linkedin_url(raw).is_some() {
        24
    } else if lower.starts_with("http://") || lower.starts_with("https://") {
        enrich_link_priority(&canonical) + 6 + osint_source_priority(&canonical)
    } else {
        0
    }
}

fn site_link_category(path: &str) -> Option<&'static str> {
    let normalized = decode_percent_utf8_lossy(path).to_lowercase();
    if normalized.trim_matches('/').is_empty() {
        return Some("root");
    }
    if normalized.contains("icra-kurulu")
        || normalized.contains("ust-yonetim")
        || normalized.contains("executive-team")
        || normalized.contains("leadership")
        || normalized.contains("senior-management")
        || normalized.contains("yonetim-kurulu")
        || normalized.contains("management")
        || normalized.contains("executive")
        || normalized.contains("team")
        || normalized.contains("yonetim")
    {
        return Some("leadership");
    }
    if normalized.contains("about")
        || normalized.contains("hakkimizda")
        || normalized.contains("kurumsal")
        || normalized.contains("company")
    {
        return Some("about");
    }
    if normalized.contains("contact") || normalized.contains("iletisim") {
        return Some("contact");
    }
    if normalized.contains("projects")
        || normalized.contains("project")
        || normalized.contains("projeler")
        || normalized.contains("proje")
    {
        return Some("projects");
    }
    if normalized.contains("services")
        || normalized.contains("service")
        || normalized.contains("solutions")
        || normalized.contains("industries")
        || normalized.contains("industry")
        || normalized.contains("sectors")
        || normalized.contains("hizmetler")
        || normalized.contains("hizmet")
        || normalized.contains("cozumler")
        || normalized.contains("sektorler")
    {
        return Some("operations");
    }
    None
}

fn osint_link_cluster_key(raw: &str) -> Option<String> {
    let canonical = canonicalize_osint_url(raw)?;
    let parsed = url::Url::parse(&canonical).ok()?;
    let host = parsed
        .host_str()
        .unwrap_or("")
        .trim_start_matches("www.")
        .to_lowercase();
    let path = parsed.path().to_lowercase();
    let normalized_path = path.trim_matches('/');

    if host.ends_with("tmb.org.tr") && (path.starts_with("/en/m/") || path.starts_with("/tr/m/")) {
        return Some("tmb_member_directory".to_string());
    }
    if host.ends_with("tmb.org.tr") && (path == "/en/members" || path == "/tr/uyeler") {
        return Some("tmb_member_directory".to_string());
    }
    if host.ends_with("mib.org.tr") && (path.contains("/firm/") || path.contains("/our-members")) {
        return Some("mib_member_directory".to_string());
    }
    if host.ends_with("imder.org.tr") && !normalized_path.is_empty() {
        return Some("imder_member_directory".to_string());
    }
    if host.ends_with("isder.org.tr") && !normalized_path.is_empty() {
        return Some("isder_member_directory".to_string());
    }
    if host.ends_with("asmud.org.tr") && path.contains("uyeler.asp") {
        return Some("asmud_member_directory".to_string());
    }
    if host.ends_with("platformder.org.tr") && path.contains("/rehber/") {
        return Some("platformder_directory".to_string());
    }
    if host.ends_with("thbb.org") && path.contains("yazismali-uyeler") {
        return Some("thbb_directory".to_string());
    }
    if host.ends_with("eder.org.tr")
        && (path.contains("/uyelerimiz") || path.contains("/our-members"))
    {
        return Some("eder_member_directory".to_string());
    }
    if host.ends_with("lojider.org.tr")
        && (path.contains("/member-list") || path.contains("/uye-listesi"))
    {
        return Some("lojider_member_directory".to_string());
    }
    if host.ends_with("tfyd.org.tr") && path.contains("/uyelerimiz") {
        return Some("tfyd_member_directory".to_string());
    }
    if host.ends_with("oss.org.tr") && (path.contains("/members") || path.contains("/uyeler")) {
        return Some("oss_member_directory".to_string());
    }
    if normalize_company_linkedin_url(&canonical).is_some() {
        return Some(format!("linkedin_company:{host}:{}", parsed.path()));
    }
    if normalize_outreach_linkedin_url(&canonical).is_some() {
        return Some(format!("linkedin_personal:{host}:{}", parsed.path()));
    }
    if let Some(category) = site_link_category(&path) {
        return Some(format!("site:{host}:{category}"));
    }
    None
}

fn merge_osint_links(existing: Vec<String>, extra: Vec<String>) -> Vec<String> {
    let mut merged = dedupe_strings(
        existing
            .into_iter()
            .chain(extra)
            .filter_map(|value| {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    return None;
                }
                Some(canonicalize_osint_url(&trimmed).unwrap_or(trimmed))
            })
            .filter(|value| !value.is_empty())
            .collect(),
    );
    merged.sort_by(|a, b| {
        osint_link_priority(b)
            .cmp(&osint_link_priority(a))
            .then_with(|| b.len().cmp(&a.len()))
            .then_with(|| a.cmp(b))
    });
    let mut filtered = Vec::with_capacity(merged.len());
    let mut seen_clusters = std::collections::HashSet::new();
    for link in merged {
        if let Some(cluster) = osint_link_cluster_key(&link) {
            if !seen_clusters.insert(cluster) {
                continue;
            }
        }
        filtered.push(link);
        if filtered.len() >= MAX_OSINT_LINKS_PER_PROSPECT {
            break;
        }
    }
    filtered
}

fn lead_has_outreach_channel(email: Option<&String>, linkedin_url: Option<&String>) -> bool {
    email
        .map(String::as_str)
        .map(email_is_actionable_outreach_email)
        .unwrap_or(false)
        || linkedin_url
            .and_then(|value| normalize_outreach_linkedin_url(value))
            .is_some()
}

fn lead_has_verified_company_signal(
    is_field_ops: bool,
    site_evidence: Option<&str>,
    llm_validated: bool,
) -> bool {
    if !is_field_ops {
        return true;
    }
    llm_validated
        || site_evidence
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
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
        if let Some(m) = re.find_iter(source).next() {
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
    let decoded = decode_basic_html_entities(&decode_html_email_entities(&no_tags));
    truncate_cleaned_text(&decoded, 20_000)
}

fn html_to_structured_text(text: &str) -> String {
    let no_script = regex_lite::Regex::new(r"(?is)<script[^>]*>.*?</script>")
        .unwrap()
        .replace_all(text, " ");
    let no_style = regex_lite::Regex::new(r"(?is)<style[^>]*>.*?</style>")
        .unwrap()
        .replace_all(&no_script, " ");
    let with_breaks = regex_lite::Regex::new(
        r"(?is)</?(?:p|div|section|article|header|footer|aside|li|ul|ol|h[1-6]|strong|b|span|a|tr|td|th|br)[^>]*>",
    )
    .unwrap()
    .replace_all(&no_style, "\n");
    let no_tags = regex_lite::Regex::new(r"(?is)<[^>]+>")
        .unwrap()
        .replace_all(&with_breaks, " ");
    let decoded = decode_basic_html_entities(&decode_html_email_entities(&no_tags));
    decoded
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn enrich_link_priority(raw_href: &str) -> i32 {
    let path = if let Ok(parsed) = url::Url::parse(raw_href) {
        parsed.path().to_lowercase()
    } else {
        raw_href
            .split('?')
            .next()
            .unwrap_or(raw_href)
            .split('#')
            .next()
            .unwrap_or(raw_href)
            .to_lowercase()
    };
    let mut score = if path.contains("icra-kurulu")
        || path.contains("ust-yonetim")
        || path.contains("executive-team")
        || path.contains("leadership")
        || path.contains("senior-management")
    {
        16
    } else if path.contains("yonetim-kurulu")
        || path.contains("management")
        || path.contains("executive")
        || path.contains("team")
    {
        12
    } else if path.contains("yonetim") || path.contains("about") || path.contains("hakkimizda") {
        8
    } else if path.contains("contact") || path.contains("iletisim") {
        7
    } else {
        0
    };

    let generic_segments = [
        "about",
        "team",
        "leadership",
        "management",
        "executive",
        "executive-team",
        "senior-management",
        "contact",
        "hakkimizda",
        "kurumsal",
        "yonetim",
        "yonetim-kurulu",
        "icra-kurulu",
        "ust-yonetim",
        "iletisim",
    ];
    if let Some(last_segment) = path.trim_matches('/').rsplit('/').next() {
        let slug_token_count = last_segment
            .split('-')
            .filter(|part| !part.is_empty())
            .filter(|part| part.chars().all(|c| c.is_ascii_alphabetic()))
            .filter(|part| !generic_segments.contains(part))
            .count();
        if slug_token_count >= 2 {
            score += 6;
        }
    }

    if raw_href.contains('#') {
        score -= 5;
    }
    score
}

fn path_looks_like_enrich_target(path: &str) -> bool {
    let normalized = path.trim().to_lowercase();
    [
        "about",
        "team",
        "leadership",
        "management",
        "executive",
        "contact",
        "services",
        "service",
        "solutions",
        "industries",
        "industry",
        "sectors",
        "projects",
        "project",
        "engineering",
        "infrastructure",
        "maintenance",
        "facility",
        "construction",
        "installation",
        "field-service",
        "dispatch",
        "hakkimizda",
        "kurumsal",
        "ekip",
        "yonetim",
        "iletisim",
        "hizmetler",
        "hizmet",
        "cozumler",
        "sektorler",
        "bakim",
        "tesis",
        "muhendislik",
        "altyapi",
        "santiye",
        "insaat",
        "taahhut",
        "projeler",
    ]
    .iter()
    .any(|kw| normalized.contains(kw))
}

fn hosts_match_for_site_enrich(left: &str, right: &str) -> bool {
    let normalize = |host: &str| host.trim().trim_end_matches('.').to_lowercase();
    let left = normalize(left);
    let right = normalize(right);
    if left == right {
        return true;
    }

    let left_stripped = left.strip_prefix("www.").unwrap_or(&left);
    let right_stripped = right.strip_prefix("www.").unwrap_or(&right);
    left_stripped == right_stripped
}

fn extract_internal_enrich_links(base_url: &url::Url, html: &str) -> Vec<String> {
    let href_re = regex_lite::Regex::new(r#"(?is)href\s*=\s*["']([^"']+)["']"#).unwrap();

    let mut ranked = Vec::<(i32, String)>::new();
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
        let host_ok = abs
            .host_str()
            .zip(base_url.host_str())
            .map(|(left, right)| hosts_match_for_site_enrich(left, right))
            .unwrap_or(false);
        if !host_ok {
            continue;
        }
        let path = abs.path().to_lowercase();
        if !path_looks_like_enrich_target(&path) {
            continue;
        }
        let key = canonicalize_osint_url(abs.as_str()).unwrap_or_else(|| abs.as_str().to_string());
        if seen.insert(key.clone()) {
            ranked.push((enrich_link_priority(&key), key));
        }
    }
    ranked.sort_by(|(score_a, url_a), (score_b, url_b)| {
        score_b
            .cmp(score_a)
            .then_with(|| url_b.len().cmp(&url_a.len()))
            .then_with(|| url_a.cmp(url_b))
    });
    ranked
        .into_iter()
        .map(|(_, url)| url)
        .take(MAX_EXTRA_SITE_ENRICH_PAGES)
        .collect()
}

fn extract_sitemap_locations(xml: &str) -> Vec<String> {
    let loc_re = regex_lite::Regex::new(r"(?is)<loc>\s*([^<\s]+)\s*</loc>").unwrap();
    loc_re
        .captures_iter(xml)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().trim().to_string()))
        .collect()
}

async fn fetch_sitemap_enrich_links(client: &reqwest::Client, base_url: &url::Url) -> Vec<String> {
    let base_host = base_url.host_str().unwrap_or("").to_lowercase();
    let mut queue = Vec::<String>::new();
    let mut queued = HashSet::<String>::new();
    let mut out = Vec::<String>::new();
    let mut seen_pages = HashSet::<String>::new();
    let mut seen_sitemaps = HashSet::<String>::new();

    if let Ok(robots_url) = base_url.join("/robots.txt") {
        if let Ok(resp) = client.get(robots_url).send().await {
            if let Ok(body) = resp.text().await {
                for line in body.lines() {
                    let trimmed = line.trim();
                    if !trimmed.to_lowercase().starts_with("sitemap:") {
                        continue;
                    }
                    let sitemap = trimmed
                        .split_once(':')
                        .map(|(_, value)| value.trim().to_string())
                        .unwrap_or_default();
                    if !sitemap.is_empty() && queued.insert(sitemap.clone()) {
                        queue.push(sitemap);
                    }
                }
            }
        }
    }

    for fallback in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"] {
        if let Ok(url) = base_url.join(fallback) {
            let key = url.to_string();
            if queued.insert(key.clone()) {
                queue.push(key);
            }
        }
    }

    while let Some(sitemap_url) = queue.pop() {
        if !seen_sitemaps.insert(sitemap_url.clone()) || seen_sitemaps.len() > 6 {
            continue;
        }
        let resp = match client.get(&sitemap_url).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !resp.status().is_success() {
            continue;
        }
        let body = match resp.text().await {
            Ok(text) => text,
            Err(_) => continue,
        };
        if body.trim().is_empty() {
            continue;
        }

        for loc in extract_sitemap_locations(&body) {
            let Ok(parsed) = url::Url::parse(&loc) else {
                continue;
            };
            let same_host = parsed
                .host_str()
                .map(|host| hosts_match_for_site_enrich(host, &base_host))
                .unwrap_or(false);
            if !same_host {
                continue;
            }

            if loc.ends_with(".xml") || body.to_lowercase().contains("<sitemapindex") {
                if queued.insert(loc.clone()) {
                    queue.push(loc);
                }
                continue;
            }

            if !path_looks_like_enrich_target(parsed.path()) {
                continue;
            }

            let normalized = canonicalize_osint_url(&loc).unwrap_or_else(|| loc.trim().to_string());
            if seen_pages.insert(normalized.clone()) {
                out.push(normalized);
            }
            if out.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
                return out;
            }
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
        "/Hakkinda/Yonetim",
        "/Hakkinda/Yonetim-Kurulu",
        "/kurumsal",
        "/kurumsal/yonetim",
        "/kurumsal/yonetim-kurulu",
        "/kurumsal/yonetim-kurulu-ve-ust-yonetim",
        "/kurumsal/ust-yonetim",
        "/hakkimizda/yonetim",
        "/hakkimizda/yonetim/yonetim-kurulu",
        "/hakkimizda/yonetim/icra-kurulu",
        "/tr/kurumsal/hakkimizda/yonetim",
        "/tr/kurumsal/hakkimizda/yonetim-kurulu",
        "/yonetim",
        "/iletisim",
        "/bize-ulasin",
        "/ekibimiz",
        "/referanslarimiz",
        "/projelerimiz",
        "/haberler",
        "/duyurular",
        "/en/management",
        "/en/team",
        "/en/contact",
    ];
    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for path in defaults {
        if let Ok(url) = base_url.join(path) {
            let key =
                canonicalize_osint_url(url.as_str()).unwrap_or_else(|| url.as_str().to_string());
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

async fn fetch_html_page(client: &reqwest::Client, url: &str, timeout_ms: u64) -> Option<String> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let resp = client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let ctype = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !ctype.is_empty() && !ctype.contains("text/html") && !ctype.contains("application/xhtml")
        {
            return None;
        }
        let body = repair_common_mojibake_utf8(&resp.text().await.ok()?);
        if body.trim().is_empty() {
            return None;
        }
        Some(body)
    })
    .await
    .ok()
    .flatten()
}

async fn fetch_company_site_html_pages(client: &reqwest::Client, domain: &str) -> SiteFetchBundle {
    let candidates = [
        format!("https://{domain}"),
        format!("https://www.{domain}"),
        format!("http://{domain}"),
    ];
    for url in candidates {
        let Some(body) = fetch_html_page(client, &url, SITE_PAGE_FETCH_TIMEOUT_MS + 400).await
        else {
            continue;
        };
        if body.trim().is_empty() {
            continue;
        }
        let mut pages = vec![SiteHtmlPage {
            url: url.clone(),
            html: body.clone(),
        }];
        let mut osint_links = vec![url.clone()];
        if let Ok(base) = url::Url::parse(&url) {
            let links = select_company_site_enrich_links(
                &base,
                &body,
                fetch_sitemap_enrich_links(client, &base).await,
            );
            osint_links = merge_osint_links(osint_links, links.clone());
            let fetches = links.into_iter().map(|link| async move {
                let html = fetch_html_page(client, &link, SITE_PAGE_FETCH_TIMEOUT_MS).await;
                (link, html)
            });
            for (link, extra) in join_all(fetches).await {
                if let Some(extra) = extra {
                    if !extra.trim().is_empty() {
                        pages.push(SiteHtmlPage {
                            url: link,
                            html: extra,
                        });
                    }
                }
            }
        }
        return SiteFetchBundle { pages, osint_links };
    }
    SiteFetchBundle::default()
}

fn select_company_site_enrich_links(
    base_url: &url::Url,
    html: &str,
    sitemap_links: Vec<String>,
) -> Vec<String> {
    let mut discovered = extract_internal_enrich_links(base_url, html);
    let mut seen = discovered.iter().cloned().collect::<HashSet<_>>();
    for sitemap_link in sitemap_links {
        if seen.insert(sitemap_link.clone()) {
            discovered.push(sitemap_link);
        }
    }

    discovered.sort_by(|a, b| {
        enrich_link_priority(b)
            .cmp(&enrich_link_priority(a))
            .then_with(|| b.len().cmp(&a.len()))
            .then_with(|| a.cmp(b))
    });

    let mut selected = Vec::<String>::new();
    let mut selected_set = HashSet::<String>::new();
    let mut selected_clusters = HashSet::<String>::new();
    for link in discovered {
        let cluster = osint_link_cluster_key(&link);
        if let Some(cluster_key) = cluster.as_ref() {
            if selected_clusters.contains(cluster_key) {
                continue;
            }
        }
        if selected_set.insert(link.clone()) {
            if let Some(cluster_key) = cluster {
                selected_clusters.insert(cluster_key);
            }
            selected.push(link);
        }
        if selected.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            return selected;
        }
    }

    for fallback in default_internal_enrich_links(base_url) {
        let cluster = osint_link_cluster_key(&fallback);
        if let Some(cluster_key) = cluster.as_ref() {
            if selected_clusters.contains(cluster_key) {
                continue;
            }
        }
        if selected_set.insert(fallback.clone()) {
            if let Some(cluster_key) = cluster {
                selected_clusters.insert(cluster_key);
            }
            selected.push(fallback);
        }
        if selected.len() >= MAX_EXTRA_SITE_ENRICH_PAGES {
            break;
        }
    }

    selected
}

fn best_site_contact_enrichment(
    bundle: SiteFetchBundle,
    title_policy: &str,
) -> SiteContactEnrichment {
    let mut best = SiteContactEnrichment::default();
    let mut best_identity_signal = -1;
    let mut osint_links = bundle.osint_links.clone();
    let empty_headers = HashMap::new();
    let mut all_tech = Vec::new();

    for page in bundle.pages {
        osint_links.push(page.url.clone());
        // Tech stack detection (TASK-28)
        all_tech.extend(detect_tech_stack(&page.html, &empty_headers));
        if let Some(url) = extract_personal_linkedin_from_text(&page.html) {
            osint_links.push(url);
        }
        if let Some(url) = extract_company_linkedin_from_text(&page.html) {
            if best.company_linkedin_url.is_none() {
                best.company_linkedin_url = Some(url.clone());
            }
            osint_links.push(url);
        }
        let (name, title, linkedin_url, email, evidence) =
            extract_contact_from_company_site_html(&page.html, title_policy);
        let identity_signal =
            site_contact_identity_signal(name.as_ref(), title.as_ref(), linkedin_url.as_ref());
        if identity_signal > best_identity_signal {
            best_identity_signal = identity_signal;
            best.name = name;
            best.title = title;
            best.linkedin_url = linkedin_url;
        }
        if best.email.is_none() && email.is_some() {
            best.email = email;
        }
        if best.evidence.is_none() && evidence.is_some() {
            best.evidence = evidence;
        }
        best.signal = site_contact_candidate_signal(
            best.name.as_ref(),
            best.title.as_ref(),
            best.linkedin_url.as_ref(),
            best.email.as_ref(),
            best.evidence.as_ref(),
        );
        if best_identity_signal >= 20 && best.email.is_some() && best.evidence.is_some() {
            break;
        }
    }

    all_tech.sort();
    all_tech.dedup();
    best.tech_stack = all_tech;
    best.osint_links = merge_osint_links(Vec::new(), osint_links);
    best
}

async fn prefetch_site_contact_enrichments(
    client: &reqwest::Client,
    candidates: &[DomainCandidate],
    max_prefetch: usize,
    title_policy: &str,
) -> HashMap<String, SiteContactEnrichment> {
    let domains = dedupe_strings(
        candidates
            .iter()
            .map(|candidate| candidate.domain.clone())
            .take(max_prefetch)
            .collect(),
    );
    let title_policy = title_policy.to_string();

    join_all(domains.into_iter().map(|domain| {
        let title_policy = title_policy.clone();
        async move {
            let bundle = tokio::time::timeout(
                Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                fetch_company_site_html_pages(client, &domain),
            )
            .await
            .unwrap_or_default();
            Some((domain, best_site_contact_enrichment(bundle, &title_policy)))
        }
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
}

fn extract_contact_from_json_ld(
    html: &str,
    title_policy: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
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
        let normalized_raw = canonicalize_contact_titles(&raw);
        let jsonld_linkedin = extract_personal_linkedin_from_text(&normalized_raw)
            .or_else(|| extract_company_linkedin_from_text(&normalized_raw));
        let jsonld_email = extract_email_from_text(&normalized_raw);

        if let Some(c) = name_job_re.captures(&normalized_raw) {
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
                return (name, title, jsonld_linkedin, jsonld_email);
            }
        }

        if let Some(c) = job_name_re.captures(&normalized_raw) {
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
                return (name, title, jsonld_linkedin, jsonld_email);
            }
        }

        if let Some(c) = founder_re.captures(&normalized_raw) {
            let name = c
                .get(1)
                .and_then(|m| normalize_person_name(m.as_str().trim()));
            if name.is_some() && title_policy != "ceo_only" {
                return (
                    name,
                    Some("Founder".to_string()),
                    jsonld_linkedin,
                    jsonld_email,
                );
            }
        }

        if jsonld_linkedin.is_some() || jsonld_email.is_some() {
            return (None, None, jsonld_linkedin, jsonld_email);
        }
    }

    (None, None, None, None)
}

fn extract_contact_from_html_person_cards(
    html: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let decoded_html = decode_basic_html_entities(&decode_html_email_entities(html));
    let patterns = [
        r#"(?is)<strong[^>]*>\s*(?:<a[^>]*>)?\s*([^<]{3,120}?)\s*(?:</a>)?\s*</strong>\s*(?:<[^>]+>\s*){0,8}<span[^>]*>\s*([^<]{2,160}?)\s*</span>"#,
        r#"(?is)<h[1-6][^>]*>\s*(?:<a[^>]*>)?\s*([^<]{3,120}?)\s*(?:</a>)?\s*</h[1-6]>\s*(?:<[^>]+>\s*){0,8}<span[^>]*>\s*([^<]{2,160}?)\s*</span>"#,
        r#"(?is)<a[^>]*href\s*=\s*["'][^"']*(?:yonetim|ust-yonetim|leadership|management|executive)[^"']*["'][^>]*>\s*([^<]{3,120}?)\s*</a>\s*(?:<[^>]+>\s*){0,8}<(?:div|span|p)[^>]*>\s*([^<]{2,160}?)\s*</(?:div|span|p)>"#,
        r#"(?is)<button[^>]*accordion-button[^>]*>\s*<span>\s*([^<]{3,120}?)\s*[–-]\s*([^<]{2,160}?)\s*</span>\s*</button>"#,
    ];

    let mut best_score = -1;
    let mut best_name = None;
    let mut best_title = None;

    for pattern in patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        for cap in re.captures_iter(&decoded_html) {
            let raw_name = cap
                .get(1)
                .map(|m| m.as_str())
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let raw_title = cap
                .get(2)
                .map(|m| m.as_str())
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let Some(name) = normalize_person_name(&raw_name) else {
                continue;
            };
            let title = normalize_contact_title(&raw_title);
            let priority = contact_title_priority(&title);
            if priority <= 0 || !title_allowed_for_policy(title_policy, &title) {
                continue;
            }
            let score = 20 + priority;
            if score > best_score {
                best_score = score;
                best_name = Some(name);
                best_title = Some(title);
            }
        }
    }

    (best_name, best_title)
}

fn extract_contact_from_meta_tags(
    html: &str,
    title_policy: &str,
) -> (Option<String>, Option<String>) {
    let decoded_html = decode_basic_html_entities(&decode_html_email_entities(html));
    let title_patterns = [
        r#"(?is)<meta[^>]*property\s*=\s*["']og:title["'][^>]*content\s*=\s*["']([^"']{3,160})["']"#,
        r#"(?is)<title>\s*([^<]{3,160})\s*</title>"#,
    ];
    let description_patterns = [
        r#"(?is)<meta[^>]*name\s*=\s*["']description["'][^>]*content\s*=\s*["']([^"']{3,320})["']"#,
        r#"(?is)<meta[^>]*property\s*=\s*["']og:description["'][^>]*content\s*=\s*["']([^"']{3,320})["']"#,
    ];

    let mut title_text = None;
    for pattern in title_patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        if let Some(cap) = re.captures(&decoded_html) {
            title_text = cap.get(1).map(|m| {
                m.as_str()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .to_string()
            });
            if title_text.is_some() {
                break;
            }
        }
    }

    let mut description_text = None;
    for pattern in description_patterns {
        let Ok(re) = regex_lite::Regex::new(pattern) else {
            continue;
        };
        if let Some(cap) = re.captures(&decoded_html) {
            description_text = cap.get(1).map(|m| {
                m.as_str()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .to_string()
            });
            if description_text.is_some() {
                break;
            }
        }
    }

    let candidate_name = title_text
        .as_deref()
        .and_then(|value| value.split(" - ").next())
        .and_then(normalize_person_name);
    let Some(name) = candidate_name else {
        return (None, None);
    };

    let normalized_description = description_text
        .as_deref()
        .map(canonicalize_contact_titles)
        .unwrap_or_default();
    let lower_description = normalized_description.to_lowercase();
    let inferred_title =
        if lower_description.contains("founder") || lower_description.contains("kurucu") {
            Some("Founder".to_string())
        } else if lower_description.contains("ceo")
            || lower_description.contains("chief executive")
            || lower_description.contains("genel müdür")
            || lower_description.contains("genel mudur")
        {
            Some("CEO".to_string())
        } else if lower_description.contains("chairman")
            || lower_description.contains("başkanı")
            || lower_description.contains("baskani")
        {
            Some("Chairman".to_string())
        } else {
            None
        }
        .filter(|title| title_allowed_for_policy(title_policy, title));

    (Some(name), inferred_title)
}

type SiteHtmlContactExtraction = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn extract_contact_from_company_site_html(
    html: &str,
    title_policy: &str,
) -> SiteHtmlContactExtraction {
    let plain = strip_html_tags(html);
    let structured = html_to_structured_text(html);
    let canonical_plain = canonicalize_contact_titles(&plain);
    let canonical_structured = canonicalize_contact_titles(&structured);
    let lower_plain = canonical_plain.to_lowercase();
    let personal_linkedin_url = extract_personal_linkedin_from_text(html)
        .or_else(|| extract_personal_linkedin_from_text(&canonical_structured))
        .or_else(|| extract_personal_linkedin_from_text(&canonical_plain));
    let company_linkedin_url = extract_company_linkedin_from_text(html)
        .or_else(|| extract_company_linkedin_from_text(&canonical_structured))
        .or_else(|| extract_company_linkedin_from_text(&canonical_plain));
    let linkedin_url = personal_linkedin_url
        .clone()
        .or_else(|| company_linkedin_url.clone());
    let mut contact_name = personal_linkedin_url
        .as_deref()
        .and_then(extract_name_from_linkedin_url)
        .and_then(|n| normalize_person_name(&n));
    let mut contact_title = default_contact_title(title_policy);

    let (jsonld_name, jsonld_title, jsonld_linkedin, jsonld_email) =
        extract_contact_from_json_ld(html, title_policy);
    let (meta_name, meta_title) = extract_contact_from_meta_tags(html, title_policy);
    let (card_name, card_title) = extract_contact_from_html_person_cards(html, title_policy);
    let (parenthesized_name, parenthesized_title) =
        extract_contact_from_parenthesized_site_text(&canonical_structured, title_policy);
    let (inline_name, inline_title) =
        extract_contact_from_inline_site_text(&canonical_structured, title_policy);
    let (structured_name, structured_title) =
        extract_contact_from_structured_site_text(&canonical_structured, title_policy);

    apply_better_site_identity_candidate(
        jsonld_name,
        jsonld_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        meta_name,
        meta_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        card_name,
        card_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        parenthesized_name,
        parenthesized_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        inline_name,
        inline_title,
        &mut contact_name,
        &mut contact_title,
    );
    apply_better_site_identity_candidate(
        structured_name,
        structured_title,
        &mut contact_name,
        &mut contact_title,
    );
    let site_linkedin = linkedin_url.or(jsonld_linkedin);
    let mut site_email = normalize_site_contact_email(jsonld_email)
        .or_else(|| extract_email_from_text(&canonical_structured))
        .or_else(|| extract_email_from_text(&canonical_plain));

    if site_email.is_none() {
        site_email = extract_email_from_text(html);
    }
    let evidence = extract_company_site_signal(&lower_plain);

    (
        contact_name,
        contact_title,
        site_linkedin,
        site_email,
        evidence,
    )
}

fn guessed_email(contact_name: Option<&str>, domain: &str) -> Option<String> {
    guess_personal_email_patterns(contact_name, domain)
        .into_iter()
        .next()
}

/// Generate multiple candidate email patterns for a contact name + domain.
/// Each guess has confidence 0.3 (speculative). Caller should MX-verify domain.
fn guess_personal_email_patterns(contact_name: Option<&str>, domain: &str) -> Vec<String> {
    let name = match contact_name {
        Some(n) if !contact_name_is_placeholder(Some(n)) => n,
        _ => return Vec::new(),
    };
    let normalized = match normalize_person_name(name) {
        Some(n) => n,
        None => return Vec::new(),
    };
    let parts: Vec<&str> = normalized
        .split_whitespace()
        .filter(|p| p.chars().all(|c| c.is_alphabetic()))
        .collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Vec::new();
    }
    let first = transliterate_turkish_ascii(parts[0]);
    let last = transliterate_turkish_ascii(parts[parts.len() - 1]);
    let first_initial = first.chars().next().unwrap_or('x');

    let mut patterns = Vec::with_capacity(5);
    // Pattern 1: first.last@domain  (most common)
    patterns.push(format!("{first}.{last}@{domain}"));
    // Pattern 2: flast@domain
    patterns.push(format!("{first_initial}{last}@{domain}"));
    // Pattern 3: first@domain
    patterns.push(format!("{first}@{domain}"));
    // Pattern 4: f.last@domain
    patterns.push(format!("{first_initial}.{last}@{domain}"));
    // Pattern 5: firstlast@domain
    patterns.push(format!("{first}{last}@{domain}"));

    patterns.retain(|e| email_syntax_valid(e));
    patterns
}

fn lead_has_person_identity(contact_name: Option<&str>, linkedin_url: Option<&String>) -> bool {
    let has_real_name = contact_name
        .map(|value| !contact_name_is_placeholder(Some(value)))
        .unwrap_or(false);
    has_real_name
        || linkedin_url
            .and_then(|value| normalize_outreach_linkedin_url(value))
            .is_some()
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
    let home_dir = state.kernel.home_dir();
    let engine = SalesEngine::new(&home_dir);
    engine.init()?;
    Ok(engine)
}

fn sales_segment_from_query(query: Option<&str>) -> SalesSegment {
    SalesSegment::from_optional(query)
}

fn ensure_sqlite_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), String> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = conn
        .prepare(&pragma)
        .map_err(|e| format!("Failed to inspect {table} columns: {e}"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| format!("Failed to query {table} columns: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to decode {table} columns: {e}"))?;
    if columns.iter().any(|existing| existing == column) {
        return Ok(());
    }

    conn.execute(
        format!("ALTER TABLE {table} ADD COLUMN {column} {definition}").as_str(),
        [],
    )
    .map_err(|e| format!("Failed to add {column} to {table}: {e}"))?;
    Ok(())
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
