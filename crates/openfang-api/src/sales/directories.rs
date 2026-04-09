async fn fetch_tmb_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tmb.org.tr/en/members",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tmb_member_candidates(&html, profile, run_sequence, MAX_TMB_DIRECTORY_CANDIDATES)
}

fn parse_tmb_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let article_re = regex_lite::Regex::new(
        r#"(?is)<article[^>]*class="member-card[^"]*"[^>]*>(.*?)</article>"#,
    )
    .unwrap();
    let detail_re =
        regex_lite::Regex::new(r#"(?is)<div[^>]*class="name"[^>]*>\s*<a[^>]*href="([^"]+)""#)
            .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<div[^>]*class="name"[^>]*>\s*<a[^>]*>(.*?)</a>"#).unwrap();
    let chairman_re =
        regex_lite::Regex::new(r#"(?is)Chairman of the Board\s*:\s*<strong>(.*?)</strong>"#)
            .unwrap();
    let web_re = regex_lite::Regex::new(
        r#"(?is)<th[^>]*>\s*Web\s*</th>\s*<td[^>]*>\s*:\s*</td>\s*<td[^>]*>\s*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in article_re.captures_iter(html) {
        let article_html = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let detail_url = detail_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|href| absolutize_source_link("https://www.tmb.org.tr", &href));
        let web_href = web_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str().trim().to_string()));
        let Some(domain) = web_href.as_deref().and_then(extract_domain) else {
            continue;
        };
        if is_blocked_company_domain(&domain) || !seen.insert(domain.clone()) {
            continue;
        }

        let company = company_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_else(|| domain.clone());
        let chairman_name = chairman_re
            .captures(article_html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .and_then(|value| normalize_turkish_source_person_name(value.trim()));
        let email =
            normalize_directory_email_for_domain(extract_email_from_text(article_html), &domain);

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
                evidence: vec![format!(
                    "TMB members directory lists {} as a Turkish contractor member with website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(vec![
                    profile.target_industry.clone(),
                    "construction".to_string(),
                    "infrastructure".to_string(),
                    "contractor association".to_string(),
                ]),
                source_links: detail_url
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .chain(std::iter::once(
                        "https://www.tmb.org.tr/en/members".to_string(),
                    ))
                    .collect(),
                phone: None,
            },
            contact_hint: SourceContactHint {
                contact_name: chairman_name,
                contact_title: Some("Chairman".to_string()),
                email,
                source: Some("TMB members directory".to_string()),
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_eud_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.eud.org.tr/en/members",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_eud_member_candidates(&html, profile, run_sequence, MAX_EUD_DIRECTORY_CANDIDATES)
}

fn parse_eud_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let link_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href=['"]([^'"]+)['"][^>]*>\s*<div[^>]*class=['"][^'"]*\bmember-box\b[^'"]*['"]"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in link_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        let Some(domain) = extract_domain(&href) else {
            continue;
        };
        if domain == "eud.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![format!(
                    "EUD members page links {} as an official Turkish electricity producer site",
                    domain
                )],
                matched_keywords: dedupe_strings(vec![
                    profile.target_industry.clone(),
                    "energy".to_string(),
                    "utility".to_string(),
                    "power generation".to_string(),
                ]),
                source_links: vec!["https://www.eud.org.tr/en/members".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("EUD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_asmud_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.asmud.org.tr/Uyeler.asp",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_asmud_member_candidates(&html, profile, run_sequence, MAX_ASMUD_DIRECTORY_CANDIDATES)
}

fn parse_asmud_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let link_re = regex_lite::Regex::new(r#"(?is)<a[^>]*href="([^"]+)""#).unwrap();
    let company_re = regex_lite::Regex::new(r#"(?is)<strong>(.*?)</strong>"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)(?:^|<br>\s*)T:\s*([^<]+)"#).unwrap();
    let email_re =
        regex_lite::Regex::new(r#"(?is)(?:^|<br>\s*)E:\s*<span[^>]*>([^<]+)</span>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="uwrap w3-card">"#).skip(1) {
        let raw_url = link_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .unwrap_or_default();
        let Some(domain) = extract_domain(&raw_url) else {
            continue;
        };
        if domain == "asmud.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(segment)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| normalize_directory_email_for_domain(Some(value), &domain));

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "asphalt".to_string(),
            "road construction".to_string(),
            "infrastructure".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("inşaat") || company_lower.contains("insaat") {
            matched_keywords.push("construction".to_string());
        }
        if company_lower.contains("makine") || company_lower.contains("makina") {
            matched_keywords.push("construction equipment".to_string());
            matched_keywords.push("equipment".to_string());
        }
        if company_lower.contains("altyapı") || company_lower.contains("altyapi") {
            matched_keywords.push("field operations".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "ASMUD members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "ASMUD members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "ASMUD members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "ASMUD members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.asmud.org.tr/Uyeler.asp".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("ASMUD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_platformder_directory_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.platformder.org.tr/rehber/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_platformder_directory_candidates(
        &html,
        profile,
        run_sequence,
        MAX_PLATFORMDER_DIRECTORY_CANDIDATES,
    )
}

fn parse_platformder_directory_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let item_re = regex_lite::Regex::new(
        r#"(?is)<li[^>]*id="item-\d+-\d+"[^>]*\bdata-title="([^"]+)"[^>]*\bdata-phone="([^"]*)"[^>]*\bdata-url="([^"]*)"[^>]*>(.*?)</li>"#,
    )
    .unwrap();
    let site_link_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href=['"]([^'"]+)['"][^>]*title=['"]Go to website['"]"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in item_re.captures_iter(html) {
        let company = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if company.is_empty() {
            continue;
        }

        let phone = cap
            .get(2)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let raw_url = cap
            .get(3)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        let item_html = cap.get(4).map(|m| m.as_str()).unwrap_or("");

        let raw_domain = if raw_url.is_empty() {
            site_link_re
                .captures(item_html)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().trim().to_string()))
                .and_then(|href| extract_domain(&href))
                .or_else(|| {
                    extract_domains_from_text(item_html)
                        .into_iter()
                        .find(|domain| domain != "platformder.org.tr")
                })
        } else {
            extract_domain(&raw_url)
        };
        let Some(domain) = raw_domain else {
            continue;
        };
        if domain == "platformder.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "platform rental".to_string(),
            "equipment rental".to_string(),
            "field service".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("platform") {
            matched_keywords.push("access platform".to_string());
        }
        if company_lower.contains("vinç") || company_lower.contains("vinc") {
            matched_keywords.push("vinç".to_string());
            matched_keywords.push("crane".to_string());
        }
        if company_lower.contains("forklift") {
            matched_keywords.push("forklift".to_string());
        }
        if company_lower.contains("lift") {
            matched_keywords.push("lift".to_string());
        }
        if company_lower.contains("makine") {
            matched_keywords.push("equipment".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "Platformder rehber lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "Platformder rehber lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.platformder.org.tr/rehber/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("Platformder rehber".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_mib_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let pages = mib_directory_pages_for_run(
        run_sequence,
        MIB_DIRECTORY_PAGE_COUNT,
        MIB_DIRECTORY_PAGES_PER_RUN,
    );
    let page_results = join_all(pages.into_iter().map(|page| async move {
        let url = if page == 1 {
            "https://mib.org.tr/en/our-members/".to_string()
        } else {
            format!("https://mib.org.tr/en/our-members/{page}/")
        };
        let html = fetch_html_page(client, &url, FREE_DIRECTORY_FETCH_TIMEOUT_MS).await;
        (page, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (page, html) in page_results {
        let Some(html) = html else {
            continue;
        };
        let page_url = if page == 1 {
            "https://mib.org.tr/en/our-members/".to_string()
        } else {
            format!("https://mib.org.tr/en/our-members/{page}/")
        };
        for mut candidate in parse_mib_member_candidates(
            &html,
            profile,
            run_sequence + page.saturating_sub(1),
            MAX_MIB_DIRECTORY_CANDIDATES,
        ) {
            candidate.candidate.source_links = merge_osint_links(
                candidate.candidate.source_links.clone(),
                vec![page_url.clone()],
            );
            let domain_key = candidate.candidate.domain.to_lowercase();
            if !seen.insert(domain_key) {
                continue;
            }
            out.push(candidate);
        }
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_MIB_DIRECTORY_CANDIDATES);
    out
}

fn mib_directory_pages_for_run(
    run_sequence: usize,
    page_count: usize,
    pages_per_run: usize,
) -> Vec<usize> {
    if page_count == 0 || pages_per_run == 0 {
        return Vec::new();
    }

    let take = pages_per_run.min(page_count);
    let start = run_sequence % page_count;
    let mut out = Vec::with_capacity(take);
    for offset in 0..take {
        out.push(((start + offset) % page_count) + 1);
    }
    out
}

fn parse_mib_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>\s*</h2>"#)
            .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*href="([^"]+)"[^>]*>\s*<span[^>]*>\s*<i[^>]*class="[^"]*fa-globe[^"]*"[^>]*>"#,
    )
    .unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)href="mailto:([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html
        .split(r#"<div data-elementor-type="loop-item""#)
        .skip(1)
    {
        let block_html = format!(r#"<div data-elementor-type="loop-item"{segment}"#);
        if !block_html.contains("firm") {
            continue;
        }
        let detail_url = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|href| absolutize_source_link("https://mib.org.tr", &href));
        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(2).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if company.is_empty() {
            continue;
        }

        let website = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())));
        let Some(domain) = website.as_deref().and_then(extract_domain) else {
            continue;
        };
        if domain == "mib.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let block_text = strip_html_tags(&decode_basic_html_entities(&block_html));
        let source_text = format!("{company} {block_text}");
        let source_lower = source_text.to_lowercase();
        let looks_relevant = text_has_field_ops_signal(&source_text)
            || source_lower.contains("makine")
            || source_lower.contains("makina")
            || source_lower.contains("platform")
            || source_lower.contains("forklift")
            || source_lower.contains("lift")
            || source_lower.contains("vinc")
            || source_lower.contains("vinç");
        if !looks_relevant {
            continue;
        }

        let email = email_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| m.as_str().trim().to_string()))
            .and_then(|value| normalize_directory_email_for_domain(Some(value), &domain));

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "industrial equipment".to_string(),
            "field equipment".to_string(),
            "machinery association".to_string(),
        ];
        if source_lower.contains("makine") || source_lower.contains("makina") {
            matched_keywords.push("makine".to_string());
        }
        if source_lower.contains("platform") {
            matched_keywords.push("access platform".to_string());
        }
        if source_lower.contains("forklift") {
            matched_keywords.push("forklift".to_string());
        }
        if source_lower.contains("lift") {
            matched_keywords.push("lift".to_string());
        }
        if source_lower.contains("vinc") || source_lower.contains("vinç") {
            matched_keywords.push("vinç".to_string());
            matched_keywords.push("crane".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![format!(
                    "MIB members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: detail_url.into_iter().collect(),
                phone: None,
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("MIB members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_imder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(index_html) = fetch_html_page(
        client,
        "https://imder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    let detail_urls =
        parse_imder_member_index_urls(&index_html, run_sequence, MAX_IMDER_DETAIL_FETCHES);
    if detail_urls.is_empty() {
        return Vec::new();
    }

    let detail_pages = join_all(detail_urls.iter().map(|url| async move {
        let html = fetch_html_page(client, url, FREE_DIRECTORY_FETCH_TIMEOUT_MS + 400).await;
        (url, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (detail_url, html) in detail_pages {
        let Some(html) = html else {
            continue;
        };
        let Some(candidate) = parse_imder_member_detail_candidate(&html, detail_url, profile)
        else {
            continue;
        };
        let domain_key = candidate.candidate.domain.to_lowercase();
        if !seen.insert(domain_key) {
            continue;
        }
        out.push(candidate);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_IMDER_DIRECTORY_CANDIDATES);
    out
}

fn parse_imder_member_index_urls(html: &str, run_sequence: usize, max_urls: usize) -> Vec<String> {
    let detail_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*class="[^"]*\belementor-post__thumbnail__link\b[^"]*"[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for cap in detail_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        if href.is_empty() || !href.contains("imder.org.tr/") {
            continue;
        }
        if !seen.insert(href.clone()) {
            continue;
        }
        out.push(href);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_urls);
    out
}

fn parse_imder_member_detail_candidate(
    html: &str,
    detail_url: &str,
    profile: &SalesProfile,
) -> Option<FreeDiscoveryCandidate> {
    let title_re = regex_lite::Regex::new(
        r#"(?is)<h1[^>]*class="[^"]*elementor-heading-title[^"]*"[^>]*>(.*?)</h1>"#,
    )
    .unwrap();
    let name_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*İsim Soyisim\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let role_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*G(?:ö|o)revi\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>\s*Telefon\s*</strong>\s*</td>\s*<td>(.*?)</td>"#)
            .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web Sitesi\s*</strong>\s*</td>\s*<td>\s*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();
    let tag_re = regex_lite::Regex::new(r#"(?is)rel="tag">(.*?)</a>"#).unwrap();

    let company = title_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value| {
            strip_html_tags(&value)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())?;

    let raw_name = name_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_title = role_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let phone = phone_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(|value_html| {
            strip_html_tags(&decode_basic_html_entities(value_html))
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_site = website_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
        .or_else(|| {
            regex_lite::Regex::new(
                r#"(?is)<strong>\s*Web Sitesi\s*</strong>\s*</td>\s*<td>(.*?)</td>"#,
            )
            .unwrap()
            .captures(html)
            .and_then(|value| value.get(1).map(|m| m.as_str()))
            .map(|value_html| {
                strip_html_tags(&decode_basic_html_entities(value_html))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty())
        });

    let domain = raw_site
        .as_deref()
        .and_then(extract_domain)
        .filter(|domain| !is_blocked_company_domain(domain))?;

    let contact_name = raw_name
        .as_deref()
        .and_then(normalize_turkish_source_person_name)
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                normalize_person_name(&rewritten)
            })
        })
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                let token_count = rewritten.split_whitespace().count();
                let alpha_only = rewritten.split_whitespace().all(|token| {
                    token
                        .chars()
                        .all(|c| c.is_alphabetic() || matches!(c, '.' | '\'' | '-'))
                });
                if (2..=4).contains(&token_count) && alpha_only {
                    Some(rewritten)
                } else {
                    None
                }
            })
        })
        .or_else(|| raw_name.as_deref().and_then(normalize_person_name));
    let contact_title = raw_title.as_deref().map(normalize_contact_title);
    let email = normalize_directory_email_for_domain(extract_email_from_text(html), &domain);

    let mut matched_keywords = vec![
        profile.target_industry.clone(),
        "construction equipment".to_string(),
        "mobile equipment".to_string(),
        "field equipment".to_string(),
        "association detail".to_string(),
    ];
    for cap in tag_re.captures_iter(html) {
        let tag = cap
            .get(1)
            .map(|m| strip_html_tags(&decode_basic_html_entities(m.as_str())))
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty());
        if let Some(tag) = tag {
            matched_keywords.push(tag);
        }
    }

    let plain = strip_html_tags(&decode_basic_html_entities(html));
    let plain_lower = plain.to_lowercase();
    for signal in [
        "bakım",
        "bakim",
        "kurulum",
        "forklift",
        "ekskavator",
        "ekskavatör",
        "telehandler",
        "vinç",
        "vinc",
        "iş makinesi",
        "is makinesi",
        "mobil iş makine",
        "construction",
        "equipment",
    ] {
        if plain_lower.contains(signal) {
            matched_keywords.push(signal.to_string());
        }
    }

    let evidence = if let Some(phone) = phone.as_deref() {
        format!(
            "IMDER member detail lists {} with official website {} and public phone {}",
            truncate_text_for_reason(&company, 120),
            domain,
            phone
        )
    } else {
        format!(
            "IMDER member detail lists {} with official website {}",
            truncate_text_for_reason(&company, 120),
            domain
        )
    };

    Some(FreeDiscoveryCandidate {
        candidate: DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 14,
            evidence: vec![
                evidence,
                truncate_text_for_reason(&format!("Source detail: {}", detail_url), 220),
            ],
            matched_keywords: dedupe_strings(matched_keywords),
            source_links: vec![detail_url.to_string()],
            phone: phone.as_deref().and_then(normalize_phone),
        },
        contact_hint: SourceContactHint {
            contact_name,
            contact_title,
            email,
            source: Some("IMDER member detail".to_string()),
        },
    })
}

async fn fetch_isder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(index_html) = fetch_html_page(
        client,
        "https://isder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    let detail_urls =
        parse_isder_member_index_urls(&index_html, run_sequence, MAX_ISDER_DETAIL_FETCHES);
    if detail_urls.is_empty() {
        return Vec::new();
    }

    let detail_pages = join_all(detail_urls.iter().map(|url| async move {
        let html = fetch_html_page(client, url, FREE_DIRECTORY_FETCH_TIMEOUT_MS + 400).await;
        (url, html)
    }))
    .await;

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();
    for (detail_url, html) in detail_pages {
        let Some(html) = html else {
            continue;
        };
        let Some(candidate) = parse_isder_member_detail_candidate(&html, detail_url, profile)
        else {
            continue;
        };
        let domain_key = candidate.candidate.domain.to_lowercase();
        if !seen.insert(domain_key) {
            continue;
        }
        out.push(candidate);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(MAX_ISDER_DIRECTORY_CANDIDATES);
    out
}

fn parse_isder_member_index_urls(html: &str, run_sequence: usize, max_urls: usize) -> Vec<String> {
    let detail_re = regex_lite::Regex::new(
        r#"(?is)<a[^>]*class="[^"]*\belementor-post__thumbnail__link\b[^"]*"[^>]*href="([^"]+)""#,
    )
    .unwrap();

    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for cap in detail_re.captures_iter(html) {
        let href = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()).trim().to_string())
            .unwrap_or_default();
        if href.is_empty() || !href.contains("isder.org.tr/") {
            continue;
        }
        if !seen.insert(href.clone()) {
            continue;
        }
        out.push(href);
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_urls);
    out
}

fn parse_isder_member_detail_candidate(
    html: &str,
    detail_url: &str,
    profile: &SalesProfile,
) -> Option<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(
        r#"(?is)<h1[^>]*class="[^"]*elementor-heading-title[^"]*"[^>]*>(.*?)</h1>"#,
    )
    .unwrap();
    let name_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*İsim Soyisim:?\s*</strong>\s*</td>\s*<td>(.*?)</tr>"#,
    )
    .unwrap();
    let role_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Ünvan Görevi:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();
    let phone_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Telefon:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();
    let website_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web Sitesi:?\s*</strong>\s*</td>\s*<td>(.*?)</td>\s*</tr>"#,
    )
    .unwrap();

    let company = company_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value| {
            strip_html_tags(&value)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())?;

    let raw_name = name_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_title = role_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let phone = phone_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());
    let raw_site = website_re
        .captures(html)
        .and_then(|value| value.get(1).map(|m| m.as_str()))
        .map(decode_basic_html_entities)
        .map(|value_html| {
            strip_html_tags(&value_html)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty());

    let domain = raw_site
        .as_deref()
        .and_then(extract_domain)
        .filter(|domain| !is_blocked_company_domain(domain))?;
    let contact_name = raw_name
        .as_deref()
        .and_then(normalize_turkish_source_person_name)
        .or_else(|| {
            raw_name.as_deref().and_then(|value| {
                let rewritten = value
                    .split_whitespace()
                    .map(turkish_title_case_token)
                    .collect::<Vec<_>>()
                    .join(" ");
                normalize_person_name(&rewritten)
            })
        });
    let contact_title = raw_title.as_deref().and_then(|value| {
        let normalized = normalize_contact_title(value);
        if contact_title_priority(&normalized) > 0 {
            Some(normalized)
        } else {
            None
        }
    });
    let email = normalize_directory_email_for_domain(extract_email_from_text(html), &domain);

    let source_text = format!(
        "{company} {}",
        strip_html_tags(&decode_basic_html_entities(html))
    );
    let source_lower = source_text.to_lowercase();
    let mut matched_keywords = vec![
        profile.target_industry.clone(),
        "industrial equipment".to_string(),
        "field equipment".to_string(),
        "material handling".to_string(),
    ];
    if source_lower.contains("istif") {
        matched_keywords.push("material handling".to_string());
    }
    if source_lower.contains("forklift") {
        matched_keywords.push("forklift".to_string());
    }
    if source_lower.contains("platform") {
        matched_keywords.push("access platform".to_string());
    }
    if source_lower.contains("vinç") || source_lower.contains("vinc") {
        matched_keywords.push("vinç".to_string());
        matched_keywords.push("crane".to_string());
    }
    if source_lower.contains("raf") {
        matched_keywords.push("storage systems".to_string());
    }
    if source_lower.contains("akü") || source_lower.contains("aku") {
        matched_keywords.push("battery".to_string());
    }
    if let Some(signal) = extract_field_ops_signal_keyword(&source_text) {
        matched_keywords.push(signal.to_string());
    }

    let evidence = if let Some(phone) = phone.as_deref() {
        format!(
            "ISDER member detail lists {} with official website {} and public phone {}",
            truncate_text_for_reason(&company, 120),
            domain,
            phone
        )
    } else {
        format!(
            "ISDER member detail lists {} with official website {}",
            truncate_text_for_reason(&company, 120),
            domain
        )
    };

    Some(FreeDiscoveryCandidate {
        candidate: DomainCandidate {
            domain,
            score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
            evidence: vec![
                evidence,
                truncate_text_for_reason(&format!("Source detail: {}", detail_url), 220),
            ],
            matched_keywords: dedupe_strings(matched_keywords),
            source_links: vec![detail_url.to_string()],
            phone: phone.as_deref().and_then(normalize_phone),
        },
        contact_hint: SourceContactHint {
            contact_name,
            contact_title,
            email,
            source: Some("ISDER member detail".to_string()),
        },
    })
}

async fn fetch_thbb_yazismali_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.thbb.org/uyelerimiz/yazismali-uyeler/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_thbb_yazismali_candidates(&html, profile, run_sequence, MAX_THBB_DIRECTORY_CANDIDATES)
}

fn parse_thbb_yazismali_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let td_re = regex_lite::Regex::new(r#"(?is)<td>(.*?)</td>"#).unwrap();
    let web_re = regex_lite::Regex::new(
        r#"(?is)<strong>\s*Web:?\s*</strong>\s*(?:&nbsp;|\s)*<a[^>]*href="([^"]+)""#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>\s*Tel:?\s*</strong>\s*(?:&nbsp;|\s)*([^<]+)"#)
            .unwrap();
    let strong_re = regex_lite::Regex::new(r#"(?is)<strong>(.*?)</strong>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in td_re.captures_iter(html) {
        let td_html = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        if !td_html.contains("Web:") || !td_html.contains("E-posta") {
            continue;
        }

        let Some(domain) = web_re
            .captures(td_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "thbb.org"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let phone = phone_re
            .captures(td_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let company = strong_re
            .captures_iter(td_html)
            .filter_map(|value| value.get(1).map(|m| m.as_str()))
            .map(decode_basic_html_entities)
            .map(|value| {
                strip_html_tags(&value)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .find(|value| {
                let lower = value.to_lowercase();
                !value.is_empty()
                    && !lower.ends_with(':')
                    && lower != "adres"
                    && lower != "tel"
                    && lower != "faks"
                    && lower != "e-posta"
                    && lower != "web"
                    && lower != "web:"
            })
            .unwrap_or_else(|| domain.clone());

        let email = normalize_directory_email_for_domain(extract_email_from_text(td_html), &domain);
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "construction equipment".to_string(),
            "field equipment".to_string(),
            "concrete equipment".to_string(),
        ];
        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(td_html))
        );
        let source_lower = source_text.to_lowercase();
        if source_lower.contains("beton") || source_lower.contains("concrete") {
            matched_keywords.push("concrete plant".to_string());
            matched_keywords.push("ready mix concrete".to_string());
        }
        if source_lower.contains("mixer") || source_lower.contains("transmikser") {
            matched_keywords.push("mixer".to_string());
        }
        if source_lower.contains("pompa") || source_lower.contains("pump") {
            matched_keywords.push("pump".to_string());
        }
        if source_lower.contains("iş makine") || source_lower.contains("is makine") {
            matched_keywords.push("iş makinesi".to_string());
        }
        if source_lower.contains("makine") || source_lower.contains("makina") {
            matched_keywords.push("equipment".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "THBB yazismali uyeler page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "THBB yazismali uyeler page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.thbb.org/uyelerimiz/yazismali-uyeler/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("THBB yazismali uyeler".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_eder_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://eder.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_eder_member_candidates(&html, profile, run_sequence, MAX_EDER_DIRECTORY_CANDIDATES)
}

fn parse_eder_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let item_re = regex_lite::Regex::new(
        r#"(?is)<div class="ui-e-ico-box" onclick="window\.open\(&#039;([^&]+?)&#039;,\s*&#039;_blank&#039;\)">(.*?)<div class="ui-e-description">\s*<p>(.*?)</p>"#,
    )
    .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in item_re.captures_iter(html) {
        let raw_url = cap
            .get(1)
            .map(|m| decode_basic_html_entities(m.as_str()))
            .unwrap_or_default();
        let Some(domain) = extract_domain(&raw_url) else {
            continue;
        };
        if domain == "eder.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let description = cap
            .get(3)
            .map(|m| strip_html_tags(&decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let mut company = description.clone();
        for suffix in [
            " E-Ticaret Yazılımları",
            " E-Ticaret Yazilimlari",
            " Tahsilat Yazılımları",
            " Tahsilat Yazilimlari",
            " Yazılımları",
            " Yazilimlari",
        ] {
            if company.ends_with(suffix) {
                company = company.trim_end_matches(suffix).trim().to_string();
                break;
            }
        }
        if company.is_empty() {
            company = domain.clone();
        }

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "digital commerce".to_string(),
            "e-commerce infrastructure".to_string(),
            "commerce software".to_string(),
        ];
        let description_lower = description.to_lowercase();
        if description_lower.contains("e-ticaret") || description_lower.contains("eticaret") {
            matched_keywords.push("e-commerce".to_string());
        }
        if description_lower.contains("tahsilat") {
            matched_keywords.push("payments".to_string());
            matched_keywords.push("collections".to_string());
        }
        if description_lower.contains("altyap") {
            matched_keywords.push("platform infrastructure".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "EDER uyelerimiz page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://eder.org.tr/uyelerimiz/".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("EDER members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_lojider_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.lojider.org.tr/Member-List",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_lojider_member_candidates(
        &html,
        profile,
        run_sequence,
        MAX_LOJIDER_DIRECTORY_CANDIDATES,
    )
}

fn parse_lojider_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(r#"(?is)<b class="d-block">(.*?)</b>"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)href="tel:[^"]+">\s*([^<]+?)\s*</a>"#).unwrap();
    let website_re =
        regex_lite::Regex::new(r#"(?is)<i[^>]*fa-paper-plane[^>]*></i>\s*<a[^>]*href="([^"]+)""#)
            .unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)<i[^>]*fa-user[^>]*></i>\s*([^<]+?)\s*</div>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="row mb-4 member-row">"#).skip(1) {
        let block_html = format!(r#"<div class="row mb-4 member-row">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "lojider.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let raw_contact = contact_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let contact_name = raw_contact
            .as_deref()
            .and_then(normalize_turkish_source_person_name)
            .or_else(|| raw_contact.as_deref().and_then(normalize_person_name));
        let email =
            normalize_directory_email_for_domain(extract_email_from_text(&block_html), &domain);

        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "logistics".to_string(),
            "freight".to_string(),
            "transport association".to_string(),
        ];
        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        if source_lower.contains("gümrük") || source_lower.contains("gumruk") {
            matched_keywords.push("customs".to_string());
        }
        if source_lower.contains("antrepo") || source_lower.contains("depo") {
            matched_keywords.push("warehousing".to_string());
        }
        if source_lower.contains("nakliye") || source_lower.contains("taş") {
            matched_keywords.push("transport".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "LojiDer member list shows {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "LojiDer member list shows {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "LojiDer member list shows {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "LojiDer member list shows {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.lojider.org.tr/Member-List".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("LojiDer members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tfyd_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tfyd.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tfyd_member_candidates(&html, profile, run_sequence, MAX_TFYD_DIRECTORY_CANDIDATES)
}

fn parse_tfyd_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str()))
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();
        if cells.len() < 4 {
            continue;
        }

        let company = cells[1].trim().to_string();
        let Some(domain) = extract_domain(&cells[2]) else {
            continue;
        };
        if company.is_empty()
            || domain == "tfyd.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let phone = normalize_phone(&cells[3]);
        let source_text = format!("{company} {}", cells.join(" "));
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "events & exhibitions".to_string(),
            "fair organization".to_string(),
            "event services".to_string(),
        ];
        if source_lower.contains("fuar") {
            matched_keywords.push("fair".to_string());
        }
        if source_lower.contains("organizasyon") {
            matched_keywords.push("event organization".to_string());
        }
        if source_lower.contains("kongre") {
            matched_keywords.push("congress".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "TFYD uyelerimiz page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "TFYD uyelerimiz page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.tfyd.org.tr/uyelerimiz".to_string()],
                phone,
            },
            contact_hint: SourceContactHint {
                source: Some("TFYD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_oss_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.oss.org.tr/en/members/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_oss_member_candidates(&html, profile, run_sequence, MAX_OSS_DIRECTORY_CANDIDATES)
}

fn parse_oss_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h5 class="card-title">\s*(.*?)</h5>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)bi bi-telephone-fill"></i>\s*([^<]+?)\s*</li>"#).unwrap();
    let website_re = regex_lite::Regex::new(r#"(?is)window\.open\('([^']+)'\)"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="card membercard">"#).skip(1) {
        let block_html = format!(r#"<div class="card membercard">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "oss.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| domain.clone());
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "automotive aftermarket".to_string(),
            "spare parts".to_string(),
            "aftermarket association".to_string(),
        ];
        if source_lower.contains("otomotiv") || source_lower.contains("automotive") {
            matched_keywords.push("automotive".to_string());
        }
        if source_lower.contains("yedek par") || source_lower.contains("spare part") {
            matched_keywords.push("spare parts".to_string());
        }
        if source_lower.contains("filtre") {
            matched_keywords.push("filters".to_string());
        }
        if source_lower.contains("suspansiyon") || source_lower.contains("süspansiyon") {
            matched_keywords.push("suspension".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "OSS members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "OSS members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.oss.org.tr/en/members/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("OSS members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_ida_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.ida.org.tr/ornek-sayfa/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_ida_member_candidates(&html, profile, run_sequence, MAX_IDA_DIRECTORY_CANDIDATES)
}

fn parse_ida_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let table_re =
        regex_lite::Regex::new(r#"(?is)<table border="0" cellspacing="0" cellpadding="3">\s*<tbody>(.*?)</tbody>\s*</table>"#)
            .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<td colspan="2"><strong>(.*?)</strong></td>"#).unwrap();
    let web_re =
        regex_lite::Regex::new(r#"(?is)<td><strong>Web:</strong></td>\s*<td><a href="([^"]+)""#)
            .unwrap();
    let contact_re = regex_lite::Regex::new(
        r#"(?is)<td[^>]*><strong>(?:Yönetici Ortak|Kurucu Ortak|Managing Partner|Genel Müdür|Genel Mudur|Temsilci|Kurucu):</strong></td>\s*<td>(.*?)</td>"#,
    )
    .unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<td><strong>Telefon:</strong></td>\s*<td>(.*?)</td>"#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for table_cap in table_re.captures_iter(html) {
        let Some(block_html) = table_cap.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let Some(domain) = web_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "ida.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let contact_name = contact_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "public relations".to_string(),
            "communications agency".to_string(),
            "brand communication".to_string(),
        ];
        if source_lower.contains("iletişim") || source_lower.contains("iletisim") {
            matched_keywords.push("communication consultancy".to_string());
        }
        if source_lower.contains("medya") {
            matched_keywords.push("media relations".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "IDA members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "IDA members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.ida.org.tr/ornek-sayfa/uyelerimiz/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                source: Some("IDA members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tesid_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://tesid.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tesid_member_candidates(&html, profile, run_sequence, MAX_TESID_DIRECTORY_CANDIDATES)
}

fn parse_tesid_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let start = html.find(r#"<div class="boxuye_detay">"#).unwrap_or(0);
    let end = html
        .find("TESİD ÜYELERİ ALT SEKTÖR DAĞILIMI")
        .unwrap_or(html.len());
    let slice = &html[start..end];
    let anchor_re = regex_lite::Regex::new(r#"(?is)<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for cap in anchor_re.captures_iter(slice) {
        let Some(raw_href) = cap.get(1).map(|m| decode_basic_html_entities(m.as_str())) else {
            continue;
        };
        let Some(domain) = extract_domain(&raw_href) else {
            continue;
        };
        if domain == "tesid.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let company = cap
            .get(2)
            .map(|m| strip_html_tags(m.as_str()))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .unwrap_or_default();
        if company.len() < 8 || company.to_lowercase().contains("tesid üyeleri alt sektör") {
            continue;
        }

        let company_lower = company.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "electronics".to_string(),
            "electronic manufacturing".to_string(),
            "hardware".to_string(),
        ];
        if company_lower.contains("yazılım") || company_lower.contains("yazilim") {
            matched_keywords.push("software".to_string());
        }
        if company_lower.contains("savunma") {
            matched_keywords.push("defense electronics".to_string());
        }
        if company_lower.contains("otomasyon") {
            matched_keywords.push("industrial automation".to_string());
        }
        if company_lower.contains("telekom") || company_lower.contains("haberleşme") {
            matched_keywords.push("telecom".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "TESID members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://tesid.org.tr/uyelerimiz".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                source: Some("TESID members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tudis_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.tudis.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tudis_member_candidates(&html, profile, run_sequence, MAX_TUDIS_DIRECTORY_CANDIDATES)
}

fn parse_tudis_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str().to_string()))
            .collect::<Vec<_>>();
        if raw_cells.len() < 3 {
            continue;
        }
        let cells = raw_cells
            .iter()
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();

        let company = cells[0].trim().to_string();
        let Some(domain) = extract_domain(&raw_cells[2]).or_else(|| extract_domain(&cells[2]))
        else {
            continue;
        };
        if company.is_empty()
            || company.contains("ÜYE FİRMA ADI")
            || domain == "tudis.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let email =
            normalize_directory_email_for_domain(extract_email_from_text(&raw_cells[1]), &domain);
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "leather".to_string(),
            "tannery".to_string(),
            "leather manufacturing".to_string(),
        ];
        let company_lower = company.to_lowercase();
        if company_lower.contains("konf") {
            matched_keywords.push("leather apparel".to_string());
        }
        if company_lower.contains("deri") {
            matched_keywords.push("leather goods".to_string());
        }

        let evidence = if let Some(email) = email.as_deref() {
            format!(
                "TUDIS members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            )
        } else {
            format!(
                "TUDIS members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.tudis.org.tr/uyelerimiz".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                email,
                source: Some("TUDIS members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_emsad_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.emsad.org.tr/TR,753/uyelerimiz.html",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_emsad_member_candidates(&html, profile, run_sequence, MAX_EMSAD_DIRECTORY_CANDIDATES)
}

fn parse_emsad_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let table_re = regex_lite::Regex::new(
        r#"(?is)<table width="100%" border="0" cellspacing="1" cellpadding="2">(.*?)</table>"#,
    )
    .unwrap();
    let company_re =
        regex_lite::Regex::new(r#"(?is)<td[^>]*align="left"[^>]*><b>(.*?)</b><br"#).unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)<b>\s*Temsilci Adı:\s*</b>\s*(.*?)<br"#).unwrap();
    let phone_re = regex_lite::Regex::new(r#"(?is)<b>\s*Tel:\s*</b>\s*(.*?)<br"#).unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)<b>\s*e-posta:\s*</b>\s*(.*?)<br"#).unwrap();
    let web_re = regex_lite::Regex::new(r#"(?is)<b>\s*web:</b>\s*<a[^>]*href="([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for table_cap in table_re.captures_iter(html) {
        let Some(block_html) = table_cap.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let Some(domain) = web_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "emsad.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let contact_name = contact_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| {
                normalize_directory_email_for_domain(extract_email_from_text(&value), &domain)
            });

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "electromechanical".to_string(),
            "electrical equipment".to_string(),
            "power equipment".to_string(),
        ];
        if source_lower.contains("transform") {
            matched_keywords.push("transformer".to_string());
        }
        if source_lower.contains("enerji") || source_lower.contains("energy") {
            matched_keywords.push("energy equipment".to_string());
        }
        if source_lower.contains("otomasyon") {
            matched_keywords.push("industrial automation".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "EMSAD members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "EMSAD members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "EMSAD members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "EMSAD members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.emsad.org.tr/TR,753/uyelerimiz.html".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("EMSAD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_tgsd_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://tgsd.org.tr/uyelerimiz/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_tgsd_member_candidates(&html, profile, run_sequence, MAX_TGSD_DIRECTORY_CANDIDATES)
}

fn parse_tgsd_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let row_re = regex_lite::Regex::new(r#"(?is)<tr[^>]*>(.*?)</tr>"#).unwrap();
    let cell_re = regex_lite::Regex::new(r#"(?is)<td[^>]*>(.*?)</td>"#).unwrap();
    let href_re = regex_lite::Regex::new(r#"(?is)<a href="([^"]+)""#).unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for row_cap in row_re.captures_iter(html) {
        let row_html = row_cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_cells = cell_re
            .captures_iter(row_html)
            .filter_map(|cell| cell.get(1).map(|m| m.as_str().to_string()))
            .collect::<Vec<_>>();
        if raw_cells.len() < 4 {
            continue;
        }
        let cells = raw_cells
            .iter()
            .map(|value| {
                strip_html_tags(&decode_basic_html_entities(value))
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>();

        if cells[1].eq_ignore_ascii_case("Adı Soyadı") || cells[2].eq_ignore_ascii_case("Firma") {
            continue;
        }

        let Some(domain) = href_re
            .captures(&raw_cells[3])
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        let company = cells[2].trim().to_string();
        if company.is_empty()
            || domain == "tgsd.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let contact_name = normalize_turkish_source_person_name(&cells[1])
            .or_else(|| normalize_person_name(&cells[1]));
        let company_lower = company.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "textile".to_string(),
            "apparel".to_string(),
            "ready-to-wear".to_string(),
        ];
        if company_lower.contains("tekstil") {
            matched_keywords.push("textile manufacturing".to_string());
        }
        if company_lower.contains("giyim") {
            matched_keywords.push("garment".to_string());
        }
        if company_lower.contains("denim") {
            matched_keywords.push("denim".to_string());
        }

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                evidence: vec![format!(
                    "TGSD members page lists {} with official website {}",
                    truncate_text_for_reason(&company, 120),
                    domain
                )],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://tgsd.org.tr/uyelerimiz/".to_string()],
                phone: None,
            },
            contact_hint: SourceContactHint {
                contact_name,
                source: Some("TGSD members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_ared_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://www.ared.org.tr/uyelerimiz",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_ared_member_candidates(&html, profile, run_sequence, MAX_ARED_DIRECTORY_CANDIDATES)
}

fn parse_ared_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re =
        regex_lite::Regex::new(r#"(?is)<h3 class="entry-title">\s*(.*?)\s*</h3>"#).unwrap();
    let contact_re =
        regex_lite::Regex::new(r#"(?is)fa-user[^>]*></i>\s*([^<]+?)\s*</span>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)fa-phone-square[^>]*></i>\s*([^<]+?)\s*</span>"#).unwrap();
    let email_re = regex_lite::Regex::new(r#"(?is)href="mailto:([^"]*)""#).unwrap();
    let website_re =
        regex_lite::Regex::new(r#"(?is)mailto:[^"]*"[^>]*>.*?</a>\s*-\s*<a[^>]*href="([^"]+)""#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html.split(r#"<div class="col-lg-12 load-post">"#).skip(1) {
        let block_html = format!(r#"<div class="col-lg-12 load-post">{segment}"#);
        let Some(domain) = website_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "ared.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company_raw) = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
        else {
            continue;
        };
        let company = company_raw
            .split(" - ")
            .next()
            .unwrap_or(company_raw.as_str())
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if company.is_empty() {
            continue;
        }

        let contact_name = contact_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .and_then(|value| {
                normalize_turkish_source_person_name(&value)
                    .or_else(|| normalize_person_name(&value))
            });
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());
        let email = email_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .and_then(|value| {
                normalize_directory_email_for_domain(extract_email_from_text(&value), &domain)
            });

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "signage".to_string(),
            "outdoor advertising".to_string(),
            "industrial printing".to_string(),
        ];
        if source_lower.contains("dijital") {
            matched_keywords.push("digital signage".to_string());
        }
        if source_lower.contains("baskı") || source_lower.contains("baski") {
            matched_keywords.push("printing services".to_string());
        }
        if source_lower.contains("şehir mobilyaları") || source_lower.contains("sehir mobilyalari")
        {
            matched_keywords.push("urban furniture".to_string());
        }

        let evidence = match (phone.as_deref(), email.as_deref()) {
            (Some(phone), Some(email)) => format!(
                "ARED members page lists {} with official website {}, public phone {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone,
                email
            ),
            (Some(phone), None) => format!(
                "ARED members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            ),
            (None, Some(email)) => format!(
                "ARED members page lists {} with official website {} and contact {}",
                truncate_text_for_reason(&company, 120),
                domain,
                email
            ),
            (None, None) => format!(
                "ARED members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            ),
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://www.ared.org.tr/uyelerimiz".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                contact_name,
                email,
                source: Some("ARED members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

async fn fetch_todeb_member_candidates(
    client: &reqwest::Client,
    profile: &SalesProfile,
    run_sequence: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let Some(html) = fetch_html_page(
        client,
        "https://todeb.org.tr/sayfa/birlik-uyeleri/39/",
        FREE_DIRECTORY_FETCH_TIMEOUT_MS,
    )
    .await
    else {
        return Vec::new();
    };

    parse_todeb_member_candidates(&html, profile, run_sequence, MAX_TODEB_DIRECTORY_CANDIDATES)
}

fn parse_todeb_member_candidates(
    html: &str,
    profile: &SalesProfile,
    run_sequence: usize,
    max_candidates: usize,
) -> Vec<FreeDiscoveryCandidate> {
    let company_re = regex_lite::Regex::new(r#"(?is)<h2>(.*?)</h2>"#).unwrap();
    let phone_re =
        regex_lite::Regex::new(r#"(?is)<strong>Telefon:\s*<br\s*/?></strong>\s*([^<]+)"#).unwrap();
    let web_re =
        regex_lite::Regex::new(r#"(?is)<strong>Web:\s*<br\s*/?></strong>\s*<a href="([^"]+)""#)
            .unwrap();

    let mut out = Vec::<FreeDiscoveryCandidate>::new();
    let mut seen = HashSet::<String>::new();

    for segment in html
        .split(r#"<div class="flexCerceve logoBorder">"#)
        .skip(1)
    {
        let block_html = format!(r#"<div class="flexCerceve logoBorder">{segment}"#);
        let Some(domain) = web_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .as_deref()
            .and_then(extract_domain)
        else {
            continue;
        };
        if domain == "todeb.org.tr"
            || is_blocked_company_domain(&domain)
            || !seen.insert(domain.clone())
        {
            continue;
        }

        let Some(company) = company_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| strip_html_tags(m.as_str())))
            .map(|value| decode_basic_html_entities(&value))
            .map(|value| repair_common_mojibake_utf8(&value))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let phone = phone_re
            .captures(&block_html)
            .and_then(|value| value.get(1).map(|m| decode_basic_html_entities(m.as_str())))
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|value| !value.is_empty());

        let source_text = format!(
            "{company} {}",
            strip_html_tags(&decode_basic_html_entities(&block_html))
        );
        let source_lower = source_text.to_lowercase();
        let mut matched_keywords = vec![
            profile.target_industry.clone(),
            "payments".to_string(),
            "electronic money".to_string(),
            "fintech".to_string(),
        ];
        if source_lower.contains("elektronik para") {
            matched_keywords.push("e-money".to_string());
        }
        if source_lower.contains("ödeme") || source_lower.contains("odeme") {
            matched_keywords.push("payment services".to_string());
        }

        let evidence = if let Some(phone) = phone.as_deref() {
            format!(
                "TODEB members page lists {} with official website {} and public phone {}",
                truncate_text_for_reason(&company, 120),
                domain,
                phone
            )
        } else {
            format!(
                "TODEB members page lists {} with official website {}",
                truncate_text_for_reason(&company, 120),
                domain
            )
        };

        out.push(FreeDiscoveryCandidate {
            candidate: DomainCandidate {
                domain: domain.clone(),
                score: MIN_DOMAIN_RELEVANCE_SCORE + 13,
                evidence: vec![evidence],
                matched_keywords: dedupe_strings(matched_keywords),
                source_links: vec!["https://todeb.org.tr/sayfa/birlik-uyeleri/39/".to_string()],
                phone: phone.as_deref().and_then(normalize_phone),
            },
            contact_hint: SourceContactHint {
                source: Some("TODEB members page".to_string()),
                ..SourceContactHint::default()
            },
        });
    }

    if !out.is_empty() {
        let offset = run_sequence % out.len();
        out.rotate_left(offset);
    }
    out.truncate(max_candidates);
    out
}

