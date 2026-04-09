#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sales_profile_draft_accepts_empty_numeric_strings() {
        let payload = r#"{
          "product_name": "Machinity",
          "product_description": "AI operations teammate",
          "target_industry": "",
          "target_geo": "",
          "sender_name": "",
          "sender_email": "",
          "sender_linkedin": "",
          "target_title_policy": "",
          "daily_target": "",
          "daily_send_cap": "",
          "schedule_hour_local": "",
          "timezone_mode": ""
        }"#;
        let parsed: SalesProfileDraft = serde_json::from_str(payload).expect("draft parse");
        assert_eq!(parsed.daily_target, None);
        assert_eq!(parsed.daily_send_cap, None);
        assert_eq!(parsed.schedule_hour_local, None);
    }

    #[test]
    fn truncate_text_for_reason_handles_utf8_safely() {
        let text = "Saha operasyonu yönetimi için çok güçlü otomasyon";
        let out = truncate_text_for_reason(text, 11);
        assert!(!out.is_empty());
    }

    #[test]
    fn candidate_field_ops_signal_ignores_only_generic_keywords() {
        let only_generic = DomainCandidate {
            domain: "example.com".to_string(),
            score: 42,
            evidence: vec!["B2B workflow automation".to_string()],
            matched_keywords: vec!["Field Operations".to_string()],
            source_links: Vec::new(),
            phone: None,
        };
        assert!(!candidate_has_field_ops_signal(&only_generic));
    }

    #[test]
    fn candidate_field_ops_signal_recognizes_platform_keywords() {
        let platform_company = DomainCandidate {
            domain: "example.com".to_string(),
            score: 42,
            evidence: vec!["Platform rental and forklift service".to_string()],
            matched_keywords: vec!["equipment rental".to_string()],
            source_links: Vec::new(),
            phone: None,
        };
        assert!(candidate_has_field_ops_signal(&platform_company));
    }

    #[test]
    fn energy_target_detection_does_not_trigger_for_plain_field_ops_profile() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "Dispatch ve saha operasyon koordinasyonu".to_string(),
            target_industry: "Construction and field service".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 5,
            daily_send_cap: 5,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };
        assert!(profile_targets_field_ops(&profile));
        assert!(!profile_targets_energy(&profile));
        assert_eq!(candidate_quality_floor(&profile), 12);
    }

    #[test]
    fn blocked_company_domain_rejects_global_giants() {
        assert!(is_blocked_company_domain("boschrexroth.com"));
        assert!(is_blocked_company_domain("cargill.com.tr"));
        assert!(is_blocked_company_domain("gmail.com"));
        assert!(is_blocked_company_domain("outlook.com"));
        assert!(!is_blocked_company_domain("altanhidrolik.com.tr"));
    }

    #[test]
    fn candidate_should_skip_for_profile_rejects_holdings_for_field_ops() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "Field ops coordination".to_string(),
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        assert!(candidate_should_skip_for_profile(
            &DomainCandidate {
                domain: "celiklerholding.net".to_string(),
                score: 40,
                evidence: vec![],
                matched_keywords: vec![],
                source_links: Vec::new(),
                phone: None,
            },
            &profile,
        ));
        assert!(!candidate_should_skip_for_profile(
            &DomainCandidate {
                domain: "ekseninsaat.com.tr".to_string(),
                score: 40,
                evidence: vec![],
                matched_keywords: vec![],
                source_links: Vec::new(),
                phone: None,
            },
            &profile,
        ));
    }

    #[test]
    fn normalize_person_name_rejects_business_phrase() {
        assert!(normalize_person_name("TechEx Sustainable Legacies Welcoming Workplace").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_department_and_office_labels() {
        assert!(normalize_person_name("Ankara Ofisi Türkiye").is_none());
        assert!(normalize_person_name("Basın Odası").is_none());
        assert!(normalize_person_name("Kurumsal Liderlik").is_none());
        assert!(normalize_person_name("Haber Finansal").is_none());
        assert!(normalize_person_name("Kişisel Verilerin Korunması Kanunu").is_none());
        assert!(normalize_person_name("Uluslararası Kredi Kuruluşları").is_none());
        assert!(normalize_person_name("Faaliyet Alanları").is_none());
        assert!(normalize_person_name("Suudi Arabistan").is_none());
        assert!(normalize_person_name("Bu Web Sitesinde Çerezler Kullanılmaktadır").is_none());
        assert!(normalize_person_name("Türk Sanayisinde Rönesans Dönemi").is_none());
        assert!(normalize_person_name("Tümüne İzin Ver").is_none());
        assert!(normalize_person_name("Sürdürülebilirlik Sosyal Sorumluluk").is_none());
        assert!(normalize_person_name("Costa Rica").is_none());
        assert!(normalize_person_name("Onursal Başkanımız").is_none());
        assert!(normalize_person_name("Paylaşma İklimi").is_none());
        assert!(normalize_person_name("Defa Kamunun Muhtelif İdarelerinden").is_none());
        assert!(normalize_person_name("İşi Veren İdare").is_none());
        assert!(normalize_person_name("İhale Yılı").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_css_fragments() {
        assert!(normalize_person_name("P Style").is_none());
        assert!(normalize_person_name("Font Verdana").is_none());
        assert!(normalize_person_name("Div Class").is_none());
    }

    #[test]
    fn normalize_person_name_rejects_photo_caption_fragments() {
        assert!(normalize_person_name("Oturanlar Soldan Sağa").is_none());
        assert!(normalize_person_name("Ayakta Soldan Saga").is_none());
        assert!(normalize_person_name("Nasil Basladik").is_none());
        assert!(normalize_person_name("Hedefimiz Politikamiz").is_none());
        assert!(normalize_person_name("CEO Aslan Uzun").is_none());
    }

    #[test]
    fn contact_title_priority_rejects_field_signal_headlines() {
        assert_eq!(
            contact_title_priority("Yapi Merkezi teknik servis ve santiye operasyonlari"),
            0
        );
    }

    #[test]
    fn extract_contact_from_search_supports_unicode_name_patterns() {
        let sample = r#"
1. Enerjisa Leadership Team
   URL: https://www.enerjisa.com.tr/leadership
   CEO: Emre Erdoğan
"#;
        let (name, title, _) = extract_contact_from_search(sample, "ceo_only");
        assert_eq!(name.as_deref(), Some("Emre Erdoğan"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn extract_contact_from_search_supports_turkish_titles() {
        let sample = r#"
1. Yapı Merkezi Yönetim
   URL: https://www.yapimerkezi.com.tr/yonetim
   Genel Müdür Aslan Uzun
"#;
        let (name, title, _) = extract_contact_from_search(sample, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Aslan Uzun"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn build_prospect_profiles_aggregates_leads_into_company_profiles() {
        let leads = vec![
            SalesLead {
                id: "lead-1".to_string(),
                run_id: "run-1".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Unknown".to_string(),
                contact_title: "".to_string(),
                linkedin_url: None,
                email: None,
                phone: None,
                reasons: vec![
                    "Field service operations".to_string(),
                    "Dispatch automation".to_string(),
                ],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 61,
                status: "new".to_string(),
                created_at: "2026-03-20T09:00:00Z".to_string(),
            },
            SalesLead {
                id: "lead-2".to_string(),
                run_id: "run-2".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Aylin Demir".to_string(),
                contact_title: "COO".to_string(),
                linkedin_url: Some("https://www.linkedin.com/in/aylindemir".to_string()),
                email: Some("aylin@acme.example".to_string()),
                phone: None,
                reasons: vec![
                    "Field service operations".to_string(),
                    "Maintenance dispatch".to_string(),
                ],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 78,
                status: "new".to_string(),
                created_at: "2026-03-21T10:30:00Z".to_string(),
            },
            SalesLead {
                id: "lead-3".to_string(),
                run_id: "run-2".to_string(),
                company: "Acme Field Ops".to_string(),
                website: "https://acme.example".to_string(),
                company_domain: "acme.example".to_string(),
                contact_name: "Mert Kaya".to_string(),
                contact_title: "Head of Operations".to_string(),
                linkedin_url: None,
                email: None,
                phone: None,
                reasons: vec!["On-site maintenance teams".to_string()],
                email_subject: String::new(),
                email_body: String::new(),
                linkedin_message: String::new(),
                score: 72,
                status: "new".to_string(),
                created_at: "2026-03-21T11:00:00Z".to_string(),
            },
        ];

        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Field operations teams icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_prospect_profiles(leads, 10, Some(&sales_profile));

        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.id, "acme.example");
        assert_eq!(profile.run_id, "run-2");
        assert_eq!(profile.company, "Acme Field Ops");
        assert_eq!(profile.fit_score, 78);
        assert_eq!(profile.profile_status, "contact_ready");
        assert_eq!(profile.primary_contact_name.as_deref(), Some("Aylin Demir"));
        assert_eq!(profile.primary_contact_title.as_deref(), Some("COO"));
        assert_eq!(profile.primary_email.as_deref(), Some("aylin@acme.example"));
        assert_eq!(
            profile.primary_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/in/aylindemir")
        );
        assert_eq!(profile.contact_count, 2);
        assert!(profile
            .matched_signals
            .iter()
            .any(|signal| signal.contains("Field service operations")));
        assert!(profile
            .matched_signals
            .iter()
            .any(|signal| signal.contains("Maintenance dispatch")));
        assert!(profile.summary.contains("Acme Field Ops"));
        assert_eq!(profile.research_status, "heuristic");
        assert!(profile.research_confidence > 0.5);
        assert!(profile.source_count >= 2);
        assert!(!profile.buyer_roles.is_empty());
        assert!(!profile.pain_points.is_empty());
        assert!(!profile.trigger_events.is_empty());
        assert!(!profile.outreach_angle.is_empty());
        assert_eq!(profile.created_at, "2026-03-20T09:00:00Z");
        assert_eq!(profile.updated_at, "2026-03-21T11:00:00Z");
    }

    #[test]
    fn build_candidate_prospect_profiles_creates_company_only_dossiers() {
        let candidates = vec![DomainCandidate {
            domain: "ornekbakim.com.tr".to_string(),
            score: 37,
            evidence: vec![
                "Confirmed by multiple discovery sources".to_string(),
                "Maintenance dispatch and field teams".to_string(),
            ],
            matched_keywords: vec![
                "field service".to_string(),
                "maintenance".to_string(),
                "facility operations".to_string(),
            ],
            source_links: vec!["https://www.tmb.org.tr/en/members".to_string()],
            phone: None,
        }];
        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Saha ekipleri icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_candidate_prospect_profiles(
            "run-prospect",
            &candidates,
            &HashMap::new(),
            10,
            &sales_profile,
        );

        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.run_id, "run-prospect");
        assert_eq!(profile.company_domain, "ornekbakim.com.tr");
        assert_eq!(profile.profile_status, "company_only");
        assert!(profile.primary_contact_name.is_none());
        assert!(profile.primary_email.is_none());
        assert!(!profile.summary.is_empty());
        assert!(!profile.matched_signals.is_empty());
        assert!(!profile.pain_points.is_empty());
        assert!(!profile.trigger_events.is_empty());
        assert_eq!(profile.research_status, "heuristic");
        assert!(profile
            .osint_links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/members"));
    }

    #[test]
    fn build_candidate_prospect_profiles_prioritizes_actionable_local_accounts() {
        let candidates = vec![
            DomainCandidate {
                domain: "celiklerholding.net".to_string(),
                score: 58,
                evidence: vec!["Corporate group overview".to_string()],
                matched_keywords: vec!["field service".to_string()],
                source_links: Vec::new(),
                phone: None,
            },
            DomainCandidate {
                domain: "ornekbakim.com.tr".to_string(),
                score: 39,
                evidence: vec!["Maintenance dispatch teams".to_string()],
                matched_keywords: vec!["field service".to_string(), "maintenance".to_string()],
                source_links: vec!["https://www.asmud.org.tr/Uyeler.asp".to_string()],
                phone: None,
            },
        ];
        let mut hints = HashMap::new();
        hints.insert(
            "ornekbakim.com.tr".to_string(),
            SourceContactHint {
                contact_name: Some("Aylin Demir".to_string()),
                contact_title: Some("COO".to_string()),
                email: Some("aylin@ornekbakim.com.tr".to_string()),
                source: Some("ASMUD members page".to_string()),
            },
        );
        let sales_profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description:
                "Saha ekipleri icin görev takibi, dispatch koordinasyonu ve WhatsApp takip otomasyonu"
                    .to_string(),
            target_industry: "field service operations".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Ayse".to_string(),
            sender_email: "ayse@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let profiles = build_candidate_prospect_profiles(
            "run-priority",
            &candidates,
            &hints,
            10,
            &sales_profile,
        );

        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].company_domain, "ornekbakim.com.tr");
        assert_eq!(profiles[0].profile_status, "contact_ready");
        assert_eq!(profiles[1].company_domain, "celiklerholding.net");
    }

    #[test]
    fn apply_cached_prospect_memory_reuses_contact_ready_cache() {
        let mut current = SalesProspectProfile {
            id: "ornekbakim.com.tr".to_string(),
            run_id: "run-current".to_string(),
            company: "Ornekbakim".to_string(),
            website: "https://ornekbakim.com.tr".to_string(),
            company_domain: "ornekbakim.com.tr".to_string(),
            fit_score: 44,
            profile_status: "company_only".to_string(),
            summary: "Current heuristic summary".to_string(),
            matched_signals: vec!["maintenance".to_string()],
            primary_contact_name: None,
            primary_contact_title: None,
            primary_email: None,
            primary_linkedin_url: None,
            company_linkedin_url: None,
            osint_links: vec!["https://ornekbakim.com.tr".to_string()],
            contact_count: 0,
            source_count: 1,
            buyer_roles: vec!["COO".to_string()],
            pain_points: vec![],
            trigger_events: vec![],
            recommended_channel: "research".to_string(),
            outreach_angle: String::new(),
            research_status: "heuristic".to_string(),
            research_confidence: 0.42,
            tech_stack: Vec::new(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
            updated_at: "2026-03-25T10:00:00Z".to_string(),
        };
        let cached = SalesProspectProfile {
            id: "ornekbakim.com.tr".to_string(),
            run_id: "run-cached".to_string(),
            company: "Ornekbakim".to_string(),
            website: "https://ornekbakim.com.tr".to_string(),
            company_domain: "ornekbakim.com.tr".to_string(),
            fit_score: 61,
            profile_status: "contact_ready".to_string(),
            summary: "Cached strong dossier".to_string(),
            matched_signals: vec!["dispatch".to_string(), "maintenance".to_string()],
            primary_contact_name: Some("Aylin Demir".to_string()),
            primary_contact_title: Some("COO".to_string()),
            primary_email: Some("aylin@ornekbakim.com.tr".to_string()),
            primary_linkedin_url: None,
            company_linkedin_url: Some("https://www.linkedin.com/company/ornekbakim/".to_string()),
            osint_links: vec![
                "https://ornekbakim.com.tr".to_string(),
                "https://www.linkedin.com/company/ornekbakim/".to_string(),
            ],
            contact_count: 1,
            source_count: 2,
            buyer_roles: vec!["COO".to_string(), "Head of Operations".to_string()],
            pain_points: vec!["Dispatch visibility".to_string()],
            trigger_events: vec!["Public field ops signal".to_string()],
            recommended_channel: "email".to_string(),
            outreach_angle: "Lead with dispatch coordination".to_string(),
            research_status: "heuristic".to_string(),
            research_confidence: 0.83,
            tech_stack: Vec::new(),
            created_at: "2026-03-24T10:00:00Z".to_string(),
            updated_at: "2026-03-24T10:00:00Z".to_string(),
        };

        apply_cached_prospect_memory(&mut current, &cached);

        assert_eq!(current.profile_status, "contact_ready");
        assert_eq!(current.primary_contact_name.as_deref(), Some("Aylin Demir"));
        assert_eq!(current.primary_contact_title.as_deref(), Some("COO"));
        assert_eq!(
            current.primary_email.as_deref(),
            Some("aylin@ornekbakim.com.tr")
        );
        assert!(current
            .matched_signals
            .iter()
            .any(|signal| signal == "dispatch"));
        assert!(current.research_confidence >= 0.83);
        assert_eq!(current.recommended_channel, "email");
    }

    #[test]
    fn latest_successful_run_id_since_accepts_profile_only_runs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run(SalesSegment::B2B).expect("begin run");
        engine
            .finish_run(&run_id, "completed", 9, 0, 0, Some("profile only"))
            .expect("finish run");
        engine
            .upsert_prospect_profiles(SalesSegment::B2B, &[SalesProspectProfile {
                id: "ornekbakim.com.tr".to_string(),
                run_id: run_id.clone(),
                company: "Ornekbakim".to_string(),
                website: "https://ornekbakim.com.tr".to_string(),
                company_domain: "ornekbakim.com.tr".to_string(),
                fit_score: 42,
                profile_status: "company_only".to_string(),
                summary: "Profiling completed.".to_string(),
                matched_signals: vec!["maintenance".to_string()],
                primary_contact_name: None,
                primary_contact_title: None,
                primary_email: None,
                primary_linkedin_url: None,
                company_linkedin_url: None,
                osint_links: vec!["https://ornekbakim.com.tr".to_string()],
                contact_count: 0,
                source_count: 1,
                buyer_roles: vec!["COO".to_string()],
                pain_points: vec!["Dispatch visibility".to_string()],
                trigger_events: vec!["Public field ops signal".to_string()],
                recommended_channel: "research".to_string(),
                outreach_angle: "Lead with dispatch coordination".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.74,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let last_run_id = engine
            .latest_successful_run_id_since(SalesSegment::B2B, None)
            .expect("latest successful run");

        assert_eq!(last_run_id.as_deref(), Some(run_id.as_str()));
    }

    #[test]
    fn recover_latest_timed_out_run_completes_partial_marketing_progress() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run(SalesSegment::B2B).expect("begin run");
        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations signal".to_string()],
            email_subject: "Machinity for field ops".to_string(),
            email_body: "Hi Aylin".to_string(),
            linkedin_message: "Hi Aylin".to_string(),
            score: 92,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        let queued = engine
            .queue_approvals_for_lead(&lead)
            .expect("queue approvals");
        assert_eq!(queued, 1);
        engine
            .upsert_prospect_profiles(SalesSegment::B2B, &[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: Some(
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ),
                osint_links: vec![
                    "https://machinity.ai".to_string(),
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let recovered = engine
            .recover_latest_timed_out_run(SalesSegment::B2B)
            .expect("recover timed out run")
            .expect("running run exists");

        assert_eq!(recovered.id, run_id);
        assert_eq!(recovered.status, "completed");
        assert_eq!(recovered.inserted, 1);
        assert_eq!(recovered.discovered, 1);
        assert_eq!(recovered.approvals_queued, 1);
        assert!(recovered
            .error
            .as_deref()
            .map(|value| value.contains("saving progress"))
            .unwrap_or(false));

        let runs = engine
            .list_runs(SalesSegment::B2B, 5)
            .expect("list runs");
        assert_eq!(runs[0].status, "completed");
        assert_eq!(runs[0].inserted, 1);
        assert_eq!(runs[0].approvals_queued, 1);
    }

    #[test]
    fn recover_latest_timed_out_run_fails_when_nothing_was_persisted() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run(SalesSegment::B2B).expect("begin run");

        let recovered = engine
            .recover_latest_timed_out_run(SalesSegment::B2B)
            .expect("recover timed out run")
            .expect("running run exists");

        assert_eq!(recovered.id, run_id);
        assert_eq!(recovered.status, "failed");
        assert_eq!(recovered.inserted, 0);
        assert_eq!(recovered.discovered, 0);
        assert_eq!(recovered.approvals_queued, 0);
        assert!(recovered
            .error
            .as_deref()
            .map(|value| value.contains("before any durable prospect dossiers"))
            .unwrap_or(false));
    }

    #[test]
    fn recover_latest_timed_out_run_if_stale_honors_age_threshold() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run(SalesSegment::B2B).expect("begin run");
        engine
            .upsert_prospect_profiles(SalesSegment::B2B, &[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: Some(
                    "https://www.linkedin.com/company/machinity/".to_string(),
                ),
                osint_links: vec!["https://machinity.ai".to_string()],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let skipped = engine
            .recover_latest_timed_out_run_if_stale(SalesSegment::B2B, 60)
            .expect("conditional recover");
        assert!(skipped.is_none());
        let runs = engine
            .list_runs(SalesSegment::B2B, 5)
            .expect("list runs");
        assert_eq!(runs[0].status, "running");

        let conn = engine.open().expect("open db");
        let old_started_at = (Utc::now() - chrono::Duration::seconds(120)).to_rfc3339();
        conn.execute(
            "UPDATE sales_runs SET started_at = ? WHERE id = ?",
            params![old_started_at, run_id],
        )
        .expect("age run");

        let recovered = engine
            .recover_latest_timed_out_run_if_stale(SalesSegment::B2B, 60)
            .expect("conditional recover")
            .expect("stale run recovered");
        assert_eq!(recovered.status, "completed");
        let runs = engine
            .list_runs(SalesSegment::B2B, 5)
            .expect("list runs");
        assert_eq!(runs[0].status, "completed");
    }

    #[test]
    fn list_runs_surfaces_live_progress_for_running_run() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let run_id = engine.begin_run(SalesSegment::B2B).expect("begin run");
        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations signal".to_string()],
            email_subject: "Machinity for field ops".to_string(),
            email_body: "Hi Aylin".to_string(),
            linkedin_message: "Hi Aylin".to_string(),
            score: 92,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        assert_eq!(
            engine
                .queue_approvals_for_lead(&lead)
                .expect("queue approvals"),
            1
        );
        engine
            .upsert_prospect_profiles(SalesSegment::B2B, &[SalesProspectProfile {
                id: "machinity.ai".to_string(),
                run_id: run_id.clone(),
                company: "Machinity".to_string(),
                website: "https://machinity.ai".to_string(),
                company_domain: "machinity.ai".to_string(),
                fit_score: 92,
                profile_status: "contact_ready".to_string(),
                summary: "Saved dossier".to_string(),
                matched_signals: vec!["field operations".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("CEO".to_string()),
                primary_email: Some("aylin@machinity.ai".to_string()),
                primary_linkedin_url: None,
                company_linkedin_url: None,
                osint_links: vec!["https://machinity.ai".to_string()],
                contact_count: 1,
                source_count: 1,
                buyer_roles: vec!["CEO".to_string()],
                pain_points: vec!["Coordination".to_string()],
                trigger_events: vec!["Expansion".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with automation".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.88,
                tech_stack: Vec::new(),
                created_at: "2026-03-25T10:00:00Z".to_string(),
                updated_at: "2026-03-25T10:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let runs = engine
            .list_runs(SalesSegment::B2B, 5)
            .expect("list runs");
        assert_eq!(runs[0].status, "running");
        assert_eq!(runs[0].inserted, 1);
        assert_eq!(runs[0].discovered, 1);
        assert_eq!(runs[0].approvals_queued, 1);
    }

    #[test]
    fn job_progress_surfaces_checkpoints_and_active_lookup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let job_id = engine
            .create_job_run("discovery", SalesSegment::B2B)
            .expect("create job");
        engine
            .set_job_stage_running(&job_id, PipelineStage::LeadGeneration)
            .expect("set stage running");
        engine
            .update_job_stage_checkpoint(
                &job_id,
                PipelineStage::LeadGeneration,
                &LeadGenerationCheckpoint {
                    total_candidates: 18,
                    processed_candidates: 7,
                    profiled_accounts: 11,
                    inserted: 2,
                    approvals_queued: 1,
                    current_domain: Some("ornek.com".to_string()),
                },
            )
            .expect("update checkpoint");

        let progress = engine
            .get_job_progress(&job_id)
            .expect("get job progress")
            .expect("job exists");
        assert_eq!(progress.status, "running");
        assert_eq!(progress.current_stage.as_deref(), Some("LeadGeneration"));

        let lead_generation = progress
            .stages
            .iter()
            .find(|stage| stage.name == "LeadGeneration")
            .expect("lead generation stage");
        let checkpoint = lead_generation
            .checkpoint
            .as_ref()
            .expect("checkpoint attached");
        assert_eq!(
            checkpoint
                .get("processed_candidates")
                .and_then(|value| value.as_u64()),
            Some(7)
        );
        assert_eq!(
            checkpoint
                .get("current_domain")
                .and_then(|value| value.as_str()),
            Some("ornek.com")
        );

        let active = engine
            .latest_running_job_progress("discovery", SalesSegment::B2B)
            .expect("active lookup")
            .expect("running job");
        assert_eq!(active.job_id, job_id);
    }

    #[test]
    fn list_approvals_skips_non_actionable_email_payloads() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let conn = engine.open().expect("open db");
        let created_at = "2026-03-26T10:00:00Z";
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-bad-generic",
                "lead-1",
                serde_json::json!({
                    "to": "info@acme.example",
                    "subject": "Generic subject",
                    "body": "Generic body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert generic approval");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-bad-consumer",
                "lead-2",
                serde_json::json!({
                    "to": "owner@gmail.com",
                    "subject": "Consumer subject",
                    "body": "Consumer body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert consumer approval");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
            params![
                "approval-good",
                "lead-3",
                serde_json::json!({
                    "to": "eray@artiplatform.com.tr",
                    "subject": "Relevant subject",
                    "body": "Relevant body",
                })
                .to_string(),
                created_at
            ],
        )
        .expect("insert valid approval");

        let approvals = engine
            .list_approvals(None, Some("pending"), 10)
            .expect("list approvals");
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].id, "approval-good");
        assert_eq!(
            approvals[0]
                .payload
                .get("to")
                .and_then(|value| value.as_str()),
            Some("eray@artiplatform.com.tr")
        );
    }

    #[test]
    fn edit_approval_updates_touch_payload_and_returns_sanitized_payload() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let lead = SalesLead {
            id: uuid::Uuid::new_v4().to_string(),
            run_id: "run-1".to_string(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: Some("https://www.linkedin.com/in/aylin-demir/".to_string()),
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["Field operations expansion".to_string()],
            email_subject: "Original subject".to_string(),
            email_body: "Original body".to_string(),
            linkedin_message: "Original LinkedIn".to_string(),
            score: 91,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T11:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&lead).expect("insert lead"));
        assert_eq!(
            engine
                .queue_approvals_for_lead(&lead)
                .expect("queue approvals"),
            2
        );

        let approval = engine
            .list_approvals(None, Some("pending"), 10)
            .expect("list approvals")
            .into_iter()
            .find(|item| item.channel == "email")
            .expect("email approval");

        let edited = engine
            .edit_approval(
                &approval.id,
                serde_json::json!({
                    "to": "aylin@machinity.ai",
                    "subject": "Updated subject",
                    "body": "Updated first line\n\nUpdated rest of body",
                }),
            )
            .expect("edit approval");

        assert_eq!(
            edited
                .payload
                .get("subject")
                .and_then(|value| value.as_str()),
            Some("Updated subject")
        );
        assert_eq!(
            edited.payload.get("body").and_then(|value| value.as_str()),
            Some("Updated first line\n\nUpdated rest of body")
        );

        let conn = engine.open().expect("open");
        let touch_payload: String = conn
            .query_row(
                "SELECT message_payload FROM touches WHERE id = ?1",
                params![approval.id],
                |row| row.get(0),
            )
            .expect("touch payload");
        let touch_payload: serde_json::Value =
            serde_json::from_str(&touch_payload).expect("decode touch payload");
        assert_eq!(
            touch_payload
                .get("subject")
                .and_then(|value| value.as_str()),
            Some("Updated subject")
        );
    }

    #[test]
    fn list_leads_filters_by_segment() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let b2b_run = engine.begin_run(SalesSegment::B2B).expect("b2b run");
        let b2c_run = engine.begin_run(SalesSegment::B2C).expect("b2c run");

        let b2b_lead = SalesLead {
            id: "lead-b2b".to_string(),
            run_id: b2b_run.clone(),
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["b2b".to_string()],
            email_subject: "Subject".to_string(),
            email_body: "Body".to_string(),
            linkedin_message: "LinkedIn".to_string(),
            score: 90,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T10:00:00Z".to_string(),
        };
        let b2c_lead = SalesLead {
            id: "lead-b2c".to_string(),
            run_id: b2c_run.clone(),
            company: "Local Fitness".to_string(),
            website: "https://localfitness.example".to_string(),
            company_domain: "localfitness.example".to_string(),
            contact_name: "Merve Kaya".to_string(),
            contact_title: "Founder".to_string(),
            linkedin_url: None,
            email: Some("merve@localfitness.example".to_string()),
            phone: None,
            reasons: vec!["b2c".to_string()],
            email_subject: "Subject".to_string(),
            email_body: "Body".to_string(),
            linkedin_message: "LinkedIn".to_string(),
            score: 88,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T11:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&b2b_lead).expect("insert b2b lead"));
        assert!(engine.insert_lead(&b2c_lead).expect("insert b2c lead"));

        let b2b_only = engine
            .list_leads(SalesSegment::B2B, 10, None)
            .expect("list b2b leads");
        let b2c_only = engine
            .list_leads(SalesSegment::B2C, 10, None)
            .expect("list b2c leads");

        assert_eq!(b2b_only.len(), 1);
        assert_eq!(b2b_only[0].id, "lead-b2b");
        assert_eq!(b2c_only.len(), 1);
        assert_eq!(b2c_only[0].id, "lead-b2c");
        assert!(engine
            .list_leads(SalesSegment::B2B, 10, Some(&b2c_run))
            .expect("cross-segment run filter")
            .is_empty());
    }

    #[test]
    fn segment_scoped_approvals_and_deliveries_ignore_other_segment() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let b2b_run = engine.begin_run(SalesSegment::B2B).expect("b2b run");
        let b2c_run = engine.begin_run(SalesSegment::B2C).expect("b2c run");

        let b2b_lead = SalesLead {
            id: "lead-b2b".to_string(),
            run_id: b2b_run,
            company: "Machinity".to_string(),
            website: "https://machinity.ai".to_string(),
            company_domain: "machinity.ai".to_string(),
            contact_name: "Aylin Demir".to_string(),
            contact_title: "CEO".to_string(),
            linkedin_url: None,
            email: Some("aylin@machinity.ai".to_string()),
            phone: None,
            reasons: vec!["b2b".to_string()],
            email_subject: "Subject".to_string(),
            email_body: "Body".to_string(),
            linkedin_message: "LinkedIn".to_string(),
            score: 90,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T10:00:00Z".to_string(),
        };
        let b2c_lead = SalesLead {
            id: "lead-b2c".to_string(),
            run_id: b2c_run,
            company: "Local Fitness".to_string(),
            website: "https://localfitness.example".to_string(),
            company_domain: "localfitness.example".to_string(),
            contact_name: "Merve Kaya".to_string(),
            contact_title: "Founder".to_string(),
            linkedin_url: None,
            email: Some("merve@localfitness.example".to_string()),
            phone: None,
            reasons: vec!["b2c".to_string()],
            email_subject: "Subject".to_string(),
            email_body: "Body".to_string(),
            linkedin_message: "LinkedIn".to_string(),
            score: 88,
            status: "draft_ready".to_string(),
            created_at: "2026-03-26T11:00:00Z".to_string(),
        };
        assert!(engine.insert_lead(&b2b_lead).expect("insert b2b lead"));
        assert!(engine.insert_lead(&b2c_lead).expect("insert b2c lead"));

        let conn = engine.open().expect("open");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?1, ?2, 'email', ?3, 'pending', ?4)",
            params![
                "approval-b2b",
                b2b_lead.id,
                serde_json::json!({"to":"aylin@machinity.ai","subject":"Subject","body":"Body"}).to_string(),
                Utc::now().to_rfc3339()
            ],
        )
        .expect("insert b2b approval");
        conn.execute(
            "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?1, ?2, 'email', ?3, 'pending', ?4)",
            params![
                "approval-b2c",
                b2c_lead.id,
                serde_json::json!({"to":"merve@localfitness.example","subject":"Subject","body":"Body"}).to_string(),
                Utc::now().to_rfc3339()
            ],
        )
        .expect("insert b2c approval");

        let b2c_approvals = engine
            .list_approvals(Some(SalesSegment::B2C), Some("pending"), 10)
            .expect("list b2c approvals");
        assert_eq!(b2c_approvals.len(), 1);
        assert_eq!(b2c_approvals[0].id, "approval-b2c");

        engine
            .record_delivery("approval-b2b", "email", "aylin@machinity.ai", "sent", None)
            .expect("record b2b delivery");
        engine
            .record_delivery(
                "approval-b2c",
                "email",
                "merve@localfitness.example",
                "sent",
                None,
            )
            .expect("record b2c delivery");

        let b2c_deliveries = engine
            .list_deliveries(Some(SalesSegment::B2C), 10)
            .expect("list b2c deliveries");
        assert_eq!(b2c_deliveries.len(), 1);
        assert_eq!(b2c_deliveries[0].approval_id, "approval-b2c");
        assert_eq!(
            engine
                .deliveries_today(SalesSegment::B2C, "utc")
                .expect("b2c deliveries today"),
            1
        );
    }

    #[test]
    fn ensure_default_sequence_template_uses_five_step_playbook() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        let conn = engine.open().expect("open");
        let template_id = engine
            .ensure_default_sequence_template(&conn)
            .expect("template");
        let steps_json: String = conn
            .query_row(
                "SELECT steps_json FROM sequence_templates WHERE id = ?1",
                params![template_id],
                |row| row.get(0),
            )
            .expect("steps json");
        let steps: serde_json::Value =
            serde_json::from_str(&steps_json).expect("decode steps json");
        let steps = steps.as_array().expect("steps array");
        assert_eq!(steps.len(), 5);
        assert_eq!(
            steps[0].get("channel").and_then(|value| value.as_str()),
            Some("email")
        );
        assert_eq!(
            steps[3].get("channel").and_then(|value| value.as_str()),
            Some("linkedin_assist")
        );
    }

    #[test]
    fn select_accounts_for_activation_logs_mid_score_exploration() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");
        let conn = engine.open().expect("open");
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params!["acct-mid", "Mid Score Account", "https://mid.example", now],
        )
        .expect("insert account");
        conn.execute(
            "INSERT INTO score_snapshots
             (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk,
              compliance_risk, activation_priority, computed_at, scoring_version)
             VALUES (?1, ?2, 0.55, 0.42, 0.51, 0.12, 0.08, 0.61, ?3, 'v1')",
            params!["score-mid", "acct-mid", now],
        )
        .expect("insert score");

        let selected = engine
            .select_accounts_for_activation(
                &conn,
                &HashMap::from([("acct-mid".to_string(), 61_i64)]),
                1,
            )
            .expect("select activation");
        assert_eq!(selected, vec!["acct-mid".to_string()]);

        let exploration_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM exploration_log WHERE account_id = ?1",
                params!["acct-mid"],
                |row| row.get(0),
            )
            .expect("count exploration log");
        assert_eq!(exploration_count, 1);
    }

    #[test]
    fn missed_signal_review_creates_policy_proposal_and_supports_lifecycle() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");
        let conn = engine.open().expect("open");
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params!["acct-policy", "Policy Account", "https://policy.example", now],
        )
        .expect("insert account");
        conn.execute(
            "INSERT INTO signals
             (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
             VALUES (?1, ?2, 'job_posting', 'Hiring dispatch operators', 'site_html', ?3, 0.9, 'campaign_window', NULL, ?3)",
            params!["signal-job-posting", "acct-policy", now],
        )
        .expect("insert signal");
        conn.execute(
            "INSERT INTO touches
             (id, sequence_instance_id, step, channel, message_payload, claims_json, evidence_ids,
              variant_id, risk_flags, sent_at, mailbox_id, created_at)
             VALUES (?1, NULL, 1, 'email', '{}', '[]', '[]', 'v1', '[]', NULL, NULL, ?2)",
            params!["touch-1", now],
        )
        .expect("insert touch");
        conn.execute(
            "INSERT INTO outcomes
             (id, touch_id, outcome_type, raw_text, classified_at, classifier_confidence)
             VALUES (?1, ?2, 'meeting_booked', 'Positive reply', ?3, 1.0)",
            params!["outcome-1", "touch-1", now],
        )
        .expect("insert outcome");
        conn.execute(
            "INSERT INTO outcome_attribution_snapshots
             (id, touch_id, account_id, snapshot_at, score_at_touch_json, active_signal_ids, unused_signal_ids,
              thesis_id, sequence_variant, message_variant, channel, mailbox_id, contextual_factors_json)
             VALUES (?1, ?2, ?3, ?4, '{}', '[]', '[]', NULL, 'default', 'v1', 'email', NULL, '{}')",
            params!["snapshot-1", "touch-1", "acct-policy", now],
        )
        .expect("insert snapshot");

        engine
            .record_missed_signal_review(
                &conn,
                "outcome-1",
                "snapshot-1",
                "acct-policy",
                "meeting_booked",
                &["signal-job-posting".to_string()],
                &[],
            )
            .expect("record missed signal review");

        let proposals = engine
            .list_policy_proposals(Some("proposed"), 10)
            .expect("list proposals");
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].rule_key, "signal_weight::job_posting");

        let approved = engine
            .update_policy_proposal_status(&proposals[0].id, "active", Some("operator"))
            .expect("approve proposal")
            .expect("proposal exists");
        assert_eq!(approved.status, "active");
        assert_eq!(approved.approved_by.as_deref(), Some("operator"));
        assert!(approved.activated_at.is_some());

        let retired = engine
            .update_policy_proposal_status(&proposals[0].id, "retired", None)
            .expect("retire proposal")
            .expect("proposal exists");
        assert_eq!(retired.status, "retired");
        assert!(retired.activated_at.is_none());
    }

    #[test]
    fn get_account_dossier_falls_back_to_prospect_profile_when_canonical_core_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = SalesEngine::new(temp.path());
        engine.init().expect("init");

        engine
            .upsert_prospect_profiles(SalesSegment::B2B, &[SalesProspectProfile {
                id: "ornekbakim.com.tr".to_string(),
                run_id: "run-fallback".to_string(),
                company: "Ornek Bakim".to_string(),
                website: "https://ornekbakim.com.tr".to_string(),
                company_domain: "ornekbakim.com.tr".to_string(),
                fit_score: 77,
                profile_status: "contact_ready".to_string(),
                summary: "Public maintenance signal and reachable operator leader.".to_string(),
                matched_signals: vec!["Saha operasyon yonetimi".to_string()],
                primary_contact_name: Some("Aylin Demir".to_string()),
                primary_contact_title: Some("COO".to_string()),
                primary_email: Some("aylin@ornekbakim.com.tr".to_string()),
                primary_linkedin_url: Some("https://www.linkedin.com/in/aylin-demir/".to_string()),
                company_linkedin_url: None,
                osint_links: vec!["https://ornekbakim.com.tr".to_string()],
                contact_count: 1,
                source_count: 2,
                buyer_roles: vec!["decision_maker".to_string()],
                pain_points: vec!["Dispatch visibility".to_string()],
                trigger_events: vec!["Public field ops hiring".to_string()],
                recommended_channel: "email".to_string(),
                outreach_angle: "Lead with faster dispatch coordination".to_string(),
                research_status: "heuristic".to_string(),
                research_confidence: 0.81,
                tech_stack: Vec::new(),
                created_at: "2026-03-26T09:00:00Z".to_string(),
                updated_at: "2026-03-26T09:00:00Z".to_string(),
            }])
            .expect("upsert prospect profile");

        let dossier = engine
            .get_account_dossier("ornekbakim.com.tr")
            .expect("dossier lookup")
            .expect("fallback dossier");

        assert_eq!(
            dossier.get("source").and_then(|value| value.as_str()),
            Some("prospect_profile_fallback")
        );
        assert_eq!(
            dossier
                .get("account")
                .and_then(|value| value.get("canonical_name"))
                .and_then(|value| value.as_str()),
            Some("Ornek Bakim")
        );
        assert_eq!(
            dossier
                .get("score")
                .and_then(|value| value.get("fit_score"))
                .and_then(|value| value.as_f64())
                .map(|value| (value * 100.0).round() as i64),
            Some(77)
        );
    }

    #[test]
    fn extract_contact_from_search_for_company_rejects_company_phrases_as_names() {
        let sample = r#"
1. Rakamlarla Rönesans - Yönetim Kurulu
   URL: https://ronesans.com/biz-kimiz#rakamlarla-ronesans
   Yönetim Kurulu ve sürdürülebilirlik sosyal sorumluluk içerikleri
"#;
        let (name, title, _linkedin, _email) = extract_contact_from_search_for_company(
            sample,
            "ceo_then_founder",
            "Ronesans",
            "ronesans.com",
        );
        assert!(name.is_none());
        assert!(title.is_none());
    }

    #[test]
    fn structured_site_text_extracts_person_card_name_and_title() {
        let html = r#"
        <div class="item">
          <strong class="body">Naci AĞBAL</strong>
          <span class="desc">İcra Kurulu Başkanı ve CEO</span>
        </div>
        <div class="item">
          <strong class="body">Senlav GÜNER</strong>
          <span class="desc">İşletme ve Bakımdan Sorumlu Başkan Yardımcısı (COO)</span>
        </div>
        "#;
        let structured = canonicalize_contact_titles(&html_to_structured_text(html));
        let (name, title) =
            extract_contact_from_structured_site_text(&structured, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Senlav Güner"));
        assert!(title
            .as_deref()
            .map(|value| value.contains("COO"))
            .unwrap_or(false));
    }

    #[test]
    fn extract_contact_from_company_site_html_prefers_person_card_over_generic_heading() {
        let html = r#"
        <html>
          <body>
            <footer>
              <a href="/biz-kimiz#faaliyet-alanlari">Faaliyet Alanları</a>
              <a href="/biz-kimiz#suudi-arabistan">Suudi Arabistan</a>
            </footer>
            <div class="person-card">
              <strong class="body">Naci AĞBAL</strong>
              <span class="desc">İcra Kurulu Başkanı ve CEO</span>
            </div>
          </body>
        </html>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Naci Ağbal"));
        assert_eq!(title.as_deref(), Some("CEO"));
    }

    #[test]
    fn extract_contact_from_company_site_html_decodes_turkish_entities_in_cards() {
        let html = r#"
        <div class="yonetimDiv">
          <h1><a href="/Yonetim-Kurulu/Basar-Arioglu">Ba&#351;ar Ar&#305;o&#287;lu</a></h1>
          <span>Y&ouml;netim Kurulu Ba&#351;kan&#305;</span>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_yapi_merkezi_management_markup() {
        let html = r#"
        <div class="yonetimDiv">
            <h1>
                <a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank">Başar Arıoğlu</a>
            </h1>
            <span>Yapı Merkezi Holding Yönetim Kurulu Başkanı</span>
        </div>
        <p>
            <strong><a href="/Yonetim-Kurulu/Ulku-Arioglu" target="_blank">Ülkü Arıoğlu</a></strong>
            <div style="clear: both"></div>
            <span>Kurucu Üye</span>
        </p>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Ülkü Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Founder"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_parenthesized_board_entries() {
        let html = r#"
        <p>
            <span style="color: rgb(105, 105, 105); font-family: verdana, geneva, sans-serif; font-size: 11px;">
                Celal KOLOĞLU (Yönetim Kurulu Üyesi),
            </span><br />
            <span style="color: rgb(105, 105, 105); font-family: verdana, geneva, sans-serif; font-size: 11px;">
                Naci KOLOĞLU (Yönetim Kurulu Başkanı)
            </span>
        </p>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Naci Koloğlu"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_full_yapi_merkezi_section() {
        let html = r#"
        <div class="section-5">
            <h1 class="heading-11" style="text-transform: uppercase;">Yönetim</h1>
            <div>
                <div class="yonetimm">
                    <div class="yonetimDiv">
                        <img alt="" src="/uploads/images/03072018201333-02.jpg" style="width: 190px; height: 286px;" />
                        <h1>
                            <a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank">Başar Arıoğlu</a>
                        </h1>
                        <span>Yapı Merkezi Holding Yönetim Kurulu Başkanı</span>
                    </div>
                    <div class="yonetimDiv">
                        <img alt="" src="/images/uploads/03072018201333.jpg" style="width:190px; height:286px;" />
                        <h1>
                            <a href="/Yonetim-Kurulu/Koksal-Anadol" target="_blank">Köksal Anadol</a>
                        </h1>
                        <span>Yapı Merkezi Holding Başkan Vekili</span>
                    </div>
                </div>
                <div>
                    <h1 class="heading-11" style="text-transform: uppercase;">
                        Yapı Merkezi Holding Üst Yönetim
                    </h1>
                    <p>
                        <strong><a>Dr. Erdem Arıoğlu</a></strong>
                    </p>
                    <p>
                        <span>Yönetim Kurulu Üyesi</span>
                    </p>
                    <p>
                        <strong><a>S. Özge Arıoğlu</a></strong><br />
                        <span>Yönetim Kurulu Üyesi</span>
                    </p>
                    <div class="yListe">
                        <h1 class="heading-11" style="text-transform: uppercase;">
                            YAPI MERKEZİ İNŞAAT VE SANAYİ A.Ş.
                        </h1>
                    </div>
                </div>

                <p>
                    <strong><a href="/Yonetim-Kurulu/Basar-Arioglu" target="_blank" style="text-decoration: none;">Başar Arıoğlu</a></strong>
                    <div style="clear: both"></div>
                    <span>Yönetim Kurulu Başkanı</span>
                </p>

                <p>
                    <strong><a href="/Yonetim-Kurulu/Ulku-Arioglu" target="_blank" style="text-decoration: none;">Ülkü Arıoğlu</a></strong>
                    <div style="clear: both"></div>
                    <span>Kurucu Üye</span>
                </p>
            </div>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Ülkü Arıoğlu"));
        assert_eq!(title.as_deref(), Some("Founder"));
    }

    #[test]
    fn extract_contact_from_company_site_html_handles_accordion_name_title_markup() {
        let html = r#"
        <div class="accordion-item">
            <h2 class="accordion-header" id="heading0-1">
                <button class="accordion-button collapsed" type="button">
                    <span>Hamdi Akın – Yönetim Kurulu Başkanı</span>
                </button>
            </h2>
        </div>
        "#;

        let (name, title, _linkedin, _email, _evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert_eq!(name.as_deref(), Some("Hamdi Akın"));
        assert_eq!(title.as_deref(), Some("Chairman"));
    }

    #[test]
    fn parse_tmb_member_candidates_extracts_domain_and_contact_hint() {
        let html = r#"
        <article class="member-card h-100">
            <div class="title">
                <div class="member-name">
                    <div class="name">
                        <a href="/en/m/test/acme-construction">ACME CONSTRUCTION INC.</a>
                    </div>
                    <div class="ceo"><i>Chairman of the Board : <strong>Ahmet YILMAZ</strong></i></div>
                </div>
            </div>
            <div class="member-info">
                <table class="table table-borderless">
                    <tbody>
                        <tr>
                            <th scope="row">Mail</th>
                            <td>:</td>
                            <td><a href="mailto:info@acmeinsaat.com.tr">info@acmeinsaat.com.tr</a></td>
                        </tr>
                        <tr>
                            <th scope="row">Web</th>
                            <td>:</td>
                            <td><a href="http://www.acmeinsaat.com.tr" rel="noreferrer" target="_blank">www.acmeinsaat.com.tr</a></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </article>
        "#;
        let profile = SalesProfile {
            target_industry: "Field ops".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tmb_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "acmeinsaat.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Ahmet Yılmaz")
        );
        assert_eq!(
            candidates[0].contact_hint.contact_title.as_deref(),
            Some("Chairman")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@acmeinsaat.com.tr")
        );
        assert!(candidates[0]
            .candidate
            .source_links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/m/test/acme-construction"));
    }

    #[test]
    fn parse_eud_member_candidates_extracts_official_member_domains() {
        let html = r#"
        <a href='https://www.aksaenerji.com.tr/tr/ana-sayfa/' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        <a href='http://www.ictasenerji.com.tr/TR/Enerji' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        <a href='' target='_blank'>
            <div class='member-box'><img alt=''></div>
        </a>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_eud_member_candidates(html, &profile, 0, 8);
        let domains = candidates
            .iter()
            .map(|candidate| candidate.candidate.domain.as_str())
            .collect::<Vec<_>>();
        assert_eq!(domains, vec!["aksaenerji.com.tr", "ictasenerji.com.tr"]);
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "utility"));
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EUD members page")
        );
    }

    #[test]
    fn parse_asmud_member_candidates_extracts_domain_and_directory_email() {
        let html = r#"
        <div class="uwrap w3-card">
          <a href="https:/www.kemetyl.com.tr" target="_blank"><div class="uimg"></div></a>
          <strong>KEMETYL KİMYA SAN. VE TİC. A.Ş.</strong>
          <hr>
          T: (312) 555 00 00<br>
          E: <span style="word-break: break-all">info@kemetyl.com.tr</span><br>
          <hr>
          Ankara
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_asmud_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "kemetyl.com.tr");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@kemetyl.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("ASMUD members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "asphalt"));
    }

    #[test]
    fn parse_platformder_directory_candidates_extracts_domain_and_source() {
        let html = r#"
        <li id="item-7653-5" class="column3"
            data-title="Asel Vinç Platform"
            data-phone="0545 891 91 41"
            data-url="https://www.aselvincplatform.com">
            <div class="pd-bottom-area">
                <p><a href="tel:05458919141" title="Call 0545 891 91 41"><i class="fa fa-phone"></i></a></p>
                <p><a href="https://www.aselvincplatform.com" target="_blank" title="Go to website"><i class="fa fa-link"></i></a></p>
            </div>
        </li>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_platformder_directory_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "aselvincplatform.com");
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "platform rental"));
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "vinç"));
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("Platformder rehber")
        );
        assert!(candidates[0].candidate.evidence[0].contains("0545 891 91 41"));
    }

    #[test]
    fn parse_platformder_directory_candidates_falls_back_to_visible_website_link() {
        let html = r#"
        <li id="item-7655-2" class="column3"
            data-title="Giray Vinç Platform"
            data-phone="0534 767 12 02"
            data-url="">
            <img src="https://www.platformder.org.tr/wp-content/plugins/simple-business-directory-pro/assets/images/list-image-placeholder.png" alt="">
            <div class="pd-bottom-area">
                <p><a href="https://www.girayplatform.com.tr" target="_blank" title="Go to website"><i class="fa fa-link"></i></a></p>
            </div>
        </li>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_platformder_directory_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "girayplatform.com.tr");
    }

    #[test]
    fn parse_mib_member_candidates_extracts_domain_and_email() {
        let html = r#"
        <div data-elementor-type="loop-item" class="elementor e-loop-item post-111 firm type-firm">
            <div class="elementor-widget-container">
                <h2 class="elementor-heading-title elementor-size-default">
                    <a href="https://mib.org.tr/en/firm/abravinc/" data-penci-link="internal">ABRA VİNÇ SANAYİ VE TİCARET A.Ş.</a>
                </h2>
            </div>
            <ul class="elementor-icon-list-items">
                <li class="elementor-icon-list-item">
                    <a href="https://www.abravinc.com.tr/" target="_blank" rel="nofollow">
                        <span class="elementor-icon-list-icon"><i aria-hidden="true" class="fas fa-globe"></i></span>
                        <span class="elementor-icon-list-text">https://www.abravinc.com.tr/</span>
                    </a>
                </li>
                <li class="elementor-icon-list-item">
                    <a href="mailto:info@abravinc.com.tr" target="_blank">
                        <span class="elementor-icon-list-icon"><i aria-hidden="true" class="fas fa-envelope"></i></span>
                        <span class="elementor-icon-list-text">info@abravinc.com.tr</span>
                    </a>
                </li>
            </ul>
        </div>
        <span class="e-load-more-spinner"></span>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_mib_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "abravinc.com.tr");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@abravinc.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("MIB members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "industrial equipment"));
        assert!(candidates[0]
            .candidate
            .source_links
            .iter()
            .any(|value| value == "https://mib.org.tr/en/firm/abravinc/"));
    }

    #[test]
    fn osint_link_priority_prefers_member_detail_over_listing() {
        let detail = "https://www.tmb.org.tr/en/m/60fe744c002ab9647e98cbd4/akm-yapi-contracting-industry-and-commerce-co-inc";
        let listing = "https://www.tmb.org.tr/en/members";
        assert!(osint_link_priority(detail) > osint_link_priority(listing));
    }

    #[test]
    fn merge_osint_links_keeps_best_directory_link_per_source() {
        let links = merge_osint_links(
            vec![
                "https://www.tmb.org.tr/en/members".to_string(),
                "https://www.tmb.org.tr/en/m/60fe744c002ab9647e98cbd4/akm-yapi-contracting-industry-and-commerce-co-inc".to_string(),
                "https://mib.org.tr/en/our-members/3/".to_string(),
                "https://mib.org.tr/en/firm/yeter-makina-2/".to_string(),
                "https://imder.org.tr/uyelerimiz/".to_string(),
                "https://imder.org.tr/cozum-makina-sanayi-ve-ticaret-ltd-sti/".to_string(),
                "https://isder.org.tr/uyelerimiz/".to_string(),
                "https://isder.org.tr/cakmak-vinc-sanayi-ve-tic-a-s/".to_string(),
            ],
            Vec::new(),
        );

        assert!(links.iter().any(|value| value.contains("/en/m/")));
        assert!(links.iter().any(|value| value.contains("/firm/")));
        assert!(links
            .iter()
            .any(|value| value == "https://imder.org.tr/cozum-makina-sanayi-ve-ticaret-ltd-sti/"));
        assert!(links
            .iter()
            .any(|value| value == "https://isder.org.tr/cakmak-vinc-sanayi-ve-tic-a-s/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://www.tmb.org.tr/en/members"));
        assert!(!links
            .iter()
            .any(|value| value == "https://mib.org.tr/en/our-members/3/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://imder.org.tr/uyelerimiz/"));
        assert!(!links
            .iter()
            .any(|value| value == "https://isder.org.tr/uyelerimiz/"));
    }

    #[test]
    fn merge_osint_links_normalizes_variants_and_keeps_diverse_site_links() {
        let links = merge_osint_links(
            vec![
                "https://akmyapi.com.tr/kopyas%C4%B1-devam-eden-projeler?utm_source=test#hero"
                    .to_string(),
                "https://www.akmyapi.com.tr/kopyası-devam-eden-projeler/".to_string(),
                "https://akmyapi.com.tr/hakkimizda".to_string(),
                "https://akmyapi.com.tr/iletisim".to_string(),
            ],
            Vec::new(),
        );

        assert_eq!(
            links
                .iter()
                .filter(|value| value.contains("devam-eden-projeler"))
                .count(),
            1
        );
        assert!(links.iter().any(|value| value.contains("/hakkimizda")));
        assert!(links.iter().any(|value| value.contains("/iletisim")));
    }

    #[test]
    fn parse_imder_member_index_urls_extracts_detail_pages() {
        let html = r#"
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/mst-is-ve-tarim-makinalari-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/" tabindex="-1"></a>
        </article>
        "#;

        let urls = parse_imder_member_index_urls(html, 0, 8);
        assert_eq!(
            urls,
            vec![
                "https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/",
                "https://imder.org.tr/mst-is-ve-tarim-makinalari-san-ve-tic-a-s/"
            ]
        );
    }

    #[test]
    fn parse_imder_member_detail_candidate_extracts_domain_contact_and_keywords() {
        let html = r#"
        <h1 class="elementor-heading-title elementor-size-default">ALTAN HİDROLİK MÜHENDİSLİK SAN. VE TİC. A.Ş.</h1>
        <table class="table table-hover">
            <tbody>
                <tr><td><strong>İsim Soyisim</strong></td><td>TEVFİK ALTAN</td></tr>
                <tr><td><strong>Görevi</strong></td><td>Yönetim Kurulu Başkanı ve Genel Müdür</td></tr>
                <tr><td><strong>Telefon</strong></td><td>+90216 593 33 00</td></tr>
                <tr><td><strong>Web Sitesi</strong></td><td><a href="https://www.altanhidrolik.com.tr/" target="_blank" rel="noopener"><strong>https://www.altanhidrolik.com.tr/</strong></a></td></tr>
            </tbody>
        </table>
        <p><strong>Mobil İş Makineleri ve Yerinde Değerlendirme, Kurulum ve Bakım Hizmetleri</strong></p>
        Tags :
        <a href="https://imder.org.tr/tag/yedek-parca/" rel="tag">yedek parça</a>
        <a href="https://imder.org.tr/tag/telehandler/" rel="tag">telehandler</a>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidate = parse_imder_member_detail_candidate(
            html,
            "https://imder.org.tr/altan-hidrolik-muhendislik-san-ve-tic-a-s/",
            &profile,
        )
        .unwrap();

        assert_eq!(candidate.candidate.domain, "altanhidrolik.com.tr");
        assert_eq!(
            candidate.contact_hint.contact_name.as_deref(),
            Some("Tevfik Altan")
        );
        assert!(matches!(
            candidate.contact_hint.contact_title.as_deref(),
            Some("Chairman") | Some("CEO") | Some("General Manager")
        ));
        assert_eq!(
            candidate.contact_hint.source.as_deref(),
            Some("IMDER member detail")
        );
        assert!(candidate
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "telehandler"));
        assert!(candidate.candidate.evidence[0].contains("+90216 593 33 00"));
    }

    #[test]
    fn parse_isder_member_index_urls_extracts_detail_pages() {
        let html = r#"
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/asko-glob-all-marketing-dis-tic-a-s/" tabindex="-1"></a>
        </article>
        <article class="elementor-post">
            <a class="elementor-post__thumbnail__link" href="https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/" tabindex="-1"></a>
        </article>
        "#;

        let urls = parse_isder_member_index_urls(html, 0, 8);
        assert_eq!(
            urls,
            vec![
                "https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/",
                "https://isder.org.tr/asko-glob-all-marketing-dis-tic-a-s/"
            ]
        );
    }

    #[test]
    fn parse_isder_member_detail_candidate_extracts_domain_contact_and_keywords() {
        let html = r#"
        <h1 class="elementor-heading-title elementor-size-default">ACARLAR MAKİNE ACARLAR DIŞ TİCARET VE MAKİNE SANAYİ A.Ş.</h1>
        <table class="table table-hover">
            <tbody>
                <tr><td><strong>İsim Soyisim:</strong></td><td><table class="table table-hover"><tbody><tr><td>SERKAN ACAR</td></tr></tbody></table></td></tr>
                <tr><td><strong>Ünvan Görevi:</strong></td><td>Genel Müdür</td></tr>
                <tr><td><strong>Telefon:</strong></td><td>+90 (216) 581 4949</td></tr>
                <tr><td><strong>Web Sitesi:</strong></td><td>http://www.acarlarmakine.com</td></tr>
            </tbody>
        </table>
        <p>İstif makineleri, forklift ve servis çözümleri sunmaktadır.</p>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidate = parse_isder_member_detail_candidate(
            html,
            "https://isder.org.tr/acarlar-makine-acarlar-dis-ticaret-ve-makine-sanayi-a-s/",
            &profile,
        )
        .unwrap();

        assert_eq!(candidate.candidate.domain, "acarlarmakine.com");
        assert_eq!(
            candidate.contact_hint.contact_name.as_deref(),
            Some("Serkan Acar")
        );
        assert_eq!(candidate.contact_hint.contact_title.as_deref(), Some("CEO"));
        assert_eq!(
            candidate.contact_hint.source.as_deref(),
            Some("ISDER member detail")
        );
        assert!(candidate
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "forklift"));
        assert!(candidate.candidate.evidence[0].contains("+90 (216) 581 4949"));
    }

    #[test]
    fn parse_thbb_yazismali_candidates_extracts_domain_and_cf_email() {
        let html = r#"
        <div class="entry-content indented">
            <table>
                <tr>
                    <td><a href="http://www.bestaff.com.tr/" target="_blank"><strong><img src="/media/logo.jpg" alt=""></strong></a></td>
                </tr>
                <tr>
                    <td><strong>Bestaff İş Makineleri Dış Tic. San. ve Tic. Ltd. Şti.<br></strong><strong>Adres:</strong> Ankara<br><strong>Tel:</strong> 0552 784 05 05<br><strong>E-posta:</strong> <a href="/cdn-cgi/l/email-protection#f79e999198b795928483969191d994989ad98385"><span class="__cf_email__" data-cfemail="0b62656d644b696e787f6a6d6d25686466257f79">[email&#160;protected]</span></a><br><strong>Web:</strong> <a href="http://www.bestaff.com.tr" target="_blank">www.bestaff.com.tr</a></td>
                </tr>
            </table>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Field operations".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_thbb_yazismali_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "bestaff.com.tr");
        assert!(candidates[0].contact_hint.email.is_some());
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("THBB yazismali uyeler")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "construction equipment"));
    }

    #[test]
    fn parse_eder_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="ui-e-ico-box" onclick="window.open(&#039;https://www.kolaymagaza.com/&#039;, &#039;_blank&#039;)">
            <div class="ui-e-box-content">
                <div class="ui-e-description">
                    <p>Kolaymağaza E-Ticaret Yazılımları</p>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "E-commerce".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_eder_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "kolaymagaza.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EDER members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "digital commerce"));
    }

    #[test]
    fn parse_lojider_member_candidates_extracts_domain_contact_and_email() {
        let html = r#"
        <div class="row mb-4 member-row">
            <div class="Uye p-3">
                <div class="row">
                    <b class="d-block">2H Gümrük ve Lojistik Hizmetleri Tic. Ltd. Şti.</b>
                </div>
                <div class="row row-cols-1 row-cols-md-2 row-cols-lg-4 mt-2">
                    <div class="col">
                        <i class="fa-solid fa-phone me-2"></i>
                        <a href="tel:02163052325">0216 305 23 25</a>
                    </div>
                    <div class="col"><i class="fa-solid fa-envelope me-2"></i><a target="_blank" href="mailto:aysun@2hgumrukleme.com.tr">aysun@2hgumrukleme.com.tr</a></div>
                    <div class="col">
                        <i class="fa-solid fa-paper-plane me-2"></i>
                        <a target="_blank" href="http://2hgumrukleme.com.tr/">2hgumrukleme.com.tr/</a>
                    </div>
                    <div class="col"><i class="fa-solid fa-user me-2"></i>Aysun KÜÇÜKÇİTRAZ</div>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Logistics".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_lojider_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "2hgumrukleme.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Aysun Küçükçitraz")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("aysun@2hgumrukleme.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("LojiDer members page")
        );
    }

    #[test]
    fn parse_tfyd_member_candidates_extracts_domain_and_phone() {
        let html = r#"
        <table>
            <tr>
                <td colspan="2"><div align="center">KURULUŞ</div></td>
                <td><div align="center">WEB SİTESİ</div></td>
                <td><div align="center">TELEFON</div></td>
            </tr>
            <tr>
                <td align="center">1</td>
                <td align="left">&nbsp;AJANS ASYA FUARCILIK ORG. LTD. ŞTİ</td>
                <td align="left">&nbsp;www.vanfuar.com&nbsp;</td>
                <td align="center">0432 215 81 80</td>
            </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Events & Exhibitions".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tfyd_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "vanfuar.com");
        assert_eq!(
            candidates[0].candidate.phone.as_deref(),
            Some("+904322158180")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TFYD members page")
        );
    }

    #[test]
    fn parse_oss_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="card membercard">
            <div class="card-body">
                <h5 class="card-title"> 5S Otomotiv İmalat San. ve Tic. A.Ş. </h5>
            </div>
            <ul class="list-group list-group-flush">
                <li class="list-group-item">
                    <i class="bi bi-telephone-fill"></i>  444 52 89
                </li>
                <li class="list-group-item">
                    <i class="bi bi-globe"></i>
                    <a onclick="window.open('http://www.5sotomotiv.com')" href="javascript:void(0)">5sotomotiv.com </a>
                </li>
            </ul>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Automotive".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_oss_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "5sotomotiv.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("OSS members page")
        );
        assert!(candidates[0]
            .candidate
            .matched_keywords
            .iter()
            .any(|value| value == "automotive aftermarket"));
    }

    #[test]
    fn parse_ida_member_candidates_extracts_domain_and_contact() {
        let html = r#"
        <table border="0" cellspacing="0" cellpadding="3">
            <tbody>
                <tr>
                    <td colspan="2"><strong>ARTI İletişim Yönetimi</strong></td>
                </tr>
                <tr>
                    <td style="white-space: nowrap;"><strong>Yönetici Ortak:</strong></td>
                    <td>Esra ŞENGÜLEN ÜNSÜR</td>
                </tr>
                <tr>
                    <td><strong>Telefon:</strong></td>
                    <td>+90 212 347 03 30</td>
                </tr>
                <tr>
                    <td><strong>Web:</strong></td>
                    <td><a href="http://www.artipr.com.tr/" target="_blank">www.artipr.com.tr</a></td>
                </tr>
            </tbody>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "PR agency".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_ida_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "artipr.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Esra Şengülen Ünsür")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("IDA members page")
        );
    }

    #[test]
    fn parse_tesid_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="boxuye_detay">
            <p><strong>BÜYÜK FİRMALAR</strong></p>
            <p><strong><a href="http://www.karel.com.tr" target="_blank">KAREL Elektronik Sanayi ve Ticaret A.Ş.</a></strong></p>
        </div>
        <p><strong><a href="https://tesid.org.tr/alt_sektor_dagilimi">TESİD ÜYELERİ ALT SEKTÖR DAĞILIMI</a></strong></p>
        "#;
        let profile = SalesProfile {
            target_industry: "Electronics".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tesid_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "karel.com.tr");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TESID members page")
        );
    }

    #[test]
    fn parse_tudis_member_candidates_extracts_domain_and_email() {
        let html = r#"
        <table>
            <tr>
                <td><strong>Cihan Deri San.A.Ş.</strong></td>
                <td>info@cihanderi.com</td>
                <td><a href="https://www.cihanderi.com/">https://www.cihanderi.com/</a></td>
            </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Leather".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tudis_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "cihanderi.com");
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@cihanderi.com")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TUDIS members page")
        );
    }

    #[test]
    fn parse_emsad_member_candidates_extracts_domain_contact_and_email() {
        let html = r#"
        <table width="100%" border="0" cellspacing="1" cellpadding="2">
          <tr>
            <td width="192" align="center" valign="top"><img class="foto1" width="128px" height="58px" src="/Resim/495,aksanpng.png?0" /></td>
            <td width="788" align="left" valign="top"><b>AKSAN PANO TANITIM İNŞ. ELK. İML. TAAH. VE PAZ. TİC. LTD. ŞTİ</b><br />
             <b>Temsilci Adı:</b> Şahin ŞANLITÜRK<br />
             <b>Adres:</b> Kahramankazan / ANKARA<br />
             <b> Tel: </b> 0312 386 12 08<br />
             <b> e-posta: </b> info@aksanpano.com.tr<br />
             <b>  web:</b> <a target="_blank" href="http://www.aksanpano.com.tr">www.aksanpano.com.tr</a><br /><br />
            </td>
          </tr>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Electromechanical".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_emsad_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "aksanpano.com.tr");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Şahin Şanlıtürk")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@aksanpano.com.tr")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("EMSAD members page")
        );
    }

    #[test]
    fn parse_tgsd_member_candidates_extracts_company_contact_and_domain() {
        let html = r#"
        <table id="aplus-uye-listesi">
            <tbody>
                <tr>
                    <td><img src="https://tgsd.org.tr/wp-content/uploads/2025/11/Suglobal_Denimvillage_logo.jpg" alt="Logo" class="aplus-logo"></td>
                    <td>Abdulhadi Karasu</td>
                    <td>Suglobal Tekstil ve Konfeksiyon San. A.Ş.</td>
                    <td><a href="https://www.denimvillage.com" target="_blank" rel="noopener">www.denimvillage.com</a></td>
                </tr>
            </tbody>
        </table>
        "#;
        let profile = SalesProfile {
            target_industry: "Textile".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_tgsd_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "denimvillage.com");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("Abdulhadi Karasu")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TGSD members page")
        );
    }

    #[test]
    fn parse_ared_member_candidates_extracts_domain_contact_and_phone() {
        let html = r#"
        <div class="col-lg-12 load-post">
            <article class="post hentry post-list post-list-small">
                <div class="content-entry-wrap">
                    <div class="entry-content">
                        <h3 class="entry-title">24 Saat Dijital Baskı - İstanbul</h3>
                    </div>
                    <div class="entry-meta-content">
                        <div class="entry-date">
                            <span><i class="fa fa-user pr-1"></i>İsa Yavuz </span><br>
                            <span><i class="fa fa-phone-square pr-1"></i>(0212) 268 28 77 </span><br>
                            <span><i class="fa fa-globe pr-1"></i><a href="mailto:info@24saatdijital.com">info@24saatdijital.com</a> - <a href="http://www.24saatdijital.com" target="_blank">http://www.24saatdijital.com</a></span><br>
                            <span><i class="fa fa-building pr-1"></i>Baskı Hizmetleri</span>
                        </div>
                    </div>
                </div>
            </article>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Signage".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_ared_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "24saatdijital.com");
        assert_eq!(
            candidates[0].contact_hint.contact_name.as_deref(),
            Some("İsa Yavuz")
        );
        assert_eq!(
            candidates[0].contact_hint.email.as_deref(),
            Some("info@24saatdijital.com")
        );
        assert_eq!(
            candidates[0].candidate.phone.as_deref(),
            Some("+902122682877")
        );
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("ARED members page")
        );
    }

    #[test]
    fn parse_todeb_member_candidates_extracts_domain_and_source() {
        let html = r#"
        <div class="flexCerceve logoBorder">
            <div class="row">
                <div class="col-lg-6">
                    <a href="https://1000pay.com" target="_blank">
                        <img src="https://todeb.org.tr/source/uye_iliskileri/uye_logolari/1000pay.png" class="img-responsive">
                    </a>
                </div>
                <div class="col-lg-6">
                    <p><h2>1000 Ödeme Hizmetleri ve Elektronik Para A.Ş.</h2>
                    <p><strong>Telefon: <br /></strong>444 10 04<br />
                    <strong>Web:<br /></strong><a href="https://1000pay.com">www.1000pay.com</a></p></p>
                </div>
            </div>
        </div>
        "#;
        let profile = SalesProfile {
            target_industry: "Payments".to_string(),
            target_geo: "TR".to_string(),
            ..SalesProfile::default()
        };

        let candidates = parse_todeb_member_candidates(html, &profile, 0, 8);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].candidate.domain, "1000pay.com");
        assert_eq!(
            candidates[0].contact_hint.source.as_deref(),
            Some("TODEB members page")
        );
    }

    #[test]
    fn source_contact_hint_overrides_placeholder_contact() {
        let hint = SourceContactHint {
            contact_name: Some("Ahmet Yılmaz".to_string()),
            contact_title: Some("Chairman".to_string()),
            email: Some("info@acmeinsaat.com.tr".to_string()),
            source: Some("TMB members directory".to_string()),
        };
        let mut contact_name = None;
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "acmeinsaat.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(contact_name.as_deref(), Some("Ahmet Yılmaz"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert!(email.is_none());
    }

    #[test]
    fn source_contact_hint_replaces_generic_team_placeholder_name() {
        let hint = SourceContactHint {
            contact_name: Some("Ahmet Yılmaz".to_string()),
            contact_title: Some("Chairman".to_string()),
            email: Some("info@acmeinsaat.com.tr".to_string()),
            source: Some("TMB members directory".to_string()),
        };
        let mut contact_name = Some("Leadership Team".to_string());
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "acmeinsaat.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(contact_name.as_deref(), Some("Ahmet Yılmaz"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert!(email.is_none());
    }

    #[test]
    fn source_contact_hint_allows_trusted_offdomain_directory_mailbox() {
        let hint = SourceContactHint {
            email: Some("cargill_turkey@cargill.com".to_string()),
            source: Some("ASMUD members page".to_string()),
            ..SourceContactHint::default()
        };
        let mut contact_name = None;
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut email = None;

        apply_source_contact_hint(
            "cargill.com.tr",
            &hint,
            &mut contact_name,
            &mut contact_title,
            &mut email,
        );

        assert_eq!(email.as_deref(), Some("cargill_turkey@cargill.com"));
    }

    #[test]
    fn merge_free_discovery_candidate_boosts_named_directory_sources() {
        let mut candidates = HashMap::<String, DomainCandidate>::new();
        let mut hints = HashMap::<String, SourceContactHint>::new();
        merge_free_discovery_candidate(
            &mut candidates,
            &mut hints,
            FreeDiscoveryCandidate {
                candidate: DomainCandidate {
                    domain: "acmeinsaat.com.tr".to_string(),
                    score: MIN_DOMAIN_RELEVANCE_SCORE + 12,
                    evidence: vec!["TMB member".to_string()],
                    matched_keywords: vec!["construction".to_string()],
                    source_links: vec!["https://www.tmb.org.tr/en/members".to_string()],
                    phone: None,
                },
                contact_hint: SourceContactHint {
                    contact_name: Some("Ahmet Yılmaz".to_string()),
                    contact_title: Some("Chairman".to_string()),
                    email: Some("info@acmeinsaat.com.tr".to_string()),
                    source: Some("TMB members directory".to_string()),
                },
            },
        );

        assert_eq!(
            candidates.get("acmeinsaat.com.tr").map(|value| value.score),
            Some(MIN_DOMAIN_RELEVANCE_SCORE + 30)
        );
        assert!(candidates
            .get("acmeinsaat.com.tr")
            .map(|value| {
                value
                    .source_links
                    .iter()
                    .any(|url| url == "https://www.tmb.org.tr/en/members")
            })
            .unwrap_or(false));
    }

    #[test]
    fn internal_enrich_links_prioritize_management_profiles() {
        let base = url::Url::parse("https://ronesans.com").unwrap();
        let html = r#"
        <a href="/biz-kimiz">Biz Kimiz</a>
        <a href="/biz-kimiz#faaliyet-alanlari">Faaliyet Alanları</a>
        <a href="/iletisim">İletişim</a>
        <a href="/ust-yonetim/dr-erman-ilicak">Erman Ilıcak</a>
        <a href="/hakkimizda/yonetim-kurulu">Yönetim Kurulu</a>
        "#;

        let links = extract_internal_enrich_links(&base, html);
        assert_eq!(
            links.first().map(|value| value.as_str()),
            Some("https://ronesans.com/ust-yonetim/dr-erman-ilicak")
        );
        assert!(links
            .iter()
            .any(|value| value == "https://ronesans.com/hakkimizda/yonetim-kurulu"));
    }

    #[test]
    fn internal_enrich_links_accept_www_redirect_aliases() {
        let base = url::Url::parse("https://akfenren.com.tr").unwrap();
        let html = r#"
        <a href="https://www.akfenren.com.tr/kurumsal/yonetim-kurulu-ve-ust-yonetim/">
            Yönetim Kurulu ve Üst Yönetim
        </a>
        "#;

        let links = extract_internal_enrich_links(&base, html);
        assert!(links.iter().any(|value| {
            value == "https://www.akfenren.com.tr/kurumsal/yonetim-kurulu-ve-ust-yonetim/"
        }));
    }

    #[test]
    fn select_company_site_enrich_links_prefers_discovered_links_before_defaults() {
        let base = url::Url::parse("https://yapimerkezi.com.tr").unwrap();
        let html = r#"
        <a href="/Uzmanlik-Alanlari/Izleme-Denetim-ve-Iletisim">İzleme</a>
        <a href="/Iletisim-Aydinlatma-Metni">İletişim</a>
        <a href="/Hakkinda/Yonetim">Yönetim</a>
        <a href="/Projeler/Devam-Eden-Projeler">Projeler</a>
        "#;

        let links = select_company_site_enrich_links(&base, html, Vec::new());
        assert!(links
            .iter()
            .any(|value| value == "https://yapimerkezi.com.tr/Hakkinda/Yonetim"));
        assert!(!links[..links.len().min(4)]
            .iter()
            .any(|value| value == "https://yapimerkezi.com.tr/hakkimizda/yonetim/icra-kurulu"));
    }

    #[test]
    fn select_company_site_enrich_links_prefers_diverse_categories() {
        let base = url::Url::parse("https://akmyapi.com.tr").unwrap();
        let html = r#"
        <a href="/kopyas%C4%B1-devam-eden-projeler?utm=1">Devam Eden Projeler</a>
        <a href="https://www.akmyapi.com.tr/kopyası-devam-eden-projeler/">Devam Eden Projeler Duplicate</a>
        <a href="/kopyası-tamamlanan-projeler">Tamamlanan Projeler</a>
        <a href="/hakkimizda">Hakkımızda</a>
        <a href="/iletisim#harita">İletişim</a>
        <a href="/yonetim-kurulu">Yönetim Kurulu</a>
        "#;

        let links = select_company_site_enrich_links(&base, html, Vec::new());
        assert_eq!(links.len(), 3);
        assert!(links.iter().any(|value| value.contains("/yonetim-kurulu")));
        assert!(links.iter().any(|value| value.contains("/hakkimizda")));
        assert!(links.iter().any(|value| value.contains("/iletisim")));
        assert!(!links.iter().any(|value| value.contains("projeler")));
    }

    #[test]
    fn normalize_contact_email_for_domain_rejects_external_domain() {
        let kept = normalize_contact_email_for_domain(
            Some("ceo@sub.example.com".to_string()),
            "example.com",
        );
        let dropped =
            normalize_contact_email_for_domain(Some("ceo@other.com".to_string()), "example.com");
        assert_eq!(kept.as_deref(), Some("ceo@sub.example.com"));
        assert!(dropped.is_none());
    }

    #[test]
    fn normalize_site_contact_email_accepts_verified_offdomain_mailbox() {
        let kept = normalize_site_contact_email(Some("yminfo@ym.com.tr".to_string()));
        assert_eq!(kept.as_deref(), Some("yminfo@ym.com.tr"));
    }

    #[test]
    fn repair_common_mojibake_utf8_recovers_turkish_text() {
        let repaired = repair_common_mojibake_utf8("ASMÃD :: Ãyelerimiz / Members");
        assert_eq!(repaired, "ASMÜD :: Üyelerimiz / Members");
    }

    #[test]
    fn extract_domain_repairs_missing_scheme_slash() {
        let domain = extract_domain("https:/www.kemetyl.com.tr");
        assert_eq!(domain.as_deref(), Some("kemetyl.com.tr"));
    }

    #[test]
    fn extract_domain_rejects_asset_tld_hosts() {
        assert!(extract_domain("https://emirliftdernekweblogo-80x80.jpg").is_none());
        assert!(extract_domain("https://hero-banner.webp").is_none());
    }

    #[test]
    fn mib_directory_pages_for_run_wraps_across_catalog() {
        assert_eq!(mib_directory_pages_for_run(0, 9, 3), vec![1, 2, 3]);
        assert_eq!(mib_directory_pages_for_run(7, 9, 3), vec![8, 9, 1]);
        assert_eq!(mib_directory_pages_for_run(8, 9, 3), vec![9, 1, 2]);
    }

    #[test]
    fn source_hint_contact_richness_bonus_prefers_named_contacts() {
        let generic = SourceContactHint {
            contact_name: Some("Leadership Team".to_string()),
            contact_title: Some("CEO/Founder".to_string()),
            email: None,
            source: Some("directory".to_string()),
        };
        let rich = SourceContactHint {
            contact_name: Some("Aylin Demir".to_string()),
            contact_title: Some("CEO".to_string()),
            email: Some("aylin@example.com".to_string()),
            source: Some("directory".to_string()),
        };

        assert_eq!(source_hint_contact_richness_bonus(&generic), 0);
        assert!(source_hint_contact_richness_bonus(&rich) >= 14);
    }

    #[test]
    fn extract_email_from_text_decodes_cloudflare_cfemail() {
        let html = r#"<a class="__cf_email__" data-cfemail="127b7c747d527f73717a7b7c7b666b3c737b">[email&#160;protected]</a>"#;
        let email = extract_email_from_text(html);
        assert_eq!(email.as_deref(), Some("info@machinity.ai"));
    }

    #[test]
    fn normalize_email_candidate_rejects_asset_filenames() {
        assert!(normalize_site_contact_email(Some("logo@2x.png".to_string())).is_none());
        assert!(normalize_site_contact_email(Some("hero@banner.webp".to_string())).is_none());
    }

    #[test]
    fn normalize_contact_email_for_domain_rejects_generic_or_consumer_inboxes() {
        assert!(normalize_contact_email_for_domain(
            Some("info@ornekbakim.com.tr".to_string()),
            "ornekbakim.com.tr"
        )
        .is_none());
        assert!(normalize_contact_email_for_domain(
            Some("info@gmail.com".to_string()),
            "gmail.com"
        )
        .is_none());
        assert_eq!(
            normalize_contact_email_for_domain(
                Some("kiralama@artiplatform.com.tr".to_string()),
                "artiplatform.com.tr"
            )
            .as_deref(),
            Some("kiralama@artiplatform.com.tr")
        );
    }

    #[test]
    fn guessed_email_requires_plausible_person_name() {
        let ok = guessed_email(Some("John Doe"), "example.com");
        let bad = guessed_email(Some("Experience Like No Other"), "example.com");
        let placeholder = guessed_email(Some("Leadership Team"), "example.com");
        assert_eq!(ok.as_deref(), Some("john.doe@example.com"));
        assert!(bad.is_none());
        assert!(placeholder.is_none());
    }

    #[test]
    fn normalize_outreach_linkedin_url_rejects_company_pages() {
        let personal = normalize_outreach_linkedin_url("https://www.linkedin.com/in/jane-doe");
        let company =
            normalize_outreach_linkedin_url("https://www.linkedin.com/company/pulsivo-salesman");
        assert_eq!(
            personal.as_deref(),
            Some("https://www.linkedin.com/in/jane-doe")
        );
        assert!(company.is_none());
    }

    #[test]
    fn lead_requires_actionable_channel() {
        assert!(lead_has_outreach_channel(
            Some(&"ceo@example.com".to_string()),
            None
        ));
        assert!(lead_has_outreach_channel(
            None,
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(!lead_has_outreach_channel(
            Some(&"info@example.com".to_string()),
            None
        ));
        assert!(!lead_has_outreach_channel(None, None));
    }

    #[test]
    fn build_prospect_profiles_downgrades_generic_inbox_only_contacts() {
        let leads = vec![SalesLead {
            id: "lead-1".to_string(),
            run_id: "run-1".to_string(),
            company: "Acme".to_string(),
            website: "https://acme.example".to_string(),
            company_domain: "acme.example".to_string(),
            contact_name: "Leadership Team".to_string(),
            contact_title: "CEO/Founder".to_string(),
            linkedin_url: None,
            email: Some("info@acme.example".to_string()),
            phone: None,
            reasons: vec!["Public evidence: listed in sector directory".to_string()],
            email_subject: String::new(),
            email_body: String::new(),
            linkedin_message: String::new(),
            score: 84,
            status: "draft_ready".to_string(),
            created_at: "2026-03-25T10:00:00Z".to_string(),
        }];

        let profiles = build_prospect_profiles(leads, 10, None);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].profile_status, "company_only");
        assert!(profiles[0].primary_email.is_none());
    }

    #[test]
    fn lead_requires_person_level_identity() {
        assert!(lead_has_person_identity(
            Some("Jane Doe"),
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(lead_has_person_identity(
            None,
            Some(&"https://www.linkedin.com/in/jane-doe".to_string())
        ));
        assert!(!lead_has_person_identity(
            Some("Leadership Team"),
            Some(&"https://www.linkedin.com/company/pulsivo-salesman".to_string())
        ));
        assert!(!lead_has_person_identity(Some("Leadership Team"), None));
    }

    #[test]
    fn field_ops_lead_requires_verified_site_signal() {
        // Non-field-ops always passes
        assert!(lead_has_verified_company_signal(
            false,
            Some("Company website mentions maintenance"),
            false
        ));
        // Field-ops with site evidence passes
        assert!(lead_has_verified_company_signal(
            true,
            Some("Company website mentions maintenance"),
            false
        ));
        // Field-ops with LLM validation passes even without site evidence
        assert!(lead_has_verified_company_signal(true, None, true));
        // Field-ops without either fails
        assert!(!lead_has_verified_company_signal(true, None, false));
        assert!(!lead_has_verified_company_signal(true, Some("   "), false));
    }

    #[test]
    fn geo_is_turkey_matches_common_variants() {
        assert!(geo_is_turkey("TR"));
        assert!(geo_is_turkey("Turkey"));
        assert!(geo_is_turkey("Türkiye"));
        assert!(!geo_is_turkey("DE"));
    }

    #[test]
    fn heuristic_plan_adds_local_field_ops_queries_for_turkey() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "AI teammate for field ops".to_string(),
            target_industry:
                "Field service operations, maintenance services, installation services".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let draft = heuristic_lead_query_plan(&profile);
        assert!(draft
            .discovery_queries
            .iter()
            .any(|q| q.contains("saha servis") || q.contains("bakim onarim")));
        assert!(draft
            .must_include_keywords
            .iter()
            .any(|kw| kw == "tesis yonetimi"));
        assert!(draft
            .discovery_queries
            .iter()
            .all(|q| !q.contains("CEO") && !q.contains("COO")));
    }

    #[test]
    fn sanitize_discovery_query_removes_exec_title_noise() {
        let sanitized =
            sanitize_discovery_query("field service maintenance companies TR COO CEO operations");
        assert_eq!(
            sanitized.as_deref(),
            Some("field service maintenance companies TR operations")
        );
    }

    #[test]
    fn normalize_discovery_focus_term_rejects_generic_noise() {
        assert!(normalize_discovery_focus_term("operations").is_none());
        assert!(normalize_discovery_focus_term("workflow").is_none());
        assert_eq!(
            normalize_discovery_focus_term("field service maintenance"),
            Some("field service maintenance".to_string())
        );
    }

    #[test]
    fn adaptive_discovery_queries_add_targeted_follow_ups_for_turkey() {
        let profile = SalesProfile {
            product_name: "Machinity".to_string(),
            product_description: "AI teammate for field ops, dispatch, maintenance workflows"
                .to_string(),
            target_industry:
                "Field service operations, maintenance services, installation services".to_string(),
            target_geo: "TR".to_string(),
            sender_name: "Machinity".to_string(),
            sender_email: "info@machinity.ai".to_string(),
            sender_linkedin: None,
            target_title_policy: "ceo_then_founder".to_string(),
            daily_target: 20,
            daily_send_cap: 20,
            schedule_hour_local: 9,
            timezone_mode: "local".to_string(),
        };

        let lead_plan = heuristic_lead_query_plan(&profile);
        let queries = build_adaptive_discovery_queries(&lead_plan, &profile, &[]);

        assert!(!queries.is_empty());
        assert!(queries.iter().any(|query| query.contains("site:.tr")));
        assert!(queries.iter().any(|query| {
            query.to_lowercase().contains("saha operasyon")
                || query.to_lowercase().contains("sirketleri")
                || query.to_lowercase().contains("firmalari")
        }));
    }

    #[test]
    fn sitemap_location_parser_extracts_urls() {
        let xml = r#"
            <urlset>
              <url><loc>https://example.com/services/maintenance</loc></url>
              <url><loc>https://example.com/contact</loc></url>
            </urlset>
        "#;
        let urls = extract_sitemap_locations(xml);
        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("/services/maintenance"));
        assert!(urls[1].contains("/contact"));
    }

    #[test]
    fn enrich_target_path_matches_service_and_contact_pages() {
        assert!(path_looks_like_enrich_target("/services/field-service"));
        assert!(path_looks_like_enrich_target("/iletisim"));
        assert!(!path_looks_like_enrich_target("/blog/company-news"));
    }

    #[test]
    fn extract_domains_from_text_pulls_referenced_company_domains() {
        let domains = extract_domains_from_text(
            "Official Website: www.ym.com.tr and contact yminfo@ym.com.tr",
        );
        assert!(domains.iter().any(|d| d == "ym.com.tr"));
    }

    #[test]
    fn extract_company_site_html_accepts_turkish_field_signal_and_offdomain_email() {
        let html = r#"
        <html>
          <body>
            <h1>Yapi Merkezi teknik servis ve santiye operasyonlari</h1>
            <p>Genel Müdür Aslan Uzun</p>
            <a href="mailto:yminfo@ym.com.tr">Bize ulasin</a>
          </body>
        </html>
        "#;
        let (_name, title, _linkedin, email, evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert!(title
            .as_deref()
            .map(|value| value.contains("CEO"))
            .unwrap_or(false));
        assert_eq!(email.as_deref(), Some("yminfo@ym.com.tr"));
        assert!(evidence
            .as_deref()
            .map(|value| value.contains("teknik servis") || value.contains("santiye"))
            .unwrap_or(false));
    }

    #[test]
    fn extract_company_site_html_rejects_navigation_noise_and_asset_emails() {
        let html = r#"
        <html>
          <head>
            <title>Anasayfa | Kolin</title>
            <meta property="og:title" content="Reconstruction de la route Kandahar-Herat" />
          </head>
          <body>
            <a href="/tr/kurumsal/kisisel-verilerin-korunmasi-kanunu">Kişisel Verilerin Korunması Kanunu</a>
            <a href="/tr/kurumsal/finansal-gostergeler">Finansal Göstergeler</a>
            <img src="/Templates/Default/assets/img/logo.png" srcset="/Templates/Default/assets/img/logo@2x.png 2x" alt="Kolin İnşaat">
            <p>Kolin İnşaat altyapı ve construction projeleri yürütür.</p>
          </body>
        </html>
        "#;

        let (name, title, _linkedin, email, evidence) =
            extract_contact_from_company_site_html(html, "ceo_then_founder");
        assert!(name.is_none());
        assert_eq!(title.as_deref(), Some("CEO/Founder"));
        assert!(email.is_none());
        assert!(evidence
            .as_deref()
            .map(|value| value.contains("altyapı") || value.contains("construction"))
            .unwrap_or(false));
    }

    #[test]
    fn site_contact_candidate_signal_prefers_named_exec_page() {
        let generic_score = site_contact_candidate_signal(
            None,
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        let executive_score = site_contact_candidate_signal(
            Some(&"Naci Ağbal".to_string()),
            Some(&"CEO".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        assert!(executive_score > generic_score);
    }

    #[test]
    fn site_contact_candidate_signal_does_not_reward_placeholder_name() {
        let placeholder_score = site_contact_candidate_signal(
            Some(&"Leadership Team".to_string()),
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        let generic_score = site_contact_candidate_signal(
            None,
            Some(&"CEO/Founder".to_string()),
            None,
            Some(&"info@example.com".to_string()),
            Some(&"Company website mentions 'kurulum'".to_string()),
        );
        assert_eq!(placeholder_score, generic_score);
    }

    #[test]
    fn site_enrichment_replaces_placeholder_identity_with_real_exec() {
        let enrichment = SiteContactEnrichment {
            name: Some("Başar Arıoğlu".to_string()),
            title: Some("Chairman".to_string()),
            linkedin_url: None,
            company_linkedin_url: Some(
                "https://www.linkedin.com/company/yapi-merkezi/".to_string(),
            ),
            email: Some("yminfo@ym.com.tr".to_string()),
            evidence: Some("Company website mentions 'altyapı'".to_string()),
            osint_links: vec![
                "https://www.linkedin.com/company/yapi-merkezi/".to_string(),
                "https://yapimerkezi.com.tr/yonetim".to_string(),
            ],
            tech_stack: Vec::new(),
            job_posting_signals: Vec::new(),
            signal: site_contact_candidate_signal(
                Some(&"Başar Arıoğlu".to_string()),
                Some(&"Chairman".to_string()),
                None,
                Some(&"yminfo@ym.com.tr".to_string()),
                Some(&"Company website mentions 'altyapı'".to_string()),
            ),
        };
        let mut contact_name = Some("Leadership Team".to_string());
        let mut contact_title = Some("CEO/Founder".to_string());
        let mut linkedin_url = None;
        let mut company_linkedin_url = None;
        let mut email = Some("yminfo@ym.com.tr".to_string());
        let mut osint_links = Vec::new();
        let mut email_from_verified_site = false;
        let mut site_evidence = Some("Company website mentions 'altyapı'".to_string());

        apply_site_contact_enrichment(
            "yapimerkezi.com.tr",
            &enrichment,
            &mut contact_name,
            &mut contact_title,
            &mut linkedin_url,
            &mut company_linkedin_url,
            &mut email,
            &mut osint_links,
            &mut email_from_verified_site,
            &mut site_evidence,
        );

        assert_eq!(contact_name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(contact_title.as_deref(), Some("Chairman"));
        assert_eq!(email.as_deref(), Some("yminfo@ym.com.tr"));
        assert_eq!(
            company_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/company/yapi-merkezi/")
        );
        assert!(osint_links
            .iter()
            .any(|value| value.contains("linkedin.com/company/yapi-merkezi")));
        assert!(email_from_verified_site);
    }

    #[test]
    fn best_site_contact_enrichment_combines_identity_with_signal_and_email() {
        let pages = SiteFetchBundle {
            osint_links: vec![
                "https://yapimerkezi.com.tr".to_string(),
                "https://yapimerkezi.com.tr/yonetim".to_string(),
            ],
            pages: vec![
                SiteHtmlPage {
                    url: "https://yapimerkezi.com.tr".to_string(),
                    html: r#"
            <html>
              <body>
                <h1>Yapı Merkezi altyapı ve inşaat projeleri</h1>
                <a href="mailto:yminfo@ym.com.tr">İletişim</a>
              </body>
            </html>
            "#
                    .to_string(),
                },
                SiteHtmlPage {
                    url: "https://yapimerkezi.com.tr/yonetim".to_string(),
                    html: r#"
            <html>
              <body>
                <p><strong><a href="/yonetim/basar-arioglu">Başar Arıoğlu</a></strong></p>
                <p><span>Yönetim Kurulu Başkanı</span></p>
              </body>
            </html>
            "#
                    .to_string(),
                },
            ],
        };

        let enrichment = best_site_contact_enrichment(pages, "ceo_then_founder");
        assert_eq!(enrichment.name.as_deref(), Some("Başar Arıoğlu"));
        assert_eq!(enrichment.title.as_deref(), Some("Chairman"));
        assert_eq!(enrichment.email.as_deref(), Some("yminfo@ym.com.tr"));
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("/yonetim")));
        assert!(enrichment
            .evidence
            .as_deref()
            .map(|value| value.contains("altyapı") || value.contains("insaat"))
            .unwrap_or(false));
    }

    #[test]
    fn best_search_contact_enrichment_keeps_company_linkedin_separate() {
        let outputs = vec![r#"
1. Bergiz Holding | LinkedIn
URL: https://www.linkedin.com/company/bergiz-holding/
Bergiz Holding resmi LinkedIn sayfasi

2. Bergiz Holding - Sirket Profili
URL: https://bergiz.com.tr/Hakkimizda/Detay/SirketProfili
Bergiz Holding altyapi ve insaat projeleri yurutur.
            "#
        .to_string()];

        let enrichment = best_search_contact_enrichment(
            &outputs,
            "ceo_then_founder",
            "Bergiz Holding",
            "bergiz.com.tr",
        );
        assert!(enrichment.linkedin_url.is_none());
        assert_eq!(
            enrichment.company_linkedin_url.as_deref(),
            Some("https://www.linkedin.com/company/bergiz-holding/")
        );
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("linkedin.com/company/bergiz-holding")));
        assert!(enrichment
            .osint_links
            .iter()
            .any(|value| value.contains("bergiz.com.tr/Hakkimizda/Detay/SirketProfili")));
    }

    #[test]
    fn site_contact_enrichment_has_signal_for_company_linkedin_only() {
        let enrichment = SiteContactEnrichment {
            company_linkedin_url: Some(
                "https://www.linkedin.com/company/bergiz-holding/".to_string(),
            ),
            ..SiteContactEnrichment::default()
        };
        assert!(site_contact_enrichment_has_signal(&enrichment));
    }

    #[test]
    fn contact_title_priority_ignores_bare_board_labels() {
        assert_eq!(contact_title_priority("Yönetim Kurulu"), 0);
        assert!(contact_title_priority("Yönetim Kurulu Başkanı") > 0);
        assert_eq!(contact_title_priority("Finansal Göstergeler"), 0);
        assert!(contact_title_priority("Finance Director") > 0);
    }

    #[test]
    fn contact_title_priority_rejects_sentence_like_titles() {
        let noisy = "Türkiye genelinde aldığımız 941 MW depolamalı yenilenebilir enerji projeleriyle gelecek nesillere daha yaşanabilir bir dünya bırakmak için var gücümüzle çalışıyor";
        assert_eq!(contact_title_priority(noisy), 0);
    }

    #[test]
    fn normalize_sales_profile_trims_and_clamps_fields() {
        let profile = SalesProfile {
            product_name: "  Machinity  ".to_string(),
            product_description: "  AI teammate for field teams  ".to_string(),
            target_industry: "  Field Operations ".to_string(),
            target_geo: " tr ".to_string(),
            sender_name: "  Machinity Team ".to_string(),
            sender_email: " founder@machinity.ai ".to_string(),
            sender_linkedin: Some("   ".to_string()),
            target_title_policy: "unexpected".to_string(),
            daily_target: 999,
            daily_send_cap: 0,
            schedule_hour_local: 44,
            timezone_mode: "UTC".to_string(),
        };

        let normalized = normalize_sales_profile(profile).expect("profile normalizes");
        assert_eq!(normalized.product_name, "Machinity");
        assert_eq!(normalized.target_geo, "TR");
        assert_eq!(normalized.sender_email, "founder@machinity.ai");
        assert!(normalized.sender_linkedin.is_none());
        assert_eq!(normalized.target_title_policy, "ceo_then_founder");
        assert_eq!(normalized.daily_target, 200);
        assert_eq!(normalized.daily_send_cap, 1);
        assert_eq!(normalized.schedule_hour_local, 23);
        assert_eq!(normalized.timezone_mode, "utc");
    }

    #[test]
    fn timestamp_matches_sales_day_uses_local_timezone_conversion() {
        let raw = "2026-01-10T23:30:00+00:00";
        let local_day = chrono::DateTime::parse_from_rfc3339(raw)
            .expect("parse")
            .with_timezone(&Local)
            .date_naive();
        assert!(timestamp_matches_sales_day(raw, local_day, "local"));
        assert!(!timestamp_matches_sales_day(
            raw,
            local_day.succ_opt().expect("next day"),
            "local"
        ));
    }

    #[test]
    fn timestamp_matches_sales_day_can_use_utc_mode() {
        let raw = "2026-01-10T23:30:00+00:00";
        let utc_day = chrono::DateTime::parse_from_rfc3339(raw)
            .expect("parse")
            .with_timezone(&Utc)
            .date_naive();
        assert!(timestamp_matches_sales_day(raw, utc_day, "utc"));
        assert!(!timestamp_matches_sales_day(
            raw,
            utc_day.succ_opt().expect("next day"),
            "utc"
        ));
    }

    // =======================================================================
    // SPEC VERIFICATION TESTS — Phase 0 Checklist
    // =======================================================================

    #[test]
    fn spec_p0_consumer_domain_rejected() {
        assert!(!is_valid_company_domain("gmail.com"));
        assert!(!is_valid_company_domain("yahoo.com"));
        assert!(!is_valid_company_domain("hotmail.com"));
        assert!(!is_valid_company_domain("outlook.com"));
        assert!(!is_valid_company_domain("protonmail.com"));
    }

    #[test]
    fn spec_p0_valid_company_domain_accepted() {
        assert!(is_valid_company_domain("machinity.com"));
        assert!(is_valid_company_domain("acme.com.tr"));
        assert!(is_valid_company_domain("example-corp.com"));
    }

    #[test]
    fn spec_p0_gov_edu_mil_domains_rejected() {
        assert!(!is_valid_company_domain("ankara.gov.tr"));
        assert!(!is_valid_company_domain("odtu.edu.tr"));
        assert!(!is_valid_company_domain("tsk.mil.tr"));
    }

    #[test]
    fn spec_p0_turkish_placeholder_detected() {
        assert!(is_placeholder_name("Başkan'ın Mesajı"));
        assert!(is_placeholder_name("baskanin mesaji"));
        assert!(is_placeholder_name("Genel Müdürün Mesajı"));
        assert!(is_placeholder_name("Hakkımızda"));
        assert!(is_placeholder_name("Vizyonumuz"));
        assert!(is_placeholder_name("İletişim"));
        assert!(is_placeholder_name("Kariyer"));
        assert!(is_placeholder_name("Yönetim Kurulu"));
    }

    #[test]
    fn spec_p0_real_names_not_placeholder() {
        assert!(!is_placeholder_name("Ali Vural"));
        assert!(!is_placeholder_name("Mehmet Kaya"));
        assert!(!is_placeholder_name("Ayşe Demir"));
    }

    #[test]
    fn spec_p0_phone_normalization_e164() {
        assert_eq!(
            normalize_phone("0530 851 89 61"),
            Some("+905308518961".to_string())
        );
        assert_eq!(
            normalize_phone("+90 530 851 89 61"),
            Some("+905308518961".to_string())
        );
        assert_eq!(
            normalize_phone("5308518961"),
            Some("+905308518961".to_string())
        );
        assert_eq!(normalize_phone("123"), None); // too short
    }

    #[test]
    fn spec_p0_email_classification() {
        assert_eq!(classify_email("info@acme.com", "acme.com"), "generic");
        assert_eq!(classify_email("ali.vural@acme.com", "acme.com"), "personal");
        assert_eq!(classify_email("user@gmail.com", "acme.com"), "consumer");
        assert_eq!(classify_email("not-an-email", "acme.com"), "invalid");
        // sales@ and hr@ are generic role mailboxes
        assert_eq!(classify_email("sales@acme.com", "acme.com"), "generic");
        assert_eq!(classify_email("hr@acme.com", "acme.com"), "generic");
    }

    #[test]
    fn spec_p0_target_geo_empty_default() {
        let profile = SalesProfile::default();
        assert!(
            profile.target_geo.is_empty(),
            "target_geo should default to empty to force user to set it"
        );
    }

    #[test]
    fn spec_p0_candidate_gateway_rejects_consumer() {
        let mut candidate = DomainCandidate {
            domain: "gmail.com".to_string(),
            ..Default::default()
        };
        assert!(!normalize_candidate_gateway(&mut candidate));
    }

    #[test]
    fn spec_p0_candidate_gateway_accepts_valid() {
        let mut candidate = DomainCandidate {
            domain: "machinity.com".to_string(),
            score: 10,
            ..Default::default()
        };
        assert!(normalize_candidate_gateway(&mut candidate));
    }

    #[test]
    fn spec_p0_candidate_gateway_normalizes_phone() {
        let mut candidate = DomainCandidate {
            domain: "example.com.tr".to_string(),
            phone: Some("0532 123 45 67".to_string()),
            ..Default::default()
        };
        assert!(normalize_candidate_gateway(&mut candidate));
        assert_eq!(candidate.phone.as_deref(), Some("+905321234567"));
    }

    // =======================================================================
    // SPEC VERIFICATION TESTS — Phase 1 Checklist
    // =======================================================================

    #[test]
    fn spec_p1_five_axis_score_struct() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.6,
            reachability_score: 0.7,
            deliverability_risk: 0.1,
            compliance_risk: 0.05,
        };
        assert!(score.fit_score > 0.0 && score.fit_score <= 1.0);
        assert!(score.deliverability_risk >= 0.0 && score.deliverability_risk <= 1.0);
    }

    #[test]
    fn spec_p1_send_gate_block_on_high_deliverability_risk() {
        let score = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.8,
            reachability_score: 0.9,
            deliverability_risk: 0.8,
            compliance_risk: 0.0,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Block { .. }));
    }

    #[test]
    fn spec_p1_send_gate_block_on_high_compliance_risk() {
        let score = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.8,
            reachability_score: 0.9,
            deliverability_risk: 0.1,
            compliance_risk: 0.6,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Block { .. }));
    }

    #[test]
    fn spec_p1_send_gate_research_on_low_reachability() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.5,
            reachability_score: 0.1,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(
            send_gate(&score),
            SendGateDecision::Research { .. }
        ));
    }

    #[test]
    fn spec_p1_send_gate_nurture_on_low_intent() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.1,
            reachability_score: 0.5,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(
            send_gate(&score),
            SendGateDecision::Nurture { .. }
        ));
    }

    #[test]
    fn spec_p1_send_gate_activate_on_good_account() {
        let score = FiveAxisScore {
            fit_score: 0.8,
            intent_score: 0.6,
            reachability_score: 0.7,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert!(matches!(send_gate(&score), SendGateDecision::Activate));
    }

    #[test]
    fn spec_p1_tier_assignment() {
        let high = FiveAxisScore {
            fit_score: 0.9,
            intent_score: 0.7,
            reachability_score: 0.8,
            deliverability_risk: 0.1,
            compliance_risk: 0.1,
        };
        assert_eq!(assign_tier(&high), "a_tier");

        let mid = FiveAxisScore {
            fit_score: 0.6,
            intent_score: 0.3,
            reachability_score: 0.5,
            deliverability_risk: 0.2,
            compliance_risk: 0.1,
        };
        assert_eq!(assign_tier(&mid), "standard");

        let low = FiveAxisScore {
            fit_score: 0.3,
            intent_score: 0.1,
            reachability_score: 0.2,
            deliverability_risk: 0.5,
            compliance_risk: 0.4,
        };
        assert_eq!(assign_tier(&low), "basic");
    }

    #[test]
    fn spec_p1_signal_horizon_classification() {
        let (horizon, expires) = classify_signal_horizon("tender", "ihale");
        assert_eq!(horizon, "immediate");
        assert!(expires.is_some());

        let (horizon, _) = classify_signal_horizon("directory_membership", "member");
        assert_eq!(horizon, "structural");

        let (horizon, _) = classify_signal_horizon("job_posting", "acil pozisyon");
        assert_eq!(horizon, "immediate");

        let (horizon, _) = classify_signal_horizon("job_posting", "saha muduru");
        assert_eq!(horizon, "campaign_window");
    }

    #[test]
    fn spec_p1_source_confidence_hierarchy() {
        assert!(source_confidence("directory_listing") > source_confidence("site_html"));
        assert!(source_confidence("site_html") > source_confidence("web_search"));
        assert!(source_confidence("web_search") > source_confidence("llm_enrichment"));
        assert!(source_confidence("llm_enrichment") > source_confidence("llm_generation"));
    }

    #[test]
    fn spec_p1_reply_classification() {
        assert_eq!(classify_reply_content("toplanti yapalim"), "meeting_booked");
        assert_eq!(classify_reply_content("ilginc gorunuyor"), "interested");
        assert_eq!(classify_reply_content("simdi degil"), "not_now");
        assert_eq!(classify_reply_content("yanlis kisi"), "wrong_person");
        assert_eq!(classify_reply_content("beni listeden cikar"), "unsubscribe");
    }

    // =======================================================================
    // NEW FEATURE TESTS
    // =======================================================================

    #[test]
    fn spec_email_pattern_guesser_produces_multiple_patterns() {
        let patterns = guess_personal_email_patterns(Some("Ali Vural"), "acme.com.tr");
        assert!(patterns.len() >= 3, "Should produce at least 3 patterns");
        assert!(patterns.contains(&"ali.vural@acme.com.tr".to_string()));
        assert!(patterns.contains(&"avural@acme.com.tr".to_string()));
        assert!(patterns.contains(&"ali@acme.com.tr".to_string()));
    }

    #[test]
    fn spec_email_pattern_guesser_handles_turkish_chars() {
        let patterns = guess_personal_email_patterns(Some("Şükrü Öztürk"), "firma.com.tr");
        assert!(!patterns.is_empty());
        // Turkish chars should be transliterated
        assert!(patterns[0].contains("sukru") || patterns[0].contains("ozturk"));
    }

    #[test]
    fn spec_email_pattern_guesser_rejects_placeholder() {
        let patterns = guess_personal_email_patterns(Some("Leadership Team"), "acme.com");
        assert!(patterns.is_empty());
    }

    #[test]
    fn spec_tech_stack_detection_finds_sap() {
        let html = r#"<html><body><script src="https://sap.com/sap-ui.js"></script></body></html>"#;
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"SAP".to_string()));
    }

    #[test]
    fn spec_tech_stack_detection_finds_hubspot() {
        let html =
            r#"<html><body><script src="//js.hs-scripts.com/1234.js"></script></body></html>"#;
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"HubSpot".to_string()));
    }

    #[test]
    fn spec_tech_stack_detection_empty_on_clean_html() {
        let html = "<html><body><p>Hello world</p></body></html>";
        let headers = HashMap::new();
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.is_empty());
    }

    #[test]
    fn spec_tech_stack_detection_uses_headers() {
        let html = "<html><body></body></html>";
        let mut headers = HashMap::new();
        headers.insert("x-powered-by".to_string(), "Express".to_string());
        let stack = detect_tech_stack(html, &headers);
        assert!(stack.contains(&"Express".to_string()));
    }

    #[test]
    fn spec_job_posting_signal_extraction() {
        let results = vec![
            SearchEntry {
                title: "Acme Corp - Saha Operasyon Yoneticisi".to_string(),
                url: "https://kariyer.net/is-ilani/acme".to_string(),
                snippet: String::new(),
            },
            SearchEntry {
                title: "Random unrelated post".to_string(),
                url: "https://example.com".to_string(),
                snippet: String::new(),
            },
        ];
        let signals = extract_job_posting_signals(&results, "Acme Corp");
        assert_eq!(signals.len(), 1);
        assert!(signals[0].0.contains("Saha"));
        assert!(signals[0].2 > 0.5); // confidence
    }

    #[test]
    fn spec_message_strategy_turkish_for_tr_geo() {
        let profile = SalesProfile {
            target_geo: "TR".to_string(),
            product_name: "TestProd".to_string(),
            ..SalesProfile::default()
        };
        let strategy = generate_message_strategy(&profile, "Acme", Some("Ali"), "signal", "ops");
        assert_eq!(strategy.language, "tr");
    }

    #[test]
    fn spec_message_copy_refuses_without_evidence() {
        let strategy = MessageStrategy {
            pain_angle: String::new(),
            trigger_evidence: String::new(),
            ..Default::default()
        };
        let profile = SalesProfile::default();
        let result = generate_message_copy(&strategy, &profile, "Acme", Some("Ali"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("REFUSED"));
    }

    #[test]
    fn spec_message_copy_succeeds_with_evidence() {
        let strategy = MessageStrategy {
            pain_angle: "field ops coordination".to_string(),
            trigger_evidence: "directory membership signal".to_string(),
            language: "en".to_string(),
            cta_type: "soft".to_string(),
            tone: "warm".to_string(),
        };
        let profile = SalesProfile {
            product_name: "TestProd".to_string(),
            sender_name: "Sender".to_string(),
            ..SalesProfile::default()
        };
        let result = generate_message_copy(&strategy, &profile, "Acme", Some("Ali"));
        assert!(result.is_ok());
        let copy = result.unwrap();
        assert!(!copy.subject.is_empty());
        assert!(!copy.body.is_empty());
    }

    #[test]
    fn spec_seniority_from_title_detects_clevel() {
        assert_eq!(seniority_from_title(Some("CEO")), "c_level");
        assert_eq!(
            seniority_from_title(Some("Chief Operating Officer")),
            "c_level"
        );
        assert_eq!(seniority_from_title(Some("Genel Müdür")), "c_level");
        assert_eq!(seniority_from_title(Some("Founder & CEO")), "c_level");
    }

    #[test]
    fn spec_seniority_from_title_detects_levels() {
        assert_eq!(seniority_from_title(Some("VP Engineering")), "vp");
        assert_eq!(seniority_from_title(Some("Director of Ops")), "director");
        assert_eq!(seniority_from_title(Some("Operations Manager")), "manager");
        assert_eq!(seniority_from_title(Some("Intern")), "unknown");
    }

    #[test]
    fn spec_transliterate_turkish_ascii() {
        assert_eq!(transliterate_turkish_ascii("Şükrü Öztürk"), "sukru ozturk");
        assert_eq!(transliterate_turkish_ascii("İstanbul"), "istanbul");
        assert_eq!(transliterate_turkish_ascii("Çağrı"), "cagri");
    }

    #[test]
    fn spec_email_syntax_valid() {
        assert!(email_syntax_valid("ali@example.com"));
        assert!(email_syntax_valid("a.b@c.d"));
        assert!(!email_syntax_valid("notanemail"));
        assert!(!email_syntax_valid("@domain.com"));
        assert!(!email_syntax_valid("user@"));
        assert!(!email_syntax_valid("user@.com"));
    }

    #[test]
    fn spec_sequence_advancement_completes_on_positive_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(temp.path());
        engine.init().unwrap();
        let conn = engine.open().unwrap();
        let now = Utc::now().to_rfc3339();
        engine.ensure_default_sequence_template(&conn).unwrap();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, created_at, updated_at) VALUES ('acc1', 'Test Co', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO contacts (id, account_id, full_name, created_at) VALUES ('c1', 'acc1', 'Ali', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO sequence_instances (id, template_id, account_id, contact_id, current_step, status, started_at, updated_at)
             VALUES ('seq1', 'default_outreach_sequence', 'acc1', 'c1', 1, 'active', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO touches (id, sequence_instance_id, step, channel, message_payload, created_at)
             VALUES ('t1', 'seq1', 1, 'email', '{}', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, classified_at) VALUES ('o1', 't1', 'meeting_booked', ?1)",
            params![now],
        ).unwrap();

        let advanced = engine.advance_sequences().unwrap();
        assert!(advanced >= 1);

        let status: String = conn
            .query_row(
                "SELECT status FROM sequence_instances WHERE id = 'seq1'",
                [],
                |r: &rusqlite::Row| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn spec_sequence_advancement_cancels_on_bounce() {
        let temp = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(temp.path());
        engine.init().unwrap();
        let conn = engine.open().unwrap();
        let now = Utc::now().to_rfc3339();
        engine.ensure_default_sequence_template(&conn).unwrap();

        conn.execute(
            "INSERT INTO accounts (id, canonical_name, created_at, updated_at) VALUES ('acc2', 'Bounce Co', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO contacts (id, account_id, full_name, created_at) VALUES ('c2', 'acc2', 'Test', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO sequence_instances (id, template_id, account_id, contact_id, current_step, status, started_at, updated_at)
             VALUES ('seq2', 'default_outreach_sequence', 'acc2', 'c2', 1, 'active', ?1, ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO touches (id, sequence_instance_id, step, channel, message_payload, created_at)
             VALUES ('t2', 'seq2', 1, 'email', '{}', ?1)",
            params![now],
        ).unwrap();
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, classified_at) VALUES ('o2', 't2', 'hard_bounce', ?1)",
            params![now],
        ).unwrap();

        engine.advance_sequences().unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM sequence_instances WHERE id = 'seq2'",
                [],
                |r: &rusqlite::Row| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "cancelled");
    }

    #[test]
    fn spec_mailbox_pool_selects_lowest_sends() {
        let mut cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    email: "a@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 8,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
                MailboxConfig {
                    email: "b@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 3,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
            ],
        };
        let picked = cfg.select_mailbox().unwrap();
        assert_eq!(picked.email, "b@send.example.com");
    }

    #[test]
    fn spec_mailbox_pool_skips_cold() {
        let mut cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    email: "cold@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "cold".into(),
                    sends_today: 0,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
                MailboxConfig {
                    email: "warm@send.example.com".into(),
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 5,
                    counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                    ..Default::default()
                },
            ],
        };
        let picked = cfg.select_mailbox().unwrap();
        assert_eq!(picked.email, "warm@send.example.com");
    }

    #[test]
    fn spec_mailbox_pool_exhausted_returns_none() {
        let mut cfg = SenderConfig {
            mailboxes: vec![MailboxConfig {
                email: "full@send.example.com".into(),
                daily_cap: 5,
                warm_state: "warm".into(),
                sends_today: 5,
                counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                ..Default::default()
            }],
        };
        assert!(cfg.select_mailbox().is_none());
    }

    #[test]
    fn spec_mailbox_warming_cap_limited() {
        let mb = MailboxConfig {
            daily_cap: 50,
            warm_state: "warming".into(),
            ..Default::default()
        };
        assert_eq!(mb.effective_cap(), 15); // warming caps at 15
    }

    #[test]
    fn spec_sender_remaining_capacity() {
        let cfg = SenderConfig {
            mailboxes: vec![
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 3,
                    ..Default::default()
                },
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "warm".into(),
                    sends_today: 7,
                    ..Default::default()
                },
                MailboxConfig {
                    daily_cap: 10,
                    warm_state: "cold".into(),
                    sends_today: 0,
                    ..Default::default()
                },
            ],
        };
        assert_eq!(cfg.remaining_capacity(), 10); // 7 + 3 from warm mailboxes
    }

    #[test]
    fn spec_mailbox_pool_parses_legacy_string_entries() {
        let pool =
            mailbox_pool_from_json(r#"["legacy@send.example.com", "SECOND@send.example.com "]"#);
        assert_eq!(pool.len(), 2);
        assert_eq!(pool[0].email, "legacy@send.example.com");
        assert_eq!(pool[1].email, "second@send.example.com");
        assert_eq!(pool[0].warm_state, "warming");
        assert_eq!(pool[0].daily_cap, 20);
    }

    #[test]
    fn spec_record_mailbox_send_persists_daily_counter() {
        let dir = tempfile::tempdir().unwrap();
        let engine = SalesEngine::new(dir.path());
        engine.init().unwrap();
        let sender_cfg = SenderConfig {
            mailboxes: vec![MailboxConfig {
                email: "warm@send.example.com".into(),
                daily_cap: 10,
                warm_state: "warm".into(),
                counter_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
                ..Default::default()
            }],
        };
        engine.save_sender_config(&sender_cfg).unwrap();

        engine.record_mailbox_send("warm@send.example.com").unwrap();
        let reloaded = engine.load_sender_config();

        assert_eq!(reloaded.mailboxes.len(), 1);
        assert_eq!(reloaded.mailboxes[0].sends_today, 1);
        assert_eq!(reloaded.mailboxes[0].email, "warm@send.example.com");
    }

    #[test]
    fn spec_context_factors_seeded_on_init() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS contextual_factors (
                id TEXT PRIMARY KEY,
                factor_type TEXT NOT NULL,
                factor_key TEXT NOT NULL,
                factor_value TEXT,
                effective_from TEXT,
                effective_until TEXT,
                source TEXT
            );",
        )
        .unwrap();
        seed_contextual_factors(&conn);
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM contextual_factors", [], |r| r.get(0))
            .unwrap();
        assert!(
            count >= 10,
            "Expected at least 10 contextual factors, got {count}"
        );
        // Check specific factors exist
        let ramazan: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM contextual_factors WHERE factor_key = 'ramazan_bayrami'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ramazan, 1);
        let kvkk: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM contextual_factors WHERE factor_key = 'kvkk'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kvkk, 1);
        // Idempotent — re-seeding should not duplicate
        seed_contextual_factors(&conn);
        let count2: i32 = conn
            .query_row("SELECT COUNT(*) FROM contextual_factors", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, count2);
    }

    #[test]
    fn spec_experiment_create_and_balanced_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, hypothesis TEXT,
                variant_a TEXT, variant_b TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS experiment_assignments (
                id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL,
                sequence_instance_id TEXT, variant TEXT NOT NULL
            );",
        )
        .unwrap();

        let exp_id = create_experiment(
            &conn,
            "subject_line_test",
            "Shorter subjects get more opens",
            "short",
            "long",
        )
        .unwrap();
        assert!(!exp_id.is_empty());

        // Assign multiple sequences — should balance a/b
        let v1 = assign_experiment_variant(&conn, &exp_id, "seq_001").unwrap();
        let v2 = assign_experiment_variant(&conn, &exp_id, "seq_002").unwrap();
        assert_eq!(v1, "a");
        assert_eq!(v2, "b");
        let v3 = assign_experiment_variant(&conn, &exp_id, "seq_003").unwrap();
        assert_eq!(v3, "a");
    }

    #[test]
    fn spec_calibration_creates_proposals_from_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sales.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outcomes (
                id TEXT PRIMARY KEY, touch_id TEXT NOT NULL,
                outcome_type TEXT NOT NULL,
                classified_at TEXT NOT NULL DEFAULT (datetime('now')),
                classifier_confidence REAL DEFAULT 1.0
            );
            CREATE TABLE IF NOT EXISTS outcome_attribution_snapshots (
                id TEXT PRIMARY KEY, touch_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                snapshot_at TEXT NOT NULL DEFAULT (datetime('now')),
                score_at_touch_json TEXT, active_signal_ids TEXT,
                unused_signal_ids TEXT, thesis_id TEXT,
                sequence_variant TEXT, message_variant TEXT,
                channel TEXT, mailbox_id TEXT, contextual_factors_json TEXT
            );
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                signal_type TEXT NOT NULL, text TEXT NOT NULL,
                source TEXT, observed_at TEXT, confidence REAL DEFAULT 0.5,
                effect_horizon TEXT, expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS retrieval_rule_versions (
                id TEXT PRIMARY KEY, rule_type TEXT NOT NULL,
                rule_key TEXT NOT NULL, old_value TEXT,
                new_value TEXT NOT NULL, proposal_source TEXT,
                backtest_result_json TEXT, holdout_result_json TEXT,
                status TEXT NOT NULL DEFAULT 'proposed',
                approved_by TEXT, activated_at TEXT,
                version INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .unwrap();

        // Not enough data → no proposals
        let proposals = calibrate_scoring_from_outcomes(&conn).unwrap();
        assert!(proposals.is_empty());

        // Insert enough outcomes with signals
        for i in 0..12 {
            let touch_id = format!("touch_{i}");
            let account_id = format!("acc_{i}");
            conn.execute(
                "INSERT INTO outcome_attribution_snapshots (id, touch_id, account_id, snapshot_at) VALUES (?1, ?2, ?3, datetime('now'))",
                params![format!("snap_{i}"), touch_id, account_id],
            ).unwrap();
            conn.execute(
                "INSERT INTO outcomes (id, touch_id, outcome_type) VALUES (?1, ?2, ?3)",
                params![
                    format!("out_{i}"),
                    touch_id,
                    if i < 8 {
                        "hard_bounce"
                    } else {
                        "meeting_booked"
                    }
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO signals (id, account_id, signal_type, text) VALUES (?1, ?2, 'directory_membership', ?3)",
                params![format!("sig_{i}"), account_id, format!("Signal {i}")],
            ).unwrap();
        }

        let proposals = calibrate_scoring_from_outcomes(&conn).unwrap();
        assert!(
            !proposals.is_empty(),
            "Should have created calibration proposals"
        );
    }

    #[test]
    fn spec_verify_domain_exists_basic() {
        // This is an async function — just verify it compiles and the signature is correct
        // Actual HTTP verification would require network access
        let _fn_exists: fn(
            &str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = bool> + Send + '_>,
        > = |domain| Box::pin(verify_domain_exists(domain));
    }
}
