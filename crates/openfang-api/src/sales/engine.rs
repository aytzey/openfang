impl SalesEngine {
    pub fn new(home_dir: &FsPath) -> Self {
        Self {
            db_path: home_dir.join("sales.db"),
        }
    }

    fn open(&self) -> Result<Connection, String> {
        Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to open sales db {}: {e}", self.db_path.display()))
    }

    pub fn init(&self) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sales_profile (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_profiles_v2 (
                segment TEXT PRIMARY KEY,
                json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_runs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                discovered INTEGER NOT NULL DEFAULT 0,
                inserted INTEGER NOT NULL DEFAULT 0,
                approvals_queued INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS leads (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                company TEXT NOT NULL,
                website TEXT NOT NULL,
                company_domain TEXT NOT NULL,
                contact_name TEXT NOT NULL,
                contact_title TEXT NOT NULL,
                linkedin_url TEXT,
                email TEXT,
                phone TEXT,
                reasons_json TEXT NOT NULL,
                email_subject TEXT NOT NULL,
                email_body TEXT NOT NULL,
                linkedin_message TEXT NOT NULL,
                score INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(company_domain, contact_name, contact_title)
            );

            CREATE TABLE IF NOT EXISTS approvals (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                decided_at TEXT
            );

            CREATE TABLE IF NOT EXISTS deliveries (
                id TEXT PRIMARY KEY,
                approval_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                recipient TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                sent_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_onboarding (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                brief_text TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales_onboarding_v2 (
                segment TEXT PRIMARY KEY,
                brief_text TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS discovered_domains (
                domain TEXT PRIMARY KEY,
                first_seen_run_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prospect_profiles (
                company_domain TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS artifacts (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT,
                raw_content TEXT,
                parse_status TEXT NOT NULL DEFAULT 'ok',
                parser_health REAL DEFAULT 1.0,
                freshness TEXT,
                legal_mode TEXT DEFAULT 'public',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS evidence (
                id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL REFERENCES artifacts(id),
                field_type TEXT NOT NULL,
                field_value TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.5,
                extraction_method TEXT,
                verified_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                display_name TEXT,
                legal_name TEXT,
                sector TEXT,
                geo TEXT,
                employee_estimate INTEGER,
                website TEXT,
                tier TEXT DEFAULT 'standard',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS account_aliases (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                alias_name TEXT NOT NULL,
                alias_type TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS domains (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                domain TEXT NOT NULL UNIQUE,
                is_primary INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                mx_valid INTEGER,
                checked_at TEXT
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                full_name TEXT,
                title TEXT,
                seniority TEXT,
                department TEXT,
                name_confidence REAL DEFAULT 0.5,
                title_confidence REAL DEFAULT 0.5,
                is_decision_maker INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS contact_methods (
                id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                channel_type TEXT NOT NULL,
                value TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                verified_at TEXT,
                classification TEXT,
                suppressed INTEGER DEFAULT 0,
                UNIQUE(contact_id, channel_type, value)
            );

            CREATE TABLE IF NOT EXISTS buyer_roles (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                contact_id TEXT REFERENCES contacts(id),
                role_type TEXT NOT NULL,
                inferred_from TEXT
            );

            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                signal_type TEXT NOT NULL,
                text TEXT NOT NULL,
                source TEXT,
                observed_at TEXT,
                confidence REAL DEFAULT 0.5,
                effect_horizon TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS signal_rationales (
                id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL REFERENCES signals(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                why_it_matters TEXT NOT NULL,
                expected_effect TEXT,
                evidence_ids TEXT,
                confidence REAL DEFAULT 0.5,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                validated_at TEXT,
                validation_result TEXT
            );

            CREATE TABLE IF NOT EXISTS score_snapshots (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                fit_score REAL NOT NULL DEFAULT 0.0,
                intent_score REAL NOT NULL DEFAULT 0.0,
                reachability_score REAL NOT NULL DEFAULT 0.0,
                deliverability_risk REAL NOT NULL DEFAULT 0.0,
                compliance_risk REAL NOT NULL DEFAULT 0.0,
                activation_priority REAL,
                computed_at TEXT NOT NULL DEFAULT (datetime('now')),
                scoring_version TEXT NOT NULL DEFAULT 'v1'
            );

            CREATE TABLE IF NOT EXISTS research_queue (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                priority INTEGER DEFAULT 0,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                assigned_at TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS account_theses (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                why_this_account TEXT NOT NULL,
                why_now TEXT,
                buyer_committee_json TEXT,
                evidence_ids TEXT,
                do_not_say TEXT,
                recommended_channel TEXT,
                recommended_pain TEXT,
                thesis_confidence REAL DEFAULT 0.0,
                thesis_status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS activation_queue (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                contact_id TEXT REFERENCES contacts(id),
                thesis_id TEXT REFERENCES account_theses(id),
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sequence_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                steps_json TEXT NOT NULL,
                icp_id TEXT,
                persona_id TEXT,
                version INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sequence_instances (
                id TEXT PRIMARY KEY,
                template_id TEXT NOT NULL REFERENCES sequence_templates(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                contact_id TEXT NOT NULL REFERENCES contacts(id),
                thesis_id TEXT REFERENCES account_theses(id),
                current_step INTEGER DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS touches (
                id TEXT PRIMARY KEY,
                sequence_instance_id TEXT REFERENCES sequence_instances(id),
                step INTEGER NOT NULL,
                channel TEXT NOT NULL,
                message_payload TEXT NOT NULL,
                claims_json TEXT,
                evidence_ids TEXT,
                variant_id TEXT,
                risk_flags TEXT,
                sent_at TEXT,
                mailbox_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS outcomes (
                id TEXT PRIMARY KEY,
                touch_id TEXT NOT NULL REFERENCES touches(id),
                outcome_type TEXT NOT NULL,
                raw_text TEXT,
                classified_at TEXT NOT NULL DEFAULT (datetime('now')),
                classifier_confidence REAL DEFAULT 1.0
            );

            CREATE TABLE IF NOT EXISTS outcome_attribution_snapshots (
                id TEXT PRIMARY KEY,
                touch_id TEXT NOT NULL REFERENCES touches(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                snapshot_at TEXT NOT NULL DEFAULT (datetime('now')),
                score_at_touch_json TEXT,
                active_signal_ids TEXT,
                unused_signal_ids TEXT,
                thesis_id TEXT,
                sequence_variant TEXT,
                message_variant TEXT,
                channel TEXT,
                mailbox_id TEXT,
                contextual_factors_json TEXT
            );

            CREATE TABLE IF NOT EXISTS missed_signal_reviews (
                id TEXT PRIMARY KEY,
                outcome_id TEXT NOT NULL REFERENCES outcomes(id),
                snapshot_id TEXT NOT NULL REFERENCES outcome_attribution_snapshots(id),
                reviewed_at TEXT NOT NULL DEFAULT (datetime('now')),
                validated_signals TEXT,
                false_positive_signals TEXT,
                missed_signals TEXT,
                timing_mistakes TEXT,
                persona_mismatch TEXT,
                channel_mismatch TEXT,
                reviewer_type TEXT DEFAULT 'auto'
            );

            CREATE TABLE IF NOT EXISTS retrieval_rule_versions (
                id TEXT PRIMARY KEY,
                rule_type TEXT NOT NULL,
                rule_key TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT NOT NULL,
                proposal_source TEXT,
                backtest_result_json TEXT,
                holdout_result_json TEXT,
                status TEXT NOT NULL DEFAULT 'proposed',
                approved_by TEXT,
                activated_at TEXT,
                version INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS contextual_factors (
                id TEXT PRIMARY KEY,
                factor_type TEXT NOT NULL,
                factor_key TEXT NOT NULL,
                factor_value TEXT,
                effective_from TEXT,
                effective_until TEXT,
                source TEXT
            );

            CREATE TABLE IF NOT EXISTS exploration_log (
                id TEXT PRIMARY KEY,
                touch_id TEXT REFERENCES touches(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                exploration_reason TEXT NOT NULL,
                exploration_type TEXT,
                outcome_id TEXT,
                learned_pattern TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS suppressions (
                id TEXT PRIMARY KEY,
                contact_method_value TEXT NOT NULL,
                reason TEXT NOT NULL,
                source_outcome_id TEXT,
                suppressed_at TEXT NOT NULL DEFAULT (datetime('now')),
                permanent INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                hypothesis TEXT,
                variant_a TEXT,
                variant_b TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS experiment_assignments (
                id TEXT PRIMARY KEY,
                experiment_id TEXT NOT NULL REFERENCES experiments(id),
                sequence_instance_id TEXT,
                variant TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_health (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL UNIQUE,
                precision REAL,
                freshness TEXT,
                parser_health REAL DEFAULT 1.0,
                legal_mode TEXT DEFAULT 'public',
                historical_reply_yield REAL,
                last_checked_at TEXT,
                auto_skip INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS job_runs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                completed_at TEXT,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS job_stages (
                id TEXT PRIMARY KEY,
                job_run_id TEXT NOT NULL REFERENCES job_runs(id),
                stage_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                checkpoint_data TEXT,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS icp_definitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sector_rules TEXT,
                geo_rules TEXT,
                size_rules TEXT,
                negative_rules TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS segments (
                id TEXT PRIMARY KEY,
                icp_id TEXT NOT NULL REFERENCES icp_definitions(id),
                name TEXT NOT NULL,
                criteria_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS personas (
                id TEXT PRIMARY KEY,
                segment_id TEXT NOT NULL REFERENCES segments(id),
                role_type TEXT NOT NULL,
                pain_angles TEXT,
                message_strategy TEXT
            );

            CREATE TABLE IF NOT EXISTS sender_policies (
                id TEXT PRIMARY KEY,
                icp_id TEXT REFERENCES icp_definitions(id),
                mailbox_pool TEXT,
                daily_cap INTEGER DEFAULT 20,
                subdomain TEXT,
                warm_state TEXT DEFAULT 'cold'
            );

            CREATE INDEX IF NOT EXISTS idx_approvals_status_created ON approvals(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_deliveries_sent ON deliveries(sent_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_run_updated ON prospect_profiles(run_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prospect_profiles_updated ON prospect_profiles(updated_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_approvals_pending_recipient
                ON approvals(channel, json_extract(payload_json, '$.to'))
                WHERE status = 'pending';
            CREATE UNIQUE INDEX IF NOT EXISTS idx_suppressions_value
                ON suppressions(contact_method_value);
        "#,
        )
        .map_err(|e| format!("Failed to initialize sales db: {e}"))?;
        ensure_sqlite_column(&conn, "sales_runs", "segment", "TEXT NOT NULL DEFAULT 'b2b'")?;
        ensure_sqlite_column(
            &conn,
            "prospect_profiles",
            "segment",
            "TEXT NOT NULL DEFAULT 'b2b'",
        )?;
        ensure_sqlite_column(&conn, "job_runs", "segment", "TEXT NOT NULL DEFAULT 'b2b'")?;
        self.migrate_legacy_to_canonical_core()?;
        seed_contextual_factors(&conn);
        Ok(())
    }

    pub fn get_profile(&self, segment: SalesSegment) -> Result<Option<SalesProfile>, String> {
        let conn = self.open()?;
        let row = conn
            .query_row(
                "SELECT json FROM sales_profiles_v2 WHERE segment = ?1",
                params![segment.as_str()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Profile query failed: {e}"))?;
        let legacy = if row.is_none() && segment == SalesSegment::B2B {
            conn.query_row("SELECT json FROM sales_profile WHERE id = 1", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(|e| format!("Legacy profile query failed: {e}"))?
        } else {
            None
        };

        row.or(legacy).map(|json| {
            serde_json::from_str::<SalesProfile>(&json)
                .map_err(|e| format!("Invalid profile JSON in DB: {e}"))
        })
        .transpose()
    }

    pub fn upsert_profile(
        &self,
        segment: SalesSegment,
        profile: &SalesProfile,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let normalized = normalize_sales_profile(profile.clone())?;
        let json =
            serde_json::to_string(&normalized).map_err(|e| format!("Serialize failed: {e}"))?;
        conn.execute(
            "INSERT INTO sales_profiles_v2 (segment, json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(segment) DO UPDATE SET
                json = excluded.json,
                updated_at = excluded.updated_at",
            params![segment.as_str(), json, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to save profile: {e}"))?;
        Ok(())
    }

    pub fn set_onboarding_brief(&self, segment: SalesSegment, brief: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "INSERT INTO sales_onboarding_v2 (segment, brief_text, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(segment) DO UPDATE SET
                brief_text = excluded.brief_text,
                updated_at = excluded.updated_at",
            params![segment.as_str(), brief, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to save onboarding brief: {e}"))?;
        Ok(())
    }

    fn get_onboarding_brief_state(
        &self,
        segment: SalesSegment,
    ) -> Result<OnboardingBriefState, String> {
        let conn = self.open()?;
        let row = conn
            .query_row(
                "SELECT brief_text, updated_at FROM sales_onboarding_v2 WHERE segment = ?1",
                params![segment.as_str()],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Onboarding brief query failed: {e}"))?;
        let legacy = if row.is_none() && segment == SalesSegment::B2B {
            conn.query_row(
                "SELECT brief_text, updated_at FROM sales_onboarding WHERE id = 1",
                [],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Legacy onboarding brief query failed: {e}"))?
        } else {
            None
        };
        let Some((brief, updated_at)) = row.or(legacy) else {
            return Ok(OnboardingBriefState::default());
        };
        let brief = brief.and_then(|v| {
            let t = v.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        });
        let updated_at = updated_at.and_then(|v| {
            let t = v.trim().to_string();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        });
        Ok(OnboardingBriefState { brief, updated_at })
    }

    pub fn get_onboarding_brief(
        &self,
        segment: SalesSegment,
    ) -> Result<Option<String>, String> {
        self.get_onboarding_brief_state(segment).map(|s| s.brief)
    }

    pub fn latest_successful_run_id_since(
        &self,
        segment: SalesSegment,
        since: Option<&str>,
    ) -> Result<Option<String>, String> {
        let conn = self.open()?;
        let (sql, with_since) = if since.is_some() {
            (
                "SELECT sr.id
                 FROM sales_runs sr
                 WHERE sr.segment = ?1
                   AND sr.status = 'completed'
                   AND sr.started_at >= ?2
                   AND EXISTS (
                     SELECT 1 FROM prospect_profiles pp
                     WHERE pp.run_id = sr.id AND pp.segment = ?1
                   )
                 ORDER BY sr.completed_at DESC
                 LIMIT 1",
                true,
            )
        } else {
            (
                "SELECT sr.id
                 FROM sales_runs sr
                 WHERE sr.segment = ?1
                   AND sr.status = 'completed'
                   AND EXISTS (
                     SELECT 1 FROM prospect_profiles pp
                     WHERE pp.run_id = sr.id AND pp.segment = ?1
                   )
                 ORDER BY sr.completed_at DESC
                 LIMIT 1",
                false,
            )
        };
        if with_since {
            conn.query_row(sql, params![segment.as_str(), since.unwrap_or_default()], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(|e| format!("Latest successful run query failed: {e}"))
        } else {
            conn.query_row(sql, params![segment.as_str()], |r| r.get::<_, String>(0))
                .optional()
                .map_err(|e| format!("Latest successful run query failed: {e}"))
        }
    }

    pub fn latest_successful_run_id(
        &self,
        segment: SalesSegment,
    ) -> Result<Option<String>, String> {
        self.latest_successful_run_id_since(segment, None)
    }

    fn latest_running_run_row(
        &self,
        segment: SalesSegment,
    ) -> Result<Option<(String, String)>, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT id, started_at
             FROM sales_runs
             WHERE segment = ?1 AND status = 'running'
             ORDER BY started_at DESC
             LIMIT 1",
            params![segment.as_str()],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|e| format!("Latest running run query failed: {e}"))
    }

    fn count_leads_for_run(&self, run_id: &str) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*) FROM leads WHERE run_id = ?",
            params![run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Lead count query failed: {e}"))
    }

    fn count_prospect_profiles_for_run(
        &self,
        segment: SalesSegment,
        run_id: &str,
    ) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*) FROM prospect_profiles WHERE segment = ?1 AND run_id = ?2",
            params![segment.as_str(), run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Prospect profile count query failed: {e}"))
    }

    fn count_approvals_for_run(&self, run_id: &str) -> Result<u32, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT COUNT(*)
             FROM approvals a
             INNER JOIN leads l ON l.id = a.lead_id
             WHERE l.run_id = ?",
            params![run_id],
            |r| r.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u32)
        .map_err(|e| format!("Approval count query failed: {e}"))
    }

    pub fn recover_latest_timed_out_run(
        &self,
        segment: SalesSegment,
    ) -> Result<Option<SalesRunRecord>, String> {
        let Some((run_id, started_at)) = self.latest_running_run_row(segment)? else {
            return Ok(None);
        };

        let inserted = self.count_leads_for_run(&run_id)?;
        let profiled = self.count_prospect_profiles_for_run(segment, &run_id)?;
        let approvals_queued = self.count_approvals_for_run(&run_id)?;
        let discovered = inserted.max(profiled);

        let (status, error_note) = if inserted > 0 || profiled > 0 {
            (
                "completed",
                Some(format!(
                    "Prospecting run exceeded the request timeout after saving progress (profiles: {profiled}, leads: {inserted}, approvals: {approvals_queued})."
                )),
            )
        } else {
            (
                "failed",
                Some(
                    "Prospecting run exceeded the request timeout before any durable prospect dossiers or actionable leads were saved."
                        .to_string(),
                ),
            )
        };

        self.finish_run(&run_id, status, discovered, inserted, approvals_queued, error_note.as_deref())?;

        Ok(Some(SalesRunRecord {
            id: run_id,
            status: status.to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered,
            inserted,
            approvals_queued,
            error: error_note,
        }))
    }

    pub fn recover_latest_timed_out_run_if_stale(
        &self,
        segment: SalesSegment,
        min_age_secs: i64,
    ) -> Result<Option<SalesRunRecord>, String> {
        let Some((_run_id, started_at)) = self.latest_running_run_row(segment)? else {
            return Ok(None);
        };
        let started_at = chrono::DateTime::parse_from_rfc3339(&started_at)
            .map_err(|e| format!("Failed to parse running run timestamp: {e}"))?
            .with_timezone(&Utc);
        if (Utc::now() - started_at).num_seconds() < min_age_secs {
            return Ok(None);
        }
        self.recover_latest_timed_out_run(segment)
    }

    fn begin_run(&self, segment: SalesSegment) -> Result<String, String> {
        let conn = self.open()?;
        let run_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO sales_runs (id, segment, status, started_at) VALUES (?1, ?2, 'running', ?3)",
            params![run_id, segment.as_str(), Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to create run row: {e}"))?;
        Ok(run_id)
    }

    fn finish_run(
        &self,
        run_id: &str,
        status: &str,
        discovered: u32,
        inserted: u32,
        approvals_queued: u32,
        error_msg: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE sales_runs SET status = ?, completed_at = ?, discovered = ?, inserted = ?, approvals_queued = ?, error = ? WHERE id = ?",
            params![
                status,
                Utc::now().to_rfc3339(),
                discovered,
                inserted,
                approvals_queued,
                error_msg,
                run_id
            ],
        )
        .map_err(|e| format!("Failed to update run row: {e}"))?;
        Ok(())
    }

    fn create_job_run(&self, job_type: &str, segment: SalesSegment) -> Result<String, String> {
        let conn = self.open()?;
        let job_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO job_runs (id, job_type, segment, status, started_at)
             VALUES (?1, ?2, ?3, 'running', ?4)",
            params![job_id, job_type, segment.as_str(), Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to create job run: {e}"))?;
        self.ensure_job_stage_rows(&conn, &job_id)?;
        Ok(job_id)
    }

    fn ensure_job_stage_rows(&self, conn: &Connection, job_id: &str) -> Result<(), String> {
        for stage in PipelineStage::ordered() {
            conn.execute(
                "INSERT OR IGNORE INTO job_stages (id, job_run_id, stage_name, status, updated_at)
                 VALUES (?1, ?2, ?3, 'pending', ?4)",
                params![
                    format!("{job_id}:{}", stage.as_str()),
                    job_id,
                    stage.as_str(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to create job stage rows: {e}"))?;
        }
        Ok(())
    }

    fn set_job_stage_running(&self, job_id: &str, stage: PipelineStage) -> Result<(), String> {
        let conn = self.open()?;
        self.ensure_job_stage_rows(&conn, job_id)?;
        conn.execute(
            "UPDATE job_stages
             SET status = 'running',
                 started_at = COALESCE(started_at, ?3),
                 updated_at = ?3
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![job_id, stage.as_str(), Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to update job stage to running: {e}"))?;
        Ok(())
    }

    fn update_job_stage_checkpoint<T: Serialize>(
        &self,
        job_id: &str,
        stage: PipelineStage,
        checkpoint: &T,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let checkpoint_data = serde_json::to_string(checkpoint)
            .map_err(|e| format!("Failed to serialize job checkpoint: {e}"))?;
        conn.execute(
            "UPDATE job_stages
             SET checkpoint_data = ?3,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![
                job_id,
                stage.as_str(),
                checkpoint_data,
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(|e| format!("Failed to update job checkpoint: {e}"))?;
        Ok(())
    }

    fn complete_job_stage<T: Serialize>(
        &self,
        job_id: &str,
        stage: PipelineStage,
        checkpoint: &T,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let checkpoint_data = serde_json::to_string(checkpoint)
            .map_err(|e| format!("Failed to serialize job checkpoint: {e}"))?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE job_stages
             SET status = 'completed',
                 checkpoint_data = ?3,
                 completed_at = ?4,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![job_id, stage.as_str(), checkpoint_data, now],
        )
        .map_err(|e| format!("Failed to complete job stage: {e}"))?;
        Ok(())
    }

    fn fail_job_stage(
        &self,
        job_id: &str,
        stage: PipelineStage,
        error_msg: &str,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE job_stages
             SET status = 'failed',
                 checkpoint_data = ?3,
                 completed_at = ?4,
                 updated_at = ?4
             WHERE job_run_id = ?1 AND stage_name = ?2",
            params![
                job_id,
                stage.as_str(),
                serde_json::json!({ "error": error_msg }).to_string(),
                now
            ],
        )
        .map_err(|e| format!("Failed to mark job stage failed: {e}"))?;
        conn.execute(
            "UPDATE job_runs SET status = 'failed', completed_at = ?2, error_message = ?3 WHERE id = ?1",
            params![job_id, now, error_msg],
        )
        .map_err(|e| format!("Failed to mark job run failed: {e}"))?;
        Ok(())
    }

    fn complete_job_run(&self, job_id: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE job_runs SET status = 'completed', completed_at = ?2, error_message = NULL WHERE id = ?1",
            params![job_id, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to complete job run: {e}"))?;
        Ok(())
    }

    fn get_job_progress(&self, job_id: &str) -> Result<Option<JobProgressResponse>, String> {
        let conn = self.open()?;
        let job = conn
            .query_row(
                "SELECT status, error_message FROM job_runs WHERE id = ?1",
                params![job_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(|e| format!("Job progress lookup failed: {e}"))?;
        let Some((status, error_message)) = job else {
            return Ok(None);
        };

        let mut stmt = conn
            .prepare(
                "SELECT stage_name, status, started_at, completed_at, checkpoint_data
                 FROM job_stages
                 WHERE job_run_id = ?1
                 ORDER BY CASE stage_name
                    WHEN 'QueryPlanning' THEN 1
                    WHEN 'Discovery' THEN 2
                    WHEN 'Merging' THEN 3
                    WHEN 'Validation' THEN 4
                    WHEN 'Filtering' THEN 5
                    WHEN 'Enrichment' THEN 6
                    WHEN 'LeadGeneration' THEN 7
                    ELSE 99 END",
            )
            .map_err(|e| format!("Prepare job stages query failed: {e}"))?;
        let stages = stmt
            .query_map(params![job_id], |row| {
                let checkpoint = row
                    .get::<_, Option<String>>(4)?
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok());
                Ok(JobStageStatus {
                    name: row.get::<_, String>(0)?,
                    status: row.get::<_, String>(1)?,
                    started_at: row.get::<_, Option<String>>(2)?,
                    completed_at: row.get::<_, Option<String>>(3)?,
                    checkpoint,
                })
            })
            .map_err(|e| format!("Job stages query failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Job stages row decode failed: {e}"))?;

        let current_stage = stages
            .iter()
            .find(|stage| stage.status == "running" || stage.status == "failed")
            .map(|stage| stage.name.clone())
            .or_else(|| {
                stages
                    .iter()
                    .rev()
                    .find(|stage| stage.status == "completed")
                    .map(|stage| stage.name.clone())
            });

        Ok(Some(JobProgressResponse {
            job_id: job_id.to_string(),
            status,
            current_stage,
            stages,
            error_message,
        }))
    }

    fn latest_running_job_progress(
        &self,
        job_type: &str,
        segment: SalesSegment,
    ) -> Result<Option<JobProgressResponse>, String> {
        let conn = self.open()?;
        let job_id = conn
            .query_row(
                "SELECT id
                 FROM job_runs
                 WHERE job_type = ?1 AND segment = ?2 AND status = 'running'
                 ORDER BY started_at DESC
                 LIMIT 1",
                params![job_type, segment.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Active job lookup failed: {e}"))?;
        let Some(job_id) = job_id else {
            return Ok(None);
        };
        self.get_job_progress(&job_id)
    }

    fn latest_completed_checkpoint(
        &self,
        job_id: &str,
    ) -> Result<Option<(PipelineStage, String)>, String> {
        let conn = self.open()?;
        let row = conn
            .query_row(
                "SELECT stage_name, checkpoint_data
                 FROM job_stages
                 WHERE job_run_id = ?1
                   AND status = 'completed'
                   AND checkpoint_data IS NOT NULL
                 ORDER BY CASE stage_name
                    WHEN 'QueryPlanning' THEN 1
                    WHEN 'Discovery' THEN 2
                    WHEN 'Merging' THEN 3
                    WHEN 'Validation' THEN 4
                    WHEN 'Filtering' THEN 5
                    WHEN 'Enrichment' THEN 6
                    WHEN 'LeadGeneration' THEN 7
                    ELSE 99 END DESC
                 LIMIT 1",
                params![job_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|e| format!("Checkpoint lookup failed: {e}"))?;
        let Some((stage_name, checkpoint_data)) = row else {
            return Ok(None);
        };
        let stage = match stage_name.as_str() {
            "QueryPlanning" => PipelineStage::QueryPlanning,
            "Discovery" => PipelineStage::Discovery,
            "Merging" => PipelineStage::Merging,
            "Validation" => PipelineStage::Validation,
            "Filtering" => PipelineStage::Filtering,
            "Enrichment" => PipelineStage::Enrichment,
            "LeadGeneration" => PipelineStage::LeadGeneration,
            _ => return Ok(None),
        };
        Ok(Some((stage, checkpoint_data)))
    }

    fn approval_already_pending(
        &self,
        conn: &Connection,
        channel: &str,
        recipient: &str,
    ) -> Result<bool, String> {
        let recipient_json_path = if channel == "email" {
            "$.to"
        } else {
            "$.profile_url"
        };
        let sql = format!(
            "SELECT COUNT(*)
             FROM approvals
             WHERE channel = ?1
               AND status = 'pending'
               AND json_extract(payload_json, '{recipient_json_path}') = ?2"
        );
        conn.query_row(sql.as_str(), params![channel, recipient], |row| {
            row.get::<_, i64>(0)
        })
        .map(|count| count > 0)
        .map_err(|e| format!("Pending approval lookup failed: {e}"))
    }

    fn is_suppressed(&self, conn: &Connection, contact_value: &str) -> Result<bool, String> {
        conn.query_row(
            "SELECT COUNT(*) FROM suppressions WHERE contact_method_value = ?1",
            params![contact_value.trim().to_lowercase()],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .map_err(|e| format!("Suppression lookup failed: {e}"))
    }

    fn suppress_contact(
        &self,
        conn: &Connection,
        value: &str,
        reason: &str,
        permanent: bool,
        source_outcome_id: Option<&str>,
    ) -> Result<(), String> {
        conn.execute(
            "INSERT OR IGNORE INTO suppressions
             (id, contact_method_value, reason, source_outcome_id, suppressed_at, permanent)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                uuid::Uuid::new_v4().to_string(),
                value.trim().to_lowercase(),
                reason,
                source_outcome_id,
                Utc::now().to_rfc3339(),
                i32::from(permanent)
            ],
        )
        .map_err(|e| format!("Failed to suppress contact: {e}"))?;
        Ok(())
    }

    fn update_source_health(&self, source_type: &str, count: usize) -> Result<(), String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        let parser_health = if count == 0 { 0.0 } else { 1.0 };
        conn.execute(
            "INSERT INTO source_health (id, source_type, parser_health, freshness, last_checked_at, auto_skip)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)
             ON CONFLICT(source_type) DO UPDATE SET
                parser_health = excluded.parser_health,
                freshness = excluded.freshness,
                last_checked_at = excluded.last_checked_at,
                auto_skip = CASE WHEN excluded.parser_health = 0.0 THEN 1 ELSE 0 END",
            params![
                uuid::Uuid::new_v4().to_string(),
                source_type,
                parser_health,
                now,
                if count == 0 { 1 } else { 0 }
            ],
        )
        .map_err(|e| format!("Failed to update source health: {e}"))?;
        Ok(())
    }

    fn should_skip_source(&self, source_type: &str) -> Result<bool, String> {
        let conn = self.open()?;
        let auto_skip = conn
            .query_row(
                "SELECT COALESCE(auto_skip, 0) FROM source_health WHERE source_type = ?1",
                params![source_type],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| format!("Source health lookup failed: {e}"))?
            .unwrap_or(0);
        Ok(auto_skip == 1)
    }

    fn list_source_health(&self) -> Result<Vec<SourceHealthRow>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, source_type, precision, freshness, parser_health, legal_mode,
                        historical_reply_yield, last_checked_at, auto_skip
                 FROM source_health
                 ORDER BY source_type ASC",
            )
            .map_err(|e| format!("Prepare source_health query failed: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SourceHealthRow {
                    id: row.get::<_, String>(0)?,
                    source_type: row.get::<_, String>(1)?,
                    precision: row.get::<_, Option<f64>>(2)?,
                    freshness: row.get::<_, Option<String>>(3)?,
                    parser_health: row.get::<_, f64>(4).unwrap_or(0.0),
                    legal_mode: row
                        .get::<_, String>(5)
                        .unwrap_or_else(|_| "public".to_string()),
                    historical_reply_yield: row.get::<_, Option<f64>>(6)?,
                    last_checked_at: row.get::<_, Option<String>>(7)?,
                    auto_skip: row.get::<_, i64>(8).unwrap_or(0) == 1,
                })
            })
            .map_err(|e| format!("Source health query failed: {e}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Source health row decode failed: {e}"))
    }

    fn update_lead_status(&self, lead_id: &str, status: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE leads SET status = ?2 WHERE id = ?1",
            params![lead_id, status],
        )
        .map_err(|e| format!("Failed to update lead status: {e}"))?;
        Ok(())
    }

    fn select_accounts_for_activation(
        &self,
        conn: &Connection,
        candidate_priorities: &HashMap<String, i64>,
        daily_target: u32,
    ) -> Result<Vec<String>, String> {
        if daily_target == 0 || candidate_priorities.is_empty() {
            return Ok(Vec::new());
        }

        let target = daily_target as usize;
        let exploit_target = ((target as f64) * ACTIVATION_EXPLOIT_RATIO).ceil() as usize;
        let candidate_ids = candidate_priorities.keys().cloned().collect::<HashSet<_>>();
        let mut selected = Vec::<String>::new();
        let mut selected_set = HashSet::<String>::new();

        let mut activation_stmt = conn
            .prepare(
                "SELECT account_id, priority
                 FROM activation_queue
                 WHERE status = 'pending'
                 ORDER BY priority DESC, created_at ASC",
            )
            .map_err(|e| format!("Failed to prepare activation selection query: {e}"))?;
        let activation_rows = activation_stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| format!("Failed to query activation queue: {e}"))?;
        for row in activation_rows {
            let (account_id, _priority) =
                row.map_err(|e| format!("Failed to decode activation queue row: {e}"))?;
            if !candidate_ids.contains(&account_id) || !selected_set.insert(account_id.clone()) {
                continue;
            }
            selected.push(account_id);
            if selected.len() >= exploit_target.min(target) {
                break;
            }
        }

        if selected.len() < target {
            let recent_exploration = conn
                .prepare(
                    "SELECT DISTINCT account_id
                     FROM exploration_log
                     WHERE created_at >= datetime('now', '-30 days')",
                )
                .and_then(|mut stmt| {
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<HashSet<_>, _>>()
                })
                .map_err(|e| format!("Failed to load exploration history: {e}"))?;

            let mut score_stmt = conn
                .prepare(
                    "SELECT s.account_id,
                            s.fit_score,
                            COALESCE(s.activation_priority, 0.0)
                     FROM score_snapshots s
                     INNER JOIN (
                        SELECT account_id, MAX(computed_at) AS computed_at
                        FROM score_snapshots
                        GROUP BY account_id
                     ) latest
                       ON latest.account_id = s.account_id
                      AND latest.computed_at = s.computed_at",
                )
                .map_err(|e| format!("Failed to prepare exploration selection query: {e}"))?;
            let mut exploratory = score_stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                })
                .map_err(|e| format!("Failed to query exploration candidates: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode exploration candidates: {e}"))?;
            exploratory.retain(|(account_id, fit_score, _priority)| {
                candidate_ids.contains(account_id)
                    && !selected_set.contains(account_id)
                    && !recent_exploration.contains(account_id)
                    && (0.3..=0.7).contains(fit_score)
            });
            exploratory.sort_by(|a, b| {
                b.2.partial_cmp(&a.2)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            let remaining_slots = target.saturating_sub(selected.len());
            for (account_id, _fit_score, _priority) in exploratory.into_iter().take(remaining_slots)
            {
                conn.execute(
                    "INSERT INTO exploration_log
                     (id, account_id, exploration_reason, exploration_type, outcome_id, learned_pattern, created_at)
                     VALUES (?1, ?2, 'scheduled_exploration', 'mid_score_priority', NULL, NULL, ?3)",
                    params![
                        stable_sales_id("explore", &[&account_id, &Utc::now().format("%Y-%m-%d").to_string()]),
                        account_id,
                        Utc::now().to_rfc3339(),
                    ],
                )
                .map_err(|e| format!("Failed to record exploration selection: {e}"))?;
                if selected_set.insert(account_id.clone()) {
                    selected.push(account_id);
                }
            }
        }

        if selected.len() < target {
            let mut fallback = candidate_priorities
                .iter()
                .map(|(account_id, priority)| (account_id.clone(), *priority))
                .collect::<Vec<_>>();
            fallback.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            for (account_id, _priority) in fallback {
                if selected.len() >= target {
                    break;
                }
                if selected_set.insert(account_id.clone()) {
                    selected.push(account_id);
                }
            }
        }

        for account_id in &selected {
            conn.execute(
                "UPDATE activation_queue
                 SET status = 'activated'
                 WHERE account_id = ?1 AND status = 'pending'",
                params![account_id],
            )
            .map_err(|e| format!("Failed to mark activation queue row activated: {e}"))?;
        }

        Ok(selected)
    }

    fn load_policy_proposal(
        &self,
        conn: &Connection,
        id: &str,
    ) -> Result<Option<SalesPolicyProposal>, String> {
        conn.query_row(
            "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                    backtest_result_json, holdout_result_json, status, approved_by,
                    activated_at, version, created_at
             FROM retrieval_rule_versions
             WHERE id = ?1",
            params![id],
            |row| {
                Ok(SalesPolicyProposal {
                    id: row.get::<_, String>(0)?,
                    rule_type: row.get::<_, String>(1)?,
                    rule_key: row.get::<_, String>(2)?,
                    old_value: row.get::<_, Option<String>>(3)?,
                    new_value: row.get::<_, String>(4)?,
                    proposal_source: row.get::<_, Option<String>>(5)?,
                    backtest_result_json: row.get::<_, Option<String>>(6)?,
                    holdout_result_json: row.get::<_, Option<String>>(7)?,
                    status: row.get::<_, String>(8)?,
                    approved_by: row.get::<_, Option<String>>(9)?,
                    activated_at: row.get::<_, Option<String>>(10)?,
                    version: row.get::<_, i64>(11)?,
                    created_at: row.get::<_, String>(12)?,
                })
            },
        )
        .optional()
        .map_err(|e| format!("Failed to load policy proposal: {e}"))
    }

    fn list_policy_proposals(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SalesPolicyProposal>, String> {
        let conn = self.open()?;
        if let Some(status) = status {
            let mut stmt = conn
                .prepare(
                    "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                            backtest_result_json, holdout_result_json, status, approved_by,
                            activated_at, version, created_at
                     FROM retrieval_rule_versions
                     WHERE status = ?1
                     ORDER BY created_at DESC
                     LIMIT ?2",
                )
                .map_err(|e| format!("Failed to prepare policy proposals query: {e}"))?;
            let rows = stmt
                .query_map(params![status, limit as i64], |row| {
                    Ok(SalesPolicyProposal {
                        id: row.get::<_, String>(0)?,
                        rule_type: row.get::<_, String>(1)?,
                        rule_key: row.get::<_, String>(2)?,
                        old_value: row.get::<_, Option<String>>(3)?,
                        new_value: row.get::<_, String>(4)?,
                        proposal_source: row.get::<_, Option<String>>(5)?,
                        backtest_result_json: row.get::<_, Option<String>>(6)?,
                        holdout_result_json: row.get::<_, Option<String>>(7)?,
                        status: row.get::<_, String>(8)?,
                        approved_by: row.get::<_, Option<String>>(9)?,
                        activated_at: row.get::<_, Option<String>>(10)?,
                        version: row.get::<_, i64>(11)?,
                        created_at: row.get::<_, String>(12)?,
                    })
                })
                .map_err(|e| format!("Failed to query policy proposals: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode policy proposals: {e}"))
        } else {
            let mut stmt = conn
                .prepare(
                    "SELECT id, rule_type, rule_key, old_value, new_value, proposal_source,
                            backtest_result_json, holdout_result_json, status, approved_by,
                            activated_at, version, created_at
                     FROM retrieval_rule_versions
                     ORDER BY created_at DESC
                     LIMIT ?1",
                )
                .map_err(|e| format!("Failed to prepare policy proposals query: {e}"))?;
            let rows = stmt
                .query_map(params![limit as i64], |row| {
                    Ok(SalesPolicyProposal {
                        id: row.get::<_, String>(0)?,
                        rule_type: row.get::<_, String>(1)?,
                        rule_key: row.get::<_, String>(2)?,
                        old_value: row.get::<_, Option<String>>(3)?,
                        new_value: row.get::<_, String>(4)?,
                        proposal_source: row.get::<_, Option<String>>(5)?,
                        backtest_result_json: row.get::<_, Option<String>>(6)?,
                        holdout_result_json: row.get::<_, Option<String>>(7)?,
                        status: row.get::<_, String>(8)?,
                        approved_by: row.get::<_, Option<String>>(9)?,
                        activated_at: row.get::<_, Option<String>>(10)?,
                        version: row.get::<_, i64>(11)?,
                        created_at: row.get::<_, String>(12)?,
                    })
                })
                .map_err(|e| format!("Failed to query policy proposals: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode policy proposals: {e}"))
        }
    }

    fn update_policy_proposal_status(
        &self,
        id: &str,
        status: &str,
        approved_by: Option<&str>,
    ) -> Result<Option<SalesPolicyProposal>, String> {
        let conn = self.open()?;
        let now = Utc::now().to_rfc3339();
        let activated_at = if status == "active" {
            Some(now.clone())
        } else {
            None
        };
        conn.execute(
            "UPDATE retrieval_rule_versions
             SET status = ?2,
                 approved_by = COALESCE(?3, approved_by),
                 activated_at = CASE
                    WHEN ?2 = 'active' THEN ?4
                    WHEN ?2 = 'retired' THEN NULL
                    ELSE activated_at
                 END
             WHERE id = ?1",
            params![id, status, approved_by, activated_at],
        )
        .map_err(|e| format!("Failed to update policy proposal status: {e}"))?;
        self.load_policy_proposal(&conn, id)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_or_refresh_policy_proposal(
        &self,
        conn: &Connection,
        rule_type: &str,
        rule_key: &str,
        old_value: Option<&str>,
        new_value: &str,
        proposal_source: &str,
        backtest_result: &serde_json::Value,
    ) -> Result<String, String> {
        let proposal_id = stable_sales_id(
            "rule_proposal",
            &[rule_type, rule_key, new_value, proposal_source],
        );
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO retrieval_rule_versions
             (id, rule_type, rule_key, old_value, new_value, proposal_source,
              backtest_result_json, holdout_result_json, status, approved_by,
              activated_at, version, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 'proposed', NULL, NULL, 1, ?8)
             ON CONFLICT(id) DO UPDATE SET
                old_value = excluded.old_value,
                new_value = excluded.new_value,
                proposal_source = excluded.proposal_source,
                backtest_result_json = excluded.backtest_result_json,
                status = CASE
                    WHEN retrieval_rule_versions.status = 'active' THEN retrieval_rule_versions.status
                    ELSE 'proposed'
                END",
            params![
                proposal_id,
                rule_type,
                rule_key,
                old_value,
                new_value,
                proposal_source,
                backtest_result.to_string(),
                now,
            ],
        )
        .map_err(|e| format!("Failed to create policy proposal: {e}"))?;
        Ok(proposal_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_missed_signal_review(
        &self,
        conn: &Connection,
        outcome_id: &str,
        snapshot_id: &str,
        account_id: &str,
        outcome_type: &str,
        active_signal_ids: &[String],
        unused_signal_ids: &[String],
    ) -> Result<(), String> {
        let mut signal_stmt = conn
            .prepare(
                "SELECT id, signal_type
                 FROM signals
                 WHERE account_id = ?1
                 ORDER BY confidence DESC, created_at DESC",
            )
            .map_err(|e| format!("Failed to prepare missed-signal query: {e}"))?;
        let all_signals = signal_stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("Failed to query missed-signal candidates: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode missed-signal candidates: {e}"))?;
        let all_signal_types = all_signals
            .iter()
            .map(|(signal_id, signal_type)| (signal_id.clone(), signal_type.clone()))
            .collect::<HashMap<_, _>>();
        let active_set = active_signal_ids.iter().cloned().collect::<HashSet<_>>();
        let unused_set = unused_signal_ids.iter().cloned().collect::<HashSet<_>>();
        let missed_signals = all_signals
            .iter()
            .filter(|(signal_id, _)| {
                !active_set.contains(signal_id) && !unused_set.contains(signal_id)
            })
            .map(|(signal_id, _)| signal_id.clone())
            .collect::<Vec<_>>();

        let positive_outcome = matches!(
            outcome_type,
            "open"
                | "click"
                | "forwarded"
                | "referral"
                | "interested"
                | "meeting_booked"
                | "closed_won"
        );
        let negative_outcome = matches!(
            outcome_type,
            "hard_bounce"
                | "soft_bounce"
                | "no_reply"
                | "auto_reply"
                | "unsubscribe"
                | "wrong_person"
                | "not_now"
                | "closed_lost"
        );
        let validated_signals = if positive_outcome {
            active_signal_ids.to_vec()
        } else {
            Vec::new()
        };
        let false_positive_signals = if negative_outcome {
            active_signal_ids.to_vec()
        } else {
            Vec::new()
        };
        let review_id = stable_sales_id("missed_signal_review", &[outcome_id, snapshot_id]);
        conn.execute(
            "INSERT INTO missed_signal_reviews
             (id, outcome_id, snapshot_id, reviewed_at, validated_signals, false_positive_signals,
              missed_signals, timing_mistakes, persona_mismatch, channel_mismatch, reviewer_type)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '[]', NULL, NULL, 'auto')
             ON CONFLICT(id) DO UPDATE SET
                reviewed_at = excluded.reviewed_at,
                validated_signals = excluded.validated_signals,
                false_positive_signals = excluded.false_positive_signals,
                missed_signals = excluded.missed_signals",
            params![
                review_id,
                outcome_id,
                snapshot_id,
                Utc::now().to_rfc3339(),
                serde_json::to_string(&validated_signals)
                    .map_err(|e| format!("Failed to encode validated signals: {e}"))?,
                serde_json::to_string(&false_positive_signals)
                    .map_err(|e| format!("Failed to encode false-positive signals: {e}"))?,
                serde_json::to_string(&missed_signals)
                    .map_err(|e| format!("Failed to encode missed signals: {e}"))?,
            ],
        )
        .map_err(|e| format!("Failed to persist missed signal review: {e}"))?;

        let (proposal_direction, driver_signal_id) =
            if let Some(signal_id) = validated_signals.first() {
                ("increase", Some(signal_id.clone()))
            } else if let Some(signal_id) = false_positive_signals.first() {
                ("decrease", Some(signal_id.clone()))
            } else {
                ("", None)
            };
        if let Some(signal_id) = driver_signal_id {
            if let Some(signal_type) = all_signal_types.get(&signal_id) {
                let proposal_payload = serde_json::json!({
                    "signal_type": signal_type,
                    "direction": proposal_direction,
                    "trigger_outcome": outcome_type,
                    "driver_signal_id": signal_id,
                    "review_id": review_id,
                });
                let backtest_result = serde_json::json!({
                    "outcome_id": outcome_id,
                    "validated_signal_count": validated_signals.len(),
                    "false_positive_signal_count": false_positive_signals.len(),
                    "missed_signal_count": missed_signals.len(),
                });
                let _ = self.create_or_refresh_policy_proposal(
                    conn,
                    "signal_weight",
                    &format!("signal_weight::{signal_type}"),
                    None,
                    &proposal_payload.to_string(),
                    &format!("auto_outcome_review::{outcome_type}"),
                    &backtest_result,
                )?;
            }
        }

        Ok(())
    }

    fn migrate_legacy_to_canonical_core(&self) -> Result<(), String> {
        let conn = self.open()?;
        let sales_profile = self.get_profile(SalesSegment::B2B).ok().flatten();

        if let Some(profile) = sales_profile.as_ref() {
            let icp_id = "default_icp";
            conn.execute(
                "INSERT INTO icp_definitions (id, name, sector_rules, geo_rules, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    sector_rules = excluded.sector_rules,
                    geo_rules = excluded.geo_rules",
                params![
                    icp_id,
                    format!("{} ICP", profile.product_name.trim()),
                    serde_json::json!([profile.target_industry]).to_string(),
                    serde_json::json!([profile.target_geo]).to_string(),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to upsert icp_definitions: {e}"))?;

            let sender_domain = email_domain(&profile.sender_email).unwrap_or_default();
            conn.execute(
                "INSERT INTO sender_policies (id, icp_id, mailbox_pool, daily_cap, subdomain, warm_state)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'warming')
                 ON CONFLICT(id) DO UPDATE SET
                    mailbox_pool = excluded.mailbox_pool,
                    daily_cap = excluded.daily_cap,
                    subdomain = excluded.subdomain",
                params![
                    "default_sender_policy",
                    icp_id,
                    serde_json::to_string(&default_mailbox_pool_from_profile(profile))
                        .map_err(|e| format!("Failed to encode default mailbox pool: {e}"))?,
                    profile.daily_send_cap as i64,
                    sender_domain,
                ],
            )
            .map_err(|e| format!("Failed to upsert sender_policies: {e}"))?;
        }

        for profile in self
            .list_stored_prospect_profiles(SalesSegment::B2B, 10_000, None)
            .unwrap_or_default()
        {
            self.migrate_prospect_profile(&conn, &profile, sales_profile.as_ref())?;
        }
        for lead in self.list_leads(10_000, None).unwrap_or_default() {
            self.migrate_lead(&conn, &lead, sales_profile.as_ref())?;
        }
        Ok(())
    }

    fn migrate_prospect_profile(
        &self,
        conn: &Connection,
        profile: &SalesProspectProfile,
        sales_profile: Option<&SalesProfile>,
    ) -> Result<(), String> {
        let account_id = stable_sales_id("acct", &[&profile.company_domain]);
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO accounts (id, canonical_name, display_name, sector, geo, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
             ON CONFLICT(id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                display_name = excluded.display_name,
                sector = COALESCE(accounts.sector, excluded.sector),
                geo = COALESCE(accounts.geo, excluded.geo),
                website = COALESCE(accounts.website, excluded.website),
                tier = excluded.tier,
                updated_at = excluded.updated_at",
            params![
                account_id,
                profile.company,
                profile.company,
                sales_profile.map(|p| p.target_industry.clone()),
                sales_profile.map(|p| p.target_geo.clone()),
                profile.website,
                "standard",
                now,
            ],
        )
        .map_err(|e| format!("Failed to migrate account from prospect profile: {e}"))?;

        let domain_id = stable_sales_id("dom", &[&profile.company_domain]);
        conn.execute(
            "INSERT INTO domains (id, account_id, domain, is_primary, verified, checked_at)
             VALUES (?1, ?2, ?3, 1, ?4, ?5)
             ON CONFLICT(domain) DO UPDATE SET
                account_id = excluded.account_id,
                verified = excluded.verified,
                checked_at = excluded.checked_at",
            params![
                domain_id,
                account_id,
                profile.company_domain,
                i32::from(is_valid_company_domain(&profile.company_domain)),
                now,
            ],
        )
        .map_err(|e| format!("Failed to migrate domain from prospect profile: {e}"))?;

        if profile.primary_contact_name.is_some() || profile.primary_contact_title.is_some() {
            let contact_id = stable_sales_id(
                "contact",
                &[
                    &profile.company_domain,
                    profile.primary_contact_name.as_deref().unwrap_or("primary"),
                ],
            );
            conn.execute(
                "INSERT INTO contacts (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                    full_name = COALESCE(excluded.full_name, contacts.full_name),
                    title = COALESCE(excluded.title, contacts.title),
                    seniority = excluded.seniority,
                    name_confidence = MAX(contacts.name_confidence, excluded.name_confidence),
                    title_confidence = MAX(contacts.title_confidence, excluded.title_confidence),
                    is_decision_maker = MAX(contacts.is_decision_maker, excluded.is_decision_maker)",
                params![
                    contact_id,
                    account_id,
                    profile.primary_contact_name,
                    profile.primary_contact_title,
                    seniority_from_title(profile.primary_contact_title.as_deref()),
                    if profile.primary_contact_name.is_some() { 0.8 } else { 0.5 },
                    if profile.primary_contact_title.is_some() { 0.7 } else { 0.5 },
                    i32::from(
                        profile
                            .primary_contact_title
                            .as_deref()
                            .map(contact_title_priority)
                            .unwrap_or(0)
                            > 0
                    ),
                    now,
                ],
            )
            .map_err(|e| format!("Failed to migrate contact from prospect profile: {e}"))?;

            self.migrate_contact_methods(
                conn,
                &contact_id,
                profile.primary_email.as_deref(),
                None,
                profile.primary_linkedin_url.as_deref(),
            )?;
        }

        for signal in profile.matched_signals.iter().take(6) {
            let signal_id = stable_sales_id("signal", &[&account_id, signal]);
            conn.execute(
                "INSERT OR IGNORE INTO signals
                 (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
                 VALUES (?1, ?2, 'site_content', ?3, 'migration', ?4, ?5, 'structural', NULL, ?4)",
                params![signal_id, account_id, signal, now, source_confidence("directory_listing")],
            )
            .map_err(|e| format!("Failed to migrate signals: {e}"))?;
        }

        for link in profile
            .osint_links
            .iter()
            .take(MAX_OSINT_LINKS_PER_PROSPECT)
        {
            let artifact_id = stable_sales_id("artifact", &[&account_id, link]);
            conn.execute(
                "INSERT OR IGNORE INTO artifacts
                 (id, source_type, source_id, raw_content, parse_status, parser_health, freshness, legal_mode, created_at)
                 VALUES (?1, 'web_search', ?2, ?2, 'ok', 1.0, ?3, 'public', ?3)",
                params![artifact_id, link, now],
            )
            .map_err(|e| format!("Failed to migrate artifact: {e}"))?;
        }

        Ok(())
    }

    fn migrate_lead(
        &self,
        conn: &Connection,
        lead: &SalesLead,
        sales_profile: Option<&SalesProfile>,
    ) -> Result<(), String> {
        let profile = SalesProspectProfile {
            id: lead.company_domain.clone(),
            run_id: lead.run_id.clone(),
            company: lead.company.clone(),
            website: lead.website.clone(),
            company_domain: lead.company_domain.clone(),
            fit_score: lead.score,
            profile_status: if lead.email.is_some() || lead.linkedin_url.is_some() {
                "contact_ready".to_string()
            } else {
                "contact_identified".to_string()
            },
            summary: lead.reasons.join(" "),
            matched_signals: lead.reasons.clone(),
            primary_contact_name: clean_profile_contact_name(&lead.contact_name),
            primary_contact_title: clean_profile_contact_field(&lead.contact_title),
            primary_email: lead.email.clone(),
            primary_linkedin_url: lead.linkedin_url.clone(),
            company_linkedin_url: None,
            osint_links: vec![lead.website.clone()],
            contact_count: 1,
            source_count: lead.reasons.len() as u32,
            buyer_roles: Vec::new(),
            pain_points: Vec::new(),
            trigger_events: Vec::new(),
            recommended_channel: if lead.email.is_some() {
                "email"
            } else {
                "linkedin"
            }
            .to_string(),
            outreach_angle: String::new(),
            research_status: "migration".to_string(),
            research_confidence: 0.7,
            tech_stack: Vec::new(),
            created_at: lead.created_at.clone(),
            updated_at: lead.created_at.clone(),
        };
        self.migrate_prospect_profile(conn, &profile, sales_profile)?;

        let account_id = stable_sales_id("acct", &[&lead.company_domain]);
        let score_id = stable_sales_id("score", &[&account_id, &lead.run_id]);
        conn.execute(
            "INSERT OR IGNORE INTO score_snapshots
             (id, account_id, fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk, activation_priority, computed_at, scoring_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'v1')",
            params![
                score_id,
                account_id,
                (lead.score as f64 / 100.0).clamp(0.0, 1.0),
                if !lead.reasons.is_empty() { 0.35 } else { 0.1 },
                if lead.email.is_some() { 0.65 } else if lead.linkedin_url.is_some() { 0.45 } else { 0.1 },
                if lead.email.is_some() { 0.2 } else { 0.45 },
                0.1,
                (lead.score as f64 / 100.0).clamp(0.0, 1.0),
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| format!("Failed to migrate score snapshot: {e}"))?;

        Ok(())
    }

    fn migrate_contact_methods(
        &self,
        conn: &Connection,
        contact_id: &str,
        email: Option<&str>,
        phone: Option<&str>,
        linkedin: Option<&str>,
    ) -> Result<(), String> {
        if let Some(email) =
            email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
        {
            let method_id = stable_sales_id("cm", &[contact_id, "email", &email]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'email', ?3, ?4, ?5, ?6, 0)",
                params![
                    method_id,
                    contact_id,
                    email,
                    source_confidence("directory_listing"),
                    Utc::now().to_rfc3339(),
                    classify_email(&email, email_domain(&email).as_deref().unwrap_or_default()),
                ],
            )
            .map_err(|e| format!("Failed to migrate email contact method: {e}"))?;
        }
        if let Some(phone) = phone.and_then(normalize_phone) {
            let method_id = stable_sales_id("cm", &[contact_id, "phone", &phone]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'phone', ?3, ?4, ?5, 'personal', 0)",
                params![
                    method_id,
                    contact_id,
                    phone,
                    source_confidence("directory_listing"),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to migrate phone contact method: {e}"))?;
        }
        if let Some(linkedin) = linkedin.and_then(normalize_outreach_linkedin_url) {
            let method_id = stable_sales_id("cm", &[contact_id, "linkedin", &linkedin]);
            conn.execute(
                "INSERT OR IGNORE INTO contact_methods
                 (id, contact_id, channel_type, value, confidence, verified_at, classification, suppressed)
                 VALUES (?1, ?2, 'linkedin', ?3, ?4, ?5, 'personal', 0)",
                params![
                    method_id,
                    contact_id,
                    linkedin,
                    source_confidence("web_search"),
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(|e| format!("Failed to migrate LinkedIn contact method: {e}"))?;
        }
        Ok(())
    }

    fn upsert_artifact(
        &self,
        conn: &Connection,
        source_type: &str,
        source_id: &str,
        raw_content: &str,
        freshness: &str,
    ) -> Result<String, String> {
        let artifact_id = stable_sales_id("artifact", &[source_type, source_id]);
        conn.execute(
            "INSERT INTO artifacts
             (id, source_type, source_id, raw_content, parse_status, parser_health, freshness, legal_mode, created_at)
             VALUES (?1, ?2, ?3, ?4, 'ok', 1.0, ?5, 'public', ?5)
             ON CONFLICT(id) DO UPDATE SET
                raw_content = excluded.raw_content,
                freshness = excluded.freshness,
                parser_health = excluded.parser_health",
            params![artifact_id, source_type, source_id, raw_content, freshness],
        )
        .map_err(|e| format!("Failed to upsert artifact: {e}"))?;
        Ok(artifact_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_evidence(
        &self,
        conn: &Connection,
        artifact_id: &str,
        field_type: &str,
        field_value: &str,
        extraction_method: &str,
        confidence: f64,
        verified_at: &str,
    ) -> Result<Option<String>, String> {
        let value = field_value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        let evidence_id = stable_sales_id("evidence", &[artifact_id, field_type, value]);
        conn.execute(
            "INSERT INTO evidence
             (id, artifact_id, field_type, field_value, confidence, extraction_method, verified_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                confidence = MAX(evidence.confidence, excluded.confidence),
                verified_at = excluded.verified_at",
            params![
                evidence_id,
                artifact_id,
                field_type,
                value,
                confidence,
                extraction_method,
                verified_at,
            ],
        )
        .map_err(|e| format!("Failed to upsert evidence: {e}"))?;
        Ok(Some(evidence_id))
    }

    fn create_signal_with_rationale(
        &self,
        conn: &Connection,
        account_id: &str,
        signal_type: &str,
        text: &str,
        source: &str,
        evidence_ids: &[String],
    ) -> Result<String, String> {
        let signal_text = truncate_cleaned_text(text, 280);
        if signal_text.trim().is_empty() {
            return Err("Signal text is empty".to_string());
        }
        let signal_id = stable_sales_id("signal", &[account_id, signal_type, &signal_text]);
        let now = Utc::now().to_rfc3339();
        let (horizon, expires_at) = classify_signal_horizon(signal_type, &signal_text);
        conn.execute(
            "INSERT INTO signals
             (id, account_id, signal_type, text, source, observed_at, confidence, effect_horizon, expires_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?6)
             ON CONFLICT(id) DO UPDATE SET
                confidence = MAX(signals.confidence, excluded.confidence),
                observed_at = excluded.observed_at,
                effect_horizon = excluded.effect_horizon,
                expires_at = excluded.expires_at",
            params![
                signal_id,
                account_id,
                signal_type,
                signal_text,
                source,
                now,
                source_confidence(source),
                horizon,
                expires_at,
            ],
        )
        .map_err(|e| format!("Failed to upsert signal: {e}"))?;

        let rationale_id = stable_sales_id("signal_rationale", &[&signal_id, account_id]);
        conn.execute(
            "INSERT INTO signal_rationales
             (id, signal_id, account_id, why_it_matters, expected_effect, evidence_ids, confidence, created_at)
             VALUES (?1, ?2, ?3, ?4, 'meeting_probability_up', ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                why_it_matters = excluded.why_it_matters,
                expected_effect = excluded.expected_effect,
                evidence_ids = excluded.evidence_ids,
                confidence = MAX(signal_rationales.confidence, excluded.confidence)",
            params![
                rationale_id,
                signal_id,
                account_id,
                generate_signal_rationale(signal_type, &signal_text),
                serde_json::to_string(evidence_ids)
                    .map_err(|e| format!("Failed to encode signal evidence ids: {e}"))?,
                source_confidence(source),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert signal rationale: {e}"))?;
        Ok(signal_id)
    }

    fn enqueue_research(
        &self,
        conn: &Connection,
        account_id: &str,
        reason: &str,
        priority: i64,
    ) -> Result<(), String> {
        let id = stable_sales_id("research", &[account_id]);
        conn.execute(
            "INSERT INTO research_queue (id, account_id, priority, reason, status, assigned_at, completed_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET
                priority = MAX(research_queue.priority, excluded.priority),
                reason = excluded.reason,
                status = CASE
                    WHEN research_queue.status = 'completed' THEN research_queue.status
                    ELSE 'pending'
                END",
            params![id, account_id, priority, reason],
        )
        .map_err(|e| format!("Failed to enqueue research: {e}"))?;
        Ok(())
    }

    fn enqueue_activation(
        &self,
        conn: &Connection,
        account_id: &str,
        contact_id: &str,
        thesis_id: &str,
        priority: i64,
    ) -> Result<(), String> {
        let id = stable_sales_id("activation", &[account_id, contact_id, thesis_id]);
        conn.execute(
            "INSERT INTO activation_queue (id, account_id, contact_id, thesis_id, priority, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)
             ON CONFLICT(id) DO UPDATE SET
                priority = MAX(activation_queue.priority, excluded.priority),
                status = CASE
                    WHEN activation_queue.status IN ('completed', 'active') THEN activation_queue.status
                    ELSE 'pending'
                END",
            params![id, account_id, contact_id, thesis_id, priority, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to enqueue activation: {e}"))?;
        Ok(())
    }

    fn ensure_default_sequence_template(&self, conn: &Connection) -> Result<String, String> {
        let template_id = "default_outreach_sequence".to_string();
        let steps_json = serde_json::json!([
            {"step": 1, "channel": "email", "delay_days": 0, "type": "initial_outreach", "description": "Short evidence-based email with a soft CTA"},
            {"step": 2, "channel": "email", "delay_days": 3, "type": "value_content", "description": "Share a teardown, case study, or relevant operational insight"},
            {"step": 3, "channel": "email", "delay_days": 5, "type": "follow_up", "description": "Reference the first touch and add one new angle"},
            {"step": 4, "channel": "linkedin_assist", "delay_days": 3, "type": "channel_switch", "description": "Manual LinkedIn follow-up for the operator"},
            {"step": 5, "channel": "email", "delay_days": 5, "type": "closing", "description": "Final polite close-the-loop email"}
        ])
        .to_string();
        conn.execute(
            "INSERT INTO sequence_templates (id, name, steps_json, icp_id, persona_id, version, created_at)
             VALUES (?1, 'Default Outreach Sequence', ?2, 'default_icp', NULL, 1, ?3)
             ON CONFLICT(id) DO UPDATE SET
                steps_json = excluded.steps_json,
                version = excluded.version",
            params![template_id, steps_json, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Failed to ensure default sequence template: {e}"))?;
        Ok(template_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_account_thesis(
        &self,
        conn: &Connection,
        profile: &SalesProfile,
        account_id: &str,
        contact_id: &str,
        evidence_ids: &[String],
        score: &FiveAxisScore,
        gate: &SendGateDecision,
    ) -> Result<String, String> {
        let account = conn
            .query_row(
                "SELECT canonical_name, COALESCE(sector, ''), COALESCE(geo, ''), COALESCE(website, '')
                 FROM accounts WHERE id = ?1",
                params![account_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(|e| format!("Failed to load account for thesis: {e}"))?;

        let mut contact_stmt = conn
            .prepare(
                "SELECT COALESCE(full_name, ''), COALESCE(title, ''), name_confidence
                 FROM contacts
                 WHERE account_id = ?1
                 ORDER BY is_decision_maker DESC, created_at ASC
                 LIMIT 4",
            )
            .map_err(|e| format!("Failed to prepare thesis contacts query: {e}"))?;
        let buyer_committee = contact_stmt
            .query_map(params![account_id], |row| {
                Ok(serde_json::json!({
                    "role": infer_buyer_role(
                        row.get::<_, String>(1)
                            .unwrap_or_default()
                            .as_str()
                    ),
                    "name": row.get::<_, String>(0).unwrap_or_default(),
                    "confidence": row.get::<_, f64>(2).unwrap_or(0.5),
                }))
            })
            .map_err(|e| format!("Failed to query thesis contacts: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode thesis contacts: {e}"))?;

        let mut signal_stmt = conn
            .prepare(
                "SELECT text FROM signals
                 WHERE account_id = ?1
                 ORDER BY confidence DESC, observed_at DESC
                 LIMIT 3",
            )
            .map_err(|e| format!("Failed to prepare thesis signals query: {e}"))?;
        let signals = signal_stmt
            .query_map(params![account_id], |row| row.get::<_, String>(0))
            .map_err(|e| format!("Failed to query thesis signals: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode thesis signals: {e}"))?;

        let why_this_account = if let Some(signal) = signals.first() {
            format!(
                "{} matches {} in {} and shows public evidence of operational activity: {}",
                account.0,
                profile.product_name,
                if account.1.trim().is_empty() {
                    profile.target_industry.as_str()
                } else {
                    account.1.as_str()
                },
                truncate_text_for_reason(signal, 180),
            )
        } else {
            format!(
                "{} matches the ICP for {} in {}",
                account.0, profile.product_name, profile.target_industry
            )
        };
        let why_now = if let Some(signal) = signals.first() {
            truncate_text_for_reason(signal, 180)
        } else {
            format!(
                "Public evidence and reachable contacts indicate a viable outbound window in {}",
                if account.2.trim().is_empty() {
                    profile.target_geo.as_str()
                } else {
                    account.2.as_str()
                }
            )
        };
        let recommended_channel = recommended_activation_channel(conn, account_id, contact_id)
            .unwrap_or_else(|| "research".to_string());
        let thesis_confidence = thesis_confidence(score);
        let thesis_status = match gate {
            SendGateDecision::Activate => "ready",
            SendGateDecision::Research { .. } => "needs_research",
            SendGateDecision::Nurture { .. } => "nurture",
            SendGateDecision::Block { .. } => "blocked",
        };
        let thesis_id = stable_sales_id("thesis", &[account_id]);
        conn.execute(
            "INSERT INTO account_theses
             (id, account_id, why_this_account, why_now, buyer_committee_json, evidence_ids, do_not_say,
              recommended_channel, recommended_pain, thesis_confidence, thesis_status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(id) DO UPDATE SET
                why_this_account = excluded.why_this_account,
                why_now = excluded.why_now,
                buyer_committee_json = excluded.buyer_committee_json,
                evidence_ids = excluded.evidence_ids,
                do_not_say = excluded.do_not_say,
                recommended_channel = excluded.recommended_channel,
                recommended_pain = excluded.recommended_pain,
                thesis_confidence = excluded.thesis_confidence,
                thesis_status = excluded.thesis_status",
            params![
                thesis_id,
                account_id,
                why_this_account,
                why_now,
                serde_json::to_string(&buyer_committee)
                    .map_err(|e| format!("Failed to encode buyer committee: {e}"))?,
                serde_json::to_string(evidence_ids)
                    .map_err(|e| format!("Failed to encode thesis evidence ids: {e}"))?,
                serde_json::json!(["Do not claim internal knowledge beyond public evidence."])
                    .to_string(),
                recommended_channel,
                truncate_text_for_reason(&profile.product_description, 180),
                thesis_confidence,
                thesis_status,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|e| format!("Failed to upsert account thesis: {e}"))?;
        Ok(thesis_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn sync_canonical_state(
        &self,
        conn: &Connection,
        profile: &SalesProfile,
        candidate: &DomainCandidate,
        company: &str,
        domain: &str,
        website: &str,
        contact_name: Option<&str>,
        contact_title: Option<&str>,
        email: Option<&str>,
        phone: Option<&str>,
        linkedin_url: Option<&str>,
        company_linkedin_url: Option<&str>,
        osint_links: &[String],
        evidence_text: &str,
        reasons: &[String],
    ) -> Result<CanonicalAccountSync, String> {
        let now = Utc::now().to_rfc3339();
        let account_id = stable_sales_id("acct", &[domain]);
        conn.execute(
            "INSERT INTO accounts
             (id, canonical_name, display_name, sector, geo, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'standard', ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                display_name = excluded.display_name,
                sector = COALESCE(NULLIF(accounts.sector, ''), excluded.sector),
                geo = COALESCE(NULLIF(accounts.geo, ''), excluded.geo),
                website = COALESCE(NULLIF(accounts.website, ''), excluded.website),
                updated_at = excluded.updated_at",
            params![
                account_id,
                company,
                company,
                profile.target_industry,
                profile.target_geo,
                website,
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert account: {e}"))?;

        let alias_company = domain_to_company(domain);
        if !alias_company.trim().is_empty() && alias_company != company {
            conn.execute(
                "INSERT OR IGNORE INTO account_aliases (id, account_id, alias_name, alias_type)
                 VALUES (?1, ?2, ?3, 'derived_domain')",
                params![
                    stable_sales_id("acct_alias", &[&account_id, &alias_company]),
                    account_id,
                    alias_company,
                ],
            )
            .map_err(|e| format!("Failed to upsert account alias: {e}"))?;
        }

        conn.execute(
            "INSERT INTO domains (id, account_id, domain, is_primary, verified, mx_valid, checked_at)
             VALUES (?1, ?2, ?3, 1, ?4, NULL, ?5)
             ON CONFLICT(domain) DO UPDATE SET
                account_id = excluded.account_id,
                is_primary = excluded.is_primary,
                verified = excluded.verified,
                checked_at = excluded.checked_at",
            params![
                stable_sales_id("domain", &[domain]),
                account_id,
                domain,
                i32::from(is_valid_company_domain(domain)),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert domain: {e}"))?;

        let cleaned_name = contact_name.and_then(clean_profile_contact_name);
        let cleaned_title = contact_title.and_then(clean_profile_contact_field);
        let contact_id = stable_sales_id(
            "contact",
            &[
                domain,
                &canonical_contact_key(domain, cleaned_name.as_deref(), email, linkedin_url),
            ],
        );
        conn.execute(
            "INSERT INTO contacts
             (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO UPDATE SET
                full_name = COALESCE(excluded.full_name, contacts.full_name),
                title = COALESCE(excluded.title, contacts.title),
                seniority = excluded.seniority,
                name_confidence = MAX(contacts.name_confidence, excluded.name_confidence),
                title_confidence = MAX(contacts.title_confidence, excluded.title_confidence),
                is_decision_maker = MAX(contacts.is_decision_maker, excluded.is_decision_maker)",
            params![
                contact_id,
                account_id,
                cleaned_name,
                cleaned_title,
                seniority_from_title(cleaned_title.as_deref()),
                if cleaned_name.is_some() { 0.9 } else { 0.4 },
                if cleaned_title.is_some() { 0.8 } else { 0.4 },
                i32::from(
                    cleaned_title
                        .as_deref()
                        .map(contact_title_priority)
                        .unwrap_or(0)
                        > 0
                ),
                now,
            ],
        )
        .map_err(|e| format!("Failed to upsert contact: {e}"))?;

        if let Some(role) = cleaned_title.as_deref().map(infer_buyer_role) {
            conn.execute(
                "INSERT OR IGNORE INTO buyer_roles (id, account_id, contact_id, role_type, inferred_from)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    stable_sales_id("buyer_role", &[&account_id, &contact_id, role]),
                    account_id,
                    contact_id,
                    role,
                    cleaned_title.clone().unwrap_or_default(),
                ],
            )
            .map_err(|e| format!("Failed to upsert buyer role: {e}"))?;
        }

        self.migrate_contact_methods(conn, &contact_id, email, phone, linkedin_url)?;

        for (channel_type, value) in [
            (
                "email",
                email.and_then(|value| normalize_email_candidate(Some(value.to_string()))),
            ),
            ("phone", phone.and_then(normalize_phone)),
            (
                "linkedin",
                linkedin_url.and_then(normalize_outreach_linkedin_url),
            ),
        ] {
            if let Some(value) = value {
                let suppressed = self.is_suppressed(conn, &value)?;
                conn.execute(
                    "UPDATE contact_methods SET suppressed = ?1
                     WHERE contact_id = ?2 AND channel_type = ?3 AND value = ?4",
                    params![i32::from(suppressed), contact_id, channel_type, value],
                )
                .map_err(|e| format!("Failed to update contact method suppression: {e}"))?;
            }
        }

        let mut evidence_ids = Vec::new();
        let primary_source_type = candidate_primary_source_type(candidate, company_linkedin_url);
        let artifact_id = self.upsert_artifact(
            conn,
            primary_source_type,
            domain,
            &truncate_cleaned_text(
                &format!(
                    "{} | {} | {}",
                    evidence_text,
                    candidate.evidence.join(" | "),
                    reasons.join(" | ")
                ),
                2000,
            ),
            &now,
        )?;
        for item in [
            self.upsert_evidence(
                conn,
                &artifact_id,
                "company_name",
                company,
                primary_source_type,
                0.9,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "domain",
                domain,
                primary_source_type,
                0.95,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "website",
                website,
                primary_source_type,
                0.9,
                &now,
            )?,
            self.upsert_evidence(
                conn,
                &artifact_id,
                "signal",
                evidence_text,
                primary_source_type,
                0.8,
                &now,
            )?,
        ]
        .into_iter()
        .flatten()
        {
            evidence_ids.push(item);
        }

        if let Some(name) = cleaned_name.as_deref() {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "contact_name",
                name,
                primary_source_type,
                0.8,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(title) = cleaned_title.as_deref() {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "title",
                title,
                primary_source_type,
                0.75,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(email) =
            email.and_then(|value| normalize_email_candidate(Some(value.to_string())))
        {
            if let Some(id) =
                self.upsert_evidence(conn, &artifact_id, "email", &email, "site_html", 0.9, &now)?
            {
                evidence_ids.push(id);
            }
        }
        if let Some(phone) = phone.and_then(normalize_phone) {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "phone",
                &phone,
                primary_source_type,
                0.8,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(linkedin) = linkedin_url.and_then(normalize_outreach_linkedin_url) {
            if let Some(id) = self.upsert_evidence(
                conn,
                &artifact_id,
                "linkedin",
                &linkedin,
                "web_search",
                0.7,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }
        if let Some(company_linkedin) =
            company_linkedin_url.and_then(normalize_company_linkedin_url)
        {
            let company_artifact = self.upsert_artifact(
                conn,
                "web_search",
                &company_linkedin,
                &company_linkedin,
                &now,
            )?;
            if let Some(id) = self.upsert_evidence(
                conn,
                &company_artifact,
                "linkedin",
                &company_linkedin,
                "web_search",
                0.65,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }

        for link in osint_links.iter().take(MAX_OSINT_LINKS_PER_PROSPECT) {
            let osint_artifact = self.upsert_artifact(conn, "web_search", link, link, &now)?;
            if let Some(id) = self.upsert_evidence(
                conn,
                &osint_artifact,
                "signal",
                link,
                "web_search",
                0.55,
                &now,
            )? {
                evidence_ids.push(id);
            }
        }

        let signal_source = if primary_source_type == "directory_listing" {
            "directory_listing"
        } else {
            "web_search"
        };
        let mut signal_texts = candidate.matched_keywords.clone();
        signal_texts.extend(candidate.evidence.iter().take(3).cloned());
        signal_texts.extend(reasons.iter().take(2).cloned());
        signal_texts.push(evidence_text.to_string());
        for signal in dedupe_strings(signal_texts)
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .take(8)
        {
            let signal_type = infer_signal_type(&signal);
            let _ = self.create_signal_with_rationale(
                conn,
                &account_id,
                signal_type,
                &signal,
                signal_source,
                &evidence_ids,
            );
        }

        let score = compute_five_axis_score(&account_id, conn)?;
        conn.execute(
            "UPDATE accounts SET tier = ?2, updated_at = ?3 WHERE id = ?1",
            params![account_id, assign_tier(&score), now],
        )
        .map_err(|e| format!("Failed to update account tier: {e}"))?;
        let gate = send_gate(&score);
        let thesis_id = self.build_account_thesis(
            conn,
            profile,
            &account_id,
            &contact_id,
            &evidence_ids,
            &score,
            &gate,
        )?;

        match &gate {
            SendGateDecision::Research { missing } => {
                self.enqueue_research(
                    conn,
                    &account_id,
                    &missing.join("; "),
                    (activation_priority(&score) * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Activate => {
                self.enqueue_activation(
                    conn,
                    &account_id,
                    &contact_id,
                    &thesis_id,
                    (activation_priority(&score) * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Nurture { reason } => {
                self.enqueue_research(
                    conn,
                    &account_id,
                    reason,
                    (score.fit_score * 100.0).round() as i64,
                )?;
            }
            SendGateDecision::Block { .. } => {}
        }

        Ok(CanonicalAccountSync { score, gate })
    }

    fn ensure_touch_for_approval(
        &self,
        conn: &Connection,
        lead: &SalesLead,
        approval_id: &str,
        channel: &str,
        payload_json: &str,
    ) -> Result<(), String> {
        let account_id = stable_sales_id("acct", &[&lead.company_domain]);
        let contact_id = stable_sales_id(
            "contact",
            &[
                &lead.company_domain,
                &canonical_contact_key(
                    &lead.company_domain,
                    clean_profile_contact_name(&lead.contact_name).as_deref(),
                    lead.email.as_deref(),
                    lead.linkedin_url.as_deref(),
                ),
            ],
        );
        let thesis_id = conn
            .query_row(
                "SELECT id FROM account_theses WHERE account_id = ?1 ORDER BY created_at DESC LIMIT 1",
                params![account_id.clone()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load thesis for touch: {e}"))?;
        let template_id = self.ensure_default_sequence_template(conn)?;
        let sequence_instance_id = stable_sales_id(
            "sequence_instance",
            &[
                &account_id,
                &contact_id,
                thesis_id.as_deref().unwrap_or("none"),
            ],
        );
        let now = Utc::now().to_rfc3339();
        let initial_step = if channel == "email" {
            1
        } else if channel == "linkedin_assist" {
            4
        } else {
            2
        };
        conn.execute(
            "INSERT OR IGNORE INTO accounts
             (id, canonical_name, display_name, website, tier, created_at, updated_at)
             VALUES (?1, ?2, ?2, ?3, 'standard', ?4, ?4)",
            params![account_id, lead.company, lead.website, now],
        )
        .map_err(|e| format!("Failed to ensure touch account: {e}"))?;
        conn.execute(
            "INSERT OR IGNORE INTO domains (id, account_id, domain, is_primary, verified, mx_valid, checked_at)
             VALUES (?1, ?2, ?3, 1, 1, NULL, ?4)",
            params![
                stable_sales_id("domain", &[&lead.company_domain]),
                account_id,
                lead.company_domain,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch domain: {e}"))?;
        conn.execute(
            "INSERT OR IGNORE INTO contacts
             (id, account_id, full_name, title, seniority, department, name_confidence, title_confidence, is_decision_maker, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)",
            params![
                contact_id,
                account_id,
                clean_profile_contact_name(&lead.contact_name),
                clean_profile_contact_field(&lead.contact_title),
                seniority_from_title(Some(&lead.contact_title)),
                0.8,
                0.8,
                i32::from(contact_title_priority(&lead.contact_title) > 0),
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch contact: {e}"))?;
        self.migrate_contact_methods(
            conn,
            &contact_id,
            lead.email.as_deref(),
            lead.phone.as_deref(),
            lead.linkedin_url.as_deref(),
        )?;
        conn.execute(
            "INSERT INTO sequence_instances
             (id, template_id, account_id, contact_id, thesis_id, current_step, status, started_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?7)
             ON CONFLICT(id) DO UPDATE SET
                updated_at = excluded.updated_at,
                current_step = MIN(sequence_instances.current_step, excluded.current_step)",
            params![
                sequence_instance_id,
                template_id,
                account_id,
                contact_id,
                thesis_id,
                initial_step,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure sequence instance: {e}"))?;

        // Auto-assign to active experiment if one exists (TASK-37)
        if let Ok(exp_id) = conn.query_row(
            "SELECT id FROM experiments WHERE status = 'active' ORDER BY created_at DESC LIMIT 1",
            [],
            |r| r.get::<_, String>(0),
        ) {
            let _ = assign_experiment_variant(conn, &exp_id, &sequence_instance_id);
        }

        let evidence_ids = thesis_id
            .as_deref()
            .and_then(|id| {
                conn.query_row(
                    "SELECT evidence_ids FROM account_theses WHERE id = ?1",
                    params![id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .ok()
                .flatten()
            })
            .unwrap_or_else(|| "[]".to_string());
        let risk_flags = if channel == "linkedin_assist" {
            serde_json::json!(["manual_action"]).to_string()
        } else {
            serde_json::json!([]).to_string()
        };
        conn.execute(
            "INSERT INTO touches
             (id, sequence_instance_id, step, channel, message_payload, claims_json, evidence_ids, variant_id, risk_flags, sent_at, mailbox_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'v1', ?8, NULL, NULL, ?9)
             ON CONFLICT(id) DO UPDATE SET
                message_payload = excluded.message_payload,
                claims_json = excluded.claims_json,
                evidence_ids = excluded.evidence_ids,
                risk_flags = excluded.risk_flags",
            params![
                approval_id,
                sequence_instance_id,
                initial_step,
                channel,
                payload_json,
                serde_json::to_string(&lead.reasons)
                    .map_err(|e| format!("Failed to encode touch claims: {e}"))?,
                evidence_ids,
                risk_flags,
                now,
            ],
        )
        .map_err(|e| format!("Failed to ensure touch: {e}"))?;
        Ok(())
    }

    /// Check all active sequence instances and advance/cancel as needed (TASK-30).
    /// Called periodically or after outcome processing.
    fn advance_sequences(&self) -> Result<u32, String> {
        let conn = self.open()?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let mut advanced = 0u32;

        // Fetch active sequences with their template steps
        let mut stmt = conn
            .prepare(
                "SELECT si.id, si.template_id, si.account_id, si.contact_id, si.thesis_id,
                        si.current_step, si.status, st.steps_json,
                        MAX(t.sent_at) as last_touch_sent
                 FROM sequence_instances si
                 JOIN sequence_templates st ON st.id = si.template_id
                 LEFT JOIN touches t ON t.sequence_instance_id = si.id AND t.sent_at IS NOT NULL
                 WHERE si.status = 'active'
                 GROUP BY si.id",
            )
            .map_err(|e| format!("Failed to prepare sequence advancement query: {e}"))?;

        type SeqRow = (
            String,
            String,
            String,
            String,
            Option<String>,
            i32,
            String,
            String,
            Option<String>,
        );
        let sequences: Vec<SeqRow> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            })
            .map_err(|e| format!("Sequence query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        for (
            seq_id,
            _template_id,
            account_id,
            contact_id,
            thesis_id,
            current_step,
            _status,
            steps_json,
            last_sent_at,
        ) in sequences
        {
            let steps: Vec<serde_json::Value> =
                serde_json::from_str(&steps_json).unwrap_or_default();

            // Check if there's a positive outcome that should end the sequence
            let positive_outcome: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM outcomes o
                     JOIN touches t ON t.id = o.touch_id
                     WHERE t.sequence_instance_id = ?1
                     AND o.outcome_type IN ('meeting_booked', 'interested', 'closed_won')",
                    params![seq_id],
                    |r| r.get::<_, i32>(0),
                )
                .unwrap_or(0)
                > 0;

            if positive_outcome {
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'completed', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Check for unsubscribe/hard_bounce → cancel
            let negative_outcome: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM outcomes o
                     JOIN touches t ON t.id = o.touch_id
                     WHERE t.sequence_instance_id = ?1
                     AND o.outcome_type IN ('hard_bounce', 'unsubscribe', 'wrong_person')",
                    params![seq_id],
                    |r| r.get::<_, i32>(0),
                )
                .unwrap_or(0)
                > 0;

            if negative_outcome {
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'cancelled', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Check if enough delay has elapsed for next step
            let total_steps = steps.len() as i32;
            let next_step = current_step + 1;
            if next_step > total_steps {
                // Sequence complete — all steps done
                let _ = conn.execute(
                    "UPDATE sequence_instances SET status = 'completed', updated_at = ?1 WHERE id = ?2",
                    params![now_str, seq_id],
                );
                advanced += 1;
                continue;
            }

            // Parse delay_days for next step
            let delay_days = steps
                .get((next_step - 1) as usize)
                .and_then(|s| s.get("delay_days"))
                .and_then(|d| d.as_i64())
                .unwrap_or(3);

            let ready_to_advance = match &last_sent_at {
                Some(sent) => chrono::DateTime::parse_from_rfc3339(sent)
                    .map(|dt| {
                        now.signed_duration_since(dt.with_timezone(&Utc)).num_days() >= delay_days
                    })
                    .unwrap_or(false),
                None => true, // No touch sent yet, first step is ready
            };

            if ready_to_advance {
                let next_channel = steps
                    .get((next_step - 1) as usize)
                    .and_then(|s| s.get("channel"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("email");

                // Advance the step counter
                let _ = conn.execute(
                    "UPDATE sequence_instances SET current_step = ?1, updated_at = ?2 WHERE id = ?3",
                    params![next_step, now_str, seq_id],
                );

                // Queue approval for next touch
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "step": next_step,
                    "channel": next_channel,
                    "account_id": account_id,
                    "contact_id": contact_id,
                    "thesis_id": thesis_id,
                    "sequence_instance_id": seq_id,
                    "auto_generated": true,
                });

                // Find the lead for this account
                let lead_id: String = conn
                    .query_row(
                        "SELECT l.id FROM leads l
                         JOIN domains d ON d.domain = l.company_domain
                         WHERE d.account_id = ?1
                         LIMIT 1",
                        params![account_id],
                        |r| r.get(0),
                    )
                    .unwrap_or_else(|_| format!("seq_{}", seq_id));

                let _ = conn.execute(
                    "INSERT OR IGNORE INTO approvals (id, lead_id, channel, payload_json, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
                    params![approval_id, lead_id, next_channel, payload.to_string(), now_str],
                );

                advanced += 1;
            }
        }

        Ok(advanced)
    }

    /// Get sequence progress for an account.
    #[allow(dead_code)]
    fn get_sequence_progress(&self, account_id: &str) -> Result<Vec<serde_json::Value>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT si.id, si.current_step, si.status, st.steps_json, si.started_at
                 FROM sequence_instances si
                 JOIN sequence_templates st ON st.id = si.template_id
                 WHERE si.account_id = ?1
                 ORDER BY si.started_at DESC",
            )
            .map_err(|e| format!("Failed to query sequence progress: {e}"))?;

        let rows: Vec<serde_json::Value> = stmt
            .query_map(params![account_id], |row| {
                let seq_id: String = row.get(0)?;
                let current_step: i32 = row.get(1)?;
                let status: String = row.get(2)?;
                let steps_json: String = row.get(3)?;
                let started_at: String = row.get(4)?;
                let steps: Vec<serde_json::Value> =
                    serde_json::from_str(&steps_json).unwrap_or_default();
                Ok(serde_json::json!({
                    "sequence_id": seq_id,
                    "current_step": current_step,
                    "total_steps": steps.len(),
                    "status": status,
                    "started_at": started_at,
                    "steps": steps,
                }))
            })
            .map_err(|e| format!("Sequence progress query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    fn insert_lead(&self, lead: &SalesLead) -> Result<bool, String> {
        let conn = self.open()?;
        let reasons_json = serde_json::to_string(&lead.reasons)
            .map_err(|e| format!("Failed to encode reasons: {e}"))?;

        match conn.execute(
            "INSERT INTO leads (id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                lead.id,
                lead.run_id,
                lead.company,
                lead.website,
                lead.company_domain,
                lead.contact_name,
                lead.contact_title,
                lead.linkedin_url,
                lead.email,
                lead.phone,
                reasons_json,
                lead.email_subject,
                lead.email_body,
                lead.linkedin_message,
                lead.score,
                lead.status,
                lead.created_at,
            ],
        ) {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("Lead insert failed: {e}")),
        }
    }

    fn queue_approvals_for_lead(&self, lead: &SalesLead) -> Result<u32, String> {
        let conn = self.open()?;
        let created_at = Utc::now().to_rfc3339();
        let mut queued = 0u32;

        if let Some(email) = &lead.email {
            if !self.approval_already_pending(&conn, "email", email)? {
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "to": email,
                    "subject": lead.email_subject,
                    "body": lead.email_body,
                    "classification": classify_email(email, &lead.company_domain),
                });
                conn.execute(
                    "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'email', ?, 'pending', ?)",
                    params![approval_id, lead.id, payload.to_string(), created_at],
                )
                .map_err(|e| format!("Queue email approval failed: {e}"))?;
                self.ensure_touch_for_approval(
                    &conn,
                    lead,
                    &approval_id,
                    "email",
                    &payload.to_string(),
                )?;
                queued += 1;
            }
        }

        if let Some(linkedin_url) = lead
            .linkedin_url
            .as_deref()
            .and_then(normalize_outreach_linkedin_url)
        {
            if !self.approval_already_pending(&conn, "linkedin_assist", &linkedin_url)? {
                let approval_id = uuid::Uuid::new_v4().to_string();
                let payload = serde_json::json!({
                    "profile_url": linkedin_url,
                    "message": lead.linkedin_message,
                    "manual_action": true,
                });
                conn.execute(
                    "INSERT INTO approvals (id, lead_id, channel, payload_json, status, created_at) VALUES (?, ?, 'linkedin_assist', ?, 'pending', ?)",
                    params![approval_id, lead.id, payload.to_string(), created_at],
                )
                .map_err(|e| format!("Queue LinkedIn approval failed: {e}"))?;
                self.ensure_touch_for_approval(
                    &conn,
                    lead,
                    &approval_id,
                    "linkedin_assist",
                    &payload.to_string(),
                )?;
                queued += 1;
            }
        }

        Ok(queued)
    }

    pub fn list_runs(
        &self,
        segment: SalesSegment,
        limit: usize,
    ) -> Result<Vec<SalesRunRecord>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, status, started_at, completed_at, discovered, inserted, approvals_queued, error
                 FROM sales_runs
                 WHERE segment = ?1
                 ORDER BY started_at DESC
                 LIMIT ?2",
            )
            .map_err(|e| format!("Prepare list runs failed: {e}"))?;

        let mut rows = stmt
            .query(params![segment.as_str(), limit as i64])
            .map_err(|e| format!("List runs query failed: {e}"))?;

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("List runs row failed: {e}"))?
        {
            let run_id = r.get::<_, String>(0).unwrap_or_default();
            let status = r.get::<_, String>(1).unwrap_or_default();
            let mut discovered = r.get::<_, i64>(4).unwrap_or(0) as u32;
            let mut inserted = r.get::<_, i64>(5).unwrap_or(0) as u32;
            let mut approvals_queued = r.get::<_, i64>(6).unwrap_or(0) as u32;

            if status == "running" {
                inserted = self.count_leads_for_run(&run_id).unwrap_or(inserted);
                let profiled = self
                    .count_prospect_profiles_for_run(segment, &run_id)
                    .unwrap_or(discovered);
                approvals_queued = self
                    .count_approvals_for_run(&run_id)
                    .unwrap_or(approvals_queued);
                discovered = inserted.max(profiled);
            }

            out.push(SalesRunRecord {
                id: run_id,
                status,
                started_at: r.get(2).unwrap_or_default(),
                completed_at: r.get(3).ok(),
                discovered,
                inserted,
                approvals_queued,
                error: r.get(7).ok(),
            });
        }

        Ok(out)
    }

    pub fn list_leads(&self, limit: usize, run_id: Option<&str>) -> Result<Vec<SalesLead>, String> {
        let conn = self.open()?;
        let sql_with_run = "SELECT id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at
                 FROM leads WHERE run_id = ? ORDER BY created_at DESC LIMIT ?";
        let sql_all = "SELECT id, run_id, company, website, company_domain, contact_name, contact_title, linkedin_url, email, phone, reasons_json, email_subject, email_body, linkedin_message, score, status, created_at
                 FROM leads ORDER BY created_at DESC LIMIT ?";

        let mut stmt = conn
            .prepare(if run_id.is_some() {
                sql_with_run
            } else {
                sql_all
            })
            .map_err(|e| format!("Prepare list leads failed: {e}"))?;

        let mut rows = if let Some(rid) = run_id {
            stmt.query(params![rid, limit as i64])
                .map_err(|e| format!("List leads query failed: {e}"))?
        } else {
            stmt.query(params![limit as i64])
                .map_err(|e| format!("List leads query failed: {e}"))?
        };

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("List leads row read failed: {e}"))?
        {
            let reasons_json: String = r.get(10).unwrap_or_else(|_| "[]".to_string());
            let reasons = serde_json::from_str::<Vec<String>>(&reasons_json).unwrap_or_default();
            out.push(SalesLead {
                id: r.get(0).unwrap_or_default(),
                run_id: r.get(1).unwrap_or_default(),
                company: r.get(2).unwrap_or_default(),
                website: r.get(3).unwrap_or_default(),
                company_domain: r.get(4).unwrap_or_default(),
                contact_name: r.get(5).unwrap_or_default(),
                contact_title: r.get(6).unwrap_or_default(),
                linkedin_url: r.get(7).ok(),
                email: r.get(8).ok(),
                phone: r.get(9).ok(),
                reasons,
                email_subject: r.get(11).unwrap_or_default(),
                email_body: r.get(12).unwrap_or_default(),
                linkedin_message: r.get(13).unwrap_or_default(),
                score: r.get::<_, i64>(14).unwrap_or(0) as i32,
                status: r.get(15).unwrap_or_default(),
                created_at: r.get(16).unwrap_or_default(),
            });
        }

        Ok(out)
    }

    pub fn list_prospect_profiles(
        &self,
        segment: SalesSegment,
        limit: usize,
        run_id: Option<&str>,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let sales_profile = self.get_profile(segment).ok().flatten();
        let mut cached = self.list_stored_prospect_profiles(segment, limit, run_id)?;
        if !cached.is_empty() {
            sort_prospect_profiles_for_harness(&mut cached, sales_profile.as_ref());
            cached.truncate(limit);
            return Ok(cached);
        }

        let scan_limit = limit.saturating_mul(12).clamp(200, 4000);
        let leads = self.list_leads(scan_limit, run_id)?;
        Ok(build_prospect_profiles(
            leads,
            limit,
            sales_profile.as_ref(),
        ))
    }

    fn list_stored_prospect_profiles(
        &self,
        segment: SalesSegment,
        limit: usize,
        run_id: Option<&str>,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let conn = self.open()?;
        let sql = if run_id.is_some() {
            "SELECT json FROM prospect_profiles WHERE segment = ?1 AND run_id = ?2 ORDER BY updated_at DESC LIMIT ?3"
        } else {
            "SELECT json FROM prospect_profiles WHERE segment = ?1 ORDER BY updated_at DESC LIMIT ?2"
        };
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("Prepare prospect_profiles list failed: {e}"))?;
        let mut out = Vec::new();
        if let Some(run_id) = run_id {
            let rows = stmt
                .query_map(params![segment.as_str(), run_id, limit as i64], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|e| format!("Query prospect_profiles by run failed: {e}"))?;
            for row in rows {
                let raw = row.map_err(|e| format!("Read prospect_profiles row failed: {e}"))?;
                if let Ok(mut profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
                    sanitize_prospect_profile(&mut profile);
                    out.push(profile);
                }
            }
        } else {
            let rows = stmt
                .query_map(params![segment.as_str(), limit as i64], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|e| format!("Query prospect_profiles failed: {e}"))?;
            for row in rows {
                let raw = row.map_err(|e| format!("Read prospect_profiles row failed: {e}"))?;
                if let Ok(mut profile) = serde_json::from_str::<SalesProspectProfile>(&raw) {
                    sanitize_prospect_profile(&mut profile);
                    out.push(profile);
                }
            }
        }
        Ok(out)
    }

    fn get_stored_prospect_profile(
        &self,
        segment: SalesSegment,
        company_domain: &str,
    ) -> Result<Option<SalesProspectProfile>, String> {
        let conn = self.open()?;
        let raw = conn
            .query_row(
                "SELECT json FROM prospect_profiles WHERE segment = ?1 AND company_domain = ?2",
                params![segment.as_str(), company_domain],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Prospect profile lookup failed: {e}"))?;
        raw.map(|payload| {
            serde_json::from_str::<SalesProspectProfile>(&payload)
                .map(|mut profile| {
                    sanitize_prospect_profile(&mut profile);
                    profile
                })
                .map_err(|e| format!("Prospect profile JSON decode failed: {e}"))
        })
        .transpose()
    }

    fn upsert_prospect_profiles(
        &self,
        segment: SalesSegment,
        profiles: &[SalesProspectProfile],
    ) -> Result<(), String> {
        if profiles.is_empty() {
            return Ok(());
        }

        let conn = self.open()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("Begin prospect_profiles transaction failed: {e}"))?;

        for profile in profiles {
            let mut sanitized = profile.clone();
            sanitize_prospect_profile(&mut sanitized);
            let payload = serde_json::to_string(&sanitized)
                .map_err(|e| format!("Serialize prospect profile failed: {e}"))?;
            tx.execute(
                "INSERT INTO prospect_profiles (company_domain, run_id, segment, json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(company_domain) DO UPDATE SET
                    run_id = excluded.run_id,
                    segment = excluded.segment,
                    json = excluded.json,
                    updated_at = excluded.updated_at",
                params![
                    sanitized.company_domain,
                    sanitized.run_id,
                    segment.as_str(),
                    payload,
                    sanitized.created_at,
                    sanitized.updated_at
                ],
            )
            .map_err(|e| format!("Upsert prospect profile failed: {e}"))?;
        }

        tx.commit()
            .map_err(|e| format!("Commit prospect_profiles transaction failed: {e}"))?;
        Ok(())
    }

    pub fn list_approvals(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SalesApproval>, String> {
        let conn = self.open()?;
        let (sql, args): (&str, Vec<String>) = if let Some(s) = status {
            (
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at FROM approvals WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                vec![s.to_string(), limit.to_string()],
            )
        } else {
            (
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at FROM approvals ORDER BY created_at DESC LIMIT ?",
                vec![limit.to_string()],
            )
        };

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("Prepare approvals query failed: {e}"))?;

        let mut rows = if args.len() == 2 {
            stmt.query(params![args[0], args[1]])
                .map_err(|e| format!("Approvals query failed: {e}"))?
        } else {
            stmt.query(params![args[0]])
                .map_err(|e| format!("Approvals query failed: {e}"))?
        };

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("Approvals row read failed: {e}"))?
        {
            let channel: String = r.get(2).unwrap_or_default();
            let payload_raw: String = r.get(3).unwrap_or_else(|_| "{}".to_string());
            let payload = serde_json::from_str::<serde_json::Value>(&payload_raw)
                .unwrap_or_else(|_| serde_json::json!({}));
            let Some(payload) = sanitize_approval_payload(&channel, payload) else {
                continue;
            };
            out.push(SalesApproval {
                id: r.get(0).unwrap_or_default(),
                lead_id: r.get(1).unwrap_or_default(),
                channel,
                payload,
                status: r.get(4).unwrap_or_default(),
                created_at: r.get(5).unwrap_or_default(),
                decided_at: r.get(6).ok(),
            });
        }

        Ok(out)
    }

    fn get_approval_by_id(
        &self,
        conn: &Connection,
        approval_id: &str,
    ) -> Result<Option<SalesApproval>, String> {
        let row = conn
            .query_row(
                "SELECT id, lead_id, channel, payload_json, status, created_at, decided_at
                 FROM approvals
                 WHERE id = ?1",
                params![approval_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?;
        let Some((id, lead_id, channel, payload_raw, status, created_at, decided_at)) = row else {
            return Ok(None);
        };
        let payload = serde_json::from_str::<serde_json::Value>(&payload_raw)
            .unwrap_or_else(|_| serde_json::json!({}));
        let payload = sanitize_approval_payload(&channel, payload)
            .ok_or_else(|| "Approval payload is not actionable".to_string())?;
        Ok(Some(SalesApproval {
            id,
            lead_id,
            channel,
            payload,
            status,
            created_at,
            decided_at,
        }))
    }

    fn edit_approval(
        &self,
        approval_id: &str,
        edited_payload: serde_json::Value,
    ) -> Result<SalesApproval, String> {
        let conn = self.open()?;
        let approval = self
            .get_approval_by_id(&conn, approval_id)?
            .ok_or_else(|| "Approval not found".to_string())?;
        if approval.status != "pending" {
            return Err(format!(
                "Approval is not editable (current status: {})",
                approval.status
            ));
        }
        let sanitized = sanitize_approval_payload(&approval.channel, edited_payload)
            .ok_or_else(|| "Edited payload is invalid or non-actionable".to_string())?;
        let payload_json = sanitized.to_string();
        conn.execute(
            "UPDATE approvals SET payload_json = ?2 WHERE id = ?1",
            params![approval_id, payload_json],
        )
        .map_err(|e| format!("Failed to update approval payload: {e}"))?;
        let _ = conn.execute(
            "UPDATE touches SET message_payload = ?2 WHERE id = ?1",
            params![approval_id, payload_json],
        );
        self.get_approval_by_id(&conn, approval_id)?
            .ok_or_else(|| "Approval disappeared after update".to_string())
    }

    fn resolve_account_id(
        &self,
        conn: &Connection,
        account_ref: &str,
    ) -> Result<Option<String>, String> {
        let trimmed = account_ref.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        if let Some(id) = conn
            .query_row(
                "SELECT id FROM accounts WHERE id = ?1",
                params![trimmed],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Account lookup by id failed: {e}"))?
        {
            return Ok(Some(id));
        }
        let lowered = trimmed.to_lowercase();
        if let Some(id) = conn
            .query_row(
                "SELECT account_id FROM domains WHERE domain = ?1",
                params![lowered],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Account lookup by domain failed: {e}"))?
        {
            return Ok(Some(id));
        }
        let derived = stable_sales_id("acct", &[lowered.as_str()]);
        conn.query_row(
            "SELECT id FROM accounts WHERE id = ?1",
            params![derived.clone()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| format!("Account lookup by derived id failed: {e}"))
    }

    fn fallback_dossier_from_prospect(&self, profile: &SalesProspectProfile) -> serde_json::Value {
        let email_classification = profile
            .primary_email
            .as_deref()
            .map(|email| classify_email(email, &profile.company_domain))
            .unwrap_or("unknown");
        let reachability_score: f64 = (if email_classification == "personal" {
            0.35_f64
        } else {
            0.0_f64
        }) + (if profile.primary_linkedin_url.is_some() {
            0.25_f64
        } else {
            0.0_f64
        }) + (if profile.primary_email.is_some() {
            0.1_f64
        } else {
            0.0_f64
        }) + (if profile.primary_contact_name.is_some() {
            0.1_f64
        } else {
            0.0_f64
        }) + (if profile.primary_contact_title.is_some() {
            0.1_f64
        } else {
            0.0_f64
        });
        let score = FiveAxisScore {
            fit_score: (profile.fit_score as f64 / 100.0).clamp(0.0, 1.0),
            intent_score: (0.12 * profile.matched_signals.len() as f64).clamp(0.0, 0.55),
            reachability_score: reachability_score.clamp(0.0, 1.0),
            deliverability_risk: if profile.primary_email.is_some() {
                if email_classification == "personal" {
                    0.18
                } else {
                    0.42
                }
            } else {
                0.35
            },
            compliance_risk: if email_classification == "personal" {
                0.1
            } else if profile.primary_email.is_some() {
                0.3
            } else {
                0.18
            },
        };
        let gate = send_gate(&score);
        let next_action = match &gate {
            SendGateDecision::Activate => {
                if profile.primary_email.is_some() {
                    "Send email now".to_string()
                } else if profile.primary_linkedin_url.is_some() {
                    "Open LinkedIn operator-assist task".to_string()
                } else {
                    "Promote to activation after one more verification pass".to_string()
                }
            }
            SendGateDecision::Research { .. } => "Research needed".to_string(),
            SendGateDecision::Nurture { .. } => {
                "Hold in nurture until a stronger timing signal appears".to_string()
            }
            SendGateDecision::Block { .. } => "Blocked until risk is reduced".to_string(),
        };
        let methods = [
            profile.primary_email.as_ref().map(|email| {
                serde_json::json!({
                    "channel_type": "email",
                    "value": email,
                    "classification": email_classification,
                    "confidence": profile.research_confidence,
                    "suppressed": false,
                })
            }),
            profile.primary_linkedin_url.as_ref().map(|url| {
                serde_json::json!({
                    "channel_type": "linkedin",
                    "value": url,
                    "classification": "personal",
                    "confidence": profile.research_confidence,
                    "suppressed": false,
                })
            }),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        serde_json::json!({
            "account_id": stable_sales_id("acct", &[profile.company_domain.as_str()]),
            "account": {
                "canonical_name": profile.company,
                "display_name": profile.company,
                "website": profile.website,
                "domain": profile.company_domain,
                "tier": if profile.fit_score >= 80 { "a_tier" } else if profile.fit_score >= 55 { "standard" } else { "basic" },
            },
            "score": {
                "fit_score": score.fit_score,
                "intent_score": score.intent_score,
                "reachability_score": score.reachability_score,
                "deliverability_risk": score.deliverability_risk,
                "compliance_risk": score.compliance_risk,
                "activation_priority": activation_priority(&score),
                "send_gate": gate,
            },
            "thesis": {
                "why_this_account": profile.summary,
                "why_now": profile.trigger_events.first().cloned().unwrap_or_else(|| "Awaiting more public evidence.".to_string()),
                "buyer_committee": (profile.buyer_roles.iter().map(|role| serde_json::json!({
                    "role": role,
                    "name": profile.primary_contact_name.clone().unwrap_or_default(),
                    "confidence": profile.research_confidence,
                })).collect::<Vec<_>>()),
                "do_not_say": vec!["Do not claim private knowledge beyond public evidence."],
                "recommended_channel": profile.recommended_channel,
                "recommended_pain": profile.outreach_angle,
                "thesis_confidence": profile.research_confidence,
                "thesis_status": if profile.research_confidence >= 0.6 { "heuristic_ready" } else { "needs_research" },
            },
            "signals": profile.matched_signals.iter().map(|signal| serde_json::json!({
                "signal_type": infer_signal_type(signal),
                "text": signal,
                "effect_horizon": classify_signal_horizon(infer_signal_type(signal), signal).0,
                "confidence": profile.research_confidence,
                "why_it_matters": generate_signal_rationale(infer_signal_type(signal), signal),
            })).collect::<Vec<_>>(),
            "contacts": [{
                "full_name": profile.primary_contact_name,
                "title": profile.primary_contact_title,
                "is_decision_maker": profile.primary_contact_title.as_deref().map(contact_title_priority).unwrap_or(0) > 0,
                "methods": methods,
            }],
            "outcomes": {
                "touches_sent": 0,
                "positive_replies": 0,
                "meetings": 0,
                "opens": 0,
                "clicks": 0,
                "hard_bounces": 0,
                "unsubscribes": 0,
                "positive_reply_rate": 0.0,
                "meeting_rate": 0.0,
            },
            "next_action": next_action,
            "source": "prospect_profile_fallback",
        })
    }

    fn get_account_dossier(&self, account_ref: &str) -> Result<Option<serde_json::Value>, String> {
        let conn = self.open()?;
        let Some(account_id) = self.resolve_account_id(&conn, account_ref)? else {
            return Ok(self
                .get_stored_prospect_profile(SalesSegment::B2B, account_ref)?
                .map(|profile| self.fallback_dossier_from_prospect(&profile)));
        };

        let account = conn
            .query_row(
                "SELECT canonical_name, COALESCE(display_name, canonical_name), COALESCE(website, ''), COALESCE(sector, ''),
                        COALESCE(geo, ''), COALESCE(tier, 'standard')
                 FROM accounts
                 WHERE id = ?1",
                params![account_id.clone()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load account dossier: {e}"))?;
        let Some(account) = account else {
            return Ok(self
                .get_stored_prospect_profile(SalesSegment::B2B, account_ref)?
                .map(|profile| self.fallback_dossier_from_prospect(&profile)));
        };

        let domain = conn
            .query_row(
                "SELECT domain FROM domains WHERE account_id = ?1 ORDER BY is_primary DESC, checked_at DESC LIMIT 1",
                params![account_id.clone()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load account domain: {e}"))?
            .unwrap_or_default();

        let score = conn
            .query_row(
                "SELECT fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk
                 FROM score_snapshots
                 WHERE account_id = ?1
                 ORDER BY computed_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(FiveAxisScore {
                        fit_score: row.get::<_, f64>(0).unwrap_or(0.0),
                        intent_score: row.get::<_, f64>(1).unwrap_or(0.0),
                        reachability_score: row.get::<_, f64>(2).unwrap_or(0.0),
                        deliverability_risk: row.get::<_, f64>(3).unwrap_or(0.0),
                        compliance_risk: row.get::<_, f64>(4).unwrap_or(0.0),
                    })
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load dossier score: {e}"))?
            .unwrap_or(compute_five_axis_score(&account_id, &conn)?);
        let gate = send_gate(&score);

        let thesis = conn
            .query_row(
                "SELECT why_this_account, COALESCE(why_now, ''), COALESCE(buyer_committee_json, '[]'),
                        COALESCE(do_not_say, '[]'), COALESCE(recommended_channel, ''),
                        COALESCE(recommended_pain, ''), thesis_confidence, thesis_status
                 FROM account_theses
                 WHERE account_id = ?1
                 ORDER BY created_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(serde_json::json!({
                        "why_this_account": row.get::<_, String>(0)?,
                        "why_now": row.get::<_, String>(1)?,
                        "buyer_committee": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(2)?).unwrap_or_else(|_| serde_json::json!([])),
                        "do_not_say": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?).unwrap_or_else(|_| serde_json::json!([])),
                        "recommended_channel": row.get::<_, String>(4)?,
                        "recommended_pain": row.get::<_, String>(5)?,
                        "thesis_confidence": row.get::<_, f64>(6).unwrap_or(0.0),
                        "thesis_status": row.get::<_, String>(7)?,
                    }))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load account thesis: {e}"))?
            .unwrap_or_else(|| serde_json::json!({
                "why_this_account": account.0.clone(),
                "why_now": "Thesis not generated yet.",
                "buyer_committee": [],
                "do_not_say": [],
                "recommended_channel": "",
                "recommended_pain": "",
                "thesis_confidence": 0.0,
                "thesis_status": "missing",
            }));

        let mut signal_stmt = conn
            .prepare(
                "SELECT s.id, s.signal_type, s.text, COALESCE(s.effect_horizon, ''), s.confidence,
                        COALESCE(sr.why_it_matters, ''), COALESCE(sr.expected_effect, ''), COALESCE(sr.evidence_ids, '[]')
                 FROM signals s
                 LEFT JOIN signal_rationales sr ON sr.signal_id = s.id
                 WHERE s.account_id = ?1
                 ORDER BY s.confidence DESC, COALESCE(s.observed_at, s.created_at) DESC
                 LIMIT 6",
            )
            .map_err(|e| format!("Failed to prepare dossier signals query: {e}"))?;
        let signals = signal_stmt
            .query_map(params![account_id.clone()], |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "signal_type": row.get::<_, String>(1)?,
                    "text": row.get::<_, String>(2)?,
                    "effect_horizon": row.get::<_, String>(3)?,
                    "confidence": row.get::<_, f64>(4).unwrap_or(0.0),
                    "why_it_matters": row.get::<_, String>(5)?,
                    "expected_effect": row.get::<_, String>(6)?,
                    "evidence_ids": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(7)?).unwrap_or_else(|_| serde_json::json!([])),
                }))
            })
            .map_err(|e| format!("Failed to query dossier signals: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode dossier signals: {e}"))?;

        let mut contact_stmt = conn
            .prepare(
                "SELECT id, COALESCE(full_name, ''), COALESCE(title, ''), COALESCE(seniority, ''),
                        name_confidence, title_confidence, COALESCE(is_decision_maker, 0)
                 FROM contacts
                 WHERE account_id = ?1
                 ORDER BY is_decision_maker DESC, name_confidence DESC, created_at ASC
                 LIMIT 8",
            )
            .map_err(|e| format!("Failed to prepare dossier contacts query: {e}"))?;
        let contact_rows = contact_stmt
            .query_map(params![account_id.clone()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, f64>(4).unwrap_or(0.0),
                    row.get::<_, f64>(5).unwrap_or(0.0),
                    row.get::<_, i64>(6).unwrap_or(0),
                ))
            })
            .map_err(|e| format!("Failed to query dossier contacts: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to decode dossier contacts: {e}"))?;
        let mut contacts = Vec::new();
        for (
            contact_id,
            full_name,
            title,
            seniority,
            name_confidence,
            title_confidence,
            is_decision_maker,
        ) in contact_rows
        {
            let mut method_stmt = conn
                .prepare(
                    "SELECT channel_type, value, COALESCE(classification, ''), confidence, verified_at, COALESCE(suppressed, 0)
                     FROM contact_methods
                     WHERE contact_id = ?1
                     ORDER BY confidence DESC, channel_type ASC",
                )
                .map_err(|e| format!("Failed to prepare dossier contact methods query: {e}"))?;
            let methods = method_stmt
                .query_map(params![contact_id.clone()], |row| {
                    Ok(serde_json::json!({
                        "channel_type": row.get::<_, String>(0)?,
                        "value": row.get::<_, String>(1)?,
                        "classification": row.get::<_, String>(2)?,
                        "confidence": row.get::<_, f64>(3).unwrap_or(0.0),
                        "verified_at": row.get::<_, Option<String>>(4)?,
                        "suppressed": row.get::<_, i64>(5).unwrap_or(0) == 1,
                    }))
                })
                .map_err(|e| format!("Failed to query dossier contact methods: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode dossier contact methods: {e}"))?;
            contacts.push(serde_json::json!({
                "id": contact_id,
                "full_name": full_name,
                "title": title,
                "seniority": seniority,
                "name_confidence": name_confidence,
                "title_confidence": title_confidence,
                "is_decision_maker": is_decision_maker == 1,
                "methods": methods,
            }));
        }

        let (touches_sent, positive_replies, meetings, opens, clicks, hard_bounces, unsubscribes) = conn
            .query_row(
                "SELECT
                    COUNT(DISTINCT CASE WHEN t.sent_at IS NOT NULL THEN t.id END),
                    SUM(CASE WHEN o.outcome_type IN ('interested', 'meeting_booked') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'meeting_booked' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'open' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'click' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'hard_bounce' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.outcome_type = 'unsubscribe' THEN 1 ELSE 0 END)
                 FROM sequence_instances si
                 LEFT JOIN touches t ON t.sequence_instance_id = si.id
                 LEFT JOIN outcomes o ON o.touch_id = t.id
                 WHERE si.account_id = ?1",
                params![account_id.clone()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0).unwrap_or(0),
                        row.get::<_, i64>(1).unwrap_or(0),
                        row.get::<_, i64>(2).unwrap_or(0),
                        row.get::<_, i64>(3).unwrap_or(0),
                        row.get::<_, i64>(4).unwrap_or(0),
                        row.get::<_, i64>(5).unwrap_or(0),
                        row.get::<_, i64>(6).unwrap_or(0),
                    ))
                },
            )
            .map_err(|e| format!("Failed to load dossier outcomes: {e}"))?;
        let sent_denom = if touches_sent <= 0 {
            1.0
        } else {
            touches_sent as f64
        };
        let next_action = match &gate {
            SendGateDecision::Activate => format!(
                "Send {} now",
                thesis
                    .get("recommended_channel")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .unwrap_or("outreach")
            ),
            SendGateDecision::Research { missing } => {
                format!("Research needed: {}", missing.join("; "))
            }
            SendGateDecision::Nurture { reason } => reason.clone(),
            SendGateDecision::Block { reason } => reason.clone(),
        };

        Ok(Some(serde_json::json!({
            "account_id": account_id,
            "account": {
                "canonical_name": account.0,
                "display_name": account.1,
                "website": account.2,
                "sector": account.3,
                "geo": account.4,
                "tier": account.5,
                "domain": domain,
            },
            "score": {
                "fit_score": score.fit_score,
                "intent_score": score.intent_score,
                "reachability_score": score.reachability_score,
                "deliverability_risk": score.deliverability_risk,
                "compliance_risk": score.compliance_risk,
                "activation_priority": activation_priority(&score),
                "send_gate": gate,
            },
            "thesis": thesis,
            "signals": signals,
            "contacts": contacts,
            "outcomes": {
                "touches_sent": touches_sent,
                "positive_replies": positive_replies,
                "meetings": meetings,
                "opens": opens,
                "clicks": clicks,
                "hard_bounces": hard_bounces,
                "unsubscribes": unsubscribes,
                "positive_reply_rate": (positive_replies as f64 / sent_denom).clamp(0.0, 1.0),
                "meeting_rate": (meetings as f64 / sent_denom).clamp(0.0, 1.0),
            },
            "next_action": next_action,
            "source": "canonical_core",
        })))
    }

    pub fn list_deliveries(&self, limit: usize) -> Result<Vec<SalesDelivery>, String> {
        let conn = self.open()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, approval_id, channel, recipient, status, error, sent_at FROM deliveries ORDER BY sent_at DESC LIMIT ?",
            )
            .map_err(|e| format!("Prepare deliveries query failed: {e}"))?;

        let mut rows = stmt
            .query(params![limit as i64])
            .map_err(|e| format!("Deliveries query failed: {e}"))?;

        let mut out = Vec::new();
        while let Some(r) = rows
            .next()
            .map_err(|e| format!("Deliveries row read failed: {e}"))?
        {
            out.push(SalesDelivery {
                id: r.get(0).unwrap_or_default(),
                approval_id: r.get(1).unwrap_or_default(),
                channel: r.get(2).unwrap_or_default(),
                recipient: r.get(3).unwrap_or_default(),
                status: r.get(4).unwrap_or_default(),
                error: r.get(5).ok(),
                sent_at: r.get(6).unwrap_or_default(),
            });
        }

        Ok(out)
    }

    fn deliveries_today(&self, timezone_mode: &str) -> Result<u32, String> {
        let conn = self.open()?;
        let today = current_sales_day(timezone_mode);
        let mut stmt = conn
            .prepare("SELECT sent_at FROM deliveries WHERE status = 'sent'")
            .map_err(|e| format!("Deliveries count prepare failed: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Deliveries count query failed: {e}"))?;

        let mut count = 0u32;
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("Deliveries count row failed: {e}"))?
        {
            let sent_at: String = row.get(0).unwrap_or_default();
            if timestamp_matches_sales_day(&sent_at, today, timezone_mode) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Load sender config from DB sender_policies table if available.
    fn load_sender_config(&self) -> SenderConfig {
        let conn = match self.open() {
            Ok(c) => c,
            Err(_) => return SenderConfig::default(),
        };
        let pool_json: String = conn
            .query_row(
                "SELECT COALESCE(mailbox_pool, '[]') FROM sender_policies ORDER BY rowid DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| "[]".to_string());
        SenderConfig {
            mailboxes: mailbox_pool_from_json(&pool_json),
        }
    }

    fn save_sender_config(&self, sender_cfg: &SenderConfig) -> Result<(), String> {
        let conn = self.open()?;
        let row_id = conn
            .query_row(
                "SELECT id FROM sender_policies ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load sender policy row: {e}"))?
            .unwrap_or_else(|| "default_sender_policy".to_string());
        let total_daily_cap = sender_cfg
            .mailboxes
            .iter()
            .map(|mailbox| mailbox.daily_cap.max(1) as i64)
            .sum::<i64>()
            .max(1);
        let subdomain = sender_cfg
            .mailboxes
            .iter()
            .find_map(|mailbox| email_domain(&mailbox.email))
            .unwrap_or_default();
        let mailbox_pool = serde_json::to_string(&sender_cfg.mailboxes)
            .map_err(|e| format!("Failed to encode sender mailbox pool: {e}"))?;
        conn.execute(
            "INSERT INTO sender_policies (id, icp_id, mailbox_pool, daily_cap, subdomain, warm_state)
             VALUES (?1, NULL, ?2, ?3, ?4, 'warming')
             ON CONFLICT(id) DO UPDATE SET
                mailbox_pool = excluded.mailbox_pool,
                daily_cap = excluded.daily_cap,
                subdomain = CASE
                    WHEN COALESCE(sender_policies.subdomain, '') = '' THEN excluded.subdomain
                    ELSE sender_policies.subdomain
                END",
            params![row_id, mailbox_pool, total_daily_cap, subdomain],
        )
        .map_err(|e| format!("Failed to persist sender policy config: {e}"))?;
        Ok(())
    }

    fn record_mailbox_send(&self, from_email: &str) -> Result<(), String> {
        let normalized_email = normalize_mailbox_address(from_email);
        if normalized_email.is_empty() {
            return Ok(());
        }

        let mut sender_cfg = self.load_sender_config();
        if sender_cfg.mailboxes.is_empty() {
            return Ok(());
        }

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let mut changed = false;
        for mailbox in &mut sender_cfg.mailboxes {
            if mailbox.counter_date != today {
                mailbox.sends_today = 0;
                mailbox.counter_date = today.clone();
                changed = true;
            }
            if normalize_mailbox_address(&mailbox.email) == normalized_email {
                mailbox.sends_today = mailbox.sends_today.saturating_add(1);
                mailbox.counter_date = today.clone();
                changed = true;
                break;
            }
        }

        if changed {
            self.save_sender_config(&sender_cfg)?;
        }
        Ok(())
    }

    async fn send_email(
        &self,
        state: &AppState,
        profile: &SalesProfile,
        to: &str,
        subject: &str,
        body: &str,
    ) -> Result<(), String> {
        // Try mailbox pool first (TASK-13), fall back to global config
        let mut sender_cfg = self.load_sender_config();
        let selected_mailbox = sender_cfg.select_mailbox().cloned();
        let (smtp_host, smtp_port, smtp_user, smtp_pass, from_email, used_mailbox_pool) =
            if let Some(mb) = selected_mailbox {
                let pass_env = if mb.smtp_pass_env.is_empty() {
                    // Fall through to global config
                    None
                } else {
                    std::env::var(&mb.smtp_pass_env).ok()
                };
                if let Some(pass) = pass_env {
                    let host = mb.smtp_host.clone();
                    let port = mb.smtp_port;
                    let user = if mb.smtp_user.is_empty() {
                        mb.email.clone()
                    } else {
                        mb.smtp_user.clone()
                    };
                    let email = mb.email.clone();
                    (host, port, user, pass, email, true)
                } else {
                    let (host, port, user, pass, email) =
                        self.resolve_global_email_config(state).await?;
                    (host, port, user, pass, email, false)
                }
            } else {
                let (host, port, user, pass, email) =
                    self.resolve_global_email_config(state).await?;
                (host, port, user, pass, email, false)
            };

        let from: Mailbox = from_email
            .parse()
            .map_err(|e| format!("Invalid sender email '{}': {e}", from_email))?;
        let recipient_email = to.trim().to_string();
        let to: Mailbox = to
            .parse()
            .map_err(|e| format!("Invalid recipient email '{to}': {e}"))?;

        let sender_domain = email_domain(&from_email)
            .ok_or_else(|| "Configured sender mailbox is invalid".to_string())?;
        let brand_domain = email_domain(&profile.sender_email)
            .ok_or_else(|| "Sales profile sender_email is invalid".to_string())?;
        if !is_valid_sending_subdomain(&sender_domain, &brand_domain) {
            return Err(format!(
                "Refusing to send from main domain '{}'; configure a sending subdomain for '{}'",
                sender_domain, brand_domain
            ));
        }

        let unsubscribe_url = format!(
            "{}/api/sales/unsubscribe?token={}",
            sales_base_url(&state.kernel),
            generate_unsubscribe_token(&recipient_email, &from_email)
        );

        let msg = Message::builder()
            .from(from)
            .to(to)
            .subject(subject)
            .header(ListUnsubscribeHeader(format!("<{}>", unsubscribe_url)))
            .header(ListUnsubscribePostHeader(
                "List-Unsubscribe=One-Click".to_string(),
            ))
            .body(body.to_string())
            .map_err(|e| format!("Failed to build email message: {e}"))?;

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp_host)
            .map_err(|e| format!("Failed to initialize SMTP relay '{}': {e}", smtp_host))?
            .port(smtp_port)
            .credentials(Credentials::new(smtp_user, smtp_pass))
            .build();

        transport
            .send(msg)
            .await
            .map_err(|e| format!("SMTP send failed: {e}"))?;

        if used_mailbox_pool {
            self.record_mailbox_send(&from_email)?;
        }

        Ok(())
    }

    /// Resolve SMTP config from global email channel configuration.
    async fn resolve_global_email_config(
        &self,
        state: &AppState,
    ) -> Result<(String, u16, String, String, String), String> {
        let channels = state.channels_config.read().await;
        let cfg = channels
            .email
            .as_ref()
            .ok_or_else(|| "Email channel is not configured".to_string())?;
        let password = std::env::var(&cfg.password_env)
            .map_err(|_| format!("Email password env '{}' is not set", cfg.password_env))?;
        Ok((
            cfg.smtp_host.clone(),
            cfg.smtp_port,
            cfg.username.clone(),
            password,
            cfg.username.clone(),
        ))
    }

    async fn send_linkedin(
        &self,
        approval_id: &str,
        profile_url: &str,
        _message: &str,
    ) -> Result<(), String> {
        self.record_delivery(
            approval_id,
            "linkedin_assist",
            profile_url,
            "operator_pending",
            None,
        )
    }

    fn record_delivery(
        &self,
        approval_id: &str,
        channel: &str,
        recipient: &str,
        status: &str,
        error_msg: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let sent_at = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO deliveries (id, approval_id, channel, recipient, status, error, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                uuid::Uuid::new_v4().to_string(),
                approval_id,
                channel,
                recipient,
                status,
                error_msg,
                sent_at,
            ],
        )
        .map_err(|e| format!("Failed to record delivery: {e}"))?;
        let _ = conn.execute(
            "UPDATE touches SET sent_at = COALESCE(sent_at, ?2) WHERE id = ?1",
            params![approval_id, sent_at],
        );
        let _ = conn.execute(
            "UPDATE sequence_instances
             SET updated_at = ?2
             WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
            params![approval_id, Utc::now().to_rfc3339()],
        );
        Ok(())
    }

    fn update_approval_status(&self, approval_id: &str, status: &str) -> Result<(), String> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE approvals SET status = ?, decided_at = ? WHERE id = ?",
            params![status, Utc::now().to_rfc3339(), approval_id],
        )
        .map_err(|e| format!("Failed to update approval status: {e}"))?;
        Ok(())
    }

    fn ingest_outcome_event(
        &self,
        delivery_id: &str,
        event_type: &str,
        raw_text: &str,
    ) -> Result<serde_json::Value, String> {
        let conn = self.open()?;
        let delivery = conn
            .query_row(
                "SELECT approval_id, channel, recipient FROM deliveries WHERE id = ?1",
                params![delivery_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to look up delivery: {e}"))?
            .ok_or_else(|| "Delivery not found".to_string())?;

        let approval = conn
            .query_row(
                "SELECT a.id, a.channel, a.payload_json, l.id, l.run_id, l.company, l.website, l.company_domain,
                        l.contact_name, l.contact_title, l.linkedin_url, l.email, l.phone, l.reasons_json,
                        l.email_subject, l.email_body, l.linkedin_message, l.score, l.status, l.created_at
                 FROM approvals a
                 JOIN leads l ON l.id = a.lead_id
                 WHERE a.id = ?1",
                params![delivery.0.clone()],
                |row| {
                    let reasons_raw = row.get::<_, String>(13)?;
                    let reasons = serde_json::from_str::<Vec<String>>(&reasons_raw).unwrap_or_default();
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        SalesLead {
                            id: row.get::<_, String>(3)?,
                            run_id: row.get::<_, String>(4)?,
                            company: row.get::<_, String>(5)?,
                            website: row.get::<_, String>(6)?,
                            company_domain: row.get::<_, String>(7)?,
                            contact_name: row.get::<_, String>(8)?,
                            contact_title: row.get::<_, String>(9)?,
                            linkedin_url: row.get::<_, Option<String>>(10)?,
                            email: row.get::<_, Option<String>>(11)?,
                            phone: row.get::<_, Option<String>>(12)?,
                            reasons,
                            email_subject: row.get::<_, String>(14)?,
                            email_body: row.get::<_, String>(15)?,
                            linkedin_message: row.get::<_, String>(16)?,
                            score: row.get::<_, i64>(17)? as i32,
                            status: row.get::<_, String>(18)?,
                            created_at: row.get::<_, String>(19)?,
                        },
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load approval/lead for outcome: {e}"))?
            .ok_or_else(|| "Approval/lead not found for delivery".to_string())?;

        self.ensure_touch_for_approval(&conn, &approval.3, &approval.0, &approval.1, &approval.2)?;

        let outcome = classify_outcome(raw_text, event_type, &approval.0);
        let outcome_id =
            stable_sales_id("outcome", &[delivery_id, event_type, &outcome.outcome_type]);
        conn.execute(
            "INSERT INTO outcomes (id, touch_id, outcome_type, raw_text, classified_at, classifier_confidence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
                raw_text = excluded.raw_text,
                classified_at = excluded.classified_at,
                classifier_confidence = excluded.classifier_confidence",
            params![
                outcome_id,
                outcome.touch_id,
                outcome.outcome_type,
                outcome.raw_text,
                Utc::now().to_rfc3339(),
                outcome.classifier_confidence,
            ],
        )
        .map_err(|e| format!("Failed to persist outcome: {e}"))?;

        let account_id = stable_sales_id("acct", &[&approval.3.company_domain]);
        let snapshot_id = stable_sales_id("outcome_snapshot", &[delivery_id, event_type]);
        let score_at_touch_json = conn
            .query_row(
                "SELECT fit_score, intent_score, reachability_score, deliverability_risk, compliance_risk
                 FROM score_snapshots
                 WHERE account_id = ?1
                 ORDER BY computed_at DESC
                 LIMIT 1",
                params![account_id.clone()],
                |row| {
                    Ok(serde_json::json!({
                        "fit_score": row.get::<_, f64>(0).unwrap_or(0.0),
                        "intent_score": row.get::<_, f64>(1).unwrap_or(0.0),
                        "reachability_score": row.get::<_, f64>(2).unwrap_or(0.0),
                        "deliverability_risk": row.get::<_, f64>(3).unwrap_or(0.0),
                        "compliance_risk": row.get::<_, f64>(4).unwrap_or(0.0),
                    }))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to load score snapshot for outcome: {e}"))?
            .unwrap_or_else(|| serde_json::json!({}));
        let active_signal_ids = {
            let mut stmt = conn
                .prepare(
                    "SELECT id FROM signals WHERE account_id = ?1 ORDER BY confidence DESC LIMIT 8",
                )
                .map_err(|e| format!("Failed to prepare signal snapshot query: {e}"))?;
            let rows = stmt
                .query_map(params![account_id.clone()], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Failed to query signal snapshot ids: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Failed to decode signal snapshot ids: {e}"))?;
            rows
        };
        let thesis_id = conn
            .query_row(
                "SELECT thesis_id FROM sequence_instances
                 WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                params![approval.0.clone()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|e| format!("Failed to load thesis id for outcome snapshot: {e}"))?
            .flatten();
        conn.execute(
            "INSERT INTO outcome_attribution_snapshots
             (id, touch_id, account_id, snapshot_at, score_at_touch_json, active_signal_ids, unused_signal_ids,
              thesis_id, sequence_variant, message_variant, channel, mailbox_id, contextual_factors_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, '[]', ?7, 'default', 'v1', ?8, NULL, ?9)
             ON CONFLICT(id) DO UPDATE SET
                score_at_touch_json = excluded.score_at_touch_json,
                active_signal_ids = excluded.active_signal_ids,
                thesis_id = excluded.thesis_id,
                channel = excluded.channel,
                contextual_factors_json = excluded.contextual_factors_json",
            params![
                snapshot_id,
                approval.0.clone(),
                account_id.clone(),
                Utc::now().to_rfc3339(),
                score_at_touch_json.to_string(),
                serde_json::to_string(&active_signal_ids)
                    .map_err(|e| format!("Failed to encode active signal ids: {e}"))?,
                thesis_id,
                delivery.1,
                serde_json::json!({
                    "delivery_id": delivery_id,
                    "recipient": delivery.2,
                    "event_type": event_type,
                })
                .to_string(),
            ],
        )
        .map_err(|e| format!("Failed to persist outcome attribution snapshot: {e}"))?;
        self.record_missed_signal_review(
            &conn,
            &outcome_id,
            &snapshot_id,
            &account_id,
            &outcome.outcome_type,
            &active_signal_ids,
            &Vec::new(),
        )?;

        match outcome.outcome_type.as_str() {
            "hard_bounce" => {
                self.suppress_contact(&conn, &delivery.2, "hard_bounce", false, Some(&outcome_id))?;
            }
            "unsubscribe" => {
                self.suppress_contact(&conn, &delivery.2, "unsubscribe", true, Some(&outcome_id))?;
            }
            "wrong_person" => {
                self.enqueue_research(
                    &conn,
                    &account_id,
                    "Wrong person outcome; find alternate contact",
                    95,
                )?;
            }
            "meeting_booked" => {
                let _ = conn.execute(
                    "UPDATE sequence_instances
                     SET status = 'completed', updated_at = ?2
                     WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                    params![approval.0.clone(), Utc::now().to_rfc3339()],
                );
            }
            "interested" | "open" | "click" | "not_now" => {
                let _ = conn.execute(
                    "UPDATE sequence_instances
                     SET status = 'active', updated_at = ?2
                     WHERE id = (SELECT sequence_instance_id FROM touches WHERE id = ?1)",
                    params![approval.0.clone(), Utc::now().to_rfc3339()],
                );
            }
            _ => {}
        }
        if matches!(outcome.outcome_type.as_str(), "hard_bounce" | "unsubscribe") {
            conn.execute(
                "UPDATE contact_methods SET suppressed = 1 WHERE value = ?1",
                params![delivery.2.trim().to_lowercase()],
            )
            .map_err(|e| format!("Failed to update suppressed contact method: {e}"))?;
        }

        // Trigger score calibration when enough outcomes accumulate (TASK-36)
        if let Ok(proposals) = calibrate_scoring_from_outcomes(&conn) {
            for proposal in &proposals {
                info!(proposal = %proposal, "Score calibration proposal created");
            }
        }

        Ok(serde_json::json!({
            "delivery_id": delivery_id,
            "touch_id": approval.0,
            "account_id": account_id,
            "outcome_type": outcome.outcome_type,
            "recipient": delivery.2,
        }))
    }

    pub async fn approve_and_send(
        &self,
        state: &AppState,
        approval_id: &str,
    ) -> Result<serde_json::Value, String> {
        let row = {
            let conn = self.open()?;
            conn.query_row(
                "SELECT id, channel, payload_json, status FROM approvals WHERE id = ?",
                params![approval_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?
        };

        let (id, channel, payload_raw, status) =
            row.ok_or_else(|| "Approval not found".to_string())?;
        if status != "pending" {
            return Err(format!(
                "Approval is not pending (current status: {status})"
            ));
        }

        let profile = self
            .get_profile(SalesSegment::B2B)?
            .ok_or_else(|| "Sales profile is not configured".to_string())?;

        let sent_today = self.deliveries_today(&profile.timezone_mode)?;
        if sent_today >= profile.daily_send_cap {
            return Err(format!(
                "Daily send cap reached ({}/{})",
                sent_today, profile.daily_send_cap
            ));
        }

        let payload: serde_json::Value = serde_json::from_str(&payload_raw)
            .map_err(|e| format!("Invalid approval payload JSON: {e}"))?;

        let result = match channel.as_str() {
            "email" => {
                let to = payload
                    .get("to")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.to".to_string())?;
                let subject = payload
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.subject".to_string())?;
                let body = payload
                    .get("body")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.body".to_string())?;
                let (suppressed, bounce_count) = {
                    let conn = self.open()?;
                    let suppressed = self.is_suppressed(&conn, to)?;
                    let domain = email_domain(to).unwrap_or_default();
                    let bounce_count = conn
                        .query_row(
                            "SELECT COUNT(*) FROM suppressions
                             WHERE contact_method_value LIKE ?1 AND reason = 'hard_bounce'",
                            params![format!("%@{domain}")],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap_or(0);
                    (suppressed, bounce_count)
                };
                if suppressed {
                    self.update_approval_status(&id, "blocked_suppressed")?;
                    let _ = self.record_delivery(&id, "email", to, "blocked_suppressed", None);
                    return Err("Recipient is suppressed".to_string());
                }
                let validation = validate_email_for_sending(to, suppressed, bounce_count).await?;
                if !validation.safe_to_send {
                    self.update_approval_status(&id, "blocked_validation")?;
                    let _ = self.record_delivery(
                        &id,
                        "email",
                        to,
                        "blocked_validation",
                        Some(&format!(
                            "syntax_valid={} mx_valid={} classification={} domain_health={:.2}",
                            validation.syntax_valid,
                            validation.mx_valid,
                            validation.classification,
                            validation.domain_health
                        )),
                    );
                    return Err(format!(
                        "Email failed pre-send validation (classification={}, mx_valid={})",
                        validation.classification, validation.mx_valid
                    ));
                }
                if let Err(send_err) = self.send_email(state, &profile, to, subject, body).await {
                    if let Err(record_err) =
                        self.record_delivery(&id, "email", to, "failed", Some(&send_err))
                    {
                        warn!(
                            approval_id = %id,
                            error = %record_err,
                            "Failed to record email delivery failure"
                        );
                    }
                    return Err(send_err);
                }
                self.update_approval_status(&id, "approved")?;
                if let Err(record_err) = self.record_delivery(&id, "email", to, "sent", None) {
                    warn!(
                        approval_id = %id,
                        error = %record_err,
                        "Failed to record email delivery after successful send"
                    );
                }
                serde_json::json!({"channel": "email", "recipient": to, "status": "sent"})
            }
            "linkedin" | "linkedin_assist" => {
                let profile_url = payload
                    .get("profile_url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.profile_url".to_string())?;
                let message = payload
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing payload.message".to_string())?;
                if let Err(send_err) = self.send_linkedin(&id, profile_url, message).await {
                    if let Err(record_err) = self.record_delivery(
                        &id,
                        "linkedin_assist",
                        profile_url,
                        "failed",
                        Some(&send_err),
                    ) {
                        warn!(
                            approval_id = %id,
                            error = %record_err,
                            "Failed to record LinkedIn delivery failure"
                        );
                    }
                    return Err(send_err);
                }
                self.update_approval_status(&id, "approved")?;
                serde_json::json!({"channel": "linkedin_assist", "recipient": profile_url, "status": "operator_pending"})
            }
            other => return Err(format!("Unsupported channel: {other}")),
        };
        Ok(result)
    }

    pub fn reject_approval(&self, approval_id: &str) -> Result<(), String> {
        let conn = self.open()?;
        let status = conn
            .query_row(
                "SELECT status FROM approvals WHERE id = ?",
                params![approval_id],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| format!("Approval lookup failed: {e}"))?
            .ok_or_else(|| "Approval not found".to_string())?;
        if status != "pending" {
            return Err(format!(
                "Approval is not pending (current status: {status})"
            ));
        }
        self.update_approval_status(approval_id, "rejected")
    }

    pub fn already_ran_today(&self, timezone_mode: &str) -> Result<bool, String> {
        let conn = self.open()?;
        let today = current_sales_day(timezone_mode);
        let mut stmt = conn
            .prepare("SELECT started_at FROM sales_runs WHERE status = 'completed'")
            .map_err(|e| format!("Run-day check prepare failed: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("Run-day check query failed: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("Run-day check row failed: {e}"))?
        {
            let started_at: String = row.get(0).unwrap_or_default();
            if timestamp_matches_sales_day(&started_at, today, timezone_mode) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn completed_runs_count(&self, segment: SalesSegment) -> Result<u32, String> {
        let conn = self.open()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sales_runs WHERE segment = ?1 AND status = 'completed'",
                params![segment.as_str()],
                |r| r.get(0),
            )
            .map_err(|e| format!("Completed-runs count failed: {e}"))?;
        Ok(count.max(0) as u32)
    }

    fn previously_discovered_domains(
        &self,
        segment: SalesSegment,
        limit: usize,
    ) -> Result<Vec<String>, String> {
        let conn = self.open()?;
        let prefix = format!("{}::", segment.as_str());
        let mut stmt = conn
            .prepare("SELECT domain FROM discovered_domains ORDER BY first_seen_at DESC LIMIT ?1")
            .map_err(|e| format!("Prepare discovered_domains query failed: {e}"))?;
        let rows = stmt
            .query_map(params![limit as i64], |row| row.get::<_, String>(0))
            .map_err(|e| format!("Query discovered_domains failed: {e}"))?;
        Ok(rows
            .flatten()
            .filter_map(|value| {
                if let Some(stripped) = value.strip_prefix(&prefix) {
                    Some(stripped.to_string())
                } else if segment == SalesSegment::B2B && !value.contains("::") {
                    Some(value)
                } else {
                    None
                }
            })
            .collect())
    }

    fn record_discovered_domain(
        &self,
        segment: SalesSegment,
        domain: &str,
        run_id: &str,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let scoped_domain = format!("{}::{domain}", segment.as_str());
        conn.execute(
            "INSERT OR IGNORE INTO discovered_domains (domain, first_seen_run_id, first_seen_at) VALUES (?1, ?2, ?3)",
            params![scoped_domain, run_id, Utc::now().to_rfc3339()],
        )
        .map_err(|e| format!("Record discovered domain failed: {e}"))?;
        Ok(())
    }

    pub async fn run_generation(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<SalesRunRecord, String> {
        self.run_generation_with_job(kernel, None, SalesSegment::B2B)
            .await
    }

    /// Run only the discovery phase — fills the account reservoir without
    /// limiting by daily_target. Returns the count of newly discovered accounts.
    pub async fn run_discovery_only(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<usize, String> {
        // Discovery reuses the full pipeline but the reservoir pattern already
        // discovers without daily_target limits (DISCOVERY_RESERVOIR_CANDIDATES).
        // This wrapper makes the intent explicit for callers.
        let record = self.run_generation(kernel).await?;
        Ok(record.discovered as usize)
    }

    /// Select best accounts from the reservoir for today's activation.
    /// Respects daily_target and applies 87/13 exploit/explore split.
    pub fn select_for_activation(&self, daily_target: u32) -> Result<Vec<String>, String> {
        let conn = self.open()?;
        let exploit_count = (daily_target as f64 * ACTIVATION_EXPLOIT_RATIO).ceil() as u32;
        let explore_count = daily_target.saturating_sub(exploit_count);

        let mut exploit_stmt = conn
            .prepare(
                "SELECT account_id FROM activation_queue
                 WHERE status = 'pending'
                 ORDER BY priority DESC LIMIT ?1",
            )
            .map_err(|e| format!("Activation exploit query failed: {e}"))?;
        let exploit_ids: Vec<String> = exploit_stmt
            .query_map(params![exploit_count], |r| r.get(0))
            .map_err(|e| format!("Activation exploit query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        let mut explore_stmt = conn
            .prepare(
                "SELECT a.id FROM accounts a
                 JOIN score_snapshots s ON s.account_id = a.id
                 WHERE s.fit_score BETWEEN 0.3 AND 0.7
                 AND a.id NOT IN (
                     SELECT account_id FROM exploration_log
                     WHERE created_at > datetime('now', '-30 days')
                 )
                 ORDER BY RANDOM() LIMIT ?1",
            )
            .map_err(|e| format!("Activation explore query failed: {e}"))?;
        let explore_ids: Vec<String> = explore_stmt
            .query_map(params![explore_count], |r| r.get(0))
            .map_err(|e| format!("Activation explore query failed: {e}"))?
            .filter_map(|r| r.ok())
            .collect();

        for acc_id in &explore_ids {
            let _ = conn.execute(
                "INSERT INTO exploration_log (id, account_id, exploration_reason, exploration_type)
                 VALUES (?1, ?2, 'scheduled_exploration', 'mid_score_random')",
                params![uuid::Uuid::new_v4().to_string(), acc_id],
            );
        }

        let mut selected = exploit_ids;
        selected.extend(explore_ids);
        Ok(selected)
    }

    pub async fn run_generation_with_job(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
        job_id: Option<&str>,
        segment: SalesSegment,
    ) -> Result<SalesRunRecord, String> {
        self.init()?;
        if segment.is_b2c() {
            return self.run_b2c_generation_with_job(kernel, job_id, segment).await;
        }
        let profile = self
            .get_profile(segment)?
            .ok_or_else(|| "Sales profile not configured".to_string())?;

        if profile.product_name.trim().is_empty()
            || profile.product_description.trim().is_empty()
            || profile.target_industry.trim().is_empty()
        {
            return Err("Sales profile is incomplete: product_name/product_description/target_industry are required".to_string());
        }
        if profile.target_geo.trim().is_empty() {
            return Err("target_geo must be set before running".to_string());
        }

        let run_sequence = self.completed_runs_count(segment)? as usize;
        let run_id = self.begin_run(segment)?;
        let started_at = Utc::now().to_rfc3339();

        let max_candidates = DISCOVERY_RESERVOIR_CANDIDATES;
        let is_field_ops = profile_targets_field_ops(&profile);
        let skip_llm_discovery = is_field_ops && geo_is_turkey(&profile.target_geo);

        // --- STAGE 1: Query Plan (LLM or heuristic fallback) ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::QueryPlanning)?;
        }
        let lead_plan = if skip_llm_discovery {
            heuristic_lead_query_plan(&profile)
        } else {
            match tokio::time::timeout(
                Duration::from_secs(LEAD_QUERY_PLAN_TIMEOUT_SECS),
                llm_build_lead_query_plan(kernel, &profile),
            )
            .await
            {
                Ok(Ok(plan)) if !plan.discovery_queries.is_empty() => plan,
                Ok(Ok(_)) => heuristic_lead_query_plan(&profile),
                Ok(Err(e)) => {
                    warn!(error = %e, "Lead query planner failed, using heuristic plan");
                    heuristic_lead_query_plan(&profile)
                }
                Err(_) => {
                    warn!("Lead query planner timed out, using heuristic plan");
                    heuristic_lead_query_plan(&profile)
                }
            }
        };
        if let Some(job_id) = job_id {
            self.complete_job_stage(job_id, PipelineStage::QueryPlanning, &lead_plan)?;
        }

        let cache = Arc::new(WebCache::new(Duration::from_secs(900)));
        let search_engine = WebSearchEngine::new(kernel.config.web.clone(), cache);
        let brave_search_engine = {
            let brave_env = kernel.config.web.brave.api_key_env.clone();
            let has_brave_key = std::env::var(&brave_env)
                .ok()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            if has_brave_key && kernel.config.web.search_provider != SearchProvider::Brave {
                let mut brave_cfg = kernel.config.web.clone();
                brave_cfg.search_provider = SearchProvider::Brave;
                Some(WebSearchEngine::new(
                    brave_cfg,
                    Arc::new(WebCache::new(Duration::from_secs(900))),
                ))
            } else {
                None
            }
        };

        // --- STAGE 2: Parallel Discovery (LLM primary + web search + directories) ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Discovery)?;
        }
        let previously_discovered = self
            .previously_discovered_domains(segment, 200)
            .unwrap_or_default();
        let llm_target = MAX_LLM_PRIMARY_CANDIDATES;
        let skip_source_llm = self.should_skip_source("llm_generation").unwrap_or(false);
        let skip_source_web = self.should_skip_source("web_search").unwrap_or(false);
        let skip_source_directory = self
            .should_skip_source("directory_listing")
            .unwrap_or(false);

        let (
            llm_candidates,
            (web_search_candidates, mut source_contact_hints, search_unavailable),
            free_candidates,
        ) = tokio::join!(
            // PRIMARY: LLM company generation
            async {
                if skip_llm_discovery || skip_source_llm {
                    Vec::new()
                } else {
                    match tokio::time::timeout(
                        Duration::from_secs(LLM_COMPANY_GENERATION_TIMEOUT_SECS),
                        llm_generate_company_candidates(
                            kernel,
                            &profile,
                            llm_target,
                            run_sequence,
                            &previously_discovered,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(candidates)) => {
                            info!(
                                count = candidates.len(),
                                "LLM primary discovery returned candidates"
                            );
                            candidates
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "LLM primary company generation failed");
                            Vec::new()
                        }
                        Err(_) => {
                            warn!("LLM primary company generation timed out");
                            Vec::new()
                        }
                    }
                }
            },
            // SECONDARY: Web search discovery
            async {
                if skip_source_web {
                    (Vec::new(), HashMap::new(), false)
                } else {
                    discover_via_web_search(
                        &search_engine,
                        &brave_search_engine,
                        &lead_plan,
                        &profile,
                        max_candidates,
                        is_field_ops,
                    )
                    .await
                }
            },
            // SUPPLEMENTAL: Turkish directory scraping
            async {
                if skip_source_directory {
                    Vec::new()
                } else {
                    fetch_free_discovery_candidates(&profile, run_sequence).await
                }
            },
        );
        let mut directory_source_counts = expected_source_counts_for_profile(&profile);
        for candidate in &free_candidates {
            if let Some(source) = candidate.contact_hint.source.as_deref() {
                let key = source_health_key(source);
                if let Some(entry) = directory_source_counts.get_mut(key) {
                    *entry += 1;
                }
            }
        }
        for (source_type, count) in directory_source_counts {
            let _ = self.update_source_health(&source_type, count);
        }
        let _ = self.update_source_health("web_search", web_search_candidates.len());
        let _ = self.update_source_health("llm_generation", llm_candidates.len());

        // --- LLM Hallucination Check (TASK-40): verify domains actually exist ---
        let mut llm_candidates = llm_candidates;
        if !llm_candidates.is_empty() {
            let mut verified = Vec::with_capacity(llm_candidates.len());
            let verify_futures: Vec<_> = llm_candidates
                .iter()
                .map(|c| verify_domain_exists(&c.domain))
                .collect();
            let results = futures::future::join_all(verify_futures).await;
            for (candidate, exists) in llm_candidates.into_iter().zip(results) {
                if exists {
                    verified.push(candidate);
                } else {
                    info!(domain = %candidate.domain, "LLM candidate domain verification failed — skipping phantom");
                }
            }
            llm_candidates = verified;
        }

        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Discovery,
                &DiscoveryCheckpoint {
                    lead_plan: lead_plan.clone(),
                    llm_candidates: llm_candidates.clone(),
                    web_candidates: web_search_candidates.clone(),
                    free_candidates: free_candidates.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                },
            )?;
        }

        // --- STAGE 3: Merge all discovery sources ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Merging)?;
        }
        let mut candidate_list = merge_all_discovery_sources(
            llm_candidates,
            web_search_candidates,
            free_candidates,
            &mut source_contact_hints,
        );
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Merging,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: Vec::new(),
                },
            )?;
        }

        // --- STAGE 4: LLM Relevance Validation ---
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Validation)?;
        }
        let mut llm_validated_domains = HashSet::<String>::new();
        let validation_count = candidate_list
            .len()
            .min(LLM_RELEVANCE_VALIDATION_BATCH_SIZE);
        let should_run_llm_validation = validation_count > 3
            && !(is_field_ops && profile.target_geo.trim().eq_ignore_ascii_case("TR"));
        if should_run_llm_validation {
            match tokio::time::timeout(
                Duration::from_secs(LLM_RELEVANCE_VALIDATION_TIMEOUT_SECS),
                llm_validate_candidate_relevance(
                    kernel,
                    &profile,
                    &candidate_list[..validation_count],
                ),
            )
            .await
            {
                Ok(Ok(validations)) => {
                    info!(validated = validations.len(), "LLM validation completed");
                    for (domain, (relevant, confidence, _)) in &validations {
                        if *relevant && *confidence >= 0.5 {
                            llm_validated_domains.insert(domain.clone());
                        }
                    }
                    apply_llm_validation_scores(&mut candidate_list, &validations);
                }
                Ok(Err(e)) => warn!(error = %e, "LLM validation failed, proceeding without"),
                Err(_) => warn!("LLM validation timed out, proceeding without"),
            }
        }
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Validation,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }

        // --- STAGE 5: Filter and Sort ---
        let mut current_stage = PipelineStage::Filtering;
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Filtering)?;
        }
        let min_candidate_score = candidate_quality_floor(&profile);
        candidate_list.retain(|c| {
            c.score >= min_candidate_score && !candidate_should_skip_for_profile(c, &profile)
        });
        candidate_list.sort_by(|a, b| {
            candidate_preseed_priority(b, source_contact_hints.get(&b.domain), &profile)
                .cmp(&candidate_preseed_priority(
                    a,
                    source_contact_hints.get(&a.domain),
                    &profile,
                ))
                .then_with(|| b.score.cmp(&a.score))
                .then_with(|| a.domain.cmp(&b.domain))
        });

        if candidate_list.is_empty() {
            let err_msg = format!(
                "No suitable companies found for '{}' in '{}'. Check network/search provider/LLM connectivity and retry.",
                profile.target_industry, profile.target_geo
            );
            self.finish_run(&run_id, "failed", 0, 0, 0, Some(&err_msg))?;
            if let Some(job_id) = job_id {
                let _ = self.fail_job_stage(job_id, current_stage, &err_msg);
            }
            return Err(err_msg);
        }
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Filtering,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }

        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Enrichment)?;
        }
        let prospect_seed_limit = DISCOVERY_PROSPECT_SEED_LIMIT;
        let seeded_prospect_profiles = match self
            .seed_prospect_profiles_for_run(
                segment,
                &run_id,
                &profile,
                kernel,
                &candidate_list,
                &source_contact_hints,
                prospect_seed_limit,
            )
            .await
        {
            Ok(profiles) => profiles,
            Err(e) => {
                warn!(run_id = %run_id, error = %e, "Failed to seed prospect profiles before lead conversion");
                Vec::new()
            }
        };
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Enrichment,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: candidate_list.clone(),
                    source_contact_hints: source_contact_hints.clone(),
                    search_unavailable,
                    llm_validated_domains: llm_validated_domains.iter().cloned().collect(),
                },
            )?;
        }
        let prospect_profile_lookup: HashMap<String, SalesProspectProfile> =
            seeded_prospect_profiles
                .iter()
                .cloned()
                .map(|profile| (profile.company_domain.clone(), profile))
                .collect();
        candidate_list.sort_by(|a, b| {
            candidate_execution_priority(
                b,
                source_contact_hints.get(&b.domain),
                prospect_profile_lookup.get(&b.domain),
                &profile,
            )
            .cmp(&candidate_execution_priority(
                a,
                source_contact_hints.get(&a.domain),
                prospect_profile_lookup.get(&a.domain),
                &profile,
            ))
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| a.domain.cmp(&b.domain))
        });
        current_stage = PipelineStage::LeadGeneration;
        let total_candidates = candidate_list.len().min(max_candidates);
        let profiled_accounts = seeded_prospect_profiles.len();
        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::LeadGeneration)?;
            self.update_job_stage_checkpoint(
                job_id,
                PipelineStage::LeadGeneration,
                &LeadGenerationCheckpoint {
                    total_candidates,
                    processed_candidates: 0,
                    profiled_accounts,
                    inserted: 0,
                    approvals_queued: 0,
                    current_domain: None,
                },
            )?;
        }

        let mut discovered = 0u32;
        let mut inserted = 0u32;
        let mut approvals_queued = 0u32;
        let mut site_headers = reqwest::header::HeaderMap::new();
        site_headers.insert(
            reqwest::header::ACCEPT_LANGUAGE,
            reqwest::header::HeaderValue::from_static("tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"),
        );
        let site_client = reqwest::Client::builder()
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            )
            .default_headers(site_headers)
            .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
            .ok();
        let max_direct_enrich_attempts = MAX_DIRECT_ENRICH_ATTEMPTS;
        let prefetched_site_enrichments = if search_unavailable {
            if let Some(client) = site_client.as_ref() {
                prefetch_site_contact_enrichments(
                    client,
                    &candidate_list,
                    max_direct_enrich_attempts.min(MAX_PREFETCH_SITE_CANDIDATES),
                    profile.target_title_policy.as_str(),
                )
                .await
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };
        // Prefetch is a latency optimization, not part of the sequential enrichment budget.
        let mut direct_enrich_attempts = 0usize;
        let mut generic_direct_enrich_retries = 0usize;
        let mut prefetched_retry_attempts = 0usize;
        let max_web_contact_search_attempts = MAX_WEB_CONTACT_SEARCH_ATTEMPTS;
        let mut web_contact_search_attempts = 0usize;
        let mut prospect_profile_updates = HashMap::<String, SalesProspectProfile>::new();
        let mut activation_candidates = HashMap::<String, ActivationLeadCandidate>::new();

        for candidate in candidate_list.iter().take(max_candidates) {
            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: Some(candidate.domain.clone()),
                    },
                )?;
            }
            discovered += 1;
            if candidate.score < min_candidate_score {
                if let Some(job_id) = job_id {
                    self.update_job_stage_checkpoint(
                        job_id,
                        PipelineStage::LeadGeneration,
                        &LeadGenerationCheckpoint {
                            total_candidates,
                            processed_candidates: discovered as usize,
                            profiled_accounts,
                            inserted,
                            approvals_queued,
                            current_domain: Some(candidate.domain.clone()),
                        },
                    )?;
                }
                continue;
            }

            let domain = &candidate.domain;
            let company = domain_to_company(domain);
            let mut email_from_verified_site = false;
            let seeded_profile = prospect_profile_lookup.get(domain);
            let seeded_name =
                seeded_profile.and_then(|profile| profile.primary_contact_name.clone());
            let seeded_title =
                seeded_profile.and_then(|profile| profile.primary_contact_title.clone());
            let seeded_linkedin = seeded_profile
                .and_then(|profile| profile.primary_linkedin_url.clone())
                .and_then(|url| normalize_outreach_linkedin_url(&url));
            let seeded_company_linkedin = seeded_profile
                .and_then(|profile| profile.company_linkedin_url.clone())
                .and_then(|url| normalize_company_linkedin_url(&url));
            let seeded_email = seeded_profile.and_then(|profile| {
                normalize_contact_email_for_domain(profile.primary_email.clone(), domain)
            });
            let mut osint_links = merge_osint_links(
                seeded_profile
                    .map(|profile| profile.osint_links.clone())
                    .unwrap_or_default(),
                vec![
                    format!("https://{domain}"),
                    seeded_linkedin.clone().unwrap_or_default(),
                    seeded_company_linkedin.clone().unwrap_or_default(),
                ],
            );
            let seeded_verified_signal = seeded_profile
                .map(prospect_profile_counts_as_verified_company_signal)
                .unwrap_or(false);
            let seeded_actionable_contact =
                lead_has_outreach_channel(seeded_email.as_ref(), seeded_linkedin.as_ref())
                    && (seeded_verified_signal
                        || lead_has_person_identity(
                            seeded_name.as_deref(),
                            seeded_linkedin.as_ref(),
                        ));

            let skip_web_contact_search = seeded_actionable_contact
                || search_unavailable
                || web_contact_search_attempts >= max_web_contact_search_attempts;
            let (
                mut contact_name,
                mut contact_title,
                mut linkedin_url,
                mut email,
                search_osint_enrichment,
            ) = if skip_web_contact_search {
                (
                    seeded_name,
                    seeded_title
                        .or_else(|| default_contact_title(profile.target_title_policy.as_str())),
                    seeded_linkedin,
                    seeded_email,
                    SiteContactEnrichment::default(),
                )
            } else {
                let mut contact_name = seeded_name;
                let mut contact_title = seeded_title;
                let mut linkedin_url = seeded_linkedin;
                let mut email: Option<String>;
                let mut search_outputs = Vec::<String>::new();
                let company_search_aliases =
                    build_company_search_aliases(&company, &candidate.evidence);
                let company_search_name = company_search_aliases
                    .first()
                    .cloned()
                    .unwrap_or_else(|| company.clone());
                web_contact_search_attempts += 1;
                let primary_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                        "site:linkedin.com/in \"{}\" (CEO OR \"Chief Executive Officer\")",
                        company_search_name
                    )
                } else {
                    format!(
                            "site:linkedin.com/in \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\")",
                            company_search_name
                        )
                };

                let domain_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                            "site:{} (\"Chief Executive Officer\" OR CEO) (leadership OR management OR executive team)",
                            domain
                        )
                } else {
                    format!(
                            "site:{} (\"Chief Executive Officer\" OR CEO OR Founder OR COO OR \"Head of Operations\") (leadership OR management OR executive team)",
                            domain
                        )
                };

                let secondary_contact_query = if profile.target_title_policy == "ceo_only" {
                    format!(
                            "\"{}\" \"{}\" (\"Chief Executive Officer\" OR CEO) (LinkedIn OR leadership OR executive team)",
                            company_search_name, domain
                        )
                } else {
                    format!(
                            "\"{}\" \"{}\" (CEO OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR leadership OR executive team)",
                            company_search_name, domain
                        )
                };

                let contact_queries = dedupe_strings(vec![
                    primary_contact_query,
                    domain_contact_query,
                    secondary_contact_query,
                ]);

                let mut contact_outputs = Vec::<String>::new();
                for (_query, result) in run_sales_search_batch(
                    &search_engine,
                    &contact_queries,
                    6,
                    Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS),
                )
                .await
                {
                    if let Ok(out) = result {
                        if !out.trim().is_empty() {
                            search_outputs.push(out.clone());
                            contact_outputs.push(out);
                        }
                    }
                }
                let contact_res = contact_outputs.join("\n");

                let (search_name, search_title, search_linkedin) =
                    extract_contact_from_search(&contact_res, profile.target_title_policy.as_str());
                let (entry_name, entry_title, entry_linkedin, entry_email) =
                    extract_contact_from_search_for_company(
                        &contact_res,
                        profile.target_title_policy.as_str(),
                        &company,
                        domain,
                    );
                if contact_name.is_none() {
                    contact_name = search_name;
                }
                if contact_title.is_none() {
                    contact_title = search_title;
                }
                if linkedin_url.is_none() {
                    linkedin_url = search_linkedin;
                }
                if contact_name.is_none() {
                    contact_name = entry_name;
                }
                if contact_title.is_none() {
                    contact_title = entry_title;
                }
                if linkedin_url.is_none() {
                    linkedin_url = entry_linkedin;
                }
                if contact_name.is_none() {
                    contact_name = linkedin_url
                        .as_deref()
                        .and_then(extract_name_from_linkedin_url);
                }
                email = seeded_email.or_else(|| {
                    normalize_contact_email_for_domain(
                        extract_email_from_text(&contact_res).or(entry_email),
                        domain,
                    )
                    .or_else(|| guessed_email(contact_name.as_deref(), domain))
                });

                if contact_name.is_none() || linkedin_url.is_none() || email.is_none() {
                    let fallback_contact_query = format!(
                            "\"{}\" \"{}\" {} (CEO OR \"Chief Executive Officer\" OR Founder OR COO OR \"Head of Operations\") (LinkedIn OR Wikipedia OR leadership OR executive team OR email)",
                            company_search_name, domain, profile.target_geo
                        );
                    let fallback_contact_res = run_sales_search(
                        &search_engine,
                        &fallback_contact_query,
                        10,
                        Duration::from_secs(SALES_CONTACT_SEARCH_TIMEOUT_SECS),
                    )
                    .await
                    .unwrap_or_default();
                    if !fallback_contact_res.trim().is_empty() {
                        search_outputs.push(fallback_contact_res.clone());
                    }
                    let (fallback_name, fallback_title, fallback_linkedin) =
                        extract_contact_from_search(
                            &fallback_contact_res,
                            profile.target_title_policy.as_str(),
                        );
                    let (
                        fallback_entry_name,
                        fallback_entry_title,
                        fallback_entry_linkedin,
                        fallback_entry_email,
                    ) = extract_contact_from_search_for_company(
                        &fallback_contact_res,
                        profile.target_title_policy.as_str(),
                        &company,
                        domain,
                    );
                    if contact_name.is_none() {
                        contact_name = fallback_name;
                    }
                    if contact_name.is_none() {
                        contact_name = fallback_entry_name;
                    }
                    if contact_title.is_none() {
                        contact_title = fallback_title;
                    }
                    if contact_title.is_none() {
                        contact_title = fallback_entry_title;
                    }
                    if linkedin_url.is_none() {
                        linkedin_url = fallback_linkedin;
                    }
                    if linkedin_url.is_none() {
                        linkedin_url = fallback_entry_linkedin;
                    }
                    if contact_name.is_none() {
                        contact_name = linkedin_url
                            .as_deref()
                            .and_then(extract_name_from_linkedin_url);
                    }
                    if email.is_none() {
                        email = normalize_contact_email_for_domain(
                            extract_email_from_text(&fallback_contact_res).or(fallback_entry_email),
                            domain,
                        )
                        .or_else(|| guessed_email(contact_name.as_deref(), domain));
                    }
                }
                let mut search_osint_enrichment = if search_outputs.is_empty() {
                    SiteContactEnrichment::default()
                } else {
                    best_search_contact_enrichment(
                        &search_outputs,
                        profile.target_title_policy.as_str(),
                        &company_search_name,
                        domain,
                    )
                };
                if search_osint_enrichment.company_linkedin_url.is_none() {
                    // 4-Layer LinkedIn search fallback (TASK-24)
                    search_osint_enrichment.company_linkedin_url =
                        find_company_linkedin_url(&company_search_name, domain, &search_engine)
                            .await;
                }
                search_osint_enrichment.osint_links = merge_osint_links(
                    search_osint_enrichment.osint_links.clone(),
                    collect_osint_links_from_search_outputs(&search_outputs, &company, domain),
                );
                (
                    contact_name,
                    contact_title,
                    linkedin_url,
                    email,
                    search_osint_enrichment,
                )
            };
            let mut company_linkedin_url = seeded_company_linkedin;
            let mut site_evidence: Option<String> = None;
            if let Some(hint) = source_contact_hints.get(domain) {
                apply_source_contact_hint(
                    domain,
                    hint,
                    &mut contact_name,
                    &mut contact_title,
                    &mut email,
                );
            }
            if let Some(enrichment) = prefetched_site_enrichments.get(domain) {
                apply_site_contact_enrichment(
                    domain,
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
            }
            let allow_prefetched_retry = prefetched_site_enrichments
                .get(domain)
                .map(|enrichment| {
                    !site_contact_enrichment_has_signal(enrichment)
                        && prefetched_retry_attempts < MAX_PREFETCH_RETRY_CANDIDATES
                })
                .unwrap_or(false);
            let needs_enrichment = contact_name.is_none()
                || contact_name_is_placeholder(contact_name.as_deref())
                || linkedin_url.is_none()
                || email.is_none()
                || (is_field_ops && site_evidence.is_none());
            let mut attempted_direct_enrich = false;
            if needs_enrichment
                && (!prefetched_site_enrichments.contains_key(domain) || allow_prefetched_retry)
                && direct_enrich_attempts < max_direct_enrich_attempts
                && site_client.is_some()
            {
                if allow_prefetched_retry {
                    prefetched_retry_attempts += 1;
                }
                direct_enrich_attempts += 1;
                attempted_direct_enrich = true;
                if let Some(client) = site_client.as_ref() {
                    if let Ok(bundle) = tokio::time::timeout(
                        Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                        fetch_company_site_html_pages(client, domain),
                    )
                    .await
                    {
                        let enrichment = best_site_contact_enrichment(
                            bundle,
                            profile.target_title_policy.as_str(),
                        );
                        apply_site_contact_enrichment(
                            domain,
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
                    }
                }
            }
            let needs_generic_identity_retry = (contact_name.is_none()
                || contact_name_is_placeholder(contact_name.as_deref()))
                && site_evidence.is_some()
                && email.is_some()
                && !attempted_direct_enrich
                && generic_direct_enrich_retries < MAX_GENERIC_DIRECT_ENRICH_RETRIES
                && site_client.is_some();
            if needs_generic_identity_retry {
                generic_direct_enrich_retries += 1;
                if let Some(client) = site_client.as_ref() {
                    if let Ok(bundle) = tokio::time::timeout(
                        Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                        fetch_company_site_html_pages(client, domain),
                    )
                    .await
                    {
                        let enrichment = best_site_contact_enrichment(
                            bundle,
                            profile.target_title_policy.as_str(),
                        );
                        apply_site_contact_enrichment(
                            domain,
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
                    }
                }
            }
            if site_contact_enrichment_has_signal(&search_osint_enrichment) {
                apply_search_contact_enrichment(
                    &search_osint_enrichment,
                    &mut contact_name,
                    &mut contact_title,
                    &mut linkedin_url,
                    &mut company_linkedin_url,
                    &mut osint_links,
                );
            }

            contact_name = contact_name.and_then(|n| normalize_person_name(&n));
            linkedin_url = linkedin_url.and_then(|url| normalize_outreach_linkedin_url(&url));
            company_linkedin_url =
                company_linkedin_url.and_then(|url| normalize_company_linkedin_url(&url));
            email = if email_from_verified_site {
                normalize_site_contact_email(email)
            } else {
                normalize_contact_email_for_domain(email, domain)
            };
            osint_links = merge_osint_links(
                osint_links,
                vec![
                    format!("https://{domain}"),
                    linkedin_url.clone().unwrap_or_default(),
                    company_linkedin_url.clone().unwrap_or_default(),
                ],
            );

            if let Some(base_profile) = seeded_profile.cloned() {
                let mut profile_update = base_profile;
                if profile_update.primary_contact_name.is_none()
                    || contact_name_is_placeholder(profile_update.primary_contact_name.as_deref())
                {
                    profile_update.primary_contact_name = contact_name.clone();
                }
                if contact_title_is_generic_default(profile_update.primary_contact_title.as_deref())
                    && !contact_title_is_generic_default(contact_title.as_deref())
                {
                    profile_update.primary_contact_title = contact_title.clone();
                }
                if profile_update.primary_email.is_none() && email.is_some() {
                    profile_update.primary_email = email.clone();
                }
                if profile_update.primary_linkedin_url.is_none() && linkedin_url.is_some() {
                    profile_update.primary_linkedin_url = linkedin_url.clone();
                }
                if profile_update.company_linkedin_url.is_none() && company_linkedin_url.is_some() {
                    profile_update.company_linkedin_url = company_linkedin_url.clone();
                }
                profile_update.osint_links =
                    merge_osint_links(profile_update.osint_links.clone(), osint_links.clone());
                profile_update.profile_status = prospect_status(
                    profile_update.primary_contact_name.as_deref(),
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                )
                .to_string();
                profile_update.recommended_channel = build_recommended_channel(
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                );
                profile_update.summary = build_prospect_summary(
                    &profile_update.company,
                    &profile_update.matched_signals,
                    profile_update.primary_contact_name.as_deref(),
                    profile_update.primary_contact_title.as_deref(),
                    profile_update.primary_email.as_deref(),
                    profile_update.primary_linkedin_url.as_deref(),
                );
                profile_update.research_confidence =
                    profile_update
                        .research_confidence
                        .max(heuristic_research_confidence(
                            profile_update.fit_score,
                            &profile_update.profile_status,
                            profile_update.source_count as usize,
                            profile_update.contact_count as usize,
                        ));
                profile_update.updated_at = Utc::now().to_rfc3339();
                prospect_profile_updates.insert(domain.clone(), profile_update);
            }
            let is_llm_validated = llm_validated_domains.contains(domain);
            let is_verified_by_memory = seeded_verified_signal;
            if !lead_has_verified_company_signal(
                is_field_ops,
                site_evidence.as_deref(),
                is_llm_validated || is_verified_by_memory,
            ) {
                continue;
            }

            // For validated companies: fill missing fields with reasonable defaults.
            if (is_llm_validated || is_verified_by_memory)
                && (contact_name.is_none() || contact_name_is_placeholder(contact_name.as_deref()))
            {
                contact_title = default_contact_title(profile.target_title_policy.as_str());
            }

            if !lead_has_outreach_channel(email.as_ref(), linkedin_url.as_ref()) {
                continue;
            }
            // Search-time LLM validation or cached dossier memory can proceed without a real person name.
            if !(is_llm_validated
                || is_verified_by_memory
                || lead_has_person_identity(contact_name.as_deref(), linkedin_url.as_ref()))
            {
                continue;
            }

            let mut score = (lead_score(&linkedin_url, &email) + candidate.score).min(100);
            if is_field_ops && site_evidence.is_some() {
                score = (score + 4).min(100);
            }

            let evidence = site_evidence
                .or_else(|| candidate.evidence.first().cloned())
                .unwrap_or_else(|| {
                    format!(
                        "{} appears in search results for {}",
                        company, profile.target_industry
                    )
                });
            let matched = if candidate.matched_keywords.is_empty() {
                profile.target_industry.clone()
            } else {
                candidate
                    .matched_keywords
                    .iter()
                    .take(4)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            let reasons = build_sales_lead_reasons(
                &profile,
                &company,
                &matched,
                &evidence,
                contact_title.as_deref(),
            );

            // Evidence-bound message generation (TASK-29): try strategy+copy first,
            // fall back to direct templates if evidence is insufficient.
            let strategy = generate_message_strategy(
                &profile,
                &company,
                contact_name.as_deref(),
                &evidence,
                &matched,
            );
            let (email_subject, email_body, linkedin_message) =
                match generate_message_copy(&strategy, &profile, &company, contact_name.as_deref())
                {
                    Ok(copy) => (copy.subject, copy.body, copy.linkedin_copy),
                    Err(_) => (
                        build_sales_email_subject(&profile, &company),
                        build_sales_email_body(
                            &profile,
                            &company,
                            contact_name.as_deref(),
                            &matched,
                            &evidence,
                        ),
                        build_sales_linkedin_message(
                            &profile,
                            &company,
                            contact_name.as_deref(),
                            &evidence,
                        ),
                    ),
                };

            let canonical = match self.sync_canonical_state(
                &self.open()?,
                &profile,
                candidate,
                &company,
                domain,
                &format!("https://{}", domain),
                contact_name.as_deref(),
                contact_title.as_deref(),
                email.as_deref(),
                candidate.phone.as_deref(),
                linkedin_url.as_deref(),
                company_linkedin_url.as_deref(),
                &osint_links,
                &evidence,
                &reasons,
            ) {
                Ok(value) => value,
                Err(e) => {
                    warn!(domain = %domain, error = %e, "Canonical account sync failed");
                    continue;
                }
            };

            let activation_score =
                ((activation_priority(&canonical.score) * 100.0).round() as i32).clamp(0, 100);
            score = score.max(activation_score);

            if !matches!(canonical.gate, SendGateDecision::Activate) {
                continue;
            }
            let lead = SalesLead {
                id: uuid::Uuid::new_v4().to_string(),
                run_id: run_id.clone(),
                company,
                website: format!("https://{}", domain),
                company_domain: domain.clone(),
                contact_name: contact_name.unwrap_or_else(|| "Leadership Team".to_string()),
                contact_title: contact_title.unwrap_or_else(|| {
                    if profile.target_title_policy == "ceo_only" {
                        "CEO".to_string()
                    } else {
                        "CEO/Founder".to_string()
                    }
                }),
                linkedin_url,
                email,
                phone: candidate.phone.as_deref().and_then(normalize_phone),
                reasons,
                email_subject,
                email_body,
                linkedin_message,
                score,
                status: "activation_candidate".to_string(),
                created_at: Utc::now().to_rfc3339(),
            };

            match self.insert_lead(&lead) {
                Ok(true) => {
                    inserted += 1;
                    let _ = self.record_discovered_domain(segment, domain, &run_id);
                    let account_id = stable_sales_id("acct", &[domain]);
                    let entry = activation_candidates
                        .entry(account_id.clone())
                        .or_insert_with(|| ActivationLeadCandidate {
                            account_id: account_id.clone(),
                            priority: activation_score as i64,
                            lead: lead.clone(),
                        });
                    if activation_score as i64 > entry.priority {
                        *entry = ActivationLeadCandidate {
                            account_id,
                            priority: activation_score as i64,
                            lead: lead.clone(),
                        };
                    }
                }
                Ok(false) => {
                    // duplicate, skip silently
                }
                Err(e) => warn!(domain = %domain, error = %e, "Lead insert failed"),
            }

            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: Some(domain.clone()),
                    },
                )?;
            }
        }

        if !activation_candidates.is_empty() {
            let conn = self.open()?;
            let candidate_priorities = activation_candidates
                .iter()
                .map(|(account_id, candidate)| (account_id.clone(), candidate.priority))
                .collect::<HashMap<_, _>>();
            let selected_accounts = self.select_accounts_for_activation(
                &conn,
                &candidate_priorities,
                profile.daily_target,
            )?;
            let selected_set = selected_accounts.into_iter().collect::<HashSet<_>>();
            for candidate in activation_candidates.into_values() {
                let lead_status = if selected_set.contains(&candidate.account_id) {
                    match self.queue_approvals_for_lead(&candidate.lead) {
                        Ok(q) => {
                            approvals_queued += q;
                            "approval_pending"
                        }
                        Err(e) => {
                            warn!(lead_id = %candidate.lead.id, error = %e, "Failed to queue selected lead approvals");
                            "activation_candidate"
                        }
                    }
                } else {
                    "activation_backlog"
                };
                if let Err(e) = self.update_lead_status(&candidate.lead.id, lead_status) {
                    warn!(lead_id = %candidate.lead.id, error = %e, "Failed to update activation lead status");
                }
            }
            if let Some(job_id) = job_id {
                self.update_job_stage_checkpoint(
                    job_id,
                    PipelineStage::LeadGeneration,
                    &LeadGenerationCheckpoint {
                        total_candidates,
                        processed_candidates: discovered as usize,
                        profiled_accounts,
                        inserted,
                        approvals_queued,
                        current_domain: None,
                    },
                )?;
            }
        }

        if !prospect_profile_updates.is_empty() {
            let updates = prospect_profile_updates.into_values().collect::<Vec<_>>();
            if let Err(e) = self.upsert_prospect_profiles(segment, &updates) {
                warn!(run_id = %run_id, error = %e, "Failed to persist OSINT-enriched prospect dossiers during run");
            }
        }

        if inserted == 0 && seeded_prospect_profiles.is_empty() {
            let err_msg = "Prospecting run completed discovery, but no durable prospect dossiers or actionable contacts could be saved for the current ICP/geo.".to_string();
            self.finish_run(
                &run_id,
                "failed",
                discovered,
                inserted,
                approvals_queued,
                Some(&err_msg),
            )?;
            if let Some(job_id) = job_id {
                let _ = self.fail_job_stage(job_id, current_stage, &err_msg);
            }
            return Err(err_msg);
        }

        let run_note = if inserted == 0 {
            Some(format!(
                "Prospecting run completed with {} profiled accounts, but no action-ready contacts were verified in this pass.",
                seeded_prospect_profiles.len()
            ))
        } else {
            None
        };

        self.finish_run(
            &run_id,
            "completed",
            discovered,
            inserted,
            approvals_queued,
            run_note.as_deref(),
        )?;
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::LeadGeneration,
                &serde_json::json!({
                    "run_id": run_id,
                    "total_candidates": total_candidates,
                    "processed_candidates": discovered,
                    "profiled_accounts": profiled_accounts,
                    "discovered": discovered,
                    "inserted": inserted,
                    "approvals_queued": approvals_queued
                }),
            )?;
            self.complete_job_run(job_id)?;
        }

        if inserted > 0 {
            if let Err(e) = self
                .refresh_prospect_profiles_for_run(segment, &run_id, &profile, kernel)
                .await
            {
                warn!(run_id = %run_id, error = %e, "Failed to refresh prospect profiles after run");
            }
        }

        Ok(SalesRunRecord {
            id: run_id,
            status: "completed".to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered,
            inserted,
            approvals_queued,
            error: run_note,
        })
    }

    async fn run_b2c_generation_with_job(
        &self,
        kernel: &openfang_kernel::OpenFangKernel,
        job_id: Option<&str>,
        segment: SalesSegment,
    ) -> Result<SalesRunRecord, String> {
        let profile = self
            .get_profile(segment)?
            .ok_or_else(|| "Sales profile not configured".to_string())?;
        if profile.product_name.trim().is_empty()
            || profile.product_description.trim().is_empty()
            || profile.target_industry.trim().is_empty()
            || profile.target_geo.trim().is_empty()
        {
            return Err(
                "B2C profile is incomplete: product_name, product_description, target_industry, and target_geo are required"
                    .to_string(),
            );
        }

        let run_sequence = self.completed_runs_count(segment)? as usize;
        let _ = run_sequence;
        let run_id = self.begin_run(segment)?;
        let started_at = Utc::now().to_rfc3339();
        let lead_plan = heuristic_b2c_query_plan(&profile);

        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::QueryPlanning)?;
            self.complete_job_stage(job_id, PipelineStage::QueryPlanning, &lead_plan)?;
        }

        let cache = Arc::new(WebCache::new(Duration::from_secs(900)));
        let search_engine = WebSearchEngine::new(kernel.config.web.clone(), cache);
        let brave_search_engine = {
            let brave_env = kernel.config.web.brave.api_key_env.clone();
            let has_brave_key = std::env::var(&brave_env)
                .ok()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            if has_brave_key && kernel.config.web.search_provider != SearchProvider::Brave {
                let mut brave_cfg = kernel.config.web.clone();
                brave_cfg.search_provider = SearchProvider::Brave;
                Some(WebSearchEngine::new(
                    brave_cfg,
                    Arc::new(WebCache::new(Duration::from_secs(900))),
                ))
            } else {
                None
            }
        };

        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Discovery)?;
        }
        let discovered_candidates = discover_b2c_social_candidates(
            &search_engine,
            brave_search_engine.as_ref(),
            &profile,
            &lead_plan,
            DISCOVERY_RESERVOIR_CANDIDATES.min(80),
        )
        .await;
        let _ = self.update_source_health("b2c_social_search", discovered_candidates.len());
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Discovery,
                &DiscoveryCheckpoint {
                    lead_plan: lead_plan.clone(),
                    llm_candidates: Vec::new(),
                    web_candidates: b2c_candidates_to_checkpoint(&discovered_candidates),
                    free_candidates: Vec::new(),
                    source_contact_hints: HashMap::new(),
                    search_unavailable: discovered_candidates.is_empty(),
                },
            )?;
        }

        if discovered_candidates.is_empty() {
            let err_msg = "No B2C public profiles were found for the current niche and geography.".to_string();
            self.finish_run(&run_id, "failed", 0, 0, 0, Some(&err_msg))?;
            if let Some(job_id) = job_id {
                let _ = self.fail_job_stage(job_id, PipelineStage::Discovery, &err_msg);
            }
            return Err(err_msg);
        }

        if let Some(job_id) = job_id {
            self.set_job_stage_running(job_id, PipelineStage::Merging)?;
            self.complete_job_stage(
                job_id,
                PipelineStage::Merging,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: b2c_candidates_to_checkpoint(&discovered_candidates),
                    source_contact_hints: HashMap::new(),
                    search_unavailable: false,
                    llm_validated_domains: Vec::new(),
                },
            )?;
            self.set_job_stage_running(job_id, PipelineStage::Validation)?;
            self.complete_job_stage(
                job_id,
                PipelineStage::Validation,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: b2c_candidates_to_checkpoint(&discovered_candidates),
                    source_contact_hints: HashMap::new(),
                    search_unavailable: false,
                    llm_validated_domains: Vec::new(),
                },
            )?;
            self.set_job_stage_running(job_id, PipelineStage::Filtering)?;
        }

        let mut filtered_candidates = discovered_candidates
            .into_iter()
            .filter(|candidate| candidate.score >= 40)
            .collect::<Vec<_>>();
        filtered_candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        filtered_candidates.truncate(DISCOVERY_PROSPECT_SEED_LIMIT.min(60));
        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Filtering,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: b2c_candidates_to_checkpoint(&filtered_candidates),
                    source_contact_hints: HashMap::new(),
                    search_unavailable: false,
                    llm_validated_domains: Vec::new(),
                },
            )?;
            self.set_job_stage_running(job_id, PipelineStage::Enrichment)?;
        }

        let enriched_candidates = enrich_b2c_candidates_with_local_market(
            &search_engine,
            brave_search_engine.as_ref(),
            &profile,
            filtered_candidates,
        )
        .await;
        let _ = self.update_source_health("b2c_local_market", enriched_candidates.len());
        let prospect_profiles = build_b2c_prospect_profiles(&run_id, &profile, enriched_candidates);
        self.upsert_prospect_profiles(segment, &prospect_profiles)?;
        for prospect in &prospect_profiles {
            let _ = self.record_discovered_domain(segment, &prospect.company_domain, &run_id);
        }

        if let Some(job_id) = job_id {
            self.complete_job_stage(
                job_id,
                PipelineStage::Enrichment,
                &CandidateCheckpoint {
                    lead_plan: lead_plan.clone(),
                    candidate_list: prospect_profiles
                        .iter()
                        .map(|profile| DomainCandidate {
                            domain: profile.company_domain.clone(),
                            score: profile.fit_score,
                            evidence: vec![profile.summary.clone()],
                            matched_keywords: profile.matched_signals.clone(),
                            source_links: profile.osint_links.clone(),
                            phone: None,
                        })
                        .collect::<Vec<_>>(),
                    source_contact_hints: HashMap::new(),
                    search_unavailable: false,
                    llm_validated_domains: Vec::new(),
                },
            )?;
            self.set_job_stage_running(job_id, PipelineStage::LeadGeneration)?;
            self.complete_job_stage(
                job_id,
                PipelineStage::LeadGeneration,
                &serde_json::json!({
                    "run_id": run_id,
                    "total_candidates": prospect_profiles.len(),
                    "processed_candidates": prospect_profiles.len(),
                    "profiled_accounts": prospect_profiles.len(),
                    "discovered": prospect_profiles.len(),
                    "inserted": 0,
                    "approvals_queued": 0
                }),
            )?;
            self.complete_job_run(job_id)?;
        }

        self.finish_run(
            &run_id,
            "completed",
            prospect_profiles.len() as u32,
            0,
            0,
            Some("B2C discovery completed with social and local-market profile matches."),
        )?;

        Ok(SalesRunRecord {
            id: run_id,
            status: "completed".to_string(),
            started_at,
            completed_at: Some(Utc::now().to_rfc3339()),
            discovered: prospect_profiles.len() as u32,
            inserted: 0,
            approvals_queued: 0,
            error: Some("B2C discovery run stores profile matches only; no outbound leads were generated.".to_string()),
        })
    }

    async fn seed_prospect_profiles_for_run(
        &self,
        segment: SalesSegment,
        run_id: &str,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
        candidates: &[DomainCandidate],
        source_contact_hints: &HashMap<String, SourceContactHint>,
        limit: usize,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let heuristic_profiles = build_candidate_prospect_profiles(
            run_id,
            candidates,
            source_contact_hints,
            limit,
            sales_profile,
        );
        let enriched = self
            .hydrate_prospect_profiles_with_cache(
                segment,
                kernel,
                sales_profile,
                heuristic_profiles,
            )
            .await;
        self.upsert_prospect_profiles(segment, &enriched)?;
        for profile in &enriched {
            let _ = self.record_discovered_domain(segment, &profile.company_domain, run_id);
        }
        Ok(enriched)
    }

    async fn refresh_prospect_profiles_for_run(
        &self,
        segment: SalesSegment,
        run_id: &str,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Result<Vec<SalesProspectProfile>, String> {
        let scan_limit = DISCOVERY_REFRESH_SCAN_LIMIT;
        let leads = self.list_leads(scan_limit, Some(run_id))?;
        if leads.is_empty() {
            return Ok(Vec::new());
        }

        let heuristic_profiles = build_prospect_profiles(leads, scan_limit, Some(sales_profile));
        let enriched = self
            .hydrate_prospect_profiles_with_cache(
                segment,
                kernel,
                sales_profile,
                heuristic_profiles,
            )
            .await;
        let enriched = self
            .enrich_prospect_profiles_with_site_osint(enriched, sales_profile)
            .await;
        let enriched = self
            .enrich_prospect_profiles_with_search_osint(enriched, sales_profile, kernel)
            .await;
        self.upsert_prospect_profiles(segment, &enriched)?;
        Ok(enriched)
    }

    async fn enrich_prospect_profiles_with_site_osint(
        &self,
        mut profiles: Vec<SalesProspectProfile>,
        sales_profile: &SalesProfile,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let client = match reqwest::Client::builder()
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            )
            .timeout(Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
        {
            Ok(client) => client,
            Err(_) => return profiles,
        };

        let osint_target_limit = DISCOVERY_OSINT_TARGET_LIMIT;
        let targets = profiles
            .iter()
            .filter(|profile| {
                profile.osint_links.len() < 2
                    || profile.company_linkedin_url.is_none()
                    || profile.primary_linkedin_url.is_none()
            })
            .take(osint_target_limit)
            .map(|profile| profile.company_domain.clone())
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return profiles;
        }

        let enrichments = join_all(targets.into_iter().map(|domain| async {
            let bundle = tokio::time::timeout(
                Duration::from_millis(DIRECT_ENRICH_TIMEOUT_MS + 400),
                fetch_company_site_html_pages(&client, &domain),
            )
            .await
            .unwrap_or_default();
            (
                domain,
                best_site_contact_enrichment(bundle, sales_profile.target_title_policy.as_str()),
            )
        }))
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();

        for profile in profiles.iter_mut() {
            if let Some(enrichment) = enrichments.get(&profile.company_domain) {
                apply_site_osint_to_profile(profile, enrichment);
            }
        }

        sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
        profiles
    }

    async fn enrich_prospect_profiles_with_search_osint(
        &self,
        mut profiles: Vec<SalesProspectProfile>,
        sales_profile: &SalesProfile,
        kernel: &openfang_kernel::OpenFangKernel,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let search_engine = WebSearchEngine::new(
            kernel.config.web.clone(),
            Arc::new(WebCache::new(Duration::from_secs(900))),
        );
        let brave_search_engine = {
            let brave_env = kernel.config.web.brave.api_key_env.clone();
            let has_brave_key = std::env::var(&brave_env)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
            if has_brave_key && kernel.config.web.search_provider != SearchProvider::Brave {
                let mut brave_cfg = kernel.config.web.clone();
                brave_cfg.search_provider = SearchProvider::Brave;
                Some(WebSearchEngine::new(
                    brave_cfg,
                    Arc::new(WebCache::new(Duration::from_secs(900))),
                ))
            } else {
                None
            }
        };

        let osint_target_limit = DISCOVERY_OSINT_TARGET_LIMIT;
        let targets = profiles
            .iter()
            .filter(|profile| {
                profile.osint_links.len() < 3
                    || profile.company_linkedin_url.is_none()
                    || profile.primary_linkedin_url.is_none()
            })
            .take(osint_target_limit)
            .map(|profile| {
                (
                    profile.company_domain.clone(),
                    profile.company.clone(),
                    profile.primary_contact_name.clone(),
                    profile.matched_signals.clone(),
                )
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return profiles;
        }

        let search_engine_ref = &search_engine;
        let brave_search_engine_ref = brave_search_engine.as_ref();
        let enrichments = stream::iter(targets.into_iter().map(
            |(domain, company, primary_name, matched_signals)| {
                let title_policy = sales_profile.target_title_policy.clone();
                let target_geo = sales_profile.target_geo.clone();
                async move {
                    let company_aliases = build_company_search_aliases(&company, &matched_signals);
                    let company_query = company_aliases
                        .first()
                        .cloned()
                        .or_else(|| {
                            if company.trim().is_empty() {
                                None
                            } else {
                                Some(company.clone())
                            }
                        })
                        .unwrap_or_else(|| domain_to_company(&domain));
                    let enrichment = search_company_osint_enrichment(
                        search_engine_ref,
                        brave_search_engine_ref,
                        &company_query,
                        &domain,
                        &title_policy,
                        &target_geo,
                        primary_name.as_deref(),
                    )
                    .await;
                    (domain, enrichment)
                }
            },
        ))
        .buffer_unordered(SALES_OSINT_PROFILE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();

        for profile in profiles.iter_mut() {
            if let Some(enrichment) = enrichments.get(&profile.company_domain) {
                apply_search_osint_to_profile(profile, enrichment);
            }
        }

        sort_prospect_profiles_for_harness(&mut profiles, Some(sales_profile));
        profiles
    }

    async fn hydrate_prospect_profiles_with_cache(
        &self,
        segment: SalesSegment,
        kernel: &openfang_kernel::OpenFangKernel,
        sales_profile: &SalesProfile,
        profiles: Vec<SalesProspectProfile>,
    ) -> Vec<SalesProspectProfile> {
        if profiles.is_empty() {
            return profiles;
        }

        let mut hydrated = Vec::with_capacity(profiles.len());
        let mut misses = Vec::new();

        for mut profile in profiles {
            match self.get_stored_prospect_profile(segment, &profile.company_domain) {
                Ok(Some(cached)) => {
                    apply_cached_prospect_memory(&mut profile, &cached);
                    if prospect_needs_llm_refresh(&profile) {
                        misses.push(profile);
                    } else {
                        hydrated.push(profile);
                    }
                }
                _ => {
                    if prospect_needs_llm_refresh(&profile) {
                        misses.push(profile);
                    } else {
                        hydrated.push(profile);
                    }
                }
            }
        }

        if misses.is_empty() {
            sort_prospect_profiles_for_harness(&mut hydrated, Some(sales_profile));
            return hydrated;
        }

        sort_prospect_profiles_for_harness(&mut misses, Some(sales_profile));
        let enrich_count = misses.len().min(6);
        let enrich_target = misses[..enrich_count].to_vec();
        let llm_enrichment = match tokio::time::timeout(
            Duration::from_secs(PROSPECT_LLM_ENRICH_TIMEOUT_SECS),
            llm_enrich_prospect_profiles(kernel, sales_profile, &enrich_target),
        )
        .await
        {
            Ok(Ok(map)) => map,
            Ok(Err(e)) => {
                warn!(error = %e, "Prospect LLM enrichment failed; keeping heuristic dossiers");
                HashMap::new()
            }
            Err(_) => {
                warn!("Prospect LLM enrichment timed out; keeping heuristic dossiers");
                HashMap::new()
            }
        };

        for mut profile in misses {
            if let Some(extra) = llm_enrichment.get(&profile.company_domain) {
                apply_llm_prospect_research(&mut profile, extra);
            }
            hydrated.push(profile);
        }

        sort_prospect_profiles_for_harness(&mut hydrated, Some(sales_profile));
        hydrated
    }
}

