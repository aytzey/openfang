pub async fn autofill_sales_profile(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
    Json(body): Json<SalesProfileAutofillRequest>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    if body.brief.trim().len() < 20 {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": "Provide a richer company brief (at least 20 chars)."}),
            ),
        );
    }

    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    if let Err(e) = engine.set_onboarding_brief(segment, body.brief.trim()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        );
    }
    let persist = body.persist.unwrap_or(true);
    let (profile, source, warnings) =
        match apply_brief_to_profile(&state, &engine, segment, body.brief.trim(), persist).await {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };

    let onboarding = match (
        engine.get_profile(segment),
        engine.get_onboarding_brief_state(segment),
    ) {
        (Ok(profile_opt), Ok(brief_state)) => {
            let home_dir = state.kernel.home_dir();
            let last_run_id = engine
                .latest_successful_run_id_since(segment, brief_state.updated_at.as_deref())
                .ok()
                .flatten();
            Some(build_onboarding_status(
                segment,
                &home_dir,
                profile_opt,
                brief_state.brief,
                last_run_id,
            ))
        }
        _ => None,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "profile": profile,
            "persisted": persist,
            "source": source,
            "warnings": warnings,
            "onboarding": onboarding
        })),
    )
}

pub async fn put_sales_onboarding_brief(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
    Json(body): Json<SalesOnboardingBriefRequest>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let brief = body.brief.trim();
    if brief.len() < 20 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Brief en az 20 karakter olmali."})),
        );
    }
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    if let Err(e) = engine.set_onboarding_brief(segment, brief) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        );
    }
    let persist = body.persist.unwrap_or(true);
    let (profile, source, warnings) =
        match apply_brief_to_profile(&state, &engine, segment, brief, persist).await {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };
    let brief_state = engine
        .get_onboarding_brief_state(segment)
        .ok()
        .unwrap_or_default();
    let home_dir = state.kernel.home_dir();
    let status = build_onboarding_status(
        segment,
        &home_dir,
        engine.get_profile(segment).ok().flatten(),
        brief_state.brief,
        engine
            .latest_successful_run_id_since(segment, brief_state.updated_at.as_deref())
            .ok()
            .flatten(),
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "profile": profile,
            "persisted": persist,
            "source": source,
            "warnings": warnings,
            "onboarding": status
        })),
    )
}

pub async fn get_sales_onboarding_status(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let profile = match engine.get_profile(segment) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let brief_state = match engine.get_onboarding_brief_state(segment) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let last_successful_run_id =
        match engine.latest_successful_run_id_since(segment, brief_state.updated_at.as_deref()) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e})),
                )
            }
        };
    let home_dir = state.kernel.home_dir();
    let status = build_onboarding_status(
        segment,
        &home_dir,
        profile,
        brief_state.brief,
        last_successful_run_id,
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": status })),
    )
}

pub async fn get_sales_profile(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_profile(segment) {
        Ok(profile) => (
            StatusCode::OK,
            Json(serde_json::json!({"profile": profile.unwrap_or_default()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn put_sales_profile(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
    Json(profile): Json<SalesProfile>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let profile = match normalize_sales_profile(profile) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.upsert_profile(segment, &profile) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "saved", "profile": profile})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn run_sales_now(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let profile = match engine.get_profile(segment) {
        Ok(Some(profile)) => profile,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Sales profile not configured"})),
            )
        }
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    if profile.product_name.trim().is_empty()
        || profile.product_description.trim().is_empty()
        || profile.target_industry.trim().is_empty()
        || profile.target_geo.trim().is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": "Sales profile is incomplete; product_name, product_description, target_industry, and target_geo are required"}),
            ),
        );
    }

    let job_id = match engine.create_job_run("discovery", segment) {
        Ok(job_id) => job_id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let kernel = state.kernel.clone();
    let home_dir = state.kernel.home_dir();
    let engine_for_task = SalesEngine::new(&home_dir);
    let spawned_job_id = job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = engine_for_task
            .run_generation_with_job(&kernel, Some(&spawned_job_id), segment)
            .await
        {
            let _ =
                engine_for_task.fail_job_stage(&spawned_job_id, PipelineStage::QueryPlanning, &err);
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "job_id": job_id,
            "status": "running",
            "current_stage": "QueryPlanning"
        })),
    )
}

pub async fn get_sales_job_progress(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_job_progress(&job_id) {
        Ok(Some(progress)) => (StatusCode::OK, Json(serde_json::json!(progress))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Job not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_active_sales_job_progress(
    State(state): State<Arc<AppState>>,
    Query(segment_query): Query<SalesSegmentQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.latest_running_job_progress("discovery", segment) {
        Ok(progress) => (StatusCode::OK, Json(serde_json::json!({ "job": progress }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn retry_sales_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
    Query(segment_query): Query<SalesSegmentQuery>,
    Json(body): Json<JobRetryRequest>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(segment_query.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let resume_stage = if body.force_fresh {
        None
    } else {
        engine
            .latest_completed_checkpoint(&job_id)
            .ok()
            .flatten()
            .map(|(stage, _)| stage.as_str().to_string())
    };
    let new_job_id = match engine.create_job_run("discovery", segment) {
        Ok(job_id) => job_id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    let kernel = state.kernel.clone();
    let home_dir = state.kernel.home_dir();
    let engine_for_task = SalesEngine::new(&home_dir);
    let spawned_job_id = new_job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = engine_for_task
            .run_generation_with_job(&kernel, Some(&spawned_job_id), segment)
            .await
        {
            let _ =
                engine_for_task.fail_job_stage(&spawned_job_id, PipelineStage::QueryPlanning, &err);
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "job_id": new_job_id,
            "status": "running",
            "resumed_from_stage": resume_stage,
            "replayed_from_scratch": true
        })),
    )
}

pub async fn list_sales_source_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.list_source_health() {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"sources": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_policy_proposals(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesPolicyProposalQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_policy_proposals(q.status.as_deref(), limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposals": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn approve_sales_policy_proposal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.update_policy_proposal_status(&id, "active", Some("operator")) {
        Ok(Some(proposal)) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposal": proposal})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Policy proposal not found"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn reject_sales_policy_proposal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.update_policy_proposal_status(&id, "retired", None) {
        Ok(Some(proposal)) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposal": proposal})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Policy proposal not found"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn sales_unsubscribe(
    State(state): State<Arc<AppState>>,
    Query(query): Query<UnsubscribeQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(_e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("<html><body><h1>PulsivoSalesman</h1><p>Unsubscribe service unavailable.</p></body></html>".to_string()),
            )
        }
    };

    let Some(email) = verify_unsubscribe_token(&query.token) else {
        return (
            StatusCode::BAD_REQUEST,
            Html(
                "<html><body><h1>PulsivoSalesman</h1><p>Invalid unsubscribe token.</p></body></html>"
                    .to_string(),
            ),
        );
    };

    let conn = match engine.open() {
        Ok(conn) => conn,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!(
                    "<html><body><h1>PulsivoSalesman</h1><p>{}</p></body></html>",
                    e
                )),
            )
        }
    };
    if let Err(e) = engine.suppress_contact(&conn, &email, "one_click_unsubscribe", true, None) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!(
                "<html><body><h1>PulsivoSalesman</h1><p>{}</p></body></html>",
                e
            )),
        );
    }
    let _ = conn.execute(
        "UPDATE contact_methods SET suppressed = 1 WHERE value = ?1",
        params![email.trim().to_lowercase()],
    );
    (
        StatusCode::OK,
        Html(format!(
            "<html><body><h1>PulsivoSalesman</h1><p>{} artik kalici olarak suppression listesinde. Bu aliciya tekrar gonderim yapilmayacak.</p></body></html>",
            email
        )),
    )
}

pub async fn sales_outcomes_webhook(
    State(state): State<Arc<AppState>>,
    Json(body): Json<OutcomeWebhookRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.ingest_outcome_event(&body.delivery_id, &body.event_type, &body.raw_text) {
        Ok(result) => {
            // After outcome ingestion, advance sequences (TASK-30)
            let advanced = engine.advance_sequences().unwrap_or(0);
            (
                StatusCode::OK,
                Json(serde_json::json!({"result": result, "sequences_advanced": advanced})),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn advance_sales_sequences(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match engine.advance_sequences() {
        Ok(count) => (StatusCode::OK, Json(serde_json::json!({"advanced": count}))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

// --- Experiment endpoints (TASK-37) ---

pub async fn list_sales_experiments(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT id, name, hypothesis, variant_a, variant_b, status, created_at
         FROM experiments ORDER BY created_at DESC LIMIT 50",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    };
    let experiments: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "id": r.get::<_, String>(0)?,
                "name": r.get::<_, String>(1)?,
                "hypothesis": r.get::<_, Option<String>>(2)?,
                "variant_a": r.get::<_, Option<String>>(3)?,
                "variant_b": r.get::<_, Option<String>>(4)?,
                "status": r.get::<_, String>(5)?,
                "created_at": r.get::<_, String>(6)?,
            }))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    (
        StatusCode::OK,
        Json(serde_json::json!({"experiments": experiments})),
    )
}

pub async fn create_sales_experiment(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let name = body["name"].as_str().unwrap_or("unnamed");
    let hypothesis = body["hypothesis"].as_str().unwrap_or("");
    let variant_a = body["variant_a"].as_str().unwrap_or("control");
    let variant_b = body["variant_b"].as_str().unwrap_or("treatment");
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match create_experiment(&conn, name, hypothesis, variant_a, variant_b) {
        Ok(id) => (
            StatusCode::OK,
            Json(serde_json::json!({"id": id, "status": "active"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_sales_experiment_results(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match get_experiment_results(&conn, &id) {
        Ok(results) => (StatusCode::OK, Json(results)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

// --- Context Factors endpoint (TASK-35) ---

pub async fn list_sales_context_factors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let bad_timing = is_bad_timing_today(&conn);
    let budget_quarter = current_budget_quarter(&conn);
    let mut stmt = match conn.prepare(
        "SELECT id, factor_type, factor_key, factor_value, effective_from, effective_until, source
         FROM contextual_factors ORDER BY effective_from",
    ) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    };
    let factors: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "id": r.get::<_, String>(0)?,
                "factor_type": r.get::<_, String>(1)?,
                "factor_key": r.get::<_, String>(2)?,
                "factor_value": r.get::<_, Option<String>>(3)?,
                "effective_from": r.get::<_, Option<String>>(4)?,
                "effective_until": r.get::<_, Option<String>>(5)?,
                "source": r.get::<_, Option<String>>(6)?,
            }))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "factors": factors,
            "bad_timing_today": bad_timing,
            "current_budget_quarter": budget_quarter,
        })),
    )
}

// --- Score Calibration endpoint (TASK-36) ---

pub async fn run_sales_calibration(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let conn = match engine.open() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    match calibrate_scoring_from_outcomes(&conn) {
        Ok(proposals) => (
            StatusCode::OK,
            Json(serde_json::json!({"proposals": proposals, "count": proposals.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_runs(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(q.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(segment, SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_runs(segment, limit) {
        Ok(runs) => (
            StatusCode::OK,
            Json(serde_json::json!({"runs": runs, "total": runs.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_leads(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(q.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(segment, SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_leads(limit, q.run_id.as_deref()) {
        Ok(leads) => (
            StatusCode::OK,
            Json(serde_json::json!({"leads": leads, "total": leads.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_prospects(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let segment = sales_segment_from_query(q.segment.as_deref());
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);
    let _ = engine.recover_latest_timed_out_run_if_stale(segment, SALES_RUN_RECOVERY_STALE_SECS);

    match engine.list_prospect_profiles(segment, limit, q.run_id.as_deref()) {
        Ok(prospects) => (
            StatusCode::OK,
            Json(serde_json::json!({"prospects": prospects, "total": prospects.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_approvals(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesApprovalQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_approvals(q.status.as_deref(), limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"approvals": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn bulk_approve_sales_approvals(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SalesApprovalBulkApproveRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let ids = dedupe_strings(body.ids);
    if ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "ids must not be empty"})),
        );
    }

    let mut approved = Vec::<serde_json::Value>::new();
    let mut failed = Vec::<serde_json::Value>::new();
    for id in ids {
        match engine.approve_and_send(&state, &id).await {
            Ok(result) => approved.push(serde_json::json!({
                "id": id,
                "result": result,
            })),
            Err(error) => failed.push(serde_json::json!({
                "id": id,
                "error": error,
            })),
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "approved": approved,
            "failed": failed,
            "approved_count": approved.len(),
            "failed_count": failed.len(),
        })),
    )
}

pub async fn edit_sales_approval(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SalesApprovalEditRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.edit_approval(&id, body.edited_payload) {
        Ok(approval) => (
            StatusCode::OK,
            Json(serde_json::json!({"approval": approval})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn get_sales_account_dossier(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.get_account_dossier(&id) {
        Ok(Some(dossier)) => (
            StatusCode::OK,
            Json(serde_json::json!({"dossier": dossier})),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Account dossier not found"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn approve_and_send(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.approve_and_send(&state, &id).await {
        Ok(result) => (StatusCode::OK, Json(serde_json::json!({"result": result}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn reject_sales_approval(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(_body): Json<SalesRejectRequest>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };

    match engine.reject_approval(&id) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"status": "rejected"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub async fn list_sales_deliveries(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SalesLeadQuery>,
) -> impl IntoResponse {
    let engine = match engine_from_state(&state) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(500);

    match engine.list_deliveries(limit) {
        Ok(items) => (
            StatusCode::OK,
            Json(serde_json::json!({"deliveries": items, "total": items.len()})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

pub fn spawn_sales_scheduler(kernel: Arc<pulsivo_salesman_kernel::PulsivoSalesmanKernel>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await;

            let home_dir = kernel.home_dir();
            let engine = SalesEngine::new(&home_dir);
            if let Err(e) = engine.init() {
                warn!(error = %e, "Sales scheduler: DB init failed");
                continue;
            }

            let profile = match engine.get_profile(SalesSegment::B2B) {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, "Sales scheduler: profile read failed");
                    continue;
                }
            };

            let now = Local::now();
            if now.hour() as u8 != profile.schedule_hour_local || now.minute() > 10 {
                continue;
            }

            match engine.already_ran_today(&profile.timezone_mode) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    warn!(error = %e, "Sales scheduler: run-day check failed");
                    continue;
                }
            }

            info!("Sales scheduler: triggering daily run");
            match tokio::time::timeout(Duration::from_secs(120), engine.run_generation(&kernel))
                .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => error!(error = %e, "Sales scheduler: run failed"),
                Err(_) => error!("Sales scheduler: run timed out"),
            }
        }
    });
}

trait OptionalRow<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalRow<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
