#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:4200}"
ITERATIONS="${ITERATIONS:-3}"
RESET_DB="${RESET_DB:-0}"
RUN_POLL_INTERVAL="${RUN_POLL_INTERVAL:-2}"
RUN_POLL_TIMEOUT="${RUN_POLL_TIMEOUT:-300}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if ! curl -fsS "$BASE_URL/api/health" >/dev/null; then
  echo "Daemon is not reachable at $BASE_URL" >&2
  exit 1
fi

if [[ "$RESET_DB" == "1" ]]; then
  rm -f "$HOME/.openfang/sales.db"
fi

BRIEF_FILE="${1:-}"
if [[ -n "$BRIEF_FILE" ]]; then
  if [[ ! -f "$BRIEF_FILE" ]]; then
    echo "Brief file not found: $BRIEF_FILE" >&2
    exit 1
  fi
  BRIEF_CONTENT="$(cat "$BRIEF_FILE")"
else
  BRIEF_CONTENT="$(cat <<'EOF'
Yeni Takim Arkadasiniz: Machinity
Projeleri takip etmek yerine projeleri yoneten bir AI ekip arkadasi.
Toplantidan yonetime, WhatsApp'tan proje panosuna: uctan uca otonom koordinasyon.
Saha operasyonu olan sirketlere odaklaniyoruz: field service, maintenance, installation, construction, facility management.
Machinity toplantiya katilir, aksiyonlari yakalar, gorevleri olusturur, dogru kisilere atar ve WhatsApp uzerinden takip eder.
Kurulum suresi 5 dakikanin altinda. Iletisim: machinity.ai info@machinity.ai
EOF
)"
fi

# Keep auth synced with local codex CLI token if available.
curl -sS -X POST "$BASE_URL/api/auth/codex/import-cli" \
  -H "content-type: application/json" \
  -d '{}' >/dev/null || true

AUTO_PAYLOAD="$(jq -n --arg brief "$BRIEF_CONTENT" '{brief:$brief, persist:true}')"
AUTO_RES="$(curl -sS -X POST "$BASE_URL/api/sales/onboarding/brief" \
  -H "content-type: application/json" \
  --data-binary "$AUTO_PAYLOAD")"

RUN_SUMMARIES_FILE="$(mktemp)"
trap 'rm -f "$RUN_SUMMARIES_FILE"' EXIT

fetch_run_record() {
  local run_id="$1"
  curl -sS "$BASE_URL/api/sales/runs?limit=200" \
    | jq --arg run_id "$run_id" '.runs[] | select(.id == $run_id)'
}

for i in $(seq 1 "$ITERATIONS"); do
  RUN_RES="$(curl -sS -X POST "$BASE_URL/api/sales/run" \
    -H "content-type: application/json" \
    -d '{}')"
  RUN_ID="$(jq -r '.run.id // empty' <<<"$RUN_RES")"

  if [[ -n "$RUN_ID" ]]; then
    DEADLINE=$(( $(date +%s) + RUN_POLL_TIMEOUT ))
    while :; do
      RUN_RECORD="$(fetch_run_record "$RUN_ID")"
      RUN_STATUS="$(jq -r '.status // empty' <<<"$RUN_RECORD")"
      if [[ "$RUN_STATUS" == "completed" || "$RUN_STATUS" == "failed" ]]; then
        RUN_RES="$(jq -n --argjson run "$RUN_RECORD" '{run:$run, error: ($run.error // null)}')"
        break
      fi
      if (( $(date +%s) >= DEADLINE )); then
        RUN_RES="$(jq -n --arg run_id "$RUN_ID" --arg error "Timed out waiting for run completion" '{run:{id:$run_id,status:"timeout",discovered:0,inserted:0}, error:$error}')"
        break
      fi
      sleep "$RUN_POLL_INTERVAL"
    done
  fi

  PROSPECTS_RES='{"prospects":[]}'
  if [[ -n "$RUN_ID" ]]; then
    PROSPECTS_RES="$(curl -sS "$BASE_URL/api/sales/prospects?limit=200&run_id=$RUN_ID")"
  fi

  jq -n \
    --arg idx "$i" \
    --argjson run "$RUN_RES" \
    --argjson prospects "$PROSPECTS_RES" \
    '
    {
      iteration: ($idx|tonumber),
      run_id: ($run.run.id // null),
      discovered: ($run.run.discovered // 0),
      inserted: ($run.run.inserted // 0),
      error: ($run.error // null),
      prospect_count: ($prospects.prospects | length),
      profile_only: (
        (($prospects.prospects | length) > 0) and (($run.run.inserted // 0) == 0)
      ),
      contact_ready: (
        $prospects.prospects
        | map(select((.profile_status // "") == "contact_ready"))
        | length
      ),
      with_email: (
        $prospects.prospects
        | map(select((.primary_email // "") != ""))
        | length
      ),
      with_linkedin: (
        $prospects.prospects
        | map(select((.primary_linkedin_url // "") != ""))
        | length
      ),
      company_only: (
        $prospects.prospects
        | map(select((.profile_status // "") == "company_only"))
        | length
      ),
      llm_enriched: (
        $prospects.prospects
        | map(select((.research_status // "") == "llm_enriched"))
        | length
      ),
      avg_confidence: (
        if (($prospects.prospects | length) == 0) then 0
        else (($prospects.prospects | map(.research_confidence // 0) | add) / ($prospects.prospects | length))
        end
      ),
      field_signal_ratio: (
        if (($prospects.prospects | length) == 0) then 0
        else (
          ($prospects.prospects | map(select(
            ((.matched_signals | join(" ") | ascii_downcase) | test("field|maintenance|facility|construction|dispatch|on-site|operations"))
          )) | length) / ($prospects.prospects | length)
        ) end
      ),
      domains_sample: ($prospects.prospects | map(.company_domain) | .[:8])
    }
    ' >>"$RUN_SUMMARIES_FILE"
done

ONBOARDING="$(curl -sS "$BASE_URL/api/sales/onboarding/status")"

jq -s \
  --argjson auto "$AUTO_RES" \
  --argjson onboarding "$ONBOARDING" \
  '
  {
    autofill: {
      source: ($auto.source // null),
      warnings: ($auto.warnings // []),
      product_name: ($auto.profile.product_name // null),
      target_industry: ($auto.profile.target_industry // null),
      target_geo: ($auto.profile.target_geo // null),
      sender_email: ($auto.profile.sender_email // null)
    },
    onboarding: {
      completed: ($onboarding.status.completed // false),
      first_run_ready: ($onboarding.status.first_run_ready // false),
      active_step: ($onboarding.status.active_step // null)
    },
    runs: .,
    totals: {
      run_count: length,
      inserted_total: (map(.inserted) | add),
      discovered_total: (map(.discovered) | add),
      prospect_count_total: (map(.prospect_count) | add),
      profile_only_runs: (map(select(.profile_only == true)) | length),
      contact_ready_total: (map(.contact_ready) | add),
      llm_enriched_total: (map(.llm_enriched) | add),
      with_email_total: (map(.with_email) | add),
      with_linkedin_total: (map(.with_linkedin) | add),
      company_only_total: (map(.company_only) | add),
      avg_confidence: (
        if (length == 0) then 0
        else (map(.avg_confidence) | add) / length
        end
      ),
      field_signal_avg: (
        if (length == 0) then 0
        else (map(.field_signal_ratio) | add) / length
        end
      )
    }
  }
  ' "$RUN_SUMMARIES_FILE"
