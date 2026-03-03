#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:4200}"
ITERATIONS="${ITERATIONS:-3}"
RESET_DB="${RESET_DB:-0}"

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

for i in $(seq 1 "$ITERATIONS"); do
  RUN_RES="$(curl -sS -X POST "$BASE_URL/api/sales/run" \
    -H "content-type: application/json" \
    -d '{}')"
  RUN_ID="$(jq -r '.run.id // empty' <<<"$RUN_RES")"

  LEADS_RES='{"leads":[]}'
  if [[ -n "$RUN_ID" ]]; then
    LEADS_RES="$(curl -sS "$BASE_URL/api/sales/leads?limit=200&run_id=$RUN_ID")"
  fi

  jq -n \
    --arg idx "$i" \
    --argjson run "$RUN_RES" \
    --argjson leads "$LEADS_RES" \
    '
    {
      iteration: ($idx|tonumber),
      run_id: ($run.run.id // null),
      discovered: ($run.run.discovered // 0),
      inserted: ($run.run.inserted // 0),
      error: ($run.error // null),
      lead_count: ($leads.leads | length),
      with_email: ($leads.leads | map(select((.email // "") != "")) | length),
      with_linkedin: ($leads.leads | map(select((.linkedin_url // "") != "")) | length),
      unknown_contact: (
        $leads.leads
        | map(select(((.contact_name // "") | ascii_downcase) == "unknown" or ((.contact_name // "") | ascii_downcase) == "leadership team"))
        | length
      ),
      field_signal_ratio: (
        if (($leads.leads | length) == 0) then 0
        else (
          ($leads.leads | map(select(
            ((.reasons | join(" ") | ascii_downcase) | test("field|maintenance|facility|construction|dispatch|on-site|operations"))
          )) | length) / ($leads.leads | length)
        ) end
      ),
      domains_sample: ($leads.leads | map(.company_domain) | .[:8])
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
      with_email_total: (map(.with_email) | add),
      with_linkedin_total: (map(.with_linkedin) | add),
      lead_count_total: (map(.lead_count) | add),
      field_signal_avg: (
        if (length == 0) then 0
        else (map(.field_signal_ratio) | add) / length
        end
      )
    }
  }
  ' "$RUN_SUMMARIES_FILE"
