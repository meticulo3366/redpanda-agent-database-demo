#!/usr/bin/env bash
# 30-changefeeds.sh : create the CockroachDB changefeeds that stream memory and
# account changes into Redpanda, if they are not already running.
#
#   agent_memory.public.memories  -> kafka topic agent.memory.cdc
#   agent_memory.public.accounts  -> kafka topic agent.accounts.cdc
#
# Idempotent: for each topic the script looks in SHOW CHANGEFEED JOBS for a job
# whose description names the topic. running/pending -> left alone, paused ->
# resumed, anything else (none, failed, canceled, succeeded) -> created.
# Runs as the CockroachDB admin (CRDB_ADMIN_URL). Contract: docs/DESIGN.md
# section 5 (changefeeds) and section 8.
set -euo pipefail

source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql mktemp

: "${REDPANDA_BROKERS:?REDPANDA_BROKERS missing from .env}"
: "${KAFKA_USER:?KAFKA_USER missing from .env}"
: "${KAFKA_PASSWORD:?KAFKA_PASSWORD missing from .env}"
: "${CRDB_DATABASE:=agent_memory}"
: "${TOPIC_MEMORY_CDC:=agent.memory.cdc}"
: "${TOPIC_ACCOUNTS_CDC:=agent.accounts.cdc}"

# SQL statements are written to a private temp file because the sink URI
# embeds the Kafka password; the file never reaches the log or the repo.
SQL_TMP="$(mktemp "${TMPDIR:-/tmp}/changefeed.XXXXXX")"
chmod 600 "$SQL_TMP"
trap 'rm -f "$SQL_TMP"' EXIT

# find_job <topic> : prints "<job_id> <status>" for the newest changefeed job
# that targets the topic, or nothing.
find_job() {
  local topic="$1"
  cat >"$SQL_TMP" <<SQL
SELECT job_id || ' ' || status
FROM [SHOW CHANGEFEED JOBS]
WHERE (description LIKE '%topic_name=${topic}%' OR topics LIKE '%${topic}%')
  AND status IN ('running', 'pending', 'paused', 'pause-requested')
ORDER BY created DESC
LIMIT 1;
SQL
  crdb_admin_psql_file "$SQL_TMP" -tA
}

# create_changefeed <table> <topic>
create_changefeed() {
  local table="$1" topic="$2"
  local sink="kafka://${REDPANDA_BROKERS}?topic_name=${topic}&tls_enabled=true&sasl_enabled=true&sasl_user=${KAFKA_USER}&sasl_password=${KAFKA_PASSWORD}&sasl_mechanism=SCRAM-SHA-256"
  cat >"$SQL_TMP" <<SQL
CREATE CHANGEFEED FOR TABLE ${CRDB_DATABASE}.public.${table}
  INTO '${sink}'
  WITH updated, diff, resolved = '10s', min_checkpoint_frequency = '5s', format = json;
SQL
  info "creating changefeed ${CRDB_DATABASE}.public.${table} -> ${topic}"
  crdb_admin_psql_file "$SQL_TMP" -tA | sed 's/^/  job_id /'
}

# ensure_changefeed <table> <topic>
ensure_changefeed() {
  local table="$1" topic="$2" found job_id status
  found="$(find_job "$topic" | head -n 1)"
  job_id="${found%% *}"
  status="${found#* }"
  if [[ -z "$found" ]]; then
    create_changefeed "$table" "$topic"
    return
  fi
  case "$status" in
    running|pending)
      ok "changefeed for ${topic} already ${status} (job ${job_id})"
      ;;
    paused|pause-requested)
      info "changefeed for ${topic} is ${status} (job ${job_id}); resuming"
      printf 'RESUME JOB %s;\n' "$job_id" >"$SQL_TMP"
      crdb_admin_psql_file "$SQL_TMP" -tA >/dev/null
      ok "resumed job ${job_id}"
      ;;
    *)
      warn "changefeed job ${job_id} for ${topic} is ${status}; creating a new one"
      create_changefeed "$table" "$topic"
      ;;
  esac
}

log "ensuring changefeeds on ${CRDB_DATABASE} -> ${REDPANDA_BROKERS}"
ensure_changefeed memories "$TOPIC_MEMORY_CDC"
ensure_changefeed accounts "$TOPIC_ACCOUNTS_CDC"

log "SHOW CHANGEFEED JOBS (demo topics)"
crdb_admin_psql "SELECT job_id, status, running_status, full_table_names, topics, high_water_timestamp
FROM [SHOW CHANGEFEED JOBS]
WHERE description LIKE '%topic_name=${TOPIC_MEMORY_CDC}%' OR description LIKE '%topic_name=${TOPIC_ACCOUNTS_CDC}%'
   OR topics LIKE '%${TOPIC_MEMORY_CDC}%' OR topics LIKE '%${TOPIC_ACCOUNTS_CDC}%'
ORDER BY created DESC;"
ok "changefeeds ready"
