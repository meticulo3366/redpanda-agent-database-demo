#!/usr/bin/env bash
# 90-teardown.sh [--all] - remove what the demo created (docs/DESIGN.md section 8).
#   default : ADP agent, Cedar policies, MCP server, the agent's (Anthropic) LLM provider,
#             guardrail, Bedrock LLM provider (in that order: the guardrail's backend
#             references the Bedrock provider); Connect pipelines
#   --all   : also changefeeds, topics, ACLs, SASL user, secrets and the CockroachDB objects
# Asks for confirmation unless FORCE=1. DRY_RUN=1 only looks things up and prints the plan.
# .env is kept so a later `make up` reuses the same passwords.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd rpk curl jq psql

ALL=0
DRY_RUN="${DRY_RUN:-0}"
for a in "$@"; do
  case "$a" in
    --all) ALL=1 ;;
    -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
    *) die "unknown argument: $a" ;;
  esac
done

# Never touch the shared providers: zeke-bedrock-latest (other demos) and
# anthropic-api-key (the org provider on the same ANTHROPIC_API_KEY secret).
for p in "$LLM_PROVIDER" "$BEDROCK_PROVIDER"; do
  case "$p" in
    zeke-bedrock-latest|anthropic-api-key) die "refusing to touch the shared provider $p" ;;
  esac
done

# Fail fast on an expired ADP login: otherwise every existence check below would
# read as "not present" and the ADP resources would be silently skipped.
rp_token >/dev/null

POLICIES=(memory-concierge-mcp memory-concierge-llm)
PIPELINES=(agent-memory-audit agent-memory-observer)
SECRETS=(AGENT_MEMORY_CRDB_DSN AGENT_MEMORY_KAFKA_USER AGENT_MEMORY_KAFKA_PASSWORD)
LEFTOVERS=()   # resources whose delete failed; reported at the end with exit 1

would() { log "DRY_RUN: would $*"; }
urlenc() { jq -rn --arg v "$1" '$v|@uri'; }

# adp_exists <kind> get <name>: 0 when the resource exists, 1 when rpk reports
# not_found (on stdout as JSON or on stderr, depending on the kind), dies on any
# other error (expired login, network, 5xx) instead of treating it as absent.
adp_exists() {
  local out
  if out="$(rpk ai "$@" -o json 2>&1)"; then
    return 0
  fi
  grep -qi 'not_found' <<<"$out" && return 1
  out="${out//[[:cntrl:]]/ }"
  die "rpk ai $*: ${out:0:300}"
}

# ---------------------------------------------------------------------------
log "Teardown plan$( (( DRY_RUN )) && printf ' (DRY_RUN)' )"
log "  ADP:        agent $AGENT_NAME, policies ${POLICIES[*]}, MCP server $MCP_SERVER_NAME, provider $LLM_PROVIDER (agent), guardrail $GUARDRAIL_NAME, provider $BEDROCK_PROVIDER (guardrail backend)"
log "  Pipelines:  ${PIPELINES[*]}"
if (( ALL )); then
  log "  Changefeeds for $TOPIC_MEMORY_CDC and $TOPIC_ACCOUNTS_CDC (cancel)"
  log "  Topics:     $TOPIC_MEMORY_CDC, $TOPIC_ACCOUNTS_CDC"
  log "  ACLs + SASL user $KAFKA_USER, secrets ${SECRETS[*]}"
  log "  CockroachDB: DROP TABLE memory_audit, transactions, accounts, memories, customers; DROP ROLE $CRDB_AGENT_USER (database $CRDB_DATABASE is kept)"
fi
if [[ "${FORCE:-0}" != "1" ]] && (( DRY_RUN == 0 )); then
  [[ -t 0 ]] || die "stdin is not a terminal; re-run with FORCE=1 to skip the confirmation"
  read -r -p "Type 'yes' to continue: " answer
  [[ "$answer" == "yes" ]] || die "aborted"
fi

# ---------------------------------------------------------------------------
info "ADP: agent $AGENT_NAME"
if adp_exists agent get "$AGENT_NAME"; then
  if (( DRY_RUN )); then
    would "stop and delete agent $AGENT_NAME"
  else
    rpk ai agent stop "$AGENT_NAME" >/dev/null 2>&1 || warn "agent stop failed; continuing with delete"
    rpk ai agent delete "$AGENT_NAME" >/dev/null || die "agent delete failed: $AGENT_NAME"
    ok "deleted agent $AGENT_NAME"
  fi
else
  ok "agent $AGENT_NAME not present"
fi
# The saved A2A context id belongs to the deleted agent; drop it in both modes so
# the next `make chat` after `make deploy-agent` starts a fresh conversation.
if (( DRY_RUN == 0 )); then
  rm -f "$STATE_DIR/chat-context"
fi

info "ADP: Cedar policies"
for p in "${POLICIES[@]}"; do
  # Policy resource names carry the policies/ prefix for get/delete.
  if adp_exists policy get "policies/$p"; then
    if (( DRY_RUN )); then
      would "delete policy $p"
    else
      rpk ai policy delete "policies/$p" >/dev/null 2>&1 || rpk ai policy delete "$p" >/dev/null || die "policy delete failed: $p"
      ok "deleted policy $p"
    fi
  else
    ok "policy $p not present"
  fi
done

info "ADP: MCP server $MCP_SERVER_NAME"
if adp_exists mcp-server get "$MCP_SERVER_NAME"; then
  if (( DRY_RUN )); then
    would "delete MCP server $MCP_SERVER_NAME"
  else
    rpk ai mcp-server delete "$MCP_SERVER_NAME" >/dev/null || die "MCP server delete failed: $MCP_SERVER_NAME"
    ok "deleted MCP server $MCP_SERVER_NAME"
  fi
else
  ok "MCP server $MCP_SERVER_NAME not present"
fi

# Providers and guardrail, in dependency order: detach the guardrail from both
# providers, delete the agent's (Anthropic) provider, delete the guardrail, then
# delete the Bedrock provider that the guardrail's backend references.
agent_provider_exists=0
bedrock_provider_exists=0
if [[ "$LLM_PROVIDER" != "$BEDROCK_PROVIDER" ]] && adp_exists llm-provider get "$LLM_PROVIDER"; then
  agent_provider_exists=1
fi
if adp_exists llm-provider get "$BEDROCK_PROVIDER"; then
  bedrock_provider_exists=1
fi

# detach_guardrail <provider>: unset .guardrail on an existing provider.
detach_guardrail() {
  local p="$1" attached
  attached="$(rpk ai llm-provider get "$p" -o json 2>/dev/null | jq -r '.guardrail // ""' 2>/dev/null || true)"
  if [[ -z "$attached" ]]; then
    ok "no guardrail attached to $p"
    return 0
  fi
  if (( DRY_RUN )); then
    would "detach guardrail $attached from provider $p"
    return 0
  fi
  # `--clear guardrail` is rpk's documented way to unset a field; the empty
  # string with an explicit update mask is the fallback for older rpk builds.
  if rpk ai llm-provider update "$p" --clear guardrail >/dev/null 2>&1 \
     || rpk ai llm-provider update "$p" --guardrail "" --update-mask guardrail >/dev/null 2>&1; then
    ok "detached guardrail $attached from $p"
  else
    warn "could not detach the guardrail from $p; continuing"
  fi
}

# delete_provider <provider>
delete_provider() {
  local p="$1"
  if (( DRY_RUN )); then
    would "delete LLM provider $p"
  elif rpk ai llm-provider delete "$p" >/dev/null; then
    ok "deleted LLM provider $p"
  else
    warn "LLM provider delete failed: $p (is a guardrail still attached to it, or does a guardrail still reference it as backend?)"
    LEFTOVERS+=("LLM provider $p")
  fi
}

info "ADP: detach guardrail from the providers"
if (( agent_provider_exists )); then
  detach_guardrail "$LLM_PROVIDER"
fi
if (( bedrock_provider_exists )); then
  detach_guardrail "$BEDROCK_PROVIDER"
fi

info "ADP: LLM provider $LLM_PROVIDER (agent)"
if [[ "$LLM_PROVIDER" == "$BEDROCK_PROVIDER" ]]; then
  ok "LLM_PROVIDER is the Bedrock provider; it is deleted after the guardrail"
elif (( agent_provider_exists )); then
  delete_provider "$LLM_PROVIDER"
else
  ok "LLM provider $LLM_PROVIDER not present"
fi

info "ADP: guardrail $GUARDRAIL_NAME"
gr_body="$(jq -n --arg n "$GUARDRAIL_NAME" '{name: $n}')"
# GetGuardrail returns {"guardrail":{"name":...}} (camelCase keys) on success and
# HTTP 404 with {"code":"not_found",...} when absent (both verified live); any other
# failure (auth, 5xx) must stop the teardown rather than read as "not present".
out="$(RP_API_OK_CODES=404 aigw_rpc GuardrailService/GetGuardrail "$gr_body")" || die "GetGuardrail failed: ${out:0:300}"
if jq -e --arg n "$GUARDRAIL_NAME" '(.name // .guardrail.name) == $n' <<<"$out" >/dev/null 2>&1; then
  if (( DRY_RUN )); then
    would "delete guardrail $GUARDRAIL_NAME"
  elif out="$(RP_API_OK_CODES=404 aigw_rpc GuardrailService/DeleteGuardrail "$gr_body")"; then
    ok "deleted guardrail $GUARDRAIL_NAME"
  else
    warn "DeleteGuardrail failed: ${out:0:300}"
    LEFTOVERS+=("guardrail $GUARDRAIL_NAME")
  fi
else
  ok "guardrail $GUARDRAIL_NAME not present"
fi

info "ADP: LLM provider $BEDROCK_PROVIDER (guardrail backend)"
if (( bedrock_provider_exists )); then
  delete_provider "$BEDROCK_PROVIDER"
else
  ok "LLM provider $BEDROCK_PROVIDER not present"
fi

# ---------------------------------------------------------------------------
info "Redpanda Connect pipelines"
list_pipelines() {
  local token="" out
  while :; do
    out="$(rp_api GET "/v1/redpanda-connect/pipelines?page_size=100${token:+&page_token=$(urlenc "$token")}")" || return 1
    jq -c '.pipelines[]?' <<<"$out"
    token="$(jq -r '.next_page_token // ""' <<<"$out")"
    [[ -n "$token" ]] || break
  done
}
pipes="$(list_pipelines)" || die "GET /v1/redpanda-connect/pipelines failed"
for p in "${PIPELINES[@]}"; do
  ids="$(jq -r --arg n "$p" 'select(.display_name == $n) | .id' <<<"$pipes")"
  if [[ -z "$ids" ]]; then
    ok "pipeline $p not present"
    continue
  fi
  # shellcheck disable=SC2086  # one id per line
  for id in $ids; do
    if (( DRY_RUN )); then
      would "stop and delete pipeline $p ($id)"
      continue
    fi
    out="$(RP_API_OK_CODES="400 404 409 412" rp_api PUT "/v1/redpanda-connect/pipelines/$id/stop")" || warn "stop pipeline $p ($id) failed: ${out:0:200}"
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/redpanda-connect/pipelines/$id")" || die "delete pipeline $p ($id) failed: ${out:0:300}"
    ok "deleted pipeline $p ($id)"
  done
done
if (( DRY_RUN == 0 )); then
  rm -f "$STATE_DIR/pipelines.json"
fi

# ---------------------------------------------------------------------------
if (( ALL )); then
  info "CockroachDB changefeeds"
  cf_where="status IN ('running','paused','pending','pause-requested','retrying') AND (description LIKE '%${TOPIC_MEMORY_CDC}%' OR description LIKE '%${TOPIC_ACCOUNTS_CDC}%' OR sink_uri LIKE '%${TOPIC_MEMORY_CDC}%' OR sink_uri LIKE '%${TOPIC_ACCOUNTS_CDC}%')"
  n="$(crdb_admin_psql "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE $cf_where" -At 2>/dev/null || echo 0)"
  if [[ "$n" =~ ^[0-9]+$ ]] && (( n > 0 )); then
    if (( DRY_RUN )); then
      would "cancel $n changefeed job(s)"
    else
      crdb_admin_psql "CANCEL JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE $cf_where)" >/dev/null
      ok "cancelled $n changefeed job(s)"
    fi
  else
    ok "no running changefeeds for the demo topics"
  fi

  info "Topics"
  for t in "$TOPIC_MEMORY_CDC" "$TOPIC_ACCOUNTS_CDC"; do
    if (( DRY_RUN )); then
      would "DELETE /v1/topics/$t"
      continue
    fi
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/topics/$(urlenc "$t")")" || die "delete topic $t failed: ${out:0:300}"
    ok "deleted topic $t (or it was already gone)"
  done

  info "ACLs for User:$KAFKA_USER"
  delete_acl() {  # <resource_type> <resource_name> <pattern_type> <operation>
    local rtype="$1" rname="$2" rpat="$3" op="$4" q out
    q="filter.resource_type=$rtype&filter.resource_name=$(urlenc "$rname")&filter.resource_pattern_type=$rpat"
    q="$q&filter.principal=$(urlenc "User:$KAFKA_USER")&filter.host=$(urlenc '*')&filter.operation=$op&filter.permission_type=PERMISSION_TYPE_ALLOW"
    if (( DRY_RUN )); then
      would "DELETE /v1/acls ($rtype $rname $rpat $op)"
      return 0
    fi
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/acls?$q")" || die "delete ACL ($rtype $rname $op) failed: ${out:0:300}"
    ok "deleted ACL $rtype $rname ($rpat) $op"
  }
  delete_acl RESOURCE_TYPE_TOPIC   "agent."        RESOURCE_PATTERN_TYPE_PREFIXED OPERATION_ALL
  delete_acl RESOURCE_TYPE_GROUP   "agent-memory"  RESOURCE_PATTERN_TYPE_PREFIXED OPERATION_ALL
  delete_acl RESOURCE_TYPE_CLUSTER "kafka-cluster" RESOURCE_PATTERN_TYPE_LITERAL  OPERATION_DESCRIBE

  info "SASL user $KAFKA_USER"
  if (( DRY_RUN )); then
    would "DELETE /v1/users/$KAFKA_USER"
  else
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/users/$(urlenc "$KAFKA_USER")")" || die "delete user $KAFKA_USER failed: ${out:0:300}"
    ok "deleted user $KAFKA_USER (or it was already gone)"
  fi

  info "Secrets"
  for s in "${SECRETS[@]}"; do
    if (( DRY_RUN )); then
      would "DELETE /v1/secrets/$s"
      continue
    fi
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/secrets/$s")" || die "delete secret $s failed: ${out:0:300}"
    ok "deleted secret $s (or it was already gone)"
  done

  info "CockroachDB objects in $CRDB_DATABASE"
  if (( DRY_RUN )); then
    would "DROP TABLE IF EXISTS memory_audit, transactions, accounts, memories, customers CASCADE; DROP ROLE IF EXISTS $CRDB_AGENT_USER"
  else
    crdb_admin_psql "DROP TABLE IF EXISTS memory_audit, transactions, accounts, memories, customers CASCADE" >/dev/null
    ok "dropped tables"
    role_n="$(crdb_admin_psql "SELECT count(*) FROM [SHOW ROLES] WHERE username = '$CRDB_AGENT_USER'" -At)"
    if [[ "$role_n" != "0" ]]; then
      crdb_admin_psql "REVOKE ALL ON DATABASE $CRDB_DATABASE FROM $CRDB_AGENT_USER" >/dev/null 2>&1 || warn "REVOKE on database failed; continuing"
      crdb_admin_psql "REVOKE ALL ON SCHEMA public FROM $CRDB_AGENT_USER" >/dev/null 2>&1 || warn "REVOKE on schema failed; continuing"
      crdb_admin_psql "DROP OWNED BY $CRDB_AGENT_USER" >/dev/null 2>&1 || warn "DROP OWNED BY failed; continuing"
      crdb_admin_psql "DROP ROLE IF EXISTS $CRDB_AGENT_USER" >/dev/null
      ok "dropped role $CRDB_AGENT_USER"
    else
      ok "role $CRDB_AGENT_USER not present"
    fi
  fi
fi

# ---------------------------------------------------------------------------
if (( ${#LEFTOVERS[@]} > 0 )); then
  warn "teardown incomplete; still present: ${LEFTOVERS[*]} (fix the error above and re-run)"
  exit 1
fi
info "Teardown complete$( (( DRY_RUN )) && printf ' (DRY_RUN, nothing changed)' )"
log ".env was kept (delete it to rotate the generated passwords on the next make preflight)"
