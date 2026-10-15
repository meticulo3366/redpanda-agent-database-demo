#!/usr/bin/env bash
# scripts/10-cockroach-setup.sh
# Applies the CockroachDB side of the demo, as the admin user, in order:
#   cockroach/01-schema.sql   tables, indexes, row-level TTL     (IF NOT EXISTS)
#   cockroach/02-roles.sql    least-privilege role agent_rw      (password from .env)
#   cockroach/03-seed.sql     customers, accounts, transactions  (ON CONFLICT DO NOTHING,
#                             then re-anchors the seeded ledger to the last 30 days)
# then prints row counts and proves agent_rw can log in.
# Contract: docs/DESIGN.md sections 5 and 8. Idempotent: safe to re-run.
#
# Usage: scripts/10-cockroach-setup.sh        (make setup-db)
# Needs .env from scripts/00-preflight.sh (CRDB_ADMIN_URL, CRDB_AGENT_PASSWORD ...).
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql

SQL_DIR="$REPO_ROOT/cockroach"
: "${CRDB_ADMIN_URL:?CRDB_ADMIN_URL missing from .env (run: make preflight)}"
: "${CRDB_AGENT_PASSWORD:?CRDB_AGENT_PASSWORD missing from .env (run: make preflight)}"
CRDB_DATABASE="${CRDB_DATABASE:-agent_memory}"
CRDB_AGENT_USER="${CRDB_AGENT_USER:-agent_rw}"

for f in 01-schema.sql 02-roles.sql 03-seed.sql; do
  [[ -f "$SQL_DIR/$f" ]] || die "missing $SQL_DIR/$f"
done

info "Applying schema ($SQL_DIR/01-schema.sql)"
crdb_admin_psql_file "$SQL_DIR/01-schema.sql"
ok "schema applied to $CRDB_DATABASE"

info "Applying role and grants for $CRDB_AGENT_USER ($SQL_DIR/02-roles.sql)"
# The password travels as a psql variable; it is never written to a committed file.
crdb_admin_psql_file "$SQL_DIR/02-roles.sql" -v agent_password="$CRDB_AGENT_PASSWORD"
ok "role $CRDB_AGENT_USER ready (SELECT on system of record, INSERT/UPDATE on memories, INSERT on memory_audit, no DELETE)"

info "Applying seed data ($SQL_DIR/03-seed.sql)"
crdb_admin_psql_file "$SQL_DIR/03-seed.sql"
ok "seed applied (seeded transactions re-anchored so the newest is ~1 day old)"

info "Row counts in $CRDB_DATABASE"
crdb_admin_psql "
SELECT 'customers'    AS table_name, count(*) AS row_count FROM customers
UNION ALL SELECT 'accounts',     count(*) FROM accounts
UNION ALL SELECT 'transactions', count(*) FROM transactions
UNION ALL SELECT 'memories',     count(*) FROM memories
UNION ALL SELECT 'memory_audit', count(*) FROM memory_audit
ORDER BY 1;"

info "Checking that $CRDB_AGENT_USER can log in"
# psql on this machine has no ~/.postgresql/root.crt, so verify-full is rewritten
# to require for this check only; the stored DSN keeps sslmode=verify-full.
agent_url="$(crdb_agent_url | sed 's/sslmode=verify-full/sslmode=require/')"
who="$(psql "$agent_url" -X -q -v ON_ERROR_STOP=1 -At -c 'SELECT current_user' 2>&1)" \
  || die "$CRDB_AGENT_USER cannot connect: $who (a fresh .env without a re-run of this script leaves the live password stale; re-run make setup-db)"
[[ "$who" == "$CRDB_AGENT_USER" ]] || die "connected as '$who', expected '$CRDB_AGENT_USER'"
ok "$CRDB_AGENT_USER can connect to $CRDB_DATABASE"

ok "CockroachDB setup complete. Next: make setup-redpanda"
