-- cockroach/02-roles.sql
-- Least-privilege role for the agent (ADP SQL MCP server) and the Redpanda
-- Connect pipelines. Contract: docs/DESIGN.md section 5.
--
-- Run as admin with the password passed as a psql variable so the value never
-- lives in a committed file:
--   psql "$CRDB_ADMIN_URL" -v ON_ERROR_STOP=1 -v agent_password='...' -f cockroach/02-roles.sql
-- Idempotent: CREATE ... IF NOT EXISTS, then ALTER resets the password on re-run.

CREATE ROLE IF NOT EXISTS agent_rw WITH LOGIN PASSWORD :'agent_password';
ALTER ROLE agent_rw WITH LOGIN PASSWORD :'agent_password';

GRANT CONNECT ON DATABASE agent_memory TO agent_rw;
GRANT USAGE ON SCHEMA agent_memory.public TO agent_rw;

-- CockroachDB grants CREATE on every database's public schema to the public
-- role by default, which would let agent_rw create tables. Verified live on
-- v26.2.7 and revoked: inside agent_memory the agent holds USAGE only, so
-- CREATE TABLE is refused by the database as well as by the MCP server's
-- blocked pattern.
REVOKE CREATE ON SCHEMA agent_memory.public FROM public;

-- The same default applies to the two built-in databases, which every role may
-- CONNECT to. Without this, a raw agent_rw connection (outside the MCP server)
-- could still CREATE a table in defaultdb or postgres and DROP what it created.
-- Note this is cluster-wide: every non-admin role loses CREATE in
-- defaultdb.public and postgres.public. On this disposable demo cluster
-- agent_rw is the only non-admin role (SHOW ROLES: admin, root, zeke, agent_rw)
-- and both databases hold no tables, so nothing else is affected. Comment this
-- line out if the cluster is ever shared with roles that rely on that default.
REVOKE CREATE ON SCHEMA defaultdb.public, postgres.public FROM public;

-- System of record: read only.
GRANT SELECT ON TABLE customers, accounts, transactions TO agent_rw;

-- Memory: the agent may add and amend (forget = UPDATE expires_at), never DELETE.
GRANT SELECT, INSERT, UPDATE ON TABLE memories TO agent_rw;

-- Audit trail: the Connect pipeline appends here with this same role (contract
-- section 3: one role for agent and pipelines), so agent_rw does hold INSERT on
-- memory_audit. The agent is kept out of it by instruction (system prompt) and
-- by the ADP transcript, not by a privilege; nobody holds UPDATE or DELETE.
GRANT SELECT, INSERT ON TABLE memory_audit TO agent_rw;

-- Deliberately NO DELETE anywhere, NO write on accounts/customers/transactions,
-- NO CREATE in any schema this role can reach.
-- Row-level TTL does the only deleting on memories, as the system, by policy.
