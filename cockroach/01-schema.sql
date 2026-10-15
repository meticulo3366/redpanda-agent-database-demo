-- cockroach/01-schema.sql
-- Agent memory on CockroachDB: schema for the Memory Concierge demo.
-- Contract: docs/DESIGN.md section 5. All DDL is idempotent and lives in
-- agent_memory.public. Run as the admin user (scripts/10-cockroach-setup.sh).
--
-- Conventions (CockroachDB best practice): explicit primary keys, UUID keys
-- for high-volume insert tables, DECIMAL for money, TIMESTAMPTZ for time,
-- covering indexes with STORING, CHECK constraints.

-- customers: the people. tax_id is deliberately sensitive (masked by data policy).
CREATE TABLE IF NOT EXISTS customers (
  customer_id UUID        NOT NULL DEFAULT gen_random_uuid(),
  email       STRING      NOT NULL,                 -- identity key used by the agent
  full_name   STRING      NOT NULL,
  segment     STRING      NOT NULL DEFAULT 'retail', -- retail | premier | business
  tax_id      STRING,                               -- SENSITIVE
  phone       STRING,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_customers PRIMARY KEY (customer_id),
  CONSTRAINT uq_customers_email UNIQUE (email)
);

-- accounts: system of record. owner_email is denormalised so the CDC event
-- carries the memory subject without a join.
CREATE TABLE IF NOT EXISTS accounts (
  account_id      INT8          NOT NULL,
  customer_id     UUID          NOT NULL REFERENCES customers (customer_id),
  owner_email     STRING        NOT NULL,
  account_number  STRING        NOT NULL,            -- SENSITIVE, partially masked
  nickname        STRING        NOT NULL,
  account_type    STRING        NOT NULL,            -- checking | savings | credit
  balance         DECIMAL(14,2) NOT NULL DEFAULT 0,
  currency        STRING        NOT NULL DEFAULT 'USD',
  overdraft_limit DECIMAL(14,2) NOT NULL DEFAULT 0,
  status          STRING        NOT NULL DEFAULT 'open',
  updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_accounts PRIMARY KEY (account_id),
  CONSTRAINT chk_account_type CHECK (account_type IN ('checking','savings','credit')),
  CONSTRAINT chk_status CHECK (status IN ('open','frozen','closed'))
);
CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts (owner_email) STORING (nickname, account_type, balance, currency, status);

-- transactions: recent activity, so "why am I overdrawn" has an answer.
CREATE TABLE IF NOT EXISTS transactions (
  txn_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
  account_id  INT8          NOT NULL REFERENCES accounts (account_id),
  amount      DECIMAL(14,2) NOT NULL,                -- negative = debit
  merchant    STRING        NOT NULL,
  category    STRING        NOT NULL,
  occurred_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_transactions PRIMARY KEY (txn_id)
);
CREATE INDEX IF NOT EXISTS idx_txn_account_time ON transactions (account_id, occurred_at DESC) STORING (amount, merchant, category);

-- memories: the agent's long-term memory. One row per remembered thing.
-- content_hash makes "the same memory stated twice" an upsert, not a duplicate
-- (ON CONFLICT (subject, content_hash) DO UPDATE ...). search powers full-text
-- recall through the inverted index. Row-level TTL forgets by policy: rows are
-- purged once expires_at passes, and that delete is also emitted by the changefeed.
CREATE TABLE IF NOT EXISTS memories (
  memory_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
  subject      STRING      NOT NULL,   -- verified caller email, from the gateway
  kind         STRING      NOT NULL,   -- preference | fact | goal | observation | episode
  content      STRING      NOT NULL,   -- one sentence, third person ("Prefers short answers")
  importance   INT2        NOT NULL DEFAULT 3,
  source       STRING      NOT NULL DEFAULT 'agent',  -- agent | event-stream | human
  source_ref   STRING,                 -- A2A context id, or CDC key/HLC for observations
  content_hash STRING      NOT NULL AS (md5(lower(subject) || '|' || lower(content))) STORED,
  search       TSVECTOR    NOT NULL AS (to_tsvector('english', content)) STORED,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '90 days',
  CONSTRAINT pk_memories PRIMARY KEY (memory_id),
  CONSTRAINT uq_memories_subject_content UNIQUE (subject, content_hash),
  CONSTRAINT chk_kind CHECK (kind IN ('preference','fact','goal','observation','episode')),
  CONSTRAINT chk_importance CHECK (importance BETWEEN 1 AND 5),
  CONSTRAINT chk_source CHECK (source IN ('agent','event-stream','human'))
) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly');
CREATE INDEX IF NOT EXISTS idx_memories_recall ON memories (subject, expires_at, importance DESC, created_at DESC) STORING (kind, content, source);
CREATE INVERTED INDEX IF NOT EXISTS idx_memories_search ON memories (search);

-- memory_audit: append-only history materialised from the CDC stream by Redpanda Connect.
CREATE TABLE IF NOT EXISTS memory_audit (
  audit_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
  memory_id   UUID        NOT NULL,     -- from the Kafka message key
  subject     STRING,                   -- after.subject, else before.subject
  op          STRING      NOT NULL,     -- insert | update | delete
  before_doc  JSONB,
  after_doc   JSONB,
  updated_hlc STRING      NOT NULL,     -- changefeed "updated" MVCC timestamp
  audited_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_memory_audit PRIMARY KEY (audit_id),
  CONSTRAINT uq_memory_audit_event UNIQUE (memory_id, updated_hlc),
  CONSTRAINT chk_audit_op CHECK (op IN ('insert','update','delete'))
);
CREATE INDEX IF NOT EXISTS idx_memory_audit_subject ON memory_audit (subject, audited_at DESC) STORING (op, memory_id);
