# Architecture

Component by component, then the path of one memory write, the memory model, the identity model, what is honestly weak, and where it can go next. Names, schemas and verified API shapes are fixed in `DESIGN.md`; this document explains them.

## Components

| Component | Name | Role |
|---|---|---|
| CockroachDB Cloud database | `agent_memory` | System of record (`customers`, `accounts`, `transactions`), long-term memory (`memories`) and its history (`memory_audit`) |
| CockroachDB role | `agent_rw` | Least privilege for the agent and pipelines: SELECT on account tables, SELECT/INSERT/UPDATE on `memories`, SELECT/INSERT on `memory_audit`, no DELETE anywhere |
| Changefeeds | on `memories` and `accounts` | Emit JSON `{before, after, updated}` envelopes with `resolved` markers to Redpanda over TLS + SASL |
| Redpanda topics | `agent.memory.cdc`, `agent.accounts.cdc` | 3 partitions, 7 day retention |
| Connect pipeline | `agent-memory-audit` | Consumes `agent.memory.cdc`, writes one `memory_audit` row per row version, idempotent on `(memory_id, updated_hlc)` |
| Connect pipeline | `agent-memory-observer` | Consumes `agent.accounts.cdc`, turns balance transitions into `observation` memories, idempotent on `(subject, content_hash)` |
| ADP LLM provider (primary) | `zeke-anthropic-agent-memory` | Anthropic API through the org-shared secret `ANTHROPIC_API_KEY`; models `claude-sonnet-4-6` (the agent's) and `claude-haiku-4-5`; transcripts on; guardrail attached and evaluated by the gateway on every input and output; bills the org Anthropic key |
| ADP LLM provider (alternative) | `zeke-bedrock-agent-memory` | Bedrock in `us-east-2` on Zeke's AWS credentials; model `us.anthropic.claude-sonnet-4-6`; transcripts on; the guardrail's Bedrock backend; guardrail attached but enforced by Bedrock only on `invoke`, never on the agent's `converse-stream` (see Limitations); bills Zeke's AWS account |
| ADP guardrail | `agent-memory-guardrail` | Bedrock-backed word filter, denied topic and PII policy, attached to both providers; on the Anthropic path the AI Gateway evaluates it (`ApplyGuardrail`) on input before forwarding and on output before delivery |
| ADP MCP server | `crdb-agent-memory` | Managed SQL MCP (driver `postgres`) over the `agent_rw` DSN; tools `query` and `execute`; regex blocklist; data policy |
| ADP Cedar policies | `memory-concierge-mcp`, `memory-concierge-llm` | The agent may call this MCP server and invoke the two demo providers, nothing else |
| ADP agent | `memory-concierge` | Runs on `zeke-anthropic-agent-memory`, model `claude-sonnet-4-6`; system prompt with the schema and the memory protocol; `max_iterations: 16` |
| Transcripts and audit log | ADP | Per-conversation messages and tool calls (a guardrail block is a failed turn); per-event Cedar decisions, and guardrail decisions when the gateway evaluated them (Anthropic path) |
| Human | `rpk ai agent a2a send` via `scripts/chat.sh` | Authenticated with `rpk ai auth login`; the gateway derives `{{.User.Email}}` from that identity |

### CockroachDB

Four design choices matter. `memories.subject` is the caller's verified email, so the identity key is the same string on both sides of the MCP boundary. `accounts.owner_email` is denormalised so an accounts CDC event carries the memory subject without a join inside the pipeline. `memories.content_hash` is a stored computed column (`md5(lower(subject) || '|' || lower(content))`) with a UNIQUE constraint, which makes memory writes idempotent for both the agent and the observer pipeline. `memories` has row-level TTL on `expires_at` (`ttl_job_cron = '@hourly'`), so forgetting is a policy, not a privilege.

The `memory_audit` table is written by the audit pipeline; the agent's role can also INSERT into it, because the agent and the pipeline share `agent_rw`, but nobody can UPDATE or DELETE it (see Limitations). Its key `(memory_id, updated_hlc)` is the CockroachDB MVCC timestamp of the row version, so replaying the topic from the beginning produces no duplicates.

### Changefeeds

Both changefeeds are created with `WITH updated, diff, resolved = '10s', min_checkpoint_frequency = '5s', format = json`. `diff` gives the `before` image, `updated` gives the HLC, and `resolved` markers (`{"resolved": "<hlc>"}`) arrive periodically and are dropped by consumers. Inserts have `before: null`, deletes have `after: null`, and TTL purges look like ordinary deletes. The Kafka key is the primary key as a JSON array (`["<uuid>"]` for memories, `[1001]` for accounts).

### Connect pipelines

Both run as managed pipelines in Redpanda Cloud (`cpu_shares: 100m`, `memory_shares: 400M`) or locally with `rpk connect streams` (`make pipelines-local`). Brokers come from the contextual `${REDPANDA_BROKERS}`, credentials from `${secrets.AGENT_MEMORY_KAFKA_USER}`, `${secrets.AGENT_MEMORY_KAFKA_PASSWORD}` and `${secrets.AGENT_MEMORY_CRDB_DSN}`. Both batch `count: 100, period: 1s` with `max_in_flight: 2`, commit offsets every second, start from `earliest`, expose Prometheus metrics, and write with `sql_raw` (driver `postgres`).

`agent-memory-audit` classifies the op from the presence of `before` and `after`, takes `memory_id` from the Kafka key, takes `subject` from `after` falling back to `before`, and inserts with `ON CONFLICT (memory_id, updated_hlc) DO NOTHING`.

`agent-memory-observer` keeps only update events on `agent.accounts.cdc` and emits a memory when `before.balance >= 0 && after.balance < 0` (overdraft) or `after.balance - before.balance >= 5000` (large deposit). Everything else is `deleted()`. The content is one third-person sentence with the account nickname and the two balances, no account number, and `source_ref` is `accounts/<account_id>@<updated_hlc>`. DECIMALs arrive as strings or numbers depending on version, so the mapping uses `.number()`.

### ADP

The agent is a manifest (`adp/agent.yaml`, rendered from `adp/agent.template.yaml` plus `adp/system-prompt.md`, with `LLM_PROVIDER` and `LLM_MODEL` substituted from `.env`) applied with `rpk ai agent apply`. It names one LLM provider, one model and one MCP server. The gateway in front of it enforces the Cedar policies on every `McpServer.*`, `McpServerTool.call` and `LLMProvider.invoke` action; the guardrail attached to the provider is evaluated by the gateway on every input before it is forwarded and on every output before it is delivered (on the Anthropic path the agent uses by default; the Bedrock path differs, see Limitations); the MCP server applies `blocked_patterns`, row limits and the query timeout to every SQL statement (the `allowed_schemas` setting is declared but not enforced by the managed SQL MCP server; schemas are restricted by the `agent_rw` grants), and the data policy reshapes `query` responses (`tax_id` redacted, `account_number` keeps the last four characters) and clamps `max_rows` requests to 100. Transcripts (`--transcripts.record-input-messages`, `--transcripts.record-output-messages`) and the audit log are platform features, not something the prompt asks for.

## One memory write, end to end

The user says "I'm saving for a house down payment by next June" in an existing conversation.

```mermaid
sequenceDiagram
  autonumber
  participant U as Presenter (rpk ai agent a2a send)
  participant GW as AI Gateway (identity, Cedar)
  participant AG as Agent memory-concierge
  participant GR as Guardrail agent-memory-guardrail (evaluated by the gateway, Bedrock ApplyGuardrail backend)
  participant LLM as Claude Sonnet 4.6 via the Anthropic API (provider zeke-anthropic-agent-memory)
  participant MCP as SQL MCP crdb-agent-memory
  participant DB as CockroachDB memories
  participant CF as CHANGEFEED memories
  participant K as Redpanda agent.memory.cdc
  participant P as Connect agent-memory-audit
  participant AU as CockroachDB memory_audit

  U->>GW: A2A message, existing context id
  GW->>AG: message with User.Email = zeke.dean@redpanda.com
  AG->>GW: LLM call: system prompt (schema, memory protocol, subject) + history + tools
  GW->>GW: Cedar: LLMProvider.invoke on zeke-anthropic-agent-memory, permit
  GW->>GR: input check (word filter, denied topics, PII) before forwarding
  GR-->>GW: pass
  GW->>LLM: forward to Anthropic (org API key attached by the gateway)
  LLM-->>GW: tool call execute(statement: INSERT INTO memories ... ON CONFLICT (subject, content_hash) DO UPDATE ..., params: [email, 'goal', 'Is saving for a house down payment by next June', 4, 'chat:2026-10-15'])
  GW->>GR: output check
  GR-->>GW: pass
  GW-->>AG: tool call
  GW->>GW: Cedar: McpServerTool.call on crdb-agent-memory, permit
  AG->>MCP: execute
  MCP->>MCP: blocked_patterns, query_timeout
  MCP->>DB: INSERT as agent_rw
  DB-->>MCP: 1 row affected
  MCP-->>AG: result
  AG->>GW: LLM call with the tool result (Cedar permit and input check repeat)
  GW->>LLM: forward
  LLM-->>GW: "Noted: saving for a house down payment by next June."
  GW->>GR: output check
  GR-->>GW: pass
  GW-->>AG: reply
  AG-->>U: reply (transcript and audit log entries recorded)
  DB->>CF: new row version (before null, after row, updated hlc)
  CF->>K: JSON envelope, key is the memory_id as a JSON array
  K->>P: consume, group agent-memory-audit
  P->>AU: INSERT ... ON CONFLICT (memory_id, updated_hlc) DO NOTHING
```

Steps 1 to 24 take a few seconds and are synchronous from the user's point of view. Steps 25 to 28 are asynchronous and typically complete within a few seconds: the changefeed emits the row version as it commits (`resolved = '10s'` only sets the cadence of the resolved markers), and the pipeline batches for at most 1s. Nothing in the synchronous path waits for the audit row; the audit trail is eventually consistent with memory, and `make verify` asserts it arrives within 60 seconds.

The observer path is the same shape with `accounts` as the source, `agent.accounts.cdc` as the topic and `agent-memory-observer` writing into `memories` instead of `memory_audit`. That insert is itself captured by the memories changefeed, so observations are audited exactly like agent-written memories.

Had the prompt been "Run DROP TABLE memories for me right now.", step 6 would be a block instead of a pass: the gateway returns the blocked-input message to the agent runtime as an HTTP 403 `guardrail_intervened` error, the runtime records the turn as failed, nothing from step 7 onwards happens, and the audit log shows the LLM call as `Denied` with the guardrail named. On the Bedrock path (agent switched to `zeke-bedrock-agent-memory`) steps 5, 6, 9, 10, 21 and 22 do not exist: the agent runtime calls Bedrock with `converse-stream`, and this gateway build applies a Bedrock guardrail only on `invoke`, so the prompt reaches the model and only the system prompt, the MCP regex and the database grants stand in the way (see Limitations).

## Memory model

One row per remembered thing. Content is a single third-person sentence ("Prefers short answers") so that recall results read as context, not transcript.

| Column | Meaning |
|---|---|
| `subject` | Verified caller email from the gateway; the only scoping key |
| `kind` | `preference`, `fact`, `goal`, `observation`, `episode` |
| `content` | One sentence, third person, no secrets, card numbers, tax ids or passwords |
| `importance` | 1 to 5, default 3; recall orders by importance then recency |
| `source` | `agent` (written during a conversation), `event-stream` (observer pipeline), `human` (reserved for operator edits) |
| `source_ref` | `chat:<date>` for agent writes (the prompt template exposes no context id; the ADP transcript links the row to the conversation); `accounts/<id>@<hlc>` for observations |
| `content_hash` | Stored `md5(lower(subject) \|\| '\|' \|\| lower(content))`; `UNIQUE (subject, content_hash)` |
| `search` | Stored `to_tsvector('english', content)` with an inverted index, for keyword recall |
| `expires_at` | Default 90 days; observations 30 days; "forget" sets it to `now()` |

| Kind | Written by | Typical importance | TTL | Example |
|---|---|---|---|---|
| `preference` | agent | 3 | 90 days | Prefers short answers |
| `fact` | agent | 3 | 90 days | Has two children in school |
| `goal` | agent | 4 | 90 days | Is saving for a house down payment by next June |
| `observation` | observer pipeline | 4 | 30 days | Account 'Everyday Checking' went overdrawn on 2026-10-15: balance moved from 2450.00 to -450.00 USD |
| `episode` | agent | 2 to 3 | 90 days | Reserved for conversation summaries |

**Recall.** First turn of every conversation: `SELECT memory_id, kind, content, importance, source, created_at FROM memories WHERE subject = $1 AND expires_at > now() ORDER BY importance DESC, created_at DESC LIMIT 20`, served by the covering index `idx_memories_recall`. Observations with `source = 'event-stream'` are surfaced proactively ("Since we last spoke...").

**Store.** `INSERT ... ON CONFLICT (subject, content_hash) DO UPDATE SET importance = excluded.importance, updated_at = now(), expires_at = now() + INTERVAL '90 days'`. Restating a memory refreshes it instead of duplicating it. The observer uses the same conflict target with `DO NOTHING`.

**Forget.** `UPDATE memories SET expires_at = now(), updated_at = now() WHERE subject = $1 AND memory_id = $2`. The row is invisible to recall immediately and is removed by the hourly TTL job later. Both the update and the TTL delete appear in `memory_audit`, so the history records that the memory existed, when it was forgotten and when it was physically removed.

**Audit.** `memory_audit` keeps `before_doc` and `after_doc` as JSONB for every row version, with `op` in `insert`, `update`, `delete`. "Show me the log" runs `SELECT op, subject, after_doc->>'content' AS content, audited_at FROM memory_audit WHERE subject = $1 ORDER BY audited_at DESC LIMIT 20`.

## Identity model

The presenter authenticates with `rpk ai auth login` (device flow). The gateway attaches the authenticated principal to every A2A call, and the agent's Go-template system prompt receives it as `{{.User.Email}}` (`{{if .User.Email}}You are talking to {{.User.Email}}.{{end}}`). That email is the subject of every memory query and the `owner_email` of every account query. The seed data has three customers so the scoping is testable: `zeke.dean@redpanda.com`, `alice@example.com`, `bob@example.com`.

What stops what, and where:

| Rule | Prompt | Platform policy | Database |
|---|---|---|---|
| Memory subject is the caller's email | Yes (instruction) | No | No: `agent_rw` can read every subject |
| Account queries only `WHERE owner_email = caller` | Yes (instruction) | No | No: `agent_rw` can read every owner |
| No destructive SQL | Yes | Guardrail word filter, evaluated by the gateway on the Anthropic path (not through the agent on the Bedrock path); MCP `blocked_patterns` | Yes: no DROP, DELETE, DDL or GRANT privilege |
| No writes to the system of record | Yes | No: `readonly: false`, and `blocked_patterns` match neither INSERT nor UPDATE; `allowed_schemas` is not enforced (vendor limitation), so the database column is the only schema fence | Yes: SELECT-only on `customers`, `accounts`, `transactions` |
| No investment advice | Yes | Guardrail denied topic (input and output), Anthropic path | n/a |
| Sensitive columns masked | Explains the masking | Data policy on `query` responses | No: the DB returns clear values to the MCP server |
| SSNs and card numbers never leak through chat | Yes (never store them) | Guardrail PII filter: on the Anthropic path an input match blocks the turn and an output match is anonymized; on the Bedrock `invoke` path both are anonymized in place (`{US_SOCIAL_SECURITY_NUMBER}`); the agent's Bedrock `converse-stream` path is not covered | No |
| Agent talks only to this MCP server and the two demo LLM providers | n/a | Cedar `memory-concierge-mcp`, `memory-concierge-llm` (`when { resource in [...] }` over both providers) | n/a |
| Everything is recorded | n/a | Transcripts (a guardrail block is a failed turn); audit log (`Denied` with the guardrail named when the gateway evaluated it) | `memory_audit` via CDC |
| Audit rows come only from CDC | Yes ("you only read it") | Transcript shows any direct INSERT | No: `agent_rw` can INSERT into `memory_audit` (never UPDATE or DELETE) |

The first two rows are the honest gap: subject isolation is a prompt instruction backed by nothing below the model. The last row is a smaller one: the audit table is append-only but not agent-proof. See Limitations.

## Limitations

- **SQL MCP scoping is prompt-level.** The MCP server connects as one database role for every caller. The prompt says "never query another subject", but a successful prompt injection could read Alice's accounts or memories. The fixes live below the model: a per-caller database role or connection, a view or policy that pins `subject` to the session identity, or a custom MCP tool that takes no `sql` argument and injects the subject server-side. CockroachDB row-level security policies are a candidate on this version; verify on the cluster before relying on them.
- **The data policy masks values, not row existence.** `tax_id` becomes `[redacted]` and `account_number` keeps four characters, but counts, joins, `WHERE account_number LIKE ...` predicates and ordering still operate on the clear value inside the database. It applies to `query` responses only; `execute` returns affected row counts. It protects the transcript and the model's context, not the database.
- **The guardrail does not fire through the agent on the Bedrock provider.** Observed on 2026-10-15 on `adp-production`; platform behaviour that may change. The managed agent runtime calls Bedrock with the Converse API (`model/<id>/converse-stream` in the audit log), and this gateway build injects a Bedrock guardrail only on `model/<id>/invoke`. With the agent on `zeke-bedrock-agent-memory`, destructive SQL, investment advice and PII reach the model and are stopped only by the system prompt, the MCP regex and the database grants. A direct `invoke` through the provider's proxy URL shows Bedrock intervening (`amazon-bedrock-guardrailAction: INTERVENED`, PII masked to `{US_SOCIAL_SECURITY_NUMBER}`); the same text via `converse` passes untouched. This is why the agent runs on the Anthropic provider by default.
- **Guardrails are Bedrock-backed and, on the Anthropic path, gateway-evaluated.** The word filter is a literal list (a tripwire, not a SQL parser); the denied-topic classifier is probabilistic and adds a Bedrock `ApplyGuardrail` round trip per input and per output; prompt and response text leave for Bedrock in `us-east-2` even though the model call goes to Anthropic; provisioning depends on Bedrock permissions; changes take about 30 seconds to propagate. PII anonymize-on-input short-circuits like a block on non-Bedrock providers (documented behaviour), so the PII beat shows the blocked message rather than a masked prompt. Evaluation fails closed. The MCP regex and the database grants are the deterministic stops.
- **Two providers, two bills, two kinds of key.** The Anthropic path bills the org Anthropic key (`ANTHROPIC_API_KEY`, an org-shared secret also used by the org provider `anthropic-api-key`); the Bedrock path bills Zeke's own AWS account. The guardrail's backend is the Bedrock provider, so that provider must exist even while the agent uses the Anthropic one, and deleting it breaks the guardrail on both paths.
- **The audit trail is eventually consistent.** The agent's reply does not wait for `memory_audit`. A pipeline outage delays the audit row; it does not lose it (offsets are committed after the write and the insert is idempotent), but "what did the agent remember a second ago" must be answered from `memories`, not from `memory_audit`.
- **`memory_audit` is append-only but not agent-proof.** `agent_rw` is shared by the agent and the pipeline, so it carries the pipeline's INSERT grant on `memory_audit`; a prompt injection could append a fabricated history row through `execute`. It could not alter or delete real ones (no UPDATE or DELETE grant, and `blocked_patterns` rejects DELETE), and the ADP transcript would show the call. The fix is below the model: a separate `audit_rw` role for the pipeline and SELECT-only on `memory_audit` for the agent.
- **Observations are rule-based and coarse.** Two rules, evaluated per update event. The content sentence embeds the date and both balances, so a balance that oscillates around zero produces one observation per distinct (day, before balance, after balance) transition; only an identical sentence, including redelivery of the same event, is absorbed by `ON CONFLICT DO NOTHING` on the content hash.
- **Forgetting is not instant removal.** An expired row stays on disk until the hourly TTL job runs. Recall hides it immediately; backups and the CDC topic (7 day retention) still contain it.
- **The agent confirms what the tool reported.** The prompt requires every claim to come from a tool result, but a model can still paraphrase a `1 row affected` wrongly. The transcript shows the actual call and result.
- **Identity comes from a developer login.** `rpk ai auth login` is an interactive device flow for the presenter. A production surface (web, Slack, IVR) needs its own authentication in front of the gateway so `{{.User.Email}}` is the end customer, not the operator.
- **Single region.** The demo cluster runs in `aws-us-east-1`; the region-failure survivability in the pitch is CockroachDB's, not something this demo exercises (see Multi-region CockroachDB under Extension ideas).

## Extension ideas

| Idea | What changes |
|---|---|
| Vector memory | Add an embedding column (`VECTOR` works on this cluster) to `memories`, populate it from a Connect pipeline on `agent.memory.cdc`, and recall by similarity instead of, or alongside, `importance` ordering and the `tsvector` index |
| Iceberg export of transcripts and memory history | Mirror `agent.memory.cdc` (and ADP transcripts, if exported to a topic) into Iceberg tables for long-term retention and offline evaluation outside the operational database |
| Slack trigger | A third Connect pipeline on `agent.memory.cdc` that filters `after.kind == 'observation' && after.importance >= 4` and posts to Slack, turning the memory stream into an alert feed without touching the agent |
| Multi-region CockroachDB | Make `memories` regional by row on the customer's home region so recall is local and survivable; the changefeed and audit pipeline do not change |
| Richer observers | More rules on `transactions` and `accounts` (velocity, new merchant, geography) for fraud teams; each writes an `observation` with a `source_ref` to the exact row version |
| Evaluation from transcripts | Replay transcripts against the memory table to score recall precision and "unsafe write" rate; the audit table gives the ground truth of what the agent actually stored |
| Hash-chained audit | Add a previous-row hash to `memory_audit` in the pipeline so tampering with history is detectable |
