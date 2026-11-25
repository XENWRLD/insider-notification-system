# Design notes

The long-form reasoning behind the decisions summarised in the
[README](../README.md#design-decisions). Each section states the problem, the choice,
the alternatives that were rejected and — where one exists — the measurement that
settled it.

## 1. PostgreSQL as the work queue

**Choice.** The `notifications` table *is* the queue. Workers claim with
`FOR UPDATE SKIP LOCKED` over a partial index that only covers claimable rows.

**Why.** With a broker, accepting a request means writing a row *and* publishing a
message. Those are two systems, so either the publish happens outside the transaction
(and a crash in between loses the message or delivers a message for a row that does not
exist) or an outbox table plus a relay process is built — which is a queue in PostgreSQL
with extra moving parts. Here the row and its queue entry are the same write: if the API
answered `202`, the work is queued.

**What it costs.** Throughput is bounded by one primary's write capacity, and every
claim is a short write transaction. At the scale in the brief (millions/day ≈ hundreds
per second) this is comfortable; the breaking point is roughly where a single primary's
write path saturates.

**The migration path**, if that day comes: keep the table as the source of truth, add an
outbox-style relay that publishes claimable rows to Kafka/RabbitMQ, and let workers
consume from the broker while still writing results to PostgreSQL. The state machine,
the attempt history and the API stay unchanged — only the transport between "row is
ready" and "worker picks it up" moves.

**Index design.** The claim index is
`(channel, priority_rank, available_at, id) WHERE status IN ('pending','retrying')`.
The column order mirrors the query (equality, then sort), and the partial predicate
keeps the index from growing with millions of `sent` rows. `priority_rank` exists
because `priority` is text: a text index sorts alphabetically, and
`high < low < normal` would have put low priority ahead of normal.

## 2. One pool per channel

Each channel gets its own claimer, its own senders and its own token semaphore. A single
shared pool would give head-of-line blocking: if the SMS provider starts answering in
10 s, every slot fills with SMS and email waits behind it. Channel isolation here is
structural rather than a tuning parameter — measured: with 562 SMS still queued, five
emails completed in 1.09 s.

The claimer pulls only as much work as it has idle senders. An earlier version used a
buffered dispatch channel, which held leases for rows that had not started yet: 40 rows
under lease pushed the worst-case claim→result window to 52 s against a 60 s lease, i.e.
a duplicate-send risk. With the semaphore, held leases are at most
`WORKER_CONCURRENCY` and the window is a single wave, so `WORKER_CLAIM_BATCH_SIZE` does
not enter the lease arithmetic at all.

## 3. Leases instead of lease renewal

A claim writes `status='processing'`, a fresh `lease_token` and
`lease_expires_at = now() + WORKER_LEASE_DURATION`. Results are only accepted when the
token still matches:

```sql
WITH upd AS (
  UPDATE notifications SET ... WHERE id = $1 AND lease_token = $2 RETURNING ...
)
INSERT INTO notification_attempts ... SELECT ... FROM upd
```

If the lease was lost, the CTE is empty and *no* attempt row is written either — a
zombie worker leaves no trace. This also makes `attempt_no` collisions impossible,
because `CompleteNotification` is the only writer of attempt rows.

Lease renewal was rejected: the worst-case window is already bounded and
`worker.New` enforces the two inequalities at boot, printing the required number if they
fail:

```
LeaseDuration >= 2 × (RateWaitMax + ProviderTimeout + ResultTimeout)
DrainTimeout  >= ProviderTimeout + ResultTimeout
```

With the defaults: `60s ≥ 2 × (10 + 10 + 3) = 46s` ✓. Renewal would add a heartbeat
goroutine per in-flight row to remove a risk that boot-time arithmetic already removes.

The reaper closes the loop: an expired lease returns the row to `retrying`, or to
`failed` if `attempt_count >= max_attempts`. That second condition pairs with
`attempt_count < max_attempts` in the claim query — with only one of them a row either
cycles forever or stays buried in `retrying` forever.

**Shutdown.** An in-flight provider call and its result write use
`context.WithoutCancel`: cancelling a result write would leave a *delivered* message in
`processing` for the reaper to send again. The claim query is exempt from cancellation
for a subtler reason — if SIGTERM interrupts it, pgx's cancel request races the server's
commit, and a committed `UPDATE` whose rows were never returned leaves up to a full
batch invisible until the lease expires. Shutdown therefore costs
`WORKER_CLAIM_TIMEOUT + WORKER_DRAIN_TIMEOUT = 23 s`, inside compose's 30 s
`stop_grace_period`.

## 4. Rate limiting: where the permit is taken

**The limit lives in Redis**, as a GCRA bucket keyed `notifications:rate:<channel>`. It
belongs to the bucket, not the process, so `--scale worker=3` does not triple the rate.
A test pins this by sharing one bucket across two limiter instances.

**The permit is taken in the sender, immediately before `Send`.** Taking permits before
claiming was rejected by arithmetic: you must ask for permits before you know how many
rows the claim will return, so every unused permit burns. Traffic of 5 msg/s would then
throttle itself against a 100/s limit. In the sender, one permit is always one real
send.

The cost is honest and documented: a row waiting for a permit is already `processing`,
so it cannot be cancelled during that window, and the priority order freezes for one
wave.

**Fail-closed.** If Redis is unreachable, nothing is sent. An error is treated exactly
like a denied permit: retried within the wait budget, then the row goes back to the
queue. "Release the lease immediately on error" was rejected because it turns a Redis
outage into a claim → error → release loop that hammers PostgreSQL; waiting keeps the
token held, so the semaphore provides backpressure and the claimer stops on its own.

**Why the internal rate is 96, not 100.** `RATE_LIMIT_PER_SECOND` is defined as "at most
this many in any 1-second window". In GCRA the burstable window is `rate + (burst - 1)`,
so the internal rate is derived backwards: `internalRate = perSecond - (burst - 1)`,
giving 96 for a limit of 100 and a burst of 5, and a worst-case window of exactly 100.

Burst was chosen by measurement, not taste:

| Burst | Behaviour |
|---|---|
| `burst = rate` | Internal rate collapses to 1; usable capacity disappears |
| `burst = 1` | Zero tolerance: slots missed by jittered polling never come back; 1000 messages took 14.0 s (~80/s, 20% below the limit) |
| `burst = 5` | 40 ms of tolerance catches missed slots; only ~4% of capacity unused |

**Waiting backs off** (10 ms → 250 ms, full jitter) instead of sleeping for GCRA's
`RetryAfter`. That value means "until the next permit" (~10 ms) and *everyone waiting
gets the same number*, so sleeping on it produces thousands of queries per second
against an empty bucket.

**Each permit query has its own 1 s timeout.** This was found by running it: when the
Redis container is stopped, the go-redis call blocks inside the client, and a single call
would otherwise consume the entire wait budget — no error logged, no retry attempted.
Regression test: `TestPermitWaitBoundsEachLimiterCall`.

## 5. Retry classification

The provider client reports facts, not policy: `StatusError` (code plus resolved
`Retry-After`), `TransportError`, `DecodeError`. Classification happens in
`internal/worker/classify.go` via `errors.As`.

| Provider result | Action | Reasoning |
|---|---|---|
| Any 2xx | `sent` | webhook.site answers `200`; treating only `202` as success would retry accepted messages |
| 2xx with an unparseable body | `sent`, `provider_message_id` NULL | The provider accepted it; retrying would duplicate a delivered message |
| 400, 422, unknown 4xx | `failed` immediately | The request is wrong; retrying cannot fix it |
| 401, 403, 404 | **Not** `failed` — opens the breaker | A configuration fault: the messages are valid, so they wait for the operator instead of being destroyed. Once the credential is fixed, the backlog flows |
| 429 | Retry honouring `Retry-After`, **no attempt consumed**, excluded from the breaker | Backpressure, not failure |
| 5xx, connection errors | Retry; if the breaker took over, requeue without consuming an attempt | |
| Timeout | Retry, attempt **always** consumed | The outcome is unknown — the message may have been delivered |

**The poison-message hole.** Not consuming an attempt unconditionally produced an
infinite loop for a single message against a provider that always answered 500:
interleaved successes reset the breaker's consecutive counter, so the breaker never
opened and the message cycled forever. The exemption was narrowed to "the breaker took
over".

**Requeue delay is `2 × BreakerOpenTimeout`**, not one. Claim order is
`ORDER BY priority_rank, available_at` — priority comes *before* time, so with a single
multiple a high-priority poison message is picked again by the very next half-open
probe.

**While the breaker is open the channel stops claiming.** The original plan said only
`classify.go` would change in this step; that was wrong and the note is kept in
`docs/PLAN.md`. Without stopping the claim loop, messages keep getting claimed, burn
attempts and consume shared rate-limit permits while the circuit is open. There is also
a gate for the half-open state: without it, every poll round pulled one more row while a
probe was in flight and the shared Redis budget drained within seconds.

**The breaker is local, the rate limit is shared.** A rate counter is commutative, so
one Redis key is enough. "Five consecutive failures" needs an event *ordering* across
replicas, which is distributed consensus for a mechanism whose benefit is already
mostly captured locally. The price: at the start of an outage each replica burns about
`threshold - 1` attempts, and two replicas can legitimately disagree about the state —
visible in `/metrics`, where breaker state is published per replica.

## 6. Idempotency

`Idempotency-Key` is optional and at most 255 characters. The stored hash is the
SHA-256 of the **raw request body bytes**; the body is read once and both the hash and
the JSON decode use those same bytes.

Same key + same bytes returns the stored response with `Idempotent-Replay: true`. Same
key + different bytes returns `409`. This is deliberately strict: a real retry resends
identical bytes, and the alternative — hashing a canonicalised JSON form — makes "the
same request" depend on the server's interpretation.

**The key is claimed before the work** (claim-first). Writing 1000 rows and then
discovering a replay would mean throwing that work away; instead a placeholder row is
inserted first and updated with the real response inside the same transaction, so a
rollback leaves nothing behind.

**Correctness does not depend on TTL cleanup.** An expired key is taken over inside the
same transaction (`ON CONFLICT ... DO UPDATE ... WHERE expired`). The janitor that
deletes expired keys runs in the worker (not the API, whose replicas would race each
other) and only keeps the table small.

The stored response is `jsonb`, so a replay is value-identical but may differ
byte-for-byte in key order. JSON object key order is not significant (RFC 8259), and in
exchange `jsonb` guarantees we can never store invalid JSON.

## 7. Batch writes

1–1000 items, one transaction, `pgx.CopyFrom`. `available_at`, `created_at` and
`updated_at` are left out of the COPY column list so the database stamps them — a single
clock source. All-or-nothing: if one item is invalid nothing is written, and *every*
bad row comes back in one response keyed `notifications[3].recipient`, so a 1000-item
batch is fixed in one pass rather than discovered one error at a time.

Scheduled items in a batch needed care: COPY cannot say "use the default for this row"
(passing NULL violates `NOT NULL`), so a mixed batch would have forced the application
clock onto the non-scheduled rows. Instead, after the COPY, a single statement fixes
only the scheduled rows inside the same transaction:

```sql
UPDATE notifications AS n SET available_at = s.send_at
FROM unnest($1::uuid[], $2::timestamptz[]) AS s(id, send_at)
WHERE n.id = s.id
```

When nothing is scheduled the statement never runs.

## 8. Scheduled notifications

`send_at` reuses `available_at` — no new column, no migration, no change to the claim
query, which already filters `available_at <= now()`. A scheduled row waits in `pending`,
so it remains cancellable until it is claimed.

Validation: RFC3339 with a timezone (a date alone is rejected, because a zone-less date
means different instants for client and server), at most 30 days ahead (unbounded
scheduling would park rows in the partial claim index forever, paying for them on every
scan), and not in the past — with one minute of tolerance, because a client computing
"now" can be a few seconds behind the server. A value inside the tolerance is treated as
"send immediately".

Parsing lives in the domain layer with the other string inputs, which is what makes the
batch error key `notifications[1].send_at` come for free.

The trade-off: after the first retry, `available_at` moves to the backoff time and the
originally requested instant is gone. A second column would mean two sources for "when
should this go"; the first attempt's timestamp remains in `notification_attempts`.

## 9. Observability

**One `/metrics` endpoint, on the API.** The worker opens no HTTP port. Three replicas
exposing their own ports means three scrape targets and merge logic, while the database
is already the single source of truth. The cost is that the values are **gauges**, not
counters — the metric names say so.

**Queue depth counts only active statuses**, so cost comes from the active set rather
than table size; `TestQueueDepthQueryAvoidsSeqScan` pins the plan with `EXPLAIN`. It is
split in two: `notifications_queue_depth` counts rows that are ready now
(`available_at <= now()`) and `notifications_deferred_depth` counts rows whose time has
not come (scheduled, or waiting out a retry backoff). Without the split, 10,000
notifications scheduled for next week look exactly like a blocked queue, and the alert
threshold you would set on depth becomes meaningless.

**Terminal counts are windowed** (last 15 minutes). A cumulative count was measured and
rejected: its `WHERE` clause matches the partial index predicate exactly, so the planner
goes straight to a `Seq Scan` (317 buffers at 15,500 rows) and scrape cost grows with
the table. The windowed version does a range scan on the covering metrics index
(migration `000005`), so no heap access is needed.

**Two latencies, together.** End-to-end (`created_at → updated_at`, queueing included)
and the provider call's own duration (`notification_attempts.duration_ms`). Separately
each is misleading; together they answer "is the slowness ours or theirs" — measured,
p50 1.65 s end-to-end against 0.021 s in the provider, so the delay is queueing and
throttling.

**Breaker state travels through Redis** (`internal/breakerstate`) because the breaker
lives in the worker process, the worker has no port, and the state is in no table. Keys
are per replica (`<channel>:<hostname>`) with a TTL of three times the publish interval,
and every worker rewrites its states periodically — publishing only on change would
never refresh the TTL, so a healthy channel's metric would silently disappear.

**`/metrics` is unauthenticated, so it is protected against cost:** a 5 s cache plus
singleflight (concurrent scrapes share one query round), a 2 s timeout per query, and
queries run sequentially. If one query fails the endpoint still answers and
`notifications_metrics_collect_errors` reports it.

**Correlation IDs** are generated (or accepted from `X-Correlation-ID`) by middleware,
returned on every response, and written to the notification row — which is how the API's
ID reaches the worker's logs even though they are separate processes:
`docker compose logs worker | grep <correlation-id>` finds the delivery of a request.

## 10. API contract choices

**A flat error envelope instead of RFC 9457.** Every error response — 404 and 405
included — is `{error, message, fields?, correlation_id}`. `problem+json` would add a
media type and a schema for clients to learn; the flat envelope gives field errors as a
map that binds directly to a form, and `correlation_id` is always present so a support
request maps to one log line. The `error` codes have a single source
(`internal/httpapi/errors.go`), and tests keep the OpenAPI enum in sync with the Go
constants — one of them parses the file with `go/parser` so that adding a constant
without listing it fails the build.

**`202`, not `201`.** The record exists, the delivery has not happened. For the same
reason `status: sent` is documented as "the provider accepted it", not as proof of
receipt.

**All field errors in one response.** The single exception: if `channel` is invalid, the
recipient and content *formats* are not validated (there is no way to know which rules
apply), only emptiness.

**Invalid UUID → `404`, not `400`.** An ID that cannot be parsed cannot name a record
that exists, so "not found" is the more honest answer.

**`attempts` is always an array**, `[]` when nothing has been tried — never `null` and
never absent, so a client never has to distinguish "no information" from "no attempts".
The list endpoint returns real attempt data too, fetched for the whole page in one query
(no N+1), ordered to match the unique index.

**Cursors are opaque** — `base64(created_at RFC3339Nano + " " + id)`. RFC3339Nano is
required: losing microseconds makes pagination skip or repeat rows that share a second.
The next page is detected by fetching `limit + 1` rows. The `WHERE` clause is built
dynamically in Go, because the `$n IS NULL OR ...` pattern degrades the cursor condition
from an `Index Cond` to a `Filter` under pgx's statement cache and generic plans —
`TestListQueryUsesCursorIndex` asserts the plan.

**Date ranges are half-open `[from, to)` and require a timezone**, so consecutive
windows never double-count a boundary row.

**SMS length follows the real GSM 03.38 table**: 160 septets if the text is encodable,
otherwise 70 **UTF-16 code units** (an emoji counts as 2). Lower-case `ç` and
`ş ğ ı İ` are not in the table while upper-case `Ç` and `ö ü Ö Ü` are, so one Turkish
character can drop the limit from 160 to 70 — the validation message says which encoding
applied, because "why 70?" is otherwise a mystery.

## 11. Validation lives in the domain layer

Transport parses, the domain decides. `internal/domain` takes strings and returns
`*ValidationError` with every bad field; the HTTP layer only maps that to the envelope
(adding the `notifications[i].` prefix for batches). Database `CHECK` constraints repeat
the same rules as a second line of defence — deliberate duplication: the application
should not be the only thing standing between a bad value and the table.

## 12. What was deliberately left out

- **WebSocket status updates, template system, distributed tracing.** Each is a feature
  surface; the time went into the depth of the engine instead. The hooks exist (Redis
  pub/sub is already a dependency, `template_id` is reserved in the schema).
- **Priority aging.** Documented in the README as a known limit, with the reason:
  adding an age term to the sort key changes the claim index and every plan decision
  built on it, which is not a change to make without load measurements.
- **k6 load tests and a Grafana dashboard.** The measurements in the README were taken
  with the mock provider and SQL queries instead; a dashboard would have been a nicer
  demo but not a stronger argument.
