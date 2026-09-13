# Digest — Plan

A self-hosted feed aggregator. Pulls RSS/Atom sources (blogs, YouTube, subreddits, GitHub
releases, arXiv), extracts full article text, dedupes items appearing across sources, indexes
for full-text search, and emails a ranked daily digest.

This document holds the phasing, the schema, the design decisions, and the reasoning behind
them. `CLAUDE.md` stays short and instruction-only; the thinking lives here.

## Goals

Two, equally weighted:

1. **Daily use.** If it isn't in the morning routine, it failed.
2. **Interview-grade.** The parts worth discussing are the fetch scheduler, the dedup pipeline,
   and the failure handling — not the infrastructure list.

**Stack:** Java 21+, Spring Boot 3.x, Postgres. Deployed on a VPS from Phase 0.

## Scope and non-goals

**In scope:** feed ingestion, article extraction, dedup/clustering, full-text search, ranked
email digest, browsing UI.

**Out of scope, deliberately:**

| Tech | Verdict |
|---|---|
| **Redis** | Single instance, single user — no distributed state to hold. Caffeine in-process covers caching. Redis would earn its place for cross-instance rate limiting; there are no cross instances. |
| **Lucene / Elasticsearch** | A second index to keep in sync, back up, and rebuild — hand-rolling the consistency guarantees ES exists to provide. Postgres `tsvector` + GIN handles this scale. Real cost: `ts_rank_cd` is not BM25 and relevance is measurably worse. Acceptable for a personal archive. |

Knowing what was left out, and why, is part of the deliverable. Keep this table current.

---

## Architecture decisions

Structural choices that are expensive to reverse. Preferences (build tool, Lombok, HTTP client)
live in the Decision inventory further down; these are the ones with teeth.

### 1. Queue mechanism — decided 2026-09-11

**Postgres `SELECT … FOR UPDATE SKIP LOCKED`.** Not Kafka, not RabbitMQ, not an embedded job
library.

We need a *work queue*. Kafka is a *distributed log*. They solve different problems, and the
second costs far more to operate.

**The measurement.** 200 feeds hourly ≈ 0.055 events/sec; 2–5k items/day ≈ 0.06/sec; worst-case
aligned burst ≈ 3/sec. Postgres `SKIP LOCKED` does thousands of claims/sec on modest hardware.
That is four-plus orders of magnitude of headroom.

**The correctness argument, which matters more than the throughput one.** With Postgres, enqueuing
work and writing the data are one transaction:

```sql
BEGIN;
  INSERT INTO item (...);
  INSERT INTO item_content (item_id, status) VALUES (..., 'pending');
COMMIT;
```

With Kafka they are two systems with no shared transaction — the **dual-write problem**. DB commits
and the publish fails, and an item exists that will never be extracted, silently. The publish
succeeds and the DB rolls back, and a consumer receives a message about a row that doesn't exist.
The standard remedy is the transactional outbox pattern: write to an `outbox` table in the same
transaction, then relay it to Kafka — which means building a Postgres-backed queue *anyway* and
putting Kafka on top of it. For this workload Kafka doesn't replace the simple solution; it sits
on top of one.

Exactly-once wouldn't rescue this either: Kafka's EOS covers read-process-write within Kafka. Our
consumers do HTTP fetches and send email, so we're at at-least-once regardless and need idempotent
handlers either way — which is most of the work Kafka appears to save.

**Replay is also better in Postgres, not merely adequate.** `UPDATE item_content SET
status='pending' WHERE extractor < 'v2'` is selective. Kafka replay is "rewind to offset N and
reprocess everything after," which can't express "only failed extractions from this domain."

**What we build instead:**

- Work items are rows with a `status` and a `next_attempt_at`.
- Workers claim with `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`.
- The `(next_attempt_at) WHERE status = 'pending'` partial index *is* the queue.
- Optionally add `LISTEN`/`NOTIFY` later so consumers wake on enqueue instead of polling — a
  latency optimization only. NOTIFY is fire-and-forget, so a down consumer misses it; the polling
  loop stays as the backstop.
- Behind a narrow interface, so Phase 8 can swap in a Kafka implementation for comparison.

**Known cost of this choice:** high-churn queue tables generate dead tuples, so autovacuum needs
attention if throughput ever grows meaningfully. A non-issue at our volume; noting it so it isn't
a surprise.

**Reverse this decision if** — and only if — one of these becomes true:

1. Multiple consumer types need the same stream independently at different rates (extraction *and*
   a notifier *and* an embedding job), where competing status columns on one table get ugly.
2. Consumers must scale across machines with partitioned ordering guarantees.
3. Producers and consumers land on different teams' release cycles.
4. Sustained throughput reaches ~10k/sec, where vacuum churn becomes a real ops problem.
5. The log itself must be the source of truth for rebuilding derived state (event sourcing).

Only #4 is theoretically reachable, and it requires growing roughly five orders of magnitude.

**Rejected alternatives:** in-JVM `ThreadPoolTaskExecutor` (work lost on restart, can't query
what's pending — acceptable Phase 1, wrong by Phase 3); JobRunr/db-scheduler (fine libraries, but
hand-rolling ~20 lines we fully understand serves the learning goal better); RabbitMQ (better fit
than Kafka if a broker were needed, but still a second system with the same dual-write problem).

### 2. Process topology — decided 2026-09-12

**One artifact, role flags.** `digest.roles=fetch,extract,web`, with `@ConditionalOnProperty`
gating each scheduler and the web layer.

*Why:* a single process is simplest but couples failure domains — a 10MB page OOMing the extractor
would take down the fetcher and UI with it. Separate services fix that at real ops cost. Role flags
cost almost nothing today and turn "split them apart" into a deploy-config change rather than a
refactor.

*Gotcha:* only one process may run a given scheduler, or you double-fetch. Until the instance count
is settled, run all roles in one process and treat the flags as latent capability.

### 3. Tenancy — decided 2026-09-12

**Single user. Permanently.** No `user_id` on `item_state`, `digest`, or feed subscriptions. Auth
is one credential.

*Why:* retrofitting tenancy means rewriting essentially every query and index, so the cost of
being wrong is real — but a half-built tenancy model nobody uses is worse than a clean
single-user system with a documented migration path. State this in the README: "designed for one
user; here's what would change for multi-tenancy."

*Gotcha:* resist "just in case" `user_id` columns. An always-1 column is a lie that costs index
width and invites code that pretends to be multi-tenant.

### 4. Failure representation — decided 2026-09-12

**Sealed result types, not exceptions.** `sealed interface FetchOutcome permits Ok, NotModified,
HttpError, Timeout, ParseError, Empty`, consumed with exhaustive pattern-matching `switch`.

*Why:* feeds fail constantly — failure is the normal path, not the exceptional one. Modelling it
as exceptions means classifying by catching and inspecting, which is backwards. The sealed type
mirrors `fetch_attempt.outcome`, and the compiler flags every unhandled case when a variant is
added. It also exercises Java 21 sealed interfaces, records, and exhaustive switch directly.

*Gotcha:* keep genuine bugs (NPE, programming errors) as exceptions. Result types are for expected
domain failures, not for swallowing defects.

### 5. Source-specific handling — decided 2026-09-12

**Normalize at the edge via per-source adapters.** `bytes → [FeedAdapter] → ParsedFeed/ParsedItem
→ source-agnostic core pipeline.` `GenericRssAdapter` covers ~90%; specialized adapters delegate
to it and enrich. Spring injects `List<FeedAdapter>` and selects on `feed.kind`; unknown kinds fall
back to generic rather than failing.

*Why:* the defining property is that dedup, extraction, search, and digest have **zero** knowledge
of source types. Only adapters know YouTube uses `media:group`. This is the standard pattern in
heterogeneous ingestion systems.

*Gotchas, both commonly botched:*
- Adapters own **request shaping as well as parsing** — Reddit's UA, GitHub's token, arXiv's
  etiquette. A parse-only interface leaks request quirks back into the fetcher.
- `ParsedItem` (off the wire, immutable record) stays **separate from** `Item` (JPA entity).
  Reusing the entity as a parse target couples parsing to persistence. Adapter-specific leftovers
  go in `item.extras` jsonb.
- Store `kind` on the feed; don't sniff per fetch.

### 6. Time source — decided 2026-09-12

**Inject `java.time.Clock`.** One `@Bean Clock clock() { return Clock.systemUTC(); }`. No
`Instant.now()`, `LocalDate.now()`, or `System.currentTimeMillis()` anywhere else. Tests use
`Clock.fixed(...)` or a mutable advancing clock.

*Why:* nearly all the logic here is time arithmetic — backoff curves, staleness detection, digest
windows, lease expiry. Injected, "feed failed 5 times over 3 days, when's the next attempt?" is a
millisecond unit test. Otherwise you sleep in tests or skip them. Cheap now, pervasive later.

*Gotchas:*
- `@Scheduled` does **not** use this Clock — it has its own timing. The Clock governs the logic's
  notion of now, not when jobs fire. Test the method the scheduler calls, not the firing.
- Store UTC, but the digest window needs a configured zone. Derive `ZonedDateTime` from
  `Clock` + zone at the boundary; never let a local zone leak inward.

### 7. Transaction boundaries — decided 2026-09-12

**One transaction per fetch cycle, covering only DB writes, with zero I/O inside it.**

```
(no tx)  HTTP GET → parse → sanitize → in-memory ParsedFeed
(tx)     batch upsert items  ON CONFLICT (feed_id, dedup_key) DO NOTHING
         update feed bookkeeping (next_fetch_at, etag, last_modified, body_hash, failures)
         insert fetch_attempt
(commit)
```

*Why items and feed bookkeeping share one transaction:* writing items then crashing before
advancing `next_fetch_at` causes a re-fetch and reprocess. Harmless (it's idempotent) but wasteful,
and it makes `fetch_attempt.items_new` lie — and that table is the debugging spine, so it has to be
truthful.

*Why batch rather than per-item:* by write time the data is already validated. Poison items are a
*parse*-step concern.

*Gotcha:* **sanitize during parse so the write cannot partially fail.** A null byte in a title makes
Postgres reject the whole batch. Strip at parse time rather than reaching for per-item savepoints.
On the failure path, still write a `fetch_attempt` row and update backoff in its own small
transaction.

### 8. Work claiming — decided 2026-09-12

**Lease-based, with expired-lease recovery folded into the claim query.**

```sql
UPDATE item_content
SET status = 'in_progress',
    lease_expires_at = now() + interval '5 minutes',
    attempts = attempts + 1
WHERE item_id IN (
  SELECT item_id FROM item_content
  WHERE (status = 'pending' AND next_attempt_at <= now())
     OR (status = 'in_progress' AND lease_expires_at < now())
  ORDER BY next_attempt_at
  FOR UPDATE SKIP LOCKED
  LIMIT 10
)
RETURNING item_id;
```

*Why:* holding a row lock across a 30-second article fetch means a 30-second transaction — the
thing that makes people wrongly conclude Postgres queues don't work. Claim commits immediately;
work happens after. Folding expired leases into the claim removes the need for a separate sweeper.

*Gotcha — the one people get backwards:* **increment `attempts` at claim, not at failure.** If it
only increments on failure, a hard crash never increments and the item retries forever — an
infinite loop on exactly the pathological pages most likely to crash you. Incrementing at claim
means a crash still counts and poison items eventually give up. Set lease duration at ~3–5× p99
work time.

For *fetch* scheduling the equivalent is pushing `next_fetch_at` forward at claim time — a lease by
another name, and what prevents double-fetching.

### 9. Idempotency — decided 2026-09-12

**Every handler must be safe to run twice.** Data writes get a unique constraint plus
`ON CONFLICT`; side effects get a conditional state transition.

| Surface | Mechanism |
|---|---|
| `item` | `unique (feed_id, dedup_key)` + `ON CONFLICT DO NOTHING` |
| `item_search` | `ON CONFLICT (item_id) DO UPDATE` — upsert |
| `item_content` | Naturally idempotent; guard the status transition, not the write |
| `fetch_attempt` | Append-only; duplicates harmless and informative |
| Digest email | `UPDATE digest SET status='sending' WHERE id=? AND status='pending'` — zero rows affected means someone else has it, bail |

*The honest limit:* **SMTP is the irreducible at-least-once boundary.** Crash between the relay
accepting the message and the commit landing, and it sends twice. Narrow the window, use a
relay-side idempotency key where supported, and accept the rest. Documenting this precisely is
worth more than claiming exactly-once.

### 10. Execution model — decided 2026-09-12

**GitHub Actions cron + Neon Postgres. Zero hosting cost. No VPS.** The application stays
invocation-agnostic: the fetch cycle is a plain service method, triggered by an HTTP endpoint or
CLI during development and by a scheduled workflow in production.

**The cron wakes the app; the app decides what's due.** The workflow fires hourly and
unconditionally; `next_fetch_at` and the lease-claim query decide what actually runs.

*Why that split:*
- **DST-proof.** GitHub cron is UTC. A hardcoded `0 14 * * *` for a 7am digest drifts an hour
  twice a year. Letting the app ask "is it past 7am in the configured zone, and have I sent
  today?" makes DST arithmetic in a unit-testable class.
- **Drift-proof.** GitHub delays scheduled runs under load, sometimes 10+ minutes. A late
  wake-up still does the right thing.
- **Testable.** Schedule logic lives in Java behind the injected `Clock`, not in a YAML string.

*Two workflows, not one:*
- `build.yml` on push → build, test, publish image to GHCR
- `fetch.yml` on cron (`17 * * * *`, off-peak) → pull image, run with secrets

Rebuilding on every tick would waste 1–2 minutes sixty times a day. Splitting it also produces a
real CI/CD pipeline rather than a cron job.

*Constraints accepted:*

| Constraint | Consequence |
|---|---|
| Neon free tier = 0.5 GB | ~10–15k articles with full text. `item_content` is the first thing to prune. The real ceiling on this design. |
| Repo must be public | Unlimited Actions minutes + free GHCR. Hourly private-repo runs would eat ~1,400 of 2,000 free minutes. It's a portfolio project; public is right anyway. |
| GH disables scheduled workflows after 60 days of repo inactivity | Non-issue during development; needs an alert later. |
| Fresh JVM + Spring context per run (~5–10s) | No in-memory state survives between runs. Caffeine caching is pointless here — vindicates skipping Redis. |
| Overlapping runs if one exceeds the interval | Add a `concurrency:` group. Lease-based claiming already makes overlap *safe*; preventing it is cheaper than handling it. |

*Known limit:* GitHub Actions cannot host an always-on web process. See decision 11.

### 11. Interface — decided 2026-09-12

**The email is the product. The UI is local-only.** Run as `digest.roles=web` on the laptop
against Neon, when wanted. Never deployed, never exposed.

*Why a UI at all, given the email:* four things email cannot do — search the archive, write back
read state (the input to Phase 6 ranking), manage feeds, and show feed health. The first two
matter: the archive and the ranking are both inert without an interface.

*Scope decisions that follow:*
- **A minimal read-only browse + search page moves to Phase 5**, alongside search itself. Search
  without an interface is a feature you can't use; the page is what makes that phase real.
- **Auth is deferred until something is actually exposed.** Spring Security guarding an app bound
  to `127.0.0.1` is theater, and cutting it is exactly the "complexity added for resume reasons"
  this plan is meant to resist. "I didn't add auth because nothing was exposed" is the better
  answer.
- **No SPA.** htmx + Thymeleaf. A local-only tool doesn't justify a frontend build in CI.
- **Kept in Phase 7:** the JSON API, keyset pagination, DTO-vs-entity boundaries, feed management,
  feed health view. That's where the backend interview content lives — REST design and pagination,
  not the HTML.

*Rejected for now:* a reader-compatible API (Fever/Google Reader), which would give a polished
mobile client like NetNewsWire for free including read-state sync. Genuinely attractive, but it
needs an always-on server. Revisit if hosting ever appears.

---

## Phases

Estimates assume evenings and weekends while learning Java, and are padded — feed work runs
long because feeds are broken.

### Phase 0 — Skeleton that ships · 1 weekend

Spring Boot app exposing only `/actuator/health`. Flyway with one migration. Docker Compose with
Postgres for local development. One Testcontainers test that boots the context against real
Postgres. `build.yml` running `./mvnw verify` and publishing an image to GHCR. Neon project
created, `fetch.yml` running that image on an hourly cron.

**Teaches:** auto-configuration and what `@SpringBootApplication` actually enables, profiles,
`@ConfigurationProperties` binding.

**Why first:** shipping is never a scary phase if it's never a phase. Every later commit ships.
The alternative — six weeks local, then a deploy sprint — is where side projects die.

### Phase 1 — Fetch, store, email · 1–2 weeks

5–10 feeds seeded via migration. One scheduled method fetches and parses with Rome, inserts new
items, skips seen ones. A 7am job renders a Thymeleaf HTML email of the last day and sends via an
SMTP relay.

**Teaches:** the bean container and constructor injection, `@Transactional` and what a transaction
boundary actually is, Spring Data JPA / `JdbcClient`, scheduling, templating.

**Exit criterion:** a useful email arrives. Everything below is improvement on a working system.

### Phase 2 — Fetching that survives reality · 2 weeks

Per-feed scheduling via `next_fetch_at`. Conditional GET. Exponential backoff. A `fetch_attempt`
row per attempt. Connect/read/total timeouts. Concurrent fetching.

Micrometer + Prometheus + Grafana land here. Structured JSON logging with MDC carrying `feed_id`
and a correlation id.

**Teaches:** virtual threads, executors, timeout and cancellation semantics, and the interaction
between virtual threads and a bounded connection pool. This is the phase with the real
concurrency lesson.

### Phase 3 — Full-text extraction · 2–3 weeks

Most feeds ship truncated summaries. Fetch the article, run readability-style extraction, store
the text. Items land `pending`; workers claim with `SKIP LOCKED`, retry with backoff, mark
permanent failures.

**Teaches:** idempotent work queue design, per-host politeness and rate limiting, failure
isolation, retry classification.

### Phase 4 — Dedup and clustering · 2 weeks

Two distinct problems, commonly conflated:

- **Exact** — same item twice from one feed because its GUID changed. Handled by `dedup_key`.
- **Cross-source** — same story in HN, a subreddit, and the author's blog. Layered, cheapest
  first: canonical URL match → title similarity → content shingle/SimHash.

**Teaches:** SimHash/MinHash, text normalization, set-based SQL instead of loops.

**Do URL canonicalization first and measure it** before writing any similarity code. It is most
of the practical win and it is a grubby heuristic pile, not an algorithm.

### Phase 5 — Search, and the page that makes it usable · 1.5–2 weeks

`tsvector` over title + extracted text, GIN index, `/search` with ranking and keyset pagination.
Plus a **minimal read-only browse + search page** (htmx + Thymeleaf, local-only, no auth).

**Teaches:** `EXPLAIN ANALYZE`, GIN vs GiST, why `ts_rank` can't use the index, index-only scans.

**Why the page lands here rather than Phase 7:** search with no interface is a feature you can't
use. Building the smallest possible page alongside the index is what makes this phase real instead
of theoretical — and it's the first point where the archive becomes something you'd actually reach
for. See decision 11.

### Phase 6 — Ranked digest · 1–2 weeks

Stop emailing chronologically. Score by source quality, recency, read history, keyword match. Top
N plus an "and 40 more" tail.

**Teaches:** feature engineering without ML, a feedback loop, and making a scoring function
debuggable. Store score components per digest item — six weeks later "I recomputed it and got a
different answer" is a miserable place to be.

### Phase 7 — Local API and management UI · 2 weeks

Builds on the Phase 5 page: mark read, star, manage feeds, per-feed health view backed by
`fetch_attempt`. JSON API underneath. Local-only, `digest.roles=web`, no auth.

**Teaches:** Spring MVC, REST design, keyset pagination, DTO mapping and why entities don't cross
the wire.

**Deliberately not here:** Spring Security. Nothing is exposed to a network, so auth would be
theater — added the day something is, and not before (decision 11). Read-state writeback is the
part that matters most, since it's the input Phase 6's ranking needs to learn from.

### Phase 8 — Deliberate scaling exercise · optional, 1–2 weeks

Implement a Kafka backend for the Phase 3 queue interface. Load-test both. Write up the crossover
with numbers. Framed this way it is an experiment with a result, not an ornament — and it's the
phase that interviews best.

---

## Schema sketch

`timestamptz` everywhere, always UTC. Never bare `timestamp`.

```
feed
  id                    bigint identity PK
  url                   text not null unique      -- what you fetch
  site_url              text                      -- feed's <link>
  title                 text
  kind                  text not null             -- rss|atom|youtube|reddit|github|arxiv
  active                boolean not null default true
  fetch_interval_s      int not null default 3600
  next_fetch_at         timestamptz not null default now()
  last_success_at       timestamptz
  consecutive_failures  int not null default 0
  etag                  text                      -- store raw, never parsed
  last_modified         text                      -- ditto
  body_hash             bytea                     -- sha256; fallback when ETag lies
  created_at            timestamptz not null default now()
```
Index: `(next_fetch_at) WHERE active` — partial, and the scheduler's only query. Keep it that way.

```
fetch_attempt
  id            bigint identity PK
  feed_id       bigint not null → feed
  started_at    timestamptz not null
  duration_ms   int
  outcome       text not null   -- ok|not_modified|http_error|timeout|dns|parse_error|empty
  http_status   int
  bytes         int
  items_seen    int
  items_new     int
  error_class   text            -- exception simple name
  error_detail  text            -- truncate to ~1KB
```
Index: `(feed_id, started_at desc)`. Fastest-growing table and the best debugging asset in the
project — when a feed goes quiet this says whether it's 403ing, returning empty, or genuinely
idle. Prune beyond ~90 days.

```
item
  id             bigint identity PK
  feed_id        bigint not null → feed
  dedup_key      text not null      -- computed; see fallback chain
  dedup_key_ver  int not null       -- so the chain can change without fragmenting history
  guid           text               -- raw, as claimed
  url            text               -- raw, as claimed
  canonical_url  text               -- normalized + redirects resolved
  title          text
  author         text
  published_at   timestamptz        -- CLAIMED. often garbage. nullable.
  first_seen_at  timestamptz not null default now()   -- yours. trustworthy.
  content_html   text               -- as supplied in the feed
  content_hash   bytea
  cluster_id     bigint → cluster   -- nullable
  lang           text
  extras         jsonb              -- source-specific fields
```
Indexes: `unique (feed_id, dedup_key)`; `(first_seen_at desc)`; `(canonical_url)`;
`(cluster_id) WHERE cluster_id is not null`.

```
item_content            -- 1:1 with item; extracted full text
  item_id         bigint PK → item
  status          text not null   -- pending|ok|failed|skipped|paywalled|js_required
  attempts        int not null default 0
  next_attempt_at timestamptz
  extractor       text            -- 'readability4j@1.0.8' — version it
  text            text
  html            text
  word_count      int
  lead_image_url  text
  error           text
```
Index: `(next_attempt_at) WHERE status = 'pending'` — this partial index *is* the work queue.

```
item_state
  item_id        bigint PK → item
  read_at        timestamptz
  starred        boolean not null default false
  hidden         boolean not null default false
  digest_sent_at timestamptz

cluster
  id                      bigint identity PK
  representative_item_id  bigint → item
  simhash                 bigint
  created_at              timestamptz not null default now()

item_search
  item_id  bigint PK → item
  tsv      tsvector          -- GIN index here

digest
  id, sent_at, subject, item_count, status

digest_item
  digest_id, item_id, rank, score, score_components jsonb
```

**Note on `item_search`:** the obvious move is a generated `tsvector` column on `item`. It doesn't
work — searchable text spans `item.title` and `item_content.text`, and a generated column can't
see across tables. A separate table also keeps GIN update churn off the hot row and makes a
tokenizer change a `TRUNCATE` + backfill instead of a full-table rewrite.

---

## Locked decisions

Expensive or impossible to reverse once real data exists.

**1. The `dedup_key` fallback chain.** Feeds supply `<guid>`, or `<id>`, or nothing. Some
regenerate GUIDs on every edit; some reuse them across different posts; some use the URL, which
changes on a host migration. Chain: stable GUID → canonical URL → `hash(normalized_title +
published_date)`. **Version it** via `dedup_key_ver`. Changing the chain unversioned means every
existing item stops matching its future self and the archive fragments permanently. This is the
single most expensive thing to get wrong.

**2. `published_at` vs `first_seen_at`.** Feeds lie constantly — items dated 1970, dated in the
future, all sharing the feed's build date, dates that change every fetch. Sort and window on
`first_seen_at`; display `published_at`. One bad feed otherwise poisons ordering forever.

**3. No Postgres `ENUM` types.** `text` + `CHECK`, or app-level validation. Enum values can't be
removed, altering them inside Flyway is awkward, and new outcome values will appear.

**4. Big text lives in Postgres.** TOAST handles it; transactional consistency is free. Moving to
object storage later needs a migration plus a consistency story. Budget ~3–5 GB per 100k articles
and size the VPS disk with headroom for WAL and vacuum.

**5. One item belongs to one feed.** The same article in three feeds is three `item` rows joined
by a `cluster`. The alternative — global items, M:N to feeds — forces identity resolution at write
time when you have the least information, and makes "what did this feed publish" a join.

**6. `bigint identity` for internal PKs.** Smaller indexes, better locality, monotonic. Add a
separate slug column if non-guessable public IDs are needed later.

**7. Digest window is "since last *successful* digest,"** not "last 24 hours." The latter
silently drops a day whenever a send fails.

**8. Never merge on dedup — link.** Keep every item row, set `cluster_id`, pick a representative.
Dedup will be wrong sometimes (two posts titled "Weekly Update"); linking makes that a one-line
fix, merging makes it data loss.

---

## Decision inventory

Reversibility: 🔒 locked · ⚠️ sticky (costs a refactor) · ✅ cheap.

### Setup
| Decision | Options | Note |
|---|---|---|
| Build tool | Maven / Gradle | ✅ Maven reads better to a reviewer |
| Java version | 21 LTS / 25 LTS | ⚠️ 25 is LTS; 21 is the safer library-compat bet |
| Module layout | Single / multi | ⚠️ Single until Phase 5+ |
| Package structure | Layered / by-feature / hexagonal | ⚠️ By-feature (`feed/`, `item/`, `extract/`, `digest/`) ages better |
| Lombok | Yes / no | ✅ Records cover most of it; adding later is easy, removing isn't |
| Postgres location | Container / host apt | ⚠️ Affects backups, upgrades, local-vs-prod parity |

### Persistence
| Decision | Options | Note |
|---|---|---|
| Data access | JPA / `JdbcClient` / jOOQ / mixed | ⚠️ Mixing is defensible — JPA for entity CRUD, SQL for set-based digest/dedup. Be able to justify the split |
| Migration execution | On startup / separate CI step | ⚠️ Startup is fine at one instance; wrong at two |
| Entity mutability | Mutable entities / records | ⚠️ Hibernate requires mutable + no-arg ctor; constrains the above |
| Retention | Keep forever / prune items / prune attempts only | ⚠️ Prune `fetch_attempt`; think hard before pruning items |

### Fetching
| Decision | Options | Note |
|---|---|---|
| HTTP client | JDK `HttpClient` / `RestClient` / `WebClient` / OkHttp | ✅ Pick on timeout + redirect + size-cap ergonomics |
| Scheduling | `@Scheduled` / DB-claim `SKIP LOCKED` / Quartz / ShedLock | ⚠️ `@Scheduled` is **single-threaded by default** — one slow fetch delays the digest |
| Concurrency | Virtual threads + semaphore / fixed pool / reactive | ⚠️ Virtual threads make it trivial to starve a 10-connection pool with 500 tasks. Separate the HTTP phase from the DB-write phase |
| Interval policy | Fixed / adaptive / honor feed `ttl` | ✅ Adaptive is the interesting version |
| Failure classification | Retryable set, backoff curve, give-up threshold | ⚠️ Encode as a table, not scattered `if`s |
| Politeness | Global / per-host / `robots.txt` crawl-delay | ⚠️ Per-host matters once extracting articles |
| Conditional GET | ETag + Last-Modified + body-hash fallback | ⚠️ Many servers rotate ETags or 304 forever |
| Redirects | Follow / follow + persist 301 to `feed.url` | ⚠️ Not persisting means paying the hop forever, then silent death |
| Store raw bodies | No / last-N per feed | ✅ Last body per feed makes parser bugs reproducible offline. Cheap, high value |
| Response size cap | Yes / no | ⚠️ Uncapped + jsoup = OOM on a small VPS |
| User-Agent | Default / identifying + contact URL | ✅ Default JDK UA gets 403'd by Cloudflare |

### Extraction
| Decision | Options |
|---|---|
| Library | `readability4j` / crux / jsoup + own heuristics |
| When | On ingest / lazily on read / digest candidates only |
| JS rendering | Never / Playwright fallback for known SPA domains |
| `robots.txt` | Ignore / respect — ⚠️ you are a crawler now |
| Failure taxonomy | `failed` vs `paywalled` vs `js_required` vs `skipped` — drives retry policy |
| Text, HTML, or both | ⚠️ Both. The original is unrecoverable once the link rots |

### Dedup
| Decision | Options |
|---|---|
| Pipeline position | Write-time / async batch |
| URL canonicalization rules | 🔒 `utm_*`, AMP, `m.` subdomains, trailing slash, session IDs, redirect resolution |
| Threshold tuning | How do you *evaluate* it? Needs a small hand-labeled set or it's guessing |
| arXiv versions | Is v1/v2 the same item? SimHash says yes; you may want no |

### Search
| Decision | Options |
|---|---|
| Text search config | `english` (stemming) / `simple` / per-detected-language |
| Ranking | `ts_rank` / `ts_rank_cd` / custom blend with recency |
| Long documents | ⚠️ `tsvector` caps near 1MB — truncate or the insert throws |
| Pagination | Offset / keyset — ⚠️ keyset, decided before the API shape sets |

### Digest
| Decision | Options |
|---|---|
| SMTP relay | Postmark / SES / Resend — ⚠️ self-hosted from a VPS lands in spam |
| Send idempotency | Behavior on partial failure or double-trigger |
| Email size | ⚠️ Gmail clips over 102KB, and your own client renders the truncated version fine |
| Ranking inputs | Source quality, recency, read history, keyword match; store components |

### Ops
| Decision | Options |
|---|---|
| Deployment | Compose on VPS / systemd + jar / k3s — ⚠️ k3s is over-engineered for one box |
| Image build | CI + registry pull / build on VPS — ⚠️ a 1GB VPS OOMs building Maven beside Postgres |
| Secrets | env vars / `.env` / SOPS / Vault |
| Log aggregation | `journalctl` + grep / Loki / none |
| Alerting scope | ⚠️ Resist more than one at first: "no digest in 36h" |
| Backups | `pg_dump` cadence, destination, and **a tested restore** |
| Instance count | One / two — 🔒 retroactively justifies or kills half the infra questions |

### Testing
| Decision | Options |
|---|---|
| DB in tests | Testcontainers / H2 / embedded — ✅ Testcontainers, with container reuse enabled |
| Feed fixtures | ⚠️ A corpus of real broken feeds as golden files — encoding mismatches, BOMs, illegal control chars, missing GUIDs. Highest-value test asset in the project |
| Mocking | Mockito / hand-rolled fakes / real objects |

### Interface
| Decision | Options |
|---|---|
| UI | htmx + Thymeleaf / React SPA — ⚠️ a SPA adds a frontend build to CI and invites JS scrutiny on a Java portfolio |
| Auth | Session / JWT / OAuth via provider |
| OPML import/export | ✅ Cheap, and it's the migration path in and out |
| Reader-compatible API (Fever / Google Reader / Miniflux) | ⚠️ Would let existing mobile RSS apps read Digest instead of building a client. Worth considering *before* a custom UI |

---

## Production concerns

| Concern | Phase | Note |
|---|---|---|
| Flyway | 0 | From commit one. Hibernate `ddl-auto` never past `validate` |
| Docker Compose | 0 | Postgres locally; Prometheus/Grafana added Phase 2 |
| Testcontainers | 0 | One context-load test first. Enable reuse or the loop is unbearable |
| CI | 0 | Build + test on push; deploy step lands Phase 1 |
| Structured logging | 2 | JSON + MDC with `feed_id`. Before volume, not after |
| Micrometer/Prometheus | 2 | Counters per outcome, timers per phase, gauge on queue depth |
| Grafana | 2–3 | Two dashboards: feed health, pipeline throughput |
| Alerting | 3 | One alert |
| Backups | 3 | `pg_dump` to object storage. **Test a restore** — untested backups aren't backups |
| Spring Security | 7 | With the UI |

---

## Known hard parts

Where the time actually goes.

- **Encoding and malformed XML.** HTTP header says UTF-8, XML prolog says ISO-8859-1, bytes are
  Windows-1252. Plus BOMs and raw control characters illegal in XML 1.0 that make Rome throw.
  Needs a sanitizing pass before parsing and an explicit charset precedence rule.
- **Broken conditional GET.** Servers returning a fresh ETag every request, or 200 regardless of
  `If-Modified-Since`, or 304 forever after publishing. Body-hash covers the first two; only a
  max-staleness forced refetch covers the third.
- **403s from the default User-Agent.** Cloudflare and many CDNs block the JDK UA outright.
- **Silent feed death.** Valid 200, valid XML, zero items — or the domain got parked and the ad
  page happens to parse. Error rate 0%, content zero. Needs a "no new items in N× the usual
  interval" check, which is only queryable because `fetch_attempt` exists.
- **Feed windows.** Most feeds expose the last 10–20 items. Three days down on a high-volume feed
  loses the overflow permanently. No fix — a reason to make the fetch loop crash-resilient early.
- **Extraction fails on 20–30% of the web.** JS-rendered SPAs give an empty shell, paywalls give a
  teaser, newsletter platforms nest oddly. Partial extraction is normal; record it honestly and
  fall back to the feed's summary.
- **Extraction memory.** A 10MB page parsed by jsoup allocates far more than 10MB. One
  pathological page can OOM the JVM and take fetching down with it.
- **Per-source quirks.** YouTube puts real content in `media:group`. Reddit rate-limits hard and
  demands a descriptive UA. GitHub release feeds republish on edits. arXiv v1/v2/v3 look like
  distinct items and mostly shouldn't be — a dedup case SimHash gets wrong because the abstract
  barely changes.
- **HTML email is the worst platform you'll target.** Tables and inline styles, no modern CSS, and
  Gmail's 102KB clip.
- **Postgres FTS edges.** ~1MB `tsvector` cap. `ts_rank` doesn't normalize for document length
  without the flag, so long articles dominate. GIN pending-list buffering makes occasional queries
  mysteriously slow.
- **Virtual threads vs the connection pool.** Unbounded fan-out against a bounded pool produces
  connection timeouts that look like Postgres problems.
- **`@Scheduled` is single-threaded by default.** Surprises everyone exactly once.
- **The VPS.** Disk fills (WAL, logs, extracted text). The OOM killer picks Postgres because it
  has the largest RSS. Default JVM heap is a fraction of RAM. Server, JVM, and Postgres timezones
  disagreeing produces 3am digests.

---

## Open questions

Not yet decided; each changes work downstream.

1. **What gets pruned when Neon's 0.5 GB fills?** `item_content.html` first, then `text` for old
   items, keeping `item` rows forever? Not urgent, but it's the ceiling on decision 10 and worth
   deciding before the archive matters.

*Resolved:* tenancy (decision 3, single-user); Java version (25 LTS — JEP 491 removed
virtual-thread pinning on `synchronized`, which Phase 2 depends on); instance count (decision 10 —
one ephemeral run per cron tick, so leader election is moot); UI vs reader API (decision 11).

---

## Working order

Start Phase 0 and 1. The fastest way to kill this is to design Phase 4's clustering before a
single email has arrived.
