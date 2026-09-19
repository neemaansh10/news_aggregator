# Scalable News Aggregator — System Design

**Author:** Akshat Chaudhary
**Scope:** Ingest millions of articles per day from thousands of publishers, collapse
duplicates, group independent coverage of the same real-world event into a single
*story*, rank those stories, and serve a de-duplicated feed to millions of users.

**Companion code:** the `newsagg/` package — a working, runnable implementation
of everything described here, with a 122-test suite. Every algorithm, threshold
and guarantee in this document is executable; the numbers in the calibration
tables were measured from that code, not estimated. See [README.md](README.md)
for how to run it and for the module layout.

---

## Table of contents

1. [The actual problem](#1-the-actual-problem)
2. [Design principles](#2-design-principles)
3. [High-level architecture](#3-high-level-architecture)
4. [Component responsibilities](#4-component-responsibilities)
5. [Data ingestion and processing pipeline](#5-data-ingestion-and-processing-pipeline)
6. [Duplicate detection and article similarity](#6-duplicate-detection-and-article-similarity)
7. [Story clustering](#7-story-clustering)
8. [Ranking and re-ranking](#8-ranking-and-re-ranking)
9. [Database and data model](#9-database-and-data-model)
10. [Caching strategy](#10-caching-strategy)
11. [APIs](#11-apis)
12. [Idempotency and the no-duplicate guarantee](#12-idempotency-and-the-no-duplicate-guarantee)
13. [Late-arriving and updated articles](#13-late-arriving-and-updated-articles)
14. [Scalability](#14-scalability)
15. [Fault tolerance and failure scenarios](#15-fault-tolerance-and-failure-scenarios)
16. [Key trade-offs](#16-key-trade-offs)
17. [Observability and SLOs](#17-observability-and-slos)
18. [Running it on your laptop](#18-running-it-on-your-laptop)
19. [Production deployment](#19-production-deployment)
20. [What I would build next](#20-what-i-would-build-next)

---

## 1. The actual problem

The brief looks like a crawler-plus-database problem. It isn't. The hard
requirement is buried in the last paragraph:

> The system should not simply identify identical URLs/articles as duplicates. It
> should recognize that multiple articles from different sources are reporting the
> same underlying story and represent them as a single story in the user's feed,
> while using the number and quality of sources reporting that story as a ranking
> signal.

That single sentence forces three decisions that shape the entire design:

**1. The unit of the product is a story, not an article.** The feed, the ranking,
the cache and the "don't repeat yourself" guarantee all operate on a *cluster* of
articles. Articles become evidence for a story rather than items in their own right.

**2. "Duplicate" is not one problem, it is four.** The same URL arriving twice, the
same text under a different URL, a wire story with a rewritten headline, and two
journalists independently covering the same press conference are four different
problems with four different right answers. Treating them uniformly either
over-merges (distinct events collapse into one) or under-merges (the feed fills
with near-identical items). Section 6 handles them as a cascade.

**3. Counting sources is a trap.** If ten outlets all republish the same Reuters
wire copy, that is *one* source of information, not ten. A naive source count makes
syndicated content dominate the ranking — exactly the failure mode users describe
as "the same story ten times". The system therefore distinguishes **independent
reporting** from **syndicated copies** and discounts the latter. This is the single
most important modelling decision in the ranking layer.

### Scale targets

| Dimension | Target | Derived load |
|---|---|---|
| Articles ingested | 5 M / day | ~58/s mean, ~250/s peak (4× diurnal) |
| Sources polled | 50,000 feeds | ~600 fetches/s at a 90 s mean cadence |
| Active stories | ~600 K / day, ~1.5 M in a 72 h window | |
| Users | 5 M DAU, ~12 feed calls each | ~700 RPS mean, 3–5 K RPS peak |
| Freshness SLO | publish → visible in feed | p95 < 60 s |
| Feed latency SLO | | p95 < 150 ms |

---

## 2. Design principles

These are the rules I held myself to; most of the non-obvious choices later in the
document fall out of them.

**Invariants live in the schema, not in application code.** "A user never sees the
same story twice" is a `PRIMARY KEY (user_id, story_id)`. "The same URL is never
stored twice" is a `UNIQUE` index. Code that enforces a rule can be bypassed by the
next feature; a constraint cannot.

**Every stage is idempotent.** The pipeline is at-least-once end to end. Rather
than chase exactly-once delivery — which is expensive and usually a lie — every
stage is safe to replay. That turns retries, redeliveries and crash recovery from
correctness problems into performance problems.

**Cheap filters before expensive ones.** Exact hash lookups run on every article;
LSH runs on what survives; vector comparison runs only against a shortlisted
handful of candidate stories. The expensive operation never sees the full corpus.

**Thresholds are measured, not guessed.** Every similarity threshold in the code
was calibrated against labelled same-event / different-event pairs, and the
measurements are reproduced in this document. A threshold without a measurement
behind it is a bug waiting for production traffic.

**Degrade, don't fail.** A dead source, a Redis outage, or a Kafka partition
falling behind must reduce quality, not availability. Every external dependency has
a defined fallback.

---

## 3. High-level architecture

```
                         ┌──────────────────────────────────────┐
  RSS / Atom  ──┐        │        FETCH TIER (stateless)        │
  Publisher API ├───────▶│  scheduler · fetchers · rate limiter │
  Push webhook ─┘        │  conditional GET · circuit breakers  │
                         └───────────────────┬──────────────────┘
                                             │  topic: raw.articles
                                             │  key = source_id
                                             ▼
                         ┌──────────────────────────────────────┐
                         │      NORMALISER (stateless, N×)      │
                         │  HTML strip · URL canonicalisation   │
                         │  language · topics · entities        │
                         │  content hash · SimHash · MinHash    │
                         └───────────────────┬──────────────────┘
                                             │  topic: normalised.articles
                                             │  key = blocking_key
                                             ▼
                         ┌──────────────────────────────────────┐
     ┌──────────┐        │   STORY ENGINE (sharded by key)      │        ┌──────────┐
     │  Redis   │◀──────▶│  L1 exact  → L2 LSH  → L3 semantic   │◀──────▶│ Postgres │
     │  cache   │        │  cluster assign · centroid update    │        │  primary │
     └──────────┘        │  source counting · scoring           │        └────┬─────┘
                         └───────────────────┬──────────────────┘             │
                                             │  topic: story.updates          │ replica
                                             ▼                                ▼
                         ┌──────────────────────────────────────┐        ┌──────────┐
                         │   RANKER + MAINTENANCE (periodic)    │        │  read    │
                         │  decay re-scoring · merge pass       │        │ replicas │
                         │  retention · DF flush                │        └────┬─────┘
                         └──────────────────────────────────────┘             │
                                                                              ▼
                         ┌──────────────────────────────────────────────────────────┐
   Clients ─────────────▶│              FEED API (stateless, N×)                    │
                         │  ranked feed · delivery ledger · cursor replay · caching │
                         └──────────────────────────────────────────────────────────┘
```

### Why this shape

**The fetch tier is separated from processing** because they fail differently and
scale differently. Fetching is I/O-bound, latency-dominated and hostage to other
people's servers; processing is CPU-bound and predictable. Coupling them means one
slow publisher stalls the pipeline.

**The bus between stages is a log, not a queue.** Kafka's retained, replayable log
means a bad deploy in the story engine is recoverable by resetting the consumer
offset and replaying, rather than by re-crawling 50,000 publishers.

**The story engine is the only stateful stage,** and it is sharded by a *blocking
key* (Section 7) so that articles which could plausibly belong to the same story
always land on the same partition. That gives single-writer-per-story semantics
with no distributed locking on the hot path — the single most important scalability
decision in the design.

**The read path never touches the write path.** The feed API reads pre-computed
scores from replicas and cache. Ranking is materialised by a background job, so a
traffic spike cannot slow down clustering, and a clustering backlog cannot slow
down the feed.

---

## 4. Component responsibilities

| Component | Module | Responsibility | State | Scaling |
|---|---|---|---|---|
| **Fetch scheduler** | `ingestion/fetcher.py` | Decide which sources are due; claim them | Source cadence in DB | Leader-elected, 1 active |
| **Fetcher** | `ingestion/fetcher.py` | HTTP with conditional GET, parse RSS/JSON, emit raw articles | Stateless | Horizontal, N pods |
| **Normaliser** | `ingestion/normaliser.py` | Clean, canonicalise, fingerprint, classify | Stateless | Horizontal, N pods |
| **Story engine** | `clustering/engine.py` | Dedup cascade, cluster assignment, centroid and counter maintenance | Owns stories | Sharded by blocking key |
| **Ranker** | `ranking/ranker.py` | Periodic re-scoring so recency decay applies | Stateless | 1–2 replicas |
| **Merge pass** | `pipeline/jobs.py` | Repair over-split clusters | Stateless | 1 replica, periodic |
| **Feed API** | `api/routes/feed.py` | Ranked feed, delivery ledger, cursor replay | Stateless | Horizontal, N pods |
| **Retention job** | `pipeline/jobs.py` | Archive cold stories, prune index tables | Stateless | CronJob |

The package mirrors this table: each subpackage is one deployable service, and
the shared libraries (`text/`, `db/`, `infra/`) have no dependencies on any of
them, so the import graph is acyclic and one-directional.

---

## 5. Data ingestion and processing pipeline

### 5.1 Fetching

Three things here are load-bearing in production and usually missing from
first-draft implementations:

**Conditional GET.** Every source row stores the last `ETag` and `Last-Modified`.
Subsequent polls send `If-None-Match` / `If-Modified-Since`, and most return a
304 with an empty body. At 50,000 feeds polled every 90 seconds this is the
difference between ~2 TB/day of redundant downloads and roughly 5% of that.

**Adaptive cadence.** A poll that yields nothing multiplies the interval by 1.5
(capped at one hour); a productive poll multiplies it by 0.8. Quiet feeds drift
towards hourly and free capacity for fast-moving ones, without any manual tuning.

**Per-source circuit breakers.** After five consecutive failures the source trips
and is skipped for an exponentially growing cooldown. This protects *their* origin
as much as our worker pool — without it, a publisher having an outage gets hammered
by a retry storm at exactly the wrong moment.

Politeness is enforced by a per-domain token bucket, so a publisher with forty feeds
on one host is not hit by forty simultaneous requests.

### 5.2 Normalisation

Normalisation is the highest-leverage step in the system: a canonicalisation bug
surfaces downstream as thousands of phantom "new stories". It is written as a pure
function (`RawArticle → NormalisedArticle`) so it can be unit-tested exhaustively
without any infrastructure.

**URL canonicalisation** — the identity of a document:

| Step | Example |
|---|---|
| Force https, lowercase host, drop `www.` and default ports | `HTTP://WWW.Ft.com:443/x` → `https://ft.com/x` |
| Collapse `//`, strip trailing `/` and `/amp` suffixes | `/news//a/amp/` → `/news/a` |
| Remove tracking params (40+: `utm_*`, `fbclid`, `gclid`, `ocid`, …) | `?utm_source=rss&fbclid=z` → `` |
| Sort remaining params, drop the fragment | `?b=2&a=1#top` → `?a=1&b=2` |

This is verified end to end in the implementation: `https://ft.com/content/fed-holds-rates/?fbclid=zzz&utm_campaign=social`
and `https://www.ft.com/content/fed-holds-rates?utm_source=x` resolve to the same
`url_hash` and the second is rejected as an exact duplicate.

**Text normalisation** — Unicode NFKD, accent stripping, smart-quote and dash
folding, case folding, whitespace collapse. Applied before every hash and every
tokenisation, so cosmetic encoding differences never create false distinctions.

**Enrichment** — language (hard blocking key; we never cluster across languages),
topics, and entities (Section 6.4).

**Defensive clamping** — a `published_at` more than two hours in the future is
rewritten to now. Publishers really do post-date articles to pin themselves to the
top of date-sorted feeds; without this, one source can monopolise the front page.

### 5.3 Stage contract

Each stage consumes from one topic and produces to the next, commits its offset only
after successful handling, and isolates poison messages (one malformed article must
never stall a partition; in production it is routed to a DLQ topic with the failure
attached).

---

## 6. Duplicate detection and article similarity

This is the core of the system. Four layers run as a cascade, each one narrowing
the input to the next.

```
   article
      │
      ├─ L1a  canonical URL hash  ─────────── exact  ──▶ suppress (pure replay)
      │        one indexed lookup
      │
      ├─ L1b  content hash  ────────────────  exact  ──▶ syndicated / duplicate
      │        one indexed lookup
      │
      ├─ L2a  SimHash + LSH bands  ─────────  near   ──▶ syndicated / near-dup
      │        one indexed lookup + popcounts
      │
      ├─ L3   composite similarity vs        ─ same  ──▶ join story
      │        story centroids                event
      │        blocking lookup + ≤60 dot products
      │
      └─ L2b  MinHash vs that story's        ─ near  ──▶ syndicated / near-dup
               members (bounded, precise)
```

Note the ordering: **L2b runs after L3**, not before. That is deliberate and it is
the part of the design I would most want to explain in an interview.

### 6.1 L1 — exact duplicates

Two hashes, both single indexed lookups:

- `url_hash` = SHA-256 of the canonical URL, with a `UNIQUE` constraint. This makes
  exact-duplicate rejection a *database guarantee* rather than an application check,
  which is what makes the whole ingest path safely retryable.
- `content_hash` = SHA-256 of normalised title + first 8 KB of normalised body.
  Catches the same text republished under a different URL.

A content-hash match from a **different** source is not noise — it is syndication,
and it is recorded as such rather than thrown away, because it still tells us that
outlet is carrying the story (Section 8).

### 6.2 L2a — near-duplicates via SimHash and LSH

SimHash produces a 64-bit locality-sensitive fingerprint: documents differing only
by a rewritten headline, a boilerplate footer or a few edited sentences land a small
Hamming distance apart.

Scanning every stored fingerprint is O(N) and hopeless at 5 M articles/day, so the
64 bits are split into **8 bands of 8 bits**, each indexed. By the pigeonhole
principle, two fingerprints within Hamming distance ≤ 7 **must** agree on at least
one band — so a band-equality lookup is a *complete* candidate generator at that
threshold, with no recall loss, using one indexed query.

**The non-obvious part: the fingerprint must be body-dominant.**

The instinctive implementation upweights the headline, since the headline carries
the meaning. Measured, that is exactly wrong for this task — a rewritten headline
over an untouched wire body is the most common near-duplicate in news, and a
title-heavy fingerprint puts that pair outside any usable threshold:

| SimHash weighting | Wire copy, rewritten headline | Independent article, same event | Different event |
|---|---|---|---|
| Title-heavy (title 4×, title shingles 3×) | **18 bits** ✗ | 20 | 34 |
| Body-dominant (title 1×, body 2×, body 3-shingles 3×) | **9 bits** ✓ | 22–27 | 31–36 |

Body-dominant weighting halves the distance for true near-duplicates while *widening*
the gap to genuinely independent reporting. The code uses the second row.

### 6.3 L2b — MinHash within the cluster

Even at 9 bits, the wire copy above sits outside the pigeonhole-complete threshold
of 7. Raising the SimHash threshold to reach it would drag in unrelated articles and
break the recall guarantee.

So the work is split. LSH does the cheap global sweep. Then, *after* an article has
been routed to a story, a **MinHash** comparison runs against that story's existing
members — a bounded set (one cluster, ≤25 members), which can therefore afford to be
precise. MinHash over body 3-shingles gives an unbiased Jaccard estimate from a
64-value signature, with no need to re-read article bodies.

Measured on body 3-shingles:

| Pair type | Jaccard |
|---|---|
| Wire copy with rewritten headline | **0.74** |
| Independent articles, same event | ≤ 0.12 |
| Different events | 0.00 |

Threshold **0.45** sits in a gap six times wider than the noise.

This two-part design is a general pattern worth naming: *approximate and global,
then exact and local*. The approximate stage bounds the candidate set; the exact
stage is affordable only because the approximate stage ran first.

### 6.4 L3 — semantic similarity: same event, different words

L1 and L2 answer "is this the same *article*". L3 answers "is this the same
*story*", which is the question the product actually cares about, and it needs a
representation where different journalists writing about one event look alike.

**Streaming TF-IDF.** A batch-fitted vectoriser is wrong for a never-ending stream:
the vocabulary and document frequencies move continuously. Instead, terms are hashed
into a fixed 2¹⁸ space and document frequencies are maintained *online*. This gives
no vocabulary to fit, ship or version; O(1) memory per term; stable dimensionality
forever; and brand-new vocabulary ("Europa plumes") usable the moment it appears.
Vectors are L2-normalised and truncated to the top 192 dimensions, so cosine
similarity is a plain sparse dot product and centroids are small enough to store as
JSON.

**Two calibration findings that materially changed the result:**

| Encoder variant | Same-event cosine | Different-event cosine |
|---|---|---|
| Title 3×, title bigrams 2× | 0.21 – 0.34 | ≤ 0.02 |
| Title 2×, **no bigrams** | **0.28 – 0.47** | ≤ 0.02 |

Title bigrams look attractive and are actively harmful: two outlets essentially
never phrase a headline the same way, so the bigrams are unmatched mass that
inflates the vector norm and roughly halves the cosine between genuine same-event
pairs. Removing them nearly doubled the signal at no cost to separation. Title
weight above 2× similarly drowns out the body vocabulary, which is the part
different outlets actually have in common.

**Entity overlap.** Cosine alone is blind to synonymy — it scores "Fed" against
"Federal Reserve" near zero. But two articles about the same event almost always
share the same proper nouns and the same numbers ("Japan", "6.4", "Europa"), even
when the framing is completely different. Entities are extracted with a
capitalisation-and-numeral heuristic (transparent, dependency-free, and swappable
for spaCy or a transformer NER behind the same interface), including the multi-word
form, which is near-unique: `japan_meteorological_agency`.

**The composite.** The two signals fail in different directions, which is precisely
why they are combined:

```
similarity = 0.70 · cosine(TF-IDF)  +  0.30 · jaccard(entities)
```

| Pair type | Composite similarity |
|---|---|
| Same event, different outlets | **0.20 – 0.66** |
| Different events | **≤ 0.04** |

Roughly 5× separation, which is what makes a single global threshold viable at all.

### 6.5 Calibration procedure

Thresholds are not universal constants; they depend on the source mix, the
languages, and how much body text the feeds actually provide. The re-calibration
loop:

1. Sample ~500 article pairs from the live corpus, stratified so same-event pairs
   are not swamped by the overwhelming majority of unrelated ones.
2. Label them (two annotators, adjudicate disagreements — inter-annotator agreement
   on "same event" is around 0.9 κ, which is the realistic ceiling).
3. Sweep the threshold and plot precision/recall.
4. Pick the operating point from the product's error preference. For a news feed,
   **over-splitting is much cheaper than over-merging**: a duplicated story is an
   annoyance, whereas merging two distinct events produces a story whose headline
   contradicts its own source list. So the threshold is set conservatively high, and
   the offline merge pass (Section 7.4) cleans up the resulting splits.
5. Re-run monthly and whenever the source mix changes materially.

---

## 7. Story clustering

### 7.1 Online, single-pass clustering

News clustering must be **incremental**: articles arrive continuously, and the feed
must reflect an event within seconds. Batch k-means style algorithms are unusable —
they need the full corpus, a chosen *k*, and a full re-run per batch.

The algorithm is leader–follower (online centroid) clustering:

```
for each incoming article:
    candidates ← stories sharing a high-weight term, within the time window
    best       ← argmax over candidates of  similarity(article, story)
                   × time-proximity bonus × topic-agreement bonus
    if similarity(best) ≥ 0.30:  join best,  update centroid incrementally
    else:                        create a new story with this article as seed
```

Cost is O(candidates), not O(stories). No *k*. Single pass. Naturally incremental.

### 7.2 Blocking — the part that makes it scale

A linear scan over 1.5 M active stories per article is 75 billion comparisons a day.
Blocking reduces that to tens.

Two mechanisms, applied together:

**Inverted index on centroid terms.** Each story's top 64 centroid dimensions are
indexed in `story_terms`. An incoming article looks up only stories sharing one of
its own top 24 terms, restricted to active stories in the last 72 hours and the same
language. Typically ~60 candidates survive, and exact similarity runs only on those.

**Partition key.** `blocking_key = language : primary_topic : 48-hour bucket`.
Articles that could not possibly belong to the same story are kept apart, and — more
importantly — the key is the **Kafka partition key**. Every article that might join
a given story routes to the same consumer, which means each story has exactly one
writer. No distributed locks, no optimistic-concurrency retries on the hot path.

The 48-hour bucket (rather than 24) prevents events that straddle midnight from being
split by the bucketing itself.

### 7.3 Centroid maintenance

Joining an article updates the centroid as a streaming mean, re-truncated to the top
192 dimensions and re-normalised. Entities accumulate with *counts* rather than as a
flat set, then get pruned by frequency — an entity mentioned by many members
characterises the event, while a one-off mention is usually incidental.

Two deliberate details:

- **Duplicates never move the centroid.** If a heavily syndicated wire story were
  allowed to update it repeatedly, the cluster would drift towards that single
  phrasing and stop accepting original reporting.
- **The inverted index is rewritten lazily** — always for the first three members,
  then every fourth join. The centroid barely moves after a few members, so
  rewriting 64 index rows per article is wasted I/O.

### 7.4 The merge pass — repairing over-splits

Because the assign threshold is set conservatively (Section 6.5), the system
deliberately over-splits. A periodic pass (every 2 minutes) compares story
signatures within a language/day group and merges pairs scoring ≥ 0.26.

Two clusters of one event genuinely do diverge early — the first articles may use
"blast" while later ones use "explosion", or, as observed in the implementation's
own test corpus, one outlet writes "Fed" while four write "Federal Reserve" and the
lexical overlap is initially too thin to join. Once more coverage arrives, the
centroids converge and the merge catches it.

**Why the merge threshold (0.26) sits *below* the assign threshold (0.30)** — this
looks backwards and is not:

- A centroid is a mean over members, and averaging shrinks the distinctive term
  weights that drive cosine. Cluster-to-cluster scores are therefore systematically
  damped relative to article-to-cluster scores.
- A merge rests on more evidence: two aggregates agreeing, rather than one article
  matching one aggregate.
- The merge pass runs offline, where a mistake is observable and reversible, rather
  than on the ingest hot path.

Merging is not symmetric. The cluster with the most independent sources absorbs the
other, so the surviving story keeps the richest centroid and the longest history.
The absorbed story is marked `merged`, keeps a `merged_into_id` pointer so old
story IDs continue to resolve, and — critically — **its delivery records are
propagated forward** so the merge cannot resurface the event in a user's feed
(Section 12).

### 7.5 Verified behaviour

Run against a corpus of 22 articles covering 5 real events plus deliberately planted
duplicates, the pipeline produces:

```
articles stored     : 20        (2 exact-URL replays rejected at the boundary)
duplicates detected : syndicated=2
stories formed      : 5         (exactly the 5 planted events)
stories merged      : 1         (the "Fed" / "Federal Reserve" split, repaired)
```

Against **live RSS feeds** from BBC, The Guardian and NPR simultaneously, it grouped
three independently-written headlines into one story:

```
[0.195]  3 sources | US and Denmark reach deal over Greenland after Trump annexation threat
     guardian      US and Denmark reach security deal after Trump's threats to take Greenland
     bbc-world     US and Denmark reach deal over Greenland after Trump annexation threat
     npr-world     U.S. and Denmark reach deal to build U.S. military presence in Greenland
```

---

## 8. Ranking and re-ranking

```
score = ( Wa·authority + Wb·breadth + Wv·velocity ) × recency  +  Wr·relevance

          Wa = 0.45      Wb = 0.30      Wv = 0.15      Wr = 0.10
```

| Term | Definition | Why |
|---|---|---|
| **authority** | `log1p(Σ source reliability, syndication-discounted)` ÷ norm | How much *credible* coverage exists |
| **breadth** | `log1p(count of independent sources)` ÷ norm | How many *different* outlets confirm it |
| **velocity** | `log1p(new independent sources in the last hour)` ÷ norm | Separates breaking from merely well-covered |
| **recency** | `0.5 ^ (effective_age / 8 h)` | Freshness decay |
| **relevance** | per-user topic affinity, 0–1 | Personalisation |

### The decisions inside the formula

**Logarithmic source counts.** The second independent source confirming a story is
enormously more informative than the twentieth. Linear counting lets a single
mega-covered event flatten everything else in the feed.

**Recency is multiplicative, not additive.** This is the difference between a feed
that stays fresh and one that doesn't. If recency were a summed term, a story with
overwhelming coverage could outscore everything for days. As a multiplier, decay
applies to the *whole* score — nothing escapes it.

**Effective age blends event time and last activity (70/30).** Pure event time
buries a developing story that is still gaining coverage; pure last-activity lets a
trivial late follow-up resurrect a dead one. The blend keeps developing stories
alive without reviving stale ones.

**Personalisation is added *after* decay,** and capped at `Wr = 0.10`. A user's
topic affinity can reorder comparable stories; it can never let a stale niche item
outrank a major breaking event. This is a deliberate editorial position: the feed
is a news product, not a preference-maximiser.

**Syndication discount (0.35).** Independent reporting counts at full source
reliability; a syndicated copy of another outlet's text counts at 35%. It still
counts for something — an outlet choosing to carry a story is weak evidence of
importance — but it is not independent confirmation. Without this, a single wire
story republished by forty outlets outranks a genuine exclusive.

**Source reliability is an explicit 0–1 input,** not something inferred. It is
editorial policy and belongs under human control, versioned and auditable. Inferring
trust from engagement is how aggregators end up promoting whichever source is best
at engagement bait.

### Re-ranking

Scores are recomputed for all active stories every 30 seconds. Without this, decay
would only be applied when a story happened to receive a new article, and quiet
stories would never age out. Recomputation is a single indexed sweep over the active
window. Per-user personalisation is applied at read time over an over-fetched
candidate window, so the expensive global ranking stays shared across all users.

---

## 9. Database and data model

**PostgreSQL** as the system of record. The workload is relational (articles belong
to stories, stories aggregate sources), needs multi-row transactional integrity
(delivery ledger + page materialisation must commit together), and depends on unique
constraints for correctness. Those are exactly Postgres's strengths. The
implementation runs on SQLite by default purely so the project has zero setup cost;
the same SQLAlchemy models target Postgres by changing one environment variable.

```
  sources ──< articles >── stories ──< story_terms
                  │            │
                  │            └──< feed_deliveries >── (user)
                  │            └──< feed_pages
                  └──< simhash_bands
```

### Core tables

**`sources`** — registry plus operational state: `reliability` (ranking input),
`etag` / `last_modified` (conditional GET), `poll_interval_s` / `next_poll_at`
(adaptive cadence), `consecutive_failures` / `breaker_open_until` (circuit breaker).
Operational state lives with the source rather than in memory so that any fetcher
pod can pick up any source, and a restart loses nothing.

**`articles`** — one row per fetched article, *including duplicates*. Duplicates are
marked, never deleted:

- `url_hash` **UNIQUE** — the exact-duplicate guarantee, in the schema
- `content_hash`, `simhash`, `minhash` — the three fingerprints
- `dup_of_id` + `dup_kind` ∈ {`exact_url`, `exact_content`, `near_dup`, `syndicated`}
- `story_id`, `similarity`, `revision`

Keeping duplicates matters for three reasons: source counting needs to know an
outlet carried the story even if the text was identical; "12 sources reporting" must
be auditable back to the actual articles; and deleting rows makes replayed events
non-idempotent.

**`stories`** — the user-facing unit. Carries the `centroid` (sparse, JSON),
`entity_counts`, denormalised `independent_source_count` / `syndicated_source_count`
/ `authority` / `velocity` / `score`, and `merged_into_id` for merge chains.
`version` increments on every material change, which drives cache keys and lets the
delivery ledger reason about updates.

**`story_terms`** — the inverted index that makes blocking possible; indexed on
`term`.

**`simhash_bands`** — the LSH index, one row per (band, value, article), indexed on
`(band_idx, band_val, created_at)`.

**`feed_deliveries`** — `PRIMARY KEY (user_id, story_id)`. The no-duplicate
guarantee.

**`feed_pages`** — `PRIMARY KEY (user_id, cursor)`. Materialised page for exact
cursor replay.

**`idempotency_keys`** — stored request fingerprint + response for safe write retries.

**`outbox`** — transactional outbox: a state change and its event commit in the same
transaction, and a relay publishes to Kafka afterwards. This is what makes the
pipeline at-least-once *without* dual-write data loss.

**`term_stats` / `corpus_stats`** — persisted streaming document frequencies, so
IDF survives a restart.

### Denormalisation and why counters are recomputed

Source counts and scores are denormalised onto `stories` because the feed query must
filter and sort on them, and a per-request aggregate over `articles` would be far too
slow at feed QPS.

They are **recomputed from `articles`** on every change rather than incremented.
Counters drift under retries, merges and partial failures; a recount is always
correct and is a single indexed aggregate over one story's rows. That trade —
slightly more write cost for an invariant that cannot silently rot — is worth it for
a number that is displayed to users and drives ranking.

### Partitioning at scale

- `articles` — `RANGE` partitioned by `published_at`, monthly. Old partitions detach
  to cold storage in one DDL statement instead of a multi-hour `DELETE`.
- `simhash_bands` — partitioned daily. The dedup window is 7 days, so retention is
  `DROP PARTITION`, not a tombstone-generating delete.
- `feed_deliveries` — `HASH` partitioned by `user_id` (this is the largest table:
  5 M users × ~200 retained stories ≈ 1 B rows). Every query is user-scoped, so hash
  partitioning is a clean fit.
- Article **bodies** move to object storage (S3) after 48 hours, leaving the row with
  a pointer. Bodies are ~85% of the bytes and are only needed on the hot path during
  clustering.

---

## 10. Caching strategy

Five layers, each with an explicit invalidation rule. The important discipline is
that every cached value has a defined way to become wrong and a defined way to be
corrected — a cache without an invalidation story is a bug with a TTL.

| Layer | Contents | TTL | Invalidation |
|---|---|---|---|
| **CDN** | Anonymous top stories, images | 30 s | TTL only |
| **Application** | `/v1/stories` ranked list | 15 s | Prefix purge on story update |
| **Story detail** | Full story payload, keyed `story:{id}:v{version}` | 60 s | Version in key → new version, new key |
| **Feed pages** | Materialised page per cursor | 1 h | Immutable by construction |
| **Local (in-process)** | Hot centroids, source registry, IDF table | 30–60 s | Periodic refresh |

**Version-keyed story cache.** Because the cache key embeds `story.version`, an
updated story simply misses on a new key. There is no invalidation race, no
stale-read window, and no purge fan-out — the class of bug where a user sees "3
sources" on a story that has 12 is structurally impossible.

**Feed pages are immutable.** A materialised page is a historical record of what was
served, so it never needs invalidating — it only needs expiring.

**Redis is optional, not required.** The implementation falls back to an in-process
LRU with identical semantics. A Redis outage degrades cache hit rate and cross-pod
sharing; it does not take the system down. This is tested by simply not configuring
Redis, which is the default.

**Cache stampede** on a breaking story is handled by serving stale-while-revalidate
with a short lock, so one request recomputes and the rest are served the previous
value rather than thundering onto the database.

---

## 11. APIs

Full interactive documentation at `/docs` (FastAPI generates OpenAPI from the same
type annotations the code is validated with, so the spec cannot drift from the
implementation).

### Read

```http
GET /v1/feed?user_id=&limit=&cursor=&topic=&language=&min_sources=
             &personalise=&resurface_updates=
Header: Idempotency-Key: <optional, makes the first page replayable>
```

Ranked, de-duplicated, cursor-paginated feed. Returns each story with its full
signal breakdown so the ranking is explainable rather than a black box:

```jsonc
{
  "stories": [{
    "id": "s_207d4690d98699c6da488aec",
    "title": "Federal Reserve holds interest rates steady as inflation cools",
    "url": "https://reuters.com/...",
    "topics": ["business"],
    "score": 0.6571,
    "signals": {
      "independent_sources": 9,
      "syndicated_sources": 0,
      "articles": 9,
      "authority": 5.41,
      "velocity": 6.0,
      "recency_multiplier": 0.8849
    },
    "sources": [{ "name": "reuters.com", "reliability": 0.95, "independent": true }, ...],
    "source_count": 9,
    "event_time": "2026-09-19T12:26:00Z",
    "version": 14
  }],
  "next_cursor": "eyJzIjowLjUxMDYwOSwiaSI6InNfMGM2Yj...",
  "replayed": false
}
```

```http
GET /v1/stories?limit=&min_sources=&topic=    # stateless ranked list, cacheable
GET /v1/stories/{id}                          # detail + every member article
```

`GET /v1/stories/{id}` follows `merged_into_id` chains, so a story ID handed out
before a merge keeps resolving to the surviving story instead of 404-ing. It returns
the full article list annotated with each article's relation (`original`,
`syndicated`, `near_dup`), which makes the de-duplication decisions auditable.

### Write

```http
POST /v1/ingest                Header: Idempotency-Key: <optional>
POST /v1/sources               GET /v1/sources
POST /v1/users/{id}/affinity
DELETE /v1/users/{id}/deliveries          # dev helper: forget what a user saw
POST /v1/admin/merge-pass                 # force reconciliation
```

### Operations

```http
GET /healthz    # liveness  — is the process able to serve at all
GET /readyz     # readiness — can we actually reach our dependencies (503 if not)
GET /metrics    # Prometheus
GET /stats      # pipeline counters
```

The liveness/readiness split matters in Kubernetes: a pod that cannot reach the
database should be pulled out of the load balancer (`readyz` fails) but **not**
restarted (`healthz` passes), because restarting it fixes nothing and a restart loop
turns a database blip into a full outage.

---

## 12. Idempotency and the no-duplicate guarantee

The brief asks for two things that sound similar and are not: *idempotency* (a
retried write must not double-apply) and *no duplicate stories in the user feed*
(a read-side guarantee). They need different mechanisms.

### Write idempotency — four independent layers

**1. Unique constraint on `url_hash`.** The strongest layer, because it holds no
matter what the application does. A replayed article with the same URL and unchanged
content is a no-op; the same URL with *changed* content takes the revision path
(Section 13).

**2. Content-addressed detection.** Even under a different URL, identical text is
recognised by `content_hash` and recorded as a duplicate rather than stored twice.

**3. `Idempotency-Key` header.** The stored response is replayed verbatim for a
retried request. Reusing a key with a *different* body returns **409** rather than
silently accepting — a key collision means a client bug, and failing loudly is
better than corrupting data quietly.

**4. Transactional outbox.** The state change and its event commit together, so a
crash between "wrote to DB" and "published to Kafka" cannot lose the event.

Verified end to end:

```
POST /v1/ingest  (Idempotency-Key: batch-001)          → 202  created: 1
POST /v1/ingest  (same key, same body)                 → 202  created: 1, no new row
POST /v1/ingest  (same key, different body)            → 409
POST /v1/ingest  (same URL, different tracking params) → 202  duplicate: exact_url
```

### Feed de-duplication — the read-side guarantee

Three mechanisms together:

**The story is the unit.** Because the feed serves clusters, receiving the same
article from ten sources produces one feed item by construction. This is the
structural reason the guarantee holds; everything else is defence in depth.

**The delivery ledger.** `feed_deliveries` with `PRIMARY KEY (user_id, story_id)`.
Every page excludes everything already in the ledger, and the ledger write is
idempotent by virtue of the primary key. This is a schema guarantee, not a code
convention.

**Merge propagation.** When story A merges into story B, every delivery record for A
is copied forward to B in a single statement. Without this, a user who saw A would
see the same event again as B once the clusters merged — the exact bug the brief
warns about, arriving through the back door.

### Cursor replay — the failure mode most feeds get wrong

Naive keyset pagination over a feed that is being re-ranked underneath it silently
*loses* items: a client whose request times out retries, scores have shifted in the
meantime, and stories that were marked delivered are never actually displayed.

So a cursor is a **page token**, and `feed_pages` stores the exact story IDs served
under it. Re-requesting a cursor replays that page byte-identically.

But caching the *head* of the feed unconditionally would freeze it for the whole
TTL — newly broken stories would never reach a polling client, which is the opposite
of what a near-real-time product needs. The resolution:

| Request | Behaviour |
|---|---|
| No cursor | Always computes fresh — the head stays live |
| No cursor + `Idempotency-Key` | Replayable under that key — opt-in retry safety |
| With cursor | Always replays that exact page |

Verified:

```
HEAD poll #1  (user bob)               → AI chip, Fed            replayed: false
HEAD poll #2  (user bob)               → Earthquake, Election    replayed: false   ← live, no repeats
HEAD + Idempotency-Key (user carol)    → AI chip, Fed            replayed: false
HEAD + same key         (user carol)   → AI chip, Fed            replayed: true    ← identical
```

---

## 13. Late-arriving and updated articles

Three distinct cases, three distinct behaviours.

### A. A late article about an existing story

The common case, and it needs no special handling — that is the point of the design.
The clustering window is 72 hours, so an article arriving hours after the event is
scored against existing centroids like any other. It joins, the source counts are
recomputed, `last_activity_at` advances and the score rises.

Verified: an FT article about the Fed decision, pushed hours after the original five,
joined the existing story at similarity 0.667 and lifted it from 5 to 6 independent
sources — then from 6 to 9 as Bloomberg, WSJ and The Economist followed, moving the
story from rank 2 to rank 1.

Critically, `first_seen_at` and `event_time` are **preserved** (`event_time` only
ever moves *earlier*, to the earliest member). Recency decay measures the age of the
event, not the age of our knowledge of it, so late coverage cannot make an old event
masquerade as breaking news.

### B. A publisher edits an article in place

Corrections, developing stories, headline A/B tests. Detected by the same
`url_hash` arriving with a different `content_hash`. The row is kept, `revision` is
incremented, fingerprints are recomputed, and the LSH index entries are replaced.

The article is deliberately **not** re-clustered. Most in-place edits are copy edits,
and re-clustering on every edit churns the feed for no informational gain. Only a
change large enough to alter the fingerprints — which the revision path recomputes —
would affect anything downstream.

### C. A story gains substantially more coverage after a user saw it

This is the genuine tension with the no-duplicate guarantee, and the honest answer
is that the product rule is "never show the same story *as if it were new*", not
"never show it again".

So resurfacing is **opt-in** (`resurface_updates=true`, off by default) and gated:

- at least **2 more** independent sources than when the user saw it, **and**
- at least **50% more** than when the user saw it

Both conditions matter. The absolute floor stops trivial churn; the relative one
stops enormous stories from resurfacing forever. A resurfaced story is tagged
`is_update: true` with `previously_seen_source_count`, so the client can render
"5 more outlets are now reporting this" rather than presenting it as new. The
delivery watermark advances on re-delivery, so the same growth cannot resurface a
story twice.

Verified:

```
bob polls normally                        → Fed story absent (already delivered)
bob polls resurface_updates=true          → Fed story returns, tagged
                                            [UPDATE: was 5 sources, now 9]
bob polls resurface_updates=true again    → 0 stories (watermark advanced)
```

### D. Out-of-order events

The pipeline is keyed on event time, not arrival time. Kafka partitioning by
`blocking_key` gives ordering only within a partition, so the story engine is written
to be order-independent: counters are recomputed from the full article set rather
than incremented, and `event_time` takes a minimum. An article arriving out of order
produces the same final state as one arriving in order. Events later than the
72-hour window are handled by a backfill job rather than the streaming path.

---

## 14. Scalability

### Capacity model

| Stage | Per-unit cost | At 250 articles/s peak | Pods |
|---|---|---|---|
| Fetch | ~1 conn/source, 90 s cadence | ~600 fetches/s | 12 |
| Normalise | ~8 ms CPU (hashing dominates) | 2 CPU-s/s | 6 |
| Story engine | ~25 ms (blocking lookup + ≤60 comparisons) | 6 CPU-s/s | 16 (sharded) |
| Ranker | Indexed sweep of ~1.5 M rows / 30 s | ~0.3 CPU | 2 |
| Feed API | ~12 ms p50 (mostly cache + one indexed query) | 3–5 K RPS | 40 |

### Where each axis scales

**Fetching** is embarrassingly parallel — partition the source table by hash and add
pods. The scheduler is the only singleton, and it does nothing but claim due rows.

**Normalisation** is stateless and scales linearly with partitions.

**The story engine is the hard one**, because clustering is inherently stateful.
It scales by *blocking key*, which is what makes horizontal scaling possible at all:
every article that could join a given story routes to the same partition, so each
partition owns a disjoint slice of story space and needs no coordination. Adding
partitions increases throughput linearly; the only cost is a slightly higher
cross-partition split rate, which the merge pass repairs.

**Storage** scales by partitioning (Section 9) plus offloading article bodies to
object storage after 48 hours.

**The read path** scales with stateless API pods and read replicas. Feed queries are
user-scoped and hit a hash-partitioned table, so they parallelise cleanly.

### Handling a breaking-news spike

A major event produces a 10× spike concentrated on *one* cluster, which is the
pathological case for a partitioned design: one partition gets hot while the others
idle.

Mitigations, in order of effect: the LSH and exact-hash layers absorb most of the
spike cheaply, since a large share of the volume is syndicated copies that never
reach the expensive clustering path; the centroid for a hot story is pinned in local
memory; index rewrites are throttled for large clusters (the centroid of a
500-member story does not meaningfully move); and consumer lag on a single partition
degrades freshness for that one story rather than for the system.

---

## 15. Fault tolerance and failure scenarios

| Failure | Detection | Behaviour | Recovery |
|---|---|---|---|
| Source returns 5xx / times out | Fetch error counter | Retry ×3 with full-jitter backoff; circuit breaker opens after 5 consecutive failures | Half-open probe after exponential cooldown |
| Source serves malformed XML | Parse exception | Article dropped, source marked degraded | Alert if the failure rate exceeds 20% over 15 min |
| Publisher floods with spam | Ingest-rate anomaly per source | Per-source rate cap; reliability lowered | Manual review |
| Normaliser pod crashes | Liveness probe | Kafka rebalances the partition; uncommitted offsets replay | Automatic, at-least-once |
| Poison message | Handler exception | Isolated and routed to DLQ; the partition keeps moving | Manual replay after a fix |
| Story-engine partition lags | Consumer lag metric | Freshness degrades for that blocking key only | Scale partitions; backfill |
| Postgres primary fails | Health check | Reads continue from replicas; writes fail fast | Managed failover (~30 s) |
| Redis unavailable | Connection error | Falls back to in-process LRU; hit rate drops | Reconnect on recovery |
| Kafka unavailable | Producer error | Outbox retains events; ingestion backpressures | Relay drains on recovery |
| Clock skew between publishers | Future `published_at` | Clamped to now + 2 h | — |
| Over-merge (two events in one story) | Manual report / topic-coherence monitor | Story split via admin tooling | Raise threshold; re-calibrate |
| Over-split (one event, many stories) | Duplicate-rate monitor | Merge pass repairs within 2 min | Lower merge threshold |

### The failures I would actually worry about

**Silent quality regression.** Everything stays green — no errors, no lag — while
clustering quality drifts because a large source changed its feed format and started
emitting truncated bodies. This is the most dangerous failure mode because no
infrastructure alarm fires. Mitigation: track the distribution of cluster sizes,
singleton rate, and mean body length *per source*, and alert on distribution shift
rather than on errors.

**Cascading merges.** A single bad merge joins two events; the combined centroid
becomes broader and more permissive; it attracts a third event; repeat. Mitigations:
merge only within a blocking group, require both clusters to agree (not one article
against a cluster), cap merges per pass, and monitor for stories whose member
entities have low mutual coherence.

**Feed starvation.** A user who reads heavily exhausts the delivery ledger and gets
an empty feed. Mitigations: `resurface_updates` for materially-developed stories,
ledger entries expiring after 30 days, and a topic-diversity floor so one dominant
topic cannot consume the whole window.

---

## 16. Key trade-offs

**Online clustering vs. batch re-clustering.**
*Chosen:* online, single-pass. *Cost:* the result depends on arrival order, and
early articles have outsized influence on the centroid. *Why:* batch re-clustering
cannot deliver a sub-minute freshness SLO. The merge pass recovers most of the
quality difference at a fraction of the cost.

**TF-IDF + entities vs. neural embeddings.**
*Chosen:* streaming TF-IDF plus entity overlap. *Cost:* blind to synonymy — "Fed"
vs "Federal Reserve" scored near zero and required the merge pass to repair. *Why:*
no GPU, no model-versioning problem, no cold-start on new vocabulary, fully
explainable, and ~0.1 ms per comparison instead of ~10 ms. The `Signature` /
`similarity()` seam exists precisely so a sentence-transformer backend drops in
without touching anything else — and that is the first upgrade I would make.

**Conservative assign threshold, aggressive merge pass.**
*Chosen:* over-split, then repair. *Cost:* a brief window where one event appears as
two stories. *Why:* for a news feed, over-merging is far more damaging — a story
whose headline contradicts its own source list destroys trust, whereas a transient
duplicate is a minor annoyance.

**Denormalised counters, recomputed not incremented.**
*Chosen:* recompute on change. *Cost:* extra write amplification. *Why:* incremented
counters drift under retries and merges, and this number is displayed to users *and*
drives ranking. Correctness wins.

**Delivery ledger vs. a client-side seen-set.**
*Chosen:* server-side ledger. *Cost:* the largest table in the system (~1 B rows).
*Why:* it is the only approach that works across devices and survives a reinstall,
and it is cleanly hash-partitionable by `user_id`.

**Postgres vs. a dedicated vector database.**
*Chosen:* Postgres with an inverted index. *Cost:* blocking is coarser than true ANN
search. *Why:* one fewer system to operate, and transactional consistency between
clustering state and the delivery ledger. At 10× scale I would move similarity search
to pgvector (same database, HNSW index) before adding Vespa or Milvus.

**Kafka vs. a simple queue.**
*Chosen:* a retained log. *Cost:* operational complexity. *Why:* replay. A bad
clustering deploy is recoverable by resetting an offset rather than by re-crawling
the internet.

---

## 17. Observability and SLOs

| SLO | Target | Measured by |
|---|---|---|
| Ingest freshness | p95 publish → feed < 60 s | `newsagg_pipeline_lag_seconds` |
| Feed latency | p95 < 150 ms | Request histogram |
| Duplicate leak rate | < 0.1% of feed items | Sampled audit |
| Clustering purity | > 95% on a labelled sample | Weekly offline eval |
| Availability | 99.9% | Uptime probe |

Metrics exposed at `/metrics`: articles fetched per source, duplicates by kind,
stories created and merged, per-stage latency histograms, pipeline lag, active story
count, fetch errors per source, feed pages served (fresh vs. replayed).

Beyond infrastructure metrics, the quality signals worth alerting on are the ones
that catch silent regressions: singleton-cluster rate, mean sources per story,
duplicate rate within delivered feeds, and per-source body-length distribution.

---

## 18. Running it on your laptop

### Prerequisites

Python **3.9 or newer** (the code is written to run on the macOS system Python; 3.11+
is faster). Nothing else — no database server, no Redis, no Kafka. All optional
infrastructure has a working in-process fallback.

### Setup

```bash
cd news-aggregator          # the project root, where main.py lives

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

There is also a `Makefile`: `make install`, `make demo`, `make serve`,
`make test`, `make lint`, `make clean`.

On Windows, activate with `.venv\Scripts\activate` instead.

### 1. Create the schema

```bash
python main.py init-db
```

Creates `newsagg.db` (SQLite) in the project directory.

### 2. Run the offline demo — start here

```bash
python main.py demo
```

This seeds a corpus of 22 articles covering 5 real-world events, including planted
exact duplicates, a verbatim syndicated copy, and a reworded wire story, then runs
the complete pipeline and prints the ranked feed. It needs no network and takes a
few seconds. It is the fastest way to see every layer of de-duplication and
clustering working at once.

Expected output:

```
articles stored     : 20
duplicates detected : syndicated=2
stories formed      : 5
stories merged      : 1

1. [0.5671] Chipmaker launches AI accelerator, targets data centre demand
   independent=6  syndicated=0  articles=6  authority=4.58  recency=x0.941
   sources: cnbc.com, dailybuzzfeednews.example, nytimes.com, reuters.com, ...
...
```

### 3. Start the API

```bash
python main.py serve
```

Then open:

- <http://127.0.0.1:8000/> — minimal live feed UI
- <http://127.0.0.1:8000/docs> — interactive OpenAPI documentation
- <http://127.0.0.1:8000/stats> — pipeline counters

Try the guarantees directly:

```bash
# Ranked feed; page 2 via the returned cursor
curl -s "http://127.0.0.1:8000/v1/feed?user_id=alice&limit=3" | python -m json.tool

# Poll again - already-delivered stories never reappear
curl -s "http://127.0.0.1:8000/v1/feed?user_id=alice&limit=3" | python -m json.tool

# Push an article; retry with the same Idempotency-Key and nothing is double-created
curl -s -X POST http://127.0.0.1:8000/v1/ingest \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"articles":[{"source":"ft.com","reliability":0.9,
       "url":"https://www.ft.com/content/fed-holds-rates",
       "title":"Federal Reserve leaves interest rates unchanged as inflation eases",
       "body":"The Federal Reserve held its benchmark interest rate steady on Wednesday, with policymakers pointing to cooling inflation."}]}' \
  | python -m json.tool
```

### 4. Ingest real news

```bash
python main.py add-source bbc-world  https://feeds.bbci.co.uk/news/world/rss.xml --reliability 0.92
python main.py add-source guardian   https://www.theguardian.com/world/rss       --reliability 0.88
python main.py add-source npr-world  https://feeds.npr.org/1004/rss.xml          --reliability 0.85

python main.py poll-once     # fetch once and exit
# or just run `serve` - it polls continuously in the background
```

With those three feeds the system clusters genuinely independent coverage, for
example grouping BBC, Guardian and NPR reports of the same Greenland story under
three completely different headlines.

### Tests

```bash
pip install pytest pytest-asyncio
python -m pytest tests/ -q          # 122 tests, ~1s, no network
```

The suite asserts the product guarantees rather than implementation details:
the dedup cascade collapses each kind of duplicate, independent coverage forms
one story, syndication is discounted in authority, the merge pass repairs an
over-split, ingestion is idempotent, and a user never receives the same story
twice.

Several tests deliberately lock in the calibration claims from section 6 - for
example that a rewritten-headline wire copy stays *inside* the SimHash threshold
while independent reporting on the same event stays *outside* it, and that the
worst same-event similarity beats the best different-event similarity by more
than 2x. These are the numbers that rot silently: "improving" the fingerprint by
upweighting headlines degrades de-duplication quality without raising a single
error anywhere in the system. Encoding them as tests is what converts a tuning
decision into an engineering one.

### Configuration

Every setting is overridable by environment variable with the `NEWSAGG_` prefix, or
via a `.env` file:

```bash
NEWSAGG_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/newsagg
NEWSAGG_REDIS_URL=redis://localhost:6379/0
NEWSAGG_EVENT_BUS=kafka
NEWSAGG_KAFKA_BOOTSTRAP=localhost:9092
NEWSAGG_ASSIGN_THRESHOLD=0.30
NEWSAGG_HALF_LIFE_H=8.0
NEWSAGG_LOG_JSON=true
```

For Postgres, also `pip install asyncpg`; for Redis, `pip install redis`; for
Prometheus metrics, `pip install prometheus-client`. Each is detected at startup and
the system logs which backend it selected.

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` | venv not activated | `source .venv/bin/activate` |
| `demo` shows 0 stories | Stale database from an older schema | `rm newsagg.db*` then `init-db` |
| Feed returns `[]` | All stories already delivered to that `user_id` | Use a new `user_id`, or `DELETE /v1/users/{id}/deliveries` |
| `/metrics` says not installed | Optional dependency | `pip install prometheus-client` |
| RSS sources fetch nothing | Corporate proxy / TLS interception | Check `python main.py poll-once` logs |

---

## 19. Production deployment

The reference implementation is a single process for readability. The production
topology splits it along the boundaries already marked in the module docstring:

```yaml
services:                                                   # module -> service
  fetch-scheduler:  { replicas: 1,  cmd: poll-scheduler }   # ingestion.fetcher
  fetcher:          { replicas: 12, cmd: fetch-worker }     # ingestion.fetcher
  normaliser:       { replicas: 6,  cmd: normalise-worker } # ingestion.normaliser
  story-engine:     { replicas: 16, cmd: story-worker }     # clustering.engine
  ranker:           { replicas: 2,  cmd: rank-worker }      # ranking.ranker
  feed-api:         { replicas: 40, cmd: serve }            # api.app
  maintenance:      { cron: "*/2 * * * *", cmd: merge-pass } # pipeline.jobs

infrastructure:
  postgres:  primary + 3 read replicas, partitioned (Section 9)
  redis:     6-node cluster
  kafka:     3 brokers, RF=3, 16 partitions on normalised.articles
  s3:        article bodies older than 48 h
```

Notes that matter in practice: **story-engine replicas must equal Kafka partitions**
on `normalised.articles`, or the single-writer-per-story property is lost. Scaling
partitions requires draining first, since re-partitioning changes which consumer owns
which blocking key. Deploys of the story engine should be rolling with a drain, not
recreate, so in-flight clustering state is not abandoned mid-batch.

---

## 20. What I would build next

Ordered by value per unit of effort:

1. **Sentence-transformer embeddings behind the existing `Signature` interface,**
   with pgvector + HNSW for candidate retrieval. This directly fixes the one
   demonstrated weakness — synonymy — and would let the assign threshold rise, which
   reduces reliance on the merge pass.
2. **A proper NER model** (spaCy or a small transformer) replacing the capitalisation
   heuristic, plus a Wikidata alias gazetteer so "Fed" and "Federal Reserve" resolve
   to the same entity at extraction time rather than being repaired downstream.
3. **A labelled evaluation harness** in CI: a frozen set of article pairs and expected
   clusterings, so a threshold change that improves one case and breaks three is
   caught before it ships. This is what turns threshold tuning from guesswork into
   engineering.
4. **Learned ranking** — replace the hand-weighted formula with a model trained on
   engagement, keeping the current formula as the cold-start prior and a guardrail.
5. **Story summarisation** — an LLM-generated neutral summary synthesised across the
   cluster's independent sources, which is genuinely more useful than any single
   outlet's lede.
6. **Editorial tooling** — split, merge and suppress operations with an audit trail.
   Any system making automated editorial judgements at this scale needs a human
   override path.
