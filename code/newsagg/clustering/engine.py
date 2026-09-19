"""De-duplication cascade and online story clustering - the heart of the system.

Four layers run as a cascade, each cheaper and more precise than the next::

    L1a EXACT      canonical URL hash            one indexed lookup
    L1b EXACT      content hash                  one indexed lookup
    L2a NEAR       SimHash + LSH bands           indexed lookup + popcounts
    L3  SEMANTIC   composite similarity vs       blocking lookup + <=60 dots
                   story centroids
    L2b NEAR       MinHash vs that story's       bounded, precise
                   members

L2b deliberately runs *after* L3: once an article is routed to a story, the
comparison set is bounded, so it can afford to be exact. That is what catches
the wire copy whose headline was rewritten far enough to slip past the global
SimHash threshold.
"""

from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import SETTINGS
from ..db.models import Article, FeedDelivery, SimhashBand, Source, Story, StoryTerm
from ..db.session import insert_ignore, new_id
from ..infra.cache import CACHE
from ..infra.event_bus import TOPIC_STORY_UPDATES, Event, EventBus
from ..ingestion.schemas import NormalisedArticle
from ..observability import (
    M_DUPES,
    M_INGESTED,
    M_PIPELINE_LAG,
    M_STORIES_MERGED,
    M_STORIES_NEW,
    log,
)
from ..ranking.scoring import compute_score
from ..text.fingerprint import (
    hamming,
    minhash_jaccard,
    simhash_bands,
    unhex64,
)
from ..text.normalise import utcnow
from ..text.vectorise import (
    VECTORISER,
    Signature,
    SparseVec,
    l2_normalise,
    merge_centroid,
    similarity,
    top_k,
)

@dataclass
class DedupVerdict:
    kind: Optional[str]  # None | exact_url | exact_content | near_dup | syndicated
    duplicate_of: Optional[str] = None
    story_id: Optional[str] = None
    similarity: Optional[float] = None


@dataclass
class IngestResult:
    status: str  # created | duplicate | updated | rejected
    article_id: Optional[str] = None
    story_id: Optional[str] = None
    dup_kind: Optional[str] = None
    similarity: Optional[float] = None
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


class StoryEngine:
    """
    Owns dedup + clustering + story bookkeeping.

    Concurrency model: all mutations for a given `blocking_key` are serialised
    by an in-process lock.  In the distributed deployment that same guarantee
    comes from Kafka partitioning (blocking_key is the partition key), so at
    most one consumer ever writes a given story.  The optional cross-partition
    drift is repaired asynchronously by `merge_pass()`.
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._join_counts: Dict[str, int] = defaultdict(int)

    # -- entry point -------------------------------------------------------
    async def ingest(
        self, session: AsyncSession, art: NormalisedArticle
    ) -> IngestResult:
        async with self._locks[art.blocking_key]:
            return await self._ingest_locked(session, art)

    async def _ingest_locked(
        self, session: AsyncSession, art: NormalisedArticle
    ) -> IngestResult:
        # ---------- L1a: exact URL ----------------------------------------
        existing = (
            await session.execute(
                select(Article).where(Article.url_hash == art.url_hash)
            )
        ).scalar_one_or_none()

        if existing is not None:
            if existing.content_hash == art.content_hash:
                # Pure replay. Fully idempotent: nothing changes.
                M_DUPES.labels("exact_url").inc()
                return IngestResult(
                    status="duplicate",
                    article_id=existing.id,
                    story_id=existing.story_id,
                    dup_kind="exact_url",
                )
            # Same URL, new content => the publisher updated the article.
            return await self._apply_revision(session, existing, art)

        sig = Signature(
            vec=VECTORISER.encode(art.title, art.body), entities=set(art.entities)
        )
        if sig.empty:
            return IngestResult(status="rejected", reason="no_extractable_terms")

        verdict = await self._dedup(session, art)

        article = Article(
            id=new_id("a_"),
            source_id=art.source_id,
            external_id=art.external_id,
            url=art.url,
            canonical_url=art.canonical_url,
            url_hash=art.url_hash,
            title=art.title,
            summary=art.summary,
            body=art.body,
            author=art.author,
            language=art.language,
            image_url=art.image_url,
            topics=art.topics,
            content_hash=art.content_hash,
            simhash=art.simhash,
            minhash=art.minhash,
            published_at=art.published_at,
            fetched_at=utcnow(),
            updated_at=utcnow(),
            dup_of_id=verdict.duplicate_of,
            dup_kind=verdict.kind,
            similarity=verdict.similarity,
        )

        if verdict.story_id:
            # A duplicate inherits its twin's story; no re-clustering needed.
            article.story_id = verdict.story_id
            session.add(article)
            await self._index_simhash(session, article)
            await self._refresh_story(session, verdict.story_id, article, sig,
                                      joined=verdict.kind is None)
            await self._commit(session)
            if verdict.kind:
                M_DUPES.labels(verdict.kind).inc()
            return IngestResult(
                status="duplicate" if verdict.kind else "created",
                article_id=article.id,
                story_id=verdict.story_id,
                dup_kind=verdict.kind,
                similarity=verdict.similarity,
            )

        # ---------- L3: semantic clustering -------------------------------
        story_id, sim_score = await self._assign_story(session, art, sig, article)
        article.story_id = story_id
        article.similarity = sim_score

        # ---------- L2b: precise within-cluster duplicate check -----------
        # Now that the article is routed to a story, we can afford an accurate
        # comparison against that story's members.  This is what catches the
        # wire copy whose headline was rewritten far enough to slip past the
        # global SimHash threshold.
        twin = await self._within_story_duplicate(session, story_id, art)
        if twin is not None:
            twin_article, estimated_jaccard = twin
            article.dup_of_id = twin_article.id
            article.dup_kind = (
                "near_dup"
                if twin_article.source_id == art.source_id
                else "syndicated"
            )
            article.similarity = round(estimated_jaccard, 4)
            M_DUPES.labels(article.dup_kind).inc()

        session.add(article)
        await self._index_simhash(session, article)
        # A duplicate contributes no new information, so it must not move the
        # centroid - otherwise a heavily syndicated wire story would drag the
        # cluster towards its own phrasing and stop accepting original reporting.
        await self._refresh_story(
            session, story_id, article, sig, joined=article.dup_kind is None
        )
        if article.dup_kind is None:
            VECTORISER.observe(sig.vec)
        await self._commit(session)

        M_INGESTED.inc()
        lag = (utcnow() - art.published_at).total_seconds()
        if 0 <= lag < 86_400:
            M_PIPELINE_LAG.set(lag)

        return IngestResult(
            status="duplicate" if article.dup_kind else "created",
            article_id=article.id,
            story_id=story_id,
            dup_kind=article.dup_kind,
            similarity=article.similarity,
        )

    async def _within_story_duplicate(
        self, session: AsyncSession, story_id: str, art: NormalisedArticle
    ) -> Optional[Tuple[Article, float]]:
        """MinHash comparison against the members of one story. Bounded cost."""
        if not art.minhash:
            return None
        members = (
            await session.execute(
                select(Article)
                .where(Article.story_id == story_id, Article.minhash.isnot(None))
                .order_by(Article.published_at.desc())
                .limit(SETTINGS.dup_check_members)
            )
        ).scalars().all()

        best: Optional[Article] = None
        best_jaccard = 0.0
        for member in members:
            estimate = minhash_jaccard(art.minhash, member.minhash)
            if estimate > best_jaccard:
                best, best_jaccard = member, estimate

        if best is not None and best_jaccard >= SETTINGS.near_dup_jaccard:
            # Always attribute to the *original*, never to another duplicate,
            # so dup_of_id chains stay one level deep.
            root = best
            if best.dup_of_id:
                candidate = await session.get(Article, best.dup_of_id)
                if candidate is not None:
                    root = candidate
            return root, best_jaccard
        return None

    # -- dedup layers ------------------------------------------------------
    async def _dedup(
        self, session: AsyncSession, art: NormalisedArticle
    ) -> DedupVerdict:
        window_start = utcnow() - timedelta(hours=SETTINGS.dedup_window_h)

        # ---------- L1b: exact content (different URL, identical text) ----
        twin = (
            await session.execute(
                select(Article)
                .where(
                    Article.content_hash == art.content_hash,
                    Article.published_at >= window_start,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if twin is not None:
            kind = "exact_content" if twin.source_id == art.source_id else "syndicated"
            return DedupVerdict(kind, twin.id, twin.story_id, 1.0)

        # ---------- L2: near-duplicate via SimHash LSH --------------------
        fingerprint = unhex64(art.simhash)
        bands = simhash_bands(fingerprint, SETTINGS.simhash_bands)
        clauses = [
            and_(SimhashBand.band_idx == i, SimhashBand.band_val == v)
            for i, v in enumerate(bands)
        ]
        candidate_ids = (
            await session.execute(
                select(SimhashBand.article_id)
                .where(or_(*clauses), SimhashBand.created_at >= window_start)
                .limit(400)
            )
        ).scalars().all()

        if candidate_ids:
            candidates = (
                await session.execute(
                    select(Article).where(Article.id.in_(list(set(candidate_ids))))
                )
            ).scalars().all()

            best: Optional[Article] = None
            best_distance = 65
            for cand in candidates:
                dist = hamming(fingerprint, unhex64(cand.simhash))
                if dist < best_distance:
                    best, best_distance = cand, dist

            if best is not None and best_distance <= SETTINGS.simhash_hamming_max:
                similarity = 1.0 - (best_distance / 64.0)
                # Same source  -> a genuine duplicate, suppress it entirely.
                # Other source -> syndication: it still "reports" the story, but
                #                 it is not independent confirmation, so it is
                #                 discounted in the authority signal.
                kind = "near_dup" if best.source_id == art.source_id else "syndicated"
                return DedupVerdict(kind, best.id, best.story_id, similarity)

        return DedupVerdict(kind=None)

    # -- clustering --------------------------------------------------------
    @staticmethod
    def story_signature(story: Story) -> Signature:
        return Signature(
            vec={int(k): float(v) for k, v in (story.centroid or {}).items()},
            entities=set((story.entity_counts or {}).keys()),
        )

    async def _assign_story(
        self,
        session: AsyncSession,
        art: NormalisedArticle,
        sig: Signature,
        article: Article,
    ) -> Tuple[str, float]:
        candidates = await self._candidate_stories(session, art, sig)

        best_id: Optional[str] = None
        best_score = 0.0
        for story in candidates:
            score = similarity(sig, self.story_signature(story))
            # Time proximity bonus: two articles hours apart about the same
            # things are far more likely to be one event than two days apart.
            hours_apart = abs(
                (art.published_at - story.event_time).total_seconds()
            ) / 3600.0
            score *= 1.0 + 0.10 * math.exp(-hours_apart / 12.0)
            if set(art.topics) & set(story.topics or []):
                score *= 1.04
            if score > best_score:
                best_id, best_score = story.id, score

        if best_id and best_score >= SETTINGS.assign_threshold:
            return best_id, round(min(best_score, 1.0), 4)

        story = Story(
            id=new_id("s_"),
            title=art.title,
            summary=art.summary,
            language=art.language,
            blocking_key=art.blocking_key,
            representative_article_id=article.id,
            centroid={str(k): v for k, v in sig.vec.items()},
            entity_counts={},
            member_count=0,
            topics=art.topics,
            first_seen_at=utcnow(),
            last_activity_at=utcnow(),
            event_time=art.published_at,
        )
        session.add(story)
        await session.flush()
        M_STORIES_NEW.inc()
        return story.id, 1.0

    async def _candidate_stories(
        self, session: AsyncSession, art: NormalisedArticle, sig: Signature
    ) -> List[Story]:
        """
        Blocking step.  A linear scan over active stories is O(N) per article and
        dies at scale; instead we look up only the stories that share a
        high-weight term with this article, via the inverted index.
        """
        query_terms = [t for t, _ in sorted(sig.vec.items(), key=lambda kv: -kv[1])][
            : SETTINGS.candidate_terms
        ]
        if not query_terms:
            return []

        window_start = utcnow() - timedelta(hours=SETTINGS.cluster_window_h)
        rows = (
            await session.execute(
                select(StoryTerm.story_id, StoryTerm.term, StoryTerm.weight)
                .join(Story, Story.id == StoryTerm.story_id)
                .where(
                    StoryTerm.term.in_(query_terms),
                    Story.status == "active",
                    Story.language == art.language,
                    Story.last_activity_at >= window_start,
                )
                .limit(5_000)
            )
        ).all()
        if not rows:
            return []

        # Rough overlap score purely to shortlist; exact scoring follows.
        partial: Dict[str, float] = defaultdict(float)
        for story_id, term, weight in rows:
            partial[story_id] += weight * sig.vec.get(int(term), 0.0)

        shortlist = [
            sid
            for sid, _ in sorted(partial.items(), key=lambda kv: -kv[1])[
                : SETTINGS.candidate_limit
            ]
        ]
        if not shortlist:
            return []
        return list(
            (
                await session.execute(select(Story).where(Story.id.in_(shortlist)))
            ).scalars().all()
        )

    # -- story bookkeeping -------------------------------------------------
    async def _refresh_story(
        self,
        session: AsyncSession,
        story_id: str,
        article: Article,
        sig: Signature,
        joined: bool,
    ) -> None:
        story = await session.get(Story, story_id)
        if story is None:
            return

        if joined:
            centroid = {int(k): float(v) for k, v in (story.centroid or {}).items()}
            centroid = merge_centroid(
                centroid, story.member_count, sig.vec, SETTINGS.centroid_top_k
            )
            story.centroid = {str(k): v for k, v in centroid.items()}

            # Entities accumulate with counts, then get pruned by frequency:
            # an entity mentioned by many members characterises the event,
            # a one-off mention is usually incidental.
            counts: Dict[str, int] = dict(story.entity_counts or {})
            for entity in sig.entities:
                counts[entity] = counts.get(entity, 0) + 1
            if len(counts) > SETTINGS.story_entity_cap:
                counts = dict(
                    sorted(counts.items(), key=lambda kv: -kv[1])[
                        : SETTINGS.story_entity_cap
                    ]
                )
            story.entity_counts = counts

            story.member_count += 1
            self._join_counts[story_id] += 1
            # Rewriting 192 index rows per article is wasteful; the centroid
            # barely moves after the first few members.
            if (
                story.member_count <= 3
                or self._join_counts[story_id] % SETTINGS.reindex_every == 0
            ):
                await self._reindex_story_terms(session, story_id, centroid)

        story.last_activity_at = utcnow()
        story.event_time = min(story.event_time, article.published_at)
        story.topics = sorted(set((story.topics or []) + (article.topics or [])))[:3]
        story.version += 1

        await session.flush()
        await self._recount(session, story)
        await self._score(session, story)

        await self.bus.publish(
            Event(
                TOPIC_STORY_UPDATES,
                story_id,
                {"story_id": story_id, "version": story.version},
            )
        )
        await CACHE.delete_prefix(f"story:{story_id}")
        await CACHE.delete_prefix("feed:")

    async def _reindex_story_terms(
        self, session: AsyncSession, story_id: str, centroid: SparseVec
    ) -> None:
        await session.execute(delete(StoryTerm).where(StoryTerm.story_id == story_id))
        rows = [
            {"story_id": story_id, "term": int(t), "weight": float(w)}
            for t, w in sorted(centroid.items(), key=lambda kv: -kv[1])[:64]
        ]
        if rows:
            await session.execute(insert_ignore(StoryTerm, rows))

    async def _recount(self, session: AsyncSession, story: Story) -> None:
        """
        Recompute source counts from the articles table rather than incrementing
        counters.  Counters drift under retries and merges; a recount is always
        correct and is a single indexed aggregate.
        """
        rows = (
            await session.execute(
                select(Article.source_id, Article.dup_kind).where(
                    Article.story_id == story.id
                )
            )
        ).all()

        independent: Set[int] = set()
        syndicated: Set[int] = set()
        visible = 0
        for source_id, dup_kind in rows:
            if dup_kind in ("exact_url", "exact_content", "near_dup"):
                continue  # adds no information at all
            visible += 1
            if dup_kind == "syndicated":
                syndicated.add(source_id)
            else:
                independent.add(source_id)
        syndicated -= independent  # never double-count a source

        reliabilities = dict(
            (
                await session.execute(
                    select(Source.id, Source.reliability).where(
                        Source.id.in_(list(independent | syndicated) or [-1])
                    )
                )
            ).all()
        )

        authority = sum(reliabilities.get(s, 0.5) for s in independent)
        authority += SETTINGS.syndication_discount * sum(
            reliabilities.get(s, 0.5) for s in syndicated
        )

        # Velocity = independent sources that joined in the last hour: this is
        # what distinguishes a story that is *breaking* from one that is merely
        # well covered.
        since = utcnow() - timedelta(hours=SETTINGS.velocity_window_h)
        recent = await session.scalar(
            select(func.count(func.distinct(Article.source_id))).where(
                Article.story_id == story.id,
                Article.fetched_at >= since,
                Article.dup_kind.is_(None),
            )
        )

        story.article_count = visible
        story.independent_source_count = len(independent)
        story.syndicated_source_count = len(syndicated)
        story.authority = round(authority, 4)
        story.velocity = float(recent or 0)

        # Representative article: prefer the most reliable independent source.
        best = (
            await session.execute(
                select(Article.id, Source.reliability)
                .join(Source, Source.id == Article.source_id)
                .where(Article.story_id == story.id, Article.dup_kind.is_(None))
                .order_by(Source.reliability.desc(), Article.published_at.asc())
                .limit(1)
            )
        ).first()
        if best:
            story.representative_article_id = best[0]
            rep = await session.get(Article, best[0])
            if rep:
                story.title = rep.title
                story.summary = rep.summary

    async def _score(self, session: AsyncSession, story: Story) -> None:
        story.score = compute_score(
            authority=story.authority,
            independent_sources=story.independent_source_count,
            velocity=story.velocity,
            event_time=story.event_time,
            last_activity=story.last_activity_at,
        )

    async def _apply_revision(
        self, session: AsyncSession, existing: Article, art: NormalisedArticle
    ) -> IngestResult:
        """
        Publisher updated an article in place (corrections, developing stories).
        Keep the row, bump the revision, re-fingerprint, and let the story
        re-derive its counters.  We deliberately do NOT re-cluster a settled
        article: it churns the feed for what is usually a copy edit.
        """
        existing.title = art.title
        existing.summary = art.summary
        existing.body = art.body
        existing.content_hash = art.content_hash
        existing.simhash = art.simhash
        existing.minhash = art.minhash
        existing.topics = art.topics
        existing.updated_at = utcnow()
        existing.revision += 1
        await session.execute(
            delete(SimhashBand).where(SimhashBand.article_id == existing.id)
        )
        await self._index_simhash(session, existing)
        if existing.story_id:
            story = await session.get(Story, existing.story_id)
            if story:
                story.last_activity_at = utcnow()
                story.version += 1
                await self._recount(session, story)
                await self._score(session, story)
        await self._commit(session)
        await CACHE.delete_prefix("feed:")
        return IngestResult(
            status="updated", article_id=existing.id, story_id=existing.story_id
        )

    async def _index_simhash(self, session: AsyncSession, article: Article) -> None:
        value = unhex64(article.simhash)
        rows = [
            {
                "band_idx": i,
                "band_val": v,
                "article_id": article.id,
                "created_at": article.published_at,
            }
            for i, v in enumerate(simhash_bands(value, SETTINGS.simhash_bands))
        ]
        await session.execute(insert_ignore(SimhashBand, rows))

    @staticmethod
    async def _commit(session: AsyncSession) -> None:
        try:
            await session.commit()
        except IntegrityError:
            # Concurrent insert of the same URL; the unique index did its job.
            await session.rollback()
            raise

    # -- periodic reconciliation ------------------------------------------
    async def merge_pass(self, session: AsyncSession) -> int:
        """
        Repairs over-splitting.  Two stories can diverge when early articles use
        different vocabulary ("blast" vs "explosion") and only converge once more
        coverage arrives.  Comparing centroids after the fact catches this.
        Runs offline, so a slow O(k^2) scan within a blocking key is acceptable.
        """
        window_start = utcnow() - timedelta(hours=SETTINGS.cluster_window_h)
        stories = (
            await session.execute(
                select(Story)
                .where(Story.status == "active", Story.last_activity_at >= window_start)
                .order_by(Story.blocking_key, Story.first_seen_at)
            )
        ).scalars().all()

        # Group by *language and day bucket only*, not the full blocking key:
        # two clusters of one event can easily be classified under different
        # topics ("business" vs "politics"), and those are exactly the splits
        # this pass exists to repair.
        def merge_group(story: Story) -> str:
            parts = story.blocking_key.split(":")
            return f"{parts[0]}:{parts[-1]}" if len(parts) >= 2 else story.blocking_key

        by_key: Dict[str, List[Story]] = defaultdict(list)
        for s in stories:
            by_key[merge_group(s)].append(s)

        merged = 0
        for group in by_key.values():
            # Biggest, best-sourced cluster absorbs the others, so the surviving
            # story keeps the richest centroid and the longest history.
            group.sort(key=lambda s: (-s.independent_source_count, s.first_seen_at))
            absorbed: Set[str] = set()
            for i, primary in enumerate(group):
                if primary.id in absorbed:
                    continue
                for other in group[i + 1 :]:
                    if other.id in absorbed:
                        continue
                    score = similarity(
                        self.story_signature(primary), self.story_signature(other)
                    )
                    if score >= SETTINGS.merge_threshold:
                        await self._merge(session, primary, other)
                        absorbed.add(other.id)
                        merged += 1
        if merged:
            await session.commit()
            await CACHE.delete_prefix("feed:")
            M_STORIES_MERGED.inc(merged)
            log.info("merge pass absorbed %d stories", merged)
        return merged

    async def _merge(
        self, session: AsyncSession, primary: Story, secondary: Story
    ) -> None:
        await session.execute(
            update(Article)
            .where(Article.story_id == secondary.id)
            .values(story_id=primary.id)
        )
        await session.execute(
            delete(StoryTerm).where(StoryTerm.story_id == secondary.id)
        )

        # Anyone who already saw the secondary story has effectively seen the
        # primary one - propagate the delivery record so the merge can never
        # resurface the same event in a user's feed.
        deliveries = (
            await session.execute(
                select(FeedDelivery.user_id).where(
                    FeedDelivery.story_id == secondary.id
                )
            )
        ).scalars().all()
        if deliveries:
            await session.execute(
                insert_ignore(
                    FeedDelivery,
                    [
                        {
                            "user_id": u,
                            "story_id": primary.id,
                            "story_version": primary.version,
                            "delivered_at": utcnow(),
                        }
                        for u in deliveries
                    ],
                )
            )

        c1 = {int(k): float(v) for k, v in (primary.centroid or {}).items()}
        c2 = {int(k): float(v) for k, v in (secondary.centroid or {}).items()}
        n1, n2 = max(primary.member_count, 1), max(secondary.member_count, 1)
        acc: SparseVec = {t: w * n1 for t, w in c1.items()}
        for t, w in c2.items():
            acc[t] = acc.get(t, 0.0) + w * n2
        combined = l2_normalise(
            top_k({t: w / (n1 + n2) for t, w in acc.items()}, SETTINGS.centroid_top_k)
        )

        combined_entities: Dict[str, int] = dict(primary.entity_counts or {})
        for entity, count in (secondary.entity_counts or {}).items():
            combined_entities[entity] = combined_entities.get(entity, 0) + count
        if len(combined_entities) > SETTINGS.story_entity_cap:
            combined_entities = dict(
                sorted(combined_entities.items(), key=lambda kv: -kv[1])[
                    : SETTINGS.story_entity_cap
                ]
            )

        primary.centroid = {str(k): v for k, v in combined.items()}
        primary.entity_counts = combined_entities
        primary.member_count = n1 + n2
        primary.first_seen_at = min(primary.first_seen_at, secondary.first_seen_at)
        primary.event_time = min(primary.event_time, secondary.event_time)
        primary.last_activity_at = max(
            primary.last_activity_at, secondary.last_activity_at
        )
        primary.topics = sorted(set((primary.topics or []) + (secondary.topics or [])))[:5]
        primary.version += 1

        secondary.status = "merged"
        secondary.merged_into_id = primary.id
        secondary.score = -1.0

        await session.flush()
        await self._reindex_story_terms(session, primary.id, combined)
        await self._recount(session, primary)
        await self._score(session, primary)
