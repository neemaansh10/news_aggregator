"""The ranked, de-duplicated feed - where the product guarantees are enforced."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Query
from sqlalchemy import and_, func, or_, select, update

from ...config import SETTINGS
from ...db.models import Article, FeedDelivery, FeedPage, Story
from ...db.session import db_session, insert_ignore
from ...infra.cache import CACHE
from ...observability import M_FEED_SERVED
from ...ranking.ranker import RANKER
from ...ranking.scoring import compute_score
from ...text.normalise import utcnow
from ..cursors import decode_cursor, encode_cursor
from ..serializers import story_to_dict

router = APIRouter(tags=["feed"])

@router.get("/v1/feed", tags=["feed"])
async def get_feed(
    user_id: str = Query("anonymous", max_length=64),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    topic: Optional[str] = Query(None),
    language: Optional[str] = Query(None),
    min_sources: int = Query(1, ge=1, le=50),
    personalise: bool = Query(True),
    resurface_updates: bool = Query(
        False,
        description=(
            "Re-surface an already-delivered story when substantially more "
            "independent sources have since confirmed it. Flagged as an update, "
            "never presented as new."
        ),
    ),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
) -> Dict[str, Any]:
    """
    Ranked, de-duplicated story feed.

    Three guarantees, all enforced by storage rather than by convention:

    1. **No duplicate stories, ever.** `feed_deliveries` is a ledger keyed on
       (user_id, story_id).  Every page excludes everything already in it, and
       story merges propagate delivery records forward - so even when two
       clusters turn out to be one event, the user sees it once.

    2. **A cursor always replays the same page.** `feed_pages` stores the exact
       story ids served for a cursor.  A client retrying on a flaky network gets
       byte-identical content instead of silently skipping stories - which is the
       usual way naive keyset pagination quietly loses items from a feed that is
       being re-ranked underneath it.

    3. **The head of the feed stays live.** A request with no cursor always
       computes a fresh page, so newly broken stories appear on the next poll.
       Retry safety for that first page is opt-in via `Idempotency-Key`, because
       caching the head unconditionally would freeze the feed for the whole TTL -
       the exact opposite of what a near-real-time product needs.

    `resurface_updates` is the deliberate, explicit exception to (1): a story
    that has gained substantially more independent confirmation since the user
    saw it can return, tagged `is_update` with a `delivered_source_count`, so
    the client can render it as "5 more outlets are now reporting this" rather
    than as a new item.  Off by default.
    """
    # A cursor is a replayable page token. The head is only replayable when the
    # caller explicitly asks for it with an idempotency key.
    cursor_key: Optional[str] = cursor or (
        f"head:{idempotency_key}" if idempotency_key else None
    )

    async with db_session() as session:
        # ---- idempotent replay -------------------------------------------
        page = (
            await session.get(FeedPage, (user_id, cursor_key))
            if cursor_key is not None
            else None
        )
        if page is not None:
            stories = (
                await session.execute(
                    select(Story).where(Story.id.in_(page.story_ids or []))
                )
            ).scalars().all()
            order = {sid: i for i, sid in enumerate(page.story_ids or [])}
            stories.sort(key=lambda s: order.get(s.id, 1_000))
            M_FEED_SERVED.labels("replay").inc()
            return {
                "stories": [await story_to_dict(session, s) for s in stories],
                "next_cursor": page.next_cursor,
                "replayed": True,
            }

        # ---- fresh page ---------------------------------------------------
        window_start = utcnow() - timedelta(hours=SETTINGS.cluster_window_h * 2)
        delivered = select(FeedDelivery.story_id).where(FeedDelivery.user_id == user_id)

        stmt = (
            select(Story)
            .where(
                Story.status == "active",
                Story.last_activity_at >= window_start,
                Story.independent_source_count >= min_sources,
                Story.id.notin_(delivered),
            )
            .order_by(Story.score.desc(), Story.id.desc())
            .limit(limit * 4)  # over-fetch: personalisation reorders the window
        )
        if language:
            stmt = stmt.where(Story.language == language)

        decoded = decode_cursor(cursor) if cursor else None
        if decoded:
            score_at, id_at = decoded
            stmt = stmt.where(
                or_(
                    Story.score < score_at,
                    and_(Story.score == score_at, Story.id < id_at),
                )
            )

        candidates = list((await session.execute(stmt)).scalars().all())
        seen_counts: Dict[str, int] = {}

        if resurface_updates:
            # A story the user already saw returns only if the amount of
            # independent confirmation has grown by at least 2 sources AND 50%.
            # Both conditions matter: the absolute floor stops trivial churn,
            # the relative one stops huge stories from resurfacing forever.
            grown = (
                await session.execute(
                    select(Story, FeedDelivery.story_version)
                    .join(FeedDelivery, FeedDelivery.story_id == Story.id)
                    .where(
                        FeedDelivery.user_id == user_id,
                        Story.status == "active",
                        Story.last_activity_at >= window_start,
                    )
                    .limit(200)
                )
            ).all()
            for story, _delivered_version in grown:
                prior = await session.scalar(
                    select(func.count(func.distinct(Article.source_id))).where(
                        Article.story_id == story.id,
                        Article.dup_kind.is_(None),
                        Article.fetched_at
                        <= (
                            await session.scalar(
                                select(FeedDelivery.delivered_at).where(
                                    FeedDelivery.user_id == user_id,
                                    FeedDelivery.story_id == story.id,
                                )
                            )
                            or utcnow()
                        ),
                    )
                )
                prior = int(prior or 0)
                now_count = story.independent_source_count
                if now_count - prior >= 2 and now_count >= prior * 1.5:
                    seen_counts[story.id] = prior
                    candidates.append(story)

        if topic:
            candidates = [s for s in candidates if topic in (s.topics or [])]

        # ---- personalised re-rank ----------------------------------------
        if personalise and user_id != "anonymous":
            affinity = await RANKER.affinity(session, user_id)
            if affinity:
                for s in candidates:
                    s.score = compute_score(
                        authority=s.authority,
                        independent_sources=s.independent_source_count,
                        velocity=s.velocity,
                        event_time=s.event_time,
                        last_activity=s.last_activity_at,
                        relevance=RANKER.relevance(s.topics or [], affinity),
                    )

        # Always re-sort: resurfaced stories were appended out of rank order.
        candidates.sort(key=lambda s: (-s.score, s.id))
        selected = candidates[:limit]

        next_cursor = (
            encode_cursor(selected[-1].score, selected[-1].id)
            if len(selected) == limit
            else None
        )

        if selected:
            # Ledger write and page materialisation share ONE transaction: a
            # crash between them would either lose the page or mark stories the
            # user never received as delivered.
            fresh = [s for s in selected if s.id not in seen_counts]
            if fresh:
                await session.execute(
                    insert_ignore(
                        FeedDelivery,
                        [
                            {
                                "user_id": user_id,
                                "story_id": s.id,
                                "story_version": s.version,
                                "delivered_at": utcnow(),
                            }
                            for s in fresh
                        ],
                    )
                )
            for s in selected:
                if s.id in seen_counts:
                    # Re-delivery: advance the watermark so the same growth
                    # cannot resurface the story a second time.
                    await session.execute(
                        update(FeedDelivery)
                        .where(
                            FeedDelivery.user_id == user_id,
                            FeedDelivery.story_id == s.id,
                        )
                        .values(story_version=s.version, delivered_at=utcnow())
                    )
            if cursor_key is not None:
                await session.execute(
                    insert_ignore(
                        FeedPage,
                        [
                            {
                                "user_id": user_id,
                                "cursor": cursor_key,
                                "story_ids": [s.id for s in selected],
                                "next_cursor": next_cursor,
                                "created_at": utcnow(),
                            }
                        ],
                    )
                )
            await session.commit()

        payload = []
        for s in selected:
            item = await story_to_dict(session, s)
            if s.id in seen_counts:
                item["is_update"] = True
                item["previously_seen_source_count"] = seen_counts[s.id]
            payload.append(item)

        M_FEED_SERVED.labels("fresh").inc()
        return {"stories": payload, "next_cursor": next_cursor, "replayed": False}


@router.get("/v1/stories", tags=["feed"])
async def list_stories(
    limit: int = Query(20, ge=1, le=100),
    min_sources: int = Query(1, ge=1, le=50),
    topic: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Stateless ranked listing - no delivery ledger, safe to poll and cache."""
    cache_key = f"feed:top:{limit}:{min_sources}:{topic or '*'}"
    cached = await CACHE.get(cache_key)
    if cached is not None:
        return cached

    async with db_session() as session:
        window_start = utcnow() - timedelta(hours=SETTINGS.cluster_window_h * 2)
        rows = (
            await session.execute(
                select(Story)
                .where(
                    Story.status == "active",
                    Story.last_activity_at >= window_start,
                    Story.independent_source_count >= min_sources,
                )
                .order_by(Story.score.desc())
                .limit(limit * 3)
            )
        ).scalars().all()
        if topic:
            rows = [s for s in rows if topic in (s.topics or [])]
        payload = {"stories": [await story_to_dict(session, s) for s in rows[:limit]]}

    await CACHE.set(cache_key, payload, SETTINGS.feed_cache_ttl_s)
    return payload


@router.get("/v1/stories/{story_id}", tags=["feed"])
async def get_story(story_id: str) -> Dict[str, Any]:
    async with db_session() as session:
        story = await session.get(Story, story_id)
        if story is None:
            raise HTTPException(404, "story not found")
        # Follow the merge chain so old ids keep resolving after a merge.
        hops = 0
        while story.merged_into_id and hops < 10:
            nxt = await session.get(Story, story.merged_into_id)
            if nxt is None:
                break
            story, hops = nxt, hops + 1

        cache_key = f"story:{story.id}:v{story.version}"
        cached = await CACHE.get(cache_key)
        if cached is not None:
            return cached
        payload = await story_to_dict(session, story, include_articles=True)
        await CACHE.set(cache_key, payload, SETTINGS.story_cache_ttl_s)
        return payload
