"""Periodic re-scoring and per-user relevance.

Without a periodic sweep, recency decay would only apply when a story happened
to receive a new article, and quiet stories would never age out of the feed.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import SETTINGS
from ..db.models import Story, UserAffinity
from ..infra.cache import CACHE
from ..observability import M_ACTIVE_STORIES
from ..text.normalise import utcnow
from .scoring import compute_score

class Ranker:
    """Periodically re-scores active stories so recency decay actually applies."""

    def __init__(self) -> None:
        self._affinity_cache: Dict[str, Dict[str, float]] = {}

    async def refresh_all(self, session: AsyncSession) -> int:
        window_start = utcnow() - timedelta(hours=SETTINGS.cluster_window_h * 2)
        stories = (
            await session.execute(
                select(Story).where(
                    Story.status == "active", Story.last_activity_at >= window_start
                )
            )
        ).scalars().all()
        for story in stories:
            story.score = compute_score(
                authority=story.authority,
                independent_sources=story.independent_source_count,
                velocity=story.velocity,
                event_time=story.event_time,
                last_activity=story.last_activity_at,
            )
        if stories:
            await session.commit()
            await CACHE.delete_prefix("feed:")
        M_ACTIVE_STORIES.set(len(stories))
        return len(stories)

    async def affinity(self, session: AsyncSession, user_id: str) -> Dict[str, float]:
        if user_id in self._affinity_cache:
            return self._affinity_cache[user_id]
        rows = (
            await session.execute(
                select(UserAffinity.topic, UserAffinity.weight).where(
                    UserAffinity.user_id == user_id
                )
            )
        ).all()
        weights = {t: float(w) for t, w in rows}
        self._affinity_cache[user_id] = weights
        return weights

    @staticmethod
    def relevance(topics: Sequence[str], affinity: Dict[str, float]) -> float:
        if not affinity or not topics:
            return 0.0
        return max(0.0, min(1.0, max(affinity.get(t, 0.0) for t in topics)))


RANKER = Ranker()
