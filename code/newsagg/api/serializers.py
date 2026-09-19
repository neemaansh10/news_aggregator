"""Story -> JSON. Exposes the full signal breakdown so ranking is explainable
rather than a black box."""

from __future__ import annotations

from typing import Any, Dict, Set

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import Article, Source, Story
from ..ranking.scoring import recency_multiplier
from ..text.normalise import iso_z

async def story_to_dict(
    session: AsyncSession, story: Story, include_articles: bool = False
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": story.id,
        "title": story.title,
        "summary": story.summary,
        "topics": story.topics or [],
        "language": story.language,
        "score": round(story.score, 6),
        "signals": {
            "independent_sources": story.independent_source_count,
            "syndicated_sources": story.syndicated_source_count,
            "articles": story.article_count,
            "authority": round(story.authority, 4),
            "velocity": story.velocity,
            "recency_multiplier": round(
                recency_multiplier(story.event_time, story.last_activity_at), 4
            ),
        },
        "event_time": iso_z(story.event_time),
        "first_seen_at": iso_z(story.first_seen_at),
        "last_activity_at": iso_z(story.last_activity_at),
        "version": story.version,
    }

    rows = (
        await session.execute(
            select(Article, Source.name, Source.reliability)
            .join(Source, Source.id == Article.source_id)
            .where(Article.story_id == story.id)
            .order_by(Article.published_at.asc())
        )
    ).all()

    sources = []
    articles = []
    seen_sources: Set[str] = set()
    for art, source_name, reliability in rows:
        if art.dup_kind in ("exact_url", "exact_content", "near_dup"):
            continue
        if source_name not in seen_sources:
            seen_sources.add(source_name)
            sources.append(
                {
                    "name": source_name,
                    "reliability": round(reliability, 3),
                    "independent": art.dup_kind is None,
                }
            )
        if include_articles:
            articles.append(
                {
                    "id": art.id,
                    "title": art.title,
                    "url": art.canonical_url,
                    "source": source_name,
                    "author": art.author,
                    "published_at": iso_z(art.published_at),
                    "relation": art.dup_kind or "original",
                    "similarity": art.similarity,
                }
            )

    payload["sources"] = sources
    payload["source_count"] = len(sources)
    if include_articles:
        payload["articles"] = articles
    if story.representative_article_id:
        rep = await session.get(Article, story.representative_article_id)
        if rep:
            payload["url"] = rep.canonical_url
            payload["image_url"] = rep.image_url
    return payload
