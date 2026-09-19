"""Background maintenance: re-ranking, merge reconciliation, retention."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta

from sqlalchemy import delete, update

from ..config import SETTINGS
from ..clustering.engine import StoryEngine
from ..db.models import FeedPage, IdempotencyKey, SimhashBand, Story
from ..db.session import db_session
from ..observability import log
from ..ranking.ranker import RANKER
from ..text.normalise import utcnow
from ..text.vectorise import VECTORISER

async def job_rank_refresh(stop: asyncio.Event) -> None:
    while not stop.is_set():
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=SETTINGS.rank_refresh_s)
        if stop.is_set():
            return
        try:
            async with db_session() as session:
                await RANKER.refresh_all(session)
        except Exception:
            log.exception("rank refresh failed")


async def job_merge_pass(stop: asyncio.Event, engine: StoryEngine) -> None:
    while not stop.is_set():
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=SETTINGS.merge_pass_s)
        if stop.is_set():
            return
        try:
            async with db_session() as session:
                await engine.merge_pass(session)
        except Exception:
            log.exception("merge pass failed")


async def job_flush_df(stop: asyncio.Event) -> None:
    while not stop.is_set():
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=SETTINGS.df_flush_s)
        if stop.is_set():
            return
        try:
            async with db_session() as session:
                await VECTORISER.flush(session)
        except Exception:
            log.exception("df flush failed")


async def job_retention(stop: asyncio.Event) -> None:
    """Archive cold stories and prune index tables that only serve hot lookups."""
    while not stop.is_set():
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=3600)
        if stop.is_set():
            return
        try:
            cutoff = utcnow() - timedelta(days=SETTINGS.story_retention_days)
            band_cutoff = utcnow() - timedelta(hours=SETTINGS.dedup_window_h)
            page_cutoff = utcnow() - timedelta(seconds=SETTINGS.feed_page_ttl_s)
            idem_cutoff = utcnow() - timedelta(seconds=SETTINGS.idempotency_ttl_s)
            async with db_session() as session:
                await session.execute(
                    update(Story)
                    .where(Story.status == "active", Story.last_activity_at < cutoff)
                    .values(status="archived")
                )
                await session.execute(
                    delete(SimhashBand).where(SimhashBand.created_at < band_cutoff)
                )
                await session.execute(
                    delete(FeedPage).where(FeedPage.created_at < page_cutoff)
                )
                await session.execute(
                    delete(IdempotencyKey).where(
                        IdempotencyKey.created_at < idem_cutoff
                    )
                )
                await session.commit()
        except Exception:
            log.exception("retention job failed")
