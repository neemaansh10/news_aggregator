"""Polls sources on their own cadence.

Three things here are load-bearing in production and usually missing from
first-draft implementations: conditional GET (most polls become 304s), a
per-source circuit breaker (one dead feed must not consume the worker pool or
spam its origin), and adaptive backoff (quiet feeds are polled less often).
"""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import or_, select, update

from ..config import SETTINGS
from ..db.models import Source
from ..db.session import db_session
from ..infra.event_bus import TOPIC_RAW, Event, EventBus
from ..infra.resilience import RATE_LIMITER, retry_async
from ..observability import M_FETCH_ERRORS, M_FETCHED, log
from ..text.normalise import strip_html, to_naive_utc, utcnow
from .schemas import RawArticle

try:  # pragma: no cover - optional dependency
    import feedparser

    HAVE_FEEDPARSER = True
except Exception:  # pragma: no cover
    feedparser = None  # type: ignore
    HAVE_FEEDPARSER = False

class Fetcher:
    """
    Polls enabled sources on their own cadence.

    Three things here are load-bearing in production and usually missing from
    toy implementations:
      1. Conditional GET (ETag / If-Modified-Since) - most polls become 304s,
         cutting bandwidth and upstream load by an order of magnitude.
      2. A per-source circuit breaker - one dead feed must not consume the
         worker pool or spam its origin.
      3. Adaptive backoff - quiet feeds are polled less often, which frees
         capacity for fast-moving ones.
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self._client: Optional[httpx.AsyncClient] = None
        self._sem = asyncio.Semaphore(SETTINGS.fetch_concurrency)

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=SETTINGS.http_timeout_s,
            follow_redirects=True,
            headers={"User-Agent": SETTINGS.user_agent},
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
        )

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    async def run_scheduler(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("scheduler tick failed")
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=SETTINGS.scheduler_tick_s
                )

    async def tick(self) -> None:
        """Claim every source whose next_poll_at is due and fetch in parallel."""
        now = utcnow()
        async with db_session() as session:
            rows = (
                await session.execute(
                    select(Source)
                    .where(
                        Source.enabled.is_(True),
                        or_(Source.next_poll_at.is_(None), Source.next_poll_at <= now),
                        or_(
                            Source.breaker_open_until.is_(None),
                            Source.breaker_open_until <= now,
                        ),
                    )
                    .limit(200)
                )
            ).scalars().all()
        if not rows:
            return
        await asyncio.gather(*(self._poll_source(s) for s in rows))

    async def _poll_source(self, source: Source) -> None:
        async with self._sem:
            await RATE_LIMITER.acquire(source.domain)
            try:
                items, etag, last_mod = await retry_async(
                    lambda: self._fetch(source),
                    attempts=3,
                    retry_on=(httpx.HTTPError, asyncio.TimeoutError),
                )
            except Exception as exc:
                await self._record_failure(source, exc)
                return
            await self._record_success(source, etag, last_mod, len(items))
            for item in items:
                await self.bus.publish(
                    Event(TOPIC_RAW, str(source.id), item.to_payload())
                )
            M_FETCHED.labels(source.name).inc(len(items))

    async def _fetch(
        self, source: Source
    ) -> Tuple[List[RawArticle], Optional[str], Optional[str]]:
        assert self._client is not None
        if not source.feed_url:
            return [], None, None

        headers: Dict[str, str] = {}
        if source.etag:
            headers["If-None-Match"] = source.etag
        if source.last_modified:
            headers["If-Modified-Since"] = source.last_modified

        resp = await self._client.get(source.feed_url, headers=headers)
        if resp.status_code == 304:
            return [], source.etag, source.last_modified
        resp.raise_for_status()

        etag = resp.headers.get("ETag")
        last_mod = resp.headers.get("Last-Modified")

        if source.kind == "api":
            items = self._parse_json_api(source, resp.text)
        else:
            items = await asyncio.to_thread(self._parse_feed, source, resp.text)
        return items, etag, last_mod

    @staticmethod
    def _parse_feed(source: Source, body: str) -> List[RawArticle]:
        if not HAVE_FEEDPARSER:
            log.warning("feedparser not installed; cannot parse RSS for %s", source.name)
            return []
        parsed = feedparser.parse(body)  # type: ignore
        out: List[RawArticle] = []
        for entry in parsed.entries[:200]:
            link = entry.get("link") or ""
            title = strip_html(entry.get("title") or "")
            if not link or not title:
                continue

            content = ""
            if entry.get("content"):
                content = " ".join(c.get("value", "") for c in entry["content"])
            content = strip_html(content) or strip_html(entry.get("summary") or "")

            published: Optional[datetime] = None
            for key in ("published_parsed", "updated_parsed"):
                if entry.get(key):
                    published = datetime(*entry[key][:6])
                    break

            image = None
            for media in entry.get("media_content", []) or []:
                if media.get("url"):
                    image = media["url"]
                    break

            out.append(
                RawArticle(
                    source_id=source.id,
                    url=link,
                    title=title,
                    body=content[: SETTINGS.max_body_chars],
                    summary=strip_html(entry.get("summary") or "")[:1000],
                    author=entry.get("author"),
                    published_at=published,
                    external_id=entry.get("id") or entry.get("guid"),
                    image_url=image,
                )
            )
        return out

    @staticmethod
    def _parse_json_api(source: Source, body: str) -> List[RawArticle]:
        """Generic JSON adapter. Real deployments register one class per API."""
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return []
        items = data.get("articles") or data.get("results") or data.get("items") or []
        out: List[RawArticle] = []
        for it in items[:200]:
            url = it.get("url") or it.get("link")
            title = it.get("title") or it.get("headline")
            if not url or not title:
                continue
            pub = it.get("publishedAt") or it.get("published_at") or it.get("date")
            published = None
            if pub:
                with suppress(Exception):
                    published = to_naive_utc(
                        datetime.fromisoformat(str(pub).replace("Z", "+00:00"))
                    )
            out.append(
                RawArticle(
                    source_id=source.id,
                    url=url,
                    title=strip_html(title),
                    body=strip_html(it.get("content") or it.get("body") or "")[
                        : SETTINGS.max_body_chars
                    ],
                    summary=strip_html(it.get("description") or "")[:1000],
                    author=it.get("author"),
                    published_at=published,
                    external_id=str(it.get("id") or ""),
                    image_url=it.get("urlToImage") or it.get("image"),
                )
            )
        return out

    async def _record_success(
        self, source: Source, etag: Optional[str], last_mod: Optional[str], n: int
    ) -> None:
        # Adaptive cadence: empty polls slow down, productive polls speed up.
        interval = source.poll_interval_s
        if n == 0:
            interval = min(SETTINGS.max_poll_interval_s, int(interval * 1.5))
        else:
            interval = max(SETTINGS.default_poll_interval_s // 3, int(interval * 0.8))
        async with db_session() as session:
            await session.execute(
                update(Source)
                .where(Source.id == source.id)
                .values(
                    etag=etag,
                    last_modified=last_mod,
                    last_polled_at=utcnow(),
                    next_poll_at=utcnow() + timedelta(seconds=interval),
                    poll_interval_s=interval,
                    consecutive_failures=0,
                    breaker_open_until=None,
                )
            )
            await session.commit()

    async def _record_failure(self, source: Source, exc: BaseException) -> None:
        M_FETCH_ERRORS.labels(source.name).inc()
        failures = source.consecutive_failures + 1
        values: Dict[str, Any] = {
            "consecutive_failures": failures,
            "last_polled_at": utcnow(),
            "next_poll_at": utcnow() + timedelta(seconds=source.poll_interval_s),
        }
        if failures >= SETTINGS.breaker_fail_threshold:
            # Exponential trip window, capped - stop hammering a dead origin.
            cooldown = SETTINGS.breaker_cooldown_s * min(
                8, 2 ** (failures - SETTINGS.breaker_fail_threshold)
            )
            values["breaker_open_until"] = utcnow() + timedelta(seconds=cooldown)
            log.warning("circuit breaker OPEN for %s (%ss)", source.name, cooldown)
        log.warning("fetch failed for %s: %s", source.name, exc)
        async with db_session() as session:
            await session.execute(
                update(Source).where(Source.id == source.id).values(**values)
            )
            await session.commit()
