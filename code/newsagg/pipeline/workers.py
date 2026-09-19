"""Wires the stages together: raw -> normalised -> story."""

from __future__ import annotations

import asyncio
import time
from typing import List

from ..config import SETTINGS
from ..db.session import db_session
from ..infra.event_bus import (
    TOPIC_NORMALISED,
    TOPIC_RAW,
    Event,
    EventBus,
    InMemoryEventBus,
)
from ..ingestion.normaliser import normalise
from ..ingestion.schemas import NormalisedArticle, RawArticle
from ..observability import log, timed
from ..clustering.engine import StoryEngine

class Pipeline:
    """Wires the stages together: raw -> normalised -> story."""

    def __init__(self, bus: EventBus, engine: StoryEngine) -> None:
        self.bus = bus
        self.engine = engine
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        for i in range(SETTINGS.normalise_workers):
            q = self.bus.subscribe(TOPIC_RAW)
            self._tasks.append(
                asyncio.create_task(self._normalise_worker(q, i), name=f"norm-{i}")
            )
        for i in range(SETTINGS.story_workers):
            q = self.bus.subscribe(TOPIC_NORMALISED)
            self._tasks.append(
                asyncio.create_task(self._story_worker(q, i), name=f"story-{i}")
            )
        log.info(
            "pipeline started: %d normalisers, %d story workers",
            SETTINGS.normalise_workers,
            SETTINGS.story_workers,
        )

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _normalise_worker(self, q: "asyncio.Queue[Event]", idx: int) -> None:
        while not self._stop.is_set():
            ev = await q.get()
            try:
                async with timed("normalise"):
                    raw = RawArticle.from_payload(ev.payload)
                    norm = normalise(raw)
                if norm is None:
                    continue
                await self.bus.publish(
                    Event(TOPIC_NORMALISED, norm.blocking_key, norm.to_payload())
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Poison-message isolation: one bad article must never stall a
                # partition.  Production routes this to a DLQ topic.
                log.exception("normalise worker %d failed", idx)
            finally:
                q.task_done()

    async def _story_worker(self, q: "asyncio.Queue[Event]", idx: int) -> None:
        while not self._stop.is_set():
            ev = await q.get()
            try:
                async with timed("story_engine"):
                    art = NormalisedArticle.from_payload(ev.payload)
                    async with db_session() as session:
                        await self.engine.ingest(session, art)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("story worker %d failed", idx)
            finally:
                q.task_done()

    async def drain(self, timeout: float = 30.0) -> None:
        """Test/demo helper: wait until the in-memory queues are empty."""
        if not isinstance(self.bus, InMemoryEventBus):
            await asyncio.sleep(timeout)
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            queues = [q for qs in self.bus._queues.values() for q in qs]
            if all(q.empty() for q in queues):
                await asyncio.sleep(0.35)
                queues = [q for qs in self.bus._queues.values() for q in qs]
                if all(q.empty() for q in queues):
                    return
            await asyncio.sleep(0.1)
