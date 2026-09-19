"""Process lifecycle: builds the object graph, starts workers, tears down.

``STATE`` is rebound by :func:`startup`, so consumers must read it through this
module (``runtime.STATE``) rather than importing the name by value.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, List, Optional

from fastapi import FastAPI

from .clustering.engine import StoryEngine
from .config import SETTINGS
from .db.session import db_session, dispose_engine, init_db
from .infra.cache import CACHE
from .infra.event_bus import BUS, EventBus
from .ingestion.fetcher import Fetcher
from .observability import configure_logging, log
from .pipeline.jobs import job_flush_df, job_merge_pass, job_rank_refresh, job_retention
from .pipeline.workers import Pipeline
from .text.normalise import utcnow
from .text.vectorise import VECTORISER

@dataclass
class AppState:
    bus: EventBus
    engine: StoryEngine
    fetcher: Fetcher
    pipeline: Pipeline
    stop: asyncio.Event
    jobs: List[asyncio.Task]
    started_at: datetime


STATE: Optional[AppState] = None


async def startup(run_fetcher: bool = True) -> AppState:
    global STATE
    await init_db()
    await CACHE.connect()
    await BUS.start()

    async with db_session() as session:
        await VECTORISER.load(session)

    engine = StoryEngine(BUS)
    fetcher = Fetcher(BUS)
    await fetcher.start()
    pipeline = Pipeline(BUS, engine)
    await pipeline.start()

    stop = asyncio.Event()
    jobs = [
        asyncio.create_task(job_rank_refresh(stop), name="rank-refresh"),
        asyncio.create_task(job_merge_pass(stop, engine), name="merge-pass"),
        asyncio.create_task(job_flush_df(stop), name="df-flush"),
        asyncio.create_task(job_retention(stop), name="retention"),
    ]
    if run_fetcher:
        jobs.append(
            asyncio.create_task(fetcher.run_scheduler(stop), name="fetch-scheduler")
        )

    STATE = AppState(BUS, engine, fetcher, pipeline, stop, jobs, utcnow())
    return STATE


async def shutdown() -> None:
    global STATE
    if STATE is None:
        return
    STATE.stop.set()
    await STATE.pipeline.stop()
    for job in STATE.jobs:
        job.cancel()
    await asyncio.gather(*STATE.jobs, return_exceptions=True)
    async with db_session() as session:
        await VECTORISER.flush(session)
    await STATE.fetcher.stop()
    await STATE.bus.stop()
    await CACHE.close()
    await dispose_engine()
    STATE = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await startup(run_fetcher=True)
    log.info("API ready on http://%s:%d", SETTINGS.api_host, SETTINGS.api_port)
    try:
        yield
    finally:
        await shutdown()
