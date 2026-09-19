"""Test fixtures.

Each test gets a fresh, isolated SQLite database in a temp directory, so tests
never share state and can run in any order. The settings object is mutated
before any engine is created, which is why `_isolated_database` is autouse and
session-scoped ordering matters.
"""

import asyncio
import os
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from newsagg.config import SETTINGS


@pytest.fixture(autouse=True)
def _reset_global_state():
    """Reset process-wide singletons between tests.

    The vectoriser and ranker hold caches that would otherwise leak document
    frequencies and affinities across tests and make results order-dependent.
    """
    from newsagg.ranking.ranker import RANKER
    from newsagg.text.vectorise import VECTORISER

    VECTORISER._df = {}
    VECTORISER._doc_count = 0
    VECTORISER._dirty = set()
    RANKER._affinity_cache = {}
    yield


@pytest_asyncio.fixture
async def temp_db(tmp_path, monkeypatch):
    """Point the engine at a throwaway database and tear it down afterwards."""
    import newsagg.db.session as session_mod

    db_path = tmp_path / "test.db"
    monkeypatch.setattr(SETTINGS, "database_url", f"sqlite+aiosqlite:///{db_path}")
    # Force a fresh engine bound to the temp database.
    session_mod._engine = None
    session_mod._Session = None

    await session_mod.init_db()
    yield db_path
    await session_mod.dispose_engine()


@pytest_asyncio.fixture
async def engine(temp_db):
    """A StoryEngine wired to an in-memory bus - no Kafka, no network."""
    from newsagg.clustering.engine import StoryEngine
    from newsagg.infra.event_bus import InMemoryEventBus

    return StoryEngine(InMemoryEventBus())


@pytest_asyncio.fixture
async def client(temp_db):
    """HTTP client bound to the ASGI app, bypassing the network stack.

    Routes are exercised through the real app object, so routing, validation and
    serialisation are all covered - only the socket is skipped.
    """
    from newsagg.api.app import app
    from newsagg.clustering.engine import StoryEngine
    from newsagg.infra.event_bus import InMemoryEventBus
    from newsagg.ingestion.fetcher import Fetcher
    from newsagg.pipeline.workers import Pipeline
    from newsagg import runtime
    from newsagg.text.normalise import utcnow

    bus = InMemoryEventBus()
    story_engine = StoryEngine(bus)
    runtime.STATE = runtime.AppState(
        bus=bus,
        engine=story_engine,
        fetcher=Fetcher(bus),
        pipeline=Pipeline(bus, story_engine),
        stop=asyncio.Event(),
        jobs=[],
        started_at=utcnow(),
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    runtime.STATE = None
