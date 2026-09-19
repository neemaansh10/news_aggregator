"""Engine, session factory, schema creation and dialect-aware helpers."""

from __future__ import annotations

import hashlib
import random
import time
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncIterator, Dict, List, Optional

from sqlalchemy import event, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import SETTINGS
from ..observability import log
from .models import Base, CorpusStat, TermStat

# -- Engine / session management ----------------------------------------------

_engine: Optional[AsyncEngine] = None
_Session: Optional[async_sessionmaker] = None


def get_engine() -> AsyncEngine:
    global _engine, _Session
    if _engine is None:
        url = SETTINGS.database_url
        kwargs: Dict[str, Any] = {"echo": False, "future": True}
        if not url.startswith("sqlite"):
            kwargs.update(pool_size=20, max_overflow=10, pool_pre_ping=True,
                          pool_recycle=1800)
        _engine = create_async_engine(url, **kwargs)

        if url.startswith("sqlite"):
            # WAL + relaxed sync: SQLite is the dev substitute for Postgres and
            # needs concurrent-reader support to behave like one.
            @event.listens_for(_engine.sync_engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _rec):  # pragma: no cover
                cur = dbapi_conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
                cur.execute("PRAGMA busy_timeout=8000")
                cur.execute("PRAGMA foreign_keys=ON")
                cur.close()

        _Session = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def session_factory() -> async_sessionmaker:
    get_engine()
    assert _Session is not None
    return _Session


@asynccontextmanager
async def db_session() -> AsyncIterator[AsyncSession]:
    async with session_factory()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("schema ready at %s", SETTINGS.database_url)


def _dialect() -> str:
    return "postgresql" if "postgres" in SETTINGS.database_url else "sqlite"


def insert_ignore(model: Any, rows: List[Dict[str, Any]]) -> Any:
    """Dialect-aware INSERT ... ON CONFLICT DO NOTHING."""
    if _dialect() == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert(model).values(rows).on_conflict_do_nothing()
    from sqlalchemy.dialects.sqlite import insert as sq_insert

    return sq_insert(model).values(rows).on_conflict_do_nothing()


async def upsert_term_stat(session: AsyncSession, term: int, df: int) -> None:
    res = await session.execute(
        update(TermStat).where(TermStat.term == term).values(df=df)
    )
    if res.rowcount == 0:
        with suppress(IntegrityError):
            await session.execute(insert_ignore(TermStat, [{"term": term, "df": df}]))


async def upsert_corpus_stat(session: AsyncSession, key: str, value: int) -> None:
    res = await session.execute(
        update(CorpusStat).where(CorpusStat.key == key).values(value=value)
    )
    if res.rowcount == 0:
        with suppress(IntegrityError):
            await session.execute(
                insert_ignore(CorpusStat, [{"key": key, "value": value}])
            )


def new_id(prefix: str = "") -> str:
    raw = hashlib.blake2b(
        f"{time.time_ns()}:{random.getrandbits(64)}".encode(), digest_size=12
    ).hexdigest()
    return f"{prefix}{raw}"


async def dispose_engine() -> None:
    """Release the connection pool. Call on shutdown and at the end of CLI runs."""
    global _engine, _Session
    if _engine is not None:
        await _engine.dispose()
        _engine, _Session = None, None
