"""Query helpers shared by the API and the pipeline."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..text.normalise import domain_of
from .models import Source

async def get_or_create_source(
    session: AsyncSession, name: str, reliability: Optional[float] = None
) -> Source:
    src = (
        await session.execute(select(Source).where(Source.name == name))
    ).scalar_one_or_none()
    if src is not None:
        return src
    src = Source(
        name=name,
        domain=domain_of(name) or name.lower(),
        reliability=reliability if reliability is not None else 0.6,
        kind="push",
        next_poll_at=None,
        enabled=False,  # push sources are never polled
    )
    session.add(src)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        src = (
            await session.execute(select(Source).where(Source.name == name))
        ).scalar_one()
    return src
