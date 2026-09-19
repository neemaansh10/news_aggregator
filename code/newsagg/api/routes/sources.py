"""Source registry administration."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from ...db.models import Source
from ...db.session import db_session
from ...text.normalise import domain_of, iso_z, utcnow
from ..schemas import SourceIn

router = APIRouter(tags=["sources"])

@router.get("/v1/sources", tags=["sources"])
async def list_sources() -> Dict[str, Any]:
    async with db_session() as session:
        rows = (
            await session.execute(select(Source).order_by(Source.reliability.desc()))
        ).scalars().all()
        return {
            "sources": [
                {
                    "id": s.id,
                    "name": s.name,
                    "domain": s.domain,
                    "kind": s.kind,
                    "reliability": s.reliability,
                    "enabled": s.enabled,
                    "feed_url": s.feed_url,
                    "poll_interval_s": s.poll_interval_s,
                    "consecutive_failures": s.consecutive_failures,
                    "breaker_open_until": iso_z(s.breaker_open_until),
                    "last_polled_at": iso_z(s.last_polled_at),
                }
                for s in rows
            ]
        }


@router.post("/v1/sources", tags=["sources"], status_code=201)
async def create_source(payload: SourceIn) -> Dict[str, Any]:
    async with db_session() as session:
        existing = (
            await session.execute(select(Source).where(Source.name == payload.name))
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(409, f"source '{payload.name}' already exists")
        src = Source(
            name=payload.name,
            domain=domain_of(payload.feed_url or payload.name) or payload.name.lower(),
            feed_url=payload.feed_url,
            kind=payload.kind,
            reliability=payload.reliability,
            language=payload.language,
            poll_interval_s=payload.poll_interval_s,
            enabled=bool(payload.feed_url),
            next_poll_at=utcnow(),
        )
        session.add(src)
        await session.commit()
        return {"id": src.id, "name": src.name, "enabled": src.enabled}
