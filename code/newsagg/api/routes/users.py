"""Per-user personalisation and delivery-ledger administration."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body
from sqlalchemy import delete

from ...db.models import FeedDelivery, FeedPage, UserAffinity
from ...db.session import db_session, insert_ignore
from ...ranking.ranker import RANKER

router = APIRouter(tags=["users"])

@router.post("/v1/users/{user_id}/affinity", tags=["feed"])
async def set_affinity(user_id: str, weights: Dict[str, float] = Body(...)) -> Dict[str, Any]:
    async with db_session() as session:
        await session.execute(
            delete(UserAffinity).where(UserAffinity.user_id == user_id)
        )
        rows = [
            {"user_id": user_id, "topic": t, "weight": max(0.0, min(1.0, float(w)))}
            for t, w in weights.items()
        ]
        if rows:
            await session.execute(insert_ignore(UserAffinity, rows))
        await session.commit()
    RANKER._affinity_cache.pop(user_id, None)
    return {"user_id": user_id, "topics": len(weights)}


@router.delete("/v1/users/{user_id}/deliveries", tags=["feed"])
async def reset_deliveries(user_id: str) -> Dict[str, Any]:
    """Dev helper: forget what a user has been shown."""
    async with db_session() as session:
        await session.execute(
            delete(FeedDelivery).where(FeedDelivery.user_id == user_id)
        )
        await session.execute(delete(FeedPage).where(FeedPage.user_id == user_id))
        await session.commit()
    return {"user_id": user_id, "reset": True}
