"""Liveness, readiness, metrics and pipeline counters.

The liveness/readiness split matters under Kubernetes: a pod that cannot reach
the database should leave the load balancer (``/readyz`` fails) but must **not**
be restarted (``/healthz`` passes) - restarting fixes nothing, and a restart
loop turns a database blip into a full outage.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import func, select, text as sql_text

from ... import runtime
from ...config import SETTINGS
from ...db.models import Article, Source, Story
from ...db.session import db_session
from ...observability import CONTENT_TYPE_LATEST, HAVE_PROM, generate_latest
from ...ranking.ranker import RANKER
from ...runtime import AppState
from ...text.normalise import iso_z, utcnow
from ...text.vectorise import VECTORISER
from ..deps import require_state

router = APIRouter(tags=["ops"])

@router.get("/healthz", tags=["ops"])
async def healthz() -> Dict[str, Any]:
    """Liveness: is the process able to serve at all."""
    return {"status": "ok", "ts": iso_z(utcnow())}


@router.get("/readyz", tags=["ops"])
async def readyz() -> JSONResponse:
    """Readiness: can we actually reach our dependencies."""
    checks: Dict[str, str] = {}
    healthy = True
    try:
        async with db_session() as session:
            await session.execute(sql_text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {exc}"
        healthy = False
    checks["pipeline"] = "ok" if runtime.STATE is not None else "starting"
    if runtime.STATE is None:
        healthy = False
    return JSONResponse(
        {"status": "ready" if healthy else "degraded", "checks": checks},
        status_code=200 if healthy else 503,
    )


@router.get("/metrics", tags=["ops"])
async def metrics() -> PlainTextResponse:
    if not HAVE_PROM:
        return PlainTextResponse("prometheus_client not installed\n")
    return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)


@router.get("/stats", tags=["ops"])
async def stats() -> Dict[str, Any]:
    async with db_session() as session:
        active_window = utcnow() - timedelta(hours=SETTINGS.cluster_window_h)
        return {
            "sources": await session.scalar(select(func.count()).select_from(Source)),
            "articles": await session.scalar(select(func.count()).select_from(Article)),
            "articles_suppressed": await session.scalar(
                select(func.count())
                .select_from(Article)
                .where(Article.dup_kind.isnot(None))
            ),
            "stories_total": await session.scalar(
                select(func.count()).select_from(Story)
            ),
            "stories_active": await session.scalar(
                select(func.count())
                .select_from(Story)
                .where(Story.status == "active", Story.last_activity_at >= active_window)
            ),
            "stories_merged": await session.scalar(
                select(func.count()).select_from(Story).where(Story.status == "merged")
            ),
            "corpus_docs": VECTORISER._doc_count,
            "uptime_s": int((utcnow() - runtime.STATE.started_at).total_seconds())
            if runtime.STATE
            else 0,
        }


@router.post("/v1/admin/merge-pass", tags=["ops"])
async def admin_merge_pass(state: AppState = Depends(require_state)) -> Dict[str, Any]:
    async with db_session() as session:
        merged = await state.engine.merge_pass(session)
        rescored = await RANKER.refresh_all(session)
    return {"merged": merged, "rescored": rescored}
