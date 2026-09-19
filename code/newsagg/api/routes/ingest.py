"""Push ingestion for publishers and internal crawlers.

Idempotency works at two levels: ``Idempotency-Key`` replays the stored response
for a retried request, and even without a key the pipeline is naturally
idempotent (``url_hash`` is unique, identical content is a no-op), so an
at-least-once guarantee upstream is sufficient.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import suppress
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from sqlalchemy.exc import IntegrityError

from ...db.models import IdempotencyKey
from ...db.repositories import get_or_create_source
from ...db.session import db_session, insert_ignore
from ...ingestion.normaliser import normalise
from ...ingestion.schemas import RawArticle
from ...runtime import AppState
from ...text.normalise import to_naive_utc, utcnow
from ..deps import require_state
from ..schemas import IngestIn

router = APIRouter(tags=["ingest"])

@router.post("/v1/ingest", tags=["ingest"], status_code=202)
async def ingest(
    payload: IngestIn = Body(...),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    state: AppState = Depends(require_state),
) -> Dict[str, Any]:
    """
    Push ingestion endpoint for publishers / internal crawlers.

    Idempotency works at two levels:
      * `Idempotency-Key` replays the exact stored response for a retried
        request, and rejects the same key carrying a *different* body (409).
      * Even without a key, the pipeline is naturally idempotent: url_hash is
        unique and identical content is a no-op, so an at-least-once delivery
        guarantee upstream is sufficient.
    """
    body_fingerprint = hashlib.sha256(
        json.dumps(payload.model_dump(mode="json"), sort_keys=True, default=str).encode()
    ).hexdigest()[:64]

    async with db_session() as session:
        if idempotency_key:
            prior = await session.get(IdempotencyKey, idempotency_key)
            if prior is not None:
                if prior.request_fingerprint != body_fingerprint:
                    raise HTTPException(
                        409, "Idempotency-Key reused with a different request body"
                    )
                return prior.response_json

        results: List[Dict[str, Any]] = []
        for item in payload.articles:
            source = await get_or_create_source(session, item.source, item.reliability)
            raw = RawArticle(
                source_id=source.id,
                url=item.url,
                title=item.title,
                body=item.body or "",
                summary=item.summary or "",
                author=item.author,
                published_at=to_naive_utc(item.published_at),
                external_id=item.external_id,
                image_url=item.image_url,
            )
            norm = normalise(raw)
            if norm is None:
                results.append({"url": item.url, "status": "rejected",
                                "reason": "unparseable"})
                continue
            try:
                outcome = await state.engine.ingest(session, norm)
            except IntegrityError:
                results.append({"url": item.url, "status": "duplicate",
                                "dup_kind": "race"})
                continue
            results.append(dict(url=item.url, **outcome.to_dict()))

        response = {
            "accepted": len(results),
            "created": sum(1 for r in results if r.get("status") == "created"),
            "duplicates": sum(1 for r in results if r.get("status") == "duplicate"),
            "updated": sum(1 for r in results if r.get("status") == "updated"),
            "results": results,
        }

        if idempotency_key:
            with suppress(IntegrityError):
                await session.execute(
                    insert_ignore(
                        IdempotencyKey,
                        [
                            {
                                "key": idempotency_key,
                                "request_fingerprint": body_fingerprint,
                                "response_json": response,
                                "created_at": utcnow(),
                            }
                        ],
                    )
                )
                await session.commit()
        return response
