"""RawArticle -> NormalisedArticle. A pure function, trivially unit-testable."""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from ..config import SETTINGS
from ..infra.event_bus import blocking_key
from ..text.fingerprint import hex64, minhash, simhash64
from ..text.normalise import (
    canonical_url,
    classify_topics,
    content_hash,
    detect_language,
    extract_entities,
    strip_html,
    to_naive_utc,
    tokenize,
    url_hash,
    utcnow,
)
from .schemas import NormalisedArticle, RawArticle

def normalise(raw: RawArticle) -> Optional[NormalisedArticle]:
    """Pure function: RawArticle -> NormalisedArticle. Trivially unit-testable."""
    title = strip_html(raw.title).strip()
    if not title or len(title) < 8:
        return None

    canon = canonical_url(raw.url)
    if not canon:
        return None

    body = strip_html(raw.body or raw.summary or "")[: SETTINGS.max_body_chars]
    tokens = tokenize(f"{title} {body}")
    language = detect_language(tokenize(f"{title} {body}", keep_stopwords=True))
    topics = classify_topics(tokens)
    entities = extract_entities(title, body)

    published = to_naive_utc(raw.published_at) or utcnow()
    # Guard against sources publishing far-future dates to pin themselves to the
    # top of date-sorted feeds - a real and common abuse.
    if published > utcnow() + timedelta(hours=2):
        published = utcnow()

    return NormalisedArticle(
        source_id=raw.source_id,
        url=raw.url,
        canonical_url=canon,
        url_hash=url_hash(canon),
        title=title[:600],
        summary=(raw.summary or body[:400])[:1000],
        body=body,
        author=raw.author,
        language=language,
        topics=topics,
        entities=sorted(entities),
        image_url=raw.image_url,
        external_id=raw.external_id,
        published_at=published,
        content_hash=content_hash(title, body),
        simhash=hex64(simhash64(title, body)),
        minhash=minhash(title, body),
        blocking_key=blocking_key(language, topics, published),
    )
