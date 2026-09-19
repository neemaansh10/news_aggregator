"""SQLAlchemy models. The schema *is* the specification.

Invariants live here rather than in application code:

* ``articles.url_hash`` UNIQUE        -> the same URL can never be stored twice
* ``articles.dup_of_id``              -> duplicates are kept, not deleted
                                         (needed for audit + source counting)
* ``feed_deliveries`` PK(user, story) -> a story reaches a user at most once
* ``idempotency_keys.key`` UNIQUE     -> writes are safely retryable
* ``feed_pages`` PK(user, cursor)     -> a feed cursor always replays the same page
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

from ..text.normalise import utcnow

class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), unique=True)
    domain: Mapped[str] = mapped_column(String(160), index=True)
    feed_url: Mapped[Optional[str]] = mapped_column(String(1024))
    kind: Mapped[str] = mapped_column(String(24), default="rss")  # rss|api|push
    # 0..1 editorial trust score; drives the authority component of ranking.
    reliability: Mapped[float] = mapped_column(Float, default=0.6)
    language: Mapped[str] = mapped_column(String(8), default="en")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    poll_interval_s: Mapped[int] = mapped_column(Integer, default=180)
    # Conditional-GET state: turns most polls into a 4-byte 304 response.
    etag: Mapped[Optional[str]] = mapped_column(String(256))
    last_modified: Mapped[Optional[str]] = mapped_column(String(128))
    last_polled_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    next_poll_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    # Circuit breaker state
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    breaker_open_until: Mapped[Optional[datetime]] = mapped_column(DateTime)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Story(Base):
    """The user-facing unit: one real-world event, many articles."""

    __tablename__ = "stories"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    title: Mapped[str] = mapped_column(String(600))
    summary: Mapped[Optional[str]] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String(8), default="en", index=True)
    blocking_key: Mapped[str] = mapped_column(String(64), index=True)

    representative_article_id: Mapped[Optional[str]] = mapped_column(String(40))
    centroid: Mapped[Dict[str, float]] = mapped_column(JSON, default=dict)
    # entity -> how many member articles mentioned it. Frequency lets us prune
    # to the entities that actually characterise the event.
    entity_counts: Mapped[Dict[str, int]] = mapped_column(JSON, default=dict)
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    topics: Mapped[List[str]] = mapped_column(JSON, default=list)

    article_count: Mapped[int] = mapped_column(Integer, default=0)
    independent_source_count: Mapped[int] = mapped_column(Integer, default=0)
    syndicated_source_count: Mapped[int] = mapped_column(Integer, default=0)
    authority: Mapped[float] = mapped_column(Float, default=0.0)
    velocity: Mapped[float] = mapped_column(Float, default=0.0)
    score: Mapped[float] = mapped_column(Float, default=0.0, index=True)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    last_activity_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    # Event time of the earliest member article - the honest "when did this
    # happen", as opposed to "when did our crawler notice".
    event_time: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    status: Mapped[str] = mapped_column(String(16), default="active", index=True)
    merged_into_id: Mapped[Optional[str]] = mapped_column(String(40), index=True)
    # Bumped on every material change; lets clients cache and lets the delivery
    # ledger reason about "has this story changed since the user saw it".
    version: Mapped[int] = mapped_column(Integer, default=1)

    __table_args__ = (
        Index("ix_stories_rank", "status", "score"),
        Index("ix_stories_window", "status", "blocking_key", "last_activity_at"),
    )


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)
    external_id: Mapped[Optional[str]] = mapped_column(String(256))

    url: Mapped[str] = mapped_column(String(2048))
    canonical_url: Mapped[str] = mapped_column(String(2048))
    url_hash: Mapped[str] = mapped_column(String(40), unique=True, index=True)

    title: Mapped[str] = mapped_column(String(600))
    summary: Mapped[Optional[str]] = mapped_column(Text)
    body: Mapped[Optional[str]] = mapped_column(Text)
    author: Mapped[Optional[str]] = mapped_column(String(256))
    language: Mapped[str] = mapped_column(String(8), default="en")
    image_url: Mapped[Optional[str]] = mapped_column(String(2048))
    topics: Mapped[List[str]] = mapped_column(JSON, default=list)

    content_hash: Mapped[str] = mapped_column(String(40), index=True)
    simhash: Mapped[str] = mapped_column(String(16), index=True)
    minhash: Mapped[Optional[str]] = mapped_column(String(600))

    published_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    revision: Mapped[int] = mapped_column(Integer, default=1)

    story_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("stories.id"), index=True
    )
    # NULL = original reporting. Otherwise points at the article it duplicates.
    dup_of_id: Mapped[Optional[str]] = mapped_column(String(40), index=True)
    # exact_url | exact_content | near_dup | syndicated
    dup_kind: Mapped[Optional[str]] = mapped_column(String(24), index=True)
    similarity: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        Index("ix_articles_story_dup", "story_id", "dup_kind"),
        Index("ix_articles_recent", "published_at", "content_hash"),
    )

    @property
    def suppressed(self) -> bool:
        """Hidden from the story's article list (it adds no information)."""
        return self.dup_kind in ("exact_url", "exact_content", "near_dup")


class SimhashBand(Base):
    """LSH index: one row per (band position, band value, article)."""

    __tablename__ = "simhash_bands"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    band_idx: Mapped[int] = mapped_column(Integer)
    band_val: Mapped[str] = mapped_column(String(8))
    article_id: Mapped[str] = mapped_column(String(40), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)

    __table_args__ = (Index("ix_band_lookup", "band_idx", "band_val", "created_at"),)


class StoryTerm(Base):
    """Inverted index over story centroids - the clustering blocking layer."""

    __tablename__ = "story_terms"

    story_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    term: Mapped[int] = mapped_column(Integer, primary_key=True)
    weight: Mapped[float] = mapped_column(Float)

    __table_args__ = (Index("ix_story_terms_term", "term"),)


class TermStat(Base):
    __tablename__ = "term_stats"
    term: Mapped[int] = mapped_column(Integer, primary_key=True)
    df: Mapped[int] = mapped_column(Integer, default=0)


class CorpusStat(Base):
    __tablename__ = "corpus_stats"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[int] = mapped_column(Integer, default=0)


class FeedDelivery(Base):
    """
    The ledger that makes 'never show the same story twice' a hard guarantee
    rather than a best effort.  Composite PK means the write is idempotent.
    """

    __tablename__ = "feed_deliveries"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    story_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    story_version: Mapped[int] = mapped_column(Integer, default=1)
    delivered_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class FeedPage(Base):
    """Materialised feed page, keyed by cursor -> replaying a cursor is exact."""

    __tablename__ = "feed_pages"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    cursor: Mapped[str] = mapped_column(String(128), primary_key=True)
    story_ids: Mapped[List[str]] = mapped_column(JSON, default=list)
    next_cursor: Mapped[Optional[str]] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    response_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class Outbox(Base):
    """
    Transactional outbox.  A state change and its event are committed in the
    same DB transaction; a relay publishes to Kafka afterwards.  This is what
    makes the pipeline at-least-once without dual-write data loss.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(64), index=True)
    partition_key: Mapped[str] = mapped_column(String(128))
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)


class UserAffinity(Base):
    """Per-user topic weights used by the personalised relevance term."""

    __tablename__ = "user_affinities"
    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    topic: Mapped[str] = mapped_column(String(32), primary_key=True)
    weight: Mapped[float] = mapped_column(Float, default=0.0)
