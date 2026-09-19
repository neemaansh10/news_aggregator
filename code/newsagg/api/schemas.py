"""Request models. FastAPI derives the OpenAPI spec from these, so the published
contract cannot drift from what the code actually validates."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

class ArticleIn(BaseModel):
    source: str = Field(..., description="Source name or domain (auto-registered)")
    url: str
    title: str
    body: Optional[str] = None
    summary: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None
    reliability: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Used only when auto-registering the source"
    )


class IngestIn(BaseModel):
    articles: List[ArticleIn] = Field(..., min_length=1, max_length=500)


class SourceIn(BaseModel):
    name: str
    feed_url: Optional[str] = None
    kind: str = "rss"
    reliability: float = Field(0.6, ge=0.0, le=1.0)
    language: str = "en"
    poll_interval_s: int = Field(180, ge=30, le=86_400)
