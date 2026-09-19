"""Wire formats for the pipeline stages.

``RawArticle`` is whatever a source gave us; ``NormalisedArticle`` is what the
normaliser produces. Both serialise to plain dicts so they can travel over Kafka
without any ORM or framework coupling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..text.normalise import iso_z, to_naive_utc

@dataclass
class RawArticle:
    """Whatever a source gave us, before any of our own processing."""

    source_id: int
    url: str
    title: str
    body: str = ""
    summary: str = ""
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    external_id: Optional[str] = None
    image_url: Optional[str] = None

    def to_payload(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        d["published_at"] = iso_z(self.published_at)
        return d

    @staticmethod
    def from_payload(d: Dict[str, Any]) -> "RawArticle":
        pub = d.get("published_at")
        return RawArticle(
            source_id=int(d["source_id"]),
            url=d["url"],
            title=d.get("title") or "",
            body=d.get("body") or "",
            summary=d.get("summary") or "",
            author=d.get("author"),
            published_at=(
                to_naive_utc(datetime.fromisoformat(pub.replace("Z", "+00:00")))
                if pub
                else None
            ),
            external_id=d.get("external_id"),
            image_url=d.get("image_url"),
        )


@dataclass
class NormalisedArticle:
    source_id: int
    url: str
    canonical_url: str
    url_hash: str
    title: str
    summary: str
    body: str
    author: Optional[str]
    language: str
    topics: List[str]
    entities: List[str]
    image_url: Optional[str]
    external_id: Optional[str]
    published_at: datetime
    content_hash: str
    simhash: str
    minhash: str
    blocking_key: str

    def to_payload(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        d["published_at"] = iso_z(self.published_at)
        return d

    @staticmethod
    def from_payload(d: Dict[str, Any]) -> "NormalisedArticle":
        d = dict(d)
        d["published_at"] = to_naive_utc(
            datetime.fromisoformat(d["published_at"].replace("Z", "+00:00"))
        )
        return NormalisedArticle(**d)
