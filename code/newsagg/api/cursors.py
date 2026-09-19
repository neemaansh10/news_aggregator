"""Opaque feed cursors.

A cursor is a *page token*, not just a position: :mod:`newsagg.api.routes.feed`
stores the exact story ids served under it, so re-requesting a cursor replays
that page byte-identically.
"""

from __future__ import annotations

import base64
import json
from typing import Optional, Tuple

def encode_cursor(score: float, story_id: str) -> str:
    raw = json.dumps({"s": round(score, 8), "i": story_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> Optional[Tuple[float, str]]:
    try:
        pad = "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(cursor + pad).decode())
        return float(data["s"]), str(data["i"])
    except Exception:
        return None
