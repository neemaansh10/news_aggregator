"""The ranking formula::

    score = ( Wa*authority + Wb*breadth + Wv*velocity ) * recency + Wr*relevance

* **authority** - sum of source reliabilities, syndication-discounted.
* **breadth**   - count of *independent* sources. Logarithmic, because the
  second source confirming a story is far more informative than the twentieth.
* **velocity**  - new independent sources in the last hour; separates breaking
  from merely well-covered.
* **recency**   - exponential half-life decay, applied *multiplicatively* so
  nothing escapes it. This is the difference between a feed that stays fresh
  and one that doesn't.
* **relevance** - per-user topic affinity, added after decay and capped, so
  personalisation can reorder comparable stories but never let a stale niche
  item outrank a major breaking event.
"""

from __future__ import annotations

import math
from datetime import datetime

from ..config import SETTINGS
from ..text.normalise import utcnow

_AUTHORITY_NORM = math.log1p(12.0)  # ~12 reliability-points saturates
_BREADTH_NORM = math.log1p(25.0)  # ~25 independent sources saturates
_VELOCITY_NORM = math.log1p(10.0)


def recency_multiplier(event_time: datetime, last_activity: datetime) -> float:
    """
    Decay from a blend of event time and last activity.  Pure event time buries
    a developing story; pure last-activity lets a trivial late follow-up revive
    a stale one.  The 70/30 blend keeps developing stories alive without
    resurrecting dead ones.
    """
    now = utcnow()
    age_event_h = max(0.0, (now - event_time).total_seconds() / 3600.0)
    age_activity_h = max(0.0, (now - last_activity).total_seconds() / 3600.0)
    effective_age = 0.7 * age_event_h + 0.3 * age_activity_h
    return 0.5 ** (effective_age / SETTINGS.half_life_h)


def compute_score(
    authority: float,
    independent_sources: int,
    velocity: float,
    event_time: datetime,
    last_activity: datetime,
    relevance: float = 0.0,
) -> float:
    a = min(1.0, math.log1p(max(0.0, authority)) / _AUTHORITY_NORM)
    b = min(1.0, math.log1p(max(0, independent_sources)) / _BREADTH_NORM)
    v = min(1.0, math.log1p(max(0.0, velocity)) / _VELOCITY_NORM)

    base = (
        SETTINGS.w_authority * a
        + SETTINGS.w_breadth * b
        + SETTINGS.w_velocity * v
    )
    decayed = base * recency_multiplier(event_time, last_activity)
    return round(decayed + SETTINGS.w_relevance * max(0.0, min(1.0, relevance)), 6)
