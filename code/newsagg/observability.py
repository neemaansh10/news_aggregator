"""Structured logging and Prometheus metrics.

Metrics degrade to no-ops when ``prometheus_client`` is absent, so call sites
never branch on whether monitoring is installed.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Sequence

from .config import SETTINGS

try:  # pragma: no cover - optional dependency
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter as PromCounter,
        Gauge as PromGauge,
        Histogram as PromHistogram,
        generate_latest,
    )

    HAVE_PROM = True
except Exception:  # pragma: no cover
    HAVE_PROM = False
    CONTENT_TYPE_LATEST = "text/plain"
    generate_latest = None  # type: ignore

# SECTION 2 - OBSERVABILITY (structured logs + metrics)
# =============================================================================


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for k, v in getattr(record, "extra_fields", {}).items():
            payload[k] = v
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    if SETTINGS.log_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(name)-18s %(message)s")
        )
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(SETTINGS.log_level.upper())
    logging.getLogger("httpx").setLevel("WARNING")
    logging.getLogger("sqlalchemy.engine").setLevel("WARNING")


log = logging.getLogger("newsagg")


class _NoopMetric:
    """Stand-in so the code path is identical with or without prometheus."""

    def labels(self, *a: Any, **k: Any) -> "_NoopMetric":
        return self

    def inc(self, *a: Any, **k: Any) -> None:
        pass

    def set(self, *a: Any, **k: Any) -> None:
        pass

    def observe(self, *a: Any, **k: Any) -> None:
        pass


def _counter(name: str, doc: str, labels: Sequence[str] = ()) -> Any:
    return PromCounter(name, doc, list(labels)) if HAVE_PROM else _NoopMetric()


def _gauge(name: str, doc: str, labels: Sequence[str] = ()) -> Any:
    return PromGauge(name, doc, list(labels)) if HAVE_PROM else _NoopMetric()


def _histogram(name: str, doc: str, labels: Sequence[str] = ()) -> Any:
    return PromHistogram(name, doc, list(labels)) if HAVE_PROM else _NoopMetric()


M_FETCHED = _counter("newsagg_articles_fetched_total", "Articles fetched", ["source"])
M_INGESTED = _counter("newsagg_articles_ingested_total", "Articles accepted")
M_DUPES = _counter("newsagg_duplicates_total", "Duplicates suppressed", ["kind"])
M_STORIES_NEW = _counter("newsagg_stories_created_total", "Stories created")
M_STORIES_MERGED = _counter("newsagg_stories_merged_total", "Stories merged")
M_ACTIVE_STORIES = _gauge("newsagg_active_stories", "Active stories in window")
M_PIPELINE_LAG = _gauge("newsagg_pipeline_lag_seconds", "publish->indexed lag")
M_STAGE_LATENCY = _histogram(
    "newsagg_stage_seconds", "Per-stage processing latency", ["stage"]
)
M_FEED_SERVED = _counter("newsagg_feed_pages_total", "Feed pages served", ["cached"])
M_FETCH_ERRORS = _counter("newsagg_fetch_errors_total", "Fetch errors", ["source"])


@asynccontextmanager
async def timed(stage: str) -> AsyncIterator[None]:
    t0 = time.perf_counter()
    try:
        yield
    finally:
        M_STAGE_LATENCY.labels(stage).observe(time.perf_counter() - t0)
