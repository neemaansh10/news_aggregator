"""FastAPI application assembly.

Every route lives in its own module under :mod:`newsagg.api.routes` and is
mounted here as an ``APIRouter``. Keeping assembly separate from the routes
means a route module never imports the app, which is what keeps the import
graph acyclic.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from ..runtime import lifespan
from .routes import feed, ingest, ops, sources, users
from .ui import DEMO_HTML

app = FastAPI(
    title="News Aggregator API",
    version="1.0.0",
    description=(
        "Ingests articles from many publishers, collapses duplicates, clusters "
        "coverage into stories, and serves a ranked, de-duplicated feed."
    ),
    lifespan=lifespan,
)

app.include_router(ops.router)
app.include_router(ingest.router)
app.include_router(sources.router)
app.include_router(feed.router)
app.include_router(users.router)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> str:
    """Minimal feed viewer, so the system can be inspected without a client."""
    return DEMO_HTML
