"""News Aggregator — scalable ingestion, de-duplication, story clustering and ranking.

Package layout (each subpackage is an independently deployable service in the
production topology described in SYSTEM_DESIGN.md):

    config          typed settings, environment-driven
    observability   structured logging + Prometheus metrics
    text/           normalisation, fingerprinting, vectorisation   [pure library]
    db/             SQLAlchemy models, session management, queries
    infra/          cache, event bus, resilience primitives
    ingestion/      fetchers and the normaliser                    [fetcher, normaliser]
    clustering/     de-duplication cascade + story engine          [story-engine]
    ranking/        scoring formula and periodic re-ranker         [ranker]
    pipeline/       stage workers and maintenance jobs
    api/            FastAPI application and routes                 [feed-api]
    demo/           offline corpus for a network-free demonstration
"""

__version__ = "1.0.0"
