"""Typed, environment-driven configuration for every component.

Nothing in the pipeline reads ``os.environ`` directly - a single typed settings
object is what makes the system safe to reconfigure per environment. Override
any field with a ``NEWSAGG_``-prefixed environment variable or a ``.env`` file.
"""

from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

# SECTION 1 - CONFIGURATION
# =============================================================================
# Every tunable lives here and is overridable via environment variables with the
# NEWSAGG_ prefix or a .env file.  Nothing in the pipeline reads os.environ
# directly - a single typed settings object is what makes the system safe to
# reconfigure per environment.
# =============================================================================


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEWSAGG_", env_file=".env", extra="ignore"
    )

    # -- Runtime ------------------------------------------------------------
    environment: str = "local"
    log_level: str = "INFO"
    log_json: bool = False

    # -- Infrastructure -----------------------------------------------------
    # SQLite by default so the project runs with zero external services.
    # Swap to: postgresql+asyncpg://user:pass@localhost:5432/newsagg
    database_url: str = "sqlite+aiosqlite:///./newsagg.db"
    redis_url: Optional[str] = None  # e.g. redis://localhost:6379/0
    event_bus: str = "memory"  # memory | kafka
    kafka_bootstrap: str = "localhost:9092"

    api_host: str = "127.0.0.1"
    api_port: int = 8000

    # -- Pipeline concurrency ----------------------------------------------
    fetch_concurrency: int = 16
    normalise_workers: int = 4
    story_workers: int = 2  # sharded by blocking key, see StoryEngine
    http_timeout_s: float = 12.0
    user_agent: str = "NewsAggregatorBot/1.0 (+https://example.com/bot)"
    max_body_chars: int = 20_000

    # -- Source polling -----------------------------------------------------
    scheduler_tick_s: int = 15
    default_poll_interval_s: int = 180
    max_poll_interval_s: int = 3600
    breaker_fail_threshold: int = 5
    breaker_cooldown_s: int = 300

    # -- De-duplication -----------------------------------------------------
    # 8 bands x 8 bits.  Pigeonhole: any pair within Hamming distance <= 7 must
    # agree on at least one band, so band lookup is a complete candidate
    # generator at this threshold - no recall loss, one indexed query.
    simhash_bands: int = 8
    simhash_hamming_max: int = 7
    # Within-cluster MinHash check, for wire copies whose headline was rewritten
    # far enough to escape the SimHash threshold.
    near_dup_jaccard: float = 0.45
    dedup_window_h: int = 168  # only look back 7 days for duplicates
    dup_check_members: int = 25  # members compared per within-cluster check

    # -- Clustering ---------------------------------------------------------
    # Thresholds are calibrated against a labelled pair set, not guessed - see
    # `similarity()` and the calibration section of SYSTEM_DESIGN.md.
    cluster_window_h: int = 72  # stories older than this stop accepting joins
    w_sim_cosine: float = 0.70  # lexical weight in the composite similarity
    w_sim_entity: float = 0.30  # entity-overlap weight
    assign_threshold: float = 0.30  # similarity >= this -> join existing story
    # Merge sits BELOW assign on purpose.  A centroid is a mean over members,
    # and averaging shrinks the distinctive term weights that drive cosine, so
    # cluster-to-cluster scores are systematically damped versus
    # article-to-cluster.  A merge also rests on more evidence (two aggregates
    # agreeing) and is reviewable offline, so the lower bar is the safer one.
    merge_threshold: float = 0.26  # story-to-story similarity -> merge
    centroid_top_k: int = 192  # sparse centroid dimensionality cap
    candidate_terms: int = 24  # query terms used for blocking
    candidate_limit: int = 60  # max candidate stories scored exactly
    reindex_every: int = 4  # rewrite inverted index every N joins
    story_entity_cap: int = 96  # entities retained per story

    # -- Ranking ------------------------------------------------------------
    half_life_h: float = 8.0
    w_authority: float = 0.45
    w_breadth: float = 0.30
    w_velocity: float = 0.15
    w_relevance: float = 0.10
    syndication_discount: float = 0.35
    velocity_window_h: float = 1.0

    # -- Caching ------------------------------------------------------------
    feed_cache_ttl_s: int = 15
    story_cache_ttl_s: int = 60
    feed_page_ttl_s: int = 3600  # idempotent replay window for a feed cursor
    idempotency_ttl_s: int = 86_400

    # -- Retention ----------------------------------------------------------
    story_retention_days: int = 30
    rank_refresh_s: int = 30
    merge_pass_s: int = 120
    df_flush_s: int = 30


SETTINGS = Settings()
