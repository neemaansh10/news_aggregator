"""Command-line entrypoints.

Each subcommand maps to a container command in the production topology:
``serve`` is the feed-api, ``poll-once`` is the fetch cycle, ``demo`` is the
network-free demonstration used in CI smoke tests.
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from .config import SETTINGS
from .db.models import Article, Source, Story
from .db.session import db_session, dispose_engine, init_db
from .demo.fixtures import seed_demo
from .observability import configure_logging
from .ranking.ranker import RANKER
from .ranking.scoring import recency_multiplier
from .runtime import shutdown, startup
from .text.normalise import domain_of, utcnow
from .text.vectorise import VECTORISER


async def cmd_init_db() -> None:
    configure_logging()
    await init_db()
    print(f"Schema created at {SETTINGS.database_url}")
    await dispose_engine()


async def cmd_demo() -> None:
    """Seed the offline corpus, run the full pipeline, print the ranked feed."""
    configure_logging()
    state = await startup(run_fetcher=False)
    try:
        stats_in = await seed_demo(state)
        print(f"\nPublished {stats_in['published']} raw articles "
              f"covering {stats_in['events']} real-world events.\n")
        await state.pipeline.drain(timeout=45)

        async with db_session() as session:
            merged = await state.engine.merge_pass(session)
            await RANKER.refresh_all(session)
            await VECTORISER.flush(session)

        async with db_session() as session:
            total = await session.scalar(select(func.count()).select_from(Article))
            dupes = (
                await session.execute(
                    select(Article.dup_kind, func.count())
                    .where(Article.dup_kind.isnot(None))
                    .group_by(Article.dup_kind)
                )
            ).all()
            stories = (
                await session.execute(
                    select(Story)
                    .where(Story.status == "active")
                    .order_by(Story.score.desc())
                )
            ).scalars().all()

            print("=" * 78)
            print("PIPELINE RESULT")
            print("=" * 78)
            print(f"  articles stored     : {total}")
            print(f"  duplicates detected : "
                  f"{', '.join(f'{k}={c}' for k, c in dupes) or 'none'}")
            print(f"  stories formed      : {len(stories)}")
            print(f"  stories merged      : {merged}")
            print()
            print("=" * 78)
            print("RANKED FEED   (* marks a syndicated copy of another source's text)")
            print("=" * 78)
            for i, story in enumerate(stories, 1):
                rows = (
                    await session.execute(
                        select(Source.name, Article.dup_kind)
                        .join(Article, Article.source_id == Source.id)
                        .where(Article.story_id == story.id)
                    )
                ).all()
                names = sorted(
                    {
                        f"{n}{'*' if k == 'syndicated' else ''}"
                        for n, k in rows
                        if k not in ("exact_url", "exact_content", "near_dup")
                    }
                )
                print(f"\n{i}. [{story.score:.4f}] {story.title}")
                print(
                    f"   independent={story.independent_source_count}  "
                    f"syndicated={story.syndicated_source_count}  "
                    f"articles={story.article_count}  "
                    f"authority={story.authority:.2f}  "
                    f"recency=x{recency_multiplier(story.event_time, story.last_activity_at):.3f}"
                )
                print(f"   topics: {', '.join(story.topics or [])}")
                print(f"   sources: {', '.join(names)}")
            print("\n" + "=" * 78)
            print("Now run:  python main.py serve     ->  http://127.0.0.1:8000")
            print("=" * 78 + "\n")
    finally:
        await shutdown()


async def cmd_add_source(name: str, feed_url: str, reliability: float) -> None:
    configure_logging()
    await init_db()
    async with db_session() as session:
        src = Source(
            name=name,
            domain=domain_of(feed_url) or name,
            feed_url=feed_url,
            kind="rss",
            reliability=reliability,
            enabled=True,
            next_poll_at=utcnow(),
        )
        session.add(src)
        try:
            await session.commit()
            print(f"Added source {name} -> {feed_url}")
        except IntegrityError:
            await session.rollback()
            print(f"Source {name} already exists")
    await dispose_engine()


async def cmd_poll_once() -> None:
    """Fetch every due source once, process, and exit. Useful in cron/CI."""
    configure_logging()
    state = await startup(run_fetcher=False)
    try:
        await state.fetcher.tick()
        await state.pipeline.drain(timeout=60)
        async with db_session() as session:
            await state.engine.merge_pass(session)
            n = await RANKER.refresh_all(session)
        print(f"Poll complete. {n} active stories re-ranked.")
    finally:
        await shutdown()


def cmd_serve() -> None:
    import uvicorn

    uvicorn.run(
        "newsagg.api.app:app",
        host=SETTINGS.api_host,
        port=SETTINGS.api_port,
        log_level=SETTINGS.log_level.lower(),
        reload=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="news-aggregator",
        description="Scalable news aggregation, de-duplication and ranking engine.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the database schema")
    sub.add_parser("demo", help="seed an offline corpus and show the ranked feed")
    sub.add_parser("serve", help="run the HTTP API + live pipeline")
    sub.add_parser("poll-once", help="fetch all due sources once, then exit")

    add = sub.add_parser("add-source", help="register an RSS/Atom source")
    add.add_argument("name")
    add.add_argument("feed_url")
    add.add_argument("--reliability", type=float, default=0.6)

    args = parser.parse_args()

    if args.command == "serve":
        cmd_serve()
        return

    # Build the coroutine lazily: instantiating all of them eagerly would leave
    # un-awaited coroutines behind and emit RuntimeWarnings.
    if args.command == "add-source":
        runner = cmd_add_source(args.name, args.feed_url, args.reliability)
    elif args.command == "init-db":
        runner = cmd_init_db()
    elif args.command == "demo":
        runner = cmd_demo()
    elif args.command == "poll-once":
        runner = cmd_poll_once()
    else:  # pragma: no cover - argparse already rejects unknown commands
        parser.error(f"unknown command: {args.command}")
        return

    try:
        asyncio.run(runner)
    except KeyboardInterrupt:
        print("\ninterrupted")
