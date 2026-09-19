"""End-to-end tests against a real (temporary) database.

These assert the product guarantees from the brief, not implementation details:
duplicates are collapsed, independent coverage clusters into one story,
syndication is discounted, ingestion is idempotent, and a user never sees the
same story twice.
"""

import asyncio
from datetime import timedelta

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

from newsagg.clustering.engine import StoryEngine
from newsagg.config import SETTINGS
from newsagg.db.models import Article, Story
from newsagg.db.repositories import get_or_create_source
from newsagg.db.session import db_session
from newsagg.ingestion.normaliser import normalise
from newsagg.ingestion.schemas import RawArticle
from newsagg.text.normalise import utcnow

pytestmark = pytest.mark.asyncio


async def ingest(engine, session, source_name, url, title, body, reliability=0.8,
                 published=None):
    """Push one article through normalise -> dedup -> cluster."""
    source = await get_or_create_source(session, source_name, reliability)
    raw = RawArticle(
        source_id=source.id,
        url=url,
        title=title,
        body=body,
        published_at=published or utcnow(),
    )
    norm = normalise(raw)
    assert norm is not None, f"normaliser rejected {title!r}"
    return await engine.ingest(session, norm)


FED_COVERAGE = [
    ("reuters.com", "https://reuters.com/fed-1",
     "Federal Reserve holds interest rates steady as inflation cools",
     "The Federal Reserve left its benchmark interest rate unchanged on Wednesday, "
     "citing easing inflation pressures while signalling it remains cautious about "
     "cutting borrowing costs too quickly. Policymakers voted unanimously."),
    ("bbc.co.uk", "https://bbc.co.uk/fed-2",
     "US central bank leaves borrowing costs on hold",
     "The Federal Reserve has decided against changing interest rates, holding its "
     "benchmark rate steady as inflation slows. Markets had widely expected the move."),
    ("nytimes.com", "https://nytimes.com/fed-3",
     "Fed stands pat on rates amid cooling inflation data",
     "Federal Reserve officials held interest rates steady on Wednesday as recent "
     "inflation data showed price pressures easing across the US economy."),
]


class TestDeduplication:
    async def test_exact_url_replay_is_a_noop(self, engine):
        async with db_session() as s:
            first = await ingest(engine, s, *FED_COVERAGE[0])
            second = await ingest(engine, s, *FED_COVERAGE[0])
        assert first.status == "created"
        assert second.status == "duplicate" and second.dup_kind == "exact_url"
        assert second.article_id == first.article_id

    async def test_tracking_params_do_not_create_a_second_article(self, engine):
        src, url, title, body = FED_COVERAGE[0]
        async with db_session() as s:
            await ingest(engine, s, src, url, title, body)
            again = await ingest(
                engine, s, src, url + "?utm_source=twitter&fbclid=abc", title, body
            )
            count = await s.scalar(select(func.count()).select_from(Article))
        assert again.dup_kind == "exact_url"
        assert count == 1

    async def test_verbatim_republication_is_syndication(self, engine):
        """Same text, different outlet: counted, but not independent."""
        _, _, title, body = FED_COVERAGE[0]
        async with db_session() as s:
            await ingest(engine, s, "reuters.com", "https://reuters.com/x", title, body)
            copy = await ingest(engine, s, "aljazeera.com", "https://aljazeera.com/x",
                                title, body)
        assert copy.dup_kind == "syndicated"

    async def test_rewritten_headline_wire_copy_is_caught(self, engine):
        """The case global LSH cannot reach at a safe threshold (L2b)."""
        body = ("A magnitude 6.4 earthquake struck off the coast of northern Japan on "
                "Friday, the Japan Meteorological Agency said. No tsunami warning was "
                "issued and there were no immediate reports of casualties or damage.")
        async with db_session() as s:
            await ingest(engine, s, "reuters.com", "https://reuters.com/q1",
                         "Magnitude 6.4 earthquake strikes off the coast of northern Japan",
                         body)
            reworded = await ingest(
                engine, s, "theguardian.com", "https://theguardian.com/q2",
                "6.4 magnitude quake hits off northern Japan coast",
                body.replace("said", "reported").replace("damage", "serious damage"),
            )
        assert reworded.dup_kind == "syndicated"

    async def test_independent_reporting_is_not_a_duplicate(self, engine):
        async with db_session() as s:
            await ingest(engine, s, *FED_COVERAGE[0])
            other = await ingest(engine, s, *FED_COVERAGE[1])
        assert other.dup_kind is None, "independent article wrongly suppressed"


class TestClustering:
    async def test_independent_coverage_forms_one_story(self, engine):
        async with db_session() as s:
            results = [await ingest(engine, s, *a) for a in FED_COVERAGE]
            stories = await s.scalar(
                select(func.count()).select_from(Story).where(Story.status == "active")
            )
        assert len({r.story_id for r in results}) == 1
        assert stories == 1

    async def test_unrelated_events_stay_separate(self, engine):
        async with db_session() as s:
            a = await ingest(engine, s, *FED_COVERAGE[0])
            b = await ingest(
                engine, s, "techcrunch.com", "https://techcrunch.com/chip",
                "Chipmaker unveils next-generation AI accelerator",
                "The company announced a new AI accelerator chip that doubles training "
                "throughput for large language models while cutting power consumption.",
            )
        assert a.story_id != b.story_id

    async def test_source_counts_reflect_independence(self, engine):
        async with db_session() as s:
            res = [await ingest(engine, s, *a) for a in FED_COVERAGE]
            # one verbatim republication on top
            await ingest(engine, s, "aljazeera.com", "https://aljazeera.com/fed-copy",
                         FED_COVERAGE[0][2], FED_COVERAGE[0][3])
            story = await s.get(Story, res[0].story_id)
        assert story.independent_source_count == 3
        assert story.syndicated_source_count == 1

    async def test_syndication_is_discounted_in_authority(self, engine):
        """Ten outlets running one wire story is not ten confirmations."""
        _, _, title, body = FED_COVERAGE[0]
        async with db_session() as s:
            first = await ingest(engine, s, "reuters.com", "https://r.com/a",
                                 title, body, reliability=0.9)
            solo = await s.get(Story, first.story_id)
            solo_authority = solo.authority
            await ingest(engine, s, "copy1.com", "https://c1.com/a", title, body,
                         reliability=0.9)
            await s.refresh(solo)
            with_copy = solo.authority
        gain = with_copy - solo_authority
        assert 0 < gain < 0.9, f"syndicated copy contributed {gain:.2f}, expected a discount"

    async def test_merge_pass_repairs_an_over_split(self, engine):
        """'Fed' vs 'Federal Reserve' splits initially and converges later."""
        async with db_session() as s:
            await ingest(engine, s, "apnews.com", "https://ap.com/fed",
                         "Fed keeps rates unchanged, points to slowing inflation",
                         "The US central bank kept interest rates on hold at its policy "
                         "meeting, saying inflation has continued to cool but remains "
                         "above the two percent target.")
            for a in FED_COVERAGE:
                await ingest(engine, s, *a)
            before = await s.scalar(
                select(func.count()).select_from(Story).where(Story.status == "active")
            )
            await engine.merge_pass(s)
            after = await s.scalar(
                select(func.count()).select_from(Story).where(Story.status == "active")
            )
        assert after <= before
        assert after == 1, f"merge pass left {after} stories for one event"

    async def test_late_article_joins_and_preserves_event_time(self, engine):
        """Recency must measure the age of the event, not of our knowledge of it."""
        old = utcnow() - timedelta(hours=10)
        async with db_session() as s:
            first = await ingest(engine, s, *FED_COVERAGE[0], published=old)
            story = await s.get(Story, first.story_id)
            original_event_time = story.event_time
            late = await ingest(engine, s, *FED_COVERAGE[1], published=utcnow())
            await s.refresh(story)
        assert late.story_id == first.story_id
        assert story.event_time == original_event_time


class TestApiGuarantees:
    async def test_feed_never_repeats_a_story(self, client, engine):
        async with db_session() as s:
            for a in FED_COVERAGE:
                await ingest(engine, s, *a)
            await ingest(engine, s, "techcrunch.com", "https://tc.com/chip",
                         "Chipmaker unveils next-generation AI accelerator",
                         "The company announced a new AI accelerator chip that doubles "
                         "training throughput for large language models.")

        seen = []
        for _ in range(4):
            r = await client.get("/v1/feed", params={"user_id": "u1", "limit": 1})
            seen += [st["id"] for st in r.json()["stories"]]
        assert len(seen) == len(set(seen)), "a story was delivered twice"

    async def test_cursor_replays_the_same_page(self, client, engine):
        async with db_session() as s:
            for a in FED_COVERAGE:
                await ingest(engine, s, *a)
            await ingest(engine, s, "techcrunch.com", "https://tc.com/chip",
                         "Chipmaker unveils next-generation AI accelerator",
                         "The company announced a new AI accelerator chip that doubles "
                         "training throughput for large language models.")

        first = await client.get("/v1/feed", params={"user_id": "u2", "limit": 1})
        cursor = first.json()["next_cursor"]
        assert cursor

        a = await client.get("/v1/feed", params={"user_id": "u2", "limit": 1,
                                                 "cursor": cursor})
        b = await client.get("/v1/feed", params={"user_id": "u2", "limit": 1,
                                                 "cursor": cursor})
        assert b.json()["replayed"] is True
        assert [s["id"] for s in a.json()["stories"]] == \
               [s["id"] for s in b.json()["stories"]]

    async def test_head_stays_live_without_an_idempotency_key(self, client, engine):
        async with db_session() as s:
            for a in FED_COVERAGE:
                await ingest(engine, s, *a)
            await ingest(engine, s, "techcrunch.com", "https://tc.com/chip",
                         "Chipmaker unveils next-generation AI accelerator",
                         "The company announced a new AI accelerator chip that doubles "
                         "training throughput for large language models.")

        one = await client.get("/v1/feed", params={"user_id": "u3", "limit": 1})
        two = await client.get("/v1/feed", params={"user_id": "u3", "limit": 1})
        assert two.json()["replayed"] is False
        assert one.json()["stories"][0]["id"] != two.json()["stories"][0]["id"]

    async def test_head_is_replayable_with_an_idempotency_key(self, client, engine):
        async with db_session() as s:
            for a in FED_COVERAGE:
                await ingest(engine, s, *a)

        h = {"Idempotency-Key": "retry-1"}
        one = await client.get("/v1/feed", params={"user_id": "u4", "limit": 1}, headers=h)
        two = await client.get("/v1/feed", params={"user_id": "u4", "limit": 1}, headers=h)
        assert two.json()["replayed"] is True
        assert [s["id"] for s in one.json()["stories"]] == \
               [s["id"] for s in two.json()["stories"]]

    async def test_ingest_idempotency_key_replays(self, client):
        body = {"articles": [{
            "source": "ft.com", "reliability": 0.9,
            "url": "https://ft.com/fed-idem",
            "title": "Federal Reserve leaves interest rates unchanged as inflation eases",
            "body": "The Federal Reserve held its benchmark interest rate steady on "
                    "Wednesday, with policymakers pointing to cooling inflation.",
        }]}
        h = {"Idempotency-Key": "ingest-1"}
        a = await client.post("/v1/ingest", json=body, headers=h)
        b = await client.post("/v1/ingest", json=body, headers=h)
        assert a.status_code == 202 and a.json()["created"] == 1
        assert b.json() == a.json()

    async def test_ingest_key_reuse_with_different_body_is_rejected(self, client):
        h = {"Idempotency-Key": "ingest-2"}
        await client.post("/v1/ingest", headers=h, json={"articles": [{
            "source": "a.com", "url": "https://a.com/1",
            "title": "A headline long enough to pass validation",
            "body": "Body text for the first request."}]})
        clash = await client.post("/v1/ingest", headers=h, json={"articles": [{
            "source": "b.com", "url": "https://b.com/2",
            "title": "A different headline entirely here",
            "body": "Body text for the second request."}]})
        assert clash.status_code == 409

    async def test_story_detail_exposes_dedup_relations(self, client, engine):
        _, _, title, body = FED_COVERAGE[0]
        async with db_session() as s:
            first = await ingest(engine, s, "reuters.com", "https://r.com/d", title, body)
            await ingest(engine, s, "aljazeera.com", "https://alj.com/d", title, body)

        r = await client.get(f"/v1/stories/{first.story_id}")
        relations = {a["relation"] for a in r.json()["articles"]}
        assert "original" in relations and "syndicated" in relations

    async def test_health_endpoints(self, client):
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/readyz")).status_code == 200
