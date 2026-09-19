"""Offline corpus exercising every branch of the pipeline.

Five real-world-shaped events with independent coverage from many outlets, plus
an exact URL replay (L1a), the same text under a new URL (L1b), a lightly
reworded wire copy (L2), and varied headlines per event (L3).
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any, Dict, List, Tuple

from sqlalchemy import select

from ..db.models import Source
from ..db.repositories import get_or_create_source
from ..db.session import db_session
from ..infra.event_bus import TOPIC_RAW, Event
from ..ingestion.schemas import RawArticle
from ..runtime import AppState
from ..text.normalise import utcnow

DEMO_SOURCES: List[Tuple[str, float]] = [
    ("reuters.com", 0.95),
    ("apnews.com", 0.94),
    ("bbc.co.uk", 0.92),
    ("nytimes.com", 0.90),
    ("theguardian.com", 0.88),
    ("aljazeera.com", 0.82),
    ("cnbc.com", 0.80),
    ("techcrunch.com", 0.70),
    ("dailybuzzfeednews.example", 0.35),
]

DEMO_EVENTS: List[Dict[str, Any]] = [
    {
        "age_h": 2,
        "articles": [
            ("reuters.com", "Federal Reserve holds interest rates steady as inflation cools",
             "The Federal Reserve left its benchmark interest rate unchanged on Wednesday, "
             "citing easing inflation pressures while signalling it remains cautious about "
             "cutting borrowing costs too quickly. Policymakers voted unanimously."),
            ("apnews.com", "Fed keeps rates unchanged, points to slowing inflation",
             "The US central bank kept interest rates on hold at its policy meeting, saying "
             "inflation has continued to cool but remains above the two percent target. "
             "The decision was unanimous among voting members."),
            ("bbc.co.uk", "US central bank leaves borrowing costs on hold",
             "The Federal Reserve has decided against changing interest rates, holding its "
             "benchmark rate steady as inflation slows. Markets had widely expected the move."),
            ("cnbc.com", "Fed holds rates: what it means for markets and mortgages",
             "The Federal Reserve held its benchmark interest rate steady, a decision that "
             "leaves mortgage rates and borrowing costs broadly unchanged. Stocks edged higher "
             "as investors digested the inflation outlook."),
            ("nytimes.com", "Fed stands pat on rates amid cooling inflation data",
             "Federal Reserve officials held interest rates steady on Wednesday as recent "
             "inflation data showed price pressures easing across the US economy."),
        ],
    },
    {
        "age_h": 5,
        "articles": [
            ("reuters.com", "Magnitude 6.4 earthquake strikes off the coast of northern Japan",
             "A magnitude 6.4 earthquake struck off the coast of northern Japan on Friday, "
             "the Japan Meteorological Agency said. No tsunami warning was issued and there "
             "were no immediate reports of casualties or major damage."),
            # Pure syndication: a wire story republished verbatim under a new URL (L1b).
            ("aljazeera.com", "Magnitude 6.4 earthquake strikes off the coast of northern Japan",
             "A magnitude 6.4 earthquake struck off the coast of northern Japan on Friday, "
             "the Japan Meteorological Agency said. No tsunami warning was issued and there "
             "were no immediate reports of casualties or major damage."),
            # Near-duplicate: same wire copy, lightly reworded headline + lede (L2).
            ("theguardian.com", "6.4 magnitude quake hits off northern Japan coast",
             "A magnitude 6.4 earthquake struck off the coast of northern Japan on Friday, "
             "the Japan Meteorological Agency reported. No tsunami warning was issued and "
             "there were no immediate reports of casualties or serious damage."),
            ("bbc.co.uk", "Strong earthquake shakes northern Japan, no tsunami warning",
             "Buildings swayed in northern Japan after a strong undersea earthquake, but "
             "authorities said there was no tsunami risk. Residents reported shaking lasting "
             "nearly a minute."),
        ],
    },
    {
        "age_h": 1,
        "articles": [
            ("techcrunch.com", "Chipmaker unveils next-generation AI accelerator",
             "The company announced a new AI accelerator chip that it says doubles training "
             "throughput for large language models while cutting power consumption per "
             "operation. Shipments begin next quarter."),
            ("cnbc.com", "New AI chip announcement sends semiconductor stocks higher",
             "Semiconductor shares climbed after the chipmaker revealed its next-generation "
             "AI accelerator, which promises roughly double the training performance of the "
             "previous generation for large language models."),
            ("reuters.com", "Chipmaker launches AI accelerator, targets data centre demand",
             "The chipmaker unveiled a new artificial intelligence accelerator aimed at data "
             "centre customers, claiming large gains in training throughput and power "
             "efficiency for large language models."),
            ("nytimes.com", "A faster AI chip arrives as data centre demand surges",
             "A new artificial intelligence accelerator was announced on Tuesday, promising "
             "faster training of large language models at lower power, as data centre "
             "operators race to add capacity."),
            ("theguardian.com", "AI chip race intensifies with new accelerator launch",
             "The competition to supply artificial intelligence data centres intensified "
             "with the launch of a new accelerator chip promising higher training throughput "
             "and better power efficiency."),
            ("dailybuzzfeednews.example", "This new AI chip is INSANE and here's why",
             "A new artificial intelligence accelerator chip was announced and it doubles "
             "training throughput for large language models. Data centre demand is surging."),
        ],
    },
    {
        "age_h": 14,
        "articles": [
            ("bbc.co.uk", "Ruling party wins narrow parliamentary majority in national election",
             "The governing party has secured a slim parliamentary majority after a closely "
             "fought national election, official results showed, setting up a difficult term "
             "for the incoming government."),
            ("apnews.com", "Governing party holds on with thin majority after vote count",
             "Final results from the national election gave the ruling party a narrow "
             "parliamentary majority, with opposition parties gaining ground in several "
             "key regions."),
            ("aljazeera.com", "Election results confirm slim majority for governing party",
             "Official election results confirmed a narrow parliamentary majority for the "
             "governing party, after a campaign dominated by the cost of living and housing."),
        ],
    },
    {
        "age_h": 30,
        "articles": [
            ("nytimes.com", "Scientists confirm water vapour plumes erupting from Europa",
             "Researchers reported strong evidence of water vapour plumes venting from the "
             "surface of Europa, Jupiter's icy moon, strengthening the case that a subsurface "
             "ocean could be sampled by a passing spacecraft."),
            ("theguardian.com", "Water plumes detected on Jupiter's moon Europa, study finds",
             "A new study reports detection of water vapour plumes erupting from Europa, "
             "supporting the theory that the icy moon hides a liquid ocean beneath its crust."),
        ],
    },
]


def _demo_url(domain: str, title: str, salt: str = "") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:70]
    return f"https://www.{domain}/news/{slug}{salt}?utm_source=rss&utm_medium=feed"


async def seed_demo(state: AppState) -> Dict[str, int]:
    """Build the demo corpus and push it through the real pipeline."""
    async with db_session() as session:
        for name, reliability in DEMO_SOURCES:
            src = await get_or_create_source(session, name, reliability)
            if src.reliability != reliability:
                src.reliability = reliability
        await session.commit()

        source_ids = dict(
            (
                await session.execute(select(Source.name, Source.id))
            ).all()
        )

    raws: List[RawArticle] = []
    for event_spec in DEMO_EVENTS:
        published = utcnow() - timedelta(hours=event_spec["age_h"])
        for offset, (domain, title, body) in enumerate(event_spec["articles"]):
            raws.append(
                RawArticle(
                    source_id=source_ids[domain],
                    url=_demo_url(domain, title),
                    title=title,
                    body=body,
                    summary=body[:200],
                    author=None,
                    published_at=published + timedelta(minutes=7 * offset),
                )
            )

    # Exact URL replay (L1a): identical article delivered twice by the broker.
    raws.append(raws[0])
    # Same canonical URL wearing different tracking parameters.
    replay = raws[2]
    raws.append(
        RawArticle(
            source_id=replay.source_id,
            url=replay.url.replace("utm_source=rss", "utm_source=twitter&fbclid=abc123"),
            title=replay.title,
            body=replay.body,
            summary=replay.summary,
            published_at=replay.published_at,
        )
    )

    for raw in raws:
        await state.bus.publish(Event(TOPIC_RAW, str(raw.source_id), raw.to_payload()))

    return {"published": len(raws), "events": len(DEMO_EVENTS)}
