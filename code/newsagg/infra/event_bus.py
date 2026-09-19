"""Event bus abstraction over Kafka, with an in-memory implementation.

Topics and partition keys::

    raw.articles         key = source_id      (per-source ordering)
    normalised.articles  key = blocking_key   (co-locates clusterable articles)
    story.updates        key = story_id

The ``normalised.articles`` key is the load-bearing one: every article that
could plausibly join a given story lands on the same partition, so each story
has exactly one writer and no distributed lock is needed on the hot path.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Sequence

from ..config import SETTINGS
from ..observability import log
from ..text.normalise import utcnow

#   normalised.articles partition key = blocking_key(lang:topic:day)
#                       -> every article that could plausibly cluster together
#                          lands on the same partition, so a story is only ever
#                          written by one consumer and no distributed lock is
#                          needed on the hot path.
#   story.updates       partition key = story_id
# =============================================================================


@dataclass
class Event:
    topic: str
    key: str
    payload: Dict[str, Any]
    ts: datetime = field(default_factory=utcnow)


class EventBus:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish(self, event: Event) -> None: ...
    def subscribe(self, topic: str) -> "asyncio.Queue[Event]": ...


class InMemoryEventBus(EventBus):
    """Backpressured asyncio queues - the local stand-in for Kafka."""

    def __init__(self, maxsize: int = 10_000) -> None:
        self._queues: Dict[str, List["asyncio.Queue[Event]"]] = defaultdict(list)
        self._maxsize = maxsize

    async def start(self) -> None:
        log.info("event bus: in-memory")

    async def stop(self) -> None:
        pass

    async def publish(self, ev: Event) -> None:
        for q in self._queues.get(ev.topic, []):
            await q.put(ev)  # awaits when full == backpressure

    def subscribe(self, topic: str) -> "asyncio.Queue[Event]":
        q: "asyncio.Queue[Event]" = asyncio.Queue(maxsize=self._maxsize)
        self._queues[topic].append(q)
        return q


class KafkaEventBus(EventBus):  # pragma: no cover - requires a broker
    """Drop-in Kafka implementation; identical call sites."""

    def __init__(self, bootstrap: str) -> None:
        self._bootstrap = bootstrap
        self._producer: Any = None
        self._tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer  # type: ignore

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
            key_serializer=lambda k: k.encode(),
            enable_idempotence=True,  # exactly-once *producer* semantics
            acks="all",
            compression_type="lz4",
            linger_ms=20,
        )
        await self._producer.start()
        log.info("event bus: kafka at %s", self._bootstrap)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._producer:
            await self._producer.stop()

    async def publish(self, ev: Event) -> None:
        await self._producer.send_and_wait(ev.topic, ev.payload, key=ev.key)

    def subscribe(self, topic: str) -> "asyncio.Queue[Event]":
        from aiokafka import AIOKafkaConsumer  # type: ignore

        q: "asyncio.Queue[Event]" = asyncio.Queue(maxsize=5_000)

        async def _pump() -> None:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self._bootstrap,
                group_id=f"newsagg-{topic}",
                enable_auto_commit=False,  # commit only after successful handling
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode()),
            )
            await consumer.start()
            try:
                async for msg in consumer:
                    await q.put(
                        Event(topic=topic, key=msg.key or "", payload=msg.value)
                    )
                    await consumer.commit()
            finally:
                await consumer.stop()

        self._tasks.append(asyncio.create_task(_pump()))
        return q


def build_event_bus() -> EventBus:
    if SETTINGS.event_bus == "kafka":
        return KafkaEventBus(SETTINGS.kafka_bootstrap)
    return InMemoryEventBus()


BUS: EventBus = build_event_bus()

TOPIC_RAW = "raw.articles"
TOPIC_NORMALISED = "normalised.articles"
TOPIC_STORY_UPDATES = "story.updates"


def blocking_key(language: str, topics: Sequence[str], event_time: datetime) -> str:
    """
    Co-location key for clustering.  Articles that cannot possibly belong to the
    same story (different language, different topic, different day) are kept
    apart, which is what lets the story engine scale horizontally.  A 2-day
    bucket rather than 1 avoids splitting events that straddle midnight.
    """
    primary = topics[0] if topics else "general"
    bucket = int(event_time.timestamp() // (48 * 3600))
    return f"{language}:{primary}:{bucket}"
