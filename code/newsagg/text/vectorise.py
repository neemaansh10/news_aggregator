"""Streaming TF-IDF, entity-aware signatures, and the composite similarity.

A batch-fitted vectoriser is wrong for a never-ending stream: the vocabulary and
the document frequencies move continuously. Terms are hashed into a fixed 2**18
space and document frequencies are maintained online.

Swap :meth:`Vectoriser.encode` for a sentence-transformer / pgvector backend and
nothing else in the system changes.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional, Set

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import SETTINGS
from ..db.models import CorpusStat, TermStat
from ..db.session import upsert_corpus_stat, upsert_term_stat
from ..observability import log
from .normalise import extract_entities, jaccard, tokenize

# A batch-fitted TF-IDF is wrong for a never-ending stream: the vocabulary and
# the document frequencies move continuously.  Instead we keep *online* document
# frequencies and hash terms into a fixed 2^18 space, which gives:
#   * no vocabulary to fit, ship, or version
#   * O(1) memory per term and stable dimensionality forever
#   * new vocabulary ("Europa plumes") usable the moment it appears
#
# The vector is L2-normalised and truncated to the top-K dimensions, so cosine
# similarity is a plain sparse dot product and centroids stay small enough to
# store as JSON.  Swap `Vectoriser.encode` for a sentence-transformer / pgvector
# backend and nothing else in the system changes.
# =============================================================================

HASH_DIM = 1 << 18


def term_id(term: str) -> int:
    return (
        int.from_bytes(
            hashlib.blake2b(term.encode("utf-8"), digest_size=4).digest(), "big"
        )
        % HASH_DIM
    )


SparseVec = Dict[int, float]


def l2_normalise(vec: SparseVec) -> SparseVec:
    norm = math.sqrt(sum(v * v for v in vec.values()))
    if norm <= 0:
        return {}
    return {k: v / norm for k, v in vec.items()}


def top_k(vec: SparseVec, k: int) -> SparseVec:
    if len(vec) <= k:
        return dict(vec)
    keep = sorted(vec.items(), key=lambda kv: -abs(kv[1]))[:k]
    return dict(keep)


def cosine(a: SparseVec, b: SparseVec) -> float:
    """Both inputs are assumed L2-normalised, so cosine == dot product."""
    if not a or not b:
        return 0.0
    if len(a) > len(b):
        a, b = b, a
    return sum(w * b.get(t, 0.0) for t, w in a.items())


class Vectoriser:
    """
    Maintains global document frequencies with a write-behind cache.

    In a multi-replica deployment `_df` is a Redis hash mutated with HINCRBY;
    here it is an in-process dict persisted to `term_stats`, which is the same
    contract with a different storage engine.  DF drift between replicas is
    harmless - IDF only needs to be approximately right.
    """

    def __init__(self) -> None:
        self._df: Dict[int, int] = {}
        self._doc_count: int = 0
        self._dirty: Set[int] = set()
        self._lock = asyncio.Lock()

    # -- lifecycle ---------------------------------------------------------
    async def load(self, session: AsyncSession) -> None:
        rows = (await session.execute(select(TermStat.term, TermStat.df))).all()
        self._df = {int(t): int(d) for t, d in rows}
        cnt = await session.scalar(
            select(CorpusStat.value).where(CorpusStat.key == "doc_count")
        )
        self._doc_count = int(cnt or 0)
        log.info("vectoriser loaded: %d terms, %d docs", len(self._df), self._doc_count)

    async def flush(self, session: AsyncSession) -> None:
        async with self._lock:
            dirty, self._dirty = self._dirty, set()
            doc_count = self._doc_count
        if not dirty:
            return
        for tid in dirty:
            await upsert_term_stat(session, tid, self._df.get(tid, 0))
        await upsert_corpus_stat(session, "doc_count", doc_count)
        await session.commit()

    # -- scoring -----------------------------------------------------------
    def idf(self, tid: int) -> float:
        df = self._df.get(tid, 0)
        return math.log((self._doc_count + 1.0) / (df + 1.0)) + 1.0

    def encode(self, title: str, body: str, k: Optional[int] = None) -> SparseVec:
        """
        Unigrams only, with the headline upweighted 2x.

        Two choices here were calibrated against a labelled set of same-event /
        different-event pairs rather than guessed:

        * **No title bigrams.**  They look attractive but two outlets almost
          never phrase a headline the same way, so the bigrams are unmatched
          mass that inflates the vector norm and *halves* the cosine between
          genuine same-event pairs.  Removing them lifted same-event similarity
          from ~0.22 to ~0.42 with no loss of cross-event separation.
        * **Title weight 2.0, not 3.0.**  Above 2.0 the headline dominates and
          the body's shared vocabulary - the part different outlets actually
          have in common - stops contributing.
        """
        tf: Dict[str, float] = defaultdict(float)
        for t in tokenize(title):
            tf[t] += 2.0
        for t in tokenize(body)[:400]:
            tf[t] += 1.0
        if not tf:
            return {}

        raw: SparseVec = defaultdict(float)
        for term, freq in tf.items():
            tid = term_id(term)
            raw[tid] += (1.0 + math.log(freq)) * self.idf(tid)

        return l2_normalise(top_k(dict(raw), k or SETTINGS.centroid_top_k))

    def observe(self, vec: SparseVec) -> None:
        """Record one document's terms into the global DF statistics."""
        self._doc_count += 1
        for tid in vec:
            self._df[tid] = self._df.get(tid, 0) + 1
            self._dirty.add(tid)


VECTORISER = Vectoriser()


@dataclass
class Signature:
    """Everything the clustering layer needs to compare two pieces of text."""

    vec: SparseVec
    entities: Set[str]

    @property
    def empty(self) -> bool:
        return not self.vec


def build_signature(title: str, body: str) -> Signature:
    return Signature(
        vec=VECTORISER.encode(title, body), entities=extract_entities(title, body)
    )


def similarity(a: Signature, b: Signature) -> float:
    """
    Composite event similarity in [0, 1].

        similarity = Wc * cosine(tf-idf)  +  We * jaccard(entities)

    The two signals fail in different directions, which is exactly why they are
    combined:

    * Cosine is strong on shared vocabulary but blind to synonymy - it scores
      "Fed" against "Federal Reserve" near zero.
    * Entity overlap is strong on *who and what* but fires on any two articles
      that merely mention the same country, so it is useless alone.

    Measured on a labelled pair set, the composite separates same-event pairs
    (0.20 - 0.66) from different-event pairs (<= 0.04) by roughly 5x, which is
    what makes a single global threshold viable.  See SYSTEM_DESIGN.md for the
    re-calibration procedure when the source mix changes.
    """
    lexical = cosine(a.vec, b.vec)
    entity = jaccard(a.entities, b.entities)
    return SETTINGS.w_sim_cosine * lexical + SETTINGS.w_sim_entity * entity


def merge_centroid(
    centroid: SparseVec, n: int, vec: SparseVec, k: int
) -> SparseVec:
    """Streaming mean of member vectors, re-truncated and re-normalised."""
    if not centroid:
        return dict(vec)
    acc: SparseVec = {t: w * n for t, w in centroid.items()}
    for t, w in vec.items():
        acc[t] = acc.get(t, 0.0) + w
    acc = {t: w / (n + 1) for t, w in acc.items()}
    return l2_normalise(top_k(acc, k))
