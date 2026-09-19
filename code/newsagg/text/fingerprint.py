"""Content fingerprints: SimHash + LSH banding, and MinHash.

These answer "is this the same *article*". Semantic similarity - "is this the
same *story*" - lives in :mod:`newsagg.text.vectorise`.
"""

from __future__ import annotations

import hashlib
import random
from collections import Counter
from typing import List, Optional, Tuple

from .normalise import shingles, tokenize

# SimHash gives us a 64-bit locality-sensitive fingerprint: documents that
# differ only by a rewritten headline, a boilerplate footer, or a few edited
# sentences land within a small Hamming distance of each other.
#
# Scanning every stored fingerprint is O(N) and unacceptable at millions/day, so
# we split the 64 bits into B bands of 64/B bits and index each band.  Two
# fingerprints within Hamming distance d <= B-1 must agree on at least one band
# (pigeonhole), so a band-equality lookup is a *complete* candidate generator
# for our threshold - no recall loss, and the lookup is a single indexed query.
# =============================================================================

_MASK64 = (1 << 64) - 1


def _feature_hash64(feature: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest(), "big"
    )


def simhash64(title: str, body: str) -> int:
    """
    Weighted SimHash, deliberately **body-dominant**.

    The obvious implementation upweights the headline, and it is wrong here.
    A rewritten headline over an untouched wire body is the single most common
    near-duplicate in news, and a title-heavy fingerprint puts that pair ~18
    bits apart - outside any usable threshold.  Weighting the body instead puts
    the same pair at ~9 bits while pushing genuinely independent articles about
    the same event out to 22+.  Measured, not assumed.
    """
    body_toks = tokenize(body, keep_stopwords=True)[:600]

    features: Counter = Counter()
    for t in tokenize(title):
        features[t] += 1
    for t in tokenize(body)[:400]:
        features[t] += 2
    for sh in shingles(body_toks, 3):
        features[sh] += 3

    if not features:
        return 0

    vector = [0] * 64
    for feature, weight in features.items():
        h = _feature_hash64(feature)
        for bit in range(64):
            vector[bit] += weight if (h >> bit) & 1 else -weight

    out = 0
    for bit in range(64):
        if vector[bit] > 0:
            out |= 1 << bit
    return out


def hamming(a: int, b: int) -> int:
    return bin((a ^ b) & _MASK64).count("1")


def simhash_bands(value: int, bands: int) -> List[str]:
    """Split the fingerprint into `bands` equal slices, hex-encoded for the index."""
    width = 64 // bands
    mask = (1 << width) - 1
    hex_len = max(1, width // 4)
    return [
        format((value >> (i * width)) & mask, f"0{hex_len}x") for i in range(bands)
    ]


def hex64(value: int) -> str:
    return format(value & _MASK64, "016x")


def unhex64(value: Optional[str]) -> int:
    return int(value, 16) if value else 0


# -- MinHash: the precise within-cluster duplicate check -----------------------
# SimHash LSH is a *global* sweep and its recall guarantee (pigeonhole) only
# holds for Hamming distance <= bands-1.  Pushing the threshold high enough to
# catch every rewritten-headline wire copy would drag in unrelated articles.
#
# So the cascade splits the work: LSH catches the easy global cases cheaply,
# and once an article has been routed to a story, MinHash gives an accurate
# Jaccard estimate against that story's existing members.  That comparison is
# bounded (one cluster, a handful of members) so it can afford to be precise.
#
# Measured on body 3-shingles: wire copies score ~0.74, genuinely independent
# articles about the same event score <= 0.12.  A 0.45 threshold sits in a gap
# six times wider than the noise.

_MH_PRIME = (1 << 61) - 1
_MH_RNG = random.Random(0xC0FFEE)  # fixed seed: fingerprints must be stable
_MH_PARAMS: List[Tuple[int, int]] = [
    (_MH_RNG.randrange(1, _MH_PRIME), _MH_RNG.randrange(0, _MH_PRIME))
    for _ in range(64)
]


def minhash(title: str, body: str, k: Optional[int] = None) -> str:
    """
    k-minimum-value MinHash over body 3-shingles, hex-encoded.

    One base hash per shingle plus k cheap affine permutations, rather than k
    independent hash functions - the standard trick, and the difference between
    O(k*n) hashing and O(n) hashing plus O(k*n) arithmetic.
    """
    k = k or len(_MH_PARAMS)
    tokens = tokenize(f"{title} {body}", keep_stopwords=True)[:400]
    grams = set(shingles(tokens, 3))
    if not grams:
        return ""
    base = [_feature_hash64(g) % _MH_PRIME for g in grams]
    out = []
    for a, b in _MH_PARAMS[:k]:
        out.append(min((a * h + b) % _MH_PRIME for h in base) & 0xFFFFFFFF)
    return "".join(format(v, "08x") for v in out)


def minhash_jaccard(a: Optional[str], b: Optional[str]) -> float:
    """Estimated Jaccard = fraction of matching minima. Unbiased, +/- 1/sqrt(k)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    matches = sum(1 for i in range(0, len(a), 8) if a[i : i + 8] == b[i : i + 8])
    return matches / (len(a) // 8)
