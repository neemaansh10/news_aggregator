"""Vectoriser, composite similarity, and the ranking formula.

These lock in the calibration findings documented in SYSTEM_DESIGN.md sections
6.4 and 8 - the ones that changed the design after measurement.
"""

import itertools
import math
from datetime import timedelta

import pytest

from newsagg.config import SETTINGS
from newsagg.ranking.scoring import compute_score, recency_multiplier
from newsagg.text.normalise import utcnow
from newsagg.text.vectorise import (
    VECTORISER,
    Signature,
    build_signature,
    cosine,
    l2_normalise,
    merge_centroid,
    similarity,
    top_k,
)

FED = [
    ("Federal Reserve holds interest rates steady as inflation cools",
     "The Federal Reserve left its benchmark interest rate unchanged on Wednesday, "
     "citing easing inflation pressures while signalling it remains cautious about "
     "cutting borrowing costs too quickly. Policymakers voted unanimously."),
    ("Fed keeps rates unchanged, points to slowing inflation",
     "The US central bank kept interest rates on hold at its policy meeting, saying "
     "inflation has continued to cool but remains above the two percent target."),
    ("US central bank leaves borrowing costs on hold",
     "The Federal Reserve has decided against changing interest rates, holding its "
     "benchmark rate steady as inflation slows. Markets had widely expected the move."),
]
CHIP = [
    ("Chipmaker unveils next-generation AI accelerator",
     "The company announced a new AI accelerator chip that it says doubles training "
     "throughput for large language models while cutting power consumption."),
    ("New AI chip announcement sends semiconductor stocks higher",
     "Semiconductor shares climbed after the chipmaker revealed its next-generation "
     "AI accelerator, which promises roughly double the training performance."),
    ("Chipmaker launches AI accelerator, targets data centre demand",
     "The chipmaker unveiled a new artificial intelligence accelerator aimed at data "
     "centre customers, claiming large gains in training throughput and efficiency."),
]
EUROPA = [
    ("Scientists confirm water vapour plumes erupting from Europa",
     "Researchers reported strong evidence of water vapour plumes venting from the "
     "surface of Europa, Jupiter's icy moon, strengthening the case for an ocean."),
    ("Water plumes detected on Jupiter's moon Europa, study finds",
     "A new study reports detection of water vapour plumes erupting from Europa, "
     "supporting the theory that the icy moon hides a liquid ocean beneath its crust."),
]
GROUPS = {"fed": FED, "chip": CHIP, "europa": EUROPA}


def same_event_pairs():
    for name, items in GROUPS.items():
        for a, b in itertools.combinations(items, 2):
            yield name, a, b


def different_event_pairs():
    names = list(GROUPS)
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            for a in GROUPS[names[i]]:
                for b in GROUPS[names[j]]:
                    yield a, b


class TestVectorMath:
    def test_l2_normalise_unit_length(self):
        v = l2_normalise({1: 3.0, 2: 4.0})
        assert math.isclose(math.sqrt(sum(x * x for x in v.values())), 1.0)

    def test_l2_normalise_empty(self):
        assert l2_normalise({}) == {}

    def test_top_k_keeps_largest(self):
        assert set(top_k({1: 0.1, 2: 0.9, 3: 0.5}, 2)) == {2, 3}

    def test_cosine_identity_and_orthogonality(self):
        v = l2_normalise({1: 1.0, 2: 1.0})
        assert math.isclose(cosine(v, v), 1.0, abs_tol=1e-9)
        assert cosine(v, l2_normalise({3: 1.0})) == 0.0

    def test_cosine_with_empty(self):
        assert cosine({}, {1: 1.0}) == 0.0


class TestEncoder:
    def test_encode_is_normalised_and_capped(self):
        vec = VECTORISER.encode(*FED[0])
        assert vec
        assert len(vec) <= SETTINGS.centroid_top_k
        assert math.isclose(math.sqrt(sum(x * x for x in vec.values())), 1.0, abs_tol=1e-9)

    def test_empty_input_yields_empty_vector(self):
        assert VECTORISER.encode("", "") == {}

    def test_signature_carries_entities(self):
        sig = build_signature(*FED[0])
        assert sig.vec and "federal_reserve" in sig.entities
        assert not sig.empty


class TestCompositeSimilarity:
    """The separation property that makes a single global threshold viable."""

    def test_self_similarity_is_high(self):
        sig = build_signature(*FED[0])
        assert similarity(sig, sig) > 0.9

    @pytest.mark.parametrize("name,a,b", list(same_event_pairs()))
    def test_same_event_pairs_score_meaningfully(self, name, a, b):
        assert similarity(build_signature(*a), build_signature(*b)) >= 0.15

    @pytest.mark.parametrize("a,b", list(different_event_pairs()))
    def test_different_event_pairs_score_near_zero(self, a, b):
        assert similarity(build_signature(*a), build_signature(*b)) <= 0.10

    def test_separation_margin(self):
        """Worst same-event pair must clearly beat the best different-event pair."""
        worst_same = min(
            similarity(build_signature(*a), build_signature(*b))
            for _, a, b in same_event_pairs()
        )
        best_diff = max(
            similarity(build_signature(*a), build_signature(*b))
            for a, b in different_event_pairs()
        )
        assert worst_same > best_diff * 2, (
            f"margin collapsed: same={worst_same:.3f} diff={best_diff:.3f}"
        )

    def test_merge_threshold_is_below_assign_threshold(self):
        """Centroids are means, so cluster-to-cluster scores are damped.

        See SYSTEM_DESIGN.md section 7.4 for why this ordering is deliberate.
        """
        assert SETTINGS.merge_threshold < SETTINGS.assign_threshold


class TestCentroid:
    def test_first_member_becomes_the_centroid(self):
        v = VECTORISER.encode(*FED[0])
        assert merge_centroid({}, 0, v, 192) == v

    def test_centroid_stays_normalised_and_capped(self):
        c = VECTORISER.encode(*CHIP[0])
        for title, body in CHIP[1:]:
            c = merge_centroid(c, 1, VECTORISER.encode(title, body), 64)
        assert len(c) <= 64
        assert math.isclose(math.sqrt(sum(x * x for x in c.values())), 1.0, abs_tol=1e-9)

    def test_centroid_moves_toward_new_members(self):
        a, b = VECTORISER.encode(*CHIP[0]), VECTORISER.encode(*CHIP[1])
        merged = merge_centroid(a, 1, b, 192)
        assert cosine(merged, b) > cosine(a, b)


class TestRanking:
    def test_recency_halves_at_the_half_life(self):
        now = utcnow()
        old = now - timedelta(hours=SETTINGS.half_life_h)
        assert math.isclose(recency_multiplier(old, old), 0.5, rel_tol=0.02)

    def test_recency_decreases_monotonically(self):
        now = utcnow()
        ages = [recency_multiplier(now - timedelta(hours=h), now - timedelta(hours=h))
                for h in (0, 4, 12, 48)]
        assert ages == sorted(ages, reverse=True)

    def test_more_independent_sources_scores_higher(self):
        now = utcnow()
        low = compute_score(2.0, 2, 0, now, now)
        high = compute_score(8.0, 9, 0, now, now)
        assert high > low

    def test_breadth_has_diminishing_returns(self):
        """Source 2 is far more informative than source 20."""
        now = utcnow()
        step_early = compute_score(4.0, 4, 0, now, now) - compute_score(2.0, 2, 0, now, now)
        step_late = compute_score(22.0, 22, 0, now, now) - compute_score(20.0, 20, 0, now, now)
        assert step_early > step_late

    def test_recency_is_multiplicative(self):
        """A huge old story must not outrank a modest fresh one."""
        now = utcnow()
        old = now - timedelta(hours=72)
        assert compute_score(2.0, 2, 0, now, now) > compute_score(12.0, 25, 0, old, old)

    def test_velocity_boosts_breaking_stories(self):
        now = utcnow()
        assert compute_score(4.0, 4, 6, now, now) > compute_score(4.0, 4, 0, now, now)

    def test_relevance_cannot_dominate(self):
        """Personalisation reorders comparable stories; it never overrides scale."""
        now = utcnow()
        niche_max_relevance = compute_score(0.9, 1, 0, now, now, relevance=1.0)
        major_no_relevance = compute_score(9.0, 12, 4, now, now, relevance=0.0)
        assert major_no_relevance > niche_max_relevance

    def test_score_is_bounded(self):
        now = utcnow()
        assert 0.0 <= compute_score(999.0, 999, 999, now, now, relevance=1.0) <= 1.5
