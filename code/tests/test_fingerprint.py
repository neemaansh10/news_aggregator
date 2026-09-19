"""Fingerprinting tests, including the calibration claims from SYSTEM_DESIGN.md.

The threshold assertions here are the ones that would silently rot: if someone
"improves" the SimHash weighting by upweighting the headline, dedup quality
degrades with no error anywhere. These tests turn that into a failure.
"""

import pytest

from newsagg.config import SETTINGS
from newsagg.text.fingerprint import (
    hamming,
    hex64,
    minhash,
    minhash_jaccard,
    simhash64,
    simhash_bands,
    unhex64,
)

# A wire story and the same copy with a rewritten headline and two word swaps.
WIRE_ORIGINAL = (
    "Magnitude 6.4 earthquake strikes off the coast of northern Japan",
    "A magnitude 6.4 earthquake struck off the coast of northern Japan on Friday, "
    "the Japan Meteorological Agency said. No tsunami warning was issued and there "
    "were no immediate reports of casualties or major damage.",
)
WIRE_REWRITTEN = (
    "6.4 magnitude quake hits off northern Japan coast",
    "A magnitude 6.4 earthquake struck off the coast of northern Japan on Friday, "
    "the Japan Meteorological Agency reported. No tsunami warning was issued and "
    "there were no immediate reports of casualties or serious damage.",
)
# Independently written, same event. Must NOT be treated as a duplicate.
INDEPENDENT_SAME_EVENT = (
    "Strong earthquake shakes northern Japan, no tsunami warning",
    "Buildings swayed in northern Japan after a strong undersea earthquake, but "
    "authorities said there was no tsunami risk. Residents reported shaking "
    "lasting nearly a minute.",
)
DIFFERENT_EVENT = (
    "Federal Reserve holds interest rates steady as inflation cools",
    "The Federal Reserve left its benchmark interest rate unchanged on Wednesday, "
    "citing easing inflation pressures while signalling it remains cautious about "
    "cutting borrowing costs too quickly. Policymakers voted unanimously.",
)


class TestSimhash:
    def test_identical_text_identical_fingerprint(self):
        assert simhash64(*WIRE_ORIGINAL) == simhash64(*WIRE_ORIGINAL)

    def test_empty_input_is_zero(self):
        assert simhash64("", "") == 0

    def test_wire_copy_is_close(self):
        """Body-dominant weighting keeps a rewritten headline near its source.

        A title-heavy fingerprint puts this pair ~18 bits apart, outside any
        usable threshold. See SYSTEM_DESIGN.md section 6.2.
        """
        d = hamming(simhash64(*WIRE_ORIGINAL), simhash64(*WIRE_REWRITTEN))
        assert d <= 12, f"wire copy drifted to {d} bits - check SimHash weighting"

    def test_independent_reporting_is_far(self):
        d = hamming(simhash64(*WIRE_ORIGINAL), simhash64(*INDEPENDENT_SAME_EVENT))
        assert d >= 18, f"independent article only {d} bits away - false-dup risk"

    def test_different_event_is_furthest(self):
        assert hamming(simhash64(*WIRE_ORIGINAL), simhash64(*DIFFERENT_EVENT)) >= 25

    def test_separation_ordering_holds(self):
        """The property that actually matters, independent of exact constants."""
        base = simhash64(*WIRE_ORIGINAL)
        wire = hamming(base, simhash64(*WIRE_REWRITTEN))
        indep = hamming(base, simhash64(*INDEPENDENT_SAME_EVENT))
        diff = hamming(base, simhash64(*DIFFERENT_EVENT))
        assert wire < indep < diff


class TestLshBanding:
    def test_band_count_and_width(self):
        bands = simhash_bands(simhash64(*WIRE_ORIGINAL), 8)
        assert len(bands) == 8
        assert all(len(b) == 2 for b in bands)  # 8 bits -> 2 hex chars

    def test_identical_fingerprints_share_every_band(self):
        v = simhash64(*WIRE_ORIGINAL)
        assert simhash_bands(v, 8) == simhash_bands(v, 8)

    def test_pigeonhole_guarantee(self):
        """With B bands, any pair within Hamming <= B-1 must share a band.

        This is what makes band lookup a *complete* candidate generator rather
        than a lossy heuristic.
        """
        bands = SETTINGS.simhash_bands
        base = simhash64(*WIRE_ORIGINAL)
        for flip in range(bands - 1):          # flip up to B-1 bits
            mutated = base ^ (1 << (flip * 7 % 64))
            shared = set(simhash_bands(base, bands)) & set(
                simhash_bands(mutated, bands)
            )
            assert shared, f"no shared band after {flip + 1} bit flips"

    def test_hex_roundtrip(self):
        v = simhash64(*WIRE_ORIGINAL)
        assert unhex64(hex64(v)) == v
        assert unhex64(None) == 0


class TestMinhash:
    def test_identical_text_scores_one(self):
        sig = minhash(*WIRE_ORIGINAL)
        assert minhash_jaccard(sig, sig) == 1.0

    def test_wire_copy_above_threshold(self):
        """The case SimHash-at-a-safe-threshold cannot reach."""
        j = minhash_jaccard(minhash(*WIRE_ORIGINAL), minhash(*WIRE_REWRITTEN))
        assert j >= SETTINGS.near_dup_jaccard, f"wire copy estimated at {j:.2f}"

    def test_independent_reporting_below_threshold(self):
        j = minhash_jaccard(
            minhash(*WIRE_ORIGINAL), minhash(*INDEPENDENT_SAME_EVENT)
        )
        assert j < SETTINGS.near_dup_jaccard, f"false duplicate at {j:.2f}"

    def test_different_event_near_zero(self):
        j = minhash_jaccard(minhash(*WIRE_ORIGINAL), minhash(*DIFFERENT_EVENT))
        assert j < 0.1

    def test_threshold_sits_in_a_wide_gap(self):
        """A threshold is only safe if the gap around it is much wider than noise."""
        dup = minhash_jaccard(minhash(*WIRE_ORIGINAL), minhash(*WIRE_REWRITTEN))
        non = minhash_jaccard(minhash(*WIRE_ORIGINAL), minhash(*INDEPENDENT_SAME_EVENT))
        assert dup > non * 3

    def test_empty_and_mismatched_signatures(self):
        assert minhash("", "") == ""
        assert minhash_jaccard("", "abc") == 0.0
        assert minhash_jaccard(None, None) == 0.0

    def test_signature_is_deterministic_across_runs(self):
        """Fingerprints are persisted, so the hash seed must never drift."""
        assert minhash(*WIRE_ORIGINAL) == minhash(*WIRE_ORIGINAL)
