"""Normalisation is the highest-leverage step in the system, so it gets the
densest tests: a canonicalisation bug surfaces downstream as thousands of
phantom "new stories", far from where it was introduced.
"""

from datetime import datetime, timezone

import pytest

from newsagg.text.normalise import (
    canonical_url,
    classify_topics,
    content_hash,
    detect_language,
    domain_of,
    extract_entities,
    iso_z,
    jaccard,
    normalise_text,
    strip_html,
    to_naive_utc,
    tokenize,
    url_hash,
)


class TestCanonicalUrl:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("http://www.ft.com/a", "https://ft.com/a"),
            ("HTTPS://WWW.FT.COM/A", "https://ft.com/A"),          # host folds, path does not
            ("https://ft.com/a/", "https://ft.com/a"),             # trailing slash
            ("https://ft.com//a//b", "https://ft.com/a/b"),        # duplicate slashes
            ("https://ft.com/a/amp", "https://ft.com/a"),          # AMP suffix
            ("https://ft.com/a#section", "https://ft.com/a"),      # fragment
            ("https://ft.com:443/a", "https://ft.com/a"),          # default port
            ("ft.com/a", "https://ft.com/a"),                      # scheme inferred
        ],
    )
    def test_canonical_forms(self, raw, expected):
        assert canonical_url(raw) == expected

    def test_tracking_params_removed(self):
        noisy = "https://ft.com/a?utm_source=rss&fbclid=abc&gclid=x&ocid=y"
        assert canonical_url(noisy) == "https://ft.com/a"

    def test_meaningful_params_kept_and_sorted(self):
        assert canonical_url("https://ft.com/a?b=2&a=1") == "https://ft.com/a?a=1&b=2"

    def test_tracking_variants_collapse_to_one_identity(self):
        """The property the dedup layer actually depends on."""
        a = "https://www.ft.com/content/fed-holds-rates?utm_source=x"
        b = "https://ft.com/content/fed-holds-rates/?fbclid=zzz&utm_campaign=social"
        assert url_hash(canonical_url(a)) == url_hash(canonical_url(b))

    def test_distinct_articles_stay_distinct(self):
        assert canonical_url("https://ft.com/a") != canonical_url("https://ft.com/b")

    def test_malformed_input_does_not_raise(self):
        for bad in ("", "   ", "not a url", "http://[bad"):
            canonical_url(bad)  # must not raise


class TestStripHtml:
    def test_tags_removed_entities_decoded(self):
        assert strip_html("<p>Rates &amp; <b>bonds</b></p>") == "Rates & bonds"

    def test_script_and_style_content_dropped(self):
        html = "<div>Real<script>var x=1;</script><style>p{}</style>text</div>"
        assert "var x" not in strip_html(html)
        assert "p{}" not in strip_html(html)

    def test_none_and_empty(self):
        assert strip_html(None) == ""
        assert strip_html("") == ""


class TestNormaliseText:
    def test_accents_and_case_folded(self):
        assert normalise_text("Café RÉSUMÉ") == "cafe resume"

    def test_smart_punctuation_folded(self):
        assert normalise_text("“quote” – dash’s") == '"quote" - dash\'s'

    def test_whitespace_collapsed(self):
        assert normalise_text("a\n\t  b") == "a b"


class TestTokenize:
    def test_stopwords_dropped_by_default(self):
        assert "the" not in tokenize("the interest rates")

    def test_stopwords_kept_when_requested(self):
        assert "the" in tokenize("the interest rates", keep_stopwords=True)

    def test_multi_character_numbers_survive(self):
        assert "2026" in tokenize("the 2026 election")

    def test_single_characters_are_dropped(self):
        """Bare single characters are high-frequency and low-information.

        Precise figures are not lost - they are carried by the *entity* channel
        instead ("#6.4"), which is one of the reasons similarity combines two
        signals rather than relying on bag-of-words alone.
        """
        assert tokenize("magnitude 6 quake") == ["magnitude", "quake"]
        assert "#6.4" in extract_entities("Magnitude 6.4 quake", "Japan was hit.")


class TestContentHash:
    def test_insensitive_to_markup_and_case(self):
        assert content_hash("Title", "<b>Body</b> text") == content_hash(
            "TITLE", "Body   text"
        )

    def test_different_content_differs(self):
        assert content_hash("A", "one") != content_hash("A", "two")


class TestEntities:
    def test_proper_nouns_and_numbers_extracted(self):
        ents = extract_entities(
            "Magnitude 6.4 earthquake strikes Japan",
            "The Japan Meteorological Agency said on Friday.",
        )
        assert "japan" in ents
        assert "#6.4" in ents
        assert "japan_meteorological_agency" in ents  # multi-word form is near-unique

    def test_sentence_initial_words_are_not_entities(self):
        """Without this filter every article 'contains' the entity 'The'."""
        ents = extract_entities("A quiet day", "The market closed. However, it rose.")
        assert "the" not in ents
        assert "however" not in ents

    def test_jaccard_bounds(self):
        assert jaccard({"a"}, {"a"}) == 1.0
        assert jaccard({"a"}, {"b"}) == 0.0
        assert jaccard(set(), {"a"}) == 0.0


class TestClassifyTopics:
    def test_single_lexicon_hit_is_rejected_as_noise(self):
        """'Final results from the election' must not be classified as sport."""
        topics = classify_topics(tokenize("final results from the national election"))
        assert "sport" not in topics

    def test_strong_signal_classified(self):
        topics = classify_topics(
            tokenize("inflation rates markets economy bank investors")
        )
        assert "business" in topics

    def test_always_returns_something(self):
        assert classify_topics([]) == ["general"]


class TestTime:
    def test_aware_converted_to_naive_utc(self):
        aware = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        naive = to_naive_utc(aware)
        assert naive.tzinfo is None and naive.hour == 12

    def test_naive_passed_through(self):
        naive = datetime(2026, 1, 1, 12, 0)
        assert to_naive_utc(naive) == naive

    def test_iso_z_suffix(self):
        assert iso_z(datetime(2026, 1, 1)).endswith("Z")
        assert iso_z(None) is None


def test_domain_of():
    assert domain_of("https://www.bbc.co.uk/news") == "bbc.co.uk"
    assert domain_of("garbage") == ""


def test_detect_language():
    assert detect_language(tokenize("the quick brown fox is in the house",
                                    keep_stopwords=True)) == "en"
