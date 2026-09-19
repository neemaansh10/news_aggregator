"""Text, URL and time normalisation - the highest-leverage step in the system.

A canonicalisation bug surfaces downstream as thousands of phantom "new
stories", so everything here is a pure function and exhaustively unit-testable.

All timestamps are handled as **naive UTC**: SQLite silently drops tzinfo, and
normalising at the boundary removes an entire class of aware/naive comparison
bugs. ``Z`` is re-appended at the API edge.
"""

from __future__ import annotations

import hashlib
import html as html_lib
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Set
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Normalisation is the single highest-leverage step in the whole system: a
# canonicalisation bug shows up downstream as thousands of false "new stories".
# =============================================================================


def utcnow() -> datetime:
    """Naive UTC 'now'. See module docstring for why naive."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def iso_z(dt: Optional[datetime]) -> Optional[str]:
    return None if dt is None else dt.isoformat(timespec="seconds") + "Z"


# Query parameters that never change the identity of a document.
TRACKING_PARAMS: Set[str] = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_name", "utm_reader", "utm_brand", "utm_social",
    "fbclid", "gclid", "dclid", "msclkid", "igshid", "mc_cid", "mc_eid",
    "ref", "referrer", "source", "cmpid", "ncid", "smid", "partner",
    "sh", "spm", "at_medium", "at_campaign", "__twitter_impression",
    "CMP", "ito", "icid", "ocid", "sref", "guccounter", "amp",
}

_AMP_SUFFIX = re.compile(r"/(amp|amp\.html|amp/)$", re.IGNORECASE)
_MULTISLASH = re.compile(r"/{2,}")
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<(script|style)\b.*?</\1>", re.IGNORECASE | re.DOTALL)
_WS_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")

STOPWORDS: Set[str] = set(
    """
a an the and or but if then than that this these those of in on at to for with from by
as is are was were be been being it its it's he she they them his her their we you i
not no nor so such too very can will just don should now about after again against all
almost also am among any because before below between both during each few further
have has had having here how into more most only other our out over own same some
what when where which while who whom why would could may might must shall into up down
said says say new news report reports reported according told amid via update updates
""".split()
)


def strip_html(raw: Optional[str]) -> str:
    if not raw:
        return ""
    txt = _SCRIPT_RE.sub(" ", raw)
    txt = _TAG_RE.sub(" ", txt)
    txt = html_lib.unescape(txt)
    return _WS_RE.sub(" ", txt).strip()


def normalise_text(raw: Optional[str]) -> str:
    """Case-fold, strip accents and collapse whitespace."""
    if not raw:
        return ""
    txt = unicodedata.normalize("NFKD", raw)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = txt.replace("’", "'").replace("‘", "'")
    txt = txt.replace("“", '"').replace("”", '"')
    txt = txt.replace("–", "-").replace("—", "-")
    return _WS_RE.sub(" ", txt).strip().lower()


def tokenize(raw: Optional[str], keep_stopwords: bool = False) -> List[str]:
    if not raw:
        return []
    toks = _TOKEN_RE.findall(normalise_text(raw))
    if keep_stopwords:
        return toks
    return [t for t in toks if len(t) > 1 and t not in STOPWORDS]


def shingles(tokens: Sequence[str], n: int = 3) -> List[str]:
    if len(tokens) < n:
        return ["_".join(tokens)] if tokens else []
    return ["_".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


def canonical_url(raw: str) -> str:
    """
    Reduce a URL to a stable identity:
      * force https, lowercase host, drop 'www.' and default ports
      * collapse duplicate slashes, drop trailing slash and /amp suffixes
      * remove tracking parameters, sort the survivors, drop the fragment
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        p = urlparse(raw)
    except ValueError:
        return raw

    netloc = (p.hostname or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    try:
        port = p.port
    except ValueError:
        port = None
    if port and port not in (80, 443):
        netloc = f"{netloc}:{port}"

    path = _MULTISLASH.sub("/", p.path or "/")
    path = _AMP_SUFFIX.sub("", path)
    if len(path) > 1:
        path = path.rstrip("/")
    path = path or "/"

    params = [
        (k, v)
        for k, v in parse_qsl(p.query, keep_blank_values=False)
        if k.lower() not in {x.lower() for x in TRACKING_PARAMS}
    ]
    params.sort()
    return urlunparse(("https", netloc, path, "", urlencode(params), ""))


def url_hash(canon: str) -> str:
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:40]


def content_hash(title: str, body: str) -> str:
    """Exact-content identity, insensitive to markup, whitespace and case.

    HTML is stripped here as well as in the normaliser. That is deliberate
    belt-and-braces: this hash decides whether two articles are the same
    document, so a caller that forgets to strip first must not silently get a
    different identity for the same content. ``strip_html`` is idempotent, so
    the extra pass never changes the hash for the normal pipeline path.
    """
    payload = f"{normalise_text(strip_html(title))} {normalise_text(strip_html(body))[:8000]}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:40]


def domain_of(raw_url: str) -> str:
    try:
        host = (urlparse(raw_url).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


# -- Lightweight topic classifier ---------------------------------------------
# Deliberately a transparent lexicon rather than a model: it is explainable,
# needs no training data, and is trivially swappable for a real classifier
# behind the same `classify_topics()` interface.
TOPIC_LEXICON: Dict[str, Set[str]] = {
    "business": {"market", "markets", "stocks", "shares", "economy", "inflation",
                 "rates", "bank", "earnings", "revenue", "trade", "tariff",
                 "investors", "recession", "gdp", "currency", "bond", "ipo"},
    "technology": {"ai", "chip", "chips", "software", "startup", "semiconductor",
                   "app", "cloud", "data", "cyber", "robot", "quantum", "gpu",
                   "model", "platform", "device", "silicon", "algorithm"},
    "science": {"research", "study", "scientists", "space", "nasa", "climate",
                "telescope", "physics", "genome", "orbit", "moon", "mars",
                "discovery", "experiment", "species", "fossil"},
    "health": {"health", "disease", "virus", "vaccine", "patients", "hospital",
               "outbreak", "drug", "cancer", "clinical", "who", "infection"},
    "politics": {"election", "president", "parliament", "senate", "minister",
                 "vote", "votes", "campaign", "policy", "congress", "party",
                 "government", "bill", "coalition", "referendum"},
    "world": {"war", "conflict", "troops", "border", "refugees", "sanctions",
              "summit", "treaty", "diplomatic", "ceasefire", "strike", "un"},
    "sport": {"match", "goal", "tournament", "league", "champion", "olympic",
              "coach", "season", "final", "cup", "score", "player"},
    "disaster": {"earthquake", "magnitude", "hurricane", "flood", "wildfire",
                 "tsunami", "evacuated", "quake", "storm", "landslide", "eruption"},
}


def classify_topics(tokens: Sequence[str], limit: int = 2) -> List[str]:
    """
    Requires two lexicon hits before assigning a topic.  A single hit is almost
    always incidental - "final results from the election" matched `sport` on the
    word "final" - and noisy topics leak straight into the blocking key, which
    splits clusters that should have stayed together.
    """
    tset = set(tokens)
    scored = [(t, len(tset & words)) for t, words in TOPIC_LEXICON.items()]
    scored.sort(key=lambda x: -x[1])
    strong = [(t, n) for t, n in scored if n >= 2]
    if strong:
        return [t for t, _ in strong[:limit]]
    return [scored[0][0]] if scored and scored[0][1] > 0 else ["general"]


# -- Entity extraction ---------------------------------------------------------
# Two articles about the same event almost always share the *same proper nouns
# and the same numbers* ("Japan", "6.4", "Europa"), even when the reporters pick
# completely different verbs and framing.  That makes entity overlap a far more
# event-specific signal than bag-of-words similarity, and it is what rescues
# cases like "Chipmaker unveils accelerator" vs "AI chip race intensifies".
#
# This is a capitalisation + numeral heuristic, not a trained NER model.  It is
# deliberately transparent and dependency-free; swapping in spaCy or a
# transformer NER behind `extract_entities()` requires no other change.

_CAP_SEQUENCE = re.compile(
    r"\b[A-Z][a-zA-Z]{2,}(?:\s+(?:of|the|de|and|for|und)?\s*[A-Z][a-zA-Z]{2,}){0,3}\b"
)
_ACRONYM = re.compile(r"\b[A-Z]{2,6}\b")
_NUMERIC = re.compile(r"\b\d+(?:[.,]\d+)?\b")
_ENTITY_GLUE = {"of", "the", "de", "and", "for", "und"}

# Words that are capitalised only because they start a sentence, plus generic
# news nouns.  Without this filter every article "contains" the entity "The".
_ENTITY_NOISE: Set[str] = {
    "the", "this", "that", "these", "those", "there", "their", "they", "them",
    "but", "and", "for", "however", "meanwhile", "according", "officials",
    "authorities", "researchers", "scientists", "analysts", "experts",
    "reuters", "associated", "press", "reporting", "editing", "additional",
    "monday", "sunday", "saturday", "week", "year", "years", "people",
    "government", "company", "companies", "markets", "market", "shares",
    "stocks", "data", "news", "report", "reports", "study", "new", "one",
    "two", "three", "first", "last", "next", "other", "more", "most", "some",
    "it", "its", "he", "she", "we", "you", "his", "her",
}


def extract_entities(title: str, body: str, limit: int = 40) -> Set[str]:
    """Proper nouns, acronyms and salient numbers, normalised to lowercase."""
    found: Set[str] = set()
    for text in (title or "", (body or "")[:2000]):
        for match in _CAP_SEQUENCE.findall(text):
            words = [w for w in match.split() if w.lower() not in _ENTITY_GLUE]
            for word in words:
                lower = word.lower()
                if (
                    len(lower) > 2
                    and lower not in _ENTITY_NOISE
                    and lower not in STOPWORDS
                ):
                    found.add(lower)
            if len(words) > 1:
                # The multi-word form is the strongest signal of all:
                # "japan_meteorological_agency" is effectively unique.
                found.add("_".join(w.lower() for w in words))
        for acronym in _ACRONYM.findall(text):
            if acronym.lower() not in _ENTITY_NOISE:
                found.add(acronym.lower())
        for number in _NUMERIC.findall(text):
            # Bare small integers are noise; magnitudes and counts are not.
            if len(number) > 1 or number not in "0123456789":
                found.add("#" + number.replace(",", "."))
    return set(sorted(found)[:limit])


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def detect_language(tokens: Sequence[str]) -> str:
    """
    Cheap English detector via stopword ratio.  Real deployments should use
    fastText lid.176 or CLD3; the interface is what matters here because
    language is a hard *blocking* key - we never cluster across languages.
    """
    if not tokens:
        return "und"
    hits = sum(1 for t in tokens[:120] if t in STOPWORDS)
    return "en" if hits / min(len(tokens), 120) > 0.06 else "und"
