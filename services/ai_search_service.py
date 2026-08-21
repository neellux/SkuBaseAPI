"""Verify a listing's MPN, brand colour, title and description against the web.

What an operator does by hand today: press the MPN field's search menu, read a
Google result, retype the value. This does the same lookup in one call, using the
same brand / MPN / colour / style combinations that menu offers, the product
photographs, and the tag text PhotoManagement already transcribed.

Measured against corrections operators really made (feeding the model
`original_data` and scoring against `data`): 11/11 exact on manufacturer_sku,
8/10 on brand_color. The evidence is almost always the tag rather than the web,
which is why the tag text is fed in explicitly and why the prompt tells the model
it outranks any retailer.

SUGGESTIONS ONLY. Nothing here is ever written to `listings.data`.

Auth. Two paths, not priced alike:
  apikey  `[ai] gemini_api_key`. $14 per 1,000 grounded prompts, first 5,000 a
          month free across Gemini 3.x.
  vertex  service-account.json against project 433271307736. $35 per 1,000, and
          no free tier for Gemini 3.x, so a silent fallback is a silent 2.5x.
"apikey" is tried first and every fallback is logged at WARNING.
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, quote_plus, urlparse

import httpx
from config import config
from exceptions.ai_search_exceptions import (
    PermanentAISearchError,
    TransientAISearchError,
    classify,
)
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

# Set once at import, exactly as ai_service.py does. The prototype set it per
# call, which mutates process-wide env from whatever task happens to be running.
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")

SCHEMA_VERSION = 1

try:
    _AI = config.get("ai", {})
    GEMINI_API_KEY = _AI.get("gemini_api_key")
    AI_SEARCH_MODEL = _AI.get("ai_search_model", "gemini-3.5-flash-lite")
    AI_SEARCH_AUTH = _AI.get("ai_search_auth", "auto")
    AI_SEARCH_TEMPERATURE = float(_AI.get("ai_search_temperature", 0.2))
    AI_SEARCH_MAX_IMAGES = int(_AI.get("ai_search_max_images", 8))
    AI_SEARCH_CHECK_URLS = bool(_AI.get("ai_search_check_urls", True))
except Exception as e:  # pragma: no cover - config shape is fixed
    logger.error(f"Error loading verification config: {e}")
    GEMINI_API_KEY = None
    AI_SEARCH_MODEL = "gemini-3.5-flash-lite"
    AI_SEARCH_AUTH = "auto"
    AI_SEARCH_TEMPERATURE = 0.2
    AI_SEARCH_MAX_IMAGES = 8
    AI_SEARCH_CHECK_URLS = True

VERTEX_PROJECT = "433271307736"
# Grounding is served from the global endpoint, not us-central1.
VERTEX_LOCATION = "global"

# Published list prices, read off ai.google.dev and cloud.google.com on
# 2026-08-11. Grounding is billed per grounded prompt, not per search query the
# model fires inside it, so a run that searched 14 times costs the same as one
# that searched once. It also dominates: tokens are ~$0.008 against $0.014.
PRICE_PER_M_INPUT = 0.30
PRICE_PER_M_OUTPUT = 2.50
GROUNDING_PRICE = {"apikey": 14.0 / 1000, "vertex": 35.0 / 1000}

# In-call retries are deliberately few. A worker sleeping inside a retry holds
# one of only three slots; anything longer than a blip belongs back on the queue
# with a next_attempt_at, where it costs no worker time at all.
MAX_INLINE_ATTEMPTS = 2
INLINE_BACKOFF_SECONDS = 5.0

# The model's vocabulary on the left, this app's field names on the right. The
# mapping is applied once, at write time, so nothing downstream ever sees "mpn".
FIELD_TO_LISTING_KEY = {
    "mpn": "manufacturer_sku",
    "brand": "brand_name",
    "brand_color": "brand_color",
    "color": "standard_color",
    "title": "title",
    # Derived rather than answered: _material_from_sources builds this verdict
    # from what the pages published, so nothing arrives under a "material" key.
    # It stays listed because the fingerprint and the surfaced set are keyed off
    # these values, and an edit to the material field should still go stale.
    "material": "material",
    "description": "description",
}

# What the UI surfaces. Widening this is a one-line change with no migration and
# no re-run, because every field above is asked for and stored regardless.
SURFACED_FIELDS = ("manufacturer_sku", "brand_color", "material", "title", "description")

# description's verified_value is advisory prose ("all features supported", or
# the contradicted features), never a replacement value, so Apply must be
# impossible for it. title is informational too: consignment tags carry junk
# product names and the title is template-generated anyway.
NON_APPLICABLE_FIELDS = frozenset({"description", "title"})

_PROMPT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "prompts")

try:
    with open(os.path.join(_PROMPT_DIR, "ai_search_system_prompt.txt")) as f:
        SYSTEM_PROMPT = f.read()
except OSError as e:  # pragma: no cover
    logger.error(f"Could not load verification system prompt: {e}")
    SYSTEM_PROMPT = ""

# The second, ungrounded call's own instructions. Deliberately not part of the
# search prompt: see extract_source_materials.
try:
    with open(os.path.join(_PROMPT_DIR, "ai_search_material_prompt.txt")) as f:
        MATERIAL_PROMPT = f.read()
except OSError as e:  # pragma: no cover
    logger.error(f"Could not load the material prompt: {e}")
    MATERIAL_PROMPT = ""


# The shape PhotoManagementNew's washtag pass already produces
# (utils/washtag_ai_analyzer.py: Material / MaterialComponent / MaterialItem),
# reused verbatim so the search and the tag reading speak one vocabulary and both
# go through format_material below. percentage is a string rather than a nullable
# integer: response_json_schema is happier with it, and "" reads the same as the
# rest of this schema's "empty string if the page does not state one".
MATERIAL_SCHEMA = {
    "type": "object",
    "description": (
        "Fibre composition, structured. The field's exact wording is assembled from "
        "this, so give the parts and not a sentence."
    ),
    "properties": {
        "components": {
            "type": "array",
            "description": (
                "Empty array when the page states no composition. Never calculate a "
                "percentage that is missing and never name a fibre that is not stated."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "component_name": {
                        "type": "string",
                        "description": (
                            "Lowercase. A single or unlabelled section, and anything the "
                            "page calls self, main or fabric, is 'shell'; otherwise keep "
                            "what is printed: lining, trim, filling, body, contrast, rib. "
                            "Footwear has three: upper, lining, outer sole."
                        ),
                    },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": (
                                        "Fibre in lowercase English, translated from whatever "
                                        "the page prints: coton -> cotton, laine -> wool, "
                                        "poliammide -> polyamide, elasthanne -> elastane. "
                                        "Expand ISO codes carrying a percentage: CO cotton, "
                                        "PL/PES polyester, WO/WV wool, PA/NY polyamide, "
                                        "EL/EA/SP elastane, VI/CV viscose, LI linen, AC "
                                        "acrylic, CA acetate, LY/CLY lyocell, MD/CMD modal, "
                                        "SE silk, RA ramie. Footwear fibres are leather, "
                                        "coated leather, textile, textiles or synthetics, "
                                        "other materials. Blends are separate items in the "
                                        "same component."
                                    ),
                                },
                                "percentage": {
                                    "type": "string",
                                    "description": (
                                        "Digits only, e.g. \"55\", adding up to 100 within a "
                                        "component. Empty string where the page states none, "
                                        "which is usual for footwear, bags and accessories "
                                        "and is a complete answer rather than a partial one."
                                    ),
                                },
                            },
                            "required": ["name", "percentage"],
                        },
                    },
                },
                "required": ["component_name", "items"],
            },
        }
    },
    "required": ["components"],
}

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "sources": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_name": {
                        "type": "string",
                        "description": "Retailer or site name, e.g. Farfetch, SSENSE",
                    },
                    "title": {"type": "string", "description": "Exact product title as published"},
                    "mpn": {
                        "type": "string",
                        "description": "Manufacturer part number / style code, exactly as printed. Empty string if the page does not state one.",
                    },
                    "brand": {"type": "string"},
                    "sku": {
                        "type": "string",
                        "description": "The site's own SKU or product ID, empty string if none",
                    },
                    "brand_color": {
                        "type": "string",
                        "description": "The brand's own colour name, e.g. Military. Empty string if the page does not state one.",
                    },
                    "color": {"type": "string", "description": "Plain generic colour, e.g. Green"},
                    "url": {"type": "string", "description": "Direct URL to the product page"},
                    "description": {
                        "type": "string",
                        "description": (
                            "The product description this page publishes, copied verbatim as "
                            "plain text. Keep the construction and material detail; drop "
                            "shipping, returns, sizing-chart and marketing boilerplate. Empty "
                            "string if the page has no description."
                        ),
                    },
                    "image_match": {
                        "type": "string",
                        "enum": ["same_product", "same_style_other_colour", "similar_only", "unknown"],
                        "description": "How the source matches the supplied photographs",
                    },
                    "confidence": {"type": "number", "description": "0 to 1"},
                },
                "required": [
                    "source_name",
                    "title",
                    "mpn",
                    "brand",
                    "sku",
                    "brand_color",
                    "color",
                    "url",
                    "description",
                    "image_match",
                    "confidence",
                ],
            },
        },
        "verdict": {
            "type": "object",
            "properties": {
                "mpn": {"$ref": "#/$defs/field_verdict"},
                "brand": {"$ref": "#/$defs/field_verdict"},
                "brand_color": {"$ref": "#/$defs/field_verdict"},
                "color": {"$ref": "#/$defs/field_verdict"},
                "title": {"$ref": "#/$defs/field_verdict"},
                "description": {"$ref": "#/$defs/field_verdict"},
            },
            "required": [
                "mpn",
                "brand",
                "brand_color",
                "color",
                "title",
                "description",
            ],
        },
        "label": {
            "type": "object",
            "description": (
                "What the washtag / care label / bag sticker says. Fill this from the tag "
                "text alone, whether or not the web search finds anything: on many items it "
                "is the only evidence there is."
            ),
            "properties": {
                "brand": {
                    "type": "string",
                    "description": "Brand exactly as printed, e.g. AMIRI. Empty string if the tag does not name one.",
                },
                "title": {
                    "type": "string",
                    "description": "The product name printed on the tag, e.g. COTTON WOVEN PANT. Empty string if the tag does not name one.",
                },
                "mpn": {
                    "type": "string",
                    "description": "Style code and colourway code joined with a single underscore, SIZE SUFFIX DROPPED: 'AW23MKB005 100' -> AW23MKB005_100, 'M5056-CMIW424-29' -> M5056_CMIW424.",
                },
                "sku": {
                    "type": "string",
                    "description": "The full code exactly as printed, character for character, size suffix included: M5056-CMIW424-29. Do not join, normalise or drop anything here.",
                },
                "colour_name": {
                    "type": "string",
                    "description": "The colourway name as printed, e.g. WHITE, MILITARY",
                },
                "upc": {"type": "string", "description": "UPC / barcode digits only, spaces removed"},
                "size": {"type": "string", "description": "Size as printed, e.g. M, 29"},
                "other_codes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Lot, RN, CA and any other codes, verbatim",
                },
                "is_manufacturer_tag": {
                    "type": "boolean",
                    "description": "False when the tag is a consignment or resale note rather than the manufacturer's own label",
                },
            },
            "required": [
                "brand",
                "title",
                "mpn",
                "sku",
                "colour_name",
                "upc",
                "size",
                "other_codes",
                "is_manufacturer_tag",
            ],
        },
        "notes": {
            "type": "string",
            "description": "Anything the operator should know: conflicting codes, no direct match found, only resale listings, and so on",
        },
    },
    "required": ["sources", "verdict", "label", "notes"],
    "$defs": {
        "field_verdict": {
            "type": "object",
            "properties": {
                "listing_value": {"type": "string"},
                "verified_value": {
                    "type": "string",
                    "description": (
                        "What the field should be on the evidence. The tag's wording wins "
                        "whenever the tag states it; otherwise the value the sources agree "
                        "on. Empty string only if neither the tag nor any source states it."
                    ),
                },
                "evidence": {
                    "type": "string",
                    "enum": ["label", "web", "both", "none"],
                    "description": "Where verified_value came from",
                },
                "status": {"type": "string", "enum": ["confirmed", "conflict", "not_found"]},
                "agreeing_sources": {"type": "integer"},
                "reason": {"type": "string"},
            },
            "required": [
                "listing_value",
                "verified_value",
                "evidence",
                "status",
                "agreeing_sources",
                "reason",
            ],
        }
    },
}

# Grounded answers often report the citation redirect rather than the page it
# points at. Left alone, every source's domain reads as this one Google host and
# the site:-scoped search becomes nonsense, so these are resolved to the page
# they land on before anything is derived from them.
GROUNDING_REDIRECT_HOST = "vertexaisearch.cloud.google.com"


def is_grounding_redirect(url: str) -> bool:
    return GROUNDING_REDIRECT_HOST in (url or "")


# Stored source descriptions and notes are truncated so a pathological answer
# cannot write a megabyte row into a column that is read on every listing open.
MAX_SOURCE_DESCRIPTION = 1200
MAX_NOTES = 2000


def is_configured() -> bool:
    """False when neither auth path can work, so callers can 503 rather than fail."""
    if AI_SEARCH_AUTH == "apikey":
        return bool(GEMINI_API_KEY)
    if AI_SEARCH_AUTH == "vertex":
        return os.path.exists(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))
    return bool(GEMINI_API_KEY) or os.path.exists(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    )


def is_washtag(url: str) -> bool:
    return "washtag" in (url or "").lower()


def domain_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower().removeprefix("www.")
    except ValueError:
        return ""


def strip_html(text: Any) -> str:
    """Flatten the listing's stored HTML description into readable lines.

    `description` is stored as an HTML bullet list, and handing the model raw
    <ul><li><p> markup buries the features it is meant to check.
    """
    if not text:
        return ""
    out = re.sub(r"<li[^>]*>", "\n- ", str(text))
    out = re.sub(r"<br\s*/?>", "\n", out)
    out = re.sub(r"<[^>]+>", "", out)
    out = unescape(out)
    return "\n".join(line.strip() for line in out.splitlines() if line.strip())


def build_search_options(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    """The same five combinations ListingView's MPN search menu offers.

    Mirrors ProductSearchWidget in UI/src/components/ListingView.jsx: colour
    falls back to standard_color when brand_color is empty, and a narrowed
    combination is only offered once every part it names is filled in, so a
    listing with no brand does not quietly collapse "Brand + MPN" into a bare MPN
    search. The two must stay identical -- an operator comparing the suggestion
    against what the menu finds has to be running the same query.
    """

    def clean(part: Any) -> str:
        return str(part or "").strip()

    def join(*parts: Any) -> str:
        return " ".join(p for p in (clean(x) for x in parts) if p)

    def complete(*parts: Any) -> bool:
        return all(clean(p) != "" for p in parts)

    brand = fields.get("brand_name")
    color = fields.get("brand_color") or fields.get("standard_color")
    style = fields.get("style_name")
    mpn = fields.get("manufacturer_sku")

    options = [
        ("All details", join(brand, mpn, color, style)),
        ("MPN", join(mpn) if complete(mpn) else ""),
        ("Brand + MPN", join(brand, mpn) if complete(brand, mpn) else ""),
        ("Style + MPN", join(style, mpn) if complete(style, mpn) else ""),
        ("Brand + Style", join(brand, style) if complete(brand, style) else ""),
    ]
    return [(label, query) for label, query in options if query]


async def read_tag_text(product_id: str) -> Optional[Dict[str, Any]]:
    """The tag text PhotoManagement already transcribed, from the photography DB.

    PhotoManagementNew runs its own washtag pass and stores the exact,
    unprocessed text per tag image in washtagdata.washtag_data.tag_texts[].text,
    along with a readability_status and the parsed material and country. Every
    code this service needs is already in there, so re-OCRing the same images
    with a second model would pay twice for one answer and let the two drift.

    Returns None when there is no row (~6% of listings), and the caller falls
    back to sending the washtag photographs, which is the benchmarked path.
    """
    from tortoise import connections

    try:
        conn = connections.get("photography_db")
        rows = await conn.execute_query_dict(
            "SELECT washtag_data FROM washtagdata WHERE product_id = $1 "
            "ORDER BY created_at DESC LIMIT 1",
            [product_id],
        )
    except Exception as e:
        # The photography DB being unreachable must not fail an AI search: the
        # image fallback still produces a correct answer, just more expensively.
        logger.warning(f"Could not read tag text for {product_id}: {type(e).__name__}: {e}")
        return None

    if not rows:
        return None
    data = rows[0].get("washtag_data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None
    if not isinstance(data, dict):
        return None

    texts = []
    for entry in data.get("tag_texts") or []:
        text = (entry or {}).get("text")
        if text and str(text).strip():
            texts.append(
                {
                    "text": str(text).strip(),
                    "readability": (entry or {}).get("readability_status") or "",
                }
            )
    if not texts:
        return None
    return {
        "tag_texts": texts,
        "country_of_origin": data.get("country_of_origin"),
        "material": data.get("material"),
    }


# PhotoManagement stores every fibre and component lowercase; this field is Title
# Case. "or" stays down because its footwear vocabulary includes "textiles or
# synthetics", and str.title() would render that "Textiles Or Synthetics".
_LOWER_IN_TITLE = frozenset({"or", "and"})


def _same_text(a: Any, b: Any) -> bool:
    """Equal ignoring case and whitespace runs, newlines included."""
    return " ".join(str(a or "").split()).lower() == " ".join(str(b or "").split()).lower()


def _title_case(text: str) -> str:
    return " ".join(
        word.capitalize() if i == 0 or word.lower() not in _LOWER_IN_TITLE else word.lower()
        for i, word in enumerate(text.split())
    )


def format_material(composition: Optional[Dict[str, Any]]) -> str:
    """Render a parsed composition the way this app's material field is written.

    The one place the wording is decided, for the model's answer and for
    PhotoManagement's washtag parse alike -- they share a schema precisely so
    they can share this. Leaving the punctuation to the prompt would mean the
    search's "Shell: 100% Cotton" and the tag pass's could drift a comma apart on
    the same garment, and there would be nothing to catch it.

    The convention is the one the 3,803 filled listings already use: a single
    component drops the prefix ("100% Cotton"), several get one prefixed line
    each. Those newlines are load-bearing -- the SellerCloud and 1nventory
    description templates split on them to make one <div> per component -- so a
    collapsed value ships a run-on line to the storefront.

    An empty percentage is not a parse failure: footwear, bags and most
    accessories publish the fibre with no share at all ("Upper: Leather").
    """
    lines = []
    for component in (composition or {}).get("components") or []:
        parts = []
        for item in component.get("items") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            share = str(item.get("percentage") or "").strip()
            parts.append(f"{share}% {_title_case(name)}" if share.isdigit() else _title_case(name))
        if parts:
            lines.append(
                (_title_case(str(component.get("component_name") or "").strip()), ", ".join(parts))
            )
    if not lines:
        return ""
    if len(lines) == 1:
        return lines[0][1]
    return "\n".join(f"{name}: {value}" if name else value for name, value in lines)


# Everything that is not a letter or a digit, so a value the tag prints as
# "AW23MKB005 100" still matches an mpn joined with an underscore, and
# "M5056-CMIW424-29" still matches one with the size dropped.
_NOT_ALNUM = re.compile(r"[^A-Z0-9]+")

# The label keys that are meant to be a transcription and can therefore be
# checked character by character. is_manufacturer_tag is a judgement, not a
# quotation, and other_codes is checked element by element.
_TRANSCRIBED_LABEL_KEYS = ("brand", "title", "mpn", "sku", "colour_name", "upc", "size")

# Below this a needle matches by accident: size "M" is in almost any text.
_MIN_CHECKABLE = 3


def _printed(value: Any, haystack: str) -> bool:
    needle = _NOT_ALNUM.sub("", str(value or "").upper())
    if len(needle) < _MIN_CHECKABLE:
        # Too short to check either way, so it is left as the model gave it.
        return True
    return needle in haystack


def drop_unprinted_label_values(
    label: Dict[str, Any], tag_text: Optional[Dict[str, Any]]
) -> List[str]:
    """Blank anything in the label block that the tag does not actually print.

    The model fills this block from the web and from the listing's own fields
    even when the prompt forbids it. AMR-MACC-0176's care label carries a brand,
    a size and an RN number and no style code whatsoever, and it still returned
    mpn SS23MAH014_420 (the stockists' code) and sku SS23MAH014_BLUE (ours).
    The UI renders those rows as "MPN on tag", which turns a borrowed value into
    a claim about the physical item -- the one claim an operator cannot check
    without walking to the shelf, and the one they are most entitled to trust.

    Only runs where the transcription exists, which is the only ground truth
    there is. On the ~6% with no washtagdata row the photographs went to the
    model instead and there is nothing to check against, so the block stands.

    Returns the keys it cleared, for the log and the diagnostics.
    """
    if not tag_text:
        return []
    haystack = _NOT_ALNUM.sub(
        "",
        " ".join(t.get("text") or "" for t in tag_text.get("tag_texts") or []).upper(),
    )
    if not haystack:
        return []

    dropped = []
    for key in _TRANSCRIBED_LABEL_KEYS:
        if label.get(key) and not _printed(label[key], haystack):
            dropped.append(key)
            label[key] = ""
    codes = label.get("other_codes")
    if isinstance(codes, list):
        kept = [c for c in codes if _printed(c, haystack)]
        if len(kept) != len(codes):
            dropped.append("other_codes")
            label["other_codes"] = kept
    return dropped


# A code worth searching: mixed letters and digits, long enough not to be a size
# or a care symbol. Matches PS23MAB005, AW23MKB005, MPVCK966, M5056.
_CODE_TOKEN = re.compile(r"\b(?=[A-Z0-9]*[A-Z])(?=[A-Z0-9]*\d)[A-Z0-9]{5,}\b")
# A style code followed by its colourway code, as tags print them.
_CODE_PLUS_COLOURWAY = re.compile(
    r"\b((?=[A-Z0-9]*[A-Z])(?=[A-Z0-9]*\d)[A-Z0-9]{5,})[ \-_/|]+(\d{2,4})\b"
)
_UPC = re.compile(r"\b\d[\d ]{10,16}\d\b")


def tag_derived_queries(tag_text: Optional[Dict[str, Any]], fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Search phrasings built from what the TAG says, not from our own fields.

    build_search_options mirrors the MPN menu in ListingView and seeds every
    query from listings.data. That is the right thing for a menu an operator
    drives, and the wrong thing here: when the stored MPN is in this app's own
    format the queries go looking for a string no retailer has ever published.
    Measured on AMR-MACC-0211, four of the five seeded phrasings were built
    around PS23MAB005_CAROLINA_BLUE and found nothing, while the manufacturer's
    own PS23MAB005 450 sat unused in the tag text.

    So the codes actually printed on the garment get their own queries. Written
    both joined and split because retailers disagree (PS23MAB005-450,
    PS23MAB005 450, PS23MAB005450), and the UPC gets one of its own: it is the
    single most selective identifier a tag carries.
    """
    if not tag_text:
        return []
    blob = "\n".join(entry["text"] for entry in tag_text.get("tag_texts") or [])
    if not blob:
        return []
    upper = blob.upper()
    brand = str(fields.get("brand_name") or "").strip()

    options: List[Tuple[str, str]] = []
    seen = set()

    def add(label: str, query: str) -> None:
        query = " ".join(query.split())
        key = query.lower()
        if query and key not in seen:
            seen.add(key)
            options.append((label, query))

    # Style + colourway, the pairing that identifies the exact product.
    for style, colourway in _CODE_PLUS_COLOURWAY.findall(upper)[:3]:
        add("Tag code", f"{style}-{colourway}")
        add("Tag code", f"{style} {colourway}")
        if brand:
            add("Brand + tag code", f"{brand} {style} {colourway}")

    # Bare style codes, for when the colourway is written somewhere else.
    for code in list(dict.fromkeys(_CODE_TOKEN.findall(upper)))[:4]:
        if brand:
            add("Brand + style code", f"{brand} {code}")
        else:
            add("Style code", code)

    # A UPC names one product and nothing else, and the barcode databases index
    # them, so it is the highest-yield single query on the tag.
    for raw in _UPC.findall(blob)[:2]:
        digits = raw.replace(" ", "")
        if 11 <= len(digits) <= 14:
            add("UPC", digits)

    return options


GCS_PRODUCTS_BUCKET = "https://storage.googleapis.com/lux_products"


async def product_images_from_db(product_id: str) -> Optional[List[str]]:
    """Image URLs from photography.productimages instead of probing GCS.

    sellercloud_service.get_product_images finds images by HEADing every slot in
    the bucket, which is authoritative but slow: measured at ~4.3s per product,
    about a fifth of a whole AI search, for information the photography app
    already recorded when it uploaded them. Reading the table instead is ~0.2s,
    and listing creation pays the same cost again for the aspects call.

    Resolution is PER SECTION, not per row, which is the part that is easy to get
    wrong. A product accumulates several rows, and washtags only ever appear on
    batch_creation ones (3,127 of 3,209 carry them, against 1 of 24,626 'manual'
    rows). A re-shoot writes a fresh 'manual' row with no washtags, so taking the
    newest row alone silently drops the tag - and the tag is the evidence this
    whole feature rests on. Images therefore come from the newest row that has
    any, washtags from the newest batch_creation row that has any.

    Returns None when the product has no rows at all, so the caller can fall back
    to probing.

    Known imprecision: the table can name a blob that was never uploaded (2 of 18
    products sampled claimed a 9th image GCS did not have). Harmless here because
    fetch_images skips a download that fails, which is also why this is used for
    verification and NOT swapped into get_product_images -- a phantom URL in the
    listing gallery would render as a broken image.
    """
    from services.product_resolver import SkuResolutionError, resolve_parent
    from tortoise import connections

    try:
        parent = await resolve_parent(product_id)
    except SkuResolutionError as e:
        # A SKU with no registered parent is a real data condition, not a bug:
        # fall back to probing. Deliberately NOT a bare `except Exception` -- one
        # of those here swallowed a missing import as though it were an
        # unresolvable SKU, and the fallback made it invisible.
        logger.warning(f"No registered parent for {product_id}: {e}")
        return None

    try:
        rows = await connections.get("photography_db").execute_query_dict(
            """
            SELECT product_images_count, washtag_count, image_source
            FROM productimages
            WHERE product_id = $1
            ORDER BY created_at DESC
            """,
            [parent],
        )
    except Exception as e:
        logger.warning(f"Could not read productimages for {parent}: {type(e).__name__}: {e}")
        return None

    if not rows:
        return None

    image_count = next((r["product_images_count"] for r in rows if (r["product_images_count"] or 0) > 0), 0)
    washtag_count = next(
        (
            r["washtag_count"]
            for r in rows
            if (r["washtag_count"] or 0) > 0 and r["image_source"] == "batch_creation"
        ),
        0,
    )
    # Fall back to any row for washtags: the source labels are a strong
    # convention rather than a constraint, and one 'manual' row does carry them.
    if not washtag_count:
        washtag_count = next((r["washtag_count"] for r in rows if (r["washtag_count"] or 0) > 0), 0)

    if not image_count and not washtag_count:
        return None

    prefix = f"{GCS_PRODUCTS_BUCKET}/{quote(parent, safe='/')}"
    urls = [f"{prefix}/{i}_1500.jpg" for i in range(1, (image_count or 0) + 1)]
    urls += [f"{prefix}/washtag_{i}.jpg" for i in range(1, (washtag_count or 0) + 1)]
    return urls


def select_images(urls: List[str], limit: int, include_washtags: bool) -> List[str]:
    """Pick which photographs to send.

    With tag text in hand the washtags carry nothing the prompt does not already
    have, so they are dropped and the budget goes to product shots. Without it,
    they claim their slots first: get_product_images returns product shots then
    washtags, so a plain urls[:limit] would drop exactly the images worth sending.
    """
    products = [u for u in urls if not is_washtag(u)]
    if not include_washtags:
        return products[:limit]
    washtags = [u for u in urls if is_washtag(u)]
    keep_washtags = washtags[:limit]
    keep_products = products[: max(0, limit - len(keep_washtags))]
    return keep_products + keep_washtags


async def fetch_images(urls: List[str]) -> List[Tuple[str, bytes]]:
    """Download the photographs as bytes.

    The Gemini API key path only accepts Files API or YouTube URIs, so an
    arbitrary https link cannot be passed by reference the way ai_service does
    with gs:// on Vertex. Downloading keeps both auth paths on one code path.

    Deliberately not downscaled. ai_service._process_image_url thumbnails to
    1024px and reusing it would cut cost, but resolution is what carries a style
    code off a tag, and the benchmark was measured at full size.
    """
    out: List[Tuple[str, bytes]] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:

        async def get(url: str):
            try:
                r = await client.get(url)
                r.raise_for_status()
                return url, r.content
            except Exception as e:
                logger.warning(f"Skipped image {url}: {type(e).__name__}: {e}")
                return url, None

        for url, content in await asyncio.gather(*(get(u) for u in urls)):
            if content:
                out.append((url, content))
    return out


def own_label_markers(listing_fields: Dict[str, Any], product_ids: List[str]) -> List[str]:
    """Strings that identify OUR barcode label rather than the maker's tag.

    The photographs include the label this warehouse prints and sticks on the
    bag, and it carries our SKU and our current MPN. Left unmarked the model
    reads it as manufacturer evidence and "confirms" our value against our own
    sticker, which is circular: it can never catch the error it is there to
    catch. Measured on AMR-MACC-0211, that is exactly what happened -- it
    returned PS23MAB005_CAROLINA_BLUE, our format, while the maker's
    PS23MAB005 450 was printed two lines above it.

    These are values we already know, so pointing them out is deterministic
    rather than a guess about which line looks internal.
    """
    markers = [str(p).strip() for p in product_ids if str(p or "").strip()]
    mpn = str(listing_fields.get("manufacturer_sku") or "").strip()
    if mpn:
        markers.append(mpn)
    # The bare parent, since the label prints SKU/SIZE.
    markers.extend([m.split("/")[0] for m in markers if "/" in m])
    return list(dict.fromkeys(markers))


def build_prompt(
    fields: Dict[str, Any],
    options: List[Tuple[str, str]],
    tag_text: Optional[Dict[str, Any]],
    image_urls: List[str],
    tag_options: Optional[List[Tuple[str, str]]] = None,
    own_markers: Optional[List[str]] = None,
) -> str:
    listing_block = "\n".join(
        f"  {k}: {fields.get(k) or '(empty)'}"
        for k in (
            "title",
            "brand_name",
            "manufacturer_sku",
            "brand_color",
            "standard_color",
            "style_name",
            "product_type",
            "material",
            "GENDER",
        )
    )
    description = strip_html(fields.get("description"))
    if description:
        listing_block += "\n  description:\n" + "\n".join(
            f"    {line}" for line in description.splitlines()
        )

    # Tag-derived queries go FIRST. They are built from what the maker printed,
    # while the ones below are built from our own stored fields and are only as
    # good as the value being checked.
    all_options = list(tag_options or []) + list(options)
    options_block = "\n".join(
        f"  {i + 1}. {label}: {query}" for i, (label, query) in enumerate(all_options)
    )

    markers_block = ""
    if own_markers:
        markers_block = (
            "\nOur own barcode label is stuck on this item and appears in the tag\n"
            "photographs alongside the maker's. Any line containing one of these is\n"
            "OURS, not the manufacturer's, and is not evidence of anything -- it is a\n"
            "copy of the value you are being asked to check:\n"
            + "\n".join(f"    {m}" for m in own_markers)
            + "\nRead the maker's own printing instead, even where the two disagree.\n"
        )

    if tag_text:
        lines = []
        for i, entry in enumerate(tag_text["tag_texts"], 1):
            suffix = (
                f"  [readability: {entry['readability']}]"
                if entry["readability"] and entry["readability"] != "complete"
                else ""
            )
            lines.append(f"  Tag {i}:{suffix}\n    " + entry["text"].replace("\n", "\n    "))
        tag_block = (
            "\nText already transcribed from this item's washtag / care label /\n"
            "bag sticker. This is the manufacturer's own wording and outranks any\n"
            "retailer. Read every code out of it before you search:\n\n"
            + "\n".join(lines)
            + "\n"
        )
    else:
        washtags = [u for u in image_urls if is_washtag(u)]
        tag_block = (
            f"\nNo transcribed tag text is available, so the last {len(washtags)} photograph(s)\n"
            "are washtag / care-label / bag-sticker shots. Read every code off them\n"
            "before you search.\n"
            if washtags
            else "\nNo tag text and no tag photographs are available for this item.\n"
        )

    return f"""Identify this product and verify the listing's fields against the web.

What the listing records:
{listing_block}
{tag_block}{markers_block}
Search phrasings to work through, best first. The "Tag code" and "UPC" ones are
built from what the maker printed on the garment and are the ones most likely to
find a real retailer page; the rest are built from our own stored fields and are
only as good as the value being checked. Run your own variations too:
{options_block}

{len(image_urls)} photograph(s) of the actual item follow.

Return the top 5 sources with the exact title, mpn, brand, sku, brand_color,
color, url and description each one publishes, the label block read off the tag,
then a per-field verdict comparing the listing's values against the evidence."""


def _make_client(auth: str):
    if auth == "vertex":
        return genai.Client(vertexai=True, project=VERTEX_PROJECT, location=VERTEX_LOCATION)
    if not GEMINI_API_KEY:
        raise PermanentAISearchError("No Gemini API key is configured")
    return genai.Client(api_key=GEMINI_API_KEY)


def _generate(
    auth: str,
    prompt: str,
    images: List[Tuple[str, bytes]],
    schema: Dict[str, Any],
    system_instruction: str,
    grounded: bool = True,
):
    """One model call. Raises the classified exception types, never raw SDK ones.

    Parameterised so the material extraction rides the same auth fallback and
    retry path as the search rather than growing a second copy of it.
    """
    parts = [types.Part.from_text(text=prompt)]
    for url, content in images:
        mime = "image/png" if url.lower().endswith(".png") else "image/jpeg"
        parts.append(types.Part.from_bytes(data=content, mime_type=mime))

    cfg = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=AI_SEARCH_TEMPERATURE,
        # url_context alongside search lets it open the pages it finds rather
        # than answering from snippets. It does not fire reliably, which is why
        # every reported URL is checked afterwards.
        tools=(
            [
                types.Tool(google_search=types.GoogleSearch()),
                types.Tool(url_context=types.UrlContext()),
            ]
            if grounded
            else None
        ),
        response_mime_type="application/json",
        response_json_schema=schema,
        safety_settings=[
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF"),
        ],
    )
    contents = [types.Content(role="user", parts=parts)]

    attempts = ["apikey", "vertex"] if auth == "auto" else [auth]
    last: Optional[Exception] = None
    for path in attempts:
        if path == "apikey" and not GEMINI_API_KEY:
            continue
        for attempt in range(1, MAX_INLINE_ATTEMPTS + 1):
            try:
                client = _make_client(path)
                return path, client.models.generate_content(
                    model=AI_SEARCH_MODEL, contents=contents, config=cfg
                )
            except Exception as e:
                last = e
                err = classify(e)
                throttled = "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e)
                if auth == "auto" and path == "apikey" and throttled:
                    logger.warning(
                        "Gemini API key has no grounding quota (429); falling back to Vertex, "
                        "which bills $35 per 1,000 grounded prompts against $14 and has no "
                        "free tier for Gemini 3.x"
                    )
                    break
                if isinstance(err, PermanentAISearchError) or attempt == MAX_INLINE_ATTEMPTS:
                    raise err
                delay = INLINE_BACKOFF_SECONDS + random.uniform(0, 1)
                logger.warning(
                    f"Verification call failed ({type(e).__name__}), retrying in {delay:.1f}s"
                )
                # Runs inside asyncio.to_thread, so this blocks one worker thread
                # and not the event loop.
                time.sleep(delay)
    raise classify(last or Exception("No auth path available"))


def response_text(response) -> str:
    """The answer text, without the SDK's warning on every access.

    Gemini 3 returns a thought_signature part next to the answer, and
    response.text logs a "non-text parts in the response" warning each time it
    steps over one.
    """
    if not response.candidates:
        return ""
    parts = response.candidates[0].content.parts or []
    return "".join(p.text for p in parts if getattr(p, "text", None))


def _usage_and_cost(response, auth: str, grounded: bool = True) -> Dict[str, Any]:
    # The grounding fee is per grounded prompt. The material extraction runs no
    # tools, so it pays tokens only -- fractions of a cent against $0.014.
    fee = GROUNDING_PRICE.get(auth, GROUNDING_PRICE["vertex"]) if grounded else 0.0
    usage = getattr(response, "usage_metadata", None)
    if not usage:
        return {"tokens": {}, "cost_usd": fee}

    def count(name: str) -> int:
        return getattr(usage, name, None) or 0

    # The search results the model reads back land in tool_use_prompt tokens and
    # its reasoning in thoughts. Both are billed, and on a grounded run they are
    # the larger half, so pricing prompt + candidates alone understates by ~half.
    billed_in = count("prompt_token_count") + count("tool_use_prompt_token_count")
    billed_out = count("candidates_token_count") + count("thoughts_token_count")
    cost = (
        billed_in / 1e6 * PRICE_PER_M_INPUT
        + billed_out / 1e6 * PRICE_PER_M_OUTPUT
        + fee
    )
    return {
        "tokens": {
            "prompt": count("prompt_token_count"),
            "tool_use": count("tool_use_prompt_token_count"),
            "thinking": count("thoughts_token_count"),
            "answer": count("candidates_token_count"),
            "cached": count("cached_content_token_count"),
            "total": count("total_token_count"),
        },
        "cost_usd": round(cost, 5),
    }


def search_terms_for(source: Dict[str, Any], label: Dict[str, Any], fields: Dict[str, Any]) -> List[str]:
    """Terms for a site:-scoped Google search, strongest identifier first.

    Measured: `site:americanrag.com M5056 CMIW424` returns the exact product page
    as the first result, and so does brand + the source's own title. Piling on
    the listing's whole field set does not --
    `site:garmentory.com Purple Brand Cotton Woven Pant Olive Green` returned a
    different brand's trousers. Three terms is the cap for that reason.
    """
    candidates = [
        source.get("mpn") or label.get("mpn") or fields.get("manufacturer_sku"),
        source.get("brand") or fields.get("brand_name"),
        source.get("title"),
    ]
    out: List[str] = []
    for term in candidates:
        clean = str(term or "").strip()
        if clean and not any(clean.lower() == o.lower() for o in out):
            out.append(clean)
    return out[:3]


def build_search_url(url: str, terms: List[str]) -> Tuple[str, str]:
    """A domain-scoped Google search for a source, as (query, url).

    Every source gets one, not just the dead ones. Grounded answers name the
    right retailer and invent the path, and there is no way to recover the real
    one from the response: the grounding chunks carry only redirect URIs. A
    search scoped to that domain lands the operator on the page the model read,
    which is the same move ListingView's MPN menu already makes.

    inurl: rather than site:, and terms unquoted. site: is the stricter operator
    and an exact phrase is the stricter match, but strict is not what helps here:
    the product is known to exist and the job is to land on its page, so a query
    that returns a near-miss beats one that returns nothing. A retailer that
    words its title differently from the source we recorded drops out of a
    quoted search entirely.
    """
    host = domain_of(url)
    query = " ".join(([f"inurl:{host}"] if host else []) + list(terms)).strip()
    return query, f"https://www.google.com/search?q={quote_plus(query)}"


async def check_urls(urls: List[str]) -> Dict[str, Tuple[str, str, str]]:
    """Fetch each reported URL: (token, prose, final_url).

    final_url is where the fetch actually landed, which is what turns a citation
    redirect into the page it stands for.

    The model's url_context tool does not reliably fire when Google Search is
    also enabled, so it answers from snippets and hands back a real domain with
    an invented path. Measured on one listing: 4 of 5 URLs 404'd. That reads as a
    genuine source right up until an operator clicks it.
    """
    headers = {
        # Retailers routinely 403 an unadorned client. A browser UA does not make
        # the check authoritative, it just keeps a live page from reading as dead.
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
    }
    results: Dict[str, Tuple[str, str]] = {}
    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=headers) as client:

        async def check(url: str):
            if not url:
                return url, ("none", "no url", "")
            target = url if url.startswith("http") else f"https://{url}"
            try:
                r = await client.get(target)
            except Exception as e:
                return url, ("error", f"unreachable ({type(e).__name__})", "")
            landed = str(r.url)
            if r.status_code == 404:
                return url, ("not_found", "404 does not exist", landed)
            if r.status_code in (403, 429):
                return url, ("blocked", f"{r.status_code} blocked, could not verify", landed)
            if r.status_code >= 400:
                return url, ("error", str(r.status_code), landed)
            # A Shopify storefront answers 200 on an unknown handle only after
            # redirecting, so a landing page reached from a product URL is the
            # same signal as a 404. Only applies within one host: a citation
            # redirect is SUPPOSED to leave for another domain.
            if domain_of(landed) == domain_of(target) and landed.rstrip("/") != target.rstrip("/"):
                return url, ("redirected", f"200 but redirected to {r.url}", landed)
            return url, ("ok", "200 OK", landed)

        for url, verdict in await asyncio.gather(*(check(u) for u in urls)):
            results[url] = verdict
    return results


def input_fingerprint(fields: Dict[str, Any], image_urls: List[str]) -> str:
    """Pins a verdict to the input it was formed against.

    Lets the UI say "checked against an older value" instead of quietly showing a
    verdict about an MPN the operator has since fixed.
    """
    payload = json.dumps(
        {
            "fields": {k: fields.get(k) for k in sorted(FIELD_TO_LISTING_KEY.values())},
            "images": sorted(image_urls),
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


async def run_for_listing(
    listing,
    *,
    reason: str = "manual",
    requested_by: Optional[str] = None,
    use_original_data: bool = False,
) -> Dict[str, Any]:
    """Verify a listing that already exists, and return the blob to store.

    Verifies `listing.data`, the values the operator is about to submit, not
    `original_data`. The prototype defaulted the other way because its question
    was "was creation wrong"; production's is "is this right now".
    `use_original_data` keeps the benchmark's question available to the backfill.
    """
    # original_data was never backfilled, so a listing predating it falls back to
    # data rather than verifying an empty form.
    source = (listing.original_data or listing.data) if use_original_data else listing.data
    source_key = "original_data" if (use_original_data and listing.original_data) else "data"
    return await run_for_fields(
        dict(source or {}),
        product_id=listing.info_product_id or listing.product_id,
        parent_product_id=listing.product_id,
        reason=reason,
        requested_by=requested_by,
        source_key=source_key,
    )


async def run_for_fields(
    fields: Dict[str, Any],
    *,
    product_id: str,
    parent_product_id: Optional[str] = None,
    reason: str = "manual",
    requested_by: Optional[str] = None,
    source_key: str = "data",
) -> Dict[str, Any]:
    """Verify a set of field values, with no listing row required.

    Split out from run_for_listing so creation can run this alongside the aspects
    call, before the row exists. Everything it needs -- brand, MPN, style, the
    photographs, the tag -- is keyed off the product, not off the listing.
    """
    from services.sellercloud_service import sellercloud_service

    if not is_configured():
        raise PermanentAISearchError("AI search is not configured")

    fields = dict(fields or {})
    parent_product_id = parent_product_id or product_id

    options = build_search_options(fields)
    if not options:
        raise PermanentAISearchError("Add a brand, MPN or style first")

    tag_text = await read_tag_text(product_id)
    if tag_text is None and parent_product_id != product_id:
        tag_text = await read_tag_text(parent_product_id)

    all_images = await product_images_from_db(product_id)
    if all_images is None:
        logger.info(f"No productimages row for {product_id}; falling back to probing GCS")
        all_images = await sellercloud_service.get_product_images(product_id)
    selected = select_images(all_images, AI_SEARCH_MAX_IMAGES, include_washtags=tag_text is None)
    images = await fetch_images(selected)
    if not images:
        raise PermanentAISearchError("This listing has no usable photographs")

    tag_options = tag_derived_queries(tag_text, fields)
    markers = own_label_markers(fields, [parent_product_id, product_id])
    prompt = build_prompt(
        fields,
        options,
        tag_text,
        [u for u, _ in images],
        tag_options=tag_options,
        own_markers=markers,
    )
    auth, response = await asyncio.to_thread(
        _generate, AI_SEARCH_AUTH, prompt, images, RESPONSE_SCHEMA, SYSTEM_PROMPT
    )

    text = response_text(response)
    if not text:
        finish = response.candidates[0].finish_reason if response.candidates else "unknown"
        raise TransientAISearchError(
            "The AI search service returned nothing", detail=f"finish_reason={finish}"
        )
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        # Almost always MAX_TOKENS. Retrying the same prompt reproduces it.
        raise PermanentAISearchError(
            "Could not read the AI search result", detail=f"{e}: {text[:300]}"
        )

    return await _shape_result(
        raw, fields, response, auth, tag_text, [u for u, _ in images], reason,
        requested_by, source_key, tag_options,
    )


MATERIAL_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "sources": {
            "type": "array",
            "description": "One entry per numbered source that states a composition. Omit the others.",
            "items": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "The number printed beside that source, as digits",
                    },
                    "material_composition": MATERIAL_SCHEMA,
                },
                "required": ["index", "material_composition"],
            },
        }
    },
    "required": ["sources"],
}


async def extract_source_materials(sources: List[Dict[str, Any]], auth: str) -> Dict[str, Any]:
    """Read the composition out of the descriptions the search already collected.

    A second call rather than a wider first one, and the reason is measured. When
    the composition was asked for inside the search, the colour reading degraded
    -- brand_color started coming back as "GLYPH ANTIQUE WHITE" and "CANVAS CARGO
    VINTAGE", neighbouring words dragged off the washtag. Five nested objects per
    source is a real change to what the model is producing alongside the fields
    that regressed, and the benchmark could not separate that from its own noise
    at the sample sizes on offer. Splitting the work means it does not have to:
    the search sends byte-identical inputs to what it sent before this feature.

    Cheap enough not to weigh against that. No tools, no images, and only the
    descriptions already in hand, so it pays tokens and not the $0.014 grounding
    fee -- well under a tenth of a cent. It also only runs for the ~29% of
    products whose washtag states no composition.

    Never raises: a listing that keeps its empty material field is exactly where
    it was, and failing the whole search over it would be a poor trade.
    """
    numbered, prompt_rows = [], []
    for src in sources:
        description = str(src.get("description") or "").strip()
        if not description:
            continue
        numbered.append(src)
        prompt_rows.append(
            f"{len(numbered)}. {src.get('domain') or src.get('source_name') or 'source'}"
            f" - {src.get('title') or ''}\n   {description}"
        )
    if not prompt_rows:
        return {"tokens": {}, "cost_usd": 0.0}

    prompt = (
        "Read the fibre composition out of each product page's own text below.\n\n"
        + "\n\n".join(prompt_rows)
    )
    try:
        used_auth, response = await asyncio.to_thread(
            _generate, auth, prompt, [], MATERIAL_EXTRACTION_SCHEMA, MATERIAL_PROMPT, False
        )
        raw = json.loads(response_text(response) or "{}")
    except Exception as e:
        logger.warning(f"Material extraction failed: {type(e).__name__}: {e}")
        return {"tokens": {}, "cost_usd": 0.0}

    for entry in raw.get("sources") or []:
        index = str(entry.get("index") or "").strip()
        if not index.isdigit() or not 1 <= int(index) <= len(numbered):
            continue
        numbered[int(index) - 1]["material"] = format_material(entry.get("material_composition"))
    return _usage_and_cost(response, used_auth, grounded=False)


def _material_from_sources(sources: List[Dict[str, Any]], listing_value: Any) -> Dict[str, Any]:
    """The composition verdict, built from what the pages published.

    Not asked of the model, because asking does not work. It reads each page's
    composition accurately -- four of five sources came back "Upper: Leather /
    Lining: Leather, Textile / Outer Sole: Other Materials" on the same boot --
    but asked for one overall answer it holds back for want of a full percentage
    breakdown, and reports not_found on a T-shirt whose stockist plainly says
    cotton. Counting what the pages actually said avoids the judgement call
    entirely, and it makes the evidence honestly "web": this verdict only exists
    where the washtag pass found nothing.

    An exact photo match wins over the model's own ordering, since a page for a
    different colourway of the same style is still the wrong garment to take a
    lining from.
    """
    published = [src for src in sources if src.get("material")]
    if not published:
        return {
            "verified_value": "",
            "listing_value": listing_value or "",
            "status": "not_found",
            "evidence": "none",
            "applicable": True,
            "agreeing_sources": 0,
            "reason": "No source published a composition for this item.",
        }

    best = next(
        (src for src in published if src.get("image_match") == "same_product"), published[0]
    )
    value = best["material"]
    agreeing = sum(1 for src in published if _same_text(src["material"], value))
    return {
        "verified_value": value,
        "listing_value": listing_value or "",
        "status": "confirmed" if _same_text(listing_value, value) else "conflict",
        "evidence": "web",
        "applicable": True,
        "agreeing_sources": agreeing,
        "reason": (
            f"{agreeing} of the {len(published)} source(s) publishing a composition say this. "
            "The tag states none."
        ),
    }


async def _shape_result(
    raw, fields, response, auth, tag_text, image_urls, reason, requested_by,
    source_key, tag_options=None,
) -> Dict[str, Any]:
    """Translate the model's vocabulary into this app's, and attach the extras."""
    from services.product_service import format_mpn

    # Two copies deliberately. The block as the model returned it stays the
    # better set of search terms even where it is not a transcription: a code it
    # read off a retailer is exactly what to search that retailer for. It just
    # must not be shown to an operator as something the tag says.
    model_label = dict(raw.get("label") or {})
    label = dict(model_label)
    unprinted = drop_unprinted_label_values(label, tag_text)
    if unprinted:
        logger.info(f"Label values the tag does not print, cleared: {', '.join(unprinted)}")
    if label.get("mpn"):
        # Compared against listings.data.manufacturer_sku under the same rule SKU
        # creation uses, so "AW23MKB005 100" and "AW23MKB005_100" are one value.
        label["mpn_normalized"] = format_mpn(str(label["mpn"]))

    sources = [dict(src) for src in (raw.get("sources") or [])]
    for src in sources:
        # Formatted here, never taken as prose: the operator compares a source's
        # composition against the field's, and they have to be written alike or
        # the comparison is about punctuation.
        src["material"] = format_material(src.pop("material_composition", None))
        if src.get("description"):
            src["description"] = str(src["description"])[:MAX_SOURCE_DESCRIPTION]

    # Order matters here. The URLs are checked FIRST, because a grounded answer
    # often cites the redirect rather than the page, and every derived field --
    # the domain on the chip, the site: search behind it -- would otherwise be
    # computed from vertexaisearch.cloud.google.com and be identical and useless
    # on every source.
    if AI_SEARCH_CHECK_URLS and sources:
        statuses = await check_urls([s.get("url") or "" for s in sources])
        for src in sources:
            reported = src.get("url") or ""
            token, prose, landed = statuses.get(reported, ("none", "", ""))
            if is_grounding_redirect(reported) and landed and not is_grounding_redirect(landed):
                # Keep what the model said, but treat where it landed as the
                # source from here on.
                src["reported_url"] = reported
                src["url"] = landed
            src["url_status"] = token
            src["url_status_detail"] = prose
            src["url_ok"] = token == "ok"
    else:
        for src in sources:
            src["url_status"] = "unchecked"
            src["url_status_detail"] = ""
            src["url_ok"] = False

    unresolved = 0
    for src in sources:
        url = src.get("url") or ""
        if is_grounding_redirect(url):
            # Followed and still a redirect, or checking was off. There is no
            # real domain to scope a search to, so drop the url entirely rather
            # than show a Google host as though it were the retailer.
            unresolved += 1
            src["reported_url"] = url
            src["url"] = ""
            url = ""
        src["domain"] = domain_of(url)
        query, search_url = build_search_url(url, search_terms_for(src, model_label, fields))
        src["search_query"] = query
        src["search_url"] = search_url
    if unresolved:
        logger.warning(
            f"{unresolved} of {len(sources)} sources cited a grounding redirect that could "
            "not be resolved; their searches fall back to an unscoped query"
        )

    verdicts = {}
    for model_key, verdict in (raw.get("verdict") or {}).items():
        listing_key = FIELD_TO_LISTING_KEY.get(model_key)
        if not listing_key:
            continue
        v = dict(verdict or {})
        v["applicable"] = listing_key not in NON_APPLICABLE_FIELDS
        if listing_key == "manufacturer_sku" and v.get("verified_value"):
            v["normalized_value"] = format_mpn(str(v["verified_value"]))
        verdicts[listing_key] = v

    # Composition is dropped outright wherever the washtag pass already read one
    # off this garment: that parse is what fills the field in the first place, so
    # a stockist's page can only disagree with the item in the box. What is left
    # is the gap the search exists to close, and it is not small -- 893 of 3,116
    # photographed products have a washtag row with no composition in it at all
    # (belts, socks, hats, jewellery, and tags too worn to read), and on those
    # the field is empty today with nothing else coming to fill it.
    #
    # Removed rather than marked, so the field shows no adornment at all. A card
    # reading "confirmed" on every garment would be an icon that never means
    # anything, and operators stop reading those.
    material_usage = {"tokens": {}, "cost_usd": 0.0}
    if ((tag_text or {}).get("material") or {}).get("components"):
        verdicts.pop("material", None)
    else:
        material_usage = await extract_source_materials(sources, auth)
        verdicts["material"] = _material_from_sources(sources, fields.get("material"))

    if (
        label.get("mpn_normalized")
        and verdicts.get("manufacturer_sku", {}).get("normalized_value")
        and label["mpn_normalized"] != verdicts["manufacturer_sku"]["normalized_value"]
        and verdicts["manufacturer_sku"].get("evidence") in ("label", "both")
    ):
        logger.warning(
            "Verification label/verdict MPN disagree while claiming label evidence: "
            f"label={label['mpn_normalized']} verdict={verdicts['manufacturer_sku']['normalized_value']}"
        )

    usage = _usage_and_cost(response, auth)
    meta = response.candidates[0].grounding_metadata if response.candidates else None

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "done",
        "model": AI_SEARCH_MODEL,
        "auth": auth,
        "reason": reason,
        "requested_by": requested_by,
        "cost_usd": round(usage["cost_usd"] + material_usage["cost_usd"], 5),
        "error": None,
        "input": {
            "source": source_key,
            "fingerprint": input_fingerprint(fields, image_urls),
            "image_urls": image_urls,
            "tag_text_available": tag_text is not None,
            "search_options": [
                {"label": lbl, "query": q}
                for lbl, q in (tag_options or []) + build_search_options(fields)
            ],
        },
        "label": label,
        "fields": verdicts,
        "sources": sources,
        "notes": str(raw.get("notes") or "")[:MAX_NOTES],
        "diagnostics": {
            "searches": list(getattr(meta, "web_search_queries", None) or []),
            "tokens": usage["tokens"],
            "material_tokens": material_usage["tokens"],
            "label_dropped": unprinted,
            "country_of_origin": (tag_text or {}).get("country_of_origin"),
        },
    }
