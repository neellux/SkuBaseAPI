"""Suggest a catalogue style_name for a listing, as a verdict for the AI search card.

WHY THIS IS NOT ASKED OF THE SEARCH MODEL. style_name is a house convention, not a fact
about the product: Denim Tears does not publish "Aran Scarf". The search prompt's standing
rule is "Report only what a page or a tag actually states. Never infer, normalise or repair
a value", which is right for a verification verdict and exactly wrong here. So this is a
separate text-only pass that reads the evidence the search has already gathered and applies
the house format to it. `_material_from_sources` sets the same precedent: a derived verdict
folded into `fields` that the search model never answered.

WHAT THE EVIDENCE IS WORTH, measured over 313 operator edits (listings that were edited and
then submitted to at least one platform, diffing `data` against `original_data`):

    washtag (label.title)   reaches the operator's answer on   7 rows (2.2%), never verbatim
    web title (fields.title) reaches the operator's answer on 53 rows (17%),  verbatim x4

A washtag prints style codes, colourways, composition and care. The marketing style name is
not on it. That is the reverse of manufacturer_sku and brand_color, where the tag is
decisive, so the tag is deliberately NOT passed here.

NEITHER IS THE WEB TITLE, IN PRODUCTION, AND THAT IS WHAT MAKES THIS PASS FREE. It looked
like the one evidence source worth having, but dropping it scored 82/159 against the same
82/159, fixing 7 rows and breaking 7, against a noise floor of 3 fixed / 5 broke on two
runs of an identical prompt. The prompt already fences it off ("never let it replace a
supplier name that already reads correctly"), so it was being paid for and then ignored:
when it adds anything beyond the supplier name the operator keeps 53% and discards 47%, and
trusting it wholesale produced the worst misses in testing ("PUREBOOST SNEAKERS" ->
"Pureboost 5 Running Shoes"). `web_title` therefore stays on the signature for the bench's
`--web-title` ablation, and ai_search_service does not pass it. Passing one in production
would put this call back BEHIND the search instead of beside it, for nothing.

PHOTOGRAPHS ARE DELIBERATELY NOT SENT. Paired on 159 listings, adding five product shots
scored 48% against text-only's 54% on the rows that have them, fixing 3 and breaking 9: the
images make the model describe what it sees instead of keeping the supplier's wording
("GEL-NYC 2.0" -> "GEL-NYC 2.0 Sneakers", "Hardies NYC Beanie" -> "Hardies NYC Floral
Beanie"). Do not add them back without re-running the bench.

MEASURED: 52% exact against the operator's own value, on 159 held-out listings, against a
13% baseline of leaving the supplier name alone. Re-run with
`API/.venv/bin/python test_scripts/style_name_bench.py`. The ceiling is not the model: 21 of
42 repeated supplier names in the corpus have more than one accepted operator answer, so a
meaningful share of the set has no single right answer. This is a SUGGESTION and is never
auto-applied.
"""

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import litellm
from config import config

logger = logging.getLogger(__name__)

_PROMPT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "prompts")
_SYSTEM_PATH = os.path.join(_PROMPT_DIR, "style_name_system_prompt.txt")
_EXAMPLES_PATH = os.path.join(_PROMPT_DIR, "style_name_examples.txt")

_AI = config.get("ai", {})
# Falls back to the aspects settings, which are the exact model and effort the bench scored
# (openai/gpt-5.6-luna at reasoning_effort=high). Overridable so this can be moved off the
# aspects model without touching code.
MODEL = _AI.get("style_name_model") or _AI.get("aspects_model")
API_KEY = _AI.get("style_name_api_key") or _AI.get("aspects_api_key")
REASONING_EFFORT = _AI.get("style_name_reasoning_effort") or _AI.get("aspects_reasoning_effort") or "high"

# Six covers the median of five stored sources with room to spare, and caps a pathological
# row. Ordered as the search ranked them, so the best match is never the one dropped.
MAX_SOURCE_TITLES = 6

# Longest value seen in the corpus is well under this. A model that runs away with a
# sentence is refused rather than surfaced.
MAX_LENGTH = 120

# This runs inside an AI search job, and the search poller has only three worker slots. An
# unbounded call would let one stalled request hold a slot indefinitely; the call itself
# measures 2-5s, so a minute is generous and still bounded.
TIMEOUT_SECONDS = 60

# One retry, because a single attempt measurably loses listings: a backfill run over prod
# hit litellm's own "'Usage' object has no attribute 'server_tool_use'" on 1 of 12 calls,
# which is a transient fault in the client rather than anything about the listing. Retrying
# is close to free here because this call runs CONCURRENTLY with the 5-47s grounded search,
# so a second 2-5s attempt still finishes first.
MAX_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 2.0


def is_configured() -> bool:
    return bool(MODEL and API_KEY)


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def item_block(
    *,
    style_name: str,
    brand: str,
    product_type: str,
    colour: str = "",
    supplier_title: str = "",
    web_title: str = "",
    source_titles: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """One product rendered for the prompt.

    Examples and the live item use this same function, so a live item never looks different
    from what the model was shown. The garment tag is not a field here: see the module note.

    The shipped examples carry no source titles while live items do. That asymmetry is
    deliberate and is what was measured: the 19/30 run used exactly this shape. Adding
    sources to the 48 examples would change the prompt the score was taken on.
    """
    lines = [
        f"Supplier name: {_norm(style_name) or '(empty)'}",
        f"Brand: {_norm(brand) or '(unknown)'}",
        f"Product type: {_norm(product_type) or '(unknown)'}",
    ]
    if _norm(colour):
        lines.append(f"Colour (strip this from the name): {_norm(colour)}")
    if _norm(supplier_title):
        lines.append(f"Supplier title: {_norm(supplier_title)}")
    if _norm(web_title):
        lines.append(f"Web product title: {_norm(web_title)}")
    rows = [
        (_norm(t.get("domain") or t.get("source_name")), _norm(t.get("title")),
         t.get("image_match"))
        for t in (source_titles or [])
    ]
    rows = [r for r in rows if r[1]][:MAX_SOURCE_TITLES]
    if rows:
        lines.append("Web product titles:")
        for domain, title, match in rows:
            # The colourway warning is carried through rather than filtered: a page for
            # another colour of the same style still names the style correctly, and the
            # prompt says so. Hiding it would lose a usable name.
            mark = "" if match == "same_product" else f"  [{match}]"
            lines.append(f"  {domain or 'source'}: {title}{mark}")
    return "\n".join(lines)


def load_examples() -> List[Dict[str, str]]:
    """The few-shot pairs, read from the tab-separated file beside the prompt.

    Tab separated .txt rather than JSON because .gitignore blanket-ignores *.json and this
    is a runtime asset the service cannot start without; every other prompt asset in this
    repo is a tracked utils/prompts/*.txt too. Comment and blank lines are skipped, the
    first real line is the header.
    """
    rows: List[Dict[str, str]] = []
    header: Optional[List[str]] = None
    with open(_EXAMPLES_PATH) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            if header is None:
                header = parts
                continue
            rows.append(dict(zip(header, parts)))
    return rows


def _load_system_prompt() -> str:
    """The shipped prompt with its shipped examples. Built once at import.

    The examples are regenerated from a scored bench run
    (`test_scripts/style_name_bench.py --dump-examples`), so what production sends is what
    the 52% was measured on rather than a hand-edited drift from it. They are rendered
    through item_block, the same function the live item goes through, so the two can never
    show the model a different shape of input.
    """
    with open(_SYSTEM_PATH) as f:
        template = f.read()
    shots = "\n\n".join(
        item_block(
            style_name=e.get("o_style"),
            brand=e.get("brand"),
            product_type=e.get("product_type"),
            colour=e.get("o_color"),
            supplier_title=e.get("o_title"),
            # Deliberately not passed: production sends no web title, so neither may the
            # examples. See the module note.
        )
        + f"\n-> {_norm(e.get('d_style'))}"
        for e in load_examples()
    )
    return template.replace("{examples}", shots)


try:
    SYSTEM_PROMPT = _load_system_prompt()
    EXAMPLE_COUNT = SYSTEM_PROMPT.count("Supplier name:")
except Exception as e:  # noqa: BLE001
    logger.error(f"Could not build the style_name prompt: {e}")
    SYSTEM_PROMPT = ""
    EXAMPLE_COUNT = 0


def _verdict(
    *, suggested: str, listing_value: str, reason: str, status: str, evidence: str
) -> Dict[str, Any]:
    """The shape every AI search field verdict has, so the card renders this with no
    special-casing. agreeing_sources is 0 on purpose: this value is not read off any
    source page, so claiming agreement would be a false citation."""
    return {
        "verified_value": suggested,
        "listing_value": listing_value,
        "status": status,
        "evidence": evidence,
        "applicable": True,
        "agreeing_sources": 0,
        "reason": reason,
    }


def _cost_of(response: Any) -> float:
    """What this call cost, so the row's cost_usd stays the whole truth.

    litellm prices the response from its own model table. A pricing gap must not fail the
    job, so an unpriceable response is reported as 0.0 and logged rather than raised.
    """
    try:
        return float(litellm.completion_cost(completion_response=response) or 0.0)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Could not price the style_name call ({type(e).__name__}: {e})")
        return 0.0


async def suggest(
    fields: Dict[str, Any],
    *,
    web_title: Optional[str] = None,
    source_titles: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], float]:
    """The style_name verdict for one listing, and what the call cost.

    Never raises: a failure is a verdict that says nothing, because the search job around
    this must still store its other fields.
    """
    listing_value = _norm(fields.get("style_name"))
    if not is_configured() or not SYSTEM_PROMPT:
        return _verdict(
            suggested="", listing_value=listing_value, status="not_found", evidence="none",
            reason="No model is configured for style name suggestions.",
        ), 0.0

    user = item_block(
        style_name=fields.get("style_name"),
        brand=fields.get("brand_name"),
        product_type=fields.get("product_type"),
        colour=fields.get("brand_color") or fields.get("standard_color"),
        supplier_title=fields.get("title"),
        web_title=web_title,
        source_titles=source_titles,
    )
    # No temperature: the gpt-5.x reasoning models accept only the default and 400 on
    # anything else, so this call is not reproducible run to run even at a fixed effort.
    # Measured run-to-run churn is ~8 rows in 70, which is why a change here has to be
    # judged on fixed-vs-broke counts and not on a net score.
    suggested = model_reason = ""
    cost = 0.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = await litellm.acompletion(
                model=MODEL,
                api_key=API_KEY,
                reasoning_effort=REASONING_EFFORT,
                # See ai_service: litellm's per-model param map lags new releases, and
                # without naming it here a newer model rejects reasoning_effort outright.
                allowed_openai_params=["reasoning_effort"],
                timeout=TIMEOUT_SECONDS,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user},
                ],
            )
            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                content = re.sub(r"^```(?:json)?|```$", "", content).strip()
            parsed = json.loads(content)
            suggested = _norm(parsed.get("style_name"))
            model_reason = _norm(parsed.get("reason"))
            cost = _cost_of(response)
            break
        except Exception as e:  # noqa: BLE001
            if attempt < MAX_ATTEMPTS:
                logger.warning(
                    f"style_name attempt {attempt} failed for {listing_value!r} "
                    f"({type(e).__name__}: {e}); retrying"
                )
                await asyncio.sleep(RETRY_BACKOFF_SECONDS)
                continue
            logger.exception(f"style_name suggestion failed for {listing_value!r}")
            return _verdict(
                suggested="", listing_value=listing_value, status="not_found",
                evidence="none",
                reason=f"The style name pass did not complete ({type(e).__name__}).",
            ), 0.0

    if not suggested or len(suggested) > MAX_LENGTH:
        # A runaway answer is dropped rather than shown. An operator who sees one absurd
        # suggestion stops reading the card, which costs more than the miss.
        logger.warning(
            f"Discarding style_name suggestion of {len(suggested)} chars for "
            f"{listing_value!r}: {suggested[:120]!r}"
        )
        return _verdict(
            suggested="", listing_value=listing_value, status="not_found", evidence="none",
            reason="The style name pass returned nothing usable.",
        ), cost

    # EXACT, not case-insensitive. For most fields a case difference is a formatting
    # opinion, which is why the card compares brand_color loosely: it has its own Title
    # Case toggle. For style_name the casing IS the suggestion. 211 of the 374 operator
    # edits this was built from were nothing but ALLCAPS -> Title Case, so treating
    # "TAPE ARROW HOODIE" and "Tape Arrow Hoodie" as agreement marked the single most
    # common and most useful suggestion "confirmed" and hid it from the card entirely.
    agrees = suggested == listing_value
    return _verdict(
        suggested=suggested,
        listing_value=listing_value,
        status="confirmed" if agrees else "conflict",
        # "house" rather than label/web/both: this is the catalogue's own naming
        # convention applied to the supplier name, not something a source states.
        evidence="house",
        reason=model_reason or (
            "The supplier name already follows the house format."
            if agrees
            else "House naming format applied to the supplier name."
        ),
    ), cost
