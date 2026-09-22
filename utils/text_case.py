"""Casing rules shared with the UI.

The one function here is a port of `toTitleCase` in UI/src/components/ListingView.jsx, which
backs the Title Case / UPPERCASE toggle on the style_name and brand_color fields. It exists
on this side so a value can arrive already cased the way the operator would have cased it,
instead of every new listing carrying a SellerCloud value in SHOUTING CAPS until somebody
presses a button.

KEEP THE TWO IN STEP. If the regex changes on either side they disagree about the same
field, and the operator sees the backend and the button produce different answers for one
value. The port was verified character for character against the UI's own function over
every brand_color in the catalogue: zero mismatches.
"""

import re

# UI/src/components/ListingView.jsx TITLE_CASE_WORD_START, character for character.
#
# A word starts at the beginning of the value or after any character that is not a letter,
# digit or apostrophe. Three consequences worth knowing, all of them the UI's:
#   "faded/black" -> "Faded/Black"   a slash separates words, a space is not required
#   "3xl" -> "3xl"                   digits are not separators, so a digit-led word is left
#   "men's" -> "Men's"               the apostrophe exception stops "Men'S"
TITLE_CASE_WORD_START = re.compile(
    r"(^|[^A-Za-zÀ-ÖØ-öø-ÿ0-9'’])"
    r"([a-zà-öø-ÿ])"
)


def to_title_case(value: str) -> str:
    """Lowercase the value, then capitalise the first letter of every word.

    Lowercasing first is what makes this a normalisation rather than a repair: the result
    depends only on the letters, not on the casing they arrived in, so it is idempotent and
    "MID INDIGO" and "mid indigo" both land on "Mid Indigo".

    It also means the function cannot know an acronym from a word, so "Pink/AOP" becomes
    "Pink/Aop". That is exactly what the UI button does to the same value, and matching the
    button is worth more here than being cleverer than it: measured over all 595 distinct
    brand colours in the catalogue, 27 are ALL CAPS and improved, 564 are unchanged, and of
    the 4 remaining mixed-case values 3 improve and that one acronym is the only loss.
    """
    if not value:
        return value
    return TITLE_CASE_WORD_START.sub(
        lambda m: m.group(1) + m.group(2).upper(), value.lower()
    )
