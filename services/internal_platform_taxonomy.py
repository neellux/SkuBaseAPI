"""Lookup tables for the consignment pipeline. Pure data, no I/O.

Category and gender both come from the listing-options mappings now, loaded per cycle by
services/internal_platform_type_map.py and passed into the pure rules. What remains here
is the translation between Lux's vocabulary and Shop The Sample's, which is genuinely a
code decision rather than a per-platform setting.

KNOWN GAP, accepted 2026-07-23
------------------------------
The category tags we emit do not all match the destination store's live vocabulary.
Measured against 250 live tagged products and 270 smart collections, the live set is:

    ACC, JACKET, PANTS, SHIRT, SHOE, SWEATER, DRESS, JEWELRY, SKIRT, SCARF,
    GLASSES, VEST, GLOVE, SOCK, SWIM, BLAZER

This is survivable ONLY because tag writes are additive (tagsAdd / scoped tagsRemove),
never a replace. A replace would delete the correct live tag and substitute a wrong one,
moving 78% of products out of the category collections they currently occupy. See
services/shopify_admin.py for the enforcement.

Closing the gap now means editing the mappings in the listing-options UI rather than
editing this file, which was the point of moving them into the database.
"""

from __future__ import annotations

from typing import Final, Mapping

# Tags this automation owns and is therefore allowed to remove. Anything not in this
# set belongs to STS (stock:low, arrival:new, 424, and their category tags) and must
# survive every write we make.
OWNED_TAG_PREFIXES: Final[frozenset[str]] = frozenset(
    {"SHOPTHESAMPLE", "MEN", "WOMEN", "GIRLS", "BOYS"}
)

# Listing-options gender -> the STS tags it implies. Keys are the EXACT values stored in
# listingoptions_types_parents.gender, verified against both lux_skubase and
# lux_skubase_test on 2026-07-28: title case, no padding, and the same six values in each.
#
# "Does Not Apply" and "Unisex" both mean the product is not gender-specific, so it
# belongs in BOTH the men's and women's collections rather than neither. They map
# identically; the two values exist because listing options records them separately, not
# because Shop The Sample treats them differently.
#
# Any value NOT listed here returns None and the product is skipped as
# underivable_gender. Listing with a partial tag set puts a product into the wrong
# collections, which is worse than not listing it - so this map is a whitelist, and it
# stays one. All six values stored in listingoptions_types_parents.gender are now mapped.
LO_GENDER_TO_STS: Final[Mapping[str, tuple[str, ...]]] = {
    "Mens": ("MEN",),
    "Womens": ("WOMEN",),
    "Girls": ("GIRLS",),
    "Boys": ("BOYS",),
    "Does Not Apply": ("MEN", "WOMEN"),
    "Unisex": ("MEN", "WOMEN"),
}

# Built once. Lookups normalise both sides so a casing edit in the options UI cannot
# silently start skipping an entire gender, while the literal above stays greppable
# against the data as stored.
_LO_GENDER_INDEX: Final[Mapping[str, tuple[str, ...]]] = {
    k.strip().casefold(): v for k, v in LO_GENDER_TO_STS.items()
}


def gender_tags_for(lo_gender: str | None) -> tuple[str, ...] | None:
    """STS gender tags for a listing-options gender, or None if it does not map.

    None covers a blank gender, a type whose parent row is missing, and any value the
    options UI grows that nobody has mapped yet - all skip-and-flag conditions rather
    than "no gender tag".
    """
    if not lo_gender:
        return None
    return _LO_GENDER_INDEX.get(lo_gender.strip().casefold())

VENDOR_OVERRIDES: Final[Mapping[str, str]] = {
    "OFF-WHITE C/O VIRGIL ABLOH": "OFF-WHITE",
    "PURPLE": "PURPLE BRAND",
}



def normalize_vendor(vendor: str | None) -> str | None:
    """Uppercase, then apply the canonical overrides.

    Smart collection `vendor equals` rules are exact-match and case-sensitive, so a
    product whose vendor is 'Rhude' matches no rule written for 'RHUDE'. This is why
    vendor normalization has to happen before collection membership can work.
    """
    if not vendor or not vendor.strip():
        return None
    upper = vendor.strip().upper()
    return VENDOR_OVERRIDES.get(upper, upper)


def normalize_title(title: str | None) -> str | None:
    """Uppercase the destination product title. None when there is nothing to normalize.

    Syncio copies the title verbatim from 1nventory, where casing is whatever the person
    who created the product typed. Shop The Sample presents them uppercase, so this is the
    same correction normalize_vendor makes and for a related reason - except that vendor
    casing also breaks smart-collection membership, while this one is purely presentation.

    No override table, unlike VENDOR_OVERRIDES. A title is free text rather than a value
    matched against collection rules, so there is nothing for an exception list to fix;
    upper() is the whole rule. Returning None for a blank title means the caller plans no
    write rather than trying to set an empty string.
    """
    if not title or not title.strip():
        return None
    return title.strip().upper()


# derive_parent_sku() lived here: parent = everything before the last "/" in a variant
# SKU. It is gone on purpose. Parent resolution belongs to the products DB
# (child_products.sku -> parent_sku, via services/product_resolver.py), which both
# pollers now use. The string form was only ever a proxy: it rejected the 1,893
# registered child SKUs that carry no size suffix, and it could not see a merge, so it
# reported the pre-merge parent as though nothing had happened. Do not reintroduce it -
# see the note in the source poller, "String-splitting is deliberately not a resolution
# path - it would invent unregistered parents."


def is_sts_native_sku(variant_sku: str | None) -> bool:
    """STS's own products use numeric SKUs prefixed with 'i' (e.g. i175851)."""
    if not variant_sku:
        return False
    sku = variant_sku.strip()
    return len(sku) > 1 and sku[0] in ("i", "I") and sku[1:].isdigit()
