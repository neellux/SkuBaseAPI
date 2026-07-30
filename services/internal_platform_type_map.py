"""Product type -> destination category tag and gender tags, from the listing options.

Replaces the hardcoded PTN_TAG_MAP and the prefix-matching derive_gender. Both halves of
the STS tag set now come from the same place, which is the point: they had already drifted
apart, with desired_tags writing a category from the hardcoded map while the destination
poller validated the product against the database one. The tag written was not the tag
that had been checked.

  category  listingoptions_types_default_list, keyed on platform_id. Editable in the
            options UI without a deploy, which matters because the destination's category
            vocabulary is owned by Shop The Sample rather than by us.

  gender    listingoptions_types.parent_id -> listingoptions_types_parents.gender,
            translated to STS tags by taxonomy.LO_GENDER_TO_STS. The gender lives on the
            PARENT, so a type with no parent has no gender and is skipped.

I/O layer. The pure rules module stays database-free; a poller loads this once per cycle
and passes it in.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Mapping

from tortoise import connections

from services.internal_platform_taxonomy import gender_tags_for

logger = logging.getLogger(__name__)

# The listing-options row that carries the type value. Mirrors the convention recorded
# for sizes: primary_table_column names the column on the primary table.
TYPE_COLUMN = "type"

# Below this many category mappings, NOTHING runs. 221 exist as of 2026-07-29.
#
# This is the direct test that replaces two proxies. min_candidate_set_size and
# max_candidate_set_shrink_pct inferred a broken input from the shape of the output -
# they fired on a real sell-through and stayed silent on a half-loaded taxonomy that
# still left enough products qualifying. This asks the actual question.
#
# It matters most on the path that cannot be undone. An empty or truncated taxonomy makes
# qualifies() reject EVERY product as unmapped_product_type; on a delist pass every tagged
# product then starts soaking toward deletion. That is the SPO mapping wipe shape exactly:
# an unexpectedly small input set treated as authoritative.
#
# A hard constant, not config, on purpose - it is a correctness floor, not an operational
# dial, and it must hold in an environment nobody has tuned yet. Production has 0 mappings
# until they are seeded, so this also stops a freshly deployed prod poller from
# classifying the whole catalog as unmapped before anyone notices.
MIN_TAXONOMY_MAPPINGS = 100


@dataclass(frozen=True, slots=True)
class TypeTaxonomy:
    """Everything the tag rules need to know about product types, for one platform.

    Both maps are keyed on the lower-cased Lux type so lookups tolerate casing drift
    between Shopify's productType and the Lux master list.
    """

    category: Mapping[str, str]
    gender: Mapping[str, tuple[str, ...]]

    def category_for(self, product_type: str | None) -> str | None:
        if not product_type:
            return None
        return self.category.get(product_type.strip().lower())

    def gender_for(self, product_type: str | None) -> tuple[str, ...] | None:
        if not product_type:
            return None
        return self.gender.get(product_type.strip().lower())


def check_taxonomy_health(taxonomy: TypeTaxonomy,
                          floor: int = MIN_TAXONOMY_MAPPINGS) -> str | None:
    """Breach message if the taxonomy is too thin to act on, else None.

    Call this BEFORE anything else in a cycle - before the Shopify scan, and long before
    any write. Every downstream decision reads this map, so a thin one does not degrade
    gracefully: it silently reclassifies the entire catalog.

    Only the category half is measured. Gender is legitimately sparse - a type with no
    parent row has no gender and is skipped by design - so a floor on it would fire on
    correct data. Category is the half that must be complete for qualifies() to mean
    anything.
    """
    n = len(taxonomy.category)
    if n < floor:
        return (
            f"taxonomy has {n} category mappings, below the floor of {floor}; "
            "refusing to run - every product would be misread as unmapped_product_type"
        )
    return None


async def load_taxonomy(platform_id: str) -> TypeTaxonomy:
    """Both halves in ONE query per cycle, not one query each.

    The gender join is LEFT, not INNER, deliberately. A type with no parent row must still
    appear in the result so it resolves to "no gender" and is skipped explicitly. An INNER
    join would drop it from the map entirely, making it indistinguishable from a product
    type that does not exist in listing options at all - two different data problems that
    deserve different fixes.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT t.type AS lux_type, d.platform_value, p.gender "
        "FROM listingoptions_types t "
        "LEFT JOIN listingoptions_types_default_list d "
        "       ON d.primary_id = t.id AND d.platform_id = $1 "
        "      AND d.primary_table_column = $2 "
        "LEFT JOIN listingoptions_types_parents p ON p.id = t.parent_id "
        "WHERE t.type IS NOT NULL",
        [platform_id, TYPE_COLUMN],
    )

    category: dict[str, str] = {}
    gender: dict[str, tuple[str, ...]] = {}
    for r in rows:
        key = r["lux_type"].strip().lower()
        value = (r["platform_value"] or "").strip()
        if value:
            category[key] = value
        tags = gender_tags_for(r["gender"])
        if tags:
            gender[key] = tags

    logger.info(
        "internal_platforms: taxonomy for %s - %d types, %d with a category mapping, "
        "%d with a usable gender",
        platform_id, len(rows), len(category), len(gender),
    )
    return TypeTaxonomy(category=category, gender=gender)
