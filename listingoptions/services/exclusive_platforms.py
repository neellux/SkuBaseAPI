"""Platforms a type can be mapped to only one of.

goat and goat_sneakers are the same marketplace with two sheet layouts. The type is
what routes a listing to one or the other, so a type belongs to exactly one of them.
A brand does not: a brand sells shoes and everything else, so it can be mapped to
both. The rule is written down once, here, and enforced where type mappings are
written:

    mapped to one    -> the other is added to the record's excluded_platforms
    mapped to both   -> refused. The operator clears one first; a mapping is never
                        removed on their behalf.

It is enforced at write time, into the column every reader already honours (the
submit gates, listing_required_platforms, the catalog, the listing view), rather than
derived at read time in each of them. The price is that the array carries no
provenance, so removing a mapping does not lift the other platform's exclusion: an
exclusion the rule added and one an operator ticked look the same.

Applies only to EXCLUSIVE_TABLES. Brands and colours are mapped per platform as usual.

Which of the pair a type goes to is decided by its parent's division (division_route):
footwear to goat_sneakers, everything else to goat. Every type save adds the other one
to its exclusions and refuses a mapping to it, so a new type is routed before anyone
maps it. Moving a type to another division also lifts the exclusion its old division
added. A type with no parent is left to its mappings alone.
"""

from typing import Any, Final, Iterable, Mapping

# Tables whose records the rule applies to. Each must allow_exclude, since the rule
# writes excluded_platforms; applies_to checks both.
EXCLUSIVE_TABLES: Final[frozenset[str]] = frozenset({"types"})

EXCLUSIVE_PLATFORMS: Final[Mapping[str, str]] = {
    "goat": "goat_sneakers",
    "goat_sneakers": "goat",
}


FOOTWEAR_DIVISION: Final = "Footwear"


def division_route(division: str | None) -> tuple[str, str] | None:
    """(platform a type goes to, platform it is excluded from) for its parent's division.

    None when the division is unknown (no parent): the rule decides nothing then.
    """
    if not division or not str(division).strip():
        return None
    if str(division).strip().lower() == FOOTWEAR_DIVISION.lower():
        return "goat_sneakers", "goat"
    return "goat", "goat_sneakers"


def applies_to(table_schema: Any) -> bool:
    """Whether records of this table (a listingoptions Schema) follow the rule."""
    return (
        getattr(table_schema, "table", None) in EXCLUSIVE_TABLES
        and bool(getattr(table_schema, "allow_exclude", False))
    )


def exclusive_counterpart(platform_id: str) -> str | None:
    """The platform a record mapped to `platform_id` is excluded from, if any."""
    return EXCLUSIVE_PLATFORMS.get(platform_id)


def resolve_exclusive(mapped: Iterable[str]) -> tuple[set[str], tuple[str, str] | None]:
    """What a record's mapped platforms imply.

    Returns (platforms to exclude, conflict). `conflict` is the pair that are both
    mapped, in a stable order, and means the save must be refused; the exclusions are
    then empty, because excluding a platform the record is mapped to would unlink it.
    """
    mapped = set(mapped)
    to_exclude: set[str] = set()
    for platform_id in sorted(mapped):
        other = EXCLUSIVE_PLATFORMS.get(platform_id)
        if other is None:
            continue
        if other in mapped:
            return set(), tuple(sorted((platform_id, other)))
        to_exclude.add(other)
    return to_exclude, None
