"""Grailed sheet keys: "<child sku>_<MMDDYYYY>".

The Grailed AppScript (addListings) returns one key per row it added to the sheet: our own
child sku plus the sheet batch date. It is a sheet row key, not a Grailed id, but it is the
only Grailed id SkuBase has, so it is what external_listing_ids stores for a Grailed child.

A sku may itself contain "_" (NER-XACC-0003/7_1-2 is a real child), so a key splits on the
LAST "_" that is followed by exactly eight digits, and the date is parsed rather than
compared as a string: MMDDYYYY strings sort 12312024 after 01152026. The digits are ASCII:
the AppScript writes its dates that way.

No imports from the app, so the one-off backfill script can use this without config.toml.
"""

import logging
import re
from datetime import date
from typing import Any, Iterable, Optional, Tuple

logger = logging.getLogger(__name__)

_KEY = re.compile(r"(.+)_([0-9]{8})")

# A dropped reference is logged by repr, cut to this many characters.
_SAMPLE_LENGTH = 80


def split_sheet_key(value: Any) -> Optional[Tuple[str, str]]:
    """(sku, "MMDDYYYY") for anything shaped like a sheet key, the date unchecked, or None."""
    if not isinstance(value, str):
        return None
    match = _KEY.fullmatch(value.strip())
    return (match.group(1), match.group(2)) if match else None


def parse_sheet_key(value: Any) -> Optional[Tuple[str, date]]:
    """(sku, date) for a sheet key, or None for anything that is not one."""
    split = split_sheet_key(value)
    if split is None:
        return None
    sku, mmddyyyy = split
    try:
        return sku, date(int(mmddyyyy[4:]), int(mmddyyyy[:2]), int(mmddyyyy[2:4]))
    except ValueError:
        return None


def sheet_keys_by_sku(references: Optional[Iterable[Any]]) -> dict[str, str]:
    """{sku: key} for the keys in an added_references list. Anything else is dropped, with
    a warning naming how many and one of them: its child gets no id."""
    keys: dict[str, str] = {}
    dropped: list[Any] = []
    total = 0
    for ref in references or []:
        total += 1
        parsed = parse_sheet_key(ref)
        if parsed:
            keys[parsed[0]] = ref.strip()
        else:
            dropped.append(ref)
    if dropped:
        logger.warning(
            "Grailed: dropped %d of %d added references, not <sku>_<MMDDYYYY> keys; sample %s",
            len(dropped),
            total,
            repr(dropped[0])[:_SAMPLE_LENGTH],
        )
    return keys
