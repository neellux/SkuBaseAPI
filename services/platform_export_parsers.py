"""Turn a platform's own product export into a set of child SKUs plus skipped rows.

One parser per platform behind a single interface (KTD6): everything downstream of here
is platform independent. A platform with no parser offers no Import control (R1), which
is why has_parser is part of the public surface.

What a parser does NOT do: it never looks at the platform's validation errors. A row's
presence means the platform has that product (R5), so the SPO workbook's Error Details
sheet is ignored entirely. It also never decides anything about SkuBase's own records;
whether a parent is known, listed, or stale is the planner's job (U3).
"""

import io
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ExportRefused(Exception):
    """The parser cannot recognise the file (R6). Nothing is staged.

    The message is operator-facing and goes straight into a snackbar, so it stays one
    short line and says what to do about it.
    """


# Skip reasons a parser can produce. The planner adds its own for a SKU the products
# database does not know; both end up in the preview's "cannot act on" block.
BLANK = "Empty SKU cell"


@dataclass
class ParsedExport:
    """The parser's whole output. Nothing here has touched the database."""

    platform_id: str
    skus: list[str]
    skipped: list[dict[str, Any]] = field(default_factory=list)
    rows_read: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "skus": len(self.skus),
            "skipped": len(self.skipped),
        }


def _read_table(content: bytes, filename: str, sheet: Optional[str]):
    """Read an upload into a DataFrame, following ProductService.validate_bulk_import.

    pandas is already the repo's reader for uploaded spreadsheets, and dtype=str keeps a
    numeric-looking size such as 38.5 from arriving as a float and stringifying back
    differently than the SKU in the database.
    """
    import pandas as pd

    name = (filename or "").lower()
    try:
        if name.endswith(".csv"):
            # September's SPO feed was a CSV of exactly this sheet, so CSV stays in.
            return pd.read_csv(io.BytesIO(content), dtype=str)
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(content), sheet_name=sheet, dtype=str)
    except ValueError as exc:
        # pandas raises ValueError for a missing sheet_name.
        if sheet and sheet.lower() in str(exc).lower():
            raise ExportRefused(f"This file has no {sheet} sheet") from exc
        raise ExportRefused("Could not read this file") from exc
    except Exception as exc:
        logger.warning("Export parse failed for %s: %s", filename, exc)
        raise ExportRefused("Could not read this file") from exc

    raise ExportRefused("Use a .xlsx or .csv export")


def _sku_column(df, column: str) -> str:
    """Find the SKU column case-insensitively, or refuse (R6)."""
    found = {str(c).strip().lower(): c for c in df.columns}
    key = column.strip().lower()
    if key not in found:
        raise ExportRefused(f"No {column} column in this file")
    return found[key]


def parse_spo_export(content: bytes, filename: str) -> ParsedExport:
    """Parse the SPO website product export.

    Shape: a `Data` sheet whose first row is human headers and whose second row is the
    api codes SPO imports by (`sku`, `variantId`, `image-link-1`). The SKU column holds
    `<parent>/<size>`.
    """
    df = _read_table(content, filename, sheet="Data")
    col = _sku_column(df, "SKU")

    values = [str(v).strip() for v in df[col].dropna().tolist()]
    values = [v for v in values if v and v.lower() != "nan"]

    # Drop the api-code row by what it says, not by its position: its SKU cell is the
    # literal api code "sku". Position alone is fragile, and "has no slash" cannot be
    # the test because real rows legitimately lack one.
    if values and values[0].lower() == col.strip().lower():
        values = values[1:]

    if not values:
        # The safety-critical refusal. An empty or wrong-sheet file otherwise reconciles
        # to "the platform has nothing", which would delist the entire catalog.
        raise ExportRefused("No SKUs found in this file")

    skus: list[str] = []
    seen: set[str] = set()
    skipped: list[dict[str, Any]] = []
    for value in values:
        # Every value is a child SKU candidate. A parser never inspects its SHAPE: a
        # child sku is not reliably "<parent>/<size>". Across the products database 1.7%
        # of active children are not that shape - 1,579 where the child sku IS the parent
        # sku (single-size products), 1,130 "<parent>/<something else>", and 133 with a
        # non-slash suffix such as MFMSP08-1. Requiring a "/" here discarded 16 products
        # SPO really lists, and because a discarded row is not "in the file" the reconcile
        # would then have delisted all 16. Resolving a sku to its parent is a database
        # lookup, which is the planner's job.
        if value not in seen:
            seen.add(value)
            skus.append(value)

    return ParsedExport(
        platform_id="spo",
        skus=skus,
        skipped=skipped,
        rows_read=len(values),
    )


# platform_id -> parser. Absence is meaningful: it is what hides the Import control.
PARSERS: dict[str, Callable[[bytes, str], ParsedExport]] = {
    "spo": parse_spo_export,
}


def has_parser(platform_id: str) -> bool:
    return platform_id in PARSERS


def supported_platforms() -> list[str]:
    return sorted(PARSERS)


def parse(platform_id: str, content: bytes, filename: str) -> ParsedExport:
    """Parse an upload for one platform. Raises ExportRefused."""
    parser = PARSERS.get(platform_id)
    if parser is None:
        raise ExportRefused(f"No import parser for {platform_id}")
    if not content:
        raise ExportRefused("That file is empty")
    return parser(content, filename)
