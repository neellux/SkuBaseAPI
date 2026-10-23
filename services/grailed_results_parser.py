"""Parse the files Grailed's sheet processor writes to Drive after each run.

Every run (about every 2 hours) writes two files under
``<root>/<YYYY>/<Mon>/<M/D/YYYY>/{Success,Errors,Other}``:

- ``<account>-successes-YYYY-MM-DD--HHhMM.csv``: one row per event this run ("Listing
  created! ID: N", "Listing updated! ID: N - price", "Listing deleted! ID: N").
- ``<account>-errors-YYYY-MM-DD--HHhMM.csv``: a snapshot of every sheet row the processor
  currently refuses, one message per row.

In the real root these are native Google Sheets exported as CSV; an older temp copy holds
uploaded ``.csv.xlsx`` files, so both formats parse to the same rows. The stamp in the name
is UTC. Day folders are named ``10/21/2026`` in the real root and ``10-21-2026`` in the
temp copy.

The columns are the Grailed sheet's own, found by header name: ``external_seller_reference``
(``<child sku>_<MMDDYYYY>``) and ``status_message`` (successes) or ``error_message``
(errors). An empty file is reported as empty rather than as "no results": an empty error
file is not proof that nothing failed.

Pure parsing: no I/O, no database, no config. Callers fetch the bytes and decide what the
rows mean.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional

from utils.grailed_keys import split_sheet_key

REFERENCE_COLUMN = "external_seller_reference"
MESSAGE_COLUMNS = {"successes": "status_message", "errors": "error_message"}
FORMATS = ("csv", "xlsx")

CREATED = "created"
UPDATED = "updated"
DELETED = "deleted"
ERROR = "error"
SOLD_DELETE_ERROR = "sold_delete_error"
UNKNOWN = "unknown"

_CREATED = re.compile(r"Listing created! ID:\s*([0-9]+)")
_UPDATED = re.compile(r"Listing updated! ID:\s*([0-9]+)(?:\s+-\s+.+)?", re.DOTALL)
_DELETED = re.compile(r"Listing deleted! ID:\s*([0-9]+)")
_SOLD_DELETE = "error deleting listing that has already sold"

_RUN_NAME = re.compile(
    r"[0-9]+-(successes|errors)-([0-9]{4}-[0-9]{2}-[0-9]{2}--[0-9]{2}h[0-9]{2})\.csv(?:\.xlsx)?"
)
_RUN_KEY = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})--([0-9]{2})h([0-9]{2})")
_YEAR_FOLDER = re.compile(r"[0-9]{4}")
_DATE_FOLDER = re.compile(r"([0-9]{1,2})([/-])([0-9]{1,2})\2([0-9]{4})")

# English on purpose: calendar.month_name follows the process locale.
_MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)

# A header listed in a parse error is cut to this many characters.
_HEADER_SAMPLE_LENGTH = 200


@dataclass(frozen=True)
class ResultRow:
    sku: str
    reference: str
    kind: str  # created | updated | deleted | error | sold_delete_error | unknown
    grailed_id: Optional[str]  # digits after "ID: " in a success message, else None
    message: str  # verbatim, stripped


@dataclass(frozen=True)
class ParsedFile:
    rows: tuple[ResultRow, ...]
    empty: bool  # no data rows at all, including no header
    unknown_count: int  # rows whose message matched no known shape


class ResultParseError(ValueError):
    """A result file that cannot be read, or has rows but not the columns we read."""


# --- run files --------------------------------------------------------------------


def parse_run_file(data: bytes, fmt: str, file_kind: str, name: str = "") -> ParsedFile:
    """Typed rows from one run file.

    ``fmt`` is "csv" (UTF-8, BOM optional) or "xlsx" (first sheet). ``file_kind`` is
    "successes" or "errors". Rows with a blank reference are skipped; they still make the
    file non-empty.
    """
    if fmt not in FORMATS:
        raise ValueError(f"unknown Grailed result file format: {fmt!r}")
    if file_kind not in MESSAGE_COLUMNS:
        raise ValueError(f"unknown Grailed result file kind: {file_kind!r}")

    table = _read_csv(data, name) if fmt == "csv" else _read_xlsx(data, name)
    return _parse_table(table, file_kind, name)


def classify(message: str, file_kind: str) -> tuple[str, Optional[str]]:
    """(kind, grailed_id) for one stripped message from a file of ``file_kind``."""
    if file_kind == "errors":
        if message.casefold().startswith(_SOLD_DELETE):
            return SOLD_DELETE_ERROR, None
        return ERROR, None
    for kind, pattern in ((CREATED, _CREATED), (UPDATED, _UPDATED), (DELETED, _DELETED)):
        match = pattern.fullmatch(message)
        if match:
            return kind, match.group(1)
    return UNKNOWN, None


def sku_of(reference: str) -> str:
    """The child sku: a final "_" plus 8 digits removed, otherwise the whole reference."""
    split = split_sheet_key(reference)
    return split[0] if split else reference


def _parse_table(table: list[list[Any]], file_kind: str, name: str) -> ParsedFile:
    lines = iter(table)
    header = None
    for line in lines:
        if _has_content(line):
            header = [_text(cell).replace("﻿", "").strip().lower() for cell in line]
            break
    data_lines = [line for line in lines if _has_content(line)]
    if header is None or not data_lines:
        return ParsedFile(rows=(), empty=True, unknown_count=0)

    message_column = MESSAGE_COLUMNS[file_kind]
    missing = [col for col in (REFERENCE_COLUMN, message_column) if col not in header]
    if missing:
        found = ", ".join(h for h in header if h)[:_HEADER_SAMPLE_LENGTH]
        raise ResultParseError(
            f"Grailed result file {_label(name)} has no {' or '.join(missing)} column"
            f" (headers: {found})"
        )
    ref_idx = header.index(REFERENCE_COLUMN)
    msg_idx = header.index(message_column)

    rows = []
    for line in data_lines:
        reference = _cell(line, ref_idx)
        if not reference:
            continue
        message = _cell(line, msg_idx)
        kind, grailed_id = classify(message, file_kind)
        rows.append(
            ResultRow(
                sku=sku_of(reference),
                reference=reference,
                kind=kind,
                grailed_id=grailed_id,
                message=message,
            )
        )
    return ParsedFile(
        rows=tuple(rows),
        empty=False,
        unknown_count=sum(1 for r in rows if r.kind == UNKNOWN),
    )


def _read_csv(data: bytes, name: str) -> list[list[Any]]:
    try:
        text = bytes(data).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ResultParseError(f"Grailed result file {_label(name)} is not UTF-8: {exc}") from exc
    try:
        return list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        raise ResultParseError(
            f"Grailed result file {_label(name)} is not valid CSV: {exc}"
        ) from exc


def _read_xlsx(data: bytes, name: str) -> list[list[Any]]:
    if not data:
        return []
    import openpyxl

    try:
        wb = openpyxl.load_workbook(io.BytesIO(bytes(data)), read_only=True, data_only=True)
    except Exception as exc:  # BadZipFile, KeyError, InvalidFileException, ...
        raise ResultParseError(
            f"Grailed result file {_label(name)} is not a readable workbook: {exc}"
        ) from exc
    try:
        if not wb.worksheets:
            return []
        ws = wb.worksheets[0]
        # Read-only mode trusts the file's declared dimensions; exporters get them wrong.
        ws.reset_dimensions()
        return [list(line) for line in ws.iter_rows(values_only=True)]
    finally:
        wb.close()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _cell(line: list[Any], idx: int) -> str:
    return _text(line[idx]).strip() if idx < len(line) else ""


def _has_content(line: list[Any]) -> bool:
    return any(_text(cell).strip() for cell in line)


def _label(name: str) -> str:
    return repr(name) if name else "(unnamed)"


# --- names --------------------------------------------------------------------------


def parse_run_name(name: str) -> Optional[tuple[str, str]]:
    """("YYYY-MM-DD--HHhMM", "successes" | "errors") for a run file name, else None."""
    if not isinstance(name, str):
        return None
    match = _RUN_NAME.fullmatch(name.strip())
    if not match:
        return None
    run_key = match.group(2)
    try:
        run_stamp_utc(run_key)
    except ValueError:
        return None
    return run_key, match.group(1)


def run_stamp_utc(run_key: str) -> datetime:
    """The UTC moment in a run key such as "2026-10-21--08h31"."""
    match = _RUN_KEY.fullmatch(run_key)
    if not match:
        raise ValueError(f"not a Grailed run key: {run_key!r}")
    year, month, day, hour, minute = (int(part) for part in match.groups())
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def parse_year_folder(name: str) -> Optional[int]:
    """2026 for "2026", else None."""
    text = name.strip() if isinstance(name, str) else ""
    return int(text) if _YEAR_FOLDER.fullmatch(text) else None


def parse_month_folder(name: str) -> Optional[int]:
    """10 for "Oct" (any case; "Sept" and "October" also work), else None."""
    text = name.strip().lower() if isinstance(name, str) else ""
    if len(text) < 3 or not text.isalpha():
        return None
    for number, month in enumerate(_MONTHS, start=1):
        if month.startswith(text):
            return number
    return None


def parse_date_folder(name: str) -> Optional[date]:
    """The date in a "10/21/2026" or "10-21-2026" day folder, else None."""
    text = name.strip() if isinstance(name, str) else ""
    match = _DATE_FOLDER.fullmatch(text)
    if not match:
        return None
    try:
        return date(int(match.group(4)), int(match.group(1)), int(match.group(3)))
    except ValueError:
        return None
