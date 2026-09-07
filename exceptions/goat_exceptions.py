"""GOAT platform errors, split by what the poller should DO about them.

The split is the codebase's existing "did we actually send it?" rule, made into
types so the poller never has to re-derive it from an HTTP status:

  GoatBuildError      the row could not be built (unmapped brand/colour, missing
                      goat_code). Nothing left the building. Fail that one row,
                      keep the batch going.
  GoatTransientError  the call never executed (429, 5xx, timeout, a Drive rate
                      limit). Retrying is still ONE genuine attempt, so the rows
                      requeue. Same contract as ShopifyTransientError.
  GoatPermanentError  a retry cannot fix it (bad id, revoked access, quota).
                      Fail.

`str(exc)` is safe to put in error_display: it is a short sentence naming the
thing that went wrong, with no credential, file id or service-account address in
it. `.detail` carries the technical text and is redacted before it is stored,
because Google's 4xx bodies echo the service account email and the file id.
"""

import re
from typing import Any

# Google error bodies echo the caller's identity and the file it was reaching
# for. Neither belongs in platform_meta, which is append-only and permanent.
_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com"), "<service-account>"),
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "<email>"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S), "<key>"),
    (re.compile(r"\b(ya29|shpat|shpss)\.[\w.\-]+"), "<token>"),
)

MAX_DETAIL_LENGTH = 2000


def redact(text: Any) -> str:
    """Strip credentials and identities out of text bound for storage."""
    out = str(text)
    for pattern, replacement in _REDACTIONS:
        out = pattern.sub(replacement, out)
    return out[:MAX_DETAIL_LENGTH]


class GoatError(Exception):
    """Base. `str(self)` is operator-safe; `.detail` is technical and redacted."""

    def __init__(self, message: str, *, detail: Any | None = None) -> None:
        super().__init__(message)
        self.detail = redact(detail if detail is not None else message)


class GoatBuildError(GoatError):
    """The row could not be built. One row fails; the batch continues.

    The message names the offending value (brand, colour, sizing scheme) because
    it is shown to the operator as error_display and "Failed to build the row"
    tells them nothing actionable.
    """


class GoatTransientError(GoatError):
    """The call did not execute. Requeue; this is still one genuine attempt."""


class GoatPermanentError(GoatError):
    """A retry cannot fix this. Fail the row."""
