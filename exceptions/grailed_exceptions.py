"""Grailed Drive result-file errors, split by what the poller should DO about them.

  GrailedDriveTransientError  the call never completed (429, 5xx, a rate-limit 403,
                              a timeout, no access token). Try again next cycle.
  GrailedDrivePermanentError  a retry cannot fix it (folder or file not found,
                              access revoked, an unsupported file type). Surface it.

`str(exc)` is a short sentence prefixed "Grailed Drive: ", safe to show and store.
`.detail` carries the technical text. Both go through goat_exceptions.redact(),
because Google's 4xx bodies echo the service-account address and token errors can
too.
"""

from typing import Any

from exceptions.goat_exceptions import redact

PREFIX = "Grailed Drive: "


class GrailedDriveError(Exception):
    """Base. `str(self)` is operator-safe; `.detail` is technical and redacted."""

    def __init__(self, message: str, *, detail: Any | None = None) -> None:
        text = str(message)
        if not text.startswith(PREFIX):
            text = PREFIX + text
        text = redact(text)
        super().__init__(text)
        self.detail = redact(detail if detail is not None else text)


class GrailedDriveTransientError(GrailedDriveError):
    """The call did not complete. Try again later."""


class GrailedDrivePermanentError(GrailedDriveError):
    """A retry cannot fix this."""
