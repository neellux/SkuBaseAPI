"""Errors raised by ProductVerificationService.

Classified once, here, so the poller can decide whether to retry by catching a
type rather than by string-matching an exception message. Gemini reports a rate
limit as "429 RESOURCE_EXHAUSTED" and a hung backend as "503 UNAVAILABLE ...
Deadline expired", and the prototype's poller matched on those substrings; that
worked but put the retry policy in the wrong place and broke silently the first
time a message changed.
"""

from typing import Optional


class AISearchError(Exception):
    """Base for anything that stops a listing being verified."""

    def __init__(self, message: str, detail: Optional[str] = None):
        super().__init__(message)
        # `message` is short and operator-facing: it lands in the stored result
        # and, through it, in the UI. `detail` carries the provider's own text
        # for the log and the job row.
        self.message = message
        self.detail = detail


class TransientAISearchError(AISearchError):
    """Worth trying again later: rate limits, 503s, network trouble.

    `retry_after` is the provider's own hint in seconds when it gives one. The
    poller uses it in place of its own backoff curve, since a server that tells
    you when to come back knows better than a doubling constant does.
    """

    def __init__(self, message: str, detail: Optional[str] = None, retry_after: Optional[float] = None):
        super().__init__(message, detail)
        self.retry_after = retry_after


class PermanentAISearchError(AISearchError):
    """Retrying cannot help: nothing to search on, no key, a rejected request.

    Marked failed on the first occurrence. A listing with no brand, no MPN and no
    style name will still have none of those in ten minutes, and burning three
    attempts and three grounded prompts to rediscover that costs real money.
    """


# Substrings that mean "the provider is busy or throttling", used only at the
# boundary where a raw SDK exception is first caught and converted. Nothing
# downstream of that point looks at exception text.
TRANSIENT_MARKERS = (
    "RESOURCE_EXHAUSTED",
    "429",
    "503",
    "UNAVAILABLE",
    "Deadline",
    "DEADLINE_EXCEEDED",
    "INTERNAL",
    "500",
)


def classify(exc: Exception) -> AISearchError:
    """Convert a raw SDK or transport exception into one of the two classes."""
    if isinstance(exc, AISearchError):
        return exc
    text = str(exc)
    if any(marker in text for marker in TRANSIENT_MARKERS):
        return TransientAISearchError(
            "The AI search service is busy", detail=f"{type(exc).__name__}: {text[:500]}"
        )
    return PermanentAISearchError(
        "Verification failed", detail=f"{type(exc).__name__}: {text[:500]}"
    )
