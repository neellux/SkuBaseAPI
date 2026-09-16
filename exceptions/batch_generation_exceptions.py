"""Errors raised while generating a batch's listings in the background.

Classified once, here, so GenerationPoller decides retry, failure or pause by catching a
type rather than by string-matching messages at every call site. Mirrors
exceptions/ai_search_exceptions.py.

Every `message` is one of the fixed strings below. It is stored as
batch_generation_jobs.error_display and shown to operators, so it must never carry a
provider's raw text; that goes in `detail`, which only reaches the log and the job's
`error` column.
"""

import asyncio
from typing import Optional

PRODUCT_GONE = "Product no longer exists"
NOT_IN_SELLERCLOUD = "Not found in SellerCloud"
NO_IMAGES = "No product images found"
IMAGE_STORE_UNAVAILABLE = "Image store unavailable"
RATE_LIMITED = "AI provider rate limited"
AI_FAILED = "AI generation failed"
SELLERCLOUD_UNAVAILABLE = "SellerCloud unavailable"
TIMED_OUT = "Generation timed out"
SAVE_FAILED = "Could not save listing"
GENERIC = "Generation error"
PAUSED = "Generation paused"

DISPLAY_STRINGS = frozenset(
    {
        PRODUCT_GONE,
        NOT_IN_SELLERCLOUD,
        NO_IMAGES,
        IMAGE_STORE_UNAVAILABLE,
        RATE_LIMITED,
        AI_FAILED,
        SELLERCLOUD_UNAVAILABLE,
        TIMED_OUT,
        SAVE_FAILED,
        GENERIC,
        PAUSED,
    }
)

# Substrings meaning "the provider is throttling". Only looked at where a raw SDK
# exception is first converted; nothing downstream reads exception text.
RATE_LIMIT_MARKERS = ("RESOURCE_EXHAUSTED", "429", "rate limit", "RateLimitError", "insufficient_quota")


class GenerationError(Exception):
    """Base for anything that stops one product's listing being generated."""

    def __init__(self, message: str, detail: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.detail = detail


class TransientGenerationError(GenerationError):
    """Worth another attempt later.

    `trips_breaker` marks failures that say the outside world is unhealthy (the model
    provider, SellerCloud, the image store), as opposed to one odd product; enough of them
    in a row pause all claiming. `rate_limited` also starts a short cool-off.
    """

    def __init__(
        self,
        message: str,
        detail: Optional[str] = None,
        retry_after: Optional[float] = None,
        rate_limited: bool = False,
        trips_breaker: bool = True,
    ):
        super().__init__(message, detail)
        self.retry_after = retry_after
        self.rate_limited = rate_limited
        self.trips_breaker = trips_breaker


class PermanentGenerationError(GenerationError):
    """Retrying cannot help: the product is gone, unknown to SellerCloud, or has no photos."""


class AIGenerationError(TransientGenerationError):
    """A model call failed in a path that requires AI (ListingService require_ai=True).

    Raised instead of the silent `{}` / None the standalone listing path keeps, which is how
    a provider outage used to produce listings with no AI content at all.
    """

    def __init__(self, detail: str):
        rate_limited = any(marker in detail for marker in RATE_LIMIT_MARKERS)
        super().__init__(
            RATE_LIMITED if rate_limited else AI_FAILED,
            detail=detail,
            rate_limited=rate_limited,
        )


class LeaseLost(Exception):
    """The job was requeued, removed, or its batch deleted while this attempt ran.

    Not a GenerationError: nothing is recorded, because another attempt (or nobody) now owns
    the job and anything this attempt wrote has been rolled back.
    """


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:500]}"


def classify(exc: BaseException) -> GenerationError:
    """Convert anything raised by an attempt into a GenerationError."""
    if isinstance(exc, GenerationError):
        return exc

    # Imported here: exception modules stay free of service imports.
    from services.product_resolver import SkuResolutionError

    if isinstance(exc, SkuResolutionError):
        return PermanentGenerationError(PRODUCT_GONE, detail=_describe(exc))

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return TransientGenerationError(TIMED_OUT, detail=_describe(exc))

    text = _describe(exc)
    if any(marker in text for marker in RATE_LIMIT_MARKERS):
        return TransientGenerationError(RATE_LIMITED, detail=text, rate_limited=True)

    try:
        import httpx

        if isinstance(exc, httpx.HTTPError):
            # Model SDK errors arrive wrapped as AIGenerationError, so a bare httpx error
            # here comes from the SellerCloud client.
            return TransientGenerationError(SELLERCLOUD_UNAVAILABLE, detail=text)
    except ImportError:  # pragma: no cover
        pass

    try:
        from tortoise.exceptions import BaseORMException

        if isinstance(exc, BaseORMException):
            return TransientGenerationError(SAVE_FAILED, detail=text, trips_breaker=False)
    except ImportError:  # pragma: no cover
        pass

    try:
        import asyncpg

        if isinstance(exc, asyncpg.PostgresError):
            return TransientGenerationError(SAVE_FAILED, detail=text, trips_breaker=False)
    except ImportError:  # pragma: no cover
        pass

    return TransientGenerationError(GENERIC, detail=text, trips_breaker=False)
