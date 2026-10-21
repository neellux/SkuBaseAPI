"""Platform-specific actions that run after a delist, a Restore or a listing ID saved by hand.

A delist, a Restore and an ID save change SkuBase's own records (platform_delist_service,
listing_id_service). Some platforms also need something done outside SkuBase when that
happens. Each such platform registers a Handler in HANDLERS; a platform with none runs
nothing.

    goat, goat_sneakers   the GOAT link on the 1nventory Shopify product
                          (goat_shopify_link): the goat.goat_sku metafield, and the
                          gsync tag that stands for STV

The hooks run AFTER the SkuBase change commits, outside its transaction and lock, and never
raise: the change is true whatever the platform says, so it stands either way. A handler
records its outcome where its hook says and logs it; nothing is shown in the UI.

    after_delist(platform_id, delist_id, parent_sku, scope, child_skus)
        scope None: the whole product. Its outcome goes on the entry,
        platform_delists.platform_action (migrations/add_platform_delists_platform_action.sql),
        which is also what its after_restore reads back.
    after_restore(platform_id, delist_id, parent_sku)
        the entry was restored, so undo what after_delist did.
    after_id_change(platform_id, parent_sku, child_skus, changes, submission_id)
        ids saved by hand: changes are {"sku", "level", "old", "new"} with a new id
        (a cleared id is a delist, and runs after_delist instead). Its outcome goes on the
        manual row, submission_id, as steps.
"""

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from services import goat_shopify_link

logger = logging.getLogger(__name__)

Hook = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class Handler:
    after_delist: Optional[Hook] = None
    after_restore: Optional[Hook] = None
    after_id_change: Optional[Hook] = None


_GOAT = Handler(
    after_delist=goat_shopify_link.after_delist,
    after_restore=goat_shopify_link.after_restore,
    after_id_change=goat_shopify_link.after_id_change,
)

HANDLERS: Dict[str, Handler] = {platform_id: _GOAT for platform_id in goat_shopify_link.VARIANTS}


async def _run(hook: str, platform_id: str, what: str, **kwargs: Any) -> None:
    run = getattr(HANDLERS.get(platform_id), hook, None)
    if run is None:
        return
    try:
        await run(platform_id=platform_id, **kwargs)
    except Exception:
        logger.exception("platform action %s on %s failed for %s", hook, platform_id, what)


async def after_delist(
    *,
    platform_id: str,
    delist_id: str,
    parent_sku: str,
    scope: Optional[Sequence[str]],
    child_skus: Sequence[str],
) -> None:
    await _run(
        "after_delist",
        platform_id,
        f"delist {delist_id} of {parent_sku}",
        delist_id=delist_id,
        parent_sku=parent_sku,
        scope=scope,
        child_skus=child_skus,
    )


async def after_restore(*, platform_id: str, delist_id: str, parent_sku: str) -> None:
    await _run(
        "after_restore",
        platform_id,
        f"restored delist {delist_id} of {parent_sku}",
        delist_id=delist_id,
        parent_sku=parent_sku,
    )


async def after_id_change(
    *,
    platform_id: str,
    parent_sku: str,
    child_skus: Sequence[str],
    changes: Sequence[Dict[str, Any]],
    submission_id: int,
) -> None:
    await _run(
        "after_id_change",
        platform_id,
        f"manual row {submission_id} of {parent_sku}",
        parent_sku=parent_sku,
        child_skus=child_skus,
        changes=changes,
        submission_id=submission_id,
    )
