"""Batch view reads and the operator actions for background generation.

GET  /listings/batch/products           the batch header plus one slim row per product
POST /listings/batch/generation/retry   requeue failed products (one, or all in a batch)
POST /listings/batch/generation/remove  drop a queued or failed product from its batch

Query parameters only. The POST routes also require X-Requested-With: SkuBase, which the
UI's sendRequest sets on every request: a custom header forces a CORS preflight, so a
cross-site form cannot drive them with the operator's cookie.

Registered with the auth service like every other route: the GET under view_batches, both
POSTs under manage_batches.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from models.api_models import (
    BatchProductsResponse,
    GenerationRemoveResponse,
    GenerationRetryResponse,
)
from services import generation_queue
from services.batch_service import BatchService
from utils.load_app_data import add_user_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/listings", tags=["batch generation"])

SKUBASE_CLIENT_HEADER = "SkuBase"


async def require_skubase_client(
    x_requested_with: Optional[str] = Header(None, alias="X-Requested-With"),
) -> None:
    if x_requested_with != SKUBASE_CLIENT_HEADER:
        raise HTTPException(status_code=403, detail="Request not allowed")


def _user_id(request: Request) -> Optional[str]:
    user = getattr(request.state, "user", None) or {}
    return user.get("id")


def _kick() -> None:
    try:
        from services.generation_poller import generation_poller

        generation_poller.kick()
    except Exception:  # noqa: BLE001
        logger.exception("Could not kick the generation poller; its interval will pick the work up")


@router.get("/batch/products", response_model=BatchProductsResponse)
async def get_batch_products(
    batch_id: int = Query(..., description="Batch ID"),
    if_version: Optional[str] = Query(
        None,
        max_length=64,
        description="The version from the previous response; a match returns {unchanged: true}",
    ),
    include_values: bool = Query(
        False,
        description="Include product_values in the header (large); the first load wants it, polls do not",
    ),
):
    result = await BatchService.get_batch_products(
        batch_id, if_version=if_version, include_values=include_values
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    if not result.get("unchanged"):
        result["batch"] = await add_user_data(
            data=result["batch"], keys=["assigned_to", "created_by"], new_keys=["name"]
        )
    return result


@router.post(
    "/batch/generation/retry",
    response_model=GenerationRetryResponse,
    dependencies=[Depends(require_skubase_client)],
)
async def retry_generation(
    request: Request,
    batch_id: int = Query(..., description="Batch ID"),
    job_id: Optional[int] = Query(
        None, description="One product's job; omitted, every failed product in the batch"
    ),
):
    requeued = await generation_queue.retry_failed(batch_id, job_id)
    if job_id is not None and requeued == 0:
        raise HTTPException(status_code=404, detail="Nothing to retry")
    logger.info(
        f"Generation retry by {_user_id(request)}: batch {batch_id}, "
        f"job {job_id if job_id is not None else 'all failed'}, {requeued} requeued"
    )
    if requeued:
        _kick()
    return {"requeued": requeued}


@router.post(
    "/batch/generation/remove",
    response_model=GenerationRemoveResponse,
    dependencies=[Depends(require_skubase_client)],
)
async def remove_generation_job(
    request: Request,
    job_id: int = Query(..., description="The product's generation job"),
):
    result = await generation_queue.remove_job(job_id)
    if not result["removed"]:
        reason = result["reason"]
        if reason == "running":
            raise HTTPException(status_code=409, detail="Still generating, try again in a moment")
        if reason == "done":
            raise HTTPException(status_code=409, detail="Already generated")
        raise HTTPException(status_code=404, detail="Not found")

    # A removed job leaves no row behind, so the log is the record.
    logger.info(
        f"Generation job removed by {_user_id(request)}: job {job_id}, "
        f"batch {result['batch_id']}, product {result['product_id']}"
    )
    return result
