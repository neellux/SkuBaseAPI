import logging

from fastapi import APIRouter, HTTPException
from models.api_models import BatchResponse, CreateBatchRequest
from services.batch_service import BatchService
from utils.load_app_data import add_user_data
from exceptions.batch_exceptions import BatchCreationError

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/create_batch", response_model=BatchResponse)
async def create_batch_public(request_data: CreateBatchRequest):
    created_by = "system"

    try:
        batch = await BatchService.create_batch(request_data, created_by)

        batch_dict = batch.model_dump()
        batch_with_user_data = await add_user_data(
            data=batch_dict, keys=["assigned_to", "created_by"], new_keys=["name"]
        )

        if batch_with_user_data.get("listings"):
            batch_with_user_data["listings"] = await add_user_data(
                data=batch_with_user_data["listings"],
                keys=["assigned_to", "created_by"],
                new_keys=["name"],
            )

        return batch_with_user_data

    except BatchCreationError as e:
        logger.error(f"Batch creation failed: {e}")
        raise HTTPException(status_code=400, detail=e.to_dict())
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create batch: {str(e)}")
