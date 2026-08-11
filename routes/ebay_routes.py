import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from services.ebay_aspect_service import DEFAULT_MARKETPLACE, ebay_aspect_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ebay", tags=["ebay"])


class EbayAspectSettingsEntry(BaseModel):
    """Operator-owned settings for one aspect NAME, across every category offering it.

    Deliberately carries no eBay-derived field. There is no `is_required`, `options`,
    `cardinality` or `max` here: those are eBay's, they differ per category, and storing a
    copy would let it survive a tree reload and mask the newer truth.
    """

    aspect_name: str
    enabled: bool = True
    source: str = Field(
        default="type_based",
        description="mapped_field, form or type_based",
    )
    mapped_field: Optional[str] = None
    mapped_table: Optional[str] = None
    mapped_column: Optional[str] = None

    display_name: Optional[str] = None
    # The SellerCloud import-file column this aspect becomes (Size -> EbaySize). Free text:
    # no convention is settled. Null falls back to a derived default at read time.
    sellercloud_field: Optional[str] = None
    # The one field here that is NOT aspect level: the default value for the category named
    # in the query string, and only that one. Omit the key entirely to leave the stored map
    # alone; send it as null to clear this category's entry. The service rejects it when no
    # category_id accompanies the request.
    category_default: Optional[Any] = None
    ai_tagging: bool = False
    ui_size: Optional[int] = Field(default=None, ge=1, le=12)
    min_length: Optional[int] = Field(default=None, ge=0)
    regex: Optional[str] = None
    default_value: Optional[Any] = None


class SaveAspectSettingsRequest(BaseModel):

    settings: List[EbayAspectSettingsEntry]


@router.get("/categories")
async def list_mapped_categories(
    marketplace_id: str = Query(DEFAULT_MARKETPLACE, description="eBay marketplace"),
    include_unmapped_types: bool = Query(
        False, description="Also return Lux types with no usable eBay category"
    ),
):
    """eBay categories that at least one Lux product type maps to.

    Deliberately scoped: a category nothing maps to can never be reached by a listing.
    """
    try:
        categories = await ebay_aspect_service.get_mapped_categories(marketplace_id)
        payload: Dict[str, Any] = {"categories": categories, "count": len(categories)}
        if include_unmapped_types:
            payload["unmapped_types"] = await ebay_aspect_service.get_unmapped_types()
        return payload
    except Exception as e:
        logger.error(f"Error listing eBay categories: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not load eBay categories")


@router.get("/category_aspects")
async def get_category_aspects(
    category_id: str = Query(..., description="eBay leaf category id"),
    marketplace_id: str = Query(DEFAULT_MARKETPLACE),
):
    """Every aspect a category offers, merged with its saved configuration."""
    try:
        result = await ebay_aspect_service.get_category_aspects(category_id, marketplace_id)
    except Exception as e:
        logger.error(f"Error loading aspects for {category_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not load category aspects")

    if result is None:
        raise HTTPException(status_code=404, detail=f"Unknown eBay category {category_id}")
    return result


@router.get("/aspect_values")
async def search_aspect_values(
    values_id: str = Query(..., description="Value list reference from an aspect"),
    search: str = Query("", description="Substring filter"),
    limit: int = Query(50, ge=1, le=500),
):
    """Typeahead over one stored value list.

    Used for the 8.5% of lists too large to inline; the largest holds 79,116 values.
    """
    try:
        return await ebay_aspect_service.search_aspect_values(values_id, search, limit)
    except Exception as e:
        logger.error(f"Error searching aspect values {values_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not search aspect values")


@router.get("/listing_defaults")
async def get_listing_defaults(
    product_type: str = Query(..., description="Lux product type of the listing"),
    brand: Optional[str] = Query(
        None, description="Listing's brand, for the platform-exclusion check"
    ),
    ebay_category_id: Optional[str] = Query(
        None,
        description=(
            "The listing's chosen category. Ignored unless it is one the type maps to; "
            "omitted falls back to the type's default."
        ),
    ),
):
    """Aspects carrying a default for the eBay category this product type maps to.

    Defaults are never written into a listing's data, so this is the only way a listing
    learns what it will send for an aspect nobody filled in. A value in listings.data under
    the aspect name is an override and wins.

    `excluded_by` comes back as "type" or "brand" when this listing is not going to eBay at
    all, and the caller hides the whole eBay section rather than showing an empty one.
    """
    try:
        category_id = await ebay_aspect_service.resolve_listing_category(
            product_type, ebay_category_id
        )
        if not category_id:
            return {"category": None, "aspects": []}
        result = await ebay_aspect_service.get_listing_defaults(
            product_type, category_id, brand
        )
    except Exception as e:
        logger.error(f"Error loading eBay defaults for {product_type!r}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not load eBay defaults")

    # A type with no eBay category is the normal state for 103 of 239 types, not an error.
    return result or {"category": None, "aspects": []}


@router.get("/aspect_impact")
async def get_aspect_impact(
    aspect_name: str = Query(..., description="eBay aspect name"),
    marketplace_id: str = Query(DEFAULT_MARKETPLACE),
):
    """Which categories an aspect-level change reaches, and where eBay overrides it.

    Settings are per aspect name, so the editor shows this count before saving.
    """
    try:
        return await ebay_aspect_service.get_aspect_impact(aspect_name, marketplace_id)
    except Exception as e:
        logger.error(f"Error computing impact for {aspect_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not compute aspect impact")


@router.put("/aspect_settings")
async def save_aspect_settings(
    request: SaveAspectSettingsRequest,
    category_id: Optional[str] = Query(
        None, description="Category to return refreshed after saving"
    ),
    marketplace_id: str = Query(DEFAULT_MARKETPLACE),
):
    """Upsert aspect-level settings.

    These apply to every category offering the aspect, which is why the editor states the
    count first. Only aspects in the payload are touched, so a save from a filtered view
    cannot wipe settings the operator could not see.
    """
    # The re-read is inside the try on purpose. Left outside, a failure there escaped this
    # handler entirely and surfaced as the generic "Internal server error", which hid a
    # real bug in the response builder behind a message that pointed nowhere.
    # `category_default` has three states, not two: absent, null (clear this category's
    # entry) and a value. model_dump() flattens absent into null, which would have every
    # save from a view that does not touch defaults quietly clear them, so the key is
    # dropped again unless the client actually sent it.
    payload = []
    for entry in request.settings:
        data = entry.model_dump()
        if "category_default" not in entry.model_fields_set:
            data.pop("category_default", None)
        payload.append(data)

    try:
        written = await ebay_aspect_service.save_aspect_settings(
            payload, marketplace_id, category_id
        )
        refreshed = (
            await ebay_aspect_service.get_category_aspects(category_id, marketplace_id)
            if category_id
            else None
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error saving aspect settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not save aspect settings")

    return (refreshed or {}) | {"saved": written}


@router.post("/acknowledge_changes")
async def acknowledge_changes(
    aspect_name: str = Query(..., description="eBay aspect name"),
    marketplace_id: str = Query(DEFAULT_MARKETPLACE),
):
    """Clear the reload-change flags for one aspect once the operator has seen them."""
    try:
        return {"acknowledged": await ebay_aspect_service.acknowledge_changes(
            aspect_name, marketplace_id
        )}
    except Exception as e:
        logger.error(f"Error acknowledging changes for {aspect_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not acknowledge changes")
