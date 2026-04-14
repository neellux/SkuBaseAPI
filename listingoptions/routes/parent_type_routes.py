from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict
from pydantic import BaseModel, Field

from services.parent_type_service import ParentTypeService
from listingoptions.models.db_models import ParentType, Gender, ReportingCategory
import uuid
import traceback
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/parent_types", tags=["Parent Types"])


class ParentTypeCreateSchema(BaseModel):
    division: str
    dept_code: int
    class_name: str
    class_code_suffix: int = Field(..., ge=0, le=99)
    gender: Gender
    reporting_category: ReportingCategory


class ParentTypeUpdateSchema(BaseModel):
    id: uuid.UUID
    division: str
    dept_code: int
    class_name: str
    class_code_suffix: int = Field(..., ge=0, le=99)
    gender: Gender
    reporting_category: ReportingCategory


class ParentTypeResponse(BaseModel):
    id: uuid.UUID
    division: str
    dept_code: int
    dept: str
    class_code: int
    class_name: str
    gender: Optional[Gender] = None
    reporting_category: Optional[ReportingCategory] = None

    class Config:
        orm_mode = True
        from_attributes = True


class PaginatedParentTypeResponse(BaseModel):
    total: int
    items: List[ParentTypeResponse]


@router.get("", response_model=PaginatedParentTypeResponse)
async def get_all_parent_types(
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    fetch_all: bool = Query(False),
):
    try:
        result = await ParentTypeService.get_all_parent_types(
            search=search, page=page, page_size=page_size, fetch_all=fetch_all
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/divisions", response_model=List[Dict])
async def get_divisions():
    try:
        return await ParentTypeService.get_divisions()
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/genders", response_model=List[str])
async def get_genders():
    return [g.value for g in Gender]


@router.get("/reporting_categories", response_model=List[str])
async def get_reporting_categories():
    return [rc.value for rc in ReportingCategory]


@router.get("/departments", response_model=List[Dict])
async def get_departments(division: str):
    try:
        return await ParentTypeService.get_departments(division)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/classes", response_model=List[Dict])
async def get_classes(division: str, dept: str):
    try:
        return await ParentTypeService.get_classes(division, dept)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/hierarchy", response_model=Dict)
async def get_parent_hierarchy(
    parent_id: str = Query(..., description="Parent type ID"),
):
    try:
        return await ParentTypeService.get_parent_hierarchy(parent_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("", status_code=204)
async def delete_parent_type(id: uuid.UUID = Query(...)):
    try:
        await ParentTypeService.delete_parent_type(id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("", response_model=ParentTypeResponse)
async def update_parent_type(payload: ParentTypeUpdateSchema):
    try:
        updated_entry = await ParentTypeService.update_parent_type(
            parent_type_id=payload.id,
            division=payload.division,
            dept_code=payload.dept_code,
            class_name=payload.class_name,
            class_code_suffix=payload.class_code_suffix,
            gender=payload.gender,
            reporting_category=payload.reporting_category,
        )
        return updated_entry
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("", response_model=ParentTypeResponse, status_code=201)
async def create_parent_type(payload: ParentTypeCreateSchema):
    try:
        new_entry = await ParentTypeService.create_parent_type(
            division=payload.division,
            dept_code=payload.dept_code,
            class_name=payload.class_name,
            class_code_suffix=payload.class_code_suffix,
            gender=payload.gender,
            reporting_category=payload.reporting_category,
        )
        return new_entry
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
