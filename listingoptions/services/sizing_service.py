import uuid
from typing import List, Dict, Any, Optional
from tortoise import Tortoise
from tortoise.exceptions import DoesNotExist, IntegrityError
from tortoise.transactions import in_transaction

from listingoptions.models.db_models import SizingScheme
from listingoptions.models.api_models import (
    SizingSchemeEntryCreate,
    SizingSchemeEntryDB,
    FullSizingSchemeCreate,
    SizingSchemeDetailResponse,
    SizingSchemeEntryBase,
    SizingSchemeEntryWithId,
    UpdateSizeOrderRequest,
    SizingSchemeListedName,
    AllSizingSchemesResponse,
)
import logging
import asyncio
from listingoptions.services.database_service import DatabaseService
from listingoptions.services.spreadsheet_service import spreadsheet_service

logger = logging.getLogger(__name__)


class SizingService:
    @staticmethod
    async def get_sizing_type_options() -> List[str]:
        try:
            schema = await DatabaseService.get_table_schema(DatabaseService._table("types"))
            if not schema or not schema.column_schema:
                logger.warning("Schema for 'types' table not found or is empty.")
                return []

            for column in schema.column_schema:
                if column.get("name") == "sizing_types":
                    return column.get("options", [])

            logger.warning("'sizing_types' column not found in 'types' table schema.")
            return []
        except Exception as e:
            logger.error(f"Error retrieving sizing type options: {str(e)}")
            raise

    @staticmethod
    async def get_platform_default_sizes() -> Dict[str, List[str]]:
        try:
            sql = f'SELECT platform_id, platform_value FROM "listingoptions_sizes_default_list" ORDER BY platform_id, platform_value;'

            result_list = await Tortoise.get_connection("default").execute_query_dict(sql)

            platform_sizes_map: Dict[str, List[str]] = {}
            for item in result_list:
                platform_id = str(item.get("platform_id"))
                platform_value = item.get("platform_value")

                if platform_id and platform_value is not None:
                    if platform_id not in platform_sizes_map:
                        platform_sizes_map[platform_id] = []
                    platform_sizes_map[platform_id].append(platform_value)

            return platform_sizes_map
        except Exception as e:
            logger.error(f"Error retrieving platform default sizes: {str(e)}")
            raise

    @staticmethod
    async def get_all_sizing_scheme_names() -> List[SizingSchemeListedName]:
        try:
            sql = f'SELECT DISTINCT sizing_scheme FROM "listingoptions_sizing_schemes" ORDER BY sizing_scheme;'

            result_list = await Tortoise.get_connection("default").execute_query_dict(sql)

            scheme_names = [
                item["sizing_scheme"] for item in result_list if "sizing_scheme" in item
            ]

            return [SizingSchemeListedName(name=name) for name in scheme_names]
        except Exception as e:
            logger.error(f"Error retrieving all sizing scheme names in service: {str(e)}")
            raise

    @staticmethod
    async def get_all_sizing_schemes_with_details() -> AllSizingSchemesResponse:
        try:
            all_entries = await SizingScheme.all().order_by("sizing_scheme", "order")

            schemes_dict = {}
            for entry in all_entries:
                scheme_name = entry.sizing_scheme
                if scheme_name not in schemes_dict:
                    schemes_dict[scheme_name] = {
                        "sizes": [],
                        "sizing_types": entry.sizing_types,
                    }
                schemes_dict[scheme_name]["sizes"].append(
                    SizingSchemeEntryWithId(id=entry.id, size=entry.size, order=entry.order)
                )

            schemes = [
                SizingSchemeDetailResponse(
                    sizing_scheme=name,
                    sizes=data["sizes"],
                    sizing_types=data["sizing_types"],
                )
                for name, data in schemes_dict.items()
            ]

            schemes.sort(key=lambda x: x.sizing_scheme)

            return AllSizingSchemesResponse(schemes=schemes)

        except Exception as e:
            logger.error(f"Error retrieving all sizing schemes with details: {str(e)}")
            raise

    @staticmethod
    async def get_all_sizes_with_schemes() -> List[dict]:
        try:
            entries = await SizingScheme.all().values("size", "sizing_scheme")

            size_map = {}
            for entry in entries:
                size = entry["size"]
                scheme = entry["sizing_scheme"]
                if size not in size_map:
                    size_map[size] = set()
                size_map[size].add(scheme)

            return [
                {"size": size, "sizing_schemes": sorted(list(schemes))}
                for size, schemes in sorted(size_map.items())
            ]
        except Exception as e:
            logger.error(f"Error retrieving all sizes with schemes: {str(e)}")
            raise

    @staticmethod
    async def get_sizing_scheme_details(
        scheme_name: str,
    ) -> Optional[SizingSchemeDetailResponse]:
        try:
            entries = await SizingScheme.filter(sizing_scheme=scheme_name).order_by("order")
            if not entries:
                return None

            sizing_types = entries[0].sizing_types if entries else None

            return SizingSchemeDetailResponse(
                sizing_scheme=scheme_name,
                sizes=[
                    SizingSchemeEntryWithId(id=e.id, size=e.size, order=e.order) for e in entries
                ],
                sizing_types=sizing_types,
            )
        except DoesNotExist:
            logger.info(f"Sizing scheme '{scheme_name}' not found when fetching details.")
            return None
        except Exception as e:
            logger.error(f"Error retrieving details for sizing scheme {scheme_name}: {str(e)}")
            raise

    @staticmethod
    async def add_size_to_scheme(
        scheme_name: str, entry_create: SizingSchemeEntryCreate
    ) -> SizingSchemeEntryDB:
        try:
            new_entry = await SizingScheme.create(
                sizing_scheme=scheme_name,
                size=entry_create.size,
                order=entry_create.order,
            )
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
            return SizingSchemeEntryDB.from_orm(new_entry)
        except IntegrityError:
            logger.warning(
                f"Integrity error adding size '{entry_create.size}' to scheme '{scheme_name}'. Likely duplicate size."
            )
            raise ValueError(
                f"Size '{entry_create.size}' already exists in scheme '{scheme_name}'."
            )
        except Exception as e:
            logger.error(f"Error adding size to scheme {scheme_name}: {str(e)}")
            raise

    @staticmethod
    async def create_full_sizing_scheme(
        scheme_create: FullSizingSchemeCreate,
    ) -> SizingSchemeDetailResponse:
        async with in_transaction():
            if await SizingScheme.filter(sizing_scheme=scheme_create.sizing_scheme).exists():
                raise ValueError(
                    f"Sizing scheme '{scheme_create.sizing_scheme}' already exists or has entries. Cannot create as new."
                )

            if not scheme_create.sizes:
                raise ValueError("Cannot create a sizing scheme with no sizes.")

            if len(set(s.size for s in scheme_create.sizes)) != len(scheme_create.sizes):
                raise ValueError(
                    "Duplicate sizes provided in the creation request for the same scheme."
                )
            if len(set(s.order for s in scheme_create.sizes)) != len(scheme_create.sizes):
                raise ValueError(
                    "Duplicate orders provided in the creation request for the same scheme."
                )

            created_db_entries = []
            for size_entry in scheme_create.sizes:
                entry = await SizingScheme.create(
                    sizing_scheme=scheme_create.sizing_scheme,
                    size=size_entry.size,
                    order=size_entry.order,
                    sizing_types=scheme_create.sizing_types,
                )
                created_db_entries.append(entry)

            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
            return SizingSchemeDetailResponse(
                sizing_scheme=scheme_create.sizing_scheme,
                sizes=sorted(
                    [SizingSchemeEntryWithId.from_orm(e) for e in created_db_entries],
                    key=lambda x: x.order,
                ),
                sizing_types=scheme_create.sizing_types,
            )

    @staticmethod
    async def update_scheme_size_orders(
        scheme_name: str, update_request: UpdateSizeOrderRequest
    ) -> SizingSchemeDetailResponse:
        async with in_transaction():
            new_scheme_name = (
                update_request.new_sizing_scheme.strip()
                if update_request.new_sizing_scheme
                and update_request.new_sizing_scheme.strip() != scheme_name
                else scheme_name
            )

            existing_entries = await SizingScheme.filter(sizing_scheme=scheme_name).order_by(
                "order"
            )
            if not existing_entries and not update_request.sizes:
                if new_scheme_name != scheme_name:
                    raise DoesNotExist(
                        f"Sizing scheme '{scheme_name}' not found, cannot rename an empty or non-existent scheme."
                    )
                return SizingSchemeDetailResponse(sizing_scheme=scheme_name, sizes=[])

            if (
                not existing_entries
                and await SizingScheme.filter(sizing_scheme=scheme_name).exists()
            ):
                pass
            elif not existing_entries:
                raise DoesNotExist(f"Sizing scheme '{scheme_name}' not found, cannot update.")

            if new_scheme_name != scheme_name:
                if await SizingScheme.filter(sizing_scheme=new_scheme_name).exists():
                    raise ValueError(
                        f"Sizing scheme with name '{new_scheme_name}' already exists. Please choose a unique name."
                    )
                await SizingScheme.filter(sizing_scheme=scheme_name).update(
                    sizing_scheme=new_scheme_name
                )
                for entry in existing_entries:
                    entry.sizing_scheme = new_scheme_name

            if len(set(s.size for s in update_request.sizes)) != len(update_request.sizes):
                raise ValueError("Duplicate sizes provided in the update request.")
            if len(set(s.order for s in update_request.sizes)) != len(update_request.sizes):
                raise ValueError("Duplicate orders provided in the update request.")

            existing_sizes_map = {e.size: e for e in existing_entries}
            request_sizes_map = {s.size: s for s in update_request.sizes}

            sizes_to_delete = set(existing_sizes_map.keys()) - set(request_sizes_map.keys())
            if sizes_to_delete:
                await SizingScheme.filter(
                    sizing_scheme=new_scheme_name, size__in=list(sizes_to_delete)
                ).delete()

            updated_entries = []
            for size_data in update_request.sizes:
                if size_data.size in existing_sizes_map:
                    entry = existing_sizes_map[size_data.size]
                    entry.order = size_data.order
                    entry.sizing_types = update_request.sizing_types
                    await entry.save(update_fields=["order", "sizing_types", "updated_at"])
                    updated_entries.append(entry)
                else:
                    new_entry = await SizingScheme.create(
                        sizing_scheme=new_scheme_name,
                        size=size_data.size,
                        order=size_data.order,
                        sizing_types=update_request.sizing_types,
                    )
                    updated_entries.append(new_entry)

        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        final_entries = [SizingSchemeEntryWithId.from_orm(e) for e in updated_entries]

        return SizingSchemeDetailResponse(
            sizing_scheme=new_scheme_name,
            sizes=sorted(final_entries, key=lambda x: x.order),
            sizing_types=update_request.sizing_types,
        )

    @staticmethod
    async def delete_size_from_scheme(scheme_name: str, size_value: str) -> bool:
        deleted_count = await SizingScheme.filter(
            sizing_scheme=scheme_name, size=size_value
        ).delete()
        if deleted_count == 0:
            logger.warning(f"No size '{size_value}' found in scheme '{scheme_name}' to delete.")
            raise DoesNotExist(f"Size '{size_value}' not found in scheme '{scheme_name}'.")
        logger.info(f"Deleted size '{size_value}' from scheme '{scheme_name}'.")
        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        return True

    @staticmethod
    async def delete_sizing_scheme(scheme_name: str) -> bool:
        deleted_count = await SizingScheme.filter(sizing_scheme=scheme_name).delete()
        if deleted_count == 0:
            logger.warning(f"No sizing scheme '{scheme_name}' found to delete.")
            raise DoesNotExist(f"Sizing scheme '{scheme_name}' not found.")
        logger.info(f"Deleted sizing scheme '{scheme_name}' and {deleted_count} associated sizes.")
        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        return True

    @staticmethod
    async def get_size_entry(scheme_name: str, size_value: str) -> Optional[SizingSchemeEntryDB]:
        entry = await SizingScheme.get_or_none(sizing_scheme=scheme_name, size=size_value)
        if entry:
            return SizingSchemeEntryDB.from_orm(entry)
        return None

    @staticmethod
    async def update_single_size_entry(
        scheme_name: str, current_size_value: str, entry_update: SizingSchemeEntryCreate
    ) -> SizingSchemeEntryDB:
        async with in_transaction():
            entry = await SizingScheme.get_or_none(
                sizing_scheme=scheme_name, size=current_size_value
            )
            if not entry:
                raise DoesNotExist(
                    f"Size '{current_size_value}' not found in scheme '{scheme_name}' for update."
                )

            if entry_update.size != current_size_value:
                if await SizingScheme.filter(
                    sizing_scheme=scheme_name, size=entry_update.size
                ).exists():
                    raise ValueError(
                        f"Cannot update to size '{entry_update.size}' as it already exists in scheme '{scheme_name}'."
                    )
                entry.size = entry_update.size

            entry.order = entry_update.order
            await entry.save(update_fields=["size", "order", "updated_at"])
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
            return SizingSchemeEntryDB.from_orm(entry)

    @staticmethod
    async def get_sizing_scheme_entries_by_name(
        scheme_name: str,
    ) -> List[SizingSchemeEntryDB]:
        entries = await SizingScheme.filter(sizing_scheme=scheme_name).order_by("order")
        if not entries:
            return []
        return [SizingSchemeEntryDB.from_orm(e) for e in entries]

    @staticmethod
    async def export_all_sizing_schemes() -> List[Dict[str, Any]]:
        try:
            all_entries = await SizingScheme.all().order_by("sizing_scheme", "order")

            rows = []
            for entry in all_entries:
                sizing_types_str = ""
                if entry.sizing_types and isinstance(entry.sizing_types, list):
                    sizing_types_str = ", ".join(entry.sizing_types)

                rows.append(
                    {
                        "Sizing Scheme": entry.sizing_scheme,
                        "Size": entry.size,
                        "Sizing Types": sizing_types_str,
                    }
                )

            return rows
        except Exception as e:
            logger.error(f"Error exporting sizing schemes: {str(e)}")
            raise
