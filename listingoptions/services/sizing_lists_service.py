import asyncio
import logging
import uuid
from typing import Any, List, Optional

from tortoise import Tortoise

from listingoptions.models.api_models import (
    PaginatedSizingListPlatformEntryResponse,
    PaginationParams,
    SizingListPlatformEntryCreate,
    SizingListPlatformEntryDetail,
    SizingListPlatformEntryUpdate,
)
from listingoptions.services.spreadsheet_service import spreadsheet_service

logger = logging.getLogger(__name__)

TABLE = '"listingoptions_sizes_default_list"'


class SizingListService:
    @staticmethod
    async def create_sizing_list_entry(
        entry_create: SizingListPlatformEntryCreate,
    ) -> SizingListPlatformEntryDetail:
        conn = Tortoise.get_connection("default")

        scheme_rows = await conn.execute_query_dict(
            'SELECT id FROM "listingoptions_sizing_schemes" WHERE id = $1',
            [str(entry_create.sizing_scheme_entry_id)],
        )
        if not scheme_rows:
            raise ValueError(
                f"SizingScheme entry with ID {entry_create.sizing_scheme_entry_id} not found."
            )

        platform_rows = await conn.execute_query_dict(
            'SELECT id FROM "listingoptions_platforms" WHERE id = $1',
            [entry_create.platform_id],
        )
        if not platform_rows:
            raise ValueError(f"Platform with ID {entry_create.platform_id} not found.")

        try:
            sql = f"""
                INSERT INTO {TABLE} (primary_id, platform_id, platform_value, primary_table_column)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """
            rows = await conn.execute_query_dict(
                sql,
                [
                    str(entry_create.sizing_scheme_entry_id),
                    entry_create.platform_id,
                    entry_create.platform_value,
                    entry_create.sizing_type,
                ],
            )
            new_id = rows[0]["id"]
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
            return await SizingListService.get_sizing_list_entry_by_id(new_id)
        except Exception as e:
            error_str = str(e)
            if "unique" in error_str.lower() or "duplicate" in error_str.lower():
                logger.warning(f"Integrity error creating sizing list entry: {e}")
                raise ValueError(
                    "Failed to create sizing list entry. This combination of Sizing Scheme size, Platform, and Sizing Type may already exist."
                )
            logger.error(f"Error creating sizing list entry: {error_str}")
            raise

    @staticmethod
    async def get_sizing_list_entry_by_id(
        entry_id: uuid.UUID,
    ) -> Optional[SizingListPlatformEntryDetail]:
        conn = Tortoise.get_connection("default")
        sql = f"""
            SELECT sl.id,
                   sl.primary_id AS sizing_scheme_entry_id,
                   sl.platform_id,
                   sl.platform_value,
                   sl.primary_table_column AS sizing_type,
                   sl.created_at,
                   sl.updated_at,
                   ss.sizing_scheme AS sizing_scheme_name,
                   ss.size AS size_value,
                   ss."order" AS size_order,
                   p.name AS platform_name
            FROM {TABLE} sl
            JOIN "listingoptions_sizing_schemes" ss ON sl.primary_id = ss.id
            JOIN "listingoptions_platforms" p ON sl.platform_id = p.id
            WHERE sl.id = $1
        """
        rows = await conn.execute_query_dict(sql, [str(entry_id)])
        if not rows:
            return None

        r = rows[0]
        return SizingListPlatformEntryDetail(
            id=r["id"],
            sizing_scheme_entry_id=r["sizing_scheme_entry_id"],
            platform_id=r["platform_id"],
            platform_value=r["platform_value"],
            sizing_type=r["sizing_type"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
            sizing_scheme_name=r["sizing_scheme_name"],
            size_value=r["size_value"],
            size_order=r["size_order"],
            platform_name=r["platform_name"],
        )

    @staticmethod
    async def get_all_sizing_list_entries(
        pagination: PaginationParams,
        sizing_scheme_name_filter: Optional[str] = None,
        platform_id_filter: Optional[str] = None,
        size_value_filter: Optional[str] = None,
        platform_value_filter: Optional[str] = None,
        sizing_type_filter: Optional[str] = None,
    ) -> PaginatedSizingListPlatformEntryResponse:

        select_fields = """
            sl.id,
            sl.primary_id AS sizing_scheme_entry_id,
            sl.platform_id,
            sl.platform_value,
            sl.primary_table_column AS sizing_type,
            sl.created_at,
            sl.updated_at,
            ss.sizing_scheme AS sizing_scheme_name,
            ss.size AS size_value,
            ss."order" AS size_order,
            p.name AS platform_name,
            COUNT(*) OVER() as total_records
        """

        from_clause = f"""
            FROM {TABLE} sl
            JOIN "listingoptions_sizing_schemes" ss ON sl.primary_id = ss.id
            JOIN "listingoptions_platforms" p ON sl.platform_id = p.id
        """

        where_conditions = []
        query_params: List[Any] = []
        param_idx = 1

        if sizing_scheme_name_filter:
            where_conditions.append(f"ss.sizing_scheme ILIKE ${param_idx}")
            query_params.append(f"%{sizing_scheme_name_filter}%")
            param_idx += 1
        if platform_id_filter:
            where_conditions.append(f"sl.platform_id = ${param_idx}")
            query_params.append(platform_id_filter)
            param_idx += 1
        if size_value_filter:
            where_conditions.append(f"ss.size ILIKE ${param_idx}")
            query_params.append(f"%{size_value_filter}%")
            param_idx += 1
        if platform_value_filter:
            where_conditions.append(f"sl.platform_value ILIKE ${param_idx}")
            query_params.append(f"%{platform_value_filter}%")
            param_idx += 1
        if sizing_type_filter:
            where_conditions.append(f"sl.primary_table_column = ${param_idx}")
            query_params.append(sizing_type_filter)
            param_idx += 1

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        order_by_clause = (
            'ORDER BY ss.sizing_scheme ASC, ss."order" ASC, p.name ASC, sl.platform_value ASC'
        )

        limit_offset_clause = f"LIMIT ${param_idx} OFFSET ${param_idx + 1}"
        query_params.append(pagination.page_size)
        query_params.append((pagination.page - 1) * pagination.page_size)

        sql_query = f"""
            SELECT {select_fields}
            {from_clause}
            {where_clause}
            {order_by_clause}
            {limit_offset_clause}
        """

        connection = Tortoise.get_connection("default")
        fetched_records = await connection.execute_query_dict(sql_query, query_params)

        detailed_entries: List[SizingListPlatformEntryDetail] = []
        total_count = 0

        if fetched_records:
            total_count = fetched_records[0]["total_records"]
            for record in fetched_records:
                detailed_entries.append(
                    SizingListPlatformEntryDetail(
                        id=record["id"],
                        sizing_scheme_entry_id=record["sizing_scheme_entry_id"],
                        platform_id=record["platform_id"],
                        platform_value=record["platform_value"],
                        sizing_type=record["sizing_type"],
                        created_at=record["created_at"],
                        updated_at=record["updated_at"],
                        sizing_scheme_name=record["sizing_scheme_name"],
                        size_value=record["size_value"],
                        size_order=record["size_order"],
                        platform_name=record["platform_name"],
                    )
                )

        total_pages = (
            (total_count + pagination.page_size - 1) // pagination.page_size
            if pagination.page_size > 0
            else 0
        )
        return PaginatedSizingListPlatformEntryResponse(
            items=detailed_entries,
            total=total_count,
            page=pagination.page,
            page_size=pagination.page_size,
            total_pages=total_pages if total_pages > 0 else 1,
        )

    @staticmethod
    async def update_sizing_list_entry(
        entry_id: uuid.UUID, entry_update: SizingListPlatformEntryUpdate
    ) -> Optional[SizingListPlatformEntryDetail]:
        conn = Tortoise.get_connection("default")

        current = await conn.execute_query_dict(
            f"SELECT id, primary_id, platform_id, primary_table_column FROM {TABLE} WHERE id = $1",
            [str(entry_id)],
        )
        if not current:
            return None

        row = current[0]
        new_platform_value = (
            entry_update.platform_value if entry_update.platform_value is not None else None
        )
        new_sizing_type = (
            entry_update.sizing_type
            if entry_update.sizing_type is not None
            else row["primary_table_column"]
        )

        check_sql = f"""
            SELECT id FROM {TABLE}
            WHERE primary_id = $1 AND platform_id = $2 AND primary_table_column = $3 AND id != $4
        """
        conflicts = await conn.execute_query_dict(
            check_sql,
            [
                str(row["primary_id"]),
                row["platform_id"],
                new_sizing_type,
                str(entry_id),
            ],
        )
        if conflicts:
            raise ValueError(
                f"Update failed: An entry with the same Sizing Scheme, Platform, and Sizing Type already exists."
            )

        set_parts = ["updated_at = now()"]
        params: List[Any] = []
        idx = 1

        if entry_update.platform_value is not None:
            set_parts.append(f"platform_value = ${idx}")
            params.append(entry_update.platform_value)
            idx += 1
        if entry_update.sizing_type is not None:
            set_parts.append(f"primary_table_column = ${idx}")
            params.append(entry_update.sizing_type)
            idx += 1

        params.append(str(entry_id))
        update_sql = f"""
            UPDATE {TABLE}
            SET {", ".join(set_parts)}
            WHERE id = ${idx}
        """
        await conn.execute_query(update_sql, params)

        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))

        return await SizingListService.get_sizing_list_entry_by_id(entry_id)

    @staticmethod
    async def delete_sizing_list_entry(entry_id: uuid.UUID) -> bool:
        conn = Tortoise.get_connection("default")
        result = await conn.execute_query(f"DELETE FROM {TABLE} WHERE id = $1", [str(entry_id)])
        deleted_count = result[0]
        if deleted_count == 0:
            logger.warning(f"Sizing list entry with ID {entry_id} not found for deletion.")
            return False
        logger.info(f"Sizing list entry with ID {entry_id} deleted successfully.")
        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        return True
