import logging
from typing import Any, Dict, List

import orjson
from tortoise import connections

logger = logging.getLogger(__name__)


class ListingOptionsService:
    async def get_tables(self) -> List[Dict[str, Any]]:
        try:
            conn = connections.get("default")

            query = """
                SELECT
                    "table",
                    display_name,
                    primary_business_column,
                    column_schema,
                    list_schema,
                    list_type,
                    created_at,
                    updated_at
                FROM listingoptions_schema
            """

            results = await conn.execute_query_dict(query)

            tables = []
            for row in results:
                column_schema = row.get("column_schema")
                if isinstance(column_schema, str):
                    try:
                        column_schema = orjson.loads(column_schema)
                    except orjson.JSONDecodeError:
                        column_schema = []

                list_schema = row.get("list_schema")
                if isinstance(list_schema, str):
                    try:
                        list_schema = orjson.loads(list_schema)
                    except orjson.JSONDecodeError:
                        list_schema = []

                tables.append(
                    {
                        "table": row["table"],
                        "display_name": row.get("display_name"),
                        "primary_business_column": row.get("primary_business_column"),
                        "column_schema": column_schema or [],
                        "list_schema": list_schema or [],
                        "list_type": row.get("list_type", "default"),
                        "created_at": row.get("created_at"),
                        "updated_at": row.get("updated_at"),
                    }
                )

            logger.info(f"Successfully fetched {len(tables)} tables from database")
            return tables

        except Exception as e:
            logger.error(f"Error fetching tables from database: {e}")
            raise

    async def get_product_type_info(self, product_type: str) -> Dict[str, Any]:
        try:
            conn = connections.get("default")

            query = """
                SELECT
                    tp.gender,
                    t.item_weight_oz,
                    t.type,
                    t.aliases
                FROM listingoptions_types t
                LEFT JOIN listingoptions_types_parents tp ON t.parent_id = tp.id
                WHERE LOWER(t.type) = LOWER($1)
                   OR EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements_text(t.aliases) AS alias
                       WHERE LOWER(alias) = LOWER($1)
                   )
                LIMIT 1
            """

            result = await conn.execute_query_dict(query, [product_type])

            if not result:
                logger.warning(f"No product type info found for type: {product_type}")
                return {
                    "gender": None,
                    "item_weight_oz": None,
                    "type": None,
                    "aliases": None,
                    "is_alias_match": False,
                }

            row = result[0]
            canonical_type = row.get("type")
            is_alias_match = product_type != canonical_type

            logger.info(
                f"Successfully fetched product type info for {product_type}: "
                f"canonical_type={canonical_type}, is_alias_match={is_alias_match}, "
                f"gender={row.get('gender')}, weight={row.get('item_weight_oz')}"
            )

            return {
                "gender": row.get("gender"),
                "item_weight_oz": row.get("item_weight_oz"),
                "type": canonical_type,
                "aliases": row.get("aliases"),
                "is_alias_match": is_alias_match,
            }

        except Exception as e:
            logger.error(f"Error fetching product type info for {product_type}: {e}")
            raise

    async def get_platforms(self) -> List[Dict[str, Any]]:
        try:
            conn = connections.get("default")

            query = "SELECT id, name, icon, icon_mime_type FROM listingoptions_platforms"

            results = await conn.execute_query_dict(query)

            platforms = [
                {
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "icon": row.get("icon"),
                    "icon_mime_type": row.get("icon_mime_type"),
                }
                for row in results
            ]

            platforms.append(
                {
                    "id": "sellercloud",
                    "name": "Sellercloud",
                    "icon": "PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAuMDIyIDAuMDAwIDQxLjkgMjYuODYxIj4KICA8Zz4KICAgIDxwYXRoIGQ9Ik0xMC4wMTIsMjEuMDhhOS44OTEsOS44OTEsMCwxLDAsMCwxOS43ODFsMTMuMjY5LDBzMy41MjkuMTUxLDQuNy0yLjI0NkMzMC4wNTMsMzQuMzU5LDIxLjQzMSwyMS4wNywxMC4wMTIsMjEuMDhaIgogICAgICAgICAgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMCAtMTQuMDAyKSIKICAgICAgICAgIGZpbGw9IiMwM2YiLz4KICAgIDxwYXRoIGQ9Ik01MC4wOTEsMTMuNDNhMTMuNDMsMTMuNDMsMCwxLDAtMTMuNDMsMTMuNDNINDcuNDI1UzUwLjA5MSwxNi45MTksNTAuMDkxLDEzLjQzWiIKICAgICAgICAgIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0xNS40NzEgMCkiCiAgICAgICAgICBmaWxsPSIjMDZmIi8+CiAgICA8cGF0aCBkPSJNNjguMzA3LDI5LjI0YTguNTIyLDguNTIyLDAsMCwxLDAsMTcuMDQzSDU2Ljg3MXMtMi45ODUsMC00LjAyOS0xLjkzN0M1MC44NzUsNDAuNzYzLDU4LjQ2NiwyOS4yMzMsNjguMzA3LDI5LjI0WiIKICAgICAgICAgIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0zNC45MzMgLTE5LjQyMikiCiAgICAgICAgICBmaWxsPSIjMDlmIi8+CiAgPC9nPgo8L3N2Zz4K",
                    "icon_mime_type": "image/svg+xml",
                }
            )

            logger.info(f"Successfully fetched {len(platforms)} platforms from database")
            return platforms

        except Exception as e:
            logger.error(f"Error fetching platforms from database: {e}")
            raise

    async def get_platform_type(self, product_type: str, platform: str) -> str | None:
        try:
            conn = connections.get("default")

            query = """
                SELECT tdl.platform_value
                FROM listingoptions_types_default_list tdl
                JOIN listingoptions_types t ON tdl.primary_id = t.id
                WHERE tdl.platform_id = $2
                  AND (
                      LOWER(t.type) = LOWER($1)
                      OR EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements_text(t.aliases) AS alias
                          WHERE LOWER(alias) = LOWER($1)
                      )
                  )
                LIMIT 1
            """

            result = await conn.execute_query_dict(query, [product_type, platform])

            if not result:
                logger.warning(f"No {platform} type mapping found for: {product_type}")
                return None

            platform_value = result[0].get("platform_value")
            logger.info(f"{platform} type for '{product_type}': {platform_value}")
            return platform_value

        except Exception as e:
            logger.error(f"Error fetching {platform} type for {product_type}: {e}")
            raise

    async def check_unmapped_sizes(
        self, sizing_scheme: str, sizes: list, platforms: list, sizing_type: str = None
    ) -> list:
        conn = connections.get("default")
        result = []
        for platform_id in platforms:
            if platform_id == "sellercloud":
                continue
            mapped = await conn.execute_query_dict(
                """
                SELECT ss.size FROM listingoptions_sizing_schemes ss
                JOIN listingoptions_sizes_default_list sdl ON sdl.primary_id = ss.id
                WHERE ss.sizing_scheme = $1 AND sdl.platform_id = $2 AND ss.size = ANY($3)
                    AND (sdl.sizing_type = $4 OR sdl.sizing_type IS NULL)
                """,
                [sizing_scheme, platform_id, sizes, sizing_type],
            )
            mapped_sizes = {r["size"] for r in mapped}
            missing = [s for s in sizes if s not in mapped_sizes]
            if missing:
                platform_info = await conn.execute_query_dict(
                    "SELECT name FROM listingoptions_platforms WHERE id = $1", [platform_id]
                )
                result.append(
                    {
                        "platform_id": platform_id,
                        "platform_name": (
                            platform_info[0]["name"] if platform_info else platform_id
                        ),
                        "missing_sizes": missing,
                    }
                )
        return result

    async def get_platform_size_records(
        self, platform_id: str, sizing_type: str = None
    ) -> Dict[str, list]:
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            """
            SELECT sdl.platform_value,
                   COALESCE(
                       ARRAY_AGG(ss.sizing_scheme || ':' || ss.size)
                       FILTER (WHERE sdl.primary_id IS NOT NULL), '{}'
                   ) as internal_values
            FROM listingoptions_sizes_default_list sdl
            LEFT JOIN listingoptions_sizing_schemes ss ON sdl.primary_id = ss.id
            WHERE sdl.platform_id = $1
                AND (sdl.sizing_type = $2 OR sdl.sizing_type IS NULL)
            GROUP BY sdl.platform_value
            ORDER BY sdl.platform_value
            """,
            [platform_id, sizing_type],
        )
        return {row["platform_value"]: row["internal_values"] for row in rows}

    async def save_size_mapping(
        self, sizing_scheme_entry_id, platform_id, platform_value, sizing_type=None
    ):
        conn = connections.get("default")

        if not platform_value:
            existing = await conn.execute_query_dict(
                """
                SELECT sdl.id, sdl.platform_value
                FROM listingoptions_sizes_default_list sdl
                WHERE sdl.primary_id = $1::uuid AND sdl.platform_id = $2
                    AND sdl.primary_table_column = 'size'
                    AND sdl.sizing_type = $3
                """,
                [sizing_scheme_entry_id, platform_id, sizing_type],
            )
            if not existing:
                return {"success": True, "already_deleted": True}

            old_pv = existing[0]["platform_value"]
            await conn.execute_query(
                """
                DELETE FROM listingoptions_sizes_default_list
                WHERE primary_id = $1::uuid AND platform_id = $2
                    AND primary_table_column = 'size'
                    AND sizing_type = $3
                """,
                [sizing_scheme_entry_id, platform_id, sizing_type],
            )
            remaining = await conn.execute_query_dict(
                """
                SELECT 1 FROM listingoptions_sizes_default_list
                WHERE platform_id = $1 AND platform_value = $2 AND (sizing_type = $3 OR sizing_type IS NULL) LIMIT 1
                """,
                [platform_id, old_pv, sizing_type],
            )
            if not remaining:
                await conn.execute_query(
                    """
                    INSERT INTO listingoptions_sizes_default_list
                        (primary_id, platform_value, platform_id, primary_table_column, sizing_type, created_at, updated_at)
                    VALUES (NULL, $1, $2, 'size', NULL, NOW(), NOW())
                    """,
                    [old_pv, platform_id],
                )
            return {"success": True, "deleted": True, "deleted_platform_value": old_pv}

        existing = await conn.execute_query_dict(
            """
            SELECT sdl.id, sdl.platform_value
            FROM listingoptions_sizes_default_list sdl
            WHERE sdl.primary_id = $1::uuid AND sdl.platform_id = $2
                AND sdl.primary_table_column = 'size'
                AND sdl.sizing_type = $3
            """,
            [sizing_scheme_entry_id, platform_id, sizing_type],
        )

        if existing and existing[0]["platform_value"] == platform_value:
            return {"success": True, "already_mapped": True}

        if existing and existing[0]["platform_value"] != platform_value:
            return {
                "error": "already_mapped",
                "message": (
                    f'This size is already mapped to "{existing[0]["platform_value"]}". '
                    f"Remove the existing mapping first."
                ),
                "existing_platform_value": existing[0]["platform_value"],
            }

        await conn.execute_query(
            """
            INSERT INTO listingoptions_sizes_default_list
                (primary_id, platform_value, platform_id, primary_table_column, sizing_type, created_at, updated_at)
            VALUES ($1::uuid, $2, $3, 'size', $4, NOW(), NOW())
            """,
            [sizing_scheme_entry_id, platform_value, platform_id, sizing_type],
        )

        return {"success": True}

    async def get_mapped_platform_sizes(
        self, sizing_scheme: str, sizes: list, platform_id: str, sizing_type: str = None
    ) -> Dict[str, str]:
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            """
            SELECT ss.size, sdl.platform_value
            FROM listingoptions_sizing_schemes ss
            JOIN listingoptions_sizes_default_list sdl ON sdl.primary_id = ss.id
            WHERE ss.sizing_scheme = $1
              AND ss.size = ANY($2)
              AND sdl.platform_id = $3
              AND sdl.primary_table_column = 'size'
              AND (sdl.sizing_type = $4 OR sdl.sizing_type IS NULL)
            """,
            [sizing_scheme, sizes, platform_id, sizing_type],
        )
        return {row["size"]: row["platform_value"] for row in rows}

    async def get_spo_value_codes(
        self, pairs: List[tuple[str, str]]
    ) -> Dict[tuple[str, str], str]:
        """Resolve (list_code, label) pairs to ShopSimon value codes in one query."""
        if not pairs:
            return {}
        conn = connections.get("default")
        list_codes = [lc for lc, _ in pairs]
        labels = [lb for _, lb in pairs]
        rows = await conn.execute_query_dict(
            """
            SELECT DISTINCT list_code, label, value_code
            FROM config_spo_value_lists
            WHERE (list_code, label) IN (
                SELECT * FROM unnest($1::text[], $2::text[])
            )
            """,
            [list_codes, labels],
        )
        return {(r["list_code"], r["label"]): r["value_code"] for r in rows}

    async def get_color_info(self, color: str) -> Dict[str, Any]:
        try:
            conn = connections.get("default")

            query = """
                SELECT
                    c.color,
                    c.aliases
                FROM listingoptions_colors c
                WHERE LOWER(c.color) = LOWER($1)
                   OR EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements_text(c.aliases) AS alias
                       WHERE LOWER(alias) = LOWER($1)
                   )
                LIMIT 1
            """

            result = await conn.execute_query_dict(query, [color])

            if not result:
                logger.warning(f"No color info found for: {color}")
                return {"color": None, "aliases": None, "is_alias_match": False}

            row = result[0]
            canonical_color = row.get("color")
            is_alias_match = color != canonical_color

            logger.info(
                f"Successfully fetched color info for {color}: "
                f"canonical_color={canonical_color}, is_alias_match={is_alias_match}"
            )

            return {
                "color": canonical_color,
                "aliases": row.get("aliases"),
                "is_alias_match": is_alias_match,
            }

        except Exception as e:
            logger.error(f"Error fetching color info for {color}: {e}")
            raise


listing_options_service = ListingOptionsService()
