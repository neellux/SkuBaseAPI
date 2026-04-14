import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from tortoise import connections

logger = logging.getLogger(__name__)

FIELD_TYPE_TO_POSTGRES = {
    "text": "TEXT",
    "number": "NUMERIC",
    "bool": "BOOLEAN",
    "text_list": "TEXT[]",
    "rich_text": "TEXT",
}


def get_postgres_type(field_type: str) -> str:
    return FIELD_TYPE_TO_POSTGRES.get(field_type, "TEXT")


class ProductInfoService:

    @staticmethod
    async def _get_connection():
        return connections.get("product_db")

    @staticmethod
    async def ensure_table_exists() -> bool:
        try:
            conn = await ProductInfoService._get_connection()
            await conn.execute_query("""
                CREATE TABLE IF NOT EXISTS product_info (
                    parent_sku VARCHAR(100) PRIMARY KEY REFERENCES parent_products(sku)
                        ON DELETE CASCADE ON UPDATE CASCADE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Ensured product_info table exists")
            return True
        except Exception as e:
            logger.error(f"Failed to ensure product_info table exists: {e}")
            return False

    @staticmethod
    async def column_exists(column_name: str) -> bool:
        try:
            conn = await ProductInfoService._get_connection()
            result = await conn.execute_query_dict(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'product_info' AND column_name = $1
                """,
                [column_name],
            )
            return len(result) > 0
        except Exception as e:
            logger.error(f"Failed to check if column {column_name} exists: {e}")
            return False

    @staticmethod
    async def get_column_type(column_name: str) -> Optional[str]:
        try:
            conn = await ProductInfoService._get_connection()
            result = await conn.execute_query_dict(
                """
                SELECT data_type, udt_name
                FROM information_schema.columns
                WHERE table_name = 'product_info' AND column_name = $1
                """,
                [column_name],
            )
            if result:
                data_type = result[0].get("data_type")
                udt_name = result[0].get("udt_name")
                if data_type == "ARRAY":
                    return f"{udt_name.lstrip('_')}[]".upper()
                return data_type.upper()
            return None
        except Exception as e:
            logger.error(f"Failed to get column type for {column_name}: {e}")
            return None

    @staticmethod
    async def add_column(field_name: str, field_type: str) -> Dict[str, Any]:
        result = {"success": False, "field_name": field_name, "error": None, "warning": None}

        if not re.match(r"^[a-z][a-z0-9_]*$", field_name):
            result["error"] = f"Invalid field name: {field_name}. Must be lowercase snake_case."
            logger.warning(result["error"])
            return result

        try:
            if await ProductInfoService.column_exists(field_name):
                result["success"] = True
                result["warning"] = f"Column {field_name} already exists"
                logger.info(result["warning"])
                return result

            postgres_type = get_postgres_type(field_type)

            conn = await ProductInfoService._get_connection()
            await conn.execute_query(
                f'ALTER TABLE product_info ADD COLUMN "{field_name}" {postgres_type}'
            )

            result["success"] = True
            logger.info(f"Added column {field_name} ({postgres_type}) to product_info")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Failed to add column {field_name}: {e}")

        return result

    @staticmethod
    async def sync_columns_with_template(field_definitions: List[Dict]) -> Dict[str, Any]:
        results = {"added": [], "skipped": [], "errors": [], "type_mismatches": []}

        if not field_definitions:
            return results

        for field_def in field_definitions:
            field_name = field_def.get("name")
            field_type = field_def.get("type")

            if not field_name or not field_type:
                continue

            if not re.match(r"^[a-z][a-z0-9_]*$", field_name):
                results["errors"].append(
                    {
                        "field": field_name,
                        "error": "Invalid field name format. Must be lowercase snake_case.",
                    }
                )
                continue

            existing_type = await ProductInfoService.get_column_type(field_name)

            if existing_type:
                expected_type = get_postgres_type(field_type)
                if existing_type != expected_type:
                    results["type_mismatches"].append(
                        {
                            "field": field_name,
                            "existing_type": existing_type,
                            "requested_type": expected_type,
                        }
                    )
                results["skipped"].append(field_name)
            else:
                add_result = await ProductInfoService.add_column(field_name, field_type)
                if add_result["success"]:
                    results["added"].append(field_name)
                else:
                    results["errors"].append(
                        {"field": field_name, "error": add_result.get("error")}
                    )

        logger.info(
            f"Sync results: added={len(results['added'])}, "
            f"skipped={len(results['skipped'])}, errors={len(results['errors'])}"
        )
        return results

    @staticmethod
    async def validate_parent_sku_exists(parent_sku: str) -> bool:
        try:
            conn = await ProductInfoService._get_connection()
            result = await conn.execute_query_dict(
                "SELECT 1 FROM parent_products WHERE sku = $1", [parent_sku]
            )
            return len(result) > 0
        except Exception as e:
            logger.error(f"Failed to validate parent SKU {parent_sku}: {e}")
            return False

    @staticmethod
    async def validate_unique_fields(
        parent_sku: str, data: Dict[str, Any], field_definitions: List[Dict]
    ) -> List[str]:
        errors = []
        unique_fields = [f for f in field_definitions if f.get("is_unique")]

        if not unique_fields:
            return errors

        conn = await ProductInfoService._get_connection()

        for field in unique_fields:
            field_name = field.get("name")
            value = data.get(field_name)

            if value is None:
                continue

            if not await ProductInfoService.column_exists(field_name):
                continue

            try:
                existing = await conn.execute_query_dict(
                    f'SELECT parent_sku FROM product_info WHERE "{field_name}" = $1 AND parent_sku != $2',
                    [value, parent_sku],
                )
                if existing:
                    errors.append(
                        f"Value '{value}' for field '{field_name}' already exists "
                        f"(used by SKU: {existing[0]['parent_sku']})"
                    )
            except Exception as e:
                logger.warning(f"Failed to check uniqueness for {field_name}: {e}")

        return errors

    @staticmethod
    async def upsert_product_info(
        parent_sku: str, data: Dict[str, Any], field_definitions: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        result = {"success": False, "error": None, "parent_sku": parent_sku}

        try:
            if not await ProductInfoService.validate_parent_sku_exists(parent_sku):
                result["error"] = f"Parent SKU {parent_sku} does not exist in parent_products"
                logger.warning(result["error"])
                return result

            if field_definitions:
                unique_errors = await ProductInfoService.validate_unique_fields(
                    parent_sku, data, field_definitions
                )
                if unique_errors:
                    result["error"] = "; ".join(unique_errors)
                    return result

            conn = await ProductInfoService._get_connection()
            columns_result = await conn.execute_query_dict("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'product_info'
                  AND column_name NOT IN ('parent_sku', 'created_at', 'updated_at')
                """)
            existing_columns = {row["column_name"] for row in columns_result}

            columns = ["parent_sku"]
            values = [parent_sku]
            placeholders = ["$1"]
            update_parts = []

            param_idx = 2
            for col_name in existing_columns:
                if col_name in data:
                    columns.append(f'"{col_name}"')
                    values.append(data[col_name])
                    placeholders.append(f"${param_idx}")
                    update_parts.append(f'"{col_name}" = EXCLUDED."{col_name}"')
                    param_idx += 1

            if not update_parts:
                await conn.execute_query(
                    """
                    INSERT INTO product_info (parent_sku) VALUES ($1)
                    ON CONFLICT (parent_sku) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                    """,
                    [parent_sku],
                )
            else:
                query = f"""
                    INSERT INTO product_info ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (parent_sku) DO UPDATE SET
                        {', '.join(update_parts)},
                        updated_at = CURRENT_TIMESTAMP
                """
                await conn.execute_query(query, values)

            result["success"] = True
            logger.info(f"Upserted product_info for SKU {parent_sku}")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Failed to upsert product_info for SKU {parent_sku}: {e}")

        return result

    @staticmethod
    async def get_product_info(parent_sku: str) -> Optional[Dict[str, Any]]:
        try:
            conn = await ProductInfoService._get_connection()
            result = await conn.execute_query_dict(
                "SELECT * FROM product_info WHERE parent_sku = $1", [parent_sku]
            )
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get product_info for SKU {parent_sku}: {e}")
            return None

    @staticmethod
    async def mark_column_removed(field_name: str) -> bool:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"_removed_{field_name}_{timestamp}"

            conn = await ProductInfoService._get_connection()
            await conn.execute_query(
                f'ALTER TABLE product_info RENAME COLUMN "{field_name}" TO "{new_name}"'
            )
            logger.info(f"Renamed column {field_name} to {new_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to rename column {field_name}: {e}")
            return False
