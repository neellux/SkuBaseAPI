from typing import List, Dict, Any, Optional, Tuple
import uuid
import orjson
from tortoise import Tortoise
from tortoise.exceptions import DoesNotExist, IntegrityError
from listingoptions.models.db_models import Schema, Platform
from listingoptions.models.api_models import (
    ColumnDefinition,
    CreateTableRequest,
    AddColumnRequest,
    UpdateColumnRequest,
    ListSchemaDefinition,
    ListSchemaDefinitionUpdate,
    DefaultListEntry,
    SizingListEntry,
    FuzzyCheckResponse,
    SizingSchemeDetailResponse,
    SizingSchemeEntryWithId,
)
import pandas as pd
import logging
import traceback
import asyncio
from listingoptions.services.spreadsheet_service import spreadsheet_service
from fastapi import HTTPException
import re


def validate_sql_identifier(name: str) -> str:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


logger = logging.getLogger(__name__)


class DatabaseService:

    _schema_cache: Dict[str, Schema] = {}
    _platform_cache: Dict[str, Platform] = {}

    TABLE_PREFIX = "listingoptions_"

    @staticmethod
    def _table(name: str) -> str:
        if name.startswith(DatabaseService.TABLE_PREFIX):
            return name
        return f"{DatabaseService.TABLE_PREFIX}{name}"

    @staticmethod
    def get_sql_type(column_type: str) -> str:
        type_mapping = {
            "text": "TEXT",
            "number": "NUMERIC",
            "bool": "BOOLEAN",
            "text_list": "JSONB",
            "platform_list": "JSONB",
        }
        return type_mapping.get(column_type, "TEXT")

    @staticmethod
    async def table_exists(table_name: str) -> bool:
        try:
            sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )
            """
            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, [DatabaseService._table(table_name)]
            )
            return result_list[0]["exists"] if result_list else False
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            raise

    @staticmethod
    async def column_exists(table_name: str, column_name: str) -> bool:
        try:
            sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = $1 
                    AND column_name = $2
                )
            """
            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, [DatabaseService._table(table_name), column_name]
            )
            return result_list[0]["exists"] if result_list else False
        except Exception as e:
            logger.error(
                f"Error checking if column {column_name} in table {table_name} exists: {str(e)}"
            )
            raise

    @staticmethod
    async def create_table(request: CreateTableRequest) -> bool:
        try:
            if await DatabaseService.table_exists(request.table_name):
                raise ValueError(f"Table {request.table_name} already exists")

            table_sql = f"""
                CREATE TABLE "{DatabaseService._table(request.table_name)}" (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    "{request.primary_business_column}" TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """
            await Tortoise.get_connection("default").execute_script(table_sql)

            gin_index_sql = f'CREATE INDEX "idx_{DatabaseService._table(request.table_name)}_gin_{request.primary_business_column}" ON "{DatabaseService._table(request.table_name)}" USING GIN ("{request.primary_business_column}" gin_trgm_ops)'
            await Tortoise.get_connection("default").execute_script(gin_index_sql)

            sort_index_sql = f'CREATE INDEX "idx_{DatabaseService._table(request.table_name)}_sort_{request.primary_business_column}" ON "{DatabaseService._table(request.table_name)}" ("{request.primary_business_column}")'
            await Tortoise.get_connection("default").execute_script(sort_index_sql)

            primary_column_def = ColumnDefinition(
                name=request.primary_business_column,
                display_name=request.primary_business_display_name,
                type="text",
                is_required=True,
                fuzzy_check=True,
                is_primary_column=True,
                order=0,
            )

            schema_entry = await Schema.create(
                table=DatabaseService._table(request.table_name),
                display_name=request.display_name,
                primary_business_column=request.primary_business_column,
                column_schema=[primary_column_def.dict()],
                list_schema=[],
                list_type=request.list_type,
            )
            DatabaseService._schema_cache[request.table_name] = schema_entry

            await DatabaseService.create_mapping_table(request.table_name, request.list_type)

            logger.info(
                f"Created table {request.table_name} with schema entry and initial '{request.list_type}' list table."
            )
            return True

        except Exception as e:
            logger.error(f"Error creating table {request.table_name}: {str(e)}")
            raise

    @staticmethod
    async def add_column(request: AddColumnRequest) -> bool:
        try:
            if not await DatabaseService.table_exists(request.table_name):
                raise ValueError(f"Table {request.table_name} does not exist")

            if await DatabaseService.column_exists(request.table_name, request.column.name):
                raise ValueError(
                    f"Column {request.column.name} already exists in table {request.table_name}"
                )

            column_def = request.column
            if column_def.multiselect:
                column_def.type = "text_list"

            if column_def.type == "number" and column_def.options and len(column_def.options) > 0:
                try:
                    column_def.options = [float(o) for o in column_def.options]
                except (ValueError, TypeError):
                    raise ValueError("All options for a 'number' type must be numeric.")

            sql_type = DatabaseService.get_sql_type(column_def.type)
            alter_sql = f'ALTER TABLE "{DatabaseService._table(request.table_name)}" ADD COLUMN "{request.column.name}" {sql_type}'
            if column_def.is_unique and column_def.type in ["text", "number"]:
                alter_sql += " UNIQUE"
            await Tortoise.get_connection("default").execute_script(alter_sql)

            schema_entry = await Schema.get(table=request.table_name)
            column_schema = schema_entry.column_schema or []
            column_def.order = len(column_schema)
            column_schema.append(column_def.dict())
            schema_entry.column_schema = column_schema
            await schema_entry.save()
            DatabaseService._schema_cache[request.table_name] = schema_entry

            should_create_gin_index = column_def.fuzzy_check or (
                column_def.is_unique and column_def.type == "text_list"
            )
            if should_create_gin_index:
                gin_index_sql = ""
                if column_def.type == "text":
                    gin_index_sql = f'CREATE INDEX "idx_{DatabaseService._table(request.table_name)}_gin_{request.column.name}" ON "{DatabaseService._table(request.table_name)}" USING GIN ("{request.column.name}" gin_trgm_ops)'
                elif column_def.type == "text_list":
                    gin_index_sql = f'CREATE INDEX "idx_{DatabaseService._table(request.table_name)}_gin_{request.column.name}" ON "{DatabaseService._table(request.table_name)}" USING GIN ("{request.column.name}")'

                if gin_index_sql:
                    await Tortoise.get_connection("default").execute_script(gin_index_sql)
                    logger.info(
                        f"Created GIN index for column {request.column.name} in table {request.table_name}"
                    )

            logger.info(f"Added column {request.column.name} to table {request.table_name}")
            return True

        except Exception as e:
            logger.error(f"Error adding column to table {request.table_name}: {str(e)}")
            raise

    @staticmethod
    async def update_column(request: UpdateColumnRequest) -> bool:
        try:
            schema_entry = await Schema.get(table=request.table_name)

            column_schema = schema_entry.column_schema or []
            column_to_update_index = -1
            column_data = {}

            for i, col_def in enumerate(column_schema):
                if col_def.get("name") == request.column_name:
                    column_to_update_index = i
                    column_data = col_def
                    break

            if column_to_update_index == -1:
                raise ValueError(
                    f"Column {request.column_name} not found in table {request.table_name}"
                )

            update_data = request.update_data.model_dump(exclude_unset=True)

            column_data.update(update_data)

            validated_column = ColumnDefinition(**column_data)

            column_schema[column_to_update_index] = validated_column.model_dump()

            schema_entry.column_schema = column_schema
            await schema_entry.save()

            DatabaseService._schema_cache[request.table_name] = schema_entry

            logger.info(f"Updated column {request.column_name} in table {request.table_name}")
            return True

        except Exception as e:
            logger.error(
                f"Error updating column {request.column_name} in table {request.table_name}: {str(e)}"
            )
            raise

    @staticmethod
    async def reorder_columns(table_name: str, ordered_column_names: List[str]) -> bool:
        try:
            schema_entry = await Schema.get(table=table_name)
            column_schema = schema_entry.column_schema or []

            if not column_schema:
                return True

            primary_col_name = schema_entry.primary_business_column
            col_map = {col["name"]: col for col in column_schema}

            current_non_primary_cols = {c for c in col_map.keys() if c != primary_col_name}
            if set(ordered_column_names) != current_non_primary_cols:
                raise ValueError(
                    "The provided column list does not match the non-primary columns in the schema."
                )

            new_schema = []

            if primary_col_name in col_map:
                primary_col_def = col_map.pop(primary_col_name)
                primary_col_def["order"] = 0
                new_schema.append(primary_col_def)

            for i, col_name in enumerate(ordered_column_names):
                if col_name in col_map:
                    col_def = col_map.pop(col_name)
                    col_def["order"] = i + 1
                    new_schema.append(col_def)

            for i, col_def in enumerate(col_map.values()):
                col_def["order"] = len(new_schema) + i
                new_schema.append(col_def)

            schema_entry.column_schema = new_schema
            await schema_entry.save()
            DatabaseService._schema_cache[table_name] = schema_entry

            logger.info(f"Reordered columns for table {table_name}")
            return True

        except DoesNotExist:
            raise ValueError(f"Table {table_name} not found.")
        except Exception as e:
            logger.error(f"Error reordering columns for table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_table_schema(table_name: str) -> Optional[Schema]:
        if table_name in DatabaseService._schema_cache:
            return DatabaseService._schema_cache[table_name]
        try:
            schema = await Schema.get(table=table_name)
            DatabaseService._schema_cache[table_name] = schema
            return schema
        except DoesNotExist:
            return None

    @staticmethod
    async def list_tables() -> List[Schema]:
        schemas = await Schema.all()
        for s in schemas:
            DatabaseService._schema_cache[s.table] = s
        return schemas

    @staticmethod
    async def create_mapping_table(table_name: str, list_type: str) -> bool:
        mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
        try:
            if await DatabaseService.table_exists(mapping_table_name):
                logger.info(f"Mapping table {mapping_table_name} already exists")
                return True

            sql = ""
            if list_type == "default":
                sql = f"""
                    CREATE TABLE "{mapping_table_name}" (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        primary_id UUID NULL REFERENCES "{DatabaseService._table(table_name)}"(id) ON DELETE SET NULL,
                        platform_value TEXT NOT NULL,
                        platform_id TEXT NOT NULL REFERENCES "{DatabaseService._table("platforms")}"(id) ON DELETE RESTRICT,
                        value TEXT,
                        primary_table_column TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        UNIQUE (primary_id, platform_id, primary_table_column)
                    )
                """
            elif list_type == "sizing":
                sql = f"""
                    CREATE TABLE "{mapping_table_name}" (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        sizing_scheme TEXT NOT NULL,
                        platform_value TEXT NOT NULL,
                        platform TEXT NOT NULL REFERENCES "{DatabaseService._table("platforms")}"(id) ON DELETE RESTRICT,
                        value TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """
            else:
                raise ValueError(f"Invalid list type: {list_type}")

            await Tortoise.get_connection("default").execute_script(sql)

            if list_type == "default":
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_{mapping_table_name}_platform_id" ON "{mapping_table_name}" (platform_id)'
                )
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_sort_{mapping_table_name}_platform_value" ON "{mapping_table_name}" (platform_value)'
                )
            elif list_type == "sizing":
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_{mapping_table_name}_platform" ON "{mapping_table_name}" (platform)'
                )

            await Tortoise.get_connection("default").execute_script(
                f'CREATE INDEX "idx_gin_{mapping_table_name}_platform_value" ON "{mapping_table_name}" USING GIN (platform_value gin_trgm_ops)'
            )
            if list_type == "sizing":
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_gin_{mapping_table_name}_value" ON "{mapping_table_name}" USING GIN (value gin_trgm_ops)'
                )

            if list_type == "default":
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_gin_composite_default_{mapping_table_name}" ON "{mapping_table_name}" '
                    f"USING GIN (platform_id, primary_table_column gin_trgm_ops, platform_value gin_trgm_ops, value gin_trgm_ops)"
                )
            elif list_type == "sizing":
                await Tortoise.get_connection("default").execute_script(
                    f'CREATE INDEX "idx_gin_composite_sizing_{mapping_table_name}" ON "{mapping_table_name}" '
                    f"USING GIN (sizing_scheme gin_trgm_ops, platform_value gin_trgm_ops, value gin_trgm_ops)"
                )

            logger.info(
                f"Created mapping table {mapping_table_name} with individual and composite indexes."
            )
            return True

        except Exception as e:
            logger.error(f"Error creating mapping table {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_table_records(
        table_name: str,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[Dict[str, Any]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> Tuple[List[Dict[str, Any]], int]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                return [], 0

            where_conditions = []
            params = []

            if filters:
                for column, value in filters.items():
                    if value is not None:
                        where_conditions.append(
                            f'"{DatabaseService._table(table_name)}"."{column}" = ${len(params) + 1}'
                        )
                        params.append(value)

            if search:
                search_conditions = []
                if schema.column_schema:
                    search_param_index = len(params) + 1
                    for col_def in schema.column_schema:
                        col_type = col_def.get("type")
                        if col_type in ["text", "text_list", "number"]:
                            col_name = f'"{DatabaseService._table(table_name)}"."{col_def["name"]}"'
                            if col_type in ["text_list", "number"]:
                                search_conditions.append(
                                    f"{col_name}::text ILIKE ${search_param_index}"
                                )
                            else:
                                search_conditions.append(f"{col_name} ILIKE ${search_param_index}")
                    if table_name == "types":
                        types_search_cols = [
                            "division",
                            "dept",
                            "gender",
                            "class_name",
                            "reporting_category",
                        ]
                        for col in types_search_cols:
                            search_conditions.append(f"tp.{col} ILIKE ${search_param_index}")
                    if search_conditions:
                        where_conditions.append(f"({' OR '.join(search_conditions)})")
                        params.append(f"%{search}%")

            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            final_query_params = list(params)
            limit_param_index = len(final_query_params) + 1
            offset_param_index = len(final_query_params) + 2
            final_query_params.extend([page_size, (page - 1) * page_size])

            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")

            platforms_subquery = f""",
            (
                SELECT jsonb_agg(DISTINCT dl.platform_id)
                FROM "{mapping_table_name}" dl
                WHERE dl.primary_id = t.id
            ) as platforms
            """

            types_join_clause = ""
            types_select_clause = ""
            cte_join_clause = ""
            if table_name == "types":
                types_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id'
                types_select_clause = (
                    ", tp.division, tp.dept, tp.gender, tp.class_name, tp.reporting_category"
                )
                if search:
                    cte_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON "{DatabaseService._table(table_name)}".parent_id = tp.id'

            sort_column = sort_by if sort_by else schema.primary_business_column
            sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"

            if sort_by and schema.column_schema:
                valid_columns = {col["name"] for col in schema.column_schema}
                valid_columns.add("created_at")
                if sort_by not in valid_columns:
                    logger.warning(f"Invalid sort column '{sort_by}', falling back to created_at")
                    sort_column = schema.primary_business_column

            main_query = f"""
            WITH filtered_ids AS (
                SELECT "{DatabaseService._table(table_name)}".id, "{DatabaseService._table(table_name)}".{sort_column} FROM "{DatabaseService._table(table_name)}" {cte_join_clause} {where_clause}
            ),
            paginated_ids AS (
                SELECT id, COUNT(*) OVER() as total_records
                FROM filtered_ids
                ORDER BY {sort_column} {sort_direction}
                LIMIT ${limit_param_index} OFFSET ${offset_param_index}
            )
            SELECT
                t.*,
                pi.total_records
                {types_select_clause}
                {platforms_subquery}
            FROM "{DatabaseService._table(table_name)}" t
            JOIN paginated_ids pi ON t.id = pi.id
            {types_join_clause}
            ORDER BY t.{sort_column} {sort_direction}
            """
            fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                main_query, final_query_params
            )

            if not fetched_records:
                return [], 0

            total = fetched_records[0]["total_records"]

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            record_dicts = []

            for record in fetched_records:
                r = dict(record)
                del r["total_records"]

                for col_name, value in r.items():
                    if value is None:
                        continue

                    col_def = column_definitions.get(col_name)

                    if col_name == "platforms" and isinstance(value, str):
                        try:
                            r[col_name] = orjson.loads(value)
                        except (orjson.JSONDecodeError, TypeError):
                            logger.warning(
                                f"Could not decode JSON string for column '{col_name}' in table '{table_name}'. Value: {value}"
                            )
                        continue

                    if not col_def:
                        continue

                    col_type = col_def.get("type")

                    try:
                        if col_type == "number":
                            r[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                r[col_name] = value.lower() in ("true", "t", "1")
                            else:
                                r[col_name] = bool(value)
                        elif col_type in ["text_list", "platform_list"] and isinstance(value, str):
                            r[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(
                            f"Could not convert value '{value}' for column '{col_name}' (type: {col_type}) in table '{table_name}': {e}"
                        )

                record_dicts.append(r)

            return record_dicts, total

        except Exception as e:
            logger.error(
                f"Error getting records from table {table_name}: {str(traceback.format_exc())}"
            )
            raise

    @staticmethod
    async def get_platform_mappings_for_record(table_name: str, record_id: str) -> Dict[str, Any]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                return {}

            primary_business_column = schema.primary_business_column
            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")

            sql = f"""
                SELECT
                    jsonb_object_agg(
                        'platform_mapping_for_' || m.platform_id || '_of_{primary_business_column}',
                        m.platform_value
                    ) FILTER (WHERE m.platform_id IS NOT NULL) as platform_mappings
                FROM "{mapping_table_name}" as m
                WHERE m.primary_id = $1 AND m.primary_table_column = $2
            """
            params = [record_id, primary_business_column]
            result = await Tortoise.get_connection("default").execute_query_dict(sql, params)

            if result and result[0]["platform_mappings"]:
                mappings = result[0]["platform_mappings"]
                if isinstance(mappings, str):
                    return orjson.loads(mappings)
                return mappings
            return {}
        except Exception as e:
            logger.error(
                f"Error getting platform mappings for record {record_id} from table {table_name}: {str(traceback.format_exc())}"
            )
            raise

    @staticmethod
    async def insert_record(table_name: str, data: Dict[str, Any]) -> str:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                raise ValueError(f"Schema not found for table {table_name}")

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            if table_name == "types" and "parent_id" in data:
                column_definitions["parent_id"] = {"name": "parent_id", "type": "uuid"}
                column_definitions["type_code"] = {"name": "type_code", "type": "int"}

            column_types = {c["name"]: c.get("type") for c in (schema.column_schema or [])}
            if table_name == "types" and "parent_id" in data:
                column_types["parent_id"] = "uuid"
                column_types["type_code"] = "int"

            filtered_data = {key: value for key, value in data.items() if key in column_definitions}

            record_id = str(uuid.uuid4())

            columns = ["id"] + list(filtered_data.keys())

            processed_values = [record_id]
            for column, value in filtered_data.items():
                col_type = column_types.get(column)
                if col_type in ["text_list", "platform_list"] and isinstance(value, (list, dict)):
                    processed_values.append(orjson.dumps(value).decode("utf-8"))
                else:
                    processed_values.append(value)

            quoted_columns = [f'"{col}"' for col in columns]
            placeholders = [f"${i + 1}" for i in range(len(processed_values))]
            sql = f"""
                INSERT INTO "{DatabaseService._table(table_name)}" ({", ".join(quoted_columns)})
                VALUES ({", ".join(placeholders)})
                RETURNING id
            """

            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, processed_values
            )
            returned_id = result_list[0]["id"] if result_list else None
            if returned_id is None:
                logger.error(f"Insert into {table_name} did not return an ID. Data: {data}")
                raise IntegrityError(f"Insert into {table_name} failed to return an ID.")

            logger.info(f"Inserted record {returned_id} into table {table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return str(returned_id)

        except Exception as e:
            logger.error(f"Error inserting record into table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_record_by_id(table_name: str, record_id: str) -> Optional[Dict[str, Any]]:
        try:
            sql = f'SELECT * FROM "{DatabaseService._table(table_name)}" WHERE id = $1'
            result = await Tortoise.get_connection("default").execute_query_dict(sql, [record_id])
            if result:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Error getting record by ID from table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def update_record(
        table_name: str, record_id: str, data: Dict[str, Any], permissions: List[str]
    ) -> bool:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                raise ValueError(f"Schema not found for table {table_name}")

            primary_column_name = schema.primary_business_column
            if primary_column_name in data and "edit_record_names" not in permissions:
                original_record = await DatabaseService.get_record_by_id(table_name, record_id)
                if original_record and str(original_record.get(primary_column_name)) != str(
                    data[primary_column_name]
                ):
                    raise HTTPException(
                        status_code=403,
                        detail="Not authorized to edit record names.",
                    )

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}

            if table_name == "types" and "parent_id" in data:
                column_definitions["parent_id"] = {"name": "parent_id", "type": "uuid"}
                column_definitions["type_code"] = {"name": "type_code", "type": "int"}

            column_types = {c["name"]: c.get("type") for c in (schema.column_schema or [])}
            if table_name == "types" and "parent_id" in data:
                column_types["parent_id"] = "uuid"
                column_types["type_code"] = "int"

            filtered_data = {key: value for key, value in data.items() if key in column_definitions}

            set_parts = []
            query_params = []

            if not filtered_data:
                logger.warning(
                    f"No valid data fields provided for update on record {record_id} in table {table_name}. Only updating timestamp."
                )
            else:
                for i, (column, value) in enumerate(filtered_data.items()):
                    set_parts.append(f'"{column}" = ${i + 1}')

                    col_type = column_types.get(column)
                    if col_type in ["text_list", "platform_list"] and isinstance(
                        value, (list, dict)
                    ):
                        query_params.append(orjson.dumps(value).decode("utf-8"))
                    else:
                        query_params.append(value)

            set_parts.append("updated_at = NOW()")

            id_param_index = len(query_params) + 1
            query_params.append(record_id)

            sql = f"""
                UPDATE "{DatabaseService._table(table_name)}"
                SET {", ".join(set_parts)}
                WHERE id = ${id_param_index}
            """

            await Tortoise.get_connection("default").execute_query(sql, query_params)
            logger.info(f"Updated record {record_id} in table {table_name}")

            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))

            return True

        except Exception as e:
            logger.error(f"Error updating record {record_id} in table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def delete_record(table_name: str, record_id: str) -> bool:
        try:
            sql = f'DELETE FROM "{DatabaseService._table(table_name)}" WHERE id = $1'
            await Tortoise.get_connection("default").execute_query(sql, [record_id])
            logger.info(f"Deleted record {record_id} from table {table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return True

        except Exception as e:
            logger.error(f"Error deleting record {record_id} from table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def enable_fuzzy_for_primary_business_columns() -> int:
        try:
            schemas = await Schema.all()
            updated_count = 0

            for schema in schemas:
                if not schema.primary_business_column or not schema.column_schema:
                    continue

                column_schema = schema.column_schema.copy()
                updated = False

                for i, column_def in enumerate(column_schema):
                    if column_def.get("name") == schema.primary_business_column:
                        if not column_def.get("fuzzy_check", False):
                            column_schema[i]["fuzzy_check"] = True
                            updated = True
                            break

                if updated:
                    schema.column_schema = column_schema
                    await schema.save()
                    DatabaseService._schema_cache[schema.table] = schema
                    updated_count += 1
                    logger.info(
                        f"Enabled fuzzy checking for primary business column in table {schema.table}"
                    )

            return updated_count

        except Exception as e:
            logger.error(f"Error enabling fuzzy checking for primary business columns: {str(e)}")
            raise

    @staticmethod
    async def fuzzy_check_value(
        table_name: str,
        column_name: str,
        value: str,
        threshold: float = 0.3,
        exclude_record_id: Optional[str] = None,
    ) -> Tuple[List[str], bool]:
        try:
            params_exact = [value]
            exclude_clause_exact = ""
            if exclude_record_id:
                params_exact.append(exclude_record_id)
                exclude_clause_exact = f" AND id != ${len(params_exact)}"

            exact_match_sql = f"""
                SELECT "{column_name}" FROM "{DatabaseService._table(table_name)}"
                WHERE "{column_name}" = $1
                {exclude_clause_exact}
                LIMIT 1
            """
            exact_match_result = await Tortoise.get_connection("default").execute_query_dict(
                exact_match_sql, params_exact
            )

            exact_matches = [exact_match_result[0][column_name]] if exact_match_result else []

            params_similar = [value, threshold]
            exclude_clause_similar = ""
            if exclude_record_id:
                params_similar.append(exclude_record_id)
                exclude_clause_similar = f" AND id != ${len(params_similar)}"

            similarity_sql = f"""
                SELECT "{column_name}"
                FROM "{DatabaseService._table(table_name)}"
                WHERE similarity("{column_name}", $1) > $2
                AND "{column_name}" != $1
                {exclude_clause_similar}
                ORDER BY similarity("{column_name}", $1) DESC
                LIMIT 10
            """
            similar_records_dicts = await Tortoise.get_connection("default").execute_query_dict(
                similarity_sql, params_similar
            )

            similar_values = [record[column_name] for record in similar_records_dicts]

            return similar_values, exact_matches

        except Exception as e:
            logger.error(f"Error performing fuzzy check on {table_name}.{column_name}: {str(e)}")
            raise

    @staticmethod
    async def batch_fuzzy_check_values(
        table_name: str,
        columns_to_check: Dict[str, Any],
        threshold: float = 0.3,
        exclude_record_id: Optional[str] = None,
    ) -> Dict[str, FuzzyCheckResponse]:
        if not columns_to_check:
            return {}

        response_map: Dict[str, FuzzyCheckResponse] = {}

        schema = await DatabaseService.get_table_schema(table_name)
        if not schema:
            logger.error(f"Schema not found for table {table_name} in batch_fuzzy_check_values")
            return {}

        column_definitions = {c["name"]: c for c in schema.column_schema}
        conn = Tortoise.get_connection("default")

        for col_name, value in columns_to_check.items():
            col_def = column_definitions.get(col_name)
            if not col_def:
                continue

            is_unique = col_def.get("is_unique", False)
            is_fuzzy = col_def.get("fuzzy_check", False)

            if not (is_unique or is_fuzzy):
                continue

            current_response = FuzzyCheckResponse(similar_values=[], exact_matches=[])

            exclude_clause = ""
            base_params: List[Any] = []
            if exclude_record_id:
                exclude_clause = "AND t.id != $1"
                base_params.append(exclude_record_id)

            if col_def.get("type") == "text_list" and isinstance(value, list):
                sanitized_values = [v for v in value if v and str(v).strip()]
                if not sanitized_values:
                    continue

                if is_unique and len(sanitized_values) != len(set(sanitized_values)):
                    duplicates = list(
                        set([v for v in sanitized_values if sanitized_values.count(v) > 1])
                    )
                    current_response.exact_matches.extend(duplicates)

                if is_unique and not current_response.exact_matches:
                    params = list(base_params) + [sanitized_values]
                    sql = f"""
                        SELECT value
                        FROM "{DatabaseService._table(table_name)}" t, unnest(${(len(base_params) + 1)}::text[]) as value
                        WHERE t."{col_name}" ? value {exclude_clause}
                    """
                    try:
                        results = await conn.execute_query_dict(sql, params)
                        if results:
                            current_response.exact_matches.extend([r["value"] for r in results])

                    except Exception as e:
                        logger.error(
                            f"Error on text_list exact match for {table_name}.{col_name}: {e}"
                        )

                if is_fuzzy and not current_response.exact_matches:
                    params = list(base_params) + [sanitized_values, threshold]
                    sql = f"""
                        WITH input_values (v) as (SELECT * FROM unnest(${len(base_params) + 1}::text[]))
                        SELECT iv.v as input_value, elem.value as similar_value
                        FROM "{DatabaseService._table(table_name)}" t
                        CROSS JOIN LATERAL jsonb_array_elements_text(t."{col_name}") as elem
                        CROSS JOIN input_values iv
                        WHERE similarity(elem.value, iv.v) > ${len(base_params) + 2} AND elem.value != iv.v {exclude_clause}
                        ORDER BY iv.v, similarity(elem.value, iv.v) DESC
                    """
                    try:
                        result = await conn.execute_query_dict(sql, params)
                        similar_matches = [r["similar_value"] for r in result]
                        if similar_matches:
                            current_response.similar_values.extend(similar_matches)
                    except Exception as e:
                        logger.error(
                            f"Error on text_list fuzzy match for {table_name}.{col_name}: {e}"
                        )
            else:
                if is_unique:
                    params = list(base_params) + [value]
                    sql = f"""
                        SELECT "{col_name}" FROM "{DatabaseService._table(table_name)}" t WHERE t."{col_name}" = ${len(base_params) + 1} {exclude_clause} LIMIT 1
                    """
                    try:
                        result = await conn.execute_query_dict(sql, params)
                        if result:
                            current_response.exact_matches.append(result[0][col_name])
                    except Exception as e:
                        logger.error(f"Error on exact match for {table_name}.{col_name}: {e}")

                if is_fuzzy and col_def.get("type") == "text":
                    params = list(base_params) + [value, threshold]
                    sql = f"""
                        SELECT "{col_name}" as similar_value FROM "{DatabaseService._table(table_name)}" t
                        WHERE similarity(t."{col_name}", ${len(base_params) + 1}) > ${len(base_params) + 2}
                        AND t."{col_name}" != ${len(base_params) + 1} {exclude_clause}
                        ORDER BY similarity(t."{col_name}", ${len(base_params) + 1}) DESC LIMIT 10
                    """
                    try:
                        result = await conn.execute_query_dict(sql, params)
                        similar_values = [r["similar_value"] for r in result]
                        if similar_values:
                            current_response.similar_values.extend(similar_values)

                    except Exception as e:
                        logger.error(f"Error on fuzzy match for {table_name}.{col_name}: {e}")

            if current_response.exact_matches or current_response.similar_values:
                response_map[col_name] = current_response

        return response_map

    @staticmethod
    async def add_list_schema_definition(
        table_name: str, list_def_create: ListSchemaDefinition
    ) -> ListSchemaDefinition:
        if table_name == "sizing_lists":
            table_name = "sizes"

        table_schema = await Schema.get_or_none(table=table_name)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found.")

        if not await Platform.exists(id=list_def_create.platform_id):
            raise ValueError(f"Platform {list_def_create.platform_id} does not exist.")

        current_list_schemas = table_schema.list_schema or []

        for existing_def_dict in current_list_schemas:
            existing_def = ListSchemaDefinition(**existing_def_dict)
            if (
                existing_def.platform_id == list_def_create.platform_id
                and existing_def.list_type == list_def_create.list_type
            ):
                raise ValueError(
                    f"A {list_def_create.list_type} list definition for platform "
                    f"{list_def_create.platform_id} already exists for table {table_name}."
                )

        new_def_dict = list_def_create.model_dump()
        current_list_schemas.append(new_def_dict)
        table_schema.list_schema = current_list_schemas
        await table_schema.save()
        DatabaseService._schema_cache[table_name] = table_schema
        logger.info(
            f"Added list schema for {list_def_create.platform_id} ({list_def_create.list_type}) to table {table_name}"
        )
        return list_def_create

    @staticmethod
    async def get_list_schema_definitions(
        table_name: str,
    ) -> List[ListSchemaDefinition]:
        table_schema = await Schema.get_or_none(table=table_name)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found.")

        return [ListSchemaDefinition(**ls_dict) for ls_dict in table_schema.list_schema or []]

    @staticmethod
    async def update_list_schema_definition(
        table_name: str,
        platform_id: str,
        list_type: str,
        list_def_update: ListSchemaDefinitionUpdate,
    ) -> ListSchemaDefinition:
        table_schema = await Schema.get_or_none(table=table_name)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found.")

        updated_list_schemas = []
        found = False
        updated_definition = None

        for def_dict in table_schema.list_schema or []:
            current_def = ListSchemaDefinition(**def_dict)
            if current_def.platform_id == platform_id and current_def.list_type == list_type:
                found = True
                update_data = list_def_update.model_dump(exclude_unset=True)

                updated_def_dict = current_def.model_dump()
                updated_def_dict.update(update_data)

                try:
                    updated_definition = ListSchemaDefinition(**updated_def_dict)
                except Exception as e:
                    raise ValueError(f"Validation error during update: {str(e)}")

                updated_list_schemas.append(updated_definition.model_dump())
            else:
                updated_list_schemas.append(def_dict)

        if not found:
            raise ValueError(
                f"No {list_type} list definition found for platform {platform_id} in table {table_name}."
            )

        if (
            updated_definition
            and updated_definition.min_length is not None
            and updated_definition.max_length is not None
        ):
            if updated_definition.max_length < updated_definition.min_length:
                raise ValueError(
                    "max_length must be greater than or equal to min_length for platform_value"
                )

        table_schema.list_schema = updated_list_schemas
        await table_schema.save()
        DatabaseService._schema_cache[table_name] = table_schema
        logger.info(f"Updated list schema for {platform_id} ({list_type}) in table {table_name}")

        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))

        if not updated_definition:
            raise ValueError("Update failed unexpectedly.")
        return updated_definition

    @staticmethod
    async def delete_list_schema_definition(
        table_name: str, platform_id: str, list_type: str
    ) -> None:
        table_schema = await Schema.get_or_none(table=table_name)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found.")

        original_list_schemas = table_schema.list_schema or []
        updated_list_schemas = []
        deleted = False

        for def_dict in original_list_schemas:
            current_def = ListSchemaDefinition(**def_dict)
            if not (current_def.platform_id == platform_id and current_def.list_type == list_type):
                updated_list_schemas.append(def_dict)
            else:
                deleted = True

        if not deleted:
            raise ValueError(
                f"No {list_type} list definition found for platform {platform_id} to delete from table {table_name}."
            )

        table_schema.list_schema = updated_list_schemas
        await table_schema.save()
        DatabaseService._schema_cache[table_name] = table_schema

        logger.info(f"Deleted list schema for {platform_id} ({list_type}) from table {table_name}")

    @staticmethod
    async def get_list_records(
        table_name: str,
        list_type: str,
        platform_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        sizing_type: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        if table_name == "sizes":
            mapping_table_name = DatabaseService._table("sizes_default_list")
            params: List[Any] = []
            where_parts: List[str] = []
            idx = 1

            if platform_id:
                where_parts.append(f"dl.platform_id = ${idx}")
                params.append(platform_id)
                idx += 1

            if sizing_type:
                where_parts.append(
                    f"(dl.primary_table_column = ${idx} OR dl.primary_id IS NULL OR dl.primary_table_column IS NULL)"
                )
                params.append(sizing_type)
                idx += 1

            if search:
                like = f"%{search}%"
                where_parts.append(
                    f"(dl.platform_value ILIKE ${idx} OR (ss.sizing_scheme || ':' || ss.size) ILIKE ${idx + 1})"
                )
                params.extend([like, like])
                idx += 2

            where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

            limit_placeholder = f"${idx}"
            params.append(page_size)
            offset_placeholder = f"${idx + 1}"
            params.append((page - 1) * page_size)

            query = f"""
                WITH filtered_and_grouped AS (
                    SELECT
                        (array_agg(dl.id))[1] as id,
                        dl.platform_id,
                        dl.platform_value,
                        COALESCE(ARRAY_AGG(ss.sizing_scheme || ':' || ss.size) FILTER (WHERE ss.id IS NOT NULL), '{{}}') as internal_values,
                        MIN(dl.created_at) as created_at,
                        MAX(dl.updated_at) as updated_at
                    FROM "{DatabaseService._table("sizes_default_list")}" dl
                    LEFT JOIN "{DatabaseService._table("sizing_schemes")}" ss ON dl.primary_id = ss.id
                    {where_sql}
                    GROUP BY dl.platform_id, dl.platform_value
                ),
                counted AS (
                    SELECT *, COUNT(*) OVER() as total_records FROM filtered_and_grouped
                )
                SELECT * FROM counted
                ORDER BY platform_value ASC
                LIMIT {limit_placeholder} OFFSET {offset_placeholder}
            """
            rows = await Tortoise.get_connection("default").execute_query_dict(query, params)
            if not rows:
                return [], 0
            total = rows[0]["total_records"]
            for r in rows:
                del r["total_records"]
            return rows, total
        try:
            if list_type == "default":
                schema = await DatabaseService.get_table_schema(table_name)
                if not schema or not schema.primary_business_column:
                    return [], 0
                primary_col = schema.primary_business_column

                mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
                quoted_mapping_table = f'"{mapping_table_name}"'
                quoted_main_table = f'"{DatabaseService._table(table_name)}"'

                params: List[Any] = []
                where_parts: List[str] = []
                idx = 1

                if platform_id:
                    where_parts.append(f"dl.platform_id = ${idx}")
                    params.append(platform_id)
                    idx += 1

                if search:
                    like = f"%{search}%"
                    where_parts.append(
                        f'(dl.platform_value ILIKE ${idx} OR t."{primary_col}" ILIKE ${idx + 1})'
                    )
                    params.extend([like, like])
                    idx += 2

                where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

                limit_placeholder = f"${idx}"
                params.append(page_size)
                offset_placeholder = f"${idx + 1}"
                params.append((page - 1) * page_size)

                sort_column = sort_by if sort_by else "platform_value"
                sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"

                valid_sort_columns = {
                    "updated_at",
                    "created_at",
                    "platform_value",
                    "platform_id",
                }
                if sort_by and sort_by not in valid_sort_columns:
                    logger.warning(
                        f"Invalid sort column '{sort_by}' for default list, falling back to updated_at"
                    )
                    sort_column = "platform_value"

                query = f"""
                    WITH filtered_and_grouped AS (
                        SELECT
                            (array_agg(dl.id))[1] as id,
                            dl.platform_id,
                            dl.platform_value,
                            ARRAY_AGG(t."{primary_col}") FILTER (WHERE t."{primary_col}" IS NOT NULL) as internal_values,
                            MIN(dl.created_at) as created_at,
                            MAX(dl.updated_at) as updated_at
                        FROM {quoted_mapping_table} dl
                        LEFT JOIN {quoted_main_table} t ON dl.primary_id = t.id
                        {where_sql}
                        GROUP BY dl.platform_id, dl.platform_value
                    ),
                    counted AS (
                        SELECT *, COUNT(*) OVER() as total_records FROM filtered_and_grouped
                    )
                    SELECT * FROM counted
                    ORDER BY {sort_column} {sort_direction}
                    LIMIT {limit_placeholder} OFFSET {offset_placeholder}
                """
                rows = await Tortoise.get_connection("default").execute_query_dict(query, params)
                if not rows:
                    return [], 0
                total = rows[0]["total_records"]
                for r in rows:
                    del r["total_records"]
                return rows, total

            mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            where_conditions = []
            params = []
            param_idx_counter = 0

            if platform_id:
                param_idx_counter += 1
                where_conditions.append(f"platform_id = ${param_idx_counter}")
                params.append(platform_id)

            if search:
                search_conditions = []
                param_idx_counter += 1
                search_conditions.append(f"platform_value ILIKE ${param_idx_counter}")
                params.append(f"%{search}%")

                param_idx_counter += 1
                search_conditions.append(f"value ILIKE ${param_idx_counter}")
                params.append(f"%{search}%")

                if list_type == "sizing":
                    param_idx_counter += 1
                    search_conditions.append(f"sizing_scheme ILIKE ${param_idx_counter}")
                    params.append(f"%{search}%")
                elif list_type == "default":
                    param_idx_counter += 1
                    search_conditions.append(f"primary_table_column ILIKE ${param_idx_counter}")
                    params.append(f"%{search}%")

                where_conditions.append(f"({' OR '.join(search_conditions)})")

            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

            offset = (page - 1) * page_size
            params_for_query = list(params)

            param_idx_counter += 1
            limit_placeholder = f"${param_idx_counter}"
            params_for_query.append(page_size)

            param_idx_counter += 1
            offset_placeholder = f"${param_idx_counter}"
            params_for_query.append(offset)

            sort_column = sort_by if sort_by else "platform_value"
            sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"

            valid_sort_columns = {
                "created_at",
                "updated_at",
                "platform_value",
                "value",
                "sizing_scheme",
                "platform",
            }
            if sort_by and sort_by not in valid_sort_columns:
                logger.warning(
                    f"Invalid sort column '{sort_by}' for sizing list, falling back to created_at"
                )
                sort_column = "platform_value"

            records_sql = f"""
                SELECT *, COUNT(*) OVER() as total_records
                FROM {quoted_mapping_table_name}
                {where_clause}
                ORDER BY {sort_column} {sort_direction}
                LIMIT {limit_placeholder} OFFSET {offset_placeholder}
            """
            fetched_records_with_total = await Tortoise.get_connection(
                "default"
            ).execute_query_dict(records_sql, params_for_query)

            if not fetched_records_with_total:
                return [], 0

            total = fetched_records_with_total[0]["total_records"]
            record_dicts = []
            for record in fetched_records_with_total:
                r = dict(record)
                del r["total_records"]
                record_dicts.append(r)

            return record_dicts, total

        except Exception as e:
            logger.error(f"Error getting records from {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def add_default_list_entry(table_name: str, entry: DefaultListEntry) -> int:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            await DatabaseService.create_mapping_table(table_name, "default")

            platform = await DatabaseService.get_platform_by_id(entry.platform_id)
            if not platform:
                raise ValueError(f"Platform {entry.platform_id} does not exist")

            sql = f"""
                INSERT INTO {quoted_mapping_table_name}
                (primary_id, platform_value, platform_id, primary_table_column)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """

            params_insert = [
                entry.primary_id,
                entry.platform_value,
                entry.platform_id,
                entry.primary_table_column,
            ]
            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, params_insert
            )
            returned_id = result_list[0]["id"] if result_list else None

            if returned_id is None:
                logger.error(
                    f"Insert into {mapping_table_name} did not return an ID. Entry: {entry}"
                )
                raise IntegrityError(f"Insert into {mapping_table_name} failed to return an ID.")

            logger.info(f"Added entry {returned_id} to {mapping_table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return returned_id

        except Exception as e:
            logger.error(f"Error adding entry to {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def add_sizing_list_entry(table_name: str, entry: SizingListEntry) -> int:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_sizing_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            await DatabaseService.create_mapping_table(table_name, "sizing")

            platform = await DatabaseService.get_platform_by_id(entry.platform)
            if not platform:
                raise ValueError(f"Platform {entry.platform} does not exist")

            sql = f"""
                INSERT INTO {quoted_mapping_table_name}
                (sizing_scheme, platform_value, platform, value)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """

            params_insert = [
                entry.sizing_scheme,
                entry.platform_value,
                entry.platform,
                entry.value,
            ]
            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, params_insert
            )
            returned_id = result_list[0]["id"] if result_list else None

            if returned_id is None:
                logger.error(
                    f"Insert into {mapping_table_name} did not return an ID. Entry: {entry}"
                )
                raise IntegrityError(f"Insert into {mapping_table_name} failed to return an ID.")

            logger.info(f"Added entry {returned_id} to {mapping_table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return returned_id

        except Exception as e:
            logger.error(f"Error adding entry to {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def update_default_list_entry(
        table_name: str, entry_id: int, entry: DefaultListEntry
    ) -> bool:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            platform = await DatabaseService.get_platform_by_id(entry.platform_id)
            if not platform:
                raise ValueError(f"Platform {entry.platform_id} does not exist")

            sql = f"""
                UPDATE {quoted_mapping_table_name}
                SET primary_id = $1, platform_value = $2, platform_id = $3, value = $4,
                    primary_table_column = $5, updated_at = NOW()
                WHERE id = $6
            """

            params_update = [
                entry.primary_id,
                entry.platform_value,
                entry.platform_id,
                entry.value,
                entry.primary_table_column,
                entry_id,
            ]
            await Tortoise.get_connection("default").execute_query(sql, params_update)

            logger.info(f"Updated entry {entry_id} in {mapping_table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return True

        except Exception as e:
            logger.error(f"Error updating entry {entry_id} in {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def update_sizing_list_entry(
        table_name: str, entry_id: int, entry: SizingListEntry
    ) -> bool:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_sizing_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            platform = await DatabaseService.get_platform_by_id(entry.platform)
            if not platform:
                raise ValueError(f"Platform {entry.platform} does not exist")

            sql = f"""
                UPDATE {quoted_mapping_table_name}
                SET sizing_scheme = $1, platform_value = $2, platform = $3,
                    value = $4, updated_at = NOW()
                WHERE id = $5
            """

            params_update = [
                entry.sizing_scheme,
                entry.platform_value,
                entry.platform,
                entry.value,
                entry_id,
            ]
            await Tortoise.get_connection("default").execute_query(sql, params_update)

            logger.info(f"Updated entry {entry_id} in {mapping_table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return True

        except Exception as e:
            logger.error(f"Error updating entry {entry_id} in {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def delete_list_entry(table_name: str, list_type: str, entry_id: int) -> bool:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            sql = f"DELETE FROM {quoted_mapping_table_name} WHERE id = $1"
            await Tortoise.get_connection("default").execute_query(sql, [entry_id])

            logger.info(f"Deleted entry {entry_id} from {mapping_table_name}")
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return True

        except Exception as e:
            logger.error(f"Error deleting entry {entry_id} from {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_platforms_for_table(table_name: str) -> List[Dict[str, Any]]:
        try:
            default_list_table = DatabaseService._table(f"{table_name}_default_list")
            sizing_list_table = DatabaseService._table(f"{table_name}_sizing_list")

            quoted_default_list_table = f'"{default_list_table}"'
            quoted_sizing_list_table = f'"{sizing_list_table}"'
            quoted_platforms_table = f'"{DatabaseService._table("platforms")}"'

            platforms = set()

            sql_default = f"""
                SELECT DISTINCT p.id, p.display_name
                FROM {quoted_default_list_table} dl
                JOIN {quoted_platforms_table} p ON dl.platform_id = p.id::text 
            """
            records = await Tortoise.get_connection("default").execute_query_dict(sql_default)
            for record in records:
                platforms.add(
                    (
                        str(record["id"]),
                        record.get("display_name", str(record["id"])),
                    )
                )

            platform_list = [{"id": pid, "display_name": pname} for pid, pname in platforms]

            return platform_list

        except Exception as e:
            logger.error(f"Error getting platforms for table {table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_list_entry_by_id(
        table_name: str, list_type: str, entry_id: int
    ) -> Optional[Dict[str, Any]]:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            if not await DatabaseService.table_exists(mapping_table_name):
                return None

            sql = f"SELECT * FROM {quoted_mapping_table_name} WHERE id = $1"
            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, [entry_id]
            )

            if not result_list:
                return None

            return result_list[0]

        except Exception as e:
            logger.error(f"Error getting entry {entry_id} from {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def bulk_import_list_entries(
        table_name: str,
        list_type: str,
        entries: List[Dict[str, Any]],
        platform_id_for_upload: Optional[str] = None,
    ) -> int:
        if not entries:
            return 0

        mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
        quoted_mapping_table_name = f'"{mapping_table_name}"'
        conn = Tortoise.get_connection("default")
        total_rows_affected = 0
        CHUNK_SIZE = 1000

        try:
            table_schema = await DatabaseService.get_table_schema(table_name)
            if not table_schema or not table_schema.primary_business_column:
                raise ValueError(f"Primary business column not defined for table {table_name}.")
            primary_business_column_name = table_schema.primary_business_column

            platform_ids_to_check = set()
            if platform_id_for_upload:
                platform_ids_to_check.add(platform_id_for_upload)
            else:
                for entry in entries:
                    if platform_id := entry.get("platform_id") or entry.get("platform"):
                        platform_ids_to_check.add(platform_id)

            valid_platform_ids = set()
            if platform_ids_to_check:
                placeholders = ", ".join([f"${i + 1}" for i in range(len(platform_ids_to_check))])
                sql_valid_platforms = f'SELECT id FROM "{DatabaseService._table("platforms")}" WHERE id IN ({placeholders})'
                valid_platforms_result = await conn.execute_query_dict(
                    sql_valid_platforms, list(platform_ids_to_check)
                )
                valid_platform_ids = {str(p["id"]) for p in valid_platforms_result}

            if list_type == "default":
                columns = "(primary_id, platform_value, platform_id, primary_table_column)"
                num_columns = 4
            elif list_type == "sizing":
                columns = "(sizing_scheme, platform_value, platform, value)"
                num_columns = 4
            else:
                logger.warning(f"Invalid list_type {list_type} for bulk import. Skipping.")
                return 0

            for i in range(0, len(entries), CHUNK_SIZE):
                chunk = entries[i : i + CHUNK_SIZE]
                params_to_insert = []

                for entry_data in chunk:
                    platform_id_to_use = (
                        platform_id_for_upload
                        or entry_data.get("platform_id")
                        or entry_data.get("platform")
                    )

                    if not platform_id_to_use or platform_id_to_use not in valid_platform_ids:
                        logger.warning(
                            f"Skipping entry due to invalid or missing platform_id: {platform_id_to_use} in {entry_data}"
                        )
                        continue

                    current_platform_value = entry_data.get("Values") or entry_data.get(
                        "platform_value"
                    )
                    if not current_platform_value:
                        logger.warning(
                            f"Skipping entry due to missing 'platform_value' or 'Values' field: {entry_data}"
                        )
                        continue

                    if list_type == "default":
                        current_primary_id = entry_data.get("primary_id")
                        current_primary_table_column = (
                            primary_business_column_name
                            if platform_id_for_upload
                            else entry_data.get("primary_table_column")
                        )

                        if not platform_id_for_upload and not current_primary_table_column:
                            logger.warning(
                                f"Skipping entry due to missing 'primary_table_column': {entry_data}"
                            )
                            continue

                        params_to_insert.extend(
                            [
                                current_primary_id,
                                current_platform_value,
                                platform_id_to_use,
                                current_primary_table_column,
                            ]
                        )
                    elif list_type == "sizing":
                        if platform_id_for_upload:
                            logger.warning(
                                f"Skipping entry: Uploading only 'Values' is not supported for sizing lists. Entry: {entry_data}"
                            )
                            continue

                        if "sizing_scheme" not in entry_data:
                            logger.warning(
                                f"Skipping entry due to missing 'sizing_scheme': {entry_data}"
                            )
                            continue

                        params_to_insert.extend(
                            [
                                entry_data.get("sizing_scheme"),
                                current_platform_value,
                                platform_id_to_use,
                                entry_data.get("value"),
                            ]
                        )

                if not params_to_insert:
                    continue

                num_rows = len(params_to_insert) // num_columns
                value_placeholders = []
                param_idx = 1
                for _ in range(num_rows):
                    placeholders = f"({', '.join([f'${i}' for i in range(param_idx, param_idx + num_columns)])})"
                    value_placeholders.append(placeholders)
                    param_idx += num_columns

                sql = f"""
                    INSERT INTO {quoted_mapping_table_name} {columns}
                    VALUES {", ".join(value_placeholders)}
                    ON CONFLICT DO NOTHING
                """

                result = await conn.execute_query(sql, params_to_insert)
                rows_affected = result[0] if isinstance(result, tuple) else result
                total_rows_affected += rows_affected if rows_affected is not None else 0

            logger.info(
                f"Successfully imported {total_rows_affected} entries into {mapping_table_name}"
            )
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
            return total_rows_affected

        except Exception as e:
            logger.error(f"Error bulk importing entries into {mapping_table_name}: {str(e)}")
            raise

    @staticmethod
    async def get_all_platform_values_for_dropdown(
        table_name: str, platform_ids: List[str], primary_table_column: str
    ) -> Dict[str, List[str]]:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            if not platform_ids:
                return {}

            placeholders = [f"${i + 1}" for i in range(len(platform_ids))]
            platform_in_clause = f"({', '.join(placeholders)})"

            sql = f"""
                SELECT DISTINCT platform_id, platform_value
                FROM {quoted_mapping_table_name}
                WHERE platform_id IN {platform_in_clause} 
                AND primary_table_column = ${len(platform_ids) + 1}
                AND platform_value IS NOT NULL AND platform_value != ''
                ORDER BY platform_id, platform_value
            """

            params = platform_ids + [primary_table_column]
            result_list = await Tortoise.get_connection("default").execute_query_dict(sql, params)

            platform_values_dict = {platform_id: [] for platform_id in platform_ids}
            for record in result_list:
                platform = record["platform_id"]
                platform_value = record["platform_value"]
                if platform in platform_values_dict:
                    platform_values_dict[platform].append(platform_value)

            return platform_values_dict

        except Exception as e:
            logger.error(
                f"Error getting all platform values for dropdown from {mapping_table_name}: {str(e)}"
            )
            return {platform_id: [] for platform_id in platform_ids}

    @staticmethod
    async def get_platform_values_for_dropdown(
        table_name: str, platform_id: str, primary_table_column: str
    ) -> List[str]:
        try:
            mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
            quoted_mapping_table_name = f'"{mapping_table_name}"'

            if not await DatabaseService.table_exists(mapping_table_name):
                return []

            sql = f"""
                SELECT DISTINCT platform_value
                FROM {quoted_mapping_table_name}
                WHERE platform_id = $1 AND primary_table_column = $2
                AND platform_value IS NOT NULL AND platform_value != ''
                ORDER BY platform_value
            """

            result_list = await Tortoise.get_connection("default").execute_query_dict(
                sql, [platform_id, primary_table_column]
            )

            return [record["platform_value"] for record in result_list]

        except Exception as e:
            logger.error(
                f"Error getting platform values for dropdown from {mapping_table_name}: {str(e)}"
            )
            return []

    @staticmethod
    async def get_record_id_by_column_value(
        table_name: str,
        column_name: str,
        value: Any,
        exclude_record_id: Optional[str] = None,
    ) -> Optional[uuid.UUID]:
        try:
            params = [value]
            exclude_clause = ""
            if exclude_record_id:
                params.append(exclude_record_id)
                exclude_clause = f" AND id != ${len(params)}"

            sql = f"""SELECT id FROM "{DatabaseService._table(table_name)}" WHERE "{column_name}" = $1{exclude_clause} LIMIT 1"""

            result_list = await Tortoise.get_connection("default").execute_query_dict(sql, params)

            if result_list and result_list[0]["id"]:
                return uuid.UUID(str(result_list[0]["id"]))
            return None
        except Exception as e:
            logger.error(
                f"Error retrieving record ID from table '{table_name}' by column '{column_name}' for value '{value}': {str(e)}"
            )
            return None

    @staticmethod
    async def get_records_by_column_search(
        table_name: str, column_name: str, search_value: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                raise ValueError(f"Table '{table_name}' not found.")

            column_exists_in_schema = any(c["name"] == column_name for c in schema.column_schema)
            if not column_exists_in_schema:
                raise ValueError(
                    f"Column '{column_name}' not found in schema for table '{table_name}'."
                )

            sql = f"""
                SELECT * FROM "{DatabaseService._table(table_name)}"
                WHERE "{column_name}"::text ILIKE $1
                LIMIT $2
            """
            params = [f"%{search_value}%", limit]

            records = await Tortoise.get_connection("default").execute_query_dict(sql, params)

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            record_dicts = []

            for record in records:
                r = dict(record)
                for col_name, value in r.items():
                    if value is None:
                        continue

                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue

                    col_type = col_def.get("type")

                    try:
                        if col_type == "number":
                            r[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                r[col_name] = value.lower() in ("true", "t", "1")
                            else:
                                r[col_name] = bool(value)
                        elif col_type in ["text_list", "platform_list"] and isinstance(value, str):
                            r[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(
                            f"Could not convert value '{value}' for column '{col_name}' (type: {col_type}) in table '{table_name}': {e}"
                        )
                record_dicts.append(r)

            return record_dicts

        except Exception as e:
            logger.error(
                f"Error getting records from table {table_name} by column search: {str(e)}"
            )
            raise

    @staticmethod
    async def create_or_update_default_list_entry(
        table_name: str,
        platform_id: str,
        platform_value: str,
        record_id_from_main_table: uuid.UUID,
        internal_value: str,
        primary_business_column_name: str,
    ) -> None:
        mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
        quoted_mapping_table_name = f'"{mapping_table_name}"'

        try:
            if not await DatabaseService.table_exists(mapping_table_name):
                logger.error(
                    f"Default list table {mapping_table_name} does not exist. Cannot create/update entry for "
                    f"platform {platform_id} with value '{platform_value}' for main record {record_id_from_main_table}."
                )
                return

            if not await DatabaseService.get_platform_by_id(platform_id):
                logger.warning(
                    f"Platform with ID '{platform_id}' does not exist. Skipping default list entry creation/update for "
                    f"platform_value '{platform_value}' in {mapping_table_name} for main record {record_id_from_main_table}."
                )
                return

            find_sql = f"""
                SELECT id FROM {quoted_mapping_table_name}
                WHERE platform_id = $1 AND platform_value = $2
                LIMIT 1
            """
            existing_entry_list = await Tortoise.get_connection("default").execute_query_dict(
                find_sql, [platform_id, platform_value]
            )

            if existing_entry_list:
                existing_entry_id = existing_entry_list[0]["id"]
                update_sql = f"""
                    UPDATE {quoted_mapping_table_name}
                    SET primary_id = $1, primary_table_column = $2, updated_at = NOW()
                    WHERE id = $3
                """
                await Tortoise.get_connection("default").execute_query(
                    update_sql,
                    [
                        record_id_from_main_table,
                        primary_business_column_name,
                        existing_entry_id,
                    ],
                )
                logger.info(
                    f"Updated entry ID {existing_entry_id} in {mapping_table_name}: platform '{platform_id}', "
                    f"platform_value '{platform_value}' linked to main record {record_id_from_main_table}."
                )
            else:
                insert_sql = f"""
                    INSERT INTO {quoted_mapping_table_name}
                    (primary_id, platform_value, platform_id, primary_table_column, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    RETURNING id
                """
                params_insert = [
                    record_id_from_main_table,
                    platform_value,
                    platform_id,
                    primary_business_column_name,
                ]
                insert_result = await Tortoise.get_connection("default").execute_query_dict(
                    insert_sql, params_insert
                )

                if insert_result and insert_result[0]["id"]:
                    new_entry_id = insert_result[0]["id"]
                    logger.info(
                        f"Created new entry ID {new_entry_id} in {mapping_table_name}: platform '{platform_id}', "
                        f"platform_value '{platform_value}' linked to main record {record_id_from_main_table}."
                    )
                else:
                    logger.error(
                        f"Failed to create new entry in {mapping_table_name} for platform '{platform_id}', "
                        f"platform_value '{platform_value}'. No ID returned after insert."
                    )
            asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))

        except Exception as e:
            logger.error(
                f"Error in create_or_update_default_list_entry for {mapping_table_name} "
                f"(platform: {platform_id}, platform_value: '{platform_value}', main_record_id: {record_id_from_main_table}): {str(e)}"
            )

    @staticmethod
    async def bulk_upsert_default_list_entries(
        table_name: str,
        platform_mappings: List[Tuple[str, str]],
        record_id_from_main_table: uuid.UUID,
        internal_value: str,
        primary_business_column_name: str,
    ) -> Tuple[int, int]:
        mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
        quoted_mapping_table_name = f'"{mapping_table_name}"'
        updated_count = 0
        inserted_count = 0

        try:
            conn = Tortoise.get_connection("default")
            if not await DatabaseService.table_exists(mapping_table_name):
                logger.error(
                    f"Default list table {mapping_table_name} does not exist. Cannot bulk upsert entries."
                )
                return 0, 0

            current_mappings_sql = f"""
                SELECT id, platform_id, platform_value
                FROM {quoted_mapping_table_name}
                WHERE primary_id = $1
            """
            current_mappings_list = await conn.execute_query_dict(
                current_mappings_sql, [record_id_from_main_table]
            )
            current_mappings: Dict[str, Tuple[str, uuid.UUID]] = {
                row["platform_id"]: (row["platform_value"], row["id"])
                for row in current_mappings_list
            }

            desired_mappings: Dict[str, Optional[str]] = {
                pid: pval for pid, pval in platform_mappings
            }
            all_platform_ids = set(current_mappings.keys()) | set(desired_mappings.keys())

            mappings_to_add: Dict[str, str] = {}
            mappings_to_remove: Dict[str, str] = {}
            mappings_to_swap: Dict[str, Tuple[str, str]] = {}

            for pid in all_platform_ids:
                current_val, _ = current_mappings.get(pid, (None, None))
                desired_val = desired_mappings.get(pid)

                if current_val != desired_val:
                    if current_val and desired_val:
                        mappings_to_swap[pid] = (current_val, desired_val)
                    elif desired_val:
                        mappings_to_add[pid] = desired_val
                    elif current_val:
                        mappings_to_remove[pid] = current_val

            values_to_unmap = {(pid, val) for pid, val in mappings_to_remove.items()} | {
                (pid, old_val) for pid, (old_val, _) in mappings_to_swap.items()
            }

            if values_to_unmap:
                conditions = []
                params = []
                param_idx = 1
                for pid, pval in values_to_unmap:
                    conditions.append(
                        f"(platform_id = ${param_idx} AND platform_value = ${param_idx + 1})"
                    )
                    params.extend([pid, pval])
                    param_idx += 2

                count_sql = f"""
                    SELECT platform_id, platform_value, COUNT(primary_id) as link_count
                    FROM {quoted_mapping_table_name}
                    WHERE {" OR ".join(conditions)}
                    GROUP BY platform_id, platform_value
                """
                counts = await conn.execute_query_dict(count_sql, params)
                count_map = {
                    (row["platform_id"], row["platform_value"]): row["link_count"] for row in counts
                }

                pids_for_update_to_null = []
                pids_for_delete_link = []
                for pid, pval in values_to_unmap:
                    link_count = count_map.get((pid, pval), 0)
                    if link_count == 1:
                        pids_for_update_to_null.append(current_mappings[pid][1])
                    elif link_count > 1:
                        pids_for_delete_link.append(current_mappings[pid][1])

                if pids_for_update_to_null:
                    update_placeholders = ", ".join(
                        f"${i + 1}" for i in range(len(pids_for_update_to_null))
                    )
                    update_sql = f"""
                        UPDATE {quoted_mapping_table_name}
                        SET primary_id = NULL, updated_at = NOW()
                        WHERE id IN ({update_placeholders})
                    """
                    await conn.execute_query(update_sql, pids_for_update_to_null)
                    updated_count += len(pids_for_update_to_null)

                if pids_for_delete_link:
                    delete_placeholders = ", ".join(
                        f"${i + 1}" for i in range(len(pids_for_delete_link))
                    )
                    delete_sql = f"""
                        UPDATE {quoted_mapping_table_name}
                        SET primary_id = NULL, updated_at = NOW()
                        WHERE id IN ({delete_placeholders}) AND primary_id = ${len(pids_for_delete_link) + 1}
                    """
                    await conn.execute_query(
                        delete_sql, pids_for_delete_link + [record_id_from_main_table]
                    )
                    updated_count += len(pids_for_delete_link)

            values_to_map = {(pid, val) for pid, val in mappings_to_add.items()} | {
                (pid, new_val) for pid, (_, new_val) in mappings_to_swap.items()
            }

            if values_to_map:
                placeholders_map = []
                params_map = []
                param_idx = 1
                for pid, pval in values_to_map:
                    placeholders_map.append(
                        f"(platform_id = ${param_idx} AND platform_value = ${param_idx + 1})"
                    )
                    params_map.extend([pid, pval])
                    param_idx += 2

                find_placeholders_sql = f"""
                    SELECT id, platform_id, platform_value FROM {quoted_mapping_table_name}
                    WHERE primary_id IS NULL AND ({" OR ".join(placeholders_map)})
                """
                placeholders_to_update = await conn.execute_query_dict(
                    find_placeholders_sql, params_map
                )

                placeholders_map: Dict[Tuple[str, str], uuid.UUID] = {
                    (row["platform_id"], row["platform_value"]): row["id"]
                    for row in placeholders_to_update
                }

                pids_to_update = []
                inserts_to_perform = []

                for pid, pval in values_to_map:
                    if (pid, pval) in placeholders_map:
                        pids_to_update.append(placeholders_map[(pid, pval)])
                    else:
                        inserts_to_perform.append(
                            (
                                record_id_from_main_table,
                                pval,
                                pid,
                                primary_business_column_name,
                            )
                        )

                if pids_to_update:
                    update_placeholders_sql = ", ".join(
                        f"${i + 1}" for i in range(len(pids_to_update))
                    )
                    sql_update_placeholders = f"""
                        UPDATE {quoted_mapping_table_name}
                        SET primary_id = ${len(pids_to_update) + 1}, updated_at = NOW()
                        WHERE id IN ({update_placeholders_sql})
                    """
                    await conn.execute_query(
                        sql_update_placeholders,
                        pids_to_update + [record_id_from_main_table],
                    )
                    updated_count += len(pids_to_update)

                if inserts_to_perform:
                    insert_sql_start = f"""
                        INSERT INTO {quoted_mapping_table_name}
                        (primary_id, platform_value, platform_id, primary_table_column, created_at, updated_at)
                        VALUES """

                    value_placeholders = []
                    insert_params = []
                    param_idx = 1
                    for pk_id, p_val, p_id, pbc_name in inserts_to_perform:
                        placeholders = f"(${param_idx}, ${param_idx + 1}, ${param_idx + 2}, ${param_idx + 3}, NOW(), NOW())"
                        value_placeholders.append(placeholders)
                        insert_params.extend([pk_id, p_val, p_id, pbc_name])
                        param_idx += 4

                    if value_placeholders:
                        full_insert_sql = (
                            insert_sql_start
                            + ", ".join(value_placeholders)
                            + " ON CONFLICT (primary_id, platform_id, primary_table_column) DO UPDATE SET "
                            "platform_value = EXCLUDED.platform_value, updated_at = NOW()"
                        )
                        await conn.execute_query(full_insert_sql, insert_params)
                        inserted_count = len(inserts_to_perform)

            if updated_count > 0 or inserted_count > 0:
                logger.info(
                    f"Bulk upsert for {mapping_table_name} (main record: {record_id_from_main_table}): "
                    f"{updated_count} updated, {inserted_count} inserted."
                )
                asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))

            return updated_count, inserted_count

        except Exception as e:
            logger.error(
                f"Error in bulk_upsert_default_list_entries for {mapping_table_name} "
                f"(main_record_id: {record_id_from_main_table}): {traceback.format_exc()}"
            )
            raise

    @staticmethod
    async def sync_default_list_internal_values(
        table_name: str,
        platform_id: str,
        platform_value: str,
        internal_values: List[str],
        force: bool = False,
        sizing_type: Optional[str] = None,
    ) -> Tuple[int, int, int]:
        mapping_table = DatabaseService._table(f"{table_name}_default_list")
        quoted_mapping_table = f'"{mapping_table}"'
        conn = Tortoise.get_connection("default")

        if not await DatabaseService.table_exists(mapping_table):
            raise ValueError(f"Mapping table {mapping_table} does not exist")

        if table_name == "sizes":
            pids_new = set(internal_values) if internal_values else set()
            primary_col = sizing_type if sizing_type else "size"
        else:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema or not schema.primary_business_column:
                raise ValueError("Primary business column undefined for table")
            primary_col = schema.primary_business_column

            pids_new = set()
            if internal_values:
                sql_get_ids = f'SELECT id FROM "{DatabaseService._table(table_name)}" WHERE "{primary_col}" = ANY($1)'
                records = await conn.execute_query_dict(sql_get_ids, [list(internal_values)])
                pids_new = {str(r["id"]) for r in records}

        if force and pids_new:
            pids_list = list(pids_new)
            pids_placeholders = ", ".join([f"${i + 3}" for i in range(len(pids_list))])
            find_where = (
                f"platform_id = $1 AND platform_value != $2 AND primary_id IN ({pids_placeholders})"
            )
            find_params = [platform_id, platform_value] + pids_list
            if sizing_type:
                find_where += f" AND primary_table_column = ${len(pids_list) + 3}"
                find_params.append(sizing_type)
            sql_find_old_pvs = (
                f"SELECT DISTINCT platform_value FROM {quoted_mapping_table} WHERE {find_where}"
            )
            old_pv_records = await conn.execute_query_dict(sql_find_old_pvs, find_params)
            old_pvs_affected = {r["platform_value"] for r in old_pv_records}

            delete_placeholders = ", ".join([f"${i + 2}" for i in range(len(pids_list))])
            del_where = f"platform_id = $1 AND primary_id IN ({delete_placeholders})"
            del_params = [platform_id] + pids_list
            if sizing_type:
                del_where += f" AND primary_table_column = ${len(pids_list) + 2}"
                del_params.append(sizing_type)
            sql_delete_forced = f"DELETE FROM {quoted_mapping_table} WHERE {del_where}"
            await conn.execute_query(sql_delete_forced, del_params)

            for pv_old in old_pvs_affected:
                sql_check_any = f"SELECT 1 FROM {quoted_mapping_table} WHERE platform_id = $1 AND platform_value = $2 LIMIT 1"
                has_any_row = await conn.execute_query_dict(sql_check_any, [platform_id, pv_old])
                if not has_any_row:
                    sql_insert_null = f"""
                        INSERT INTO {quoted_mapping_table} (primary_id, platform_value, platform_id, primary_table_column)
                        VALUES (NULL, $1, $2, $3)
                    """
                    await conn.execute_query(sql_insert_null, [pv_old, platform_id, primary_col])

        if sizing_type:
            sql_get_current = f"SELECT primary_id FROM {quoted_mapping_table} WHERE platform_id = $1 AND platform_value = $2 AND primary_id IS NOT NULL AND primary_table_column = $3"
            current_pid_records = await conn.execute_query_dict(
                sql_get_current, [platform_id, platform_value, sizing_type]
            )
        else:
            sql_get_current = f"SELECT primary_id FROM {quoted_mapping_table} WHERE platform_id = $1 AND platform_value = $2 AND primary_id IS NOT NULL"
            current_pid_records = await conn.execute_query_dict(
                sql_get_current, [platform_id, platform_value]
            )
        pids_current = {str(r["primary_id"]) for r in current_pid_records}

        pids_to_add = pids_new - pids_current
        pids_to_remove = pids_current - pids_new

        deleted_count = 0
        if pids_to_remove:
            pids_remove_list = list(pids_to_remove)
            placeholders = ", ".join([f"${i + 3}" for i in range(len(pids_remove_list))])
            rm_sql = f"DELETE FROM {quoted_mapping_table} WHERE platform_id = $1 AND platform_value = $2 AND primary_id IN ({placeholders})"
            rm_params = [platform_id, platform_value] + pids_remove_list
            if sizing_type:
                rm_sql += f" AND primary_table_column = ${len(pids_remove_list) + 3}"
                rm_params.append(sizing_type)
            result = await conn.execute_query(rm_sql, rm_params)
            deleted_count = result[0] if isinstance(result, tuple) and result else 0

        added_count = 0
        if pids_to_add:
            insert_params = []
            for pid in pids_to_add:
                insert_params.extend([pid, platform_value, platform_id, primary_col])

            num_rows = len(pids_to_add)
            cols = "(primary_id, platform_value, platform_id, primary_table_column)"
            placeholders = ", ".join(
                [
                    f"(${(i * 4) + 1}, ${(i * 4) + 2}, ${(i * 4) + 3}, ${(i * 4) + 4})"
                    for i in range(num_rows)
                ]
            )
            sql_insert = f"INSERT INTO {quoted_mapping_table} {cols} VALUES {placeholders}"
            await conn.execute_query(sql_insert, insert_params)
            added_count = num_rows

        final_pids_count = len(pids_current) - deleted_count + added_count
        if final_pids_count == 0:
            if sizing_type:
                sql_check_any = f"SELECT 1 FROM {quoted_mapping_table} WHERE platform_id=$1 AND platform_value=$2 LIMIT 1"
                has_any = await conn.execute_query_dict(
                    sql_check_any, [platform_id, platform_value]
                )
                if not has_any:
                    sql_insert_null = f"""
                        INSERT INTO {quoted_mapping_table} (primary_id, platform_value, platform_id, primary_table_column)
                        VALUES (NULL, $1, $2, $3)
                    """
                    await conn.execute_query(
                        sql_insert_null, [platform_value, platform_id, primary_col]
                    )
                    added_count += 1
            else:
                sql_check_null = f"SELECT 1 FROM {quoted_mapping_table} WHERE platform_id=$1 AND platform_value=$2 AND primary_id IS NULL"
                has_null = await conn.execute_query_dict(
                    sql_check_null, [platform_id, platform_value]
                )
                if not has_null:
                    sql_delete_all = f"DELETE FROM {quoted_mapping_table} WHERE platform_id = $1 AND platform_value = $2"
                    await conn.execute_query(sql_delete_all, [platform_id, platform_value])

                    sql_insert_null = f"""
                        INSERT INTO {quoted_mapping_table} (primary_id, platform_value, platform_id, primary_table_column)
                        VALUES (NULL, $1, $2, $3)
                    """
                    await conn.execute_query(
                        sql_insert_null, [platform_value, platform_id, primary_col]
                    )
                    added_count += 1

        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
        return added_count, deleted_count, 0

    @staticmethod
    async def check_internal_value_conflicts(
        table_name: str,
        platform_id: str,
        platform_value: str,
        internal_values: List[str],
        sizing_type: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        if not internal_values:
            return []

        conn = Tortoise.get_connection("default")
        mapping_table = DatabaseService._table(f"{table_name}_default_list")
        quoted_mapping_table = f'"{mapping_table}"'

        if not await DatabaseService.table_exists(mapping_table):
            return []

        if table_name == "sizes":
            placeholders = ", ".join([f"${i+1}" for i in range(len(internal_values))])
            sizing_type_clause = ""
            params = list(internal_values) + [platform_id, platform_value]
            if sizing_type:
                sizing_type_clause = f"AND dl.primary_table_column = ${len(internal_values)+3}"
                params.append(sizing_type)
            sql_conflict_check = f"""
                SELECT ss.sizing_scheme || ':' || ss.size as internal_value,
                       dl.platform_value as conflicting_platform_value
                FROM {quoted_mapping_table} dl
                JOIN "{DatabaseService._table("sizing_schemes")}" ss ON dl.primary_id = ss.id
                WHERE dl.primary_id::text IN ({placeholders})
                  AND dl.platform_id = ${len(internal_values)+1}
                  AND dl.platform_value != ${len(internal_values)+2}
                  AND dl.primary_id IS NOT NULL
                  {sizing_type_clause}
            """
            conflict_results = await conn.execute_query_dict(sql_conflict_check, params)
            return [
                {
                    "internal_value": str(r["internal_value"]),
                    "conflicting_platform_value": str(r["conflicting_platform_value"]),
                }
                for r in conflict_results
            ]

        schema = await DatabaseService.get_table_schema(table_name)
        if not schema or not schema.primary_business_column:
            raise ValueError("Primary business column undefined for table")
        primary_col = schema.primary_business_column

        sql_conflict_check = f"""
            WITH input_records AS (
                SELECT id, "{primary_col}" as internal_value
                FROM "{DatabaseService._table(table_name)}"
                WHERE "{primary_col}" = ANY($1)
            )
            SELECT
                ir.internal_value,
                dl.platform_value as conflicting_platform_value
            FROM {quoted_mapping_table} dl
            JOIN input_records ir ON dl.primary_id = ir.id
            WHERE dl.platform_id = $2
            AND dl.platform_value != $3
            AND dl.primary_id IS NOT NULL
        """
        params = [internal_values, platform_id, platform_value]
        conflict_results = await conn.execute_query_dict(sql_conflict_check, params)

        return [
            {
                "internal_value": str(r["internal_value"]),
                "conflicting_platform_value": str(r["conflicting_platform_value"]),
            }
            for r in conflict_results
        ]

    @staticmethod
    async def check_type_code_exists(
        type_code: int, exclude_record_id: Optional[str] = None
    ) -> Optional[str]:
        try:
            params: List[Any] = [type_code]
            exclude_clause = ""
            if exclude_record_id:
                params.append(exclude_record_id)
                exclude_clause = f" AND id != ${len(params)}"

            sql = f"""SELECT "type" FROM "{DatabaseService._table("types")}" WHERE "type_code" = $1{exclude_clause} LIMIT 1"""

            result = await Tortoise.get_connection("default").execute_query_dict(sql, params)
            if result:
                return result[0].get("type")
            return None
        except Exception as e:
            logger.error(f"Error checking for type_code '{type_code}': {str(e)}")
            raise

    @staticmethod
    async def get_records_by_primary_column_search(
        table_name: str,
        search_value: str,
        page: int = 1,
        page_size: int = 50,
    ) -> Tuple[List[Dict[str, Any]], int]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema or not schema.primary_business_column:
                raise ValueError(f"Table '{table_name}' or its primary business column not found.")

            primary_col = schema.primary_business_column

            alias_col_name: Optional[str] = None
            for col_def in schema.column_schema or []:
                if col_def.get("type") == "text_list" and col_def.get("name") in {
                    "aliases",
                    "alias",
                }:
                    alias_col_name = col_def["name"]
                    break

            params: List[Any] = []
            search_param_placeholder = "$1"
            params.append(f"%{search_value}%")

            where_parts = [f't."{primary_col}"::text ILIKE {search_param_placeholder}']
            if alias_col_name:
                where_parts.append(
                    f'EXISTS (SELECT 1 FROM jsonb_array_elements_text(t."{alias_col_name}") AS alias_val WHERE alias_val ILIKE {search_param_placeholder})'
                )

            where_clause_sql = " OR ".join(where_parts)

            limit_param_placeholder = f"${len(params) + 1}"
            params.append(page_size)
            offset_param_placeholder = f"${len(params) + 1}"
            params.append((page - 1) * page_size)

            sql = f"""
                WITH filtered_ids AS (
                    SELECT id, created_at
                    FROM \"{DatabaseService._table(table_name)}\" t
                    WHERE {where_clause_sql}
                ),
                paginated_ids AS (
                    SELECT id, COUNT(*) OVER() AS total_records
                    FROM filtered_ids
                    ORDER BY created_at DESC
                    LIMIT {limit_param_placeholder} OFFSET {offset_param_placeholder}
                )
                SELECT t.*, pi.total_records
                FROM \"{DatabaseService._table(table_name)}\" t
                JOIN paginated_ids pi ON t.id = pi.id
                ORDER BY t.created_at DESC
            """
            fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                sql, params
            )

            if not fetched_records:
                return [], 0

            total_records = fetched_records[0]["total_records"]

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}

            processed_records: List[Dict[str, Any]] = []
            for record in fetched_records:
                rec_dict = dict(record)
                rec_dict.pop("total_records", None)
                rec_dict.pop(primary_col, None)
                if alias_col_name:
                    rec_dict.pop(alias_col_name, None)

                for col_name, value in list(rec_dict.items()):
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            rec_dict[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                rec_dict[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                rec_dict[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            rec_dict[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError):
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table '{table_name}'."
                        )

                processed_records.append(rec_dict)

            return processed_records, total_records
        except Exception as e:
            logger.error(f"Error fetching records for search in table '{table_name}': {e}")
            raise

    @staticmethod
    async def get_records_by_primary_column_exact_search(
        table_name: str,
        search_value: str,
        page: int = 1,
        page_size: int = 50,
        exclude_primary_and_alias: bool = True,
        search_alias: bool = True,
    ) -> Tuple[List[Dict[str, Any]], int]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema or not schema.primary_business_column:
                raise ValueError(f"Table '{table_name}' or its primary business column not found.")

            primary_col = schema.primary_business_column

            alias_col_name: Optional[str] = None
            for col_def in schema.column_schema or []:
                if col_def.get("type") == "text_list" and col_def.get("name") in {
                    "aliases",
                    "alias",
                }:
                    alias_col_name = col_def["name"]
                    break

            types_join_clause = ""
            types_select_clause = ""
            cte_join_clause = ""
            if table_name == "types":
                types_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id'
                types_select_clause = (
                    ", tp.division, tp.dept, tp.gender, tp.class_name, tp.reporting_category"
                )
                cte_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id'

            params: List[Any] = []
            search_param_placeholder = "$1"
            params.append(search_value.lower())

            where_parts = [f'LOWER(t."{primary_col}"::text) = LOWER({search_param_placeholder})']
            if alias_col_name and search_alias:
                where_parts.append(
                    f'EXISTS (SELECT 1 FROM jsonb_array_elements_text(t."{alias_col_name}") AS alias_val WHERE LOWER(alias_val) = LOWER({search_param_placeholder}))'
                )

            where_clause_sql = " OR ".join(where_parts)

            limit_param_placeholder = f"${len(params) + 1}"
            params.append(page_size)
            offset_param_placeholder = f"${len(params) + 1}"
            params.append((page - 1) * page_size)

            sql = f"""
                WITH filtered_ids AS (
                    SELECT t.id, t.created_at
                    FROM \"{DatabaseService._table(table_name)}\" t {cte_join_clause}
                    WHERE {where_clause_sql}
                ),
                paginated_ids AS (
                    SELECT id, COUNT(*) OVER() AS total_records
                    FROM filtered_ids
                    ORDER BY created_at DESC
                    LIMIT {limit_param_placeholder} OFFSET {offset_param_placeholder}
                )
                SELECT t.*, pi.total_records{types_select_clause}
                FROM \"{DatabaseService._table(table_name)}\" t
                JOIN paginated_ids pi ON t.id = pi.id
                {types_join_clause}
                ORDER BY t.created_at DESC
            """
            fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                sql, params
            )

            if not fetched_records:
                return [], 0

            total_records = fetched_records[0]["total_records"]

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}

            processed_records: List[Dict[str, Any]] = []
            for record in fetched_records:
                rec_dict = dict(record)
                rec_dict.pop("total_records", None)

                if exclude_primary_and_alias:
                    rec_dict.pop(primary_col, None)
                    if alias_col_name:
                        rec_dict.pop(alias_col_name, None)

                for col_to_exclude in columns_to_exclude:
                    rec_dict.pop(col_to_exclude, None)

                for col_name, value in list(rec_dict.items()):
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            rec_dict[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                rec_dict[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                rec_dict[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            rec_dict[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError):
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table '{table_name}'."
                        )

                processed_records.append(rec_dict)

            return processed_records, total_records
        except Exception as e:
            logger.error(f"Error fetching records for exact search in table '{table_name}': {e}")
            raise

    @staticmethod
    async def get_records_by_primary_column_exact_search_bulk(
        table_name: str,
        search_values: List[str],
        search_alias: bool = True,
    ) -> List[Dict[str, Any]]:
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema or not schema.primary_business_column:
                raise ValueError(f"Table '{table_name}' or its primary business column not found.")

            primary_col = schema.primary_business_column

            alias_col_name: Optional[str] = None
            for col_def in schema.column_schema or []:
                if col_def.get("type") == "text_list" and col_def.get("name") in {
                    "aliases",
                    "alias",
                }:
                    alias_col_name = col_def["name"]
                    break

            types_join_clause = ""
            types_select_clause = ""
            if table_name == "types":
                types_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id'
                types_select_clause = (
                    ", tp.division, tp.dept, tp.gender, tp.class_name, tp.reporting_category"
                )

            lowercase_search_values = [value.lower() for value in search_values]
            params: List[Any] = [lowercase_search_values]
            search_param_placeholder = "$1"

            where_parts = [f'LOWER(t."{primary_col}"::text) = ANY({search_param_placeholder})']
            if alias_col_name and search_alias:
                where_parts.append(
                    f'EXISTS (SELECT 1 FROM jsonb_array_elements_text(t."{alias_col_name}") AS alias_val WHERE LOWER(alias_val) = ANY({search_param_placeholder}))'
                )

            where_clause_sql = " OR ".join(where_parts)

            sql = f"""
                SELECT t.*{types_select_clause}
                FROM "{DatabaseService._table(table_name)}" t
                {types_join_clause}
                WHERE {where_clause_sql}
                ORDER BY t.created_at DESC
            """
            fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                sql, params
            )

            if not fetched_records:
                return []

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}

            processed_records: List[Dict[str, Any]] = []
            for record in fetched_records:
                rec_dict = dict(record)
                rec_dict.pop("total_records", None)

                for col_to_exclude in columns_to_exclude:
                    rec_dict.pop(col_to_exclude, None)

                for col_name, value in list(rec_dict.items()):
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            rec_dict[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                rec_dict[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                rec_dict[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            rec_dict[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError):
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table '{table_name}'."
                        )

                processed_records.append(rec_dict)

            return processed_records

        except Exception as e:
            logger.error(
                f"Error fetching records for bulk exact search in table '{table_name}': {e}"
            )
            raise

    @staticmethod
    async def get_all_types() -> List[Dict[str, Any]]:
        try:
            schema = await DatabaseService.get_table_schema("types")
            if not schema:
                return []

            sql = f"""
                SELECT
                    t.*,
                    tp.division,
                    tp.dept,
                    tp.gender,
                    tp.class_name,
                    tp.reporting_category
                FROM "{DatabaseService._table("types")}" t
                LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id
                ORDER BY t.type ASC
            """

            fetched_records = await Tortoise.get_connection("default").execute_query_dict(sql)

            if not fetched_records:
                return []

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}

            processed_records: List[Dict[str, Any]] = []
            for record in fetched_records:
                rec_dict = dict(record)

                for col_to_exclude in columns_to_exclude:
                    rec_dict.pop(col_to_exclude, None)

                for col_name, value in list(rec_dict.items()):
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            rec_dict[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                rec_dict[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                rec_dict[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            rec_dict[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError):
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table 'types'."
                        )

                processed_records.append(rec_dict)

            return processed_records

        except Exception as e:
            logger.error(f"Error getting all types: {str(e)}")
            raise

    @staticmethod
    async def get_all_brands() -> List[Dict[str, Any]]:
        try:
            schema = await DatabaseService.get_table_schema("brands")
            if not schema:
                return []

            sql = f"""
                SELECT *
                FROM "{DatabaseService._table("brands")}"
                ORDER BY brand ASC
            """

            fetched_records = await Tortoise.get_connection("default").execute_query_dict(sql)

            if not fetched_records:
                return []

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "created_at", "updated_at"}

            processed_records: List[Dict[str, Any]] = []
            for record in fetched_records:
                rec_dict = dict(record)

                for col_to_exclude in columns_to_exclude:
                    rec_dict.pop(col_to_exclude, None)

                for col_name, value in list(rec_dict.items()):
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            rec_dict[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                rec_dict[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                rec_dict[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            rec_dict[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError):
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table 'brands'."
                        )

                processed_records.append(rec_dict)

            return processed_records

        except Exception as e:
            logger.error(f"Error getting all brands: {str(e)}")
            raise

    @staticmethod
    async def add_aliases_to_record(
        table_name: str,
        new_aliases: List[str],
        record_id: Optional[str] = None,
        primary_column_value: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if not (bool(record_id) ^ bool(primary_column_value)):
                raise ValueError(
                    "Either 'record_id' or 'primary_column_value' must be provided, but not both"
                )

            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                raise ValueError(f"Table '{table_name}' not found")

            primary_col = schema.primary_business_column
            if not primary_col:
                raise ValueError(f"Primary business column not defined for table '{table_name}'")

            alias_col_name = next(
                (
                    col_def["name"]
                    for col_def in schema.column_schema or []
                    if col_def.get("type") == "text_list"
                    and col_def.get("name") in {"aliases", "alias"}
                ),
                None,
            )

            if not alias_col_name:
                raise ValueError(f"Table '{table_name}' does not have an alias column.")

            if record_id:
                where_clause = "id = $1"
                params = [record_id]
            else:
                where_clause = f'"{primary_col}"::text = $1'
                params = [primary_column_value]

            current_record_list = await Tortoise.get_connection("default").execute_query_dict(
                f'SELECT * FROM "{DatabaseService._table(table_name)}" WHERE {where_clause}', params
            )

            if not current_record_list:
                return {
                    "record_found": False,
                    "data": None,
                    "added_aliases": [],
                    "failed_aliases": [],
                }

            current_record = current_record_list[0]
            actual_record_id = current_record["id"]
            current_primary_value = current_record[primary_col]

            existing_aliases = current_record.get(alias_col_name) or []
            if isinstance(existing_aliases, str):
                existing_aliases = orjson.loads(existing_aliases)

            existing_aliases_lower = {str(a).lower() for a in existing_aliases}

            aliases_to_check = []
            processed_lower = set()
            for alias in new_aliases:
                if alias and alias.strip():
                    alias_lower = alias.strip().lower()
                    if (
                        alias_lower not in existing_aliases_lower
                        and alias_lower not in processed_lower
                    ):
                        aliases_to_check.append(alias.strip())
                        processed_lower.add(alias_lower)

            added_aliases = []
            failed_aliases = []

            if aliases_to_check:
                lower_aliases_to_check = [a.lower() for a in aliases_to_check]
                placeholders = ", ".join([f"${i + 1}" for i in range(len(lower_aliases_to_check))])

                primary_check_query = f'SELECT "{primary_col}" FROM "{DatabaseService._table(table_name)}" WHERE LOWER("{primary_col}"::text) IN ({placeholders}) AND id != $${actual_record_id}$$'
                conflicts_primary = await Tortoise.get_connection("default").execute_query_dict(
                    primary_check_query, lower_aliases_to_check
                )

                conflict_map_primary = {
                    str(row[primary_col]).lower(): str(row[primary_col])
                    for row in conflicts_primary
                }

                alias_check_query = f"""
                    SELECT "{primary_col}", value as alias
                    FROM "{DatabaseService._table(table_name)}", jsonb_array_elements_text("{alias_col_name}") as value
                    WHERE LOWER(value) IN ({placeholders}) AND id != $${actual_record_id}$$
                """
                conflicts_alias = await Tortoise.get_connection("default").execute_query_dict(
                    alias_check_query, lower_aliases_to_check
                )

                conflict_map_aliases = {
                    str(row["alias"]).lower(): {
                        "color": str(row[primary_col]),
                        "alias": str(row["alias"]),
                    }
                    for row in conflicts_alias
                }

                for alias in aliases_to_check:
                    alias_lower = alias.lower()
                    if alias_lower == str(current_primary_value).lower():
                        failed_aliases.append(
                            {
                                "alias": alias,
                                "existing_color": str(current_primary_value),
                                "existing_alias": str(current_primary_value),
                            }
                        )
                    elif alias_lower in conflict_map_primary:
                        collided_val = conflict_map_primary[alias_lower]
                        failed_aliases.append(
                            {
                                "alias": alias,
                                "existing_color": collided_val,
                                "existing_alias": collided_val,
                            }
                        )
                    elif alias_lower in conflict_map_aliases:
                        info = conflict_map_aliases[alias_lower]
                        failed_aliases.append(
                            {
                                "alias": alias,
                                "existing_color": info["color"],
                                "existing_alias": info["alias"],
                            }
                        )
                    else:
                        added_aliases.append(alias)

            if added_aliases:
                updated_aliases = existing_aliases + added_aliases
                await DatabaseService.update_record(
                    table_name,
                    actual_record_id,
                    {alias_col_name: updated_aliases},
                    [],
                )

            updated_record_list = await Tortoise.get_connection("default").execute_query_dict(
                f'SELECT * FROM "{DatabaseService._table(table_name)}" WHERE id = $1',
                [actual_record_id],
            )

            updated_record_data = None
            if updated_record_list:
                column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
                columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}
                record = dict(updated_record_list[0])

                for col_to_exclude in columns_to_exclude:
                    record.pop(col_to_exclude, None)

                for col_name, value in record.items():
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            record[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                record[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                record[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            record[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(f"Could not convert value for column '{col_name}': {e}")

                asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update(table_name))
                updated_record_data = record

            return {
                "record_found": True,
                "data": updated_record_data,
                "added_aliases": added_aliases,
                "failed_aliases": failed_aliases,
            }

        except Exception as e:
            logger.error(f"Error adding aliases to record in table '{table_name}': {e}")
            raise

    @staticmethod
    async def bulk_add_aliases_to_records(
        table_name: str,
        aliases_map: Dict[str, List[str]],
    ) -> Dict[str, Any]:
        results = {}
        for primary_value, aliases in aliases_map.items():
            try:
                response = await DatabaseService.add_aliases_to_record(
                    table_name=table_name,
                    new_aliases=aliases,
                    primary_column_value=primary_value,
                )
                results[primary_value] = response
            except Exception as e:
                logger.error(
                    f"Failed to add aliases for '{primary_value}' in table '{table_name}': {e}"
                )
                results[primary_value] = {
                    "data": None,
                    "added_aliases": [],
                    "failed_aliases": [
                        {
                            "alias": a,
                            "existing_color": "N/A",
                            "existing_alias": f"Error: {e}",
                        }
                        for a in aliases
                    ],
                }
        return results

    @staticmethod
    async def get_all_platforms() -> List[Platform]:
        if not DatabaseService._platform_cache:
            await DatabaseService._populate_platform_cache()
        return list(DatabaseService._platform_cache.values())

    @staticmethod
    async def get_platform_by_id(platform_id: str) -> Optional[Platform]:
        if not DatabaseService._platform_cache:
            await DatabaseService._populate_platform_cache()
        return DatabaseService._platform_cache.get(platform_id)

    @staticmethod
    async def _populate_platform_cache() -> None:
        try:
            platforms = await Platform.all()
            DatabaseService._platform_cache = {platform.id: platform for platform in platforms}
            logger.info(f"Platform cache populated with {len(platforms)} platforms")
        except Exception as e:
            logger.error(f"Error populating platform cache: {str(e)}")
            raise

    @staticmethod
    async def invalidate_platform_cache() -> None:
        DatabaseService._platform_cache.clear()
        logger.info("Platform cache invalidated")

    @staticmethod
    async def update_platform_cache(platform: Platform) -> None:
        DatabaseService._platform_cache[platform.id] = platform
        logger.info(f"Platform cache updated for platform {platform.id}")

    @staticmethod
    async def remove_platform_from_cache(platform_id: str) -> None:
        DatabaseService._platform_cache.pop(platform_id, None)
        logger.info(f"Platform {platform_id} removed from cache")

    @staticmethod
    async def get_conflicting_platform_value(
        table_name: str, platform_id: str, primary_id: str
    ) -> Optional[str]:
        mapping_table = DatabaseService._table(f"{table_name}_default_list")
        quoted_mapping_table = f'"{mapping_table}"'
        conn = Tortoise.get_connection("default")
        try:
            sql = f"SELECT platform_value FROM {quoted_mapping_table} WHERE platform_id = $1 AND primary_id = $2"
            result = await conn.execute_query_dict(sql, [platform_id, primary_id])
            if result:
                return result[0]["platform_value"]
            return None
        except Exception as e:
            logger.error(f"Error getting conflicting platform value: {e}")
            return None

    @staticmethod
    async def get_primary_business_column_value_by_id(
        table_name: str, record_id: str
    ) -> Optional[str]:
        conn = Tortoise.get_connection("default")
        try:
            schema = await DatabaseService.get_table_schema(table_name)
            if not schema or not schema.primary_business_column:
                logger.error(f"Primary business column not found for table {table_name}")
                return None

            primary_col = schema.primary_business_column
            quoted_table_name = f'"{DatabaseService._table(table_name)}"'
            quoted_primary_col = f'"{primary_col}"'

            sql = f"SELECT {quoted_primary_col} FROM {quoted_table_name} WHERE id = $1"
            result = await conn.execute_query_dict(sql, [record_id])

            if result:
                return result[0][primary_col]
            return None
        except Exception as e:
            logger.error(f"Error getting primary business column value: {e}")
            return None

    @staticmethod
    async def get_all_records_for_export(
        table_name: str,
    ) -> List[Dict[str, Any]]:
        try:
            if table_name == "sizes":
                sizing_schemes_sql = f'SELECT sizing_scheme, size, "order", sizing_types FROM "{DatabaseService._table("sizing_schemes")}" ORDER BY sizing_scheme, "order"'
                sizing_schemes_records = await Tortoise.get_connection(
                    "default"
                ).execute_query_dict(sizing_schemes_sql)

                sizing_list_table = DatabaseService._table("sizing_lists")
                sizing_list_records = []
                if await DatabaseService.table_exists(sizing_list_table):
                    sizing_list_sql = f"""
                        SELECT
                            ss.sizing_scheme,
                            ss.size as value,
                            sl.platform_id,
                            sl.platform_value
                        FROM "{DatabaseService._table("sizing_lists")}" sl
                        JOIN "{DatabaseService._table("sizing_schemes")}" ss ON sl.sizing_scheme_entry_id = ss.id
                    """
                    sizing_list_records = await Tortoise.get_connection(
                        "default"
                    ).execute_query_dict(sizing_list_sql)

                all_platforms = await DatabaseService.get_all_platforms()
                platform_dict = {p.id: p.name for p in all_platforms}

                mappings = {}
                for rec in sizing_list_records:
                    key = (rec["sizing_scheme"], rec["value"])
                    platform_name = platform_dict.get(rec["platform_id"])
                    if platform_name:
                        if key not in mappings:
                            mappings[key] = {}
                        mappings[key][f"{platform_name} Size"] = rec["platform_value"]

                result_records = []
                for scheme in sizing_schemes_records:
                    key = (scheme["sizing_scheme"], scheme["size"])

                    sizing_types_val = scheme.get("sizing_types")
                    if sizing_types_val and isinstance(sizing_types_val, str):
                        try:
                            sizing_types_val = orjson.loads(sizing_types_val)
                        except orjson.JSONDecodeError:
                            sizing_types_val = []

                    row = {
                        "Sizing Scheme": scheme["sizing_scheme"],
                        "Size": scheme["size"],
                        "Size Types": (
                            ", ".join(sizing_types_val)
                            if isinstance(sizing_types_val, list)
                            else ""
                        ),
                    }
                    if key in mappings:
                        row.update(mappings[key])
                    result_records.append(row)

                return result_records

            schema = await DatabaseService.get_table_schema(table_name)
            if not schema:
                return []

            primary_col = schema.primary_business_column

            types_join_clause = ""
            types_select_clause = ""
            if table_name == "types":
                types_join_clause = f'LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id'
                types_select_clause = (
                    ", tp.division, tp.dept, tp.gender, tp.class_name, tp.reporting_category"
                )

            main_query = f"""
            SELECT
                t.*
                {types_select_clause}
            FROM "{DatabaseService._table(table_name)}" t
            {types_join_clause}
            ORDER BY t."{primary_col}" ASC
            """

            fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                main_query
            )

            if not fetched_records:
                return []

            mapping_records = await Tortoise.get_connection("default").execute_query_dict(
                f'select * from "{DatabaseService._table(f"{table_name}_default_list")}" where primary_id is not null'
            )

            all_platforms = await DatabaseService.get_all_platforms()

            platform_dict = {p.id: p for p in all_platforms}

            mapping_records_df = pd.DataFrame(mapping_records)
            primary_col_display = schema.primary_business_column

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            record_dicts = []
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}

            for record in fetched_records:
                r = dict(record)
                record_id = r["id"]

                for col_name, value in r.items():
                    if value is None:
                        continue

                    col_def = column_definitions.get(col_name)

                    if not col_def:
                        continue

                    col_type = col_def.get("type")

                    try:
                        if col_type == "number":
                            r[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                r[col_name] = value.lower() in ("true", "t", "1")
                            else:
                                r[col_name] = bool(value)
                        elif col_type in ["text_list", "platform_list"] and isinstance(value, str):
                            r[col_name] = ", ".join(orjson.loads(value))
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(
                            f"Could not convert value '{value}' for column '{col_name}' (type: {col_type}) in table '{table_name}': {e}"
                        )

                for col_to_exclude in columns_to_exclude:
                    r.pop(col_to_exclude, None)
                try:
                    platform_values = mapping_records_df[
                        mapping_records_df["primary_id"] == record_id
                    ]

                    for index, row in platform_values.iterrows():
                        platform_id = row["platform_id"]
                        platform_name = platform_dict[platform_id].name
                        r[f"{platform_name} {primary_col_display}"] = row["platform_value"]
                except Exception as e:
                    logger.error(f"Error getting platform values: {traceback.format_exc()}")
                    pass
                record_dicts.append(r)

            return record_dicts

        except Exception as e:
            logger.error(
                f"Error getting all records from table {table_name} for export: {str(traceback.format_exc())}"
            )
            raise

    @staticmethod
    async def get_all_list_records_for_export(
        table_name: str,
        list_type: str,
        platform_id: str,
    ) -> List[Dict[str, Any]]:
        try:
            platform = await DatabaseService.get_platform_by_id(platform_id)
            platform_name = platform.name if platform else platform_id

            if list_type == "default":
                if table_name == "sizes":
                    query = f"""
                        SELECT dl.platform_value,
                               CASE WHEN ss.id IS NOT NULL THEN ss.sizing_scheme || ':' || ss.size ELSE '' END as internal_value
                        FROM "{DatabaseService._table("sizes_default_list")}" dl
                        LEFT JOIN "{DatabaseService._table("sizing_schemes")}" ss ON dl.primary_id = ss.id
                        WHERE dl.platform_id = $1
                        ORDER BY dl.platform_value, ss.sizing_scheme, ss.size
                    """
                    rows = await Tortoise.get_connection("default").execute_query_dict(
                        query, [platform_id]
                    )
                    return [
                        {
                            f"{platform_name} Size": row["platform_value"],
                            "Lux Size": row["internal_value"] or "",
                        }
                        for row in rows
                    ]

                schema = await DatabaseService.get_table_schema(table_name)
                if not schema or not schema.primary_business_column:
                    return []

                primary_col = schema.primary_business_column
                primary_col_display = primary_col
                for col_def in schema.column_schema:
                    if col_def.get("name") == primary_col:
                        primary_col_display = col_def.get("display_name", primary_col)
                        break

                mapping_table_name = DatabaseService._table(f"{table_name}_default_list")
                quoted_mapping_table = f'"{mapping_table_name}"'
                quoted_main_table = f'"{DatabaseService._table(table_name)}"'

                query = f"""
                    SELECT
                        dl.platform_value,
                        t."{primary_col}" as internal_value
                    FROM {quoted_mapping_table} dl
                    LEFT JOIN {quoted_main_table} t ON dl.primary_id = t.id
                    WHERE dl.platform_id = $1
                    ORDER BY dl.platform_value, t."{primary_col}"
                """
                rows = await Tortoise.get_connection("default").execute_query_dict(
                    query, [platform_id]
                )

                export_rows = []
                for row in rows:
                    export_row = {
                        f"{platform_name} {primary_col_display}": row["platform_value"],
                        f"Lux {primary_col_display}": (
                            row["internal_value"] if row["internal_value"] is not None else ""
                        ),
                    }
                    export_rows.append(export_row)

            if list_type == "sizing":
                mapping_table_name = DatabaseService._table(f"{table_name}_{list_type}_list")
                quoted_mapping_table_name = f'"{mapping_table_name}"'

                records_sql = f"""
                    SELECT sizing_scheme, platform_value, value
                    FROM {quoted_mapping_table_name}
                    WHERE platform_id = $1
                    ORDER BY sizing_scheme, platform_value
                """

                fetched_records = await Tortoise.get_connection("default").execute_query_dict(
                    records_sql, [platform_id]
                )

                export_rows = []
                for record in fetched_records:
                    export_row = {
                        "Sizing Scheme": record["sizing_scheme"],
                        f"{platform_name} Size": record["platform_value"],
                        "Lux Size": record["value"],
                    }
                    export_rows.append(export_row)

            return export_rows

        except Exception as e:
            logger.error(f"Error getting all list records for export: {str(e)}")
            raise

    @staticmethod
    async def get_product_types_by_class(class_name: str) -> List[Dict[str, Any]]:
        try:
            types_query = f"""
                SELECT
                    t.*,
                    tp.division,
                    tp.dept,
                    tp.gender,
                    tp.class_name,
                    tp.reporting_category
                FROM "{DatabaseService._table("types")}" t
                LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id
                WHERE t.parent_id IN (
                    SELECT id FROM "{DatabaseService._table("types_parents")}"
                    WHERE class_name ILIKE $1
                )
                ORDER BY t.type_code ASC
            """
            types_results = await Tortoise.get_connection("default").execute_query_dict(
                types_query, [f"%{class_name}%"]
            )

            schema = await DatabaseService.get_table_schema("types")
            if not schema:
                return []

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}
            processed_records = []

            for record in types_results:
                r = dict(record)

                for col_to_exclude in columns_to_exclude:
                    r.pop(col_to_exclude, None)

                for col_name, value in r.items():
                    if value is None:
                        continue

                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue

                    col_type = col_def.get("type")

                    try:
                        if col_type == "number":
                            r[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                r[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                r[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            r[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table 'types': {e}"
                        )

                processed_records.append(r)

            return processed_records

        except Exception as e:
            logger.error(f"Error getting product types by class '{class_name}': {str(e)}")
            raise

    @staticmethod
    async def get_sizing_schemes_by_product_type(
        product_type: str,
    ) -> List[SizingSchemeDetailResponse]:
        try:
            query = f"""
                WITH unaggregated_schemes AS (
                    SELECT
                        ss.sizing_scheme,
                        ss.sizing_types,
                        ss.id,
                        ss.size,
                        ss."order"
                    FROM "{DatabaseService._table("sizing_schemes")}" ss
                    WHERE EXISTS (
                        SELECT 1 FROM "{DatabaseService._table("types")}" t
                        WHERE t."type" = $1
                        AND t.sizing_types IS NOT NULL AND t.sizing_types != ''
                        AND ss.sizing_types @> to_jsonb(t.sizing_types)
                    )
                )
                SELECT
                    sizing_scheme,
                    sizing_types,
                    json_agg(
                        json_build_object('id', id, 'size', size, 'order', "order")
                        ORDER BY "order"
                    ) as sizes
                FROM unaggregated_schemes
                GROUP BY sizing_scheme, sizing_types
                ORDER BY sizing_scheme
            """

            results = await Tortoise.get_connection("default").execute_query_dict(
                query, [product_type]
            )

            sizing_schemes = []
            for result in results:
                sizes = []
                if result["sizes"]:
                    sizes_data = result["sizes"]
                    if isinstance(sizes_data, str):
                        sizes_data = orjson.loads(sizes_data)

                    for size_dict in sizes_data:
                        sizes.append(
                            SizingSchemeEntryWithId(
                                id=size_dict["id"],
                                size=size_dict["size"],
                                order=size_dict["order"],
                            )
                        )

                sizing_types = result["sizing_types"]
                if isinstance(sizing_types, str):
                    sizing_types = orjson.loads(sizing_types)

                sizing_schemes.append(
                    SizingSchemeDetailResponse(
                        sizing_scheme=result["sizing_scheme"],
                        sizes=sizes,
                        sizing_types=sizing_types,
                    )
                )

            return sizing_schemes

        except Exception as e:
            logger.error(
                f"Error getting sizing schemes by product type '{product_type}': {str(traceback.format_exc())}"
            )
            raise

    @staticmethod
    async def bulk_get_product_types_by_class(
        class_names: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        try:
            if not class_names:
                return {}

            types_query = f"""
                SELECT
                    t.*,
                    tp.division,
                    tp.dept,
                    tp.gender,
                    tp.class_name,
                    tp.reporting_category
                FROM "{DatabaseService._table("types")}" t
                LEFT JOIN "{DatabaseService._table("types_parents")}" tp ON t.parent_id = tp.id
                WHERE t.parent_id IN (
                    SELECT id FROM "{DatabaseService._table("types_parents")}"
                    WHERE class_name = ANY($1)
                )
                ORDER BY tp.class_name, t.type_code ASC
            """
            types_results = await Tortoise.get_connection("default").execute_query_dict(
                types_query, [class_names]
            )

            schema = await DatabaseService.get_table_schema("types")
            if not schema:
                return {}

            column_definitions = {c["name"]: c for c in (schema.column_schema or [])}
            columns_to_exclude = {"id", "parent_id", "created_at", "updated_at"}

            processed_records = {}
            for record in types_results:
                class_name = record.get("class_name")
                if not class_name:
                    continue

                r = dict(record)
                for col_to_exclude in columns_to_exclude:
                    r.pop(col_to_exclude, None)

                for col_name, value in r.items():
                    if value is None:
                        continue
                    col_def = column_definitions.get(col_name)
                    if not col_def:
                        continue
                    col_type = col_def.get("type")
                    try:
                        if col_type == "number":
                            r[col_name] = float(value)
                        elif col_type == "bool":
                            if isinstance(value, str):
                                r[col_name] = value.lower() in {"true", "t", "1"}
                            else:
                                r[col_name] = bool(value)
                        elif col_type in {"text_list", "platform_list"} and isinstance(value, str):
                            r[col_name] = orjson.loads(value)
                    except (ValueError, TypeError, orjson.JSONDecodeError) as e:
                        logger.warning(
                            f"Could not convert value for column '{col_name}' in table 'types': {e}"
                        )

                if class_name not in processed_records:
                    processed_records[class_name] = []
                processed_records[class_name].append(r)

            return processed_records

        except Exception as e:
            logger.error(f"Error getting bulk product types by class: {str(e)}")
            raise

    @staticmethod
    async def bulk_get_sizing_schemes_by_product_type(
        product_types: List[str],
    ) -> Dict[str, List[SizingSchemeDetailResponse]]:
        try:
            if not product_types:
                return {}

            query = f"""
                WITH matching_types AS (
                    SELECT "type", sizing_types
                    FROM "{DatabaseService._table("types")}"
                    WHERE "type" = ANY($1)
                    AND sizing_types IS NOT NULL AND sizing_types::text != '[]' AND sizing_types::text != ''
                ),
                unaggregated_schemes AS (
                    SELECT
                        mt."type" as product_type,
                        ss.sizing_scheme,
                        ss.sizing_types,
                        ss.id,
                        ss.size,
                        ss."order"
                    FROM "{DatabaseService._table("sizing_schemes")}" ss
                    JOIN matching_types mt ON ss.sizing_types @> to_jsonb(mt.sizing_types)
                )
                SELECT
                    product_type,
                    sizing_scheme,
                    sizing_types,
                    json_agg(
                        json_build_object('id', id, 'size', size, 'order', "order")
                        ORDER BY "order"
                    ) as sizes
                FROM unaggregated_schemes
                GROUP BY product_type, sizing_scheme, sizing_types
                ORDER BY product_type, sizing_scheme
            """

            results = await Tortoise.get_connection("default").execute_query_dict(
                query, [product_types]
            )

            schemes_by_type = {pt: [] for pt in product_types}
            for result in results:
                sizes = []
                if result["sizes"]:
                    sizes_data = result["sizes"]
                    if isinstance(sizes_data, str):
                        sizes_data = orjson.loads(sizes_data)

                    for size_dict in sizes_data:
                        sizes.append(
                            SizingSchemeEntryWithId(
                                id=size_dict["id"],
                                size=size_dict["size"],
                                order=size_dict["order"],
                            )
                        )

                sizing_types = result["sizing_types"]
                if isinstance(sizing_types, str):
                    sizing_types = orjson.loads(sizing_types)

                scheme = SizingSchemeDetailResponse(
                    sizing_scheme=result["sizing_scheme"],
                    sizes=sizes,
                    sizing_types=sizing_types,
                )

                product_type = result["product_type"]
                if product_type in schemes_by_type:
                    schemes_by_type[product_type].append(scheme)

            return schemes_by_type

        except Exception as e:
            logger.error(
                f"Error getting bulk sizing schemes by product type: {str(traceback.format_exc())}"
            )
            raise
