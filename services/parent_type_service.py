import logging
from typing import List, Dict, Optional
from tortoise.expressions import Q
from tortoise.functions import Min, Max, Count
from tortoise.exceptions import DoesNotExist
from tortoise import Tortoise
from tortoise.transactions import in_transaction
import traceback
from listingoptions.models.db_models import ParentType, Gender, ReportingCategory

logger = logging.getLogger(__name__)


class ParentTypeService:
    @staticmethod
    async def get_all_parent_types(
        search: Optional[str] = None,
        page: int = 1,
        page_size: int = 10,
        fetch_all: bool = False,
    ) -> Dict:
        if fetch_all:
            items = await ParentType.all()
            return {"total": len(items), "items": items}

        query = ParentType.all()
        if search:
            query = query.filter(
                Q(division__icontains=search)
                | Q(dept__icontains=search)
                | Q(class_name__icontains=search)
                | Q(gender__icontains=search)
                | Q(reporting_category__icontains=search)
            )

        total_count = await query.count()
        items = await query.offset((page - 1) * page_size).limit(page_size)

        return {"total": total_count, "items": items}

    @staticmethod
    async def check_class_code_conflict(new_class_code: int, old_class_code: int) -> List[Dict]:
        try:
            conn = Tortoise.get_connection("default")
            sql = """
                SELECT t.type, t.type_code, c.candidate_type, c.candidate_code
                FROM "listingoptions_types" t
                JOIN (
                    SELECT CAST($1 || RIGHT(CAST(type_code AS TEXT), 2) AS INTEGER) AS candidate_code, type as candidate_type
                    FROM "listingoptions_types"
                    WHERE LEFT(CAST(type_code AS TEXT), 3) = $2
                ) c ON t.type_code = c.candidate_code
            """
            conflicting_types = await conn.execute_query_dict(
                sql, [str(new_class_code), str(old_class_code)]
            )
            return conflicting_types
        except Exception as e:
            logger.error(f"Error checking class code conflict: {traceback.format_exc()}")
            raise ValueError("Error checking class code conflict")

    @staticmethod
    async def update_parent_type(
        parent_type_id,
        division: str,
        dept_code: int,
        class_name: str,
        class_code_suffix: int,
        gender: Gender,
        reporting_category: ReportingCategory,
    ) -> ParentType:
        async with in_transaction("default") as conn:
            try:
                parent_type_to_update = (
                    await ParentType.filter(id=parent_type_id).using_db(conn).get()
                )
            except DoesNotExist:
                raise ValueError("ParentType not found.")

            old_class_code = parent_type_to_update.class_code

            prefix_map = {
                Gender.MENS: "Mens",
                Gender.WOMENS: "Womens",
                Gender.BOYS: "Boys",
                Gender.GIRLS: "Girls",
                Gender.UNISEX: "Unisex",
            }

            if gender in prefix_map:
                expected_prefix = prefix_map[gender]
                if not class_name.startswith(expected_prefix):
                    raise ValueError(f"Class name must start with the prefix '{expected_prefix}'.")

            base_entry = await ParentType.filter(division=division, dept_code=dept_code).first()

            if not base_entry:
                raise ValueError("Selected Division and Department combination not found.")

            dept = base_entry.dept
            is_single_digit_dept = dept_code < 10

            if is_single_digit_dept:
                if not (1 <= class_code_suffix <= 99):
                    raise ValueError(
                        "For single-digit departments, the suffix must be between 1 and 99."
                    )
                new_class_code = int(f"{dept_code}{str(class_code_suffix).zfill(2)}")
            else:
                if not (1 <= class_code_suffix <= 9):
                    raise ValueError(
                        "For double-digit departments, the suffix must be between 1 and 9."
                    )
                new_class_code = int(f"{dept_code}{str(class_code_suffix)}")

            if old_class_code != new_class_code:
                conflicting_types = await ParentTypeService.check_class_code_conflict(
                    new_class_code, old_class_code
                )
                if conflicting_types:
                    conflict_details = ", ".join(
                        [
                            f"'{item['type']}' (Code: {item['type_code']})"
                            for item in conflicting_types
                        ]
                    )
                    raise ValueError(
                        f"New class code {new_class_code} conflicts with existing types: {conflict_details}."
                    )

            existing_class = await ParentType.filter(
                Q(class_code=new_class_code)
                | Q(division=division, dept=dept, class_name=class_name),
                id__not=parent_type_id,
            ).first()

            if existing_class:
                if existing_class.class_code == new_class_code:
                    raise ValueError(
                        f"Code {new_class_code} is already used by class '{existing_class.class_name}'."
                    )
                if existing_class.class_name.lower() == class_name.lower():
                    raise ValueError(
                        f"Class name '{class_name}' already exists with code {existing_class.class_code}."
                    )
                raise ValueError(
                    f"Entry conflicts with existing class: '{existing_class.class_name}' (Code: {existing_class.class_code})."
                )

            parent_type_to_update.division = division
            parent_type_to_update.dept_code = dept_code
            parent_type_to_update.dept = dept
            parent_type_to_update.class_code = new_class_code
            parent_type_to_update.class_name = class_name
            parent_type_to_update.gender = gender
            parent_type_to_update.reporting_category = reporting_category

            await parent_type_to_update.save(using_db=conn)

            if old_class_code != new_class_code:
                update_sql = 'UPDATE "listingoptions_types" SET type_code = CAST($1 || RIGHT(CAST(type_code AS TEXT), 2) AS INTEGER) WHERE LEFT(CAST(type_code AS TEXT), 3) = $2'
                await conn.execute_query(update_sql, [str(new_class_code), str(old_class_code)])

            return parent_type_to_update

    @staticmethod
    async def get_divisions() -> List[Dict]:
        divisions = await ParentType.all().order_by("division").distinct().values("division")
        return divisions

    @staticmethod
    async def get_departments(division: str) -> List[Dict]:
        departments_query = (
            await ParentType.filter(division=division)
            .order_by("dept_code")
            .values("dept_code", "dept")
        )
        unique_departments = {}
        for dept in departments_query:
            if dept["dept"] not in unique_departments:
                unique_departments[dept["dept"]] = dept
        return list(unique_departments.values())

    @staticmethod
    async def get_classes(division: str, dept: str) -> List[Dict]:
        classes = (
            await ParentType.filter(division=division, dept=dept)
            .order_by("class_code")
            .values("id", "class_code", "class_name", "gender")
        )
        return classes

    @staticmethod
    async def create_parent_type(
        division: str,
        dept_code: int,
        class_name: str,
        class_code_suffix: int,
        gender: Gender,
        reporting_category: ReportingCategory,
    ) -> ParentType:
        prefix_map = {
            Gender.MENS: "Mens",
            Gender.WOMENS: "Womens",
            Gender.BOYS: "Boys",
            Gender.GIRLS: "Girls",
            Gender.UNISEX: "Unisex",
        }

        if gender in prefix_map:
            expected_prefix = prefix_map[gender]
            if not class_name.startswith(expected_prefix):
                raise ValueError(f"Class name must start with the prefix '{expected_prefix}'.")

        base_entry = await ParentType.filter(division=division, dept_code=dept_code).first()

        if not base_entry:
            raise ValueError("Selected Division and Department combination not found.")

        dept = base_entry.dept

        is_single_digit_dept = dept_code < 10

        if is_single_digit_dept:
            if not (1 <= class_code_suffix <= 99):
                raise ValueError(
                    "For single-digit departments, the suffix must be between 1 and 99."
                )
            new_class_code = int(f"{dept_code}{str(class_code_suffix).zfill(2)}")
        else:
            if not (1 <= class_code_suffix <= 9):
                raise ValueError(
                    "For double-digit departments, the suffix must be between 1 and 9."
                )
            new_class_code = int(f"{dept_code}{str(class_code_suffix)}")

        existing_class = await ParentType.filter(
            Q(class_code=new_class_code) | Q(division=division, dept=dept, class_name=class_name)
        ).first()

        if existing_class:
            if existing_class.class_code == new_class_code:
                raise ValueError(
                    f"Code {new_class_code} is already used by class '{existing_class.class_name}'."
                )
            if existing_class.class_name.lower() == class_name.lower():
                raise ValueError(
                    f"Class name '{class_name}' already exists with code {existing_class.class_code}."
                )
            raise ValueError(
                f"Entry conflicts with existing class: '{existing_class.class_name}' (Code: {existing_class.class_code})."
            )

        new_parent_type = await ParentType.create(
            division=division,
            dept_code=dept_code,
            dept=dept,
            class_code=new_class_code,
            class_name=class_name,
            gender=gender,
            reporting_category=reporting_category,
        )

        return new_parent_type

    @staticmethod
    async def get_parent_hierarchy(parent_id: str) -> Dict:
        try:
            parent_type = await ParentType.get(id=parent_id)
            return {
                "id": str(parent_type.id),
                "division": parent_type.division,
                "dept_code": parent_type.dept_code,
                "dept": parent_type.dept,
                "class_id": str(parent_type.id),
                "class_code": parent_type.class_code,
                "class_name": parent_type.class_name,
                "gender": parent_type.gender.value if parent_type.gender else None,
                "reporting_category": (
                    parent_type.reporting_category.value if parent_type.reporting_category else None
                ),
            }
        except Exception as e:
            logger.error(f"Error getting parent hierarchy for ID {parent_id}: {str(e)}")
            raise ValueError(f"Parent type not found")

    @staticmethod
    async def delete_parent_type(parent_type_id: str):
        async with in_transaction("default") as conn:
            try:
                existing_types_query = 'select * from "listingoptions_types" where parent_id = $1'
                existing_types = await conn.execute_query_dict(
                    existing_types_query, [parent_type_id]
                )

                if existing_types:
                    raise ValueError("Class has types associated with it.")

                parent_type_to_delete = (
                    await ParentType.filter(id=parent_type_id).using_db(conn).get()
                )

                types_query = (
                    'update "listingoptions_types" set type_code = null where parent_id is null'
                )
                await conn.execute_query(types_query)

            except DoesNotExist:
                raise ValueError("Class not found.")

            await parent_type_to_delete.delete(using_db=conn)
