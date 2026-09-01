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
    MAX_US_SIZE_LENGTH,
)
import logging
import asyncio
from listingoptions.services.database_service import DatabaseService
from listingoptions.services.spreadsheet_service import spreadsheet_service

logger = logging.getLogger(__name__)

# goat_code and region_code are SCHEME-level, denormalized onto every size row of the scheme (the
# same shape as sizing_types) because there is no sizing scheme table. Every write path normalizes
# blank to NULL so the column holds two states, not three - an empty string would silently satisfy
# every future `WHERE goat_code IS NULL` check.
MAX_GOAT_CODE_LENGTH = 100
MAX_REGION_CODE_LENGTH = 20

# The fields that may go into the blanket scheme-wide UPDATE in update_scheme_size_orders. That
# statement writes one value to every row of the scheme, so it is only safe for scheme-level
# columns. us_size is PER SIZE ROW and must never appear here: adding it would stamp a single
# value across the whole scheme and destroy every distinct US size in it.
SCHEME_LEVEL_FIELDS = ("sizing_types", "goat_code", "region_code", "require_us_size")


def _blank_to_none(value: Optional[str]) -> Optional[str]:
    """'', '   ' and None all mean 'no value'. One representation of empty, everywhere."""
    if value is None:
        return None
    return value.strip() or None


def _validate_code_lengths(goat_code: Optional[str], region_code: Optional[str]) -> None:
    """Length-check here rather than leaning on Pydantic's max_length.

    A Pydantic rejection is a 422, and sendRequest.js renders every 422 as "Invalid request"
    without reading `detail` - the same uselessness that keeps required-ness out of the models.
    Raising ValueError gets it converted to a 400 whose message the operator can act on.
    """
    if goat_code is not None and len(goat_code) > MAX_GOAT_CODE_LENGTH:
        raise ValueError(f"GOAT code must be {MAX_GOAT_CODE_LENGTH} characters or fewer.")
    if region_code is not None and len(region_code) > MAX_REGION_CODE_LENGTH:
        raise ValueError(f"Region code must be {MAX_REGION_CODE_LENGTH} characters or fewer.")


def _require_complete_us_sizes(pairs) -> None:
    """A scheme that declares it needs US sizes must actually have them all.

    `pairs` is (size, effective us_size). Half-filled is the worst state available: the submit
    gate blocks on the missing ones, and the person who hits that block is a lister mid-listing
    rather than whoever curates the scheme. The editor blocks this client-side; this is the
    backstop for every other caller.

    ValueError, so the route turns it into a 400 whose detail the UI renders. Kept short - it goes
    into a snackbar.
    """
    missing = [size for size, us in pairs if not (us or "").strip()]
    if not missing:
        return
    shown = ", ".join(missing[:6]) + (", ..." if len(missing) > 6 else "")
    raise ValueError(
        f"{len(missing)} size{'' if len(missing) == 1 else 's'} still need a US size: {shown}. "
        'Fill them in, or turn off "Requires a US size".'
    )


def _validate_us_sizes(sizes) -> None:
    """Same reasoning as _validate_code_lengths: a 400 the operator can act on, not a 500.

    us_size is varchar(50). Without this an over-long value reaches Tortoise, whose
    ValidationError is not a ValueError subclass, so it falls past the route's `except ValueError`
    into the generic handler and returns a 500 with raw ORM text.
    """
    for size_data in sizes:
        us_size = getattr(size_data, "us_size", None)
        if us_size is not None and len(us_size) > MAX_US_SIZE_LENGTH:
            raise ValueError(
                f"US size for '{size_data.size}' must be "
                f"{MAX_US_SIZE_LENGTH} characters or fewer."
            )


class SizingService:
    @staticmethod
    async def get_sizing_type_options() -> List[str]:
        try:
            schema = await DatabaseService.get_table_schema("types")
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
            sql = f'SELECT DISTINCT platform_id, platform_value FROM "listingoptions_sizes_default_list" ORDER BY platform_id, platform_value;'

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
                        "goat_code": entry.goat_code,
                        "region_code": entry.region_code,
                        "require_us_size": entry.require_us_size,
                    }
                schemes_dict[scheme_name]["sizes"].append(
                    SizingSchemeEntryWithId(
                        id=entry.id, size=entry.size, order=entry.order,
                        us_size=entry.us_size,
                    )
                )

            schemes = [
                SizingSchemeDetailResponse(
                    sizing_scheme=name,
                    sizes=data["sizes"],
                    sizing_types=data["sizing_types"],
                    goat_code=data["goat_code"],
                    region_code=data["region_code"],
                    require_us_size=data["require_us_size"],
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

            # Scheme-level values are identical on every row of the scheme, so any row answers.
            head = entries[0]

            return SizingSchemeDetailResponse(
                sizing_scheme=scheme_name,
                sizes=[
                    SizingSchemeEntryWithId(
                        id=e.id, size=e.size, order=e.order, us_size=e.us_size
                    )
                    for e in entries
                ],
                sizing_types=head.sizing_types,
                goat_code=head.goat_code,
                region_code=head.region_code,
                require_us_size=head.require_us_size,
            )
        except DoesNotExist:
            logger.info(f"Sizing scheme '{scheme_name}' not found when fetching details.")
            return None
        except Exception as e:
            logger.error(f"Error retrieving details for sizing scheme {scheme_name}: {str(e)}")
            raise

    # add_size_to_scheme / POST /listingoptions/sizing_schemes/sizes was removed 2026-08-25.
    # It created a row with only sizing_scheme, size and order and never checked the scheme
    # existed, so POST ?scheme_name=Typo invented a whole new scheme with NULL sizing_types,
    # goat_code and region_code - bypassing FullSizingSchemeCreate and making required-on-create
    # false. It had no callers: the editor's Add Size button only mutates local state, which
    # update_scheme_size_orders then persists.

    @staticmethod
    async def create_full_sizing_scheme(
        scheme_create: FullSizingSchemeCreate,
    ) -> SizingSchemeDetailResponse:
        async with in_transaction("default"):
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

            # Required on create only. Existing schemes predate these fields and must stay
            # editable, so UpdateSizeOrderRequest deliberately carries no equivalent check.
            goat_code = _blank_to_none(scheme_create.goat_code)
            region_code = _blank_to_none(scheme_create.region_code)
            if not goat_code:
                raise ValueError("GOAT code is required for a new sizing scheme.")
            if not region_code:
                raise ValueError("Region code is required for a new sizing scheme.")
            _validate_code_lengths(goat_code, region_code)

            _validate_us_sizes(scheme_create.sizes)
            if scheme_create.require_us_size:
                _require_complete_us_sizes(
                    [(s.size, s.us_size) for s in scheme_create.sizes]
                )

            created_db_entries = []
            for size_entry in scheme_create.sizes:
                entry = await SizingScheme.create(
                    sizing_scheme=scheme_create.sizing_scheme,
                    size=size_entry.size,
                    order=size_entry.order,
                    sizing_types=scheme_create.sizing_types,
                    goat_code=goat_code,
                    region_code=region_code,
                    require_us_size=bool(scheme_create.require_us_size),
                    us_size=size_entry.us_size or None,
                )
                created_db_entries.append(entry)

            response = SizingSchemeDetailResponse(
                sizing_scheme=scheme_create.sizing_scheme,
                sizes=sorted(
                    [SizingSchemeEntryWithId.from_orm(e) for e in created_db_entries],
                    key=lambda x: x.order,
                ),
                sizing_types=scheme_create.sizing_types,
                goat_code=goat_code,
                region_code=region_code,
                require_us_size=bool(scheme_create.require_us_size),
            )

        # Outside the transaction on purpose. asyncio.create_task copies the current context, and
        # Tortoise resolves connections through a ContextVar, so a task spawned inside the block
        # inherits the transaction's connection and can run against it mid-COMMIT or after it has
        # returned to the pool. update_scheme_size_orders already triggers from outside.
        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        return response

    @staticmethod
    async def update_scheme_size_orders(
        scheme_name: str, update_request: UpdateSizeOrderRequest
    ) -> SizingSchemeDetailResponse:
        async with in_transaction("default"):
            new_scheme_name = (
                update_request.new_sizing_scheme.strip()
                if update_request.new_sizing_scheme
                and update_request.new_sizing_scheme.strip() != scheme_name
                else scheme_name
            )

            # select_for_update because resolving an omitted scheme-level field reads its current
            # value and writes it back. Without the lock two concurrent PUTs lose an update: A
            # reads 'eu_shoe', B commits 'us_shoe', A writes 'eu_shoe' back over it.
            existing_entries = (
                await SizingScheme.filter(sizing_scheme=scheme_name)
                .select_for_update()
                .order_by("order")
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

            # Resolve every requested size to an existing row, preferring the row id. This is what
            # makes a rename an UPDATE rather than delete-plus-create: matching on the size string
            # alone reads '40' -> '41' as "'40' removed, '41' added", which deletes the row and
            # takes everything keyed on its id with it (sizing_lists rows via ON DELETE CASCADE,
            # sizes_default_list rows orphaned via ON DELETE SET NULL).
            existing_by_id = {str(e.id): e for e in existing_entries}
            existing_by_size = {e.size: e for e in existing_entries}

            resolved: List[tuple] = []
            claimed: set = set()
            for size_data in update_request.sizes:
                entry = None
                if size_data.id is not None:
                    entry = existing_by_id.get(str(size_data.id))
                if entry is None:
                    # No id (older client, or a newly added size) - fall back to the size string.
                    entry = existing_by_size.get(size_data.size)
                if entry is not None and str(entry.id) in claimed:
                    entry = None  # already taken by an earlier request row; treat this as new
                if entry is not None:
                    claimed.add(str(entry.id))
                resolved.append((entry, size_data))

            # Delete by id, not by size string. A renamed row's OLD size is absent from the
            # request, so a string-keyed diff would delete the very row we just matched.
            ids_to_delete = [e.id for e in existing_entries if str(e.id) not in claimed]
            if ids_to_delete:
                await SizingScheme.filter(id__in=ids_to_delete).delete()

            # Resolve the scheme-level fields once, for the whole scheme. A field the caller omits
            # arrives as None and keeps whatever the scheme already has, so an omission can neither
            # wipe it nor leave rows disagreeing; a field sent blank resolves to NULL, which is how
            # an operator clears one. `current` is never None here: every path where
            # existing_entries is empty has already returned or raised above.
            current = existing_entries[0]
            _validate_code_lengths(update_request.goat_code, update_request.region_code)
            _validate_us_sizes(update_request.sizes)
            scheme_values = {
                "sizing_types": (
                    update_request.sizing_types
                    if update_request.sizing_types is not None
                    else current.sizing_types
                ),
                "goat_code": (
                    _blank_to_none(update_request.goat_code)
                    if update_request.goat_code is not None
                    else current.goat_code
                ),
                "region_code": (
                    _blank_to_none(update_request.region_code)
                    if update_request.region_code is not None
                    else current.region_code
                ),
                "require_us_size": (
                    update_request.require_us_size
                    if update_request.require_us_size is not None
                    else current.require_us_size
                ),
            }

            # Park every renamed row on a unique placeholder before writing final values.
            # unique_together is (sizing_scheme, size) and is not deferrable, so renaming
            # '40'->'41' while a row still holds '41' would violate it mid-loop - which happens
            # whenever an operator shifts a run of sizes along. Both passes are inside the
            # transaction, so the placeholder is never visible outside it.
            renames = [
                (entry, size_data.size)
                for entry, size_data in resolved
                if entry is not None and entry.size != size_data.size
            ]
            for entry, _ in renames:
                await SizingScheme.filter(id=entry.id).update(size=f"__renaming_{entry.id}")

            updated_entries = []
            for entry, size_data in resolved:
                if entry is not None:
                    entry.size = size_data.size
                    entry.order = size_data.order
                    # us_size is PER SIZE ROW, so it is written here and never in the blanket
                    # scheme-wide UPDATE below. The model validator has already turned a blank
                    # into None, so an omission and an explicit clear arrive identically - which
                    # is fine because the editor always sends every size it is displaying.
                    fields = ["size", "order", "updated_at"]
                    if size_data.us_size is not None:
                        # "" means the operator cleared the field; None means they did not send
                        # it. Only the former writes, and it writes NULL - so the column holds
                        # two states on disk while the request can express all three.
                        entry.us_size = size_data.us_size or None
                        fields.insert(2, "us_size")
                    await entry.save(update_fields=fields)
                    updated_entries.append(entry)
                else:
                    new_entry = await SizingScheme.create(
                        sizing_scheme=new_scheme_name,
                        size=size_data.size,
                        order=size_data.order,
                        us_size=size_data.us_size or None,
                    )
                    updated_entries.append(new_entry)

            # One statement covers surviving rows and rows just created above, which is what makes
            # "every row of a scheme agrees" true by construction rather than by each branch
            # remembering. It also self-heals any pre-existing drift on the next save.
            if scheme_values["require_us_size"]:
                _require_complete_us_sizes(
                    [
                        (
                            size_data.size,
                            size_data.us_size
                            if size_data.us_size is not None
                            else (entry.us_size if entry is not None else None),
                        )
                        for entry, size_data in resolved
                    ]
                )

            # Guard the invariant rather than trusting a comment: this statement writes one
            # value to every row of the scheme, so a row-level column here would destroy every
            # distinct value in it. us_size is exactly such a column.
            assert set(scheme_values) <= set(SCHEME_LEVEL_FIELDS), (
                f"scheme-wide UPDATE may only carry scheme-level fields, got {set(scheme_values)}"
            )
            await SizingScheme.filter(sizing_scheme=new_scheme_name).update(**scheme_values)
            for entry in updated_entries:
                for field, value in scheme_values.items():
                    setattr(entry, field, value)

        asyncio.create_task(spreadsheet_service.trigger_spreadsheet_update("sizes"))
        final_entries = [SizingSchemeEntryWithId.from_orm(e) for e in updated_entries]

        return SizingSchemeDetailResponse(
            sizing_scheme=new_scheme_name,
            sizes=sorted(final_entries, key=lambda x: x.order),
            # From the resolved values, not the request: a PUT that omitted a field must report
            # what is actually stored.
            sizing_types=scheme_values["sizing_types"],
            goat_code=scheme_values["goat_code"],
            region_code=scheme_values["region_code"],
            require_us_size=scheme_values["require_us_size"],
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
        async with in_transaction("default"):
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
                        "GOAT Code": entry.goat_code or "",
                        "Region Code": entry.region_code or "",
                        # The only way to get us_size back out. It is hand-entered and has no
                        # external source, unlike goat_code which has its TSV.
                        "US Size": entry.us_size or "",
                    }
                )

            return rows
        except Exception as e:
            logger.error(f"Error exporting sizing schemes: {str(e)}")
            raise
