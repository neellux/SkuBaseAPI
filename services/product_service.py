import asyncio
import json
import logging
import orjson
import re
import traceback
from typing import Dict, List, Any, Optional
from tortoise import connections

from services import sellercloud_sync_queue
from services.sellercloud_internal_service import (
    SellercloudPermanentError,
    sellercloud_internal_service,
)
from services.sellercloud_service import sellercloud_service

logger = logging.getLogger(__name__)


def format_mpn(mpn: str) -> str:
    """Format the mpn for sku creation / comparison."""
    # Replace non-word characters with underscores
    mpn = re.sub(r"\W", "_", mpn)
    # Remove leading or trailing underscores
    mpn = re.sub(r"^_+|_+$", "", mpn)
    # Replace multiple underscores with a single one
    mpn = re.sub(r"_+", "_", mpn)
    # Convert to uppercase
    return mpn.upper()


JOB_HANDLERS = {
    "TRANSFER_INVENTORY_SC": "_execute_transfer_job",
    "TRANSFER_UPCS_KEYWORDS_SC": "_execute_transfer_upcs_keywords_job",
    "DISABLE_PRODUCT_SC": "_execute_disable_job",
}


def _size_failures_from_errors(errors: Any) -> List[Dict[str, str]]:
    """Flatten add_skus `errors` into per-size `{size, upc?, error}` items with
    specific, human-readable messages (e.g. "UPC already in use — Already
    assigned to SKU: X"), matching the `failures` shape used elsewhere.

    add_skus errors come in two shapes: UPC-validation wrappers that carry a
    `failed_skus` list, and per-sku errors that carry a `sku` directly.
    """

    def _size_of(sku: str) -> str:
        sku = sku or ""
        return sku.split("/")[-1] if "/" in sku else sku

    out: List[Dict[str, str]] = []
    for entry in errors or []:
        if not isinstance(entry, dict):
            out.append({"size": "", "error": str(entry)})
            continue
        failed = entry.get("failed_skus")
        if failed:
            for fs in failed:
                msg = fs.get("error") or "Invalid UPC"
                if fs.get("detail"):
                    msg = f"{msg} — {fs['detail']}"
                item = {"size": _size_of(fs.get("sku")), "error": msg}
                if fs.get("upc"):
                    item["upc"] = str(fs["upc"])
                out.append(item)
        elif entry.get("sku"):
            out.append(
                {"size": _size_of(entry.get("sku")), "error": entry.get("error", "Failed")}
            )
        else:
            out.append({"size": "", "error": entry.get("error", "Failed")})
    return out


class ProductService:

    @staticmethod
    async def _get_connection():
        return connections.get("product_db")

    @staticmethod
    def _size_rank(parent_sku, size, parent_scheme, size_order):
        """Canonical rank for a size within its parent.

        Returns float('inf') when the parent has no sizing_scheme, the scheme is
        not in the map, the size is not in the scheme, or the scheme row's order
        is NULL. inf-ranked rows fall back to alphabetical-by-size via the tiebreak
        in apply_size_sort, and never crash the sort.
        """
        scheme = parent_scheme.get(parent_sku)            # None if parent_sku missing/None
        rank = size_order.get((scheme, size))             # None if not found
        return float("inf") if rank is None else rank     # guard against NULL "order" too

    @staticmethod
    async def apply_size_sort(rows, key_tail=lambda r: ()):
        """Best-effort: sort export rows in place by canonical size within each parent.

        Each row must carry 'parent_sku' and 'size'. Rows are ordered by
        (parent_sku, canonical size order, size, *key_tail). On ANY failure to load
        the sizing maps (DB error, missing tables, etc.) the rows are returned
        untouched in their existing (SQL) order, so the export still succeeds.
        """
        try:
            product_conn = connections.get("product_db")
            default_conn = connections.get("default")

            parent_rows = await product_conn.execute_query_dict(
                "SELECT sku, sizing_scheme FROM parent_products"
            )
            parent_scheme = {r["sku"]: r["sizing_scheme"] for r in parent_rows}

            scheme_rows = await default_conn.execute_query_dict(
                'SELECT sizing_scheme, size, "order" FROM listingoptions_sizing_schemes'
            )
            size_order = {(r["sizing_scheme"], r["size"]): r["order"] for r in scheme_rows}
        except Exception:
            logger.warning(
                "size sort maps unavailable; keeping default export order", exc_info=True
            )
            return rows

        rows.sort(
            key=lambda r: (
                (r.get("parent_sku") or ""),
                ProductService._size_rank(
                    r.get("parent_sku"), r.get("size"), parent_scheme, size_order
                ),
                (r.get("size") or ""),
                *key_tail(r),
            )
        )
        return rows

    @staticmethod
    async def _execute_transfer_job(
        child_sku: str, target_child_sku: str, **kwargs
    ) -> Dict[str, Any]:
        try:
            result = await sellercloud_internal_service.transfer_all_inventory(
                from_sku=child_sku, to_sku=target_child_sku
            )

            if not result.get("success"):
                error = result.get("error", "Unknown error")
                result["user_message"] = "Failed to transfer inventory"
            elif result.get("no_inventory"):
                result["user_message"] = f"No inventory to transfer from {child_sku}"
            else:
                summary = result.get("summary", {})
                transferred = summary.get("transferred_qty", 0)
                failed = summary.get("failed_qty", 0)
                if failed > 0:
                    result["user_message"] = (
                        f"Transferred {transferred} of {transferred + failed} units"
                    )
                else:
                    result["user_message"] = f"Successfully transferred {transferred} units"

            return result
        except Exception:
            return {
                "success": False,
                "error": traceback.format_exc(),
                "user_message": "Failed to transfer inventory",
            }

    @staticmethod
    async def _execute_disable_job(child_sku: str, **kwargs) -> Dict[str, Any]:
        try:
            success = await sellercloud_service.disable_product(child_sku)
            if success:
                conn = await ProductService._get_connection()
                await conn.execute_query(
                    "UPDATE child_products SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE sku = $1",
                    [child_sku],
                )
            return {
                "success": success,
                "sku": child_sku,
                "user_message": f"Product {child_sku} has been disabled",
            }
        except Exception:
            return {
                "success": False,
                "sku": child_sku,
                "error": traceback.format_exc(),
                "user_message": "Failed to disable product",
            }

    @staticmethod
    async def _execute_transfer_upcs_keywords_job(
        child_sku: str, target_child_sku: str, is_placeholder: bool = False, **kwargs
    ) -> Dict[str, Any]:
        transferred_upcs: List[str] = []
        transferred_keywords: List[str] = []

        def _failure(label: str, value: str, exc: Exception) -> Dict[str, Any]:
            if isinstance(exc, SellercloudPermanentError):
                user_message = str(exc)
            else:
                user_message = (
                    "SellerCloud temporarily unavailable — please retry the reassignment"
                )
            logger.error(f"Failed to transfer {label} {value}: {exc}")
            return {
                "success": False,
                "error": f"Failed to transfer {label} {value}: {exc}",
                "user_message": user_message,
                "transferred_upcs": transferred_upcs,
                "transferred_keywords": transferred_keywords,
            }

        try:
            conn = await ProductService._get_connection()

            upcs_result = await conn.execute_query_dict(
                "SELECT upc, is_primary_upc FROM child_upcs WHERE child_sku = $1", [child_sku]
            )

            primary_upc = None
            secondary_upcs = []
            for row in upcs_result:
                if row["is_primary_upc"]:
                    primary_upc = row["upc"]
                else:
                    secondary_upcs.append(row["upc"])

            keywords_result = await conn.execute_query_dict(
                "SELECT keywords FROM child_products WHERE sku = $1", [child_sku]
            )
            keywords = (
                keywords_result[0]["keywords"]
                if keywords_result and keywords_result[0]["keywords"]
                else []
            )

            logger.info(
                f"Transferring from {child_sku} to {target_child_sku}: "
                f"primary_upc={primary_upc}, secondary_upcs={secondary_upcs}, keywords={keywords}"
            )

            if primary_upc:
                try:
                    # Clear BasicInfo UPC on source — direct call, not alias management
                    clear_result = await sellercloud_service.update_product_upc(
                        child_sku, ""
                    )
                    if not clear_result.get("success"):
                        raise Exception(
                            f"Failed to clear BasicInfo UPC on {child_sku}: {clear_result}"
                        )
                    logger.info(f"Cleared primary UPC from {child_sku} in SellerCloud")

                    # Best-effort: remove the alias from source. The primary may not
                    # exist as an alias on source (it lives in BasicInfo), so tolerate
                    # both permanent and transient failures here.
                    try:
                        await sellercloud_internal_service.sync_delete_alias(
                            child_sku, primary_upc
                        )
                    except Exception as src_del_err:
                        logger.debug(
                            f"Alias delete for {primary_upc} from source (non-fatal): {src_del_err}"
                        )

                    if not is_placeholder:
                        # Add to target as a non-primary alias (matches DB shape below)
                        await sellercloud_internal_service.sync_add_alias(
                            target_child_sku, primary_upc, is_primary=False
                        )

                    await conn.execute_query(
                        "DELETE FROM child_upcs WHERE upc = $1", [primary_upc]
                    )
                    await conn.execute_query(
                        "INSERT INTO child_upcs (upc, child_sku, is_primary_upc) VALUES ($1, $2, FALSE)",
                        [primary_upc, target_child_sku],
                    )

                    transferred_upcs.append(primary_upc)
                    logger.info(f"Transferred primary UPC {primary_upc} to {target_child_sku}")

                except Exception as e:
                    return _failure("primary UPC", primary_upc, e)

            for upc in secondary_upcs:
                try:
                    # Best-effort source removal — alias may already be gone
                    try:
                        await sellercloud_internal_service.sync_delete_alias(
                            child_sku, upc
                        )
                    except Exception as src_del_err:
                        logger.warning(
                            f"Source alias delete for {upc} from {child_sku} non-fatal: {src_del_err}"
                        )

                    if not is_placeholder:
                        await sellercloud_internal_service.sync_add_alias(
                            target_child_sku, upc, is_primary=False
                        )

                    # DB swap only after SC ops succeed — prevents losing the row
                    # if the target add fails after the source delete
                    await conn.execute_query(
                        "DELETE FROM child_upcs WHERE upc = $1", [upc]
                    )
                    await conn.execute_query(
                        "INSERT INTO child_upcs (upc, child_sku) VALUES ($1, $2)",
                        [upc, target_child_sku],
                    )

                    transferred_upcs.append(upc)
                    logger.info(f"Transferred secondary UPC {upc} to {target_child_sku}")

                except Exception as e:
                    return _failure("secondary UPC", upc, e)

            for keyword in keywords:
                try:
                    try:
                        await sellercloud_internal_service.sync_delete_alias(
                            child_sku, keyword
                        )
                    except Exception as src_del_err:
                        logger.warning(
                            f"Source keyword delete for {keyword} from {child_sku} non-fatal: {src_del_err}"
                        )

                    await sellercloud_internal_service.sync_add_alias(
                        target_child_sku, keyword, is_primary=False
                    )

                    # DB swap only after SC ops succeed
                    await conn.execute_query(
                        "UPDATE child_products SET keywords = array_remove(keywords, $1), updated_at = CURRENT_TIMESTAMP WHERE sku = $2",
                        [keyword, child_sku],
                    )
                    await conn.execute_query(
                        "UPDATE child_products SET keywords = array_append(COALESCE(keywords, '{}'), $1), updated_at = CURRENT_TIMESTAMP WHERE sku = $2",
                        [keyword, target_child_sku],
                    )

                    transferred_keywords.append(keyword)
                    logger.info(f"Transferred keyword {keyword} to {target_child_sku}")

                except Exception as e:
                    return _failure("keyword", keyword, e)

            total_transferred = len(transferred_upcs) + len(transferred_keywords)
            return {
                "success": True,
                "from_sku": child_sku,
                "to_sku": target_child_sku,
                "transferred_upcs": transferred_upcs,
                "transferred_keywords": transferred_keywords,
                "user_message": f"Transferred {total_transferred} items ({len(transferred_upcs)} UPCs, {len(transferred_keywords)} keywords)",
            }

        except Exception as e:
            logger.error(f"Error in transfer UPCs/keywords job: {traceback.format_exc()}")
            return {
                "success": False,
                "error": traceback.format_exc(),
                "user_message": "Failed to transfer UPCs and keywords",
                "transferred_upcs": transferred_upcs,
                "transferred_keywords": transferred_keywords,
            }

    @staticmethod
    async def get_reassign_preview(
        child_sku: str, new_parent_sku: str, target_child_sku: str
    ) -> Dict[str, Any]:
        conn = await ProductService._get_connection()
        errors = []

        child_result = await conn.execute_query_dict(
            "SELECT sku, parent_sku, size, is_active FROM child_products WHERE sku = $1",
            [child_sku],
        )
        if not child_result or not child_result[0].get("is_active"):
            errors.append(f"Child SKU '{child_sku}' not found or inactive")

        parent_result = await conn.execute_query_dict(
            "SELECT sku, title FROM parent_products WHERE sku = $1 AND is_active = TRUE",
            [new_parent_sku],
        )
        if not parent_result:
            errors.append(f"Parent SKU '{new_parent_sku}' not found or inactive")

        target_result = await conn.execute_query_dict(
            "SELECT sku, parent_sku, size FROM child_products WHERE sku = $1 AND is_active = TRUE",
            [target_child_sku],
        )
        target_pending = False
        pending_size = ""
        if not target_result:
            # The destination child may not exist yet: the reassign flow creates
            # the placeholder size on submit. Accept only the deterministic
            # {parent}/{size} SKU that add_placeholder_size_to_parent would
            # create, so the preview can still report the source inventory that
            # is going to be transferred.
            prefix = f"{new_parent_sku}/"
            if target_child_sku.startswith(prefix) and target_child_sku[len(prefix) :]:
                target_pending = True
                pending_size = target_child_sku[len(prefix) :]
            else:
                errors.append(f"Target child SKU '{target_child_sku}' not found or inactive")
        elif target_result[0].get("parent_sku") != new_parent_sku:
            errors.append(
                f"Target child '{target_child_sku}' does not belong to parent '{new_parent_sku}'"
            )

        if errors:
            return {"success": False, "errors": errors, "can_proceed": False}

        inventory = await sellercloud_internal_service.get_inventory_preview(child_sku)

        jobs = await conn.execute_query_dict(
            """SELECT code, name, description, execution_order
               FROM job_types
               WHERE is_active = TRUE AND applies_to_secondary = TRUE
               ORDER BY execution_order""",
            [],
        )

        return {
            "success": True,
            "can_proceed": True,
            "from_child": {
                "sku": child_sku,
                "current_parent_sku": child_result[0].get("parent_sku"),
                "size": child_result[0].get("size"),
            },
            "to_parent": {"sku": new_parent_sku, "title": parent_result[0].get("title")},
            "to_child": {
                "sku": target_child_sku,
                "size": target_result[0].get("size") if target_result else pending_size,
                "pending": target_pending,
            },
            "inventory": inventory,
            "planned_jobs": jobs,
        }

    @staticmethod
    async def add_product(
        child_sku: str,
        title: str,
        company_code: int,
        upc: Optional[str] = None,
        mpn: Optional[str] = None,
        brand_code: Optional[str] = None,
        type_code: Optional[str] = None,
        serial_number: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            sku_data = {
                child_sku: {
                    "title": title,
                }
            }

            if mpn:
                sku_data[child_sku]["mpn"] = mpn
            if brand_code:
                sku_data[child_sku]["brand_code"] = brand_code
            if type_code:
                sku_data[child_sku]["type_code"] = type_code
            if serial_number is not None:
                sku_data[child_sku]["serial_number"] = serial_number
            if upc:
                sku_data[child_sku]["upc"] = upc

            result = await conn.execute_query_dict(
                "SELECT add_skus($1::jsonb, $2) as result",
                [orjson.dumps(sku_data).decode(), company_code],
            )

            if result and result[0].get("result"):
                db_result = result[0]["result"]
                if isinstance(db_result, str):
                    db_result = orjson.loads(db_result)

                if db_result.get("success"):
                    assignments = db_result.get("assignments", {})
                    child_info = assignments.get(child_sku, {})

                    return {
                        "success": True,
                        "child_sku": child_sku,
                        "parent_sku": child_info.get("parent_sku"),
                        "size": child_info.get("size"),
                        "is_primary": child_info.get("is_primary", False),
                        "parent_created": child_info.get("parent_created", False),
                        "errors": None,
                    }
                else:
                    return {
                        "success": False,
                        "child_sku": child_sku,
                        "parent_sku": None,
                        "size": None,
                        "is_primary": False,
                        "parent_created": False,
                        "errors": db_result.get("errors", []),
                    }

            return {
                "success": False,
                "child_sku": child_sku,
                "parent_sku": None,
                "size": None,
                "is_primary": False,
                "parent_created": False,
                "errors": [{"error": "No result from database"}],
            }

        except Exception as e:
            logger.error(f"Error adding product {child_sku}: {e}")
            return {
                "success": False,
                "child_sku": child_sku,
                "parent_sku": None,
                "size": None,
                "is_primary": False,
                "parent_created": False,
                "errors": [{"error": str(e)}],
            }

    @staticmethod
    async def bulk_add_sizes_to_parent(
        parent_sku: str, sizes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Add several sizes to an existing parent in one call.

        Reuses the single-size add_size_to_parent path per row. A manual UPC is
        passed through; blank/omitted UPCs are auto-assigned by the add_skus DB
        function (advisory-locked, no client pre-fetch). SellerCloud creation is
        inherently per-child, so a per-size failure is reported without aborting
        the rest.
        """
        children: List[str] = []
        failures: List[Dict[str, str]] = []

        for sz in sizes:
            size = str(sz.get("size", "")).strip()
            if not size:
                failures.append({"size": "", "error": "Size is required"})
                continue
            try:
                result = await ProductService.add_size_to_parent(
                    parent_sku=parent_sku,
                    size=size,
                    upc=sz.get("upc"),
                    cost_price=float(sz.get("unit_price")),
                )
                if result.get("success"):
                    children.append(result.get("new_child_sku") or f"{parent_sku}/{size}")
                else:
                    failures.append(
                        {"size": size, "error": result.get("error", "Failed to add size")}
                    )
            except Exception as e:
                logger.error(f"bulk_add_sizes: size '{size}' failed: {traceback.format_exc()}")
                failures.append({"size": size, "error": str(e)})

        return {
            "success": len(children) > 0,
            "parent_sku": parent_sku,
            "children": children,
            "failures": failures or None,
        }

    @staticmethod
    async def check_brand_mpn(brand: str, mpn: str) -> Dict[str, Any]:
        """Check whether an active parent already exists for this brand + mpn.

        The mpn is normalized with format_mpn so that values like
        '1201A906-0012' and '1201A906_0012' compare equal.
        """
        try:
            conn = await ProductService._get_connection()
            normalized = format_mpn(mpn or "")
            rows = await conn.execute_query_dict(
                "SELECT sku FROM parent_products "
                "WHERE brand = $1 AND mpn = $2 AND is_active = TRUE LIMIT 1",
                [brand, normalized],
            )
            if rows:
                return {"success": True, "exists": True, "sku": rows[0]["sku"]}
            return {"success": True, "exists": False, "sku": None}
        except Exception:
            logger.error(f"Error checking brand+mpn: {traceback.format_exc()}")
            return {"success": False, "error": "Failed to check brand and MPN"}

    @staticmethod
    async def create_sku_with_sizes(
        company_code: int,
        brand: str,
        brand_code: str,
        mpn: str,
        title: str,
        product_type: str,
        type_code: str,
        sizing_scheme: str,
        style_name: str,
        brand_color: str,
        color: str,
        sizes: List[Dict[str, Any]],
        retail_price: float = None,
        country_of_origin: str = None,
        season: str = None,
    ) -> Dict[str, Any]:
        """Create a brand new product (parent + child sizes).

        Generates the parent SKU as {brand_code}-{type_code}-{serial}, creates a
        full product per size on SellerCloud (unit price -> SiteCost, UPC from the
        shared get_next_upc allocator), then writes the parent + children to the
        local DB via add_skus. Attribute push to SellerCloud children (brand,
        color, etc.) is done by the route via update_children_basic_info.
        """
        try:
            conn = await ProductService._get_connection()

            brand_code = (brand_code or "").strip().upper()
            type_code = (type_code or "").strip().upper()
            mpn_fmt = format_mpn(mpn or "")

            if not brand_code or not type_code:
                return {"success": False, "error": "brand_code and type_code are required"}
            if not sizes:
                return {"success": False, "error": "At least one size is required"}

            # Guard: brand + mpn must be unique.
            existing = await conn.execute_query_dict(
                "SELECT sku FROM parent_products "
                "WHERE brand = $1 AND mpn = $2 AND is_active = TRUE LIMIT 1",
                [brand, mpn_fmt],
            )
            if existing:
                return {
                    "success": False,
                    "error": f"A product already exists for {brand} + {mpn_fmt}: {existing[0]['sku']}",
                }

            # Next serial for this brand_code + type_code.
            serial_row = await conn.execute_query_dict(
                "SELECT COALESCE(MAX(serial_number), 0) + 1 AS next "
                "FROM parent_products WHERE brand_code = $1 AND type_code = $2",
                [brand_code, type_code],
            )
            serial = serial_row[0]["next"]
            if serial > 9999:
                return {
                    "success": False,
                    "error": f"Serial range exhausted for {brand_code}-{type_code}",
                }
            parent_sku = f"{brand_code}-{type_code}-{serial:04d}"

            # 1) Write parent + children locally via add_skus FIRST. Omit the UPC
            #    for auto rows so add_skus assigns one (advisory-locked) and pass a
            #    manual UPC when the user entered one. add_skus is atomic and
            #    returns assignments[sku].upc.
            sku_data = {}
            for sz in sizes:
                size = str(sz["size"]).strip()
                info = {
                    "title": title,
                    "brand": brand,
                    "brand_code": brand_code,
                    "type_code": type_code,
                    "serial_number": serial,
                    "product_type": product_type,
                    "style_name": style_name,
                    "sizing_scheme": sizing_scheme,
                    "brand_color": brand_color,
                    "color": color,
                    "mpn": mpn_fmt,
                }
                manual_upc = re.sub(r"[^0-9]", "", str(sz.get("upc") or ""))
                if manual_upc:
                    info["upc"] = manual_upc
                sku_data[f"{parent_sku}/{size}"] = info

            db_result = await conn.execute_query_dict(
                "SELECT add_skus($1::jsonb, $2) as result",
                [orjson.dumps(sku_data).decode(), company_code],
            )
            result_data = db_result[0]["result"] if db_result else None
            if isinstance(result_data, str):
                result_data = orjson.loads(result_data)
            if not result_data or not result_data.get("success"):
                errors = (result_data or {}).get("errors", [])
                logger.error(f"add_skus failed for {parent_sku}: {errors}")
                return {
                    "success": False,
                    "parent_sku": parent_sku,
                    "error": "Some sizes could not be added",
                    "failures": _size_failures_from_errors(errors),
                }
            assignments = result_data.get("assignments", {})

            # 2) Create each child on SellerCloud with the UPC add_skus assigned,
            #    retrying up to 3x. Local rows persist; per-size failures are
            #    aggregated (the caller lists them for re-sync).
            async def _retry_sc(coro_func, task_name):
                last_error = None
                for attempt in range(1, 4):
                    try:
                        return await coro_func()
                    except Exception as e:
                        last_error = e
                        logger.warning(f"{task_name} attempt {attempt}/3 failed: {e}")
                        if attempt < 3:
                            await asyncio.sleep(1 * (2 ** (attempt - 1)))
                raise last_error

            # SKU-level custom columns (same for every size). SIZE is added
            # per-child below.
            base_columns = []
            if country_of_origin:
                base_columns.append(
                    {"ColumnName": "COUNTRY_OF_ORIGIN", "Value": country_of_origin}
                )
            if season:
                base_columns.append({"ColumnName": "FASHION_SEASON", "Value": season})

            # All children already exist locally (add_skus succeeded), so the
            # product is created regardless of SellerCloud outcome.
            all_children: List[str] = []
            failures: List[Dict[str, str]] = []
            for sz in sizes:
                size = str(sz["size"]).strip()
                unit_price = float(sz["unit_price"])
                child_sku = f"{parent_sku}/{size}"
                all_children.append(child_sku)
                upc = (assignments.get(child_sku) or {}).get("upc")
                try:
                    await _retry_sc(
                        lambda c=child_sku, s=size, p=unit_price, u=upc: sellercloud_service.create_product(
                            product_sku=c,
                            product_name=f"{title} SIZE {s}",
                            company_id=company_code,
                            site_cost=p,
                            product_type_name=product_type,
                            brand_name=brand,
                            upc=u,
                        ),
                        f"create_product {child_sku}",
                    )
                except Exception:
                    logger.error(
                        f"Failed to create {child_sku} on SellerCloud after retries: {traceback.format_exc()}"
                    )
                    failures.append({"size": size, "error": "Failed to create on SellerCloud"})
                    continue

                # Custom columns: SIZE + SKU-level COUNTRY_OF_ORIGIN / FASHION_SEASON
                # (non-fatal, retried).
                columns = [{"ColumnName": "SIZE", "Value": size}] + base_columns
                try:
                    await _retry_sc(
                        lambda c=child_sku, cols=columns: sellercloud_service._make_request(
                            "PUT",
                            "/Products/CustomColumns",
                            data={"ProductID": c, "CustomColumns": cols},
                        ),
                        f"custom_columns {child_sku}",
                    )
                except Exception as e:
                    logger.warning(f"Non-fatal: failed custom columns for {child_sku}: {e}")

                # Retail price -> SellerCloud ListPrice (non-fatal, retried).
                if retail_price is not None:
                    try:
                        await _retry_sc(
                            lambda c=child_sku: sellercloud_service._make_request(
                                "PUT",
                                "/Catalog/AdvancedInfo",
                                data={
                                    "ProductID": c,
                                    "Fields": [
                                        {"Name": "ListPrice", "Value": retail_price}
                                    ],
                                },
                            ),
                            f"list_price {child_sku}",
                        )
                    except Exception as e:
                        logger.warning(f"Non-fatal: failed ListPrice for {child_sku}: {e}")

            logger.info(
                f"Created new product {parent_sku}: {len(all_children) - len(failures)} on "
                f"SellerCloud, {len(failures)} failed"
            )
            return {
                "success": True,
                "parent_sku": parent_sku,
                "children": all_children,
                "failures": failures or None,
            }

        except Exception:
            logger.error(f"Error creating SKU: {traceback.format_exc()}")
            return {"success": False, "error": "Internal server error"}

    @staticmethod
    async def get_cost_price(parent_sku: str) -> Dict[str, Any]:
        """Return the cost price (SellerCloud SiteCost) used to prepopulate the
        Add Size dialog.

        Mirrors add_size_to_parent / add_placeholder_size_to_parent by reading
        the parent's primary active child as the template and returning its
        SiteCost. Returns cost_price=None (still success) when there is no active
        child to read a cost from.
        """
        try:
            conn = await ProductService._get_connection()
            template_result = await conn.execute_query_dict(
                "SELECT sku FROM child_products "
                "WHERE parent_sku = $1 AND is_active = TRUE "
                "ORDER BY is_primary DESC LIMIT 1",
                [parent_sku],
            )
            if not template_result:
                return {"success": True, "cost_price": None}

            template_child_sku = template_result[0]["sku"]
            template_data = await sellercloud_service.get_catalog_item(
                template_child_sku, only_required_fields=False
            )
            if not template_data:
                return {"success": True, "cost_price": None}

            site_cost = template_data.get("SiteCost")
            try:
                cost_price = (
                    float(site_cost) if site_cost not in (None, "") else None
                )
            except (TypeError, ValueError):
                cost_price = None
            return {"success": True, "cost_price": cost_price}
        except Exception:
            logger.error(
                f"Error fetching cost price for {parent_sku}: {traceback.format_exc()}"
            )
            return {"success": False, "error": "Failed to fetch cost price"}

    @staticmethod
    async def add_size_to_parent(
        parent_sku: str,
        size: str,
        upc: Optional[str] = None,
        cost_price: float = None,
    ) -> Dict[str, Any]:

        async def _retry_async(coro_func, *args, max_retries=3, delay=1, task_name="task"):
            last_error = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await coro_func(*args)
                except Exception as e:
                    last_error = e
                    logger.warning(f"{task_name} attempt {attempt}/{max_retries} failed: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(delay * (2 ** (attempt - 1)))
            raise last_error

        try:
            conn = await ProductService._get_connection()
            new_child_sku = f"{parent_sku}/{size}"

            parent_result = await conn.execute_query_dict(
                "SELECT title, mpn, brand, brand_code, type_code, serial_number, company_code, "
                "product_type, style_name, sizing_scheme, brand_color, color "
                "FROM parent_products WHERE sku = $1 AND is_active = TRUE",
                [parent_sku],
            )
            if not parent_result:
                return {
                    "success": False,
                    "error": f"Parent product '{parent_sku}' not found or inactive",
                }

            parent = parent_result[0]

            existing_child = await conn.execute_query_dict(
                "SELECT sku, is_active FROM child_products WHERE sku = $1",
                [new_child_sku],
            )
            if existing_child:
                status = "active" if existing_child[0]["is_active"] else "inactive"
                return {
                    "success": False,
                    "error": f"Child SKU '{new_child_sku}' already exists ({status})",
                }

            template_result = await conn.execute_query_dict(
                "SELECT sku FROM child_products "
                "WHERE parent_sku = $1 AND is_active = TRUE "
                "ORDER BY is_primary DESC LIMIT 1",
                [parent_sku],
            )
            if not template_result:
                return {
                    "success": False,
                    "error": f"Parent '{parent_sku}' has no active children to use as template",
                }

            template_child_sku = template_result[0]["sku"]

            template_data = await sellercloud_service.get_catalog_item(
                template_child_sku, only_required_fields=False
            )
            if not template_data:
                return {
                    "success": False,
                    "error": f"Template child '{template_child_sku}' not found on SellerCloud",
                }

            list_price = template_data.get("ListPrice", "")
            product_name = f"{parent['title']} SIZE {size} ${list_price}"
            product_type_name = template_data.get("ProductType", "")
            brand_name = template_data.get("BrandName", parent.get("brand", ""))

            # 1) Write the child locally via add_skus FIRST: it copies the
            #    parent's attributes, assigns the UPC (omit for auto; pass a manual
            #    UPC when provided), validates it, and returns the assigned UPC.
            sku_data = {new_child_sku: {"title": parent["title"]}}
            for field in (
                "mpn",
                "brand",
                "brand_code",
                "type_code",
                "product_type",
                "style_name",
                "sizing_scheme",
                "brand_color",
                "color",
            ):
                if parent.get(field):
                    sku_data[new_child_sku][field] = parent[field]
            if parent.get("serial_number") is not None:
                sku_data[new_child_sku]["serial_number"] = parent["serial_number"]
            manual_upc = re.sub(r"[^0-9]", "", str(upc or ""))
            if manual_upc:
                sku_data[new_child_sku]["upc"] = manual_upc

            try:
                db_result = await conn.execute_query_dict(
                    "SELECT add_skus($1::jsonb, $2) as result",
                    [orjson.dumps(sku_data).decode(), parent["company_code"]],
                )
                result_data = db_result[0]["result"] if db_result else None
                if isinstance(result_data, str):
                    result_data = orjson.loads(result_data)
                if not result_data or not result_data.get("success"):
                    errors = (result_data or {}).get("errors", [])
                    logger.error(f"add_skus failed for {new_child_sku}: {errors}")
                    size_failures = _size_failures_from_errors(errors)
                    return {
                        "success": False,
                        "new_child_sku": new_child_sku,
                        "error": (
                            size_failures[0]["error"]
                            if size_failures
                            else "Failed to add size"
                        ),
                    }
            except Exception:
                logger.error(f"Failed to add {new_child_sku} to local DB: {traceback.format_exc()}")
                return {
                    "success": False,
                    "new_child_sku": new_child_sku,
                    "error": "Internal server error",
                }

            assigned_upc = (
                result_data.get("assignments", {}).get(new_child_sku) or {}
            ).get("upc")

            # 2) Create on SellerCloud with the UPC add_skus assigned, retrying up
            #    to 3x. The local row persists; a failure is reported for re-sync.
            try:
                await _retry_async(
                    lambda: sellercloud_service.create_product(
                        product_sku=new_child_sku,
                        product_name=product_name,
                        company_id=parent["company_code"],
                        site_cost=cost_price,
                        product_type_name=product_type_name,
                        brand_name=brand_name,
                        upc=assigned_upc,
                    ),
                    max_retries=3,
                    delay=1,
                    task_name="create_product",
                )
                logger.info(f"Created product {new_child_sku} on SellerCloud")
            except Exception:
                logger.error(
                    f"Failed to create product {new_child_sku} on SellerCloud after retries: {traceback.format_exc()}"
                )
                return {
                    "success": False,
                    "local_created": True,
                    "new_child_sku": new_child_sku,
                    "error": "Failed to create product on SellerCloud",
                }

            async def update_advanced_info():
                fields = [{"Name": "ProductName", "Value": product_name}]
                for field_name in ["BrandName", "ManufacturerSKU", "ListPrice", "LongDescription"]:
                    val = template_data.get(field_name)
                    if val:
                        fields.append({"Name": field_name, "Value": val})
                shipping_weight = template_data.get("ShippingWeight")
                if shipping_weight:
                    try:
                        total_oz = int(shipping_weight)
                        fields.append({"Name": "PackageWeightLbs", "Value": total_oz // 16})
                        fields.append({"Name": "PackageWeightOz", "Value": total_oz % 16})
                    except (ValueError, TypeError):
                        pass
                await sellercloud_service._make_request(
                    "PUT",
                    "/Catalog/AdvancedInfo",
                    data={"ProductID": new_child_sku, "Fields": fields},
                )

            async def copy_columns():
                await sellercloud_service.copy_custom_columns(
                    template_child_sku, new_child_sku, overrides={"SIZE": size}
                )

            async def upload_image():
                image_url = template_data.get("ImageUrl")
                if image_url:
                    await sellercloud_service.upload_product_image(new_child_sku, image_url)

            tasks = [
                _retry_async(
                    update_advanced_info, max_retries=3, delay=1, task_name="AdvancedInfo"
                ),
                _retry_async(copy_columns, max_retries=3, delay=1, task_name="CustomColumns"),
                _retry_async(upload_image, max_retries=3, delay=1, task_name="ImageUpload"),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_names = ["AdvancedInfo", "CustomColumns", "ImageUpload"]
                    logger.warning(
                        f"Non-fatal: {task_names[i]} failed for {new_child_sku}: {result}"
                    )

            return {
                "success": True,
                "new_child_sku": new_child_sku,
                "parent_sku": parent_sku,
                "size": size,
            }

        except Exception as e:
            logger.error(f"Error adding size to parent {parent_sku}: {traceback.format_exc()}")
            return {"success": False, "error": "Internal server error"}

    @staticmethod
    async def add_placeholder_size_to_parent(
        parent_sku: str,
        size: str,
    ) -> Dict[str, Any]:

        async def _retry_async(coro_func, *args, max_retries=3, delay=1, task_name="task"):
            last_error = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await coro_func(*args)
                except Exception as e:
                    last_error = e
                    logger.warning(f"{task_name} attempt {attempt}/{max_retries} failed: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(delay * (2 ** (attempt - 1)))
            raise last_error

        try:
            conn = await ProductService._get_connection()
            new_child_sku = f"{parent_sku}/{size}"

            parent_result = await conn.execute_query_dict(
                "SELECT title, mpn, brand, brand_code, type_code, serial_number, company_code, "
                "product_type, style_name, sizing_scheme, brand_color, color "
                "FROM parent_products WHERE sku = $1 AND is_active = TRUE",
                [parent_sku],
            )
            if not parent_result:
                return {
                    "success": False,
                    "error": f"Parent product '{parent_sku}' not found or inactive",
                }

            parent = parent_result[0]

            existing_child = await conn.execute_query_dict(
                "SELECT sku, is_active FROM child_products WHERE sku = $1",
                [new_child_sku],
            )
            if existing_child:
                status = "active" if existing_child[0]["is_active"] else "inactive"
                return {
                    "success": False,
                    "error": f"Child SKU '{new_child_sku}' already exists ({status})",
                }

            template_result = await conn.execute_query_dict(
                "SELECT sku FROM child_products "
                "WHERE parent_sku = $1 AND is_active = TRUE "
                "ORDER BY is_primary DESC LIMIT 1",
                [parent_sku],
            )
            if not template_result:
                return {
                    "success": False,
                    "error": f"Parent '{parent_sku}' has no active children to use as template",
                }

            template_child_sku = template_result[0]["sku"]

            template_data = await sellercloud_service.get_catalog_item(
                template_child_sku, only_required_fields=False
            )
            if not template_data:
                return {
                    "success": False,
                    "error": f"Template child '{template_child_sku}' not found on SellerCloud",
                }

            list_price = template_data.get("ListPrice", "")
            product_name = f"{parent['title']} SIZE {size} ${list_price}"
            product_type_name = template_data.get("ProductType", "")
            brand_name = template_data.get("BrandName", parent.get("brand", ""))
            site_cost = template_data.get("SiteCost", 0.0)

            # 1) add_skus first: copies parent attributes, auto-assigns the UPC,
            #    returns it.
            sku_data = {new_child_sku: {"title": parent["title"]}}
            for field in (
                "mpn",
                "brand",
                "brand_code",
                "type_code",
                "product_type",
                "style_name",
                "sizing_scheme",
                "brand_color",
                "color",
            ):
                if parent.get(field):
                    sku_data[new_child_sku][field] = parent[field]
            if parent.get("serial_number") is not None:
                sku_data[new_child_sku]["serial_number"] = parent["serial_number"]

            try:
                db_result = await conn.execute_query_dict(
                    "SELECT add_skus($1::jsonb, $2) as result",
                    [orjson.dumps(sku_data).decode(), parent["company_code"]],
                )
                result_data = db_result[0]["result"] if db_result else None
                if isinstance(result_data, str):
                    result_data = orjson.loads(result_data)
                if not result_data or not result_data.get("success"):
                    errors = (result_data or {}).get("errors", [])
                    logger.error(f"add_skus failed for placeholder {new_child_sku}: {errors}")
                    size_failures = _size_failures_from_errors(errors)
                    return {
                        "success": False,
                        "new_child_sku": new_child_sku,
                        "error": (
                            size_failures[0]["error"]
                            if size_failures
                            else "Failed to add size"
                        ),
                    }
            except Exception:
                logger.error(
                    f"Failed to add placeholder {new_child_sku} to local DB: {traceback.format_exc()}"
                )
                return {
                    "success": False,
                    "new_child_sku": new_child_sku,
                    "error": "Internal server error",
                }

            next_upc = (
                result_data.get("assignments", {}).get(new_child_sku) or {}
            ).get("upc")

            # 2) Create on SellerCloud with the assigned UPC, retrying up to 3x.
            try:
                await _retry_async(
                    lambda: sellercloud_service.create_product(
                        product_sku=new_child_sku,
                        product_name=product_name,
                        company_id=parent["company_code"],
                        site_cost=site_cost,
                        product_type_name=product_type_name,
                        brand_name=brand_name,
                        upc=next_upc,
                    ),
                    max_retries=3,
                    delay=1,
                    task_name="create_product",
                )
                logger.info(
                    f"Created placeholder product {new_child_sku} on SellerCloud with UPC {next_upc}"
                )
            except Exception:
                logger.error(
                    f"Failed to create placeholder {new_child_sku} on SellerCloud after retries: {traceback.format_exc()}"
                )
                return {
                    "success": False,
                    "local_created": True,
                    "new_child_sku": new_child_sku,
                    "error": "Failed to create product on SellerCloud",
                }

            async def update_advanced_info():
                fields = [{"Name": "ProductName", "Value": product_name}]
                for field_name in ["BrandName", "ManufacturerSKU", "ListPrice", "LongDescription"]:
                    val = template_data.get(field_name)
                    if val:
                        fields.append({"Name": field_name, "Value": val})
                shipping_weight = template_data.get("ShippingWeight")
                if shipping_weight:
                    try:
                        total_oz = int(shipping_weight)
                        fields.append({"Name": "PackageWeightLbs", "Value": total_oz // 16})
                        fields.append({"Name": "PackageWeightOz", "Value": total_oz % 16})
                    except (ValueError, TypeError):
                        pass
                await sellercloud_service._make_request(
                    "PUT",
                    "/Catalog/AdvancedInfo",
                    data={"ProductID": new_child_sku, "Fields": fields},
                )

            async def copy_columns():
                await sellercloud_service.copy_custom_columns(
                    template_child_sku, new_child_sku, overrides={"SIZE": size}
                )

            async def upload_image():
                image_url = template_data.get("ImageUrl")
                if image_url:
                    await sellercloud_service.upload_product_image(new_child_sku, image_url)

            tasks = [
                _retry_async(
                    update_advanced_info, max_retries=3, delay=1, task_name="AdvancedInfo"
                ),
                _retry_async(copy_columns, max_retries=3, delay=1, task_name="CustomColumns"),
                _retry_async(upload_image, max_retries=3, delay=1, task_name="ImageUpload"),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_names = ["AdvancedInfo", "CustomColumns", "ImageUpload"]
                    logger.warning(
                        f"Non-fatal: {task_names[i]} failed for {new_child_sku}: {result}"
                    )

            logger.info(f"Created placeholder child {new_child_sku} with UPC {next_upc}")
            return {
                "success": True,
                "new_child_sku": new_child_sku,
                "parent_sku": parent_sku,
                "size": size,
            }

        except Exception as e:
            logger.error(
                f"Error adding placeholder size to parent {parent_sku}: {traceback.format_exc()}"
            )
            return {"success": False, "error": "Internal server error"}

    @staticmethod
    async def update_parent_product(
        sku: str,
        title: Optional[str] = None,
        product_type: Optional[str] = None,
        sizing_scheme: Optional[str] = None,
        style_name: Optional[str] = None,
        brand_color: Optional[str] = None,
        color: Optional[str] = None,
        mpn: Optional[str] = None,
        brand: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            set_parts = []
            params = []
            param_idx = 1

            if title is not None:
                set_parts.append(f"title = ${param_idx}")
                params.append(title)
                param_idx += 1
            if product_type is not None:
                set_parts.append(f"product_type = ${param_idx}")
                params.append(product_type)
                param_idx += 1
            if sizing_scheme is not None:
                set_parts.append(f"sizing_scheme = ${param_idx}")
                params.append(sizing_scheme)
                param_idx += 1
            if style_name is not None:
                set_parts.append(f"style_name = ${param_idx}")
                params.append(style_name)
                param_idx += 1
            if brand_color is not None:
                set_parts.append(f"brand_color = ${param_idx}")
                params.append(brand_color)
                param_idx += 1
            if color is not None:
                set_parts.append(f"color = ${param_idx}")
                params.append(color)
                param_idx += 1
            if mpn is not None:
                set_parts.append(f"mpn = ${param_idx}")
                params.append(mpn)
                param_idx += 1
            if brand is not None:
                set_parts.append(f"brand = ${param_idx}")
                params.append(brand)
                param_idx += 1

            if not set_parts:
                return {"success": False, "error": "No fields to update"}

            set_parts.append("updated_at = CURRENT_TIMESTAMP")
            params.append(sku)

            query = (
                f"UPDATE parent_products SET {', '.join(set_parts)} "
                f"WHERE sku = ${param_idx} AND is_active = TRUE "
                f"RETURNING sku, title, product_type, sizing_scheme, style_name, brand_color, color, mpn, brand"
            )

            result = await conn.execute_query_dict(query, params)
            if not result:
                return {"success": False, "error": f"Parent product '{sku}' not found"}
            row = result[0]
            return {
                "success": True,
                "sku": row["sku"],
                "title": row.get("title"),
                "product_type": row.get("product_type"),
                "sizing_scheme": row.get("sizing_scheme"),
                "style_name": row.get("style_name"),
                "brand_color": row.get("brand_color"),
                "color": row.get("color"),
                "mpn": row.get("mpn"),
                "brand": row.get("brand"),
            }
        except Exception as e:
            logger.error(f"Error updating parent product: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}


    @staticmethod
    def _validate_washtag_selections(
        selections: Optional[List[Dict]], old_parent_sku: str, new_parent_sku: str
    ) -> Optional[List[Dict]]:
        """Normalise the operator's washtag choice for storage, or raise ValueError.

        Stores `side` ("old" or "new"), not a parent SKU. The SKU would be a foreign
        key value into parent_products, duplicated from old_parent_sku / new_parent_sku
        on this very row, where no constraint can enforce it and a rename would leave
        it silently stale. A side is resolved against those FK columns at apply time,
        so the pair can only ever come from the ledger and naming a third product stops
        being possible rather than merely being rejected.

        `index` stays an ordinal because it addresses a position in
        productimages.washtag_data in a different database, so nothing there is
        referenceable anyway; `md5_hash` is the drift guard for it.

        None (no choice) and [] (clear them) are different and both meaningful, so this
        preserves the distinction rather than collapsing empty to None.
        """
        if selections is None:
            return None

        by_sku = {}
        if old_parent_sku:
            by_sku[old_parent_sku] = "old"
        if new_parent_sku:
            by_sku[new_parent_sku] = "new"

        normalised = []
        for sel in selections:
            get = sel.get if isinstance(sel, dict) else lambda k: getattr(sel, k, None)
            side = get("side")
            if side is None:
                # Accept a SKU from older callers and fold it down to a side.
                pid = get("product_id")
                side = by_sku.get(pid)
                if side is None:
                    raise ValueError(
                        f"Washtag selection names {pid}, which is not part of this reassignment"
                    )
            elif side not in ("old", "new"):
                raise ValueError(f"Washtag selection has an unknown side {side!r}")
            elif side == "old" and not old_parent_sku:
                raise ValueError("This product has no previous parent to take washtags from")

            normalised.append(
                {"side": side, "index": get("index"), "md5_hash": get("md5_hash")}
            )
        return normalised

    @staticmethod
    async def _apply_washtag_selections(
        selections: Optional[List[Dict]], old_parent_sku: str, new_parent_sku: str
    ) -> Optional[Dict[str, Any]]:
        """Copy the chosen washtags onto the new parent. Never raises.

        Runs after the reassignment has already succeeded. A GCS or photography-DB
        problem here must not fail or half-roll-back the SellerCloud job chain, so this
        follows the same rule as _queue_sellercloud_sync: a logged warning, not a
        failed operation. The selection is persisted on the assignment row, so a failure
        stays replayable.
        """
        if selections is None:
            return None
        try:
            from services.image_service import image_service

            # Resolve each stored side against this row's own FK columns. image_service
            # is a generic primitive over real product ids, so the SKU is reconstituted
            # here rather than persisted; that is the whole point of storing a side.
            by_side = {"old": old_parent_sku, "new": new_parent_sku}
            resolved = [
                {
                    "product_id": by_side[sel["side"]],
                    "index": sel["index"],
                    "md5_hash": sel.get("md5_hash"),
                }
                for sel in selections
            ]

            result = await image_service.replace_washtags(
                target_product_id=new_parent_sku,
                selections=resolved,
                source_parent=old_parent_sku,
                # Deliberately not asserting a timestamp: the choice was made before
                # the reassignment ran, so there is no read here to assert against, and
                # a stale one would silently drop the copy. The advisory lock and the
                # guarded UPDATE still protect the row.
                allow_clear=True,
            )
            if not result.get("success"):
                logger.warning(
                    f"Washtag copy onto {new_parent_sku} failed: {result.get('error')}"
                )
            return result
        except Exception:
            logger.warning(
                f"Washtag copy onto {new_parent_sku} raised; the reassignment stands:\n"
                f"{traceback.format_exc()}"
            )
            return {"success": False, "error": "Could not update washtags"}


    @staticmethod
    async def reassign_child_parent(
        child_sku: str,
        new_parent_sku: str,
        target_child_sku: str,
        created_by: Optional[str] = None,
        washtag_selections: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            child_result = await conn.execute_query_dict(
                """
                SELECT cp.sku, cp.parent_sku, cp.size, cp.is_primary
                FROM child_products cp
                WHERE cp.sku = $1 AND cp.is_active = TRUE
                """,
                [child_sku],
            )

            if not child_result:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "Child SKU not found",
                }

            old_parent_sku = child_result[0].get("parent_sku")

            # Validate before anything is written: a selection naming a third product
            # is a bad request, not a half-done reassignment.
            try:
                washtags = ProductService._validate_washtag_selections(
                    washtag_selections, old_parent_sku, new_parent_sku
                )
            except ValueError as e:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": str(e),
                }

            parent_result = await conn.execute_query_dict(
                """
                SELECT sku FROM parent_products
                WHERE sku = $1 AND is_active = TRUE
                """,
                [new_parent_sku],
            )

            if not parent_result:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "Parent SKU not found",
                }

            target_result = await conn.execute_query_dict(
                """
                SELECT cp.sku, cp.parent_sku,
                       (SELECT upc FROM child_upcs WHERE child_sku = cp.sku AND is_primary_upc = TRUE LIMIT 1) as primary_upc
                FROM child_products cp
                WHERE cp.sku = $1 AND cp.is_active = TRUE
                """,
                [target_child_sku],
            )

            if not target_result:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "Target child SKU not found",
                }

            if target_result[0].get("parent_sku") != new_parent_sku:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "Target child SKU does not belong to the specified parent",
                }

            assignment_result = await conn.execute_query_dict(
                """
                INSERT INTO parent_child_assignments (
                    old_child_sku,
                    old_parent_sku,
                    new_parent_sku,
                    is_primary_assignment,
                    target_primary_sku,
                    created_by,
                    washtag_selections
                ) VALUES ($1, $2, $3, FALSE, $4, $5, $6::jsonb)
                RETURNING id
                """,
                [
                    child_sku,
                    old_parent_sku,
                    new_parent_sku,
                    target_child_sku,
                    created_by,
                    orjson.dumps(washtags).decode() if washtags is not None else None,
                ],
            )

            if not assignment_result:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "Failed to create assignment",
                }

            assignment_id = assignment_result[0]["id"]

            is_placeholder = (target_result[0].get("primary_upc") or "").startswith("77777")
            placeholder_upc = None
            source_primary_upc = None
            if is_placeholder:
                placeholder_upc = target_result[0]["primary_upc"]
                source_primary = await conn.execute_query_dict(
                    "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE LIMIT 1",
                    [child_sku],
                )
                source_primary_upc = source_primary[0]["upc"] if source_primary else None

            job_types_result = await conn.execute_query_dict("""
                SELECT code, max_retries, execution_order
                FROM job_types
                WHERE is_active = TRUE AND applies_to_secondary = TRUE
                ORDER BY execution_order ASC
                """)

            if not job_types_result:
                return {
                    "success": False,
                    "child_sku": child_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "message": "No job types configured for secondary assignments",
                }

            job_ids = {}
            initial_result_data = orjson.dumps(
                {
                    "from_sku": child_sku,
                    "to_sku": target_child_sku,
                    "old_parent_sku": old_parent_sku,
                }
            ).decode()

            for job_type in job_types_result:
                job_result = await conn.execute_query_dict(
                    """
                    INSERT INTO assignment_jobs (
                        assignment_id, job_type_code, max_attempts, scheduled_at, result_data
                    ) VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
                    RETURNING id
                    """,
                    [assignment_id, job_type["code"], job_type["max_retries"], initial_result_data],
                )
                job_ids[job_type["code"]] = job_result[0]["id"]

            await conn.execute_query(
                "UPDATE parent_child_assignments SET total_jobs = $1 WHERE id = $2",
                [len(job_types_result), assignment_id],
            )

            job_context = {
                "child_sku": child_sku,
                "target_child_sku": target_child_sku,
                "old_parent_sku": old_parent_sku,
                "is_placeholder": is_placeholder,
            }

            last_result = None
            all_success = True
            executed_jobs = []

            for job_type in job_types_result:
                job_code = job_type["code"]
                job_id = job_ids[job_code]

                handler_name = JOB_HANDLERS.get(job_code)
                if not handler_name:
                    logger.warning(f"No handler for job type: {job_code}, skipping")
                    continue

                await conn.execute_query_dict(
                    "SELECT update_job_status($1::BIGINT, 'in_progress'::job_status)", [job_id]
                )

                handler = getattr(ProductService, handler_name)
                result = await handler(**job_context)
                last_result = result
                executed_jobs.append({"code": job_code, "job_id": job_id, "result": result})

                if result.get("success"):
                    await conn.execute_query_dict(
                        "SELECT update_job_status($1::BIGINT, 'completed'::job_status)", [job_id]
                    )
                else:
                    error_log = result.get("error") or result.get("user_message") or "Unknown error"
                    await conn.execute_query_dict(
                        "SELECT update_job_status($1::BIGINT, 'failed'::job_status, $2, $3)",
                        [job_id, error_log, orjson.dumps(result).decode()],
                    )
                    all_success = False
                    break

            if is_placeholder and all_success and source_primary_upc and placeholder_upc:
                try:
                    await conn.execute_query(
                        "DELETE FROM child_upcs WHERE child_sku = $1 AND upc = $2",
                        [target_child_sku, placeholder_upc],
                    )
                    await conn.execute_query(
                        "UPDATE child_upcs SET is_primary_upc = TRUE WHERE child_sku = $1 AND upc = $2",
                        [target_child_sku, source_primary_upc],
                    )
                    await sellercloud_service.update_product_upc(
                        target_child_sku, source_primary_upc
                    )
                    logger.info(
                        f"Placeholder cleanup: {target_child_sku} UPC {placeholder_upc} -> {source_primary_upc}"
                    )
                except Exception as cleanup_err:
                    logger.error(
                        f"Placeholder cleanup failed for {target_child_sku}: {cleanup_err}"
                    )

            if all_success:
                washtag_copy = await ProductService._apply_washtag_selections(
                    washtags, old_parent_sku, new_parent_sku
                )
                return {
                    "success": True,
                    "assignment_id": assignment_id,
                    "child_sku": child_sku,
                    "old_parent_sku": old_parent_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "jobs_executed": len(executed_jobs),
                    "washtag_copy": washtag_copy,
                    "message": f"{child_sku} assigned to {new_parent_sku} successfully",
                }
            else:
                return {
                    "success": False,
                    "assignment_id": assignment_id,
                    "child_sku": child_sku,
                    "old_parent_sku": old_parent_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "jobs_executed": len(executed_jobs),
                    "message": (
                        last_result.get("user_message", "Failed to change parent")
                        if last_result
                        else "Failed to change parent"
                    ),
                }

        except Exception as e:
            logger.error(f"Error changing parent with transfer: {e}")
            return {
                "success": False,
                "child_sku": child_sku,
                "new_parent_sku": new_parent_sku,
                "target_child_sku": target_child_sku,
                "message": "Failed to update parent",
            }

    @staticmethod
    async def search_products(
        query: str, is_parent: Optional[bool] = None, limit: int = 50
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            search_term = query.strip()
            search_lower = search_term.lower()
            search_prefix = f"{search_term}%"
            search_lower_prefix = f"{search_lower}%"
            # Skip contains search for very short queries — trigram indexes have poor
            # selectivity below 3 chars and would fall back to seq scan anyway.
            search_lower_contains = f"%{search_lower}%" if len(search_lower) >= 3 else None

            want_parents = is_parent is None or is_parent is True
            want_children = is_parent is None or is_parent is False

            async def _run_parents() -> List[Dict[str, Any]]:
                rows = await conn.execute_query_dict(
                    """
                    WITH candidates AS (
                        (SELECT sku, 0 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND LOWER(sku) = $1
                         LIMIT $4)
                        UNION ALL
                        (SELECT sku, 1 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND LOWER(mpn) = $1
                         LIMIT $4)
                        UNION ALL
                        (SELECT sku, 2 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND LOWER(sku) LIKE $2
                         LIMIT $4)
                        UNION ALL
                        (SELECT sku, 3 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND LOWER(mpn) LIKE $2
                         LIMIT $4)
                        UNION ALL
                        (SELECT sku, 3 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND LOWER(title) LIKE $2
                         LIMIT $4)
                        UNION ALL
                        (SELECT sku, 3 AS rank
                         FROM parent_products
                         WHERE is_active = TRUE AND title ILIKE $3
                         LIMIT $4)
                    ),
                    deduped AS (
                        SELECT sku, MIN(rank) AS rank
                        FROM candidates
                        GROUP BY sku
                    )
                    SELECT
                        pp.sku,
                        pp.title,
                        pp.mpn,
                        pp.brand,
                        d.rank AS _rank,
                        (SELECT COUNT(*) FROM child_products cp
                         WHERE cp.parent_sku = pp.sku AND cp.is_active = TRUE) AS child_count
                    FROM deduped d
                    JOIN parent_products pp ON pp.sku = d.sku
                    ORDER BY d.rank, pp.sku
                    LIMIT $4
                    """,
                    [search_lower, search_lower_prefix, search_lower_contains, limit],
                )
                return [
                    {
                        "sku": p["sku"],
                        "title": p.get("title"),
                        "mpn": p.get("mpn"),
                        "brand": p.get("brand"),
                        "size": None,
                        "is_primary": None,
                        "parent_sku": None,
                        "child_count": p.get("child_count", 0),
                        "is_parent": True,
                        "_rank": p.get("_rank"),
                    }
                    for p in rows
                ]

            async def _run_children() -> List[Dict[str, Any]]:
                is_numeric = search_term.isdigit()
                # Keywords allow letters, digits, and a safe symbol set, so any term
                # made only of those is a keyword candidate (a numeric term is also a
                # UPC candidate).
                is_keyword_candidate = bool(re.fullmatch(r"[A-Za-z0-9_./+#&@()-]+", search_term))

                # Build params and branches together. Only reference parameters
                # that are actually used — PostgreSQL can't infer types for
                # unreferenced placeholders.
                child_params: List[Any] = []

                def _p(value: Any) -> str:
                    child_params.append(value)
                    return f"${len(child_params)}"

                p_lower = _p(search_lower)
                p_lower_prefix = _p(search_lower_prefix)
                p_term = _p(search_term) if is_keyword_candidate else None
                p_prefix = _p(search_prefix) if is_keyword_candidate else None
                p_contains = _p(search_lower_contains) if search_lower_contains is not None else None
                p_limit = _p(limit)

                child_branches: List[str] = []
                if is_numeric:
                    child_branches.append(
                        f"(SELECT cu.child_sku AS sku, 0 AS rank FROM child_upcs cu WHERE cu.upc = {p_term} LIMIT {p_limit})"
                    )
                if is_keyword_candidate:
                    child_branches.append(
                        f"(SELECT cp.sku, 0 AS rank FROM child_products cp WHERE cp.is_active = TRUE AND {p_term} = ANY(cp.keywords) LIMIT {p_limit})"
                    )
                child_branches += [
                    f"(SELECT cp.sku, 1 AS rank FROM child_products cp WHERE cp.is_active = TRUE AND LOWER(cp.sku) = {p_lower} LIMIT {p_limit})",
                    f"(SELECT cp.sku, 2 AS rank FROM parent_products pp JOIN child_products cp ON cp.parent_sku = pp.sku AND cp.is_active = TRUE WHERE pp.is_active = TRUE AND LOWER(pp.mpn) = {p_lower} LIMIT {p_limit})",
                    f"(SELECT cp.sku, 3 AS rank FROM child_products cp WHERE cp.is_active = TRUE AND LOWER(cp.sku) LIKE {p_lower_prefix} LIMIT {p_limit})",
                    f"(SELECT cp.sku, 3 AS rank FROM parent_products pp JOIN child_products cp ON cp.parent_sku = pp.sku AND cp.is_active = TRUE WHERE pp.is_active = TRUE AND LOWER(pp.mpn) LIKE {p_lower_prefix} LIMIT {p_limit})",
                    f"(SELECT cp.sku, 3 AS rank FROM parent_products pp JOIN child_products cp ON cp.parent_sku = pp.sku AND cp.is_active = TRUE WHERE pp.is_active = TRUE AND LOWER(pp.title) LIKE {p_lower_prefix} LIMIT {p_limit})",
                ]
                if is_numeric:
                    child_branches.append(
                        f"(SELECT cu.child_sku AS sku, 3 AS rank FROM child_upcs cu WHERE cu.upc LIKE {p_prefix} LIMIT {p_limit})"
                    )
                if is_keyword_candidate:
                    child_branches.append(
                        f"(SELECT cp.sku, 3 AS rank FROM child_products cp, unnest(cp.keywords) AS k WHERE cp.is_active = TRUE AND k LIKE {p_prefix} LIMIT {p_limit})"
                    )
                if p_contains is not None:
                    child_branches.append(
                        f"(SELECT cp.sku, 4 AS rank FROM parent_products pp JOIN child_products cp ON cp.parent_sku = pp.sku AND cp.is_active = TRUE WHERE pp.is_active = TRUE AND pp.title ILIKE {p_contains} LIMIT {p_limit})"
                    )

                union_sep = "\n                        UNION ALL\n                        "
                rows = await conn.execute_query_dict(
                    f"""
                    WITH candidates AS (
                        {union_sep.join(child_branches)}
                    ),
                    deduped AS (
                        SELECT sku, MIN(rank) AS rank
                        FROM candidates
                        GROUP BY sku
                    )
                    SELECT
                        cp.sku,
                        cp.size,
                        cp.is_primary,
                        cp.parent_sku,
                        cp.keywords,
                        pp.title,
                        pp.mpn,
                        pp.brand,
                        d.rank AS _rank
                    FROM deduped d
                    JOIN child_products cp ON cp.sku = d.sku AND cp.is_active = TRUE
                    LEFT JOIN parent_products pp ON cp.parent_sku = pp.sku
                    ORDER BY d.rank, cp.sku
                    LIMIT {p_limit}
                    """,
                    child_params,
                )
                return [
                    {
                        "sku": c["sku"],
                        "title": c.get("title"),
                        "mpn": c.get("mpn"),
                        "brand": c.get("brand"),
                        "size": c.get("size"),
                        "is_primary": c.get("is_primary"),
                        "parent_sku": c.get("parent_sku"),
                        "child_count": None,
                        "is_parent": False,
                        "_keywords": c.get("keywords") or [],
                        "_rank": c.get("_rank"),
                    }
                    for c in rows
                ]

            # Resolve a reassigned secondary SKU to its live primary's child row in
            # one JOIN. Runs in parallel with the main search; only used as a
            # fallback when no exact match was found.
            async def _run_secondary() -> Optional[Dict[str, Any]]:
                rows = await conn.execute_query_dict(
                    """
                    SELECT cp.sku, cp.size, cp.is_primary, cp.parent_sku,
                           pp.title, pp.mpn, pp.brand
                    FROM secondary_skus s
                    JOIN child_products cp
                      ON cp.sku = s.current_primary_sku AND cp.is_active = TRUE
                    LEFT JOIN parent_products pp ON cp.parent_sku = pp.sku
                    WHERE LOWER(s.secondary_sku) = LOWER($1)
                    LIMIT 1
                    """,
                    [search_term],
                )
                if not rows:
                    return None
                c = rows[0]
                return {
                    "sku": c["sku"],
                    "title": c.get("title"),
                    "mpn": c.get("mpn"),
                    "brand": c.get("brand"),
                    "size": c.get("size"),
                    "is_primary": c.get("is_primary"),
                    "parent_sku": c.get("parent_sku"),
                    "child_count": None,
                    "is_parent": False,
                }

            # Resolve a reassigned OLD PARENT (already known to be exact + fully
            # emptied) to its single new parent. NOT part of the per-search batch:
            # its `secondary_skus` scan isn't index-backed, so it runs lazily and
            # only when the top hit is an exact, childless parent (see below) —
            # normal searches never pay for it.
            async def _resolve_reassigned_parent(
                old_parent_sku: str,
            ) -> Optional[Dict[str, Any]]:
                targets = await conn.execute_query_dict(
                    """
                    SELECT DISTINCT cp.parent_sku AS new_parent_sku
                    FROM secondary_skus s
                    JOIN child_products cp ON cp.sku = s.current_primary_sku
                                          AND cp.is_active = TRUE
                    WHERE left(s.secondary_sku, length($1) + 1) = $1 || '/'
                    LIMIT 2
                    """,
                    [old_parent_sku],
                )
                if (
                    len(targets) != 1
                    or not targets[0]["new_parent_sku"]
                    or targets[0]["new_parent_sku"] == old_parent_sku
                ):
                    return None
                new_parent_sku = targets[0]["new_parent_sku"]
                rows = await conn.execute_query_dict(
                    """
                    SELECT pp.sku, pp.title, pp.mpn, pp.brand,
                           (SELECT COUNT(*) FROM child_products cp
                            WHERE cp.parent_sku = pp.sku AND cp.is_active = TRUE) AS child_count
                    FROM parent_products pp
                    WHERE pp.sku = $1 AND pp.is_active = TRUE
                    LIMIT 1
                    """,
                    [new_parent_sku],
                )
                if not rows:
                    return None
                p = rows[0]
                return {
                    "sku": p["sku"],
                    "title": p.get("title"),
                    "mpn": p.get("mpn"),
                    "brand": p.get("brand"),
                    "size": None,
                    "is_primary": None,
                    "parent_sku": None,
                    "child_count": p.get("child_count", 0),
                    "is_parent": True,
                }

            parent_task = _run_parents() if want_parents else None
            child_task = _run_children() if want_children else None
            # Secondary lookup is child-context only.
            secondary_task = _run_secondary() if want_children else None

            tasks = [t for t in (parent_task, child_task, secondary_task) if t is not None]
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            gathered_iter = iter(gathered)

            def _take(default):
                val = next(gathered_iter)
                if isinstance(val, Exception):
                    logger.error(f"search_products subquery failed: {val}")
                    return default
                return val

            parent_results = _take([]) if parent_task else []
            child_results = _take([]) if child_task else []
            secondary_result = _take(None) if secondary_task else None

            # Sort the merged list by SQL rank so the top result is the best
            # exact-equality hit across both parents and children.
            results = sorted(
                [*parent_results, *child_results],
                key=lambda r: (r.get("_rank") if r.get("_rank") is not None else 99),
            )

            # An "exact" match auto-selects on the UI. Qualifying SQL branches:
            #   child  rank 0 = UPC/keyword equality
            #   child  rank 1 = child SKU equality
            # MPN equality (parent rank 1, child rank 2) must NOT auto-select —
            # MPNs are not unique enough to load a product on. Parent SKU equality
            # (rank 0) and any prefix/contains (>=3 child, >=2 parent) also must
            # not auto-select.
            exact_match = False
            if results:
                top = results[0]
                top_rank = top.get("_rank")
                if top_rank is not None and not top.get("is_parent"):
                    exact_match = top_rank <= 1

            # If the term is an exact secondary SKU and the main search didn't
            # already nail an exact match, swap in the live primary as the sole
            # exact match so the UI's auto-select redirects to the active SKU.
            # Child secondary wins over parent reassignment (a SKU is either a
            # child or a parent — defensive precedence).
            if not exact_match and secondary_result is not None:
                results = [secondary_result]
                exact_match = True
            elif not exact_match and results:
                # Only pay for the reassigned-parent lookup when the user typed an
                # EXACT parent SKU (rank 0) that is now childless — the sole case
                # that can redirect. Every other search skips it entirely.
                top = results[0]
                if (
                    top.get("is_parent")
                    and top.get("_rank") == 0
                    and not top.get("child_count")
                ):
                    reassigned = await _resolve_reassigned_parent(top["sku"])
                    if reassigned is not None:
                        results = [reassigned]
                        exact_match = True

            for r in results:
                r.pop("_keywords", None)
                r.pop("_rank", None)

            return {"results": results[:limit], "total": len(results), "exact_match": exact_match}

        except Exception as e:
            logger.error(f"Error searching products with query '{query}': {e}")
            return {"results": [], "total": 0, "exact_match": False}

    @staticmethod
    async def get_product_details(sku: str) -> Dict[str, Any]:
        """
        Resolve any SKU (parent or child) to a unified parent-shaped payload.

        Always returns the parent's fields at the top level plus a `children`
        list. When the input SKU was a child, `selected_child` carries that
        child's size/UPCs/keywords. When the input was a parent itself,
        `selected_child` is `None`.
        """
        try:
            conn = await ProductService._get_connection()

            # (a) Reassigned CHILD redirect. Exact match first (unique index);
            # fall back to a case-insensitive match so a wrong-cased secondary
            # SKU still redirects to its live primary.
            redirect_result = await conn.execute_query_dict(
                "SELECT current_primary_sku FROM secondary_skus WHERE secondary_sku = $1",
                [sku],
            )
            if not redirect_result:
                redirect_result = await conn.execute_query_dict(
                    "SELECT current_primary_sku FROM secondary_skus WHERE LOWER(secondary_sku) = LOWER($1) LIMIT 1",
                    [sku],
                )
            if redirect_result:
                return {
                    "success": True,
                    "sku": sku,
                    "is_parent": None,
                    "redirect_to": redirect_result[0]["current_primary_sku"],
                    "error": None,
                }

            # (b) Resolve the input SKU to (parent_sku, child_sku) in one query.
            # `child_sku` is set only when the input matches an active child.
            # Exact match — index-backed hot path.
            resolved = await conn.execute_query_dict(
                """
                SELECT
                    COALESCE(cp.parent_sku, pp.sku) AS parent_sku,
                    cp.sku                          AS child_sku
                FROM (SELECT $1::text AS sku) i
                LEFT JOIN child_products  cp
                       ON cp.sku = i.sku AND cp.is_active = TRUE
                LEFT JOIN parent_products pp
                       ON pp.sku = i.sku AND pp.is_active = TRUE
                WHERE cp.sku IS NOT NULL OR pp.sku IS NOT NULL
                LIMIT 1
                """,
                [sku],
            )

            if not resolved:
                # (c) Case-insensitive fallback. If the SKU matches a product in
                # a different casing, redirect to its canonical stored form so
                # downstream child/parent matching in the UI stays correct.
                canonical = await conn.execute_query_dict(
                    """
                    SELECT cp.sku AS child_sku, pp.sku AS parent_sku
                    FROM (SELECT $1::text AS sku) i
                    LEFT JOIN child_products  cp
                           ON LOWER(cp.sku) = LOWER(i.sku) AND cp.is_active = TRUE
                    LEFT JOIN parent_products pp
                           ON LOWER(pp.sku) = LOWER(i.sku) AND pp.is_active = TRUE
                    WHERE cp.sku IS NOT NULL OR pp.sku IS NOT NULL
                    LIMIT 1
                    """,
                    [sku],
                )
                if canonical:
                    canonical_sku = canonical[0]["child_sku"] or canonical[0]["parent_sku"]
                    if canonical_sku and canonical_sku != sku:
                        return {
                            "success": True,
                            "sku": sku,
                            "is_parent": None,
                            "redirect_to": canonical_sku,
                            "error": None,
                        }
                return {
                    "success": False,
                    "sku": sku,
                    "is_parent": None,
                    "error": "Product not found",
                }

            parent_sku = resolved[0]["parent_sku"]
            child_sku = resolved[0]["child_sku"]

            parent_payload = await ProductService._build_parent_payload(
                conn, parent_sku
            )
            if not parent_payload:
                return {
                    "success": False,
                    "sku": sku,
                    "is_parent": None,
                    "error": "Parent product not found",
                }

            # (d) Reassigned PARENT redirect. When the input resolved to a parent
            # that has been fully emptied (no active children remaining), and all
            # of its former children now live under a single new parent, redirect
            # there — mirroring the child-redirect behavior. A child SKU is
            # `<parent>/<size>` and never changes on reassignment, so the old
            # parent's former children are exactly the `secondary_skus` rows whose
            # secondary_sku is prefixed `<parent>/`. Each resolves through the view
            # (which walks reassignment chains A -> B -> C fully and is cycle-guarded)
            # to its live primary; we take that primary's current parent. Deriving
            # from the view (not parent_child_assignments) keeps this consistent
            # with the child redirect and robust to assignment-ledger pruning.
            if child_sku is None and not parent_payload.get("children"):
                parent_redirect = await conn.execute_query_dict(
                    """
                    SELECT DISTINCT cp.parent_sku AS new_parent_sku
                    FROM secondary_skus s
                    JOIN child_products cp ON cp.sku = s.current_primary_sku
                                          AND cp.is_active = TRUE
                    WHERE left(s.secondary_sku, length($1) + 1) = $1 || '/'
                    LIMIT 2
                    """,
                    [parent_sku],
                )
                if (
                    len(parent_redirect) == 1
                    and parent_redirect[0]["new_parent_sku"]
                    and parent_redirect[0]["new_parent_sku"] != parent_sku
                ):
                    return {
                        "success": True,
                        "sku": sku,
                        "is_parent": None,
                        "redirect_to": parent_redirect[0]["new_parent_sku"],
                        "error": None,
                    }

            # Children list entries are full SelectedChild equivalents — when
            # the requested SKU was a child, just point `selected_child` at
            # the matching entry.
            selected_child = None
            if child_sku:
                selected_child = next(
                    (c for c in parent_payload["children"] if c["sku"] == child_sku),
                    None,
                )

            return {**parent_payload, "selected_child": selected_child}

        except Exception as e:
            logger.error(f"Error getting product details for '{sku}': {e}")
            return {"success": False, "sku": sku, "is_parent": None, "error": str(e)}

    @staticmethod
    async def _build_parent_payload(conn, parent_sku: str) -> Optional[Dict[str, Any]]:
        parent_result = await conn.execute_query_dict(
            """
            SELECT
                pp.sku,
                pp.title,
                pp.mpn,
                pp.brand,
                pp.type_code,
                pp.serial_number,
                pp.company_code,
                pp.product_type,
                pp.sizing_scheme,
                pp.style_name,
                pp.brand_color,
                pp.color
            FROM parent_products pp
            WHERE pp.sku = $1 AND pp.is_active = TRUE
            """,
            [parent_sku],
        )

        if not parent_result:
            return None

        parent = parent_result[0]

        children_result = await conn.execute_query_dict(
            """
            SELECT
                cp.sku,
                cp.size,
                cp.is_primary,
                cp.keywords,
                cu.upc,
                cu.is_primary_upc,
                cu.upc_type
            FROM child_products cp
            LEFT JOIN child_upcs cu ON cu.child_sku = cp.sku
            WHERE cp.parent_sku = $1 AND cp.is_active = TRUE
            ORDER BY cp.size, cp.is_primary DESC, cu.is_primary_upc DESC
            """,
            [parent_sku],
        )

        children_map: Dict[str, Dict[str, Any]] = {}
        for c in children_result:
            sku_key = c["sku"]
            if sku_key not in children_map:
                children_map[sku_key] = {
                    "sku": sku_key,
                    "size": c["size"],
                    "is_primary": c["is_primary"],
                    "parent_sku": parent_sku,
                    "primary_upc": None,
                    "all_upcs": [],
                    "keywords": c.get("keywords") or [],
                }
            if c.get("upc"):
                children_map[sku_key]["all_upcs"].append(
                    {
                        "upc": c["upc"],
                        "is_primary_upc": c["is_primary_upc"],
                        "upc_type": c.get("upc_type"),
                    }
                )
                if c["is_primary_upc"]:
                    children_map[sku_key]["primary_upc"] = c["upc"]

        children = list(children_map.values())

        parent_sizing_scheme = parent.get("sizing_scheme")
        if parent_sizing_scheme:
            default_conn = connections.get("default")
            scheme_rows = await default_conn.execute_query_dict(
                """
                SELECT size, "order"
                FROM listingoptions_sizing_schemes
                WHERE sizing_scheme = $1
                ORDER BY "order"
                """,
                [parent_sizing_scheme],
            )
            if scheme_rows:
                size_order = {row["size"]: row["order"] for row in scheme_rows}
                children.sort(
                    key=lambda c: (
                        size_order.get(c["size"], float("inf")),
                        not c["is_primary"],
                    )
                )

        return {
            "success": True,
            "sku": parent_sku,
            "is_parent": True,
            "title": parent.get("title"),
            "mpn": parent.get("mpn"),
            "brand": parent.get("brand"),
            "type_code": parent.get("type_code"),
            "serial_number": parent.get("serial_number"),
            "company_code": parent.get("company_code"),
            "product_type": parent.get("product_type"),
            "sizing_scheme": parent.get("sizing_scheme"),
            "style_name": parent.get("style_name"),
            "brand_color": parent.get("brand_color"),
            "color": parent.get("color"),
            "child_count": len(children),
            "children": children,
            "error": None,
        }

    @staticmethod
    async def get_bulk_reassign_preview(old_parent_sku: str, new_parent_sku: str) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            old_parent_result = await conn.execute_query_dict(
                """SELECT sku, title FROM parent_products
                   WHERE sku = $1 AND is_active = TRUE""",
                [old_parent_sku],
            )
            if not old_parent_result:
                return {"success": False, "error": f"Old parent SKU '{old_parent_sku}' not found"}

            new_parent_result = await conn.execute_query_dict(
                """SELECT sku, title FROM parent_products
                   WHERE sku = $1 AND is_active = TRUE""",
                [new_parent_sku],
            )
            if not new_parent_result:
                return {"success": False, "error": f"New parent SKU '{new_parent_sku}' not found"}

            if old_parent_sku == new_parent_sku:
                return {"success": False, "error": "Old and new parent cannot be the same"}

            old_children = await conn.execute_query_dict(
                """SELECT sku, size, is_primary
                   FROM child_products
                   WHERE parent_sku = $1 AND is_active = TRUE
                   ORDER BY size, is_primary DESC""",
                [old_parent_sku],
            )

            if not old_children:
                return {"success": False, "error": f"Old parent '{old_parent_sku}' has no children"}

            new_children = await conn.execute_query_dict(
                """SELECT sku, size, is_primary
                   FROM child_products
                   WHERE parent_sku = $1 AND is_active = TRUE
                   ORDER BY size, is_primary DESC""",
                [new_parent_sku],
            )

            new_size_map = {}
            for nc in new_children:
                size = nc["size"]
                if size not in new_size_map:
                    new_size_map[size] = nc

            mappings = []
            unmapped_count = 0
            for oc in old_children:
                old_size = oc["size"]
                matched_new = new_size_map.get(old_size)

                if matched_new:
                    mappings.append(
                        {
                            "old_child": {"sku": oc["sku"], "size": old_size},
                            "new_child": {"sku": matched_new["sku"], "size": matched_new["size"]},
                            "auto_matched": True,
                        }
                    )
                else:
                    mappings.append(
                        {
                            "old_child": {"sku": oc["sku"], "size": old_size},
                            "new_child": None,
                            "auto_matched": False,
                        }
                    )
                    unmapped_count += 1

            return {
                "success": True,
                "old_parent": {
                    "sku": old_parent_sku,
                    "title": old_parent_result[0].get("title"),
                    "child_count": len(old_children),
                },
                "new_parent": {
                    "sku": new_parent_sku,
                    "title": new_parent_result[0].get("title"),
                    "child_count": len(new_children),
                    "children": [{"sku": nc["sku"], "size": nc["size"]} for nc in new_children],
                },
                "mappings": mappings,
                "unmapped_count": unmapped_count,
                "can_proceed": unmapped_count == 0,
            }

        except Exception as e:
            logger.error(f"Error getting bulk reassign preview: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    async def create_bulk_reassignment(
        old_parent_sku: str,
        new_parent_sku: str,
        mappings: List[Dict[str, str]],
        created_by: Optional[str] = None,
        washtag_selections: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            if not mappings:
                return {"success": False, "error": "No mappings provided"}

            try:
                washtags = ProductService._validate_washtag_selections(
                    washtag_selections, old_parent_sku, new_parent_sku
                )
            except ValueError as e:
                return {"success": False, "error": str(e)}

            bulk_result = await conn.execute_query_dict(
                """INSERT INTO bulk_reassignments
                       (old_parent_sku, new_parent_sku, total_count, created_by, washtag_selections)
                   VALUES ($1, $2, $3, $4, $5::jsonb)
                   RETURNING id""",
                [
                    old_parent_sku,
                    new_parent_sku,
                    len(mappings),
                    created_by,
                    orjson.dumps(washtags).decode() if washtags is not None else None,
                ],
            )

            if not bulk_result:
                return {"success": False, "error": "Failed to create bulk reassignment"}

            bulk_id = bulk_result[0]["id"]

            assignment_ids = []
            failed_mappings = []

            for mapping in mappings:
                old_child_sku = mapping.get("old_child_sku")
                new_child_sku = mapping.get("new_child_sku")

                if not old_child_sku or not new_child_sku:
                    failed_mappings.append(
                        {
                            "old_child_sku": old_child_sku,
                            "new_child_sku": new_child_sku,
                            "error": "Missing SKU in mapping",
                        }
                    )
                    continue

                child_info = await conn.execute_query_dict(
                    "SELECT parent_sku FROM child_products WHERE sku = $1", [old_child_sku]
                )

                if not child_info:
                    failed_mappings.append(
                        {
                            "old_child_sku": old_child_sku,
                            "new_child_sku": new_child_sku,
                            "error": f"Source child {old_child_sku} not found",
                        }
                    )
                    continue

                old_parent = child_info[0]["parent_sku"]

                target_exists = await conn.execute_query_dict(
                    "SELECT 1 FROM child_products WHERE sku = $1", [new_child_sku]
                )

                if not target_exists:
                    failed_mappings.append(
                        {
                            "old_child_sku": old_child_sku,
                            "new_child_sku": new_child_sku,
                            "error": f"Target child {new_child_sku} not found in local database",
                        }
                    )
                    continue

                existing_pending = await conn.execute_query_dict(
                    """SELECT id FROM parent_child_assignments
                       WHERE new_parent_sku = $1 AND old_child_sku = $2 AND status = 'pending'""",
                    [new_parent_sku, old_child_sku],
                )

                if existing_pending:
                    failed_mappings.append(
                        {
                            "old_child_sku": old_child_sku,
                            "new_child_sku": new_child_sku,
                            "error": "Pending assignment already exists for this combination",
                        }
                    )
                    continue

                try:
                    assignment_result = await conn.execute_query_dict(
                        """INSERT INTO parent_child_assignments (
                               old_child_sku, old_parent_sku, new_parent_sku,
                               is_primary_assignment, target_primary_sku,
                               bulk_reassignment_id, created_by
                           ) VALUES ($1, $2, $3, FALSE, $4, $5, $6)
                           RETURNING id""",
                        [
                            old_child_sku,
                            old_parent,
                            new_parent_sku,
                            new_child_sku,
                            bulk_id,
                            created_by,
                        ],
                    )

                    if not assignment_result:
                        failed_mappings.append(
                            {
                                "old_child_sku": old_child_sku,
                                "new_child_sku": new_child_sku,
                                "error": "Failed to create assignment (unknown reason)",
                            }
                        )
                        continue

                except Exception as insert_error:
                    logger.error(f"Failed to create assignment for {old_child_sku}: {insert_error}")
                    failed_mappings.append(
                        {
                            "old_child_sku": old_child_sku,
                            "new_child_sku": new_child_sku,
                            "error": "Failed to update parent",
                        }
                    )
                    continue

                if assignment_result:
                    assignment_id = assignment_result[0]["id"]
                    assignment_ids.append(assignment_id)

                    job_types = await conn.execute_query_dict(
                        """SELECT code, max_retries FROM job_types
                           WHERE is_active = TRUE AND applies_to_secondary = TRUE
                           ORDER BY execution_order"""
                    )

                    initial_data = orjson.dumps(
                        {
                            "from_sku": old_child_sku,
                            "to_sku": new_child_sku,
                            "old_parent_sku": old_parent,
                        }
                    ).decode()

                    for jt in job_types:
                        await conn.execute_query(
                            """INSERT INTO assignment_jobs (assignment_id, job_type_code, max_attempts, scheduled_at, result_data)
                               VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)""",
                            [assignment_id, jt["code"], jt["max_retries"], initial_data],
                        )

                    await conn.execute_query(
                        "UPDATE parent_child_assignments SET total_jobs = $1 WHERE id = $2",
                        [len(job_types), assignment_id],
                    )

            return {
                "success": True,
                "bulk_assignment_id": bulk_id,
                "total_mappings": len(assignment_ids),
                "failed_mappings": failed_mappings,
                "status": "pending",
            }

        except Exception as e:
            logger.error(f"Error creating bulk reassignment: {e}")
            return {"success": False, "error": "Failed to create bulk reassignment"}

    @staticmethod
    async def get_bulk_reassignment_status(bulk_id: int) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            bulk_result = await conn.execute_query_dict(
                """SELECT id, old_parent_sku, new_parent_sku, status,
                          total_count, completed_count, failed_count,
                          created_at, started_at, completed_at
                   FROM bulk_reassignments WHERE id = $1""",
                [bulk_id],
            )

            if not bulk_result:
                return {"success": False, "error": "Bulk reassignment not found"}

            bulk = bulk_result[0]

            assignments = await conn.execute_query_dict(
                """SELECT pca.id, pca.old_child_sku, pca.target_primary_sku as new_child_sku,
                          pca.status, pca.completed_jobs, pca.total_jobs
                   FROM parent_child_assignments pca
                   WHERE pca.bulk_reassignment_id = $1
                   ORDER BY pca.id""",
                [bulk_id],
            )

            current_sku = None
            for a in assignments:
                if a["status"] == "in_progress":
                    current_sku = a["old_child_sku"]
                    break

            return {
                "success": True,
                "bulk_assignment_id": bulk_id,
                "old_parent_sku": bulk["old_parent_sku"],
                "new_parent_sku": bulk["new_parent_sku"],
                "status": bulk["status"],
                "total": bulk["total_count"],
                "completed": bulk["completed_count"],
                "failed": bulk["failed_count"],
                "current_sku": current_sku,
                "created_at": bulk["created_at"].isoformat() if bulk["created_at"] else None,
                "started_at": bulk["started_at"].isoformat() if bulk["started_at"] else None,
                "completed_at": bulk["completed_at"].isoformat() if bulk["completed_at"] else None,
                "assignments": [
                    {
                        "assignment_id": a["id"],
                        "old_child_sku": a["old_child_sku"],
                        "new_child_sku": a["new_child_sku"],
                        "status": a["status"],
                        "completed_jobs": a["completed_jobs"],
                        "total_jobs": a["total_jobs"],
                    }
                    for a in assignments
                ],
            }

        except Exception as e:
            logger.error(f"Error getting bulk reassignment status: {e}")
            return {"success": False, "error": "Failed to get bulk reassignment status"}


    @staticmethod
    async def _apply_bulk_washtag_selections(bulk_id: int) -> None:
        """Apply a bulk reassignment's stored washtag choice, once, at the end.

        Idempotent by way of replace_washtags' own no-op short-circuit, which matters
        because the UI polls /process and can reach the terminal tick more than once.
        """
        try:
            conn = await ProductService._get_connection()
            rows = await conn.execute_query_dict(
                """SELECT old_parent_sku, new_parent_sku, washtag_selections
                   FROM bulk_reassignments WHERE id = $1""",
                [bulk_id],
            )
            if not rows or rows[0]["washtag_selections"] is None:
                return

            raw = rows[0]["washtag_selections"]
            selections = orjson.loads(raw) if isinstance(raw, (str, bytes)) else raw
            await ProductService._apply_washtag_selections(
                selections, rows[0]["old_parent_sku"], rows[0]["new_parent_sku"]
            )
        except Exception:
            logger.warning(
                f"Bulk washtag copy for {bulk_id} raised; the reassignments stand:\n"
                f"{traceback.format_exc()}"
            )

    @staticmethod
    async def process_next_bulk_assignment(bulk_id: int) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            pending = await conn.execute_query_dict(
                """SELECT pca.id, pca.old_child_sku, pca.target_primary_sku, pca.new_parent_sku, pca.old_parent_sku,
                          (SELECT upc FROM child_upcs WHERE child_sku = pca.target_primary_sku AND is_primary_upc = TRUE LIMIT 1) as target_primary_upc
                   FROM parent_child_assignments pca
                   WHERE pca.bulk_reassignment_id = $1 AND pca.status = 'pending'
                   ORDER BY pca.id
                   LIMIT 1""",
                [bulk_id],
            )

            if not pending:
                in_progress = await conn.execute_query_dict(
                    """SELECT id FROM parent_child_assignments
                       WHERE bulk_reassignment_id = $1 AND status = 'in_progress'
                       LIMIT 1""",
                    [bulk_id],
                )

                if in_progress:
                    return {
                        "success": True,
                        "status": "in_progress",
                        "message": "Assignment still processing",
                    }

                # Terminal tick: nothing pending and nothing in flight. Reached exactly
                # once, and on both the completed and the failed outcome, which is what
                # lets a partially failed bulk still carry the washtags across. Washtags
                # are parent-level, so this must run here rather than per assignment.
                await ProductService._apply_bulk_washtag_selections(bulk_id)

                return await ProductService.get_bulk_reassignment_status(bulk_id)

            assignment = pending[0]
            assignment_id = assignment["id"]
            child_sku = assignment["old_child_sku"]
            target_child_sku = assignment["target_primary_sku"]
            old_parent_sku = assignment["old_parent_sku"]
            new_parent_sku = assignment["new_parent_sku"]

            is_placeholder = (assignment.get("target_primary_upc") or "").startswith("77777")
            placeholder_upc = None
            source_primary_upc = None
            if is_placeholder:
                placeholder_upc = assignment["target_primary_upc"]
                source_primary = await conn.execute_query_dict(
                    "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE LIMIT 1",
                    [child_sku],
                )
                source_primary_upc = source_primary[0]["upc"] if source_primary else None

            job_types = await conn.execute_query_dict(
                """SELECT aj.id as job_id, aj.job_type_code
                   FROM assignment_jobs aj
                   JOIN job_types jt ON aj.job_type_code = jt.code
                   WHERE aj.assignment_id = $1 AND aj.status = 'pending'
                   ORDER BY jt.execution_order""",
                [assignment_id],
            )

            if not job_types:
                await conn.execute_query(
                    "UPDATE parent_child_assignments SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = $1",
                    [assignment_id],
                )
                return {"success": True, "status": "processed", "assignment_id": assignment_id}

            job_context = {
                "child_sku": child_sku,
                "target_child_sku": target_child_sku,
                "old_parent_sku": old_parent_sku,
                "is_placeholder": is_placeholder,
            }

            all_success = True
            for job in job_types:
                job_id = job["job_id"]
                job_code = job["job_type_code"]

                handler_name = JOB_HANDLERS.get(job_code)
                if not handler_name:
                    logger.warning(f"No handler for job type: {job_code}, skipping")
                    continue

                await conn.execute_query_dict(
                    "SELECT update_job_status($1::BIGINT, 'in_progress'::job_status)", [job_id]
                )

                handler = getattr(ProductService, handler_name)
                result = await handler(**job_context)

                if result.get("success"):
                    await conn.execute_query_dict(
                        "SELECT update_job_status($1::BIGINT, 'completed'::job_status)", [job_id]
                    )
                else:
                    error_log = result.get("error") or result.get("user_message") or "Unknown error"
                    await conn.execute_query_dict(
                        "SELECT update_job_status($1::BIGINT, 'failed'::job_status, $2, $3)",
                        [job_id, error_log, orjson.dumps(result).decode()],
                    )
                    all_success = False
                    break

            if is_placeholder and all_success and source_primary_upc and placeholder_upc:
                try:
                    await conn.execute_query(
                        "DELETE FROM child_upcs WHERE child_sku = $1 AND upc = $2",
                        [target_child_sku, placeholder_upc],
                    )
                    await conn.execute_query(
                        "UPDATE child_upcs SET is_primary_upc = TRUE WHERE child_sku = $1 AND upc = $2",
                        [target_child_sku, source_primary_upc],
                    )
                    await sellercloud_service.update_product_upc(
                        target_child_sku, source_primary_upc
                    )
                    logger.info(
                        f"Bulk placeholder cleanup: {target_child_sku} UPC {placeholder_upc} -> {source_primary_upc}"
                    )
                except Exception as cleanup_err:
                    logger.error(
                        f"Bulk placeholder cleanup failed for {target_child_sku}: {cleanup_err}"
                    )

            return {
                "success": True,
                "status": "processed",
                "assignment_id": assignment_id,
                "child_sku": child_sku,
                "all_jobs_success": all_success,
            }

        except Exception as e:
            logger.error(f"Error processing bulk assignment: {e}")
            return {"success": False, "error": "Failed to process assignment"}

    # ========================================================================
    # UPC Management
    # ========================================================================

    @staticmethod
    async def add_upc(sku: str, upc: str) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            # Verify child SKU exists
            child_check = await conn.execute_query_dict(
                "SELECT sku FROM child_products WHERE sku = $1", [sku]
            )
            if not child_check:
                return {"success": False, "error": f"Child SKU '{sku}' not found"}

            # Check if UPC already exists
            existing = await conn.execute_query_dict(
                "SELECT child_sku FROM child_upcs WHERE upc = $1", [upc]
            )
            if existing:
                existing_sku = existing[0]["child_sku"]
                if existing_sku == sku:
                    return {"success": False, "error": "UPC already exists for this SKU"}
                return {"success": False, "error": f"UPC already exists for SKU: {existing_sku}"}

            # Determine whether this UPC will become primary (first UPC for this child)
            existing_count = await conn.execute_query_dict(
                "SELECT COUNT(*) AS cnt FROM child_upcs WHERE child_sku = $1", [sku]
            )
            will_be_primary = existing_count[0]["cnt"] == 0

            # Sync to SellerCloud first — leave DB untouched on failure so the UI can retry
            try:
                await sellercloud_internal_service.sync_add_alias(
                    sku, upc, is_primary=will_be_primary
                )
            except SellercloudPermanentError as e:
                logger.info(f"Permanent SellerCloud failure adding {upc} to {sku}: {e}")
                return {"success": False, "error": str(e)}
            except Exception as e:
                logger.error(
                    f"Transient SellerCloud failure adding {upc} to {sku}: {e}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "error": "SellerCloud is temporarily unavailable. Please try again.",
                }

            # Insert UPC (DB trigger sets is_primary_upc if first UPC)
            await conn.execute_query(
                "INSERT INTO child_upcs (upc, child_sku) VALUES ($1, $2)", [upc, sku]
            )

            # Read back to get is_primary_upc and upc_type
            inserted = await conn.execute_query_dict(
                """SELECT is_primary_upc,
                    CASE WHEN LENGTH(upc) = 8 THEN 'EAN-8'
                         WHEN LENGTH(upc) = 12 THEN 'UPC-A'
                         ELSE 'EAN-13'
                    END as upc_type
                FROM child_upcs WHERE upc = $1""",
                [upc],
            )
            is_primary = inserted[0]["is_primary_upc"] if inserted else False
            upc_type = inserted[0]["upc_type"] if inserted else "EAN-13"

            return {"success": True, "sku": sku, "upc": upc, "is_primary": is_primary, "upc_type": upc_type}

        except Exception as e:
            logger.error(f"Error adding UPC {upc} to {sku}: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    async def set_primary_upc(sku: str, upc: str) -> Dict[str, Any]:
        if len(upc) == 8:
            return {"success": False, "error": "EAN-8 UPCs cannot be set as primary"}

        try:
            conn = await ProductService._get_connection()

            # Verify the UPC actually exists for this SKU before touching SellerCloud
            exists = await conn.execute_query_dict(
                "SELECT 1 FROM child_upcs WHERE child_sku = $1 AND upc = $2",
                [sku, upc],
            )
            if not exists:
                return {"success": False, "error": f"UPC '{upc}' not found for SKU '{sku}'"}

            # Fetch the current primary so we know what to demote in SellerCloud
            current_primary_rows = await conn.execute_query_dict(
                "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE",
                [sku],
            )
            old_primary = current_primary_rows[0]["upc"] if current_primary_rows else None

            # Sync to SellerCloud first — leave DB untouched on failure
            try:
                await sellercloud_internal_service.sync_change_primary(
                    sku, new_primary=upc, old_primary=old_primary
                )
            except SellercloudPermanentError as e:
                logger.info(
                    f"Permanent SellerCloud failure setting primary for {sku} "
                    f"({old_primary} -> {upc}): {e}"
                )
                return {"success": False, "error": str(e)}
            except Exception as e:
                logger.error(
                    f"Transient SellerCloud failure setting primary for {sku} "
                    f"({old_primary} -> {upc}): {e}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "error": "SellerCloud is temporarily unavailable. Please try again.",
                }

            result = await conn.execute_query_dict(
                "SELECT update_primary_upc_for_child($1, $2) as result", [sku, upc]
            )
            db_result = (
                json.loads(result[0]["result"])
                if isinstance(result[0]["result"], str)
                else result[0]["result"]
            )

            if not db_result.get("success"):
                return {"success": False, "error": db_result.get("error", "Failed to update primary UPC")}

            return {
                "success": True,
                "sku": sku,
                "old_primary_upc": db_result.get("old_primary_upc"),
                "new_primary_upc": upc,
                "message": db_result.get("message", "Primary UPC updated successfully"),
            }

        except Exception as e:
            logger.error(f"Error setting primary UPC for {sku}: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    async def delete_upc(sku: str, upc: str) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            # Verify UPC exists and belongs to this SKU
            upc_check = await conn.execute_query_dict(
                "SELECT upc, is_primary_upc FROM child_upcs WHERE upc = $1 AND child_sku = $2",
                [upc, sku],
            )
            if not upc_check:
                return {"success": False, "error": f"UPC '{upc}' not found for SKU '{sku}'"}

            if upc_check[0]["is_primary_upc"]:
                return {"success": False, "error": "Cannot delete primary UPC. Set a different primary first."}

            # Sync to SellerCloud first — leave DB untouched on failure so the UI can retry
            try:
                await sellercloud_internal_service.sync_delete_alias(sku, upc)
            except SellercloudPermanentError as e:
                logger.info(f"Permanent SellerCloud failure deleting {upc} from {sku}: {e}")
                return {"success": False, "error": str(e)}
            except Exception as e:
                logger.error(
                    f"Transient SellerCloud failure deleting {upc} from {sku}: {e}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "error": "SellerCloud is temporarily unavailable. Please try again.",
                }

            await conn.execute_query("DELETE FROM child_upcs WHERE upc = $1", [upc])

            return {"success": True, "sku": sku, "upc": upc}

        except Exception as e:
            logger.error(f"Error deleting UPC {upc} from {sku}: {e}")
            return {"success": False, "error": str(e)}

    # ========================================================================
    # Keyword Management (synced to SellerCloud as non-primary aliases)
    # ========================================================================

    @staticmethod
    async def add_keyword(sku: str, keyword: str) -> Dict[str, Any]:
        clean_keyword = keyword.strip()
        try:
            conn = await ProductService._get_connection()

            # Verify child SKU exists
            child_check = await conn.execute_query_dict(
                "SELECT sku FROM child_products WHERE sku = $1", [sku]
            )
            if not child_check:
                return {"success": False, "error": f"Child SKU '{sku}' not found"}

            # Validate keyword via DB function
            validation_result = await conn.execute_query_dict(
                "SELECT validate_keyword($1, $2) as result", [clean_keyword, sku]
            )
            validation_raw = validation_result[0]["result"]
            validation = (
                json.loads(validation_raw) if isinstance(validation_raw, str) else validation_raw
            )

            if not validation.get("valid"):
                return {"success": False, "error": validation.get("error", "Invalid keyword")}

            clean_keyword = validation.get("keyword", clean_keyword)

            # Guard against duplicates before pushing to SellerCloud
            existing = await conn.execute_query_dict(
                "SELECT 1 FROM child_products WHERE sku = $1 AND $2 = ANY(keywords)",
                [sku, clean_keyword],
            )
            if existing:
                return {"success": False, "error": "Keyword already exists for this SKU"}

            # Sync to SellerCloud first — leave DB untouched on failure so the UI can retry
            try:
                await sellercloud_internal_service.sync_add_alias(
                    sku, clean_keyword, is_primary=False
                )
            except SellercloudPermanentError as e:
                logger.info(
                    f"Permanent SellerCloud failure adding keyword {clean_keyword} to {sku}: {e}"
                )
                return {"success": False, "error": str(e)}
            except Exception as e:
                logger.error(
                    f"Transient SellerCloud failure adding keyword {clean_keyword} to {sku}: {e}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "error": "SellerCloud is temporarily unavailable. Please try again.",
                }

            # Add to keywords array
            await conn.execute_query(
                """UPDATE child_products
                   SET keywords = array_append(COALESCE(keywords, '{}'), $1),
                       updated_at = CURRENT_TIMESTAMP
                   WHERE sku = $2""",
                [clean_keyword, sku],
            )

            return {"success": True, "sku": sku, "keyword": clean_keyword}

        except Exception as e:
            logger.error(f"Error adding keyword {keyword} to {sku}: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    async def delete_keyword(sku: str, keyword: str) -> Dict[str, Any]:
        clean_keyword = keyword.strip()
        try:
            conn = await ProductService._get_connection()

            # Verify keyword exists for this SKU
            check = await conn.execute_query_dict(
                "SELECT sku FROM child_products WHERE sku = $1 AND $2 = ANY(keywords)",
                [sku, clean_keyword],
            )
            if not check:
                return {"success": False, "error": f"Keyword '{clean_keyword}' not found for SKU '{sku}'"}

            # Sync to SellerCloud first — leave DB untouched on failure so the UI can retry
            try:
                await sellercloud_internal_service.sync_delete_alias(sku, clean_keyword)
            except SellercloudPermanentError as e:
                logger.info(
                    f"Permanent SellerCloud failure deleting keyword {clean_keyword} from {sku}: {e}"
                )
                return {"success": False, "error": str(e)}
            except Exception as e:
                logger.error(
                    f"Transient SellerCloud failure deleting keyword {clean_keyword} from {sku}: {e}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "error": "SellerCloud is temporarily unavailable. Please try again.",
                }

            await conn.execute_query(
                """UPDATE child_products
                   SET keywords = array_remove(keywords, $1),
                       updated_at = CURRENT_TIMESTAMP
                   WHERE sku = $2""",
                [clean_keyword, sku],
            )

            return {"success": True, "sku": sku, "keyword": clean_keyword}

        except Exception as e:
            logger.error(f"Error deleting keyword {keyword} from {sku}: {e}")
            return {"success": False, "error": str(e)}

    # ========================================================================
    # Bulk Import (DB only, no SellerCloud sync)
    # ========================================================================

    @staticmethod
    def _calculate_upc_check_digit(upc_base: str) -> int:
        digits = [int(d) for d in upc_base]
        if len(digits) in (7, 11):
            total = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(digits))
        else:
            total = sum(d * (1 if i % 2 == 0 else 3) for i, d in enumerate(digits))
        return (10 - (total % 10)) % 10

    @staticmethod
    def _validate_upc_checksum(upc: str) -> bool:
        if len(upc) not in (8, 12, 13) or not upc.isdigit():
            return False
        return ProductService._calculate_upc_check_digit(upc[:-1]) == int(upc[-1])

    @staticmethod
    def _is_valid_barcode(code: str) -> bool:
        if not code.isdigit() or len(code) not in (8, 12, 13):
            return False
        return ProductService._validate_upc_checksum(code)

    @staticmethod
    async def validate_bulk_import(content: bytes, filename: str) -> Dict[str, Any]:
        import base64
        import io
        import pandas as pd

        VALID_ACTIONS = {"Primary", "Secondary", "Keyword", "Delete"}
        errors = []
        items = []
        error_by_index = {}

        try:
            fname = filename.lower()
            try:
                if fname.endswith(".csv"):
                    df = pd.read_csv(io.BytesIO(content), dtype=str)
                elif fname.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(io.BytesIO(content), dtype=str)
                else:
                    return {"valid": False, "errors": [{"row": 0, "field": "file", "message": "Unsupported file format. Use CSV or Excel (.xlsx, .xls)"}], "items": []}
            except Exception as e:
                return {"valid": False, "errors": [{"row": 0, "field": "file", "message": f"Failed to parse file: {str(e)}"}], "items": []}

            # Normalize columns
            df.columns = df.columns.str.strip().str.lower()
            column_map = {
                "product": "sku", "child sku": "sku", "childsku": "sku", "sku": "sku",
                "upc": "value", "barcode": "value", "keyword": "value", "value": "value",
                "type": "action", "action": "action", "type/action": "action",
            }
            df = df.rename(columns=column_map)

            required_columns = {"sku", "value", "action"}
            missing = required_columns - set(df.columns)
            if missing:
                return {"valid": False, "errors": [{"row": 0, "field": "columns", "message": f"Missing required columns: {', '.join(missing)}. Expected: Product, UPC, Type/Action"}], "items": []}

            conn = await ProductService._get_connection()

            # Batch lookups
            file_skus = df["sku"].dropna().astype(str).str.strip().unique().tolist()
            existing_skus: set = set()
            inactive_skus: set = set()
            secondary_to_primary: dict = {}
            if file_skus:
                r = await conn.execute_query_dict(
                    "SELECT sku, is_active FROM child_products WHERE sku = ANY($1)", [file_skus]
                )
                existing_skus = {row["sku"] for row in r}
                inactive_skus = {row["sku"] for row in r if not row["is_active"]}

                s = await conn.execute_query_dict(
                    "SELECT secondary_sku, current_primary_sku FROM secondary_skus WHERE secondary_sku = ANY($1)",
                    [file_skus],
                )
                secondary_to_primary = {row["secondary_sku"]: row["current_primary_sku"] for row in s}

            # Values are matched as-is (trimmed only, never char-stripped). Each value
            # is looked up against both UPCs and keywords; non-matching ones just don't
            # appear in the maps.
            file_values = df["value"].dropna().astype(str).apply(lambda v: v.strip()).unique().tolist()
            file_values = [v for v in file_values if v]

            upc_to_sku = {}
            upc_is_primary = {}
            if file_values:
                r = await conn.execute_query_dict(
                    "SELECT upc, child_sku, is_primary_upc FROM child_upcs WHERE upc = ANY($1)", [file_values]
                )
                upc_to_sku = {row["upc"]: row["child_sku"] for row in r}
                upc_is_primary = {row["upc"]: row["is_primary_upc"] for row in r}

            keyword_to_sku = {}
            if file_values:
                r = await conn.execute_query_dict(
                    "SELECT keyword, sku FROM (SELECT unnest(keywords) as keyword, sku FROM child_products WHERE keywords && $1::text[]) sub",
                    [file_values],
                )
                keyword_to_sku = {row["keyword"]: row["sku"] for row in r}

            sku_primary_upc = {}
            if file_skus:
                r = await conn.execute_query_dict(
                    "SELECT child_sku, upc FROM child_upcs WHERE child_sku = ANY($1) AND is_primary_upc = TRUE", [file_skus]
                )
                sku_primary_upc = {row["child_sku"]: row["upc"] for row in r}

            # Intra-CSV duplicate detection
            seen_values: set = set()

            # Validate rows
            for idx, row in df.iterrows():
                row_num = idx + 2
                sku = str(row.get("sku", "")).strip() if pd.notna(row.get("sku")) else ""
                value = str(row.get("value", "")).strip() if pd.notna(row.get("value")) else ""
                action = str(row.get("action", "")).strip() if pd.notna(row.get("action")) else ""

                if not sku:
                    error_by_index[idx] = "Product (SKU) is required"
                    errors.append({"row": row_num, "sku": None, "value": value or None, "field": "Product", "message": error_by_index[idx]})
                    continue
                if not value:
                    error_by_index[idx] = "UPC is required"
                    errors.append({"row": row_num, "sku": sku, "value": None, "field": "UPC", "message": error_by_index[idx]})
                    continue
                if not action:
                    error_by_index[idx] = "Type/Action is required"
                    errors.append({"row": row_num, "sku": sku, "value": value, "field": "Type/Action", "message": error_by_index[idx]})
                    continue

                action_normalized = action.capitalize()
                if action_normalized not in VALID_ACTIONS:
                    error_by_index[idx] = f"Invalid action '{action}'. Must be: Primary, Secondary, Keyword, Delete"
                    errors.append({"row": row_num, "sku": sku, "value": value, "field": "Type/Action", "message": error_by_index[idx]})
                    continue

                # Never strip characters from the value; validate it as-is (trimmed).
                item_value = value

                if action_normalized in ("Primary", "Secondary") and not re.fullmatch(r"\d+", value):
                    error_by_index[idx] = "UPC must contain only digits"
                    errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                    continue

                if sku not in existing_skus:
                    error_by_index[idx] = f"SKU '{sku}' not found in database"
                    errors.append({"row": row_num, "sku": sku, "value": item_value, "field": "Product", "message": error_by_index[idx]})
                    continue

                # Block all operations on inactive or reassigned (secondary)
                # SKUs.  These children no longer own their own data — UPCs,
                # keywords, and inventory belong to the current primary child.
                if sku in inactive_skus:
                    primary = secondary_to_primary.get(sku)
                    if primary:
                        error_by_index[idx] = (
                            f"{sku} was transferred to {primary}. "
                            "UPCs can't be updated. Reimport using "
                            f"{primary}."
                        )
                    else:
                        error_by_index[idx] = (
                            f"{sku} is inactive. UPCs can't be updated."
                        )
                    errors.append(
                        {
                            "row": row_num,
                            "sku": sku,
                            "value": item_value,
                            "field": "Product",
                            "message": error_by_index[idx],
                        }
                    )
                    continue
                if sku in secondary_to_primary:
                    primary = secondary_to_primary[sku]
                    error_by_index[idx] = (
                        f"{sku} was transferred to {primary}. "
                        "UPCs can't be updated. Reimport using "
                        f"{primary}."
                    )
                    errors.append(
                        {
                            "row": row_num,
                            "sku": sku,
                            "value": item_value,
                            "field": "Product",
                            "message": error_by_index[idx],
                        }
                    )
                    continue

                # Intra-CSV duplicate check
                if item_value in seen_values:
                    error_by_index[idx] = f"Duplicate value in import file"
                    errors.append({"row": row_num, "sku": sku, "value": item_value, "field": "UPC", "message": error_by_index[idx]})
                    continue
                seen_values.add(item_value)

                classification = None
                source_sku = None

                if action_normalized in ("Primary", "Secondary"):
                    if len(value) not in (8, 12, 13):
                        error_by_index[idx] = f"UPC must be 8, 12, or 13 digits (got {len(value)})"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if not ProductService._validate_upc_checksum(value):
                        error_by_index[idx] = "Invalid UPC"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if action_normalized == "Primary" and len(value) == 8:
                        error_by_index[idx] = "EAN-8 cannot be set as primary"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "Type/Action", "message": error_by_index[idx]})
                        continue

                    if value in upc_to_sku:
                        owner = upc_to_sku[value]
                        if owner == sku:
                            # UPC already on target — noop or promote
                            if action_normalized == "Primary" and not upc_is_primary.get(value):
                                classification = "promote_primary"
                            else:
                                classification = "noop"
                        else:
                            # UPC on a different SKU — classify as swap
                            classification = f"swap_{action_normalized.lower()}"
                            source_sku = owner
                    else:
                        classification = f"add_{action_normalized.lower()}"

                elif action_normalized == "Delete":
                    is_valid_upc = bool(re.fullmatch(r"\d+", value)) and len(value) in (8, 12, 13) and ProductService._validate_upc_checksum(value)
                    if is_valid_upc:
                        # Must actually belong to the target SKU
                        owner = upc_to_sku.get(value)
                        if owner is None:
                            error_by_index[idx] = f"UPC '{value}' not found on any SKU"
                            errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        if owner != sku:
                            error_by_index[idx] = f"Cannot delete, UPC belongs to a different SKU ({owner})"
                            errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        if sku_primary_upc.get(sku) == value:
                            error_by_index[idx] = "Cannot delete primary UPC"
                            errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        classification = "delete_upc"
                    elif not 6 <= len(value) <= 20:
                        error_by_index[idx] = f"Keyword must be 6-20 characters (got {len(value)})"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    elif not re.fullmatch(r"[A-Za-z0-9_./+#&@()-]+", value):
                        error_by_index[idx] = "Keyword has unsupported characters"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    else:
                        # Keyword delete — must actually exist on the target SKU
                        kw_owner = keyword_to_sku.get(value)
                        if kw_owner is None:
                            error_by_index[idx] = f"Keyword '{value}' not found on any SKU"
                            errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        if kw_owner != sku:
                            error_by_index[idx] = f"Cannot delete, keyword belongs to a different SKU ({kw_owner})"
                            errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        classification = "delete_keyword"

                elif action_normalized == "Keyword":
                    if not 6 <= len(value) <= 20:
                        error_by_index[idx] = f"Keyword must be 6-20 characters (got {len(value)})"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if not re.fullmatch(r"[A-Za-z0-9_./+#&@()-]+", value):
                        error_by_index[idx] = "Keyword has unsupported characters"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    # A purely numeric keyword must NOT be a valid barcode
                    if value.isdigit() and len(value) in (8, 12, 13) and ProductService._is_valid_barcode(value):
                        error_by_index[idx] = "Keyword cannot be a valid barcode (has valid checksum)"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if value in upc_to_sku:
                        error_by_index[idx] = f"Keyword conflicts with existing UPC for SKU: {upc_to_sku[value]}"
                        errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                        continue

                    if value in keyword_to_sku:
                        owner = keyword_to_sku[value]
                        if owner == sku:
                            classification = "noop"
                        else:
                            classification = "swap_keyword"
                            source_sku = owner
                    else:
                        classification = "add_keyword"

                item = {"row": row_num, "sku": sku, "value": item_value, "action": action_normalized, "classification": classification}
                if source_sku:
                    item["source_sku"] = source_sku
                items.append(item)

            # ============================================================
            # Simulate final UPC state per SKU and validate primary invariants.
            #
            # Rules:
            # 1. Every SKU must have at least one UPC after the import.
            # 2. Every SKU must have at least one primary-capable UPC (not EAN-8).
            #    EAN-8 cannot be primary, so if only EAN-8 UPCs remain, there's no
            #    way to have a valid primary.
            # 3. If the current primary is moved away and no explicit new primary is
            #    assigned, a secondary will be auto-promoted by the DB trigger.
            #    Surface this to the UI as an `auto_promotions` warning.
            # Keywords are NOT UPCs — they never count toward these checks.
            # ============================================================

            # Identify all SKUs affected by the import (either as source or target of UPC ops)
            upc_affected_skus: set = set()
            rows_affecting_sku: Dict[str, List[int]] = {}  # sku -> list of dataframe idx

            for it in items:
                cls = it.get("classification") or ""
                idx = it["row"] - 2
                if cls in ("swap_primary", "swap_secondary"):
                    upc_affected_skus.add(it["sku"])
                    upc_affected_skus.add(it["source_sku"])
                    rows_affecting_sku.setdefault(it["source_sku"], []).append(idx)
                    rows_affecting_sku.setdefault(it["sku"], []).append(idx)
                elif cls in ("add_primary", "add_secondary", "promote_primary"):
                    upc_affected_skus.add(it["sku"])
                elif cls == "delete_upc":
                    upc_affected_skus.add(it["sku"])
                    rows_affecting_sku.setdefault(it["sku"], []).append(idx)

            # Build simulated UPC state per affected SKU
            simulated_upcs: Dict[str, Dict[str, Dict[str, Any]]] = {}
            original_primary_by_sku: Dict[str, str] = {}
            explicit_new_primary_by_sku: Dict[str, str] = {}

            if upc_affected_skus:
                current_rows = await conn.execute_query_dict(
                    "SELECT child_sku, upc, is_primary_upc FROM child_upcs WHERE child_sku = ANY($1)",
                    [list(upc_affected_skus)],
                )
                for r in current_rows:
                    sku = r["child_sku"]
                    upc = r["upc"]
                    simulated_upcs.setdefault(sku, {})[upc] = {
                        "is_ean8": len(upc) == 8,
                        "is_primary": r["is_primary_upc"],
                    }
                    if r["is_primary_upc"]:
                        original_primary_by_sku[sku] = upc

                # Apply CSV changes to the simulation
                for it in items:
                    cls = it.get("classification") or ""
                    sku = it["sku"]
                    val = it["value"]
                    src = it.get("source_sku")

                    if cls == "swap_primary":
                        if src and val in simulated_upcs.get(src, {}):
                            simulated_upcs[src].pop(val, None)
                        # Demote any existing primary on target
                        for other in simulated_upcs.get(sku, {}).values():
                            other["is_primary"] = False
                        simulated_upcs.setdefault(sku, {})[val] = {
                            "is_ean8": len(val) == 8,
                            "is_primary": True,
                        }
                        explicit_new_primary_by_sku[sku] = val
                    elif cls == "swap_secondary":
                        if src and val in simulated_upcs.get(src, {}):
                            simulated_upcs[src].pop(val, None)
                        simulated_upcs.setdefault(sku, {})[val] = {
                            "is_ean8": len(val) == 8,
                            "is_primary": False,
                        }
                    elif cls == "add_primary":
                        for other in simulated_upcs.get(sku, {}).values():
                            other["is_primary"] = False
                        simulated_upcs.setdefault(sku, {})[val] = {
                            "is_ean8": len(val) == 8,
                            "is_primary": True,
                        }
                        explicit_new_primary_by_sku[sku] = val
                    elif cls == "add_secondary":
                        simulated_upcs.setdefault(sku, {})[val] = {
                            "is_ean8": len(val) == 8,
                            "is_primary": False,
                        }
                    elif cls == "promote_primary":
                        for upc, meta in simulated_upcs.get(sku, {}).items():
                            meta["is_primary"] = (upc == val)
                        explicit_new_primary_by_sku[sku] = val
                    elif cls == "delete_upc":
                        simulated_upcs.get(sku, {}).pop(val, None)

            # Validate each affected SKU's final state
            stranded_no_upcs: List[str] = []
            stranded_only_ean8: List[str] = []
            auto_promotions: List[Dict[str, Any]] = []

            for sku in upc_affected_skus:
                upcs = simulated_upcs.get(sku, {})
                if not upcs:
                    stranded_no_upcs.append(sku)
                    continue

                primary_capable = [u for u, m in upcs.items() if not m["is_ean8"]]
                if not primary_capable:
                    stranded_only_ean8.append(sku)
                    continue

                # Detect auto-promotion: current primary moved away, no explicit new primary
                original_primary = original_primary_by_sku.get(sku)
                explicit_new = explicit_new_primary_by_sku.get(sku)
                original_still_here = original_primary and original_primary in upcs
                has_any_primary = any(m["is_primary"] for m in upcs.values())

                if original_primary and not original_still_here and not explicit_new and not has_any_primary:
                    # Trigger will auto-promote the oldest primary-capable UPC
                    auto_promotions.append({
                        "sku": sku,
                        "previous_primary": original_primary,
                        "candidates": primary_capable,  # one of these will become the new primary
                    })

            # Report errors for stranded SKUs
            for sku in stranded_no_upcs:
                msg = f"Import would remove the only UPC from {sku}"
                for idx in rows_affecting_sku.get(sku, []):
                    error_by_index[idx] = msg
                    row_num = idx + 2
                    offending = next(
                        (it for it in items
                         if it["row"] == row_num
                         and (it.get("source_sku") == sku or (it.get("classification") == "delete_upc" and it["sku"] == sku))),
                        None,
                    )
                    errors.append({
                        "row": row_num,
                        "sku": offending["sku"] if offending else None,
                        "value": offending["value"] if offending else None,
                        "field": "SKU",
                        "message": msg,
                    })

            for sku in stranded_only_ean8:
                msg = f"Import would leave {sku} with only EAN-8 UPCs, which cannot be primary"
                for idx in rows_affecting_sku.get(sku, []):
                    error_by_index[idx] = msg
                    row_num = idx + 2
                    offending = next(
                        (it for it in items
                         if it["row"] == row_num
                         and (it.get("source_sku") == sku or (it.get("classification") == "delete_upc" and it["sku"] == sku))),
                        None,
                    )
                    errors.append({
                        "row": row_num,
                        "sku": offending["sku"] if offending else None,
                        "value": offending["value"] if offending else None,
                        "field": "SKU",
                        "message": msg,
                    })

            # Drop invalid items so they don't propagate to processing
            stranded_set = set(stranded_no_upcs) | set(stranded_only_ean8)
            if stranded_set:
                items = [
                    it for it in items
                    if not (
                        (it.get("classification") in ("swap_primary", "swap_secondary")
                         and it.get("source_sku") in stranded_set)
                        or (it.get("classification") == "delete_upc"
                            and it["sku"] in stranded_set)
                    )
                ]

            # Per-row transfer records — surfaced to the user for EVERY swap so
            # they see exactly what's moving between SKUs. Also used to compute
            # per-SKU donor totals (SKUs losing more than they gain).
            transfers: List[Dict[str, Any]] = []
            sku_gains: Dict[str, int] = {}
            sku_loss_primary: Dict[str, int] = {}
            sku_loss_secondary: Dict[str, int] = {}
            for it in items:
                cls = it.get("classification") or ""
                target = it["sku"]
                src = it.get("source_sku")
                val = it["value"]

                if cls == "swap_keyword" and src:
                    transfers.append({
                        "row": it["row"],
                        "value_type": "Keyword",
                        "value": val,
                        "from_sku": src,
                        "from_role": None,
                        "to_sku": target,
                        "to_role": None,
                    })
                elif cls in ("swap_primary", "swap_secondary") and src:
                    was_primary_on_source = original_primary_by_sku.get(src) == val
                    from_role = "Primary" if was_primary_on_source else "Secondary"
                    to_role = "Primary" if cls == "swap_primary" else "Secondary"
                    transfers.append({
                        "row": it["row"],
                        "value_type": "UPC",
                        "value": val,
                        "from_sku": src,
                        "from_role": from_role,
                        "to_sku": target,
                        "to_role": to_role,
                    })
                    sku_gains[target] = sku_gains.get(target, 0) + 1
                    if was_primary_on_source:
                        sku_loss_primary[src] = sku_loss_primary.get(src, 0) + 1
                    else:
                        sku_loss_secondary[src] = sku_loss_secondary.get(src, 0) + 1
                elif cls in ("add_primary", "add_secondary"):
                    sku_gains[target] = sku_gains.get(target, 0) + 1
                elif cls == "delete_upc":
                    # delete_upc is always a secondary (primary delete is blocked)
                    sku_loss_secondary[target] = sku_loss_secondary.get(target, 0) + 1

            # Donor SKUs: net UPC loss (losses > gains). Included for back-compat;
            # UI primarily uses the per-transfer list above.
            donors = {}
            all_losing_skus = set(sku_loss_primary.keys()) | set(sku_loss_secondary.keys())
            for sku in all_losing_skus:
                lost_primary = sku_loss_primary.get(sku, 0)
                lost_secondary = sku_loss_secondary.get(sku, 0)
                total_losses = lost_primary + lost_secondary
                gains = sku_gains.get(sku, 0)
                if total_losses > gains:
                    donors[sku] = {
                        "losses": total_losses,
                        "gains": gains,
                        "lost_primary": lost_primary,
                        "lost_secondary": lost_secondary,
                    }

            file_data = None
            if errors:
                df["Error"] = df.index.map(lambda i: error_by_index.get(i, ""))
                csv_with_errors = df.to_csv(index=False)
                file_data = base64.b64encode(csv_with_errors.encode()).decode()

            # Collect noop items so the UI can surface them as a warning + downloadable list
            noops = [
                {"row": it["row"], "sku": it["sku"], "value": it["value"], "action": it["action"]}
                for it in items if it.get("classification") == "noop"
            ]

            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "items": items if not errors else [],
                "file_data": file_data,
                "donors": donors,
                "auto_promotions": auto_promotions,
                "noops": noops,
                "transfers": transfers,
            }

        except Exception as e:
            logger.error(f"Error validating bulk import: {e}", exc_info=True)
            return {"valid": False, "errors": [{"row": 0, "field": "file", "message": f"Unexpected error: {str(e)}"}], "items": [], "donors": {}, "auto_promotions": [], "noops": [], "transfers": []}

    @staticmethod
    async def process_bulk_import(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        from config import config

        results = []
        max_tracked = config.get("bulk_import", {}).get("max_tracked_items", 50)
        use_tracking = len(items) <= max_tracked

        try:
            conn = await ProductService._get_connection()

            # Group items by classification
            swap_items = [i for i in items if (i.get("classification") or "").startswith("swap_")]
            noop_items = [i for i in items if i.get("classification") == "noop"]
            delete_items = [i for i in items if i.get("classification", "").startswith("delete_")]
            other_items = [
                i for i in items
                if i not in swap_items and i not in noop_items and i not in delete_items
            ]

            # 1. Noops → instant success
            for item in noop_items:
                results.append({
                    "row": item["row"], "sku": item["sku"], "value": item["value"],
                    "action": item["action"], "classification": "noop", "success": True,
                })

            # 2. Swaps → synchronous SC+DB (always tracked regardless of size)
            for item in swap_items:
                result = await ProductService._bulk_process_swap(conn, item)
                results.append(result)

            # 3. Adds (including promote_primary) → existing handlers (DB first, enqueue)
            for item in other_items:
                if item["action"] == "Primary":
                    result = await ProductService._bulk_process_primary(conn, item)
                elif item["action"] == "Secondary":
                    result = await ProductService._bulk_process_secondary(conn, item)
                elif item["action"] == "Keyword":
                    result = await ProductService._bulk_process_keyword(conn, item)
                else:
                    result = {
                        "row": item["row"], "sku": item["sku"], "value": item["value"],
                        "action": item["action"], "classification": item.get("classification"),
                        "success": False, "error": f"Unknown action: {item['action']}",
                    }
                results.append(result)

            # 4. Deletes → existing handler (DB first, enqueue)
            for item in delete_items:
                result = await ProductService._bulk_process_delete(conn, item)
                results.append(result)

            successful = sum(1 for r in results if r["success"])
            return {
                "success": (len(results) - successful) == 0,
                "total_items": len(results),
                "successful_count": successful,
                "failed_count": len(results) - successful,
                "results": results,
            }

        except Exception as e:
            logger.error(f"Error processing bulk import: {e}", exc_info=True)
            raise

    @staticmethod
    async def _bulk_process_primary(conn, item: Dict) -> Dict:
        try:
            sku, upc = item["sku"], item["value"]

            # Capture current primary so we can decide between add_primary_upc and change_primary_upc
            current_primary_rows = await conn.execute_query_dict(
                "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE",
                [sku],
            )
            current_primary = current_primary_rows[0]["upc"] if current_primary_rows else None

            # Check if UPC already exists for this SKU
            existing = await conn.execute_query_dict(
                "SELECT upc, is_primary_upc FROM child_upcs WHERE upc = $1 AND child_sku = $2", [upc, sku]
            )

            if existing and existing[0]["is_primary_upc"]:
                return {"row": item["row"], "sku": sku, "value": upc, "action": "Primary", "success": True}

            if not existing:
                # Add the UPC first
                upc_exists_other = await conn.execute_query_dict("SELECT child_sku FROM child_upcs WHERE upc = $1", [upc])
                if upc_exists_other:
                    return {"row": item["row"], "sku": sku, "value": upc, "action": "Primary", "success": False, "error": f"UPC already exists for {upc_exists_other[0]['child_sku']}"}
                await conn.execute_query("INSERT INTO child_upcs (upc, child_sku) VALUES ($1, $2)", [upc, sku])

                # Check if it auto-became primary (no prior primary on this SKU)
                check = await conn.execute_query_dict("SELECT is_primary_upc FROM child_upcs WHERE upc = $1", [upc])
                if check and check[0]["is_primary_upc"]:
                    await sellercloud_sync_queue.enqueue(sku, upc, "add_primary_upc")
                    return {"row": item["row"], "sku": sku, "value": upc, "action": "Primary", "success": True}

            # Set as primary using DB function
            result = await conn.execute_query_dict("SELECT update_primary_upc_for_child($1, $2) as result", [sku, upc])
            db_result = json.loads(result[0]["result"]) if isinstance(result[0]["result"], str) else result[0]["result"]

            success = db_result.get("success", False)
            if success:
                if current_primary and current_primary != upc:
                    await sellercloud_sync_queue.enqueue(
                        sku, upc, "change_primary_upc", old_primary_upc=current_primary
                    )
                else:
                    await sellercloud_sync_queue.enqueue(sku, upc, "add_primary_upc")

            return {"row": item["row"], "sku": sku, "value": upc, "action": "Primary", "success": success, "error": db_result.get("error")}
        except Exception as e:
            return {"row": item["row"], "sku": item["sku"], "value": item["value"], "action": "Primary", "success": False, "error": str(e)}

    @staticmethod
    async def _bulk_process_secondary(conn, item: Dict) -> Dict:
        try:
            sku, upc = item["sku"], item["value"]
            existing = await conn.execute_query_dict(
                "SELECT upc FROM child_upcs WHERE upc = $1 AND child_sku = $2", [upc, sku]
            )
            if existing:
                return {"row": item["row"], "sku": sku, "value": upc, "action": "Secondary", "success": True}

            upc_exists_other = await conn.execute_query_dict("SELECT child_sku FROM child_upcs WHERE upc = $1", [upc])
            if upc_exists_other:
                return {"row": item["row"], "sku": sku, "value": upc, "action": "Secondary", "success": False, "error": f"UPC already exists for {upc_exists_other[0]['child_sku']}"}

            await conn.execute_query("INSERT INTO child_upcs (upc, child_sku) VALUES ($1, $2)", [upc, sku])

            # If no prior primary existed, the new UPC may have auto-become primary
            check = await conn.execute_query_dict(
                "SELECT is_primary_upc FROM child_upcs WHERE upc = $1", [upc]
            )
            became_primary = bool(check and check[0]["is_primary_upc"])
            sync_type = "add_primary_upc" if became_primary else "add_secondary_upc"
            await sellercloud_sync_queue.enqueue(sku, upc, sync_type)

            return {"row": item["row"], "sku": sku, "value": upc, "action": "Secondary", "success": True}
        except Exception as e:
            return {"row": item["row"], "sku": item["sku"], "value": item["value"], "action": "Secondary", "success": False, "error": str(e)}

    @staticmethod
    async def _bulk_process_keyword(conn, item: Dict) -> Dict:
        try:
            sku, keyword = item["sku"], item["value"]
            clean_keyword = keyword.strip()

            # Check if already exists
            check = await conn.execute_query_dict(
                "SELECT sku FROM child_products WHERE sku = $1 AND $2 = ANY(keywords)", [sku, clean_keyword]
            )
            if check:
                return {"row": item["row"], "sku": sku, "value": clean_keyword, "action": "Keyword", "success": True}

            # Validate
            vr = await conn.execute_query_dict("SELECT validate_keyword($1, $2) as result", [clean_keyword, sku])
            validation = json.loads(vr[0]["result"]) if isinstance(vr[0]["result"], str) else vr[0]["result"]
            if not validation.get("valid"):
                return {"row": item["row"], "sku": sku, "value": clean_keyword, "action": "Keyword", "success": False, "error": validation.get("error")}

            await conn.execute_query(
                "UPDATE child_products SET keywords = array_append(COALESCE(keywords, '{}'), $1), updated_at = CURRENT_TIMESTAMP WHERE sku = $2",
                [clean_keyword, sku],
            )
            await sellercloud_sync_queue.enqueue(sku, clean_keyword, "add_keyword")
            return {"row": item["row"], "sku": sku, "value": clean_keyword, "action": "Keyword", "success": True}
        except Exception as e:
            return {"row": item["row"], "sku": item["sku"], "value": item["value"], "action": "Keyword", "success": False, "error": str(e)}

    @staticmethod
    async def _bulk_process_delete(conn, item: Dict) -> Dict:
        try:
            sku, value = item["sku"], item["value"]

            # Check if it's a UPC
            upc_check = await conn.execute_query_dict(
                "SELECT upc, is_primary_upc FROM child_upcs WHERE upc = $1 AND child_sku = $2", [value, sku]
            )
            if upc_check:
                if upc_check[0]["is_primary_upc"]:
                    return {"row": item["row"], "sku": sku, "value": value, "action": "Delete", "success": False, "error": "Cannot delete primary UPC"}
                await conn.execute_query("DELETE FROM child_upcs WHERE upc = $1", [value])
                await sellercloud_sync_queue.enqueue(sku, value, "delete_upc")
                return {"row": item["row"], "sku": sku, "value": value, "action": "Delete", "success": True}

            # Check if it's a keyword
            kw_check = await conn.execute_query_dict(
                "SELECT sku FROM child_products WHERE sku = $1 AND $2 = ANY(keywords)", [sku, value]
            )
            if kw_check:
                await conn.execute_query(
                    "UPDATE child_products SET keywords = array_remove(keywords, $1) WHERE sku = $2", [value, sku]
                )
                await sellercloud_sync_queue.enqueue(sku, value, "delete_keyword")
                return {"row": item["row"], "sku": sku, "value": value, "action": "Delete", "success": True}

            return {"row": item["row"], "sku": sku, "value": value, "action": "Delete", "success": False, "error": f"UPC/Keyword '{value}' not found for SKU '{sku}'"}
        except Exception as e:
            return {"row": item["row"], "sku": item["sku"], "value": item["value"], "action": "Delete", "success": False, "error": str(e)}

    @staticmethod
    async def _bulk_process_swap(conn, item: Dict) -> Dict:
        """
        DB-first swap: DB transaction commits first (source of truth). Then mirror to
        SellerCloud with tracked but non-raising failures. Result is success=True if
        DB succeeds, even if SC steps fail — operation_id lets devs trace SC divergence.
        """
        from services import sellercloud_sync_logger
        from tortoise.transactions import in_transaction

        classification = item.get("classification", "")
        target_sku = item["sku"]
        value = item["value"]
        action = item["action"]

        def _result(success, error=None, operation_id=None):
            return {
                "row": item["row"], "sku": target_sku, "value": value,
                "action": action, "classification": classification,
                "success": success, "error": error, "operation_id": operation_id,
            }

        # ====================================================================
        # KEYWORD SWAP
        # ====================================================================
        if classification == "swap_keyword":
            # Find source from DB
            kw_check = await conn.execute_query_dict(
                "SELECT sku FROM (SELECT unnest(keywords) AS kw, sku FROM child_products) sub WHERE kw = $1",
                [value],
            )
            if not kw_check:
                return _result(False, f"Keyword '{value}' not found on any SKU")
            source_sku = kw_check[0]["sku"]
            if source_sku == target_sku:
                return _result(True)  # already on target — noop

            # === DB FIRST (source of truth) ===
            try:
                async with in_transaction("product_db") as txn:
                    await txn.execute_query(
                        "UPDATE child_products SET keywords = array_remove(keywords, $1), updated_at = CURRENT_TIMESTAMP WHERE sku = $2",
                        [value, source_sku],
                    )
                    await txn.execute_query(
                        "UPDATE child_products SET keywords = array_append(COALESCE(keywords, '{}'), $1), updated_at = CURRENT_TIMESTAMP WHERE sku = $2",
                        [value, target_sku],
                    )
            except Exception as e:
                logger.error(f"DB swap failed for keyword {value} {source_sku}->{target_sku}: {e}", exc_info=True)
                return _result(False, f"DB swap failed: {e}")

            # === SC MIRROR (tracked, non-blocking) ===
            op_id = None
            async with sellercloud_sync_logger.tracked_operation(
                "swap_keyword", target_sku, value,
                source="bulk_import", source_sku=source_sku,
            ) as tracker:
                op_id = tracker.operation_id
                await ProductService._sync_keyword_swap_to_sc(tracker, source_sku, target_sku, value)

            return _result(True, operation_id=op_id)

        # ====================================================================
        # UPC SWAP (swap_primary or swap_secondary)
        # ====================================================================
        upc_check = await conn.execute_query_dict(
            "SELECT child_sku, is_primary_upc FROM child_upcs WHERE upc = $1", [value]
        )
        if not upc_check:
            return _result(False, f"UPC '{value}' not found on any SKU")
        source_sku = upc_check[0]["child_sku"]
        was_primary_on_source = upc_check[0]["is_primary_upc"]
        if source_sku == target_sku:
            return _result(True)  # already on target — noop

        make_primary = classification == "swap_primary"

        # Capture target's current primary BEFORE swap (for SC demotion mirroring)
        target_primary_rows = await conn.execute_query_dict(
            "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE", [target_sku]
        )
        target_current_primary = target_primary_rows[0]["upc"] if target_primary_rows else None

        # === DB FIRST (source of truth) ===
        # DB triggers handle invariants:
        #   trg_child_upcs_after_delete  → auto-promotes oldest remaining UPC on source if primary was deleted
        #   trg_child_upcs_before_insert → auto-demotes existing primary on target when new primary inserted
        try:
            async with in_transaction("product_db") as txn:
                await txn.execute_query("DELETE FROM child_upcs WHERE upc = $1", [value])
                if make_primary:
                    await txn.execute_query(
                        "INSERT INTO child_upcs (upc, child_sku, is_primary_upc) VALUES ($1, $2, TRUE)",
                        [value, target_sku],
                    )
                else:
                    await txn.execute_query(
                        "INSERT INTO child_upcs (upc, child_sku) VALUES ($1, $2)",
                        [value, target_sku],
                    )
        except Exception as e:
            logger.error(f"DB swap failed for UPC {value} {source_sku}->{target_sku}: {e}", exc_info=True)
            return _result(False, f"DB swap failed: {e}")

        # Query post-DB state to find the new primary on source (promoted by trigger)
        new_source_primary = None
        if was_primary_on_source:
            remaining = await conn.execute_query_dict(
                "SELECT upc FROM child_upcs WHERE child_sku = $1 AND is_primary_upc = TRUE", [source_sku]
            )
            new_source_primary = remaining[0]["upc"] if remaining else None

        # === SC MIRROR (tracked, non-blocking) ===
        op_id = None
        async with sellercloud_sync_logger.tracked_operation(
            classification, target_sku, value,
            source="bulk_import", source_sku=source_sku,
            metadata={
                "was_primary_on_source": was_primary_on_source,
                "target_current_primary": target_current_primary,
                "make_primary": make_primary,
                "new_source_primary": new_source_primary,
            },
        ) as tracker:
            op_id = tracker.operation_id
            await ProductService._sync_upc_swap_to_sc(
                tracker,
                source_sku=source_sku,
                target_sku=target_sku,
                value=value,
                was_primary_on_source=was_primary_on_source,
                make_primary=make_primary,
                target_current_primary=target_current_primary,
                new_source_primary=new_source_primary,
            )

        return _result(True, operation_id=op_id)

    @staticmethod
    async def _sync_keyword_swap_to_sc(tracker, source_sku, target_sku, value):
        """Mirror a completed DB keyword swap to SellerCloud. Never raises."""
        # Load source aliases
        try:
            source_aliases_resp = await sellercloud_internal_service.load_aliases(source_sku)
            source_dto = (source_aliases_resp.get("Data") or {}).get("DTO") or {}
            source_alias_set = {a.get("Name") for a in (source_dto.get("Aliases") or []) if a.get("Name")}
        except Exception as e:
            await tracker.record_failure(source_sku, value, "load_aliases", str(e), "Load source aliases")
            source_alias_set = set()

        # Delete keyword from source SC aliases (check first)
        if value in source_alias_set:
            try:
                del_result = await sellercloud_internal_service.save_alias(source_sku, value, action="delete")
                if not del_result.get("Success"):
                    msg = (del_result.get("Notification") or {}).get("Message", "") or ""
                    if "not found" in msg.lower() or "does not exist" in msg.lower():
                        await tracker.record_step(source_sku, value, "delete_alias", "Keyword already absent from source aliases")
                    else:
                        await tracker.record_failure(source_sku, value, "delete_alias", msg, "Delete keyword alias from source")
                        logger.error(f"SC delete keyword {value} from {source_sku}: {msg}")
                else:
                    await tracker.record_step(source_sku, value, "delete_alias", "Deleted keyword alias from source")
            except Exception as e:
                await tracker.record_failure(source_sku, value, "delete_alias", str(e), "Delete keyword alias from source")
                logger.error(f"SC delete keyword {value} from {source_sku}: {e}")
        else:
            await tracker.record_skip(source_sku, value, "delete_alias", "Keyword not in source alias list")

        # Validate alias on target
        try:
            validation = await sellercloud_internal_service.validate_alias(target_sku, value)
            if not validation.get("IsValid"):
                already = validation.get("AlreadyUsedForProduct")
                error_msg = f"Keyword {value} already used by product (ID: {already})" if already else (
                    validation.get("ErrorMessage") or (validation.get("Notification") or {}).get("Message", "") or f"Keyword {value} failed validation"
                )
                await tracker.record_failure(target_sku, value, "validate_alias", error_msg, "Validate keyword alias on target")
                logger.error(f"SC validate keyword {value} on {target_sku}: {error_msg}")
                return  # can't add alias if validation failed
            await tracker.record_step(target_sku, value, "validate_alias", "Validated keyword alias on target")
        except Exception as e:
            await tracker.record_failure(target_sku, value, "validate_alias", str(e), "Validate keyword alias on target")
            logger.error(f"SC validate keyword {value} on {target_sku}: {e}")
            return

        # Add keyword alias to target
        try:
            save_result = await sellercloud_internal_service.save_alias(target_sku, value, action="add")
            if not save_result.get("Success"):
                msg = (save_result.get("Notification") or {}).get("Message", "") or ""
                await tracker.record_failure(target_sku, value, "add_alias", msg or "Save alias failed", "Add keyword alias to target")
                logger.error(f"SC add keyword {value} to {target_sku}: {msg}")
            else:
                await tracker.record_step(target_sku, value, "add_alias", "Added keyword alias to target")
        except Exception as e:
            await tracker.record_failure(target_sku, value, "add_alias", str(e), "Add keyword alias to target")
            logger.error(f"SC add keyword {value} to {target_sku}: {e}")

    @staticmethod
    async def _sync_upc_swap_to_sc(
        tracker,
        source_sku: str,
        target_sku: str,
        value: str,
        was_primary_on_source: bool,
        make_primary: bool,
        target_current_primary: Optional[str],
        new_source_primary: Optional[str],
    ):
        """Mirror a completed DB UPC swap to SellerCloud. Never raises — logs SC failures."""
        # Load source aliases
        try:
            source_aliases_resp = await sellercloud_internal_service.load_aliases(source_sku)
            source_dto = (source_aliases_resp.get("Data") or {}).get("DTO") or {}
            source_alias_set = {a.get("Name") for a in (source_dto.get("Aliases") or []) if a.get("Name")}
        except Exception as e:
            await tracker.record_failure(source_sku, value, "load_aliases", str(e), "Load source aliases")
            source_alias_set = set()

        # ==============================================================
        # SOURCE-SIDE SC MIRRORING (only if source primary changed)
        # ==============================================================
        if was_primary_on_source:
            if new_source_primary:
                # Remove new primary from source aliases (was secondary, now primary)
                if new_source_primary in source_alias_set:
                    try:
                        del_result = await sellercloud_internal_service.save_alias(source_sku, new_source_primary, action="delete")
                        if not del_result.get("Success"):
                            msg = (del_result.get("Notification") or {}).get("Message", "") or ""
                            if "not found" in msg.lower() or "does not exist" in msg.lower():
                                await tracker.record_step(source_sku, new_source_primary, "delete_alias", "New source primary already absent from aliases")
                            else:
                                await tracker.record_failure(source_sku, new_source_primary, "delete_alias", msg, "Remove new source primary from aliases")
                                logger.error(f"SC remove new primary {new_source_primary} from {source_sku} aliases: {msg}")
                        else:
                            await tracker.record_step(source_sku, new_source_primary, "delete_alias", "Removed new source primary from aliases (promoting to primary)")
                    except Exception as e:
                        await tracker.record_failure(source_sku, new_source_primary, "delete_alias", str(e), "Remove new source primary from aliases")
                        logger.error(f"SC remove new primary {new_source_primary} from {source_sku} aliases: {e}")
                else:
                    await tracker.record_skip(source_sku, new_source_primary, "delete_alias", "New source primary not in aliases, skip delete")

                # Set new primary in BasicInfo on source
                try:
                    result = await sellercloud_service.update_product_upc(source_sku, new_source_primary)
                    if not result.get("success"):
                        error_msg = f"Failed to set primary UPC on {source_sku} to {new_source_primary}: {result}"
                        await tracker.record_failure(source_sku, new_source_primary, "set_primary_upc", error_msg, "Set new primary on source")
                        logger.error(error_msg)
                    else:
                        await tracker.record_step(source_sku, new_source_primary, "set_primary_upc", "Set new primary UPC on source")
                except Exception as e:
                    await tracker.record_failure(source_sku, new_source_primary, "set_primary_upc", str(e), "Set new primary on source")
                    logger.error(f"SC set primary {new_source_primary} on {source_sku}: {e}")
            else:
                # No UPCs remaining on source — clear BasicInfo
                try:
                    result = await sellercloud_service.update_product_upc(source_sku, "")
                    if not result.get("success"):
                        error_msg = f"Failed to clear primary UPC on {source_sku}: {result}"
                        await tracker.record_failure(source_sku, value, "clear_primary_upc", error_msg, "Clear primary UPC on source (no UPCs remaining)")
                        logger.error(error_msg)
                    else:
                        await tracker.record_step(source_sku, value, "clear_primary_upc", "Cleared primary UPC on source (no UPCs remaining)")
                except Exception as e:
                    await tracker.record_failure(source_sku, value, "clear_primary_upc", str(e), "Clear primary UPC on source")
                    logger.error(f"SC clear primary on {source_sku}: {e}")

        # Delete X alias from source (if present)
        if value in source_alias_set:
            try:
                del_result = await sellercloud_internal_service.save_alias(source_sku, value, action="delete")
                if not del_result.get("Success"):
                    msg = (del_result.get("Notification") or {}).get("Message", "") or ""
                    if "not found" in msg.lower() or "does not exist" in msg.lower():
                        await tracker.record_step(source_sku, value, "delete_alias", "UPC already absent from source aliases")
                    else:
                        await tracker.record_failure(source_sku, value, "delete_alias", msg, "Delete UPC alias from source")
                        logger.error(f"SC delete {value} from {source_sku} aliases: {msg}")
                else:
                    await tracker.record_step(source_sku, value, "delete_alias", "Deleted UPC alias from source")
            except Exception as e:
                await tracker.record_failure(source_sku, value, "delete_alias", str(e), "Delete UPC alias from source")
                logger.error(f"SC delete {value} from {source_sku} aliases: {e}")
        else:
            await tracker.record_skip(source_sku, value, "delete_alias", "UPC not in source alias list (only in BasicInfo)")

        # ==============================================================
        # TARGET-SIDE SC MIRRORING
        # When make_primary: demote Z to alias (secondary UPCs go in aliases), set X as BasicInfo
        # When secondary: just add X as alias
        # ==============================================================
        if make_primary and target_current_primary:
            # Demote Z: add to target aliases (Z is now secondary in DB, needs to be alias in SC)
            try:
                target_aliases_resp = await sellercloud_internal_service.load_aliases(target_sku)
                target_dto = (target_aliases_resp.get("Data") or {}).get("DTO") or {}
                target_alias_set = {a.get("Name") for a in (target_dto.get("Aliases") or []) if a.get("Name")}
            except Exception as e:
                await tracker.record_failure(target_sku, target_current_primary, "load_aliases", str(e), "Load target aliases for demotion")
                target_alias_set = set()

            if target_current_primary in target_alias_set:
                await tracker.record_skip(target_sku, target_current_primary, "demote_primary_upc", "Previous primary already in target aliases")
            else:
                try:
                    demote_result = await sellercloud_internal_service.save_alias(target_sku, target_current_primary, action="add")
                    if not demote_result.get("Success"):
                        msg = (demote_result.get("Notification") or {}).get("Message", "") or ""
                        await tracker.record_failure(target_sku, target_current_primary, "demote_primary_upc", msg, "Demote previous primary on target (add to aliases)")
                        logger.error(f"SC demote {target_current_primary} on {target_sku}: {msg}")
                    else:
                        await tracker.record_step(target_sku, target_current_primary, "demote_primary_upc", "Demoted previous primary on target (added to aliases)")
                except Exception as e:
                    await tracker.record_failure(target_sku, target_current_primary, "demote_primary_upc", str(e), "Demote previous primary on target")
                    logger.error(f"SC demote {target_current_primary} on {target_sku}: {e}")

        # Validate X on target
        try:
            validation = await sellercloud_internal_service.validate_alias(target_sku, value)
            if not validation.get("IsValid"):
                already = validation.get("AlreadyUsedForProduct")
                if already:
                    error_msg = f"UPC {value} is already used by another product (ID: {already})"
                else:
                    error_msg = validation.get("ErrorMessage") or (validation.get("Notification") or {}).get("Message", "") or f"UPC {value} failed validation"
                await tracker.record_failure(target_sku, value, "validate_alias", error_msg, "Validate UPC on target")
                logger.error(f"SC validate {value} on {target_sku}: {error_msg}")
                return  # can't proceed if validation failed
            await tracker.record_step(target_sku, value, "validate_alias", "Validated UPC on target")
        except Exception as e:
            await tracker.record_failure(target_sku, value, "validate_alias", str(e), "Validate UPC on target")
            logger.error(f"SC validate {value} on {target_sku}: {e}")
            return

        if make_primary:
            # Set X as BasicInfo on target (primary UPCs not in aliases)
            try:
                primary_result = await sellercloud_service.update_product_upc(target_sku, value)
                if not primary_result.get("success"):
                    error_msg = f"Failed to set primary UPC on {target_sku} to {value}: {primary_result}"
                    await tracker.record_failure(target_sku, value, "set_primary_upc", error_msg, "Set primary UPC on target")
                    logger.error(error_msg)
                else:
                    await tracker.record_step(target_sku, value, "set_primary_upc", "Set primary UPC on target")
            except Exception as e:
                await tracker.record_failure(target_sku, value, "set_primary_upc", str(e), "Set primary UPC on target")
                logger.error(f"SC set primary {value} on {target_sku}: {e}")
        else:
            # Secondary: add to target aliases
            try:
                save_result = await sellercloud_internal_service.save_alias(target_sku, value, action="add")
                if not save_result.get("Success"):
                    msg = (save_result.get("Notification") or {}).get("Message", "") or ""
                    await tracker.record_failure(target_sku, value, "add_alias", msg or "Save alias failed", "Add secondary UPC alias to target")
                    logger.error(f"SC add {value} to {target_sku} aliases: {msg}")
                else:
                    await tracker.record_step(target_sku, value, "add_alias", "Added secondary UPC alias to target")
            except Exception as e:
                await tracker.record_failure(target_sku, value, "add_alias", str(e), "Add secondary UPC alias to target")
                logger.error(f"SC add {value} to {target_sku} aliases: {e}")
