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

JOB_HANDLERS = {
    "TRANSFER_INVENTORY_SC": "_execute_transfer_job",
    "TRANSFER_UPCS_KEYWORDS_SC": "_execute_transfer_upcs_keywords_job",
    "DISABLE_PRODUCT_SC": "_execute_disable_job",
}


class ProductService:

    @staticmethod
    async def _get_connection():
        return connections.get("product_db")

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
        if not target_result:
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
            "to_child": {"sku": target_child_sku, "size": target_result[0].get("size")},
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
    async def add_size_to_parent(
        parent_sku: str,
        size: str,
        upc: str,
        cost_price: float,
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

            existing_upc = await conn.execute_query_dict(
                "SELECT child_sku FROM child_upcs WHERE upc = $1",
                [upc],
            )
            if existing_upc:
                return {
                    "success": False,
                    "error": f"UPC '{upc}' is already assigned to '{existing_upc[0]['child_sku']}'",
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

            template_data = await sellercloud_service.get_product_by_id(
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

            try:
                await sellercloud_service.create_product(
                    product_sku=new_child_sku,
                    product_name=product_name,
                    company_id=parent["company_code"],
                    site_cost=cost_price,
                    product_type_name=product_type_name,
                    brand_name=brand_name,
                    upc=upc,
                )
                logger.info(f"Created product {new_child_sku} on SellerCloud")
            except Exception as e:
                logger.error(
                    f"Failed to create product {new_child_sku} on SellerCloud: {traceback.format_exc()}"
                )
                return {"success": False, "error": "Failed to create product on SellerCloud"}

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

            sku_data = {
                new_child_sku: {
                    "title": parent["title"],
                    "upc": upc,
                }
            }
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

                if db_result and db_result[0].get("result"):
                    result_data = db_result[0]["result"]
                    if isinstance(result_data, str):
                        result_data = orjson.loads(result_data)
                    if not result_data.get("success"):
                        errors = result_data.get("errors", [])
                        logger.error(f"add_skus failed for {new_child_sku}: {errors}")
                        return {
                            "success": False,
                            "sellercloud_created": True,
                            "new_child_sku": new_child_sku,
                            "error": "Failed to add size",
                        }
            except Exception as e:
                logger.error(f"Failed to add {new_child_sku} to local DB: {traceback.format_exc()}")
                return {
                    "success": False,
                    "sellercloud_created": True,
                    "new_child_sku": new_child_sku,
                    "error": "Internal server error",
                }

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

            upc_result = await conn.execute_query_dict(
                "SELECT COALESCE(MAX(CAST(LEFT(upc, 12) AS BIGINT)), 777770000000) as max_base FROM child_upcs WHERE upc LIKE '77777%'"
            )
            next_base = upc_result[0]["max_base"] + 1
            base_str = str(next_base).zfill(12)

            digits = [int(d) for d in base_str]
            checksum = sum(digits[i] * (1 if i % 2 == 0 else 3) for i in range(12))
            check_digit = (10 - checksum % 10) % 10
            next_upc = base_str + str(check_digit)

            collision = await conn.execute_query_dict(
                "SELECT 1 FROM child_upcs WHERE upc = $1", [next_upc]
            )
            if collision:
                return {"success": False, "error": "UPC collision — please retry"}

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

            template_data = await sellercloud_service.get_product_by_id(
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

            try:
                await sellercloud_service.create_product(
                    product_sku=new_child_sku,
                    product_name=product_name,
                    company_id=parent["company_code"],
                    site_cost=site_cost,
                    product_type_name=product_type_name,
                    brand_name=brand_name,
                    upc=next_upc,
                )
                logger.info(
                    f"Created placeholder product {new_child_sku} on SellerCloud with UPC {next_upc}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to create placeholder {new_child_sku} on SellerCloud: {traceback.format_exc()}"
                )
                return {"success": False, "error": "Failed to create product on SellerCloud"}

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

            sku_data = {
                new_child_sku: {
                    "title": parent["title"],
                    "upc": next_upc,
                }
            }
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

                if db_result and db_result[0].get("result"):
                    result_data = db_result[0]["result"]
                    if isinstance(result_data, str):
                        result_data = orjson.loads(result_data)
                    if not result_data.get("success"):
                        errors = result_data.get("errors", [])
                        logger.error(f"add_skus failed for placeholder {new_child_sku}: {errors}")
                        return {
                            "success": False,
                            "sellercloud_created": True,
                            "new_child_sku": new_child_sku,
                            "error": "Failed to add size",
                        }
            except Exception as e:
                logger.error(
                    f"Failed to add placeholder {new_child_sku} to local DB: {traceback.format_exc()}"
                )
                return {
                    "success": False,
                    "sellercloud_created": True,
                    "new_child_sku": new_child_sku,
                    "error": "Internal server error",
                }

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
    async def reassign_child_parent(
        child_sku: str, new_parent_sku: str, target_child_sku: str, created_by: Optional[str] = None
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
                    created_by
                ) VALUES ($1, $2, $3, FALSE, $4, $5)
                RETURNING id
                """,
                [child_sku, old_parent_sku, new_parent_sku, target_child_sku, created_by],
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
                return {
                    "success": True,
                    "assignment_id": assignment_id,
                    "child_sku": child_sku,
                    "old_parent_sku": old_parent_sku,
                    "new_parent_sku": new_parent_sku,
                    "target_child_sku": target_child_sku,
                    "jobs_executed": len(executed_jobs),
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
            results = []

            search_term = query.strip()
            search_lower = search_term.lower()
            search_prefix = f"{search_term}%"
            search_lower_prefix = f"{search_lower}%"
            search_lower_contains = f"%{search_lower}%"

            if is_parent is None or is_parent is True:
                parents = await conn.execute_query_dict(
                    """
                    SELECT
                        pp.sku,
                        pp.title,
                        pp.mpn,
                        pp.brand,
                        (SELECT COUNT(*) FROM child_products cp WHERE cp.parent_sku = pp.sku AND cp.is_active = TRUE) as child_count
                    FROM parent_products pp
                    WHERE pp.is_active = TRUE AND (
                        LOWER(pp.sku) = $1
                        OR LOWER(pp.mpn) = $1
                        OR LOWER(pp.sku) LIKE $2
                        OR LOWER(pp.mpn) LIKE $2
                        OR LOWER(pp.title) LIKE $2
                        OR LOWER(pp.title) LIKE $3
                    )
                    ORDER BY
                        CASE
                            WHEN LOWER(pp.sku) = $1 THEN 0
                            WHEN LOWER(pp.mpn) = $1 THEN 1
                            WHEN LOWER(pp.sku) LIKE $2 THEN 2
                            ELSE 3
                        END,
                        pp.sku
                    LIMIT $4
                    """,
                    [search_lower, search_lower_prefix, search_lower_contains, limit],
                )

                for p in parents:
                    results.append(
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
                        }
                    )

            if is_parent is None or is_parent is False:
                children = await conn.execute_query_dict(
                    """
                    SELECT
                        cp.sku,
                        cp.size,
                        cp.is_primary,
                        cp.parent_sku,
                        cp.keywords,
                        pp.title,
                        pp.mpn,
                        pp.brand,
                        (SELECT upc FROM child_upcs WHERE child_sku = cp.sku AND is_primary_upc = TRUE LIMIT 1) as upc
                    FROM child_products cp
                    LEFT JOIN parent_products pp ON cp.parent_sku = pp.sku
                    WHERE cp.is_active = TRUE AND (
                        -- Exact UPC match (any UPC, primary or secondary)
                        EXISTS (SELECT 1 FROM child_upcs WHERE child_sku = cp.sku AND upc = $1)
                        -- Exact keyword match
                        OR $1 = ANY(cp.keywords)
                        -- SKU/MPN exact matches
                        OR LOWER(cp.sku) = $2
                        OR LOWER(pp.mpn) = $2
                        -- SKU/MPN prefix matches
                        OR LOWER(cp.sku) LIKE $3
                        OR LOWER(pp.mpn) LIKE $3
                        -- UPC prefix match (any UPC)
                        OR EXISTS (SELECT 1 FROM child_upcs WHERE child_sku = cp.sku AND upc LIKE $4)
                        -- Title matches
                        OR LOWER(pp.title) LIKE $3
                        OR LOWER(pp.title) LIKE $5
                        -- Keyword prefix match
                        OR EXISTS (SELECT 1 FROM unnest(cp.keywords) k WHERE k LIKE $4)
                    )
                    ORDER BY
                        CASE
                            WHEN EXISTS (SELECT 1 FROM child_upcs WHERE child_sku = cp.sku AND upc = $1) THEN 0
                            WHEN $1 = ANY(cp.keywords) THEN 0
                            WHEN LOWER(cp.sku) = $2 THEN 1
                            WHEN LOWER(pp.mpn) = $2 THEN 2
                            WHEN LOWER(cp.sku) LIKE $3 THEN 3
                            ELSE 4
                        END,
                        cp.sku
                    LIMIT $6
                    """,
                    [
                        search_term,
                        search_lower,
                        search_lower_prefix,
                        search_prefix,
                        search_lower_contains,
                        limit,
                    ],
                )

                for c in children:
                    results.append(
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
                        }
                    )

            exact_match = False
            if results:
                first = results[0]
                first_sku_lower = first["sku"].lower()
                first_mpn = first.get("mpn")
                first_keywords = first.get("_keywords", [])

                exact_match = (
                    first_sku_lower == search_lower
                    or (first_mpn and first_mpn.lower() == search_lower)
                    or search_term in first_keywords
                )

                if not exact_match and not first["is_parent"]:
                    upc_check = await conn.execute_query_dict(
                        "SELECT 1 FROM child_upcs WHERE child_sku = $1 AND upc = $2 LIMIT 1",
                        [first["sku"], search_term],
                    )
                    exact_match = len(upc_check) > 0

            for r in results:
                r.pop("_keywords", None)

            return {"results": results[:limit], "total": len(results), "exact_match": exact_match}

        except Exception as e:
            logger.error(f"Error searching products with query '{query}': {e}")
            return {"results": [], "total": 0, "exact_match": False}

    @staticmethod
    async def get_product_details(sku: str) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            type_result = await conn.execute_query_dict(
                """SELECT CASE WHEN sku = $1 THEN TRUE ELSE FALSE END as is_child
                   FROM child_products
                   WHERE (sku = $1 OR parent_sku = $1) AND is_active = TRUE
                   LIMIT 1""",
                [sku],
            )

            if not type_result:
                return {
                    "success": False,
                    "sku": sku,
                    "is_parent": None,
                    "error": "Product not found",
                }

            is_child = type_result[0]["is_child"]

            if is_child:
                child_result = await conn.execute_query_dict(
                    """
                    SELECT
                        cp.sku,
                        cp.size,
                        cp.is_primary,
                        cp.parent_sku,
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
                    FROM child_products cp
                    LEFT JOIN parent_products pp ON cp.parent_sku = pp.sku
                    WHERE cp.sku = $1 AND cp.is_active = TRUE
                    """,
                    [sku],
                )

                if not child_result:
                    return {
                        "success": False,
                        "sku": sku,
                        "is_parent": False,
                        "error": "Child product not found",
                    }

                child = child_result[0]

                upcs_result = await conn.execute_query_dict(
                    """
                    SELECT upc, is_primary_upc, upc_type
                    FROM child_upcs
                    WHERE child_sku = $1
                    """,
                    [sku],
                )

                primary_upc = None
                all_upcs = []
                for u in upcs_result:
                    all_upcs.append(
                        {
                            "upc": u["upc"],
                            "is_primary_upc": u["is_primary_upc"],
                            "upc_type": u.get("upc_type"),
                        }
                    )
                    if u["is_primary_upc"]:
                        primary_upc = u["upc"]

                keywords_result = await conn.execute_query_dict(
                    "SELECT keywords FROM child_products WHERE sku = $1",
                    [sku],
                )
                keywords = (
                    keywords_result[0]["keywords"]
                    if keywords_result and keywords_result[0]["keywords"]
                    else []
                )

                return {
                    "success": True,
                    "sku": sku,
                    "is_parent": False,
                    "title": child.get("title"),
                    "mpn": child.get("mpn"),
                    "brand": child.get("brand"),
                    "type_code": child.get("type_code"),
                    "serial_number": child.get("serial_number"),
                    "company_code": child.get("company_code"),
                    "product_type": child.get("product_type"),
                    "sizing_scheme": child.get("sizing_scheme"),
                    "style_name": child.get("style_name"),
                    "brand_color": child.get("brand_color"),
                    "color": child.get("color"),
                    "size": child.get("size"),
                    "is_primary": child.get("is_primary"),
                    "parent_sku": child.get("parent_sku"),
                    "primary_upc": primary_upc,
                    "all_upcs": all_upcs,
                    "keywords": keywords,
                    "child_count": None,
                    "children": None,
                    "error": None,
                }

            else:
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
                    [sku],
                )

                if not parent_result:
                    return {
                        "success": False,
                        "sku": sku,
                        "is_parent": True,
                        "error": "Parent product not found",
                    }

                parent = parent_result[0]

                children_result = await conn.execute_query_dict(
                    """
                    SELECT
                        cp.sku,
                        cp.size,
                        cp.is_primary,
                        cu.upc,
                        cu.is_primary_upc
                    FROM child_products cp
                    LEFT JOIN child_upcs cu ON cu.child_sku = cp.sku
                    WHERE cp.parent_sku = $1 AND cp.is_active = TRUE
                    ORDER BY cp.size, cp.is_primary DESC, cu.is_primary_upc DESC
                    """,
                    [sku],
                )

                children_map = {}
                for c in children_result:
                    sku_key = c["sku"]
                    if sku_key not in children_map:
                        children_map[sku_key] = {
                            "sku": sku_key,
                            "size": c["size"],
                            "is_primary": c["is_primary"],
                            "primary_upc": None,
                            "upcs": [],
                        }
                    if c.get("upc"):
                        children_map[sku_key]["upcs"].append(
                            {"upc": c["upc"], "is_primary_upc": c["is_primary_upc"]}
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
                    "sku": sku,
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
                    "size": None,
                    "is_primary": None,
                    "parent_sku": None,
                    "primary_upc": None,
                    "all_upcs": None,
                    "child_count": len(children),
                    "children": children,
                    "error": None,
                }

        except Exception as e:
            logger.error(f"Error getting product details for '{sku}': {e}")
            return {"success": False, "sku": sku, "is_parent": None, "error": str(e)}

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
    ) -> Dict[str, Any]:
        try:
            conn = await ProductService._get_connection()

            if not mappings:
                return {"success": False, "error": "No mappings provided"}

            bulk_result = await conn.execute_query_dict(
                """INSERT INTO bulk_reassignments (old_parent_sku, new_parent_sku, total_count, created_by)
                   VALUES ($1, $2, $3, $4)
                   RETURNING id""",
                [old_parent_sku, new_parent_sku, len(mappings), created_by],
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
        clean_keyword = re.sub(r"[^0-9]", "", keyword)
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
        clean_keyword = re.sub(r"[^0-9]", "", keyword)
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
                    df = pd.read_csv(io.BytesIO(content))
                elif fname.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(io.BytesIO(content))
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
            existing_skus = set()
            if file_skus:
                r = await conn.execute_query_dict("SELECT sku FROM child_products WHERE sku = ANY($1)", [file_skus])
                existing_skus = {row["sku"] for row in r}

            file_values = df["value"].dropna().astype(str).apply(lambda v: re.sub(r"[^0-9]", "", str(v).strip())).unique().tolist()
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

                clean_value = re.sub(r"[^0-9]", "", value)
                if not clean_value:
                    error_by_index[idx] = "UPC must contain only digits"
                    errors.append({"row": row_num, "sku": sku, "value": value, "field": "UPC", "message": error_by_index[idx]})
                    continue

                if sku not in existing_skus:
                    error_by_index[idx] = f"SKU '{sku}' not found in database"
                    errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "Product", "message": error_by_index[idx]})
                    continue

                # Intra-CSV duplicate check
                if clean_value in seen_values:
                    error_by_index[idx] = f"Duplicate value in import file"
                    errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                    continue
                seen_values.add(clean_value)

                classification = None
                source_sku = None

                if action_normalized in ("Primary", "Secondary"):
                    if len(clean_value) not in (8, 12, 13):
                        error_by_index[idx] = f"UPC must be 8, 12, or 13 digits (got {len(clean_value)})"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if not ProductService._validate_upc_checksum(clean_value):
                        error_by_index[idx] = "Invalid UPC"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if action_normalized == "Primary" and len(clean_value) == 8:
                        error_by_index[idx] = "EAN-8 cannot be set as primary"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "Type/Action", "message": error_by_index[idx]})
                        continue

                    if clean_value in upc_to_sku:
                        owner = upc_to_sku[clean_value]
                        if owner == sku:
                            # UPC already on target — noop or promote
                            if action_normalized == "Primary" and not upc_is_primary.get(clean_value):
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
                    is_valid_upc = len(clean_value) in (8, 12, 13) and ProductService._validate_upc_checksum(clean_value)
                    if is_valid_upc:
                        if sku_primary_upc.get(sku) == clean_value:
                            error_by_index[idx] = "Cannot delete primary UPC"
                            errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                            continue
                        classification = "delete_upc"
                    elif len(clean_value) < 6:
                        error_by_index[idx] = f"Value must be at least 6 digits (got {len(clean_value)})"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    else:
                        classification = "delete_keyword"

                elif action_normalized == "Keyword":
                    if len(clean_value) < 6:
                        error_by_index[idx] = f"Keyword must be at least 6 digits (got {len(clean_value)})"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if len(clean_value) in (8, 12, 13) and ProductService._is_valid_barcode(clean_value):
                        error_by_index[idx] = "Keyword cannot be a valid barcode (has valid checksum)"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue
                    if clean_value in upc_to_sku:
                        error_by_index[idx] = f"Keyword conflicts with existing UPC for SKU: {upc_to_sku[clean_value]}"
                        errors.append({"row": row_num, "sku": sku, "value": clean_value, "field": "UPC", "message": error_by_index[idx]})
                        continue

                    if clean_value in keyword_to_sku:
                        owner = keyword_to_sku[clean_value]
                        if owner == sku:
                            classification = "noop"
                        else:
                            classification = "swap_keyword"
                            source_sku = owner
                    else:
                        classification = "add_keyword"

                item = {"row": row_num, "sku": sku, "value": clean_value, "action": action_normalized, "classification": classification}
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
                msg = (
                    f"Import would leave SKU '{sku}' with no UPCs. "
                    f"A SKU must always have at least one primary UPC."
                )
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
                msg = (
                    f"Import would leave SKU '{sku}' with only EAN-8 UPCs, which cannot be primary. "
                    f"A SKU must always have at least one primary-capable UPC (UPC-A or EAN-13)."
                )
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
            clean_keyword = re.sub(r"[^0-9]", "", keyword)

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
