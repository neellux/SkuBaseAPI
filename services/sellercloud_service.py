import asyncio
import copy
import logging
import re
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import base64

import httpx
import orjson
from config import config
from exceptions.submission_exceptions import SellerCloudSubmitError
from fastapi import HTTPException
from models.db_models import AppSettings
from services.listing_options_service import listing_options_service
from services.product_resolver import child_skus_for, resolve_parent
from services.template_render import render_template, resolve_field_template
from tortoise import Tortoise

logger = logging.getLogger(__name__)

CUSTOM_COLUMN_FIELDS = {"SIZING_SCHEME", "GENDER", "HTMLDESCRIPTION_FIXED"}

CHILD_UPDATE_CONCURRENCY = 3

FIELD_NAME_OVERRIDES = {
    "ProductType": "ProductTypeName",
}

SKIP_FIELDS = {"ID"}

GENDER_MAPPING = {
    "Mens": "Men's ",
    "Womens": "Women's ",
    "Boys": "Boy's ",
    "Girls": "Girl's ",
    "Unisex": "Unisex ",
    "Does Not Apply": "",
}


class SellerCloudService:
    @staticmethod
    def _clean_html(html_content: str) -> str:
        if not html_content or not isinstance(html_content, str):
            return html_content

        cleaned = html_content

        cleaned = re.sub(
            r"<li>\s*<p>(.*?)</p>\s*</li>", r"<li>\1</li>", cleaned, flags=re.DOTALL
        )

        cleaned = re.sub(r"<p>\s*</p>", "", cleaned)

        return cleaned

    def __init__(self):
        self.base_url = config.get("sellercloud", {}).get("sellercloud_baseurl", "")
        self.username = config.get("sellercloud", {}).get("sellercloud_username", "")
        self.password = config.get("sellercloud", {}).get("sellercloud_password", "")
        self.access_token: Optional[str] = None
        self.token_type: str = "Bearer"
        self.token_expires_at: Optional[datetime] = None
        self.client: Optional[httpx.AsyncClient] = None
        self._token_lock = asyncio.Lock()

        self._token_refresh_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._token_refresh_interval = 30

        self.max_retries = 20000
        self.retry_delay = 1
        self.token_refresh_buffer = 300

    async def _get_client(self) -> httpx.AsyncClient:
        if self.client is None or self.client.is_closed:
            headers = {"Accept": "application/json"}

            if self.access_token:
                headers["Authorization"] = f"{self.token_type} {self.access_token}"
                logger.info("Creating new client with existing Authorization header")
            else:
                logger.info("Creating new client without Authorization header")

            self.client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0), headers=headers
            )
        return self.client

    async def _is_token_valid(self) -> bool:
        if not self.access_token or not self.token_expires_at:
            return False

        now_utc = datetime.now(timezone.utc)
        expires_soon = now_utc + timedelta(seconds=self.token_refresh_buffer)
        is_valid = self.token_expires_at > expires_soon

        if not is_valid:
            logger.info(
                f"Token expired or expiring soon. Expires at: {self.token_expires_at}, Current UTC: {now_utc}"
            )

        return is_valid

    async def _update_credentials(self) -> Dict[str, Any]:
        async with self._token_lock:
            if await self._is_token_valid():
                return {"access_token": self.access_token}

            auth_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0), headers={"Accept": "application/json"}
            )

            try:
                for attempt in range(self.max_retries):
                    try:
                        response = await auth_client.post(
                            f"{self.base_url}/token",
                            json={
                                "username": self.username,
                                "password": self.password,
                            },
                        )

                        response.raise_for_status()

                        creds = orjson.loads(response.content)

                        self.access_token = creds["access_token"]
                        self.token_type = creds.get("token_type", "Bearer")

                        if ".expires" in creds:
                            expires_str = creds[".expires"]
                            expires_str = expires_str.split(".")[0]
                            self.token_expires_at = datetime.fromisoformat(
                                expires_str
                            ).replace(tzinfo=timezone.utc)
                        else:
                            expires_in = creds.get("expires_in", 3600)
                            self.token_expires_at = datetime.now(
                                timezone.utc
                            ) + timedelta(seconds=int(expires_in))

                        client = await self._get_client()
                        client.headers.update(
                            {"Authorization": f"{self.token_type} {self.access_token}"}
                        )

                        logger.info(
                            f"SellerCloud credentials updated successfully. Token expires at {self.token_expires_at} UTC"
                        )

                        self._start_token_refresh_task()

                        return creds

                    except Exception as e:
                        logger.error(
                            f"Failed to update SellerCloud credentials (attempt {attempt + 1}/{self.max_retries}): {traceback.format_exc()}"
                        )

                        if attempt < self.max_retries - 1:
                            logger.info(f"Retrying in {self.retry_delay} seconds...")
                            await asyncio.sleep(self.retry_delay)
                        else:
                            logger.error(
                                "Max retries reached. Failed to update SellerCloud credentials."
                            )
                            raise Exception(
                                "Failed to authenticate with SellerCloud after maximum retries"
                            )
            finally:
                await auth_client.aclose()

    async def _ensure_authenticated(self) -> None:
        if not await self._is_token_valid():
            logger.info("Token is invalid or expiring, refreshing credentials...")
            await self._update_credentials()
        else:
            logger.debug("Token is still valid, no refresh needed")

    async def _background_token_refresh(self) -> None:
        logger.info(
            f"Starting background token refresh task (checking every {self._token_refresh_interval}s)"
        )

        while True:
            try:
                if self._shutdown_event and self._shutdown_event.is_set():
                    logger.info(
                        "Shutdown event detected, stopping background token refresh"
                    )
                    break

                try:
                    if self._shutdown_event:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self._token_refresh_interval,
                        )
                        logger.info(
                            "Shutdown signaled, stopping background token refresh"
                        )
                        break
                    else:
                        await asyncio.sleep(self._token_refresh_interval)
                except asyncio.TimeoutError:
                    pass

                try:
                    await self._ensure_authenticated()
                except Exception as e:
                    logger.error(
                        f"Error during background token refresh: {e}\n{traceback.format_exc()}"
                    )

            except asyncio.CancelledError:
                logger.info("Background token refresh task cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error in background token refresh: {e}\n{traceback.format_exc()}"
                )
                await asyncio.sleep(5)

        logger.info("Background token refresh task stopped")

    def _start_token_refresh_task(self) -> None:
        if self._token_refresh_task is None or self._token_refresh_task.done():
            if self._shutdown_event is None:
                self._shutdown_event = asyncio.Event()

            self._token_refresh_task = asyncio.create_task(
                self._background_token_refresh()
            )
            logger.info("Background token refresh task started")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        **kwargs,
    ) -> httpx.Response:
        await self._ensure_authenticated()

        client = await self._get_client()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        logger.debug(f"Making {method} request to {url}")

        try:
            response = await client.request(
                method=method,
                url=url,
                json=data if method.upper() in ["POST", "PUT", "PATCH"] else None,
                params=params,
                **kwargs,
            )

            if response.status_code == 401:
                logger.warning(
                    f"Received 401 Unauthorized. Token may have expired during request. "
                    f"Current token expires at: {self.token_expires_at}, "
                    f"Current UTC time: {datetime.now(timezone.utc)}"
                )
                logger.info("Refreshing token and retrying request...")
                await self._update_credentials()

                client = await self._get_client()

                logger.info(f"Retrying {method} request to {url}")
                response = await client.request(
                    method=method,
                    url=url,
                    json=data if method.upper() in ["POST", "PUT", "PATCH"] else None,
                    params=params,
                    **kwargs,
                )

            if response.status_code != 200:
                logger.warning(
                    f"Non-200 response {response.status_code}: {method} {url} - {response.text}"
                )
            else:
                logger.debug(f"Response status: {response.status_code}")
            response.raise_for_status()
            return response

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error {e.response.status_code}: {method} {url} - {e.response.text}"
            )
            raise
        except Exception as e:
            logger.error(f"Request failed: {method} {url} - {e}")
            raise

    async def _search_catalog(
        self, keyword: str, active_status: int = 1
    ) -> List[Dict[str, Any]]:
        """Every /Catalog item matching a keyword, paged out."""
        page_size = 50
        page_number = 1
        items: List[Dict[str, Any]] = []

        while True:
            try:
                data = await self.get(
                    "/Catalog",
                    params={
                        "model.Keyword": keyword,
                        "model.pageSize": page_size,
                        "model.pageNumber": page_number,
                        "model.activeStatus": active_status,
                    },
                )
            except Exception as e:
                logger.error(
                    f"Error searching catalog for {keyword} on page {page_number}: {e}"
                )
                raise

            products = data.get("Items", [])
            items.extend(products)

            total_count = data.get("TotalResults", 0)
            if page_number * page_size >= total_count or not products:
                break

            page_number += 1

        return items

    @staticmethod
    def _required_fields_only(
        product: Dict[str, Any], only_required_fields: bool
    ) -> Dict[str, Any]:
        if not only_required_fields:
            return product

        return {
            field: product[field]
            for field in (
                "PARENT_ID",
                "ID",
                "ImageUrl",
                "ProductName",
                "ProductType",
                "ManufacturerSKU",
                "UPC",
            )
        }

    async def get_catalog_item(
        self, product_id: str, only_required_fields: bool = True
    ) -> Optional[Dict[str, Any]]:
        """The catalog row for EXACTLY this SellerCloud product ID.

        Use this when you mean one specific SKU - reading a child's own ListPrice or
        SiteCost, for instance. The old get_product_by_id answered this question by
        chopping the ID and returning the first item sharing the derived parent, i.e. an
        arbitrary sibling, so per-child reads were silently getting another size's data.
        """
        wanted = product_id.lower()

        for item in await self._search_catalog(product_id):
            if item.get("ID", "").lower() == wanted:
                item["PARENT_ID"] = await resolve_parent(item["ID"])
                return self._required_fields_only(item, only_required_fields)

        logger.warning(f"Product with ID {product_id} not found in SellerCloud")
        return None

    async def get_product_for_listing(
        self, sku: str, only_required_fields: bool = True
    ) -> Optional[Dict[str, Any]]:
        """A representative catalog row for the product family that `sku` belongs to.

        Used to prefill a listing, where the caller wants the product's shared data and
        does not care which variant supplies it. Accepts a parent or a child SKU.
        """
        parent_sku = await resolve_parent(sku)
        family = {
            child.lower()
            for child in await child_skus_for(parent_sku, include_inactive=True)
        }
        family.add(parent_sku.lower())

        # Membership against the registered child set, never a chopped ID. A keyword
        # search for ".../BLACK" also returns ".../BLACK2"; chopping cannot tell those
        # apart and the products DB can.
        matches = [
            item
            for item in await self._search_catalog(parent_sku)
            if item.get("ID", "").lower() in family
        ]

        if not matches:
            logger.warning(
                f"No active SellerCloud products found for parent {parent_sku} (from {sku})"
            )
            return None

        # Prefer the exact SKU asked for, else any variant. For a multi-variant product
        # the SC matrix parent is inactive and already filtered out by activeStatus=1, so
        # this lands on a child row, which is what create_listing expects to prefill from.
        product = next(
            (item for item in matches if item.get("ID", "").lower() == sku.lower()),
            matches[0],
        )
        product["PARENT_ID"] = parent_sku
        return self._required_fields_only(product, only_required_fields)

    async def get_product_images(
        self, product_id: str, parent_sku: Optional[str] = None
    ) -> List[str]:
        try:
            # Images live under the parent's GCS prefix. Callers that already resolved
            # the parent pass it in so this costs no extra lookup; slashes stay literal
            # in the path, everything else is escaped, matching daily_image_import_poller.
            resolved_parent = parent_sku or await resolve_parent(product_id)
            parent_product_id = quote(resolved_parent, safe="/")

            semaphore = asyncio.Semaphore(5)

            async def check_image_exists(index: int) -> tuple[int, str, bool]:
                image_url = f"https://storage.googleapis.com/lux_products/{parent_product_id}/{index}_1500.jpg"
                async with semaphore:
                    try:
                        async with httpx.AsyncClient(timeout=10.0) as client:
                            response = await client.head(image_url)
                            exists = response.status_code == 200
                            if exists:
                                logger.debug(f"Found image: {image_url}")
                            else:
                                logger.debug(
                                    f"Image not found: {image_url} (status {response.status_code})"
                                )
                            return index, image_url, exists
                    except Exception as e:
                        logger.debug(f"Error checking image {image_url}: {e}")
                        return index, image_url, False

            async def check_washtag_exists(index: int) -> tuple[int, str, bool]:
                washtag_url = f"https://storage.googleapis.com/lux_products/{parent_product_id}/washtag_{index}.jpg"
                async with semaphore:
                    try:
                        async with httpx.AsyncClient(timeout=10.0) as client:
                            response = await client.head(washtag_url)
                            exists = response.status_code == 200
                            if exists:
                                logger.info(f"Found washtag image: {washtag_url}")
                            else:
                                logger.debug(
                                    f"Washtag image not found: {washtag_url} (status {response.status_code})"
                                )
                            return index, washtag_url, exists
                    except Exception as e:
                        logger.debug(f"Error checking washtag image {washtag_url}: {e}")
                        return index, washtag_url, False

            logger.info(
                f"Checking GCS for images 1-8 and washtags for product {parent_product_id}"
            )

            image_checks = [check_image_exists(i) for i in range(1, 9)]
            washtag_checks = [check_washtag_exists(i) for i in range(1, 4)]

            results = await asyncio.gather(*image_checks, *washtag_checks)

            image_urls = []

            for index, url, exists in sorted(results[:8]):
                if exists:
                    image_urls.append(url)

            washtag_urls = [url for _, url, exists in sorted(results[8:]) if exists]
            if washtag_urls:
                image_urls.extend(washtag_urls)
                logger.info(
                    f"Added {len(washtag_urls)} washtag images for product {parent_product_id}"
                )

            logger.info(
                f"Found {len(image_urls)} total images on GCS for product {parent_product_id}"
            )
            return image_urls

        except Exception as e:
            logger.error(
                f"Error fetching product images for {product_id}: {traceback.format_exc()}"
            )
            return []

    async def validate_product_images_on_gcs(
        self, product_id: str, max_workers: int = 3, parent_sku: Optional[str] = None
    ) -> tuple[bool, List[str], int]:
        try:
            image_urls = await self.get_product_images(product_id, parent_sku=parent_sku)

            if not image_urls:
                logger.warning(f"No product images found for product {product_id}")
                return False, [], 0

            semaphore = asyncio.Semaphore(max_workers)
            missing_images = []

            async def check_image_exists(url: str) -> tuple[str, bool]:
                async with semaphore:
                    try:
                        async with httpx.AsyncClient(timeout=10.0) as client:
                            response = await client.head(url)
                            exists = response.status_code == 200
                            if not exists:
                                logger.warning(
                                    f"Image check failed for {url}: status {response.status_code}"
                                )
                            return url, exists
                    except Exception as e:
                        logger.error(f"Error checking image {url}: {e}")
                        return url, False

            logger.info(
                f"Validating {len(image_urls)} images for product {product_id} on GCS"
            )
            results = await asyncio.gather(
                *[check_image_exists(url) for url in image_urls]
            )

            for url, exists in results:
                if not exists:
                    missing_images.append(url)

            all_valid = len(missing_images) == 0

            if all_valid:
                logger.info(
                    f"All {len(image_urls)} images validated successfully for product {product_id}"
                )
            else:
                logger.warning(
                    f"Product {product_id} has {len(missing_images)}/{len(image_urls)} missing images on GCS: {missing_images}"
                )

            return all_valid, missing_images, len(image_urls)

        except Exception as e:
            logger.error(
                f"Error validating product images for {product_id}: {traceback.format_exc()}"
            )
            return False, [], 0

    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        response = await self._make_request("GET", endpoint, params=params)
        return orjson.loads(response.content)

    async def put(self, endpoint: str, data: Optional[Dict] = None) -> httpx.Response:
        return await self._make_request("PUT", endpoint, data=data)

    async def post(self, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        response = await self._make_request("POST", endpoint, data=data)
        return orjson.loads(response.content) if response.content else {}

    async def create_product(
        self,
        product_sku: str,
        product_name: str,
        company_id: int,
        site_cost: float,
        product_type_name: str,
        brand_name: str,
        upc: str,
    ) -> Dict[str, Any]:
        payload = {
            "CompanyId": company_id,
            "ProductName": product_name,
            "ProductSKU": product_sku,
            "ProductTypeName": product_type_name,
            "BrandName": brand_name,
            "SiteCost": site_cost,
            "UPC": upc,
        }
        return await self.post("/Products", data=payload)

    async def copy_custom_columns(
        self,
        source_product_id: str,
        target_product_id: str,
        overrides: Optional[Dict[str, str]] = None,
    ) -> None:
        source_data = await self.get_catalog_item(
            source_product_id, only_required_fields=False
        )
        if not source_data:
            raise Exception(
                f"Source product {source_product_id} not found for custom column copy"
            )

        custom_columns = source_data.get("CustomColumns", [])
        columns_to_copy = []
        for col in custom_columns:
            column_name = col.get("ColumnName", "")
            value = col.get("Value")
            if value is not None and column_name:
                if overrides and column_name in overrides:
                    value = overrides[column_name]
                columns_to_copy.append({"ColumnName": column_name, "Value": value})

        if columns_to_copy:
            await self._make_request(
                "PUT",
                "/Products/CustomColumns",
                data={"ProductID": target_product_id, "CustomColumns": columns_to_copy},
            )

    async def upload_product_image(self, product_id: str, image_url: str) -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(image_url)
            response.raise_for_status()

        content_b64 = base64.b64encode(response.content).decode("utf-8")
        filename = image_url.rsplit("/", 1)[-1] if "/" in image_url else "image.jpg"

        await self._make_request(
            "POST",
            "/ProductImage",
            data={
                "ProductID": product_id,
                "Content": content_b64,
                "FileName": filename,
                "Properties": {"IsDefault": True},
            },
        )

    async def create_image_import(self, tsv_bytes: bytes) -> str:
        """Submit a tab-separated image-import file to SellerCloud, return the queued job id.

        This endpoint expects a FORM-ENCODED body (not JSON), so we bypass ``_make_request``
        (which always sends JSON) and post via the authenticated client directly.
        """
        await self._ensure_authenticated()
        client = await self._get_client()
        payload = {
            "FileContents": base64.b64encode(tsv_bytes).decode("utf-8"),
            "FileExtension": ".txt",
            "Format": 0,
        }
        url = f"{self.base_url}/Catalog/Imports/Images"
        resp = await client.post(url, data=payload, timeout=httpx.Timeout(120.0))
        if resp.status_code == 401:
            await self._update_credentials()
            client = await self._get_client()
            resp = await client.post(url, data=payload, timeout=httpx.Timeout(120.0))
        resp.raise_for_status()
        link = resp.json().get("QueuedJobLink", "") or ""
        if "id=" not in link:
            raise RuntimeError(
                f"Catalog/Imports/Images returned no job link: {resp.text[:200]}"
            )
        return link.split("id=")[1].split("&")[0]

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Fetch a queued job's details (Basic.CompletedOn, TotalRecords, TotalProcessed)."""
        return await self.get(f"/QueuedJobs/{job_id}")

    async def is_job_complete(self, job_id: str) -> bool:
        """Whether a queued job finished. Raises only when SellerCloud says it failed.

        Every other outcome, including a transport error or SellerCloud answering 500
        with "Could not find or load record with ID <id>!" for a job it has queued but
        not yet materialized, is reported as "not done yet" so the caller polls again.
        A raise here would be latched into the job row's error and the row would then
        be skipped forever, which is the trap PhotoManagementNew documents on its own
        get_sellercloud_job_status.
        """
        try:
            basic = (await self.get_job_status(job_id)).get("Basic") or {}
        except Exception as e:
            logger.debug(f"Job {job_id} status poll failed, treating as pending: {e}")
            return False

        status = basic.get("Status")
        if status == 3:
            return True
        if status in (4, 5):
            raise RuntimeError(
                f"SellerCloud job {job_id} failed: {basic.get('ErrorMessage') or status}"
            )
        return False

    async def create_image_export(self, product_ids: List[str]) -> str:
        """Queue a StandardExportKind 11 export (one row per existing product image).

        The output file lists each product's ImageID and ImageURL, which is the only way
        to build the DELETE rows an image import needs.
        """
        data = await self.post(
            "/Catalog/Exports/Basic",
            data={"StandardExportKind": 11, "ProductIds": product_ids},
        )
        link = data.get("QueuedJobLink", "") or ""
        if "id=" not in link:
            raise RuntimeError(f"Catalog/Exports/Basic returned no job link: {data}")
        return link.split("id=")[1].split("&")[0]

    async def get_job_output_file(self, job_id: str) -> bytes:
        """A finished job's output file. SellerCloud base64-encodes it, but not always."""
        response = await self._make_request(
            "GET", "/QueuedJobs/OutputFile", params={"id": job_id}
        )
        try:
            return base64.b64decode(response.content, validate=True)
        except Exception:
            return response.content

    async def update_product_upc(self, product_id: str, upc: str) -> Dict[str, Any]:
        response = await self.put(
            "/Catalog/BasicInfo",
            data={"ProductID": product_id, "UPC": upc},
        )
        if response.status_code == 200 and not response.content:
            return {"success": True}
        return orjson.loads(response.content)

    async def get_product_fields(self) -> List[Dict[str, Any]]:
        try:
            data = await self.get(
                "/Catalog",
                params={
                    "model.Keyword": "MMM-XTPS-0005",
                    "model.pageSize": 50,
                    "model.pageNumber": 1,
                    "model.activeStatus": 1,
                },
            )

            items = data.get("Items", [])
            if not items:
                logger.warning("No products found to extract field information")
                return []

            product = items[0]

            fields = [
                {"ID": field, "tags": []}
                for field in product.keys()
                if field != "CustomColumns"
            ]

            custom_columns = product.get("CustomColumns", {})
            custom_fields = [
                {"ID": field["ColumnName"], "tags": ["custom"]}
                for field in custom_columns
            ]

            return fields + custom_fields

        except Exception as e:
            logger.error(f"Error fetching product fields: {traceback.format_exc()}")
            raise

    async def get_gender_from_product_type(self, product_type: str) -> str:

        try:
            logger.info(f"Fetching gender for ProductType: {product_type}")
            data = await listing_options_service.get_product_type_info(product_type)

            gender = data.get("gender")
            if not gender:
                logger.error(f"Gender not found for ProductType: {product_type}")
                raise HTTPException(
                    status_code=404, detail="Gender not found in Listing Options"
                )

            logger.info(
                f"Successfully fetched gender '{gender}' for ProductType: {product_type}"
            )
            return gender

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Error fetching gender for ProductType '{product_type}': {traceback.format_exc()}"
            )
            raise HTTPException(status_code=500, detail="Internal server error")

    async def validate_brand_color(self, color: str, brand_color: str) -> None:
        if not color or not brand_color:
            return
        if color.lower() == brand_color.lower():
            return

        color_info = await listing_options_service.get_color_info(color)
        if not color_info.get("color"):
            raise HTTPException(
                status_code=404,
                detail=f"Color '{color}' not found in Listing Options",
            )

        brand_color_info = await listing_options_service.get_color_info(brand_color)
        if brand_color_info.get("color"):
            mapped_to = brand_color_info["color"]
            canonical_color = color_info["color"]
            if mapped_to.lower() != canonical_color.lower():
                raise HTTPException(
                    status_code=400,
                    detail=f"Brand Color '{brand_color}' is already mapped to '{mapped_to}'",
                )

    async def add_color_alias(self, color: str, brand_color: str) -> None:
        try:
            logger.info(f"Adding alias '{brand_color}' to color '{color}'")

            conn = Tortoise.get_connection("default")

            result = await conn.execute_query_dict(
                """
                SELECT id, color, aliases
                FROM listingoptions_colors
                WHERE LOWER(color) = LOWER($1)
                LIMIT 1
                """,
                [color],
            )

            if not result:
                logger.error(f"Color '{color}' not found in listing options")
                raise HTTPException(
                    status_code=404,
                    detail=f"Color '{color}' not found in Listing Options",
                )

            row = result[0]
            aliases = row.get("aliases") or []
            if isinstance(aliases, str):
                try:
                    aliases = orjson.loads(aliases)
                except orjson.JSONDecodeError:
                    aliases = []

            if brand_color.lower() in [a.lower() for a in aliases]:
                logger.info(f"Alias '{brand_color}' already exists for color '{color}'")
                return

            conflict = await conn.execute_query_dict(
                """
                SELECT color FROM listingoptions_colors
                WHERE LOWER(color) = LOWER($1)
                   OR EXISTS (
                       SELECT 1 FROM jsonb_array_elements_text(aliases) AS alias
                       WHERE LOWER(alias) = LOWER($1)
                   )
                LIMIT 1
                """,
                [brand_color],
            )

            if conflict:
                existing_color = conflict[0]["color"]
                if existing_color.lower() != color.lower():
                    logger.error(
                        f"Brand Color '{brand_color}' already mapped to '{existing_color}'"
                    )
                    raise HTTPException(
                        status_code=400,
                        detail=f"Brand Color '{brand_color}' is already mapped to '{existing_color}'",
                    )

            aliases.append(brand_color)
            await conn.execute_query(
                """
                UPDATE listingoptions_colors
                SET aliases = $1::jsonb, updated_at = NOW()
                WHERE id = $2
                """,
                [orjson.dumps(aliases).decode(), row["id"]],
            )

            logger.info(f"Successfully added alias '{brand_color}' to color '{color}'")

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Error adding alias to color '{color}': {traceback.format_exc()}"
            )
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_product_children(
        self,
        product_id: str,
        override_product_type: str = None,
        override_sizing_scheme: str = None,
        include_inactive: bool = False,
    ) -> Dict[str, Any]:
        # include_inactive is opt-in (only the /listings/children read route sets
        # it) so that write paths - notably submit_listing_to_sellercloud, which
        # turns this list into the set of SKUs it pushes updates to - keep the
        # active-only behavior and can never write to a disabled SKU.
        #
        # activeStatus: 1 = active only, 0 = inactive only, -1 = all. With -1 each
        # item carries ActiveStatus: "Active" | "InActive".
        # Accepts a parent or a child SKU; the parent comes from the products DB, never
        # from splitting the SKU. Inactive children are always included in the registered
        # set, because include_inactive exists precisely to surface disabled variants.
        parent_product_id = await resolve_parent(product_id)
        registered = {
            child.lower()
            for child in await child_skus_for(parent_product_id, include_inactive=True)
        }
        family = registered | {parent_product_id.lower()}

        all_children = [
            product
            for product in await self._search_catalog(
                parent_product_id, active_status=-1 if include_inactive else 1
            )
            if product.get("ID", "").lower() in family
        ]

        # SellerCloud keyword search is substring-ish, so this is the canary for the
        # opposite failure: children we know about that the search did not return.
        found = {p.get("ID", "").lower() for p in all_children}
        missed = registered - found
        if missed and include_inactive:
            logger.warning(
                f"SellerCloud returned no catalog row for {len(missed)} registered "
                f"child(ren) of {parent_product_id}: {sorted(missed)[:10]}"
            )

        def _is_active(product: Dict[str, Any]) -> bool:
            return product.get("ActiveStatus") == "Active"

        # A multi-variant matrix parent is itself inactive, so once inactive rows are
        # included it would otherwise show up as a bogus sizeless child. Drop it, but
        # only when real variants exist - for a single-SKU product the bare parent row
        # IS the product. The discriminator is identity against the resolved parent, not
        # the presence of a "/": an ESSX parent SKU contains slashes of its own.
        if include_inactive and any(p["ID"] != parent_product_id for p in all_children):
            all_children = [
                p
                for p in all_children
                if p["ID"] != parent_product_id or _is_active(p)
            ]

        if not all_children:
            return {
                "children": [],
                "product_type": None,
                "sizing_scheme": None,
            }

        # Derive ProductType/SIZING_SCHEME from an active child where possible: an
        # inactive child can carry a stale scheme (a disabled variant is often one
        # that was re-sized under a different scheme).
        first_product = next(
            (p for p in all_children if _is_active(p)), all_children[0]
        )
        sellercloud_product_type = first_product.get("ProductType")

        product_type = (
            override_product_type if override_product_type else sellercloud_product_type
        )

        custom_columns = first_product.get("CustomColumns", [])
        custom_columns_map = {}
        for custom_col in custom_columns:
            column_name = custom_col.get("ColumnName", "")
            if column_name:
                custom_columns_map[column_name] = custom_col.get("Value")

        sellercloud_sizing_scheme = custom_columns_map.get("SIZING_SCHEME")

        sizing_scheme = (
            override_sizing_scheme
            if override_sizing_scheme
            else sellercloud_sizing_scheme
        )

        children_data = []
        for product in all_children:
            product_id_str = product["ID"]
            product_custom_columns = product.get("CustomColumns", [])
            size_from_custom = None
            for cc in product_custom_columns:
                if cc.get("ColumnName") == "SIZE":
                    size_from_custom = cc.get("Value")
                    break

            size = size_from_custom if size_from_custom else ""

            children_data.append(
                {
                    "id": product_id_str,
                    "parent_id": parent_product_id,
                    "size": size,
                    "is_active": _is_active(product),
                }
            )

        def sort_key(child):
            size = child["size"]
            try:
                return (0, int(size))
            except (ValueError, TypeError):
                try:
                    return (0, float(size))
                except (ValueError, TypeError):
                    return (1, size)

        children_data.sort(key=sort_key)

        return {
            "children": children_data,
            "product_type": sellercloud_product_type,
            "sizing_scheme": sizing_scheme,
        }

    async def update_children_basic_info(
        self,
        parent_sku: str,
        changes: Dict[str, Any],
        field_definitions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not changes:
            return {"success": True, "updated": 0, "failed": []}

        conn = Tortoise.get_connection("product_db")
        rows = await conn.execute_query_dict(
            "SELECT sku, size FROM child_products "
            "WHERE parent_sku = $1 AND is_active = TRUE",
            [parent_sku],
        )
        if not rows:
            return {"success": True, "updated": 0, "failed": []}

        sc_field_map: Dict[str, Dict[str, Any]] = {}
        for field_def in field_definitions or []:
            local_name = field_def.get("name")
            if not local_name:
                continue
            for platform in field_def.get("platforms") or []:
                if platform.get("platform_id") == "sellercloud":
                    sc_field_map[local_name] = {
                        "field_id": platform.get("field_id") or local_name,
                        "is_custom": bool(platform.get("is_custom", False)),
                    }
                    break

        local_to_form_name = {"color": "standard_color", "mpn": "manufacturer_sku", "brand": "brand_name"}

        base_normal_fields: List[Dict[str, Any]] = []
        base_custom_fields: List[Dict[str, Any]] = []
        title_value: Optional[str] = None

        for local_name, value in changes.items():
            if value is None:
                continue
            if local_name == "title":
                title_value = value
                continue

            mapping = sc_field_map.get(local_name) or sc_field_map.get(
                local_to_form_name.get(local_name, "")
            )

            if mapping:
                sc_field_id = mapping["field_id"]
                if mapping["is_custom"]:
                    base_custom_fields.append(
                        {"ColumnName": sc_field_id, "Value": value}
                    )
                else:
                    target = FIELD_NAME_OVERRIDES.get(sc_field_id, sc_field_id)
                    base_normal_fields.append({"Name": target, "Value": value})
            elif local_name in CUSTOM_COLUMN_FIELDS:
                base_custom_fields.append({"ColumnName": local_name, "Value": value})
            else:
                logger.warning(
                    f"No SellerCloud mapping for '{local_name}'; skipping for parent {parent_sku}"
                )

        semaphore = asyncio.Semaphore(CHILD_UPDATE_CONCURRENCY)
        failures: List[Dict[str, str]] = []

        async def update_one(child_sku: str, size: str) -> None:
            async with semaphore:
                normal_fields = list(base_normal_fields)
                custom_fields = list(base_custom_fields)

                if title_value is not None:
                    list_price = ""
                    try:
                        sc_product = await self.get_catalog_item(
                            child_sku, only_required_fields=False
                        )
                        if sc_product:
                            list_price = sc_product.get("ListPrice", "") or ""
                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch ListPrice for {child_sku}: {e}"
                        )

                    if size and list_price not in ("", None):
                        product_name = f"{title_value} SIZE {size} ${list_price}"
                    elif size:
                        product_name = f"{title_value} SIZE {size}"
                    else:
                        product_name = title_value
                    normal_fields.append({"Name": "ProductName", "Value": product_name})

                try:
                    await self._update_single_product_with_retry(
                        child_sku, normal_fields, custom_fields
                    )
                except Exception as e:
                    logger.error(
                        f"SellerCloud update failed for child {child_sku}: {e}"
                    )
                    failures.append({"sku": child_sku, "error": str(e)})

        await asyncio.gather(
            *[update_one(r["sku"], r.get("size") or "") for r in rows],
            return_exceptions=False,
        )

        return {
            "success": len(failures) == 0,
            "updated": len(rows) - len(failures),
            "failed": failures,
        }

    async def _update_single_product_with_retry(
        self,
        product_id: str,
        normal_fields: List[Dict[str, Any]],
        custom_fields: List[Dict[str, Any]],
        max_retries: int = 3,
    ) -> None:
        for attempt in range(max_retries):
            try:
                if normal_fields:
                    normal_payload = {"ProductID": product_id, "Fields": normal_fields}

                    logger.info(
                        f"Updating {len(normal_fields)} normal fields for product {product_id} (attempt {attempt + 1}/{max_retries})"
                    )

                    # debug, not info: this is the full field payload, logged once
                    # per child SKU per retry attempt.
                    logger.debug(f"Normal payload: {normal_payload}")
                    response = await self._make_request(
                        "PUT", "/Catalog/AdvancedInfo", data=normal_payload
                    )

                    logger.info(
                        f"Successfully updated normal fields for product {product_id}"
                    )

                if custom_fields:
                    custom_payload = {
                        "ProductID": product_id,
                        "CustomColumns": custom_fields,
                    }

                    logger.info(
                        f"Updating {len(custom_fields)} custom columns for product {product_id} (attempt {attempt + 1}/{max_retries})"
                    )
                    await self._make_request(
                        "PUT", "/Products/CustomColumns", data=custom_payload
                    )
                    logger.info(
                        f"Successfully updated custom columns for product {product_id}"
                    )

                logger.info(
                    f"Successfully submitted listing for product {product_id} to SellerCloud"
                )
                return

            except Exception as e:
                logger.error(
                    f"Failed to update product {product_id} (attempt {attempt + 1}/{max_retries}): {e}"
                )

                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Max retries reached for product {product_id}. Giving up."
                    )
                    raise Exception(
                        f"Failed to update product {product_id} after {max_retries} attempts: {e}"
                    )

    async def _populate_description_template(
        self,
        form_data: Dict[str, Any],
        field_definitions: List[Dict[str, Any]],
        platform_id: str = "sellercloud",
    ) -> str:
        settings = await AppSettings.first()
        if not settings or not settings.field_templates:
            raise HTTPException(
                status_code=400,
                detail="Field templates not configured in settings",
            )

        template = resolve_field_template(settings.field_templates, platform_id, "description")

        if not template:
            raise HTTPException(
                status_code=400,
                detail="LongDescription template not configured in settings",
            )

        field_name_map = {}
        for field_def in field_definitions:
            field_id = field_def.get("name")
            field_title = field_def.get("title", field_id)
            if field_id:
                field_name_map[field_id] = field_title

        def _material_transform(value: Any) -> str:
            lines = [line.strip() for line in str(value).split("\n") if line.strip()]
            processed_lines = [
                re.sub(r"^Main:", "Shell:", line, flags=re.IGNORECASE) for line in lines
            ]
            return "".join(f"<div>{line}</div>" for line in processed_lines)

        # Keyed on the resolved placeholder name, which is the internal field id.
        # This was "MATERIAL" until 2026-07-30, a leftover from the SellerCloud field
        # ids that transform_listing_data_to_internal_fields.sql renamed, so the
        # transform silently never ran and the description shipped the raw value.
        # GENDER has no internal-id counterpart and stays as-is.
        value_transforms = {
            "GENDER": lambda v: GENDER_MAPPING[v] if v in GENDER_MAPPING else v,
            "material": _material_transform,
        }

        populated_template, missing_fields = render_template(
            template,
            form_data,
            value_transforms=value_transforms,
            field_labels=field_name_map,
        )

        if missing_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Required template fields missing: {', '.join(missing_fields)}",
            )

        return populated_template

    async def submit_listing_to_sellercloud(
        self,
        product_id: str,
        form_data: Dict[str, Any],
        field_definitions: List[Dict[str, Any]],
        max_workers: int = 5,
    ) -> bool:
        # Names the phase we are in, so a failure can say where it happened rather
        # than just "Failed to submit". Reported via SellerCloudSubmitError.stage.
        stage = "map_fields"
        try:
            form_data = copy.deepcopy(form_data)
            # Strip before the copy is taken: original_form_data feeds the
            # description template, and because strings are immutable, stripping
            # form_data afterwards would leave the template rendering unstripped
            # values while the SellerCloud fields got stripped ones.
            for key, value in form_data.items():
                if isinstance(value, str):
                    form_data[key] = value.strip()
            # Shallow copy on purpose: the del below must not remove
            # child_size_overrides from original_form_data, which still needs it.
            original_form_data = copy.copy(form_data)
            if "child_size_overrides" in form_data:
                del form_data["child_size_overrides"]

            sc_field_mapping = {}
            for field_def in field_definitions:
                local_name = field_def.get("name")
                if not local_name:
                    continue
                platforms = field_def.get("platforms") or []
                for platform in platforms:
                    if platform.get("platform_id") == "sellercloud":
                        sc_field_mapping[local_name] = {
                            "field_id": platform.get("field_id") or local_name,
                            "is_custom": platform.get("is_custom", False),
                        }
                        break
                if local_name not in sc_field_mapping:
                    platform_tags = field_def.get("platform_tags", [])
                    if platform_tags:
                        sc_field_mapping[local_name] = {
                            "field_id": local_name,
                            "is_custom": "custom" in platform_tags,
                        }

            transformed_form_data = {}
            for key, value in form_data.items():
                if key in sc_field_mapping:
                    sc_field_id = sc_field_mapping[key]["field_id"]
                    transformed_form_data[sc_field_id] = value
                else:
                    transformed_form_data[key] = value

            form_data = transformed_form_data
            logger.debug(
                f"Transformed {len(sc_field_mapping)} fields to SellerCloud field IDs"
            )

            if "LongDescription" in form_data and form_data["LongDescription"]:
                form_data["LongDescription"] = self._clean_html(
                    form_data["LongDescription"]
                )
                logger.info("Cleaned HTML from LongDescription field")

            if "ProductType" not in form_data or not form_data["ProductType"]:
                raise Exception(
                    "Product Type is required in form_data to fetch gender information"
                )

            logger.info("Validating required fields and fetching gender data")

            stage = "gender"
            gender = await self.get_gender_from_product_type(form_data["ProductType"])
            form_data["GENDER"] = gender
            # GENDER is derived from ProductType, not user-entered, so it is
            # absent from original_form_data. Inject it too, or a {GENDER}
            # placeholder in the description template resolves as missing.
            original_form_data["GENDER"] = gender
            # {ID} in the template is the parent SKU, which product_id already
            # holds. Overwrite rather than default: a stored empty string or a
            # stale value would otherwise reach the template unrepaired.
            original_form_data["ID"] = product_id
            logger.info(f"Added GENDER='{gender}' to form_data")

            # ShippingWeight is resolved at save time by
            # ListingService._apply_product_type_derived and is no longer in the form
            # (hide_shipping_weight_from_listing_form.sql), so this is the
            # platform-specific recalc rather than a default: it only fires for a
            # listing that reaches submit carrying no weight, i.e. a draft saved before
            # the field became derived, or one created through create_listing's
            # degraded branches, which skip derivation. spo_service has the mirror of
            # this. item_weight_oz is NOT NULL on prod with every row populated, but it
            # is nullable on TEST where 5 types have none, so the 404 below is reachable
            # there.
            stage = "weight"
            if not str(form_data.get("ShippingWeight") or "").strip():
                type_info = await listing_options_service.get_product_type_info(
                    form_data["ProductType"]
                )
                item_weight_oz = type_info.get("item_weight_oz")
                if item_weight_oz is None:
                    # Nothing downstream validates weight: the payload loop drops
                    # empty values, so falling through here would ship the product
                    # with no weight at all and still report success.
                    logger.error(
                        f"Item weight not found for ProductType: {form_data['ProductType']}"
                    )
                    raise HTTPException(
                        status_code=404,
                        detail="Item weight not found in Listing Options",
                    )
                form_data["ShippingWeight"] = int(item_weight_oz)
                logger.info(
                    f"Derived ShippingWeight={int(item_weight_oz)} from ProductType "
                    f"'{form_data['ProductType']}' (listing had none)"
                )

            logger.info("Populating product description template")
            stage = "description"
            populated_description = await self._populate_description_template(
                original_form_data, field_definitions
            )
            form_data["LongDescription"] = populated_description
            form_data["HTMLDESCRIPTION_FIXED"] = populated_description
            logger.info(
                f"Successfully populated description template ({len(populated_description)} chars)"
            )

            # product_id is listing.product_id, which is already a parent, so this is
            # normally a no-op identity resolve. It is kept so that a legacy listing
            # still holding a chopped phantom parent fails loudly and by name here,
            # rather than quietly resolving to an empty child set and writing nothing.
            stage = "resolve_parent"
            parent_product_id = await resolve_parent(product_id)

            logger.info(f"Fetching children for parent product {parent_product_id}")
            stage = "fetch_children"
            # Deliberately active-only (include_inactive defaults to False): a
            # disabled SKU must never enter all_product_ids below and get written to.
            children_data = await self.get_product_children(parent_product_id)

            child_size_overrides = original_form_data.get("child_size_overrides", {})

            size_map = {}
            for child in children_data.get("children", []):
                child_id = child["id"]
                size_map[child_id] = child_size_overrides.get(child_id, child["size"])

            # Update every variant child SKU. The parent SKU itself is only
            # updated for single-SKU products, and only when the listing
            # actually mapped a size for it (present in child_size_overrides);
            # otherwise skip it. Multi-variant matrix parents are inactive and
            # never returned by get_product_children anyway.
            all_product_ids = []
            for child in children_data.get("children", []):
                child_id = child.get("id")
                if not child_id:
                    continue
                if child_id == parent_product_id and child_id not in child_size_overrides:
                    continue
                all_product_ids.append(child_id)

            if not all_product_ids:
                raise Exception(
                    f"No products found for {parent_product_id} in SellerCloud."
                )

            logger.info(
                f"Updating {len(all_product_ids)} SellerCloud product(s) for {parent_product_id}"
            )

            list_price = form_data.get("ListPrice", "")

            sellercloud_field_map = {}
            for field_def in field_definitions:
                local_name = field_def.get("name")
                if not local_name:
                    continue
                platforms = field_def.get("platforms") or []
                for platform in platforms:
                    if platform.get("platform_id") == "sellercloud":
                        platform_tags = platform.get("platform_tags") or []
                        sellercloud_field_map[local_name] = {
                            "field_id": platform.get("field_id"),
                            "is_custom": platform.get("is_custom", False),
                            "platform_tags": platform_tags,
                        }
                        break
                if local_name not in sellercloud_field_map:
                    platform_tags = field_def.get("platform_tags", [])
                    if platform_tags:
                        sellercloud_field_map[local_name] = {
                            "field_id": local_name,
                            "is_custom": "custom" in platform_tags,
                            "platform_tags": platform_tags,
                        }

            normal_fields = []
            custom_fields = []

            for field_name, field_value in form_data.items():
                if field_value is None or field_value == "":
                    continue

                if field_name in SKIP_FIELDS:
                    continue

                if field_name in CUSTOM_COLUMN_FIELDS:
                    custom_fields.append(
                        {"ColumnName": field_name, "Value": field_value}
                    )
                    continue

                sc_mapping = sellercloud_field_map.get(field_name)

                if sc_mapping:
                    sc_field_id = sc_mapping["field_id"]
                    is_custom = sc_mapping["is_custom"]

                    if is_custom:
                        custom_fields.append(
                            {"ColumnName": sc_field_id, "Value": field_value}
                        )
                    else:
                        if sc_field_id == "ShippingWeight":
                            try:
                                total_oz = int(field_value)
                                lbs = total_oz // 16
                                oz = total_oz % 16
                                normal_fields.append(
                                    {"Name": "PackageWeightLbs", "Value": lbs}
                                )
                                normal_fields.append(
                                    {"Name": "PackageWeightOz", "Value": oz}
                                )
                                continue
                            except (ValueError, TypeError) as e:
                                logger.error(
                                    f"Failed to convert ShippingWeight to int: {field_value}, error: {e}"
                                )
                                raise ValueError(
                                    f"ShippingWeight must be convertible to integer, got: {field_value}"
                                )

                        target_field_name = FIELD_NAME_OVERRIDES.get(
                            sc_field_id, sc_field_id
                        )
                        normal_fields.append(
                            {"Name": target_field_name, "Value": field_value}
                        )
                else:
                    if field_name not in CUSTOM_COLUMN_FIELDS:
                        target_field_name = FIELD_NAME_OVERRIDES.get(
                            field_name, field_name
                        )

                        if field_name == "ShippingWeight":
                            try:
                                total_oz = int(field_value)
                                lbs = total_oz // 16
                                oz = total_oz % 16

                                normal_fields.append(
                                    {"Name": "PackageWeightLbs", "Value": lbs}
                                )

                                normal_fields.append(
                                    {"Name": "PackageWeightOz", "Value": oz}
                                )

                                continue
                            except (ValueError, TypeError) as e:
                                logger.error(
                                    f"Failed to convert ShippingWeight to int: {field_value}, error: {e}"
                                )
                                raise ValueError(
                                    f"ShippingWeight must be convertible to integer, got: {field_value}"
                                )

                        normal_fields.append(
                            {"Name": target_field_name, "Value": field_value}
                        )

            stage = "update_children"
            semaphore = asyncio.Semaphore(max_workers)
            # Collected per child rather than letting the first failure escape the
            # gather. One failing child still fails the whole submission (raised
            # below), but every child now runs to a known outcome instead of the
            # siblings continuing detached with nobody reading their result. Same
            # shape as update_children_basic_info above.
            failures: List[Dict[str, str]] = []
            succeeded: List[str] = []

            async def update_with_semaphore(pid: str):
                async with semaphore:
                    child_normal_fields = [
                        {"Name": f["Name"], "Value": f["Value"]} for f in normal_fields
                    ]

                    child_custom_fields = [
                        {"ColumnName": f["ColumnName"], "Value": f["Value"]}
                        for f in custom_fields
                    ]

                    size = size_map.get(pid, "")
                    if size and list_price:
                        for field in child_normal_fields:
                            if field["Name"] == "ProductName":
                                field["Value"] = (
                                    f"{field['Value']} SIZE {size} ${list_price}"
                                )
                                break

                    if size:
                        child_custom_fields.append(
                            {"ColumnName": "SIZE", "Value": size}
                        )

                    try:
                        await self._update_single_product_with_retry(
                            pid, child_normal_fields, child_custom_fields
                        )
                        succeeded.append(pid)
                    except Exception as e:
                        logger.error(f"SellerCloud update failed for child {pid}: {e}")
                        failures.append({"sku": pid, "error": str(e)[:300]})

            logger.info(f"Starting concurrent updates with max_workers={max_workers}")
            update_tasks = [update_with_semaphore(pid) for pid in all_product_ids]

            await asyncio.gather(*update_tasks)

            if failures:
                # The successful children are already written on the SellerCloud
                # side and are not rolled back, so they are reported too.
                raise SellerCloudSubmitError(
                    f"{len(failures)} of {len(all_product_ids)} child products failed",
                    stage=stage,
                    failures=failures,
                    succeeded=succeeded,
                )

            logger.info(
                f"Successfully submitted listing to SellerCloud for all {len(all_product_ids)} child products"
            )
            return True

        except SellerCloudSubmitError:
            raise
        except HTTPException as e:
            logger.error(f"Failed to submit listing to SellerCloud: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"Failed to submit listing to SellerCloud: {traceback.format_exc()}"
            )
            raise SellerCloudSubmitError(
                "Failed to submit to SellerCloud", stage=stage
            ) from e

    async def disable_product(self, product_id: str) -> bool:
        try:
            logger.info(f"Disabling product {product_id} in SellerCloud")

            payload = {"ProductID": product_id, "IsActive": False}

            logger.info(f"Disabling product {product_id} with payload: {payload}")

            response = await self._make_request(
                "PUT", "/Catalog/BasicInfo", data=payload
            )

            logger.info(f"Successfully disabled product {product_id}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to disable product {product_id}: {traceback.format_exc()}"
            )
            raise Exception(f"Failed to make product {product_id} inactive")

    async def initialize(self):
        logger.info("Initializing SellerCloud service with immediate authentication")
        await self._ensure_authenticated()
        logger.info("SellerCloud service initialized successfully")
        return self

    async def close(self):
        if self._token_refresh_task and not self._token_refresh_task.done():
            logger.info("Stopping background token refresh task...")

            if self._shutdown_event:
                self._shutdown_event.set()

            try:
                await asyncio.wait_for(self._token_refresh_task, timeout=5.0)
                logger.info("Background token refresh task stopped gracefully")
            except asyncio.TimeoutError:
                logger.warning(
                    "Background token refresh task did not stop in time, cancelling..."
                )
                self._token_refresh_task.cancel()
                try:
                    await self._token_refresh_task
                except asyncio.CancelledError:
                    logger.info("Background token refresh task cancelled")
            except Exception as e:
                logger.error(f"Error stopping background token refresh task: {e}")

        if self.client and not self.client.is_closed:
            await self.client.aclose()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


sellercloud_service = SellerCloudService()
