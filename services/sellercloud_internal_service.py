import asyncio
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
import orjson
from config import config

logger = logging.getLogger(__name__)

SELLERCLOUD_INTERNAL_CONFIG = config.get("sellercloud_internal", {})

SC_SYNC_MAX_RETRIES = 3
SC_SYNC_RETRY_DELAY = 1  # seconds

_TRANSIENT_MESSAGE_MARKERS = (
    "toolbox operation failed",
    "internal server error",
    "timeout",
    "temporarily unavailable",
)


class SellercloudPermanentError(Exception):
    """Raised when a SellerCloud operation fails in a way that retries cannot fix.
    str(e) is the user-facing message the UI should display."""


def _is_transient_message(msg: Optional[str]) -> bool:
    if not msg:
        return True
    msg_lower = msg.lower()
    return any(marker in msg_lower for marker in _TRANSIENT_MESSAGE_MARKERS)


def _classify_sc_failure(op_name: str, result: Dict[str, Any]) -> None:
    """Inspect a Success=false response and raise either SellercloudPermanentError
    (don't retry, message shown to user) or a plain Exception (retry)."""
    notification = result.get("Notification") or {}
    msg = notification.get("Message") or ""
    if _is_transient_message(msg):
        raise Exception(f"{op_name}: transient SellerCloud failure: {msg or '<empty>'}")
    raise SellercloudPermanentError(msg)


class SellercloudInternalService:
    def __init__(self):
        self.base_url = SELLERCLOUD_INTERNAL_CONFIG.get("sellercloud_baseurl", "")
        self.username = SELLERCLOUD_INTERNAL_CONFIG.get("sellercloud_username", "")
        self.password = SELLERCLOUD_INTERNAL_CONFIG.get("sellercloud_password", "")
        self.access_token: Optional[str] = None
        self.token_type: str = "Bearer"
        self.token_expires_at: Optional[datetime] = None
        self.client: Optional[httpx.AsyncClient] = None
        self._token_lock = asyncio.Lock()

        self._token_refresh_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._token_refresh_interval = 30

        self.max_retries = 3
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

            self.client = httpx.AsyncClient(timeout=httpx.Timeout(30.0), headers=headers)
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
                            logger.info(f"Raw .expires value from SellerCloud: {expires_str}")
                            self.token_expires_at = datetime.fromisoformat(expires_str)
                        else:
                            expires_in = creds.get("expires_in", 3600)
                            self.token_expires_at = datetime.now(timezone.utc) + timedelta(
                                seconds=int(expires_in)
                            )

                        client = await self._get_client()
                        client.headers.update(
                            {"Authorization": f"{self.token_type} {self.access_token}"}
                        )

                        logger.info(
                            f"SellerCloud Internal credentials updated. Token expires at {self.token_expires_at}"
                        )

                        self._start_token_refresh_task()

                        return creds

                    except Exception as e:
                        logger.error(
                            f"Failed to update credentials (attempt {attempt + 1}/{self.max_retries}): {traceback.format_exc()}"
                        )

                        if attempt < self.max_retries - 1:
                            logger.info(f"Retrying in {self.retry_delay} seconds...")
                            await asyncio.sleep(self.retry_delay)
                        else:
                            logger.error("Max retries reached. Failed to authenticate.")
                            raise Exception("Failed to authenticate with SellerCloud Internal API")
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
                    logger.info("Shutdown event detected, stopping background token refresh")
                    break

                try:
                    if self._shutdown_event:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self._token_refresh_interval,
                        )
                        logger.info("Shutdown signaled, stopping background token refresh")
                        break
                    else:
                        await asyncio.sleep(self._token_refresh_interval)
                except asyncio.TimeoutError:
                    pass

                try:
                    await self._ensure_authenticated()
                except Exception as e:
                    logger.error(f"Error during background token refresh: {e}")

            except asyncio.CancelledError:
                logger.info("Background token refresh task cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in background token refresh: {e}")
                await asyncio.sleep(5)

        logger.info("Background token refresh task stopped")

    def _start_token_refresh_task(self) -> None:
        if self._token_refresh_task is None or self._token_refresh_task.done():
            if self._shutdown_event is None:
                self._shutdown_event = asyncio.Event()

            self._token_refresh_task = asyncio.create_task(self._background_token_refresh())
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

            logger.debug(f"Response status: {response.status_code}")
            response.raise_for_status()
            return response

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {method} {url} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {method} {url} - {e}")
            raise

    async def post(self, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        response = await self._make_request("POST", endpoint, data=data)
        return orjson.loads(response.content)

    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        response = await self._make_request("GET", endpoint, params=params)
        return orjson.loads(response.content)

    async def _get_all_warehouses(self, sku: str) -> List[Dict[str, Any]]:
        all_warehouses = []
        page = 1
        while True:
            response = await self.get(
                "/Inventory/Warehouse/Get",
                params={
                    "request[productID]": sku,
                    "request[pageNumber]": str(page),
                    "request[pageSize]": "50",
                    "request[warehouseInventoryFilter]": "0",
                },
            )
            warehouses = response.get("Data", {}).get("Warehouses", [])
            all_warehouses.extend(warehouses)
            total = response.get("Data", {}).get("Total", 0)
            if len(all_warehouses) >= total or not warehouses:
                break
            page += 1
        return all_warehouses

    async def _get_all_bins(self, sku: str) -> List[Dict[str, Any]]:
        all_bins = []
        page = 1
        while True:
            response = await self.get(
                "/Inventory/WarehouseBin/GetInventoryInfo",
                params={
                    "request[productID]": sku,
                    "request[pageNumber]": str(page),
                    "request[pageSize]": "50",
                },
            )
            bins = response.get("Data", {}).get("Bins", [])
            all_bins.extend(bins)
            total = response.get("Data", {}).get("TotalItems", 0)
            if len(all_bins) >= total or not bins:
                break
            page += 1
        return all_bins

    async def get_product_warehouses(self, sku: str) -> Dict[str, Any]:
        params = {
            "request[productID]": sku,
            "request[pageNumber]": "1",
            "request[pageSize]": "50",
            "request[warehouseID]": "",
            "request[warehouseInventoryFilter]": "0",
            "request[OrderColumn]": "",
            "request[IsAscending]": "true",
        }
        try:
            result = await self.get("/Inventory/Warehouse/Get", params=params)
            if result.get("Success"):
                return {
                    "success": True,
                    "warehouses": result.get("Data", {}).get("Warehouses", []),
                    "total": result.get("Data", {}).get("Total", 0),
                }
            return {"success": False, "error": "API returned failure", "warehouses": []}
        except Exception as e:
            logger.error(f"Failed to get warehouses for {sku}: {e}")
            return {"success": False, "error": str(e), "warehouses": []}

    async def get_warehouse_bins(self, sku: str, warehouse_id: int) -> Dict[str, Any]:
        params = {
            "request[productID]": sku,
            "request[pageNumber]": "1",
            "request[pageSize]": "50",
        }
        try:
            result = await self.get("/Inventory/WarehouseBin/GetInventoryInfo", params=params)
            if result.get("Success"):
                all_bins = result.get("Data", {}).get("Bins", [])
                warehouse_bins = [b for b in all_bins if b.get("WarehouseID") == warehouse_id]
                return {
                    "success": True,
                    "bins": warehouse_bins,
                    "total": len(warehouse_bins),
                }
            return {"success": False, "error": "API returned failure", "bins": []}
        except Exception as e:
            logger.error(f"Failed to get bins for {sku} in warehouse {warehouse_id}: {e}")
            return {"success": False, "error": str(e), "bins": []}

    async def get_inventory_preview(self, sku: str) -> Dict[str, Any]:
        result = {
            "sku": sku,
            "warehouses": [],
            "summary": {
                "total_qty": 0,
                "sellable_qty": 0,
                "non_sellable_qty": 0,
                "warehouse_count": 0,
                "bin_count": 0,
            },
        }

        try:
            all_warehouses = await self._get_all_warehouses(sku)

            all_bins = await self._get_all_bins(sku)

            bins_by_warehouse: Dict[int, List[Dict[str, Any]]] = {}
            for bin_item in all_bins:
                wh_id = bin_item.get("WarehouseID")
                if wh_id not in bins_by_warehouse:
                    bins_by_warehouse[wh_id] = []
                bins_by_warehouse[wh_id].append(bin_item)

            for wh in all_warehouses:
                qty = int(wh.get("PhysicalQty", 0))
                if qty <= 0:
                    continue

                wh_data = {
                    "id": wh["ID"],
                    "name": wh["Name"],
                    "qty": qty,
                    "is_sellable": wh.get("IsSellable", True),
                    "enforce_bins": wh.get("EnforceBins", False),
                    "bins": [],
                }

                if wh_data["enforce_bins"] and wh["ID"] in bins_by_warehouse:
                    for bin_item in bins_by_warehouse[wh["ID"]]:
                        bin_qty = int(bin_item.get("QtyAvailable", 0))
                        if bin_qty > 0:
                            wh_data["bins"].append(
                                {
                                    "id": bin_item["BinID"],
                                    "name": bin_item["BinName"],
                                    "qty": bin_qty,
                                    "is_sellable": bin_item.get("IsSellable", True),
                                }
                            )
                            result["summary"]["bin_count"] += 1

                result["warehouses"].append(wh_data)
                result["summary"]["warehouse_count"] += 1
                result["summary"]["total_qty"] += qty
                if wh_data["is_sellable"]:
                    result["summary"]["sellable_qty"] += qty
                else:
                    result["summary"]["non_sellable_qty"] += qty

        except Exception as e:
            logger.error(f"Failed to get inventory preview for {sku}: {e}")
            result["error"] = str(e)

        return result

    async def transfer_inventory(
        self,
        from_sku: str,
        to_sku: str,
        warehouse_id: int,
        qty: int,
        from_bin: int = -1,
        to_bin: int = -1,
        reason: str = "Parent Change Transfer",
    ) -> Dict[str, Any]:
        payload = {
            "FromWarehouseID": str(warehouse_id),
            "ToWarehouseID": warehouse_id,
            "FromSKU": from_sku,
            "ToSKU": to_sku,
            "Qty": qty,
            "TransferReason": reason,
            "SerialNumbers": "",
            "LotNumber": "",
            "FromBin": from_bin,
            "ToBin": to_bin,
        }
        try:
            result = await self.post("/Transfer/SkuToSkuTransfer/CreateNewTransfer", data=payload)
            if result.get("Success"):
                return {
                    "success": True,
                    "warehouse_id": warehouse_id,
                    "qty": qty,
                    "from_bin": from_bin,
                    "to_bin": to_bin,
                }
            return {
                "success": False,
                "warehouse_id": warehouse_id,
                "qty": qty,
                "error": result.get("Notification", {}).get("Message", "Transfer failed"),
            }
        except Exception as e:
            logger.error(f"Transfer failed {from_sku} -> {to_sku}: {e}")
            return {
                "success": False,
                "warehouse_id": warehouse_id,
                "qty": qty,
                "error": str(e),
            }

    async def transfer_all_inventory(
        self,
        from_sku: str,
        to_sku: str,
    ) -> Dict[str, Any]:
        result = {
            "success": False,
            "from_sku": from_sku,
            "to_sku": to_sku,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "warehouses": [],
            "summary": {
                "total_qty": 0,
                "transferred_qty": 0,
                "failed_qty": 0,
            },
        }

        try:
            all_warehouses = await self._get_all_warehouses(from_sku)
        except Exception as e:
            result["error"] = f"Failed to fetch warehouses: {str(e)}"
            return result

        warehouses_with_qty = [w for w in all_warehouses if w.get("PhysicalQty", 0) > 0]

        if not warehouses_with_qty:
            result["success"] = True
            result["no_inventory"] = True
            return result

        try:
            all_bins = await self._get_all_bins(from_sku)
        except Exception as e:
            logger.warning(f"Failed to fetch bins for {from_sku}: {e}")
            all_bins = []

        bins_by_warehouse: Dict[int, List[Dict[str, Any]]] = {}
        for bin_item in all_bins:
            wh_id = bin_item.get("WarehouseID")
            if wh_id not in bins_by_warehouse:
                bins_by_warehouse[wh_id] = []
            bins_by_warehouse[wh_id].append(bin_item)

        for warehouse in warehouses_with_qty:
            wh_id = warehouse.get("ID")
            wh_name = warehouse.get("Name", "Unknown")
            enforce_bins = warehouse.get("EnforceBins", False)
            physical_qty = int(warehouse.get("PhysicalQty", 0))

            wh_result = {
                "id": wh_id,
                "name": wh_name,
                "bins": enforce_bins,
                "status": "pending",
            }

            if not enforce_bins:
                wh_result["qty"] = physical_qty
                result["summary"]["total_qty"] += physical_qty

                transfer_resp = await self.transfer_inventory(
                    from_sku=from_sku,
                    to_sku=to_sku,
                    warehouse_id=wh_id,
                    qty=physical_qty,
                    from_bin=-1,
                    to_bin=-1,
                )

                if transfer_resp.get("success"):
                    wh_result["status"] = "completed"
                    result["summary"]["transferred_qty"] += physical_qty
                else:
                    wh_result["status"] = "failed"
                    wh_result["error"] = transfer_resp.get("error")
                    result["summary"]["failed_qty"] += physical_qty
            else:
                warehouse_bins = bins_by_warehouse.get(wh_id, [])
                bins_with_qty = [b for b in warehouse_bins if b.get("QtyAvailable", 0) > 0]

                if not bins_with_qty:
                    wh_result["status"] = "skipped"
                    wh_result["error"] = "No bins with inventory"
                    result["warehouses"].append(wh_result)
                    continue

                wh_result["transfers"] = []
                wh_completed = 0
                wh_failed = 0

                for bin_info in bins_with_qty:
                    bin_id = bin_info.get("BinID")
                    bin_qty = int(bin_info.get("QtyAvailable", 0))
                    result["summary"]["total_qty"] += bin_qty

                    transfer_resp = await self.transfer_inventory(
                        from_sku=from_sku,
                        to_sku=to_sku,
                        warehouse_id=wh_id,
                        qty=bin_qty,
                        from_bin=bin_id,
                        to_bin=bin_id,
                    )

                    bin_transfer = {
                        "bin": bin_id,
                        "name": bin_info.get("BinName", "Unknown"),
                        "qty": bin_qty,
                    }

                    if transfer_resp.get("success"):
                        bin_transfer["status"] = "completed"
                        result["summary"]["transferred_qty"] += bin_qty
                        wh_completed += bin_qty
                    else:
                        bin_transfer["status"] = "failed"
                        bin_transfer["error"] = transfer_resp.get("error")
                        result["summary"]["failed_qty"] += bin_qty
                        wh_failed += bin_qty

                    wh_result["transfers"].append(bin_transfer)

                if wh_failed == 0:
                    wh_result["status"] = "completed"
                elif wh_completed == 0:
                    wh_result["status"] = "failed"
                else:
                    wh_result["status"] = "partial"

            result["warehouses"].append(wh_result)

        result["completed_at"] = datetime.now(timezone.utc).isoformat()

        if result["summary"]["failed_qty"] == 0 and result["summary"]["transferred_qty"] > 0:
            result["success"] = True
        elif result["summary"]["transferred_qty"] > 0:
            result["success"] = True
            result["partial"] = True

        return result

    async def validate_alias(self, product_id: str, alias: str) -> Dict[str, Any]:
        return await self.post(
            "/Toolbox/Product/AliasesTool/ValidateAliasNameForAdd",
            data={"ProductId": product_id, "Alias": alias},
        )

    async def save_alias(self, product_id: str, alias: str, action: str = "add") -> Dict[str, Any]:
        row_status = 2 if action == "add" else 1
        payload = {
            "ToolParameters": {"Id": product_id},
            "DTO": {
                "ShadowOfProductId": "",
                "Aliases": [{"RowStatus": row_status, "Name": alias}],
            },
        }
        logger.info(f"save_alias payload: {payload}")
        result = await self.post("/Toolbox/Product/AliasesTool/Save", data=payload)
        logger.info(f"save_alias response: {result}")
        return result

    async def load_aliases(self, product_id: str) -> Dict[str, Any]:
        return await self.post(
            "/Toolbox/Product/AliasesTool/Load",
            data={"Id": product_id},
        )

    async def _retry_sc(self, op_name: str, coro_factory):
        last_error: Optional[Exception] = None
        for attempt in range(1, SC_SYNC_MAX_RETRIES + 1):
            try:
                return await coro_factory()
            except SellercloudPermanentError:
                raise
            except Exception as e:
                last_error = e
                logger.warning(
                    f"{op_name} attempt {attempt}/{SC_SYNC_MAX_RETRIES} failed: {e}"
                )
                if attempt < SC_SYNC_MAX_RETRIES:
                    await asyncio.sleep(SC_SYNC_RETRY_DELAY)
        assert last_error is not None
        raise last_error

    def _check_sc_success(self, op_name: str, result: Dict[str, Any]) -> None:
        if not result.get("Success"):
            _classify_sc_failure(op_name, result)

    async def sync_add_alias(self, sku: str, value: str, is_primary: bool) -> None:
        """Validate + add alias; if primary, also update BasicInfo UPC. Retries toolbox failures."""

        async def _do_validate():
            validation = await self.validate_alias(sku, value)
            if not validation.get("IsValid"):
                already = validation.get("AlreadyUsedForProduct")
                if already:
                    raise SellercloudPermanentError(
                        f"UPC {value} is already used by another product (ID: {already})"
                    )
                error_msg = validation.get("ErrorMessage") or (
                    validation.get("Notification") or {}
                ).get("Message", "")
                if _is_transient_message(error_msg):
                    raise Exception(f"validate_alias transient failure: {error_msg}")
                raise SellercloudPermanentError(
                    error_msg or f"UPC {value} failed validation in SellerCloud"
                )
            return validation

        async def _do_save():
            result = await self.save_alias(sku, value, action="add")
            self._check_sc_success(f"save_alias(add {value} to {sku})", result)
            return result

        async def _do_basicinfo():
            from services.sellercloud_service import sellercloud_service

            result = await sellercloud_service.update_product_upc(sku, value)
            if not result.get("success"):
                raise Exception(f"update_product_upc failed: {result}")
            return result

        await self._retry_sc(f"validate_alias({sku},{value})", _do_validate)
        await self._retry_sc(f"save_alias(add {sku},{value})", _do_save)
        if is_primary:
            await self._retry_sc(f"update_product_upc({sku},{value})", _do_basicinfo)

    async def sync_delete_alias(self, sku: str, value: str) -> None:
        """Delete alias from SellerCloud. Retries toolbox failures; tolerates 'not found'."""

        async def _do_delete():
            result = await self.save_alias(sku, value, action="delete")
            if not result.get("Success"):
                msg = (result.get("Notification") or {}).get("Message", "") or ""
                if "not found" in msg.lower() or "does not exist" in msg.lower():
                    logger.info(f"Alias {value} already absent from {sku} in SellerCloud")
                    return result
                self._check_sc_success(f"save_alias(delete {value} from {sku})", result)
            return result

        await self._retry_sc(f"save_alias(delete {sku},{value})", _do_delete)

    async def sync_change_primary(
        self, sku: str, new_primary: str, old_primary: Optional[str]
    ) -> None:
        """Demote old primary to alias, remove new primary from aliases, update BasicInfo UPC."""

        async def _do_load():
            aliases_response = await self.load_aliases(sku)
            if aliases_response.get("Success") is False:
                self._check_sc_success(f"load_aliases({sku})", aliases_response)
            dto = (aliases_response.get("Data") or {}).get("DTO") or {}
            return {
                a.get("Name")
                for a in (dto.get("Aliases") or [])
                if a.get("Name")
            }

        async def _do_add_old(alias: str):
            async def _inner():
                result = await self.save_alias(sku, alias, action="add")
                self._check_sc_success(f"save_alias(add old primary {alias})", result)
                return result

            return await self._retry_sc(
                f"save_alias(add old primary {alias} to {sku})", _inner
            )

        async def _do_remove_new(alias: str):
            async def _inner():
                result = await self.save_alias(sku, alias, action="delete")
                self._check_sc_success(
                    f"save_alias(remove new primary {alias})", result
                )
                return result

            return await self._retry_sc(
                f"save_alias(remove new primary {alias} from {sku})", _inner
            )

        async def _do_basicinfo():
            from services.sellercloud_service import sellercloud_service

            result = await sellercloud_service.update_product_upc(sku, new_primary)
            if not result.get("success"):
                raise Exception(f"update_product_upc failed: {result}")
            return result

        existing = await self._retry_sc(f"load_aliases({sku})", _do_load)

        if old_primary and old_primary not in existing:
            await _do_add_old(old_primary)

        if new_primary in existing:
            await _do_remove_new(new_primary)

        await self._retry_sc(
            f"update_product_upc({sku},{new_primary})", _do_basicinfo
        )

    async def initialize(self):
        logger.info("Initializing SellerCloud Internal service...")
        await self._ensure_authenticated()
        logger.info("SellerCloud Internal service initialized successfully")
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
                logger.warning("Task did not stop in time, cancelling...")
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


sellercloud_internal_service = SellercloudInternalService()
