import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import httpx
import logging
import orjson
import pandas as pd

logger = logging.getLogger(__name__)


class SpreadsheetService:
    def __init__(self):
        self.appscript_url = os.environ.get("APPSCRIPT_URL")
        self.appscript_secret = os.environ.get("APPSCRIPT_SECRET")
        self.rate_limit = timedelta(
            seconds=int(os.environ.get("SPREADSHEET_UPDATE_RATE_LIMIT_SECONDS", 30))
        )

        self._table_states: Dict[str, Dict[str, Any]] = {}

        if not self.appscript_url or not self.appscript_secret:
            logger.warning(
                "SpreadsheetService disabled: APPSCRIPT_URL or APPSCRIPT_SECRET not set."
            )
            self.enabled = False
        else:
            self.enabled = True

        self.client = httpx.AsyncClient(follow_redirects=True, timeout=120)

        if self.enabled:
            asyncio.create_task(self._periodic_checker())

    async def _periodic_checker(self):
        while True:
            await asyncio.sleep(30)
            logger.info("SpreadsheetService periodic checker running...")
            for table_name, state in list(self._table_states.items()):
                if state.get("update_needed") and not state.get("is_updating"):
                    if (
                        datetime.now() - state.get("last_update_start_time", datetime.min)
                        >= self.rate_limit
                    ):
                        logger.info(f"Periodic checker triggering update for table: {table_name}")
                        asyncio.create_task(self._run_update(table_name))

    def get_table_state(self, table_name: str) -> Dict[str, Any]:
        return self._table_states.setdefault(
            table_name,
            {
                "is_updating": False,
                "last_update_start_time": datetime.min,
                "update_needed": False,
            },
        )

    async def trigger_spreadsheet_update(self, table_name: str):
        if not self.enabled:
            return

        state = self.get_table_state(table_name)

        if state["update_needed"]:
            logger.info(f"Update for {table_name} already pending. Skipping trigger.")
            return

        if state["is_updating"]:
            logger.info(f"Update for {table_name} in progress. Marking as needed.")
            state["update_needed"] = True
            return

        time_since_last_call = datetime.now() - state["last_update_start_time"]

        if time_since_last_call < self.rate_limit:
            logger.info(f"Rate limit for {table_name} not passed. Marking as needed for later.")
            state["update_needed"] = True
            return

        logger.info(f"Immediate update triggered for table: {table_name}")
        asyncio.create_task(self._run_update(table_name))

    async def _run_update(self, table_name: str):
        from listingoptions.services.database_service import DatabaseService

        state = self.get_table_state(table_name)

        if state["is_updating"]:
            logger.warning(f"Update for {table_name} called while another update was in progress.")
            state["update_needed"] = True
            return

        state["is_updating"] = True
        state["last_update_start_time"] = datetime.now()
        state["update_needed"] = False

        try:
            logger.info(f"Exporting data for table: {table_name}")
            data = await DatabaseService.get_all_records_for_export(table_name)

            if not data:
                logger.info(f"No data to export for table {table_name}. Skipping API call.")
                state["is_updating"] = False
                if state.get("update_needed"):
                    asyncio.create_task(self.trigger_spreadsheet_update(table_name))
                return

            schema = await DatabaseService.get_table_schema(table_name)

            df = pd.DataFrame(data) if data else pd.DataFrame()

            column_map = (
                {col["name"]: col["display_name"] for col in schema.column_schema}
                if table_name != "sizes"
                else {column_name: column_name for column_name in list(df.columns)}
            )

            ordered_columns = (
                [
                    col["name"]
                    for col in sorted(schema.column_schema, key=lambda c: c.get("order", 999))
                ]
                if table_name != "sizes"
                else list(df.columns)
            )

            if table_name == "types":
                extra_cols = [
                    "division",
                    "dept",
                    "gender",
                    "class_name",
                    "reporting_category",
                ]
                for col in extra_cols:
                    if col not in ordered_columns:
                        ordered_columns.append(col)
                    if col not in column_map:
                        column_map[col] = col.replace("_", " ").title()

            if not df.empty:
                for col in df.columns:
                    if col not in ordered_columns:
                        ordered_columns.append(col)
                    if col not in column_map:
                        column_map[col] = col.replace("_", " ").title()

            final_columns = [col for col in ordered_columns if col in df.columns]
            df = df[final_columns]

            df.rename(columns=column_map, inplace=True)

            data = df.to_dict(orient="records")

            payload = {
                "secret": self.appscript_secret,
                "action": "updateSheets",
                "tableName": table_name,
                "data": data,
            }

            json_payload = orjson.dumps(payload)

            logger.info(f"Calling App Script for table: {table_name}")
            response = await self.client.post(
                self.appscript_url,
                content=json_payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            response_json = response.json()
            if response_json.get(
                "status"
            ) == "error" and "Table configs not found" in response_json.get("message", ""):
                logger.warning(
                    f"App Script returned 'Table configs not found' for {table_name}. Not treating as an error."
                )
            elif response_json.get("status") != "ok":
                logger.error(f"App Script update failed for {table_name}: {response.text}")
            else:
                logger.info(
                    f"App Script update successful for {table_name}: {response_json.get('message')}"
                )

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error calling App Script for {table_name}: {e.response.status_code} - {e.response.text}"
            )
        except Exception as e:
            logger.error(f"Error during spreadsheet update for {table_name}: {e}", exc_info=True)
        finally:
            state["is_updating"] = False
            if state.get("update_needed"):
                logger.info(
                    f"Update was requested for {table_name} during the last run. Triggering new update check."
                )
                asyncio.create_task(self.trigger_spreadsheet_update(table_name))


spreadsheet_service = SpreadsheetService()
