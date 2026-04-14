import asyncio
import logging
from copy import deepcopy
from typing import Any, List

import httpx
import orjson
from config import config
from fastapi import HTTPException

logger = logging.getLogger(__name__)

app_users = {}
app_settings = {}
lock = asyncio.Lock()


refresh_interval = config["auth"]["app_data_refresh_interval"]
max_retries = config["auth"].get("app_data_max_retries", 3)
retry_delay = config["auth"].get("app_data_retry_delay", 1)
client = httpx.AsyncClient()


async def load_app_data():
    global app_users, app_settings

    for attempt in range(max_retries):
        try:
            response = await client.get(
                config["auth"]["auth_endpoint"] + "/app_data/users",
                params={"short_name": config["auth"]["short_name"]},
            )
            response.raise_for_status()
            new_users = orjson.loads(response.content)

            response = await client.get(
                config["auth"]["auth_endpoint"] + "/app_data/settings",
                params={"short_name": config["auth"]["short_name"]},
            )
            response.raise_for_status()
            new_settings = orjson.loads(response.content)

            async with lock:
                if new_users:
                    app_users.clear()
                    app_users.update(new_users)

                if new_settings:
                    app_settings.clear()
                    app_settings.update(new_settings)

            return
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)


async def get_settings():
    async with lock:
        return deepcopy(app_settings)


async def get_users():
    async with lock:
        return deepcopy(app_users)


async def start_app_data_refresh():
    while True:
        try:
            await load_app_data()
        except Exception as e:
            logger.error(f"Error refreshing app data: {e}")
        await asyncio.sleep(refresh_interval)


async def add_user_data(
    data: Any,
    keys: List[str],
    new_keys: List[str],
    separator: str = "_",
) -> Any:
    SUPPORTED_KEYS = {"name"}

    if not set(new_keys).issubset(SUPPORTED_KEYS):
        raise ValueError(f"Invalid keys. Supported keys are: {SUPPORTED_KEYS}")

    if not data:
        return data

    user_data = await get_users()
    is_dict = isinstance(data, dict)
    items = [data] if is_dict else data

    for item in items:
        is_item_dict = isinstance(item, dict)
        for old_key in keys:
            if (is_item_dict and old_key not in item) or (
                not is_item_dict and not hasattr(item, old_key)
            ):
                continue

            prefix = old_key.rstrip("_id")
            user_id = item.get(old_key) if is_item_dict else getattr(item, old_key)

            if user_id and user_id in user_data:
                update_values = {
                    f"{prefix}{separator}{key}": user_data[user_id].get(key, "") for key in new_keys
                }
            else:
                update_values = {f"{prefix}{separator}{key}": "" for key in new_keys}

            if is_item_dict:
                item.update(update_values)
            else:
                for new_key, value in update_values.items():
                    if hasattr(item, new_key):
                        setattr(item, new_key, value)

    return items[0] if is_dict else items


async def verify_user(email: str, password: str, app_short_name: str, roles: List[str]):
    response = await client.post(
        config["auth"]["auth_endpoint"] + "/verify_user",
        json={"email": email, "password": password, "app_short_name": app_short_name},
    )
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Invalid credentials")
    if not any(role in response.json()["roles"] for role in roles):
        raise HTTPException(
            status_code=403,
            detail="User does not have required permissions to validate the action.",
        )
    return response.json()
