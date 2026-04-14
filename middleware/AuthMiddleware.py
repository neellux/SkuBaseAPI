import orjson
from fastapi.responses import ORJSONResponse
from httpx import AsyncClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import logging
from typing import Optional, List
import httpx

from config import config

auth_client = AsyncClient(timeout=60)
logger = logging.getLogger(__name__)


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, excluded_paths: Optional[List[str]] = None):
        super().__init__(app)
        self.excluded_paths = excluded_paths or []

    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS" or any(
            request.url.path.startswith(path) for path in self.excluded_paths
        ):
            return await call_next(request)

        if not request.headers.get("Cookie"):
            return ORJSONResponse(status_code=401, content={"detail": "Unauthorized"})

        headers = {
            "X-REQUEST-PATH": request.url.path,
            "X-REQUEST-METHOD": request.method,
            "X-APP-SHORT-NAME": config["auth"]["short_name"],
            "Cookie": request.headers.get("Cookie"),
        }

        try:
            response = await auth_client.get(
                url=f"{config['auth']['auth_endpoint']}/auth",
                headers=headers,
            )
            response.raise_for_status()
            request.state.user = orjson.loads(response.content)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                return ORJSONResponse(
                    status_code=403,
                    content={
                        "detail": "You do not have sufficient access to perform this action. Contact your administrator."
                    },
                )
            return ORJSONResponse(status_code=401, content={"detail": "Unauthorized"})
        except Exception as e:
            logger.error(f"Authentication request failed: {e}")
            return ORJSONResponse(
                status_code=500, content={"detail": "Internal authentication error"}
            )

        return await call_next(request)


async def verify_websocket_scope_auth(scope: dict) -> Optional[dict]:
    scope_headers = {k.decode("utf-8"): v.decode("utf-8") for k, v in scope.get("headers", [])}

    if not scope_headers.get("cookie"):
        logger.warning("WebSocket handshake: Missing Cookie for authentication.")
        return None

    auth_headers = {}
    auth_headers["X-REQUEST-PATH"] = "/auth"
    auth_headers["X-REQUEST-METHOD"] = "GET"
    auth_headers["X-APP-SHORT-NAME"] = config["auth"]["short_name"]
    auth_headers["Cookie"] = scope_headers["cookie"]

    try:
        response = await auth_client.get(
            url=config["auth"]["auth_endpoint"] + "/auth",
            headers=auth_headers,
        )
        if response.status_code == 200:
            user_data = orjson.loads(response.content)
            logger.info(f"WebSocket authentication successful for user: {user_data.get('id')}")
            return user_data
        elif response.status_code == 403:
            logger.warning(
                f"WebSocket authentication forbidden (403) for Cookie: {auth_headers['Cookie'][:30]}..."
            )
            return None
        else:
            logger.warning(
                f"WebSocket authentication failed ({response.status_code}) for Cookie: {auth_headers['Cookie'][:30]}..."
            )
            return None
    except httpx.RequestError as e:
        logger.error(f"HTTPX RequestError during WebSocket authentication: {e}")
        return None
    except orjson.JSONDecodeError as e:
        logger.error(f"ORJSONDecodeError during WebSocket authentication response processing: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during WebSocket authentication: {e}", exc_info=True)
        return None
