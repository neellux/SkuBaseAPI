import logging

from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger(__name__)


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
    """Convert unhandled exceptions into a 500 response.

    This must be registered INSIDE CORSMiddleware so the error response
    travels back out through the CORS layer and receives the
    Access-Control-Allow-Origin header. The default
    @app.exception_handler(Exception) runs in Starlette's
    ServerErrorMiddleware, which sits outside all user middleware, so its
    response never gets CORS headers; the browser then blocks it and the UI
    reports a generic network/connection error instead of the real 500.
    """

    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            logger.error(f"Unhandled exception: {exc}", exc_info=True)
            return ORJSONResponse(
                status_code=500, content={"detail": "Internal server error"}
            )
