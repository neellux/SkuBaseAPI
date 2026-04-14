import asyncio
import logging

from config import CORS_ORIGINS, LOGGING_LEVEL, TORTOISE_ORM_CONFIG, config
from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from middleware.AuthMiddleware import AuthMiddleware
from middleware.CORSMiddleware import CORSMiddleware
from routes.api_routes import router as api_router
from routes.image_routes import router as image_router
from routes.listing_routes import router as listing_router
from routes.product_routes import router as product_router
from routes.settings_routes import router as settings_router
from routes.template_routes import router as template_router
from listingoptions.routes.table_routes import router as lo_table_router
from listingoptions.routes.list_routes import router as lo_list_router
from listingoptions.routes.platform_routes import router as lo_platform_router
from listingoptions.routes.sizing_routes import router as lo_sizing_router
from listingoptions.routes.sizing_lists_routes import router as lo_sizing_lists_router
from listingoptions.routes.parent_type_routes import router as lo_parent_type_router
from listingoptions.routes.api_routes import router as lo_api_router
from services.image_service import image_service
from services.sellercloud_internal_service import sellercloud_internal_service
from services.sellercloud_service import sellercloud_service
from services.spo_poller import spo_poller
from services.submission_poller import submission_poller
from tortoise.contrib.fastapi import register_tortoise
from utils.load_app_data import app_users, load_app_data

logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Listing API",
    description="API for managing dynamic database tables and mapping lists.",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    default_response_class=ORJSONResponse,
)

api_app = FastAPI(
    title="Listing Public API",
    description="Publicly accessible endpoints.",
    version="1.0.0",
    redoc_url=None,
    default_response_class=ORJSONResponse,
)

app.add_middleware(AuthMiddleware, excluded_paths=["/api", "/docs", "/openapi.json"])
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.add_middleware(
    GZipMiddleware,
    minimum_size=1000,
)


app.include_router(template_router)
app.include_router(listing_router)
app.include_router(settings_router)
app.include_router(product_router)
app.include_router(image_router)

app.include_router(lo_table_router, prefix="/listingoptions")
app.include_router(lo_list_router, prefix="/listingoptions")
app.include_router(lo_platform_router, prefix="/listingoptions")
app.include_router(lo_sizing_router, prefix="/listingoptions")
app.include_router(lo_sizing_lists_router, prefix="/listingoptions")
app.include_router(lo_parent_type_router, prefix="/listingoptions")

api_app.include_router(api_router)
api_app.include_router(lo_api_router, prefix="/listingoptions")
app.mount("/api", api_app)

register_tortoise(
    app,
    config=TORTOISE_ORM_CONFIG,
    add_exception_handlers=True,
    generate_schemas=False,
)


async def periodic_tasks_1min():
    while True:
        await load_app_data()
        await asyncio.sleep(1 * 60)


@app.on_event("startup")
async def startup_event():
    logger.info("Listing API startup...")

    logger.info("Initializing SellerCloud service...")
    await sellercloud_service.initialize()
    logger.info("SellerCloud service initialized successfully")

    logger.info("Initializing SellerCloud Internal service...")
    await sellercloud_internal_service.initialize()
    logger.info("SellerCloud Internal service initialized successfully")

    logger.info("Initializing Image service...")
    await image_service.initialize()
    logger.info("Image service initialized successfully")

    asyncio.create_task(periodic_tasks_1min())

    await submission_poller.start()
    await spo_poller.start()


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Listing API shutdown...")

    await spo_poller.stop()
    await submission_poller.stop()

    logger.info("Closing SellerCloud service...")
    await sellercloud_service.close()
    logger.info("SellerCloud service closed successfully")

    logger.info("Closing SellerCloud Internal service...")
    await sellercloud_internal_service.close()
    logger.info("SellerCloud Internal service closed successfully")

    logger.info("Closing Image service...")
    await image_service.close()
    logger.info("Image service closed successfully")


@app.get("/")
async def root():
    return {
        "message": "Listing API",
        "version": "1.0.0",
    }


@app.get("/app_settings")
async def user_access(request: Request):
    return {"permissions": request.state.user.get("permissions", [])}


@app.get("/app_users")
async def get_app_users():
    users = [
        {"id": user_id, "name": user_data["name"]}
        for user_id, user_data in app_users.items()
        if any(role.startswith(config["auth"]["short_name"] + "_") for role in user_data["roles"])
        and not any(role.endswith("_dev") for role in user_data["roles"])
    ]
    return users


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {str(exc)}")
    return ORJSONResponse(status_code=500, content={"error": "Internal server error"})
