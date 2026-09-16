import toml

with open("config.toml", "r") as f:
    config = toml.load(f)

LOGGING_LEVEL = config.get("app", {}).get("logging_level", "INFO").upper()


DB_CONFIG = config.get("database", {})
# Tortoise's asyncpg pool defaults to 5 connections, shared by every background poller and
# all UI traffic. Background generation runs up to [generation_poller] max_running_total jobs
# that each make short reads, so the default connection gets a larger pool. The Postgres
# server (shared by TEST and prod) allows 200 connections.
DB_POOL_MAXSIZE = int(DB_CONFIG.get("pool_maxsize", 20))
DB_URL = f"postgres://{DB_CONFIG['db_user']}:{DB_CONFIG['db_password']}@{DB_CONFIG['db_host']}:{DB_CONFIG['db_port']}/{DB_CONFIG['db_name']}?minsize=1&maxsize={DB_POOL_MAXSIZE}"

PRODUCT_DB_CONFIG = config.get("product_database", {})
PRODUCT_DB_URL = f"postgres://{PRODUCT_DB_CONFIG['db_user']}:{PRODUCT_DB_CONFIG['db_password']}@{PRODUCT_DB_CONFIG['db_host']}:{PRODUCT_DB_CONFIG['db_port']}/{PRODUCT_DB_CONFIG['db_name']}"

PHOTO_DB_CONFIG = config.get("photography_database", {})
PHOTO_DB_URL = f"postgres://{PHOTO_DB_CONFIG['db_user']}:{PHOTO_DB_CONFIG['db_password']}@{PHOTO_DB_CONFIG['db_host']}:{PHOTO_DB_CONFIG['db_port']}/{PHOTO_DB_CONFIG['db_name']}"

TORTOISE_ORM_CONFIG = {
    "connections": {
        "default": DB_URL,
        "product_db": PRODUCT_DB_URL,
        "photography_db": PHOTO_DB_URL,
    },
    "apps": {
        "models": {
            "models": ["models.db_models", "aerich.models"],
            "default_connection": "default",
        },
        "listingoptions": {
            "models": ["listingoptions.models.db_models"],
            "default_connection": "default",
        },
    },
    "use_tz": True,
}

CORS_ORIGINS = config.get("cors", {}).get("allowed_origins", ["*"])

API_KEY = config.get("api", {}).get("api_key", "")

LISTING_OPTIONS = config.get("listing_options", {})
