"""Async Mongo client wrapper. Single client per process (motor pools internally)."""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class MongoDB:
    client: AsyncIOMotorClient | None = None
    db: AsyncIOMotorDatabase | None = None


_mongo = MongoDB()


async def connect(settings: Settings) -> None:
    if _mongo.client is not None:
        return
    _mongo.client = AsyncIOMotorClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    _mongo.db = _mongo.client[settings.mongo_db]
    await _ensure_indexes(settings)
    logger.info("Connected to MongoDB at %s db=%s", settings.mongo_uri, settings.mongo_db)


async def disconnect() -> None:
    if _mongo.client is not None:
        _mongo.client.close()
        _mongo.client = None
        _mongo.db = None
        logger.info("MongoDB connection closed")


def get_db() -> AsyncIOMotorDatabase:
    if _mongo.db is None:
        raise RuntimeError("MongoDB not initialized - call connect() on startup")
    return _mongo.db


async def _ensure_indexes(settings: Settings) -> None:
    db = _mongo.db
    # idempotent; safe to call on every boot
    await db[settings.mongo_transactions_collection].create_index("transaction_id", unique=True)
    await db[settings.mongo_transactions_collection].create_index("card_id")
    # the /transactions list sorts by timestamp desc; Cosmos DB's Mongo API
    # requires an index on any server-side sort field (real MongoDB would sort
    # in memory), so this index is what makes that endpoint work in Azure.
    await db[settings.mongo_transactions_collection].create_index("timestamp")
    await db[settings.mongo_frauds_collection].create_index("transaction_id")
    await db[settings.mongo_frauds_collection].create_index("detected_at")
