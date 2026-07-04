"""Read side for transactions. Writes go through Kafka -> Spark, not here."""
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings


class TransactionRepository:
    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:
        self._col = db[settings.mongo_transactions_collection]

    async def list(self, limit: int = 50, skip: int = 0, card_id: str | None = None) -> list[dict]:
        query: dict = {}
        if card_id:
            query["card_id"] = card_id
        cursor = (
            self._col.find(query, {"_id": 0})
            .sort("timestamp", -1)
            .skip(skip)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def get(self, transaction_id: str) -> dict | None:
        return await self._col.find_one({"transaction_id": transaction_id}, {"_id": 0})

    async def count(self) -> int:
        return await self._col.estimated_document_count()
