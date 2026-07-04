"""Read side for fraud alerts produced by the Spark pipeline."""
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings


class FraudRepository:
    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:
        self._col = db[settings.mongo_frauds_collection]

    async def list(
        self,
        limit: int = 50,
        skip: int = 0,
        reason: str | None = None,
    ) -> list[dict]:
        query: dict = {}
        if reason:
            # reasons is an array field
            query["reasons"] = reason.upper()
        cursor = (
            self._col.find(query, {"_id": 0})
            .sort("detected_at", -1)
            .skip(skip)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def count(self) -> int:
        return await self._col.estimated_document_count()
