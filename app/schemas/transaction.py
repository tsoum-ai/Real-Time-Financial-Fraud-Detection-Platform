"""Transaction request/response models."""
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TransactionIn(BaseModel):
    """What a client POSTs. transaction_id/timestamp are optional - we fill them in."""
    transaction_id: str = Field(default_factory=lambda: str(uuid4()))
    card_id: str = Field(..., description="Masked or tokenized card identifier")
    user_id: str
    amount: float = Field(..., gt=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    merchant: str
    country: str = Field(..., min_length=2, max_length=2, description="ISO 3166-1 alpha-2")
    timestamp: datetime = Field(default_factory=_utcnow)

    @field_validator("country", "currency")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    def to_document(self) -> dict:
        doc = self.model_dump()
        # store as native datetime; mongo handles it
        doc["timestamp"] = self.timestamp
        return doc


class TransactionOut(TransactionIn):
    ingested_at: datetime | None = None


class TransactionAccepted(BaseModel):
    """Returned from POST /transactions - the write to Kafka was accepted."""
    transaction_id: str
    status: str = "accepted"
