"""Fraud alert model - mirrors what the Spark job writes to Mongo."""
from datetime import datetime

from pydantic import BaseModel


class FraudAlert(BaseModel):
    transaction_id: str
    card_id: str
    user_id: str
    amount: float
    currency: str = "USD"
    country: str
    # one or more rule codes that fired, e.g. ["LARGE_AMOUNT", "HIGH_RISK_COUNTRY"]
    reasons: list[str]
    score: float
    detected_at: datetime
