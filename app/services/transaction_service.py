"""Application service: turns an incoming transaction into a Kafka event.

Kept deliberately thin - the API layer shouldn't know about Kafka, and the
producer shouldn't know about HTTP. This sits in between.
"""
import asyncio

from app.messaging.producer import KafkaProducer
from app.schemas.transaction import TransactionAccepted, TransactionIn
from app.logging_config import get_logger

logger = get_logger(__name__)


class TransactionService:
    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer

    async def submit(self, txn: TransactionIn) -> TransactionAccepted:
        payload = txn.model_dump(mode="json")
        # produce() is sync/non-blocking; run in a thread to keep the loop free
        await asyncio.to_thread(self._producer.produce, txn.card_id, payload)
        logger.info(
            "Accepted txn %s card=%s amount=%.2f %s",
            txn.transaction_id,
            txn.card_id,
            txn.amount,
            txn.country,
        )
        return TransactionAccepted(transaction_id=txn.transaction_id)
