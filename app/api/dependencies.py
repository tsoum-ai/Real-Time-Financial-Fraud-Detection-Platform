"""FastAPI dependency providers. Wiring lives here so routes stay declarative."""
from fastapi import Depends, Request

from app.config import Settings, get_settings
from app.db.mongodb import get_db
from app.messaging.producer import KafkaProducer
from app.repositories.fraud_repository import FraudRepository
from app.repositories.transaction_repository import TransactionRepository
from app.services.transaction_service import TransactionService


def get_producer(request: Request) -> KafkaProducer:
    # single producer instance is created on startup and stashed on app.state
    return request.app.state.producer


def get_transaction_service(
    producer: KafkaProducer = Depends(get_producer),
) -> TransactionService:
    return TransactionService(producer)


def get_transaction_repo(
    settings: Settings = Depends(get_settings),
) -> TransactionRepository:
    return TransactionRepository(get_db(), settings)


def get_fraud_repo(settings: Settings = Depends(get_settings)) -> FraudRepository:
    return FraudRepository(get_db(), settings)
