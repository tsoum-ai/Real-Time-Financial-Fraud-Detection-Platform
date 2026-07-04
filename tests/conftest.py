"""Shared fixtures. We stub Mongo (mongomock) and Kafka (fake) so the API tests
run with no external services - handy for CI."""
import pytest
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient


class FakeProducer:
    """Stand-in for the confluent-kafka wrapper; just records what was sent."""

    def __init__(self, settings=None):
        self.messages: list[tuple[str, dict]] = []

    def produce(self, key, value):
        self.messages.append((key, value))

    def flush(self, timeout: float = 10.0) -> int:
        return 0

    def close(self) -> None:
        pass


@pytest.fixture
async def client(monkeypatch):
    from app.db import mongodb as mongo_mod

    mock_client = AsyncMongoMockClient()
    mock_db = mock_client["fraud_platform"]

    async def fake_connect(settings):
        mongo_mod._mongo.client = mock_client
        mongo_mod._mongo.db = mock_db

    async def fake_disconnect():
        pass

    monkeypatch.setattr(mongo_mod, "connect", fake_connect)
    monkeypatch.setattr(mongo_mod, "disconnect", fake_disconnect)
    monkeypatch.setattr("app.main.KafkaProducer", FakeProducer)

    from app.main import app

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac, mock_db
