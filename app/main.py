"""FastAPI entrypoint. Wires lifespan (Mongo + Kafka), routers and OpenAPI metadata."""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import frauds, health, transactions
from app.config import get_settings
from app.db import mongodb
from app.logging_config import configure_logging, get_logger
from app.messaging.producer import KafkaProducer

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    await mongodb.connect(settings)
    app.state.producer = KafkaProducer(settings)
    logger.info("API started (env=%s)", settings.app_env)
    yield
    # shutdown
    app.state.producer.close()
    await mongodb.disconnect()
    logger.info("API stopped")


app = FastAPI(
    title="Real-Time Financial Fraud Detection Platform",
    description=(
        "Ingests card transactions, streams them through Kafka + Spark, applies fraud "
        "rules and persists alerts to MongoDB."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(transactions.router)
app.include_router(frauds.router)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": "fraud-detection-platform", "docs": "/docs"}
