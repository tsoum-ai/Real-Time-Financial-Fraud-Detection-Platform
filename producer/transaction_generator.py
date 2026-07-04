"""Standalone traffic generator.

Publishes synthetic transactions straight to Kafka (bypassing the API so we can
push volume). A configurable fraction are deliberately "suspicious" so the Spark
rules have something to catch. Run with: python -m producer.transaction_generator
"""
import json
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.config import get_settings
from app.logging_config import configure_logging, get_logger
from app.messaging.producer import KafkaProducer

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger("producer")

# small fixed pools keep the data coherent (same cards reused -> rapid/dup patterns)
_MERCHANTS = ["Amazon", "Walmart", "Steam", "Uber", "Apple", "Shell", "Netflix", "IKEA"]
_SAFE_COUNTRIES = ["US", "GB", "DE", "FR", "CA", "JP", "AU", "IN"]
_HIGH_RISK = list(settings.high_risk_countries) or ["NG", "RU"]
_CARDS = [f"card_{i:04d}" for i in range(50)]
_USERS = [f"user_{i:03d}" for i in range(50)]

_running = True


def _stop(*_):
    global _running
    _running = False
    logger.info("Shutdown signal received, draining...")


def _normal_txn() -> dict:
    return {
        "transaction_id": str(uuid4()),
        "card_id": random.choice(_CARDS),
        "user_id": random.choice(_USERS),
        "amount": round(random.uniform(5, 800), 2),
        "currency": "USD",
        "merchant": random.choice(_MERCHANTS),
        "country": random.choice(_SAFE_COUNTRIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _suspicious_txn() -> dict:
    """Bias towards one of the fraud patterns the Spark job looks for."""
    txn = _normal_txn()
    flavor = random.choice(["large", "high_risk", "rapid"])
    if flavor == "large":
        txn["amount"] = round(random.uniform(12_000, 90_000), 2)
    elif flavor == "high_risk":
        txn["country"] = random.choice(_HIGH_RISK)
    else:  # rapid - hammer a single card so the windowed count trips
        txn["card_id"] = "card_0000"
        txn["amount"] = round(random.uniform(50, 500), 2)
    return txn


def _load_seed_transactions() -> list[dict]:
    # optionally replay a seed file first so a fresh DB isn't empty
    seed = Path(__file__).resolve().parent.parent / "data" / "sample_transactions.json"
    if not seed.exists():
        return []
    try:
        return json.loads(seed.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read seed file: %s", exc)
        return []


def run() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = KafkaProducer(settings)
    logger.info(
        "Generator up -> topic=%s brokers=%s interval=%.2fs fraud_ratio=%.2f",
        settings.kafka_transactions_topic,
        settings.kafka_bootstrap_servers,
        settings.gen_interval_seconds,
        settings.gen_fraud_ratio,
    )

    # replay seed data once on boot
    for txn in _load_seed_transactions():
        producer.produce(txn.get("card_id", "seed"), txn)
    producer.flush()

    sent = 0
    try:
        while _running:
            txn = (
                _suspicious_txn()
                if random.random() < settings.gen_fraud_ratio
                else _normal_txn()
            )
            producer.produce(txn["card_id"], txn)
            sent += 1
            if sent % 20 == 0:
                logger.info("Produced %d transactions", sent)
            time.sleep(settings.gen_interval_seconds)
    finally:
        producer.close()
        logger.info("Generator stopped after %d transactions", sent)


if __name__ == "__main__":
    run()
