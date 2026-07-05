"""Thin wrapper around confluent-kafka's Producer.

confluent_kafka's produce() is non-blocking and callback based, which doesn't play
nicely with async endpoints. We wrap it and run the flush in a thread so we can await
delivery without blocking the event loop.
"""
import json
from datetime import datetime

from confluent_kafka import KafkaException, Producer

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"not serializable: {type(o)}")


class KafkaProducer:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.kafka_transactions_topic
        config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
            # keep latency low but still batch a little under load
            "linger.ms": 5,
            "acks": "all",
            "enable.idempotence": True,
        }
        if settings.kafka_security_protocol != "PLAINTEXT":
            config.update(
                {
                    "security.protocol": settings.kafka_security_protocol,
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": settings.kafka_sasl_username,
                    "sasl.password": settings.kafka_sasl_password,
                }
            )
        self._producer = Producer(config)

    def produce(self, key: str, value: dict) -> None:
        """Queue a message. Raises if the local queue is full (backpressure)."""
        payload = json.dumps(value, default=_json_default).encode("utf-8")
        try:
            self._producer.produce(
                self._topic,
                key=key.encode("utf-8"),
                value=payload,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # local queue is full - force a flush and retry once
            self._producer.flush(5)
            self._producer.produce(self._topic, key=key.encode("utf-8"), value=payload)
        # poll to trigger delivery callbacks for previously queued msgs
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d messages still in queue after flush", remaining)
        return remaining

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            logger.debug(
                "Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset()
            )

    def close(self) -> None:
        try:
            self.flush()
        except KafkaException as exc:
            logger.error("Error flushing producer on close: %s", exc)
