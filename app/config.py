"""Central settings. Everything is env-driven so the same image runs in any env."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    app_env: str = "development"

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_transactions_topic: str = "transactions"
    kafka_client_id: str = "fraud-platform"

    # Mongo
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "fraud_platform"
    mongo_transactions_collection: str = "transactions"
    mongo_frauds_collection: str = "fraud_alerts"

    # Fraud thresholds (Spark reads these too)
    fraud_amount_threshold: float = 10_000.0
    fraud_rapid_txn_window_seconds: int = 60
    fraud_rapid_txn_count: int = 5
    fraud_high_risk_countries: str = "NG,RU,KP,IR,SY"

    # Generator
    gen_interval_seconds: float = 1.0
    gen_fraud_ratio: float = 0.15

    @property
    def high_risk_countries(self) -> set[str]:
        return {c.strip().upper() for c in self.fraud_high_risk_countries.split(",") if c.strip()}


@lru_cache
def get_settings() -> Settings:
    # cached so we don't re-parse .env on every request
    return Settings()
