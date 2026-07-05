"""Spark Structured Streaming pipeline.

Reads transactions from Kafka, persists the raw record to MongoDB, applies the
fraud rules and writes any resulting alerts to a separate collection.

Kept self-contained (reads config straight from env) so the Spark image doesn't
need the FastAPI dependency tree - just pyspark + the connector jars.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from spark.fraud_rules import apply_stateless, stateless_rules


# --- config (env-driven, matches .env) ---
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "fraud_platform")
TXN_COLLECTION = os.getenv("MONGO_TRANSACTIONS_COLLECTION", "transactions")
FRAUD_COLLECTION = os.getenv("MONGO_FRAUDS_COLLECTION", "fraud_alerts")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
MAX_OFFSETS = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "1000")

# SASL/SSL - only needed against a managed broker (e.g. Azure Event Hubs' Kafka
# endpoint); leave protocol as PLAINTEXT for the local docker-compose broker.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

AMOUNT_THRESHOLD = float(os.getenv("FRAUD_AMOUNT_THRESHOLD", "10000"))
RAPID_WINDOW = int(os.getenv("FRAUD_RAPID_TXN_WINDOW_SECONDS", "60"))
RAPID_COUNT = int(os.getenv("FRAUD_RAPID_TXN_COUNT", "5"))
HIGH_RISK = [
    c.strip().upper()
    for c in os.getenv("FRAUD_HIGH_RISK_COUNTRIES", "NG,RU,KP,IR,SY").split(",")
    if c.strip()
]

# Kafka message schema (JSON produced by the API / generator)
TXN_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("card_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("merchant", StringType()),
        StructField("country", StringType()),
        StructField("timestamp", TimestampType()),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("fraud-detection-stream")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # small shuffle partition count - this is a demo, not a cluster
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_stream(spark: SparkSession) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    )
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        jaas_config = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";'
        )
        reader = (
            reader.option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
            .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
            .option("kafka.sasl.jaas.config", jaas_config)
        )
    raw = reader.load()
    # value is bytes -> json -> flatten
    return (
        raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"))
        .select("t.*")
        .filter(F.col("transaction_id").isNotNull())
    )


def _write_mongo(df: DataFrame, collection: str) -> None:
    (
        df.write.format("mongodb")
        .mode("append")
        .option("database", MONGO_DB)
        .option("collection", collection)
        .save()
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink. Runs per micro-batch so we can do two writes + a join."""
    if batch_df.rdd.isEmpty():
        return

    batch_df = batch_df.cache()  # reused for raw write + rule eval
    try:
        # 1) persist raw transactions
        _write_mongo(batch_df.withColumn("ingested_at", F.current_timestamp()), TXN_COLLECTION)

        # 2) stateless + duplicate-card rules
        rules = stateless_rules(AMOUNT_THRESHOLD, HIGH_RISK)
        scored = apply_stateless(batch_df, rules)

        # 3) rapid-fire: many txns per card inside the batch window
        rapid_cards = (
            batch_df.groupBy("card_id")
            .agg(F.count("*").alias("cnt"))
            .filter(F.col("cnt") >= RAPID_COUNT)
            .select("card_id")
        )
        scored = scored.join(
            rapid_cards.withColumn("_rapid", F.lit(True)), on="card_id", how="left"
        )
        scored = scored.withColumn(
            "reasons",
            F.when(
                F.col("_rapid").isNotNull(),
                F.array_union(F.col("reasons"), F.array(F.lit("RAPID_TXN"))),
            ).otherwise(F.col("reasons")),
        ).withColumn(
            "score",
            F.when(F.col("_rapid").isNotNull(), F.round(F.col("score") + 0.4, 2)).otherwise(
                F.col("score")
            ),
        )

        # 4) anything with at least one reason is an alert
        alerts = scored.filter(F.size("reasons") > 0).select(
            "transaction_id",
            "card_id",
            "user_id",
            "amount",
            "currency",
            "country",
            "reasons",
            "score",
            F.current_timestamp().alias("detected_at"),
        )

        if not alerts.rdd.isEmpty():
            _write_mongo(alerts, FRAUD_COLLECTION)
            print(f"[batch {batch_id}] wrote {alerts.count()} fraud alert(s)")
    finally:
        batch_df.unpersist()


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Streaming from kafka={KAFKA_BROKERS} topic={TOPIC} -> mongo={MONGO_DB}")

    stream = read_stream(spark)
    query = (
        stream.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
