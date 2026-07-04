"""Spark rule tests. Skipped automatically when pyspark isn't installed
(e.g. the lightweight API test env), so the suite still runs green in CI."""
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from spark.fraud_rules import apply_stateless, stateless_rules  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("rule-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _rows(df):
    return {r["transaction_id"]: set(r["reasons"]) for r in df.collect()}


def test_large_amount_and_high_risk(spark):
    data = [
        ("t1", "card_a", "u1", 50000.0, "US"),
        ("t2", "card_b", "u2", 100.0, "NG"),
        ("t3", "card_c", "u3", 20.0, "US"),
    ]
    df = spark.createDataFrame(data, ["transaction_id", "card_id", "user_id", "amount", "country"])
    rules = stateless_rules(threshold=10000, countries=["NG", "RU"])
    result = _rows(apply_stateless(df, rules))

    assert "LARGE_AMOUNT" in result["t1"]
    assert "HIGH_RISK_COUNTRY" in result["t2"]
    assert result["t3"] == set()  # clean txn


def test_duplicate_card_flagged(spark):
    data = [
        ("t1", "card_x", "u1", 30.0, "US"),
        ("t2", "card_x", "u1", 40.0, "US"),
        ("t3", "card_y", "u2", 25.0, "US"),
    ]
    df = spark.createDataFrame(data, ["transaction_id", "card_id", "user_id", "amount", "country"])
    rules = stateless_rules(threshold=10000, countries=[])
    result = _rows(apply_stateless(df, rules))

    assert "DUPLICATE_CARD" in result["t1"]
    assert "DUPLICATE_CARD" in result["t2"]
    assert "DUPLICATE_CARD" not in result["t3"]
