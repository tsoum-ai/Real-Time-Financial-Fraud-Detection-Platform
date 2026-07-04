"""Fraud detection rules expressed as Spark SQL column expressions.

Each rule returns a boolean Column plus a code string. Keeping them as pure
expressions (rather than UDFs) lets Catalyst optimize and avoids per-row Python.
The two stateless rules run on the raw stream; the rapid-fire rule needs a
windowed aggregation and is applied separately in the streaming job.
"""
from dataclasses import dataclass
from typing import Callable, List

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass(frozen=True)
class Rule:
    code: str
    weight: float
    predicate: Callable[[DataFrame], Column]


def large_amount(threshold: float) -> Rule:
    return Rule(
        code="LARGE_AMOUNT",
        weight=0.5,
        predicate=lambda df: F.col("amount") >= F.lit(threshold),
    )


def high_risk_country(countries: List[str]) -> Rule:
    # empty list -> never fires, guard against isin([]) weirdness
    codes = [c.upper() for c in countries] or ["__none__"]
    return Rule(
        code="HIGH_RISK_COUNTRY",
        weight=0.4,
        predicate=lambda df: F.upper(F.col("country")).isin(codes),
    )


def stateless_rules(threshold: float, countries: List[str]) -> List[Rule]:
    return [large_amount(threshold), high_risk_country(countries)]


def apply_stateless(df: DataFrame, rules: List[Rule]) -> DataFrame:
    """Attach a `reasons` array and cumulative `score` for the per-record rules.

    Duplicate-card usage is folded in here too: if the same card_id shows up more
    than once inside the current micro-batch we flag it. This is a pragmatic
    batch-local check - the windowed rapid-fire rule handles the cross-batch case.
    """
    reason_cols = []
    score_col = F.lit(0.0)
    for rule in rules:
        pred = rule.predicate(df)
        reason_cols.append(F.when(pred, F.lit(rule.code)))
        score_col = score_col + F.when(pred, F.lit(rule.weight)).otherwise(F.lit(0.0))

    # DUPLICATE_CARD: >1 txn for a card within this batch
    dup_count = F.count("*").over(Window.partitionBy("card_id"))
    is_dup = dup_count > 1
    reason_cols.append(F.when(is_dup, F.lit("DUPLICATE_CARD")))
    score_col = score_col + F.when(is_dup, F.lit(0.3)).otherwise(F.lit(0.0))

    reasons = F.array_compact(F.array(*reason_cols))
    return df.withColumn("reasons", reasons).withColumn("score", F.round(score_col, 2))
