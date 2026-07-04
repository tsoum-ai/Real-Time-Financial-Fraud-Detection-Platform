import pytest
from pydantic import ValidationError

from app.schemas.transaction import TransactionIn


def test_defaults_are_populated():
    txn = TransactionIn(
        card_id="card_1", user_id="u1", amount=100, merchant="Amazon", country="us"
    )
    assert txn.transaction_id  # auto uuid
    assert txn.country == "US"  # upper-cased
    assert txn.currency == "USD"
    assert txn.timestamp is not None


def test_amount_must_be_positive():
    with pytest.raises(ValidationError):
        TransactionIn(
            card_id="c", user_id="u", amount=0, merchant="m", country="US"
        )


def test_country_must_be_two_chars():
    with pytest.raises(ValidationError):
        TransactionIn(
            card_id="c", user_id="u", amount=10, merchant="m", country="USA"
        )


def test_to_document_roundtrip():
    txn = TransactionIn(card_id="c", user_id="u", amount=10, merchant="m", country="gb")
    doc = txn.to_document()
    assert doc["country"] == "GB"
    assert doc["transaction_id"] == txn.transaction_id
