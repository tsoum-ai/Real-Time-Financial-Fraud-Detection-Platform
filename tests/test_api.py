async def test_health_ok(client):
    ac, _ = client
    resp = await ac.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] in {"ok", "degraded"}


async def test_post_transaction_accepted(client):
    ac, _ = client
    payload = {
        "card_id": "card_1",
        "user_id": "u1",
        "amount": 250.0,
        "merchant": "Amazon",
        "country": "US",
    }
    resp = await ac.post("/transactions", json=payload)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["transaction_id"]


async def test_post_transaction_rejects_bad_amount(client):
    ac, _ = client
    resp = await ac.post(
        "/transactions",
        json={"card_id": "c", "user_id": "u", "amount": -5, "merchant": "m", "country": "US"},
    )
    assert resp.status_code == 422


async def test_list_transactions_reads_from_db(client):
    ac, db = client
    await db["transactions"].insert_one(
        {
            "transaction_id": "t1",
            "card_id": "card_9",
            "user_id": "u9",
            "amount": 12.0,
            "currency": "USD",
            "merchant": "Uber",
            "country": "US",
            "timestamp": "2026-07-01T00:00:00Z",
        }
    )
    resp = await ac.get("/transactions")
    assert resp.status_code == 200
    data = resp.json()
    assert any(t["transaction_id"] == "t1" for t in data)


async def test_list_frauds_filter(client):
    ac, db = client
    await db["fraud_alerts"].insert_many(
        [
            {
                "transaction_id": "f1",
                "card_id": "c1",
                "user_id": "u1",
                "amount": 50000,
                "currency": "USD",
                "country": "RU",
                "reasons": ["LARGE_AMOUNT", "HIGH_RISK_COUNTRY"],
                "score": 0.9,
                "detected_at": "2026-07-01T00:00:00Z",
            },
            {
                "transaction_id": "f2",
                "card_id": "c2",
                "user_id": "u2",
                "amount": 20,
                "currency": "USD",
                "country": "US",
                "reasons": ["DUPLICATE_CARD"],
                "score": 0.3,
                "detected_at": "2026-07-01T00:01:00Z",
            },
        ]
    )
    resp = await ac.get("/frauds", params={"reason": "large_amount"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["transaction_id"] == "f1"
