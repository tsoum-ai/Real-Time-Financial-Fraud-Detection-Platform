# Real-Time Financial Fraud Detection Platform

Streaming platform that ingests card transactions, runs them through Kafka and
Spark Structured Streaming, applies a set of fraud rules, and stores both the raw
transactions and any fraud alerts in MongoDB. A FastAPI service sits on top for
ingestion and querying.

```
Generator ─┐
           ├─► FastAPI (POST /transactions) ─► Kafka topic ─► Spark Streaming ─► rules ─► MongoDB ─► FastAPI (GET /transactions, /frauds)
Generator ─┘
```

## System Architecture

![System Architecture](architecture.png)

The standalone generator publishes directly to Kafka to create load; the API is
there for real clients and for reading results back out.

## Stack

- Python 3.12 + FastAPI
- Apache Kafka (Confluent images)
- Apache Spark 3.5 Structured Streaming (PySpark)
- MongoDB 7
- Docker Compose for local orchestration
- GitHub Actions for CI

## Fraud rules

Implemented in `spark/fraud_rules.py` and wired into the micro-batch in
`spark/streaming_job.py`. All thresholds are env-configurable.

| Code                | Trigger                                                        |
|---------------------|---------------------------------------------------------------|
| `LARGE_AMOUNT`      | `amount >= FRAUD_AMOUNT_THRESHOLD`                            |
| `HIGH_RISK_COUNTRY` | country in `FRAUD_HIGH_RISK_COUNTRIES`                         |
| `DUPLICATE_CARD`    | same card seen more than once in the batch                    |
| `RAPID_TXN`         | `>= FRAUD_RAPID_TXN_COUNT` txns for one card within the window |

A transaction that trips one or more rules is written to `fraud_alerts` with the
list of `reasons` and an aggregate `score`.

## Quick start

```bash
cp .env.example .env
docker-compose up --build
```

That brings up Zookeeper, Kafka, MongoDB, the API, the Spark job and the
transaction generator. Give it ~30s for Kafka and Spark to settle, then:

- Swagger UI: http://localhost:8000/docs
- Health:     http://localhost:8000/health

The generator starts producing immediately (including a few deliberately
suspicious transactions), so `GET /frauds` should return alerts within a minute.

## API

| Method | Path                 | Description                              |
|--------|----------------------|------------------------------------------|
| POST   | `/transactions`      | Publish a transaction to Kafka (202)     |
| GET    | `/transactions`      | List stored transactions (paginated)     |
| GET    | `/transactions/{id}` | Fetch a single transaction               |
| GET    | `/frauds`            | List fraud alerts, optional `reason` filter |
| GET    | `/health`            | Liveness + Mongo ping                    |

Example:

```bash
curl -X POST http://localhost:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{"card_id":"card_1","user_id":"u1","amount":55000,"merchant":"Apple","country":"RU"}'

curl "http://localhost:8000/frauds?reason=LARGE_AMOUNT"
```

## Project layout

```
.
├── app/                     # FastAPI service
│   ├── api/                 # routers + dependency wiring
│   ├── db/                  # Mongo connection/indexes
│   ├── messaging/           # Kafka producer wrapper
│   ├── repositories/        # read-side data access
│   ├── schemas/             # Pydantic models
│   ├── services/            # application layer
│   ├── config.py            # env-driven settings
│   └── main.py              # app + lifespan
├── producer/                # standalone transaction generator
├── spark/                   # streaming job + fraud rules
├── data/                    # seed dataset
├── docker/                  # per-service Dockerfiles
├── tests/                   # pytest (API + rule tests)
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Configuration

Everything is driven from `.env` (see `.env.example`). The most useful knobs:

- `FRAUD_AMOUNT_THRESHOLD` – large-amount cutoff
- `FRAUD_HIGH_RISK_COUNTRIES` – comma-separated ISO country codes
- `FRAUD_RAPID_TXN_COUNT` / `FRAUD_RAPID_TXN_WINDOW_SECONDS` – rapid-fire rule
- `GEN_FRAUD_RATIO` / `GEN_INTERVAL_SECONDS` – generator behaviour

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# run the API against locally running Kafka/Mongo (see docker-compose ports)
uvicorn app.main:app --reload

# run the test suite (Spark tests are skipped unless pyspark is installed)
pytest -q
```

To also run the Spark rule tests locally: `pip install -r requirements-spark.txt`.

## CI

`.github/workflows/ci.yml` runs ruff, a compile check and the test suite on every
push/PR, then builds the API and producer images and validates the compose file.

## Azure deployment

The same containers run on Azure with managed data services in place of the
self-hosted Kafka/Mongo: no code fork, just different env vars.

| Local (docker-compose) | Azure |
|---|---|
| Zookeeper + Kafka | **Event Hubs** (Kafka-compatible endpoint, SASL_SSL) |
| MongoDB | **Cosmos DB for MongoDB API** |
| api / spark / producer containers | **Azure Container Apps** |
| — | **Container Registry** (images), **Key Vault** (secrets), **Azure Files** (Spark checkpoint durability) |

Deployment is split into two layers, mirroring how platform and app teams
divide responsibility in practice:

- **Platform layer** (`infra/platform.bicep`): the registry, Event Hubs,
  Cosmos DB, Key Vault, and the Container Apps environment — *including the RBAC
  role assignments* that let the app identity pull images and read secrets.
  Because it creates role assignments, it's deployed once by someone with
  role-assignment rights (Owner / User Access Administrator):

  ```bash
  az group create -n fraud-detection-platform -l eastus
  az deployment group create -g fraud-detection-platform \
    -n platform --template-file infra/platform.bicep
  ```

- **App layer** (`infra/apps.bicep` + `.github/workflows/deploy-azure.yml`):
  the three Container Apps. This is the frequent, least-privilege path — the CI
  identity only needs **Contributor** on the resource group. On push to `main`
  the workflow reads the platform layer's outputs, builds and pushes the images
  to ACR, then deploys the apps. It authenticates via OIDC federated credentials
  (no stored client secret).

To wire up CI: create an Azure AD app registration with a federated credential
for this repo (`repo:<owner>/<repo>:ref:refs/heads/main` and, because the deploy
job uses a `production` environment, also `repo:<owner>/<repo>:environment:production`),
grant it Contributor on the resource group, and set these repo secrets:
`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`. The app's
Kafka/Mongo settings (`KAFKA_SECURITY_PROTOCOL=SASL_SSL`, etc.) are the same env
vars used locally — see the Azure section of `.env.example`.

## Notes / known limitations

- The rapid-fire and duplicate-card checks are evaluated per micro-batch rather
  than over a true event-time session window. Good enough for the demo; for
  production you'd move to a stateful `groupBy(window(...))` with watermarking.
- Kafka topics are auto-created. For production, pre-create them with an explicit
  partition/replication scheme.
- No auth on the API yet — add an API key or JWT layer before exposing it.
