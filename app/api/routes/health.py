from fastapi import APIRouter

from app.db.mongodb import get_db

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    """Liveness + a cheap Mongo ping. Used by the container healthcheck."""
    mongo_ok = True
    try:
        await get_db().command("ping")
    except Exception:
        mongo_ok = False

    status = "ok" if mongo_ok else "degraded"
    return {"status": status, "dependencies": {"mongodb": "up" if mongo_ok else "down"}}
