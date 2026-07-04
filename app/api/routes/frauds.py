from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_fraud_repo
from app.repositories.fraud_repository import FraudRepository
from app.schemas.fraud import FraudAlert

router = APIRouter(prefix="/frauds", tags=["frauds"])


@router.get("", response_model=list[FraudAlert])
async def list_frauds(
    limit: int = Query(50, ge=1, le=500),
    skip: int = Query(0, ge=0),
    reason: str | None = Query(None, description="Filter by rule code, e.g. LARGE_AMOUNT"),
    repo: FraudRepository = Depends(get_fraud_repo),
) -> list[dict]:
    return await repo.list(limit=limit, skip=skip, reason=reason)
