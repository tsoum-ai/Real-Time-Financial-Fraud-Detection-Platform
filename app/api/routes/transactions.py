from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.dependencies import get_transaction_repo, get_transaction_service
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionAccepted, TransactionIn, TransactionOut
from app.services.transaction_service import TransactionService

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.post("", response_model=TransactionAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_transaction(
    txn: TransactionIn,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionAccepted:
    """Publish a transaction to Kafka. Returns 202 - processing is async."""
    try:
        return await service.submit(txn)
    except Exception as exc:  # producer/broker down - surface as 503
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to enqueue transaction: {exc}",
        ) from exc


@router.get("", response_model=list[TransactionOut])
async def list_transactions(
    limit: int = Query(50, ge=1, le=500),
    skip: int = Query(0, ge=0),
    card_id: str | None = Query(None, description="Filter by card"),
    repo: TransactionRepository = Depends(get_transaction_repo),
) -> list[dict]:
    return await repo.list(limit=limit, skip=skip, card_id=card_id)


@router.get("/{transaction_id}", response_model=TransactionOut)
async def get_transaction(
    transaction_id: str,
    repo: TransactionRepository = Depends(get_transaction_repo),
) -> dict:
    doc = await repo.get(transaction_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return doc
