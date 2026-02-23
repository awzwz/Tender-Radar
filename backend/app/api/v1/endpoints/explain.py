from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from app.core.security import require_viewer
from app.services.explain import generate_explanation

router = APIRouter()


@router.get("/lots/{lot_id}/explain")
async def explain_lot(
    lot_id: int,
    force: bool = Query(False, description="Force regenerate even if cached"),
    _=Depends(require_viewer),
):
    """
    Generate AI explanation for a lot's risk score.
    Only available for high-risk or uncertain scores (unless force=true).
    LLM is used ONLY for explanation — never for scoring decisions.
    """
    result = await generate_explanation("lot", str(lot_id), force=force)
    if result is None:
        raise HTTPException(status_code=404, detail="Lot not found or no risk score computed")
    return result


@router.get("/tenders/{trd_buy_id}/explain")
async def explain_tender(
    trd_buy_id: int,
    force: bool = Query(False, description="Force regenerate even if cached"),
    _=Depends(require_viewer),
):
    """
    Generate AI explanation for a tender's risk.
    Aggregates across all lots in the tender.
    """
    result = await generate_explanation("lot", str(trd_buy_id), force=force)
    if result is None:
        raise HTTPException(status_code=404, detail="Tender not found or no risk score computed")
    return result
