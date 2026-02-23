from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, String, cast
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import TrdBuy, Lot, RiskScore

router = APIRouter()


@router.get("/{trd_buy_id}")
async def get_tender_detail(
    trd_buy_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Tender card with all its lots and their risk scores."""
    tender_result = await db.execute(select(TrdBuy).where(TrdBuy.id == trd_buy_id))
    tender = tender_result.scalar_one_or_none()
    if not tender:
        raise HTTPException(status_code=404, detail="Tender not found")

    lots_result = await db.execute(
        select(Lot, RiskScore.score, RiskScore.level, RiskScore.top_reasons_jsonb)
        .join(RiskScore, (RiskScore.entity_type == "lot") & (RiskScore.entity_id == cast(Lot.id, String)), isouter=True)
        .where(Lot.trd_buy_id == trd_buy_id, Lot.is_deleted == False)
        .order_by(RiskScore.score.desc().nullslast())
    )
    lots = lots_result.all()

    return {
        "tender": {
            "id": tender.id,
            "number_anno": tender.number_anno,
            "name_ru": tender.name_ru,
            "org_bin": tender.org_bin,
            "total_sum": float(tender.total_sum or 0),
            "publish_date": str(tender.publish_date) if tender.publish_date else None,
            "start_date": str(tender.start_date) if tender.start_date else None,
            "end_date": str(tender.end_date) if tender.end_date else None,
            "ref_trade_methods_id": tender.ref_trade_methods_id,
            "ref_buy_status_id": tender.ref_buy_status_id,
            "singl_org_sign": tender.singl_org_sign,
        },
        "lots": [
            {
                "lot_id": r.Lot.id,
                "name_ru": r.Lot.name_ru,
                "amount": float(r.Lot.amount or 0),
                "customer_bin": r.Lot.customer_bin,
                "risk_score": r.score,
                "risk_level": r.level or "UNKNOWN",
                "top_reasons": r.top_reasons_jsonb or [],
            }
            for r in lots
        ],
    }
