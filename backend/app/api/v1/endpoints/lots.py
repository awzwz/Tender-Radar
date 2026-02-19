from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Lot, TrdBuy, Contract, RiskFlag, RiskScore

router = APIRouter()


@router.get("/{lot_id}")
async def get_lot_detail(
    lot_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Full lot card: details + all risk flags with evidence."""
    lot_result = await db.execute(select(Lot).where(Lot.id == lot_id))
    lot = lot_result.scalar_one_or_none()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")

    # Get tender
    tender = None
    if lot.trd_buy_id:
        t_result = await db.execute(select(TrdBuy).where(TrdBuy.id == lot.trd_buy_id))
        tender = t_result.scalar_one_or_none()

    # Get contract
    contract_result = await db.execute(
        select(Contract).where(
            Contract.trd_buy_id == lot.trd_buy_id,
            Contract.is_deleted == False,
        ).limit(1)
    )
    contract = contract_result.scalar_one_or_none()

    # Get risk score
    score_result = await db.execute(
        select(RiskScore).where(
            RiskScore.entity_type == "lot",
            RiskScore.entity_id == str(lot_id),
        )
    )
    risk_score = score_result.scalar_one_or_none()

    # Get all risk flags
    flags_result = await db.execute(
        select(RiskFlag).where(
            RiskFlag.entity_type == "lot",
            RiskFlag.entity_id == str(lot_id),
        )
    )
    flags = flags_result.scalars().all()

    return {
        "lot": {
            "id": lot.id,
            "name_ru": lot.name_ru,
            "amount": float(lot.amount or 0),
            "customer_bin": lot.customer_bin,
            "customer_name": lot.customer_name,
            "trd_buy_id": lot.trd_buy_id,
            "dumping_flag": lot.dumping_flag,
            "ref_lot_status_id": lot.ref_lot_status_id,
        },
        "tender": {
            "id": tender.id if tender else None,
            "number_anno": tender.number_anno if tender else None,
            "name_ru": tender.name_ru if tender else None,
            "publish_date": str(tender.publish_date) if tender and tender.publish_date else None,
            "start_date": str(tender.start_date) if tender and tender.start_date else None,
            "end_date": str(tender.end_date) if tender and tender.end_date else None,
            "ref_trade_methods_id": tender.ref_trade_methods_id if tender else None,
        } if tender else None,
        "contract": {
            "id": contract.id if contract else None,
            "supplier_biin": contract.supplier_biin if contract else None,
            "contract_sum_wnds": float(contract.contract_sum_wnds or 0) if contract else None,
            "sign_date": str(contract.sign_date) if contract and contract.sign_date else None,
            "plan_exec_date": str(contract.plan_exec_date) if contract and contract.plan_exec_date else None,
            "fakt_exec_date": str(contract.fakt_exec_date) if contract and contract.fakt_exec_date else None,
            "parent_id": contract.parent_id if contract else None,
        } if contract else None,
        "risk": {
            "score": risk_score.score if risk_score else None,
            "level": risk_score.level if risk_score else "UNKNOWN",
            "top_reasons": risk_score.top_reasons_jsonb if risk_score else [],
            "computed_at": str(risk_score.computed_at) if risk_score else None,
        },
        "flags": [
            {
                "code": f.indicator_code,
                "triggered": f.flag_bool,
                "value": f.value_numeric,
                "evidence": f.evidence_jsonb,
            }
            for f in flags
        ],
    }
