from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, String, cast
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Subject, Contract, Lot, RiskScore

router = APIRouter()


@router.get("/{bin}")
async def get_customer_profile(
    bin: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Customer profile: company info, top suppliers, risky lots."""
    subject_result = await db.execute(
        select(Subject).where(Subject.bin == bin).limit(1)
    )
    subject = subject_result.scalar_one_or_none()

    # Contract stats
    stats_result = await db.execute(
        select(
            func.count(Contract.id).label("total_contracts"),
            func.sum(Contract.contract_sum_wnds).label("total_sum"),
            func.count(func.distinct(Contract.supplier_biin)).label("unique_suppliers"),
        ).where(Contract.customer_bin == bin, Contract.is_deleted == False)
    )
    stats = stats_result.first()

    # Top suppliers
    top_suppliers_result = await db.execute(
        select(Contract.supplier_biin, func.count(Contract.id).label("cnt"), func.sum(Contract.contract_sum_wnds).label("total"))
        .where(Contract.customer_bin == bin, Contract.is_deleted == False)
        .group_by(Contract.supplier_biin)
        .order_by(func.count(Contract.id).desc())
        .limit(5)
    )
    top_suppliers = top_suppliers_result.all()

    # High-risk lots for this customer
    risky_lots_result = await db.execute(
        select(Lot.id, Lot.name_ru, Lot.amount, RiskScore.score, RiskScore.level)
        .join(RiskScore, (RiskScore.entity_type == "lot") & (RiskScore.entity_id == cast(Lot.id, String)), isouter=True)
        .where(Lot.customer_bin == bin, Lot.is_deleted == False, RiskScore.level == "HIGH")
        .order_by(RiskScore.score.desc())
        .limit(10)
    )
    risky_lots = risky_lots_result.all()

    return {
        "company": {
            "bin": bin,
            "name_ru": subject.name_ru if subject else None,
            "name_kz": subject.name_kz if subject else None,
            "organizer": subject.organizer if subject else None,
            "is_single_org": subject.is_single_org if subject else None,
        },
        "stats": {
            "total_contracts": stats.total_contracts or 0,
            "total_sum": float(stats.total_sum or 0),
            "unique_suppliers": stats.unique_suppliers or 0,
        },
        "top_suppliers": [
            {"supplier_biin": r.supplier_biin, "contract_count": r.cnt, "total_sum": float(r.total or 0)}
            for r in top_suppliers
        ],
        "high_risk_lots": [
            {"lot_id": r.id, "name_ru": r.name_ru, "amount": float(r.amount or 0), "score": r.score, "level": r.level}
            for r in risky_lots
        ],
    }
