from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, String, cast, case, or_
from typing import Optional
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Subject, Contract, Lot, RiskScore

router = APIRouter()


@router.get("/")
async def list_customers(
    search: Optional[str] = Query(None, description="Search by BIN or name"),
    min_high_lots: Optional[int] = Query(None, description="Minimum number of HIGH risk lots"),
    sort_by: str = Query("high_lots", description="Sort by: high_lots, avg_score, total_amount, total_lots"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Paginated list of customers aggregated from lots in local DB."""
    high_flag = case((RiskScore.level == "HIGH", 1), else_=0)
    medium_flag = case((RiskScore.level == "MEDIUM", 1), else_=0)

    q = (
        select(
            Lot.customer_bin.label("bin"),
            func.min(Lot.customer_name).label("customer_name"),
            func.count(Lot.id).label("total_lots"),
            func.count(func.distinct(Lot.trd_buy_id)).label("total_tenders"),
            func.coalesce(func.sum(Lot.amount), 0).label("total_amount"),
            func.coalesce(func.avg(RiskScore.score_final), 0).label("avg_risk_score"),
            func.coalesce(func.sum(high_flag), 0).label("high_risk_lots"),
            func.coalesce(func.sum(medium_flag), 0).label("medium_risk_lots"),
        )
        .join(
            RiskScore,
            and_(RiskScore.entity_type == "lot", RiskScore.entity_id == cast(Lot.id, String)),
            isouter=True,
        )
        .where(Lot.is_deleted == False)
        .group_by(Lot.customer_bin)
    )

    if search:
        s = f"%{search}%"
        q = q.where(or_(Lot.customer_bin.ilike(s), Lot.customer_name.ilike(s)))

    if min_high_lots is not None:
        q = q.having(func.coalesce(func.sum(high_flag), 0) >= min_high_lots)

    # Count total
    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    # Sort
    sort_col = {
        "high_lots": func.coalesce(func.sum(high_flag), 0).desc(),
        "avg_score": func.coalesce(func.avg(RiskScore.score_final), 0).desc(),
        "total_amount": func.coalesce(func.sum(Lot.amount), 0).desc(),
        "total_lots": func.count(Lot.id).desc(),
    }.get(sort_by, func.coalesce(func.sum(high_flag), 0).desc())

    q = q.order_by(sort_col).offset((page - 1) * limit).limit(limit)
    rows = (await db.execute(q)).all()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": [
            {
                "bin": r.bin,
                "customer_name": r.customer_name,
                "total_lots": r.total_lots,
                "total_tenders": r.total_tenders,
                "total_amount": float(r.total_amount or 0),
                "avg_risk_score": round(float(r.avg_risk_score or 0), 1),
                "high_risk_lots": int(r.high_risk_lots or 0),
                "medium_risk_lots": int(r.medium_risk_lots or 0),
            }
            for r in rows
        ],
    }


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
