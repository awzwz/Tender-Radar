from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, String, cast
from typing import Optional
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Lot, RiskScore, RiskFlag, TrdBuy, Contract

router = APIRouter()


@router.get("/stats")
async def get_dashboard_stats(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Dashboard stats: risk distribution, totals, averages."""
    # Total lots
    total_lots_result = await db.execute(
        select(func.count(Lot.id)).where(Lot.is_deleted == False)
    )
    total_lots = total_lots_result.scalar() or 0

    # Risk distribution
    dist_result = await db.execute(
        select(RiskScore.level, func.count(RiskScore.id))
        .where(RiskScore.entity_type == "lot")
        .group_by(RiskScore.level)
    )
    distribution = {r[0]: r[1] for r in dist_result.all()}

    # Scored lots count
    scored_lots = sum(distribution.values())

    # Average score
    avg_result = await db.execute(
        select(func.avg(RiskScore.score_final))
        .where(RiskScore.entity_type == "lot")
    )
    avg_score = round(float(avg_result.scalar() or 0), 1)

    # Triggered flags count
    flags_result = await db.execute(
        select(func.count(RiskFlag.id))
        .where(RiskFlag.entity_type == "lot", RiskFlag.flag_bool == True)
    )
    total_flags_triggered = flags_result.scalar() or 0

    return {
        "total_lots": total_lots,
        "scored_lots": scored_lots,
        "unscored_lots": total_lots - scored_lots,
        "high": distribution.get("HIGH", 0),
        "medium": distribution.get("MEDIUM", 0),
        "low": distribution.get("LOW", 0),
        "avg_score": avg_score,
        "total_flags_triggered": total_flags_triggered,
    }


@router.get("")
async def get_dashboard_lots(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    level: Optional[str] = Query(None, description="LOW/MEDIUM/HIGH"),
    customer_bin: Optional[str] = None,
    supplier_biin: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort_by: str = Query("score", description="score/date/amount"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Dashboard: paginated list of lots with risk scores."""
    offset = (page - 1) * limit

    # Build query joining lots with risk_scores
    query = (
        select(
            Lot.id,
            Lot.name_ru,
            Lot.amount,
            Lot.customer_bin,
            Lot.customer_name,
            Lot.trd_buy_id,
            TrdBuy.number_anno,
            TrdBuy.publish_date,
            RiskScore.score,
            RiskScore.score_rules,
            RiskScore.score_ml,
            RiskScore.score_final,
            RiskScore.level,
            RiskScore.top_reasons_jsonb,
        )
        .join(RiskScore, and_(
            RiskScore.entity_type == "lot",
            RiskScore.entity_id == cast(Lot.id, String),
        ), isouter=True)
        .join(TrdBuy, TrdBuy.id == Lot.trd_buy_id, isouter=True)
        .where(Lot.is_deleted == False)
    )

    if level:
        query = query.where(RiskScore.level == level)
    if customer_bin:
        query = query.where(Lot.customer_bin == customer_bin)
    if supplier_biin:
        # Join through Contract to filter by supplier BIN/IIN — fixes bug E
        query = query.join(
            Contract,
            and_(
                Contract.trd_buy_id == Lot.trd_buy_id,
                Contract.is_deleted == False,
            ),
            isouter=False,
        ).where(Contract.supplier_biin == supplier_biin)
    if date_from:
        query = query.where(TrdBuy.publish_date >= date_from)
    if date_to:
        query = query.where(TrdBuy.publish_date <= date_to)

    if sort_by == "score":
        query = query.order_by(RiskScore.score_final.desc().nullslast())
    elif sort_by == "date":
        query = query.order_by(TrdBuy.publish_date.desc())
    elif sort_by == "amount":
        query = query.order_by(Lot.amount.desc())

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginate
    result = await db.execute(query.offset(offset).limit(limit))
    rows = result.all()

    items = [
        {
            "lot_id": r.id,
            "lot_name": r.name_ru,
            "amount": float(r.amount or 0),
            "customer_bin": r.customer_bin,
            "customer_name": r.customer_name,
            "trd_buy_id": r.trd_buy_id,
            "tender_number": r.number_anno,
            "publish_date": str(r.publish_date) if r.publish_date else None,
            "risk_score": r.score_final,
            "score_rules": r.score_rules,
            "score_ml": r.score_ml,
            "score_final": r.score_final,
            "risk_level": r.level or "UNKNOWN",
            "top_reasons": r.top_reasons_jsonb or [],
        }
        for r in rows
    ]

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": items,
    }
