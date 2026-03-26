from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from typing import Optional
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Subject, Contract, Rnu, RiskScore

router = APIRouter()


@router.get("/")
async def list_suppliers(
    search: Optional[str] = Query(None, description="Search by BIIN or name"),
    blacklisted: Optional[bool] = Query(None, description="Filter by RNU blacklist status"),
    sort_by: str = Query("total_sum", description="Sort by: total_sum, total_contracts, unique_customers"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Paginated list of suppliers aggregated from contracts in local DB."""
    # Subquery: active blacklisted BIINs
    rnu_sq = (
        select(Rnu.supplier_biin)
        .where(Rnu.is_active == True)
        .distinct()
        .subquery()
    )

    # Main aggregation query
    q = (
        select(
            Contract.supplier_biin.label("biin"),
            func.min(Subject.name_ru).label("name_ru"),
            func.count(Contract.id).label("total_contracts"),
            func.coalesce(func.sum(Contract.contract_sum_wnds), 0).label("total_sum"),
            func.count(func.distinct(Contract.customer_bin)).label("unique_customers"),
            func.max(Contract.sign_date).label("last_contract_date"),
            func.count(rnu_sq.c.supplier_biin).label("rnu_count"),
        )
        .join(Subject, or_(Subject.bin == Contract.supplier_biin, Subject.iin == Contract.supplier_biin), isouter=True)
        .join(rnu_sq, rnu_sq.c.supplier_biin == Contract.supplier_biin, isouter=True)
        .where(Contract.is_deleted == False)
        .group_by(Contract.supplier_biin)
    )

    if search:
        s = f"%{search}%"
        q = q.where(or_(Contract.supplier_biin.ilike(s), Subject.name_ru.ilike(s)))

    if blacklisted is True:
        q = q.having(func.count(rnu_sq.c.supplier_biin) > 0)
    elif blacklisted is False:
        q = q.having(func.count(rnu_sq.c.supplier_biin) == 0)

    # Count total
    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    # Sort
    sort_col = {
        "total_sum": func.coalesce(func.sum(Contract.contract_sum_wnds), 0).desc(),
        "total_contracts": func.count(Contract.id).desc(),
        "unique_customers": func.count(func.distinct(Contract.customer_bin)).desc(),
    }.get(sort_by, func.coalesce(func.sum(Contract.contract_sum_wnds), 0).desc())

    q = q.order_by(sort_col).offset((page - 1) * limit).limit(limit)
    rows = (await db.execute(q)).all()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": [
            {
                "biin": r.biin,
                "name_ru": r.name_ru,
                "total_contracts": r.total_contracts,
                "total_sum": float(r.total_sum or 0),
                "unique_customers": r.unique_customers,
                "last_contract_date": str(r.last_contract_date) if r.last_contract_date else None,
                "is_blacklisted": r.rnu_count > 0,
            }
            for r in rows
        ],
    }


@router.get("/{biin}")
async def get_supplier_profile(
    biin: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Supplier profile: company info, win stats, RNU status, risk history."""
    # Company info
    subject_result = await db.execute(
        select(Subject).where(
            (Subject.bin == biin) | (Subject.iin == biin)
        ).limit(1)
    )
    subject = subject_result.scalar_one_or_none()
    if not subject:
        raise HTTPException(status_code=404, detail="Supplier not found")

    # Contract stats
    stats_result = await db.execute(
        select(
            func.count(Contract.id).label("total_contracts"),
            func.sum(Contract.contract_sum_wnds).label("total_sum"),
            func.count(func.distinct(Contract.customer_bin)).label("unique_customers"),
        ).where(Contract.supplier_biin == biin, Contract.is_deleted == False)
    )
    stats = stats_result.first()

    # Top customers
    top_customers_result = await db.execute(
        select(Contract.customer_bin, func.count(Contract.id).label("cnt"), func.sum(Contract.contract_sum_wnds).label("total"))
        .where(Contract.supplier_biin == biin, Contract.is_deleted == False)
        .group_by(Contract.customer_bin)
        .order_by(func.count(Contract.id).desc())
        .limit(5)
    )
    top_customers = top_customers_result.all()

    # RNU status
    rnu_result = await db.execute(
        select(Rnu).where(Rnu.supplier_biin == biin, Rnu.is_active == True).limit(1)
    )
    rnu = rnu_result.scalar_one_or_none()

    return {
        "company": {
            "biin": biin,
            "name_ru": subject.name_ru,
            "name_kz": subject.name_kz,
            "regdate": str(subject.regdate) if subject.regdate else None,
            "crdate": str(subject.crdate) if subject.crdate else None,
            "type_supplier": subject.type_supplier,
            "mark_small_employer": subject.mark_small_employer,
            "mark_resident": subject.mark_resident,
            "oked_list": subject.oked_list,
            "email": subject.email,
            "phone": subject.phone,
        },
        "stats": {
            "total_contracts": stats.total_contracts or 0,
            "total_sum": float(stats.total_sum or 0),
            "unique_customers": stats.unique_customers or 0,
        },
        "top_customers": [
            {"customer_bin": r.customer_bin, "contract_count": r.cnt, "total_sum": float(r.total or 0)}
            for r in top_customers
        ],
        "rnu": {
            "is_active": True,
            "reason": rnu.reason if rnu else None,
            "start_date": str(rnu.start_date) if rnu and rnu.start_date else None,
            "system_id": rnu.system_id if rnu else None,
        } if rnu else {"is_active": False},
    }
