from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import Optional
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Subject, Contract, Rnu, RiskScore

router = APIRouter()


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
