"""
All 16 risk indicators for the Tender Risk Radar.
Each indicator returns a dict: {flag: bool, value: float, evidence: dict}
"""
import logging
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, text

from app.models.procurement import (
    TrdBuy, Lot, TrdApp, TrdAppLot, Contract, Subject, Rnu, TreasuryPay,
)

logger = logging.getLogger(__name__)


# ─── 1. SHORT_DEADLINE ───────────────────────────────────────────────────────

async def check_short_deadline(db: AsyncSession, trd_buy_id: int) -> dict:
    """Срок приёма заявок менее 3 рабочих дней."""
    result = await db.execute(
        select(TrdBuy.start_date, TrdBuy.end_date).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.start_date or not row.end_date:
        return {"flag": False, "value": None, "evidence": {}}

    delta_days = (row.end_date - row.start_date).days
    flag = delta_days < 3
    return {
        "flag": flag,
        "value": float(delta_days),
        "evidence": {
            "start_date": str(row.start_date),
            "end_date": str(row.end_date),
            "deadline_days": delta_days,
            "threshold": 3,
        },
    }


# ─── 2. FEW_BIDS ─────────────────────────────────────────────────────────────

async def check_few_bids(db: AsyncSession, trd_buy_id: int) -> dict:
    """Количество заявок 1-2 при открытом конкурсе."""
    result = await db.execute(
        select(func.count(TrdApp.id)).where(TrdApp.buy_id == trd_buy_id)
    )
    bid_count = result.scalar() or 0
    flag = 0 < bid_count <= 2
    return {
        "flag": flag,
        "value": float(bid_count),
        "evidence": {"bid_count": bid_count, "threshold": 2},
    }


# ─── 3. LOT_SPLITTING ────────────────────────────────────────────────────────

async def check_lot_splitting(db: AsyncSession, trd_buy_id: int) -> dict:
    """Разбивка крупной закупки на много мелких лотов."""
    result = await db.execute(
        select(func.count(Lot.id), func.sum(Lot.amount), func.avg(Lot.amount))
        .where(Lot.trd_buy_id == trd_buy_id, Lot.is_deleted == False)
    )
    row = result.first()
    lot_count = row[0] or 0
    total_sum = float(row[1] or 0)
    avg_amount = float(row[2] or 0)

    # Flag: many lots (>5) with small individual amounts but large total
    flag = lot_count > 5 and avg_amount < 5_000_000 and total_sum > 10_000_000
    return {
        "flag": flag,
        "value": float(lot_count),
        "evidence": {
            "lot_count": lot_count,
            "total_sum": total_sum,
            "avg_lot_amount": avg_amount,
        },
    }


# ─── 4. RECURRING_WINNER ─────────────────────────────────────────────────────

async def check_recurring_winner(db: AsyncSession, customer_bin: str, supplier_biin: str) -> dict:
    """Один поставщик выигрывает >70% тендеров у данного заказчика."""
    total_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.customer_bin == customer_bin,
            Contract.is_deleted == False,
        )
    )
    total = total_result.scalar() or 0
    if total == 0:
        return {"flag": False, "value": 0.0, "evidence": {}}

    supplier_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.customer_bin == customer_bin,
            Contract.supplier_biin == supplier_biin,
            Contract.is_deleted == False,
        )
    )
    supplier_count = supplier_result.scalar() or 0
    win_rate = supplier_count / total
    flag = win_rate > 0.70 and total >= 5

    return {
        "flag": flag,
        "value": round(win_rate * 100, 1),
        "evidence": {
            "customer_bin": customer_bin,
            "supplier_biin": supplier_biin,
            "supplier_contracts": supplier_count,
            "total_contracts": total,
            "win_rate_pct": round(win_rate * 100, 1),
            "threshold_pct": 70,
        },
    }


# ─── 5. SUPPLIER_CONCENTRATION ───────────────────────────────────────────────

async def check_supplier_concentration(db: AsyncSession, supplier_biin: str) -> dict:
    """>80% контрактов поставщика с одним заказчиком."""
    total_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.supplier_biin == supplier_biin,
            Contract.is_deleted == False,
        )
    )
    total = total_result.scalar() or 0
    if total < 5:
        return {"flag": False, "value": 0.0, "evidence": {}}

    top_result = await db.execute(
        select(Contract.customer_bin, func.count(Contract.id).label("cnt"))
        .where(Contract.supplier_biin == supplier_biin, Contract.is_deleted == False)
        .group_by(Contract.customer_bin)
        .order_by(text("cnt DESC"))
        .limit(1)
    )
    top = top_result.first()
    if not top:
        return {"flag": False, "value": 0.0, "evidence": {}}

    concentration = top.cnt / total
    flag = concentration > 0.80

    return {
        "flag": flag,
        "value": round(concentration * 100, 1),
        "evidence": {
            "supplier_biin": supplier_biin,
            "top_customer_bin": top.customer_bin,
            "top_customer_contracts": top.cnt,
            "total_contracts": total,
            "concentration_pct": round(concentration * 100, 1),
        },
    }


# ─── 6. ADDENDUM_VALUE_INCREASE ──────────────────────────────────────────────

async def check_addendum_value_increase(db: AsyncSession, contract_id: int) -> dict:
    """Допсоглашение увеличило сумму договора >20%."""
    result = await db.execute(
        select(Contract.contract_sum_wnds, Contract.root_id, Contract.parent_id)
        .where(Contract.id == contract_id)
    )
    row = result.first()
    if not row or not row.root_id:
        return {"flag": False, "value": None, "evidence": {}}

    # Get root contract
    root_result = await db.execute(
        select(Contract.contract_sum_wnds).where(Contract.id == row.root_id)
    )
    root = root_result.first()
    if not root or not root.contract_sum_wnds or not row.contract_sum_wnds:
        return {"flag": False, "value": None, "evidence": {}}

    increase_pct = (float(row.contract_sum_wnds) - float(root.contract_sum_wnds)) / float(root.contract_sum_wnds)
    flag = increase_pct > 0.20

    return {
        "flag": flag,
        "value": round(increase_pct * 100, 1),
        "evidence": {
            "root_contract_id": row.root_id,
            "original_sum": float(root.contract_sum_wnds),
            "current_sum": float(row.contract_sum_wnds),
            "increase_pct": round(increase_pct * 100, 1),
            "threshold_pct": 20,
        },
    }


# ─── 7. WIN_MIN_THEN_ADDENDUM ────────────────────────────────────────────────

async def check_win_min_then_addendum(db: AsyncSession, contract_id: int) -> dict:
    """Выигрыш по минимальной цене, затем немедленное допсоглашение."""
    result = await db.execute(
        select(Contract).where(Contract.id == contract_id)
    )
    contract = result.scalar_one_or_none()
    if not contract or not contract.root_id:
        return {"flag": False, "value": None, "evidence": {}}

    # Find addendum (child contract)
    addendum_result = await db.execute(
        select(Contract.sign_date, Contract.contract_sum_wnds)
        .where(Contract.parent_id == contract_id)
        .order_by(Contract.sign_date)
        .limit(1)
    )
    addendum = addendum_result.first()
    if not addendum or not addendum.sign_date or not contract.sign_date:
        return {"flag": False, "value": None, "evidence": {}}

    days_to_addendum = (addendum.sign_date - contract.sign_date).days
    flag = days_to_addendum <= 30  # Addendum within 30 days of signing

    return {
        "flag": flag,
        "value": float(days_to_addendum),
        "evidence": {
            "contract_sign_date": str(contract.sign_date),
            "addendum_sign_date": str(addendum.sign_date),
            "days_to_addendum": days_to_addendum,
            "addendum_sum": float(addendum.contract_sum_wnds or 0),
        },
    }


# ─── 8. WEIRD_EXECUTION_TIME ─────────────────────────────────────────────────

async def check_weird_execution_time(db: AsyncSession, contract_id: int) -> dict:
    """Аномально короткий (<7 дней) или длинный (>730 дней) срок выполнения."""
    result = await db.execute(
        select(Contract.sign_date, Contract.plan_exec_date).where(Contract.id == contract_id)
    )
    row = result.first()
    if not row or not row.sign_date or not row.plan_exec_date:
        return {"flag": False, "value": None, "evidence": {}}

    exec_days = (row.plan_exec_date - row.sign_date).days
    flag = exec_days < 7 or exec_days > 730

    return {
        "flag": flag,
        "value": float(exec_days),
        "evidence": {
            "sign_date": str(row.sign_date),
            "plan_exec_date": str(row.plan_exec_date),
            "execution_days": exec_days,
            "anomaly": "too_short" if exec_days < 7 else "too_long",
        },
    }


# ─── 9. RNU_FLAG ─────────────────────────────────────────────────────────────

async def check_rnu_flag(db: AsyncSession, supplier_biin: str) -> dict:
    """Поставщик в реестре недобросовестных поставщиков."""
    result = await db.execute(
        select(Rnu.id, Rnu.reason, Rnu.start_date, Rnu.system_id)
        .where(Rnu.supplier_biin == supplier_biin, Rnu.is_active == True)
        .limit(1)
    )
    rnu = result.first()
    flag = rnu is not None

    return {
        "flag": flag,
        "value": 1.0 if flag else 0.0,
        "evidence": {
            "rnu_id": rnu.id if rnu else None,
            "reason": rnu.reason if rnu else None,
            "start_date": str(rnu.start_date) if rnu else None,
            "system_id": rnu.system_id if rnu else None,
        } if flag else {},
    }


# ─── 10. DUMPING_FLAG ────────────────────────────────────────────────────────

async def check_dumping_flag(db: AsyncSession, lot_id: int) -> dict:
    """Зафиксирован демпинг цены в лоте."""
    result = await db.execute(
        select(Lot.dumping_flag, Lot.amount).where(Lot.id == lot_id)
    )
    row = result.first()
    if not row:
        return {"flag": False, "value": None, "evidence": {}}

    return {
        "flag": bool(row.dumping_flag),
        "value": 1.0 if row.dumping_flag else 0.0,
        "evidence": {"lot_id": lot_id, "dumping_flag": row.dumping_flag, "lot_amount": float(row.amount or 0)},
    }


# ─── 11. NEW_COMPANY_BIG_CONTRACT ────────────────────────────────────────────

async def check_new_company_big_contract(
    db: AsyncSession, supplier_biin: str, contract_sum: float
) -> dict:
    """Компания зарегистрирована <1 года назад, контракт >10M тенге."""
    result = await db.execute(
        select(Subject.regdate, Subject.crdate, Subject.name_ru)
        .where(or_(Subject.bin == supplier_biin, Subject.iin == supplier_biin))
        .limit(1)
    )
    row = result.first()
    if not row:
        return {"flag": False, "value": None, "evidence": {}}

    reg_date = row.regdate or row.crdate
    if not reg_date:
        return {"flag": False, "value": None, "evidence": {}}

    company_age_days = (datetime.utcnow() - reg_date).days
    flag = company_age_days < 365 and contract_sum > 10_000_000

    return {
        "flag": flag,
        "value": float(company_age_days),
        "evidence": {
            "supplier_biin": supplier_biin,
            "company_name": row.name_ru,
            "regdate": str(reg_date),
            "company_age_days": company_age_days,
            "contract_sum": contract_sum,
            "threshold_sum": 10_000_000,
        },
    }


# ─── 12. PAYMENT_WITHOUT_ACT ─────────────────────────────────────────────────

async def check_payment_without_act(db: AsyncSession, contract_id: int) -> dict:
    """Казначейский платёж без акта выполнения работ."""
    pay_result = await db.execute(
        select(func.count(TreasuryPay.id), func.sum(TreasuryPay.pay_amount))
        .where(TreasuryPay.contract_id == contract_id)
    )
    pay_row = pay_result.first()
    pay_count = pay_row[0] or 0
    pay_sum = float(pay_row[1] or 0)

    if pay_count == 0:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Check if acts exist for this contract
    act_result = await db.execute(
        text("SELECT COUNT(*) FROM acts WHERE contract_id = :cid"),
        {"cid": contract_id}
    )
    act_count = act_result.scalar() or 0

    flag = pay_count > 0 and act_count == 0

    return {
        "flag": flag,
        "value": float(pay_sum),
        "evidence": {
            "contract_id": contract_id,
            "payment_count": pay_count,
            "total_paid": pay_sum,
            "act_count": act_count,
        },
    }


# ─── 13. HIGH_WIN_RATE_FEW_BIDS ──────────────────────────────────────────────

async def check_high_win_rate_few_bids(db: AsyncSession, supplier_biin: str) -> dict:
    """Win-rate >90% при среднем числе заявок <3 в тендере."""
    # Total tenders where supplier participated
    participated = await db.execute(
        select(func.count(func.distinct(TrdApp.buy_id)))
        .where(TrdApp.supplier_biin == supplier_biin)
    )
    total_participated = participated.scalar() or 0
    if total_participated < 5:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Tenders where supplier won
    won = await db.execute(
        select(func.count(func.distinct(Contract.trd_buy_id)))
        .where(Contract.supplier_biin == supplier_biin, Contract.is_deleted == False)
    )
    total_won = won.scalar() or 0
    win_rate = total_won / total_participated if total_participated > 0 else 0

    # Average bids per tender
    avg_bids_result = await db.execute(
        select(func.avg(text("bid_count")))
        .select_from(
            select(TrdApp.buy_id, func.count(TrdApp.id).label("bid_count"))
            .where(TrdApp.buy_id.in_(
                select(TrdApp.buy_id).where(TrdApp.supplier_biin == supplier_biin)
            ))
            .group_by(TrdApp.buy_id)
            .subquery()
        )
    )
    avg_bids = float(avg_bids_result.scalar() or 0)
    flag = win_rate > 0.90 and avg_bids < 3

    return {
        "flag": flag,
        "value": round(win_rate * 100, 1),
        "evidence": {
            "supplier_biin": supplier_biin,
            "total_participated": total_participated,
            "total_won": total_won,
            "win_rate_pct": round(win_rate * 100, 1),
            "avg_bids_per_tender": round(avg_bids, 1),
        },
    }


# ─── 14. CAROUSEL_PATTERN ────────────────────────────────────────────────────

async def check_carousel_pattern(db: AsyncSession, customer_bin: str) -> dict:
    """Паттерн чередования победителей А→В→С→А у одного заказчика."""
    result = await db.execute(
        select(Contract.supplier_biin, Contract.sign_date)
        .where(Contract.customer_bin == customer_bin, Contract.is_deleted == False)
        .order_by(Contract.sign_date)
        .limit(50)
    )
    rows = result.all()
    if len(rows) < 6:
        return {"flag": False, "value": 0.0, "evidence": {}}

    suppliers = [r.supplier_biin for r in rows]
    # Detect cycling: check if pattern repeats (A,B,C,A,B,C or similar)
    unique_suppliers = list(dict.fromkeys(suppliers))  # preserve order
    if len(unique_suppliers) < 2:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Count how many times the sequence "resets" to a previous supplier
    rotations = 0
    seen = set()
    for s in suppliers:
        if s in seen:
            rotations += 1
            seen = {s}
        else:
            seen.add(s)

    flag = rotations >= 2 and len(unique_suppliers) >= 2

    return {
        "flag": flag,
        "value": float(rotations),
        "evidence": {
            "customer_bin": customer_bin,
            "unique_winners": len(unique_suppliers),
            "rotation_count": rotations,
            "winner_sequence": suppliers[:10],
        },
    }


# ─── 15. LAST_MINUTE_CHANGES ─────────────────────────────────────────────────

async def check_last_minute_changes(db: AsyncSession, trd_buy_id: int) -> dict:
    """Изменение условий тендера за <24ч до дедлайна (через journal)."""
    result = await db.execute(
        select(TrdBuy.end_date, TrdBuy.last_update_at).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.end_date or not row.last_update_at:
        return {"flag": False, "value": None, "evidence": {}}

    hours_before_deadline = (row.end_date - row.last_update_at).total_seconds() / 3600
    flag = 0 < hours_before_deadline < 24

    return {
        "flag": flag,
        "value": round(hours_before_deadline, 1),
        "evidence": {
            "end_date": str(row.end_date),
            "last_update": str(row.last_update_at),
            "hours_before_deadline": round(hours_before_deadline, 1),
        },
    }


# ─── 16. COMMON_REQUISITES ───────────────────────────────────────────────────

async def check_common_requisites(db: AsyncSession, trd_buy_id: int) -> dict:
    """Участники конкурса имеют общий адрес или телефон."""
    # Get all bidders for this tender
    bidders_result = await db.execute(
        select(func.distinct(TrdApp.supplier_biin))
        .where(TrdApp.buy_id == trd_buy_id)
    )
    biins = [r[0] for r in bidders_result.all() if r[0]]
    if len(biins) < 2:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Get their contact info
    subjects_result = await db.execute(
        select(Subject.bin, Subject.iin, Subject.phone, Subject.email)
        .where(or_(Subject.bin.in_(biins), Subject.iin.in_(biins)))
    )
    subjects = subjects_result.all()

    phones = [s.phone for s in subjects if s.phone]
    emails = [s.email for s in subjects if s.email]

    common_phones = [p for p in set(phones) if phones.count(p) > 1]
    common_emails = [e for e in set(emails) if emails.count(e) > 1]

    flag = bool(common_phones or common_emails)

    return {
        "flag": flag,
        "value": float(len(common_phones) + len(common_emails)),
        "evidence": {
            "bidder_count": len(biins),
            "common_phones": common_phones,
            "common_emails": common_emails,
        },
    }
