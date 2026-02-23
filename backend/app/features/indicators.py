"""
31 risk indicators for the Tender Risk Radar.
Each indicator returns: {flag: bool, value: float|None, evidence: dict}
"""
import logging
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, text

from app.models.procurement import (
    TrdBuy, Lot, TrdApp, TrdAppLot, Contract, Subject, Rnu,
    TreasuryPay, ContractAct, ContractPayment, ContractSpecSum,
)

logger = logging.getLogger(__name__)

_EMPTY = {"flag": False, "value": None, "evidence": {}}


# ─── 1. LOT_SPLITTING ────────────────────────────────────────────────────────

async def check_lot_splitting(db: AsyncSession, trd_buy_id: int) -> dict:
    """Splitting a large procurement into many smaller lots."""
    result = await db.execute(
        select(func.count(Lot.id), func.sum(Lot.amount), func.avg(Lot.amount))
        .where(Lot.trd_buy_id == trd_buy_id, Lot.is_deleted == False)
    )
    row = result.first()
    lot_count = row[0] or 0
    total_sum = float(row[1] or 0)
    avg_amount = float(row[2] or 0)

    flag = lot_count > 5 and avg_amount < 5_000_000 and total_sum > 10_000_000
    return {
        "flag": flag,
        "value": float(lot_count),
        "evidence": {"lot_count": lot_count, "total_sum": total_sum, "avg_lot_amount": avg_amount},
    }


# ─── 2. SHORT_DEADLINE ───────────────────────────────────────────────────────

async def check_short_deadline(db: AsyncSession, trd_buy_id: int) -> dict:
    """Bid submission window < 3 days."""
    result = await db.execute(
        select(TrdBuy.start_date, TrdBuy.end_date).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.start_date or not row.end_date:
        return _EMPTY
    delta_days = (row.end_date - row.start_date).days
    flag = delta_days < 3
    return {
        "flag": flag, "value": float(delta_days),
        "evidence": {"start_date": str(row.start_date), "end_date": str(row.end_date),
                     "deadline_days": delta_days, "threshold": 3},
    }


# ─── 3. FEW_BIDS ─────────────────────────────────────────────────────────────

async def check_few_bids(db: AsyncSession, trd_buy_id: int) -> dict:
    """Very low competition: only 1-2 bids."""
    result = await db.execute(
        select(func.count(TrdApp.id)).where(TrdApp.buy_id == trd_buy_id)
    )
    bid_count = result.scalar() or 0
    flag = 0 < bid_count <= 2
    return {"flag": flag, "value": float(bid_count), "evidence": {"bid_count": bid_count, "threshold": 2}}


# ─── 4. RECURRING_WINNER ─────────────────────────────────────────────────────

async def check_recurring_winner(db: AsyncSession, customer_bin: str, supplier_biin: str) -> dict:
    """Same supplier wins >70% of contracts for this customer (min 5)."""
    total_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.customer_bin == customer_bin, Contract.is_deleted == False,
        )
    )
    total = total_result.scalar() or 0
    if total < 5:
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
        "flag": flag, "value": round(win_rate * 100, 1),
        "evidence": {"customer_bin": customer_bin, "supplier_biin": supplier_biin,
                     "supplier_contracts": supplier_count, "total_contracts": total,
                     "win_rate_pct": round(win_rate * 100, 1)},
    }


# ─── 5. COMMON_REQUISITES ────────────────────────────────────────────────────

async def check_common_requisites(db: AsyncSession, trd_buy_id: int) -> dict:
    """Different bidders share phone/email."""
    bidders_result = await db.execute(
        select(func.distinct(TrdApp.supplier_biin)).where(TrdApp.buy_id == trd_buy_id)
    )
    biins = [r[0] for r in bidders_result.all() if r[0]]
    if len(biins) < 2:
        return {"flag": False, "value": 0.0, "evidence": {}}

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
        "flag": flag, "value": float(len(common_phones) + len(common_emails)),
        "evidence": {"bidder_count": len(biins), "common_phones": common_phones, "common_emails": common_emails},
    }


# ─── 6. NEW_COMPANY_BIG_CONTRACT ─────────────────────────────────────────────

async def check_new_company_big_contract(db: AsyncSession, supplier_biin: str, contract_sum: float) -> dict:
    """Company registered < 1 year ago wins contract > 10M."""
    result = await db.execute(
        select(Subject.regdate, Subject.crdate, Subject.name_ru)
        .where(or_(Subject.bin == supplier_biin, Subject.iin == supplier_biin))
        .limit(1)
    )
    row = result.first()
    if not row:
        return _EMPTY
    reg_date = row.regdate or row.crdate
    if not reg_date:
        return _EMPTY
    company_age_days = (datetime.utcnow() - reg_date).days
    flag = company_age_days < 365 and contract_sum > 10_000_000
    return {
        "flag": flag, "value": float(company_age_days),
        "evidence": {"supplier_biin": supplier_biin, "company_name": row.name_ru,
                     "regdate": str(reg_date), "company_age_days": company_age_days,
                     "contract_sum": contract_sum},
    }


# ─── 7. CAROUSEL_PATTERN ─────────────────────────────────────────────────────

async def check_carousel_pattern(db: AsyncSession, customer_bin: str) -> dict:
    """Winner rotation pattern A→B→C→A for same customer."""
    result = await db.execute(
        select(Contract.supplier_biin, Contract.sign_date)
        .where(Contract.customer_bin == customer_bin, Contract.is_deleted == False)
        .order_by(Contract.sign_date).limit(50)
    )
    rows = result.all()
    if len(rows) < 6:
        return {"flag": False, "value": 0.0, "evidence": {}}

    suppliers = [r.supplier_biin for r in rows]
    unique_suppliers = list(dict.fromkeys(suppliers))
    if len(unique_suppliers) < 2:
        return {"flag": False, "value": 0.0, "evidence": {}}

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
        "flag": flag, "value": float(rotations),
        "evidence": {"customer_bin": customer_bin, "unique_winners": len(unique_suppliers),
                     "rotation_count": rotations, "winner_sequence": suppliers[:10]},
    }


# ─── 8. RNU_FLAG ─────────────────────────────────────────────────────────────

async def check_rnu_flag(db: AsyncSession, supplier_biin: str) -> dict:
    """Supplier is in the unreliable suppliers registry."""
    result = await db.execute(
        select(Rnu.id, Rnu.reason, Rnu.start_date, Rnu.system_id)
        .where(Rnu.supplier_biin == supplier_biin, Rnu.is_active == True)
        .limit(1)
    )
    rnu = result.first()
    flag = rnu is not None
    return {
        "flag": flag, "value": 1.0 if flag else 0.0,
        "evidence": {
            "rnu_id": rnu.id, "reason": rnu.reason,
            "start_date": str(rnu.start_date), "system_id": rnu.system_id,
        } if flag else {},
    }


# ─── 9. DUMPING_FLAG ─────────────────────────────────────────────────────────

async def check_dumping_flag(db: AsyncSession, lot_id: int) -> dict:
    """Strong price dumping detected."""
    result = await db.execute(select(Lot.dumping_flag, Lot.amount).where(Lot.id == lot_id))
    row = result.first()
    if not row:
        return _EMPTY
    return {
        "flag": bool(row.dumping_flag), "value": 1.0 if row.dumping_flag else 0.0,
        "evidence": {"lot_id": lot_id, "dumping_flag": row.dumping_flag, "lot_amount": float(row.amount or 0)},
    }


# ─── 10. IDENTICAL_BID_PRICES ────────────────────────────────────────────────

async def check_identical_bid_prices(db: AsyncSession, trd_buy_id: int) -> dict:
    """Different suppliers submit identical prices for the same lot."""
    # Find lots for this tender
    lot_ids_result = await db.execute(
        select(Lot.id).where(Lot.trd_buy_id == trd_buy_id)
    )
    lot_ids = [r[0] for r in lot_ids_result.all()]
    if not lot_ids:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Find duplicate prices per lot
    subq = (
        select(
            TrdAppLot.lot_id,
            TrdAppLot.price,
            func.count(func.distinct(TrdApp.supplier_biin)).label("supplier_cnt"),
        )
        .join(TrdApp, TrdApp.id == TrdAppLot.trd_app_id)
        .where(TrdAppLot.lot_id.in_(lot_ids), TrdAppLot.price.isnot(None))
        .group_by(TrdAppLot.lot_id, TrdAppLot.price)
        .having(func.count(func.distinct(TrdApp.supplier_biin)) > 1)
    )
    result = await db.execute(subq)
    duplicates = result.all()

    flag = len(duplicates) > 0
    return {
        "flag": flag, "value": float(len(duplicates)),
        "evidence": {"identical_price_groups": len(duplicates),
                     "details": [{"lot_id": d.lot_id, "price": float(d.price), "suppliers": d.supplier_cnt}
                                 for d in duplicates[:5]]},
    }


# ─── 11. TINY_WIN_MARGIN ─────────────────────────────────────────────────────

async def check_tiny_win_margin(db: AsyncSession, trd_buy_id: int) -> dict:
    """Very small difference between best and second-best bid (< 1%)."""
    lot_ids_result = await db.execute(
        select(Lot.id).where(Lot.trd_buy_id == trd_buy_id)
    )
    lot_ids = [r[0] for r in lot_ids_result.all()]
    if not lot_ids:
        return {"flag": False, "value": None, "evidence": {}}

    min_margin_pct = None
    for lot_id in lot_ids[:10]:  # cap to avoid slow queries
        prices_result = await db.execute(
            select(TrdAppLot.price)
            .where(TrdAppLot.lot_id == lot_id, TrdAppLot.price.isnot(None), TrdAppLot.price > 0)
            .order_by(TrdAppLot.price)
            .limit(2)
        )
        prices = [float(r.price) for r in prices_result.all()]
        if len(prices) >= 2 and prices[0] > 0:
            margin_pct = (prices[1] - prices[0]) / prices[0] * 100
            if min_margin_pct is None or margin_pct < min_margin_pct:
                min_margin_pct = margin_pct

    if min_margin_pct is None:
        return {"flag": False, "value": None, "evidence": {}}

    flag = min_margin_pct < 1.0
    return {
        "flag": flag, "value": round(min_margin_pct, 2),
        "evidence": {"min_margin_pct": round(min_margin_pct, 2), "threshold_pct": 1.0},
    }


# ─── 12. LATE_BID_SUBMISSION ─────────────────────────────────────────────────

async def check_late_bid_submission(db: AsyncSession, trd_buy_id: int) -> dict:
    """Bids submitted in the last 60 minutes before deadline."""
    tender_result = await db.execute(
        select(TrdBuy.end_date).where(TrdBuy.id == trd_buy_id)
    )
    tender = tender_result.first()
    if not tender or not tender.end_date:
        return _EMPTY

    cutoff = tender.end_date - timedelta(minutes=60)
    late_result = await db.execute(
        select(func.count(TrdApp.id))
        .where(TrdApp.buy_id == trd_buy_id, TrdApp.date_apply >= cutoff)
    )
    late_count = late_result.scalar() or 0

    total_result = await db.execute(
        select(func.count(TrdApp.id)).where(TrdApp.buy_id == trd_buy_id)
    )
    total_count = total_result.scalar() or 0

    flag = late_count > 0 and total_count > 0
    return {
        "flag": flag, "value": float(late_count),
        "evidence": {"late_bids": late_count, "total_bids": total_count,
                     "deadline": str(tender.end_date), "cutoff_minutes": 60},
    }


# ─── 13. REPEAT_TENDER ───────────────────────────────────────────────────────

async def check_repeat_tender(db: AsyncSession, trd_buy_id: int) -> dict:
    """Tender is a repeat (has parent_id)."""
    result = await db.execute(
        select(TrdBuy.parent_id, TrdBuy.repeat_start_date, TrdBuy.repeat_end_date)
        .where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row:
        return _EMPTY
    flag = row.parent_id is not None
    return {
        "flag": flag, "value": 1.0 if flag else 0.0,
        "evidence": {"parent_id": row.parent_id,
                     "repeat_start": str(row.repeat_start_date) if row.repeat_start_date else None,
                     "repeat_end": str(row.repeat_end_date) if row.repeat_end_date else None},
    }


# ─── 14. CANCELLED_TENDER ────────────────────────────────────────────────────

async def check_cancelled_tender(db: AsyncSession, trd_buy_id: int) -> dict:
    """Tender has cancel status (ref_buy_status_id in cancel range)."""
    result = await db.execute(
        select(TrdBuy.ref_buy_status_id).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row:
        return _EMPTY
    # Status IDs for cancelled tenders (210, 250, etc. — common in goszakup)
    cancel_statuses = {210, 250, 260}
    flag = row.ref_buy_status_id in cancel_statuses if row.ref_buy_status_id else False
    return {
        "flag": flag, "value": float(row.ref_buy_status_id or 0),
        "evidence": {"ref_buy_status_id": row.ref_buy_status_id, "is_cancelled": flag},
    }


# ─── 15. PAUSED_TENDER ───────────────────────────────────────────────────────

async def check_paused_tender(db: AsyncSession, trd_buy_id: int) -> dict:
    """Tender was paused (ref_buy_status_id in paused range)."""
    result = await db.execute(
        select(TrdBuy.ref_buy_status_id).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row:
        return _EMPTY
    pause_statuses = {220, 230}
    flag = row.ref_buy_status_id in pause_statuses if row.ref_buy_status_id else False
    return {
        "flag": flag, "value": float(row.ref_buy_status_id or 0),
        "evidence": {"ref_buy_status_id": row.ref_buy_status_id, "is_paused": flag},
    }


# ─── 16. NIGHT_OR_WEEKEND_PUBLISH ────────────────────────────────────────────

async def check_night_or_weekend_publish(db: AsyncSession, trd_buy_id: int) -> dict:
    """Tender published at unusual time (night or weekend)."""
    result = await db.execute(
        select(TrdBuy.publish_date).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.publish_date:
        return _EMPTY
    dt = row.publish_date
    is_night = dt.hour < 7 or dt.hour >= 22
    is_weekend = dt.weekday() >= 5
    flag = is_night or is_weekend
    return {
        "flag": flag, "value": float(dt.hour),
        "evidence": {"publish_date": str(dt), "hour": dt.hour,
                     "weekday": dt.weekday(), "is_night": is_night, "is_weekend": is_weekend},
    }


# ─── 17. SHORT_DISCUSSION_WINDOW ─────────────────────────────────────────────

async def check_short_discussion_window(db: AsyncSession, trd_buy_id: int) -> dict:
    """Discussion window < 2 days."""
    result = await db.execute(
        select(TrdBuy.discus_start_date, TrdBuy.discus_end_date).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.discus_start_date or not row.discus_end_date:
        return _EMPTY
    delta = (row.discus_end_date - row.discus_start_date).days
    flag = delta < 2
    return {
        "flag": flag, "value": float(delta),
        "evidence": {"discus_start": str(row.discus_start_date), "discus_end": str(row.discus_end_date),
                     "discussion_days": delta, "threshold": 2},
    }


# ─── 18. LAST_MINUTE_CHANGES ─────────────────────────────────────────────────

async def check_last_minute_changes(db: AsyncSession, trd_buy_id: int) -> dict:
    """Tender updated < 24h before deadline."""
    result = await db.execute(
        select(TrdBuy.end_date, TrdBuy.last_update_at).where(TrdBuy.id == trd_buy_id)
    )
    row = result.first()
    if not row or not row.end_date or not row.last_update_at:
        return _EMPTY
    hours_before = (row.end_date - row.last_update_at).total_seconds() / 3600
    flag = 0 < hours_before < 24
    return {
        "flag": flag, "value": round(hours_before, 1),
        "evidence": {"end_date": str(row.end_date), "last_update": str(row.last_update_at),
                     "hours_before_deadline": round(hours_before, 1)},
    }


# ─── 19. SUPPLIER_CONCENTRATION ──────────────────────────────────────────────

async def check_supplier_concentration(db: AsyncSession, supplier_biin: str) -> dict:
    """>80% contracts with one customer."""
    total_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.supplier_biin == supplier_biin, Contract.is_deleted == False,
        )
    )
    total = total_result.scalar() or 0
    if total < 5:
        return {"flag": False, "value": 0.0, "evidence": {}}

    top_result = await db.execute(
        select(Contract.customer_bin, func.count(Contract.id).label("cnt"))
        .where(Contract.supplier_biin == supplier_biin, Contract.is_deleted == False)
        .group_by(Contract.customer_bin).order_by(text("cnt DESC")).limit(1)
    )
    top = top_result.first()
    if not top:
        return {"flag": False, "value": 0.0, "evidence": {}}
    concentration = top.cnt / total
    flag = concentration > 0.80
    return {
        "flag": flag, "value": round(concentration * 100, 1),
        "evidence": {"supplier_biin": supplier_biin, "top_customer_bin": top.customer_bin,
                     "top_customer_contracts": top.cnt, "total_contracts": total},
    }


# ─── 20. CUSTOMER_WINNER_CONCENTRATION ───────────────────────────────────────

async def check_customer_winner_concentration(db: AsyncSession, customer_bin: str) -> dict:
    """Top-3 suppliers take >80% of customer's contracts."""
    total_result = await db.execute(
        select(func.count(Contract.id)).where(
            Contract.customer_bin == customer_bin, Contract.is_deleted == False,
        )
    )
    total = total_result.scalar() or 0
    if total < 5:
        return {"flag": False, "value": 0.0, "evidence": {}}

    top_result = await db.execute(
        select(Contract.supplier_biin, func.count(Contract.id).label("cnt"))
        .where(Contract.customer_bin == customer_bin, Contract.is_deleted == False)
        .group_by(Contract.supplier_biin).order_by(text("cnt DESC")).limit(3)
    )
    top_rows = top_result.all()
    top3_count = sum(r.cnt for r in top_rows)
    concentration = top3_count / total
    flag = concentration > 0.80
    return {
        "flag": flag, "value": round(concentration * 100, 1),
        "evidence": {"customer_bin": customer_bin, "top3_contracts": top3_count,
                     "total_contracts": total, "concentration_pct": round(concentration * 100, 1)},
    }


# ─── 21. HIGH_WIN_RATE_FEW_BIDS ──────────────────────────────────────────────

async def check_high_win_rate_few_bids(db: AsyncSession, supplier_biin: str) -> dict:
    """Win-rate >90% while average bids < 3."""
    participated = await db.execute(
        select(func.count(func.distinct(TrdApp.buy_id))).where(TrdApp.supplier_biin == supplier_biin)
    )
    total_participated = participated.scalar() or 0
    if total_participated < 5:
        return {"flag": False, "value": 0.0, "evidence": {}}

    won = await db.execute(
        select(func.count(func.distinct(Contract.trd_buy_id)))
        .where(Contract.supplier_biin == supplier_biin, Contract.is_deleted == False)
    )
    total_won = won.scalar() or 0
    win_rate = total_won / total_participated

    avg_bids_result = await db.execute(
        select(func.avg(text("bid_count")))
        .select_from(
            select(TrdApp.buy_id, func.count(TrdApp.id).label("bid_count"))
            .where(TrdApp.buy_id.in_(select(TrdApp.buy_id).where(TrdApp.supplier_biin == supplier_biin)))
            .group_by(TrdApp.buy_id).subquery()
        )
    )
    avg_bids = float(avg_bids_result.scalar() or 0)
    flag = win_rate > 0.90 and avg_bids < 3
    return {
        "flag": flag, "value": round(win_rate * 100, 1),
        "evidence": {"supplier_biin": supplier_biin, "total_participated": total_participated,
                     "total_won": total_won, "win_rate_pct": round(win_rate * 100, 1),
                     "avg_bids_per_tender": round(avg_bids, 1)},
    }


# ─── 22. ADDENDUM_VALUE_INCREASE ─────────────────────────────────────────────

async def check_addendum_value_increase(db: AsyncSession, contract_id: int) -> dict:
    """Contract value increased >20% via addendum chain."""
    result = await db.execute(
        select(Contract.contract_sum_wnds, Contract.root_id, Contract.parent_id)
        .where(Contract.id == contract_id)
    )
    row = result.first()
    if not row or not row.root_id:
        return _EMPTY

    root_result = await db.execute(
        select(Contract.contract_sum_wnds).where(Contract.id == row.root_id)
    )
    root = root_result.first()
    if not root or not root.contract_sum_wnds or not row.contract_sum_wnds:
        return _EMPTY

    increase_pct = (float(row.contract_sum_wnds) - float(root.contract_sum_wnds)) / float(root.contract_sum_wnds)
    flag = increase_pct > 0.20
    return {
        "flag": flag, "value": round(increase_pct * 100, 1),
        "evidence": {"root_contract_id": row.root_id, "original_sum": float(root.contract_sum_wnds),
                     "current_sum": float(row.contract_sum_wnds), "increase_pct": round(increase_pct * 100, 1)},
    }


# ─── 23. WIN_MIN_THEN_ADDENDUM ───────────────────────────────────────────────

async def check_win_min_then_addendum(db: AsyncSession, contract_id: int) -> dict:
    """Addendum within 30 days of signing."""
    result = await db.execute(select(Contract).where(Contract.id == contract_id))
    contract = result.scalar_one_or_none()
    if not contract or not contract.sign_date:
        return _EMPTY

    addendum_result = await db.execute(
        select(Contract.sign_date, Contract.contract_sum_wnds)
        .where(Contract.parent_id == contract_id)
        .order_by(Contract.sign_date).limit(1)
    )
    addendum = addendum_result.first()
    if not addendum or not addendum.sign_date:
        return _EMPTY

    days = (addendum.sign_date - contract.sign_date).days
    flag = days <= 30
    return {
        "flag": flag, "value": float(days),
        "evidence": {"contract_sign_date": str(contract.sign_date),
                     "addendum_sign_date": str(addendum.sign_date), "days_to_addendum": days,
                     "addendum_sum": float(addendum.contract_sum_wnds or 0)},
    }


# ─── 24. WEIRD_EXECUTION_TIME ────────────────────────────────────────────────

async def check_weird_execution_time(db: AsyncSession, contract_id: int) -> dict:
    """Execution time < 7 days or > 730 days."""
    result = await db.execute(
        select(Contract.sign_date, Contract.plan_exec_date).where(Contract.id == contract_id)
    )
    row = result.first()
    if not row or not row.sign_date or not row.plan_exec_date:
        return _EMPTY
    exec_days = (row.plan_exec_date - row.sign_date).days
    flag = exec_days < 7 or exec_days > 730
    return {
        "flag": flag, "value": float(exec_days),
        "evidence": {"sign_date": str(row.sign_date), "plan_exec_date": str(row.plan_exec_date),
                     "execution_days": exec_days, "anomaly": "too_short" if exec_days < 7 else "too_long"},
    }


# ─── 25. HIGH_PREPAY ─────────────────────────────────────────────────────────

async def check_high_prepay(db: AsyncSession, contract_id: int) -> dict:
    """Advance payment > 50% of contract sum."""
    contract_result = await db.execute(
        select(Contract.contract_sum_wnds).where(Contract.id == contract_id)
    )
    c = contract_result.first()
    if not c or not c.contract_sum_wnds or float(c.contract_sum_wnds) <= 0:
        return _EMPTY

    prepay_result = await db.execute(
        select(func.sum(TreasuryPay.prepay_sum))
        .where(TreasuryPay.contract_id == contract_id)
    )
    prepay = float(prepay_result.scalar() or 0)
    if prepay <= 0:
        return {"flag": False, "value": 0.0, "evidence": {}}

    ratio = prepay / float(c.contract_sum_wnds)
    flag = ratio > 0.50
    return {
        "flag": flag, "value": round(ratio * 100, 1),
        "evidence": {"contract_sum": float(c.contract_sum_wnds), "prepay_sum": prepay,
                     "prepay_pct": round(ratio * 100, 1)},
    }


# ─── 26. PAYMENTS_EXCEED_CONTRACT ────────────────────────────────────────────

async def check_payments_exceed_contract(db: AsyncSession, contract_id: int) -> dict:
    """Sum of payments exceeds contract value."""
    contract_result = await db.execute(
        select(Contract.contract_sum_wnds).where(Contract.id == contract_id)
    )
    c = contract_result.first()
    if not c or not c.contract_sum_wnds or float(c.contract_sum_wnds) <= 0:
        return _EMPTY

    # Try ContractPayment first, fallback to TreasuryPay
    pay_result = await db.execute(
        select(func.sum(ContractPayment.pay_sum)).where(ContractPayment.contract_id == contract_id)
    )
    total_paid = float(pay_result.scalar() or 0)
    if total_paid == 0:
        pay_result2 = await db.execute(
            select(func.sum(TreasuryPay.pay_amount)).where(TreasuryPay.contract_id == contract_id)
        )
        total_paid = float(pay_result2.scalar() or 0)

    if total_paid <= 0:
        return {"flag": False, "value": 0.0, "evidence": {}}

    ratio = total_paid / float(c.contract_sum_wnds)
    flag = ratio > 1.0
    return {
        "flag": flag, "value": round(ratio * 100, 1),
        "evidence": {"contract_sum": float(c.contract_sum_wnds), "total_paid": total_paid,
                     "ratio_pct": round(ratio * 100, 1)},
    }


# ─── 27. PAYMENT_WITHOUT_ACT ─────────────────────────────────────────────────

async def check_payment_without_act(db: AsyncSession, contract_id: int) -> dict:
    """Payments exist but no acts for this contract."""
    pay_result = await db.execute(
        select(func.count(TreasuryPay.id), func.sum(TreasuryPay.pay_amount))
        .where(TreasuryPay.contract_id == contract_id)
    )
    pay_row = pay_result.first()
    pay_count = pay_row[0] or 0
    pay_sum = float(pay_row[1] or 0)
    if pay_count == 0:
        return {"flag": False, "value": 0.0, "evidence": {}}

    act_result = await db.execute(
        select(func.count(ContractAct.id)).where(ContractAct.contract_id == contract_id)
    )
    act_count = act_result.scalar() or 0
    flag = pay_count > 0 and act_count == 0
    return {
        "flag": flag, "value": float(pay_sum),
        "evidence": {"contract_id": contract_id, "payment_count": pay_count,
                     "total_paid": pay_sum, "act_count": act_count},
    }


# ─── 28. OVERDUE_EXECUTION ───────────────────────────────────────────────────

async def check_overdue_execution(db: AsyncSession, contract_id: int) -> dict:
    """Actual completion later than planned."""
    result = await db.execute(
        select(Contract.plan_exec_date, Contract.fakt_exec_date).where(Contract.id == contract_id)
    )
    row = result.first()
    overdue_days = 0

    if row and row.plan_exec_date and row.fakt_exec_date:
        overdue_days = (row.fakt_exec_date - row.plan_exec_date).days

    # Also check ContractAct overdue
    act_result = await db.execute(
        select(func.max(ContractAct.day_overdue)).where(ContractAct.contract_id == contract_id)
    )
    act_overdue = act_result.scalar() or 0
    overdue_days = max(overdue_days, act_overdue)

    flag = overdue_days > 0
    return {
        "flag": flag, "value": float(overdue_days),
        "evidence": {"contract_id": contract_id, "overdue_days": overdue_days,
                     "plan_exec_date": str(row.plan_exec_date) if row and row.plan_exec_date else None,
                     "fakt_exec_date": str(row.fakt_exec_date) if row and row.fakt_exec_date else None},
    }


# ─── 29. FINES_PRESENT ───────────────────────────────────────────────────────

async def check_fines_present(db: AsyncSession, contract_id: int) -> dict:
    """Fines/penalties exist in acts."""
    result = await db.execute(
        select(func.sum(ContractAct.sum_fine), func.count(ContractAct.id))
        .where(ContractAct.contract_id == contract_id, ContractAct.sum_fine > 0)
    )
    row = result.first()
    total_fine = float(row[0] or 0)
    fine_count = row[1] or 0
    flag = total_fine > 0
    return {
        "flag": flag, "value": total_fine,
        "evidence": {"contract_id": contract_id, "total_fine": total_fine, "acts_with_fines": fine_count},
    }


# ─── 30. LOW_EXECUTION_RATE ──────────────────────────────────────────────────

async def check_low_execution_rate(db: AsyncSession, contract_id: int) -> dict:
    """factSum much lower than totalSum (< 50%)."""
    result = await db.execute(
        select(func.sum(ContractSpecSum.fact_sum), func.sum(ContractSpecSum.total_sum))
        .where(ContractSpecSum.contract_id == contract_id)
    )
    row = result.first()
    fact = float(row[0] or 0)
    total = float(row[1] or 0)
    if total <= 0:
        return {"flag": False, "value": None, "evidence": {}}
    rate = fact / total
    flag = rate < 0.50
    return {
        "flag": flag, "value": round(rate * 100, 1),
        "evidence": {"contract_id": contract_id, "fact_sum": fact, "total_sum": total,
                     "execution_rate_pct": round(rate * 100, 1)},
    }


# ─── 31. BANK_DETAILS_REUSE ──────────────────────────────────────────────────

async def check_bank_details_reuse(db: AsyncSession, supplier_biin: str) -> dict:
    """Same bank details (IIK+BIK) used by different companies."""
    # Get this supplier's bank details
    bank_result = await db.execute(
        select(Contract.supplier_iik, Contract.supplier_bik)
        .where(Contract.supplier_biin == supplier_biin,
               Contract.supplier_iik.isnot(None), Contract.supplier_bik.isnot(None))
        .limit(1)
    )
    bank = bank_result.first()
    if not bank or not bank.supplier_iik or not bank.supplier_bik:
        return {"flag": False, "value": 0.0, "evidence": {}}

    # Find other suppliers using the same bank details
    other_result = await db.execute(
        select(func.count(func.distinct(Contract.supplier_biin)))
        .where(
            Contract.supplier_iik == bank.supplier_iik,
            Contract.supplier_bik == bank.supplier_bik,
            Contract.supplier_biin != supplier_biin,
        )
    )
    other_count = other_result.scalar() or 0
    flag = other_count > 0
    return {
        "flag": flag, "value": float(other_count),
        "evidence": {"supplier_biin": supplier_biin, "iik": bank.supplier_iik,
                     "bik": bank.supplier_bik, "other_suppliers_count": other_count},
    }
