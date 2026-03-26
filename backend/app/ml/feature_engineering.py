"""
Feature engineering: extract real numerical features from DB tables (Lot, Contract, TrdBuy, TrdApp, etc.)
These features are INDEPENDENT of the rule-based indicator flags.
"""
import logging
from typing import Optional
import numpy as np
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.procurement import (
    Lot, Contract, TrdBuy, TrdApp, TrdAppLot,
    ContractAct, ContractPayment, ContractSpecSum,
    Subject, GraphFeature, TenderFeature, WeakLabel,
)

logger = logging.getLogger(__name__)

NUMERICAL_FEATURES = [
    "log_amount",
    "log_contract_sum",
    "bid_count",
    "unique_bidder_count",
    "deadline_days",
    "discussion_days",
    "execution_days_planned",
    "execution_days_actual",
    "execution_overrun_ratio",
    "price_to_plan_ratio",
    "win_margin_pct",
    "min_bid_ratio",
    "payment_to_contract_ratio",
    "act_sum_to_contract_ratio",
    "fine_ratio",
    "max_overdue_days",
    "addendum_count",
    "addendum_sum_ratio",
    "supplier_age_days",
    "supplier_total_contracts",
    "supplier_win_rate",
    "customer_total_tenders",
    "customer_unique_winners",
    "customer_top_winner_share",
    "graph_cobid_partners",
    "graph_top_supplier_share",
    "graph_win_rotation_index",
    "lot_count_in_tender",
    "is_single_source",
    "is_construction",
]

BINARY_INDICATORS = [
    "LOT_SPLITTING", "SHORT_DEADLINE", "FEW_BIDS", "RECURRING_WINNER",
    "COMMON_REQUISITES", "NEW_COMPANY_BIG_CONTRACT", "CAROUSEL_PATTERN",
    "RNU_FLAG", "DUMPING_FLAG", "IDENTICAL_BID_PRICES", "TINY_WIN_MARGIN",
    "LATE_BID_SUBMISSION", "REPEAT_TENDER", "CANCELLED_TENDER", "PAUSED_TENDER",
    "NIGHT_OR_WEEKEND_PUBLISH", "SHORT_DISCUSSION_WINDOW", "LAST_MINUTE_CHANGES",
    "SUPPLIER_CONCENTRATION", "CUSTOMER_WINNER_CONCENTRATION", "HIGH_WIN_RATE_FEW_BIDS",
    "ADDENDUM_VALUE_INCREASE", "WIN_MIN_THEN_ADDENDUM", "WEIRD_EXECUTION_TIME",
    "HIGH_PREPAY", "PAYMENTS_EXCEED_CONTRACT", "PAYMENT_WITHOUT_ACT",
    "OVERDUE_EXECUTION", "FINES_PRESENT", "LOW_EXECUTION_RATE", "BANK_DETAILS_REUSE",
]

# ML models use ONLY numerical features (no binary indicators — avoids data leakage)
ML_FEATURES = NUMERICAL_FEATURES

# Full feature list kept for TenderFeature storage and backward compatibility
ALL_FEATURES = NUMERICAL_FEATURES + BINARY_INDICATORS


def _safe_log(val, default=0.0):
    if val is None or val <= 0:
        return default
    return float(np.log1p(float(val)))


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_ratio(numerator, denominator, default=0.0):
    n = _safe_float(numerator)
    d = _safe_float(denominator)
    if d == 0:
        return default
    return n / d


def _days_between(d1, d2, default=0.0):
    if d1 is None or d2 is None:
        return default
    delta = (d2 - d1).total_seconds() / 86400.0
    return max(0.0, delta)


async def extract_features_for_lot(lot_id: int, db: AsyncSession) -> dict:
    """Extract a rich numerical+binary feature vector for a single lot."""

    lot_r = await db.execute(select(Lot).where(Lot.id == lot_id))
    lot = lot_r.scalar_one_or_none()
    if not lot:
        return {}

    features = {}

    features["log_amount"] = _safe_log(lot.amount)
    features["is_single_source"] = 1.0 if (lot.singl_org_sign or 0) == 1 else 0.0
    features["is_construction"] = 1.0 if (lot.is_construction_work or 0) == 1 else 0.0

    trd = None
    if lot.trd_buy_id:
        trd_r = await db.execute(select(TrdBuy).where(TrdBuy.id == lot.trd_buy_id))
        trd = trd_r.scalar_one_or_none()

    if trd:
        features["lot_count_in_tender"] = _safe_float(trd.count_lots, 1.0)
        features["deadline_days"] = _days_between(trd.start_date, trd.end_date)
        features["discussion_days"] = _days_between(trd.discus_start_date, trd.discus_end_date)
    else:
        features["lot_count_in_tender"] = 1.0
        features["deadline_days"] = 0.0
        features["discussion_days"] = 0.0

    bid_r = await db.execute(
        select(func.count(TrdApp.id), func.count(func.distinct(TrdApp.supplier_biin)))
        .where(TrdApp.buy_id == lot.trd_buy_id)
    )
    bid_row = bid_r.first()
    features["bid_count"] = float(bid_row[0]) if bid_row else 0.0
    features["unique_bidder_count"] = float(bid_row[1]) if bid_row else 0.0

    contract_r = await db.execute(
        select(Contract).where(
            Contract.trd_buy_id == lot.trd_buy_id,
            Contract.is_deleted == False,
        ).limit(1)
    )
    contract = contract_r.scalar_one_or_none()

    if contract:
        features["log_contract_sum"] = _safe_log(contract.contract_sum_wnds)
        features["execution_days_planned"] = _days_between(contract.sign_date, contract.plan_exec_date)
        features["execution_days_actual"] = _days_between(contract.sign_date, contract.fakt_exec_date)
        planned = features["execution_days_planned"]
        actual = features["execution_days_actual"]
        features["execution_overrun_ratio"] = _safe_ratio(actual, planned) if planned > 0 else 0.0
        features["price_to_plan_ratio"] = _safe_ratio(contract.fakt_sum, contract.contract_sum_wnds)

        app_lot_r = await db.execute(
            select(TrdAppLot.price).where(TrdAppLot.lot_id == lot_id).order_by(TrdAppLot.price.asc())
        )
        prices = [_safe_float(r[0]) for r in app_lot_r.all() if r[0] is not None and float(r[0]) > 0]
        if len(prices) >= 2:
            features["win_margin_pct"] = (prices[1] - prices[0]) / prices[1] * 100.0
            features["min_bid_ratio"] = prices[0] / _safe_float(lot.amount, 1.0)
        else:
            features["win_margin_pct"] = 0.0
            features["min_bid_ratio"] = 0.0

        pay_r = await db.execute(
            select(func.sum(ContractPayment.pay_sum)).where(ContractPayment.contract_id == contract.id)
        )
        total_pay = _safe_float((pay_r.scalar()))
        features["payment_to_contract_ratio"] = _safe_ratio(total_pay, contract.contract_sum_wnds)

        act_r = await db.execute(
            select(
                func.sum(ContractAct.sum_act),
                func.sum(ContractAct.sum_fine),
                func.max(ContractAct.day_overdue),
            ).where(ContractAct.contract_id == contract.id)
        )
        act_row = act_r.first()
        features["act_sum_to_contract_ratio"] = _safe_ratio(act_row[0], contract.contract_sum_wnds) if act_row else 0.0
        features["fine_ratio"] = _safe_ratio(act_row[1], contract.contract_sum_wnds) if act_row else 0.0
        features["max_overdue_days"] = _safe_float(act_row[2]) if act_row else 0.0

        addendum_r = await db.execute(
            select(func.count(Contract.id), func.sum(Contract.contract_sum_wnds))
            .where(Contract.root_id == contract.id, Contract.id != contract.id)
        )
        add_row = addendum_r.first()
        features["addendum_count"] = _safe_float(add_row[0]) if add_row else 0.0
        features["addendum_sum_ratio"] = _safe_ratio(add_row[1], contract.contract_sum_wnds) if add_row else 0.0

        if contract.supplier_biin:
            subj_r = await db.execute(
                select(Subject.regdate).where(Subject.bin == contract.supplier_biin).limit(1)
            )
            subj_row = subj_r.first()
            if subj_row and subj_row[0] and contract.sign_date:
                features["supplier_age_days"] = _days_between(subj_row[0], contract.sign_date)
            else:
                features["supplier_age_days"] = 0.0

            sc_r = await db.execute(
                select(func.count(Contract.id)).where(
                    Contract.supplier_biin == contract.supplier_biin,
                    Contract.is_deleted == False,
                )
            )
            features["supplier_total_contracts"] = float(sc_r.scalar() or 0)

            sw_r = await db.execute(
                select(func.count(Contract.id)).where(
                    Contract.supplier_biin == contract.supplier_biin,
                    Contract.is_deleted == False,
                    Contract.ref_contract_status_id.in_([420, 430]),
                )
            )
            wins = float(sw_r.scalar() or 0)
            features["supplier_win_rate"] = _safe_ratio(wins, features["supplier_total_contracts"])
        else:
            features["supplier_age_days"] = 0.0
            features["supplier_total_contracts"] = 0.0
            features["supplier_win_rate"] = 0.0
    else:
        for k in ["log_contract_sum", "execution_days_planned", "execution_days_actual",
                   "execution_overrun_ratio", "price_to_plan_ratio", "win_margin_pct",
                   "min_bid_ratio", "payment_to_contract_ratio", "act_sum_to_contract_ratio",
                   "fine_ratio", "max_overdue_days", "addendum_count", "addendum_sum_ratio",
                   "supplier_age_days", "supplier_total_contracts", "supplier_win_rate"]:
            features[k] = 0.0

    if lot.customer_bin:
        ct_r = await db.execute(
            select(func.count(TrdBuy.id)).where(TrdBuy.org_bin == lot.customer_bin)
        )
        features["customer_total_tenders"] = float(ct_r.scalar() or 0)

        cw_r = await db.execute(
            select(func.count(func.distinct(Contract.supplier_biin))).where(
                Contract.customer_bin == lot.customer_bin,
                Contract.is_deleted == False,
            )
        )
        features["customer_unique_winners"] = float(cw_r.scalar() or 0)

        top_r = await db.execute(
            text("""
                SELECT supplier_biin, count(*) as cnt
                FROM contract
                WHERE customer_bin = :cbin AND is_deleted = false
                GROUP BY supplier_biin
                ORDER BY cnt DESC
                LIMIT 1
            """),
            {"cbin": lot.customer_bin},
        )
        top_row = top_r.first()
        if top_row and features["customer_unique_winners"] > 0:
            total_cust_contracts_r = await db.execute(
                select(func.count(Contract.id)).where(
                    Contract.customer_bin == lot.customer_bin,
                    Contract.is_deleted == False,
                )
            )
            total_cc = float(total_cust_contracts_r.scalar() or 1)
            features["customer_top_winner_share"] = float(top_row[1]) / total_cc * 100.0
        else:
            features["customer_top_winner_share"] = 0.0
    else:
        features["customer_total_tenders"] = 0.0
        features["customer_unique_winners"] = 0.0
        features["customer_top_winner_share"] = 0.0

    gf_r = await db.execute(
        select(GraphFeature.features_jsonb).where(
            GraphFeature.entity_type == "lot",
            GraphFeature.entity_id == str(lot_id),
        ).limit(1)
    )
    gf_row = gf_r.first()
    gf = gf_row[0] if gf_row else {}
    features["graph_cobid_partners"] = _safe_float(gf.get("supplier_frequent_cobid_partners_count"))
    features["graph_top_supplier_share"] = _safe_float(gf.get("customer_top_supplier_win_share"))
    features["graph_win_rotation_index"] = _safe_float(gf.get("customer_win_rotation_index"))

    tf_r = await db.execute(
        select(TenderFeature.features_jsonb).where(
            TenderFeature.entity_type == "lot",
            TenderFeature.entity_id == str(lot_id),
        ).limit(1)
    )
    tf_row = tf_r.first()
    indicator_flags = tf_row[0] if tf_row else {}
    for ind in BINARY_INDICATORS:
        v = indicator_flags.get(ind)
        if v is None:
            features[ind] = 0.0
        elif isinstance(v, bool):
            features[ind] = 1.0 if v else 0.0
        else:
            features[ind] = _safe_float(v)

    return features


def features_dict_to_array(features: dict, columns: list = None) -> np.ndarray:
    """Convert feature dict to numpy array aligned with given column list.
    Defaults to ALL_FEATURES for backward compatibility.
    Use columns=ML_FEATURES for ML model input (30 numerical only).
    """
    cols = columns or ALL_FEATURES
    vec = []
    for col in cols:
        v = features.get(col, 0.0)
        if isinstance(v, bool):
            vec.append(1.0 if v else 0.0)
        else:
            try:
                vec.append(float(v))
            except (ValueError, TypeError):
                vec.append(0.0)
    return np.array(vec, dtype=np.float64)


async def build_dataset(limit: int = None, ml_only: bool = False) -> tuple:
    """
    Build dataset from DB.
    If ml_only=True: returns (entity_ids, X_ml) with 30 numerical features only.
    If ml_only=False: returns (entity_ids, X_all, y_weak) with 61 features (backward compat).
    """
    columns = ML_FEATURES if ml_only else ALL_FEATURES

    async with AsyncSessionLocal() as db:
        q = select(Lot.id).where(Lot.is_deleted == False)
        if limit:
            q = q.limit(limit)
        lot_r = await db.execute(q)
        lot_ids = [r[0] for r in lot_r.all()]

        weak_map = {}
        if not ml_only:
            wl_r = await db.execute(
                select(WeakLabel.entity_id, WeakLabel.weak_proba).where(WeakLabel.entity_type == "lot")
            )
            weak_map = {r.entity_id: float(r.weak_proba) for r in wl_r.all()}

    logger.info("Building dataset for %d lots (ml_only=%s)...", len(lot_ids), ml_only)

    entity_ids = []
    X_list = []
    y_list = []

    async with AsyncSessionLocal() as db:
        for i, lot_id in enumerate(lot_ids):
            try:
                feats = await extract_features_for_lot(lot_id, db)
                if not feats:
                    continue
                arr = features_dict_to_array(feats, columns=columns)
                entity_ids.append(str(lot_id))
                X_list.append(arr)
                if not ml_only:
                    wp = weak_map.get(str(lot_id), np.nan)
                    y_list.append(wp)
            except Exception as e:
                logger.debug("Skip lot %s: %s", lot_id, e)
            if (i + 1) % 500 == 0:
                logger.info("  ... extracted %d / %d lots", i + 1, len(lot_ids))

    X = np.array(X_list, dtype=np.float64) if X_list else np.empty((0, len(columns)))
    logger.info("Dataset built: %d samples, %d features", X.shape[0], X.shape[1] if len(X.shape) > 1 else 0)

    if ml_only:
        return entity_ids, X

    y = np.array(y_list, dtype=np.float64)
    return entity_ids, X, y
