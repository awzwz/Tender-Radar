"""
Weak supervision: labeling functions (LF) + label model (weighted majority / EM-like).
Produces weak_proba per entity; stored in weak_labels table.
"""
import logging
from datetime import datetime
from typing import Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.procurement import WeakLabel, TenderFeature

logger = logging.getLogger(__name__)

# LF return: 1 = suspicious, -1 = not suspicious, 0 = abstain
ABSTAIN = 0
SUSPICIOUS = 1
NOT_SUSPICIOUS = -1


def lf_rnu_flag(features: dict) -> int:
    if features.get("RNU_FLAG") == 1 or features.get("RNU_FLAG") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_carousel(features: dict) -> int:
    if features.get("CAROUSEL_PATTERN") == 1 or features.get("CAROUSEL_PATTERN") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_recurring_few_bids(features: dict) -> int:
    if (features.get("RECURRING_WINNER") == 1 or features.get("RECURRING_WINNER") is True) and (
        features.get("FEW_BIDS") == 1 or features.get("FEW_BIDS") is True
    ):
        return SUSPICIOUS
    return ABSTAIN


def lf_short_deadline_last_minute(features: dict) -> int:
    if (features.get("SHORT_DEADLINE") == 1 or features.get("SHORT_DEADLINE") is True) and (
        features.get("LAST_MINUTE_CHANGES") == 1 or features.get("LAST_MINUTE_CHANGES") is True
    ):
        return SUSPICIOUS
    return ABSTAIN


def lf_win_min_then_addendum(features: dict) -> int:
    if features.get("WIN_MIN_THEN_ADDENDUM") == 1 or features.get("WIN_MIN_THEN_ADDENDUM") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_payment_without_act(features: dict) -> int:
    if features.get("PAYMENT_WITHOUT_ACT") == 1 or features.get("PAYMENT_WITHOUT_ACT") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_few_bids(features: dict) -> int:
    if features.get("FEW_BIDS") == 1 or features.get("FEW_BIDS") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_common_requisites(features: dict) -> int:
    if features.get("COMMON_REQUISITES") == 1 or features.get("COMMON_REQUISITES") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_new_company_big_contract(features: dict) -> int:
    if features.get("NEW_COMPANY_BIG_CONTRACT") == 1 or features.get("NEW_COMPANY_BIG_CONTRACT") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_addendum_value_increase(features: dict) -> int:
    if features.get("ADDENDUM_VALUE_INCREASE") == 1 or features.get("ADDENDUM_VALUE_INCREASE") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_payments_exceed_contract(features: dict) -> int:
    if features.get("PAYMENTS_EXCEED_CONTRACT") == 1 or features.get("PAYMENTS_EXCEED_CONTRACT") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_fines_present(features: dict) -> int:
    if features.get("FINES_PRESENT") == 1 or features.get("FINES_PRESENT") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_overdue_execution(features: dict) -> int:
    if features.get("OVERDUE_EXECUTION") == 1 or features.get("OVERDUE_EXECUTION") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_bank_details_reuse(features: dict) -> int:
    if features.get("BANK_DETAILS_REUSE") == 1 or features.get("BANK_DETAILS_REUSE") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_identical_bid_prices(features: dict) -> int:
    if features.get("IDENTICAL_BID_PRICES") == 1 or features.get("IDENTICAL_BID_PRICES") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_tiny_win_margin(features: dict) -> int:
    if features.get("TINY_WIN_MARGIN") == 1 or features.get("TINY_WIN_MARGIN") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_lot_splitting(features: dict) -> int:
    if features.get("LOT_SPLITTING") == 1 or features.get("LOT_SPLITTING") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_high_win_rate_few_bids(features: dict) -> int:
    if features.get("HIGH_WIN_RATE_FEW_BIDS") == 1 or features.get("HIGH_WIN_RATE_FEW_BIDS") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_dumping(features: dict) -> int:
    if features.get("DUMPING_FLAG") == 1 or features.get("DUMPING_FLAG") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_supplier_concentration(features: dict) -> int:
    if features.get("SUPPLIER_CONCENTRATION") == 1 or features.get("SUPPLIER_CONCENTRATION") is True:
        return SUSPICIOUS
    return ABSTAIN


def lf_customer_winner_concentration(features: dict) -> int:
    if features.get("CUSTOMER_WINNER_CONCENTRATION") == 1 or features.get("CUSTOMER_WINNER_CONCENTRATION") is True:
        return SUSPICIOUS
    return ABSTAIN


# Less suspicious / abstain when many indicators are 0
def lf_many_zeros(features: dict) -> int:
    ones = sum(1 for k, v in features.items() if v == 1 or v is True)
    if ones <= 1:
        return NOT_SUSPICIOUS
    return ABSTAIN


ALL_LFS = [
    ("lf_rnu", lf_rnu_flag),
    ("lf_carousel", lf_carousel),
    ("lf_recurring_few_bids", lf_recurring_few_bids),
    ("lf_short_deadline_last_minute", lf_short_deadline_last_minute),
    ("lf_win_min_then_addendum", lf_win_min_then_addendum),
    ("lf_payment_without_act", lf_payment_without_act),
    ("lf_few_bids", lf_few_bids),
    ("lf_common_requisites", lf_common_requisites),
    ("lf_new_company_big_contract", lf_new_company_big_contract),
    ("lf_addendum_value_increase", lf_addendum_value_increase),
    ("lf_payments_exceed_contract", lf_payments_exceed_contract),
    ("lf_fines_present", lf_fines_present),
    ("lf_overdue_execution", lf_overdue_execution),
    ("lf_bank_details_reuse", lf_bank_details_reuse),
    ("lf_identical_bid_prices", lf_identical_bid_prices),
    ("lf_tiny_win_margin", lf_tiny_win_margin),
    ("lf_lot_splitting", lf_lot_splitting),
    ("lf_high_win_rate_few_bids", lf_high_win_rate_few_bids),
    ("lf_dumping", lf_dumping),
    ("lf_supplier_concentration", lf_supplier_concentration),
    ("lf_customer_winner_concentration", lf_customer_winner_concentration),
    ("lf_many_zeros", lf_many_zeros),
]


def _weighted_majority_vote(L: np.ndarray) -> np.ndarray:
    """
    L: (n_samples, n_lfs), values in {-1, 0, 1}.
    Return weak_proba in [0, 1] per sample (simple weighted average of positive votes).
    """
    n = L.shape[0]
    probs = np.zeros(n)
    for i in range(n):
        row = L[i]
        pos = np.sum(row == SUSPICIOUS)
        neg = np.sum(row == NOT_SUSPICIOUS)
        abstain = np.sum(row == ABSTAIN)
        total_votes = pos + neg
        if total_votes == 0:
            probs[i] = 0.5
        else:
            probs[i] = pos / total_votes
    return probs


async def run_weak_labeling(version: str = None) -> dict:
    """
    Load tender_features, run all LFs, aggregate with weighted majority, upsert weak_labels.
    """
    version = version or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(TenderFeature.entity_id, TenderFeature.features_jsonb).where(
                TenderFeature.entity_type == "lot"
            )
        )
        rows = result.all()

    if not rows:
        return {"version": version, "labeled": 0}

    entity_ids = []
    L_list = []
    lf_names = [name for name, _ in ALL_LFS]

    for row in rows:
        entity_ids.append(row.entity_id)
        features = row.features_jsonb or {}
        votes = []
        for name, lf in ALL_LFS:
            v = lf(features)
            votes.append(v)
        L_list.append(votes)

    L = np.array(L_list, dtype=np.int32)
    weak_proba = _weighted_majority_vote(L)
    lf_votes_per_entity = [
        {lf_names[j]: int(L[i, j]) for j in range(L.shape[1])}
        for i in range(L.shape[0])
    ]

    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        for i, eid in enumerate(entity_ids):
            stmt = pg_insert(WeakLabel).values({
                "entity_type": "lot",
                "entity_id": eid,
                "weak_proba": float(weak_proba[i]),
                "version": version,
                "lf_votes_jsonb": lf_votes_per_entity[i],
                "created_at": now,
            })
            stmt = stmt.on_conflict_do_update(
                constraint="uq_weak_labels_entity",
                set_={
                    "weak_proba": stmt.excluded.weak_proba,
                    "version": stmt.excluded.version,
                    "lf_votes_jsonb": stmt.excluded.lf_votes_jsonb,
                    "created_at": stmt.excluded.created_at,
                },
            )
            await db.execute(stmt)
        await db.commit()

    return {"version": version, "labeled": len(entity_ids)}


async def get_weak_proba_for_entity(entity_id: str, version: str = None) -> float | None:
    """Return latest weak_proba for entity (optional version filter)."""
    from sqlalchemy import and_
    async with AsyncSessionLocal() as db:
        q = select(WeakLabel.weak_proba).where(
            and_(
                WeakLabel.entity_type == "lot",
                WeakLabel.entity_id == entity_id,
            )
        ).order_by(WeakLabel.created_at.desc()).limit(1)
        if version:
            q = q.where(WeakLabel.version == version)
        r = await db.execute(q)
        row = r.first()
    return float(row[0]) if row else None
