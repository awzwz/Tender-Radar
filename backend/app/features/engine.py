"""
Feature Engine: orchestrates all 16 indicators and computes RiskScore.
"""
import logging
import yaml
import os
from datetime import datetime
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.procurement import Lot, Contract, TrdBuy, RiskFlag, RiskScore
from app.features import indicators as ind

logger = logging.getLogger(__name__)

# Load weights config
_WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), "weights.yaml")
with open(_WEIGHTS_PATH) as f:
    _CONFIG = yaml.safe_load(f)

WEIGHTS = {k: v["weight"] for k, v in _CONFIG["indicators"].items()}
MAX_SCORE = sum(WEIGHTS.values())
THRESHOLDS = _CONFIG["thresholds"]


def _normalize_score(raw: float) -> float:
    """Normalize raw weighted sum to 0-100."""
    return round(min(100.0, (raw / MAX_SCORE) * 100), 1)


def _get_level(score: float) -> str:
    if score <= THRESHOLDS["low_max"]:
        return "LOW"
    elif score <= THRESHOLDS["medium_max"]:
        return "MEDIUM"
    return "HIGH"


class FeatureEngine:
    """
    Computes risk flags and scores for lots, tenders, suppliers, and customers.
    """

    async def run(self, entity_ids: list = None) -> dict:
        """Recompute features for all lots (or a specific list)."""
        summary = {"lots_processed": 0, "errors": 0}

        async with AsyncSessionLocal() as db:
            # Get all lot IDs to process
            if entity_ids:
                lot_ids = entity_ids
            else:
                result = await db.execute(
                    select(Lot.id).where(Lot.is_deleted == False).limit(10000)
                )
                lot_ids = [r[0] for r in result.all()]

        for lot_id in lot_ids:
            try:
                await self.compute_lot_score(lot_id)
                summary["lots_processed"] += 1
            except Exception as e:
                logger.error(f"Error computing score for lot {lot_id}: {e}")
                summary["errors"] += 1

        return summary

    async def compute_lot_score(self, lot_id: int) -> dict:
        """Compute full risk score for a single lot."""
        async with AsyncSessionLocal() as db:
            # Get lot info
            lot_result = await db.execute(
                select(Lot).where(Lot.id == lot_id)
            )
            lot = lot_result.scalar_one_or_none()
            if not lot:
                return {}

            # Get associated contract
            contract_result = await db.execute(
                select(Contract).where(
                    Contract.trd_buy_id == lot.trd_buy_id,
                    Contract.is_deleted == False,
                ).limit(1)
            )
            contract = contract_result.scalar_one_or_none()

            flags = {}

            # ── Lot-level indicators ──────────────────────────────────────────
            flags["DUMPING_FLAG"] = await ind.check_dumping_flag(db, lot_id)

            # ── Tender-level indicators ───────────────────────────────────────
            if lot.trd_buy_id:
                flags["SHORT_DEADLINE"] = await ind.check_short_deadline(db, lot.trd_buy_id)
                flags["FEW_BIDS"] = await ind.check_few_bids(db, lot.trd_buy_id)
                flags["LOT_SPLITTING"] = await ind.check_lot_splitting(db, lot.trd_buy_id)
                flags["LAST_MINUTE_CHANGES"] = await ind.check_last_minute_changes(db, lot.trd_buy_id)
                flags["COMMON_REQUISITES"] = await ind.check_common_requisites(db, lot.trd_buy_id)

            # ── Customer-level indicators ─────────────────────────────────────
            if lot.customer_bin and contract and contract.supplier_biin:
                flags["RECURRING_WINNER"] = await ind.check_recurring_winner(
                    db, lot.customer_bin, contract.supplier_biin
                )
                flags["CAROUSEL_PATTERN"] = await ind.check_carousel_pattern(db, lot.customer_bin)

            # ── Supplier-level indicators ─────────────────────────────────────
            if contract and contract.supplier_biin:
                flags["SUPPLIER_CONCENTRATION"] = await ind.check_supplier_concentration(
                    db, contract.supplier_biin
                )
                flags["RNU_FLAG"] = await ind.check_rnu_flag(db, contract.supplier_biin)
                flags["HIGH_WIN_RATE_FEW_BIDS"] = await ind.check_high_win_rate_few_bids(
                    db, contract.supplier_biin
                )
                flags["NEW_COMPANY_BIG_CONTRACT"] = await ind.check_new_company_big_contract(
                    db, contract.supplier_biin, float(contract.contract_sum_wnds or 0)
                )

            # ── Contract-level indicators ─────────────────────────────────────
            if contract:
                flags["ADDENDUM_VALUE_INCREASE"] = await ind.check_addendum_value_increase(
                    db, contract.id
                )
                flags["WIN_MIN_THEN_ADDENDUM"] = await ind.check_win_min_then_addendum(
                    db, contract.id
                )
                flags["WEIRD_EXECUTION_TIME"] = await ind.check_weird_execution_time(
                    db, contract.id
                )
                flags["PAYMENT_WITHOUT_ACT"] = await ind.check_payment_without_act(
                    db, contract.id
                )

            # ── Compute score ─────────────────────────────────────────────────
            raw_score = sum(
                WEIGHTS.get(code, 0) * (1 if result.get("flag") else 0)
                for code, result in flags.items()
            )
            score = _normalize_score(raw_score)
            level = _get_level(score)

            # Top 3 reasons (highest weight flags that are True)
            top_reasons = sorted(
                [
                    {
                        "code": code,
                        "weight": WEIGHTS.get(code, 0),
                        "evidence": result.get("evidence", {}),
                        "description": _CONFIG["indicators"].get(code, {}).get("description", ""),
                    }
                    for code, result in flags.items()
                    if result.get("flag")
                ],
                key=lambda x: x["weight"],
                reverse=True,
            )[:3]

            # ── Persist flags ─────────────────────────────────────────────────
            now = datetime.utcnow()
            flag_rows = [
                {
                    "entity_type": "lot",
                    "entity_id": str(lot_id),
                    "indicator_code": code,
                    "flag_bool": result.get("flag", False),
                    "value_numeric": result.get("value"),
                    "evidence_jsonb": result.get("evidence", {}),
                    "computed_at": now,
                }
                for code, result in flags.items()
            ]
            if flag_rows:
                stmt = pg_insert(RiskFlag).values(flag_rows)
                stmt = stmt.on_conflict_do_nothing()
                await db.execute(stmt)

            # ── Persist score ─────────────────────────────────────────────────
            score_stmt = pg_insert(RiskScore).values([{
                "entity_type": "lot",
                "entity_id": str(lot_id),
                "score": score,
                "level": level,
                "top_reasons_jsonb": top_reasons,
                "computed_at": now,
            }])
            score_stmt = score_stmt.on_conflict_do_update(
                index_elements=["entity_type", "entity_id"],
                set_={"score": score, "level": level, "top_reasons_jsonb": top_reasons, "computed_at": now},
            )
            await db.execute(score_stmt)
            await db.commit()

            return {"lot_id": lot_id, "score": score, "level": level, "flags_triggered": len(top_reasons)}
