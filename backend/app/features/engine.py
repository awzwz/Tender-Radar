"""
Feature Engine: orchestrates 31 indicators, computes risk scores (rules + ML + final).
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
from app.models.procurement import (
    Lot, Contract, TrdBuy, RiskFlag, RiskScore, TenderFeature,
)
from app.features import indicators as ind

logger = logging.getLogger(__name__)

# Load weights config
_WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), "weights.yaml")
with open(_WEIGHTS_PATH) as f:
    _CONFIG = yaml.safe_load(f)

WEIGHTS = {k: v["weight"] for k, v in _CONFIG["indicators"].items()}
MAX_SCORE = sum(WEIGHTS.values())
THRESHOLDS = _CONFIG["thresholds"]

# All indicator codes in evaluation order
ALL_INDICATORS = list(WEIGHTS.keys())


def _normalize_score(raw: float) -> float:
    """Normalize raw weighted sum to 0-100."""
    return round(min(100.0, (raw / MAX_SCORE) * 100), 1)


def _get_level(score: float) -> str:
    if score <= THRESHOLDS["low_max"]:
        return "LOW"
    elif score <= THRESHOLDS["medium_max"]:
        return "MEDIUM"
    return "HIGH"


def _get_anomaly_predictor():
    try:
        from app.ml.anomaly import get_anomaly_predictor
        return get_anomaly_predictor()
    except Exception:
        return None


def _get_weak_predictor():
    try:
        from app.ml.weak_model import get_weak_predictor
        return get_weak_predictor()
    except Exception:
        return None


class FeatureEngine:
    """
    Computes risk flags, rule-based score, anomaly/weak/composite, and final score.
    """

    def __init__(self):
        self._anomaly_packed = _get_anomaly_predictor()
        self._weak_packed = _get_weak_predictor()
        if self._anomaly_packed:
            logger.info("Anomaly model loaded")
        if self._weak_packed:
            logger.info("Weak model loaded")

    async def run(self, entity_ids: list = None) -> dict:
        """Recompute features for all lots (or a specific list)."""
        summary = {"lots_processed": 0, "flags_triggered": 0, "errors": 0}

        async with AsyncSessionLocal() as db:
            if entity_ids:
                lot_ids = entity_ids
            else:
                result = await db.execute(
                    select(Lot.id).where(Lot.is_deleted == False).limit(50000)
                )
                lot_ids = [r[0] for r in result.all()]

        logger.info(f"Feature recompute starting for {len(lot_ids)} lots")

        for lot_id in lot_ids:
            try:
                result = await self.compute_lot_score(lot_id)
                summary["lots_processed"] += 1
                summary["flags_triggered"] += result.get("flags_triggered", 0)
                if summary["lots_processed"] % 100 == 0:
                    logger.info(f"Processed {summary['lots_processed']}/{len(lot_ids)} lots...")
            except Exception as e:
                logger.error(f"Error computing score for lot {lot_id}: {e}")
                summary["errors"] += 1

        logger.info(
            f"Feature recompute DONE: {summary['lots_processed']} lots scored, "
            f"{summary['flags_triggered']} flags triggered, {summary['errors']} errors"
        )
        return summary

    async def compute_lot_score(self, lot_id: int) -> dict:
        """Compute full risk score for a single lot."""
        async with AsyncSessionLocal() as db:
            # Get lot info
            lot_result = await db.execute(select(Lot).where(Lot.id == lot_id))
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

            customer_bin = lot.customer_bin
            supplier_biin = contract.supplier_biin if contract else None

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
                flags["IDENTICAL_BID_PRICES"] = await ind.check_identical_bid_prices(db, lot.trd_buy_id)
                flags["TINY_WIN_MARGIN"] = await ind.check_tiny_win_margin(db, lot.trd_buy_id)
                flags["LATE_BID_SUBMISSION"] = await ind.check_late_bid_submission(db, lot.trd_buy_id)
                flags["REPEAT_TENDER"] = await ind.check_repeat_tender(db, lot.trd_buy_id)
                flags["CANCELLED_TENDER"] = await ind.check_cancelled_tender(db, lot.trd_buy_id)
                flags["PAUSED_TENDER"] = await ind.check_paused_tender(db, lot.trd_buy_id)
                flags["NIGHT_OR_WEEKEND_PUBLISH"] = await ind.check_night_or_weekend_publish(db, lot.trd_buy_id)
                flags["SHORT_DISCUSSION_WINDOW"] = await ind.check_short_discussion_window(db, lot.trd_buy_id)

            # ── Customer-level indicators ─────────────────────────────────────
            if customer_bin:
                flags["CUSTOMER_WINNER_CONCENTRATION"] = await ind.check_customer_winner_concentration(
                    db, customer_bin
                )
            if customer_bin and supplier_biin:
                flags["RECURRING_WINNER"] = await ind.check_recurring_winner(db, customer_bin, supplier_biin)
                flags["CAROUSEL_PATTERN"] = await ind.check_carousel_pattern(db, customer_bin)

            # ── Supplier-level indicators ─────────────────────────────────────
            if supplier_biin:
                flags["SUPPLIER_CONCENTRATION"] = await ind.check_supplier_concentration(db, supplier_biin)
                flags["RNU_FLAG"] = await ind.check_rnu_flag(db, supplier_biin)
                flags["HIGH_WIN_RATE_FEW_BIDS"] = await ind.check_high_win_rate_few_bids(db, supplier_biin)
                flags["NEW_COMPANY_BIG_CONTRACT"] = await ind.check_new_company_big_contract(
                    db, supplier_biin, float(contract.contract_sum_wnds or 0)
                )
                flags["BANK_DETAILS_REUSE"] = await ind.check_bank_details_reuse(db, supplier_biin)

            # ── Contract-level indicators ─────────────────────────────────────
            if contract:
                flags["ADDENDUM_VALUE_INCREASE"] = await ind.check_addendum_value_increase(db, contract.id)
                flags["WIN_MIN_THEN_ADDENDUM"] = await ind.check_win_min_then_addendum(db, contract.id)
                flags["WEIRD_EXECUTION_TIME"] = await ind.check_weird_execution_time(db, contract.id)
                flags["PAYMENT_WITHOUT_ACT"] = await ind.check_payment_without_act(db, contract.id)
                flags["HIGH_PREPAY"] = await ind.check_high_prepay(db, contract.id)
                flags["PAYMENTS_EXCEED_CONTRACT"] = await ind.check_payments_exceed_contract(db, contract.id)
                flags["OVERDUE_EXECUTION"] = await ind.check_overdue_execution(db, contract.id)
                flags["FINES_PRESENT"] = await ind.check_fines_present(db, contract.id)
                flags["LOW_EXECUTION_RATE"] = await ind.check_low_execution_rate(db, contract.id)

            # ── Compute rule-based score ──────────────────────────────────────
            raw_score = sum(
                WEIGHTS.get(code, 0) * (1 if result.get("flag") else 0)
                for code, result in flags.items()
            )
            score_rules = _normalize_score(raw_score)
            level = _get_level(score_rules)

            # ── Build feature vector for ML ───────────────────────────────────
            feature_vector = {}
            for code in ALL_INDICATORS:
                r = flags.get(code, {})
                feature_vector[code] = r.get("value") if r.get("value") is not None else (
                    1.0 if r.get("flag") else 0.0
                )

            # ── Anomaly scoring (additive signal; does not replace score) ──────
            anomaly_score = None
            anomaly_model_version = None
            anomaly_explanation_jsonb = None
            if self._anomaly_packed:
                try:
                    from app.ml.anomaly import predict_anomaly
                    out = predict_anomaly(feature_vector)
                    if out:
                        anomaly_score, anomaly_model_version, anomaly_explanation_jsonb = out
                except Exception as e:
                    logger.debug("Anomaly prediction skipped: %s", e)

            # ── Weak model scoring (additive signal) ───────────────────────────
            weak_proba = None
            weak_score = None
            weak_model_version = None
            weak_explanation_jsonb = None
            if self._weak_packed:
                try:
                    from app.ml.weak_model import predict_weak
                    out = predict_weak(feature_vector)
                    if out:
                        weak_proba, weak_score, weak_model_version, weak_explanation_jsonb = out
                except Exception as e:
                    logger.debug("Weak model prediction skipped: %s", e)

            # ── Graph features (for composite; optional) ───────────────────────
            graph_score = None
            try:
                from app.features.graph_features import get_graph_features_for_lot
                gf = await get_graph_features_for_lot(lot_id)
                if gf is not None:
                    # Derive single 0..100 score: high top_supplier_win_share = risk, low rotation = risk
                    top_share = float(gf.get("customer_top_supplier_win_share") or 0)
                    rotation = int(gf.get("customer_win_rotation_index") or 0)
                    if rotation > 0:
                        graph_score = min(100.0, top_share)  # 0..100
                    else:
                        graph_score = 50.0
            except Exception as e:
                logger.debug("Graph features skipped: %s", e)

            # ── ML scoring (REMOVED) ─────────────────────────────────────────
            score_ml = None

            # ── Composite score (only when mode=composite) ─────────────────────
            from app.core.config import settings
            mode = getattr(settings, "risk_scoring_mode", "hybrid") or "hybrid"
            delta_max = getattr(settings, "composite_delta_max", 15.0) or 15.0
            composite_score = None
            components_jsonb = None
            if mode == "composite":
                # f: map each component 0..100 to contribution in [-1, 1]; missing = 0 (neutral 50)
                def _to_contrib(s):
                    if s is None:
                        return 0.0
                    return (s / 50.0) - 1.0  # 50 -> 0, 100 -> 1, 0 -> -1
                a_contrib = _to_contrib(anomaly_score)
                w_contrib = _to_contrib(weak_score)
                g_contrib = _to_contrib(graph_score)
                n = sum(1 for x in (anomaly_score, weak_score, graph_score) if x is not None)
                if n > 0:
                    avg_contrib = (a_contrib + w_contrib + g_contrib) / 3.0  # treat missing as 0
                    avg_contrib = max(-1.0, min(1.0, avg_contrib))
                    composite_score = round(score_rules + delta_max * avg_contrib, 1)
                    composite_score = max(0.0, min(100.0, composite_score))
                else:
                    composite_score = round(score_rules, 1)
                components_jsonb = {
                    "anomaly_score": anomaly_score,
                    "weak_score": weak_score,
                    "graph_score": graph_score,
                }

            # ── Final score (by mode) ─────────────────────────────────────────
            if mode == "rules":
                score_final = round(score_rules)
            elif mode == "composite" and composite_score is not None:
                score_final = round(composite_score)
            elif score_ml is not None and mode in ("ml", "hybrid"):
                score_final = round(
                    100 * (settings.ml_rules_weight * score_rules / 100 + settings.ml_ml_weight * score_ml)
                )
            else:
                score_final = round(score_rules)

            # Guardrail for hybrid mode:
            # do not escalate to HIGH purely by ML when rule evidence is weak.
            if mode == "hybrid" and score_ml is not None and score_rules < 10 and score_final > THRESHOLDS["medium_max"]:
                score_final = THRESHOLDS["medium_max"]

            score_final = max(0, min(100, score_final))
            level = _get_level(score_final)

            # Top reasons (highest weight flags that are True)
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
            )[:5]

            # Append ML explanations so the UI receives them naturally
            if anomaly_score and anomaly_score > 50:
                a_ev = {}
                if anomaly_explanation_jsonb and "top_abnormal_features" in anomaly_explanation_jsonb:
                    for i, tf in enumerate(anomaly_explanation_jsonb["top_abnormal_features"][:3]):
                        val = round(tf.get("value", 0), 2)
                        a_ev[f"Аномалия: {tf.get('feature')}"] = str(val)

                top_reasons.insert(0, {
                    "code": "ML_ANOMALY",
                    "weight": 100,
                    "description": f"ML Структурная аномалия (уверенность: {round(anomaly_score, 1)}%)",
                    "evidence": a_ev
                })
            
            if weak_score and weak_score > 50:
                w_ev = {}
                if weak_explanation_jsonb and "top_features" in weak_explanation_jsonb:
                    for i, tf in enumerate(weak_explanation_jsonb["top_features"][:3]):
                        imp = round(tf.get("importance", 0) * 100)
                        w_ev[f"Влияние: {tf.get('feature')}"] = f"{imp}%"

                top_reasons.insert(0, {
                    "code": "ML_WEAK_SIGNAL",
                    "weight": 100,
                    "description": f"ML Графовые паттерны риска (уверенность: {round(weak_score, 1)}%)",
                    "evidence": w_ev
                })

            # ── Persist flags (upsert) ────────────────────────────────────────
            now = datetime.utcnow()
            for code, result in flags.items():
                flag_stmt = pg_insert(RiskFlag).values({
                    "entity_type": "lot",
                    "entity_id": str(lot_id),
                    "indicator_code": code,
                    "flag_bool": result.get("flag", False),
                    "value_numeric": result.get("value"),
                    "evidence_jsonb": result.get("evidence", {}),
                    "computed_at": now,
                })
                flag_stmt = flag_stmt.on_conflict_do_update(
                    constraint="uq_risk_flags_entity_indicator",
                    set_={
                        "flag_bool": flag_stmt.excluded.flag_bool,
                        "value_numeric": flag_stmt.excluded.value_numeric,
                        "evidence_jsonb": flag_stmt.excluded.evidence_jsonb,
                        "computed_at": flag_stmt.excluded.computed_at,
                    },
                )
                await db.execute(flag_stmt)

            # ── Persist score (upsert) ────────────────────────────────────────
            score_stmt = pg_insert(RiskScore).values({
                "entity_type": "lot",
                "entity_id": str(lot_id),
                "score": score_final,
                "score_rules": score_rules,
                "score_ml": score_ml,
                "score_final": score_final,
                "level": level,
                "top_reasons_jsonb": top_reasons,
                "computed_at": now,
                "anomaly_score": anomaly_score,
                "anomaly_model_version": anomaly_model_version,
                "anomaly_explanation_jsonb": anomaly_explanation_jsonb,
                "weak_proba": weak_proba,
                "weak_score": weak_score,
                "weak_model_version": weak_model_version,
                "weak_explanation_jsonb": weak_explanation_jsonb,
                "composite_score": composite_score,
                "components_jsonb": components_jsonb,
            })
            score_stmt = score_stmt.on_conflict_do_update(
                constraint="uq_risk_scores_entity",
                set_={
                    "score": score_final,
                    "score_rules": score_rules,
                    "score_ml": score_ml,
                    "score_final": score_final,
                    "level": level,
                    "top_reasons_jsonb": top_reasons,
                    "computed_at": now,
                    "anomaly_score": anomaly_score,
                    "anomaly_model_version": anomaly_model_version,
                    "anomaly_explanation_jsonb": anomaly_explanation_jsonb,
                    "weak_proba": weak_proba,
                    "weak_score": weak_score,
                    "weak_model_version": weak_model_version,
                    "weak_explanation_jsonb": weak_explanation_jsonb,
                    "composite_score": composite_score,
                    "components_jsonb": components_jsonb,
                },
            )
            await db.execute(score_stmt)

            # ── Persist feature vector (for ML training) ──────────────────────
            feat_stmt = pg_insert(TenderFeature).values({
                "entity_type": "lot",
                "entity_id": str(lot_id),
                "features_jsonb": feature_vector,
                "computed_at": now,
            })
            feat_stmt = feat_stmt.on_conflict_do_update(
                constraint="uq_tender_features_entity",
                set_={
                    "features_jsonb": feat_stmt.excluded.features_jsonb,
                    "computed_at": feat_stmt.excluded.computed_at,
                },
            )
            await db.execute(feat_stmt)

            await db.commit()

            return {
                "lot_id": lot_id,
                "score_rules": score_rules,
                "score_ml": score_ml,
                "score_final": score_final,
                "level": level,
                "flags_triggered": len(top_reasons),
            }
