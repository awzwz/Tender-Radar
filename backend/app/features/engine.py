"""
Feature Engine v2: orchestrates 31 indicators, computes risk scores.
New scoring formula: rules + ML ensemble + graph features.
ML operates INDEPENDENTLY from rules (no guardrails that kill ML signal).
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

# Scoring weights: how much each signal contributes to final score
ALPHA_RULES = 0.55   # rule-based score weight
BETA_ML = 0.35       # ML ensemble weight
GAMMA_GRAPH = 0.10   # graph features weight


def _normalize_score(raw: float) -> float:
    """Cap raw weighted sum at 100. Weights are designed as direct score points."""
    return round(min(100.0, raw), 1)


def _get_level(score: float) -> str:
    if score <= THRESHOLDS["low_max"]:
        return "LOW"
    elif score <= THRESHOLDS["medium_max"]:
        return "MEDIUM"
    return "HIGH"


class FeatureEngine:
    """
    Computes risk flags, rule-based score, ML ensemble score, and final score.
    """

    def __init__(self):
        self._ensemble_available = False
        try:
            from app.ml.ensemble import compute_ml_ensemble
            self._compute_ml_ensemble = compute_ml_ensemble
            self._ensemble_available = True
            logger.info("ML ensemble loaded")
        except Exception as e:
            logger.warning("ML ensemble not available: %s", e)
            self._compute_ml_ensemble = None

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

            # ── Combo boosters: dangerous combinations amplify risk ──────────
            triggered_codes = {code for code, r in flags.items() if r.get("flag")}
            combo_bonus = 0
            triggered_combos = []
            for combo_def in _CONFIG.get("combo_boosters", []):
                combo_codes = set(combo_def.get("codes", []))
                if combo_codes.issubset(triggered_codes):
                    combo_bonus += combo_def.get("bonus", 0)
                    triggered_combos.append(combo_def.get("description", ""))
            raw_score += combo_bonus

            score_rules = _normalize_score(raw_score)

            # ── Build feature vector for ML ───────────────────────────────────
            feature_vector = {}
            for code in ALL_INDICATORS:
                r = flags.get(code, {})
                feature_vector[code] = r.get("value") if r.get("value") is not None else (
                    1.0 if r.get("flag") else 0.0
                )

            # Enrich with numerical features for ML pipeline
            try:
                from app.ml.feature_engineering import extract_features_for_lot
                rich_features = await extract_features_for_lot(lot_id, db)
                if rich_features:
                    feature_vector.update(rich_features)
            except Exception as e:
                logger.debug("Rich feature extraction skipped: %s", e)

            # ── ML ensemble scoring ───────────────────────────────────────────
            ml_result = None
            ml_score = 0.0
            ml_explanation = None
            if self._compute_ml_ensemble is not None:
                try:
                    ml_result = self._compute_ml_ensemble(feature_vector)
                    if ml_result:
                        ml_score = ml_result["ml_score"]
                        ml_explanation = ml_result
                except Exception as e:
                    logger.debug("ML ensemble prediction skipped: %s", e)

            # ── Graph features ────────────────────────────────────────────────
            graph_score = 0.0
            try:
                from app.features.graph_features import get_graph_features_for_lot
                gf = await get_graph_features_for_lot(lot_id)
                if gf is not None:
                    top_share = float(gf.get("customer_top_supplier_win_share") or 0)
                    rotation = int(gf.get("customer_win_rotation_index") or 0)
                    if rotation > 0:
                        graph_score = min(100.0, top_share)
                    else:
                        graph_score = 50.0
            except Exception as e:
                logger.debug("Graph features skipped: %s", e)

            # ── Final score: weighted combination ─────────────────────────────
            # ML operates INDEPENDENTLY — no guardrails that kill ML signal
            score_final = (
                ALPHA_RULES * score_rules
                + BETA_ML * ml_score
                + GAMMA_GRAPH * graph_score
            )

            # Soft guardrail: if ML is very high but rules see nothing,
            # ensure score is at least MEDIUM-ish (ML found something rules missed)
            if score_rules < 5 and ml_score > 70:
                score_final = max(score_final, 40.0)

            score_final = round(max(0.0, min(100.0, score_final)))
            level = _get_level(score_final)

            # ── Anomaly score for backward compat (from ensemble components) ──
            anomaly_score = None
            anomaly_model_version = None
            anomaly_explanation_jsonb = None
            if ml_result and "components" in ml_result:
                anomaly_score = ml_result["components"].get("isoforest")
                anomaly_model_version = ml_result.get("versions", {}).get("isoforest")
                anomaly_explanation_jsonb = ml_result.get("explanations")

            # ── Top reasons (highest weight flags that are True) ──────────────
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

            # Append combo booster explanations
            for combo_desc in triggered_combos:
                top_reasons.insert(0, {
                    "code": "COMBO_BOOSTER",
                    "weight": 0,
                    "description": f"Опасная комбинация: {combo_desc}",
                    "evidence": {},
                })

            # Append ML explanations when ML score is significant
            if ml_result and ml_score > 40:
                ml_ev = {}
                if ml_result.get("explanations"):
                    for i, feat in enumerate(ml_result["explanations"][:3]):
                        models = ", ".join(feat.get("flagged_by", []))
                        ml_ev[f"ML: {feat.get('feature', '')}"] = f"Обнаружено: {models}"

                agreement_label = {
                    "full": "все 3 модели",
                    "partial": "2 из 3 моделей",
                    "none": "1 модель",
                }.get(ml_result.get("model_agreement", "none"), "")

                top_reasons.insert(0, {
                    "code": "ML_ENSEMBLE",
                    "weight": 100,
                    "description": f"ML Ensemble обнаружил аномалию (score: {round(ml_score, 1)}, согласие: {agreement_label})",
                    "evidence": ml_ev,
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
            score_ml_normalized = ml_score / 100.0 if ml_score else None
            score_stmt = pg_insert(RiskScore).values({
                "entity_type": "lot",
                "entity_id": str(lot_id),
                "score": score_final,
                "score_rules": score_rules,
                "score_ml": score_ml_normalized,
                "score_final": score_final,
                "level": level,
                "top_reasons_jsonb": top_reasons,
                "computed_at": now,
                "anomaly_score": anomaly_score,
                "anomaly_model_version": anomaly_model_version,
                "anomaly_explanation_jsonb": anomaly_explanation_jsonb,
                "weak_proba": None,
                "weak_score": None,
                "weak_model_version": None,
                "weak_explanation_jsonb": None,
                "composite_score": ml_score if ml_result else None,
                "components_jsonb": ml_result.get("components") if ml_result else None,
            })
            score_stmt = score_stmt.on_conflict_do_update(
                constraint="uq_risk_scores_entity",
                set_={
                    "score": score_final,
                    "score_rules": score_rules,
                    "score_ml": score_ml_normalized,
                    "score_final": score_final,
                    "level": level,
                    "top_reasons_jsonb": top_reasons,
                    "computed_at": now,
                    "anomaly_score": anomaly_score,
                    "anomaly_model_version": anomaly_model_version,
                    "anomaly_explanation_jsonb": anomaly_explanation_jsonb,
                    "weak_proba": None,
                    "weak_score": None,
                    "weak_model_version": None,
                    "weak_explanation_jsonb": None,
                    "composite_score": ml_score if ml_result else None,
                    "components_jsonb": ml_result.get("components") if ml_result else None,
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
                "score_ml": ml_score,
                "score_final": score_final,
                "level": level,
                "flags_triggered": len(top_reasons),
                "ml_agreement": ml_result.get("model_agreement") if ml_result else None,
            }
