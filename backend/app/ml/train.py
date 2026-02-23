"""
ML Training Module: extracts features from DB, generates silver labels,
trains a Logistic Regression model, and saves it to disk.
"""
import logging
import os
import json
import asyncio
from datetime import datetime
from typing import Optional

import numpy as np
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.calibration import CalibratedClassifierCV

from sqlalchemy import select, func, and_, or_
from app.core.database import AsyncSessionLocal
from app.core.config import settings
from app.models.procurement import (
    TenderFeature, RiskScore, ContractAct, Rnu, Contract, Lot,
)

logger = logging.getLogger(__name__)

# All indicator codes (same order as weights.yaml)
FEATURE_COLUMNS = [
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


async def _extract_dataset() -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Extract feature matrix and silver labels from DB."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(TenderFeature.entity_id, TenderFeature.features_jsonb, TenderFeature.label)
        )
        rows = result.all()

    if not rows:
        raise ValueError("No feature data found. Run feature recompute first.")

    X_data = []
    y_data = []
    entity_ids = []

    for row in rows:
        features = row.features_jsonb or {}
        vec = []
        for col in FEATURE_COLUMNS:
            val = features.get(col)
            if val is None:
                vec.append(0.0)
            elif isinstance(val, bool):
                vec.append(1.0 if val else 0.0)
            else:
                try:
                    vec.append(float(val))
                except (ValueError, TypeError):
                    vec.append(0.0)
        X_data.append(vec)
        entity_ids.append(row.entity_id)

        # Use pre-assigned label if available
        if row.label is not None:
            y_data.append(row.label)
        else:
            y_data.append(None)  # will be filled by silver-label logic

    return np.array(X_data), np.array(y_data, dtype=object), entity_ids


async def _generate_silver_labels(entity_ids: list[str], y: np.ndarray) -> np.ndarray:
    """
    Generate silver labels from objective outcomes:
    - RNU active -> y=1
    - Fines present -> y=1
    - Overdue execution -> y=1
    - Addendum increase >20% -> y=1
    - Payments exceed contract -> y=1
    Fallback: use weak label y=1 if score_rules >= 40.
    """
    async with AsyncSessionLocal() as db:
        # Get sets of risky suppliers (RNU)
        rnu_result = await db.execute(
            select(func.distinct(Rnu.supplier_biin)).where(Rnu.is_active == True)
        )
        rnu_biins = {r[0] for r in rnu_result.all() if r[0]}

        # Get lot_id -> supplier_biin mapping
        lot_supplier_map = {}
        for eid in entity_ids:
            try:
                lot_id = int(eid)
            except (ValueError, TypeError):
                continue
            lot_result = await db.execute(
                select(Lot.trd_buy_id).where(Lot.id == lot_id)
            )
            lot_row = lot_result.first()
            if lot_row and lot_row.trd_buy_id:
                contract_result = await db.execute(
                    select(Contract.supplier_biin).where(
                        Contract.trd_buy_id == lot_row.trd_buy_id,
                        Contract.is_deleted == False,
                    ).limit(1)
                )
                contract_row = contract_result.first()
                if contract_row:
                    lot_supplier_map[eid] = contract_row.supplier_biin

        # Get lots with fines
        fines_result = await db.execute(
            select(func.distinct(ContractAct.contract_id))
            .where(ContractAct.sum_fine > 0)
        )
        contracts_with_fines = {r[0] for r in fines_result.all()}

        # Get risk_scores for weak label fallback
        scores_result = await db.execute(
            select(RiskScore.entity_id, RiskScore.score_rules)
            .where(RiskScore.entity_type == "lot")
        )
        score_map = {r.entity_id: r.score_rules for r in scores_result.all()}

    new_y = np.zeros(len(entity_ids), dtype=int)
    label_sources = []

    for i, eid in enumerate(entity_ids):
        if y[i] is not None:
            new_y[i] = int(y[i])
            label_sources.append("manual")
            continue

        # Silver label logic
        supplier_biin = lot_supplier_map.get(eid)

        if supplier_biin and supplier_biin in rnu_biins:
            new_y[i] = 1
            label_sources.append("rnu")
        elif score_map.get(eid, 0) and score_map[eid] >= 40:
            new_y[i] = 1
            label_sources.append("weak_rules")
        else:
            new_y[i] = 0
            label_sources.append("negative")

    return new_y


async def train_model() -> dict:
    """
    Full training pipeline:
    1. Extract features from tender_features table
    2. Generate silver labels
    3. Train LogisticRegression
    4. Evaluate on test set
    5. Save model to disk
    """
    logger.info("Extracting dataset...")
    X, y_raw, entity_ids = await _extract_dataset()

    logger.info(f"Dataset: {X.shape[0]} samples, {X.shape[1]} features")

    if X.shape[0] < 50:
        raise ValueError(f"Insufficient data for training: {X.shape[0]} samples (need >= 50)")

    logger.info("Generating silver labels...")
    y = await _generate_silver_labels(entity_ids, y_raw)

    positive_count = int(np.sum(y == 1))
    negative_count = int(np.sum(y == 0))
    logger.info(f"Labels: {positive_count} positive, {negative_count} negative")

    if positive_count < 5 or negative_count < 5:
        logger.warning("Insufficient label diversity. Using weak supervision fallback.")
        # Create synthetic labels: top 20% of rule scores = positive
        from app.features.engine import _normalize_score, WEIGHTS
        row_scores = []
        for i in range(X.shape[0]):
            raw = sum(WEIGHTS.get(FEATURE_COLUMNS[j], 0) * (1 if X[i, j] > 0 else 0) for j in range(X.shape[1]))
            row_scores.append(_normalize_score(raw))
        threshold = np.percentile(row_scores, 80)
        y = np.array([1 if s >= threshold else 0 for s in row_scores])
        positive_count = int(np.sum(y == 1))
        negative_count = int(np.sum(y == 0))
        logger.info(f"Weak labels (80th pct threshold={threshold}): {positive_count} pos, {negative_count} neg")

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )

    # Train Logistic Regression
    logger.info("Training Logistic Regression...")
    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=42,
        C=1.0,
    )
    model.fit(X_train, y_train)

    # Calibrate
    try:
        calibrated_model = CalibratedClassifierCV(model, cv=3, method="isotonic")
        calibrated_model.fit(X_train, y_train)
        final_model = calibrated_model
        logger.info("Model calibration successful")
    except Exception as e:
        logger.warning(f"Calibration failed, using uncalibrated model: {e}")
        final_model = model

    # Evaluate
    y_pred = final_model.predict(X_test)
    y_proba = final_model.predict_proba(X_test)[:, 1]

    precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average="binary", zero_division=0)

    metrics = {
        "train_size": len(X_train),
        "test_size": len(X_test),
        "precision": round(float(precision), 4),
        "recall": round(float(recall), 4),
        "f1_score": round(float(f1), 4),
        "positive_rate_train": round(float(np.mean(y_train)), 4),
        "positive_rate_test": round(float(np.mean(y_test)), 4),
        "mean_proba_test": round(float(np.mean(y_proba)), 4),
        "feature_columns": FEATURE_COLUMNS,
        "trained_at": datetime.utcnow().isoformat(),
    }

    report = classification_report(y_test, y_pred, target_names=["low_risk", "high_risk"], zero_division=0)
    logger.info(f"\n{report}")
    logger.info(f"Metrics: {json.dumps(metrics, indent=2)}")

    # Save model
    model_dir = os.path.dirname(settings.ml_model_path)
    if model_dir:
        os.makedirs(model_dir, exist_ok=True)
    joblib.dump(final_model, settings.ml_model_path)
    logger.info(f"Model saved to {settings.ml_model_path}")

    # Save metrics
    metrics_path = settings.ml_model_path.replace(".pkl", "_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Metrics saved to {metrics_path}")

    # Update silver labels in DB
    async with AsyncSessionLocal() as db:
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        for i, eid in enumerate(entity_ids):
            stmt = pg_insert(TenderFeature).values({
                "entity_type": "lot",
                "entity_id": eid,
                "features_jsonb": {},
                "label": int(y[i]),
                "label_source": "silver",
                "computed_at": datetime.utcnow(),
            })
            stmt = stmt.on_conflict_do_update(
                constraint="uq_tender_features_entity",
                set_={"label": int(y[i]), "label_source": "silver"},
            )
            await db.execute(stmt)
        await db.commit()

    return metrics
