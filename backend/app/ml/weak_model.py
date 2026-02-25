"""
Weak model: train classifier (HistGradientBoosting) on weak labels.
Uses sample_weight = weak_proba for positive, 1-weak_proba for negative.
"""
import json
import logging
import os
from typing import Optional

import numpy as np
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier

# Hardcoded feature columns since train.py was removed
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

logger = logging.getLogger(__name__)

WEAK_MODEL_NAME = "weak_gbm_v1"
DEFAULT_ARTIFACT_DIR = "artifacts"


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, WEAK_MODEL_NAME, "v1")


def feature_vector_to_array(feature_vector: dict) -> np.ndarray:
    vec = []
    for col in FEATURE_COLUMNS:
        val = feature_vector.get(col)
        if val is None:
            vec.append(0.0)
        elif isinstance(val, bool):
            vec.append(1.0 if val else 0.0)
        else:
            try:
                vec.append(float(val))
            except (ValueError, TypeError):
                vec.append(0.0)
    return np.array(vec, dtype=np.float64)


def _safe_feature_importances(model) -> np.ndarray:
    """
    Return feature importances aligned with FEATURE_COLUMNS.
    HistGradientBoostingClassifier in sklearn may not expose feature_importances_;
    in that case we return zeros to keep training/inference pipeline robust.
    """
    try:
        if hasattr(model, "feature_importances_"):
            imp = np.asarray(model.feature_importances_, dtype=np.float64)
            if imp.shape[0] == len(FEATURE_COLUMNS):
                return imp
    except Exception:
        pass
    return np.zeros(len(FEATURE_COLUMNS), dtype=np.float64)


def train_weak_model(
    X: np.ndarray,
    weak_proba: np.ndarray,
    artifact_dir: Optional[str] = None,
    random_state: int = 42,
) -> dict:
    """
    X: (n_samples, n_features), weak_proba: (n_samples,) in [0, 1].
    y = (weak_proba >= 0.5).astype(int), sample_weight: for y=1 use weak_proba, for y=0 use 1-weak_proba.
    """
    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    y = (weak_proba >= 0.5).astype(np.int32)
    sw = np.where(y == 1, weak_proba, 1.0 - weak_proba)
    sw = np.clip(sw, 0.01, 0.99)

    model = HistGradientBoostingClassifier(
        max_iter=100,
        random_state=random_state,
        learning_rate=0.1,
        max_depth=5,
    )
    model.fit(X, y, sample_weight=sw)

    # Feature importances (robust across sklearn estimators)
    importances = _safe_feature_importances(model)
    top_indices = np.argsort(-importances)[:10]
    explanation = {
        "feature_importances": [
            {"feature": FEATURE_COLUMNS[i], "importance": float(importances[i])}
            for i in top_indices
        ],
    }

    metrics = {
        "model_name": WEAK_MODEL_NAME,
        "version": "v1",
        "n_samples": int(X.shape[0]),
        "feature_columns": FEATURE_COLUMNS,
        "backend": "sklearn.HistGradientBoostingClassifier",
        "top_features": [FEATURE_COLUMNS[i] for i in top_indices[:5]],
    }

    model_path = os.path.join(out_dir, "model.joblib")
    joblib.dump(model, model_path)
    metrics_path = os.path.join(out_dir, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    with open(os.path.join(out_dir, "feature_schema.json"), "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("Weak model saved to %s", out_dir)
    return {"path": out_dir, "metrics": metrics}


_weak_predictor_cache = None


def get_weak_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load weak model; return (model, version) or None."""
    global _weak_predictor_cache
    if _weak_predictor_cache is not None and not reload:
        return _weak_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.joblib")
    if not os.path.exists(model_path):
        return None
    try:
        model = joblib.load(model_path)
        _weak_predictor_cache = (model, WEAK_MODEL_NAME + "_v1")
        return _weak_predictor_cache
    except Exception as e:
        logger.warning("Failed to load weak model: %s", e)
        return None


def predict_weak(
    feature_vector: dict,
    artifact_dir: Optional[str] = None,
) -> Optional[tuple[float, float, str, dict]]:
    """Returns (weak_proba, weak_score_0_100, model_version, explanation) or None."""
    packed = get_weak_predictor(artifact_dir)
    if packed is None:
        return None
    model, version = packed
    X = feature_vector_to_array(feature_vector).reshape(1, -1)
    try:
        proba = model.predict_proba(X)[0, 1]
    except Exception:
        return None
    score_100 = round(100.0 * proba, 1)
    imp = _safe_feature_importances(model)
    top_idx = np.argsort(-imp)[:5]
    explanation = {
        "top_features": [
            {"feature": FEATURE_COLUMNS[i], "importance": float(imp[i])}
            for i in top_idx
        ],
    }
    return (float(proba), score_100, version, explanation)
