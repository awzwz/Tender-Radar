"""
Anomaly scoring with IsolationForest (unsupervised).
Train on unlabeled feature matrix; calibrate raw scores to 0..100 via train percentiles.
"""
import json
import logging
import os
from typing import Optional

import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

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

MODEL_NAME = "isoforest_v1"
DEFAULT_ARTIFACT_DIR = "artifacts"


def _feature_vector_to_array(feature_vector: dict) -> np.ndarray:
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
    return np.array(vec, dtype=np.float64).reshape(1, -1)


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, MODEL_NAME, "v1")


def train_anomaly_model(
    X: np.ndarray,
    artifact_dir: Optional[str] = None,
    random_state: int = 42,
    contamination: float = 0.1,
) -> dict:
    """
    Fit IsolationForest on X (no labels). Save model + percentiles for calibration.
    Returns metrics dict with percentiles and path.
    """
    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    model = IsolationForest(
        contamination=contamination,
        random_state=random_state,
        n_estimators=100,
        max_samples="auto",
    )
    model.fit(X)

    # Raw anomaly: decision_function gives negative for more anomalous (sklearn convention)
    raw_scores = model.decision_function(X)
    # Map to "anomaly intensity" 0=normal, higher=more anomalous (e.g. use -decision_function and scale)
    # decision_function: negative = anomaly. We want 0..100 where 100 = most anomalous.
    # So: percentile of -raw_scores (higher -raw_scores = more anomalous)
    anomaly_intensity = -raw_scores

    percentiles = {}
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        percentiles[str(p)] = float(np.percentile(anomaly_intensity, p))
    percentiles["min"] = float(np.min(anomaly_intensity))
    percentiles["max"] = float(np.max(anomaly_intensity))

    metrics = {
        "model_name": MODEL_NAME,
        "version": "v1",
        "n_samples": int(X.shape[0]),
        "n_features": int(X.shape[1]),
        "feature_columns": FEATURE_COLUMNS,
        "percentiles": percentiles,
        "contamination": contamination,
        "random_state": random_state,
    }

    model_path = os.path.join(out_dir, "model.joblib")
    joblib.dump(model, model_path)
    metrics_path = os.path.join(out_dir, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    schema_path = os.path.join(out_dir, "feature_schema.json")
    with open(schema_path, "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("Anomaly model saved to %s", out_dir)
    return {"path": out_dir, "metrics": metrics}


def _raw_to_score_100(raw_anomaly: float, percentiles: dict) -> float:
    """Map raw anomaly intensity to 0..100 using train percentiles."""
    p_min = percentiles.get("min", 0)
    p_max = percentiles.get("max", 100)
    if p_max <= p_min:
        return 50.0
    # Linear interpolation within [min, max], then clamp
    t = (raw_anomaly - p_min) / (p_max - p_min)
    return float(np.clip(100.0 * t, 0.0, 100.0))


_anomaly_predictor_cache = None


def get_anomaly_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load anomaly model and metrics; return predictor (cached). Thread-safe enough for Celery."""
    global _anomaly_predictor_cache
    if _anomaly_predictor_cache is not None and not reload:
        return _anomaly_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.joblib")
    metrics_path = os.path.join(out_dir, "metrics.json")

    if not os.path.exists(model_path) or not os.path.exists(metrics_path):
        logger.debug("Anomaly model not found at %s", out_dir)
        return None

    try:
        model = joblib.load(model_path)
        with open(metrics_path) as f:
            metrics = json.load(f)
        percentiles = metrics.get("percentiles", {})
        _anomaly_predictor_cache = (model, percentiles, MODEL_NAME + "_v1")
        return _anomaly_predictor_cache
    except Exception as e:
        logger.warning("Failed to load anomaly model: %s", e)
        return None


def predict_anomaly(
    feature_vector: dict,
    artifact_dir: Optional[str] = None,
) -> Optional[tuple[float, str, dict]]:
    """
    Returns (anomaly_score_0_100, model_version, explanation_dict) or None if model missing.
    """
    packed = get_anomaly_predictor(artifact_dir)
    if packed is None:
        return None
    model, percentiles, version = packed
    X = _feature_vector_to_array(feature_vector)
    raw = model.decision_function(X)[0]
    anomaly_intensity = -raw
    score_100 = _raw_to_score_100(anomaly_intensity, percentiles)
    # Simple explanation: which features deviate most (z-score style from 0)
    feature_vals = X.flatten()
    # Use mean=0, std=1 as reference; high absolute value = more abnormal
    abs_vals = np.abs(feature_vals)
    top_idx = np.argsort(-abs_vals)[:5]
    explanation = {
        "top_abnormal_features": [
            {"feature": FEATURE_COLUMNS[i], "value": float(feature_vals[i])}
            for i in top_idx
        ],
    }
    return (round(score_100, 1), version, explanation)
