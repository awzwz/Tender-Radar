"""
Anomaly scoring v3: IsolationForest on 30 numerical features only.
- No binary indicators (avoids data leakage)
- contamination="auto" (no arbitrary 10% assumption)
- SHAP-based explanations (real feature contributions, not abs values)
- Percentile-based calibration with severity levels
"""
import json
import logging
import os
from typing import Optional

import numpy as np
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from app.ml.feature_engineering import ML_FEATURES, features_dict_to_array

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = ML_FEATURES  # 30 numerical features only

MODEL_NAME = "isoforest_v3"
DEFAULT_ARTIFACT_DIR = "artifacts"


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, MODEL_NAME, "v1")


def train_anomaly_model(
    X: np.ndarray,
    artifact_dir: Optional[str] = None,
    random_state: int = 42,
) -> dict:
    """
    Fit IsolationForest on 30 numerical features.
    - contamination="auto" (let the model decide)
    - SHAP values computed for explainability
    """
    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    if X.shape[1] != len(FEATURE_COLUMNS):
        raise ValueError(
            f"Expected {len(FEATURE_COLUMNS)} features, got {X.shape[1]}. "
            f"Use ML_FEATURES (numerical only), not ALL_FEATURES."
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        contamination="auto",
        random_state=random_state,
        n_estimators=300,
        max_samples="auto",
        max_features=0.8,
    )
    model.fit(X_scaled)

    # Compute anomaly scores for calibration
    raw_scores = model.decision_function(X_scaled)
    anomaly_intensity = -raw_scores  # higher = more anomalous
    predictions = model.predict(X_scaled)
    n_anomalies = int(np.sum(predictions == -1))

    # Percentiles for score calibration
    percentiles = {}
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        percentiles[str(p)] = float(np.percentile(anomaly_intensity, p))
    percentiles["min"] = float(np.min(anomaly_intensity))
    percentiles["max"] = float(np.max(anomaly_intensity))

    # Severity thresholds based on percentiles
    severity_thresholds = {
        "elevated": float(np.percentile(anomaly_intensity, 90)),    # p90
        "high": float(np.percentile(anomaly_intensity, 95)),        # p95
        "critical": float(np.percentile(anomaly_intensity, 99)),    # p99
    }

    # Compute SHAP values for global feature importance
    shap_importances = _compute_shap_importances(model, X_scaled)

    metrics = {
        "model_name": MODEL_NAME,
        "version": "v3",
        "n_samples": int(X.shape[0]),
        "n_features": len(FEATURE_COLUMNS),
        "n_anomalies_detected": n_anomalies,
        "anomaly_rate": round(n_anomalies / X.shape[0] * 100, 2),
        "feature_columns": FEATURE_COLUMNS,
        "percentiles": percentiles,
        "severity_thresholds": severity_thresholds,
        "contamination": "auto",
        "global_feature_importance": shap_importances,
    }

    joblib.dump(model, os.path.join(out_dir, "model.joblib"))
    joblib.dump(scaler, os.path.join(out_dir, "scaler.joblib"))
    with open(os.path.join(out_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    with open(os.path.join(out_dir, "feature_schema.json"), "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("=" * 60)
    logger.info("ANOMALY MODEL TRAINING RESULTS (v3 — numerical only)")
    logger.info("=" * 60)
    logger.info("Samples: %d, Features: %d (numerical only)", X.shape[0], X.shape[1])
    logger.info("Anomalies detected: %d (%.1f%%)", n_anomalies, n_anomalies / X.shape[0] * 100)
    logger.info("Severity thresholds: elevated=%.4f, high=%.4f, critical=%.4f",
                severity_thresholds["elevated"], severity_thresholds["high"], severity_thresholds["critical"])
    if shap_importances:
        top3 = sorted(shap_importances, key=lambda x: x["importance"], reverse=True)[:3]
        logger.info("Top SHAP features: %s", [(f["feature"], round(f["importance"], 4)) for f in top3])
    logger.info("Model saved to %s", out_dir)
    logger.info("=" * 60)

    return {"path": out_dir, "metrics": metrics}


def _compute_shap_importances(model, X_scaled: np.ndarray) -> list:
    """Compute global SHAP feature importances. Falls back to variance-based if SHAP unavailable."""
    try:
        import shap
        explainer = shap.TreeExplainer(model)
        # Use a subsample for speed (max 1000 samples)
        n_sub = min(1000, X_scaled.shape[0])
        idx = np.random.choice(X_scaled.shape[0], n_sub, replace=False)
        shap_values = explainer.shap_values(X_scaled[idx])
        mean_abs_shap = np.mean(np.abs(shap_values), axis=0)
        return [
            {"feature": FEATURE_COLUMNS[i], "importance": round(float(mean_abs_shap[i]), 6)}
            for i in np.argsort(-mean_abs_shap)[:15]
        ]
    except Exception as e:
        logger.warning("SHAP unavailable, falling back to variance-based importance: %s", e)
        feature_var = np.var(X_scaled, axis=0)
        top_idx = np.argsort(-feature_var)[:15]
        return [
            {"feature": FEATURE_COLUMNS[i], "importance": round(float(feature_var[i]), 6)}
            for i in top_idx
        ]


def _explain_single_sample(model, scaler, X_single: np.ndarray) -> dict:
    """SHAP-based explanation for a single sample. Falls back to deviation-based."""
    try:
        import shap
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_single)
        sv = shap_values.flatten()
        top_idx = np.argsort(-np.abs(sv))[:5]
        return {
            "method": "shap",
            "top_features": [
                {
                    "feature": FEATURE_COLUMNS[i],
                    "shap_value": round(float(sv[i]), 4),
                    "direction": "suspicious" if sv[i] > 0 else "normal",
                    "raw_value": round(float(X_single[0, i]), 4),
                }
                for i in top_idx
            ],
        }
    except Exception:
        # Fallback: features furthest from 0 (after scaling) with direction
        vals = X_single.flatten()
        top_idx = np.argsort(-np.abs(vals))[:5]
        return {
            "method": "deviation",
            "top_features": [
                {
                    "feature": FEATURE_COLUMNS[i],
                    "deviation": round(float(vals[i]), 4),
                    "direction": "high" if vals[i] > 0 else "low",
                    "raw_value": round(float(vals[i]), 4),
                }
                for i in top_idx
            ],
        }


def _raw_to_score_100(raw_anomaly: float, percentiles: dict, severity_thresholds: dict) -> tuple[float, str]:
    """
    Convert raw anomaly intensity to 0-100 score with severity level.
    Uses percentile mapping, not simple min-max.
    """
    p5 = percentiles.get("5", 0)
    p95 = percentiles.get("95", 1)

    # Map p5..p95 to 0..100 (more robust than min..max)
    if p95 <= p5:
        score = 50.0
    else:
        t = (raw_anomaly - p5) / (p95 - p5)
        score = float(np.clip(100.0 * t, 0.0, 100.0))

    # Severity level based on absolute thresholds
    if raw_anomaly >= severity_thresholds.get("critical", float("inf")):
        severity = "critical"
    elif raw_anomaly >= severity_thresholds.get("high", float("inf")):
        severity = "high"
    elif raw_anomaly >= severity_thresholds.get("elevated", float("inf")):
        severity = "elevated"
    else:
        severity = "normal"

    return round(score, 1), severity


_anomaly_predictor_cache = None


def get_anomaly_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load anomaly model; return (model, scaler, percentiles, severity_thresholds, version) or None."""
    global _anomaly_predictor_cache
    if _anomaly_predictor_cache is not None and not reload:
        return _anomaly_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.joblib")
    metrics_path = os.path.join(out_dir, "metrics.json")
    scaler_path = os.path.join(out_dir, "scaler.joblib")

    if not os.path.exists(model_path) or not os.path.exists(metrics_path):
        # Fallback to v2 then v1
        for fallback_name in ["isoforest_v2", "isoforest_v1"]:
            fb_dir = os.path.join(os.path.dirname(out_dir), "..", fallback_name, "v1")
            fb_model = os.path.join(fb_dir, "model.joblib")
            fb_metrics = os.path.join(fb_dir, "metrics.json")
            if os.path.exists(fb_model) and os.path.exists(fb_metrics):
                try:
                    model = joblib.load(fb_model)
                    scaler = None
                    fb_scaler = os.path.join(fb_dir, "scaler.joblib")
                    if os.path.exists(fb_scaler):
                        scaler = joblib.load(fb_scaler)
                    with open(fb_metrics) as f:
                        metrics = json.load(f)
                    pct = metrics.get("percentiles", {})
                    sev = metrics.get("severity_thresholds", {
                        "elevated": pct.get("90", 0),
                        "high": pct.get("95", 0),
                        "critical": pct.get("99", 0),
                    })
                    _anomaly_predictor_cache = (model, scaler, pct, sev, fallback_name)
                    logger.info("Loaded fallback anomaly model: %s", fallback_name)
                    return _anomaly_predictor_cache
                except Exception:
                    continue
        return None

    try:
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None
        with open(metrics_path) as f:
            metrics = json.load(f)
        percentiles = metrics.get("percentiles", {})
        severity_thresholds = metrics.get("severity_thresholds", {})
        _anomaly_predictor_cache = (model, scaler, percentiles, severity_thresholds, MODEL_NAME)
        return _anomaly_predictor_cache
    except Exception as e:
        logger.warning("Failed to load anomaly model: %s", e)
        return None


def predict_anomaly(
    feature_vector: dict,
    artifact_dir: Optional[str] = None,
) -> Optional[tuple[float, str, str, dict]]:
    """
    Returns (anomaly_score_0_100, severity, model_version, explanation_dict) or None.
    """
    packed = get_anomaly_predictor(artifact_dir)
    if packed is None:
        return None
    model, scaler, percentiles, severity_thresholds, version = packed

    X = features_dict_to_array(feature_vector, columns=FEATURE_COLUMNS).reshape(1, -1)

    if scaler is not None:
        X = scaler.transform(X)

    raw = model.decision_function(X)[0]
    anomaly_intensity = -raw
    score_100, severity = _raw_to_score_100(anomaly_intensity, percentiles, severity_thresholds)

    explanation = _explain_single_sample(model, scaler, X)

    return (score_100, severity, version, explanation)
