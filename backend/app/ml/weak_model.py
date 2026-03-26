"""
Weak model v2: HistGradientBoosting trained on real numerical + binary features.
Proper train/test split, cross-validation, evaluation metrics.
"""
import json
import logging
import os
from typing import Optional

import numpy as np
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.metrics import (
    classification_report, confusion_matrix, f1_score,
    accuracy_score, roc_auc_score, precision_score, recall_score,
)
from sklearn.preprocessing import StandardScaler

from app.ml.feature_engineering import ALL_FEATURES, features_dict_to_array

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = ALL_FEATURES

WEAK_MODEL_NAME = "weak_gbm_v2"
DEFAULT_ARTIFACT_DIR = "artifacts"


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, WEAK_MODEL_NAME, "v1")


def feature_vector_to_array(feature_vector: dict) -> np.ndarray:
    return features_dict_to_array(feature_vector)


def _safe_feature_importances(model) -> np.ndarray:
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
    n_folds: int = 5,
) -> dict:
    """
    Train with proper evaluation:
    - Binarize labels from weak_proba
    - Stratified K-Fold cross-validation
    - Output confusion matrix, F1, accuracy, precision, recall, AUC
    - Save best model + scaler + metrics
    """
    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    nan_mask = ~np.isnan(weak_proba)
    X_valid = X[nan_mask]
    wp_valid = weak_proba[nan_mask]

    if len(X_valid) < 50:
        raise ValueError(f"Too few samples with labels: {len(X_valid)} (need >= 50)")

    y = (wp_valid >= 0.5).astype(np.int32)
    sw = np.where(y == 1, wp_valid, 1.0 - wp_valid)
    sw = np.clip(sw, 0.05, 0.95)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_valid)

    model = HistGradientBoostingClassifier(
        max_iter=200,
        random_state=random_state,
        learning_rate=0.05,
        max_depth=6,
        min_samples_leaf=10,
        l2_regularization=1.0,
    )

    n_folds_actual = min(n_folds, min(np.sum(y == 0), np.sum(y == 1)))
    if n_folds_actual < 2:
        n_folds_actual = 2

    skf = StratifiedKFold(n_splits=n_folds_actual, shuffle=True, random_state=random_state)

    y_pred_cv = cross_val_predict(model, X_scaled, y, cv=skf, method="predict")
    y_proba_cv = cross_val_predict(model, X_scaled, y, cv=skf, method="predict_proba")[:, 1]

    cm = confusion_matrix(y, y_pred_cv)
    acc = accuracy_score(y, y_pred_cv)
    f1 = f1_score(y, y_pred_cv, zero_division=0)
    prec = precision_score(y, y_pred_cv, zero_division=0)
    rec = recall_score(y, y_pred_cv, zero_division=0)
    try:
        auc = roc_auc_score(y, y_proba_cv)
    except ValueError:
        auc = 0.0

    cls_report = classification_report(y, y_pred_cv, target_names=["CLEAN", "SUSPICIOUS"], output_dict=True)

    model.fit(X_scaled, y, sample_weight=sw)

    importances = _safe_feature_importances(model)
    top_indices = np.argsort(-importances)[:15]

    metrics = {
        "model_name": WEAK_MODEL_NAME,
        "version": "v2",
        "n_samples": int(X_scaled.shape[0]),
        "n_features": len(FEATURE_COLUMNS),
        "n_positive": int(np.sum(y == 1)),
        "n_negative": int(np.sum(y == 0)),
        "class_balance": round(float(np.mean(y)), 4),
        "cv_folds": n_folds_actual,
        "accuracy": round(acc, 4),
        "f1_score": round(f1, 4),
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "roc_auc": round(auc, 4),
        "confusion_matrix": cm.tolist(),
        "classification_report": cls_report,
        "feature_columns": FEATURE_COLUMNS,
        "top_features": [
            {"feature": FEATURE_COLUMNS[i], "importance": round(float(importances[i]), 6)}
            for i in top_indices if importances[i] > 0
        ],
        "backend": "sklearn.HistGradientBoostingClassifier",
    }

    joblib.dump(model, os.path.join(out_dir, "model.joblib"))
    joblib.dump(scaler, os.path.join(out_dir, "scaler.joblib"))
    with open(os.path.join(out_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    with open(os.path.join(out_dir, "feature_schema.json"), "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("=" * 60)
    logger.info("WEAK MODEL TRAINING RESULTS (v2)")
    logger.info("=" * 60)
    logger.info("Samples: %d (pos=%d, neg=%d, balance=%.2f%%)",
                len(y), np.sum(y == 1), np.sum(y == 0), np.mean(y) * 100)
    logger.info("CV Folds: %d", n_folds_actual)
    logger.info("Accuracy:  %.4f", acc)
    logger.info("F1 Score:  %.4f", f1)
    logger.info("Precision: %.4f", prec)
    logger.info("Recall:    %.4f", rec)
    logger.info("ROC AUC:   %.4f", auc)
    logger.info("Confusion Matrix:")
    logger.info("  TN=%d  FP=%d", cm[0][0], cm[0][1])
    logger.info("  FN=%d  TP=%d", cm[1][0], cm[1][1])
    logger.info("Top features: %s", [FEATURE_COLUMNS[i] for i in top_indices[:5]])
    logger.info("Model saved to %s", out_dir)
    logger.info("=" * 60)

    return {"path": out_dir, "metrics": metrics}


_weak_predictor_cache = None


def get_weak_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load weak model; return (model, scaler, version) or None."""
    global _weak_predictor_cache
    if _weak_predictor_cache is not None and not reload:
        return _weak_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.joblib")
    scaler_path = os.path.join(out_dir, "scaler.joblib")

    if not os.path.exists(model_path):
        # Fallback to v1
        v1_dir = os.path.join(os.path.dirname(out_dir), "..", "weak_gbm_v1", "v1")
        v1_path = os.path.join(v1_dir, "model.joblib")
        if os.path.exists(v1_path):
            try:
                model = joblib.load(v1_path)
                _weak_predictor_cache = (model, None, "weak_gbm_v1")
                return _weak_predictor_cache
            except Exception:
                pass
        return None

    try:
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None
        _weak_predictor_cache = (model, scaler, WEAK_MODEL_NAME + "_v2")
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
    model, scaler, version = packed
    X = feature_vector_to_array(feature_vector).reshape(1, -1)

    if scaler is not None:
        X = scaler.transform(X)

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
            for i in top_idx if imp[i] > 0
        ],
    }
    return (float(proba), score_100, version, explanation)
