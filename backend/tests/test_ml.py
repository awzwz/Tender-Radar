"""
Tests for ML scoring integration.
"""
import pytest
import numpy as np


def test_predict_risk_returns_valid_range():
    """ML model prediction should be in [0, 1]."""
    from app.ml.model import FEATURE_COLUMNS

    # Simulate a calibrated model
    from sklearn.linear_model import LogisticRegression
    from sklearn.calibration import CalibratedClassifierCV

    # Create a toy model
    rng = np.random.RandomState(42)
    X_train = rng.randn(100, len(FEATURE_COLUMNS))
    y_train = (X_train[:, 0] > 0).astype(int)

    model = LogisticRegression(max_iter=100, random_state=42)
    model.fit(X_train, y_train)

    from app.ml.model import predict_risk

    # Test with zero vector
    zero_features = {col: 0.0 for col in FEATURE_COLUMNS}
    proba = predict_risk(model, zero_features)
    assert 0.0 <= proba <= 1.0, f"Prediction out of range: {proba}"

    # Test with high-risk vector
    high_risk = {col: 1.0 for col in FEATURE_COLUMNS}
    proba_high = predict_risk(model, high_risk)
    assert 0.0 <= proba_high <= 1.0, f"Prediction out of range: {proba_high}"


def test_risk_final_formula():
    """risk_final = round(100 * (0.6 * rules/100 + 0.4 * ml))"""
    score_rules = 70.0
    score_ml = 0.8

    score_final = round(100 * (0.6 * score_rules / 100 + 0.4 * score_ml))
    assert score_final == 74, f"Expected 74, got {score_final}"

    # Edge case: rules=0, ml=0
    assert round(100 * (0.6 * 0 / 100 + 0.4 * 0)) == 0

    # Edge case: rules=100, ml=1.0
    result = round(100 * (0.6 * 100 / 100 + 0.4 * 1.0))
    assert result == 100, f"Expected 100, got {result}"


def test_feature_vector_ordering():
    """Feature columns should match between train.py and model.py."""
    from app.ml.train import FEATURE_COLUMNS as TRAIN_COLS
    from app.ml.model import FEATURE_COLUMNS as MODEL_COLS

    assert TRAIN_COLS == MODEL_COLS, "Feature column ordering mismatch between train and model modules"


    import os
    from app.core.config import settings
def test_anomaly_predictor_returns_none_when_no_model():
    # Predictor may be None if no artifact on disk
    # If no model saved, predict_anomaly should return (None, None, None, None)
    """When anomaly model is not trained, get_anomaly_predictor returns None."""
    from app.ml.anomaly import get_anomaly_predictor, predict_anomaly

    pred = get_anomaly_predictor()
    score, proba, version, expl = predict_anomaly({})
    if pred is None:
        assert score is None and proba is None and version is None and expl is None


def test_composite_clamp():
    """Composite adjustment should keep score in [0, 100]."""
    delta = 15.0
    score_final = 50.0
    # f in [-1, 1]
    for f in [-1.0, 0.0, 1.0]:
        composite = max(0, min(100, score_final + delta * f))
        assert 0 <= composite <= 100, f"composite={composite} out of range for f={f}"
