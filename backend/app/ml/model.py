"""
ML Model Module: loads trained model and provides prediction interface.
Thread-safe singleton pattern.
"""
import logging
import os
import threading
from typing import Optional

import numpy as np
import joblib

from app.core.config import settings

logger = logging.getLogger(__name__)

_model = None
_lock = threading.Lock()

# Must match FEATURE_COLUMNS in train.py
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


def get_model():
    """Load model from disk (singleton, thread-safe)."""
    global _model
    if _model is not None:
        return _model

    with _lock:
        if _model is not None:
            return _model

        model_path = settings.ml_model_path
        if not os.path.exists(model_path):
            logger.warning(f"ML model not found at {model_path}")
            return None

        try:
            _model = joblib.load(model_path)
            logger.info(f"ML model loaded from {model_path}")
            return _model
        except Exception as e:
            logger.error(f"Failed to load ML model: {e}")
            return None


def predict_risk(model, feature_vector: dict) -> float:
    """
    Predict risk probability from a feature vector dict.
    Returns P(risky) in [0, 1].
    """
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

    X = np.array([vec])
    try:
        proba = model.predict_proba(X)[0, 1]
        return float(proba)
    except Exception as e:
        logger.error(f"ML prediction failed: {e}")
        return 0.5  # Return neutral if prediction fails


def reload_model():
    """Force reload of the model (e.g., after retraining)."""
    global _model
    with _lock:
        _model = None
    return get_model()
