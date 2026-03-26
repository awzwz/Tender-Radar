"""
ML Ensemble: aggregates IsolationForest + Autoencoder + HDBSCAN scores.
Weighted average with agreement bonus.
"""
import logging
from typing import Optional

from app.ml.feature_engineering import ML_FEATURES

logger = logging.getLogger(__name__)

# Ensemble weights
WEIGHT_ISOFOREST = 0.40
WEIGHT_AUTOENCODER = 0.35
WEIGHT_HDBSCAN = 0.25

# Agreement bonus thresholds
AGREEMENT_THRESHOLD = 60  # score > this = "model thinks it's suspicious"
AGREEMENT_BONUS_2 = 10    # bonus when 2+ models agree
AGREEMENT_BONUS_3 = 15    # bonus when all 3 agree


def compute_ml_ensemble(feature_vector: dict) -> Optional[dict]:
    """
    Run all 3 ML models and aggregate their scores.

    Returns dict with:
        ml_score: float (0-100) — ensemble score
        components: dict — individual model scores
        explanations: list — merged top explanations
        model_agreement: str — "none", "partial", "full"
        models_available: int — how many models loaded successfully
    Or None if no models are available.
    """
    from app.ml.anomaly import predict_anomaly
    from app.ml.autoencoder import predict_autoencoder
    from app.ml.clustering import predict_hdbscan

    scores = {}
    explanations = {}
    versions = {}

    # 1. IsolationForest
    try:
        iso_result = predict_anomaly(feature_vector)
        if iso_result is not None:
            score, severity, version, expl = iso_result
            scores["isoforest"] = score
            explanations["isoforest"] = expl
            versions["isoforest"] = version
    except Exception as e:
        logger.debug("IsolationForest prediction failed: %s", e)

    # 2. Autoencoder
    try:
        ae_result = predict_autoencoder(feature_vector)
        if ae_result is not None:
            score, version, expl = ae_result
            scores["autoencoder"] = score
            explanations["autoencoder"] = expl
            versions["autoencoder"] = version
    except Exception as e:
        logger.debug("Autoencoder prediction failed: %s", e)

    # 3. HDBSCAN
    try:
        hdb_result = predict_hdbscan(feature_vector)
        if hdb_result is not None:
            score, version, expl = hdb_result
            scores["hdbscan"] = score
            explanations["hdbscan"] = expl
            versions["hdbscan"] = version
    except Exception as e:
        logger.debug("HDBSCAN prediction failed: %s", e)

    if not scores:
        return None

    # Weighted average (only from available models)
    weights = {
        "isoforest": WEIGHT_ISOFOREST,
        "autoencoder": WEIGHT_AUTOENCODER,
        "hdbscan": WEIGHT_HDBSCAN,
    }

    total_weight = sum(weights[k] for k in scores)
    if total_weight == 0:
        return None

    weighted_sum = sum(scores[k] * weights[k] for k in scores)
    ml_score = weighted_sum / total_weight

    # Agreement bonus
    high_models = [k for k, v in scores.items() if v > AGREEMENT_THRESHOLD]
    n_agree = len(high_models)

    if n_agree >= 3:
        agreement = "full"
        ml_score += AGREEMENT_BONUS_3
    elif n_agree >= 2:
        agreement = "partial"
        ml_score += AGREEMENT_BONUS_2
    else:
        agreement = "none"

    ml_score = round(max(0.0, min(100.0, ml_score)), 1)

    # Merge explanations: collect top features from all models
    merged_explanations = _merge_explanations(explanations)

    return {
        "ml_score": ml_score,
        "components": {k: round(v, 1) for k, v in scores.items()},
        "versions": versions,
        "explanations": merged_explanations,
        "model_agreement": agreement,
        "models_available": len(scores),
    }


def _merge_explanations(explanations: dict) -> list:
    """
    Merge explanations from all models into a unified list of top suspicious features.
    Features mentioned by multiple models get higher priority.
    """
    feature_mentions = {}  # feature_name -> {"count": int, "details": list}

    for model_name, expl in explanations.items():
        if not expl:
            continue

        features_list = []

        # IsolationForest SHAP explanations
        if "top_features" in expl:
            for f in expl["top_features"]:
                features_list.append(f.get("feature", ""))

        # Autoencoder reconstruction errors
        if "top_anomalous_features" in expl:
            for f in expl["top_anomalous_features"]:
                features_list.append(f.get("feature", ""))

        # HDBSCAN deviations
        if "top_deviating_features" in expl:
            for f in expl["top_deviating_features"]:
                features_list.append(f.get("feature", ""))

        for feat_name in features_list:
            if not feat_name:
                continue
            if feat_name not in feature_mentions:
                feature_mentions[feat_name] = {"count": 0, "models": []}
            feature_mentions[feat_name]["count"] += 1
            feature_mentions[feat_name]["models"].append(model_name)

    # Sort by mention count (multi-model agreement first), then alphabetically
    sorted_features = sorted(
        feature_mentions.items(),
        key=lambda x: (-x[1]["count"], x[0]),
    )

    return [
        {
            "feature": feat_name,
            "flagged_by": info["models"],
            "agreement_count": info["count"],
        }
        for feat_name, info in sorted_features[:7]
    ]
