"""
HDBSCAN clustering anomaly detector.
Points that don't fit into dense clusters are outliers.
Trained on 30 numerical features only.
"""
import json
import logging
import os
from typing import Optional

import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

from app.ml.feature_engineering import ML_FEATURES, features_dict_to_array

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = ML_FEATURES
MODEL_NAME = "hdbscan_v1"
DEFAULT_ARTIFACT_DIR = "artifacts"


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, MODEL_NAME, "v1")


def train_hdbscan(
    X: np.ndarray,
    artifact_dir: Optional[str] = None,
    min_cluster_size: int = 50,
    min_samples: int = 10,
) -> dict:
    """
    Train HDBSCAN on 30 numerical features.
    Outlier scores (0-1) give anomaly probability.
    """
    import hdbscan

    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    if X.shape[1] != len(FEATURE_COLUMNS):
        raise ValueError(f"Expected {len(FEATURE_COLUMNS)} features, got {X.shape[1]}")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
        prediction_data=True,
    )
    clusterer.fit(X_scaled)

    labels = clusterer.labels_
    outlier_scores = clusterer.outlier_scores_
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int(np.sum(labels == -1))

    # Cluster profiles (mean features per cluster)
    cluster_profiles = {}
    for cl in range(n_clusters):
        mask = labels == cl
        cl_mean = np.mean(X_scaled[mask], axis=0)
        cl_size = int(np.sum(mask))
        top_idx = np.argsort(-np.abs(cl_mean))[:5]
        cluster_profiles[str(cl)] = {
            "size": cl_size,
            "top_features": [
                {"feature": FEATURE_COLUMNS[i], "mean_value": round(float(cl_mean[i]), 4)}
                for i in top_idx
            ],
        }

    # Percentiles of outlier scores
    percentiles = {}
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        percentiles[str(p)] = float(np.percentile(outlier_scores, p))
    percentiles["min"] = float(np.min(outlier_scores))
    percentiles["max"] = float(np.max(outlier_scores))

    metrics = {
        "model_name": MODEL_NAME,
        "version": "v1",
        "n_samples": int(X.shape[0]),
        "n_features": len(FEATURE_COLUMNS),
        "n_clusters": n_clusters,
        "n_noise_points": n_noise,
        "noise_rate": round(n_noise / X.shape[0] * 100, 2),
        "feature_columns": FEATURE_COLUMNS,
        "percentiles": percentiles,
        "cluster_profiles": cluster_profiles,
        "min_cluster_size": min_cluster_size,
        "min_samples": min_samples,
    }

    joblib.dump(clusterer, os.path.join(out_dir, "model.joblib"))
    joblib.dump(scaler, os.path.join(out_dir, "scaler.joblib"))
    with open(os.path.join(out_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    with open(os.path.join(out_dir, "feature_schema.json"), "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("=" * 60)
    logger.info("HDBSCAN TRAINING RESULTS (v1)")
    logger.info("=" * 60)
    logger.info("Samples: %d, Features: %d", X.shape[0], X.shape[1])
    logger.info("Clusters found: %d, Noise points: %d (%.1f%%)",
                n_clusters, n_noise, n_noise / X.shape[0] * 100)
    logger.info("Outlier score p50=%.4f, p95=%.4f, p99=%.4f",
                percentiles["50"], percentiles["95"], percentiles["99"])
    for cl_id, prof in list(cluster_profiles.items())[:3]:
        logger.info("Cluster %s (n=%d): %s", cl_id, prof["size"],
                     [f["feature"] for f in prof["top_features"][:3]])
    logger.info("Model saved to %s", out_dir)
    logger.info("=" * 60)

    return {"path": out_dir, "metrics": metrics}


_hdbscan_predictor_cache = None


def get_hdbscan_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load HDBSCAN model; return (clusterer, scaler, percentiles, cluster_profiles, version) or None."""
    global _hdbscan_predictor_cache
    if _hdbscan_predictor_cache is not None and not reload:
        return _hdbscan_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.joblib")
    metrics_path = os.path.join(out_dir, "metrics.json")
    scaler_path = os.path.join(out_dir, "scaler.joblib")

    if not os.path.exists(model_path):
        return None

    try:
        clusterer = joblib.load(model_path)
        scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None
        with open(metrics_path) as f:
            metrics = json.load(f)
        percentiles = metrics.get("percentiles", {})
        cluster_profiles = metrics.get("cluster_profiles", {})

        _hdbscan_predictor_cache = (clusterer, scaler, percentiles, cluster_profiles, MODEL_NAME)
        return _hdbscan_predictor_cache
    except Exception as e:
        logger.warning("Failed to load HDBSCAN model: %s", e)
        return None


def predict_hdbscan(
    feature_vector: dict,
    artifact_dir: Optional[str] = None,
) -> Optional[tuple[float, str, dict]]:
    """
    Returns (outlier_score_0_100, model_version, explanation) or None.
    """
    packed = get_hdbscan_predictor(artifact_dir)
    if packed is None:
        return None

    import hdbscan

    clusterer, scaler, percentiles, cluster_profiles, version = packed
    X = features_dict_to_array(feature_vector, columns=FEATURE_COLUMNS).reshape(1, -1)

    if scaler is not None:
        X = scaler.transform(X)

    # Approximate predict: find nearest cluster and compute membership
    try:
        labels, strengths = hdbscan.approximate_predict(clusterer, X)
        label = int(labels[0])
        strength = float(strengths[0])
    except Exception:
        label = -1
        strength = 0.0

    # Compute distance to cluster centroids for outlier scoring
    if label == -1:
        # Noise point — compute distance to nearest cluster centroid
        outlier_raw = 1.0 - strength
    else:
        outlier_raw = 1.0 - strength

    # Map to 0-100 using percentiles
    p5 = percentiles.get("5", 0)
    p95 = percentiles.get("95", 1)
    if p95 <= p5:
        score_100 = 50.0
    else:
        t = (outlier_raw - p5) / (p95 - p5)
        score_100 = float(np.clip(100.0 * t, 0.0, 100.0))

    # Explanation
    explanation = {
        "cluster_label": label,
        "cluster_membership_strength": round(strength, 4),
        "is_noise": label == -1,
    }

    if label >= 0 and str(label) in cluster_profiles:
        prof = cluster_profiles[str(label)]
        explanation["cluster_size"] = prof.get("size", 0)
        explanation["cluster_characterization"] = prof.get("top_features", [])[:3]
    elif label == -1:
        explanation["reason"] = "Point does not belong to any dense cluster"

    # Feature deviation from nearest cluster
    if hasattr(clusterer, "weighted_cluster_centroid_") or label >= 0:
        try:
            # Compare to overall training mean (scaler mean = 0 after scaling)
            deviations = np.abs(X.flatten())
            top_dev_idx = np.argsort(-deviations)[:5]
            explanation["top_deviating_features"] = [
                {"feature": FEATURE_COLUMNS[i], "deviation": round(float(deviations[i]), 4)}
                for i in top_dev_idx
            ]
        except Exception:
            pass

    return (round(score_100, 1), version, explanation)
