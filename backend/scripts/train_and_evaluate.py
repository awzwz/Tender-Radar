#!/usr/bin/env python3
"""
ML Training & Evaluation Script v2
====================================
Trains 3-model ensemble: IsolationForest + Autoencoder + HDBSCAN.
All models use 30 numerical features only (no binary indicators).

Usage (from backend/):
    python -m scripts.train_and_evaluate [--limit N]

Output:
    - Per-model metrics (anomaly rate, reconstruction error, clusters)
    - Ensemble agreement analysis
    - Score distribution
    - Saved model artifacts in artifacts/
"""
import sys
import os
import asyncio
import argparse
import logging

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def print_separator(title: str = ""):
    print("\n" + "=" * 70)
    if title:
        print(f"  {title}")
        print("=" * 70)


def print_anomaly_metrics(metrics: dict):
    print(f"\n  Samples:           {metrics['n_samples']}")
    print(f"  Features:          {metrics['n_features']} (numerical only)")
    print(f"  Anomalies found:   {metrics['n_anomalies_detected']} ({metrics['anomaly_rate']:.1f}%)")
    print(f"  Contamination:     {metrics['contamination']}")
    p = metrics.get("percentiles", {})
    print(f"  Score percentiles: p50={p.get('50', 0):.3f}  p90={p.get('90', 0):.3f}  p99={p.get('99', 0):.3f}")
    sev = metrics.get("severity_thresholds", {})
    if sev:
        print(f"  Severity: elevated={sev.get('elevated', 0):.4f}  high={sev.get('high', 0):.4f}  critical={sev.get('critical', 0):.4f}")
    if "global_feature_importance" in metrics:
        print("\n  Top SHAP Features:")
        for i, f in enumerate(metrics["global_feature_importance"][:10], 1):
            bar = "#" * max(1, int(f["importance"] * 100))
            print(f"    {i:2d}. {f['feature']:40s} {f['importance']:.6f}  {bar}")


def print_autoencoder_metrics(metrics: dict):
    print(f"\n  Samples:           {metrics['n_samples']}")
    print(f"  Architecture:      {metrics['architecture']}")
    print(f"  Epochs trained:    {metrics['epochs_trained']}")
    print(f"  Best val loss:     {metrics['best_val_loss']:.6f}")
    print(f"  Final train loss:  {metrics['final_train_loss']:.6f}")
    p = metrics.get("percentiles", {})
    print(f"  Recon error p50={p.get('50', 0):.6f}  p90={p.get('90', 0):.6f}  p99={p.get('99', 0):.6f}")
    if "hardest_features" in metrics:
        print("\n  Hardest-to-Reconstruct Features:")
        for i, f in enumerate(metrics["hardest_features"][:10], 1):
            print(f"    {i:2d}. {f['feature']:40s} MSE={f['mean_mse']:.6f}")


def print_hdbscan_metrics(metrics: dict):
    print(f"\n  Samples:           {metrics['n_samples']}")
    print(f"  Clusters found:    {metrics['n_clusters']}")
    print(f"  Noise points:      {metrics['n_noise_points']} ({metrics['noise_rate']:.1f}%)")
    print(f"  Min cluster size:  {metrics['min_cluster_size']}")
    p = metrics.get("percentiles", {})
    print(f"  Outlier score p50={p.get('50', 0):.4f}  p90={p.get('90', 0):.4f}  p99={p.get('99', 0):.4f}")
    for cl_id, prof in list(metrics.get("cluster_profiles", {}).items())[:5]:
        feats = ", ".join(f["feature"] for f in prof.get("top_features", [])[:3])
        print(f"    Cluster {cl_id} (n={prof['size']}): {feats}")


async def main(limit: int = None):
    from app.ml.feature_engineering import build_dataset, ML_FEATURES

    # Step 1: Build dataset (numerical features only)
    print_separator("STEP 1: Building Feature Dataset (30 numerical features)")
    entity_ids, X = await build_dataset(limit=limit, ml_only=True)
    print(f"  Dataset: {X.shape[0]} samples x {X.shape[1]} features")
    print(f"  Features: {len(ML_FEATURES)} numerical (no binary indicators)")

    if X.shape[0] < 100:
        print(f"\n  ERROR: Only {X.shape[0]} samples. Need at least 100. Run ETL + feature recompute first.")
        return

    # Feature statistics
    print("\n  Feature Statistics:")
    for i, name in enumerate(ML_FEATURES):
        col = X[:, i]
        nz = np.count_nonzero(col)
        if nz > 0:
            print(f"    {name:40s}  non-zero={nz:>6}  mean={np.mean(col):>10.4f}  std={np.std(col):>10.4f}  max={np.max(col):>10.2f}")

    # Step 2: Train IsolationForest
    print_separator("STEP 2: Training IsolationForest (anomaly detection)")
    try:
        from app.ml.anomaly import train_anomaly_model
        iso_result = train_anomaly_model(X)
        print_anomaly_metrics(iso_result["metrics"])
    except Exception as e:
        print(f"  IsolationForest training failed: {e}")
        import traceback
        traceback.print_exc()
        iso_result = None

    # Step 3: Train Autoencoder
    print_separator("STEP 3: Training Autoencoder (reconstruction-based)")
    try:
        from app.ml.autoencoder import train_autoencoder
        ae_result = train_autoencoder(X)
        print_autoencoder_metrics(ae_result["metrics"])
    except Exception as e:
        print(f"  Autoencoder training failed: {e}")
        import traceback
        traceback.print_exc()
        ae_result = None

    # Step 4: Train HDBSCAN
    print_separator("STEP 4: Training HDBSCAN (density-based clustering)")
    try:
        from app.ml.clustering import train_hdbscan
        hdb_result = train_hdbscan(X)
        print_hdbscan_metrics(hdb_result["metrics"])
    except Exception as e:
        print(f"  HDBSCAN training failed: {e}")
        import traceback
        traceback.print_exc()
        hdb_result = None

    # Step 5: Ensemble evaluation
    print_separator("STEP 5: Ensemble Agreement Analysis")
    _evaluate_ensemble(X, entity_ids)

    # Summary
    successful = sum(1 for r in [iso_result, ae_result, hdb_result] if r is not None)
    print_separator("TRAINING COMPLETE")
    print(f"  Models trained:        {successful}/3")
    print(f"  Total samples:         {X.shape[0]}")
    print(f"  Feature dimensions:    {X.shape[1]} (numerical only)")
    print(f"  Artifacts saved to:    artifacts/")
    if successful < 3:
        print(f"\n  WARNING: {3 - successful} model(s) failed. Check logs above.")


def _evaluate_ensemble(X: np.ndarray, entity_ids: list):
    """Run ensemble on all samples and analyze agreement."""
    from app.ml.feature_engineering import ML_FEATURES, features_dict_to_array

    try:
        from app.ml.anomaly import predict_anomaly
        from app.ml.autoencoder import predict_autoencoder
        from app.ml.clustering import predict_hdbscan
    except Exception as e:
        print(f"  Cannot evaluate ensemble: {e}")
        return

    iso_scores = []
    ae_scores = []
    hdb_scores = []

    # Build feature dicts and predict
    for i in range(min(len(entity_ids), X.shape[0])):
        fv = {ML_FEATURES[j]: float(X[i, j]) for j in range(X.shape[1])}

        try:
            r = predict_anomaly(fv)
            iso_scores.append(r[0] if r else np.nan)
        except Exception:
            iso_scores.append(np.nan)

        try:
            r = predict_autoencoder(fv)
            ae_scores.append(r[0] if r else np.nan)
        except Exception:
            ae_scores.append(np.nan)

        try:
            r = predict_hdbscan(fv)
            hdb_scores.append(r[0] if r else np.nan)
        except Exception:
            hdb_scores.append(np.nan)

    iso_arr = np.array(iso_scores)
    ae_arr = np.array(ae_scores)
    hdb_arr = np.array(hdb_scores)

    # Score distribution
    for name, arr in [("IsolationForest", iso_arr), ("Autoencoder", ae_arr), ("HDBSCAN", hdb_arr)]:
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0:
            print(f"\n  {name}: no valid predictions")
            continue
        print(f"\n  {name} score distribution:")
        print(f"    mean={np.mean(valid):.1f}  std={np.std(valid):.1f}  "
              f"p25={np.percentile(valid, 25):.1f}  p50={np.percentile(valid, 50):.1f}  "
              f"p75={np.percentile(valid, 75):.1f}  p95={np.percentile(valid, 95):.1f}")
        high = np.sum(valid > 60)
        print(f"    High risk (>60): {high} ({high / len(valid) * 100:.1f}%)")

    # Agreement analysis
    all_valid = ~(np.isnan(iso_arr) | np.isnan(ae_arr) | np.isnan(hdb_arr))
    n_valid = np.sum(all_valid)
    if n_valid > 0:
        agree_high = np.sum(
            (iso_arr[all_valid] > 60) & (ae_arr[all_valid] > 60) & (hdb_arr[all_valid] > 60)
        )
        agree_2_of_3 = np.sum(
            ((iso_arr[all_valid] > 60).astype(int) + (ae_arr[all_valid] > 60).astype(int) + (hdb_arr[all_valid] > 60).astype(int)) >= 2
        )
        print(f"\n  Agreement (n={n_valid} samples with all 3 scores):")
        print(f"    All 3 models agree (>60): {agree_high} ({agree_high / n_valid * 100:.1f}%)")
        print(f"    2+ models agree (>60):    {agree_2_of_3} ({agree_2_of_3 / n_valid * 100:.1f}%)")

        # Top 10 most suspicious by ensemble
        ensemble_scores = (
            0.4 * iso_arr[all_valid] + 0.35 * ae_arr[all_valid] + 0.25 * hdb_arr[all_valid]
        )
        top10_idx = np.argsort(-ensemble_scores)[:10]
        valid_ids = np.array(entity_ids)[all_valid]
        print(f"\n  Top 10 most suspicious lots (by ensemble score):")
        for rank, idx in enumerate(top10_idx, 1):
            print(f"    {rank:2d}. lot_id={valid_ids[idx]:>10s}  "
                  f"ensemble={ensemble_scores[idx]:.1f}  "
                  f"iso={iso_arr[all_valid][idx]:.1f}  "
                  f"ae={ae_arr[all_valid][idx]:.1f}  "
                  f"hdb={hdb_arr[all_valid][idx]:.1f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train & evaluate ML ensemble (v2)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of lots to process")
    args = parser.parse_args()

    asyncio.run(main(limit=args.limit))
