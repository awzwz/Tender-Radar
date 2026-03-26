"""
Autoencoder anomaly detector: learns to reconstruct normal patterns.
High reconstruction error = anomalous tender.
Trained on 30 numerical features only (no binary indicators).
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
MODEL_NAME = "autoencoder_v1"
DEFAULT_ARTIFACT_DIR = "artifacts"

# Architecture: 30 -> 20 -> 10 -> 5 -> 10 -> 20 -> 30
HIDDEN_DIMS = [20, 10, 5]


def _get_artifact_dir(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", DEFAULT_ARTIFACT_DIR)
    return os.path.join(base, MODEL_NAME, "v1")


def _build_model(input_dim: int):
    """Build autoencoder model using PyTorch."""
    import torch
    import torch.nn as nn

    class Autoencoder(nn.Module):
        def __init__(self, input_dim, hidden_dims):
            super().__init__()
            # Encoder
            encoder_layers = []
            prev_dim = input_dim
            for h in hidden_dims:
                encoder_layers.append(nn.Linear(prev_dim, h))
                encoder_layers.append(nn.ReLU())
                encoder_layers.append(nn.BatchNorm1d(h))
                prev_dim = h
            self.encoder = nn.Sequential(*encoder_layers)

            # Decoder (mirror)
            decoder_layers = []
            for h in reversed(hidden_dims[:-1]):
                decoder_layers.append(nn.Linear(prev_dim, h))
                decoder_layers.append(nn.ReLU())
                decoder_layers.append(nn.BatchNorm1d(h))
                prev_dim = h
            decoder_layers.append(nn.Linear(prev_dim, input_dim))
            self.decoder = nn.Sequential(*decoder_layers)

        def forward(self, x):
            z = self.encoder(x)
            return self.decoder(z)

    return Autoencoder(input_dim, HIDDEN_DIMS)


def train_autoencoder(
    X: np.ndarray,
    artifact_dir: Optional[str] = None,
    epochs: int = 80,
    batch_size: int = 256,
    learning_rate: float = 1e-3,
    random_state: int = 42,
) -> dict:
    """
    Train autoencoder on 30 numerical features.
    Returns metrics dict with reconstruction error distribution.
    """
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset

    out_dir = _get_artifact_dir(artifact_dir)
    os.makedirs(out_dir, exist_ok=True)

    if X.shape[1] != len(FEATURE_COLUMNS):
        raise ValueError(f"Expected {len(FEATURE_COLUMNS)} features, got {X.shape[1]}")

    torch.manual_seed(random_state)
    np.random.seed(random_state)

    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Split for validation (90/10)
    n_val = max(1, int(0.1 * len(X_scaled)))
    idx = np.random.permutation(len(X_scaled))
    X_train = X_scaled[idx[n_val:]]
    X_val = X_scaled[idx[:n_val]]

    train_tensor = torch.FloatTensor(X_train)
    val_tensor = torch.FloatTensor(X_val)
    train_loader = DataLoader(TensorDataset(train_tensor), batch_size=batch_size, shuffle=True)

    model = _build_model(len(FEATURE_COLUMNS))
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)
    criterion = nn.MSELoss(reduction="mean")

    best_val_loss = float("inf")
    patience_counter = 0
    train_losses = []

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        for (batch,) in train_loader:
            optimizer.zero_grad()
            output = model(batch)
            loss = criterion(output, batch)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * batch.size(0)
        epoch_loss /= len(X_train)
        train_losses.append(epoch_loss)

        # Validation
        model.eval()
        with torch.no_grad():
            val_output = model(val_tensor)
            val_loss = criterion(val_output, val_tensor).item()

        scheduler.step(val_loss)

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), os.path.join(out_dir, "model.pt"))
        else:
            patience_counter += 1
            if patience_counter >= 15:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break

        if (epoch + 1) % 10 == 0:
            logger.info("Epoch %d/%d — train_loss=%.6f, val_loss=%.6f", epoch + 1, epochs, epoch_loss, val_loss)

    # Load best model
    model.load_state_dict(torch.load(os.path.join(out_dir, "model.pt"), weights_only=True))
    model.eval()

    # Compute reconstruction errors for all data (for calibration)
    all_tensor = torch.FloatTensor(X_scaled)
    with torch.no_grad():
        reconstructed = model(all_tensor).numpy()

    per_sample_mse = np.mean((X_scaled - reconstructed) ** 2, axis=1)
    per_feature_mse = np.mean((X_scaled - reconstructed) ** 2, axis=0)

    # Percentiles for score calibration
    percentiles = {}
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        percentiles[str(p)] = float(np.percentile(per_sample_mse, p))
    percentiles["min"] = float(np.min(per_sample_mse))
    percentiles["max"] = float(np.max(per_sample_mse))

    # Feature reconstruction difficulty (which features are hardest to reconstruct)
    top_hard_features = np.argsort(-per_feature_mse)[:10]

    metrics = {
        "model_name": MODEL_NAME,
        "version": "v1",
        "n_samples": int(X.shape[0]),
        "n_features": len(FEATURE_COLUMNS),
        "architecture": f"{len(FEATURE_COLUMNS)}-{'-'.join(map(str, HIDDEN_DIMS))}-{'-'.join(map(str, reversed(HIDDEN_DIMS[:-1])))}-{len(FEATURE_COLUMNS)}",
        "epochs_trained": len(train_losses),
        "best_val_loss": round(best_val_loss, 6),
        "final_train_loss": round(train_losses[-1], 6),
        "feature_columns": FEATURE_COLUMNS,
        "percentiles": percentiles,
        "hardest_features": [
            {"feature": FEATURE_COLUMNS[i], "mean_mse": round(float(per_feature_mse[i]), 6)}
            for i in top_hard_features
        ],
    }

    joblib.dump(scaler, os.path.join(out_dir, "scaler.joblib"))
    with open(os.path.join(out_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    with open(os.path.join(out_dir, "feature_schema.json"), "w") as f:
        json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    logger.info("=" * 60)
    logger.info("AUTOENCODER TRAINING RESULTS (v1)")
    logger.info("=" * 60)
    logger.info("Samples: %d, Features: %d", X.shape[0], X.shape[1])
    logger.info("Architecture: %s", metrics["architecture"])
    logger.info("Epochs: %d, Best val loss: %.6f", len(train_losses), best_val_loss)
    logger.info("Reconstruction error p50=%.6f, p95=%.6f, p99=%.6f",
                percentiles["50"], percentiles["95"], percentiles["99"])
    logger.info("Hardest features: %s", [FEATURE_COLUMNS[i] for i in top_hard_features[:3]])
    logger.info("Model saved to %s", out_dir)
    logger.info("=" * 60)

    return {"path": out_dir, "metrics": metrics}


_ae_predictor_cache = None


def get_autoencoder_predictor(artifact_dir: Optional[str] = None, reload: bool = False):
    """Load autoencoder; return (model, scaler, percentiles, version) or None."""
    global _ae_predictor_cache
    if _ae_predictor_cache is not None and not reload:
        return _ae_predictor_cache

    out_dir = _get_artifact_dir(artifact_dir)
    model_path = os.path.join(out_dir, "model.pt")
    scaler_path = os.path.join(out_dir, "scaler.joblib")
    metrics_path = os.path.join(out_dir, "metrics.json")

    if not os.path.exists(model_path):
        return None

    try:
        import torch

        model = _build_model(len(FEATURE_COLUMNS))
        model.load_state_dict(torch.load(model_path, weights_only=True))
        model.eval()

        scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None
        with open(metrics_path) as f:
            metrics = json.load(f)
        percentiles = metrics.get("percentiles", {})

        _ae_predictor_cache = (model, scaler, percentiles, MODEL_NAME)
        return _ae_predictor_cache
    except Exception as e:
        logger.warning("Failed to load autoencoder: %s", e)
        return None


def predict_autoencoder(
    feature_vector: dict,
    artifact_dir: Optional[str] = None,
) -> Optional[tuple[float, str, dict]]:
    """
    Returns (reconstruction_score_0_100, model_version, explanation) or None.
    explanation includes per-feature reconstruction errors.
    """
    packed = get_autoencoder_predictor(artifact_dir)
    if packed is None:
        return None

    import torch

    model, scaler, percentiles, version = packed
    X = features_dict_to_array(feature_vector, columns=FEATURE_COLUMNS).reshape(1, -1)

    if scaler is not None:
        X = scaler.transform(X)

    X_tensor = torch.FloatTensor(X)
    with torch.no_grad():
        reconstructed = model(X_tensor).numpy()

    # Per-sample MSE
    sample_mse = float(np.mean((X - reconstructed) ** 2))

    # Per-feature error (for explanation)
    per_feature_error = (X.flatten() - reconstructed.flatten()) ** 2
    top_idx = np.argsort(-per_feature_error)[:5]

    # Score: map MSE to 0-100 using percentiles
    p5 = percentiles.get("5", 0)
    p95 = percentiles.get("95", 1)
    if p95 <= p5:
        score_100 = 50.0
    else:
        t = (sample_mse - p5) / (p95 - p5)
        score_100 = float(np.clip(100.0 * t, 0.0, 100.0))

    explanation = {
        "reconstruction_error": round(sample_mse, 6),
        "top_anomalous_features": [
            {
                "feature": FEATURE_COLUMNS[i],
                "reconstruction_error": round(float(per_feature_error[i]), 6),
                "input_value": round(float(X[0, i]), 4),
                "reconstructed_value": round(float(reconstructed[0, i]), 4),
            }
            for i in top_idx
        ],
    }

    return (round(score_100, 1), version, explanation)
