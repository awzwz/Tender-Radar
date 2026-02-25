import asyncio
import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.etl.backfill import BackfillETL
from app.features.engine import FeatureEngine
from app.etl.tasks import compute_graph_features, run_weak_labeling, train_anomaly_model, train_weak_model

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("golden_strategy")

async def run_strategy():
    logger.info("=== STARTING GOLDEN STANDARD STRATEGY ===")
    
    # 1. Targeted ETL (Hunter Mode)
    logger.info("Step 1: Running Hunter Mode ETL (Targeting RNU & Key Industries)")
    etl = BackfillETL(date_from="2024-01-01", date_to="2024-12-31", bootstrap_limit=0) # Targeted for 2024
    etl_summary = await etl.run(hunter_mode=True)
    logger.info(f"ETL Summary: {etl_summary}")
    
    # 2. Feature Engine
    logger.info("Step 2: Computing features for the new data")
    engine = FeatureEngine()
    engine_summary = await engine.run()
    logger.info(f"Feature Engine Summary: {engine_summary}")
    
    # 3. New ML Pipeline (Graph -> Weak -> Anomaly)
    logger.info("Step 3: Running Unsupervised & Weak Supervision Pipeline")
    
    logger.info("  -> Computing Graph Features")
    graph_summary = await compute_graph_features(batch_size=50)
    logger.info(f"  Graph Summary: {graph_summary}")
    
    logger.info("  -> Running Weak Labeling (Rule + Graph Hueristics)")
    weak_label_summary = await run_weak_labeling()
    logger.info(f"  Weak Label Summary: {weak_label_summary}")
    
    logger.info("  -> Training Anomaly Model (Isolation Forest)")
    try:
        anomaly_summary = train_anomaly_model()
        logger.info(f"  Anomaly Model Summary: {anomaly_summary}")
    except Exception as e:
        logger.error(f"  Anomaly Model failed (maybe not enough data yet): {e}")

    logger.info("  -> Training Weak GBM Model")
    try:
        gbm_summary = train_weak_model()
        logger.info(f"  Weak GBM Summary: {gbm_summary}")
    except Exception as e:
        logger.error(f"  Weak GBM failed (maybe not enough data yet): {e}")

    logger.info("=== GOLDEN STANDARD STRATEGY COMPLETED ===")

if __name__ == "__main__":
    asyncio.run(run_strategy())
