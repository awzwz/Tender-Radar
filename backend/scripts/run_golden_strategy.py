import asyncio
import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.etl.backfill import BackfillETL
from app.features.engine import FeatureEngine
# from app.ml.train import main as train_main # Assuming it has a main() or similar

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
    
    # 3. Model Training
    logger.info("Step 3: Training the ML model on the enriched dataset")
    # We'll run the training script as a separate process to be safe
    import subprocess
    train_proc = subprocess.run(["python", "app/ml/train.py"], capture_output=True, text=True)
    if train_proc.returncode == 0:
        logger.info("Training successful!")
        logger.info(train_proc.stdout)
    else:
        logger.error(f"Training failed: {train_proc.stderr}")

    logger.info("=== GOLDEN STANDARD STRATEGY COMPLETED ===")

if __name__ == "__main__":
    asyncio.run(run_strategy())
