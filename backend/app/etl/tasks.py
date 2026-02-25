import asyncio
import logging
from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

logger = logging.getLogger(__name__)

celery_app = Celery(
    "tender_radar",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Almaty",
    enable_utc=True,
    beat_schedule={
        # Run incremental ETL every day at 02:00 Almaty time
        "daily-incremental-etl": {
            "task": "app.etl.tasks.run_incremental",
            "schedule": crontab(hour=2, minute=0),
        },
        # Recompute features every day at 04:00 (after ETL finishes)
        "daily-feature-recompute": {
            "task": "app.etl.tasks.run_feature_recompute",
            "schedule": crontab(hour=4, minute=0),
        },
        "daily-graph-features": {
            "task": "app.etl.tasks.compute_graph_features",
            "schedule": crontab(hour=3, minute=30),
        },
        "daily-weak-labeling": {
            "task": "app.etl.tasks.run_weak_labeling",
            "schedule": crontab(hour=5, minute=0),
        },
        "weekly-train-anomaly": {
            "task": "app.etl.tasks.train_anomaly_model",
            "schedule": crontab(hour=6, minute=0, day_of_week=0),
        },
        "weekly-train-weak-model": {
            "task": "app.etl.tasks.train_weak_model",
            "schedule": crontab(hour=7, minute=0, day_of_week=0),
        },
    },
)


def run_async(coro):
    """Helper to run async code in Celery sync context."""
    from app.core.database import dispose_engine
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(dispose_engine())
        loop.close()


@celery_app.task(name="app.etl.tasks.run_backfill", bind=True, max_retries=2)
def run_backfill(self, date_from: str = None, date_to: str = None, bootstrap_limit: int = 0):
    """Celery task: run full backfill ETL."""
    from app.etl.backfill import BackfillETL
    from app.core.config import settings as s

    date_from = date_from or s.etl_backfill_date_from
    date_to = date_to or s.etl_backfill_date_to
    bootstrap_limit = bootstrap_limit or s.etl_bootstrap_limit

    logger.info(f"Starting backfill ETL: {date_from} → {date_to} (limit={bootstrap_limit})")
    try:
        summary = run_async(BackfillETL(date_from, date_to, bootstrap_limit).run())
        logger.info(f"Backfill complete: {summary}")

        # Auto-trigger risk scoring after successful ETL
        logger.info("Triggering feature recompute after backfill...")
        run_feature_recompute.delay()

        return summary
    except Exception as exc:
        logger.error(f"Backfill failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(name="app.etl.tasks.run_incremental", bind=True, max_retries=3)
def run_incremental(self, date_from: str = None, date_to: str = None):
    """Celery task: run incremental ETL from journal."""
    from app.etl.incremental import IncrementalETL

    logger.info(f"Starting incremental ETL: {date_from} → {date_to}")
    try:
        summary = run_async(IncrementalETL(date_from, date_to).run())
        logger.info(f"Incremental ETL complete: {summary}")

        # Auto-trigger risk scoring after successful incremental ETL
        logger.info("Triggering feature recompute after incremental ETL...")
        run_feature_recompute.delay()

        return summary
    except Exception as exc:
        logger.error(f"Incremental ETL failed: {exc}")
        raise self.retry(exc=exc, countdown=120)


@celery_app.task(name="app.etl.tasks.run_feature_recompute")
def run_feature_recompute(entity_ids: list = None, limit: int = None):
    """Celery task: recompute risk features and scores. Optional limit to process only N lots."""
    from app.features.engine import FeatureEngine
    from app.core.database import AsyncSessionLocal
    from sqlalchemy import select
    from app.models.procurement import Lot

    async def _run():
        if limit is not None and limit > 0 and not entity_ids:
            async with AsyncSessionLocal() as db:
                r = await db.execute(
                    select(Lot.id).where(Lot.is_deleted == False).limit(limit)
                )
                ids = [row[0] for row in r.all()]
            return await FeatureEngine().run(entity_ids=ids)
        return await FeatureEngine().run(entity_ids=entity_ids)

    logger.info("Starting feature recompute for: %s", entity_ids or ("limit=%s" % limit) or "all")
    try:
        summary = run_async(_run())
        logger.info("Feature recompute complete: %s", summary)
        return summary
    except Exception as exc:
        logger.error("Feature recompute failed: %s", exc)
        raise


@celery_app.task(name="app.etl.tasks.run_score_lot")
def run_score_lot(lot_id: int):
    """Celery task: compute risk score for a single lot (on-demand)."""
    from app.features.engine import FeatureEngine

    logger.info(f"On-demand scoring for lot {lot_id}")
    try:
        result = run_async(FeatureEngine().compute_lot_score(lot_id))
        logger.info(f"Lot {lot_id} scored: {result}")
        return result
    except Exception as exc:
        logger.error(f"Scoring lot {lot_id} failed: {exc}")
        raise


@celery_app.task(name="app.etl.tasks.run_ml_train")
def run_ml_train():
    """Celery task: train ML model on computed features."""
    from app.ml.train import train_model

    logger.info("Starting ML model training...")
    try:
        metrics = run_async(train_model())
        logger.info(f"ML training complete: {metrics}")
        return metrics
    except Exception as exc:
        logger.error(f"ML training failed: {exc}")
        raise


@celery_app.task(name="app.etl.tasks.run_q1_2024_etl", bind=True)
def run_q1_2024_etl_task(self, step: str = "all"):
    """Celery task: Q1 2024 ETL (lots, contracts, trd_app, subject) with checkpoints. No retry on failure."""
    from app.etl.etl_q1_2024 import run_q1_2024_etl

    logger.info("Starting Q1 2024 ETL step=%s...", step)
    try:
        summary = run_async(run_q1_2024_etl(step=step))
        logger.info("Q1 2024 ETL complete: %s", summary)
        return summary
    except Exception as e:
        logger.exception("Q1 2024 ETL failed: %s", e)
        raise RuntimeError(f"Q1 2024 ETL failed: {type(e).__name__}: {e}") from None


@celery_app.task(name="app.etl.tasks.train_anomaly_model")
def train_anomaly_model():
    """Celery task: train IsolationForest anomaly model on tender features (unlabeled)."""
    from app.ml.anomaly import train_anomaly_model as _train
    from app.core.database import AsyncSessionLocal
    from app.models.procurement import TenderFeature
    from sqlalchemy import select
    import numpy as np
    from app.ml.train import FEATURE_COLUMNS

    logger.info("Building anomaly dataset...")
    async def _build():
        async with AsyncSessionLocal() as db:
            r = await db.execute(
                select(TenderFeature.entity_id, TenderFeature.features_jsonb).where(
                    TenderFeature.entity_type == "lot"
                )
            )
            rows = r.all()
        if not rows:
            raise ValueError("No tender features; run feature recompute first.")
        X = []
        for row in rows:
            features = row.features_jsonb or {}
            vec = []
            for col in FEATURE_COLUMNS:
                v = features.get(col)
                if v is None:
                    vec.append(0.0)
                elif isinstance(v, bool):
                    vec.append(1.0 if v else 0.0)
                else:
                    try:
                        vec.append(float(v))
                    except (ValueError, TypeError):
                        vec.append(0.0)
            X.append(vec)
        return np.array(X, dtype=np.float64)

    X = run_async(_build())
    logger.info("Training anomaly model on %s samples...", X.shape[0])
    result = _train(X)
    return result


@celery_app.task(name="app.etl.tasks.run_weak_labeling")
def run_weak_labeling(version: str = None):
    """Celery task: run weak labeling pipeline and save to weak_labels."""
    from app.ml.weak_labeling import run_weak_labeling as _run
    logger.info("Starting weak labeling...")
    summary = run_async(_run(version=version))
    logger.info("Weak labeling complete: %s", summary)
    return summary


@celery_app.task(name="app.etl.tasks.train_weak_model")
def train_weak_model():
    """Celery task: train weak model (GBM) on weak labels."""
    from app.ml.weak_model import train_weak_model as _train
    from app.core.database import AsyncSessionLocal
    from app.models.procurement import TenderFeature, WeakLabel
    from sqlalchemy import select
    import numpy as np
    from app.ml.train import FEATURE_COLUMNS

    logger.info("Building weak model dataset...")
    async def _build():
        async with AsyncSessionLocal() as db:
            r = await db.execute(
                select(TenderFeature.entity_id, TenderFeature.features_jsonb).where(
                    TenderFeature.entity_type == "lot"
                )
            )
            tf_rows = {row.entity_id: row.features_jsonb for row in r.all()}
            w = await db.execute(
                select(WeakLabel.entity_id, WeakLabel.weak_proba).where(WeakLabel.entity_type == "lot")
            )
            w_rows = {row.entity_id: row.weak_proba for row in w.all()}
        entity_ids = [eid for eid in tf_rows if eid in w_rows]
        if len(entity_ids) < 50:
            raise ValueError("Insufficient weak labels; run weak labeling first.")
        X = []
        probs = []
        for eid in entity_ids:
            features = tf_rows.get(eid) or {}
            vec = []
            for col in FEATURE_COLUMNS:
                v = features.get(col)
                if v is None:
                    vec.append(0.0)
                elif isinstance(v, bool):
                    vec.append(1.0 if v else 0.0)
                else:
                    try:
                        vec.append(float(v))
                    except (ValueError, TypeError):
                        vec.append(0.0)
            X.append(vec)
            probs.append(w_rows[eid])
        return np.array(X, dtype=np.float64), np.array(probs, dtype=np.float64)

    X, weak_proba = run_async(_build())
    logger.info("Training weak model on %s samples...", X.shape[0])
    result = _train(X, weak_proba)
    return result


@celery_app.task(name="app.etl.tasks.compute_graph_features")
def compute_graph_features(batch_size: int = 5000):
    """Celery task: compute graph features (co-bidding, rotation) and upsert graph_features."""
    from app.features.graph_features import compute_graph_features as _compute
    logger.info("Computing graph features...")
    summary = run_async(_compute(batch_size=batch_size))
    logger.info("Graph features complete: %s", summary)
    return summary
