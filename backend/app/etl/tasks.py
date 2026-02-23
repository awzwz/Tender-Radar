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
def run_feature_recompute(entity_ids: list = None):
    """Celery task: recompute risk features and scores."""
    from app.features.engine import FeatureEngine

    logger.info(f"Starting feature recompute for: {entity_ids or 'all'}")
    try:
        summary = run_async(FeatureEngine().run(entity_ids=entity_ids))
        logger.info(f"Feature recompute complete: {summary}")
        return summary
    except Exception as exc:
        logger.error(f"Feature recompute failed: {exc}")
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
