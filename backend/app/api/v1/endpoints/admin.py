from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from pydantic import BaseModel
from typing import Optional
from app.core.database import get_db
from app.core.security import require_admin
from app.models.procurement import EtlRun

router = APIRouter()


class BackfillRequest(BaseModel):
    date_from: Optional[str] = None
    date_to: Optional[str] = None


class IncrementalRequest(BaseModel):
    date_from: Optional[str] = None
    date_to: Optional[str] = None


@router.post("/etl/backfill")
async def trigger_backfill(
    req: BackfillRequest,
    background_tasks: BackgroundTasks,
    _=Depends(require_admin),
):
    """Trigger full backfill ETL (runs in background via Celery)."""
    from app.etl.tasks import run_backfill
    task = run_backfill.delay(req.date_from, req.date_to)
    return {"message": "Backfill started", "task_id": task.id}


@router.post("/etl/incremental")
async def trigger_incremental(
    req: IncrementalRequest,
    _=Depends(require_admin),
):
    """Trigger incremental ETL from journal."""
    from app.etl.tasks import run_incremental
    task = run_incremental.delay(req.date_from, req.date_to)
    return {"message": "Incremental ETL started", "task_id": task.id}


@router.post("/etl/q1-2024")
async def trigger_q1_2024_etl(
    step: str = "all",
    _=Depends(require_admin),
):
    """Trigger Q1 2024 ETL (lots, contracts, trd_app, subject) with checkpoints. step: all | lots | contracts | trd_app | subject."""
    from app.etl.tasks import run_q1_2024_etl_task
    task = run_q1_2024_etl_task.delay(step=step)
    return {"message": "Q1 2024 ETL started", "task_id": task.id}


@router.get("/etl/status")
async def get_etl_status(
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Get recent ETL run history."""
    result = await db.execute(
        select(EtlRun).order_by(desc(EtlRun.started_at)).limit(limit)
    )
    runs = result.scalars().all()
    return [
        {
            "id": r.id,
            "run_type": r.run_type,
            "started_at": str(r.started_at),
            "finished_at": str(r.finished_at) if r.finished_at else None,
            "status": r.status,
            "summary": r.summary_jsonb,
        }
        for r in runs
    ]


@router.post("/features/recompute")
async def trigger_feature_recompute(
    entity_ids: Optional[list] = None,
    limit: Optional[int] = None,
    _=Depends(require_admin),
):
    """Trigger risk feature recomputation. Optional limit=N to process only first N lots."""
    from app.etl.tasks import run_feature_recompute
    task = run_feature_recompute.delay(entity_ids, limit)
    return {"message": "Feature recompute started", "task_id": task.id}


@router.post("/ml/train")
async def trigger_ml_train(
    _=Depends(require_admin),
):
    """Trigger ML model training on computed features."""
    from app.etl.tasks import run_ml_train
    task = run_ml_train.delay()
    return {"message": "ML training started", "task_id": task.id}


@router.get("/health/scoring")
async def scoring_health_check(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Smoke test: check if scoring pipeline is working."""
    from sqlalchemy import func
    from app.models.procurement import Lot, RiskScore, RiskFlag

    # Count lots
    lots_count = (await db.execute(
        select(func.count(Lot.id)).where(Lot.is_deleted == False)
    )).scalar() or 0

    # Count scored lots
    scored_count = (await db.execute(
        select(func.count(RiskScore.id)).where(RiskScore.entity_type == "lot")
    )).scalar() or 0

    # Count flags
    flags_count = (await db.execute(
        select(func.count(RiskFlag.id)).where(RiskFlag.entity_type == "lot")
    )).scalar() or 0

    # Count triggered flags
    triggered_count = (await db.execute(
        select(func.count(RiskFlag.id))
        .where(RiskFlag.entity_type == "lot", RiskFlag.flag_bool == True)
    )).scalar() or 0

    # Risk level distribution
    dist_result = await db.execute(
        select(RiskScore.level, func.count(RiskScore.id))
        .where(RiskScore.entity_type == "lot")
        .group_by(RiskScore.level)
    )
    distribution = {r[0]: r[1] for r in dist_result.all()}

    healthy = scored_count > 0 and flags_count > 0

    return {
        "healthy": healthy,
        "lots_total": lots_count,
        "lots_scored": scored_count,
        "lots_unscored": lots_count - scored_count,
        "scoring_coverage_pct": round(scored_count / lots_count * 100, 1) if lots_count > 0 else 0,
        "flags_total": flags_count,
        "flags_triggered": triggered_count,
        "risk_distribution": distribution,
    }


@router.post("/ml/train-anomaly")
async def trigger_train_anomaly(_=Depends(require_admin)):
    """Trigger anomaly model training (IsolationForest)."""
    from app.etl.tasks import train_anomaly_model
    task = train_anomaly_model.delay()
    return {"message": "Anomaly model training started", "task_id": task.id}


@router.post("/ml/weak-labeling")
async def trigger_weak_labeling(_=Depends(require_admin)):
    """Trigger weak labeling pipeline."""
    from app.etl.tasks import run_weak_labeling
    task = run_weak_labeling.delay()
    return {"message": "Weak labeling started", "task_id": task.id}


@router.post("/ml/train-weak-model")
async def trigger_train_weak_model(_=Depends(require_admin)):
    """Trigger weak model (GBM) training."""
    from app.etl.tasks import train_weak_model
    task = train_weak_model.delay()
    return {"message": "Weak model training started", "task_id": task.id}


@router.post("/ml/compute-graph-features")
async def trigger_compute_graph_features(_=Depends(require_admin)):
    """Trigger graph features computation."""
    from app.etl.tasks import compute_graph_features
    task = compute_graph_features.delay()
    return {"message": "Graph features computation started", "task_id": task.id}
