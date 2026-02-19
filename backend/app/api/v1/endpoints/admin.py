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
    _=Depends(require_admin),
):
    """Trigger risk feature recomputation."""
    from app.etl.tasks import run_feature_recompute
    task = run_feature_recompute.delay(entity_ids)
    return {"message": "Feature recompute started", "task_id": task.id}
