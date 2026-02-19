from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from app.core.database import get_db
from app.core.security import require_analyst, get_current_user
from app.models.procurement import AnalystNote
from app.models.user import User

router = APIRouter()


class NoteCreate(BaseModel):
    entity_type: str  # lot/tender/supplier/customer
    entity_id: str
    note_text: str
    label: Optional[str] = None  # SUSPICIOUS/FALSE_POSITIVE/NEEDS_REVIEW/VERIFIED


@router.post("")
async def create_note(
    req: NoteCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_analyst),
):
    note = AnalystNote(
        entity_type=req.entity_type,
        entity_id=req.entity_id,
        note_text=req.note_text,
        label=req.label,
        created_by=current_user.id,
        created_at=datetime.utcnow(),
    )
    db.add(note)
    await db.commit()
    await db.refresh(note)
    return {"id": note.id, "message": "Note created"}


@router.get("")
async def get_notes(
    entity_type: str = Query(...),
    entity_id: str = Query(...),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_analyst),
):
    result = await db.execute(
        select(AnalystNote)
        .where(AnalystNote.entity_type == entity_type, AnalystNote.entity_id == entity_id)
        .order_by(AnalystNote.created_at.desc())
    )
    notes = result.scalars().all()
    return [
        {
            "id": n.id,
            "note_text": n.note_text,
            "label": n.label,
            "created_by": n.created_by,
            "created_at": str(n.created_at),
        }
        for n in notes
    ]
