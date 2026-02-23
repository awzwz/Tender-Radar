"""
Tests for upsert logic on risk_scores and risk_flags tables.
Validates UNIQUE constraints prevent duplicates.
"""
import pytest
import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select, func

from app.core.database import Base
from app.models.procurement import RiskScore, RiskFlag


import pytest_asyncio
from sqlalchemy.pool import NullPool

@pytest_asyncio.fixture
async def engine():
    """Create a test database engine using the real DB URL."""
    from app.core.config import settings
    engine = create_async_engine(settings.database_url, echo=False, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session(engine):
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as db:
        yield db


class TestRiskScoreUpsert:
    """Test that risk_scores upsert works correctly with UNIQUE constraint."""

    @pytest.mark.asyncio
    async def test_insert_then_update(self, session: AsyncSession):
        now = datetime.utcnow()
        entity_id = "test_lot_upsert_1"

        # First insert
        stmt = pg_insert(RiskScore).values({
            "entity_type": "lot",
            "entity_id": entity_id,
            "score": 45.0,
            "score_rules": 50.0,
            "score_ml": 0.35,
            "score_final": 45.0,
            "level": "MEDIUM",
            "top_reasons_jsonb": [{"code": "FEW_BIDS"}],
            "computed_at": now,
        })
        stmt = stmt.on_conflict_do_update(
            constraint="uq_risk_scores_entity",
            set_={"score": 45.0, "score_rules": 50.0, "score_ml": 0.35,
                   "score_final": 45.0, "level": "MEDIUM", "computed_at": now},
        )
        await session.execute(stmt)
        await session.commit()

        # Second insert with different score (should UPDATE, not INSERT)
        stmt2 = pg_insert(RiskScore).values({
            "entity_type": "lot",
            "entity_id": entity_id,
            "score": 80.0,
            "score_rules": 75.0,
            "score_ml": 0.85,
            "score_final": 80.0,
            "level": "HIGH",
            "top_reasons_jsonb": [{"code": "RNU_FLAG"}],
            "computed_at": now,
        })
        stmt2 = stmt2.on_conflict_do_update(
            constraint="uq_risk_scores_entity",
            set_={"score": 80.0, "score_rules": 75.0, "score_ml": 0.85,
                   "score_final": 80.0, "level": "HIGH", "computed_at": now},
        )
        await session.execute(stmt2)
        await session.commit()

        # Verify only 1 row exists
        count_result = await session.execute(
            select(func.count(RiskScore.id)).where(
                RiskScore.entity_type == "lot",
                RiskScore.entity_id == entity_id,
            )
        )
        count = count_result.scalar()
        assert count == 1, f"Expected 1 row, got {count}"

        # Verify it has the updated score
        row_result = await session.execute(
            select(RiskScore).where(
                RiskScore.entity_type == "lot",
                RiskScore.entity_id == entity_id,
            )
        )
        row = row_result.scalar_one()
        assert row.score_final == 80.0
        assert row.level == "HIGH"


class TestRiskFlagUpsert:
    """Test that risk_flags upsert works correctly with UNIQUE constraint."""

    @pytest.mark.asyncio
    async def test_insert_then_update(self, session: AsyncSession):
        now = datetime.utcnow()
        entity_id = "test_lot_flag_1"

        # First insert
        stmt = pg_insert(RiskFlag).values({
            "entity_type": "lot",
            "entity_id": entity_id,
            "indicator_code": "FEW_BIDS",
            "flag_bool": True,
            "value_numeric": 2.0,
            "evidence_jsonb": {"bid_count": 2},
            "computed_at": now,
        })
        stmt = stmt.on_conflict_do_update(
            constraint="uq_risk_flags_entity_indicator",
            set_={"flag_bool": True, "value_numeric": 2.0, "computed_at": now},
        )
        await session.execute(stmt)
        await session.commit()

        # Update same flag with different value
        stmt2 = pg_insert(RiskFlag).values({
            "entity_type": "lot",
            "entity_id": entity_id,
            "indicator_code": "FEW_BIDS",
            "flag_bool": False,
            "value_numeric": 5.0,
            "evidence_jsonb": {"bid_count": 5},
            "computed_at": now,
        })
        stmt2 = stmt2.on_conflict_do_update(
            constraint="uq_risk_flags_entity_indicator",
            set_={"flag_bool": False, "value_numeric": 5.0, "computed_at": now},
        )
        await session.execute(stmt2)
        await session.commit()

        # Verify only 1 row
        count_result = await session.execute(
            select(func.count(RiskFlag.id)).where(
                RiskFlag.entity_type == "lot",
                RiskFlag.entity_id == entity_id,
                RiskFlag.indicator_code == "FEW_BIDS",
            )
        )
        count = count_result.scalar()
        assert count == 1, f"Expected 1 row, got {count}"

        # Verify updated
        row_result = await session.execute(
            select(RiskFlag).where(
                RiskFlag.entity_type == "lot",
                RiskFlag.entity_id == entity_id,
                RiskFlag.indicator_code == "FEW_BIDS",
            )
        )
        row = row_result.scalar_one()
        assert row.flag_bool is False
        assert row.value_numeric == 5.0
