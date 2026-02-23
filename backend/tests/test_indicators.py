"""
Unit tests for risk indicator computations.
Uses mocked DB sessions to test indicator logic without a real database.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta


# Helpers to mock DB results
def _mock_row(**kwargs):
    """Create a mock object with attributes."""
    m = MagicMock()
    for k, v in kwargs.items():
        setattr(m, k, v)
    return m


def _mock_result(first_val=None, scalar_val=None, all_val=None):
    """Create a mock for db.execute() result."""
    result = AsyncMock()
    if first_val is not None:
        result.first = MagicMock(return_value=first_val)
    if scalar_val is not None:
        result.scalar = MagicMock(return_value=scalar_val)
        result.scalar_one_or_none = MagicMock(return_value=scalar_val)
    if all_val is not None:
        result.all = MagicMock(return_value=all_val)
    return result


@pytest.mark.asyncio
async def test_short_deadline_flag():
    """Short deadline: < 3 days should trigger."""
    from app.features.indicators import check_short_deadline

    db = AsyncMock()
    now = datetime.utcnow()
    # 2-day window
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(start_date=now, end_date=now + timedelta(days=2))
    ))
    result = await check_short_deadline(db, 123)
    assert result["flag"] is True
    assert result["value"] == 2.0


@pytest.mark.asyncio
async def test_short_deadline_no_flag():
    """Normal deadline: >= 3 days should not trigger."""
    from app.features.indicators import check_short_deadline

    db = AsyncMock()
    now = datetime.utcnow()
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(start_date=now, end_date=now + timedelta(days=10))
    ))
    result = await check_short_deadline(db, 123)
    assert result["flag"] is False
    assert result["value"] == 10.0


@pytest.mark.asyncio
async def test_few_bids_flag():
    """1-2 bids should trigger FEW_BIDS."""
    from app.features.indicators import check_few_bids

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_mock_result(scalar_val=2))
    result = await check_few_bids(db, 456)
    assert result["flag"] is True
    assert result["value"] == 2.0


@pytest.mark.asyncio
async def test_few_bids_no_flag():
    """5+ bids should not trigger FEW_BIDS."""
    from app.features.indicators import check_few_bids

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_mock_result(scalar_val=5))
    result = await check_few_bids(db, 456)
    assert result["flag"] is False


@pytest.mark.asyncio
async def test_night_publish():
    """Publishing at 23:00 should trigger NIGHT_OR_WEEKEND_PUBLISH."""
    from app.features.indicators import check_night_or_weekend_publish

    db = AsyncMock()
    night_date = datetime(2025, 1, 15, 23, 0)  # Wednesday 23:00
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(publish_date=night_date)
    ))
    result = await check_night_or_weekend_publish(db, 789)
    assert result["flag"] is True
    assert result["evidence"]["is_night"] is True


@pytest.mark.asyncio
async def test_weekend_publish():
    """Publishing on Saturday should trigger NIGHT_OR_WEEKEND_PUBLISH."""
    from app.features.indicators import check_night_or_weekend_publish

    db = AsyncMock()
    saturday = datetime(2025, 1, 18, 10, 0)  # Saturday 10:00
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(publish_date=saturday)
    ))
    result = await check_night_or_weekend_publish(db, 789)
    assert result["flag"] is True
    assert result["evidence"]["is_weekend"] is True


@pytest.mark.asyncio
async def test_repeat_tender_flag():
    """Tender with parent_id should trigger REPEAT_TENDER."""
    from app.features.indicators import check_repeat_tender

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(parent_id=100, repeat_start_date=None, repeat_end_date=None)
    ))
    result = await check_repeat_tender(db, 123)
    assert result["flag"] is True


@pytest.mark.asyncio
async def test_repeat_tender_no_flag():
    """Tender without parent_id should not trigger."""
    from app.features.indicators import check_repeat_tender

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_mock_result(
        first_val=_mock_row(parent_id=None, repeat_start_date=None, repeat_end_date=None)
    ))
    result = await check_repeat_tender(db, 123)
    assert result["flag"] is False
