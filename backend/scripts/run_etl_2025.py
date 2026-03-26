import asyncio
import logging
from datetime import datetime
import sys
import os

# Ensure backend root is in PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.etl.client import OWSClient, GQL_TRD_BUY
from app.etl.etl_q1_2024 import (
    _get_cursor, _set_cursor, _chunks,
    _load_lots, _load_contracts, _load_trd_app, _parse_dt
)
from app.models.procurement import TrdBuy
from app.core.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATE_FROM = "2025-01-01"
DATE_TO = "2025-12-31"

CURSOR_TRD_BUY = "y2025_trd_buy"

# Override module-level cursors locally to avoid affecting others
import app.etl.etl_q1_2024 as etl_q1
etl_q1.CURSOR_LOTS = "y2025_lots"
etl_q1.CURSOR_CONTRACTS = "y2025_contracts"
etl_q1.CURSOR_TRD_APP = "y2025_trd_app"


async def _process_trd_buy_batch(batch: list[dict]) -> int:
    rows = []
    for item in batch:
        rows.append({
            "id": item["id"],
            "number_anno": item.get("numberAnno"),
            "name_ru": item.get("nameRu"),
            "name_kz": item.get("nameKz"),
            "ref_trade_methods_id": item.get("refTradeMethodsId"),
            "publish_date": _parse_dt(item.get("publishDate")),
            "start_date": _parse_dt(item.get("startDate")),
            "end_date": _parse_dt(item.get("endDate")),
            "total_sum": float(item.get("totalSum") or 0.0),
            "count_lots": item.get("countLots", 0),
            "ref_buy_status_id": item.get("refBuyStatusId"),
            "org_bin": item.get("orgBin"),
            "system_id": item.get("systemId"),
            "is_light_industry": item.get("isLightIndustry", 0),
            "is_construction_work": item.get("isConstructionWork", 0),
            "discus_start_date": _parse_dt(item.get("discusStartDate")),
            "discus_end_date": _parse_dt(item.get("discusEndDate")),
            "last_update_at": _parse_dt(item.get("indexDate")),
            "is_deleted": False
        })
    if not rows:
        return 0
    
    async with AsyncSessionLocal() as db:
        stmt = pg_insert(TrdBuy).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "ref_buy_status_id": stmt.excluded.ref_buy_status_id,
                "total_sum": stmt.excluded.total_sum,
                "discus_start_date": stmt.excluded.discus_start_date,
                "discus_end_date": stmt.excluded.discus_end_date,
                "last_update_at": stmt.excluded.last_update_at,
            }
        )
        await db.execute(stmt)
        await db.commit()
    return len(rows)


async def _load_trd_buy(client: OWSClient) -> int:
    # 1. Get initial cursor
    initial_cursor_val = await _get_cursor(CURSOR_TRD_BUY)
    initial_after = int(initial_cursor_val) if initial_cursor_val else 0
    
    total = 0
    vars_payload = {
        "filter": {
            "publishDate": [DATE_FROM, DATE_TO]
        }
    }
    
    logger.info(f"Extracting Tenders (TrdBuy) for 2025 (starting from cursor {initial_after})...")
    
    try:
        async for batch, next_id in client.graphql_paginate(
            GQL_TRD_BUY, "TrdBuy", 
            variables=vars_payload, 
            page_size=200,
            initial_after=initial_after
        ):
            upserted = await _process_trd_buy_batch(batch)
            total += upserted
            
            # Save cursor after each successful batch
            await _set_cursor(CURSOR_TRD_BUY, str(next_id))
            
            if total % 1000 == 0:
                logger.info(f"Loaded {total} tenders so far... (Current ID: {next_id})")
    except Exception as e:
        logger.error(f"Error fetching TrdBuy: {e}")
        
    return total

async def _y2025_buy_ids() -> list[int]:
    async with AsyncSessionLocal() as db:
        df = datetime.fromisoformat(DATE_FROM)
        dt = datetime.fromisoformat(DATE_TO)
        r = await db.execute(
            select(TrdBuy.id).where(
                and_(
                    TrdBuy.publish_date >= df,
                    TrdBuy.publish_date <= dt,
                )
            ).order_by(TrdBuy.id)
        )
        ids = [row[0] for row in r.all()]
    return ids

async def run_2025_etl():
    client = OWSClient()
    
    # 1. Load TrdBuy (Tenders)
    logger.info("=== STEP 1: LOAD TRD_BUY ===")
    tenders_count = await _load_trd_buy(client)
    logger.info(f"Total upserted tenders: {tenders_count}")
    
    # Get all IDs
    buy_ids = await _y2025_buy_ids()
    logger.info(f"Found {len(buy_ids)} tenders in DB for 2025.")
    
    if not buy_ids:
        logger.warning("No tenders to process for 2025, exiting.")
        return
        
    # 2. Load Lots
    logger.info("=== STEP 2: LOAD LOTS ===")
    lots_count = await _load_lots(client, buy_ids)
    logger.info(f"Total upserted lots: {lots_count}")
    
    # 3. Load Contracts
    logger.info("=== STEP 3: LOAD CONTRACTS ===")
    contracts_count = await _load_contracts(client, buy_ids)
    logger.info(f"Total upserted contracts: {contracts_count}")
    
    # 4. Load TrdApp
    logger.info("=== STEP 4: LOAD TRD_APP ===")
    trd_app_count = await _load_trd_app(client, buy_ids)
    logger.info(f"Total upserted trd_app: {trd_app_count}")
    
    logger.info("=== ETL 2025 COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    asyncio.run(run_2025_etl())
