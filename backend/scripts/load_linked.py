"""
load_linked.py — загружает lots и trd_app только для trd_buy_id,
которые уже есть в таблице contract (Feb 2026).
Затем запускает Feature Engine на связанных лотах.

Запуск:
  docker exec -d tender_radar_backend bash -c "python scripts/load_linked.py > /tmp/linked.log 2>&1"
"""
import asyncio
import logging
import sys
sys.path.insert(0, "/app")

from datetime import datetime
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.etl.client import OWSClient
from app.models.procurement import Lot, TrdApp, TrdAppLot, RiskScore, RiskFlag
from app.features.engine import FeatureEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("load_linked")


def _dt(v):
    if not v: return None
    if isinstance(v, datetime): return v
    try: return datetime.fromisoformat(str(v).replace(" ", "T").split(".")[0])
    except: return None

def _f(v):
    try: return float(v) if v is not None else None
    except: return None


async def get_contract_buy_ids() -> set:
    """Вернёт trd_buy_id из уже загруженных контрактов."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("SELECT DISTINCT trd_buy_id FROM contract WHERE trd_buy_id IS NOT NULL AND is_deleted = false")
        )
        ids = {row[0] for row in result.all()}
    logger.info(f"Found {len(ids)} unique trd_buy_ids in contracts")
    return ids


async def load_lots_for_ids(client: OWSClient, buy_ids: set) -> int:
    """Загрузить лоты, отфильтровав по trd_buy_id."""
    logger.info("=== Loading LOTS for %d buy_ids ===", len(buy_ids))
    count = 0
    matched = 0

    async for batch, _ in client.paginate("/v3/lots"):
        rows = []
        for item in batch:
            bid = item.get("trd_buy_id") or item.get("buy_id")
            if bid not in buy_ids:
                continue
            rows.append({
                "id": item["id"],
                "trd_buy_id": bid,
                "lot_number": item.get("lot_number") or str(item.get("id", "")),
                "name_ru": item.get("name_ru"),
                "name_kz": item.get("name_kz"),
                "amount": _f(item.get("amount")),
                "customer_bin": item.get("customer_bin"),
                "customer_name": item.get("customer_name_ru"),
                "dumping_flag": bool(item.get("dumping_flag", False)),
                "union_lots_flag": bool(item.get("union_lots_flag", False)),
                "ref_lot_status_id": item.get("ref_lot_status_id"),
                "singl_org_sign": item.get("singl_org_sign", 0),
                "is_light_industry": item.get("is_light_industry", 0),
                "is_construction_work": item.get("is_construction_work", 0),
                "disable_person_id": item.get("disable_person_id", 0),
                "system_id": item.get("system_id"),
                "last_update_at": _dt(item.get("index_date")),
                "is_deleted": False,
            })

        count += len(batch)  # total API records scanned
        if rows:
            async with AsyncSessionLocal() as db:
                stmt = pg_insert(Lot).values(rows).on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: pg_insert(Lot).excluded[c] for c in
                          ["amount", "ref_lot_status_id", "dumping_flag", "last_update_at"]},
                )
                await db.execute(stmt)
                await db.commit()
            matched += len(rows)
            logger.info(f"Lots matched: {matched} (scanned: {count})")

        if matched >= 10_000:
            logger.info("Reached 10k lots limit, stopping.")
            break

        # Stop after scanning 200k API records (avoid too long run)
        if count >= 200_000:
            logger.info("Scanned 200k records, stopping lots load.")
            break

    logger.info(f"=== Lots done: {matched} loaded ===")
    return matched


async def load_trd_app_for_ids(client: OWSClient, buy_ids: set) -> int:
    """Загрузить заявки только для нужных тендеров."""
    logger.info("=== Loading TRD_APP for %d buy_ids ===", len(buy_ids))
    count = 0
    matched = 0

    async for batch, _ in client.paginate("/v3/trd-app"):
        app_rows, lot_rows = [], []
        for item in batch:
            if item.get("buy_id") not in buy_ids:
                continue
            app_rows.append({
                "id": item["id"],
                "buy_id": item.get("buy_id"),
                "supplier_id": item.get("supplier_id"),
                "supplier_biin": item.get("supplier_bin_iin"),
                "cr_fio": item.get("cr_fio"),
                "mod_fio": item.get("mod_fio"),
                "prot_id": item.get("prot_id"),
                "prot_number": str(item.get("prot_number", "")),
                "date_apply": _dt(item.get("date_apply")),
                "system_id": item.get("system_id"),
                "last_update_at": _dt(item.get("index_date")),
            })
            for al in item.get("app_lots", []):
                lot_rows.append({
                    "id": al["id"],
                    "trd_app_id": item["id"],
                    "lot_id": al.get("lot_id"),
                    "status_id": al.get("status_id"),
                    "price": _f(al.get("price")),
                    "amount": _f(al.get("amount")),
                    "discount_value": al.get("discount_value"),
                    "discount_price": _f(al.get("discount_price")),
                })

        count += len(batch)
        if app_rows:
            async with AsyncSessionLocal() as db:
                stmt = pg_insert(TrdApp).values(app_rows).on_conflict_do_update(
                    index_elements=["id"],
                    set_={"last_update_at": pg_insert(TrdApp).excluded.last_update_at},
                )
                await db.execute(stmt)
                if lot_rows:
                    stmt2 = pg_insert(TrdAppLot).values(lot_rows).on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: pg_insert(TrdAppLot).excluded[c] for c in ["status_id", "price", "amount"]},
                    )
                    await db.execute(stmt2)
                await db.commit()
            matched += len(app_rows)
            logger.info(f"TrdApp matched: {matched} (scanned: {count})")

        if matched >= 10_000:
            logger.info("Reached 10k trd_app limit, stopping.")
            break

        if count >= 200_000:
            logger.info("Scanned 200k records, stopping trd_app load.")
            break

    logger.info(f"=== TrdApp done: {matched} loaded ===")
    return matched


async def run_feature_engine_on_linked_lots() -> dict:
    """Запустить Feature Engine только на лотах, у которых есть контракт."""
    logger.info("=== Running Feature Engine on linked lots ===")
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            text("""
                SELECT l.id FROM lots l
                JOIN contract c ON c.trd_buy_id = l.trd_buy_id
                WHERE l.is_deleted = false AND c.is_deleted = false
                LIMIT 5000
            """)
        )
        lot_ids = [r[0] for r in result.all()]

    logger.info(f"Lots with matching contracts: {len(lot_ids)}")

    if not lot_ids:
        logger.warning("No linked lots found! Falling back to all lots.")
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text("SELECT id FROM lots WHERE is_deleted = false LIMIT 5000")
            )
            lot_ids = [r[0] for r in result.all()]

    fe = FeatureEngine()
    summary = await fe.run(entity_ids=lot_ids)
    logger.info(f"Feature Engine done: {summary}")
    return summary


async def main():
    client = OWSClient()

    buy_ids = await get_contract_buy_ids()

    lots_count = await load_lots_for_ids(client, buy_ids)
    app_count = await load_trd_app_for_ids(client, buy_ids)

    logger.info(f"=== Data load complete: lots={lots_count}, trd_app={app_count} ===")

    summary = await run_feature_engine_on_linked_lots()
    logger.info(f"=== ALL DONE: {summary} ===")


if __name__ == "__main__":
    asyncio.run(main())
