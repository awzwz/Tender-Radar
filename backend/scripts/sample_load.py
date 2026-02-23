"""
sample_load.py — загружает по 10k записей для каждой таблицы
кроме trd_buy (уже загружен). Потом запускает Feature Engine.

Запуск внутри контейнера:
  docker exec tender_radar_backend python scripts/sample_load.py
"""
import asyncio
import logging
import sys
import os
sys.path.insert(0, "/app")

from datetime import datetime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.core.database import AsyncSessionLocal
from app.etl.client import OWSClient
from app.models.procurement import (
    Lot, TrdApp, TrdAppLot, Contract, Rnu, TreasuryPay,
)
from app.features.engine import FeatureEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sample_load")

MAX_ROWS = 10_000  # лимит на каждую таблицу


def _parse_dt(val):
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace(" ", "T").split(".")[0])
    except Exception:
        return None


def _safe_decimal(val):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


async def load_lots(client: OWSClient) -> int:
    count = 0
    logger.info("=== Loading LOTS (limit %d) ===", MAX_ROWS)
    async with AsyncSessionLocal() as db:
        async for batch, next_cursor in client.paginate("/v3/lots"):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "trd_buy_id": item.get("trd_buy_id") or item.get("buy_id"),
                    "lot_number": item.get("lot_number") or str(item.get("id", "")),
                    "name_ru": item.get("name_ru"),
                    "name_kz": item.get("name_kz"),
                    "amount": _safe_decimal(item.get("amount")),
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
                    "last_update_at": _parse_dt(item.get("index_date")),
                    "is_deleted": False,
                })
            if rows:
                stmt = pg_insert(Lot).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in ["amount", "ref_lot_status_id", "dumping_flag", "last_update_at"]},
                )
                await db.execute(stmt)
                await db.commit()
                count += len(rows)
                logger.info("Lots upserted: %d", count)

            if count >= MAX_ROWS:
                logger.info("Reached limit %d for lots, stopping.", MAX_ROWS)
                break
    return count


async def load_trd_app(client: OWSClient) -> int:
    count = 0
    logger.info("=== Loading TRD_APP (limit %d) ===", MAX_ROWS)
    async with AsyncSessionLocal() as db:
        async for batch, next_cursor in client.paginate("/v3/trd-app"):
            app_rows, app_lot_rows = [], []
            for item in batch:
                app_rows.append({
                    "id": item["id"],
                    "buy_id": item.get("buy_id"),
                    "supplier_id": item.get("supplier_id"),
                    "supplier_biin": item.get("supplier_bin_iin"),
                    "cr_fio": item.get("cr_fio"),
                    "mod_fio": item.get("mod_fio"),
                    "prot_id": item.get("prot_id"),
                    "prot_number": str(item.get("prot_number", "")),
                    "date_apply": _parse_dt(item.get("date_apply")),
                    "system_id": item.get("system_id"),
                    "last_update_at": _parse_dt(item.get("index_date")),
                })
                for al in item.get("app_lots", []):
                    app_lot_rows.append({
                        "id": al["id"],
                        "trd_app_id": item["id"],
                        "lot_id": al.get("lot_id"),
                        "status_id": al.get("status_id"),
                        "price": _safe_decimal(al.get("price")),
                        "amount": _safe_decimal(al.get("amount")),
                        "discount_value": al.get("discount_value"),
                        "discount_price": _safe_decimal(al.get("discount_price")),
                    })

            if app_rows:
                stmt = pg_insert(TrdApp).values(app_rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={"last_update_at": stmt.excluded.last_update_at},
                )
                await db.execute(stmt)
            if app_lot_rows:
                stmt = pg_insert(TrdAppLot).values(app_lot_rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in ["status_id", "price", "amount"]},
                )
                await db.execute(stmt)
            await db.commit()
            count += len(app_rows)
            logger.info("TrdApp upserted: %d", count)

            if count >= MAX_ROWS:
                logger.info("Reached limit %d for trd_app, stopping.", MAX_ROWS)
                break
    return count


async def load_contracts(client: OWSClient) -> int:
    count = 0
    logger.info("=== Loading CONTRACTS (limit %d) ===", MAX_ROWS)
    async with AsyncSessionLocal() as db:
        async for batch, next_cursor in client.paginate("/v3/contract"):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "trd_buy_id": item.get("trd_buy_id"),
                    "contract_number": item.get("contract_number"),
                    "contract_number_sys": item.get("contract_number_sys"),
                    "trd_buy_number_anno": item.get("trd_buy_number_anno"),
                    "customer_bin": item.get("customer_bin"),
                    "supplier_biin": item.get("supplier_biin"),
                    "contract_sum_wnds": _safe_decimal(item.get("contract_sum_wnds")),
                    "sign_date": _parse_dt(item.get("sign_date") or item.get("crdate")),
                    "plan_exec_date": _parse_dt(item.get("plan_exec_date")),
                    "fakt_exec_date": _parse_dt(item.get("fakt_exec_date")),
                    "fakt_sum": _safe_decimal(item.get("fakt_sum")),
                    "ref_contract_status_id": item.get("ref_contract_status_id"),
                    "ref_contract_type_id": item.get("ref_contract_type_id"),
                    "parent_id": item.get("parent_id"),
                    "root_id": item.get("root_id"),
                    "supplier_legal_address": item.get("supplier_legal_address"),
                    "customer_legal_address": item.get("customer_legal_address"),
                    "is_gu": item.get("is_gu", 0),
                    "exchange_rate": item.get("exchange_rate"),
                    "system_id": item.get("system_id"),
                    "last_update_at": _parse_dt(item.get("last_update_date")),
                    "is_deleted": False,
                })
            if rows:
                stmt = pg_insert(Contract).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in [
                        "contract_sum_wnds", "fakt_sum", "fakt_exec_date",
                        "ref_contract_status_id", "last_update_at",
                    ]},
                )
                await db.execute(stmt)
                await db.commit()
                count += len(rows)
                logger.info("Contracts upserted: %d", count)

            if count >= MAX_ROWS:
                logger.info("Reached limit %d for contracts, stopping.", MAX_ROWS)
                break
    return count


async def load_rnu(client: OWSClient) -> int:
    count = 0
    logger.info("=== Loading RNU (full — small table) ===")
    async with AsyncSessionLocal() as db:
        async for batch, next_cursor in client.paginate("/v3/rnu"):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "pid": item.get("pid"),
                    "supplier_biin": item.get("biin") or item.get("iin"),
                    "supplier_name_ru": item.get("name_ru"),
                    "start_date": _parse_dt(item.get("start_date")),
                    "end_date": _parse_dt(item.get("end_date")),
                    "reason": item.get("reason_ru") or item.get("reason"),
                    "system_id": item.get("system_id", 3),
                    "is_active": True,
                })
            if rows:
                stmt = pg_insert(Rnu).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in ["end_date", "is_active"]},
                )
                await db.execute(stmt)
                await db.commit()
                count += len(rows)
                logger.info("RNU upserted: %d", count)
            # RNU is small, load all
    return count


async def load_treasury_pay(client: OWSClient) -> int:
    count = 0
    logger.info("=== Loading TREASURY_PAY (limit %d) ===", MAX_ROWS)
    async with AsyncSessionLocal() as db:
        async for batch, next_cursor in client.paginate("/v3/treasury-pay"):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "nom_za": item.get("nom_za"),
                    "contract_id": item.get("contract_id"),
                    "dt_reg": _parse_dt(item.get("dt_reg")),
                    "supplier": item.get("supplier"),
                    "rnn_supplier": item.get("rnn_supplier"),
                    "nom_dog": item.get("nom_dog"),
                    "dt_dog": _parse_dt(item.get("dt_dog")),
                    "item_description": item.get("item_description"),
                    "pay_amount": _safe_decimal(item.get("pay_amount")),
                    "pay_date": _parse_dt(item.get("pay_date")),
                    "ppn": item.get("ppn"),
                    "espk": item.get("espk"),
                    "gu": item.get("gu"),
                    "fin_source": item.get("fin_source"),
                    "index_date": _parse_dt(item.get("index_date")),
                    "system_id": item.get("system_id"),
                })
            if rows:
                stmt = pg_insert(TreasuryPay).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={"pay_amount": stmt.excluded.pay_amount},
                )
                await db.execute(stmt)
                await db.commit()
                count += len(rows)
                logger.info("TreasuryPay upserted: %d", count)

            if count >= MAX_ROWS:
                logger.info("Reached limit %d for treasury_pay, stopping.", MAX_ROWS)
                break
    return count


async def run_feature_engine():
    logger.info("=== Running Feature Engine ===")
    engine = FeatureEngine()
    summary = await engine.run()
    logger.info("Feature Engine done: %s", summary)
    return summary


async def main():
    client = OWSClient()
    summary = {}

    summary["lots"] = await load_lots(client)
    summary["trd_app"] = await load_trd_app(client)
    summary["contracts"] = await load_contracts(client)
    summary["rnu"] = await load_rnu(client)
    summary["treasury_pay"] = await load_treasury_pay(client)

    logger.info("=== Load complete: %s ===", summary)

    summary["feature_engine"] = await run_feature_engine()
    logger.info("=== ALL DONE: %s ===", summary)


if __name__ == "__main__":
    asyncio.run(main())
