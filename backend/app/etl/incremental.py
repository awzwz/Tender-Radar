import logging
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import update
from app.etl.client import OWSClient
from app.etl.backfill import _parse_dt, _safe_decimal
from app.models.procurement import (
    TrdBuy, Lot, TrdApp, TrdAppLot, Contract, Subject, Rnu,
    EtlRun, EtlCursor,
)
from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

ENTITY_ENDPOINT_MAP = {
    "TrdBuy": "/v3/trd-buy",
    "Lots": "/v3/lots",
    "TrdApp": "/v3/trd-app",
    "Contract": "/v3/contract",
    "Subject": "/v3/subject/biin",
    "Rnu": "/v3/rnu",
}


class IncrementalETL:
    """
    Daily incremental update using /v3/journal.
    Fetches changed objects and upserts/soft-deletes them.
    """

    def __init__(self, date_from: str = None, date_to: str = None):
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.date_from = date_from or yesterday
        self.date_to = date_to or today
        self.client = OWSClient()

    async def run(self) -> dict:
        summary = {"processed": 0, "updated": 0, "deleted": 0, "errors": 0}
        run_id = await self._start_run()

        try:
            journal_entries = await self.client.get_journal(self.date_from, self.date_to)
            logger.info(f"Journal entries fetched: {len(journal_entries)}")

            for entry in journal_entries:
                try:
                    await self._process_entry(entry, summary)
                    summary["processed"] += 1
                except Exception as e:
                    logger.error(f"Error processing journal entry {entry}: {e}")
                    summary["errors"] += 1

            await self._update_cursor()
            await self._finish_run(run_id, "success", summary)
        except Exception as e:
            logger.error(f"Incremental ETL failed: {e}")
            await self._finish_run(run_id, "failed", {"error": str(e)})
            raise

        return summary

    async def _process_entry(self, entry: dict, summary: dict):
        """Process a single journal entry (update or delete)."""
        entity_type = entry.get("entity_type") or entry.get("object_type")
        entity_id = entry.get("entity_id") or entry.get("object_id")
        action = entry.get("action", "U")  # U=update, D=delete

        if not entity_type or not entity_id:
            return

        if action == "D":
            await self._soft_delete(entity_type, entity_id)
            summary["deleted"] += 1
        else:
            await self._fetch_and_upsert(entity_type, entity_id)
            summary["updated"] += 1

    async def _soft_delete(self, entity_type: str, entity_id: str):
        """Mark entity as deleted (is_deleted=True)."""
        model_map = {
            "TrdBuy": TrdBuy,
            "Lots": Lot,
            "Contract": Contract,
            "Subject": Subject,
        }
        model = model_map.get(entity_type)
        if not model:
            return

        async with AsyncSessionLocal() as db:
            await db.execute(
                update(model)
                .where(model.id == int(entity_id))
                .values(is_deleted=True, last_update_at=datetime.utcnow())
            )
            await db.commit()

    async def _fetch_and_upsert(self, entity_type: str, entity_id: str):
        """Fetch updated object from API and upsert into DB."""
        endpoint = ENTITY_ENDPOINT_MAP.get(entity_type)
        if not endpoint:
            return

        obj = await self.client.fetch_by_id(endpoint, entity_id)
        if not obj:
            return

        async with AsyncSessionLocal() as db:
            if entity_type == "TrdBuy":
                await self._upsert_trd_buy(db, obj)
            elif entity_type == "Lots":
                await self._upsert_lot(db, obj)
            elif entity_type == "Contract":
                await self._upsert_contract(db, obj)
            elif entity_type == "Subject":
                await self._upsert_subject(db, obj)
            await db.commit()

    async def _upsert_trd_buy(self, db, item: dict):
        row = {
            "id": item["id"],
            "number_anno": item.get("number_anno"),
            "name_ru": item.get("name_ru"),
            "name_kz": item.get("name_kz"),
            "ref_trade_methods_id": item.get("ref_trade_methods_id"),
            "publish_date": _parse_dt(item.get("publish_date")),
            "start_date": _parse_dt(item.get("start_date")),
            "end_date": _parse_dt(item.get("end_date")),
            "total_sum": _safe_decimal(item.get("total_sum")),
            "ref_buy_status_id": item.get("ref_buy_status_id"),
            "org_bin": item.get("org_bin"),
            "system_id": item.get("system_id"),
            "last_update_at": datetime.utcnow(),
            "is_deleted": False,
        }
        stmt = pg_insert(TrdBuy).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={c: stmt.excluded[c] for c in ["ref_buy_status_id", "total_sum", "last_update_at"]},
        )
        await db.execute(stmt)

    async def _upsert_lot(self, db, item: dict):
        row = {
            "id": item["id"],
            "trd_buy_id": item.get("trd_buy_id") or item.get("buy_id"),
            "name_ru": item.get("name_ru"),
            "amount": _safe_decimal(item.get("amount")),
            "customer_bin": item.get("customer_bin"),
            "dumping_flag": bool(item.get("dumping_flag", False)),
            "ref_lot_status_id": item.get("ref_lot_status_id"),
            "system_id": item.get("system_id"),
            "last_update_at": datetime.utcnow(),
            "is_deleted": False,
        }
        stmt = pg_insert(Lot).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={c: stmt.excluded[c] for c in ["amount", "ref_lot_status_id", "dumping_flag", "last_update_at"]},
        )
        await db.execute(stmt)

    async def _upsert_contract(self, db, item: dict):
        row = {
            "id": item["id"],
            "trd_buy_id": item.get("trd_buy_id"),
            "customer_bin": item.get("customer_bin"),
            "supplier_biin": item.get("supplier_biin"),
            "contract_sum_wnds": _safe_decimal(item.get("contract_sum_wnds")),
            "sign_date": _parse_dt(item.get("sign_date") or item.get("crdate")),
            "plan_exec_date": _parse_dt(item.get("plan_exec_date")),
            "fakt_exec_date": _parse_dt(item.get("fakt_exec_date")),
            "fakt_sum": _safe_decimal(item.get("fakt_sum")),
            "ref_contract_status_id": item.get("ref_contract_status_id"),
            "parent_id": item.get("parent_id"),
            "root_id": item.get("root_id"),
            "system_id": item.get("system_id"),
            "last_update_at": datetime.utcnow(),
            "is_deleted": False,
        }
        stmt = pg_insert(Contract).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={c: stmt.excluded[c] for c in [
                "contract_sum_wnds", "fakt_sum", "fakt_exec_date", "ref_contract_status_id", "last_update_at"
            ]},
        )
        await db.execute(stmt)

    async def _upsert_subject(self, db, item: dict):
        row = {
            "id": item["pid"],
            "bin": item.get("bin"),
            "iin": item.get("iin"),
            "name_ru": item.get("name_ru"),
            "regdate": _parse_dt(item.get("regdate")),
            "crdate": _parse_dt(item.get("crdate")),
            "mark_small_employer": item.get("mark_small_employer", 0),
            "mark_resident": item.get("mark_resident", 1),
            "system_id": item.get("system_id"),
            "last_update_at": datetime.utcnow(),
            "is_deleted": False,
        }
        stmt = pg_insert(Subject).values([row])
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={c: stmt.excluded[c] for c in ["name_ru", "regdate", "mark_small_employer", "last_update_at"]},
        )
        await db.execute(stmt)

    async def _update_cursor(self):
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(EtlCursor).values([{
                "source_name": "journal",
                "cursor_value": self.date_to,
                "updated_at": datetime.utcnow(),
            }])
            stmt = stmt.on_conflict_do_update(
                index_elements=["source_name"],
                set_={"cursor_value": stmt.excluded.cursor_value, "updated_at": stmt.excluded.updated_at},
            )
            await db.execute(stmt)
            await db.commit()

    async def _start_run(self) -> int:
        async with AsyncSessionLocal() as db:
            run = EtlRun(
                run_type="incremental",
                started_at=datetime.utcnow(),
                status="running",
                summary_jsonb={"date_from": self.date_from, "date_to": self.date_to},
            )
            db.add(run)
            await db.commit()
            await db.refresh(run)
            return run.id

    async def _finish_run(self, run_id: int, status: str, summary: dict):
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(EtlRun)
                .where(EtlRun.id == run_id)
                .values(finished_at=datetime.utcnow(), status=status, summary_jsonb=summary)
            )
            await db.commit()
