import logging
from datetime import datetime, date
from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.etl.client import OWSClient
from app.models.procurement import (
    RawTrdBuy, RawLots, RawTrdApp, RawContract, RawSubject, RawRnu,
    TrdBuy, Lot, TrdApp, TrdAppLot, Contract, Subject, Rnu, TreasuryPay,
    EtlRun, EtlCursor,
)
from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


def _parse_dt(val: Any) -> datetime | None:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace(" ", "T").split(".")[0])
    except Exception:
        return None


def _safe_decimal(val: Any):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


class BackfillETL:
    """
    Loads 1 year of historical data from OWS V3 into PostgreSQL.
    Uses upsert (INSERT ... ON CONFLICT DO UPDATE) for idempotency.
    """

    def __init__(self, date_from: str, date_to: str):
        self.date_from = date_from
        self.date_to = date_to
        self.client = OWSClient()

    async def run(self) -> dict:
        summary = {}
        run_id = await self._start_run()

        try:
            # summary["subject"] = await self._load_subjects()
            summary["trd_buy"] = await self._load_trd_buy()
            summary["lots"] = await self._load_lots()
            summary["trd_app"] = await self._load_trd_app()
            summary["contract"] = await self._load_contracts()
            summary["rnu"] = await self._load_rnu()
            summary["treasury_pay"] = await self._load_treasury_pay()
            await self._finish_run(run_id, "success", summary)
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            await self._finish_run(run_id, "failed", {"error": str(e)})
            raise

        return summary

    # ─── Loaders ─────────────────────────────────────────────────────────────

    async def _load_subjects(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_subject_all():
                rows = []
                for item in batch:
                    rows.append({
                        "id": item["pid"],
                        "bin": item.get("bin"),
                        "iin": item.get("iin"),
                        "inn": item.get("inn"),
                        "unp": item.get("unp"),
                        "name_ru": item.get("name_ru"),
                        "name_kz": item.get("name_kz"),
                        "full_name_ru": item.get("full_name_ru"),
                        "regdate": _parse_dt(item.get("regdate")),
                        "crdate": _parse_dt(item.get("crdate")),
                        "year": item.get("year"),
                        "type_supplier": item.get("type_supplier"),
                        "mark_small_employer": item.get("mark_small_employer", 0),
                        "mark_resident": item.get("mark_resident", 1),
                        "mark_patronymic_producer": item.get("mark_patronymic_producer", 0),
                        "mark_national_company": item.get("mark_national_company", 0),
                        "mark_world_company": item.get("mark_world_company", 0),
                        "mark_state_monopoly": item.get("mark_state_monopoly", 0),
                        "mark_natural_monopoly": item.get("mark_natural_monopoly", 0),
                        "oked_list": item.get("oked_list"),
                        "krp_code": item.get("krp_code"),
                        "kse_code": item.get("kse_code"),
                        "ref_kopf_code": item.get("ref_kopf_code"),
                        "qvazi": item.get("qvazi", 0),
                        "customer": item.get("customer", 0),
                        "supplier": item.get("supplier", 0),
                        "organizer": item.get("organizer", 0),
                        "is_single_org": item.get("is_single_org", 0),
                        "email": item.get("email"),
                        "phone": item.get("phone"),
                        "website": item.get("website"),
                        "country_code": str(item.get("country_code", "")),
                        "system_id": item.get("system_id"),
                        "last_update_at": _parse_dt(item.get("last_update_date")),
                        "is_deleted": False,
                    })
                if rows:
                    stmt = pg_insert(Subject).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: stmt.excluded[c] for c in [
                            "name_ru", "regdate", "crdate", "mark_small_employer",
                            "mark_resident", "email", "phone", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    logger.info(f"Subjects upserted: {count}")
        return count

    async def _load_trd_buy(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_trd_buy_all():
                rows = []
                for item in batch:
                    pub_date = _parse_dt(item.get("publish_date"))
                    # Filter by date range
                    if pub_date and not (
                        datetime.fromisoformat(self.date_from) <= pub_date <= datetime.fromisoformat(self.date_to)
                    ):
                        continue
                    rows.append({
                        "id": item["id"],
                        "number_anno": item.get("number_anno"),
                        "name_ru": item.get("name_ru"),
                        "name_kz": item.get("name_kz"),
                        "ref_trade_methods_id": item.get("ref_trade_methods_id"),
                        "publish_date": pub_date,
                        "start_date": _parse_dt(item.get("start_date")),
                        "end_date": _parse_dt(item.get("end_date")),
                        "total_sum": _safe_decimal(item.get("total_sum")),
                        "ref_buy_status_id": item.get("ref_buy_status_id"),
                        "org_bin": item.get("org_bin"),
                        "system_id": item.get("system_id"),
                        "singl_org_sign": item.get("singl_org_sign", 0),
                        "is_light_industry": item.get("is_light_industry", 0),
                        "is_construction_work": item.get("is_construction_work", 0),
                        "last_update_at": _parse_dt(item.get("index_date")),
                        "is_deleted": False,
                    })
                if rows:
                    stmt = pg_insert(TrdBuy).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: stmt.excluded[c] for c in [
                            "ref_buy_status_id", "total_sum", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    logger.info(f"TrdBuy upserted: {count}")
        return count

    async def _load_lots(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_lots_all():
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
                        set_={c: stmt.excluded[c] for c in [
                            "amount", "ref_lot_status_id", "dumping_flag", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    logger.info(f"Lots upserted: {count}")
        return count

    async def _load_trd_app(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_trd_app_all():
                app_rows = []
                app_lot_rows = []
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
                logger.info(f"TrdApp upserted: {count}")
        return count

    async def _load_contracts(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_contract_all():
                rows = []
                for item in batch:
                    sign_date = _parse_dt(item.get("sign_date") or item.get("crdate"))
                    if sign_date and not (
                        datetime.fromisoformat(self.date_from) <= sign_date <= datetime.fromisoformat(self.date_to)
                    ):
                        continue
                    rows.append({
                        "id": item["id"],
                        "trd_buy_id": item.get("trd_buy_id"),
                        "contract_number": item.get("contract_number"),
                        "contract_number_sys": item.get("contract_number_sys"),
                        "trd_buy_number_anno": item.get("trd_buy_number_anno"),
                        "customer_bin": item.get("customer_bin"),
                        "supplier_biin": item.get("supplier_biin"),
                        "contract_sum_wnds": _safe_decimal(item.get("contract_sum_wnds")),
                        "sign_date": sign_date,
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
                    logger.info(f"Contracts upserted: {count}")
        return count

    async def _load_rnu(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_rnu_all():
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
                    logger.info(f"RNU upserted: {count}")
        return count

    async def _load_treasury_pay(self) -> int:
        count = 0
        async with AsyncSessionLocal() as db:
            async for batch in self.client.fetch_treasury_pay_all():
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
                    logger.info(f"TreasuryPay upserted: {count}")
        return count

    # ─── ETL Run Tracking ────────────────────────────────────────────────────

    async def _get_cursor(self, source_name: str) -> str | None:
        async with AsyncSessionLocal() as db:
            cursor = await db.get(EtlCursor, source_name)
            return cursor.cursor_value if cursor else None

    async def _save_cursor(self, source_name: str, cursor_value: str):
        if not cursor_value:
            return
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(EtlCursor).values(
                source_name=source_name,
                cursor_value=cursor_value,
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["source_name"],
                set_={"cursor_value": stmt.excluded.cursor_value, "updated_at": stmt.excluded.updated_at},
            )
            await db.execute(stmt)
            await db.commit()

    async def _start_run(self) -> int:
        async with AsyncSessionLocal() as db:
            run = EtlRun(
                run_type="backfill",
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
            from sqlalchemy import update
            await db.execute(
                update(EtlRun)
                .where(EtlRun.id == run_id)
                .values(finished_at=datetime.utcnow(), status=status, summary_jsonb=summary)
            )
            await db.commit()

    # ─── Loaders with Checkpoints ─────────────────────────────────────────────

    async def _load_subjects(self) -> int:
        count = 0
        source = "backfill_subjects"
        
        # Restore cursor
        start_cursor = await self._get_cursor(source)
        if start_cursor:
             # Inject cursor into client's method via a hack or update client to accept starting url
             # Since client.paginate takes endpoint, and we updated it to accept full URL if it starts with http
             endpoint = start_cursor
             logger.info(f"Resuming subjects from cursor: {start_cursor}")
        else:
             endpoint = "/v3/subject/all"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
                rows = []
                for item in batch:
                    rows.append({
                        "id": item["pid"],
                        "bin": item.get("bin"),
                        "iin": item.get("iin"),
                        "inn": item.get("inn"),
                        "unp": item.get("unp"),
                        "name_ru": item.get("name_ru"),
                        "name_kz": item.get("name_kz"),
                        "full_name_ru": item.get("full_name_ru"),
                        "regdate": _parse_dt(item.get("regdate")),
                        "crdate": _parse_dt(item.get("crdate")),
                        "year": item.get("year"),
                        "type_supplier": item.get("type_supplier"),
                        "mark_small_employer": item.get("mark_small_employer", 0),
                        "mark_resident": item.get("mark_resident", 1),
                        "mark_patronymic_producer": item.get("mark_patronymic_producer", 0),
                        "mark_national_company": item.get("mark_national_company", 0),
                        "mark_world_company": item.get("mark_world_company", 0),
                        "mark_state_monopoly": item.get("mark_state_monopoly", 0),
                        "mark_natural_monopoly": item.get("mark_natural_monopoly", 0),
                        "oked_list": item.get("oked_list"),
                        "krp_code": item.get("krp_code"),
                        "kse_code": item.get("kse_code"),
                        "ref_kopf_code": item.get("ref_kopf_code"),
                        "qvazi": item.get("qvazi", 0),
                        "customer": item.get("customer", 0),
                        "supplier": item.get("supplier", 0),
                        "organizer": item.get("organizer", 0),
                        "is_single_org": item.get("is_single_org", 0),
                        "email": item.get("email"),
                        "phone": item.get("phone"),
                        "website": item.get("website"),
                        "country_code": str(item.get("country_code", "")),
                        "system_id": item.get("system_id"),
                        "last_update_at": _parse_dt(item.get("last_update_date")),
                        "is_deleted": False,
                    })
                if rows:
                    stmt = pg_insert(Subject).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: stmt.excluded[c] for c in [
                            "name_ru", "regdate", "crdate", "mark_small_employer",
                            "mark_resident", "email", "phone", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    
                    # Save cursor
                    if next_cursor:
                        await self._save_cursor(source, next_cursor)
                        
                    logger.info(f"Subjects upserted: {count}")
        return count

    async def _load_trd_buy(self) -> int:
        count = 0
        source = "backfill_trd_buy"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/trd-buy"
        
        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
                rows = []
                for item in batch:
                    pub_date = _parse_dt(item.get("publish_date"))
                    # Filter by date range
                    if pub_date and not (
                        datetime.fromisoformat(self.date_from) <= pub_date <= datetime.fromisoformat(self.date_to)
                    ):
                        continue
                    rows.append({
                        "id": item["id"],
                        "number_anno": item.get("number_anno"),
                        "name_ru": item.get("name_ru"),
                        "name_kz": item.get("name_kz"),
                        "ref_trade_methods_id": item.get("ref_trade_methods_id"),
                        "publish_date": pub_date,
                        "start_date": _parse_dt(item.get("start_date")),
                        "end_date": _parse_dt(item.get("end_date")),
                        "total_sum": _safe_decimal(item.get("total_sum")),
                        "ref_buy_status_id": item.get("ref_buy_status_id"),
                        "org_bin": item.get("org_bin"),
                        "system_id": item.get("system_id"),
                        "singl_org_sign": item.get("singl_org_sign", 0),
                        "is_light_industry": item.get("is_light_industry", 0),
                        "is_construction_work": item.get("is_construction_work", 0),
                        "last_update_at": _parse_dt(item.get("index_date")),
                        "is_deleted": False,
                    })
                if rows:
                    stmt = pg_insert(TrdBuy).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: stmt.excluded[c] for c in [
                            "ref_buy_status_id", "total_sum", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    
                if next_cursor:
                    await self._save_cursor(source, next_cursor)
                    
                if rows:
                    logger.info(f"TrdBuy upserted: {count}")
        return count

    async def _load_lots(self) -> int:
        count = 0
        source = "backfill_lots"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/lots"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
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
                        set_={c: stmt.excluded[c] for c in [
                            "amount", "ref_lot_status_id", "dumping_flag", "last_update_at",
                        ]},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)

                if next_cursor:
                    await self._save_cursor(source, next_cursor)
                    
                if rows:
                    logger.info(f"Lots upserted: {count}")
        return count

    async def _load_trd_app(self) -> int:
        count = 0
        source = "backfill_trd_app"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/trd-app"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
                app_rows = []
                app_lot_rows = []
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

                if next_cursor:
                    await self._save_cursor(source, next_cursor)
                    
                logger.info(f"TrdApp upserted: {count}")
        return count

    async def _load_contracts(self) -> int:
        count = 0
        source = "backfill_contracts"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/contract"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
                rows = []
                for item in batch:
                    sign_date = _parse_dt(item.get("sign_date") or item.get("crdate"))
                    if sign_date and not (
                        datetime.fromisoformat(self.date_from) <= sign_date <= datetime.fromisoformat(self.date_to)
                    ):
                        continue
                    rows.append({
                        "id": item["id"],
                        "trd_buy_id": item.get("trd_buy_id"),
                        "contract_number": item.get("contract_number"),
                        "contract_number_sys": item.get("contract_number_sys"),
                        "trd_buy_number_anno": item.get("trd_buy_number_anno"),
                        "customer_bin": item.get("customer_bin"),
                        "supplier_biin": item.get("supplier_biin"),
                        "contract_sum_wnds": _safe_decimal(item.get("contract_sum_wnds")),
                        "sign_date": sign_date,
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

                if next_cursor:
                    await self._save_cursor(source, next_cursor)
                    
                if rows:
                    logger.info(f"Contracts upserted: {count}")
        return count

    async def _load_rnu(self) -> int:
        count = 0
        source = "backfill_rnu"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/rnu"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
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
                
                if next_cursor:
                    await self._save_cursor(source, next_cursor)

                if rows:
                    logger.info(f"RNU upserted: {count}")
        return count

    async def _load_treasury_pay(self) -> int:
        count = 0
        source = "backfill_treasury_pay"
        start_cursor = await self._get_cursor(source)
        endpoint = start_cursor if start_cursor else "/v3/treasury-pay"

        async with AsyncSessionLocal() as db:
            async for batch, next_cursor in self.client.paginate(endpoint):
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

                if next_cursor:
                    await self._save_cursor(source, next_cursor)

                if rows:
                    logger.info(f"TreasuryPay upserted: {count}")
        return count
