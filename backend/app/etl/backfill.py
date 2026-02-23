"""
BackfillETL: loads historical data from OWS V3 GraphQL into PostgreSQL.
Uses upsert (INSERT ... ON CONFLICT DO UPDATE) for idempotency.
Supports a bootstrap_limit to cap records for ML training.
"""
import logging
from datetime import datetime
from typing import Any
from sqlalchemy import select, func, and_, or_, text, cast, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.etl.client import (
    OWSClient,
    GQL_TRD_BUY, GQL_LOTS, GQL_TRD_APP, GQL_CONTRACT, GQL_CONTRACT_FLAT,
    GQL_SUBJECT, GQL_RNU, GQL_TREASURY_PAY,
    GQL_CONTRACT_ACT, GQL_CONTRACT_PAYMENT, GQL_CONTRACT_SPEC_SUM,
)
from app.models.procurement import (
    TrdBuy, Lot, TrdApp, TrdAppLot, Contract, Subject, Rnu, TreasuryPay,
    ContractAct, ContractPayment, ContractSpecSum,
    EtlRun, EtlCursor,
)
from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


def _parse_dt(val: Any) -> datetime | None:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    # normalize common ISO formats
    s = s.replace(' ', 'T')
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    # drop fractional seconds if present
    if '.' in s:
        s = s.split('.', 1)[0]
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None



def _safe_decimal(val: Any):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


class BackfillETL:
    """
    Loads historical data from OWS V3 GraphQL into PostgreSQL.
    Uses upsert for idempotency, GraphQL pagination, and optional bootstrap_limit.
    """

    def __init__(self, date_from: str, date_to: str, bootstrap_limit: int = 0):
        self.date_from = date_from
        self.date_to = date_to
        # OWS API requires microsecond-precision dates for Elasticsearch filters
        self.ows_date_from = f"{date_from} 00:00:00.000000"
        self.ows_date_to = f"{date_to} 23:59:59.999999"
        self.bootstrap_limit = bootstrap_limit  # 0 = no limit
        self.client = OWSClient()

    async def run(self, hunter_mode: bool = False) -> dict:
        summary = {}
        run_id = await self._start_run()

        try:
            if hunter_mode:
                logger.info("Starting HUNTER MODE: targeting RNU companies and key industries")
                async with AsyncSessionLocal() as db:
                    rnu_count = (await db.execute(select(func.count()).select_from(Rnu))).scalar()
                    if rnu_count < 1000:
                        summary["rnu_stats"] = await self._load_rnu()
                    
                    sub_count = (await db.execute(select(func.count()).select_from(Subject))).scalar()
                    if sub_count < 1000:
                        summary["subject_stats"] = await self._load_subjects()
                
                summary["hunter_mode"] = await self._run_hunter_mode()
            else:
                summary["trd_buy"] = await self._load_trd_buy()
                summary["lots"] = await self._load_lots()
                summary["trd_app"] = await self._load_trd_app()
                summary["contract"] = await self._load_contracts()
                summary["contract_act"] = await self._load_contract_acts()

            await self._finish_run(run_id, "success", summary)
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            await self._finish_run(run_id, "failed", {"error": str(e)})
            raise

        return summary

    async def _run_hunter_mode(self) -> dict:
        """
        Targeted extraction:
        1. Get BINs of RNU companies.
        2. Get BINs of companies in IT/Medicine/Construction (via OKED).
        3. Load all their contracts and associated tenders.
        """
        async with AsyncSessionLocal() as db:
            # 1. RNU BINS
            rnu_result = await db.execute(select(Rnu.supplier_biin).where(Rnu.is_active == True))
            rnu_bins = {r[0] for r in rnu_result.all() if r[0]}
            
            # 2. Key Industry BINS (OKED filters)
            # 62/63 = IT, 86 = Health, 41/42/43 = Construction
            industry_query = select(Subject.bin).where(or_(
                cast(Subject.oked_list, String).like('62%'),
                cast(Subject.oked_list, String).like('63%'),
                cast(Subject.oked_list, String).like('86%'),
                cast(Subject.oked_list, String).like('41%'),
                cast(Subject.oked_list, String).like('42%'),
                cast(Subject.oked_list, String).like('43%')
            ))
            # Note: oked_list is BigInteger in models, but often needs to be handled as string for prefix
            # If it's a number, we might need a different approach. Let's assume it's castable or we use ranges.
            ind_result = await db.execute(industry_query)
            ind_bins = {r[0] for r in ind_result.all() if r[0]}
            # Prioritize RNU bins first (most valuable for ML training)
            target_bins = list(rnu_bins) + list(ind_bins - rnu_bins)
            logger.info(f"Hunter Mode targets: {len(target_bins)} unique BINs ({len(rnu_bins)} RNU + {len(ind_bins - rnu_bins)} industry)")

        if not target_bins:
            return {"status": "no_targets_found"}

        # Load contracts for top targets (one BIN per API call)
        contract_count = await self._load_contracts_by_supplier(target_bins)
        
        return {
            "target_bins": len(target_bins),
            "contracts_loaded": contract_count
        }

    async def _load_contracts_by_supplier(self, bins: list[str]) -> int:
        """Fetch contracts one BIN at a time (API requires String, not array)."""
        if not bins: return 0
        count = 0
        # Cap at 200 BINs total — each needs its own API call
        for bin_val in bins[:200]:
            try:
                async for batch in self.client.graphql_paginate(
                    GQL_CONTRACT, "Contract",
                    variables={"filter": {"supplierBiin": bin_val}}
                ):
                    batch_count = await self._process_contract_batch(batch)
                    count += batch_count

                    buy_ids = list({item.get("trdBuyId") for item in batch if item.get("trdBuyId")})
                    if buy_ids:
                        await self._load_trd_buy_by_ids(buy_ids)
                        await self._load_lots_by_buy_ids(buy_ids)
                        await self._load_trd_app_by_buy_ids(buy_ids)
                if count > 0 and count % 500 == 0:
                    logger.info(f"Hunter Mode: contracts upserted so far: {count}")
            except Exception as e:
                logger.warning(f"Skipping BIN {bin_val}: {e}")
                continue

        return count

    async def _load_trd_app_by_buy_ids(self, ids: list[int]) -> int:
        count = 0
        async for batch in self.client.graphql_paginate(
            GQL_TRD_APP, "TrdApp",
            variables={"filter": {"buyId": ids}}
        ):
            count += await self._process_trd_app_batch(batch)
        return count

    async def _load_trd_buy_by_ids(self, ids: list[int]) -> int:
        count = 0
        async for batch in self.client.graphql_paginate(
            GQL_TRD_BUY, "TrdBuy",
            variables={"filter": {"id": ids}}
        ):
            count += await self._process_trd_buy_batch(batch)
        return count

    async def _load_lots_by_buy_ids(self, ids: list[int]) -> int:
        count = 0
        async for batch in self.client.graphql_paginate(
            GQL_LOTS, "Lots",
            variables={"filter": {"trdBuyId": ids}}
        ):
            count += await self._process_lots_batch(batch)
        return count

    # Helper methods to avoid duplication (refactored from existing code)
    async def _process_contract_batch(self, batch: list[dict]) -> int:
        # [Implementation logic from _load_contracts block]
        # (This would be extracted from line 414-580 to avoid duplication)
        pass # To be implemented in full file update

    # ─── ETL Run Tracking ────────────────────────────────────────────────────

    async def _start_run(self) -> int:
        async with AsyncSessionLocal() as db:
            run = EtlRun(
                run_type="backfill",
                started_at=datetime.utcnow(),
                status="running",
                summary_jsonb={
                    "date_from": self.date_from,
                    "date_to": self.date_to,
                    "bootstrap_limit": self.bootstrap_limit,
                },
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

    # ─── Loaders (GraphQL) ───────────────────────────────────────────────────

    async def _load_subjects(self) -> int:
        """Load Subjects (companies/people).

        NOTE: This can be large; for MVP bootstrap we cap by bootstrap_limit if set.
        """
        count = 0
        async for batch in self.client.graphql_paginate(
            GQL_SUBJECT, "Subjects",
            max_records=self.bootstrap_limit,
            variables=None,
        ):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["pid"],
                    "bin": item.get("bin"),
                    "iin": item.get("iin"),
                    "inn": item.get("inn"),
                    "unp": item.get("unp"),
                    "name_ru": item.get("nameRu"),
                    "name_kz": item.get("nameKz"),
                    "full_name_ru": item.get("fullNameRu"),
                    "regdate": _parse_dt(item.get("regdate")),
                    "crdate": _parse_dt(item.get("crdate")),
                    "year": item.get("year"),
                    "type_supplier": item.get("typeSupplier"),
                    "mark_small_employer": item.get("markSmallEmployer", 0),
                    "mark_resident": item.get("markResident", 1),
                    "mark_patronymic_producer": item.get("markPatronymicProducer", 0),
                    "mark_national_company": item.get("markNationalCompany", 0),
                    "mark_world_company": item.get("markWorldCompany", 0),
                    "mark_state_monopoly": item.get("markStateMonopoly", 0),
                    "mark_natural_monopoly": item.get("markNaturalMonopoly", 0),
                    "oked_list": item.get("okedList"),
                    "krp_code": item.get("krpCode"),
                    "kse_code": item.get("kseCode"),
                    "ref_kopf_code": item.get("refKopfCode"),
                    "qvazi": item.get("qvazi", 0),
                    "customer": item.get("customer", 0),
                    "supplier": item.get("supplier", 0),
                    "organizer": item.get("organizer", 0),
                    "is_single_org": item.get("isSingleOrg", 0),
                    "email": item.get("email"),
                    "phone": item.get("phone"),
                    "website": item.get("website"),
                    "country_code": str(item.get("countryCode", "")),
                    "system_id": item.get("systemId"),
                    "last_update_at": _parse_dt(item.get("lastUpdateDate")),
                    "is_deleted": False,
                })

            if rows:
                async with AsyncSessionLocal() as db:
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

    async def _process_trd_buy_batch(self, batch: list[dict]) -> int:
        rows = []
        for item in batch:
            pub_date = _parse_dt(item.get("publishDate"))
            # If not in hunter mode, we might want to filter by date. 
            # But the caller should handle filters.
            rows.append({
                "id": item["id"],
                "number_anno": item.get("numberAnno"),
                "name_ru": item.get("nameRu"),
                "name_kz": item.get("nameKz"),
                "ref_trade_methods_id": item.get("refTradeMethodsId"),
                "publish_date": pub_date,
                "start_date": _parse_dt(item.get("startDate")),
                "end_date": _parse_dt(item.get("endDate")),
                "total_sum": _safe_decimal(item.get("totalSum")),
                "count_lots": item.get("countLots"),
                "ref_buy_status_id": item.get("refBuyStatusId"),
                "org_bin": item.get("orgBin"),
                "system_id": item.get("systemId"),
                "singl_org_sign": item.get("singlOrgSign", 0),
                "is_light_industry": item.get("isLightIndustry", 0),
                "is_construction_work": item.get("isConstructionWork", 0),
                "parent_id": item.get("parentId"),
                "repeat_start_date": _parse_dt(item.get("repeatStartDate")),
                "repeat_end_date": _parse_dt(item.get("repeatEndDate")),
                "discus_start_date": _parse_dt(item.get("discusStartDate")),
                "discus_end_date": _parse_dt(item.get("discusEndDate")),
                "last_update_date": _parse_dt(item.get("lastUpdateDate")),
                "last_update_at": _parse_dt(item.get("indexDate")),
                "is_deleted": False,
            })
        if rows:
            async with AsyncSessionLocal() as db:
                stmt = pg_insert(TrdBuy).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in [
                        "ref_buy_status_id", "total_sum", "count_lots",
                        "parent_id", "repeat_start_date", "repeat_end_date",
                        "discus_start_date", "discus_end_date",
                        "last_update_date", "last_update_at",
                    ]},
                )
                await db.execute(stmt)
                await db.commit()
                return len(rows)
        return 0

    async def _load_trd_buy(self) -> int:
        count = 0
        async for batch in self.client.graphql_paginate(
            GQL_TRD_BUY, "TrdBuy",
            max_records=self.bootstrap_limit,
            variables={"filter": {"publishDate": [self.ows_date_from, self.ows_date_to]}},
        ):
            filtered_batch = []
            for item in batch:
                pub_date = _parse_dt(item.get("publishDate"))
                if pub_date and (
                    datetime.fromisoformat(self.date_from) <= pub_date <= datetime.fromisoformat(self.date_to)
                ):
                    filtered_batch.append(item)
            
            if filtered_batch:
                batch_count = await self._process_trd_buy_batch(filtered_batch)
                count += batch_count
                logger.info(f"TrdBuy upserted: {count}")
        return count

    async def _process_lots_batch(self, batch: list[dict]) -> int:
        rows = []
        for item in batch:
            rows.append({
                "id": item["id"],
                "trd_buy_id": item.get("trdBuyId"),
                "lot_number": item.get("lotNumber") or str(item.get("id", "")),
                "name_ru": item.get("nameRu"),
                "name_kz": item.get("nameKz"),
                "amount": _safe_decimal(item.get("amount")),
                "customer_bin": item.get("customerBin"),
                "customer_name": item.get("customerNameRu"),
                "dumping_flag": bool(item.get("dumping", False)),
                "union_lots_flag": bool(item.get("unionLots", False)),
                "ref_lot_status_id": item.get("refLotStatusId"),
                "singl_org_sign": item.get("singlOrgSign", 0),
                "is_light_industry": item.get("isLightIndustry", 0),
                "is_construction_work": item.get("isConstructionWork", 0),
                "disable_person_id": item.get("disablePersonId", 0),
                "system_id": item.get("systemId"),
                "last_update_at": _parse_dt(item.get("indexDate")),
                "is_deleted": False,
            })
        if rows:
            async with AsyncSessionLocal() as db:
                stmt = pg_insert(Lot).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt.excluded[c] for c in [
                        "amount", "ref_lot_status_id", "dumping_flag", "last_update_at",
                    ]},
                )
                await db.execute(stmt)
                await db.commit()
                return len(rows)
        return 0

    async def _process_trd_app_batch(self, batch: list[dict]) -> int:
        app_rows = []
        app_lot_rows = []
        for item in batch:
            app_rows.append({
                "id": item["id"],
                "buy_id": item.get("buyId"),
                "supplier_id": item.get("supplierId"),
                "supplier_biin": item.get("supplierBinIin"),
                "cr_fio": item.get("crFio"),
                "mod_fio": item.get("modFio"),
                "prot_id": item.get("protId"),
                "prot_number": str(item.get("protNumber") or ""),
                "date_apply": _parse_dt(item.get("dateApply")),
                "system_id": item.get("systemId"),
                "last_update_at": _parse_dt(item.get("indexDate")),
            })
            for al in (item.get("appLots") or []):
                app_lot_rows.append({
                    "id": al["id"],
                    "trd_app_id": item["id"],
                    "lot_id": al.get("lotId"),
                    "status_id": al.get("statusId"),
                    "price": _safe_decimal(al.get("price")),
                    "amount": _safe_decimal(al.get("amount")),
                    "discount_value": al.get("discountValue"),
                    "discount_price": _safe_decimal(al.get("discountPrice")),
                })
        if app_rows:
            async with AsyncSessionLocal() as db:
                stmt = pg_insert(TrdApp).values(app_rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={"last_update_at": stmt.excluded.last_update_at},
                )
                await db.execute(stmt)
                if app_lot_rows:
                    stmt_al = pg_insert(TrdAppLot).values(app_lot_rows)
                    stmt_al = stmt_al.on_conflict_do_update(
                        index_elements=["id"],
                        set_={c: stmt_al.excluded[c] for c in ["status_id", "price", "amount"]},
                    )
                    await db.execute(stmt_al)
                await db.commit()
                return len(app_rows)
        return 0

    async def _load_trd_app(self) -> int:
        from sqlalchemy import select
        async with AsyncSessionLocal() as db:
            df, dt = datetime.fromisoformat(self.date_from), datetime.fromisoformat(self.date_to)
            ids = (await db.execute(select(TrdBuy.id).where(
                TrdBuy.publish_date >= df, TrdBuy.publish_date <= dt
            ))).scalars().all()

        if not ids: return 0
        count = 0
        for ch in self._chunks(list(ids), 200):
            async for batch in self.client.graphql_paginate(GQL_TRD_APP, "TrdApp", variables={"filter": {"buyId": ch}}):
                count += await self._process_trd_app_batch(batch)
                logger.info(f"TrdApp upserted: {count}")
        return count

    async def _process_contract_batch(self, batch: list[dict]) -> int:
        contract_rows = []
        treasury_rows = []
        payment_rows = []
        spec_rows = []
        act_rows = []

        for item in batch:
            sign_date = _parse_dt(item.get("signDate"))
            contract_rows.append({
                "id": item["id"],
                "trd_buy_id": item.get("trdBuyId"),
                "contract_number": item.get("contractNumber"),
                "contract_number_sys": item.get("contractNumberSys"),
                "trd_buy_number_anno": item.get("trdBuyNumberAnno"),
                "customer_bin": item.get("customerBin"),
                "supplier_biin": item.get("supplierBiin"),
                "contract_sum_wnds": _safe_decimal(item.get("contractSumWnds")),
                "sign_date": sign_date,
                "plan_exec_date": _parse_dt(item.get("planExecDate")),
                "fakt_exec_date": _parse_dt(item.get("faktExecDate")),
                "fakt_sum": _safe_decimal(item.get("faktSum")),
                "ref_contract_status_id": item.get("refContractStatusId"),
                "ref_contract_type_id": item.get("refContractTypeId"),
                "parent_id": item.get("parentId"),
                "root_id": item.get("rootId"),
                "supplier_legal_address": item.get("supplierLegalAddress"),
                "customer_legal_address": item.get("customerLegalAddress"),
                "supplier_iik": item.get("supplierIik"),
                "supplier_bik": item.get("supplierBik"),
                "is_gu": item.get("isGu", 0),
                "exchange_rate": _safe_decimal(item.get("exchangeRate")),
                "system_id": item.get("systemId"),
                "last_update_at": _parse_dt(item.get("lastUpdateDate")),
                "is_deleted": False,
            })

            # Nested entities (only present if GQL_CONTRACT with nesting is used)
            for tp in (item.get("TreasuryPay") or []):
                treasury_rows.append({
                    "id": tp["id"],
                    "nom_za": tp.get("nomZa"),
                    "contract_id": tp.get("contractId") or item["id"],
                    "dt_reg": _parse_dt(tp.get("dtReg")),
                    "supplier": tp.get("supplier"),
                    "rnn_supplier": tp.get("rnnSupplier"),
                    "nom_dog": tp.get("nomDog"),
                    "dt_dog": _parse_dt(tp.get("dtDog")),
                    "item_description": tp.get("itemDescription"),
                    "pay_amount": _safe_decimal(tp.get("payAmount")),
                    "prepay_sum": _safe_decimal(tp.get("prepaySum")),
                    "pay_date": _parse_dt(tp.get("payDate")),
                    "ppn": tp.get("ppn"),
                    "espk": tp.get("espk"),
                    "gu": tp.get("gu"),
                    "fin_source": tp.get("finSource"),
                    "iik_supplier": tp.get("iikSupplier"),
                    "bik_supplier": tp.get("bikSupplier"),
                    "vendor_id": tp.get("vendorId"),
                    "index_date": _parse_dt(tp.get("indexDate")),
                    "system_id": tp.get("systemId"),
                })

            for pay in (item.get("ContractPayment") or []):
                payment_rows.append({
                    "id": pay["id"],
                    "contract_id": pay.get("contractId") or item["id"],
                    "act_id": pay.get("actId"),
                    "pay_sum": _safe_decimal(pay.get("paySum")),
                    "pay_date": _parse_dt(pay.get("payDate")),
                    "ref_payment_type_id": None,
                    "system_id": pay.get("systemId"),
                    "last_update_at": _parse_dt(pay.get("indexDate")),
                })

            for spec in (item.get("ContractSpecSum") or []):
                spec_rows.append({
                    "id": spec["id"],
                    "contract_id": spec.get("contractId") or item["id"],
                    "total_sum": _safe_decimal(spec.get("totalSum")),
                    "fact_sum": _safe_decimal(spec.get("factSum")),
                    "system_id": spec.get("systemId"),
                    "last_update_at": _parse_dt(spec.get("indexDate")),
                })

            for act in (item.get("Acts") or []):
                act_rows.append({
                    "id": act["id"],
                    "contract_id": act.get("contractId") or item["id"],
                    "act_number": act.get("numberAct"),
                    "act_date": _parse_dt(act.get("aktDate")),
                    "sum_act": _safe_decimal(act.get("sumBeginning")),
                    "sum_fine": _safe_decimal(act.get("sumFine")),
                    "day_overdue": act.get("dayOverdue", 0),
                    "ref_act_status_id": act.get("statusId"),
                    "system_id": act.get("systemId"),
                    "last_update_at": _parse_dt(act.get("indexDate")),
                })

        if not contract_rows:
            return 0

        async with AsyncSessionLocal() as db2:
            stmt = pg_insert(Contract).values(contract_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={c: stmt.excluded[c] for c in [
                    "contract_sum_wnds", "fakt_sum", "fakt_exec_date",
                    "ref_contract_status_id", "supplier_iik", "supplier_bik",
                    "exchange_rate", "last_update_at",
                ]},
            )
            await db2.execute(stmt)

            if treasury_rows:
                stmt_tp = pg_insert(TreasuryPay).values(treasury_rows)
                stmt_tp = stmt_tp.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt_tp.excluded[c] for c in ["pay_amount", "prepay_sum", "pay_date", "index_date"]},
                )
                await db2.execute(stmt_tp)

            if payment_rows:
                stmt_pay = pg_insert(ContractPayment).values(payment_rows)
                stmt_pay = stmt_pay.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt_pay.excluded[c] for c in ["pay_sum", "pay_date", "last_update_at"]},
                )
                await db2.execute(stmt_pay)

            if spec_rows:
                stmt_spec = pg_insert(ContractSpecSum).values(spec_rows)
                stmt_spec = stmt_spec.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt_spec.excluded[c] for c in ["fact_sum", "last_update_at"]},
                )
                await db2.execute(stmt_spec)

            if act_rows:
                stmt_act = pg_insert(ContractAct).values(act_rows)
                stmt_act = stmt_act.on_conflict_do_update(
                    index_elements=["id"],
                    set_={c: stmt_act.excluded[c] for c in [
                        "act_date", "sum_act", "sum_fine", "day_overdue",
                        "ref_act_status_id", "last_update_at",
                    ]},
                )
                await db2.execute(stmt_act)

            await db2.commit()
            return len(contract_rows)

    async def _load_contracts(self) -> int:
        from sqlalchemy import select
        async with AsyncSessionLocal() as db:
            df, dt = datetime.fromisoformat(self.date_from), datetime.fromisoformat(self.date_to)
            trd_ids = (await db.execute(select(TrdBuy.id).where(
                TrdBuy.publish_date >= df, TrdBuy.publish_date <= dt
            ))).scalars().all()

        if not trd_ids: return 0
        total_contracts = 0
        use_nested_query = True
        for ch in self._chunks(list(trd_ids), 150):
            query = GQL_CONTRACT if use_nested_query else GQL_CONTRACT_FLAT
            try:
                async for batch in self.client.graphql_paginate(query, "Contract", variables={"filter": {"trdBuyId": ch}}):
                    total_contracts += await self._process_contract_batch(batch)
            except RuntimeError as e:
                if use_nested_query:
                    use_nested_query = False
                    async for batch in self.client.graphql_paginate(GQL_CONTRACT_FLAT, "Contract", variables={"filter": {"trdBuyId": ch}}):
                        total_contracts += await self._process_contract_batch(batch)
        return total_contracts

    def _chunks(self, lst, n):
        for i in range(0, len(lst), n): yield lst[i:i+n]

    async def _load_rnu(self) -> int:
        count = 0
        async for batch in self.client.graphql_paginate(GQL_RNU, "Rnu", variables=None):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "pid": item.get("pid"),
                    "supplier_biin": item.get("supplierBiin"),
                    "supplier_name_ru": item.get("supplierNameRu"),
                    "start_date": _parse_dt(item.get("startDate")),
                    "end_date": _parse_dt(item.get("endDate")),
                    "reason": None,
                    "system_id": item.get("systemId", 3),
                    "is_active": True,
                })
            if rows:
                async with AsyncSessionLocal() as db:
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
        async for batch in self.client.graphql_paginate(GQL_TREASURY_PAY, "TreasuryPay"):
            rows = []
            for item in batch:
                rows.append({
                    "id": item["id"],
                    "nom_za": item.get("nomZa"),
                    "contract_id": item.get("contractId"),
                    "dt_reg": _parse_dt(item.get("dtReg")),
                    "supplier": item.get("supplier"),
                    "rnn_supplier": item.get("rnnSupplier"),
                    "nom_dog": item.get("nomDog"),
                    "dt_dog": _parse_dt(item.get("dtDog")),
                    "item_description": item.get("itemDescription"),
                    "pay_amount": _safe_decimal(item.get("payAmount")),
                    "prepay_sum": _safe_decimal(item.get("prepaySum")),
                    "pay_date": _parse_dt(item.get("payDate")),
                    "ppn": item.get("ppn"),
                    "espk": item.get("espk"),
                    "gu": item.get("gu"),
                    "fin_source": item.get("finSource"),
                    "iik_supplier": item.get("iikSupplier"),
                    "bik_supplier": item.get("bikSupplier"),
                    "vendor_id": item.get("vendorId"),
                    "index_date": _parse_dt(item.get("indexDate")),
                    "system_id": item.get("systemId"),
                })
            if rows:
                async with AsyncSessionLocal() as db:
                    stmt = pg_insert(TreasuryPay).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={"pay_amount": stmt.excluded.pay_amount, "prepay_sum": stmt.excluded.prepay_sum},
                    )
                    await db.execute(stmt)
                    await db.commit()
                    count += len(rows)
                    logger.info(f"TreasuryPay upserted: {count}")
        return count

    async def _load_contract_acts(self) -> int:
        """Load contract acts — gracefully skip if not in schema."""
        count = 0
        try:
            async for batch in self.client.graphql_paginate(GQL_CONTRACT_ACT, "Acts"):
                rows = []
                for item in batch:
                    rows.append({
                        "id": item["id"],
                        "contract_id": item.get("contractId"),
                        "act_number": item.get("numberAct"),
                        "act_date": _parse_dt(item.get("aktDate")),
                        "sum_act": _safe_decimal(item.get("sumBeginning")),
                        "sum_fine": _safe_decimal(item.get("sumFine")),
                        "day_overdue": item.get("dayOverdue", 0),
                        "ref_act_status_id": item.get("statusId"),
                        "system_id": item.get("systemId"),
                        "last_update_at": _parse_dt(item.get("indexDate")),
                    })
                if rows:
                    async with AsyncSessionLocal() as db:
                        stmt = pg_insert(ContractAct).values(rows)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["id"],
                            set_={c: stmt.excluded[c] for c in [
                                "sum_act", "sum_fine", "day_overdue", "last_update_at",
                            ]},
                        )
                        await db.execute(stmt)
                        await db.commit()
                        count += len(rows)
                        logger.info(f"ContractAct upserted: {count}")
        except Exception as e:
            logger.warning(f"ContractAct loading skipped (may not be in schema): {e}")
        return count

    async def _load_contract_payments(self) -> int:
        """Load contract payments — gracefully skip if not in schema."""
        count = 0
        try:
            async for batch in self.client.graphql_paginate(GQL_CONTRACT_PAYMENT, "ContractPayment"):
                rows = []
                for item in batch:
                    rows.append({
                        "id": item["id"],
                        "contract_id": item.get("contractId"),
                        "act_id": item.get("actId"),
                        "pay_sum": _safe_decimal(item.get("paySum")),
                        "pay_date": _parse_dt(item.get("payDate")),
                        "ref_payment_type_id": item.get("refPaymentTypeId"),
                        "system_id": item.get("systemId"),
                        "last_update_at": _parse_dt(item.get("lastUpdateDate")),
                    })
                if rows:
                    async with AsyncSessionLocal() as db:
                        stmt = pg_insert(ContractPayment).values(rows)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["id"],
                            set_={c: stmt.excluded[c] for c in ["pay_sum", "last_update_at"]},
                        )
                        await db.execute(stmt)
                        await db.commit()
                        count += len(rows)
                        logger.info(f"ContractPayment upserted: {count}")
        except Exception as e:
            logger.warning(f"ContractPayment loading skipped (may not be in schema): {e}")
        return count

    async def _load_contract_spec_sums(self) -> int:
        """Load contract spec sums — gracefully skip if not in schema."""
        count = 0
        try:
            async for batch in self.client.graphql_paginate(GQL_CONTRACT_SPEC_SUM, "ContractSpecSum"):
                rows = []
                for item in batch:
                    rows.append({
                        "id": item["id"],
                        "contract_id": item.get("contractId"),
                        "total_sum": _safe_decimal(item.get("totalSum")),
                        "fact_sum": _safe_decimal(item.get("factSum")),
                        "system_id": item.get("systemId"),
                        "last_update_at": _parse_dt(item.get("lastUpdateDate")),
                    })
                if rows:
                    async with AsyncSessionLocal() as db:
                        stmt = pg_insert(ContractSpecSum).values(rows)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["id"],
                            set_={c: stmt.excluded[c] for c in ["fact_sum", "last_update_at"]},
                        )
                        await db.execute(stmt)
                        await db.commit()
                        count += len(rows)
                        logger.info(f"ContractSpecSum upserted: {count}")
        except Exception as e:
            logger.warning(f"ContractSpecSum loading skipped (may not be in schema): {e}")
        return count
