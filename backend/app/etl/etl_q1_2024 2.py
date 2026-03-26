"""
ETL Q1 2024: load lots, contracts, trd_app for tenders with publish_date in 2024-01-01..2024-03-31.
Uses EtlCursor checkpoints so the job can resume after failure.
Does not load TrdBuy (assumed already in DB). One-time subject top-up for missing BINs.
"""
import logging
from datetime import datetime
from typing import Any
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.etl.client import (
    OWSClient,
    GQL_LOTS,
    GQL_TRD_APP,
    GQL_CONTRACT,
    GQL_CONTRACT_FLAT,
    GQL_SUBJECT,
)
from app.etl.backfill import _parse_dt, _safe_decimal
from app.models.procurement import (
    TrdBuy,
    Lot,
    TrdApp,
    TrdAppLot,
    Contract,
    Subject,
    TreasuryPay,
    ContractPayment,
    ContractSpecSum,
    ContractAct,
    EtlCursor,
)
from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

DATE_FROM = "2024-01-01"
DATE_TO = "2024-03-31"
CURSOR_LOTS = "q1_2024_lots"
CURSOR_CONTRACTS = "q1_2024_contracts"
CURSOR_TRD_APP = "q1_2024_trd_app"
CHUNK_LOTS = 100
CHUNK_CONTRACTS = 150
CHUNK_TRD_APP = 100


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def _get_cursor(source_name: str) -> int:
    async with AsyncSessionLocal() as db:
        r = await db.execute(
            select(EtlCursor.cursor_value).where(EtlCursor.source_name == source_name)
        )
        row = r.scalar_one_or_none()
    if not row:
        return 0
    try:
        return int(row)
    except (TypeError, ValueError):
        return 0


async def _set_cursor(source_name: str, value: int) -> None:
    async with AsyncSessionLocal() as db:
        stmt = pg_insert(EtlCursor).values(
            source_name=source_name,
            cursor_value=str(value),
            updated_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_name"],
            set_={"cursor_value": str(value), "updated_at": datetime.utcnow()},
        )
        await db.execute(stmt)
        await db.commit()


async def _q1_buy_ids() -> list[int]:
    async with AsyncSessionLocal() as db:
        df = datetime.fromisoformat(DATE_FROM)
        dt = datetime.fromisoformat(DATE_TO)
        r = await db.execute(
            select(TrdBuy.id).where(
                and_(
                    TrdBuy.publish_date >= df,
                    TrdBuy.publish_date <= dt,
                )
            )
        )
        ids = [row[0] for row in r.all()]
    return ids


async def _process_lots_batch(batch: list[dict]) -> int:
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
    if not rows:
        return 0
    async with AsyncSessionLocal() as db:
        stmt = pg_insert(Lot).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "amount": stmt.excluded.amount,
                "ref_lot_status_id": stmt.excluded.ref_lot_status_id,
                "dumping_flag": stmt.excluded.dumping_flag,
                "last_update_at": stmt.excluded.last_update_at,
            },
        )
        await db.execute(stmt)
        await db.commit()
    return len(rows)


async def _process_trd_app_batch(batch: list[dict]) -> int:
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
    if not app_rows:
        return 0
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
                set_={
                    "status_id": stmt_al.excluded.status_id,
                    "price": stmt_al.excluded.price,
                    "amount": stmt_al.excluded.amount,
                },
            )
            await db.execute(stmt_al)
        await db.commit()
    return len(app_rows)


async def _process_contract_batch(batch: list[dict]) -> int:
    contract_rows = []
    treasury_rows = []
    payment_rows = []
    spec_rows = []
    act_rows = []

    for item in batch:
        contract_rows.append({
            "id": item["id"],
            "trd_buy_id": item.get("trdBuyId"),
            "contract_number": item.get("contractNumber"),
            "contract_number_sys": item.get("contractNumberSys"),
            "trd_buy_number_anno": item.get("trdBuyNumberAnno"),
            "customer_bin": item.get("customerBin"),
            "supplier_biin": item.get("supplierBiin"),
            "contract_sum_wnds": _safe_decimal(item.get("contractSumWnds")),
            "sign_date": _parse_dt(item.get("signDate")),
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

    async with AsyncSessionLocal() as db:
        stmt = pg_insert(Contract).values(contract_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "contract_sum_wnds": stmt.excluded.contract_sum_wnds,
                "fakt_sum": stmt.excluded.fakt_sum,
                "fakt_exec_date": stmt.excluded.fakt_exec_date,
                "ref_contract_status_id": stmt.excluded.ref_contract_status_id,
                "supplier_iik": stmt.excluded.supplier_iik,
                "supplier_bik": stmt.excluded.supplier_bik,
                "exchange_rate": stmt.excluded.exchange_rate,
                "last_update_at": stmt.excluded.last_update_at,
            },
        )
        await db.execute(stmt)
        if treasury_rows:
            stmt_tp = pg_insert(TreasuryPay).values(treasury_rows)
            stmt_tp = stmt_tp.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "pay_amount": stmt_tp.excluded.pay_amount,
                    "prepay_sum": stmt_tp.excluded.prepay_sum,
                    "pay_date": stmt_tp.excluded.pay_date,
                    "index_date": stmt_tp.excluded.index_date,
                },
            )
            await db.execute(stmt_tp)
        if payment_rows:
            stmt_pay = pg_insert(ContractPayment).values(payment_rows)
            stmt_pay = stmt_pay.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "pay_sum": stmt_pay.excluded.pay_sum,
                    "pay_date": stmt_pay.excluded.pay_date,
                    "last_update_at": stmt_pay.excluded.last_update_at,
                },
            )
            await db.execute(stmt_pay)
        if spec_rows:
            stmt_spec = pg_insert(ContractSpecSum).values(spec_rows)
            stmt_spec = stmt_spec.on_conflict_do_update(
                index_elements=["id"],
                set_={"fact_sum": stmt_spec.excluded.fact_sum, "last_update_at": stmt_spec.excluded.last_update_at},
            )
            await db.execute(stmt_spec)
        if act_rows:
            stmt_act = pg_insert(ContractAct).values(act_rows)
            stmt_act = stmt_act.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "act_date": stmt_act.excluded.act_date,
                    "sum_act": stmt_act.excluded.sum_act,
                    "sum_fine": stmt_act.excluded.sum_fine,
                    "day_overdue": stmt_act.excluded.day_overdue,
                    "ref_act_status_id": stmt_act.excluded.ref_act_status_id,
                    "last_update_at": stmt_act.excluded.last_update_at,
                },
            )
            await db.execute(stmt_act)
        await db.commit()
    return len(contract_rows)


async def _load_lots(client: OWSClient, buy_ids: list[int]) -> int:
    start = await _get_cursor(CURSOR_LOTS)
    chunks_list = list(_chunks(buy_ids, CHUNK_LOTS))
    total = 0
    for i in range(start, len(chunks_list)):
        ch = chunks_list[i]
        async for batch, _ in client.graphql_paginate(
            GQL_LOTS, "Lots", variables={"filter": {"trdBuyId": ch}}
        ):
            total += await _process_lots_batch(batch)
        await _set_cursor(CURSOR_LOTS, i + 1)
        if (i + 1) % 50 == 0:
            logger.info("Q1 2024 lots: chunk %s/%s, total upserted %s", i + 1, len(chunks_list), total)
    return total


async def _load_contracts(client: OWSClient, buy_ids: list[int]) -> int:
    start = await _get_cursor(CURSOR_CONTRACTS)
    chunks_list = list(_chunks(buy_ids, CHUNK_CONTRACTS))
    total = 0
    use_nested = True
    for i in range(start, len(chunks_list)):
        ch = chunks_list[i]
        query = GQL_CONTRACT if use_nested else GQL_CONTRACT_FLAT
        try:
            async for batch, _ in client.graphql_paginate(
                query, "Contract", variables={"filter": {"trdBuyId": ch}}
            ):
                total += await _process_contract_batch(batch)
        except Exception as e:
            if use_nested:
                use_nested = False
                async for batch, _ in client.graphql_paginate(
                    GQL_CONTRACT_FLAT, "Contract", variables={"filter": {"trdBuyId": ch}}
                ):
                    total += await _process_contract_batch(batch)
            else:
                raise
        await _set_cursor(CURSOR_CONTRACTS, i + 1)
        if (i + 1) % 30 == 0:
            logger.info("Q1 2024 contracts: chunk %s/%s, total %s", i + 1, len(chunks_list), total)
    return total


async def _load_trd_app(client: OWSClient, buy_ids: list[int]) -> int:
    start = await _get_cursor(CURSOR_TRD_APP)
    chunks_list = list(_chunks(buy_ids, CHUNK_TRD_APP))
    total = 0
    for i in range(start, len(chunks_list)):
        ch = chunks_list[i]
        async for batch, _ in client.graphql_paginate(
            GQL_TRD_APP, "TrdApp", variables={"filter": {"buyId": ch}}
        ):
            total += await _process_trd_app_batch(batch)
        await _set_cursor(CURSOR_TRD_APP, i + 1)
        if (i + 1) % 50 == 0:
            logger.info("Q1 2024 trd_app: chunk %s/%s, total %s", i + 1, len(chunks_list), total)
    return total


async def _subject_topup(client: OWSClient, buy_ids: list[int]) -> int:
    """One-time: load Subjects for BINs referenced in lots/contracts but missing from subject table."""
    async with AsyncSessionLocal() as db:
        subq_lot = select(Lot.customer_bin).where(Lot.trd_buy_id.in_(buy_ids)).where(Lot.customer_bin.isnot(None))
        subq_contract = (
            select(Contract.customer_bin).where(Contract.trd_buy_id.in_(buy_ids)).where(Contract.customer_bin.isnot(None))
            .union(
                select(Contract.supplier_biin).where(Contract.trd_buy_id.in_(buy_ids)).where(Contract.supplier_biin.isnot(None))
            )
        )
        r = await db.execute(subq_lot.union(subq_contract))
        ref_bins = {row[0] for row in r.all() if row[0]}
        if not ref_bins:
            return 0
        r2 = await db.execute(select(Subject.bin).where(Subject.bin.in_(ref_bins)))
        existing = {row[0] for row in r2.all()}
    missing = list(ref_bins - existing)[:2000]
    if not missing:
        return 0
    count = 0
    async for batch, _ in client.graphql_paginate(GQL_SUBJECT, "Subjects", max_records=50_000):
        rows = []
        for item in batch:
            bin_val = item.get("bin")
            if bin_val not in missing:
                continue
            rows.append({
                "id": item["pid"],
                "bin": bin_val,
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
            async with AsyncSessionLocal() as db2:
                stmt = pg_insert(Subject).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "name_ru": stmt.excluded.name_ru,
                        "regdate": stmt.excluded.regdate,
                        "crdate": stmt.excluded.crdate,
                        "mark_small_employer": stmt.excluded.mark_small_employer,
                        "mark_resident": stmt.excluded.mark_resident,
                        "email": stmt.excluded.email,
                        "phone": stmt.excluded.phone,
                        "last_update_at": stmt.excluded.last_update_at,
                    },
                )
                await db2.execute(stmt)
                await db2.commit()
                count += len(rows)
    return count


async def run_q1_2024_etl(step: str = "all") -> dict:
    """
    step: "all" | "lots" | "contracts" | "trd_app" | "subject"
    """
    client = OWSClient()
    buy_ids = await _q1_buy_ids()
    logger.info("Q1 2024 ETL: %s tenders (publish_date %s..%s)", len(buy_ids), DATE_FROM, DATE_TO)
    if not buy_ids:
        return {"tenders": 0, "message": "No Q1 2024 tenders in DB"}

    summary = {"tenders": len(buy_ids)}

    if step in ("all", "lots"):
        summary["lots"] = await _load_lots(client, buy_ids)
    if step in ("all", "contracts"):
        summary["contracts"] = await _load_contracts(client, buy_ids)
    if step in ("all", "trd_app"):
        summary["trd_app"] = await _load_trd_app(client, buy_ids)
    if step in ("all", "subject"):
        summary["subject_topup"] = await _subject_topup(client, buy_ids)

    return summary
