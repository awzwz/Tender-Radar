"""
Company Profile Service
Fetches and aggregates real-time data from OWS API, KGD, court cases,
complaint registry, and internal DB for any company (supplier or customer).
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from app.etl.client import OWSClient
from app.core.config import settings

logger = logging.getLogger(__name__)

# Limits to avoid timeouts on large companies
MAX_CONTRACTS = 300
MAX_TENDERS = 200
MAX_RNU = 20

# ── GraphQL Queries ──────────────────────────────────────────────────────────

GQL_SUBJECT_SEARCH = """
query($limit: Int!, $after: Int, $filter: SubjectFiltersInput) {
  Subjects(limit: $limit, after: $after, filter: $filter) {
    pid
    bin
    iin
    nameRu
    nameKz
    fullNameRu
    regdate
    crdate
    typeSupplier
    markSmallEmployer
    markResident
    markPatronymicProducer
    markNationalCompany
    email
    phone
    website
    customer
    supplier
    okedList
    krpCode
    systemId
    lastUpdateDate
  }
}
"""

GQL_CONTRACTS_FILTERED = """
query($limit: Int!, $after: Int, $filter: ContractFiltersInput) {
  Contract(limit: $limit, after: $after, filter: $filter) {
    id
    contractNumber
    trdBuyNumberAnno
    customerBin
    supplierBiin
    contractSumWnds
    faktSum
    signDate
    planExecDate
    faktExecDate
    refContractStatusId
    refContractTypeId
    supplierIik
    supplierBik
    parentId
    rootId
    systemId
    Acts {
      id
      statusId
      sumFine
      dayOverdue
      aktDate
    }
    ContractSpecSum {
      totalSum
      factSum
    }
    TreasuryPay {
      payAmount
      payDate
    }
  }
}
"""

GQL_TRDBY_FILTERED = """
query($limit: Int!, $after: Int, $filter: TrdBuyFiltersInput) {
  TrdBuy(limit: $limit, after: $after, filter: $filter) {
    id
    numberAnno
    nameRu
    totalSum
    countLots
    refBuyStatusId
    publishDate
    singlOrgSign
    refTradeMethodsId
    systemId
  }
}
"""

GQL_RNU_FILTERED = """
query($limit: Int!, $after: Int, $filter: RnuFiltersInput) {
  Rnu(limit: $limit, after: $after, filter: $filter) {
    id
    supplierBiin
    supplierNameRu
    startDate
    endDate
    systemId
    indexDate
  }
}
"""


# ── Service ──────────────────────────────────────────────────────────────────

class CompanyProfileService:
    """
    Aggregates OWS real-time data, KGD tax info, court cases, and complaints
    into a company profile. One instance per request (stateless).
    """

    def __init__(self):
        self.client = OWSClient()

    async def search(self, query: str) -> list[dict]:
        """
        Search companies by BIN/IIN (12 digits) or by name (partial match).
        Returns up to 20 matches.
        """
        query = query.strip()
        is_bin = query.replace(" ", "").isdigit() and len(query.replace(" ", "")) == 12

        if is_bin:
            bin_clean = query.replace(" ", "")
            filter_val = {"bin": bin_clean}
        else:
            import re as _re
            clean_query = (
                query
                .replace('«', '').replace('»', '')
                .replace('"', '').replace('"', '').replace('"', '')
                .replace("'", '').replace("'", '').replace("'", '')
                .replace('(', '').replace(')', '')
                .strip()
            )
            core_query = _re.sub(
                r'^(АО|ТОО|ИП|ОАО|ЗАО|ТДО|ГКП|РГП|КГП|МКК|АКБ|ПАО|НАО|ГП|ГККП|ГУ|КГУ|ЧП|ПК)\s+',
                '', clean_query, flags=_re.IGNORECASE
            ).strip()
            search_term = core_query if (core_query and core_query != clean_query) else clean_query
            filter_val = {"nameOrFullNameRu": f"{search_term}*"}

        results: list[dict] = []
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_SUBJECT_SEARCH,
                data_key="Subjects",
                max_records=20,
                page_size=20,
                variables={"filter": filter_val},
            ):
                results.extend(batch)
                break
        except Exception as e:
            logger.warning(f"Company search failed for '{query}': {e}")

        return results

    async def get_profile(self, bin_val: str) -> dict:
        """
        Full real-time company profile.
        Fetches data in parallel from OWS, KGD, courts, complaints, and internal DB.
        """
        # Phase 1: All data fetched concurrently
        fetched = await asyncio.gather(
            self._fetch_subject(bin_val),
            self._fetch_contracts_as_supplier(bin_val),
            self._fetch_contracts_as_customer(bin_val),
            self._fetch_tenders(bin_val),
            self._fetch_rnu(bin_val),
            self._fetch_complaints_safe(bin_val),
            self._fetch_kgd_safe(bin_val),
            self._fetch_court_cases_safe(bin_val),
            self._fetch_affiliations_safe(bin_val),
            return_exceptions=True,
        )

        subject = fetched[0] if not isinstance(fetched[0], Exception) else None
        supplier_contracts = fetched[1] if not isinstance(fetched[1], Exception) else []
        customer_contracts = fetched[2] if not isinstance(fetched[2], Exception) else []
        tenders = fetched[3] if not isinstance(fetched[3], Exception) else []
        rnu_records = fetched[4] if not isinstance(fetched[4], Exception) else []
        complaints = fetched[5] if not isinstance(fetched[5], Exception) else {}
        kgd_data = fetched[6] if not isinstance(fetched[6], Exception) else {}
        court_data = fetched[7] if not isinstance(fetched[7], Exception) else {}
        affiliations = fetched[8] if not isinstance(fetched[8], Exception) else {}

        # Log errors
        labels = [
            "subject", "supplier_contracts", "customer_contracts", "tenders",
            "rnu", "complaints", "kgd", "court_cases", "affiliations",
        ]
        for i, label in enumerate(labels):
            if isinstance(fetched[i], Exception):
                logger.warning(f"[company_profile] {label} fetch failed for {bin_val}: {fetched[i]}")

        supplier_metrics = self._compute_supplier_metrics(supplier_contracts)
        customer_metrics = self._compute_customer_metrics(customer_contracts, tenders)
        rnu_status = self._compute_rnu_status(rnu_records)
        overall_risk = self._compute_risk_score(
            supplier_metrics, customer_metrics, rnu_status,
            complaints, kgd_data, court_data, affiliations, subject,
        )

        return {
            "bin": bin_val,
            "fetched_at": datetime.utcnow().isoformat(),
            "subject": subject,
            "rnu": rnu_status,
            "risk": overall_risk,
            "as_supplier": {
                "contracts": supplier_contracts[:50],
                "metrics": supplier_metrics,
            },
            "as_customer": {
                "contracts": customer_contracts[:50],
                "tenders": tenders[:50],
                "metrics": customer_metrics,
            },
            "complaints": complaints,
            "kgd": kgd_data,
            "court_cases": court_data,
            "affiliations": affiliations,
        }

    # ── Private: data fetchers ────────────────────────────────────────────────

    async def _fetch_subject(self, bin_val: str) -> Optional[dict]:
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_SUBJECT_SEARCH,
                data_key="Subjects",
                max_records=1,
                page_size=1,
                variables={"filter": {"bin": bin_val}},
            ):
                if batch:
                    return batch[0]
        except Exception as e:
            logger.warning(f"Subject GraphQL fetch failed {bin_val}: {e}")

        try:
            data = await self.client.fetch_by_id("/v3/subject/biin", bin_val)
            if data and (data.get("bin") or data.get("nameRu")):
                return data
        except Exception as e:
            logger.warning(f"Subject REST fallback failed {bin_val}: {e}")
        return None

    async def _fetch_contracts_as_supplier(self, bin_val: str) -> list[dict]:
        contracts: list[dict] = []
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_CONTRACTS_FILTERED,
                data_key="Contract",
                max_records=MAX_CONTRACTS,
                page_size=50,
                variables={"filter": {"supplierBiin": bin_val}},
            ):
                contracts.extend(batch)
        except Exception as e:
            logger.warning(f"Supplier contracts fetch failed {bin_val}: {e}")
            raise
        return contracts

    async def _fetch_contracts_as_customer(self, bin_val: str) -> list[dict]:
        contracts: list[dict] = []
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_CONTRACTS_FILTERED,
                data_key="Contract",
                max_records=MAX_CONTRACTS,
                page_size=50,
                variables={"filter": {"customerBin": bin_val}},
            ):
                contracts.extend(batch)
        except Exception as e:
            logger.warning(f"Customer contracts fetch failed {bin_val}: {e}")
            raise
        return contracts

    async def _fetch_tenders(self, bin_val: str) -> list[dict]:
        tenders: list[dict] = []
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_TRDBY_FILTERED,
                data_key="TrdBuy",
                max_records=MAX_TENDERS,
                page_size=50,
                variables={"filter": {"orgBin": bin_val}},
            ):
                tenders.extend(batch)
        except Exception as e:
            logger.warning(f"Tenders fetch failed {bin_val}: {e}")
            raise
        return tenders

    async def _fetch_rnu(self, bin_val: str) -> list[dict]:
        records: list[dict] = []
        try:
            async for batch, _ in self.client.graphql_paginate(
                GQL_RNU_FILTERED,
                data_key="Rnu",
                max_records=MAX_RNU,
                page_size=MAX_RNU,
                variables={"filter": {"supplierBiin": bin_val}},
            ):
                records.extend(batch)
        except Exception as e:
            logger.warning(f"RNU fetch failed {bin_val}: {e}")
            raise
        return records

    # ── New external sources (safe wrappers) ─────────────────────────────────

    async def _fetch_complaints_safe(self, bin_val: str) -> dict:
        """Fetch complaints with graceful failure."""
        try:
            from app.services.complaints import ComplaintsService
            svc = ComplaintsService()
            return await svc.fetch_complaints(bin_val)
        except Exception as e:
            logger.warning(f"Complaints service failed for {bin_val}: {e}")
            return {}

    async def _fetch_kgd_safe(self, bin_val: str) -> dict:
        """Fetch KGD tax data with graceful failure."""
        try:
            from app.services.kgd import KGDService
            svc = KGDService()
            return await svc.fetch_tax_info(bin_val)
        except Exception as e:
            logger.warning(f"KGD service failed for {bin_val}: {e}")
            return {}

    async def _fetch_court_cases_safe(self, bin_val: str) -> dict:
        """Fetch court cases with graceful failure."""
        try:
            from app.services.court_cases import CourtCasesService
            svc = CourtCasesService()
            return await svc.fetch_court_data(bin_val, analyze_with_llm=True)
        except Exception as e:
            logger.warning(f"Court cases service failed for {bin_val}: {e}")
            return {}

    async def _fetch_affiliations_safe(self, bin_val: str) -> dict:
        """Check for affiliated companies via shared bank accounts, contacts, co-bidding."""
        try:
            return await self._check_affiliations(bin_val)
        except Exception as e:
            logger.warning(f"Affiliations check failed for {bin_val}: {e}")
            return {}

    # ── Affiliation checking (Block 3) ────────────────────────────────────────

    async def _check_affiliations(self, bin_val: str) -> dict:
        """
        Cross-check for affiliated companies using internal DB.
        Checks: shared bank accounts (IIK), shared contacts (phone/email), co-bidding.
        """
        from app.core.database import AsyncSessionLocal
        from sqlalchemy import text

        result = {
            "shared_bank_accounts": [],
            "shared_contacts": [],
            "cobid_partners": [],
            "total_links": 0,
        }

        try:
            async with AsyncSessionLocal() as db:
                # 1. Shared bank accounts (IIK)
                iik_query = text("""
                    SELECT DISTINCT c2.supplier_biin, s.name_ru, c1.supplier_iik
                    FROM contract c1
                    JOIN contract c2 ON c1.supplier_iik = c2.supplier_iik
                        AND c1.supplier_biin != c2.supplier_biin
                    LEFT JOIN subject s ON s.bin = c2.supplier_biin
                    WHERE c1.supplier_biin = :bin
                        AND c1.supplier_iik IS NOT NULL
                        AND c1.supplier_iik != ''
                        AND c1.is_deleted = false
                        AND c2.is_deleted = false
                    LIMIT 20
                """)
                rows = (await db.execute(iik_query, {"bin": bin_val})).all()
                result["shared_bank_accounts"] = [
                    {"bin": r[0], "name": r[1] or "", "iik": r[2] or "", "link_type": "bank_account"}
                    for r in rows
                ]

                # 2. Shared contacts (phone/email)
                contacts_query = text("""
                    SELECT DISTINCT s2.bin, s2.name_ru,
                        CASE
                            WHEN s1.phone IS NOT NULL AND s1.phone != '' AND s1.phone = s2.phone THEN 'phone'
                            WHEN s1.email IS NOT NULL AND s1.email != '' AND LOWER(s1.email) = LOWER(s2.email) THEN 'email'
                        END as match_type,
                        COALESCE(s1.phone, s1.email) as shared_value
                    FROM subject s1
                    JOIN subject s2 ON s1.bin != s2.bin
                        AND (
                            (s1.phone IS NOT NULL AND s1.phone != '' AND s1.phone = s2.phone)
                            OR (s1.email IS NOT NULL AND s1.email != '' AND LOWER(s1.email) = LOWER(s2.email))
                        )
                    WHERE s1.bin = :bin
                    LIMIT 20
                """)
                rows = (await db.execute(contacts_query, {"bin": bin_val})).all()
                result["shared_contacts"] = [
                    {"bin": r[0], "name": r[1] or "", "match_type": r[2] or "", "shared_value": r[3] or "", "link_type": "contact"}
                    for r in rows
                ]

                # 3. Co-bidding partners (from graph_features or direct computation)
                cobid_query = text("""
                    SELECT a2.supplier_biin, s.name_ru, COUNT(DISTINCT a1.buy_id) as times_together
                    FROM trd_app a1
                    JOIN trd_app a2 ON a1.buy_id = a2.buy_id
                        AND a1.supplier_biin != a2.supplier_biin
                    LEFT JOIN subject s ON s.bin = a2.supplier_biin
                    WHERE a1.supplier_biin = :bin
                    GROUP BY a2.supplier_biin, s.name_ru
                    HAVING COUNT(DISTINCT a1.buy_id) >= 3
                    ORDER BY times_together DESC
                    LIMIT 10
                """)
                rows = (await db.execute(cobid_query, {"bin": bin_val})).all()
                result["cobid_partners"] = [
                    {"bin": r[0], "name": r[1] or "", "times_together": r[2], "link_type": "cobid"}
                    for r in rows
                ]

                result["total_links"] = (
                    len(result["shared_bank_accounts"])
                    + len(result["shared_contacts"])
                    + len(result["cobid_partners"])
                )
        except Exception as e:
            logger.warning(f"Affiliation SQL queries failed for {bin_val}: {e}")

        return result

    # ── Private: metrics computation ──────────────────────────────────────────

    def _compute_supplier_metrics(self, contracts: list[dict]) -> dict:
        if not contracts:
            return {
                "total_contracts": 0, "total_sum": 0.0, "executed_sum": 0.0,
                "execution_rate": 0.0, "unique_customers": 0, "top_customers": [],
                "by_year": {}, "overdue_count": 0, "fines_count": 0,
                "avg_contract_size": 0.0, "treasury_paid": 0.0,
                "avg_overdue_days": 0.0, "addendum_count": 0, "addendum_rate": 0.0,
            }

        total_sum = sum(float(c.get("contractSumWnds") or 0) for c in contracts)
        executed_sum = sum(float(c.get("faktSum") or 0) for c in contracts)
        treasury_paid = sum(
            sum(float(p.get("payAmount") or 0) for p in (c.get("TreasuryPay") or []))
            for c in contracts
        )

        by_year: dict[str, dict] = {}
        customer_stats: dict[str, dict] = {}
        overdue_count = 0
        fines_count = 0
        total_overdue_days = 0
        overdue_acts = 0
        addendum_count = 0

        for c in contracts:
            sign_date = c.get("signDate") or ""
            year = sign_date[:4] if len(sign_date) >= 4 else "unknown"
            if year not in by_year:
                by_year[year] = {"count": 0, "sum": 0.0}
            by_year[year]["count"] += 1
            by_year[year]["sum"] = round(by_year[year]["sum"] + float(c.get("contractSumWnds") or 0), 2)

            cbin = c.get("customerBin") or ""
            if cbin:
                if cbin not in customer_stats:
                    customer_stats[cbin] = {"count": 0, "sum": 0.0}
                customer_stats[cbin]["count"] += 1
                customer_stats[cbin]["sum"] = round(customer_stats[cbin]["sum"] + float(c.get("contractSumWnds") or 0), 2)

            for act in (c.get("Acts") or []):
                days = act.get("dayOverdue") or 0
                if days > 0:
                    overdue_count += 1
                    total_overdue_days += days
                    overdue_acts += 1
                if float(act.get("sumFine") or 0) > 0:
                    fines_count += 1

            # Addendums: contract with parentId = addendum to original
            if c.get("parentId"):
                addendum_count += 1

        avg_overdue_days = round(total_overdue_days / overdue_acts, 1) if overdue_acts > 0 else 0.0
        addendum_rate = round(addendum_count / len(contracts) * 100, 1) if contracts else 0.0
        top_customers = sorted(customer_stats.items(), key=lambda x: x[1]["sum"], reverse=True)[:5]

        return {
            "total_contracts": len(contracts),
            "total_sum": round(total_sum, 2),
            "executed_sum": round(executed_sum, 2),
            "execution_rate": round(executed_sum / total_sum * 100 if total_sum > 0 else 0.0, 1),
            "unique_customers": len(customer_stats),
            "top_customers": [
                {"bin": k, "count": v["count"], "sum": v["sum"]}
                for k, v in top_customers
            ],
            "by_year": {k: v for k, v in sorted(by_year.items()) if k != "unknown"},
            "overdue_count": overdue_count,
            "fines_count": fines_count,
            "avg_contract_size": round(total_sum / len(contracts), 2),
            "treasury_paid": round(treasury_paid, 2),
            "avg_overdue_days": avg_overdue_days,
            "addendum_count": addendum_count,
            "addendum_rate": addendum_rate,
        }

    def _compute_customer_metrics(self, contracts: list[dict], tenders: list[dict]) -> dict:
        if not contracts and not tenders:
            return {
                "total_tenders": 0, "total_contracts": 0,
                "total_procurement_sum": 0.0, "unique_suppliers": 0,
                "top_suppliers": [], "by_year": {},
                "single_source_count": 0, "single_source_rate": 0.0,
                "cancelled_tenders": 0,
            }

        total_sum = sum(float(c.get("contractSumWnds") or 0) for c in contracts)
        single_source_count = sum(1 for t in tenders if t.get("singlOrgSign"))
        single_source_rate = round(single_source_count / len(tenders) * 100 if tenders else 0.0, 1)
        cancelled_tenders = sum(1 for t in tenders if t.get("refBuyStatusId") == 130)

        supplier_stats: dict[str, dict] = {}
        by_year: dict[str, dict] = {}

        for c in contracts:
            sbiin = c.get("supplierBiin") or ""
            if sbiin:
                if sbiin not in supplier_stats:
                    supplier_stats[sbiin] = {"count": 0, "sum": 0.0}
                supplier_stats[sbiin]["count"] += 1
                supplier_stats[sbiin]["sum"] = round(supplier_stats[sbiin]["sum"] + float(c.get("contractSumWnds") or 0), 2)

            sign_date = c.get("signDate") or ""
            year = sign_date[:4] if len(sign_date) >= 4 else "unknown"
            if year not in by_year:
                by_year[year] = {"count": 0, "sum": 0.0}
            by_year[year]["count"] += 1
            by_year[year]["sum"] = round(by_year[year]["sum"] + float(c.get("contractSumWnds") or 0), 2)

        top_suppliers = sorted(supplier_stats.items(), key=lambda x: x[1]["sum"], reverse=True)[:5]

        return {
            "total_tenders": len(tenders),
            "total_contracts": len(contracts),
            "total_procurement_sum": round(total_sum, 2),
            "unique_suppliers": len(supplier_stats),
            "top_suppliers": [
                {"bin": k, "count": v["count"], "sum": v["sum"]}
                for k, v in top_suppliers
            ],
            "by_year": {k: v for k, v in sorted(by_year.items()) if k != "unknown"},
            "single_source_count": single_source_count,
            "single_source_rate": single_source_rate,
            "cancelled_tenders": cancelled_tenders,
        }

    def _compute_rnu_status(self, records: list[dict]) -> dict:
        now = datetime.utcnow().date()
        active: list[dict] = []

        for r in records:
            end_date = r.get("endDate")
            if not end_date:
                active.append(r)
                continue
            try:
                end = datetime.strptime(str(end_date)[:10], "%Y-%m-%d").date()
                if end >= now:
                    active.append(r)
            except (ValueError, TypeError):
                active.append(r)

        return {
            "is_blacklisted": len(active) > 0,
            "active_count": len(active),
            "records": records,
        }

    def _compute_risk_score(
        self,
        supplier: dict,
        customer: dict,
        rnu: dict,
        complaints: dict = None,
        kgd: dict = None,
        court: dict = None,
        affiliations: dict = None,
        subject: dict = None,
    ) -> dict:
        """
        Rule-based risk score (0-100) from all data sources.
        Original checks + new checks with soft weights.
        """
        score = 0
        flags: list[dict] = []

        def add_flag(code: str, points: int, detail: str = ""):
            nonlocal score
            score += points
            flags.append({"code": code, "points": points, "detail": detail})

        # ── Original checks (unchanged weights) ──────────────────────────────

        # Hard flags
        if rnu.get("is_blacklisted"):
            add_flag("BLACKLISTED", 40, f"Активных записей РНУ: {rnu.get('active_count', 0)}")

        # Supplier signals
        s = supplier
        if s.get("total_contracts", 0) > 0:
            if s.get("execution_rate", 100) < 50:
                add_flag("LOW_EXECUTION_RATE", 15,
                         f"Исполнение: {s.get('execution_rate', 0)}%")
            if s.get("overdue_count", 0) > 0:
                add_flag("OVERDUE_ACTS", 10,
                         f"Просрочек: {s.get('overdue_count', 0)}")
            if s.get("fines_count", 0) > 0:
                add_flag("FINES_PRESENT", 8,
                         f"Штрафов: {s.get('fines_count', 0)}")
            top_c = s.get("top_customers", [])
            total = s.get("total_sum", 0)
            if top_c and total > 0:
                top_share = top_c[0]["sum"] / total * 100
                if top_share > 80:
                    add_flag("HIGH_CUSTOMER_CONCENTRATION", 10,
                             f"Топ заказчик: {round(top_share, 1)}% выручки")

        # Customer signals
        c = customer
        if c.get("total_tenders", 0) > 0:
            if c.get("single_source_rate", 0) > 50:
                add_flag("HIGH_SINGLE_SOURCE_RATE", 12,
                         f"Единственный источник: {c.get('single_source_rate', 0)}%")
            if c.get("cancelled_tenders", 0) > 5:
                add_flag("MANY_CANCELLED_TENDERS", 5,
                         f"Отменённых: {c.get('cancelled_tenders', 0)}")
            top_s = c.get("top_suppliers", [])
            total_p = c.get("total_procurement_sum", 0)
            if top_s and total_p > 0:
                top_share = top_s[0]["sum"] / total_p * 100
                if top_share > 70:
                    add_flag("HIGH_SUPPLIER_CONCENTRATION", 10,
                             f"Топ поставщик: {round(top_share, 1)}% закупок")

        # ── Block 2: New OWS checks (soft weights) ───────────────────────────

        # Average overdue days
        if s.get("avg_overdue_days", 0) > 30:
            add_flag("AVG_OVERDUE_DAYS", 4,
                     f"Средняя просрочка: {s.get('avg_overdue_days', 0)} дней")

        # Volume spike (>3x year-over-year)
        by_year = s.get("by_year", {})
        years_sorted = sorted(by_year.keys())
        if len(years_sorted) >= 2:
            prev_sum = by_year[years_sorted[-2]].get("sum", 0)
            curr_sum = by_year[years_sorted[-1]].get("sum", 0)
            if prev_sum > 0 and curr_sum / prev_sum > 3:
                add_flag("VOLUME_SPIKE", 5,
                         f"Рост объёма: {round(curr_sum / prev_sum, 1)}x за год")

        # Young company + big volume
        if subject and subject.get("regdate"):
            try:
                reg = datetime.strptime(str(subject["regdate"])[:10], "%Y-%m-%d").date()
                age_days = (datetime.utcnow().date() - reg).days
                total_vol = s.get("total_sum", 0) + c.get("total_procurement_sum", 0)
                if age_days < 730 and total_vol > 100_000_000:
                    add_flag("YOUNG_COMPANY_BIG_VOLUME", 6,
                             f"Возраст: {age_days} дней, объём: {round(total_vol/1e6, 1)}M ₸")
            except (ValueError, TypeError):
                pass

        # Diverse OKED (>5 different activity codes)
        if subject and subject.get("okedList"):
            oked_str = str(subject.get("okedList") or "")
            oked_codes = [o.strip() for o in oked_str.split(",") if o.strip()]
            if len(oked_codes) > 5:
                add_flag("DIVERSE_OKED", 3,
                         f"ОКЭД кодов: {len(oked_codes)}")

        # High addendum rate
        if s.get("addendum_rate", 0) > 30 and s.get("total_contracts", 0) >= 5:
            add_flag("HIGH_ADDENDUM_RATE", 5,
                     f"Дополнений: {s.get('addendum_rate', 0)}% контрактов")

        # ── Block 1: Complaints ──────────────────────────────────────────────

        if complaints and complaints.get("total", 0) > 0:
            if complaints.get("complaints_as_customer", 0) >= 3:
                add_flag("COMPLAINTS_ON_PURCHASES", 5,
                         f"Жалоб на закупки: {complaints.get('complaints_as_customer', 0)}")
            # Use LLM-analyzed score_points if available, otherwise fallback to satisfaction rate
            complaint_score = complaints.get("score_points", 0)
            if complaint_score > 0:
                add_flag("SATISFIED_COMPLAINTS_RISK", complaint_score,
                         f"Удовлетворённые жалобы: {complaints.get('satisfied_count', 0)}, "
                         f"LLM-оценка серьёзности: {complaint_score} баллов")
            elif complaints.get("satisfaction_rate", 0) > 50 and complaints.get("total", 0) >= 3:
                add_flag("HIGH_COMPLAINT_SATISFACTION_RATE", 8,
                         f"Удовлетворено: {complaints.get('satisfaction_rate', 0)}% жалоб")

        # ── Block 3: Affiliations ────────────────────────────────────────────

        if affiliations:
            if affiliations.get("shared_bank_accounts"):
                count = len(affiliations["shared_bank_accounts"])
                bins = ", ".join(a["bin"] for a in affiliations["shared_bank_accounts"][:3])
                add_flag("SHARED_BANK_ACCOUNT", 9,
                         f"Общий счёт с {count} компаниями: {bins}")
            if affiliations.get("shared_contacts"):
                count = len(affiliations["shared_contacts"])
                add_flag("SHARED_CONTACTS", 6,
                         f"Общие контакты с {count} компаниями")
            if len(affiliations.get("cobid_partners", [])) >= 3:
                count = len(affiliations["cobid_partners"])
                add_flag("FREQUENT_COBIDDERS", 5,
                         f"Частых co-bidding партнёров: {count}")

        # ── Block 4: Tax data (ba.prg.kz) ──────────────────────────────────

        if kgd and kgd.get("available"):
            # LLM-based tax analysis score
            tax_score = kgd.get("score_points", 0)
            if tax_score > 0:
                llm_analysis = kgd.get("llm_analysis") or {}
                summary = llm_analysis.get("summary", "")[:100]
                add_flag("TAX_ANOMALY", tax_score,
                         f"LLM-анализ налогов: {summary}" if summary else f"Налоговые аномалии: {tax_score} баллов")

        # ── Block 5: Court cases ─────────────────────────────────────────────

        if court:
            court_score = court.get("score_points", 0)
            if court_score > 0:
                add_flag("COURT_CASES_RISK", court_score,
                         f"LLM-оценка судебных дел: impact={court.get('avg_reliability_impact', 0)}/10")
            if court.get("as_defendant", 0) > 5:
                add_flag("MANY_COURT_CASES", 4,
                         f"Ответчик в {court.get('as_defendant', 0)} делах")

        # ── Final score ──────────────────────────────────────────────────────

        score = min(score, 100)
        level = "HIGH" if score >= 45 else "MEDIUM" if score >= 20 else "LOW"

        return {
            "score": score,
            "level": level,
            "flags": [f["code"] for f in flags],
            "flags_detail": flags,
        }
