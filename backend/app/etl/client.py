import asyncio
import logging
from typing import Any, AsyncGenerator, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.core.config import settings

logger = logging.getLogger(__name__)


# ─── GraphQL Query Templates ────────────────────────────────────────────────

GQL_TRD_BUY = """
query($limit: Int!, $after: Int, $filter: TrdBuyFiltersInput) {
  TrdBuy(limit: $limit, after: $after, filter: $filter) {
    id
    numberAnno
    nameRu
    nameKz
    refTradeMethodsId
    publishDate
    startDate
    endDate
    totalSum
    countLots
    refBuyStatusId
    orgBin
    systemId
    singlOrgSign
    isLightIndustry
    isConstructionWork
    parentId
    repeatStartDate
    repeatEndDate
    discusStartDate
    discusEndDate
    lastUpdateDate
    indexDate
  }
}
"""

GQL_LOTS = """
query($limit: Int!, $after: Int, $filter: LotsFiltersInput) {
  Lots(limit: $limit, after: $after, filter: $filter) {
    id
    trdBuyId
    lotNumber
    nameRu
    nameKz
    amount
    customerBin
    customerNameRu
    dumping
    unionLots
    refLotStatusId
    singlOrgSign
    isLightIndustry
    isConstructionWork
    disablePersonId
    systemId
    indexDate
  }
}
"""

GQL_TRD_APP = """
query($limit: Int!, $after: Int, $filter: TrdAppFiltersInput) {
  TrdApp(limit: $limit, after: $after, filter: $filter) {
    id
    buyId
    supplierId
    supplierBinIin
    crFio
    modFio
    protId
    protNumber
    dateApply
    systemId
    indexDate
    AppLots {
      id
      lotId
      statusId
      price
      amount
      discountValue
      discountPrice
    }
  }
}
"""

GQL_CONTRACT = """
query($limit: Int!, $after: Int, $filter: ContractFiltersInput) {
  Contract(limit: $limit, after: $after, filter: $filter) {
    id
    trdBuyId
    contractNumber
    contractNumberSys
    trdBuyNumberAnno
    customerBin
    supplierBiin
    contractSumWnds
    signDate
    planExecDate
    faktExecDate
    faktSum
    refContractStatusId
    refContractTypeId
    parentId
    rootId
    supplierLegalAddress
    customerLegalAddress
    supplierIik
    supplierBik
    isGu
    exchangeRate
    systemId
    lastUpdateDate
    indexDate
    TreasuryPay {
      id
      nomZa
      contractId
      dtReg
      supplier
      rnnSupplier
      nomDog
      dtDog
      itemDescription
      payAmount
      prepaySum
      payDate
      ppn
      espk
      gu
      finSource
      iikSupplier
      bikSupplier
      vendorId
      systemId
      indexDate
    }
    ContractPayment {
      id
      contractId
      actId
      paySum
      payDate
      systemId
    }
    ContractSpecSum {
      id
      contractId
      totalSum
      factSum
      systemId
    }
    Acts {
      id
      contractId
      numberAct
      aktDate
      sumBeginning
      sumFine
      dayOverdue
      statusId
      systemId
      indexDate
    }
  }
}
"""

# Fallback contract query without nested entities (if schema doesn't support nesting)
GQL_CONTRACT_FLAT = """
query($limit: Int!, $after: Int, $filter: ContractFiltersInput) {
  Contract(limit: $limit, after: $after, filter: $filter) {
    id
    trdBuyId
    contractNumber
    contractNumberSys
    trdBuyNumberAnno
    customerBin
    supplierBiin
    contractSumWnds
    signDate
    planExecDate
    faktExecDate
    faktSum
    refContractStatusId
    refContractTypeId
    parentId
    rootId
    supplierLegalAddress
    customerLegalAddress
    supplierIik
    supplierBik
    isGu
    exchangeRate
    systemId
    lastUpdateDate
    indexDate
  }
}
"""

GQL_SUBJECT = """
query($limit: Int!, $after: Int) {
  Subjects(limit: $limit, after: $after) {
    pid
    bin
    iin
    inn
    unp
    nameRu
    nameKz
    fullNameRu
    regdate
    crdate
    year
    typeSupplier
    markSmallEmployer
    markResident
    markPatronymicProducer
    markNationalCompany
    markWorldCompany
    markStateMonopoly
    markNaturalMonopoly
    okedList
    krpCode
    kseCode
    refKopfCode
    qvazi
    customer
    supplier
    organizer
    isSingleOrg
    email
    phone
    website
    countryCode
    systemId
    lastUpdateDate
  }
}
"""

GQL_RNU = """
query($limit: Int!, $after: Int) {
  Rnu(limit: $limit, after: $after) {
    id
    pid
    supplierBiin
    supplierNameRu
    startDate
    endDate
    systemId
    indexDate
  }
}
"""

GQL_CONTRACT_ACT = """
query($limit: Int!, $after: Int) {
  Acts(limit: $limit, after: $after) {
    id
    contractId
    numberAct
    aktDate
    sumBeginning
    sumFine
    dayOverdue
    statusId
    systemId
    indexDate
  }
}
"""

GQL_CONTRACT_PAYMENT = """
query($limit: Int!, $after: Int) {
  ContractPayment(limit: $limit, after: $after) {
    id
    contractId
    actId
    paySum
    payDate
    refPaymentTypeId
    systemId
    lastUpdateDate
  }
}
"""

GQL_CONTRACT_SPEC_SUM = """
query($limit: Int!, $after: Int) {
  ContractSpecSum(limit: $limit, after: $after) {
    id
    contractId
    totalSum
    factSum
    systemId
    lastUpdateDate
  }
}
"""

GQL_TREASURY_PAY = """
query($limit: Int!, $after: Int) {
  TreasuryPay(limit: $limit, after: $after) {
    id
    nomZa
    contractId
    dtReg
    supplier
    rnnSupplier
    nomDog
    dtDog
    itemDescription
    payAmount
    prepaySum
    payDate
    ppn
    espk
    gu
    finSource
    iikSupplier
    bikSupplier
    vendorId
    systemId
    indexDate
  }
}
"""


class OWSClient:
    """
    Async HTTP client for Goszakup OWS V3 API.
    Primary transport: GraphQL with limit/after pagination.
    REST kept only for journal.
    """

    def __init__(self):
        self.base_url = settings.ows_base_url
        self.token = settings.ows_token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.delay = settings.etl_rate_limit_delay
        self.page_size = settings.etl_page_size

    # ─── REST API (kept for journal) ─────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(settings.etl_max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _get(self, url: str, params: dict = None) -> dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    async def paginate(self, endpoint: str, params: dict = None) -> AsyncGenerator[tuple[list[dict], str], None]:
        """Paginate through a REST endpoint using next_page cursor."""
        url = f"{self.base_url}{endpoint}"
        params = params or {}

        if endpoint.startswith("http"):
            url = endpoint
            params = {}

        while url:
            await asyncio.sleep(self.delay)
            try:
                data = await self._get(url, params)
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                raise

            items = data.get("items", [])
            next_page = data.get("next_page", "")
            next_url = f"{self.base_url}{next_page}" if next_page else None

            if items:
                yield items, next_url or ""

            if next_page:
                url = next_url
                params = {}
            else:
                break

    # ─── GraphQL API ─────────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(settings.etl_max_retries),
        wait=wait_exponential(multiplier=1, min=5, max=60),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def graphql(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query against OWS V3."""
        payload = {
            "operationName": None,
            "query": query,
            "variables": variables or {},
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{self.base_url}/v3/graphql",
                headers=self.headers,
                json=payload,
            )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                err_msg = result["errors"][0].get("message", str(result["errors"]))
                logger.error(f"GraphQL error: {err_msg}")
                raise RuntimeError(f"GraphQL query error: {err_msg}")
            return result

    @retry(
        stop=stop_after_attempt(settings.etl_max_retries),
        wait=wait_exponential(multiplier=1, min=5, max=60),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _post_with_retry(self, url: str, headers: dict, json: dict) -> dict:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, headers=headers, json=json)
            response.raise_for_status()
            return response.json()

    async def graphql_paginate(
        self,
        query: str,
        data_key: str,
        max_records: int = 0,
        page_size: int = 50,
        variables: dict | None = None,
        initial_after: int = 0,
    ) -> AsyncGenerator[tuple[list[dict], int], None]:
        """
        Yields (batch, next_id) from a GraphQL query using `limit` and `after`.
        Stops when `max_records` is reached (if > 0) or no more records.
        """
        url = f"{self.base_url}/v3/graphql"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json", "Accept": "application/json"}

        # Merge pagination vars into the user-provided variables map, or create a new one
        vars_payload = variables.copy() if variables else {}

        last_id = initial_after
        fetched_total = 0

        while True:
            vars_payload["limit"] = page_size
            vars_payload["after"] = last_id

            payload = {
                "operationName": None, # Added operationName to match existing graphql method
                "query": query,
                "variables": vars_payload,
            }

            await asyncio.sleep(self.delay) # Keep the delay
            try:
                result = await self._post_with_retry(url, headers=headers, json=payload)
            except Exception as e:
                logger.error(f"GraphQL error for {data_key}: {e}")
                raise

            # Hard-stop on GraphQL errors instead of silently returning empty
            if "errors" in result:
                err_msg = result["errors"][0].get("message", str(result["errors"]))
                logger.error(
                    f"GraphQL error for {data_key}: {err_msg} "
                    f"(after={last_id}, limit={page_size})"
                )
                raise RuntimeError(f"GraphQL query error for {data_key}: {err_msg}")
            data = result.get("data", {}).get(data_key, [])
            if not data:
                break

            # Use extensions.pageInfo for cursor
            page_info = result.get("extensions", {}).get("pageInfo", {})
            next_id = page_info.get("lastId")

            yield data, next_id
            fetched_total += len(data)

            if max_records > 0 and fetched_total >= max_records:
                logger.info(f"Reached bootstrap limit {max_records} for {data_key}")
                break

            # Use extensions.pageInfo for cursor
            page_info = result.get("extensions", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            last_id = next_id
            if not last_id:
                break

    # ─── Journal (REST) ──────────────────────────────────────────────────────

    async def get_journal(self, date_from: str, date_to: str) -> list[dict]:
        """Fetch journal of changed objects for a date range."""
        items = []
        async for batch, _ in self.paginate(
            "/v3/journal",
            params={"date_from": date_from, "date_to": date_to},
        ):
            items.extend(batch)
        return items

    # ─── Single object fetch (REST, for incremental) ─────────────────────────

    async def fetch_by_id(self, endpoint: str, obj_id: str) -> Optional[dict]:
        """Fetch a single object by ID."""
        try:
            return await self._get(f"{self.base_url}{endpoint}/{obj_id}")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
