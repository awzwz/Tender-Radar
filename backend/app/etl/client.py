import asyncio
import logging
from typing import Any, AsyncGenerator, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.core.config import settings

logger = logging.getLogger(__name__)


class OWSClient:
    """
    Async HTTP client for Goszakup OWS V3 API.
    Supports both REST (paginated) and GraphQL endpoints.
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

    # ─── REST API ────────────────────────────────────────────────────────────

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
        """
        Paginate through a REST endpoint using next_page cursor.
        Yields batches of (items, next_page_url).
        """
        url = f"{self.base_url}{endpoint}"
        params = params or {}

        # If endpoint is already a full URL (from restored cursor), use it
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
                # Yield items and the NEXT url to fetch (checkpoint)
                yield items, next_url or ""

            if next_page:
                url = next_url
                params = {}  # next_page already has params embedded
            else:
                break

    # ─── GraphQL API ─────────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(settings.etl_max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def graphql(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query against OWS V3."""
        payload = {
            "operationName": None,
            "query": query,
            "variables": variables or {},
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/v3/graphql",
                headers=self.headers,
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def graphql_paginate(
        self, query: str, variables: dict, data_key: str, limit: int = 50
    ) -> AsyncGenerator[list[dict], None]:
        """
        Paginate through GraphQL results using lastId cursor.
        Yields batches of items.
        """
        after = 0
        variables = {**variables, "limit": limit, "after": after}

        while True:
            await asyncio.sleep(self.delay)
            try:
                result = await self.graphql(query, variables)
            except Exception as e:
                logger.error(f"GraphQL error for {data_key}: {e}")
                raise

            data = result.get("data", {}).get(data_key, [])
            if not data:
                break

            yield data

            page_info = result.get("extensions", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break

            last_id = page_info.get("lastId")
            if not last_id:
                break

            variables = {**variables, "after": last_id}

    # ─── Journal ─────────────────────────────────────────────────────────────

    async def get_journal(self, date_from: str, date_to: str) -> list[dict]:
        """Fetch journal of changed objects for a date range."""
        items = []
        async for batch in self.paginate(
            "/v3/journal",
            params={"date_from": date_from, "date_to": date_to}
        ):
            items.extend(batch)
        return items

    # ─── Convenience REST fetchers ────────────────────────────────────────────

    async def fetch_trd_buy_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/trd-buy"):
            yield batch, cursor

    async def fetch_lots_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/lots"):
            yield batch, cursor

    async def fetch_trd_app_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/trd-app"):
            yield batch, cursor

    async def fetch_contract_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/contract"):
            yield batch, cursor

    async def fetch_subject_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/subject/all"):
            yield batch, cursor

    async def fetch_rnu_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/rnu"):
            yield batch, cursor

    async def fetch_treasury_pay_all(self) -> AsyncGenerator[tuple[list[dict], str], None]:
        async for batch, cursor in self.paginate("/v3/treasury-pay"):
            yield batch, cursor

    async def fetch_by_id(self, endpoint: str, obj_id: str) -> Optional[dict]:
        """Fetch a single object by ID."""
        try:
            return await self._get(f"{self.base_url}{endpoint}/{obj_id}")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
