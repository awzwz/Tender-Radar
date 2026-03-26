"""
KGD/Tax Service — fetch tax payment data from ba.prg.kz.
Source: ba.prg.kz (business analytics portal for Kazakhstan companies).
Extracts only: tax payments by year.
LLM analyzes tax data for risk assessment and score impact.
"""
import json
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 25.0
BA_BASE = "https://ba.prg.kz"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
}


class KGDService:
    """Fetch tax payment data for a company by BIN from ba.prg.kz."""

    async def fetch_tax_info(self, bin_val: str) -> dict:
        """
        Fetch tax payment data for a company.
        1. Search ba.prg.kz for company page URL
        2. Extract tax payments from Nuxt SSR payload
        3. LLM analysis of tax patterns → score impact
        """
        results = {
            "bin": bin_val,
            "available": False,
            "tax_payments": [],       # [{year, amount, change_pct}]
            "total_tax_paid": 0.0,
            "llm_analysis": None,
            "score_points": 0,
            "source": "ba.prg.kz",
        }

        try:
            # Step 1: Find company page URL
            company_url = await self._find_company_url(bin_val)
            if not company_url:
                logger.info(f"Company {bin_val} not found on ba.prg.kz")
                return results

            # Step 2: Extract tax payments from page
            tax_data = await self._fetch_tax_data(company_url)
            if not tax_data:
                logger.info(f"No tax data extracted for {bin_val} from ba.prg.kz")
                return results

            results["tax_payments"] = tax_data
            results["total_tax_paid"] = sum(e["amount"] for e in tax_data)
            results["available"] = True

            # Step 3: LLM analysis of tax patterns
            if tax_data:
                llm_result = await self._analyze_tax_data_llm(results, bin_val)
                if llm_result:
                    results["llm_analysis"] = llm_result
                    results["score_points"] = llm_result.get("score_points", 0)

        except Exception as e:
            logger.warning(f"ba.prg.kz fetch failed for {bin_val}: {e}")

        return results

    # ── Company URL discovery ──────────────────────────────────────────────────

    async def _find_company_url(self, bin_val: str) -> Optional[str]:
        """Search ba.prg.kz to find the company page URL by BIN."""
        try:
            async with httpx.AsyncClient(
                timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=HEADERS
            ) as client:
                resp = await client.get(f"{BA_BASE}/list/", params={"text": bin_val})
                if resp.status_code != 200:
                    return None

                soup = BeautifulSoup(resp.text, "lxml")

                # Find link containing the BIN in href
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if bin_val in href and href.startswith("/"):
                        return href.rstrip("/")

        except Exception as e:
            logger.warning(f"ba.prg.kz search failed for {bin_val}: {e}")
        return None

    # ── Tax data extraction ────────────────────────────────────────────────────

    async def _fetch_tax_data(self, company_url: str) -> Optional[list[dict]]:
        """Fetch tax payments from ba.prg.kz company page."""
        try:
            full_url = f"{BA_BASE}{company_url}" if company_url.startswith("/") else company_url
            async with httpx.AsyncClient(
                timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=HEADERS
            ) as client:
                resp = await client.get(full_url)
                if resp.status_code != 200:
                    return None
                return self._extract_tax_from_nuxt(resp.text)
        except Exception as e:
            logger.warning(f"ba.prg.kz page fetch failed for {company_url}: {e}")
        return None

    def _extract_tax_from_nuxt(self, html: str) -> Optional[list[dict]]:
        """
        Extract tax payment data from Nuxt 3 SSR payload.
        Nuxt 3 embeds data as a flat JSON array in a <script> tag.
        Tax data pattern: year_int, amount_float appear as consecutive elements.
        """
        soup = BeautifulSoup(html, "lxml")

        # Find the script with ShallowReactive (Nuxt data payload)
        nuxt_array = None
        for script in soup.find_all("script"):
            content = script.string or ""
            if "ShallowReactive" in content and len(content) > 1000:
                try:
                    nuxt_array = json.loads(content)
                    break
                except json.JSONDecodeError:
                    continue

        if not nuxt_array or not isinstance(nuxt_array, list):
            return None

        arr = nuxt_array
        arr_len = len(arr)

        # Find year/amount pairs in the array
        # Pattern: ..., 2022, 944035293.2, {year: idx, value: idx}, 2023, 504088188.4, ...
        tax_entries = []
        for i in range(arr_len - 1):
            if isinstance(arr[i], int) and 2015 <= arr[i] <= 2030:
                if isinstance(arr[i + 1], (int, float)) and arr[i + 1] > 1000:
                    tax_entries.append({
                        "year": arr[i],
                        "amount": float(arr[i + 1]),
                    })

        if not tax_entries:
            return None

        # Take only the first ascending series (main tax graph, before VAT series starts)
        main_series = [tax_entries[0]]
        for j in range(1, len(tax_entries)):
            if tax_entries[j]["year"] <= tax_entries[j - 1]["year"]:
                break  # Next series starts (e.g. VAT-only payments)
            main_series.append(tax_entries[j])

        # Calculate year-over-year % change
        for j, entry in enumerate(main_series):
            if j > 0:
                prev = main_series[j - 1]["amount"]
                if prev > 0:
                    entry["change_pct"] = round((entry["amount"] - prev) / prev * 100, 1)
                else:
                    entry["change_pct"] = None
            else:
                entry["change_pct"] = None

        return main_series

    # ── LLM tax analysis ───────────────────────────────────────────────────────

    async def _analyze_tax_data_llm(self, data: dict, bin_val: str) -> Optional[dict]:
        """Analyze tax payment patterns with LLM for risk assessment."""
        try:
            from app.llm.client import LLMClient
            llm = LLMClient()
        except Exception as e:
            logger.warning(f"LLM client unavailable for tax analysis: {e}")
            return None

        system = (
            "Ты финансовый аналитик. Анализируй налоговые данные компании. "
            "Верни ТОЛЬКО валидный JSON без markdown-разметки и без ```."
        )

        tax_lines = []
        for t in data.get("tax_payments", []):
            change = f" ({'+' if (t.get('change_pct') or 0) > 0 else ''}{t.get('change_pct', 0)}%)" if t.get("change_pct") is not None else ""
            tax_lines.append(f"  {t['year']}: {t['amount']:,.2f} ₸{change}")

        prompt = (
            f"Компания БИН: {bin_val}\n\n"
            f"НАЛОГОВЫЕ ОТЧИСЛЕНИЯ ПО ГОДАМ:\n" + "\n".join(tax_lines) + "\n\n"
            f"Общая сумма налогов: {data.get('total_tax_paid', 0):,.2f} ₸\n"
            "\nОпредели и верни JSON:\n"
            "{\n"
            '  "tax_trend": "рост/снижение/стабильно/нестабильно",\n'
            '  "tax_health": "хорошее/удовлетворительное/плохое",\n'
            '  "anomalies": ["список аномалий если есть, например: резкий рост/падение налогов, минимальные отчисления"],\n'
            '  "summary": "краткий анализ в 3-4 предложения на русском",\n'
            '  "risk_level": "low/medium/high",\n'
            '  "score_points": число от 0 до 10 (0=нет проблем, 10=критические налоговые проблемы)\n'
            "}\n\n"
            "Правила оценки score_points:\n"
            "- Стабильные налоговые отчисления, плавный рост → 0-1\n"
            "- Умеренные колебания (±30%) → 1-2\n"
            "- Резкое падение налогов (>50% за год) → 3-5\n"
            "- Минимальные налоги при возможной крупной деятельности → 4-6\n"
            "- Аномальный рост с последующим падением (возможная схема) → 5-7\n"
            "- Околонулевые налоги последний год → 6-8\n"
            "- Критические аномалии → 8-10"
        )

        try:
            response = await llm.generate(system_prompt=system, user_prompt=prompt)

            json_match = re.search(r"\{[^{}]*\}", response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(response)

            # Validate and clamp score_points
            sp = result.get("score_points", 0)
            if isinstance(sp, (int, float)):
                result["score_points"] = max(0, min(10, int(sp)))
            else:
                result["score_points"] = 0

            return result
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse LLM tax analysis JSON: {e}")
            return None
        except Exception as e:
            logger.warning(f"LLM tax analysis failed for {bin_val}: {e}")
            return None
