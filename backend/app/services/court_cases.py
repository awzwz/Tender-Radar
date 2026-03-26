"""
Court Cases Service — fetch and analyze court decisions.
Primary source: sud.gov.kz (bank of judicial acts).
Fallback: office.sud.kz case search.
Parses available court data and uses LLM for reliability impact assessment.
"""
import asyncio
import json
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30.0
SUD_BASE = "https://sud.gov.kz"
MAX_CASES = 30
MAX_LLM_CASES = 5  # Only analyze top N cases with LLM (cost/time control)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://sud.gov.kz/rus/court-acts",
}


class CourtCasesService:
    """Fetch court cases for a company and analyze decisions with LLM."""

    async def fetch_court_data(self, bin_val: str, analyze_with_llm: bool = True) -> dict:
        """
        Fetch court cases for a company by BIN.
        Tries multiple sources with graceful fallback.
        """
        cases = []

        # Try multiple sources
        sources = [
            ("sud.gov.kz", self._fetch_from_sud_gov),
            ("office.sud.kz", self._fetch_from_office_sud),
        ]

        for source_name, fetcher in sources:
            try:
                result = await fetcher(bin_val)
                if result:
                    cases = result
                    logger.info(f"Court cases for {bin_val}: found {len(cases)} from {source_name}")
                    break
            except Exception as e:
                logger.debug(f"{source_name} fetch failed for {bin_val}: {e}")

        # Classify roles
        as_plaintiff = [c for c in cases if c.get("role") == "истец"]
        as_defendant = [c for c in cases if c.get("role") == "ответчик"]
        as_other = [c for c in cases if c.get("role") not in ("истец", "ответчик")]

        # LLM analysis of most relevant cases (defendant cases prioritized)
        llm_analyses = []
        avg_reliability_impact = 0.0

        if analyze_with_llm and cases:
            # Prioritize defendant cases with full text for LLM analysis
            priority_cases = sorted(
                [c for c in cases if c.get("full_text")],
                key=lambda c: (0 if c.get("role") == "ответчик" else 1, -(c.get("amount") or 0)),
            )[:MAX_LLM_CASES]

            if priority_cases:
                llm_analyses = await self._analyze_cases_with_llm(priority_cases, bin_val)
                impacts = [a.get("reliability_impact", 0) for a in llm_analyses if a]
                if impacts:
                    avg_reliability_impact = round(sum(impacts) / len(impacts), 1)

        # Map avg impact to score points
        score_points = self._impact_to_score(avg_reliability_impact)

        return {
            "total": len(cases),
            "as_plaintiff": len(as_plaintiff),
            "as_defendant": len(as_defendant),
            "as_other": len(as_other),
            "avg_reliability_impact": avg_reliability_impact,
            "score_points": score_points,
            "cases": cases[:MAX_CASES],
            "llm_analyses": llm_analyses,
        }

    def _impact_to_score(self, avg_impact: float) -> int:
        """Map average LLM reliability impact (0-10) to score points."""
        if avg_impact <= 3:
            return 0
        elif avg_impact <= 5:
            return 3
        elif avg_impact <= 6:
            return 5
        elif avg_impact <= 8:
            return 7
        else:
            return 10

    # ── Source 1: sud.gov.kz ───────────────────────────────────────────────────

    async def _fetch_from_sud_gov(self, bin_val: str) -> list[dict]:
        """Fetch court cases from sud.gov.kz bank of judicial acts."""
        cases = []

        async with httpx.AsyncClient(
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            headers=HEADERS,
            http2=True,
        ) as client:
            # Try the court acts search with keyword parameter
            try:
                search_url = f"{SUD_BASE}/rus/court-acts"
                resp = await client.get(
                    search_url,
                    params={"keyword": bin_val, "page": 0},
                )
                if resp.status_code == 200:
                    parsed = self._parse_sud_gov_page(resp.text, bin_val)
                    if parsed:
                        cases.extend(parsed)
            except Exception as e:
                logger.debug(f"sud.gov.kz keyword search failed: {e}")

            # Also try direct search with bin parameter
            if not cases:
                try:
                    resp = await client.get(
                        f"{SUD_BASE}/rus/court-acts",
                        params={"bin": bin_val},
                    )
                    if resp.status_code == 200:
                        parsed = self._parse_sud_gov_page(resp.text, bin_val)
                        if parsed:
                            cases.extend(parsed)
                except Exception as e:
                    logger.debug(f"sud.gov.kz bin search failed: {e}")

            # Try Drupal AJAX endpoint
            if not cases:
                try:
                    ajax_url = f"{SUD_BASE}/rus/system/ajax"
                    data = {
                        "keyword": bin_val,
                        "form_id": "court_acts_search_form",
                        "_triggering_element_name": "op",
                    }
                    resp = await client.post(ajax_url, data=data)
                    if resp.status_code == 200:
                        try:
                            ajax_data = resp.json()
                            for item in ajax_data:
                                if isinstance(item, dict) and item.get("data"):
                                    parsed = self._parse_sud_gov_page(item["data"], bin_val)
                                    if parsed:
                                        cases.extend(parsed)
                        except (json.JSONDecodeError, TypeError):
                            pass
                except Exception as e:
                    logger.debug(f"sud.gov.kz AJAX search failed: {e}")

            # Fetch full text for top cases
            if cases:
                fetch_tasks = []
                for case in cases[:MAX_LLM_CASES]:
                    if case.get("detail_url"):
                        fetch_tasks.append(self._fetch_case_detail(client, case))
                if fetch_tasks:
                    await asyncio.gather(*fetch_tasks, return_exceptions=True)

        return cases

    def _parse_sud_gov_page(self, html: str, bin_val: str) -> list[dict]:
        """Parse court cases from sud.gov.kz HTML."""
        soup = BeautifulSoup(html, "lxml")
        cases = []

        # Find case entries — cards, articles, or table rows
        for article in soup.find_all(["article", "div", "tr", "li"], class_=re.compile(
            r"case|item|result|card|row|node|views-row", re.IGNORECASE
        )):
            case = self._extract_case_from_element(article, bin_val)
            if case and case.get("case_number"):
                cases.append(case)

        # Also try table rows
        if not cases:
            for table in soup.find_all("table"):
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    case = self._extract_case_from_row(row, bin_val)
                    if case and case.get("case_number"):
                        cases.append(case)

        # Try finding any links with case numbers
        if not cases:
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True)
                href = link["href"]
                if re.search(r"\d{4}-\d+", text) or "court-act" in href:
                    case = {
                        "case_number": text.strip()[:50],
                        "date": "",
                        "court": "",
                        "case_type": "",
                        "parties": "",
                        "amount": None,
                        "outcome": "",
                        "role": "",
                        "detail_url": href if href.startswith("http") else f"{SUD_BASE}{href}",
                        "full_text": "",
                    }
                    cases.append(case)

        return cases[:MAX_CASES]

    # ── Source 2: office.sud.kz ────────────────────────────────────────────────

    async def _fetch_from_office_sud(self, bin_val: str) -> list[dict]:
        """Fetch court cases from office.sud.kz."""
        cases = []

        async with httpx.AsyncClient(
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            headers=HEADERS,
        ) as client:
            # Try lawsuit list search
            endpoints = [
                "https://office.sud.kz/lawsuit/lawsuitList.xhtml",
                "https://office.sud.kz/lawsuit/lawsuitSearch.xhtml",
            ]
            for endpoint in endpoints:
                try:
                    resp = await client.get(endpoint, params={"bin": bin_val})
                    if resp.status_code == 200:
                        parsed = self._parse_office_cases(resp.text, bin_val)
                        if parsed:
                            return parsed
                except Exception as e:
                    logger.debug(f"office.sud.kz endpoint {endpoint} failed: {e}")

        return cases

    # ── Shared parsing helpers ─────────────────────────────────────────────────

    def _extract_case_from_element(self, elem, bin_val: str) -> Optional[dict]:
        """Extract case info from a DOM element."""
        text = elem.get_text(separator=" ", strip=True)
        if not text or len(text) < 20:
            return None

        case = {
            "case_number": "",
            "date": "",
            "court": "",
            "case_type": "",
            "parties": "",
            "amount": None,
            "outcome": "",
            "role": "",
            "detail_url": "",
            "full_text": "",
        }

        # Extract case number
        num_match = re.search(r"(?:дело|№)\s*([^\s,;]+)", text, re.IGNORECASE)
        if num_match:
            case["case_number"] = num_match.group(1).strip()

        # Extract date
        date_match = re.search(r"(\d{2}[./]\d{2}[./]\d{4})", text)
        if date_match:
            case["date"] = date_match.group(1)

        # Extract amount
        amount_match = re.search(r"([\d\s,.]+)\s*(?:тенге|тг|₸)", text)
        if amount_match:
            try:
                case["amount"] = float(amount_match.group(1).replace(" ", "").replace(",", "."))
            except ValueError:
                pass

        # Determine case type
        text_lower = text.lower()
        if "гражданск" in text_lower:
            case["case_type"] = "гражданское"
        elif "административн" in text_lower:
            case["case_type"] = "административное"
        elif "уголовн" in text_lower:
            case["case_type"] = "уголовное"

        # Determine role
        if "ответчик" in text_lower:
            case["role"] = "ответчик"
        elif "истец" in text_lower or "заявитель" in text_lower:
            case["role"] = "истец"

        # Extract detail URL
        for link in elem.find_all("a", href=True):
            href = link["href"]
            if "court-act" in href or "detail" in href or "case" in href:
                if href.startswith("/"):
                    case["detail_url"] = f"{SUD_BASE}{href}"
                elif href.startswith("http"):
                    case["detail_url"] = href
                break

        return case if case["case_number"] else None

    def _extract_case_from_row(self, row, bin_val: str) -> Optional[dict]:
        """Extract case info from a table row."""
        cells = row.find_all("td")
        if len(cells) < 3:
            return None

        texts = [c.get_text(strip=True) for c in cells]
        all_text = " ".join(texts)

        case = {
            "case_number": texts[0] if texts else "",
            "date": "",
            "court": "",
            "case_type": "",
            "parties": texts[2] if len(texts) > 2 else "",
            "amount": None,
            "outcome": "",
            "role": "",
            "detail_url": "",
            "full_text": "",
        }

        # Find date in cells
        for t in texts:
            date_match = re.match(r"(\d{2}[./]\d{2}[./]\d{4})", t)
            if date_match:
                case["date"] = date_match.group(1)
                break

        # Determine role from party info
        all_lower = all_text.lower()
        if "ответчик" in all_lower:
            case["role"] = "ответчик"
        elif "истец" in all_lower:
            case["role"] = "истец"

        # Extract link
        for link in row.find_all("a", href=True):
            href = link["href"]
            if href.startswith("/"):
                case["detail_url"] = f"{SUD_BASE}{href}"
            elif href.startswith("http"):
                case["detail_url"] = href
            break

        return case

    def _parse_office_cases(self, html: str, bin_val: str) -> list[dict]:
        """Parse cases from office.sud.kz format."""
        soup = BeautifulSoup(html, "lxml")
        cases = []

        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                case = self._extract_case_from_row(row, bin_val)
                if case and case.get("case_number"):
                    cases.append(case)

        return cases[:MAX_CASES]

    async def _fetch_case_detail(self, client: httpx.AsyncClient, case: dict):
        """Fetch full text of a court decision."""
        url = case.get("detail_url")
        if not url:
            return

        try:
            resp = await client.get(url, headers=HEADERS)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")

                # Remove navigation elements
                for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()

                # Find the decision text content
                content = soup.find("div", class_=re.compile(r"content|text|body|decision|act|field"))
                if content:
                    case["full_text"] = content.get_text(separator="\n", strip=True)[:8000]
                else:
                    body = soup.find("body")
                    if body:
                        case["full_text"] = body.get_text(separator="\n", strip=True)[:8000]

                # Try to extract outcome from text
                text_lower = (case.get("full_text") or "").lower()
                if "удовлетворить" in text_lower:
                    case["outcome"] = "удовлетворено"
                elif "отказать" in text_lower:
                    case["outcome"] = "отказано"
                elif "прекратить" in text_lower:
                    case["outcome"] = "прекращено"

                # Try to extract court name
                court_match = re.search(r"([\w\s-]+(суд|сот)[\w\s-]*)", case.get("full_text", "")[:500], re.IGNORECASE)
                if court_match:
                    case["court"] = court_match.group(1).strip()[:100]

        except Exception as e:
            logger.debug(f"Failed to fetch case detail from {url}: {e}")

    # ── LLM Analysis ──────────────────────────────────────────────────────────

    async def _analyze_cases_with_llm(self, cases: list[dict], bin_val: str) -> list[dict]:
        """Analyze court decisions with LLM for reliability impact."""
        try:
            from app.llm.client import LLMClient
            llm = LLMClient()
        except Exception as e:
            logger.warning(f"LLM client unavailable for court analysis: {e}")
            return []

        analyses = []
        for case in cases:
            if not case.get("full_text"):
                continue

            try:
                analysis = await self._analyze_single_case(llm, case, bin_val)
                if analysis:
                    analysis["case_number"] = case.get("case_number", "")
                    analyses.append(analysis)
            except Exception as e:
                logger.debug(f"LLM analysis failed for case {case.get('case_number')}: {e}")

        return analyses

    async def _analyze_single_case(self, llm, case: dict, bin_val: str) -> Optional[dict]:
        """Analyze a single court decision with LLM."""
        system = (
            "Ты юрист-аналитик. Анализируй судебные решения Казахстана. "
            "Верни ТОЛЬКО валидный JSON без markdown-разметки и без ```."
        )

        prompt = (
            f"Компания БИН: {bin_val}\n"
            f"Номер дела: {case.get('case_number', 'неизвестно')}\n"
            f"Дата: {case.get('date', 'неизвестно')}\n\n"
            f"Текст решения:\n{case['full_text'][:4000]}\n\n"
            "Определи и верни JSON:\n"
            "{\n"
            '  "role": "истец/ответчик/третье лицо",\n'
            '  "dispute_type": "договорной/налоговый/трудовой/административный/мошенничество/другое",\n'
            '  "amount": числовая сумма или null,\n'
            '  "outcome": "выиграл/проиграл/частично/неизвестно",\n'
            '  "reliability_impact": число от 0 до 10 (0=не влияет на надёжность, 10=критично для надёжности),\n'
            '  "summary": "краткое описание в 2-3 предложения"\n'
            "}\n\n"
            "Правила оценки reliability_impact:\n"
            "- Компания-истец, защищающая свои права → 0-2\n"
            "- Мелкий договорной спор → 2-4\n"
            "- Неисполнение контракта (ответчик) → 5-7\n"
            "- Мошенничество, уклонение от налогов → 8-10\n"
            "- Банкротство → 7-9"
        )

        response = await llm.generate(system_prompt=system, user_prompt=prompt)

        # Parse JSON
        try:
            json_match = re.search(r"\{[^{}]*\}", response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(response)

            # Validate and clamp reliability_impact
            impact = result.get("reliability_impact", 0)
            if isinstance(impact, (int, float)):
                result["reliability_impact"] = max(0, min(10, impact))
            else:
                result["reliability_impact"] = 0

            return result
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse LLM court analysis JSON: {e}")
            return None
