"""
Complaints Service — fetches and parses complaints from goszakup.gov.kz registry.
Uses web scraping with correct form parameters (filter[biin_supplier], filter[bin_org]).
Analyzes satisfied/partially satisfied complaints via LLM for score impact.
"""
import asyncio
import json
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MAX_COMPLAINTS = 50
MAX_LLM_COMPLAINTS = 3  # Only analyze top N satisfied complaints with LLM
REQUEST_TIMEOUT = 30.0
GOSZAKUP_URL = "https://goszakup.gov.kz/ru/registry/complaint"


class ComplaintsService:
    """Fetch complaint data for a company by BIN from goszakup.gov.kz."""

    async def fetch_complaints(self, bin_val: str) -> dict:
        """
        Fetch complaints for a company — as supplier (object of complaint)
        and as organizer (whose procurement is being complained about).
        Two parallel requests with different filter params.
        """
        as_supplier = []
        as_customer = []

        try:
            as_supplier, as_customer = await asyncio.gather(
                self._fetch_as_supplier(bin_val),
                self._fetch_as_organizer(bin_val),
                return_exceptions=False,
            )
        except Exception as e:
            logger.warning(f"Complaints fetch failed for {bin_val}: {e}")
            # Try individually
            try:
                as_supplier = await self._fetch_as_supplier(bin_val)
            except Exception:
                pass
            try:
                as_customer = await self._fetch_as_organizer(bin_val)
            except Exception:
                pass

        # Deduplicate by complaint_number
        seen = set()
        all_complaints = []
        for c in as_supplier + as_customer:
            num = c.get("complaint_number", "")
            if num and num not in seen:
                seen.add(num)
                all_complaints.append(c)
            elif not num:
                all_complaints.append(c)

        # Compute metrics
        satisfied = [c for c in all_complaints if self._is_satisfied(c.get("status", ""))]
        rejected = [c for c in all_complaints if self._is_rejected(c.get("status", ""))]
        total_decided = [c for c in all_complaints if self._is_decided(c.get("status", ""))]
        satisfaction_rate = (
            round(len(satisfied) / len(total_decided) * 100, 1)
            if total_decided else 0.0
        )

        # LLM analysis of satisfied complaints (they indicate real violations)
        llm_analyses = []
        score_points = 0
        if satisfied:
            llm_analyses = await self._analyze_satisfied_complaints(satisfied[:MAX_LLM_COMPLAINTS], bin_val)
            score_points = self._compute_score_points(len(satisfied), satisfaction_rate, llm_analyses)

        return {
            "total": len(all_complaints),
            "complaints_as_supplier": len(as_supplier),
            "complaints_as_customer": len(as_customer),
            "satisfied_count": len(satisfied),
            "rejected_count": len(rejected),
            "satisfaction_rate": satisfaction_rate,
            "score_points": score_points,
            "llm_analyses": llm_analyses,
            "complaints": all_complaints[:MAX_COMPLAINTS],
        }

    # ── Score computation ─────────────────────────────────────────────────────

    def _compute_score_points(
        self,
        satisfied_count: int,
        satisfaction_rate: float,
        llm_analyses: list[dict],
    ) -> int:
        """
        Compute score points from complaint data.
        Satisfied complaints = confirmed violations = higher risk.
        """
        points = 0

        # Base points from satisfied count
        if satisfied_count >= 5:
            points += 8
        elif satisfied_count >= 3:
            points += 5
        elif satisfied_count >= 1:
            points += 3

        # Additional points from LLM severity analysis
        high_severity = sum(1 for a in llm_analyses if a.get("severity") == "high")
        if high_severity >= 2:
            points += 5
        elif high_severity >= 1:
            points += 3

        return min(points, 13)  # Cap at 13

    # ── Status classification ─────────────────────────────────────────────────

    @staticmethod
    def _is_satisfied(status: str) -> bool:
        """Check if complaint was satisfied (but NOT 'отказано в удовлетворении')."""
        s = status.lower()
        # "Рассмотрена – отказано в удовлетворении" contains "удовлетворен" but is a REJECTION
        if "отказано" in s or "отклонен" in s:
            return False
        return "удовлетворен" in s

    @staticmethod
    def _is_rejected(status: str) -> bool:
        s = status.lower()
        return "отказано" in s or "отклонен" in s

    @staticmethod
    def _is_decided(status: str) -> bool:
        s = status.lower()
        return "рассмотрен" in s or "удовлетворен" in s or "отказано" in s or "отклонен" in s

    # ── Web scraping ──────────────────────────────────────────────────────────

    async def _fetch_as_supplier(self, bin_val: str) -> list[dict]:
        """Fetch complaints where company is the supplier (object of complaint)."""
        params = {"filter[biin_supplier]": bin_val}
        return await self._scrape_complaints(params, bin_val, role="supplier")

    async def _fetch_as_organizer(self, bin_val: str) -> list[dict]:
        """Fetch complaints where company is the organizer (whose procurement is complained about)."""
        params = {"filter[bin_org]": bin_val}
        return await self._scrape_complaints(params, bin_val, role="organizer")

    async def _scrape_complaints(self, params: dict, bin_val: str, role: str) -> list[dict]:
        """Scrape complaint list from goszakup.gov.kz with given filter params."""
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(GOSZAKUP_URL, params=params)
            resp.raise_for_status()
            return self._parse_complaint_table(resp.text, bin_val, role)

    def _parse_complaint_table(self, html: str, bin_val: str, role: str) -> list[dict]:
        """
        Parse the main complaints data table (Table index 4 on goszakup.gov.kz).
        Table structure:
          [0] № жалобы
          [1] № возражения
          [2] Номер объявления, Способ закупки
          [3] БИН/ИИН, наименование поставщика
          [4] БИН, наименование организатора
          [5] Статус жалобы
          [6] Уполномоченный орган
          [7] Дата подачи жалобы
          [8] Срок окончания рассмотрения жалобы
          [9] Жалоба заполнена
        """
        soup = BeautifulSoup(html, "lxml")
        complaints = []

        # Find the data table — it has "№ жалобы" in header
        data_table = None
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if any("№ жалобы" in h for h in headers):
                data_table = table
                break

        if not data_table:
            logger.info(f"No complaint data table found for BIN {bin_val}")
            return []

        rows = data_table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 8:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # Extract BIN from combined "БИН + название" cells
            supplier_bin, supplier_name = self._extract_bin_name(cell_texts[3] if len(cell_texts) > 3 else "")
            organizer_bin, organizer_name = self._extract_bin_name(cell_texts[4] if len(cell_texts) > 4 else "")

            # Extract tender number from cell[2] (format: "16361004-1Открытый конкурс")
            tender_raw = cell_texts[2] if len(cell_texts) > 2 else ""
            tender_match = re.match(r"(\d[\d-]+)", tender_raw)
            tender_number = tender_match.group(1) if tender_match else ""
            procurement_method = tender_raw[len(tender_number):].strip() if tender_number else tender_raw

            # Extract complaint URL from links
            complaint_url = ""
            for link in row.find_all("a", href=True):
                href = link.get("href", "")
                if "complaint" in href and "preview" in href:
                    complaint_url = href
                    break

            complaint = {
                "complaint_number": cell_texts[0],
                "date_submitted": cell_texts[7] if len(cell_texts) > 7 else "",
                "review_deadline": cell_texts[8] if len(cell_texts) > 8 else "",
                "supplier_bin": supplier_bin,
                "supplier_name": supplier_name,
                "organizer_bin": organizer_bin,
                "organizer_name": organizer_name,
                "tender_number": tender_number,
                "procurement_method": procurement_method,
                "status": cell_texts[5] if len(cell_texts) > 5 else "",
                "authority": cell_texts[6] if len(cell_texts) > 6 else "",
                "complaint_url": complaint_url,
                "role": role,
                "source": "web",
            }
            complaints.append(complaint)

        return complaints[:MAX_COMPLAINTS]

    @staticmethod
    def _extract_bin_name(combined: str) -> tuple[str, str]:
        """Extract BIN (12 digits) and name from combined 'БИННазвание' string."""
        match = re.match(r"(\d{12})(.*)", combined)
        if match:
            return match.group(1), match.group(2).strip()
        return "", combined.strip()

    # ── LLM analysis of satisfied complaints ──────────────────────────────────

    async def _analyze_satisfied_complaints(
        self, complaints: list[dict], bin_val: str
    ) -> list[dict]:
        """Analyze satisfied complaints with LLM to assess violation severity."""
        try:
            from app.llm.client import LLMClient
            llm = LLMClient()
        except Exception as e:
            logger.warning(f"LLM client unavailable for complaint analysis: {e}")
            return []

        analyses = []
        for complaint in complaints:
            try:
                # Fetch full complaint content if URL available
                full_text = ""
                if complaint.get("complaint_url"):
                    full_text = await self._fetch_complaint_content(complaint["complaint_url"]) or ""

                analysis = await self._analyze_single_complaint(llm, complaint, bin_val, full_text)
                if analysis:
                    analysis["complaint_number"] = complaint.get("complaint_number", "")
                    analyses.append(analysis)
            except Exception as e:
                logger.debug(f"LLM analysis failed for complaint {complaint.get('complaint_number')}: {e}")

        return analyses

    async def _analyze_single_complaint(
        self, llm, complaint: dict, bin_val: str, full_text: str
    ) -> Optional[dict]:
        """Analyze a single satisfied complaint with LLM."""
        system = (
            "Ты эксперт по государственным закупкам Казахстана. "
            "Анализируй жалобу и оценивай серьёзность нарушения. "
            "Верни ТОЛЬКО валидный JSON без markdown-разметки и без ```."
        )

        context_parts = [
            f"Компания БИН: {bin_val}",
            f"Номер жалобы: {complaint.get('complaint_number', '?')}",
            f"Дата подачи: {complaint.get('date_submitted', '?')}",
            f"Статус: {complaint.get('status', '?')}",
            f"Поставщик: {complaint.get('supplier_bin', '?')} {complaint.get('supplier_name', '')}",
            f"Организатор: {complaint.get('organizer_bin', '?')} {complaint.get('organizer_name', '')}",
            f"Тендер: {complaint.get('tender_number', '?')} ({complaint.get('procurement_method', '')})",
        ]
        if full_text:
            context_parts.append(f"\nПолный текст жалобы:\n{full_text[:4000]}")

        prompt = (
            "\n".join(context_parts)
            + "\n\nОпредели и верни JSON:\n"
            "{\n"
            '  "violation_type": "тип нарушения (дискриминация ТЗ / необоснованный отказ / сговор / нарушение процедуры / коррупция / другое)",\n'
            '  "summary": "краткое описание в 2-3 предложения",\n'
            '  "severity": "low/medium/high",\n'
            '  "risk_impact": число от 0 до 10 (0=формальное нарушение, 10=критичное, коррупция/сговор)\n'
            "}\n\n"
            "Правила оценки risk_impact:\n"
            "- Формальное нарушение процедуры → 1-3\n"
            "- Необоснованный отказ поставщику → 3-5\n"
            "- Дискриминационные требования в ТЗ → 4-6\n"
            "- Признаки фаворитизма → 6-8\n"
            "- Сговор, коррупция → 8-10"
        )

        response = await llm.generate(system_prompt=system, user_prompt=prompt)

        try:
            json_match = re.search(r"\{[^{}]*\}", response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(response)

            # Validate and clamp risk_impact
            impact = result.get("risk_impact", 0)
            if isinstance(impact, (int, float)):
                result["risk_impact"] = max(0, min(10, impact))
            else:
                result["risk_impact"] = 0

            return result
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse LLM complaint analysis JSON: {e}")
            return None

    # ── Complaint content fetching ────────────────────────────────────────────

    async def _fetch_complaint_content(self, complaint_url: str) -> Optional[str]:
        """Fetch full complaint page content from goszakup.gov.kz."""
        if not complaint_url:
            return None

        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(complaint_url)
                resp.raise_for_status()
                return self._parse_complaint_page(resp.text)
        except Exception as e:
            logger.debug(f"Failed to fetch complaint content from {complaint_url}: {e}")
            return None

    def _parse_complaint_page(self, html: str) -> str:
        """Extract relevant text from complaint detail page."""
        soup = BeautifulSoup(html, "lxml")

        # Remove navigation
        for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        # Look for complaint content sections
        content_parts = []
        for div in soup.find_all("div", class_=re.compile(r"complaint|content|detail|body|card")):
            text = div.get_text(separator="\n", strip=True)
            if len(text) > 50:
                content_parts.append(text)

        if content_parts:
            return "\n\n".join(content_parts)[:8000]

        # Fallback: main body text
        body = soup.find("body")
        if body:
            return body.get_text(separator="\n", strip=True)[:8000]

        return ""
