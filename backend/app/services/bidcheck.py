"""
BidCheck: Parse technical specification PDFs and generate synthetic supplier data.
Uses LLM for parsing and generation when OpenAI key is available; falls back to mock when not.
"""
import json
import logging
import random
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Optional

from pypdf import PdfReader

from app.core.config import settings

logger = logging.getLogger(__name__)

# Min chars to consider extraction successful; otherwise try fallback
_MIN_EXTRACTED_CHARS = 50

# Mock Kazakh company names for fallback generation
_MOCK_NAMES = [
    "ТОО «СтройСервис Плюс»",
    "ИП «Астана Логистик»",
    "ТОО «ТехРесурс»",
    "ТОО «КазахСтрой»",
    "ИП «БизнесКонсалт»",
    "ТОО «ПрофМонтаж»",
    "ТОО «ИнфоТех»",
    "ИП «Альянс Подряд»",
]


def _extract_with_pypdf(content: bytes) -> str:
    """Extract text using pypdf."""
    reader = PdfReader(BytesIO(content))
    parts = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            parts.append(text)
    return "\n\n".join(parts) if parts else ""


def _extract_with_pymupdf(content: bytes) -> str:
    """Extract text using PyMuPDF (better for tables, complex layouts)."""
    try:
        import fitz  # pymupdf
        doc = fitz.open(stream=content, filetype="pdf")
        parts = []
        for page in doc:
            parts.append(page.get_text())
        doc.close()
        return "\n\n".join(p for p in parts if p).strip()
    except ImportError:
        return ""
    except Exception as e:
        logger.warning(f"PyMuPDF extract failed: {e}")
        return ""


def _extract_with_pdfplumber(content: bytes) -> str:
    """Extract text using pdfplumber (good for tables, from demo_bidcheck)."""
    try:
        import pdfplumber
        from io import BytesIO
        parts = []
        with pdfplumber.open(BytesIO(content)) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                if txt:
                    parts.append(txt)
        return "\n".join(parts) if parts else ""
    except ImportError:
        return ""
    except Exception as e:
        logger.warning(f"pdfplumber extract failed: {e}")
        return ""


def _extract_text_from_pdf(content: bytes) -> str:
    """Extract raw text from PDF. Try pdfplumber (best for appendices), then pypdf, then PyMuPDF."""
    text = _extract_with_pdfplumber(content)
    if len(text.strip()) < _MIN_EXTRACTED_CHARS:
        text = _extract_with_pypdf(content)
    if len(text.strip()) < _MIN_EXTRACTED_CHARS:
        fallback = _extract_with_pymupdf(content)
        if len(fallback) > len(text):
            logger.info("Using PyMuPDF fallback")
            return fallback
    return text


async def _parse_with_llm(text: str, filename: str) -> Optional[dict]:
    """Use OpenAI to parse extracted text into structured requirements."""
    if not settings.openai_api_key:
        return None
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        prompt = f"""Extract labor/equipment requirements from this technical specification text.
Return a JSON object with this exact structure (no markdown, no extra text):
{{
  "source_filename": "{filename}",
  "parsed_at_utc": "<ISO datetime>",
  "labor_requirements": [
    {{"role": "role name in Russian", "count": 1, "notes": "optional notes"}}
  ],
  "equipment_requirements": [],
  "other_requirements": {{}}
}}

Text to analyze:
---
{text[:12000]}
---

JSON only:"""
        resp = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content or ""
        # Strip markdown code block if present
        if raw.strip().startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        logger.warning(f"LLM parse failed: {e}")
        return None


def _parse_mock(text: str, filename: str) -> dict:
    """Fallback: build minimal requirements from extracted text keywords."""
    text_lower = text.lower()
    roles = []
    for kw, role in [
        ("инженер", "Инженер"),
        ("электромонтажник", "Электромонтажник"),
        ("сварщик", "Сварщик"),
        ("тракторист", "Тракторист-машинист"),
        ("машинист", "Машинист"),
        ("мастер итр", "Мастер ИТР"),
        ("разнорабоч", "Разнорабочий"),
        ("дизайнер", "Дизайнер"),
        ("бухгалтер", "Бухгалтер"),
        ("водитель", "Водитель"),
        ("монтажник", "Монтажник"),
        ("специалист", "Специалист"),
    ]:
        if kw in text_lower:
            roles.append({"role": role, "count": 1, "notes": "из текста ТЗ"})
    if not roles:
        roles = [{"role": "Специалист", "count": 1, "notes": "по умолчанию"}]
    return {
        "source_filename": filename,
        "parsed_at_utc": datetime.now(timezone.utc).isoformat(),
        "labor_requirements": roles,
        "equipment_requirements": [],
        "other_requirements": {},
    }


async def parse_pdf(content: bytes, filename: str) -> dict:
    """Parse PDF and return RequirementsDoc."""
    text = _extract_text_from_pdf(content)
    result = await _parse_with_llm(text, filename)
    if result is None:
        result = _parse_mock(text, filename)
    if "parsed_at_utc" not in result:
        result["parsed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return result


async def _generate_suppliers_llm(requirements: dict, count: int, seed: Optional[int]) -> Optional[dict]:
    """Use LLM to generate realistic supplier candidates."""
    if not settings.openai_api_key:
        return None
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        req_str = json.dumps(requirements, ensure_ascii=False, indent=2)
        prompt = f"""Generate {count} realistic Kazakh supplier companies that could bid on a procurement with these requirements.
Return a JSON object:
{{
  "suppliers": [
    {{
      "name": "ТОО «Company Name» or ИП «Name»",
      "bin_iin": "12 digits BIN or 12 digits IIN",
      "contacts": {{"email": "...", "phone": "+7 ..."}},
      "capabilities": ["capability1", "capability2"],
      "confidence": 0.85
    }}
  ],
  "summary": "Brief summary of generated suppliers"
}}

Requirements:
{req_str}

Generate {count} suppliers. JSON only, no markdown:"""
        resp = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0.7 if seed is None else 0.3,
        )
        raw = resp.choices[0].message.content or ""
        if raw.strip().startswith("```"):
            raw = raw.split("```")[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        logger.warning(f"LLM generate suppliers failed: {e}")
        return None


def _generate_suppliers_mock(requirements: dict, count: int, seed: Optional[int]) -> dict:
    """Fallback: generate mock suppliers."""
    if seed is not None:
        random.seed(seed)
    labor = requirements.get("labor_requirements") or []
    roles = [r.get("role", "Специалист") for r in labor] if labor else ["Специалист"]
    names = _MOCK_NAMES.copy()
    random.shuffle(names)
    suppliers = []
    for i in range(min(count, len(names))):
        suppliers.append({
            "name": names[i],
            "bin_iin": str(100000000000 + random.randint(0, 99999999999))[:12],
            "contacts": {"email": f"info@{names[i].split('«')[1].split('»')[0] if '«' in names[i] else 'company'}.kz"},
            "capabilities": roles[:3],
            "confidence": round(0.7 + random.random() * 0.25, 2),
        })
    return {
        "suppliers": suppliers,
        "summary": f"Сгенерировано {len(suppliers)} поставщиков (mock). Добавьте OPENAI_API_KEY для LLM-генерации.",
    }


async def generate_suppliers(
    requirements: dict,
    suppliers_count: int = 5,
    seed: Optional[int] = None,
) -> dict:
    """Generate supplier candidates from requirements."""
    result = await _generate_suppliers_llm(requirements, suppliers_count, seed)
    if result is None:
        result = _generate_suppliers_mock(requirements, suppliers_count, seed)
    return result


# === End2end (demo_bidcheck-3 style): requirements + suppliers with documents_text + summaries ===



async def parse_and_generate_full(content: bytes, filename: str) -> Optional[dict]:
    """
    End2end: PDF -> requirements + suppliers (with documents_text) + summaries (PASS/FAIL).
    Matches demo_bidcheck-3 output schema.
    """
    if not settings.openai_api_key:
        return None
    text = _extract_text_from_pdf(content)
    if len(text.strip()) < 20:
        logger.warning("Too little text extracted from PDF for end2end")
        return None
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        instructions = (
            "Ты — аудитор госзакупок. Работаешь строго по содержимому PDF.\n\n"
            "Задача (верни ТОЛЬКО валидный JSON, без markdown):\n"
            "A) requirements: Извлеки ВСЕ роли из раздела 'Трудовые ресурсы/Еңбек ресурстары'. "
            "Для каждой: role, count, required_documents (если указаны в PDF), notes, evidence (1-2 фрагмента).\n"
            "B) suppliers: Сгенерируй 2 поставщика: 1) profile=FULL — полностью закрывает требования; "
            "2) profile=MINOR_MISSING — не хватает ровно одного элемента. "
            "documents_text — текстовый список документов (удостоверения, дипломы, аттестаты, ФИО и т.д.).\n"
            "C) summaries: Для каждого поставщика checks по каждой роли (status: OK/FAIL/UNKNOWN), verdict (PASS только если все OK), issues только при FAIL.\n"
        )
        prompt = f"{instructions}\n\nТекст из PDF:\n---\n{text[:14000]}\n---\n\nJSON:"
        resp = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=6000,
            temperature=0.2,
        )
        raw = resp.choices[0].message.content or ""
        if raw.strip().startswith("```"):
            raw = raw.split("```")[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())
        # Normalize verdict
        for s in data.get("summaries", []):
            if any(c.get("status") == "FAIL" for c in s.get("checks", [])):
                s["verdict"] = "FAIL"
            elif all(c.get("status") == "OK" for c in s.get("checks", [])):
                s["verdict"] = "PASS"
            else:
                s["verdict"] = "FAIL"
        data["source_filename"] = filename
        data["parsed_at_utc"] = datetime.now(timezone.utc).isoformat()
        return data
    except Exception as e:
        logger.warning(f"End2end parse failed: {e}")
        return None


async def parse_and_analyze_compliance(
    ts_content: bytes,
    ts_filename: str,
    supplier_content: bytes,
    supplier_filename: str,
) -> Optional[dict]:
    """
    Analyze compliance: TS document (requirements) vs supplier document.
    Returns schema identical to demo (requirements.labor_roles, suppliers, summaries).
    """
    if not settings.openai_api_key:
        return None
    ts_text = _extract_text_from_pdf(ts_content)
    supplier_text = _extract_text_from_pdf(supplier_content)
    if len(ts_text.strip()) < 20 or len(supplier_text.strip()) < 20:
        logger.warning("Too little text extracted for compliance analysis")
        return None
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)
        instructions = """Ты — аудитор госзакупок. Сравниваешь требования ТЗ с документами поставщика.

Верни ТОЛЬКО валидный JSON (без markdown). Строго соблюдай структуру:

{
  "requirements": {
    "labor_roles": [
      {"role": "название роли", "count": число, "required_documents": ["док1", "док2"], "notes": [], "evidence": []}
    ],
    "global_notes": []
  },
  "suppliers": [
    {
      "supplier_name": "название из документа или Поставщик",
      "profile": "FULL" или "MINOR_MISSING" — FULL если все требования выполнены, иначе MINOR_MISSING,
      "documents_text": "подробное описание: роли, ФИО, номера документов, что предоставлено (как в демо)"
    }
  ],
  "summaries": [
    {
      "supplier_name": "то же что в suppliers",
      "verdict": "PASS" или "FAIL",
      "checks": [{"role": "роль", "required": "/N", "status": "OK"|"FAIL", "evidence": []}],
      "issues": [{"category": "required_documents", "finding": "описание проблемы", "evidence": []}]
    }
  ]
}

Требования:
- labor_roles: извлеки ВСЕ роли из раздела Трудовые ресурсы/Еңбек ресурстары в ТЗ.
- required_documents: массив строк (удостоверения, аттестаты и т.д.) для каждой роли.
- documents_text: структурируй по ролям с ФИО и номерами документов (как в примере демо).
- checks: по ОДНОЙ записи на КАЖДУЮ роль из labor_roles, status OK/FAIL.
- issues: заполняй ТОЛЬКО при FAIL, category обычно "required_documents"."""
        prompt = (
            f"{instructions}\n\n"
            f"--- Текст ТЗ ({ts_filename}) ---\n{ts_text[:12000]}\n\n"
            f"--- Текст документа поставщика ({supplier_filename}) ---\n{supplier_text[:12000]}\n---\n\nJSON:"
        )
        resp = await client.chat.completions.create(
            model=settings.openai_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=6000,
            temperature=0.2,
        )
        raw = resp.choices[0].message.content or ""
        if raw.strip().startswith("```"):
            raw = raw.split("```")[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())

        # Normalize to demo schema
        req = data.get("requirements") or {}
        labor_roles = req.get("labor_roles") or req.get("labor_requirements") or []
        if isinstance(labor_roles, list):
            for r in labor_roles:
                if not isinstance(r.get("required_documents"), list):
                    r["required_documents"] = r.get("required_documents") and [r["required_documents"]] or []
        else:
            labor_roles = []
        data["requirements"] = {"labor_roles": labor_roles, "global_notes": req.get("global_notes", [])}

        suppliers = data.get("suppliers")
        if not suppliers or not isinstance(suppliers, list):
            suppliers = [{"supplier_name": "Поставщик", "profile": "FULL", "documents_text": supplier_text[:3000]}]
        for sup in suppliers:
            if "profile" not in sup or sup["profile"] not in ("FULL", "MINOR_MISSING"):
                sup["profile"] = "FULL"
            if "documents_text" not in sup:
                sup["documents_text"] = supplier_text[:3000]
        data["suppliers"] = suppliers

        summaries = data.get("summaries") or []
        sup_names = [s.get("supplier_name", "Поставщик") for s in suppliers]
        for i, s in enumerate(summaries):
            s["supplier_name"] = s.get("supplier_name") or sup_names[i] if i < len(sup_names) else "Поставщик"
            checks = s.get("checks") or []
            if any(c.get("status") == "FAIL" for c in checks):
                s["verdict"] = "FAIL"
            elif all(c.get("status") == "OK" for c in checks) and checks:
                s["verdict"] = "PASS"
            else:
                s["verdict"] = "FAIL"
            s["issues"] = s.get("issues") or []
        if not summaries and suppliers:
            sn = suppliers[0].get("supplier_name", "Поставщик")
            summaries = [{
                "supplier_name": sn,
                "verdict": "PASS",
                "checks": [{"role": r.get("role", "?"), "required": f"/{r.get('count', '?')}", "status": "OK", "evidence": []}
                           for r in labor_roles],
                "issues": []
            }]
        data["summaries"] = summaries

        data["source_filename"] = ts_filename
        data["supplier_filename"] = supplier_filename
        data["parsed_at_utc"] = datetime.now(timezone.utc).isoformat()
        return data
    except Exception as e:
        logger.warning(f"Compliance analysis failed: {e}")
        return None
