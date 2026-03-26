"""
Company Profile endpoint — real-time OWS data aggregation.
GET  /company/search?q=...   → list of matching companies (by BIN or name)
GET  /company/{bin}          → full profile with analytics metrics
POST /company/{bin}/analyze  → LLM-generated analysis narrative
"""
import logging
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.core.security import require_viewer
from app.services.company_profile import CompanyProfileService
from app.llm.client import LLMClient

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — эксперт по государственным закупкам Казахстана.
Анализируй данные компании из реестра Goszakup и пиши краткий, конкретный аналитический отчёт.
Пиши на русском языке. Будь конкретным: называй числа, суммы, проценты из данных.
Структура ответа строго:
1. **Общий вывод** — 2–3 предложения об уровне риска и характере деятельности.
2. **Ключевые риски** — bullet-list (максимум 5 пунктов, только если есть реальные риски).
3. **Положительные факторы** — bullet-list (максимум 3 пункта).
4. **Рекомендация** — 1–2 предложения для аналитика.
Не добавляй disclaimers и вступительных фраз."""


def _build_prompt(profile: dict) -> str:
    subj = profile.get("subject") or {}
    risk = profile.get("risk", {})
    rnu = profile.get("rnu", {})
    sup = profile.get("as_supplier", {}).get("metrics", {})
    cust = profile.get("as_customer", {}).get("metrics", {})
    complaints = profile.get("complaints", {})
    kgd = profile.get("kgd", {})
    court = profile.get("court_cases", {})
    affiliations = profile.get("affiliations", {})

    def fmt_sum(n: float) -> str:
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.1f} млрд ₸"
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f} млн ₸"
        return f"{n:,.0f} ₸"

    lines = [
        f"КОМПАНИЯ: {subj.get('nameRu') or subj.get('fullNameRu') or 'Не определено'}",
        f"БИН: {profile.get('bin')}",
        f"Дата регистрации: {str(subj.get('regdate', ''))[:10] or 'неизвестно'}",
        f"",
        f"РИСК-ОЦЕНКА: {risk.get('level')} (score={risk.get('score')}/100)",
        f"Флаги риска: {', '.join(risk.get('flags', [])) or 'нет'}",
        f"Статус РНУ (недобросовестный поставщик): {'ДА — АКТИВНЫЙ' if rnu.get('is_blacklisted') else 'нет'}",
        f"",
        f"КАК ПОСТАВЩИК:",
        f"  Всего контрактов: {sup.get('total_contracts', 0)}",
        f"  Общая сумма контрактов: {fmt_sum(sup.get('total_sum', 0))}",
        f"  Фактически исполнено: {fmt_sum(sup.get('executed_sum', 0))} ({sup.get('execution_rate', 0)}%)",
        f"  Выплачено казначейством: {fmt_sum(sup.get('treasury_paid', 0))}",
        f"  Уникальных заказчиков: {sup.get('unique_customers', 0)}",
        f"  Просрочки актов: {sup.get('overdue_count', 0)} (средняя просрочка: {sup.get('avg_overdue_days', 0)} дней)",
        f"  Штрафы: {sup.get('fines_count', 0)}",
        f"  Средний размер контракта: {fmt_sum(sup.get('avg_contract_size', 0))}",
        f"  Доля дополнений к контрактам: {sup.get('addendum_rate', 0)}%",
    ]

    top_cust = sup.get("top_customers", [])
    if top_cust:
        total_sup = sup.get("total_sum", 1) or 1
        top = top_cust[0]
        share = round(top["sum"] / total_sup * 100, 1)
        lines.append(f"  Топ-заказчик (БИН {top['bin']}): {top['count']} контр., {fmt_sum(top['sum'])} ({share}% от выручки)")

    by_year_s = sup.get("by_year", {})
    if by_year_s:
        lines.append(f"  Динамика по годам: " + "; ".join(
            f"{y}: {v['count']} контр. / {fmt_sum(v['sum'])}"
            for y, v in sorted(by_year_s.items())[-4:]
        ))

    lines += [
        f"",
        f"КАК ЗАКАЗЧИК:",
        f"  Всего тендеров: {cust.get('total_tenders', 0)}",
        f"  Всего контрактов: {cust.get('total_contracts', 0)}",
        f"  Общий объём закупок: {fmt_sum(cust.get('total_procurement_sum', 0))}",
        f"  Уникальных поставщиков: {cust.get('unique_suppliers', 0)}",
        f"  Доля единственного источника: {cust.get('single_source_rate', 0)}% ({cust.get('single_source_count', 0)} тендеров)",
        f"  Отменённые тендеры: {cust.get('cancelled_tenders', 0)}",
    ]

    top_sup = cust.get("top_suppliers", [])
    if top_sup:
        total_cust = cust.get("total_procurement_sum", 1) or 1
        top = top_sup[0]
        share = round(top["sum"] / total_cust * 100, 1)
        lines.append(f"  Топ-поставщик (БИН {top['bin']}): {top['count']} контр., {fmt_sum(top['sum'])} ({share}% от закупок)")

    by_year_c = cust.get("by_year", {})
    if by_year_c:
        lines.append(f"  Динамика по годам: " + "; ".join(
            f"{y}: {v['count']} контр. / {fmt_sum(v['sum'])}"
            for y, v in sorted(by_year_c.items())[-4:]
        ))

    # ── Complaints section ───────────────────────────────────────────────
    if complaints and complaints.get("total", 0) > 0:
        lines += [
            f"",
            f"ЖАЛОБЫ (реестр goszakup.kz):",
            f"  Всего жалоб: {complaints.get('total', 0)}",
            f"  Как поставщик (объект жалобы): {complaints.get('complaints_as_supplier', 0)}",
            f"  Как заказчик (на закупки): {complaints.get('complaints_as_customer', 0)}",
            f"  Удовлетворено: {complaints.get('satisfied_count', 0)} ({complaints.get('satisfaction_rate', 0)}%)",
        ]

    # ── Tax payments section (ba.prg.kz) ──────────────────────────────────
    if kgd and kgd.get("available"):
        tax_payments = kgd.get("tax_payments") or []
        if tax_payments:
            lines += [
                f"",
                f"НАЛОГОВЫЕ ОТЧИСЛЕНИЯ (ba.prg.kz):",
            ]
            for tp in tax_payments:
                change = f" ({'+' if (tp.get('change_pct') or 0) > 0 else ''}{tp.get('change_pct')}%)" if tp.get("change_pct") is not None else ""
                lines.append(f"  {tp['year']}: {fmt_sum(tp['amount'])}{change}")
            lines.append(f"  Итого: {fmt_sum(kgd.get('total_tax_paid', 0))}")
            llm_tax = kgd.get("llm_analysis")
            if llm_tax and llm_tax.get("summary"):
                lines.append(f"  AI-анализ: {llm_tax['summary']}")

    # ── Court cases section ──────────────────────────────────────────────
    if court and court.get("total", 0) > 0:
        lines += [
            f"",
            f"СУДЕБНЫЕ ДЕЛА (sud.gov.kz):",
            f"  Всего дел: {court.get('total', 0)}",
            f"  Как истец: {court.get('as_plaintiff', 0)}",
            f"  Как ответчик: {court.get('as_defendant', 0)}",
            f"  Средняя оценка влияния на надёжность (LLM): {court.get('avg_reliability_impact', 0)}/10",
        ]
        for analysis in (court.get("llm_analyses") or [])[:3]:
            lines.append(f"  — Дело {analysis.get('case_number', '?')}: {analysis.get('summary', '')}")

    # ── Affiliations section ─────────────────────────────────────────────
    if affiliations and affiliations.get("total_links", 0) > 0:
        lines += [
            f"",
            f"СВЯЗАННЫЕ КОМПАНИИ:",
        ]
        for a in (affiliations.get("shared_bank_accounts") or [])[:3]:
            lines.append(f"  — Общий банковский счёт с {a.get('bin')} ({a.get('name', '')})")
        for a in (affiliations.get("shared_contacts") or [])[:3]:
            lines.append(f"  — Общий {a.get('match_type', 'контакт')} с {a.get('bin')} ({a.get('name', '')})")
        for a in (affiliations.get("cobid_partners") or [])[:3]:
            lines.append(f"  — Co-bidding {a.get('times_together', 0)}x раз с {a.get('bin')} ({a.get('name', '')})")

    return "\n".join(lines)


# ── Request schema ────────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    profile: dict[str, Any]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/search")
async def search_companies(
    q: str = Query(..., min_length=2, description="БИН (12 цифр) или название компании"),
    _=Depends(require_viewer),
):
    """Search companies in OWS registry by BIN/IIN or partial name. Returns up to 20 results."""
    service = CompanyProfileService()
    try:
        results = await service.search(q)
    except Exception as e:
        logger.error(f"Company search error: {e}")
        raise HTTPException(status_code=502, detail="OWS API недоступен или вернул ошибку")
    return {"query": q, "total": len(results), "results": results}


@router.get("/{bin}")
async def get_company_profile(
    bin: str,
    _=Depends(require_viewer),
):
    """
    Full real-time company profile fetched from OWS.
    Includes contracts (as supplier and customer), tenders, RNU status, and risk metrics.
    Note: For large companies this may take 10–30 seconds due to OWS pagination.
    """
    bin = bin.strip()
    if not bin:
        raise HTTPException(status_code=400, detail="BIN не может быть пустым")

    service = CompanyProfileService()
    try:
        profile = await service.get_profile(bin)
    except Exception as e:
        logger.error(f"Company profile error for {bin}: {e}")
        raise HTTPException(status_code=502, detail="Ошибка получения данных из OWS API")

    if (
        not profile.get("subject")
        and not profile["as_supplier"]["metrics"]["total_contracts"]
        and not profile["as_customer"]["metrics"]["total_contracts"]
    ):
        raise HTTPException(status_code=404, detail=f"Компания с БИН {bin} не найдена в реестре OWS")

    return profile


@router.post("/{bin}/analyze")
async def analyze_company(
    bin: str,
    body: AnalyzeRequest,
    _=Depends(require_viewer),
):
    """
    Generate LLM-powered analysis narrative for a company profile.
    Accepts the already-fetched profile data in request body to avoid re-fetching OWS.
    LLM is used ONLY for human-readable explanation — never for risk scoring decisions.
    """
    bin = bin.strip()
    if not bin:
        raise HTTPException(status_code=400, detail="BIN не может быть пустым")

    prompt = _build_prompt(body.profile)

    llm = LLMClient()
    try:
        narrative = await llm.generate(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=prompt,
        )
    except Exception as e:
        logger.error(f"LLM analysis error for company {bin}: {e}")
        raise HTTPException(
            status_code=503,
            detail="LLM недоступен. Проверьте OPENAI_API_KEY и повторите попытку.",
        )

    return {
        "bin": bin,
        "narrative": narrative,
        "model": llm.model,
        "prompt_preview": prompt[:300] + "..." if len(prompt) > 300 else prompt,
    }
