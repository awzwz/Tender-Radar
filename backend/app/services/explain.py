"""
Explanation Service: generates human-readable risk explanations using LLM.
Implements gating (only for high-risk / uncertain), PII masking, and caching.
"""
import re
import json
import hashlib
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.procurement import RiskScore, RiskFlag, LlmExplanation, Lot
from app.llm.client import LLMClient

logger = logging.getLogger(__name__)

# PII patterns
_PHONE_RE = re.compile(r"\+?\d[\d\-\s()]{8,15}\d")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_ADDRESS_RE = re.compile(
    r"(?:ул\.|пр\.|мкр\.|г\.\s*|город\s+|улица\s+|проспект\s+|район\s+|дом\s+|кв\.\s*)\S+(?:\s+\S+){0,5}",
    re.IGNORECASE,
)

SYSTEM_PROMPT = """You are a procurement risk analyst assistant for Kazakhstan's public procurement system.
Your role is to explain risk scores in a clear, actionable way for government auditors.
You DO NOT decide risk scores — they were computed by deterministic rules and ML models.
Your job is ONLY to explain what the scores mean and what the analyst should verify.

Rules:
- Write in Russian (Kazakh government standard).
- Be concise: bullet points, max 3-5 sentences per point.
- Always include a "Что проверить" (What to verify) checklist.
- Never reveal raw technical details (model weights, SQL queries, etc.).
- Never make up information that is not in the provided evidence.
- Also analyze specifications for manipulation signs (brand names, narrow params).
"""

SPEC_ANALYSIS_INSTRUCTION = """
В конце ответа добавь блок:
[АНАЛИЗ СПЕЦИФИКАЦИЙ]
РИСК: ДА или НЕТ
ОБОСНОВАНИЕ: 1-2 предложения (есть ли указание на бренды, модели, узкие параметры, ограничивающие конкуренцию)
"""


def _mask_pii(text: str) -> str:
    """Mask phone numbers, emails, and addresses in text."""
    text = _PHONE_RE.sub("[ТЕЛЕФОН СКРЫТ]", text)
    text = _EMAIL_RE.sub("[EMAIL СКРЫТ]", text)
    text = _ADDRESS_RE.sub("[АДРЕС СКРЫТ]", text)
    return text


def _mask_evidence(evidence: dict) -> dict:
    """Recursively mask PII in evidence dict."""
    masked = {}
    for k, v in evidence.items():
        if isinstance(v, str):
            masked[k] = _mask_pii(v)
        elif isinstance(v, dict):
            masked[k] = _mask_evidence(v)
        elif isinstance(v, list):
            masked[k] = [_mask_pii(str(item)) if isinstance(item, str) else item for item in v]
        else:
            masked[k] = v
    return masked


def _should_explain(risk_final: float, risk_ml: Optional[float]) -> bool:
    """
    Gating logic: only generate explanation for:
    - High risk (risk_final >= threshold)
    - Uncertain ML (0.45 <= risk_ml <= 0.55)
    """
    if risk_final >= settings.llm_risk_threshold:
        return True
    if risk_ml is not None and settings.llm_uncertainty_low <= risk_ml <= settings.llm_uncertainty_high:
        return True
    return False


def _build_prompt(score: RiskScore, flags: list[RiskFlag], lot_spec_text: Optional[str] = None) -> str:
    """Build the user prompt with masked evidence and optional spec text."""
    triggered = [f for f in flags if f.flag_bool]
    triggered.sort(key=lambda f: f.value_numeric or 0, reverse=True)

    lines = [
        f"Общий балл риска: {score.score_final}/100",
        f"  - Правила: {score.score_rules}/100",
        f"  - ML-модель: {score.score_ml if score.score_ml is not None else 'N/A'}",
        f"  - Уровень: {score.level}",
        "",
        "Сработавшие индикаторы:",
    ]

    for f in triggered[:10]:
        evidence = _mask_evidence(f.evidence_jsonb or {})
        lines.append(f"  • {f.indicator_code}: значение={f.value_numeric}")
        for k, v in evidence.items():
            lines.append(f"    - {k}: {v}")
        lines.append("")

    if lot_spec_text and lot_spec_text.strip():
        lines.extend([
            "",
            "Текст лота/спецификации (название и описание):",
            lot_spec_text.strip()[:2000],
            "",
            "Проанализируй: есть ли в требованиях признаки манипулятивных спецификаций — "
            "указание на конкретные бренды, модели, или узкие параметры, адаптированные под конкретного поставщика?",
            SPEC_ANALYSIS_INSTRUCTION,
        ])

    lines.extend([
        "",
        "Объясни для аналитика:",
        "1. Краткое описание рисков (3-5 пунктов)",
        "2. Контрольный список 'Что проверить' (3-5 пунктов)",
    ])

    return "\n".join(lines)


def _explanation_display_text(full_text: str) -> str:
    """Extract main explanation (without spec block) for display."""
    if not full_text:
        return ""
    if "[АНАЛИЗ СПЕЦИФИКАЦИЙ]" in full_text:
        return full_text.split("[АНАЛИЗ СПЕЦИФИКАЦИЙ]")[0].strip()
    return full_text


def _parse_spec_analysis(text: str) -> Optional[dict]:
    """Extract spec analysis block from LLM response. Returns {risky: bool, reasoning: str} or None."""
    if not text or "АНАЛИЗ СПЕЦИФИКАЦИЙ" not in text:
        return None
    try:
        start = text.find("[АНАЛИЗ СПЕЦИФИКАЦИЙ]")
        block = text[start:start + 500] if start >= 0 else ""
        risky = "РИСК: ДА" in block.upper() or "РИСК:ДА" in block.upper()
        reasoning = ""
        if "ОБОСНОВАНИЕ:" in block:
            idx = block.find("ОБОСНОВАНИЕ:")
            reasoning = block[idx + len("ОБОСНОВАНИЕ:"):].split("\n")[0].strip()
        return {"risky": risky, "reasoning": reasoning or "Не удалось извлечь"}
    except Exception:
        return None


async def generate_explanation(
    entity_type: str, entity_id: str, force: bool = False
) -> Optional[dict]:
    """
    Generate or retrieve cached explanation for an entity.
    Returns dict with explanation_text, checklist, or None if not eligible.
    """
    async with AsyncSessionLocal() as db:
        # Check cache first
        if not force:
            cached = await db.execute(
                select(LlmExplanation).where(
                    LlmExplanation.entity_type == entity_type,
                    LlmExplanation.entity_id == entity_id,
                )
            )
            cached_row = cached.scalar_one_or_none()
            if cached_row:
                full_text = cached_row.explanation_text or ""
                spec = _parse_spec_analysis(full_text)
                return {
                    "explanation": _explanation_display_text(full_text),
                    "checklist": cached_row.checklist_jsonb,
                    "spec_analysis": spec,
                    "spec_analysis": spec,
                    "risk_final": cached_row.risk_final,
                    "risk_rules": cached_row.risk_rules,
                    "risk_ml": cached_row.risk_ml,
                    "model_used": cached_row.model_used,
                    "created_at": str(cached_row.created_at),
                    "cached": True,
                }

        # Get risk score
        score_result = await db.execute(
            select(RiskScore).where(
                RiskScore.entity_type == entity_type,
                RiskScore.entity_id == entity_id,
            )
        )
        score = score_result.scalar_one_or_none()
        if not score:
            return None

        # Gating check
        if not force and not _should_explain(score.score_final or 0, score.score_ml):
            return {
                "explanation": None,
                "reason": "Risk score below explanation threshold",
                "risk_final": score.score_final,
                "eligible": False,
            }

        # Get flags
        flags_result = await db.execute(
            select(RiskFlag).where(
                RiskFlag.entity_type == entity_type,
                RiskFlag.entity_id == entity_id,
            )
        )
        flags = flags_result.scalars().all()

        # Fetch lot spec text for "lot" entity (analysis of manipulative specifications)
        lot_spec_text = None
        if entity_type == "lot":
            lot_result = await db.execute(select(Lot).where(Lot.id == int(entity_id)))
            lot_row = lot_result.scalar_one_or_none()
            if lot_row:
                parts = [p for p in [lot_row.name_ru, lot_row.name_kz] if p and str(p).strip()]
                lot_spec_text = " | ".join(parts) if parts else None

        # Build prompt
        user_prompt = _build_prompt(score, flags, lot_spec_text)
        prompt_hash = hashlib.sha256(user_prompt.encode()).hexdigest()[:16]

        # Check API key
        if not settings.openai_api_key or settings.openai_api_key.startswith("sk-your"):
            return {
                "explanation": "OpenAI API key not configured. Set OPENAI_API_KEY in .env",
                "checklist": [],
                "spec_analysis": None,
                "risk_final": score.score_final,
                "eligible": True,
                "error": "api_key_missing",
            }

        # Call LLM
        try:
            client = LLMClient()
            raw_response = await client.generate(SYSTEM_PROMPT, user_prompt)

            # Parse response into explanation, checklist, and spec_analysis
            spec_analysis = _parse_spec_analysis(raw_response)
            check_source = raw_response.split("[АНАЛИЗ СПЕЦИФИКАЦИЙ]")[0] if "[АНАЛИЗ СПЕЦИФИКАЦИЙ]" in raw_response else raw_response
            explanation_text = check_source.strip()
            checklist = []
            if "Что проверить" in check_source or "проверить" in check_source.lower():
                parts = check_source.split("Что проверить")
                if len(parts) > 1:
                    explanation_text = parts[0].strip()
                    checklist_text = parts[1]
                    checklist = [
                        line.strip().lstrip("•-0123456789.) ")
                        for line in checklist_text.split("\n")
                        if line.strip() and len(line.strip()) > 3
                    ]

            # Cache result (store full response for spec_analysis parsing on cache hit)
            stmt = pg_insert(LlmExplanation).values({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "risk_final": score.score_final,
                "risk_rules": score.score_rules,
                "risk_ml": score.score_ml,
                "explanation_text": raw_response,
                "checklist_jsonb": checklist,
                "model_used": settings.openai_model,
                "prompt_hash": prompt_hash,
                "created_at": datetime.utcnow(),
            })
            stmt = stmt.on_conflict_do_update(
                constraint="uq_llm_explanations_entity",
                set_={
                    "risk_final": score.score_final,
                    "explanation_text": raw_response,
                    "checklist_jsonb": checklist,
                    "model_used": settings.openai_model,
                    "prompt_hash": prompt_hash,
                    "created_at": datetime.utcnow(),
                },
            )
            await db.execute(stmt)
            await db.commit()

            return {
                "explanation": explanation_text,
                "checklist": checklist,
                "spec_analysis": spec_analysis,
                "risk_final": score.score_final,
                "risk_rules": score.score_rules,
                "risk_ml": score.score_ml,
                "model_used": settings.openai_model,
                "created_at": datetime.utcnow().isoformat(),
                "cached": False,
            }

        except Exception as e:
            logger.error(f"Explanation generation failed for {entity_type}/{entity_id}: {e}")
            return {
                "explanation": None,
                "error": str(e),
                "risk_final": score.score_final,
                "eligible": True,
            }
