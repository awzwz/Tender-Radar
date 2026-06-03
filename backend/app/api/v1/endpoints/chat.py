import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List

from app.core.config import settings
from app.core.security import require_viewer
from app.llm.client import LLMClient
from app.features.engine import _CONFIG as _RISK_CONFIG

logger = logging.getLogger(__name__)
router = APIRouter()

# Build the REAL indicator list from the same config the scoring engine uses,
# so the assistant never invents indicators that don't exist in the system.
_INDICATORS = _RISK_CONFIG.get("indicators", {})
_INDICATOR_LINES = "\n".join(
    f"- {code}: {meta.get('description', '')}" for code, meta in _INDICATORS.items()
)

SYSTEM_PROMPT = f"""Ты — AI-ассистент аналитика по государственным закупкам Казахстана (система Tender Radar, данные goszakup.gov.kz).

Роль: помогать аналитикам выявлять нарушения, ограничения конкуренции и манипулятивные технические спецификации в тендерах.

Манипулятивные спецификации: указание конкретных брендов, моделей или узких параметров под одного поставщика — ограничивают конкуренцию.

Индикаторы риска системы (по ним считается риск-скор). Ссылайся только на эти коды:
{_INDICATOR_LINES}

Риск-скор 0–100 — взвешенная сумма сработавших индикаторов: LOW 0–25, MEDIUM 26–45, HIGH 46+.
LLM используется только для объяснений, НЕ для скоринга.

Правила ответа:
1. Конкретика — ссылайся на реальные индикаторы выше по их коду.
2. Объясняй простым языком, почему это подозрительно.
3. Давай практические рекомендации, что проверить.
4. Отвечай на русском языке.
5. Кратко — 4–7 предложений."""


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    text: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]
    lotsContext: str = ""


@router.post("/")
async def chat(body: ChatRequest, _=Depends(require_viewer)):
    """AI Assistant chat endpoint. Forwards a real multi-turn conversation to OpenAI."""
    if not settings.openai_api_key:
        raise HTTPException(
            status_code=503,
            detail="OpenAI не настроен: задайте OPENAI_API_KEY в backend/.env и перезапустите backend.",
        )

    # System prompt + optional live context about the top risky lots.
    system = SYSTEM_PROMPT
    if body.lotsContext.strip():
        system += f"\n\nТекущие топ-лоты системы (контекст для ответа):\n{body.lotsContext.strip()}"

    # Build a proper OpenAI messages array, preserving role structure.
    # Skip any leading assistant messages (the canned UI greeting) before the
    # first user turn.
    oai_messages = [{"role": "system", "content": system}]
    seen_user = False
    for m in body.messages:
        role = "assistant" if m.role == "assistant" else "user"
        if role == "assistant" and not seen_user:
            continue
        if role == "user":
            seen_user = True
        content = (m.text or "").strip()
        if content:
            oai_messages.append({"role": role, "content": content})

    if len(oai_messages) == 1:
        raise HTTPException(status_code=400, detail="Пустой запрос")

    try:
        text = await LLMClient().chat_completion(oai_messages)
        return {"text": text}
    except Exception as e:
        logger.error("Chat completion failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Ошибка OpenAI: {e}")
