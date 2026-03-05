from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from app.core.security import require_viewer
from app.llm.client import LLMClient

router = APIRouter()

SYSTEM_PROMPT = """Ты — AI-ассистент аналитика по государственным закупкам Казахстана (система Goszakup.kz).

Твоя роль: помогать аналитикам выявлять нарушения, ограничения конкуренции и манипулятивные технические спецификации в тендерах на основе данных системы Tender Radar.

Манипулятивные спецификации: указание на конкретные бренды, модели или узкие параметры, адаптированные под конкретного поставщика — ограничивают конкуренцию.

Ты знаешь следующие индикаторы риска (fraud flags):
- SINGLE_BIDDER: только один участник тендера (монопольная закупка)
- AMOUNT_ANOMALY: сумма контракта аномально высока по сравнению с медианой по категории
- REPEATED_WINNER: поставщик регулярно побеждает у одного и того же заказчика (аффилированность)
- SHORT_TIMELINE: подозрительно короткий срок приёма заявок
- ROUND_AMOUNT: сумма заканчивается на много нулей (признак фиктивных контрактов)
- DUMPING_FLAG: цена контракта подозрительно низкая (демпинг)
- HIGH_WIN_RATE_FEW_BIDS: высокий процент побед при малом количестве участников
- CAROUSEL_PATTERN: ротация победителей между аффилированными компаниями

Риск-скор от 0 до 100: 0-12 = LOW, 13-28 = MEDIUM, 29+ = HIGH.

При ответах:
1. Будь конкретным — ссылайся на флаги и риски
2. Объясняй простым языком почему это подозрительно
3. Давай практические рекомендации что проверить
4. Отвечай на русском языке
5. Будь кратким — не более 5-7 предложений на ответ"""


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    text: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]
    lotsContext: str = ""


@router.post("/")
async def chat(
    body: ChatRequest,
    _=Depends(require_viewer),
):
    """
    AI Assistant chat endpoint. Proxies conversation to OpenAI via LLMClient.
    """
    try:
        llm = LLMClient()

        # Build full conversation as a single user prompt (since LLMClient supports system+user)
        # For multi-turn, concatenate history
        history_lines = []
        for m in body.messages[:-1]:  # all but last
            prefix = "Пользователь" if m.role == "user" else "Ассистент"
            history_lines.append(f"{prefix}: {m.text}")

        last_user = next(
            (m.text for m in reversed(body.messages) if m.role == "user"), ""
        )

        context_block = ""
        if body.lotsContext:
            context_block = f"\n\nТекущие топ-лоты системы:\n{body.lotsContext}"

        system = SYSTEM_PROMPT + context_block
        if history_lines:
            user_prompt = "\n".join(history_lines) + "\nПользователь: " + last_user
        else:
            user_prompt = last_user

        text = await llm.generate(system_prompt=system, user_prompt=user_prompt)
        return {"text": text}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
