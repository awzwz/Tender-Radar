import { NextRequest, NextResponse } from "next/server";

const OPENAI_API_BASE = "https://api.openai.com/v1";
const MODEL = "gpt-4o-mini";

const SYSTEM_PROMPT = `Ты — AI-ассистент аналитика по государственным закупкам Казахстана (система Goszakup.kz).

Твоя роль: помогать аналитикам выявлять нарушения, ограничения конкуренции и манипулятивные технические спецификации в тендерах на основе данных системы Tender Radar.

Манипулятивные спецификации: указание на конкретные бренды, модели или узкие параметры, адаптированные под конкретного поставщика — ограничивают конкуренцию.

Ты знаешь следующие индикаторы риска (fraud flags):
- SINGLE_BIDDER: только один участник тендера (монопольная закупка)
- AMOUNT_ANOMALY: сумма контракта аномально высока по сравнению с медианой по категории (robust Z-score)
- REPEATED_WINNER: поставщик регулярно побеждает у одного и того же заказчика (подозрение на аффилированность)
- SHORT_TIMELINE: подозрительно короткий срок приёма заявок (ограничивает конкуренцию)
- ROUND_AMOUNT: сумма заканчивается на много нулей (часто признак фиктивных контрактов)
- DUMPING_FLAG: цена контракта подозрительно низкая (демпинг)
- HIGH_WIN_RATE_FEW_BIDS: высокий процент побед при малом количестве участников
- CAROUSEL_PATTERN: ротация победителей между аффилированными компаниями

Риск-скор от 0 до 100: 0-12 = LOW, 13-28 = MEDIUM, 29+ = HIGH.

При ответах:
1. Будь конкретным — ссылайся на флаги и риски
2. Объясняй простым языком почему это подозрительно
3. Давай практические рекомендации что проверить
4. Если спрашивают о конкретном тендере — дай полный анализ
5. Отвечай на русском языке
6. Будь кратким и по делу — не более 5-7 предложений на ответ`;

export async function POST(req: NextRequest) {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
        return NextResponse.json({ error: "OPENAI_API_KEY not configured" }, { status: 500 });
    }

    try {
        const { messages, lotsContext } = await req.json();

        const contextBlock = "\n\nТекущие топ-лоты системы:\n" + (lotsContext || "Данные загружаются.");

        const openaiMessages: { role: "system" | "user" | "assistant"; content: string }[] = [
            { role: "system", content: SYSTEM_PROMPT + contextBlock },
        ];

        for (const m of messages as { role: string; text: string }[]) {
            if (m.role === "user") {
                openaiMessages.push({ role: "user", content: m.text });
            } else if (m.role === "assistant" && m.text) {
                openaiMessages.push({ role: "assistant", content: m.text });
            }
        }

        const response = await fetch(`${OPENAI_API_BASE}/chat/completions`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${apiKey}`,
            },
            body: JSON.stringify({
                model: MODEL,
                messages: openaiMessages,
                temperature: 0.7,
                max_tokens: 1024,
            }),
        });

        const data = await response.json();

        if (!response.ok) {
            const errMsg = data.error?.message || JSON.stringify(data);
            return NextResponse.json({ error: `OpenAI API: ${errMsg}` }, { status: 500 });
        }

        const text = data.choices?.[0]?.message?.content?.trim();
        if (!text) {
            return NextResponse.json({ error: "Empty response from OpenAI" }, { status: 500 });
        }

        return NextResponse.json({ text });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Unknown error";
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
