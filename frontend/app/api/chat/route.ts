import { NextRequest, NextResponse } from "next/server";

const GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta";
// Try models in order, fallback to next if unavailable
const MODEL_PRIORITY = [
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-flash-latest",
];

const SYSTEM_PROMPT = `Ты — AI-ассистент аналитика по государственным закупкам Казахстана (система Goszakup.kz).

Твоя роль: помогать аналитикам выявлять нарушения и коррупционные риски в тендерах на основе данных системы Tender Radar.

Ты знаешь следующие индикаторы риска (fraud flags):
- SINGLE_BIDDER: только один участник тендера (монопольная закупка)
- AMOUNT_ANOMALY: сумма контракта аномально высока по сравнению с медианой по категории (robust Z-score)
- REPEATED_WINNER: поставщик регулярно побеждает у одного и того же заказчика (подозрение на аффилированность)
- SHORT_TIMELINE: подозрительно короткий срок приёма заявок (ограничивает конкуренцию)
- ROUND_AMOUNT: сумма заканчивается на много нулей (часто признак фиктивных контрактов)
- DUMPING_FLAG: цена контракта подозрительно низкая (демпинг)
- HIGH_WIN_RATE_FEW_BIDS: высокий процент побед при малом количестве участников
- CAROUSEL_PATTERN: ротация победителей между аффилированными компаниями

Риск-скор от 0 до 100: 0-39 = LOW, 40-69 = MEDIUM, 70-100 = HIGH.

При ответах:
1. Будь конкретным — ссылайся на флаги и риски
2. Объясняй простым языком почему это подозрительно
3. Давай практические рекомендации что проверить
4. Если спрашивают о конкретном тендере — дай полный анализ
5. Отвечай на русском языке
6. Будь кратким и по делу — не более 5-7 предложений на ответ`;

export async function POST(req: NextRequest) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        return NextResponse.json({ error: "GEMINI_API_KEY not configured" }, { status: 500 });
    }

    try {
        const { messages, lotsContext } = await req.json();

        // Build conversation as a flat list of turns (Gemini requires alternating user/model)
        const userMessages = messages.filter((m: { role: string }) => m.role === "user");
        const assistantMessages = messages.filter((m: { role: string }) => m.role === "assistant");

        // Build contents array: system context first, then conversation
        const contents: { role: string; parts: { text: string }[] }[] = [
            {
                role: "user",
                parts: [{ text: SYSTEM_PROMPT + "\n\nТекущие топ-лоты системы:\n" + (lotsContext || "Данные загружаются.") }],
            },
            {
                role: "model",
                parts: [{ text: "Понял контекст! Готов анализировать тендеры. Чем могу помочь?" }],
            },
        ];

        // Add conversation history (interleaved user/model)
        const conversationLength = Math.max(userMessages.length, assistantMessages.length - 1);
        for (let i = 0; i < conversationLength; i++) {
            if (userMessages[i]) {
                contents.push({ role: "user", parts: [{ text: userMessages[i].text }] });
            }
            // Skip first assistant message (it's the welcome message, already covered)
            if (assistantMessages[i + 1]) {
                contents.push({ role: "model", parts: [{ text: assistantMessages[i + 1].text }] });
            }
        }

        const requestBody = {
            contents,
            generationConfig: {
                temperature: 0.7,
                maxOutputTokens: 1024,
            },
        };

        // Try models in priority order
        let lastError = "";
        for (const model of MODEL_PRIORITY) {
            const url = `${GEMINI_API_BASE}/models/${model}:generateContent?key=${apiKey}`;
            const response = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(requestBody),
            });

            const data = await response.json();

            if (response.ok && data.candidates?.[0]?.content?.parts?.[0]?.text) {
                return NextResponse.json({ text: data.candidates[0].content.parts[0].text });
            }

            // If 404 (model not found), try next; if quota, return error immediately
            if (data.error?.code === 429) {
                return NextResponse.json(
                    { error: `Превышен лимит запросов Gemini API. Подожди немного и повтори (429).` },
                    { status: 429 }
                );
            }
            lastError = data.error?.message || JSON.stringify(data);
            if (data.error?.code !== 404) break;
        }

        return NextResponse.json({ error: `Gemini API: ${lastError}` }, { status: 500 });
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Unknown error";
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
