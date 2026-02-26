import { NextRequest, NextResponse } from "next/server";

const MODEL = "gpt-4o-mini";

const ANALYSIS_SYSTEM = `Ты AI-аналитик по проверке соответствия документов в государственных закупках Казахстана.
Тебе предоставлены данные тендера: техническая спецификация, нормативы и документы поставщика.

Проведи ГЛУБОКИЙ АНАЛИЗ и ответь на русском языке:

1. Оцени соответствие квалификационных документов поставщика требованиям техспецификации
2. Выяви конкретные несоответствия (если есть):
   - Недостаточная квалификация персонала
   - Истёкшие или отсутствующие лицензии
   - Несоответствие опыта работ
   - Недостаточная материально-техническая база
3. Оцени завышены ли требования заказчика по сравнению с нормативами
4. Дай итоговое заключение: является ли данный тендер подозрительным

Формат ответа: развёрнутый аналитический текст на 5-8 предложений.`;

export async function POST(req: NextRequest) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return NextResponse.json({ error: "OPENAI_API_KEY not configured" }, { status: 500 });
  }

  try {
    const body = await req.json();
    const { tenderId, techspecText, normText, supplierText, summary } = body;

    const userPrompt = [
      `Тендер: ${tenderId}`,
      summary ? `Краткое описание: ${summary}` : null,
      techspecText ? `\n=== ТЕХНИЧЕСКАЯ СПЕЦИФИКАЦИЯ (извлечение) ===\n${techspecText.slice(0, 3000)}` : null,
      normText ? `\n=== НОРМАТИВНЫЙ ДОКУМЕНТ (извлечение) ===\n${normText.slice(0, 2000)}` : null,
      supplierText ? `\n=== ДОКУМЕНТЫ ПОСТАВЩИКА (извлечение) ===\n${supplierText.slice(0, 2500)}` : null,
    ].filter(Boolean).join("\n");

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          { role: "system", content: ANALYSIS_SYSTEM },
          { role: "user", content: userPrompt },
        ],
        temperature: 0.5,
        max_tokens: 1500,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      const errMsg = data.error?.message || JSON.stringify(data);
      return NextResponse.json({ error: `OpenAI: ${errMsg}` }, { status: 500 });
    }

    const analysis = data.choices?.[0]?.message?.content?.trim();
    return NextResponse.json({
      analysis,
      model: MODEL,
      tenderId,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
