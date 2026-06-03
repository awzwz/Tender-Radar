import type { LangDict } from "../types";

// Keys for CasesView ("cases.") + AIAssistant ("ai.").
export const casesDict: LangDict = {
  // ─────────────────────────────── ENGLISH ───────────────────────────────
  en: {
    // CasesView
    "cases.title": "Cases",
    "cases.hint": "Review results. Label source for ML.",
    "cases.empty": "No cases yet. Create a case in Lot Detail.",
    "cases.lotFallback": "Lot #{n}",
    "cases.mlNote": "In production: cases are exported as ground-truth labels for ML",
    "cases.status.NEW": "New",
    "cases.status.IN_REVIEW": "In review",
    "cases.status.CONFIRMED": "Confirmed",
    "cases.status.DISMISSED": "Dismissed",

    // AIAssistant
    "ai.title": "AI Assistant",
    "ai.subtitle": "OpenAI · gpt‑4o‑mini",
    "ai.safeMode": "Safe mode",
    "ai.greeting":
      "Hi! I'm an AI assistant powered by OpenAI. Ask me about a tender, a supplier or risks. For example: «What does the FEW_BIDS flag mean?» or «Explain the risks of a lot with a high score».",
    "ai.thinking": "AI is thinking...",
    "ai.input.ph": "Ask anything about tenders...",
    "ai.ask": "Ask",
    "ai.footer": "OpenAI · knows the system's top-10 lots · context updates",
    "ai.err.network": "Network error",
    "ai.err.api": "API error",
    "ai.err.unknown": "Unknown error",
    "ai.err.prefix": "⚠ Error: ",
  },

  // ─────────────────────────────── RUSSIAN ───────────────────────────────
  ru: {
    // CasesView
    "cases.title": "Cases",
    "cases.hint": "Результаты проверки. Источник меток для ML.",
    "cases.empty": "Кейсов пока нет. Создай кейс в Lot Detail.",
    "cases.lotFallback": "Лот #{n}",
    "cases.mlNote": "В проде: кейсы экспортируются как ground-truth labels для ML",
    "cases.status.NEW": "Новый",
    "cases.status.IN_REVIEW": "На проверке",
    "cases.status.CONFIRMED": "Подтверждён",
    "cases.status.DISMISSED": "Отклонён",

    // AIAssistant
    "ai.title": "AI Assistant",
    "ai.subtitle": "OpenAI · gpt‑4o‑mini",
    "ai.safeMode": "Safe mode",
    "ai.greeting":
      "Привет! Я AI‑ассистент на базе OpenAI. Спроси про тендер, поставщика или риски. Например: «Что значит флаг FEW_BIDS?» или «Объясни риски лота с высоким скором».",
    "ai.thinking": "AI думает...",
    "ai.input.ph": "Задай любой вопрос про тендеры...",
    "ai.ask": "Ask",
    "ai.footer": "OpenAI · знает топ-10 лотов системы · контекст обновляется",
    "ai.err.network": "Ошибка сети",
    "ai.err.api": "Ошибка API",
    "ai.err.unknown": "Неизвестная ошибка",
    "ai.err.prefix": "⚠ Ошибка: ",
  },

  // ─────────────────────────────── KAZAKH ───────────────────────────────
  kk: {
    // CasesView
    "cases.title": "Cases",
    "cases.hint": "Тексеру нәтижелері. ML үшін белгілер көзі.",
    "cases.empty": "Әзірге істер жоқ. Lot Detail бөлімінде іс жасаңыз.",
    "cases.lotFallback": "Лот #{n}",
    "cases.mlNote": "Өндірісте: істер ML үшін ground-truth белгілері ретінде экспортталады",
    "cases.status.NEW": "Жаңа",
    "cases.status.IN_REVIEW": "Тексеруде",
    "cases.status.CONFIRMED": "Расталды",
    "cases.status.DISMISSED": "Қабылданбады",

    // AIAssistant
    "ai.title": "AI Assistant",
    "ai.subtitle": "OpenAI · gpt‑4o‑mini",
    "ai.safeMode": "Safe mode",
    "ai.greeting":
      "Сәлем! Мен OpenAI негізіндегі AI көмекшісімін. Тендер, жеткізуші немесе тәуекелдер туралы сұраңыз. Мысалы: «FEW_BIDS жалаушасы нені білдіреді?» немесе «Жоғары ұпайы бар лоттың тәуекелдерін түсіндір».",
    "ai.thinking": "AI ойлануда...",
    "ai.input.ph": "Тендерлер туралы кез келген сұрақ қойыңыз...",
    "ai.ask": "Ask",
    "ai.footer": "OpenAI · жүйенің топ-10 лотын біледі · контекст жаңарып отырады",
    "ai.err.network": "Желі қатесі",
    "ai.err.api": "API қатесі",
    "ai.err.unknown": "Белгісіз қате",
    "ai.err.prefix": "⚠ Қате: ",
  },
};
