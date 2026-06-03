import type { LangDict } from "../types";

// Keys for DashboardView ("dash.") + FilterPanel ("filter.").
export const dashboardDict: LangDict = {
  // ─────────────────────────────── ENGLISH ───────────────────────────────
  en: {
    // DashboardView
    "dash.title": "Dashboard",
    "dash.hint": "Overall risk overview and analytics",
    "dash.toggle.lots": "Lots",
    "dash.toggle.tenders": "Tenders",
    "dash.card.avgRisk": "Avg risk",
    "dash.card.scored": "Scored: {n}",
    "dash.card.highMainLots": "HIGH (2+ high lots)",
    "dash.card.singleCase": "Single-case alerts",
    "dash.dist.title": "Risk Distribution",
    "dash.dist.hint.lots": "Lot distribution by level",
    "dash.dist.hint.tenders": "Tender distribution by level",
    "dash.dist.total": "Total",
    "dash.top.title.lots": "Top risky lots",
    "dash.top.title.tenders": "Top risky tenders",
    "dash.top.hint.lots": "Click to open the card",
    "dash.top.hint.tenders": "Aggregated risks",
    "dash.top.total": "Total: {n}",
    "dash.loading": "Loading...",
    "dash.empty": "No data yet — ETL is still loading",
    "dash.noName": "Untitled",
    "dash.tender.meta": "{number} · Lots: {lots} · HIGH lots: {high}",

    // FilterPanel
    "filter.search.label": "Quick search",
    "filter.search.ph": "number, customer...",
    "filter.minRisk": "Min risk",
    "filter.level.all": "ALL",
    "filter.selected.title": "Selected Lot",
    "filter.selected.none": "Not selected",
    "filter.selected.open": "Open",
  },

  // ─────────────────────────────── RUSSIAN ───────────────────────────────
  ru: {
    // DashboardView
    "dash.title": "Дашборд",
    "dash.hint": "Общий обзор рисков и аналитика",
    "dash.toggle.lots": "Лоты",
    "dash.toggle.tenders": "Тендеры",
    "dash.card.avgRisk": "Средний риск",
    "dash.card.scored": "Оценено: {n}",
    "dash.card.highMainLots": "HIGH (2+ высоких лотов)",
    "dash.card.singleCase": "Единичные алерты",
    "dash.dist.title": "Распределение рисков",
    "dash.dist.hint.lots": "Распределение лотов по уровням",
    "dash.dist.hint.tenders": "Распределение тендеров по уровням",
    "dash.dist.total": "Всего",
    "dash.top.title.lots": "Топ рискованных лотов",
    "dash.top.title.tenders": "Топ рискованных тендеров",
    "dash.top.hint.lots": "Нажми, чтобы открыть карточку",
    "dash.top.hint.tenders": "Агрегированные риски",
    "dash.top.total": "Всего: {n}",
    "dash.loading": "Загрузка...",
    "dash.empty": "Данных пока нет — ETL ещё загружает",
    "dash.noName": "Без названия",
    "dash.tender.meta": "{number} · Лотов: {lots} · HIGH-лотов: {high}",

    // FilterPanel
    "filter.search.label": "Быстрый поиск",
    "filter.search.ph": "номер, заказчик...",
    "filter.minRisk": "Мин. риск",
    "filter.level.all": "ALL",
    "filter.selected.title": "Выбранный лот",
    "filter.selected.none": "Не выбран",
    "filter.selected.open": "Открыть",
  },

  // ─────────────────────────────── KAZAKH ───────────────────────────────
  kk: {
    // DashboardView
    "dash.title": "Бақылау тақтасы",
    "dash.hint": "Тәуекелдердің жалпы шолуы және аналитика",
    "dash.toggle.lots": "Лоттар",
    "dash.toggle.tenders": "Тендерлер",
    "dash.card.avgRisk": "Орташа тәуекел",
    "dash.card.scored": "Бағаланды: {n}",
    "dash.card.highMainLots": "HIGH (2+ жоғары лот)",
    "dash.card.singleCase": "Жеке алерттер",
    "dash.dist.title": "Тәуекелдерді бөлу",
    "dash.dist.hint.lots": "Лоттарды деңгей бойынша бөлу",
    "dash.dist.hint.tenders": "Тендерлерді деңгей бойынша бөлу",
    "dash.dist.total": "Барлығы",
    "dash.top.title.lots": "Тәуекелі жоғары лоттар",
    "dash.top.title.tenders": "Тәуекелі жоғары тендерлер",
    "dash.top.hint.lots": "Картаны ашу үшін басыңыз",
    "dash.top.hint.tenders": "Жиынтық тәуекелдер",
    "dash.top.total": "Барлығы: {n}",
    "dash.loading": "Жүктелуде...",
    "dash.empty": "Әзірге дерек жоқ — ETL әлі жүктеп жатыр",
    "dash.noName": "Атауы жоқ",
    "dash.tender.meta": "{number} · Лоттар: {lots} · HIGH-лоттар: {high}",

    // FilterPanel
    "filter.search.label": "Жылдам іздеу",
    "filter.search.ph": "нөмір, тапсырыс беруші...",
    "filter.minRisk": "Мин. тәуекел",
    "filter.level.all": "ALL",
    "filter.selected.title": "Таңдалған лот",
    "filter.selected.none": "Таңдалмаған",
    "filter.selected.open": "Ашу",
  },
};
