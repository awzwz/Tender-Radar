// i18n dictionaries — English / Russian / Kazakh.
// Core (shell/login/onboarding) lives here; per-screen keys live in ./parts/* and
// are merged in. t(key) resolves against the active language, falling back to
// Russian, then the raw key. Supports {param} interpolation.

import type { Dict, Lang, LangDict } from "./types";
import { dashboardDict } from "./parts/dashboard";
import { listsDict } from "./parts/lists";
import { lotDetailDict } from "./parts/lotDetail";
import { casesDict } from "./parts/cases";
import { companyDict } from "./parts/company";
import { mapDict } from "./parts/map";
import { priceDict } from "./parts/price";

export type { Lang } from "./types";

export const LANGS: { code: Lang; label: string; native: string }[] = [
  { code: "en", label: "EN", native: "English" },
  { code: "ru", label: "RU", native: "Русский" },
  { code: "kk", label: "KK", native: "Қазақша" },
];

const core: LangDict = {
  // ─────────────────────────────── ENGLISH ───────────────────────────────
  en: {
    "brand.subtitle": "AI Risk Platform",

    "nav.group.menu": "Menu",
    "nav.group.tools": "Tools",
    "nav.group.ai": "AI",

    "nav.dashboard": "Dashboard",
    "nav.dashboard.hint": "Data summary",
    "nav.lots": "Lots",
    "nav.lots.hint": "Lots list",
    "nav.tenders": "Tenders",
    "nav.tenders.hint": "Tenders list",
    "nav.detail": "Lot Detail",
    "nav.detail.hint": "Lot information",
    "nav.cases": "Cases",
    "nav.cases.hint": "Workspace",
    "nav.specnorm": "Tender Analysis",
    "nav.specnorm.hint": "Spec vs Norm · LLM",
    "nav.bidcheck": "BidCheck",
    "nav.bidcheck.hint": "Parse PDF · Suppliers",
    "nav.company": "Company Intel",
    "nav.company.hint": "Dossier by BIN / name",
    "nav.ai": "AI Assistant",
    "nav.ai.hint": "OpenAI assistant",
    "nav.price": "Price Radar",
    "nav.price.hint": "Overpricing detector",

    "action.collapse": "Collapse",
    "action.logout": "Log out",
    "action.demo": "Demo",
    "action.export": "Export",
    "action.open": "Open",

    "header.lots": "{n} lots · updated daily",
    "header.noData": "No data",

    "login.tagline": "AI platform for public-procurement risk analysis",
    "login.feature.dashboard": "Risk Dashboard",
    "login.feature.dashboard.sub": "Aggregated HIGH / MEDIUM / LOW analytics",
    "login.feature.bidcheck": "BidCheck",
    "login.feature.bidcheck.sub": "Supplier compliance vs technical spec",
    "login.feature.specnorm": "Tender Analysis",
    "login.feature.specnorm.sub": "LLM analysis of technical specifications",
    "login.welcome": "Welcome!",
    "login.subtitle": "Sign in to continue",
    "login.username": "Username",
    "login.username.ph": "Enter username",
    "login.password": "Password",
    "login.signin": "Sign in",
    "login.signingIn": "Signing in...",
    "login.footer": "Tender Risk Radar · AI procurement analysis",
    "login.err.invalid": "Invalid credentials",
    "login.err.network": "No connection to server. Check your connection or backend status.",
    "login.err.generic": "Login error",

    "onboarding.title": "Welcome to Tender Radar",
    "onboarding.score.b": "Risk Score 0–100",
    "onboarding.score.t": " — overall risk level (LOW/MEDIUM/HIGH)",
    "onboarding.detail.b": "Lot Detail",
    "onboarding.detail.t": " — lot card with indicators and AI explanation",
    "onboarding.explain.b": "Explain",
    "onboarding.explain.t": " — AI analyses risks and manipulative specifications",
    "onboarding.filters.b": "Filters",
    "onboarding.filters.t": " — min risk, level, search by number/customer",
    "onboarding.run": "Run demo",
    "onboarding.skip": "Skip",

    "stub.specnorm.loading": "Tender Analysis loading...",
    "stub.specnorm.open": "Open Tender Analysis",
    "stub.bidcheck.loading": "BidCheck loading...",
    "stub.bidcheck.open": "Open BidCheck",
  },

  // ─────────────────────────────── RUSSIAN ───────────────────────────────
  ru: {
    "brand.subtitle": "AI-платформа рисков",

    "nav.group.menu": "Меню",
    "nav.group.tools": "Инструменты",
    "nav.group.ai": "AI",

    "nav.dashboard": "Дашборд",
    "nav.dashboard.hint": "Сводка данных",
    "nav.lots": "Лоты",
    "nav.lots.hint": "Список лотов",
    "nav.tenders": "Тендеры",
    "nav.tenders.hint": "Список тендеров",
    "nav.detail": "Детали лота",
    "nav.detail.hint": "Информация по лоту",
    "nav.cases": "Дела",
    "nav.cases.hint": "Рабочее место",
    "nav.specnorm": "Анализ тендера",
    "nav.specnorm.hint": "Spec vs Norm · LLM",
    "nav.bidcheck": "BidCheck",
    "nav.bidcheck.hint": "Разбор PDF · Поставщики",
    "nav.company": "Компании",
    "nav.company.hint": "Досье по БИН / имени",
    "nav.ai": "AI-ассистент",
    "nav.ai.hint": "Ассистент OpenAI",
    "nav.price": "Радар цен",
    "nav.price.hint": "Детектор переплат",

    "action.collapse": "Свернуть",
    "action.logout": "Выйти",
    "action.demo": "Демо",
    "action.export": "Экспорт",
    "action.open": "Открыть",

    "header.lots": "{n} лотов · обновляется ежедневно",
    "header.noData": "Нет данных",

    "login.tagline": "AI-платформа анализа рисков государственных закупок",
    "login.feature.dashboard": "Dashboard рисков",
    "login.feature.dashboard.sub": "Сводная аналитика HIGH / MEDIUM / LOW",
    "login.feature.bidcheck": "BidCheck",
    "login.feature.bidcheck.sub": "Проверка соответствия поставщиков ТЗ",
    "login.feature.specnorm": "Tender Analysis",
    "login.feature.specnorm.sub": "LLM-анализ технических спецификаций",
    "login.welcome": "Добро пожаловать!",
    "login.subtitle": "Войдите в систему для продолжения",
    "login.username": "Логин",
    "login.username.ph": "Введите логин",
    "login.password": "Пароль",
    "login.signin": "Войти",
    "login.signingIn": "Вход...",
    "login.footer": "Tender Risk Radar · AI-анализ госзакупок",
    "login.err.invalid": "Неверные данные",
    "login.err.network": "Нет связи с сервером. Проверьте подключение или статус бэкенда.",
    "login.err.generic": "Ошибка входа",

    "onboarding.title": "Добро пожаловать в Tender Radar",
    "onboarding.score.b": "Risk Score 0–100",
    "onboarding.score.t": " — общий уровень риска (LOW/MEDIUM/HIGH)",
    "onboarding.detail.b": "Lot Detail",
    "onboarding.detail.t": " — карточка лота с индикаторами и AI-объяснением",
    "onboarding.explain.b": "Объяснить",
    "onboarding.explain.t": " — AI анализирует риски и манипулятивные спецификации",
    "onboarding.filters.b": "Фильтры",
    "onboarding.filters.t": " — min risk, уровень, поиск по номеру/заказчику",
    "onboarding.run": "Запустить демо",
    "onboarding.skip": "Пропустить",

    "stub.specnorm.loading": "Tender Analysis загружается...",
    "stub.specnorm.open": "Открыть Tender Analysis",
    "stub.bidcheck.loading": "BidCheck загружается...",
    "stub.bidcheck.open": "Открыть BidCheck",
  },

  // ─────────────────────────────── KAZAKH ───────────────────────────────
  kk: {
    "brand.subtitle": "AI тәуекел платформасы",

    "nav.group.menu": "Мәзір",
    "nav.group.tools": "Құралдар",
    "nav.group.ai": "AI",

    "nav.dashboard": "Бақылау тақтасы",
    "nav.dashboard.hint": "Деректер жиынтығы",
    "nav.lots": "Лоттар",
    "nav.lots.hint": "Лоттар тізімі",
    "nav.tenders": "Тендерлер",
    "nav.tenders.hint": "Тендерлер тізімі",
    "nav.detail": "Лот мәліметтері",
    "nav.detail.hint": "Лот туралы ақпарат",
    "nav.cases": "Істер",
    "nav.cases.hint": "Жұмыс орны",
    "nav.specnorm": "Тендер талдауы",
    "nav.specnorm.hint": "Spec vs Norm · LLM",
    "nav.bidcheck": "BidCheck",
    "nav.bidcheck.hint": "PDF талдау · Жеткізушілер",
    "nav.company": "Компаниялар",
    "nav.company.hint": "БСН / атау бойынша дерек",
    "nav.ai": "AI көмекші",
    "nav.ai.hint": "OpenAI көмекшісі",
    "nav.price": "Баға радары",
    "nav.price.hint": "Артық баға детекторы",

    "action.collapse": "Жию",
    "action.logout": "Шығу",
    "action.demo": "Демо",
    "action.export": "Экспорт",
    "action.open": "Ашу",

    "header.lots": "{n} лот · күн сайын жаңарады",
    "header.noData": "Дерек жоқ",

    "login.tagline": "Мемлекеттік сатып алу тәуекелдерін талдау AI платформасы",
    "login.feature.dashboard": "Тәуекел дашборды",
    "login.feature.dashboard.sub": "HIGH / MEDIUM / LOW жиынтық талдауы",
    "login.feature.bidcheck": "BidCheck",
    "login.feature.bidcheck.sub": "Жеткізушінің техспецификацияға сәйкестігі",
    "login.feature.specnorm": "Тендер талдауы",
    "login.feature.specnorm.sub": "Техникалық сипаттаманы LLM талдауы",
    "login.welcome": "Қош келдіңіз!",
    "login.subtitle": "Жалғастыру үшін жүйеге кіріңіз",
    "login.username": "Логин",
    "login.username.ph": "Логинді енгізіңіз",
    "login.password": "Құпиясөз",
    "login.signin": "Кіру",
    "login.signingIn": "Кіру...",
    "login.footer": "Tender Risk Radar · сатып алуды AI талдау",
    "login.err.invalid": "Қате деректер",
    "login.err.network": "Сервермен байланыс жоқ. Қосылымды немесе бэкенд күйін тексеріңіз.",
    "login.err.generic": "Кіру қатесі",

    "onboarding.title": "Tender Radar-ға қош келдіңіз",
    "onboarding.score.b": "Risk Score 0–100",
    "onboarding.score.t": " — жалпы тәуекел деңгейі (LOW/MEDIUM/HIGH)",
    "onboarding.detail.b": "Lot Detail",
    "onboarding.detail.t": " — индикаторлар мен AI түсіндірмесі бар лот картасы",
    "onboarding.explain.b": "Түсіндіру",
    "onboarding.explain.t": " — AI тәуекелдер мен манипулятивті сипаттамаларды талдайды",
    "onboarding.filters.b": "Сүзгілер",
    "onboarding.filters.t": " — min risk, деңгей, нөмір/тапсырыс беруші бойынша іздеу",
    "onboarding.run": "Демо қосу",
    "onboarding.skip": "Өткізу",

    "stub.specnorm.loading": "Tender Analysis жүктелуде...",
    "stub.specnorm.open": "Tender Analysis ашу",
    "stub.bidcheck.loading": "BidCheck жүктелуде...",
    "stub.bidcheck.open": "BidCheck ашу",
  },
};

const PARTS: LangDict[] = [dashboardDict, listsDict, lotDetailDict, casesDict, companyDict, mapDict, priceDict];

function build(): Record<Lang, Dict> {
  const out: Record<Lang, Dict> = { en: { ...core.en }, ru: { ...core.ru }, kk: { ...core.kk } };
  for (const p of PARTS) {
    (["en", "ru", "kk"] as Lang[]).forEach((l) => Object.assign(out[l], p[l]));
  }
  return out;
}

export const dictionaries: Record<Lang, Dict> = build();
