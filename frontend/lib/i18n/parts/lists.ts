import type { LangDict } from "../types";

// Keys for RiskListView + TenderListView + shared/ui.
export const listsDict: LangDict = {
  // ─────────────────────────────── ENGLISH ───────────────────────────────
  en: {
    // RiskListView
    "riskList.title": "Risk List",
    "riskList.hint": "Sortable table · Click a header to sort",
    "riskList.itemsPage": "{n} items · page {page}/{total}",
    "riskList.loading": "Loading...",
    "riskList.empty": "No results. Lower Min risk or change the filter.",
    "riskList.col.lot": "Lot",
    "riskList.col.customer": "Customer",
    "riskList.col.amount": "Amount",
    "riskList.col.risk": "Risk",
    "riskList.col.action": "Action",
    "riskList.noName": "Untitled",
    "riskList.open": "Open",
    "riskList.loadMoreServer": "Load more from server",

    // TenderListView
    "tenderList.title": "Tenders",
    "tenderList.inBase": "{n} tenders in the database",
    "tenderList.searchPh": "Search by number / BIN...",
    "tenderList.category.all": "All",
    "tenderList.category.main_high": "MAIN HIGH",
    "tenderList.category.single_case": "Single Case",
    "tenderList.category.regular": "Regular",
    "tenderList.col.tender": "Tender",
    "tenderList.col.customerBin": "Customer BIN",
    "tenderList.col.amount": "Amount",
    "tenderList.col.lotsHighTotal": "Lots HIGH/total",
    "tenderList.col.category": "Category",
    "tenderList.col.risk": "Risk",
    "tenderList.loading": "Loading...",
    "tenderList.notFound": "No tenders found",
    "tenderList.loadMore": "Load more ({n})",

    // shared/ui
    "ui.updatedDaily": "Updated daily",
  },

  // ─────────────────────────────── RUSSIAN ───────────────────────────────
  ru: {
    // RiskListView
    "riskList.title": "Risk List",
    "riskList.hint": "Сортируемая таблица · Клик по заголовку для сортировки",
    "riskList.itemsPage": "{n} items · page {page}/{total}",
    "riskList.loading": "Загрузка...",
    "riskList.empty": "Нет результатов. Уменьши Min risk или измени фильтр.",
    "riskList.col.lot": "Лот",
    "riskList.col.customer": "Заказчик",
    "riskList.col.amount": "Сумма",
    "riskList.col.risk": "Risk",
    "riskList.col.action": "Action",
    "riskList.noName": "Без названия",
    "riskList.open": "Open",
    "riskList.loadMoreServer": "Загрузить ещё с сервера",

    // TenderListView
    "tenderList.title": "Тендеры",
    "tenderList.inBase": "{n} тендеров в базе",
    "tenderList.searchPh": "Поиск по номеру / БИН...",
    "tenderList.category.all": "Все",
    "tenderList.category.main_high": "MAIN HIGH",
    "tenderList.category.single_case": "Single Case",
    "tenderList.category.regular": "Regular",
    "tenderList.col.tender": "Тендер",
    "tenderList.col.customerBin": "БИН заказчика",
    "tenderList.col.amount": "Сумма",
    "tenderList.col.lotsHighTotal": "Лоты HIGH/всего",
    "tenderList.col.category": "Категория",
    "tenderList.col.risk": "Риск",
    "tenderList.loading": "Загрузка...",
    "tenderList.notFound": "Тендеры не найдены",
    "tenderList.loadMore": "Загрузить ещё ({n})",

    // shared/ui
    "ui.updatedDaily": "Updated daily",
  },

  // ─────────────────────────────── KAZAKH ───────────────────────────────
  kk: {
    // RiskListView
    "riskList.title": "Тәуекелдер тізімі",
    "riskList.hint": "Сұрыпталатын кесте · Сұрыптау үшін тақырыпты басыңыз",
    "riskList.itemsPage": "{n} жазба · бет {page}/{total}",
    "riskList.loading": "Жүктелуде...",
    "riskList.empty": "Нәтиже жоқ. Min risk мәнін азайтыңыз немесе сүзгіні өзгертіңіз.",
    "riskList.col.lot": "Лот",
    "riskList.col.customer": "Тапсырыс беруші",
    "riskList.col.amount": "Сома",
    "riskList.col.risk": "Тәуекел",
    "riskList.col.action": "Әрекет",
    "riskList.noName": "Атауы жоқ",
    "riskList.open": "Ашу",
    "riskList.loadMoreServer": "Серверден тағы жүктеу",

    // TenderListView
    "tenderList.title": "Тендерлер",
    "tenderList.inBase": "Базада {n} тендер",
    "tenderList.searchPh": "Нөмір / БСН бойынша іздеу...",
    "tenderList.category.all": "Барлығы",
    "tenderList.category.main_high": "MAIN HIGH",
    "tenderList.category.single_case": "Single Case",
    "tenderList.category.regular": "Regular",
    "tenderList.col.tender": "Тендер",
    "tenderList.col.customerBin": "Тапсырыс беруші БСН",
    "tenderList.col.amount": "Сома",
    "tenderList.col.lotsHighTotal": "HIGH/барлық лоттар",
    "tenderList.col.category": "Санат",
    "tenderList.col.risk": "Тәуекел",
    "tenderList.loading": "Жүктелуде...",
    "tenderList.notFound": "Тендерлер табылмады",
    "tenderList.loadMore": "Тағы жүктеу ({n})",

    // shared/ui
    "ui.updatedDaily": "Күн сайын жаңарады",
  },
};
