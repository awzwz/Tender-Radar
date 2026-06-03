import type { LangDict } from "../types";

// Keys for CompanyProfileView (namespace prefix "company.").
export const companyDict: LangDict = {
  // ─────────────────────────────── ENGLISH ───────────────────────────────
  en: {
    // Common
    "company.bin": "BIN",
    "company.common.noData": "no data",
    "company.common.collapse": "Collapse",
    "company.common.untitled": "Untitled",

    // Header
    "company.header.title": "Company Intel",
    "company.header.subtitle": "Dossier by BIN or name — Goszakup registry data in real time",

    // Search
    "company.search.placeholder": "Enter BIN (12 digits) or company name...",
    "company.search.submit": "Search",

    // Badges / labels
    "company.badge.supplier": "Supplier",
    "company.badge.customer": "Customer",
    "company.badge.unreliableSupplier": "Unreliable supplier",
    "company.label.reg": "Reg",
    "company.export.pdf": "PDF / print",

    // Tabs / sections
    "company.tab.overview": "Overview",
    "company.section.asSupplier": "As supplier",
    "company.section.asCustomer": "As customer",

    // Empty state
    "company.empty.title": "Enter a BIN or company name",
    "company.empty.subtitle": "The system will load up-to-date data straight from the Goszakup registry",

    // Loading state
    "company.loading.title": "Loading data from OWS...",
    "company.loading.sources": "Contracts · Tenders · Registry · Complaints · KGD · Courts · Affiliations",
    "company.loading.hint": "For large companies this may take 10–30 seconds",

    // Errors
    "company.err.load": "Loading error",
    "company.err.llm": "LLM error",
    "company.err.notFound": "Company \"{q}\" not found in the OWS registry. Check the name or enter a BIN.",
    "company.err.noBin": "Could not determine the company BIN. Try selecting from the list.",
    "company.err.loadProfile": "Profile loading error",
    "company.err.search": "Search error",

    // Risk flags
    "company.flag.BLACKLISTED": "In the registry of unreliable suppliers",
    "company.flag.LOW_EXECUTION_RATE": "Low contract execution rate (<50%)",
    "company.flag.OVERDUE_ACTS": "Overdue acts of completed work",
    "company.flag.FINES_PRESENT": "Fines charged",
    "company.flag.HIGH_CUSTOMER_CONCENTRATION": "High dependence on a single customer (>80%)",
    "company.flag.HIGH_SINGLE_SOURCE_RATE": "High share of single-source purchases (>50%)",
    "company.flag.MANY_CANCELLED_TENDERS": "Many cancelled tenders",
    "company.flag.HIGH_SUPPLIER_CONCENTRATION": "High dependence on a single supplier (>70%)",
    "company.flag.AVG_OVERDUE_DAYS": "High average act delay (>30 days)",
    "company.flag.VOLUME_SPIKE": "Sharp growth in contract volume (>3x per year)",
    "company.flag.YOUNG_COMPANY_BIG_VOLUME": "Young company with large contracts",
    "company.flag.DIVERSE_OKED": "Operates in >5 different OKED industries",
    "company.flag.HIGH_ADDENDUM_RATE": "High share of contract addenda (>30%)",
    "company.flag.COMPLAINTS_ON_PURCHASES": "Multiple complaints on purchases",
    "company.flag.HIGH_COMPLAINT_SATISFACTION_RATE": "High % of satisfied complaints",
    "company.flag.SATISFIED_COMPLAINTS_RISK": "Satisfied complaints (confirmed violations)",
    "company.flag.SHARED_BANK_ACCOUNT": "Shared bank account with other companies",
    "company.flag.SHARED_CONTACTS": "Shared contacts with other companies",
    "company.flag.FREQUENT_COBIDDERS": "Frequent joint participation in tenders",
    "company.flag.TAX_ANOMALY": "Tax anomalies (LLM analysis)",
    "company.flag.COURT_CASES_RISK": "Court cases — risk based on LLM analysis",
    "company.flag.MANY_COURT_CASES": "Many court cases as defendant",

    // Metrics
    "company.metric.totalContracts": "Total contracts",
    "company.metric.totalSum": "Total amount",
    "company.metric.execution": "Execution",
    "company.metric.actual": "actual",
    "company.metric.uniqueCustomers": "Unique customers",
    "company.metric.uniqueSuppliers": "Unique suppliers",
    "company.metric.fines": "Fines",
    "company.metric.overdue": "Overdue",
    "company.metric.treasuryPaid": "Paid by treasury",
    "company.metric.avgContractSize": "Avg contract amount",
    "company.metric.totalTenders": "Total tenders",
    "company.metric.procurementVolume": "Procurement volume",
    "company.metric.singleSource": "Single source",
    "company.metric.cancelledTenders": "Cancelled tenders",

    // KPI cards
    "company.kpi.contractsSupplier": "Contracts (supplier)",
    "company.kpi.tendersCustomer": "Tenders (customer)",
    "company.kpi.contractsLabel": "contracts",
    "company.kpi.rnuStatus": "RNU status",

    // RNU
    "company.rnu.listed": "Listed",
    "company.rnu.clean": "Clean",
    "company.rnu.notListed": "Not in the unreliable registry",
    "company.rnu.activeRecords": "{n} active records",
    "company.rnu.registryTitle": "Registry of unreliable suppliers",
    "company.rnu.indefinite": "indefinite",

    // Charts
    "company.chart.byYear": "Yearly dynamics",
    "company.chart.contractsAbbr": "{n} contr.",
    "company.chart.supplierByYear": "As supplier — amounts by year",
    "company.chart.customerByYear": "As customer — amounts by year",

    // Partners
    "company.partner.contractsAbbr": "{n} contr.",
    "company.partner.topCustomers": "Top customers",
    "company.partner.topSuppliers": "Top suppliers",
    "company.partner.top5Customers": "Top 5 customers by amount",
    "company.partner.top5Suppliers": "Top 5 suppliers by amount",

    // Contracts table
    "company.contracts.col.number": "Number",
    "company.contracts.col.customerBin": "Customer (BIN)",
    "company.contracts.col.supplierBin": "Supplier (BIN)",
    "company.contracts.col.sum": "Amount",
    "company.contracts.col.date": "Date",
    "company.contracts.col.status": "Status",
    "company.contracts.showAll": "Show all {n} contracts",
    "company.contracts.recent": "Recent contracts",

    // Complaints
    "company.complaints.title": "Complaints",
    "company.complaints.total": "Total complaints",
    "company.complaints.subTarget": "complaint target",
    "company.complaints.subOnPurchases": "on purchases",
    "company.complaints.satisfied": "Satisfied",
    "company.complaints.satisfiedOf": "{a} of {b}",
    "company.complaints.col.number": "Complaint No.",
    "company.complaints.col.dateSubmitted": "Date submitted",
    "company.complaints.col.status": "Status",
    "company.complaints.col.tender": "Tender",
    "company.complaints.col.organizer": "Organizer",
    "company.complaints.showAll": "Show all {n} complaints",
    "company.complaints.llmTitle": "LLM analysis of satisfied complaints",
    "company.complaints.itemLabel": "Complaint #{n}",

    // Tax
    "company.tax.title": "Tax payments",
    "company.tax.totalLabel": "Total tax payments",
    "company.tax.col.year": "Year",
    "company.tax.col.amount": "Amount",
    "company.tax.col.change": "Change",
    "company.tax.aiTitle": "AI analysis of tax data",

    // Court cases
    "company.court.title": "Court cases",
    "company.court.total": "Total cases",
    "company.court.asPlaintiff": "As plaintiff",
    "company.court.asDefendant": "As defendant",
    "company.court.reliabilityImpact": "Reliability impact",
    "company.court.llmScore": "LLM score",
    "company.court.llmTitle": "LLM analysis of rulings",
    "company.court.caseLabel": "Case #{n}",
    "company.court.amount": "Amount",

    // Affiliations
    "company.affil.title": "Affiliated companies",
    "company.affil.linksCount": "{n} links",
    "company.affil.sharedBankAccounts": "Shared bank accounts",
    "company.affil.sharedContacts": "Shared contacts",
    "company.affil.cobidders": "Co-bidding partners",
    "company.affil.timesTogether": "{n}x together",

    // AI analysis
    "company.ai.title": "AI Analysis",
    "company.ai.companyTitle": "AI Company Analysis",
    "company.ai.companySubtitle": "Expert narrative based on OWS metrics",
    "company.ai.analyzing": "Analyzing...",
    "company.ai.refresh": "Refresh",
    "company.ai.run": "Run analysis",
    "company.ai.notRun": "AI analysis not run yet",
    "company.ai.notRunHint": "Click \"Run analysis\" — GPT will study the metrics and produce an expert conclusion",
    "company.ai.sending": "Sending metrics to GPT and waiting for a response...",
    "company.ai.assistant": "AI Assistant",
    "company.ai.disclaimer": "Based on OWS data · For reference only, not a legal opinion",

    // Chat
    "company.chat.subtitle": "Knows this company's context · Ask anything",
    "company.chat.emptyTitle": "Ask something about the company",
    "company.chat.emptyExample": "For example: \"Are there signs of affiliation?\"",
    "company.chat.placeholder": "Ask a question about this company...",
    "company.chat.err.server": "Server error {status}",
    "company.chat.err.empty": "Received an empty response",
    "company.chat.err.connection": "Connection error",

    // Report (export)
    "company.report.title": "Company dossier",
    "company.report.titleShort": "Dossier",
    "company.report.analysisDate": "Analysis date",
    "company.report.rnuStatus": "RNU status",
    "company.report.rnuListedCaps": "ON THE UNRELIABLE REGISTRY",
    "company.report.rnuClean": "Clean",
    "company.report.rnuListedInline": "On the registry of unreliable suppliers",
    "company.report.rnuNotListed": "Not on the RNU registry",
    "company.report.riskLevel": "Risk level",
    "company.report.risk": "Risk",
    "company.report.riskFlags": "Risk flags",
    "company.report.noRiskFlags": "No risk flags",
    "company.report.overdueActs": "Overdue acts",
    "company.report.avgContract": "Average contract",
    "company.report.singleSourceShare": "Single-source share",
    "company.report.tendersCount": "{n} tenders",
    "company.report.col.indicator": "Indicator",
    "company.report.col.value": "Value",
    "company.report.source": "Source",
    "company.report.disclaimer": "For reference only, not a legal opinion",

    // Footer
    "company.footer.loadedAt": "Data loaded {date}",
  },

  // ─────────────────────────────── RUSSIAN ───────────────────────────────
  ru: {
    // Common
    "company.bin": "БИН",
    "company.common.noData": "нет данных",
    "company.common.collapse": "Свернуть",
    "company.common.untitled": "Без названия",

    // Header
    "company.header.title": "Company Intel",
    "company.header.subtitle": "Досье по БИН или названию — данные из реестра Goszakup в реальном времени",

    // Search
    "company.search.placeholder": "Введите БИН (12 цифр) или название компании...",
    "company.search.submit": "Найти",

    // Badges / labels
    "company.badge.supplier": "Поставщик",
    "company.badge.customer": "Заказчик",
    "company.badge.unreliableSupplier": "Недобросовестный поставщик",
    "company.label.reg": "Рег",
    "company.export.pdf": "PDF / печать",

    // Tabs / sections
    "company.tab.overview": "Обзор",
    "company.section.asSupplier": "Как поставщик",
    "company.section.asCustomer": "Как заказчик",

    // Empty state
    "company.empty.title": "Введите БИН или название компании",
    "company.empty.subtitle": "Система загрузит актуальные данные прямо из реестра Goszakup",

    // Loading state
    "company.loading.title": "Загружаем данные из OWS...",
    "company.loading.sources": "Контракты · Тендеры · Реестр · Жалобы · KGD · Суды · Аффилированность",
    "company.loading.hint": "Для крупных компаний это может занять 10–30 секунд",

    // Errors
    "company.err.load": "Ошибка загрузки",
    "company.err.llm": "Ошибка LLM",
    "company.err.notFound": "Компания \"{q}\" не найдена в реестре OWS. Проверьте название или введите БИН.",
    "company.err.noBin": "Не удалось определить БИН компании. Попробуйте выбрать из списка.",
    "company.err.loadProfile": "Ошибка загрузки профиля",
    "company.err.search": "Ошибка поиска",

    // Risk flags
    "company.flag.BLACKLISTED": "В реестре недобросовестных поставщиков",
    "company.flag.LOW_EXECUTION_RATE": "Низкий процент исполнения контрактов (<50%)",
    "company.flag.OVERDUE_ACTS": "Просроченные акты выполненных работ",
    "company.flag.FINES_PRESENT": "Начислены штрафы",
    "company.flag.HIGH_CUSTOMER_CONCENTRATION": "Высокая зависимость от одного заказчика (>80%)",
    "company.flag.HIGH_SINGLE_SOURCE_RATE": "Высокая доля единственных источников (>50%)",
    "company.flag.MANY_CANCELLED_TENDERS": "Много отменённых тендеров",
    "company.flag.HIGH_SUPPLIER_CONCENTRATION": "Высокая зависимость от одного поставщика (>70%)",
    "company.flag.AVG_OVERDUE_DAYS": "Высокая средняя просрочка актов (>30 дней)",
    "company.flag.VOLUME_SPIKE": "Резкий рост объёмов контрактов (>3x за год)",
    "company.flag.YOUNG_COMPANY_BIG_VOLUME": "Молодая компания с крупными контрактами",
    "company.flag.DIVERSE_OKED": "Работает в >5 разных отраслях ОКЭД",
    "company.flag.HIGH_ADDENDUM_RATE": "Высокая доля дополнений к контрактам (>30%)",
    "company.flag.COMPLAINTS_ON_PURCHASES": "Множественные жалобы на закупки",
    "company.flag.HIGH_COMPLAINT_SATISFACTION_RATE": "Высокий % удовлетворённых жалоб",
    "company.flag.SATISFIED_COMPLAINTS_RISK": "Удовлетворённые жалобы (подтверждённые нарушения)",
    "company.flag.SHARED_BANK_ACCOUNT": "Общий банковский счёт с другими компаниями",
    "company.flag.SHARED_CONTACTS": "Общие контакты с другими компаниями",
    "company.flag.FREQUENT_COBIDDERS": "Частые совместные участия в тендерах",
    "company.flag.TAX_ANOMALY": "Налоговые аномалии (LLM-анализ)",
    "company.flag.COURT_CASES_RISK": "Судебные дела — риск по результатам LLM-анализа",
    "company.flag.MANY_COURT_CASES": "Множество судебных дел как ответчик",

    // Metrics
    "company.metric.totalContracts": "Всего контрактов",
    "company.metric.totalSum": "Общая сумма",
    "company.metric.execution": "Исполнение",
    "company.metric.actual": "факт",
    "company.metric.uniqueCustomers": "Уникальных заказчиков",
    "company.metric.uniqueSuppliers": "Уникальных поставщиков",
    "company.metric.fines": "Штрафы",
    "company.metric.overdue": "Просрочки",
    "company.metric.treasuryPaid": "Выплачено казначейством",
    "company.metric.avgContractSize": "Ср. сумма контракта",
    "company.metric.totalTenders": "Всего тендеров",
    "company.metric.procurementVolume": "Объём закупок",
    "company.metric.singleSource": "Единственный источник",
    "company.metric.cancelledTenders": "Отменённые тендеры",

    // KPI cards
    "company.kpi.contractsSupplier": "Контракты (поставщик)",
    "company.kpi.tendersCustomer": "Тендеры (заказчик)",
    "company.kpi.contractsLabel": "контракты",
    "company.kpi.rnuStatus": "Статус РНУ",

    // RNU
    "company.rnu.listed": "В реестре",
    "company.rnu.clean": "Чистый",
    "company.rnu.notListed": "Не в реестре недобросовестных",
    "company.rnu.activeRecords": "{n} активных записей",
    "company.rnu.registryTitle": "Реестр недобросовестных поставщиков",
    "company.rnu.indefinite": "бессрочно",

    // Charts
    "company.chart.byYear": "Динамика по годам",
    "company.chart.contractsAbbr": "{n} контр.",
    "company.chart.supplierByYear": "Как поставщик — суммы по годам",
    "company.chart.customerByYear": "Как заказчик — суммы по годам",

    // Partners
    "company.partner.contractsAbbr": "{n} конт.",
    "company.partner.topCustomers": "Топ заказчики",
    "company.partner.topSuppliers": "Топ поставщики",
    "company.partner.top5Customers": "Топ-5 заказчиков по сумме",
    "company.partner.top5Suppliers": "Топ-5 поставщиков по сумме",

    // Contracts table
    "company.contracts.col.number": "Номер",
    "company.contracts.col.customerBin": "Заказчик (БИН)",
    "company.contracts.col.supplierBin": "Поставщик (БИН)",
    "company.contracts.col.sum": "Сумма",
    "company.contracts.col.date": "Дата",
    "company.contracts.col.status": "Статус",
    "company.contracts.showAll": "Показать все {n} контрактов",
    "company.contracts.recent": "Последние контракты",

    // Complaints
    "company.complaints.title": "Жалобы",
    "company.complaints.total": "Всего жалоб",
    "company.complaints.subTarget": "объект жалобы",
    "company.complaints.subOnPurchases": "на закупки",
    "company.complaints.satisfied": "Удовлетворено",
    "company.complaints.satisfiedOf": "{a} из {b}",
    "company.complaints.col.number": "№ жалобы",
    "company.complaints.col.dateSubmitted": "Дата подачи",
    "company.complaints.col.status": "Статус",
    "company.complaints.col.tender": "Тендер",
    "company.complaints.col.organizer": "Организатор",
    "company.complaints.showAll": "Показать все {n} жалоб",
    "company.complaints.llmTitle": "LLM-анализ удовлетворённых жалоб",
    "company.complaints.itemLabel": "Жалоба #{n}",

    // Tax
    "company.tax.title": "Налоговые отчисления",
    "company.tax.totalLabel": "Общая сумма налоговых отчислений",
    "company.tax.col.year": "Год",
    "company.tax.col.amount": "Сумма",
    "company.tax.col.change": "Изменение",
    "company.tax.aiTitle": "AI-анализ налоговых данных",

    // Court cases
    "company.court.title": "Судебные дела",
    "company.court.total": "Всего дел",
    "company.court.asPlaintiff": "Как истец",
    "company.court.asDefendant": "Как ответчик",
    "company.court.reliabilityImpact": "Влияние на надёжность",
    "company.court.llmScore": "LLM-оценка",
    "company.court.llmTitle": "LLM-анализ решений",
    "company.court.caseLabel": "Дело #{n}",
    "company.court.amount": "Сумма",

    // Affiliations
    "company.affil.title": "Связанные компании",
    "company.affil.linksCount": "{n} связей",
    "company.affil.sharedBankAccounts": "Общие банковские счета",
    "company.affil.sharedContacts": "Общие контакты",
    "company.affil.cobidders": "Co-bidding партнёры",
    "company.affil.timesTogether": "{n}x вместе",

    // AI analysis
    "company.ai.title": "AI Анализ",
    "company.ai.companyTitle": "AI Анализ компании",
    "company.ai.companySubtitle": "Экспертный нарратив на основе метрик OWS",
    "company.ai.analyzing": "Анализирую...",
    "company.ai.refresh": "Обновить",
    "company.ai.run": "Запустить анализ",
    "company.ai.notRun": "AI-анализ ещё не запущен",
    "company.ai.notRunHint": "Нажми «Запустить анализ» — GPT изучит метрики и выдаст экспертный вывод",
    "company.ai.sending": "Отправляю метрики в GPT и жду ответа...",
    "company.ai.assistant": "AI Ассистент",
    "company.ai.disclaimer": "На основе данных OWS · Только для справки, не является юридическим заключением",

    // Chat
    "company.chat.subtitle": "Знает контекст этой компании · Задавай любые вопросы",
    "company.chat.emptyTitle": "Спроси что-нибудь о компании",
    "company.chat.emptyExample": "Например: «Есть ли признаки аффилированности?»",
    "company.chat.placeholder": "Задай вопрос об этой компании...",
    "company.chat.err.server": "Ошибка сервера {status}",
    "company.chat.err.empty": "Получен пустой ответ",
    "company.chat.err.connection": "Ошибка соединения",

    // Report (export)
    "company.report.title": "Досье компании",
    "company.report.titleShort": "Досье",
    "company.report.analysisDate": "Дата анализа",
    "company.report.rnuStatus": "Статус РНУ",
    "company.report.rnuListedCaps": "ВНЕСЁН В РЕЕСТР НЕДОБРОСОВЕСТНЫХ",
    "company.report.rnuClean": "Чистый",
    "company.report.rnuListedInline": "В реестре недобросовестных поставщиков",
    "company.report.rnuNotListed": "Не в реестре РНУ",
    "company.report.riskLevel": "Риск-уровень",
    "company.report.risk": "Риск",
    "company.report.riskFlags": "Флаги риска",
    "company.report.noRiskFlags": "Нет флагов риска",
    "company.report.overdueActs": "Просрочки актов",
    "company.report.avgContract": "Средний контракт",
    "company.report.singleSourceShare": "Доля единственного источника",
    "company.report.tendersCount": "{n} тендеров",
    "company.report.col.indicator": "Показатель",
    "company.report.col.value": "Значение",
    "company.report.source": "Источник",
    "company.report.disclaimer": "Только для справки, не является юридическим заключением",

    // Footer
    "company.footer.loadedAt": "Данные загружены {date}",
  },

  // ─────────────────────────────── KAZAKH ───────────────────────────────
  kk: {
    // Common
    "company.bin": "БСН",
    "company.common.noData": "дерек жоқ",
    "company.common.collapse": "Жию",
    "company.common.untitled": "Атауы жоқ",

    // Header
    "company.header.title": "Company Intel",
    "company.header.subtitle": "БСН немесе атау бойынша дерек — Goszakup тізілімінен нақты уақытта",

    // Search
    "company.search.placeholder": "БСН (12 сан) немесе компания атауын енгізіңіз...",
    "company.search.submit": "Іздеу",

    // Badges / labels
    "company.badge.supplier": "Жеткізуші",
    "company.badge.customer": "Тапсырыс беруші",
    "company.badge.unreliableSupplier": "Жосықсыз жеткізуші",
    "company.label.reg": "Тіркеу",
    "company.export.pdf": "PDF / басып шығару",

    // Tabs / sections
    "company.tab.overview": "Шолу",
    "company.section.asSupplier": "Жеткізуші ретінде",
    "company.section.asCustomer": "Тапсырыс беруші ретінде",

    // Empty state
    "company.empty.title": "БСН немесе компания атауын енгізіңіз",
    "company.empty.subtitle": "Жүйе өзекті деректерді тікелей Goszakup тізілімінен жүктейді",

    // Loading state
    "company.loading.title": "OWS-тен деректер жүктелуде...",
    "company.loading.sources": "Келісімшарттар · Тендерлер · Тізілім · Шағымдар · KGD · Сот · Аффилиация",
    "company.loading.hint": "Ірі компаниялар үшін бұл 10–30 секунд алуы мүмкін",

    // Errors
    "company.err.load": "Жүктеу қатесі",
    "company.err.llm": "LLM қатесі",
    "company.err.notFound": "\"{q}\" компаниясы OWS тізілімінен табылмады. Атауын тексеріңіз немесе БСН енгізіңіз.",
    "company.err.noBin": "Компанияның БСН анықталмады. Тізімнен таңдап көріңіз.",
    "company.err.loadProfile": "Профильді жүктеу қатесі",
    "company.err.search": "Іздеу қатесі",

    // Risk flags
    "company.flag.BLACKLISTED": "Жосықсыз жеткізушілер тізілімінде",
    "company.flag.LOW_EXECUTION_RATE": "Келісімшарттарды орындаудың төмен пайызы (<50%)",
    "company.flag.OVERDUE_ACTS": "Орындалған жұмыстардың мерзімі өткен актілері",
    "company.flag.FINES_PRESENT": "Айыппұлдар есептелген",
    "company.flag.HIGH_CUSTOMER_CONCENTRATION": "Бір тапсырыс берушіге жоғары тәуелділік (>80%)",
    "company.flag.HIGH_SINGLE_SOURCE_RATE": "Жалғыз көздердің жоғары үлесі (>50%)",
    "company.flag.MANY_CANCELLED_TENDERS": "Көп болдырмаған тендерлер",
    "company.flag.HIGH_SUPPLIER_CONCENTRATION": "Бір жеткізушіге жоғары тәуелділік (>70%)",
    "company.flag.AVG_OVERDUE_DAYS": "Актілердің орташа мерзімінен өтуі жоғары (>30 күн)",
    "company.flag.VOLUME_SPIKE": "Келісімшарт көлемінің күрт өсуі (жылына >3x)",
    "company.flag.YOUNG_COMPANY_BIG_VOLUME": "Ірі келісімшарттары бар жас компания",
    "company.flag.DIVERSE_OKED": ">5 түрлі ЭҚЖЖ салаларында жұмыс істейді",
    "company.flag.HIGH_ADDENDUM_RATE": "Келісімшарт толықтыруларының жоғары үлесі (>30%)",
    "company.flag.COMPLAINTS_ON_PURCHASES": "Сатып алуларға бірнеше шағым",
    "company.flag.HIGH_COMPLAINT_SATISFACTION_RATE": "Қанағаттандырылған шағымдардың жоғары %",
    "company.flag.SATISFIED_COMPLAINTS_RISK": "Қанағаттандырылған шағымдар (расталған бұзушылықтар)",
    "company.flag.SHARED_BANK_ACCOUNT": "Басқа компаниялармен ортақ банк шоты",
    "company.flag.SHARED_CONTACTS": "Басқа компаниялармен ортақ байланыстар",
    "company.flag.FREQUENT_COBIDDERS": "Тендерлерге жиі бірлескен қатысу",
    "company.flag.TAX_ANOMALY": "Салық ауытқулары (LLM-талдау)",
    "company.flag.COURT_CASES_RISK": "Сот істері — LLM-талдау нәтижесі бойынша тәуекел",
    "company.flag.MANY_COURT_CASES": "Жауапкер ретінде көптеген сот істері",

    // Metrics
    "company.metric.totalContracts": "Барлық келісімшарттар",
    "company.metric.totalSum": "Жалпы сома",
    "company.metric.execution": "Орындау",
    "company.metric.actual": "нақты",
    "company.metric.uniqueCustomers": "Бірегей тапсырыс берушілер",
    "company.metric.uniqueSuppliers": "Бірегей жеткізушілер",
    "company.metric.fines": "Айыппұлдар",
    "company.metric.overdue": "Мерзімі өткендер",
    "company.metric.treasuryPaid": "Қазынашылық төледі",
    "company.metric.avgContractSize": "Орт. келісімшарт сомасы",
    "company.metric.totalTenders": "Барлық тендерлер",
    "company.metric.procurementVolume": "Сатып алу көлемі",
    "company.metric.singleSource": "Жалғыз көз",
    "company.metric.cancelledTenders": "Болдырмаған тендерлер",

    // KPI cards
    "company.kpi.contractsSupplier": "Келісімшарттар (жеткізуші)",
    "company.kpi.tendersCustomer": "Тендерлер (тапсырыс беруші)",
    "company.kpi.contractsLabel": "келісімшарттар",
    "company.kpi.rnuStatus": "ЖЖТ мәртебесі",

    // RNU
    "company.rnu.listed": "Тізілімде",
    "company.rnu.clean": "Таза",
    "company.rnu.notListed": "Жосықсыздар тізілімінде емес",
    "company.rnu.activeRecords": "{n} белсенді жазба",
    "company.rnu.registryTitle": "Жосықсыз жеткізушілер тізілімі",
    "company.rnu.indefinite": "мерзімсіз",

    // Charts
    "company.chart.byYear": "Жылдар бойынша динамика",
    "company.chart.contractsAbbr": "{n} келіс.",
    "company.chart.supplierByYear": "Жеткізуші ретінде — жылдар бойынша сомалар",
    "company.chart.customerByYear": "Тапсырыс беруші ретінде — жылдар бойынша сомалар",

    // Partners
    "company.partner.contractsAbbr": "{n} келіс.",
    "company.partner.topCustomers": "Үздік тапсырыс берушілер",
    "company.partner.topSuppliers": "Үздік жеткізушілер",
    "company.partner.top5Customers": "Сома бойынша үздік 5 тапсырыс беруші",
    "company.partner.top5Suppliers": "Сома бойынша үздік 5 жеткізуші",

    // Contracts table
    "company.contracts.col.number": "Нөмір",
    "company.contracts.col.customerBin": "Тапсырыс беруші (БСН)",
    "company.contracts.col.supplierBin": "Жеткізуші (БСН)",
    "company.contracts.col.sum": "Сома",
    "company.contracts.col.date": "Күні",
    "company.contracts.col.status": "Мәртебе",
    "company.contracts.showAll": "Барлық {n} келісімшартты көрсету",
    "company.contracts.recent": "Соңғы келісімшарттар",

    // Complaints
    "company.complaints.title": "Шағымдар",
    "company.complaints.total": "Барлық шағымдар",
    "company.complaints.subTarget": "шағым нысаны",
    "company.complaints.subOnPurchases": "сатып алуларға",
    "company.complaints.satisfied": "Қанағаттандырылды",
    "company.complaints.satisfiedOf": "{b} ішінен {a}",
    "company.complaints.col.number": "Шағым №",
    "company.complaints.col.dateSubmitted": "Берілген күні",
    "company.complaints.col.status": "Мәртебе",
    "company.complaints.col.tender": "Тендер",
    "company.complaints.col.organizer": "Ұйымдастырушы",
    "company.complaints.showAll": "Барлық {n} шағымды көрсету",
    "company.complaints.llmTitle": "Қанағаттандырылған шағымдардың LLM-талдауы",
    "company.complaints.itemLabel": "Шағым #{n}",

    // Tax
    "company.tax.title": "Салық аударымдары",
    "company.tax.totalLabel": "Салық аударымдарының жалпы сомасы",
    "company.tax.col.year": "Жыл",
    "company.tax.col.amount": "Сома",
    "company.tax.col.change": "Өзгеріс",
    "company.tax.aiTitle": "Салық деректерінің AI-талдауы",

    // Court cases
    "company.court.title": "Сот істері",
    "company.court.total": "Барлық істер",
    "company.court.asPlaintiff": "Талапкер ретінде",
    "company.court.asDefendant": "Жауапкер ретінде",
    "company.court.reliabilityImpact": "Сенімділікке әсері",
    "company.court.llmScore": "LLM-бағасы",
    "company.court.llmTitle": "Шешімдердің LLM-талдауы",
    "company.court.caseLabel": "Іс #{n}",
    "company.court.amount": "Сома",

    // Affiliations
    "company.affil.title": "Байланысты компаниялар",
    "company.affil.linksCount": "{n} байланыс",
    "company.affil.sharedBankAccounts": "Ортақ банк шоттары",
    "company.affil.sharedContacts": "Ортақ байланыстар",
    "company.affil.cobidders": "Co-bidding серіктестер",
    "company.affil.timesTogether": "{n}x бірге",

    // AI analysis
    "company.ai.title": "AI Талдау",
    "company.ai.companyTitle": "Компанияның AI талдауы",
    "company.ai.companySubtitle": "OWS метрикаларына негізделген сараптамалық баяндау",
    "company.ai.analyzing": "Талдап жатырмын...",
    "company.ai.refresh": "Жаңарту",
    "company.ai.run": "Талдауды бастау",
    "company.ai.notRun": "AI-талдау әлі басталған жоқ",
    "company.ai.notRunHint": "«Талдауды бастау» түймесін бас — GPT метрикаларды зерттеп, сараптамалық қорытынды береді",
    "company.ai.sending": "Метрикаларды GPT-ге жіберіп, жауап күтудемін...",
    "company.ai.assistant": "AI Көмекші",
    "company.ai.disclaimer": "OWS деректері негізінде · Тек анықтама үшін, заңды қорытынды емес",

    // Chat
    "company.chat.subtitle": "Осы компания контекстін біледі · Кез келген сұрақ қой",
    "company.chat.emptyTitle": "Компания туралы бірдеңе сұра",
    "company.chat.emptyExample": "Мысалы: «Аффилиация белгілері бар ма?»",
    "company.chat.placeholder": "Осы компания туралы сұрақ қой...",
    "company.chat.err.server": "Сервер қатесі {status}",
    "company.chat.err.empty": "Бос жауап алынды",
    "company.chat.err.connection": "Байланыс қатесі",

    // Report (export)
    "company.report.title": "Компания дерегі",
    "company.report.titleShort": "Дерек",
    "company.report.analysisDate": "Талдау күні",
    "company.report.rnuStatus": "ЖЖТ мәртебесі",
    "company.report.rnuListedCaps": "ЖОСЫҚСЫЗДАР ТІЗІЛІМІНЕ ЕНГІЗІЛГЕН",
    "company.report.rnuClean": "Таза",
    "company.report.rnuListedInline": "Жосықсыз жеткізушілер тізілімінде",
    "company.report.rnuNotListed": "ЖЖТ тізілімінде емес",
    "company.report.riskLevel": "Тәуекел деңгейі",
    "company.report.risk": "Тәуекел",
    "company.report.riskFlags": "Тәуекел жалаушалары",
    "company.report.noRiskFlags": "Тәуекел жалаушалары жоқ",
    "company.report.overdueActs": "Актілердің мерзімінен өтуі",
    "company.report.avgContract": "Орташа келісімшарт",
    "company.report.singleSourceShare": "Жалғыз көз үлесі",
    "company.report.tendersCount": "{n} тендер",
    "company.report.col.indicator": "Көрсеткіш",
    "company.report.col.value": "Мән",
    "company.report.source": "Дереккөз",
    "company.report.disclaimer": "Тек анықтама үшін, заңды қорытынды емес",

    // Footer
    "company.footer.loadedAt": "Деректер жүктелді {date}",
  },
};
