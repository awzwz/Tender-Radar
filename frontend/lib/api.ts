const getApiBase = (): string => {
    if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
    if (typeof window === "undefined") return "http://localhost:8000/api/v1";
    const host = window.location.hostname;
    return `http://${host}:8000/api/v1`;
};
const API_BASE = getApiBase();

function getToken(): string | null {
    if (typeof window === "undefined") return null;
    return localStorage.getItem("token");
}

async function apiFetch<T>(path: string, options: RequestInit = {}): Promise<T> {
    const token = getToken();
    const res = await fetch(`${API_BASE}${path}`, {
        ...options,
        headers: {
            "Content-Type": "application/json",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
            ...options.headers,
        },
    });
    if (!res.ok) {
        const error = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(error.detail || "API error");
    }
    return res.json();
}

export const api = {
    login: (username: string, password: string) => {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        return fetch(`${API_BASE}/auth/login/json`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, password }),
            signal: controller.signal,
        })
            .then(async (r) => {
                clearTimeout(timeout);
                const data = await r.json().catch(() => ({}));
                if (!r.ok) {
                    const msg = Array.isArray(data.detail) ? data.detail[0]?.msg : data.detail;
                    throw new Error(typeof msg === "string" ? msg : "Неверный логин или пароль");
                }
                return data;
            })
            .catch((e) => {
                clearTimeout(timeout);
                if (e.name === "AbortError") throw new Error("Сервер не отвечает. Запушен ли backend?");
                throw e;
            });
    },

    me: () => apiFetch<{ id: number; username: string; role: string }>("/auth/me"),

    dashboard: (params: Record<string, string | number>) => {
        const qs = new URLSearchParams(
            Object.fromEntries(Object.entries(params).map(([k, v]) => [k, String(v)]))
        ).toString();
        return apiFetch<{ total: number; page: number; limit: number; items: DashboardItem[] }>(
            `/dashboard?${qs}`
        );
    },

    dashboardStats: () =>
        apiFetch<{ total_lots: number; scored_lots: number; high: number; medium: number; low: number; avg_score: number }>(
            "/dashboard/stats"
        ),
    dashboardTenderStats: () =>
        apiFetch<{
            total_tenders: number;
            scored_tenders: number;
            high: number;
            medium: number;
            low: number;
            avg_score: number;
            main_high: number;
            single_case_alerts: number;
        }>("/dashboard/tenders/stats"),
    dashboardTenders: (params: Record<string, string | number>) => {
        const qs = new URLSearchParams(
            Object.fromEntries(Object.entries(params).map(([k, v]) => [k, String(v)]))
        ).toString();
        return apiFetch<{ total: number; page: number; limit: number; items: DashboardTenderItem[] }>(
            `/dashboard/tenders?${qs}`
        );
    },

    dashboardRegions: () =>
        apiFetch<{ regions: RegionStat[]; unmatched: number }>("/dashboard/regions"),

    lot: (id: number) => apiFetch<LotDetail>(`/lots/${id}`),

    lotIndicatorDetails: (lotId: number, code: string) =>
        apiFetch<IndicatorDetails>(`/lots/${lotId}/indicators/${code}/details`),
    tender: (id: number) => apiFetch<TenderDetail>(`/tenders/${id}`),
    supplier: (biin: string) => apiFetch<SupplierProfile>(`/suppliers/${biin}`),
    customer: (bin: string) => apiFetch<CustomerProfile>(`/customers/${bin}`),

    // AI Explanation (LLM-generated, PII-masked)
    explainLot: (id: number, force = false) =>
        apiFetch<ExplanationResponse>(`/explain/lots/${id}/explain?force=${force}`),

    explainTender: (id: number, force = false) =>
        apiFetch<ExplanationResponse>(`/explain/tenders/${id}/explain?force=${force}`),

    createNote: (body: { entity_type: string; entity_id: string; note_text: string; label?: string }) =>
        apiFetch("/notes", { method: "POST", body: JSON.stringify(body) }),

    getNotes: (entity_type: string, entity_id: string) =>
        apiFetch<Note[]>(`/notes?entity_type=${entity_type}&entity_id=${entity_id}`),

    triggerBackfill: (date_from?: string, date_to?: string) =>
        apiFetch("/admin/etl/backfill", { method: "POST", body: JSON.stringify({ date_from, date_to }) }),

    triggerMLTrain: () =>
        apiFetch("/admin/ml/train", { method: "POST" }),

    etlStatus: () => apiFetch<EtlRun[]>("/admin/etl/status"),

    // ── Company Profile (real-time OWS) ────────────────────────────────────
    companySearch: (q: string) =>
        apiFetch<{ query: string; total: number; results: CompanySearchResult[] }>(
            `/company/search?q=${encodeURIComponent(q)}`
        ),

    companyProfile: (bin: string) =>
        apiFetch<CompanyProfile>(`/company/${bin}`),

    companyAnalyze: (bin: string, profile: CompanyProfile) =>
        apiFetch<{ bin: string; narrative: string; model: string }>(
            `/company/${bin}/analyze`,
            { method: "POST", body: JSON.stringify({ profile }) }
        ),

    // ── Suppliers list ────────────────────────────────────────────────────────
    suppliersList: (params: { search?: string; blacklisted?: boolean; sort_by?: string; page?: number; limit?: number }) => {
        const p: Record<string, string> = {};
        if (params.search) p.search = params.search;
        if (params.blacklisted !== undefined) p.blacklisted = String(params.blacklisted);
        if (params.sort_by) p.sort_by = params.sort_by;
        if (params.page) p.page = String(params.page);
        if (params.limit) p.limit = String(params.limit);
        const qs = new URLSearchParams(p).toString();
        return apiFetch<{ total: number; page: number; limit: number; items: SupplierListItem[] }>(
            `/suppliers/${qs ? "?" + qs : ""}`
        );
    },

    // ── Customers list ────────────────────────────────────────────────────────
    customersList: (params: { search?: string; min_high_lots?: number; sort_by?: string; page?: number; limit?: number }) => {
        const p: Record<string, string> = {};
        if (params.search) p.search = params.search;
        if (params.min_high_lots !== undefined) p.min_high_lots = String(params.min_high_lots);
        if (params.sort_by) p.sort_by = params.sort_by;
        if (params.page) p.page = String(params.page);
        if (params.limit) p.limit = String(params.limit);
        const qs = new URLSearchParams(p).toString();
        return apiFetch<{ total: number; page: number; limit: number; items: CustomerListItem[] }>(
            `/customers/${qs ? "?" + qs : ""}`
        );
    },
};

// ── Types ─────────────────────────────────────────────────────────────────────

export type RiskLevel = "LOW" | "MEDIUM" | "HIGH" | "UNKNOWN";

export interface DashboardItem {
    lot_id: number;
    lot_name: string;
    amount: number;
    customer_bin: string;
    customer_name: string;
    trd_buy_id: number;
    tender_number: string;
    publish_date: string;
    risk_score: number;
    score_rules: number | null;
    score_ml: number | null;
    score_final: number | null;
    risk_level: RiskLevel;
    top_reasons: { code: string; description: string; weight: number }[];
}

export interface DashboardTenderItem {
    trd_buy_id: number;
    tender_number: string;
    tender_name: string;
    org_bin: string;
    publish_date: string | null;
    total_sum: number;
    risk_score: number;
    risk_level: RiskLevel;
    high_lots_count: number;
    lots_count: number;
    max_lot_score: number;
    avg_lot_score: number;
    category: "main_high" | "single_case" | "regular";
}

export interface RegionStat {
    id: string;
    name_ru: string;
    count: number;
    avg_risk: number;
    high: number;
    medium: number;
    low: number;
}

export interface LotDetail {
    lot: {
        id: number;
        name_ru: string;
        amount: number;
        customer_bin: string;
        customer_name: string;
        trd_buy_id: number;
        dumping_flag: boolean;
    };
    tender: {
        id: number;
        number_anno: string;
        name_ru: string;
        publish_date: string;
        start_date: string;
        end_date: string;
    } | null;
    contract: {
        id: number;
        supplier_biin: string;
        contract_sum_wnds: number;
        sign_date: string;
        plan_exec_date: string;
        parent_id: number | null;
    } | null;
    risk: {
        score: number;
        score_rules: number | null;
        score_ml: number | null;
        score_final: number | null;
        level: string;
        top_reasons: { code: string; description: string }[];
        computed_at: string;
    };
    flags: {
        code: string;
        triggered: boolean;
        value: number | null;
        evidence: Record<string, unknown>;
    }[];
}

export interface SpecAnalysis {
    risky: boolean;
    reasoning: string;
}

export interface ExplanationResponse {
    explanation: string | null;
    checklist?: string[];
    spec_analysis?: SpecAnalysis | null;
    risk_final?: number;
    risk_rules?: number;
    risk_ml?: number;
    model_used?: string;
    created_at?: string;
    cached?: boolean;
    eligible?: boolean;
    reason?: string;
    error?: string;
}

export interface TenderDetail {
    tender: {
        id: number;
        number_anno: string;
        name_ru: string;
        org_bin: string;
        total_sum: number;
        publish_date: string;
        ref_trade_methods_id: number;
    };
    lots: {
        lot_id: number;
        name_ru: string;
        amount: number;
        customer_bin: string;
        risk_score: number;
        risk_level: string;
        top_reasons: unknown[];
    }[];
}

export interface SupplierProfile {
    company: {
        biin: string;
        name_ru: string;
        regdate: string;
        type_supplier: number;
        mark_small_employer: number;
        mark_resident: number;
        email: string;
        phone: string;
    };
    stats: { total_contracts: number; total_sum: number; unique_customers: number };
    top_customers: { customer_bin: string; contract_count: number; total_sum: number }[];
    rnu: { is_active: boolean; reason?: string; start_date?: string };
}

export interface CustomerProfile {
    company: { bin: string; name_ru: string };
    stats: { total_contracts: number; total_sum: number; unique_suppliers: number };
    top_suppliers: { supplier_biin: string; contract_count: number; total_sum: number }[];
    high_risk_lots: { lot_id: number; name_ru: string; amount: number; score: number; level: string }[];
}

export interface Note {
    id: number;
    note_text: string;
    label: string;
    created_by: number;
    created_at: string;
}

export interface EtlRun {
    id: number;
    run_type: string;
    started_at: string;
    finished_at: string | null;
    status: string;
    summary: Record<string, unknown>;
}

export interface IndicatorDetails {
    code: string;
    customer_bin?: string;
    customer_name?: string;
    supplier_biin?: string;
    supplier_name?: string;
    rotation_count?: number;
    unique_winners?: number;
    contracts?: {
        contract_id: number;
        contract_number: string;
        sign_date: string | null;
        supplier_biin?: string;
        supplier_name?: string;
        contract_sum: number;
        tender_number?: string;
    }[];
    evidence?: Record<string, unknown>;
}

// ── Supplier / Customer list items ────────────────────────────────────────────

export interface SupplierListItem {
    biin: string;
    name_ru: string | null;
    total_contracts: number;
    total_sum: number;
    unique_customers: number;
    last_contract_date: string | null;
    is_blacklisted: boolean;
}

export interface CustomerListItem {
    bin: string;
    customer_name: string | null;
    total_lots: number;
    total_tenders: number;
    total_amount: number;
    avg_risk_score: number;
    high_risk_lots: number;
    medium_risk_lots: number;
}

// ── Company Profile (OWS real-time) ───────────────────────────────────────────

export interface CompanySearchResult {
    pid?: number;
    bin?: string;
    iin?: string;
    nameRu?: string;
    nameKz?: string;
    fullNameRu?: string;
    regdate?: string;
    email?: string;
    phone?: string;
    website?: string;
    customer?: boolean;
    supplier?: boolean;
    systemId?: number;
}

export interface CompanyYearStat {
    count: number;
    sum: number;
}

export interface CompanyPartnerStat {
    bin: string;
    count: number;
    sum: number;
}

export interface CompanySupplierMetrics {
    total_contracts: number;
    total_sum: number;
    executed_sum: number;
    execution_rate: number;
    unique_customers: number;
    top_customers: CompanyPartnerStat[];
    by_year: Record<string, CompanyYearStat>;
    overdue_count: number;
    fines_count: number;
    avg_contract_size: number;
    treasury_paid: number;
}

export interface CompanyCustomerMetrics {
    total_tenders: number;
    total_contracts: number;
    total_procurement_sum: number;
    unique_suppliers: number;
    top_suppliers: CompanyPartnerStat[];
    by_year: Record<string, CompanyYearStat>;
    single_source_count: number;
    single_source_rate: number;
    cancelled_tenders: number;
}

export interface CompanyRiskInfo {
    score: number;
    level: "LOW" | "MEDIUM" | "HIGH";
    flags: string[];
    flags_detail?: { code: string; points: number; detail: string }[];
}

export interface CompanyComplaintLLMAnalysis {
    complaint_number: string;
    violation_type: string;
    summary: string;
    severity: "low" | "medium" | "high";
    risk_impact: number;
}

export interface CompanyComplaintInfo {
    total: number;
    complaints_as_supplier: number;
    complaints_as_customer: number;
    satisfied_count: number;
    rejected_count?: number;
    satisfaction_rate: number;
    score_points?: number;
    llm_analyses?: CompanyComplaintLLMAnalysis[];
    complaints: Record<string, unknown>[];
}

export interface TaxPaymentEntry {
    year: number;
    amount: number;
    change_pct: number | null;
}

export interface TaxLLMAnalysis {
    tax_trend: string;
    tax_health: string;
    anomalies: string[];
    summary: string;
    risk_level: string;
    score_points: number;
}

export interface CompanyKGDInfo {
    bin: string;
    available: boolean;
    tax_payments: TaxPaymentEntry[];
    total_tax_paid: number;
    llm_analysis: TaxLLMAnalysis | null;
    score_points: number;
    source: string;
    tax_debt?: boolean;
    pseudo_enterprise?: boolean;
    is_unreliable_taxpayer?: boolean;
}

export interface CompanyCourtCase {
    case_number: string;
    date: string;
    court: string;
    case_type: string;
    parties: string;
    amount: number | null;
    outcome: string;
    role: string;
    detail_url: string;
    full_text: string;
}

export interface CompanyCourtLLMAnalysis {
    case_number: string;
    role: string;
    dispute_type: string;
    amount: number | null;
    outcome: string;
    reliability_impact: number;
    summary: string;
}

export interface CompanyCourtInfo {
    total: number;
    as_plaintiff: number;
    as_defendant: number;
    as_other: number;
    avg_reliability_impact: number;
    score_points: number;
    cases: CompanyCourtCase[];
    llm_analyses: CompanyCourtLLMAnalysis[];
}

export interface CompanyAffiliation {
    bin: string;
    name: string;
    match_type?: string;
    times_together?: number;
    sum?: number;
}

export interface CompanyAffiliationsInfo {
    total_links: number;
    shared_bank_accounts: CompanyAffiliation[];
    shared_contacts: CompanyAffiliation[];
    cobid_partners: CompanyAffiliation[];
}

export interface CompanyRnuStatus {
    is_blacklisted: boolean;
    active_count: number;
    records: Record<string, unknown>[];
}

export interface OWSContract {
    id: number;
    contractNumber?: string;
    trdBuyNumberAnno?: string;
    customerBin?: string;
    supplierBiin?: string;
    contractSumWnds?: number;
    faktSum?: number;
    signDate?: string;
    planExecDate?: string;
    faktExecDate?: string;
    refContractStatusId?: number;
}

export interface OWSTender {
    id: number;
    numberAnno?: string;
    nameRu?: string;
    totalSum?: number;
    countLots?: number;
    refBuyStatusId?: number;
    publishDate?: string;
    singlOrgSign?: boolean;
}

export interface CompanyProfile {
    bin: string;
    fetched_at: string;
    subject: CompanySearchResult | null;
    rnu: CompanyRnuStatus;
    risk: CompanyRiskInfo;
    as_supplier: {
        contracts: OWSContract[];
        metrics: CompanySupplierMetrics;
    };
    as_customer: {
        contracts: OWSContract[];
        tenders: OWSTender[];
        metrics: CompanyCustomerMetrics;
    };
    complaints?: CompanyComplaintInfo;
    kgd?: CompanyKGDInfo;
    court_cases?: CompanyCourtInfo;
    affiliations?: CompanyAffiliationsInfo;
}
