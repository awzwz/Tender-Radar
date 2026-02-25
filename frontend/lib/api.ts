// Только локальная работа: тот же hostname, что и у страницы (localhost или 127.0.0.1), порт 8000
const getApiBase = (): string => {
    if (typeof window === "undefined")
        return process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
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
        return fetch(`${API_BASE}/auth/login/json`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, password }),
        }).then(async (r) => {
            const data = await r.json();
            if (!r.ok) throw new Error(Array.isArray(data.detail) ? data.detail[0]?.msg : data.detail || "Ошибка входа");
            return data;
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
