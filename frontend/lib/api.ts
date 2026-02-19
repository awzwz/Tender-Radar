const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

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
        const form = new URLSearchParams({ username, password });
        return fetch(`${API_BASE}/auth/login`, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: form,
        }).then((r) => r.json());
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

    lot: (id: number) => apiFetch<LotDetail>(`/lots/${id}`),
    tender: (id: number) => apiFetch<TenderDetail>(`/tenders/${id}`),
    supplier: (biin: string) => apiFetch<SupplierProfile>(`/suppliers/${biin}`),
    customer: (bin: string) => apiFetch<CustomerProfile>(`/customers/${bin}`),

    createNote: (body: { entity_type: string; entity_id: string; note_text: string; label?: string }) =>
        apiFetch("/notes", { method: "POST", body: JSON.stringify(body) }),

    getNotes: (entity_type: string, entity_id: string) =>
        apiFetch<Note[]>(`/notes?entity_type=${entity_type}&entity_id=${entity_id}`),

    triggerBackfill: (date_from?: string, date_to?: string) =>
        apiFetch("/admin/etl/backfill", { method: "POST", body: JSON.stringify({ date_from, date_to }) }),

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
    risk_level: RiskLevel;
    top_reasons: { code: string; description: string; weight: number }[];
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
