"use client";

import React, { useState, useRef, useEffect, useCallback } from "react";
import {
    Search,
    Building2,
    ShieldAlert,
    ShieldCheck,
    TrendingUp,
    TrendingDown,
    Users,
    FileText,
    AlertTriangle,
    CheckCircle2,
    XCircle,
    Clock,
    Loader2,
    ChevronRight,
    BarChart3,
    Package,
    ArrowUpRight,
    Info,
    Sparkles,
    Bot,
    Download,
    MessageSquare,
    Send,
    ChevronDown,
    Scale,
    Landmark,
    Link2,
    Banknote,
    UserX,
    Megaphone,
} from "lucide-react";
import { api, CompanyProfile, CompanySearchResult } from "@/lib/api";
import { money } from "@/components/shared/ui";

// ── Helpers ────────────────────────────────────────────────────────────────────

function fmt(n: number) {
    if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + " млрд ₸";
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + " млн ₸";
    if (n >= 1_000) return (n / 1_000).toFixed(0) + " тыс ₸";
    return money(n);
}

function riskColor(level: string) {
    if (level === "HIGH") return "text-rose-600 dark:text-rose-400";
    if (level === "MEDIUM") return "text-amber-600 dark:text-amber-400";
    return "text-emerald-600 dark:text-emerald-400";
}

function riskBg(level: string) {
    if (level === "HIGH") return "bg-rose-500/10 border-rose-500/20";
    if (level === "MEDIUM") return "bg-amber-500/10 border-amber-500/20";
    return "bg-emerald-500/10 border-emerald-500/20";
}

function RiskBadge({ level, score }: { level: string; score: number }) {
    return (
        <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-semibold ${riskBg(level)} ${riskColor(level)}`}>
            <ShieldAlert className="h-3.5 w-3.5" />
            {level} · {score}
        </span>
    );
}

const FLAG_LABELS: Record<string, string> = {
    // Original checks
    BLACKLISTED: "В реестре недобросовестных поставщиков",
    LOW_EXECUTION_RATE: "Низкий процент исполнения контрактов (<50%)",
    OVERDUE_ACTS: "Просроченные акты выполненных работ",
    FINES_PRESENT: "Начислены штрафы",
    HIGH_CUSTOMER_CONCENTRATION: "Высокая зависимость от одного заказчика (>80%)",
    HIGH_SINGLE_SOURCE_RATE: "Высокая доля единственных источников (>50%)",
    MANY_CANCELLED_TENDERS: "Много отменённых тендеров",
    HIGH_SUPPLIER_CONCENTRATION: "Высокая зависимость от одного поставщика (>70%)",
    // OWS additional checks
    AVG_OVERDUE_DAYS: "Высокая средняя просрочка актов (>30 дней)",
    VOLUME_SPIKE: "Резкий рост объёмов контрактов (>3x за год)",
    YOUNG_COMPANY_BIG_VOLUME: "Молодая компания с крупными контрактами",
    DIVERSE_OKED: "Работает в >5 разных отраслях ОКЭД",
    HIGH_ADDENDUM_RATE: "Высокая доля дополнений к контрактам (>30%)",
    // Complaints
    COMPLAINTS_ON_PURCHASES: "Множественные жалобы на закупки",
    HIGH_COMPLAINT_SATISFACTION_RATE: "Высокий % удовлетворённых жалоб",
    SATISFIED_COMPLAINTS_RISK: "Удовлетворённые жалобы (подтверждённые нарушения)",
    // Affiliations
    SHARED_BANK_ACCOUNT: "Общий банковский счёт с другими компаниями",
    SHARED_CONTACTS: "Общие контакты с другими компаниями",
    FREQUENT_COBIDDERS: "Частые совместные участия в тендерах",
    // Tax (ba.prg.kz)
    TAX_ANOMALY: "Налоговые аномалии (LLM-анализ)",
    // Court cases
    COURT_CASES_RISK: "Судебные дела — риск по результатам LLM-анализа",
    MANY_COURT_CASES: "Множество судебных дел как ответчик",
};

// ── Sub-components ─────────────────────────────────────────────────────────────

function MetricCard({
    label,
    value,
    sub,
    tone = "neutral",
    icon,
}: {
    label: string;
    value: string | number;
    sub?: string;
    tone?: "ok" | "warn" | "danger" | "neutral";
    icon?: React.ReactNode;
}) {
    const colors = {
        ok: "border-emerald-500/20 bg-emerald-500/8",
        warn: "border-amber-500/20 bg-amber-500/8",
        danger: "border-rose-500/20 bg-rose-500/8",
        neutral: "border-[var(--border)] bg-[var(--surface)]",
    };
    const dotColors = {
        ok: "bg-emerald-500",
        warn: "bg-amber-500",
        danger: "bg-rose-500",
        neutral: "bg-slate-400",
    };
    return (
        <div className={`rounded-2xl border p-4 ${colors[tone]}`}>
            <div className="flex items-center gap-2 mb-2">
                <div className={`h-2 w-2 rounded-full ${dotColors[tone]}`} />
                {icon && <span className="text-[var(--text-muted)]">{icon}</span>}
                <span className="text-[11px] uppercase tracking-wider text-[var(--text-muted)] font-medium">{label}</span>
            </div>
            <div className="text-2xl font-bold text-[var(--text-main)] truncate">{value}</div>
            {sub && <div className="mt-1 text-[11px] text-[var(--text-muted)] truncate">{sub}</div>}
        </div>
    );
}

function PartnerBar({
    partners,
    label,
}: {
    partners: { bin: string; count: number; sum: number }[];
    label: string;
}) {
    if (!partners || partners.length === 0) return null;
    const maxSum = Math.max(...partners.map((p) => p.sum));
    return (
        <div className="space-y-2">
            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">{label}</div>
            {partners.map((p) => (
                <div key={p.bin} className="group">
                    <div className="flex items-center justify-between mb-0.5 text-xs">
                        <span className="font-mono text-[var(--text-main)] truncate max-w-[180px]">{p.bin}</span>
                        <span className="text-[var(--text-muted)] ml-2 shrink-0">{p.count} конт. · {fmt(p.sum)}</span>
                    </div>
                    <div className="h-1.5 w-full rounded-full bg-[var(--border)] overflow-hidden">
                        <div
                            className="h-full rounded-full bg-indigo-500 transition-all duration-500"
                            style={{ width: `${Math.max((p.sum / maxSum) * 100, 4)}%` }}
                        />
                    </div>
                </div>
            ))}
        </div>
    );
}

function YearChart({ byYear }: { byYear: Record<string, { count: number; sum: number }> }) {
    const entries = Object.entries(byYear).filter(([k]) => k !== "unknown").sort();
    if (entries.length === 0) return null;
    const maxSum = Math.max(...entries.map(([, v]) => v.sum));

    return (
        <div className="space-y-2">
            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">Динамика по годам</div>
            <div className="flex items-end gap-1.5 h-20">
                {entries.map(([year, v]) => (
                    <div key={year} className="flex-1 flex flex-col items-center gap-1 group">
                        <div className="relative w-full">
                            <div
                                className="w-full rounded-t bg-indigo-500/70 hover:bg-indigo-500 transition-all"
                                style={{ height: `${Math.max((v.sum / maxSum) * 64, 4)}px` }}
                                title={`${year}: ${v.count} контр. · ${fmt(v.sum)}`}
                            />
                        </div>
                        <span className="text-[9px] text-[var(--text-muted)]">{year}</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

function ContractTable({ contracts, role }: { contracts: Record<string, unknown>[]; role: "supplier" | "customer" }) {
    const [expanded, setExpanded] = useState(false);
    const shown = expanded ? contracts : contracts.slice(0, 5);
    if (contracts.length === 0) return <p className="text-xs text-[var(--text-muted)]">Нет данных</p>;
    return (
        <div>
            <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                <table className="min-w-full text-xs">
                    <thead>
                        <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)]">
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Номер</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">
                                {role === "supplier" ? "Заказчик (БИН)" : "Поставщик (БИН)"}
                            </th>
                            <th className="px-3 py-2 text-right text-[var(--text-muted)] font-medium">Сумма</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Дата</th>
                            <th className="px-3 py-2 text-center text-[var(--text-muted)] font-medium">Статус</th>
                        </tr>
                    </thead>
                    <tbody>
                        {shown.map((c, i) => {
                            const contract = c as {
                                id?: number;
                                contractNumber?: string;
                                trdBuyNumberAnno?: string;
                                customerBin?: string;
                                supplierBiin?: string;
                                contractSumWnds?: number;
                                signDate?: string;
                                refContractStatusId?: number;
                            };
                            const partner = role === "supplier" ? contract.customerBin : contract.supplierBiin;
                            const statusOk = contract.refContractStatusId === 390;
                            return (
                                <tr key={i} className="border-b border-[var(--border)] hover:bg-[var(--surface-hover)] transition-colors">
                                    <td className="px-3 py-2 font-mono text-[var(--text-main)]">
                                        {contract.contractNumber || contract.trdBuyNumberAnno || `#${contract.id}`}
                                    </td>
                                    <td className="px-3 py-2 text-[var(--text-muted)] font-mono">{partner || "—"}</td>
                                    <td className="px-3 py-2 text-right font-semibold text-[var(--text-main)]">
                                        {contract.contractSumWnds ? fmt(contract.contractSumWnds) : "—"}
                                    </td>
                                    <td className="px-3 py-2 text-[var(--text-muted)]">
                                        {contract.signDate ? contract.signDate.slice(0, 10) : "—"}
                                    </td>
                                    <td className="px-3 py-2 text-center">
                                        {statusOk
                                            ? <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 mx-auto" />
                                            : <Clock className="h-3.5 w-3.5 text-[var(--text-muted)] mx-auto" />
                                        }
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            {contracts.length > 5 && (
                <button
                    onClick={() => setExpanded(!expanded)}
                    className="mt-2 text-xs text-indigo-500 hover:text-indigo-400 transition-colors flex items-center gap-1"
                >
                    {expanded ? "Свернуть" : `Показать все ${contracts.length} контрактов`}
                    <ChevronRight className={`h-3.5 w-3.5 transition-transform ${expanded ? "rotate-90" : ""}`} />
                </button>
            )}
        </div>
    );
}

function ComplaintsTable({ complaints }: { complaints: Record<string, unknown>[] }) {
    const [expanded, setExpanded] = useState(false);
    const shown = expanded ? complaints : complaints.slice(0, 5);
    if (complaints.length === 0) return null;
    return (
        <div>
            <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                <table className="min-w-full text-xs">
                    <thead>
                        <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)]">
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">№ жалобы</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Дата подачи</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Статус</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Тендер</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">Организатор</th>
                        </tr>
                    </thead>
                    <tbody>
                        {shown.map((c, i) => {
                            const status = String(c.status || "").toLowerCase();
                            const isSatisfied = status.includes("удовлетворен");
                            const isRejected = status.includes("отказано") || status.includes("отклонен");
                            const isPending = status.includes("подана") || status.includes("рассмотрени");
                            const dateRaw = String(c.date_submitted || "");
                            const dateShort = dateRaw.length > 10 ? dateRaw.slice(0, 10) : dateRaw;
                            return (
                                <tr key={i} className="border-b border-[var(--border)] hover:bg-[var(--surface-hover)] transition-colors">
                                    <td className="px-3 py-2 font-mono text-[var(--text-main)]">{String(c.complaint_number || "—")}</td>
                                    <td className="px-3 py-2 text-[var(--text-muted)]">{dateShort || "—"}</td>
                                    <td className="px-3 py-2">
                                        {c.status ? (
                                            <span className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                                isSatisfied ? "bg-rose-500/10 text-rose-500" :
                                                isRejected ? "bg-emerald-500/10 text-emerald-500" :
                                                isPending ? "bg-amber-500/10 text-amber-500" :
                                                "bg-slate-500/10 text-slate-500"
                                            }`}>
                                                {String(c.status)}
                                            </span>
                                        ) : <span className="text-[var(--text-muted)]">—</span>}
                                    </td>
                                    <td className="px-3 py-2 font-mono text-[var(--text-muted)]">{String(c.tender_number || "—")}</td>
                                    <td className="px-3 py-2 text-[var(--text-muted)] max-w-[200px] truncate" title={String(c.organizer_name || "")}>
                                        {c.organizer_bin ? (
                                            <span><span className="font-mono">{String(c.organizer_bin)}</span>{c.organizer_name ? ` ${String(c.organizer_name).slice(0, 30)}` : ""}</span>
                                        ) : "—"}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            {complaints.length > 5 && (
                <button
                    onClick={() => setExpanded(!expanded)}
                    className="mt-2 text-xs text-indigo-500 hover:text-indigo-400 transition-colors flex items-center gap-1"
                >
                    {expanded ? "Свернуть" : `Показать все ${complaints.length} жалоб`}
                    <ChevronRight className={`h-3.5 w-3.5 transition-transform ${expanded ? "rotate-90" : ""}`} />
                </button>
            )}
        </div>
    );
}

// ── Main Component ─────────────────────────────────────────────────────────────

export default function CompanyProfileView() {
    const [query, setQuery] = useState("");
    const [suggestions, setSuggestions] = useState<CompanySearchResult[]>([]);
    const [suggestLoading, setSuggestLoading] = useState(false);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [profile, setProfile] = useState<CompanyProfile | null>(null);
    const [profileLoading, setProfileLoading] = useState(false);
    const [profileError, setProfileError] = useState<string | null>(null);
    const [profileDisplayName, setProfileDisplayName] = useState<string>("");
    const [activeTab, setActiveTab] = useState<"overview" | "supplier" | "customer">("overview");

    // LLM analysis
    const [llmLoading, setLlmLoading] = useState(false);
    const [llmNarrative, setLlmNarrative] = useState<string | null>(null);
    const [llmError, setLlmError] = useState<string | null>(null);

    // AI Chat
    const [showChat, setShowChat] = useState(false);
    const [chatMessages, setChatMessages] = useState<{ role: "user" | "assistant"; text: string }[]>([]);
    const [chatInput, setChatInput] = useState("");
    const [chatLoading, setChatLoading] = useState(false);
    const chatBottomRef = useRef<HTMLDivElement>(null);

    const searchRef = useRef<HTMLInputElement>(null);
    const suggestTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

    // Debounced autocomplete search
    useEffect(() => {
        if (suggestTimer.current) clearTimeout(suggestTimer.current);
        if (query.length < 2) {
            setSuggestions([]);
            return;
        }
        suggestTimer.current = setTimeout(async () => {
            setSuggestLoading(true);
            try {
                const res = await api.companySearch(query);
                setSuggestions(res.results || []);
                setShowSuggestions(true);
            } catch {
                setSuggestions([]);
            } finally {
                setSuggestLoading(false);
            }
        }, 400);
    }, [query]);

    const loadProfile = useCallback(async (bin: string, name?: string) => {
        setShowSuggestions(false);
        setProfileLoading(true);
        setProfileError(null);
        setProfile(null);
        setActiveTab("overview");
        setLlmNarrative(null);
        setLlmError(null);
        if (name) { setQuery(name); setProfileDisplayName(name); }
        else { setProfileDisplayName(""); }
        try {
            const data = await api.companyProfile(bin);
            setProfile(data);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : "Ошибка загрузки";
            setProfileError(msg);
        } finally {
            setProfileLoading(false);
        }
    }, []);

    const runLlmAnalysis = useCallback(async () => {
        if (!profile) return;
        setLlmLoading(true);
        setLlmError(null);
        setLlmNarrative(null);
        try {
            const res = await api.companyAnalyze(profile.bin, profile);
            setLlmNarrative(res.narrative);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : "Ошибка LLM";
            setLlmError(msg);
        } finally {
            setLlmLoading(false);
        }
    }, [profile]);

    function handleSubmit(e: React.SyntheticEvent) {
        e.preventDefault();
        const q = query.trim();
        if (!q) return;
        // BIN — load directly, no search needed
        if (/^\d{12}$/.test(q)) {
            loadProfile(q);
            return;
        }
        // Name search — always do a fresh API call with loading indicator
        setShowSuggestions(false);
        setProfileLoading(true);
        setProfileError(null);
        setProfile(null);
        setActiveTab("overview");
        setLlmNarrative(null);
        setLlmError(null);
        api.companySearch(q).then((res) => {
            if (res.results.length === 0) {
                setProfileError(`Компания "${q}" не найдена в реестре OWS. Проверьте название или введите БИН.`);
                setProfileLoading(false);
                return;
            }
            const s = res.results[0];
            const bin = s.bin || s.iin;
            if (!bin) {
                setProfileError("Не удалось определить БИН компании. Попробуйте выбрать из списка.");
                setProfileLoading(false);
                return;
            }
            const name = s.nameRu || s.fullNameRu || s.nameKz || bin;
            setProfileDisplayName(name);
            setQuery(name);
            api.companyProfile(bin).then((data) => {
                setProfile(data);
            }).catch((e: unknown) => {
                setProfileError(e instanceof Error ? e.message : "Ошибка загрузки профиля");
            }).finally(() => setProfileLoading(false));
        }).catch((e: unknown) => {
            setProfileError(e instanceof Error ? e.message : "Ошибка поиска");
            setProfileLoading(false);
        });
    }

    const isSupplier = profile
        ? profile.as_supplier.metrics.total_contracts > 0 || profile.subject?.supplier
        : false;
    const isCustomer = profile
        ? profile.as_customer.metrics.total_contracts > 0 || profile.subject?.customer
        : false;

    // Shared report data builder
    function buildReportData() {
        if (!profile) return null;
        const name = profile.subject?.nameRu || profile.subject?.fullNameRu || profile.subject?.nameKz || profileDisplayName || `БИН: ${profile.bin}`;
        const sup = profile.as_supplier.metrics;
        const cust = profile.as_customer.metrics;
        const date = new Date(profile.fetched_at).toLocaleString("ru-RU");
        const flagLines = profile.risk.flags.map((f) => FLAG_LABELS[f] || f);
        return { name, sup, cust, date, flagLines };
    }

    // Export as Markdown
    function handleExportMd() {
        const d = buildReportData();
        if (!d || !profile) return;
        const { name, sup, cust, date, flagLines } = d;
        const lines = [
            `# Досье компании: ${name}`,
            `**БИН:** ${profile.bin}`,
            `**Дата анализа:** ${date}`,
            `**Статус РНУ:** ${profile.rnu.is_blacklisted ? "ВНЕСЁН В РЕЕСТР НЕДОБРОСОВЕСТНЫХ" : "Чистый"}`,
            `**Риск-уровень:** ${profile.risk.level} (score: ${profile.risk.score}/100)`,
            ``,
            `## Флаги риска`,
            ...flagLines.map((f) => `- ${f}`),
            ``,
            `## Как поставщик`,
            `- Всего контрактов: ${sup.total_contracts}`,
            `- Общая сумма: ${fmt(sup.total_sum)}`,
            `- Исполнение: ${sup.execution_rate}% (факт: ${fmt(sup.executed_sum)})`,
            `- Уникальных заказчиков: ${sup.unique_customers}`,
            `- Просрочки актов: ${sup.overdue_count}`,
            `- Штрафы: ${sup.fines_count}`,
            `- Средний контракт: ${fmt(sup.avg_contract_size)}`,
            `- Выплачено казначейством: ${fmt(sup.treasury_paid)}`,
            ``,
            `## Как заказчик`,
            `- Всего тендеров: ${cust.total_tenders}`,
            `- Всего контрактов: ${cust.total_contracts}`,
            `- Объём закупок: ${fmt(cust.total_procurement_sum)}`,
            `- Уникальных поставщиков: ${cust.unique_suppliers}`,
            `- Доля единственного источника: ${cust.single_source_rate}% (${cust.single_source_count} тендеров)`,
            `- Отменённые тендеры: ${cust.cancelled_tenders}`,
            ``,
        ];
        if (llmNarrative) lines.push(`## AI Анализ`, ``, llmNarrative, ``);
        lines.push(`---`, `Источник: OWS API v3 goszakup.gov.kz · Tender Radar`);
        const blob = new Blob([lines.join("\n")], { type: "text/markdown;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `company_${profile.bin}_${new Date().toISOString().slice(0, 10)}.md`;
        a.click();
        URL.revokeObjectURL(url);
    }

    // Export as PDF via print dialog
    function handleExportPdf() {
        const d = buildReportData();
        if (!d || !profile) return;
        const { name, sup, cust, date, flagLines } = d;
        const riskColor = profile.risk.level === "HIGH" ? "#dc2626" : profile.risk.level === "MEDIUM" ? "#d97706" : "#059669";
        const riskBg = profile.risk.level === "HIGH" ? "#fee2e2" : profile.risk.level === "MEDIUM" ? "#fef3c7" : "#d1fae5";
        const flagsHtml = flagLines.length > 0
            ? flagLines.map((f) => `<span class="flag">${f}</span>`).join("")
            : "<span class='flag-ok'>Нет флагов риска</span>";
        const narrativeHtml = llmNarrative
            ? `<h2>AI Анализ</h2><div class="narrative">${llmNarrative.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>").replace(/\n/g, "<br/>")}</div>`
            : "";
        const html = `<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"/>
<title>Досье: ${name}</title>
<style>
  body{font-family:'Segoe UI',Arial,sans-serif;max-width:800px;margin:0 auto;padding:32px;color:#1e293b;font-size:14px;}
  h1{font-size:22px;font-weight:700;border-bottom:3px solid #6366f1;padding-bottom:12px;margin-bottom:8px;}
  h2{font-size:15px;font-weight:600;color:#6366f1;margin-top:24px;margin-bottom:8px;border-left:3px solid #6366f1;padding-left:8px;}
  .meta{color:#64748b;font-size:12px;margin-bottom:20px;}
  .risk-badge{display:inline-block;padding:4px 14px;border-radius:20px;font-weight:700;font-size:13px;background:${riskBg};color:${riskColor};}
  .flag{display:inline-block;background:#fff7ed;border:1px solid #fed7aa;color:#c2410c;padding:2px 10px;border-radius:4px;margin:2px 4px 2px 0;font-size:12px;}
  .flag-ok{color:#059669;font-size:12px;}
  table{width:100%;border-collapse:collapse;margin:10px 0;}
  th{background:#f8fafc;text-align:left;padding:8px 12px;font-weight:600;color:#475569;border-bottom:2px solid #e2e8f0;}
  td{padding:8px 12px;border-bottom:1px solid #e2e8f0;}
  td:last-child{font-weight:600;text-align:right;}
  .narrative{background:#f8fafc;border-radius:8px;padding:16px;line-height:1.7;font-size:13px;}
  .footer{margin-top:32px;padding-top:12px;border-top:1px solid #e2e8f0;color:#94a3b8;font-size:11px;}
  @media print{body{padding:16px}button{display:none}}
</style></head><body>
<h1>${name}</h1>
<div class="meta">БИН: <strong>${profile.bin}</strong> &nbsp;·&nbsp; Дата анализа: ${date} &nbsp;·&nbsp; Источник: OWS API v3</div>
<div style="margin-bottom:16px;">
  <span class="risk-badge">Риск: ${profile.risk.level} · ${profile.risk.score}/100</span>
  &nbsp; <span style="font-size:12px;color:${profile.rnu.is_blacklisted ? "#dc2626" : "#059669"}">${profile.rnu.is_blacklisted ? "⛔ В реестре недобросовестных поставщиков" : "✓ Не в реестре РНУ"}</span>
</div>
<h2>Флаги риска</h2>
<div>${flagsHtml}</div>
<h2>Как поставщик</h2>
<table>
  <tr><th>Показатель</th><th>Значение</th></tr>
  <tr><td>Всего контрактов</td><td>${sup.total_contracts}</td></tr>
  <tr><td>Общая сумма</td><td>${fmt(sup.total_sum)}</td></tr>
  <tr><td>Исполнение</td><td>${sup.execution_rate}%</td></tr>
  <tr><td>Уникальных заказчиков</td><td>${sup.unique_customers}</td></tr>
  <tr><td>Просрочки актов</td><td>${sup.overdue_count}</td></tr>
  <tr><td>Штрафы</td><td>${sup.fines_count}</td></tr>
  <tr><td>Средний контракт</td><td>${fmt(sup.avg_contract_size)}</td></tr>
  <tr><td>Выплачено казначейством</td><td>${fmt(sup.treasury_paid)}</td></tr>
</table>
<h2>Как заказчик</h2>
<table>
  <tr><th>Показатель</th><th>Значение</th></tr>
  <tr><td>Всего тендеров</td><td>${cust.total_tenders}</td></tr>
  <tr><td>Всего контрактов</td><td>${cust.total_contracts}</td></tr>
  <tr><td>Объём закупок</td><td>${fmt(cust.total_procurement_sum)}</td></tr>
  <tr><td>Уникальных поставщиков</td><td>${cust.unique_suppliers}</td></tr>
  <tr><td>Доля единственного источника</td><td>${cust.single_source_rate}%</td></tr>
  <tr><td>Отменённые тендеры</td><td>${cust.cancelled_tenders}</td></tr>
</table>
${narrativeHtml}
<div class="footer">Tender Radar · OWS API v3 goszakup.gov.kz · Только для справки, не является юридическим заключением</div>
<script>window.onload=function(){window.print();}<\/script>
</body></html>`;
        const w = window.open("", "_blank");
        if (w) { w.document.write(html); w.document.close(); }
    }

    // Build company context string for AI chat
    function buildChatContext(): string {
        if (!profile) return "";
        const name = profile.subject?.nameRu || profile.subject?.fullNameRu || profile.subject?.nameKz || profileDisplayName || `БИН ${profile.bin}`;
        const sup = profile.as_supplier.metrics;
        const cust = profile.as_customer.metrics;
        const parts = [
            `Компания: ${name} (БИН: ${profile.bin})`,
            `Риск-уровень: ${profile.risk.level} (score=${profile.risk.score}/100)`,
            `Флаги: ${profile.risk.flags.map(f => FLAG_LABELS[f] || f).join(", ") || "нет"}`,
            `РНУ (недобросовестный поставщик): ${profile.rnu.is_blacklisted ? "ДА" : "нет"}`,
            `Как поставщик: ${sup.total_contracts} контр. на ${fmt(sup.total_sum)}, исполнение ${sup.execution_rate}%, просрочки ${sup.overdue_count}, штрафы ${sup.fines_count}`,
            `Как заказчик: ${cust.total_tenders} тендеров, закупки ${fmt(cust.total_procurement_sum)}, ЕИ ${cust.single_source_rate}%`,
        ];
        if (profile.kgd?.available) {
            const taxYears = (profile.kgd.tax_payments || []).map(t => `${t.year}: ${fmt(t.amount)}${t.change_pct !== null ? ` (${t.change_pct > 0 ? "+" : ""}${t.change_pct}%)` : ""}`).join("; ");
            parts.push(`Налоги (ba.prg.kz): ${taxYears || "нет данных"}, задолженность: ${profile.kgd.tax_debt ? "ДА" : "нет"}, лжепредприятие: ${profile.kgd.pseudo_enterprise ? "ДА" : "нет"}, недобросовестный: ${profile.kgd.is_unreliable_taxpayer ? "ДА" : "нет"}`);
            if (profile.kgd.llm_analysis?.summary) {
                parts.push(`AI-анализ налогов: ${profile.kgd.llm_analysis.summary}`);
            }
        }
        if (profile.complaints && profile.complaints.total > 0) {
            parts.push(`Жалобы: ${profile.complaints.total} всего, удовлетворено ${profile.complaints.satisfaction_rate}%`);
        }
        if (profile.court_cases && profile.court_cases.total > 0) {
            parts.push(`Суды: ${profile.court_cases.total} дел, как ответчик ${profile.court_cases.as_defendant}, LLM-impact ${profile.court_cases.avg_reliability_impact}/10`);
        }
        if (profile.affiliations && profile.affiliations.total_links > 0) {
            parts.push(`Связи: ${profile.affiliations.total_links} (банк.счета: ${profile.affiliations.shared_bank_accounts.length}, контакты: ${profile.affiliations.shared_contacts.length}, co-bidding: ${profile.affiliations.cobid_partners.length})`);
        }
        if (llmNarrative) parts.push(`\nAI анализ:\n${llmNarrative}`);
        return parts.join("\n");
    }

    async function sendChatMessage() {
        const text = chatInput.trim();
        if (!text || chatLoading) return;
        const userMsg = { role: "user" as const, text };
        const newMessages = [...chatMessages, userMsg];
        setChatMessages(newMessages);
        setChatInput("");
        setChatLoading(true);
        try {
            // Call backend directly (same approach as api.ts) to avoid Next.js proxy hostname issues
            const token = localStorage.getItem("token");
            const backendUrl = `http://${window.location.hostname}:8000/api/v1/chat/`;
            const res = await fetch(backendUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    ...(token ? { Authorization: `Bearer ${token}` } : {}),
                },
                body: JSON.stringify({ messages: newMessages, lotsContext: buildChatContext() }),
            });
            const data = await res.json();
            if (!res.ok) {
                const errMsg = data.detail || data.error || `Ошибка сервера ${res.status}`;
                setChatMessages([...newMessages, { role: "assistant", text: `⚠️ ${errMsg}` }]);
            } else {
                setChatMessages([...newMessages, { role: "assistant", text: data.text || "Получен пустой ответ" }]);
            }
        } catch (err) {
            const msg = err instanceof Error ? err.message : "Ошибка соединения";
            setChatMessages([...newMessages, { role: "assistant", text: `⚠️ ${msg}` }]);
        } finally {
            setChatLoading(false);
        }
    }

    // Auto-scroll chat
    useEffect(() => {
        if (showChat) chatBottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [chatMessages, showChat]);

    return (
        <div className="space-y-6">
            {/* ── Header ── */}
            <div className="flex items-center gap-3">
                <div className="rounded-2xl bg-gradient-to-br from-indigo-500 to-violet-600 p-2.5 shadow-lg shadow-indigo-500/20">
                    <Building2 className="h-5 w-5 text-white" />
                </div>
                <div>
                    <h2 className="text-base font-bold text-[var(--text-main)]">Company Intel</h2>
                    <p className="text-xs text-[var(--text-muted)]">Досье по БИН или названию — данные из реестра Goszakup в реальном времени</p>
                </div>
                <span className="ml-auto rounded-full bg-indigo-500/10 border border-indigo-500/20 px-2.5 py-1 text-[10px] font-semibold text-indigo-500 uppercase tracking-wider">
                    Live OWS
                </span>
            </div>

            {/* ── Search Box ── */}
            <div className="relative">
                <form onSubmit={handleSubmit}>
                    <div className="relative flex items-center gap-2">
                        <div className="flex-1 relative">
                            <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--text-muted)]" />
                            <input
                                ref={searchRef}
                                type="text"
                                value={query}
                                onChange={(e) => { setQuery(e.target.value); setShowSuggestions(true); }}
                                onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
                                onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
                                placeholder="Введите БИН (12 цифр) или название компании..."
                                className="w-full rounded-2xl border border-[var(--border)] bg-[var(--surface)] pl-10 pr-4 py-3 text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-indigo-500 transition-colors"
                            />
                            {suggestLoading && (
                                <Loader2 className="absolute right-3.5 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--text-muted)] animate-spin" />
                            )}
                        </div>
                        <button
                            type="submit"
                            className="rounded-2xl bg-indigo-500 hover:bg-indigo-400 px-5 py-3 text-sm font-medium text-white transition-colors shrink-0"
                        >
                            Найти
                        </button>
                    </div>
                </form>

                {/* Autocomplete dropdown */}
                {showSuggestions && suggestions.length > 0 && (
                    <div className="absolute z-50 mt-1 w-full max-h-72 overflow-y-auto rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-xl">
                        {suggestions.map((s, i) => (
                            <button
                                key={i}
                                onMouseDown={() => {
                                    if (s.bin) loadProfile(s.bin, s.nameRu || s.bin);
                                }}
                                className="w-full flex items-center gap-3 px-4 py-3 hover:bg-[var(--surface-hover)] transition-colors text-left"
                            >
                                <div className="h-8 w-8 rounded-xl bg-indigo-500/10 border border-indigo-500/20 flex items-center justify-center shrink-0">
                                    <Building2 className="h-4 w-4 text-indigo-500" />
                                </div>
                                <div className="flex-1 min-w-0">
                                    <div className="text-sm font-medium text-[var(--text-main)] truncate">{s.nameRu || "Без названия"}</div>
                                    <div className="text-xs text-[var(--text-muted)] flex items-center gap-2 mt-0.5">
                                        <span className="font-mono">{s.bin || s.iin || "—"}</span>
                                        {s.supplier && <span className="rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">Поставщик</span>}
                                        {s.customer && <span className="rounded bg-indigo-500/10 border border-indigo-500/20 text-indigo-600 dark:text-indigo-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">Заказчик</span>}
                                    </div>
                                </div>
                                <ChevronRight className="h-4 w-4 text-[var(--text-muted)] shrink-0" />
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {/* ── Empty State ── */}
            {!profile && !profileLoading && !profileError && (
                <div className="flex flex-col items-center justify-center py-16 text-[var(--text-muted)] space-y-4">
                    <div className="relative">
                        <div className="h-20 w-20 rounded-3xl bg-indigo-500/10 border border-indigo-500/20 flex items-center justify-center">
                            <Building2 className="h-9 w-9 text-indigo-500/70" />
                        </div>
                        <div className="absolute -bottom-1 -right-1 h-6 w-6 rounded-full bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center">
                            <Search className="h-3 w-3 text-emerald-500" />
                        </div>
                    </div>
                    <div className="text-center space-y-1">
                        <p className="text-sm font-medium text-[var(--text-main)]">Введите БИН или название компании</p>
                        <p className="text-xs text-[var(--text-muted)]">Система загрузит актуальные данные прямо из реестра Goszakup</p>
                    </div>
                    <div className="flex gap-2 flex-wrap justify-center">
                        {["ТОО «Казахтелеком»", "АО «КазМунайГаз»", "160540017468"].map((ex) => (
                            <button
                                key={ex}
                                onClick={() => { setQuery(ex); searchRef.current?.focus(); }}
                                className="rounded-xl border border-[var(--border)] px-3 py-1.5 text-xs text-[var(--text-muted)] hover:text-[var(--text-main)] hover:border-[var(--border-hover)] transition-colors"
                            >
                                {ex}
                            </button>
                        ))}
                    </div>
                </div>
            )}

            {/* ── Loading ── */}
            {profileLoading && (
                <div className="flex flex-col items-center justify-center py-16 space-y-4">
                    <div className="relative">
                        <div className="h-16 w-16 rounded-2xl bg-indigo-500/10 border border-indigo-500/20 flex items-center justify-center">
                            <Loader2 className="h-7 w-7 text-indigo-500 animate-spin" />
                        </div>
                    </div>
                    <div className="text-center space-y-1">
                        <p className="text-sm font-medium text-[var(--text-main)]">Загружаем данные из OWS...</p>
                        <p className="text-xs text-[var(--text-muted)]">Контракты · Тендеры · Реестр · Жалобы · KGD · Суды · Аффилированность</p>
                        <p className="text-[10px] text-[var(--text-muted)] mt-1">Для крупных компаний это может занять 10–30 секунд</p>
                    </div>
                </div>
            )}

            {/* ── Error ── */}
            {profileError && (
                <div className="flex items-start gap-3 rounded-2xl border border-rose-500/20 bg-rose-500/8 p-4">
                    <XCircle className="h-5 w-5 text-rose-500 shrink-0 mt-0.5" />
                    <div>
                        <p className="text-sm font-medium text-rose-600 dark:text-rose-400">Ошибка загрузки</p>
                        <p className="text-xs text-[var(--text-muted)] mt-0.5">{profileError}</p>
                    </div>
                </div>
            )}

            {/* ── Profile ── */}
            {profile && !profileLoading && (
                <div className="space-y-5">
                    {/* Company Header Card */}
                    <div className={`rounded-2xl border p-5 ${profile.rnu.is_blacklisted ? "border-rose-500/30 bg-rose-500/5" : "border-[var(--border)] bg-[var(--surface)]"}`}>
                        <div className="flex items-start gap-4 flex-wrap">
                            <div className="h-14 w-14 rounded-2xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shrink-0 shadow-lg shadow-indigo-500/20">
                                <Building2 className="h-7 w-7 text-white" />
                            </div>
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 flex-wrap">
                                    <h3 className="text-lg font-bold text-[var(--text-main)] truncate">
                                        {profile.subject?.nameRu || profile.subject?.fullNameRu || profile.subject?.nameKz || profileDisplayName || `БИН: ${profile.bin}`}
                                    </h3>
                                    {profile.rnu.is_blacklisted && (
                                        <span className="rounded-full bg-rose-500/10 border border-rose-500/30 px-2.5 py-1 text-[10px] font-bold text-rose-600 dark:text-rose-400 uppercase tracking-wider flex items-center gap-1">
                                            <AlertTriangle className="h-3 w-3" /> Недобросовестный поставщик
                                        </span>
                                    )}
                                </div>
                                <div className="mt-1.5 flex items-center gap-3 flex-wrap text-xs text-[var(--text-muted)]">
                                    <span className="font-mono">{profile.bin}</span>
                                    {profile.subject?.regdate && (
                                        <span>Рег: {profile.subject.regdate.slice(0, 10)}</span>
                                    )}
                                    {isSupplier && (
                                        <span className="rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">Поставщик</span>
                                    )}
                                    {isCustomer && (
                                        <span className="rounded bg-indigo-500/10 border border-indigo-500/20 text-indigo-600 dark:text-indigo-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">Заказчик</span>
                                    )}
                                    {profile.subject?.email && (
                                        <span className="flex items-center gap-1"><Info className="h-3 w-3" />{profile.subject.email}</span>
                                    )}
                                    {profile.subject?.phone && <span>{profile.subject.phone}</span>}
                                </div>
                            </div>
                            <RiskBadge level={profile.risk.level} score={profile.risk.score} />
                        </div>

                        {/* Export + Chat buttons */}
                        <div className="mt-3 flex gap-2 flex-wrap">
                            <button
                                onClick={handleExportMd}
                                className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-1.5 text-xs font-medium text-[var(--text-muted)] hover:text-[var(--text-main)] hover:border-[var(--border-hover)] transition-colors"
                            >
                                <Download className="h-3.5 w-3.5" />
                                .md
                            </button>
                            <button
                                onClick={handleExportPdf}
                                className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-1.5 text-xs font-medium text-[var(--text-muted)] hover:text-[var(--text-main)] hover:border-[var(--border-hover)] transition-colors"
                            >
                                <Download className="h-3.5 w-3.5" />
                                PDF / печать
                            </button>
                            <button
                                onClick={() => { setShowChat(!showChat); if (!showChat && chatMessages.length === 0) setChatMessages([]); }}
                                className={`flex items-center gap-1.5 rounded-xl border px-3 py-1.5 text-xs font-medium transition-colors ${showChat ? "border-indigo-500/40 bg-indigo-500/10 text-indigo-500" : "border-[var(--border)] bg-[var(--surface-hover)] text-[var(--text-muted)] hover:text-[var(--text-main)]"}`}
                            >
                                <MessageSquare className="h-3.5 w-3.5" />
                                AI Ассистент
                                <ChevronDown className={`h-3 w-3 transition-transform ${showChat ? "rotate-180" : ""}`} />
                            </button>
                        </div>

                    {/* Risk flags */}
                        {profile.risk.flags.length > 0 && (
                            <div className="mt-4 pt-4 border-t border-[var(--border)] flex flex-wrap gap-2">
                                {profile.risk.flags.map((flag) => (
                                    <span key={flag} className="inline-flex items-center gap-1 rounded-xl bg-[var(--surface-hover)] border border-[var(--border)] px-2.5 py-1 text-[11px] text-[var(--text-muted)]">
                                        <AlertTriangle className="h-3 w-3 text-amber-500" />
                                        {FLAG_LABELS[flag] || flag}
                                    </span>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Tab Navigation */}
                    <div className="flex gap-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-1">
                        {(["overview", "supplier", "customer"] as const).map((tab) => {
                            const labels = { overview: "Обзор", supplier: "Как поставщик", customer: "Как заказчик" };
                            const counts = {
                                overview: null,
                                supplier: profile.as_supplier.metrics.total_contracts,
                                customer: profile.as_customer.metrics.total_contracts,
                            };
                            return (
                                <button
                                    key={tab}
                                    onClick={() => setActiveTab(tab)}
                                    className={`flex-1 flex items-center justify-center gap-2 rounded-xl px-3 py-2 text-sm font-medium transition-all ${activeTab === tab
                                        ? "bg-indigo-500 text-white shadow-sm"
                                        : "text-[var(--text-muted)] hover:text-[var(--text-main)]"
                                        }`}
                                >
                                    {labels[tab]}
                                    {counts[tab] !== null && counts[tab]! > 0 && (
                                        <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${activeTab === tab ? "bg-white/20 text-white" : "bg-[var(--border)] text-[var(--text-muted)]"}`}>
                                            {counts[tab]}
                                        </span>
                                    )}
                                </button>
                            );
                        })}
                    </div>

                    {/* ── Tab: Overview ── */}
                    {activeTab === "overview" && (
                        <div className="space-y-5">
                            {/* KPIs */}
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard
                                    label="Контракты (поставщик)"
                                    value={profile.as_supplier.metrics.total_contracts}
                                    sub={profile.as_supplier.metrics.total_contracts > 0 ? fmt(profile.as_supplier.metrics.total_sum) : "нет данных"}
                                    tone={profile.as_supplier.metrics.total_contracts > 0 ? "neutral" : "neutral"}
                                    icon={<Package className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Исполнение"
                                    value={`${profile.as_supplier.metrics.execution_rate}%`}
                                    sub={`факт: ${fmt(profile.as_supplier.metrics.executed_sum)}`}
                                    tone={
                                        profile.as_supplier.metrics.total_contracts === 0 ? "neutral" :
                                            profile.as_supplier.metrics.execution_rate >= 80 ? "ok" :
                                                profile.as_supplier.metrics.execution_rate >= 50 ? "warn" : "danger"
                                    }
                                    icon={<TrendingUp className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Тендеры (заказчик)"
                                    value={profile.as_customer.metrics.total_tenders}
                                    sub={`контракты: ${profile.as_customer.metrics.total_contracts}`}
                                    tone="neutral"
                                    icon={<FileText className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Статус РНУ"
                                    value={profile.rnu.is_blacklisted ? "В реестре" : "Чистый"}
                                    sub={profile.rnu.is_blacklisted ? `${profile.rnu.active_count} активных записей` : "Не в реестре недобросовестных"}
                                    tone={profile.rnu.is_blacklisted ? "danger" : "ok"}
                                    icon={profile.rnu.is_blacklisted ? <ShieldAlert className="h-3.5 w-3.5" /> : <ShieldCheck className="h-3.5 w-3.5" />}
                                />
                            </div>

                            {/* Charts Row */}
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {/* Supplier chart */}
                                {Object.keys(profile.as_supplier.metrics.by_year).length > 0 && (
                                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                        <div className="flex items-center gap-2 text-sm font-semibold text-[var(--text-main)]">
                                            <BarChart3 className="h-4 w-4 text-indigo-500" />
                                            Как поставщик — суммы по годам
                                        </div>
                                        <YearChart byYear={profile.as_supplier.metrics.by_year} />
                                        <PartnerBar partners={profile.as_supplier.metrics.top_customers} label="Топ заказчики" />
                                    </div>
                                )}

                                {/* Customer chart */}
                                {Object.keys(profile.as_customer.metrics.by_year).length > 0 && (
                                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                        <div className="flex items-center gap-2 text-sm font-semibold text-[var(--text-main)]">
                                            <BarChart3 className="h-4 w-4 text-violet-500" />
                                            Как заказчик — суммы по годам
                                        </div>
                                        <YearChart byYear={profile.as_customer.metrics.by_year} />
                                        <PartnerBar partners={profile.as_customer.metrics.top_suppliers} label="Топ поставщики" />
                                    </div>
                                )}
                            </div>

                            {/* RNU detail */}
                            {profile.rnu.is_blacklisted && (
                                <div className="rounded-2xl border border-rose-500/20 bg-rose-500/5 p-4">
                                    <div className="flex items-center gap-2 mb-3">
                                        <ShieldAlert className="h-4 w-4 text-rose-500" />
                                        <span className="text-sm font-semibold text-rose-600 dark:text-rose-400">Реестр недобросовестных поставщиков</span>
                                    </div>
                                    <div className="space-y-2">
                                        {profile.rnu.records.slice(0, 3).map((r: Record<string, unknown>, i: number) => (
                                            <div key={i} className="text-xs text-[var(--text-muted)] flex gap-2">
                                                <span className="font-mono">{String(r.startDate || "").slice(0, 10)}</span>
                                                <span>→</span>
                                                <span className="font-mono">{String(r.endDate || "бессрочно").slice(0, 10)}</span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* ── Tax Payments (ba.prg.kz) ── */}
                            {profile.kgd?.available && profile.kgd.tax_payments?.length > 0 && (
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Landmark className="h-4 w-4 text-indigo-500" />
                                        <span className="text-sm font-semibold text-[var(--text-main)]">Налоговые отчисления</span>
                                        <span className="ml-auto rounded-full bg-indigo-500/10 border border-indigo-500/20 px-2 py-0.5 text-[9px] font-semibold text-indigo-500 uppercase tracking-wider">BA.PRG.KZ</span>
                                    </div>

                                    {/* Total KPI */}
                                    <MetricCard
                                        label="Общая сумма налоговых отчислений"
                                        value={fmt(profile.kgd.total_tax_paid)}
                                        tone="neutral"
                                        icon={<Banknote className="h-3.5 w-3.5" />}
                                    />

                                    {/* Tax payments by year table */}
                                    <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                                        <table className="w-full text-xs">
                                            <thead>
                                                <tr className="bg-[var(--surface-alt)]">
                                                    <th className="px-3 py-2 text-left font-medium text-[var(--text-muted)]">Год</th>
                                                    <th className="px-3 py-2 text-right font-medium text-[var(--text-muted)]">Сумма</th>
                                                    <th className="px-3 py-2 text-right font-medium text-[var(--text-muted)]">Изменение</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-[var(--border)]">
                                                {profile.kgd.tax_payments.map((tp) => (
                                                    <tr key={tp.year} className="hover:bg-[var(--surface-alt)] transition-colors">
                                                        <td className="px-3 py-1.5 font-medium text-[var(--text-main)]">{tp.year}</td>
                                                        <td className="px-3 py-1.5 text-right text-[var(--text-main)]">{fmt(tp.amount)}</td>
                                                        <td className="px-3 py-1.5 text-right">
                                                            {tp.change_pct !== null && tp.change_pct !== undefined ? (
                                                                <span className={tp.change_pct > 0 ? "text-emerald-500" : tp.change_pct < -30 ? "text-red-500" : "text-amber-500"}>
                                                                    {tp.change_pct > 0 ? "+" : ""}{tp.change_pct}%
                                                                </span>
                                                            ) : "—"}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>

                                    {/* LLM Tax Analysis */}
                                    {profile.kgd.llm_analysis && (
                                        <div className="rounded-lg border border-indigo-500/20 bg-indigo-500/5 p-3 space-y-1">
                                            <div className="flex items-center gap-1.5">
                                                <TrendingUp className="h-3.5 w-3.5 text-indigo-500" />
                                                <span className="text-xs font-semibold text-indigo-500">AI-анализ налоговых данных</span>
                                                {profile.kgd.llm_analysis.risk_level && (
                                                    <span className={`ml-auto rounded-full px-2 py-0.5 text-[9px] font-bold uppercase ${
                                                        profile.kgd.llm_analysis.risk_level === "high" ? "bg-red-500/10 text-red-500" :
                                                        profile.kgd.llm_analysis.risk_level === "medium" ? "bg-amber-500/10 text-amber-500" :
                                                        "bg-emerald-500/10 text-emerald-500"
                                                    }`}>
                                                        {profile.kgd.llm_analysis.risk_level}
                                                    </span>
                                                )}
                                            </div>
                                            <p className="text-xs text-[var(--text-main)] leading-relaxed">{profile.kgd.llm_analysis.summary}</p>
                                            {profile.kgd.llm_analysis.anomalies && profile.kgd.llm_analysis.anomalies.length > 0 && (
                                                <ul className="text-xs text-amber-600 space-y-0.5 mt-1">
                                                    {profile.kgd.llm_analysis.anomalies.map((a, i) => (
                                                        <li key={i} className="flex items-start gap-1">
                                                            <AlertTriangle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                                                            {a}
                                                        </li>
                                                    ))}
                                                </ul>
                                            )}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* ── Complaints ── */}
                            {profile.complaints && profile.complaints.total > 0 && (
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Megaphone className="h-4 w-4 text-amber-500" />
                                        <span className="text-sm font-semibold text-[var(--text-main)]">Жалобы</span>
                                        <span className="ml-auto text-xs text-[var(--text-muted)]">goszakup.kz</span>
                                    </div>
                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                        <MetricCard label="Всего жалоб" value={profile.complaints.total} tone="neutral" icon={<FileText className="h-3.5 w-3.5" />} />
                                        <MetricCard
                                            label="Как поставщик"
                                            value={profile.complaints.complaints_as_supplier}
                                            sub="объект жалобы"
                                            tone={profile.complaints.complaints_as_supplier > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label="Как заказчик"
                                            value={profile.complaints.complaints_as_customer}
                                            sub="на закупки"
                                            tone={profile.complaints.complaints_as_customer > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label="Удовлетворено"
                                            value={`${profile.complaints.satisfaction_rate}%`}
                                            sub={`${profile.complaints.satisfied_count} из ${profile.complaints.total}`}
                                            tone={profile.complaints.satisfaction_rate > 50 ? "danger" : profile.complaints.satisfaction_rate > 25 ? "warn" : "ok"}
                                        />
                                    </div>
                                    {profile.complaints.complaints.length > 0 && (
                                        <ComplaintsTable complaints={profile.complaints.complaints} />
                                    )}
                                    {/* LLM analyses of satisfied complaints */}
                                    {profile.complaints.llm_analyses && profile.complaints.llm_analyses.length > 0 && (
                                        <div className="space-y-2 mt-2">
                                            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">LLM-анализ удовлетворённых жалоб</div>
                                            {profile.complaints.llm_analyses.map((a, i) => (
                                                <div key={i} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 space-y-1">
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        <span className="font-mono text-xs text-[var(--text-main)]">Жалоба #{a.complaint_number || i + 1}</span>
                                                        {a.violation_type && (
                                                            <span className="rounded-full bg-amber-500/10 border border-amber-500/20 px-2 py-0.5 text-[10px] text-amber-600 dark:text-amber-400">{a.violation_type}</span>
                                                        )}
                                                        {a.severity && (
                                                            <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                                                a.severity === "high" ? "bg-rose-500/10 text-rose-500" :
                                                                a.severity === "medium" ? "bg-amber-500/10 text-amber-500" : "bg-emerald-500/10 text-emerald-500"
                                                            }`}>
                                                                {a.severity}
                                                            </span>
                                                        )}
                                                        <span className={`ml-auto text-[10px] font-bold ${
                                                            a.risk_impact >= 7 ? "text-rose-500" :
                                                            a.risk_impact >= 4 ? "text-amber-500" : "text-emerald-500"
                                                        }`}>
                                                            {a.risk_impact}/10
                                                        </span>
                                                    </div>
                                                    {a.summary && <p className="text-xs text-[var(--text-main)] leading-relaxed">{a.summary}</p>}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* ── Court Cases ── */}
                            {profile.court_cases && profile.court_cases.total > 0 && (
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Scale className="h-4 w-4 text-violet-500" />
                                        <span className="text-sm font-semibold text-[var(--text-main)]">Судебные дела</span>
                                        <span className="ml-auto text-xs text-[var(--text-muted)]">sud.gov.kz</span>
                                    </div>
                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                        <MetricCard label="Всего дел" value={profile.court_cases.total} tone="neutral" icon={<Scale className="h-3.5 w-3.5" />} />
                                        <MetricCard label="Как истец" value={profile.court_cases.as_plaintiff} tone="neutral" />
                                        <MetricCard
                                            label="Как ответчик"
                                            value={profile.court_cases.as_defendant}
                                            tone={profile.court_cases.as_defendant > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label="Влияние на надёжность"
                                            value={`${profile.court_cases.avg_reliability_impact}/10`}
                                            sub="LLM-оценка"
                                            tone={
                                                profile.court_cases.avg_reliability_impact >= 7 ? "danger" :
                                                profile.court_cases.avg_reliability_impact >= 4 ? "warn" : "ok"
                                            }
                                            icon={<Bot className="h-3.5 w-3.5" />}
                                        />
                                    </div>
                                    {/* LLM analyses of individual cases */}
                                    {profile.court_cases.llm_analyses.length > 0 && (
                                        <div className="space-y-2 mt-2">
                                            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">LLM-анализ решений</div>
                                            {profile.court_cases.llm_analyses.map((a, i) => (
                                                <div key={i} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 space-y-1">
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        <span className="font-mono text-xs text-[var(--text-main)]">{a.case_number || `Дело #${i + 1}`}</span>
                                                        <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                                            a.role === "ответчик" ? "bg-rose-500/10 text-rose-600 dark:text-rose-400" : "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                                                        }`}>
                                                            {a.role}
                                                        </span>
                                                        {a.dispute_type && (
                                                            <span className="rounded-full bg-[var(--border)] px-2 py-0.5 text-[10px] text-[var(--text-muted)]">{a.dispute_type}</span>
                                                        )}
                                                        {a.outcome && (
                                                            <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                                                a.outcome === "проиграл" ? "bg-rose-500/10 text-rose-500" :
                                                                a.outcome === "выиграл" ? "bg-emerald-500/10 text-emerald-500" : "bg-amber-500/10 text-amber-500"
                                                            }`}>
                                                                {a.outcome}
                                                            </span>
                                                        )}
                                                        <span className={`ml-auto text-[10px] font-bold ${
                                                            a.reliability_impact >= 7 ? "text-rose-500" :
                                                            a.reliability_impact >= 4 ? "text-amber-500" : "text-emerald-500"
                                                        }`}>
                                                            {a.reliability_impact}/10
                                                        </span>
                                                    </div>
                                                    {a.amount && <div className="text-xs text-[var(--text-muted)]">Сумма: {fmt(a.amount)}</div>}
                                                    <p className="text-xs text-[var(--text-main)] leading-relaxed">{a.summary}</p>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* ── Affiliations ── */}
                            {profile.affiliations && profile.affiliations.total_links > 0 && (
                                <div className="rounded-2xl border border-amber-500/20 bg-amber-500/5 p-4 space-y-3">
                                    <div className="flex items-center gap-2">
                                        <Link2 className="h-4 w-4 text-amber-500" />
                                        <span className="text-sm font-semibold text-[var(--text-main)]">Связанные компании</span>
                                        <span className="ml-auto rounded-full bg-amber-500/10 border border-amber-500/20 px-2 py-0.5 text-[10px] font-bold text-amber-600 dark:text-amber-400">
                                            {profile.affiliations.total_links} связей
                                        </span>
                                    </div>

                                    {profile.affiliations.shared_bank_accounts.length > 0 && (
                                        <div className="space-y-1.5">
                                            <div className="text-[11px] font-semibold text-amber-600 dark:text-amber-400 uppercase tracking-wider flex items-center gap-1">
                                                <Banknote className="h-3 w-3" /> Общие банковские счета
                                            </div>
                                            {profile.affiliations.shared_bank_accounts.map((a, i) => (
                                                <div key={i} className="flex items-center gap-2 text-xs rounded-lg bg-[var(--surface)] border border-[var(--border)] px-3 py-2">
                                                    <span className="font-mono text-[var(--text-main)]">{a.bin}</span>
                                                    <span className="text-[var(--text-muted)] truncate">{a.name}</span>
                                                </div>
                                            ))}
                                        </div>
                                    )}

                                    {profile.affiliations.shared_contacts.length > 0 && (
                                        <div className="space-y-1.5">
                                            <div className="text-[11px] font-semibold text-amber-600 dark:text-amber-400 uppercase tracking-wider flex items-center gap-1">
                                                <Users className="h-3 w-3" /> Общие контакты
                                            </div>
                                            {profile.affiliations.shared_contacts.map((a, i) => (
                                                <div key={i} className="flex items-center gap-2 text-xs rounded-lg bg-[var(--surface)] border border-[var(--border)] px-3 py-2">
                                                    <span className="font-mono text-[var(--text-main)]">{a.bin}</span>
                                                    <span className="text-[var(--text-muted)] truncate">{a.name}</span>
                                                    {a.match_type && <span className="ml-auto rounded bg-[var(--border)] px-1.5 py-0.5 text-[9px] text-[var(--text-muted)]">{a.match_type}</span>}
                                                </div>
                                            ))}
                                        </div>
                                    )}

                                    {profile.affiliations.cobid_partners.length > 0 && (
                                        <div className="space-y-1.5">
                                            <div className="text-[11px] font-semibold text-amber-600 dark:text-amber-400 uppercase tracking-wider flex items-center gap-1">
                                                <Link2 className="h-3 w-3" /> Co-bidding партнёры
                                            </div>
                                            {profile.affiliations.cobid_partners.map((a, i) => (
                                                <div key={i} className="flex items-center gap-2 text-xs rounded-lg bg-[var(--surface)] border border-[var(--border)] px-3 py-2">
                                                    <span className="font-mono text-[var(--text-main)]">{a.bin}</span>
                                                    <span className="text-[var(--text-muted)] truncate">{a.name}</span>
                                                    <span className="ml-auto text-[10px] text-amber-600 dark:text-amber-400 font-semibold">{a.times_together}x вместе</span>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    )}

                    {/* ── Tab: Supplier ── */}
                    {activeTab === "supplier" && (
                        <div className="space-y-5">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard label="Всего контрактов" value={profile.as_supplier.metrics.total_contracts} tone="neutral" icon={<Package className="h-3.5 w-3.5" />} />
                                <MetricCard label="Общая сумма" value={fmt(profile.as_supplier.metrics.total_sum)} tone="neutral" icon={<ArrowUpRight className="h-3.5 w-3.5" />} />
                                <MetricCard
                                    label="Исполнение"
                                    value={`${profile.as_supplier.metrics.execution_rate}%`}
                                    sub={`факт: ${fmt(profile.as_supplier.metrics.executed_sum)}`}
                                    tone={profile.as_supplier.metrics.execution_rate >= 80 ? "ok" : profile.as_supplier.metrics.execution_rate >= 50 ? "warn" : "danger"}
                                    icon={<TrendingUp className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label="Уникальных заказчиков" value={profile.as_supplier.metrics.unique_customers} tone="neutral" icon={<Users className="h-3.5 w-3.5" />} />
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard
                                    label="Просрочки"
                                    value={profile.as_supplier.metrics.overdue_count}
                                    tone={profile.as_supplier.metrics.overdue_count > 0 ? "warn" : "ok"}
                                    icon={<Clock className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Штрафы"
                                    value={profile.as_supplier.metrics.fines_count}
                                    tone={profile.as_supplier.metrics.fines_count > 0 ? "danger" : "ok"}
                                    icon={<AlertTriangle className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label="Ср. сумма контракта" value={fmt(profile.as_supplier.metrics.avg_contract_size)} tone="neutral" />
                                <MetricCard label="Выплачено казначейством" value={fmt(profile.as_supplier.metrics.treasury_paid)} tone="neutral" />
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                    <YearChart byYear={profile.as_supplier.metrics.by_year} />
                                    <PartnerBar partners={profile.as_supplier.metrics.top_customers} label="Топ-5 заказчиков по сумме" />
                                </div>
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-3">Последние контракты</div>
                                    <ContractTable contracts={profile.as_supplier.contracts as unknown as Record<string, unknown>[]} role="supplier" />
                                </div>
                            </div>
                        </div>
                    )}

                    {/* ── Tab: Customer ── */}
                    {activeTab === "customer" && (
                        <div className="space-y-5">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard label="Всего тендеров" value={profile.as_customer.metrics.total_tenders} tone="neutral" icon={<FileText className="h-3.5 w-3.5" />} />
                                <MetricCard label="Всего контрактов" value={profile.as_customer.metrics.total_contracts} tone="neutral" icon={<Package className="h-3.5 w-3.5" />} />
                                <MetricCard label="Объём закупок" value={fmt(profile.as_customer.metrics.total_procurement_sum)} tone="neutral" icon={<TrendingDown className="h-3.5 w-3.5" />} />
                                <MetricCard label="Уникальных поставщиков" value={profile.as_customer.metrics.unique_suppliers} tone="neutral" icon={<Users className="h-3.5 w-3.5" />} />
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard
                                    label="Единственный источник"
                                    value={`${profile.as_customer.metrics.single_source_rate}%`}
                                    sub={`${profile.as_customer.metrics.single_source_count} тендеров`}
                                    tone={profile.as_customer.metrics.single_source_rate > 50 ? "warn" : profile.as_customer.metrics.single_source_rate > 80 ? "danger" : "ok"}
                                    icon={<ShieldAlert className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Отменённые тендеры"
                                    value={profile.as_customer.metrics.cancelled_tenders}
                                    tone={profile.as_customer.metrics.cancelled_tenders > 5 ? "warn" : "ok"}
                                    icon={<XCircle className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label="Статус РНУ"
                                    value={profile.rnu.is_blacklisted ? "В реестре" : "Чистый"}
                                    tone={profile.rnu.is_blacklisted ? "danger" : "ok"}
                                    icon={<ShieldCheck className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label="Ср. сумма контракта" value={
                                    profile.as_customer.metrics.total_contracts > 0
                                        ? fmt(profile.as_customer.metrics.total_procurement_sum / profile.as_customer.metrics.total_contracts)
                                        : "—"
                                } tone="neutral" />
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                    <YearChart byYear={profile.as_customer.metrics.by_year} />
                                    <PartnerBar partners={profile.as_customer.metrics.top_suppliers} label="Топ-5 поставщиков по сумме" />
                                </div>
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-3">Последние контракты</div>
                                    <ContractTable contracts={profile.as_customer.contracts as unknown as Record<string, unknown>[]} role="customer" />
                                </div>
                            </div>
                        </div>
                    )}

                    {/* ── LLM Analysis Block ── */}
                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
                        {/* Header row */}
                        <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border)]">
                            <div className="flex items-center gap-3">
                                <div className="h-8 w-8 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-sm">
                                    <Bot className="h-4 w-4 text-white" />
                                </div>
                                <div>
                                    <div className="text-sm font-semibold text-[var(--text-main)]">AI Анализ компании</div>
                                    <div className="text-[11px] text-[var(--text-muted)]">Экспертный нарратив на основе метрик OWS</div>
                                </div>
                            </div>
                            <button
                                onClick={runLlmAnalysis}
                                disabled={llmLoading}
                                className="flex items-center gap-2 rounded-xl bg-gradient-to-r from-indigo-500 to-violet-600 px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50 transition-all shadow-sm shadow-indigo-500/20"
                            >
                                {llmLoading
                                    ? <><Loader2 className="h-4 w-4 animate-spin" /> Анализирую...</>
                                    : <><Sparkles className="h-4 w-4" /> {llmNarrative ? "Обновить" : "Запустить анализ"}</>
                                }
                            </button>
                        </div>

                        {/* Content */}
                        {!llmNarrative && !llmLoading && !llmError && (
                            <div className="flex flex-col items-center justify-center py-10 gap-3 text-[var(--text-muted)]">
                                <Sparkles className="h-8 w-8 text-indigo-500/40" />
                                <div className="text-center">
                                    <p className="text-sm text-[var(--text-main)]">AI-анализ ещё не запущен</p>
                                    <p className="text-xs mt-0.5">Нажми «Запустить анализ» — GPT изучит метрики и выдаст экспертный вывод</p>
                                </div>
                            </div>
                        )}

                        {llmLoading && (
                            <div className="flex flex-col items-center justify-center py-10 gap-3 text-[var(--text-muted)]">
                                <Loader2 className="h-7 w-7 text-indigo-500 animate-spin" />
                                <p className="text-sm">Отправляю метрики в GPT и жду ответа...</p>
                            </div>
                        )}

                        {llmError && (
                            <div className="flex items-start gap-3 m-5 rounded-xl border border-rose-500/20 bg-rose-500/8 p-4">
                                <XCircle className="h-4 w-4 text-rose-500 shrink-0 mt-0.5" />
                                <div>
                                    <p className="text-sm font-medium text-rose-600 dark:text-rose-400">Ошибка LLM</p>
                                    <p className="text-xs text-[var(--text-muted)] mt-0.5">{llmError}</p>
                                </div>
                            </div>
                        )}

                        {llmNarrative && !llmLoading && (
                            <div className="p-5">
                                {/* Render markdown-style bold and bullet lists */}
                                <div className="prose-sm text-[var(--text-main)] space-y-3">
                                    {llmNarrative.split("\n").map((line, i) => {
                                        // Section headers: **text**
                                        const headerMatch = line.match(/^\*\*(.+)\*\*$/);
                                        if (headerMatch) {
                                            return (
                                                <p key={i} className="text-sm font-semibold text-[var(--text-main)] mt-4 first:mt-0 flex items-center gap-2">
                                                    <span className="h-1 w-4 rounded-full bg-indigo-500 inline-block" />
                                                    {headerMatch[1]}
                                                </p>
                                            );
                                        }
                                        // Numbered section: "1. **text**" or "1. text"
                                        const numberedMatch = line.match(/^(\d+)\.\s+\*\*(.+)\*\*(.*)$/);
                                        if (numberedMatch) {
                                            return (
                                                <p key={i} className="text-sm font-semibold text-[var(--text-main)] mt-4 first:mt-0 flex items-center gap-2">
                                                    <span className="h-5 w-5 rounded-full bg-indigo-500/15 border border-indigo-500/30 text-indigo-500 text-[10px] font-bold flex items-center justify-center shrink-0">
                                                        {numberedMatch[1]}
                                                    </span>
                                                    {numberedMatch[2]}{numberedMatch[3]}
                                                </p>
                                            );
                                        }
                                        // Bullet: "- text" or "• text"
                                        if (line.match(/^[-•]\s+/)) {
                                            const text = line.replace(/^[-•]\s+/, "");
                                            // Inline bold: **word**
                                            const parts = text.split(/\*\*(.+?)\*\*/);
                                            return (
                                                <div key={i} className="flex items-start gap-2 ml-2">
                                                    <span className="h-1.5 w-1.5 rounded-full bg-indigo-500 mt-1.5 shrink-0" />
                                                    <p className="text-sm text-[var(--text-main)] leading-relaxed">
                                                        {parts.map((p, j) =>
                                                            j % 2 === 1
                                                                ? <strong key={j} className="font-semibold">{p}</strong>
                                                                : p
                                                        )}
                                                    </p>
                                                </div>
                                            );
                                        }
                                        // Empty line
                                        if (!line.trim()) return <div key={i} className="h-1" />;
                                        // Inline bold in regular text
                                        const parts = line.split(/\*\*(.+?)\*\*/);
                                        return (
                                            <p key={i} className="text-sm text-[var(--text-main)] leading-relaxed">
                                                {parts.map((p, j) =>
                                                    j % 2 === 1
                                                        ? <strong key={j} className="font-semibold">{p}</strong>
                                                        : p
                                                )}
                                            </p>
                                        );
                                    })}
                                </div>
                                <div className="mt-4 pt-3 border-t border-[var(--border)] flex items-center gap-2 text-[10px] text-[var(--text-muted)]">
                                    <Bot className="h-3 w-3" />
                                    На основе данных OWS · Только для справки, не является юридическим заключением
                                </div>
                            </div>
                        )}
                    </div>

                    {/* ── AI Chat Panel ── */}
                    {showChat && (
                        <div className="rounded-2xl border border-indigo-500/20 bg-[var(--surface)] overflow-hidden">
                            <div className="flex items-center gap-3 px-4 py-3 border-b border-[var(--border)] bg-indigo-500/5">
                                <div className="h-7 w-7 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-sm">
                                    <Bot className="h-3.5 w-3.5 text-white" />
                                </div>
                                <div>
                                    <div className="text-sm font-semibold text-[var(--text-main)]">AI Ассистент</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">
                                        Знает контекст этой компании · Задавай любые вопросы
                                    </div>
                                </div>
                                <button
                                    onClick={() => setShowChat(false)}
                                    className="ml-auto text-[var(--text-muted)] hover:text-[var(--text-main)] transition-colors"
                                >
                                    <XCircle className="h-4 w-4" />
                                </button>
                            </div>

                            {/* Messages */}
                            <div className="h-64 overflow-y-auto p-4 space-y-3">
                                {chatMessages.length === 0 && (
                                    <div className="flex flex-col items-center justify-center h-full gap-2 text-[var(--text-muted)]">
                                        <MessageSquare className="h-7 w-7 text-indigo-500/40" />
                                        <p className="text-sm text-center">
                                            Спроси что-нибудь о компании<br />
                                            <span className="text-xs">Например: «Есть ли признаки аффилированности?»</span>
                                        </p>
                                    </div>
                                )}
                                {chatMessages.map((m, i) => (
                                    <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                                        <div className={`max-w-[80%] rounded-2xl px-3 py-2 text-sm ${
                                            m.role === "user"
                                                ? "bg-indigo-500 text-white rounded-br-sm"
                                                : "bg-[var(--surface-hover)] text-[var(--text-main)] border border-[var(--border)] rounded-bl-sm"
                                        }`}>
                                            {m.text}
                                        </div>
                                    </div>
                                ))}
                                {chatLoading && (
                                    <div className="flex justify-start">
                                        <div className="bg-[var(--surface-hover)] border border-[var(--border)] rounded-2xl rounded-bl-sm px-3 py-2">
                                            <Loader2 className="h-4 w-4 text-indigo-500 animate-spin" />
                                        </div>
                                    </div>
                                )}
                                <div ref={chatBottomRef} />
                            </div>

                            {/* Input */}
                            <div className="border-t border-[var(--border)] p-3 flex gap-2">
                                <input
                                    type="text"
                                    value={chatInput}
                                    onChange={(e) => setChatInput(e.target.value)}
                                    onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && sendChatMessage()}
                                    placeholder="Задай вопрос об этой компании..."
                                    className="flex-1 rounded-xl border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-indigo-500 transition-colors"
                                />
                                <button
                                    onClick={sendChatMessage}
                                    disabled={!chatInput.trim() || chatLoading}
                                    className="rounded-xl bg-indigo-500 hover:bg-indigo-400 disabled:opacity-40 px-3 py-2 text-white transition-colors"
                                >
                                    <Send className="h-4 w-4" />
                                </button>
                            </div>
                        </div>
                    )}

                    {/* Footer */}
                    <div className="flex items-center gap-2 text-[10px] text-[var(--text-muted)] pt-2 border-t border-[var(--border)]">
                        <CheckCircle2 className="h-3 w-3 text-emerald-500" />
                        Данные загружены {new Date(profile.fetched_at).toLocaleString("ru-RU")} · Источник: OWS API v3 goszakup.gov.kz
                    </div>
                </div>
            )}
        </div>
    );
}
