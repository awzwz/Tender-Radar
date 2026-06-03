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
import { useI18n } from "@/components/providers/LanguageProvider";

type TFn = (key: string, params?: Record<string, string | number>) => string;

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

// Flag CODES are stable identifiers; their human-readable labels are translated.
const FLAG_CODES = [
    "BLACKLISTED",
    "LOW_EXECUTION_RATE",
    "OVERDUE_ACTS",
    "FINES_PRESENT",
    "HIGH_CUSTOMER_CONCENTRATION",
    "HIGH_SINGLE_SOURCE_RATE",
    "MANY_CANCELLED_TENDERS",
    "HIGH_SUPPLIER_CONCENTRATION",
    "AVG_OVERDUE_DAYS",
    "VOLUME_SPIKE",
    "YOUNG_COMPANY_BIG_VOLUME",
    "DIVERSE_OKED",
    "HIGH_ADDENDUM_RATE",
    "COMPLAINTS_ON_PURCHASES",
    "HIGH_COMPLAINT_SATISFACTION_RATE",
    "SATISFIED_COMPLAINTS_RISK",
    "SHARED_BANK_ACCOUNT",
    "SHARED_CONTACTS",
    "FREQUENT_COBIDDERS",
    "TAX_ANOMALY",
    "COURT_CASES_RISK",
    "MANY_COURT_CASES",
];

function flagLabel(code: string, t: TFn): string {
    if (FLAG_CODES.includes(code)) return t(`company.flag.${code}`);
    return code;
}

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
    t,
}: {
    partners: { bin: string; count: number; sum: number }[];
    label: string;
    t: TFn;
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
                        <span className="text-[var(--text-muted)] ml-2 shrink-0">{t("company.partner.contractsAbbr", { n: p.count })} · {fmt(p.sum)}</span>
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

function YearChart({ byYear, t }: { byYear: Record<string, { count: number; sum: number }>; t: TFn }) {
    const entries = Object.entries(byYear).filter(([k]) => k !== "unknown").sort();
    if (entries.length === 0) return null;
    const maxSum = Math.max(...entries.map(([, v]) => v.sum));

    return (
        <div className="space-y-2">
            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">{t("company.chart.byYear")}</div>
            <div className="flex items-end gap-1.5 h-20">
                {entries.map(([year, v]) => (
                    <div key={year} className="flex-1 flex flex-col items-center gap-1 group">
                        <div className="relative w-full">
                            <div
                                className="w-full rounded-t bg-indigo-500/70 hover:bg-indigo-500 transition-all"
                                style={{ height: `${Math.max((v.sum / maxSum) * 64, 4)}px` }}
                                title={`${year}: ${t("company.chart.contractsAbbr", { n: v.count })} · ${fmt(v.sum)}`}
                            />
                        </div>
                        <span className="text-[9px] text-[var(--text-muted)]">{year}</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

function ContractTable({ contracts, role, t }: { contracts: Record<string, unknown>[]; role: "supplier" | "customer"; t: TFn }) {
    const [expanded, setExpanded] = useState(false);
    const shown = expanded ? contracts : contracts.slice(0, 5);
    if (contracts.length === 0) return <p className="text-xs text-[var(--text-muted)]">{t("company.common.noData")}</p>;
    return (
        <div>
            <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                <table className="min-w-full text-xs">
                    <thead>
                        <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)]">
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.contracts.col.number")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">
                                {role === "supplier" ? t("company.contracts.col.customerBin") : t("company.contracts.col.supplierBin")}
                            </th>
                            <th className="px-3 py-2 text-right text-[var(--text-muted)] font-medium">{t("company.contracts.col.sum")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.contracts.col.date")}</th>
                            <th className="px-3 py-2 text-center text-[var(--text-muted)] font-medium">{t("company.contracts.col.status")}</th>
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
                    {expanded ? t("company.common.collapse") : t("company.contracts.showAll", { n: contracts.length })}
                    <ChevronRight className={`h-3.5 w-3.5 transition-transform ${expanded ? "rotate-90" : ""}`} />
                </button>
            )}
        </div>
    );
}

function ComplaintsTable({ complaints, t }: { complaints: Record<string, unknown>[]; t: TFn }) {
    const [expanded, setExpanded] = useState(false);
    const shown = expanded ? complaints : complaints.slice(0, 5);
    if (complaints.length === 0) return null;
    return (
        <div>
            <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                <table className="min-w-full text-xs">
                    <thead>
                        <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)]">
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.complaints.col.number")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.complaints.col.dateSubmitted")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.complaints.col.status")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.complaints.col.tender")}</th>
                            <th className="px-3 py-2 text-left text-[var(--text-muted)] font-medium">{t("company.complaints.col.organizer")}</th>
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
                    {expanded ? t("company.common.collapse") : t("company.complaints.showAll", { n: complaints.length })}
                    <ChevronRight className={`h-3.5 w-3.5 transition-transform ${expanded ? "rotate-90" : ""}`} />
                </button>
            )}
        </div>
    );
}

// ── Main Component ─────────────────────────────────────────────────────────────

export default function CompanyProfileView() {
    const { t } = useI18n();
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
            const msg = e instanceof Error ? e.message : t("company.err.load");
            setProfileError(msg);
        } finally {
            setProfileLoading(false);
        }
    }, [t]);

    const runLlmAnalysis = useCallback(async () => {
        if (!profile) return;
        setLlmLoading(true);
        setLlmError(null);
        setLlmNarrative(null);
        try {
            const res = await api.companyAnalyze(profile.bin, profile);
            setLlmNarrative(res.narrative);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : t("company.err.llm");
            setLlmError(msg);
        } finally {
            setLlmLoading(false);
        }
    }, [profile, t]);

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
                setProfileError(t("company.err.notFound", { q }));
                setProfileLoading(false);
                return;
            }
            const s = res.results[0];
            const bin = s.bin || s.iin;
            if (!bin) {
                setProfileError(t("company.err.noBin"));
                setProfileLoading(false);
                return;
            }
            const name = s.nameRu || s.fullNameRu || s.nameKz || bin;
            setProfileDisplayName(name);
            setQuery(name);
            api.companyProfile(bin).then((data) => {
                setProfile(data);
            }).catch((e: unknown) => {
                setProfileError(e instanceof Error ? e.message : t("company.err.loadProfile"));
            }).finally(() => setProfileLoading(false));
        }).catch((e: unknown) => {
            setProfileError(e instanceof Error ? e.message : t("company.err.search"));
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
        const name = profile.subject?.nameRu || profile.subject?.fullNameRu || profile.subject?.nameKz || profileDisplayName || `${t("company.bin")}: ${profile.bin}`;
        const sup = profile.as_supplier.metrics;
        const cust = profile.as_customer.metrics;
        const date = new Date(profile.fetched_at).toLocaleString("ru-RU");
        const flagLines = profile.risk.flags.map((f) => flagLabel(f, t));
        return { name, sup, cust, date, flagLines };
    }

    // Export as Markdown
    function handleExportMd() {
        const d = buildReportData();
        if (!d || !profile) return;
        const { name, sup, cust, date, flagLines } = d;
        const lines = [
            `# ${t("company.report.title")}: ${name}`,
            `**${t("company.bin")}:** ${profile.bin}`,
            `**${t("company.report.analysisDate")}:** ${date}`,
            `**${t("company.report.rnuStatus")}:** ${profile.rnu.is_blacklisted ? t("company.report.rnuListedCaps") : t("company.report.rnuClean")}`,
            `**${t("company.report.riskLevel")}:** ${profile.risk.level} (score: ${profile.risk.score}/100)`,
            ``,
            `## ${t("company.report.riskFlags")}`,
            ...flagLines.map((f) => `- ${f}`),
            ``,
            `## ${t("company.section.asSupplier")}`,
            `- ${t("company.metric.totalContracts")}: ${sup.total_contracts}`,
            `- ${t("company.metric.totalSum")}: ${fmt(sup.total_sum)}`,
            `- ${t("company.metric.execution")}: ${sup.execution_rate}% (${t("company.metric.actual")}: ${fmt(sup.executed_sum)})`,
            `- ${t("company.metric.uniqueCustomers")}: ${sup.unique_customers}`,
            `- ${t("company.report.overdueActs")}: ${sup.overdue_count}`,
            `- ${t("company.metric.fines")}: ${sup.fines_count}`,
            `- ${t("company.report.avgContract")}: ${fmt(sup.avg_contract_size)}`,
            `- ${t("company.metric.treasuryPaid")}: ${fmt(sup.treasury_paid)}`,
            ``,
            `## ${t("company.section.asCustomer")}`,
            `- ${t("company.metric.totalTenders")}: ${cust.total_tenders}`,
            `- ${t("company.metric.totalContracts")}: ${cust.total_contracts}`,
            `- ${t("company.metric.procurementVolume")}: ${fmt(cust.total_procurement_sum)}`,
            `- ${t("company.metric.uniqueSuppliers")}: ${cust.unique_suppliers}`,
            `- ${t("company.report.singleSourceShare")}: ${cust.single_source_rate}% (${t("company.report.tendersCount", { n: cust.single_source_count })})`,
            `- ${t("company.metric.cancelledTenders")}: ${cust.cancelled_tenders}`,
            ``,
        ];
        if (llmNarrative) lines.push(`## ${t("company.ai.title")}`, ``, llmNarrative, ``);
        lines.push(`---`, `${t("company.report.source")}: OWS API v3 goszakup.gov.kz · Tender Radar`);
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
            : `<span class='flag-ok'>${t("company.report.noRiskFlags")}</span>`;
        const narrativeHtml = llmNarrative
            ? `<h2>${t("company.ai.title")}</h2><div class="narrative">${llmNarrative.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>").replace(/\n/g, "<br/>")}</div>`
            : "";
        const html = `<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"/>
<title>${t("company.report.titleShort")}: ${name}</title>
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
<div class="meta">${t("company.bin")}: <strong>${profile.bin}</strong> &nbsp;·&nbsp; ${t("company.report.analysisDate")}: ${date} &nbsp;·&nbsp; ${t("company.report.source")}: OWS API v3</div>
<div style="margin-bottom:16px;">
  <span class="risk-badge">${t("company.report.risk")}: ${profile.risk.level} · ${profile.risk.score}/100</span>
  &nbsp; <span style="font-size:12px;color:${profile.rnu.is_blacklisted ? "#dc2626" : "#059669"}">${profile.rnu.is_blacklisted ? "⛔ " + t("company.report.rnuListedInline") : "✓ " + t("company.report.rnuNotListed")}</span>
</div>
<h2>${t("company.report.riskFlags")}</h2>
<div>${flagsHtml}</div>
<h2>${t("company.section.asSupplier")}</h2>
<table>
  <tr><th>${t("company.report.col.indicator")}</th><th>${t("company.report.col.value")}</th></tr>
  <tr><td>${t("company.metric.totalContracts")}</td><td>${sup.total_contracts}</td></tr>
  <tr><td>${t("company.metric.totalSum")}</td><td>${fmt(sup.total_sum)}</td></tr>
  <tr><td>${t("company.metric.execution")}</td><td>${sup.execution_rate}%</td></tr>
  <tr><td>${t("company.metric.uniqueCustomers")}</td><td>${sup.unique_customers}</td></tr>
  <tr><td>${t("company.report.overdueActs")}</td><td>${sup.overdue_count}</td></tr>
  <tr><td>${t("company.metric.fines")}</td><td>${sup.fines_count}</td></tr>
  <tr><td>${t("company.report.avgContract")}</td><td>${fmt(sup.avg_contract_size)}</td></tr>
  <tr><td>${t("company.metric.treasuryPaid")}</td><td>${fmt(sup.treasury_paid)}</td></tr>
</table>
<h2>${t("company.section.asCustomer")}</h2>
<table>
  <tr><th>${t("company.report.col.indicator")}</th><th>${t("company.report.col.value")}</th></tr>
  <tr><td>${t("company.metric.totalTenders")}</td><td>${cust.total_tenders}</td></tr>
  <tr><td>${t("company.metric.totalContracts")}</td><td>${cust.total_contracts}</td></tr>
  <tr><td>${t("company.metric.procurementVolume")}</td><td>${fmt(cust.total_procurement_sum)}</td></tr>
  <tr><td>${t("company.metric.uniqueSuppliers")}</td><td>${cust.unique_suppliers}</td></tr>
  <tr><td>${t("company.report.singleSourceShare")}</td><td>${cust.single_source_rate}%</td></tr>
  <tr><td>${t("company.metric.cancelledTenders")}</td><td>${cust.cancelled_tenders}</td></tr>
</table>
${narrativeHtml}
<div class="footer">Tender Radar · OWS API v3 goszakup.gov.kz · ${t("company.report.disclaimer")}</div>
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
            `Флаги: ${profile.risk.flags.map(f => flagLabel(f, t)).join(", ") || "нет"}`,
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
            const base = process.env.NEXT_PUBLIC_API_URL || `http://${window.location.hostname}:8000/api/v1`;
            const backendUrl = `${base}/chat/`;
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
                const errMsg = data.detail || data.error || t("company.chat.err.server", { status: res.status });
                setChatMessages([...newMessages, { role: "assistant", text: `⚠️ ${errMsg}` }]);
            } else {
                setChatMessages([...newMessages, { role: "assistant", text: data.text || t("company.chat.err.empty") }]);
            }
        } catch (err) {
            const msg = err instanceof Error ? err.message : t("company.chat.err.connection");
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
                    <h2 className="text-base font-bold text-[var(--text-main)]">{t("company.header.title")}</h2>
                    <p className="text-xs text-[var(--text-muted)]">{t("company.header.subtitle")}</p>
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
                                placeholder={t("company.search.placeholder")}
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
                            {t("company.search.submit")}
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
                                    <div className="text-sm font-medium text-[var(--text-main)] truncate">{s.nameRu || t("company.common.untitled")}</div>
                                    <div className="text-xs text-[var(--text-muted)] flex items-center gap-2 mt-0.5">
                                        <span className="font-mono">{s.bin || s.iin || "—"}</span>
                                        {s.supplier && <span className="rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">{t("company.badge.supplier")}</span>}
                                        {s.customer && <span className="rounded bg-indigo-500/10 border border-indigo-500/20 text-indigo-600 dark:text-indigo-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">{t("company.badge.customer")}</span>}
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
                        <p className="text-sm font-medium text-[var(--text-main)]">{t("company.empty.title")}</p>
                        <p className="text-xs text-[var(--text-muted)]">{t("company.empty.subtitle")}</p>
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
                        <p className="text-sm font-medium text-[var(--text-main)]">{t("company.loading.title")}</p>
                        <p className="text-xs text-[var(--text-muted)]">{t("company.loading.sources")}</p>
                        <p className="text-[10px] text-[var(--text-muted)] mt-1">{t("company.loading.hint")}</p>
                    </div>
                </div>
            )}

            {/* ── Error ── */}
            {profileError && (
                <div className="flex items-start gap-3 rounded-2xl border border-rose-500/20 bg-rose-500/8 p-4">
                    <XCircle className="h-5 w-5 text-rose-500 shrink-0 mt-0.5" />
                    <div>
                        <p className="text-sm font-medium text-rose-600 dark:text-rose-400">{t("company.err.load")}</p>
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
                                        {profile.subject?.nameRu || profile.subject?.fullNameRu || profile.subject?.nameKz || profileDisplayName || `${t("company.bin")}: ${profile.bin}`}
                                    </h3>
                                    {profile.rnu.is_blacklisted && (
                                        <span className="rounded-full bg-rose-500/10 border border-rose-500/30 px-2.5 py-1 text-[10px] font-bold text-rose-600 dark:text-rose-400 uppercase tracking-wider flex items-center gap-1">
                                            <AlertTriangle className="h-3 w-3" /> {t("company.badge.unreliableSupplier")}
                                        </span>
                                    )}
                                </div>
                                <div className="mt-1.5 flex items-center gap-3 flex-wrap text-xs text-[var(--text-muted)]">
                                    <span className="font-mono">{profile.bin}</span>
                                    {profile.subject?.regdate && (
                                        <span>{t("company.label.reg")}: {profile.subject.regdate.slice(0, 10)}</span>
                                    )}
                                    {isSupplier && (
                                        <span className="rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">{t("company.badge.supplier")}</span>
                                    )}
                                    {isCustomer && (
                                        <span className="rounded bg-indigo-500/10 border border-indigo-500/20 text-indigo-600 dark:text-indigo-400 px-1.5 py-0.5 text-[9px] font-semibold uppercase">{t("company.badge.customer")}</span>
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
                                {t("company.export.pdf")}
                            </button>
                            <button
                                onClick={() => { setShowChat(!showChat); if (!showChat && chatMessages.length === 0) setChatMessages([]); }}
                                className={`flex items-center gap-1.5 rounded-xl border px-3 py-1.5 text-xs font-medium transition-colors ${showChat ? "border-indigo-500/40 bg-indigo-500/10 text-indigo-500" : "border-[var(--border)] bg-[var(--surface-hover)] text-[var(--text-muted)] hover:text-[var(--text-main)]"}`}
                            >
                                <MessageSquare className="h-3.5 w-3.5" />
                                {t("company.ai.assistant")}
                                <ChevronDown className={`h-3 w-3 transition-transform ${showChat ? "rotate-180" : ""}`} />
                            </button>
                        </div>

                    {/* Risk flags */}
                        {profile.risk.flags.length > 0 && (
                            <div className="mt-4 pt-4 border-t border-[var(--border)] flex flex-wrap gap-2">
                                {profile.risk.flags.map((flag) => (
                                    <span key={flag} className="inline-flex items-center gap-1 rounded-xl bg-[var(--surface-hover)] border border-[var(--border)] px-2.5 py-1 text-[11px] text-[var(--text-muted)]">
                                        <AlertTriangle className="h-3 w-3 text-amber-500" />
                                        {flagLabel(flag, t)}
                                    </span>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Tab Navigation */}
                    <div className="flex gap-1 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-1">
                        {(["overview", "supplier", "customer"] as const).map((tab) => {
                            const labels = { overview: t("company.tab.overview"), supplier: t("company.section.asSupplier"), customer: t("company.section.asCustomer") };
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
                                    label={t("company.kpi.contractsSupplier")}
                                    value={profile.as_supplier.metrics.total_contracts}
                                    sub={profile.as_supplier.metrics.total_contracts > 0 ? fmt(profile.as_supplier.metrics.total_sum) : t("company.common.noData")}
                                    tone={profile.as_supplier.metrics.total_contracts > 0 ? "neutral" : "neutral"}
                                    icon={<Package className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.metric.execution")}
                                    value={`${profile.as_supplier.metrics.execution_rate}%`}
                                    sub={`${t("company.metric.actual")}: ${fmt(profile.as_supplier.metrics.executed_sum)}`}
                                    tone={
                                        profile.as_supplier.metrics.total_contracts === 0 ? "neutral" :
                                            profile.as_supplier.metrics.execution_rate >= 80 ? "ok" :
                                                profile.as_supplier.metrics.execution_rate >= 50 ? "warn" : "danger"
                                    }
                                    icon={<TrendingUp className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.kpi.tendersCustomer")}
                                    value={profile.as_customer.metrics.total_tenders}
                                    sub={`${t("company.kpi.contractsLabel")}: ${profile.as_customer.metrics.total_contracts}`}
                                    tone="neutral"
                                    icon={<FileText className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.kpi.rnuStatus")}
                                    value={profile.rnu.is_blacklisted ? t("company.rnu.listed") : t("company.rnu.clean")}
                                    sub={profile.rnu.is_blacklisted ? t("company.rnu.activeRecords", { n: profile.rnu.active_count }) : t("company.rnu.notListed")}
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
                                            {t("company.chart.supplierByYear")}
                                        </div>
                                        <YearChart byYear={profile.as_supplier.metrics.by_year} t={t} />
                                        <PartnerBar partners={profile.as_supplier.metrics.top_customers} label={t("company.partner.topCustomers")} t={t} />
                                    </div>
                                )}

                                {/* Customer chart */}
                                {Object.keys(profile.as_customer.metrics.by_year).length > 0 && (
                                    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                        <div className="flex items-center gap-2 text-sm font-semibold text-[var(--text-main)]">
                                            <BarChart3 className="h-4 w-4 text-violet-500" />
                                            {t("company.chart.customerByYear")}
                                        </div>
                                        <YearChart byYear={profile.as_customer.metrics.by_year} t={t} />
                                        <PartnerBar partners={profile.as_customer.metrics.top_suppliers} label={t("company.partner.topSuppliers")} t={t} />
                                    </div>
                                )}
                            </div>

                            {/* RNU detail */}
                            {profile.rnu.is_blacklisted && (
                                <div className="rounded-2xl border border-rose-500/20 bg-rose-500/5 p-4">
                                    <div className="flex items-center gap-2 mb-3">
                                        <ShieldAlert className="h-4 w-4 text-rose-500" />
                                        <span className="text-sm font-semibold text-rose-600 dark:text-rose-400">{t("company.rnu.registryTitle")}</span>
                                    </div>
                                    <div className="space-y-2">
                                        {profile.rnu.records.slice(0, 3).map((r: Record<string, unknown>, i: number) => (
                                            <div key={i} className="text-xs text-[var(--text-muted)] flex gap-2">
                                                <span className="font-mono">{String(r.startDate || "").slice(0, 10)}</span>
                                                <span>→</span>
                                                <span className="font-mono">{r.endDate ? String(r.endDate).slice(0, 10) : t("company.rnu.indefinite")}</span>
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
                                        <span className="text-sm font-semibold text-[var(--text-main)]">{t("company.tax.title")}</span>
                                        <span className="ml-auto rounded-full bg-indigo-500/10 border border-indigo-500/20 px-2 py-0.5 text-[9px] font-semibold text-indigo-500 uppercase tracking-wider">BA.PRG.KZ</span>
                                    </div>

                                    {/* Total KPI */}
                                    <MetricCard
                                        label={t("company.tax.totalLabel")}
                                        value={fmt(profile.kgd.total_tax_paid)}
                                        tone="neutral"
                                        icon={<Banknote className="h-3.5 w-3.5" />}
                                    />

                                    {/* Tax payments by year table */}
                                    <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                                        <table className="w-full text-xs">
                                            <thead>
                                                <tr className="bg-[var(--surface-alt)]">
                                                    <th className="px-3 py-2 text-left font-medium text-[var(--text-muted)]">{t("company.tax.col.year")}</th>
                                                    <th className="px-3 py-2 text-right font-medium text-[var(--text-muted)]">{t("company.tax.col.amount")}</th>
                                                    <th className="px-3 py-2 text-right font-medium text-[var(--text-muted)]">{t("company.tax.col.change")}</th>
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
                                                <span className="text-xs font-semibold text-indigo-500">{t("company.tax.aiTitle")}</span>
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
                                        <span className="text-sm font-semibold text-[var(--text-main)]">{t("company.complaints.title")}</span>
                                        <span className="ml-auto text-xs text-[var(--text-muted)]">goszakup.kz</span>
                                    </div>
                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                        <MetricCard label={t("company.complaints.total")} value={profile.complaints.total} tone="neutral" icon={<FileText className="h-3.5 w-3.5" />} />
                                        <MetricCard
                                            label={t("company.section.asSupplier")}
                                            value={profile.complaints.complaints_as_supplier}
                                            sub={t("company.complaints.subTarget")}
                                            tone={profile.complaints.complaints_as_supplier > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label={t("company.section.asCustomer")}
                                            value={profile.complaints.complaints_as_customer}
                                            sub={t("company.complaints.subOnPurchases")}
                                            tone={profile.complaints.complaints_as_customer > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label={t("company.complaints.satisfied")}
                                            value={`${profile.complaints.satisfaction_rate}%`}
                                            sub={t("company.complaints.satisfiedOf", { a: profile.complaints.satisfied_count, b: profile.complaints.total })}
                                            tone={profile.complaints.satisfaction_rate > 50 ? "danger" : profile.complaints.satisfaction_rate > 25 ? "warn" : "ok"}
                                        />
                                    </div>
                                    {profile.complaints.complaints.length > 0 && (
                                        <ComplaintsTable complaints={profile.complaints.complaints} t={t} />
                                    )}
                                    {/* LLM analyses of satisfied complaints */}
                                    {profile.complaints.llm_analyses && profile.complaints.llm_analyses.length > 0 && (
                                        <div className="space-y-2 mt-2">
                                            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">{t("company.complaints.llmTitle")}</div>
                                            {profile.complaints.llm_analyses.map((a, i) => (
                                                <div key={i} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 space-y-1">
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        <span className="font-mono text-xs text-[var(--text-main)]">{t("company.complaints.itemLabel", { n: a.complaint_number || i + 1 })}</span>
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
                                        <span className="text-sm font-semibold text-[var(--text-main)]">{t("company.court.title")}</span>
                                        <span className="ml-auto text-xs text-[var(--text-muted)]">sud.gov.kz</span>
                                    </div>
                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                        <MetricCard label={t("company.court.total")} value={profile.court_cases.total} tone="neutral" icon={<Scale className="h-3.5 w-3.5" />} />
                                        <MetricCard label={t("company.court.asPlaintiff")} value={profile.court_cases.as_plaintiff} tone="neutral" />
                                        <MetricCard
                                            label={t("company.court.asDefendant")}
                                            value={profile.court_cases.as_defendant}
                                            tone={profile.court_cases.as_defendant > 3 ? "warn" : "neutral"}
                                        />
                                        <MetricCard
                                            label={t("company.court.reliabilityImpact")}
                                            value={`${profile.court_cases.avg_reliability_impact}/10`}
                                            sub={t("company.court.llmScore")}
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
                                            <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider">{t("company.court.llmTitle")}</div>
                                            {profile.court_cases.llm_analyses.map((a, i) => (
                                                <div key={i} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 space-y-1">
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        <span className="font-mono text-xs text-[var(--text-main)]">{a.case_number || t("company.court.caseLabel", { n: i + 1 })}</span>
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
                                                    {a.amount && <div className="text-xs text-[var(--text-muted)]">{t("company.court.amount")}: {fmt(a.amount)}</div>}
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
                                        <span className="text-sm font-semibold text-[var(--text-main)]">{t("company.affil.title")}</span>
                                        <span className="ml-auto rounded-full bg-amber-500/10 border border-amber-500/20 px-2 py-0.5 text-[10px] font-bold text-amber-600 dark:text-amber-400">
                                            {t("company.affil.linksCount", { n: profile.affiliations.total_links })}
                                        </span>
                                    </div>

                                    {profile.affiliations.shared_bank_accounts.length > 0 && (
                                        <div className="space-y-1.5">
                                            <div className="text-[11px] font-semibold text-amber-600 dark:text-amber-400 uppercase tracking-wider flex items-center gap-1">
                                                <Banknote className="h-3 w-3" /> {t("company.affil.sharedBankAccounts")}
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
                                                <Users className="h-3 w-3" /> {t("company.affil.sharedContacts")}
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
                                                <Link2 className="h-3 w-3" /> {t("company.affil.cobidders")}
                                            </div>
                                            {profile.affiliations.cobid_partners.map((a, i) => (
                                                <div key={i} className="flex items-center gap-2 text-xs rounded-lg bg-[var(--surface)] border border-[var(--border)] px-3 py-2">
                                                    <span className="font-mono text-[var(--text-main)]">{a.bin}</span>
                                                    <span className="text-[var(--text-muted)] truncate">{a.name}</span>
                                                    <span className="ml-auto text-[10px] text-amber-600 dark:text-amber-400 font-semibold">{t("company.affil.timesTogether", { n: a.times_together ?? 0 })}</span>
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
                                <MetricCard label={t("company.metric.totalContracts")} value={profile.as_supplier.metrics.total_contracts} tone="neutral" icon={<Package className="h-3.5 w-3.5" />} />
                                <MetricCard label={t("company.metric.totalSum")} value={fmt(profile.as_supplier.metrics.total_sum)} tone="neutral" icon={<ArrowUpRight className="h-3.5 w-3.5" />} />
                                <MetricCard
                                    label={t("company.metric.execution")}
                                    value={`${profile.as_supplier.metrics.execution_rate}%`}
                                    sub={`${t("company.metric.actual")}: ${fmt(profile.as_supplier.metrics.executed_sum)}`}
                                    tone={profile.as_supplier.metrics.execution_rate >= 80 ? "ok" : profile.as_supplier.metrics.execution_rate >= 50 ? "warn" : "danger"}
                                    icon={<TrendingUp className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label={t("company.metric.uniqueCustomers")} value={profile.as_supplier.metrics.unique_customers} tone="neutral" icon={<Users className="h-3.5 w-3.5" />} />
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard
                                    label={t("company.metric.overdue")}
                                    value={profile.as_supplier.metrics.overdue_count}
                                    tone={profile.as_supplier.metrics.overdue_count > 0 ? "warn" : "ok"}
                                    icon={<Clock className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.metric.fines")}
                                    value={profile.as_supplier.metrics.fines_count}
                                    tone={profile.as_supplier.metrics.fines_count > 0 ? "danger" : "ok"}
                                    icon={<AlertTriangle className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label={t("company.metric.avgContractSize")} value={fmt(profile.as_supplier.metrics.avg_contract_size)} tone="neutral" />
                                <MetricCard label={t("company.metric.treasuryPaid")} value={fmt(profile.as_supplier.metrics.treasury_paid)} tone="neutral" />
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                    <YearChart byYear={profile.as_supplier.metrics.by_year} t={t} />
                                    <PartnerBar partners={profile.as_supplier.metrics.top_customers} label={t("company.partner.top5Customers")} t={t} />
                                </div>
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-3">{t("company.contracts.recent")}</div>
                                    <ContractTable contracts={profile.as_supplier.contracts as unknown as Record<string, unknown>[]} role="supplier" t={t} />
                                </div>
                            </div>
                        </div>
                    )}

                    {/* ── Tab: Customer ── */}
                    {activeTab === "customer" && (
                        <div className="space-y-5">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard label={t("company.metric.totalTenders")} value={profile.as_customer.metrics.total_tenders} tone="neutral" icon={<FileText className="h-3.5 w-3.5" />} />
                                <MetricCard label={t("company.metric.totalContracts")} value={profile.as_customer.metrics.total_contracts} tone="neutral" icon={<Package className="h-3.5 w-3.5" />} />
                                <MetricCard label={t("company.metric.procurementVolume")} value={fmt(profile.as_customer.metrics.total_procurement_sum)} tone="neutral" icon={<TrendingDown className="h-3.5 w-3.5" />} />
                                <MetricCard label={t("company.metric.uniqueSuppliers")} value={profile.as_customer.metrics.unique_suppliers} tone="neutral" icon={<Users className="h-3.5 w-3.5" />} />
                            </div>
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                <MetricCard
                                    label={t("company.metric.singleSource")}
                                    value={`${profile.as_customer.metrics.single_source_rate}%`}
                                    sub={t("company.report.tendersCount", { n: profile.as_customer.metrics.single_source_count })}
                                    tone={profile.as_customer.metrics.single_source_rate > 50 ? "warn" : profile.as_customer.metrics.single_source_rate > 80 ? "danger" : "ok"}
                                    icon={<ShieldAlert className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.metric.cancelledTenders")}
                                    value={profile.as_customer.metrics.cancelled_tenders}
                                    tone={profile.as_customer.metrics.cancelled_tenders > 5 ? "warn" : "ok"}
                                    icon={<XCircle className="h-3.5 w-3.5" />}
                                />
                                <MetricCard
                                    label={t("company.kpi.rnuStatus")}
                                    value={profile.rnu.is_blacklisted ? t("company.rnu.listed") : t("company.rnu.clean")}
                                    tone={profile.rnu.is_blacklisted ? "danger" : "ok"}
                                    icon={<ShieldCheck className="h-3.5 w-3.5" />}
                                />
                                <MetricCard label={t("company.metric.avgContractSize")} value={
                                    profile.as_customer.metrics.total_contracts > 0
                                        ? fmt(profile.as_customer.metrics.total_procurement_sum / profile.as_customer.metrics.total_contracts)
                                        : "—"
                                } tone="neutral" />
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
                                    <YearChart byYear={profile.as_customer.metrics.by_year} t={t} />
                                    <PartnerBar partners={profile.as_customer.metrics.top_suppliers} label={t("company.partner.top5Suppliers")} t={t} />
                                </div>
                                <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-3">{t("company.contracts.recent")}</div>
                                    <ContractTable contracts={profile.as_customer.contracts as unknown as Record<string, unknown>[]} role="customer" t={t} />
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
                                    <div className="text-sm font-semibold text-[var(--text-main)]">{t("company.ai.companyTitle")}</div>
                                    <div className="text-[11px] text-[var(--text-muted)]">{t("company.ai.companySubtitle")}</div>
                                </div>
                            </div>
                            <button
                                onClick={runLlmAnalysis}
                                disabled={llmLoading}
                                className="flex items-center gap-2 rounded-xl bg-gradient-to-r from-indigo-500 to-violet-600 px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50 transition-all shadow-sm shadow-indigo-500/20"
                            >
                                {llmLoading
                                    ? <><Loader2 className="h-4 w-4 animate-spin" /> {t("company.ai.analyzing")}</>
                                    : <><Sparkles className="h-4 w-4" /> {llmNarrative ? t("company.ai.refresh") : t("company.ai.run")}</>
                                }
                            </button>
                        </div>

                        {/* Content */}
                        {!llmNarrative && !llmLoading && !llmError && (
                            <div className="flex flex-col items-center justify-center py-10 gap-3 text-[var(--text-muted)]">
                                <Sparkles className="h-8 w-8 text-indigo-500/40" />
                                <div className="text-center">
                                    <p className="text-sm text-[var(--text-main)]">{t("company.ai.notRun")}</p>
                                    <p className="text-xs mt-0.5">{t("company.ai.notRunHint")}</p>
                                </div>
                            </div>
                        )}

                        {llmLoading && (
                            <div className="flex flex-col items-center justify-center py-10 gap-3 text-[var(--text-muted)]">
                                <Loader2 className="h-7 w-7 text-indigo-500 animate-spin" />
                                <p className="text-sm">{t("company.ai.sending")}</p>
                            </div>
                        )}

                        {llmError && (
                            <div className="flex items-start gap-3 m-5 rounded-xl border border-rose-500/20 bg-rose-500/8 p-4">
                                <XCircle className="h-4 w-4 text-rose-500 shrink-0 mt-0.5" />
                                <div>
                                    <p className="text-sm font-medium text-rose-600 dark:text-rose-400">{t("company.err.llm")}</p>
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
                                    {t("company.ai.disclaimer")}
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
                                    <div className="text-sm font-semibold text-[var(--text-main)]">{t("company.ai.assistant")}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">
                                        {t("company.chat.subtitle")}
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
                                            {t("company.chat.emptyTitle")}<br />
                                            <span className="text-xs">{t("company.chat.emptyExample")}</span>
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
                                    placeholder={t("company.chat.placeholder")}
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
                        {t("company.footer.loadedAt", { date: new Date(profile.fetched_at).toLocaleString("ru-RU") })} · {t("company.report.source")}: OWS API v3 goszakup.gov.kz
                    </div>
                </div>
            )}
        </div>
    );
}
