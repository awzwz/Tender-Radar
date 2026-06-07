"use client";

import React, { useState, useMemo } from "react";
import dynamic from "next/dynamic";
import KZMap from "./KZMap";
import {
    LayoutDashboard,
    Sparkles,
    RefreshCw,
    TrendingUp,
    TrendingDown,
    Activity,
} from "lucide-react";
import { DashboardItem, DashboardTenderItem } from "@/lib/api";
import {
    Badge,
    ProgressBar,
    SectionTitle,
    StatCard,
    money,
} from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";

// Lazy-load ApexCharts to avoid SSR issues
const Chart = dynamic(() => import("react-apexcharts"), { ssr: false });

/* ── Types ──────────────────────────────────────────────────────────── */
interface DashboardViewProps {
    items: DashboardItem[];
    total: number;
    loading: boolean;
    onOpenLot: (id: number) => void;
    globalStats?: {
        high: number;
        medium: number;
        low: number;
        avg_score: number;
        scored_lots: number;
    } | null;
    tenderStats?: {
        high: number;
        medium: number;
        low: number;
        avg_score: number;
        scored_tenders: number;
        total_tenders: number;
        main_high: number;
        single_case_alerts: number;
    } | null;
    tenderItems: DashboardTenderItem[];
    tenderLoading: boolean;
}

/* ── Sparkline mini-chart ───────────────────────────────────────────── */
function Sparkline({ data, color }: { data: number[]; color: string }) {
    return (
        <Chart
            type="area"
            height={40}
            width={100}
            options={{
                chart: { sparkline: { enabled: true }, animations: { enabled: true, speed: 800 } },
                stroke: { curve: "smooth", width: 2 },
                fill: {
                    type: "gradient",
                    gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05, stops: [0, 100] },
                },
                colors: [color],
                tooltip: { enabled: false },
            }}
            series={[{ data }]}
        />
    );
}

/* ── Donut Chart ────────────────────────────────────────────────────── */
function RiskDonut({ high, medium, low }: { high: number; medium: number; low: number }) {
    const { t } = useI18n();
    const total = high + medium + low || 1;
    return (
        <Chart
            type="donut"
            height={220}
            options={{
                chart: { animations: { enabled: true, speed: 600 } },
                labels: ["HIGH", "MEDIUM", "LOW"],
                colors: ["#f43f5e", "#f59e0b", "#10b981"],
                stroke: { width: 0 },
                dataLabels: { enabled: false },
                legend: {
                    position: "bottom",
                    labels: { useSeriesColors: true },
                    fontSize: "12px",
                    fontWeight: 600,
                    markers: { size: 6, shape: "circle" as const, offsetX: -4 },
                    itemMargin: { horizontal: 10, vertical: 5 },
                },
                plotOptions: {
                    pie: {
                        donut: {
                            size: "72%",
                            labels: {
                                show: true,
                                total: {
                                    show: true,
                                    label: t("dash.dist.total"),
                                    color: "#64748B", // text-muted equivalent
                                    fontSize: "11px",
                                    formatter: () => String(total),
                                },
                                value: { color: "#1C2434", fontSize: "20px", fontWeight: "700" }, // text-main equivalent
                                name: { color: "#64748B", fontSize: "11px" },
                            },
                        },
                    },
                },
                tooltip: {
                    theme: "dark",
                    y: { formatter: (v: number) => `${v} (${((v / total) * 100).toFixed(1)}%)` },
                },
            }}
            series={[high, medium, low]}
        />
    );
}

/* ── Main Dashboard ─────────────────────────────────────────────────── */
export default function DashboardView({
    items,
    total,
    loading,
    onOpenLot,
    globalStats,
    tenderStats,
    tenderItems,
    tenderLoading,
}: DashboardViewProps) {
    const { t } = useI18n();
    const [entityMode, setEntityMode] = useState<"lots" | "tenders">("lots");

    const stats = useMemo(() => {
        if (globalStats) {
            return { high: globalStats.high, med: globalStats.medium, low: globalStats.low, avg: Math.round(globalStats.avg_score), scored: globalStats.scored_lots };
        }
        return { high: 0, med: 0, low: 0, avg: 0, scored: 0 };
    }, [globalStats]);

    const tenderStatsUi = useMemo(() => {
        if (tenderStats) {
            return {
                high: tenderStats.high, med: tenderStats.medium, low: tenderStats.low,
                avg: Math.round(tenderStats.avg_score), scored: tenderStats.scored_tenders,
                mainHigh: tenderStats.main_high, singleCase: tenderStats.single_case_alerts,
            };
        }
        return { high: 0, med: 0, low: 0, avg: 0, scored: 0, mainHigh: 0, singleCase: 0 };
    }, [tenderStats]);

    const activeStats = entityMode === "lots" ? stats : tenderStatsUi;
    const topLots = [...items].sort((a, b) => (b.risk_score ?? 0) - (a.risk_score ?? 0)).slice(0, 5);
    const topTenders = [...tenderItems].sort((a, b) => (b.risk_score ?? 0) - (a.risk_score ?? 0)).slice(0, 5);

    // Fake sparkline data based on real counts (used to generate visual trends)
    const sparkHigh = useMemo(() => Array.from({ length: 7 }, (_, i) => Math.max(0, activeStats.high + Math.round((Math.random() - 0.5) * activeStats.high * 0.3))), [activeStats.high]);
    const sparkMed = useMemo(() => Array.from({ length: 7 }, (_, i) => Math.max(0, activeStats.med + Math.round((Math.random() - 0.5) * activeStats.med * 0.3))), [activeStats.med]);
    const sparkLow = useMemo(() => Array.from({ length: 7 }, (_, i) => Math.max(0, activeStats.low + Math.round((Math.random() - 0.5) * activeStats.low * 0.3))), [activeStats.low]);

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <SectionTitle
                    icon={<LayoutDashboard className="h-4 w-4 text-indigo-200" />}
                    title={t("dash.title")}
                    hint={t("dash.hint")}
                />
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => setEntityMode("lots")}
                        className={`rounded-xl border px-3 py-1.5 text-xs font-medium transition-all ${entityMode === "lots" ? "border-indigo-500/50 bg-indigo-500/20 text-indigo-700 dark:text-indigo-200 shadow-sm shadow-indigo-500/10" : "border-[var(--border)] bg-[var(--surface)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"}`}
                    >
                        {t("dash.toggle.lots")}
                    </button>
                    <button
                        onClick={() => setEntityMode("tenders")}
                        className={`rounded-xl border px-3 py-1.5 text-xs font-medium transition-all ${entityMode === "tenders" ? "border-indigo-500/50 bg-indigo-500/20 text-indigo-700 dark:text-indigo-200 shadow-sm shadow-indigo-500/10" : "border-[var(--border)] bg-[var(--surface)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"}`}
                    >
                        {t("dash.toggle.tenders")}
                    </button>
                </div>
            </div>

            {/* ── Metric cards with sparklines ── */}
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4 stagger">
                {/* HIGH */}
                <div className="group relative rounded-2xl border border-rose-500/20 bg-rose-500/8 p-4 transition-all duration-200 hover:bg-rose-500/12 hover:-translate-y-0.5 hover:shadow-lg hover:shadow-black/5">
                    <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                            <div className="h-2.5 w-2.5 rounded-full bg-rose-500 shadow-sm shadow-rose-500/50" />
                            <span className="text-[11px] text-rose-700 dark:text-rose-200 uppercase tracking-wider font-semibold">HIGH</span>
                        </div>
                        <TrendingUp className="h-3.5 w-3.5 text-rose-500 dark:text-rose-400" />
                    </div>
                    <div className="text-3xl font-bold tracking-tight mt-1" style={{ color: "var(--text-main)" }}>{activeStats.high}</div>
                    <div className="mt-2"><Sparkline data={sparkHigh} color="#f43f5e" /></div>
                </div>

                {/* MEDIUM */}
                <div className="group relative rounded-2xl border border-amber-500/20 bg-amber-500/8 p-4 transition-all duration-200 hover:bg-amber-500/12 hover:-translate-y-0.5 hover:shadow-lg hover:shadow-black/5">
                    <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                            <div className="h-2.5 w-2.5 rounded-full bg-amber-500 shadow-sm shadow-amber-500/50" />
                            <span className="text-[11px] text-amber-700 dark:text-amber-200 uppercase tracking-wider font-semibold">MEDIUM</span>
                        </div>
                        <Activity className="h-3.5 w-3.5 text-amber-500 dark:text-amber-400" />
                    </div>
                    <div className="text-3xl font-bold tracking-tight mt-1" style={{ color: "var(--text-main)" }}>{activeStats.med}</div>
                    <div className="mt-2"><Sparkline data={sparkMed} color="#f59e0b" /></div>
                </div>

                {/* LOW */}
                <div className="group relative rounded-2xl border border-emerald-500/20 bg-emerald-500/8 p-4 transition-all duration-200 hover:bg-emerald-500/12 hover:-translate-y-0.5 hover:shadow-lg hover:shadow-black/5">
                    <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                            <div className="h-2.5 w-2.5 rounded-full bg-emerald-500 shadow-sm shadow-emerald-500/50" />
                            <span className="text-[11px] text-emerald-700 dark:text-emerald-200 uppercase tracking-wider font-semibold">LOW</span>
                        </div>
                        <TrendingDown className="h-3.5 w-3.5 text-emerald-500 dark:text-emerald-400" />
                    </div>
                    <div className="text-3xl font-bold tracking-tight mt-1" style={{ color: "var(--text-main)" }}>{activeStats.low}</div>
                    <div className="mt-2"><Sparkline data={sparkLow} color="#10b981" /></div>
                </div>

                {/* Avg Risk */}
                <div className="group relative rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 transition-all duration-200 hover:border-[var(--border-hover)] hover:bg-[var(--surface-hover)] hover:-translate-y-0.5 hover:shadow-lg hover:shadow-black/5">
                    <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                            <div className="h-2.5 w-2.5 rounded-full bg-slate-400" />
                            <span className="text-[11px] text-[var(--text-muted)] uppercase tracking-wider font-semibold">{t("dash.card.avgRisk")}</span>
                        </div>
                        <Sparkles className="h-3.5 w-3.5 text-[var(--text-muted)]" />
                    </div>
                    <div className="text-3xl font-bold text-[var(--text-main)] mt-1">{activeStats.avg}</div>
                    <div className="mt-3 h-px w-full bg-[var(--border)]" />
                    <div className="mt-2 text-[11px] text-[var(--text-muted)]">{t("dash.card.scored", { n: activeStats.scored })}</div>
                </div>
            </div>

            {/* ── Kazakhstan risk map ── */}
            <KZMap />

            {/* ── Risk Distribution ── */}
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-5">
                <SectionTitle
                    icon={<Sparkles className="h-4 w-4 text-emerald-500 dark:text-emerald-200" />}
                    title={t("dash.dist.title")}
                    hint={entityMode === "lots" ? t("dash.dist.hint.lots") : t("dash.dist.hint.tenders")}
                />
                <div className="mt-4 flex justify-center">
                    <RiskDonut high={activeStats.high} medium={activeStats.med} low={activeStats.low} />
                </div>
            </div>

            {/* ── Top risky list ── */}
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4">
                <div className="flex items-center justify-between mb-3">
                    <div>
                        <div className="text-sm font-semibold text-[var(--text-main)]">{entityMode === "lots" ? t("dash.top.title.lots") : t("dash.top.title.tenders")}</div>
                        <div className="text-[11px] text-[var(--text-muted)] mt-0.5">
                            {entityMode === "lots" ? t("dash.top.hint.lots") : t("dash.top.hint.tenders")}
                        </div>
                    </div>
                    <div className="text-[11px] text-[var(--text-muted)]">{t("dash.top.total", { n: entityMode === "lots" ? total : (tenderStats?.total_tenders ?? 0) })}</div>
                </div>

                {(entityMode === "lots" && loading) || (entityMode === "tenders" && tenderLoading) ? (
                    <div className="space-y-2">
                        {Array.from({ length: 5 }).map((_, i) => (
                            <div key={i} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                                <div className="flex items-center justify-between gap-3">
                                    <div className="flex-1 space-y-2">
                                        <div className="skeleton h-3.5 w-2/3" />
                                        <div className="skeleton h-2.5 w-2/5" />
                                    </div>
                                    <div className="skeleton h-6 w-20 rounded-full" />
                                </div>
                                <div className="skeleton h-1.5 w-full mt-2.5 rounded-full" />
                            </div>
                        ))}
                    </div>
                ) : (entityMode === "lots" ? topLots : topTenders).length === 0 ? (
                    <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-6 text-sm text-[var(--text-muted)] text-center">
                        {t("dash.empty")}
                    </div>
                ) : (
                    <div className="space-y-2 stagger">
                        {entityMode === "lots"
                            ? topLots.map((l, idx) => (
                                <button
                                    key={`${l.lot_id}-${idx}`}
                                    onClick={() => onOpenLot(l.lot_id)}
                                    className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 text-left hover:border-[var(--border-hover)] hover:-translate-y-0.5 hover:shadow-md hover:shadow-black/5 transition-all duration-200"
                                >
                                    <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                                        <div className="flex-1 min-w-0">
                                            <div className="text-sm font-medium text-[var(--text-main)] truncate">{l.lot_name || t("dash.noName")}</div>
                                            <div className="mt-0.5 text-[11px] text-[var(--text-muted)] truncate">{l.tender_number} · {l.customer_name || l.customer_bin}</div>
                                        </div>
                                        <div className="flex items-center gap-3 flex-shrink-0">
                                            <div className="text-[11px] text-[var(--text-muted)]">{money(l.amount)}</div>
                                            <Badge level={l.risk_level} score={Math.round(l.risk_score)} />
                                        </div>
                                    </div>
                                    <div className="mt-2"><ProgressBar value={l.risk_score} /></div>
                                </button>
                            ))
                            : topTenders.map((tn, idx) => (
                                <div key={`${tn.trd_buy_id}-${idx}`} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                                    <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                                        <div className="flex-1 min-w-0">
                                            <div className="text-sm font-medium text-[var(--text-main)] truncate">{tn.tender_name || t("dash.noName")}</div>
                                            <div className="mt-0.5 text-[11px] text-[var(--text-muted)] truncate">
                                                {t("dash.tender.meta", { number: tn.tender_number, lots: tn.lots_count, high: tn.high_lots_count })}
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3 flex-shrink-0">
                                            <div className="text-[11px] text-[var(--text-muted)]">{money(tn.total_sum)}</div>
                                            <Badge level={tn.risk_level} score={Math.round(tn.risk_score)} />
                                        </div>
                                    </div>
                                    <div className="mt-2"><ProgressBar value={tn.risk_score} /></div>
                                </div>
                            ))}
                    </div>
                )}
            </div>
        </div>
    );
}
