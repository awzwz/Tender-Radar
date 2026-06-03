"use client";

import React, { useCallback, useEffect, useState } from "react";
import { api, DashboardTenderItem } from "@/lib/api";
import { money, Badge } from "@/components/shared/ui";
import { ChevronDown, ChevronUp, RefreshCw } from "lucide-react";
import { useI18n } from "@/components/providers/LanguageProvider";

type Category = "all" | "main_high" | "single_case" | "regular";
type SortKey = "score" | "amount" | "date";

const CATEGORIES: Category[] = ["all", "main_high", "single_case", "regular"];

const CATEGORY_COLORS: Record<string, string> = {
    main_high: "bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300",
    single_case: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300",
    regular: "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
};

function riskColor(level: string) {
    if (level === "HIGH") return "text-rose-600 dark:text-rose-400";
    if (level === "MEDIUM") return "text-amber-600 dark:text-amber-400";
    return "text-emerald-600 dark:text-emerald-400";
}

interface Props {
    onOpenTender?: (id: number) => void;
}

export default function TenderListView({ onOpenTender }: Props) {
    const { t } = useI18n();
    const [items, setItems] = useState<DashboardTenderItem[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [loading, setLoading] = useState(false);
    const [category, setCategory] = useState<Category>("all");
    const [sortKey, setSortKey] = useState<SortKey>("score");
    const [sortDir, setSortDir] = useState<"desc" | "asc">("desc");
    const [search, setSearch] = useState("");

    const load = useCallback(async (pg: number, cat: Category, sk: SortKey) => {
        setLoading(true);
        try {
            const data = await api.dashboardTenders({
                page: pg,
                limit: 50,
                category: cat,
                sort_by: sk,
            });
            setTotal(data.total);
            if (pg === 1) {
                setItems(data.items);
            } else {
                setItems((prev) => {
                    const seen = new Set(prev.map((x) => x.trd_buy_id));
                    return [...prev, ...data.items.filter((x) => !seen.has(x.trd_buy_id))];
                });
            }
        } catch {
            // silently fail
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        setPage(1);
        load(1, category, sortKey);
    }, [category, sortKey, load]);

    function toggleSort(key: SortKey) {
        if (sortKey === key) {
            setSortDir((d) => (d === "desc" ? "asc" : "desc"));
        } else {
            setSortKey(key);
            setSortDir("desc");
        }
    }

    function SortIcon({ k }: { k: SortKey }) {
        if (sortKey !== k) return null;
        return sortDir === "desc"
            ? <ChevronDown className="inline h-3 w-3 ml-0.5" />
            : <ChevronUp className="inline h-3 w-3 ml-0.5" />;
    }

    const filtered = search.trim()
        ? items.filter((i) =>
            i.tender_number?.toLowerCase().includes(search.toLowerCase()) ||
            i.tender_name?.toLowerCase().includes(search.toLowerCase()) ||
            i.org_bin?.includes(search)
        )
        : items;

    const sorted = [...filtered].sort((a, b) => {
        let diff = 0;
        if (sortKey === "score") diff = b.risk_score - a.risk_score;
        else if (sortKey === "amount") diff = b.total_sum - a.total_sum;
        else if (sortKey === "date") diff = (b.publish_date || "").localeCompare(a.publish_date || "");
        return sortDir === "desc" ? diff : -diff;
    });

    return (
        <div className="space-y-4">
            {/* Header + filters */}
            <div className="flex flex-wrap items-center gap-3">
                <div>
                    <h2 className="text-lg font-semibold text-[var(--text-main)]">{t("tenderList.title")}</h2>
                    <p className="text-xs text-[var(--text-muted)]">{t("tenderList.inBase", { n: total })}</p>
                </div>
                <div className="ml-auto flex flex-wrap gap-2 items-center">
                    <input
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        placeholder={t("tenderList.searchPh")}
                        className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-1 focus:ring-indigo-500"
                    />
                    {/* Category filter */}
                    <div className="flex gap-1">
                        {CATEGORIES.map((c) => (
                            <button
                                key={c}
                                onClick={() => { setCategory(c); setPage(1); }}
                                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${category === c
                                    ? "bg-indigo-500 text-white"
                                    : "border border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"
                                    }`}
                            >
                                {t(`tenderList.category.${c}`)}
                            </button>
                        ))}
                    </div>
                    <button
                        onClick={() => { setPage(1); load(1, category, sortKey); }}
                        className="rounded-lg border border-[var(--border)] p-1.5 text-[var(--text-muted)] hover:bg-[var(--surface-hover)] transition"
                    >
                        <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                    </button>
                </div>
            </div>

            {/* Table */}
            <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                <table className="w-full text-sm">
                    <thead className="border-b border-[var(--border)] bg-[var(--surface)]">
                        <tr>
                            <th className="px-4 py-3 text-left font-medium text-[var(--text-muted)]">{t("tenderList.col.tender")}</th>
                            <th className="px-4 py-3 text-left font-medium text-[var(--text-muted)]">{t("tenderList.col.customerBin")}</th>
                            <th
                                className="px-4 py-3 text-right font-medium text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-main)]"
                                onClick={() => toggleSort("amount")}
                            >
                                {t("tenderList.col.amount")} <SortIcon k="amount" />
                            </th>
                            <th className="px-4 py-3 text-center font-medium text-[var(--text-muted)]">{t("tenderList.col.lotsHighTotal")}</th>
                            <th className="px-4 py-3 text-center font-medium text-[var(--text-muted)]">{t("tenderList.col.category")}</th>
                            <th
                                className="px-4 py-3 text-right font-medium text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-main)]"
                                onClick={() => toggleSort("score")}
                            >
                                {t("tenderList.col.risk")} <SortIcon k="score" />
                            </th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border)]">
                        {loading && sorted.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="px-4 py-12 text-center text-[var(--text-muted)]">
                                    <RefreshCw className="h-5 w-5 animate-spin mx-auto mb-2" />
                                    {t("tenderList.loading")}
                                </td>
                            </tr>
                        ) : sorted.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="px-4 py-12 text-center text-[var(--text-muted)]">
                                    {t("tenderList.notFound")}
                                </td>
                            </tr>
                        ) : (
                            sorted.map((item, idx) => (
                                <tr
                                    key={`${item.trd_buy_id}-${idx}`}
                                    onClick={() => onOpenTender?.(item.trd_buy_id)}
                                    className="hover:bg-[var(--surface-hover)] transition cursor-pointer"
                                >
                                    <td className="px-4 py-3 max-w-[260px]">
                                        <div className="font-mono text-xs text-indigo-500 dark:text-indigo-400">{item.tender_number}</div>
                                        <div className="text-[var(--text-main)] truncate text-xs mt-0.5">{item.tender_name || "—"}</div>
                                    </td>
                                    <td className="px-4 py-3 font-mono text-xs text-[var(--text-muted)]">{item.org_bin}</td>
                                    <td className="px-4 py-3 text-right font-medium text-[var(--text-main)] whitespace-nowrap">
                                        {money(item.total_sum)}
                                    </td>
                                    <td className="px-4 py-3 text-center">
                                        <span className={item.high_lots_count > 0 ? "text-rose-600 dark:text-rose-400 font-semibold" : "text-[var(--text-muted)]"}>
                                            {item.high_lots_count}
                                        </span>
                                        <span className="text-[var(--text-muted)]"> / {item.lots_count}</span>
                                    </td>
                                    <td className="px-4 py-3 text-center">
                                        <span className={`rounded-md px-2 py-0.5 text-[10px] font-semibold uppercase ${CATEGORY_COLORS[item.category] || CATEGORY_COLORS.regular}`}>
                                            {item.category === "main_high" ? t("tenderList.category.main_high") : item.category === "single_case" ? t("tenderList.category.single_case") : t("tenderList.category.regular")}
                                        </span>
                                    </td>
                                    <td className="px-4 py-3 text-right">
                                        <span className={`font-semibold ${riskColor(item.risk_level)}`}>
                                            {Math.round(item.risk_score)}
                                        </span>
                                        <span className="text-[10px] text-[var(--text-muted)] ml-1">{item.risk_level}</span>
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            {/* Load more */}
            {sorted.length < total && (
                <div className="flex justify-center">
                    <button
                        onClick={() => {
                            const next = page + 1;
                            setPage(next);
                            load(next, category, sortKey);
                        }}
                        disabled={loading}
                        className="rounded-xl border border-[var(--border)] px-6 py-2 text-sm text-[var(--text-muted)] hover:bg-[var(--surface-hover)] transition disabled:opacity-50"
                    >
                        {loading ? t("tenderList.loading") : t("tenderList.loadMore", { n: total - sorted.length })}
                    </button>
                </div>
            )}
        </div>
    );
}
