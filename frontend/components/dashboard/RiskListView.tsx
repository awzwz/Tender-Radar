"use client";

import React, { useState, useMemo } from "react";
import {
    ListFilter,
    RefreshCw,
    ChevronUp,
    ChevronDown,
    ChevronsUpDown,
    ArrowRight,
} from "lucide-react";
import { DashboardItem } from "@/lib/api";
import { Badge, Chip, ProgressBar, SectionTitle, money } from "@/components/shared/ui";

/* ── Types ──────────────────────────────────────────────────────────── */
type SortKey = "risk_score" | "amount" | "lot_name" | "customer_name";
type SortDir = "asc" | "desc";

interface RiskListViewProps {
    items: DashboardItem[];
    loading: boolean;
    onOpenLot: (id: number) => void;
    onLoadMore: () => void;
    hasMore: boolean;
}

/* ── Sort icon ──────────────────────────────────────────────────────── */
function SortIcon({ active, dir }: { active: boolean; dir: SortDir }) {
    if (!active) return <ChevronsUpDown className="h-3 w-3 text-[var(--text-muted)] opacity-50" />;
    return dir === "asc"
        ? <ChevronUp className="h-3 w-3 text-indigo-500 dark:text-indigo-400" />
        : <ChevronDown className="h-3 w-3 text-indigo-500 dark:text-indigo-400" />;
}

/* ── Pagination ─────────────────────────────────────────────────────── */
function Pagination({
    page, totalPages, onPage,
}: {
    page: number; totalPages: number; onPage: (p: number) => void;
}) {
    const pages = useMemo(() => {
        const arr: (number | "...")[] = [];
        for (let i = 1; i <= totalPages; i++) {
            if (i <= 2 || i >= totalPages - 1 || Math.abs(i - page) <= 1) {
                arr.push(i);
            } else if (arr[arr.length - 1] !== "...") {
                arr.push("...");
            }
        }
        return arr;
    }, [page, totalPages]);

    return (
        <div className="flex items-center justify-center gap-1 mt-4">
            {pages.map((p, i) =>
                p === "..." ? (
                    <span key={`dots-${i}`} className="px-2 text-xs text-[var(--text-muted)] opacity-70">…</span>
                ) : (
                    <button
                        key={p}
                        onClick={() => onPage(p as number)}
                        className={`min-w-[28px] rounded-lg px-2 py-1.5 text-xs font-medium transition-all ${page === p
                            ? "bg-indigo-500/20 border border-indigo-500/40 text-indigo-700 dark:text-indigo-200"
                            : "text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)]"
                            }`}
                    >
                        {p}
                    </button>
                )
            )}
        </div>
    );
}

/* ── Main component ─────────────────────────────────────────────────── */
export default function RiskListView({
    items, loading, onOpenLot, onLoadMore, hasMore,
}: RiskListViewProps) {
    const [sortKey, setSortKey] = useState<SortKey>("risk_score");
    const [sortDir, setSortDir] = useState<SortDir>("desc");
    const [tablePage, setTablePage] = useState(1);
    const perPage = 15;

    const toggleSort = (key: SortKey) => {
        if (sortKey === key) {
            setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        } else {
            setSortKey(key);
            setSortDir("desc");
        }
        setTablePage(1);
    };

    const sorted = useMemo(() => {
        const arr = [...items];
        arr.sort((a, b) => {
            let va: string | number, vb: string | number;
            switch (sortKey) {
                case "risk_score": va = a.risk_score ?? 0; vb = b.risk_score ?? 0; break;
                case "amount": va = a.amount ?? 0; vb = b.amount ?? 0; break;
                case "lot_name": va = (a.lot_name ?? "").toLowerCase(); vb = (b.lot_name ?? "").toLowerCase(); break;
                case "customer_name": va = (a.customer_name ?? "").toLowerCase(); vb = (b.customer_name ?? "").toLowerCase(); break;
            }
            if (va < vb) return sortDir === "asc" ? -1 : 1;
            if (va > vb) return sortDir === "asc" ? 1 : -1;
            return 0;
        });
        return arr;
    }, [items, sortKey, sortDir]);

    const totalPages = Math.max(1, Math.ceil(sorted.length / perPage));
    const pageItems = sorted.slice((tablePage - 1) * perPage, tablePage * perPage);

    const colBtn = (key: SortKey, label: string) => (
        <button
            onClick={() => toggleSort(key)}
            className="flex items-center gap-1 text-[11px] font-semibold text-[var(--text-muted)] uppercase tracking-wider hover:text-[var(--text-main)] transition-colors"
        >
            {label}
            <SortIcon active={sortKey === key} dir={sortDir} />
        </button>
    );

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <SectionTitle
                    icon={<ListFilter className="h-4 w-4 text-indigo-500 dark:text-indigo-200" />}
                    title="Risk List"
                    hint="Сортируемая таблица · Клик по заголовку для сортировки"
                />
                <div className="text-[11px] text-[var(--text-muted)]">
                    {sorted.length} items · page {tablePage}/{totalPages}
                </div>
            </div>

            {loading && items.length === 0 ? (
                <div className="flex items-center justify-center py-16 text-[var(--text-muted)]">
                    <RefreshCw className="h-5 w-5 animate-spin mr-2" /> Загрузка...
                </div>
            ) : items.length === 0 ? (
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-8 text-sm text-[var(--text-muted)] text-center">
                    Нет результатов. Уменьши Min risk или измени фильтр.
                </div>
            ) : (
                <>
                    {/* Table header */}
                    <div className="hidden md:grid grid-cols-[1fr_120px_120px_100px_80px] gap-3 px-4 py-2 border-b border-[var(--border)]">
                        {colBtn("lot_name", "Лот")}
                        {colBtn("customer_name", "Заказчик")}
                        {colBtn("amount", "Сумма")}
                        {colBtn("risk_score", "Risk")}
                        <div className="text-[11px] text-[var(--text-muted)] text-center">Action</div>
                    </div>

                    {/* Rows */}
                    <div className="space-y-1.5">
                        {pageItems.map((l, idx) => (
                            <div
                                key={`${l.lot_id}-${idx}`}
                                className="group rounded-xl border border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-hover)] hover:border-[var(--border-hover)] transition-all"
                            >
                                {/* Desktop row */}
                                <div className="hidden md:grid grid-cols-[1fr_120px_120px_100px_80px] gap-3 items-center px-4 py-3">
                                    <div className="min-w-0">
                                        <div className="text-sm font-medium text-[var(--text-main)] truncate transition-colors">{l.lot_name || "Без названия"}</div>
                                        <div className="mt-0.5 text-[10px] text-[var(--text-muted)] truncate">{l.tender_number} · {l.publish_date?.slice(0, 10)}</div>
                                        {l.top_reasons.length > 0 && (
                                            <div className="mt-1.5 flex flex-wrap gap-1">
                                                {l.top_reasons.slice(0, 3).map((r, i) => (
                                                    <Chip key={`${r.code}-${i}`}>{r.code}</Chip>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                    <div className="text-xs text-[var(--text-muted)] truncate">{l.customer_name || l.customer_bin}</div>
                                    <div className="text-xs text-[var(--text-muted)] tabular-nums">{money(l.amount)}</div>
                                    <div>
                                        <Badge level={l.risk_level} score={Math.round(l.risk_score)} />
                                        <div className="mt-1.5"><ProgressBar value={l.risk_score} /></div>
                                    </div>
                                    <div className="flex justify-center">
                                        <button
                                            onClick={() => onOpenLot(l.lot_id)}
                                            className="rounded-lg bg-indigo-500/15 border border-indigo-500/25 px-2.5 py-1.5 text-[11px] font-medium text-indigo-700 dark:text-indigo-200 hover:bg-indigo-500/25 transition-all"
                                        >
                                            Open
                                        </button>
                                    </div>
                                </div>

                                {/* Mobile card */}
                                <div className="md:hidden p-3">
                                    <div className="flex items-start justify-between gap-2">
                                        <div className="flex-1 min-w-0">
                                            <div className="text-sm font-medium text-[var(--text-main)] truncate">{l.lot_name || "Без названия"}</div>
                                            <div className="mt-0.5 text-[10px] text-[var(--text-muted)] truncate">{l.tender_number} · {l.customer_name || l.customer_bin}</div>
                                        </div>
                                        <Badge level={l.risk_level} score={Math.round(l.risk_score)} />
                                    </div>
                                    <div className="mt-2 flex items-center justify-between">
                                        <div className="text-xs text-[var(--text-muted)]">{money(l.amount)}</div>
                                        <button onClick={() => onOpenLot(l.lot_id)} className="rounded-lg bg-indigo-500 px-3 py-1 text-xs text-white hover:bg-indigo-400 transition">Open</button>
                                    </div>
                                    <div className="mt-2"><ProgressBar value={l.risk_score} /></div>
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Pagination */}
                    <Pagination page={tablePage} totalPages={totalPages} onPage={setTablePage} />

                    {/* Load more from server */}
                    {hasMore && (
                        <button
                            onClick={onLoadMore}
                            disabled={loading}
                            className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] py-3 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] transition disabled:opacity-40"
                        >
                            {loading ? "Загрузка..." : "Загрузить ещё с сервера"}
                        </button>
                    )}
                </>
            )}
        </div>
    );
}
