"use client";

import React, { useCallback, useEffect, useState } from "react";
import { api, CustomerListItem, CustomerProfile } from "@/lib/api";
import { money } from "@/components/shared/ui";
import { RefreshCw, X, ShieldAlert } from "lucide-react";

type SortKey = "high_lots" | "avg_score" | "total_amount" | "total_lots";

const SORT_LABELS: Record<SortKey, string> = {
    high_lots: "HIGH лоты",
    avg_score: "Avg Score",
    total_amount: "Сумма",
    total_lots: "Лотов",
};

function riskBand(avg: number) {
    if (avg >= 30) return "HIGH";
    if (avg >= 15) return "MEDIUM";
    return "LOW";
}

function riskColor(avg: number) {
    if (avg >= 30) return "text-rose-600 dark:text-rose-400";
    if (avg >= 15) return "text-amber-600 dark:text-amber-400";
    return "text-emerald-600 dark:text-emerald-400";
}

interface SlideInProps {
    bin: string;
    onClose: () => void;
}

function CustomerSlideIn({ bin, onClose }: SlideInProps) {
    const [data, setData] = useState<CustomerProfile | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    useEffect(() => {
        setLoading(true);
        setError("");
        api.customer(bin)
            .then(setData)
            .catch(() => setError("Не удалось загрузить профиль заказчика"))
            .finally(() => setLoading(false));
    }, [bin]);

    return (
        <div className="fixed inset-0 z-50 flex justify-end">
            <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />
            <div className="relative w-full max-w-lg bg-[var(--surface)] border-l border-[var(--border)] shadow-2xl overflow-y-auto flex flex-col">
                <div className="sticky top-0 flex items-center justify-between px-5 py-4 border-b border-[var(--border)] bg-[var(--surface)] z-10">
                    <div>
                        <div className="text-sm font-semibold text-[var(--text-main)]">Профиль заказчика</div>
                        <div className="font-mono text-xs text-[var(--text-muted)]">{bin}</div>
                    </div>
                    <button onClick={onClose} className="rounded-xl p-2 hover:bg-[var(--surface-hover)] text-[var(--text-muted)] transition">
                        <X className="h-5 w-5" />
                    </button>
                </div>

                <div className="flex-1 px-5 py-4 space-y-5">
                    {loading && <div className="flex justify-center py-10"><RefreshCw className="h-5 w-5 animate-spin text-[var(--text-muted)]" /></div>}
                    {error && <div className="text-rose-500 text-sm">{error}</div>}
                    {data && (
                        <>
                            <div>
                                <div className="text-base font-semibold text-[var(--text-main)]">{data.company.name_ru || "—"}</div>
                            </div>

                            <div className="grid grid-cols-3 gap-3">
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{data.stats.total_contracts}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Контракты</div>
                                </div>
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{money(data.stats.total_sum)}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Объём закупок</div>
                                </div>
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{data.stats.unique_suppliers}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Поставщиков</div>
                                </div>
                            </div>

                            {data.high_risk_lots?.length > 0 && (
                                <div>
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-2 flex items-center gap-1.5">
                                        <ShieldAlert className="h-3.5 w-3.5 text-rose-500" />
                                        HIGH-риск лоты
                                    </div>
                                    <div className="space-y-2">
                                        {data.high_risk_lots.map((l) => (
                                            <div key={l.lot_id} className="rounded-lg bg-rose-500/5 border border-rose-500/20 px-3 py-2">
                                                <div className="text-xs text-[var(--text-main)] truncate">{l.name_ru}</div>
                                                <div className="flex items-center justify-between mt-1">
                                                    <span className="text-[10px] text-[var(--text-muted)]">{money(l.amount)}</span>
                                                    <span className="text-[10px] font-semibold text-rose-600 dark:text-rose-400">score {Math.round(l.score)}</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {data.top_suppliers?.length > 0 && (
                                <div>
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-2">Топ поставщики</div>
                                    <div className="space-y-2">
                                        {data.top_suppliers.map((s) => (
                                            <div key={s.supplier_biin} className="flex items-center justify-between rounded-lg bg-[var(--surface-hover)] px-3 py-2">
                                                <span className="font-mono text-xs text-[var(--text-muted)]">{s.supplier_biin}</span>
                                                <div className="text-right">
                                                    <div className="text-xs font-medium text-[var(--text-main)]">{money(s.total_sum)}</div>
                                                    <div className="text-[10px] text-[var(--text-muted)]">{s.contract_count} контр.</div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

export default function CustomersListView() {
    const [items, setItems] = useState<CustomerListItem[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState("");
    const [sortKey, setSortKey] = useState<SortKey>("high_lots");
    const [onlyWithHighLots, setOnlyWithHighLots] = useState(false);
    const [selectedBin, setSelectedBin] = useState<string | null>(null);

    const load = useCallback(async (pg: number, sk: SortKey, onlyHigh: boolean, q?: string) => {
        setLoading(true);
        try {
            const data = await api.customersList({
                page: pg,
                limit: 50,
                sort_by: sk,
                search: q || undefined,
                min_high_lots: onlyHigh ? 1 : undefined,
            });
            setTotal(data.total);
            if (pg === 1) {
                setItems(data.items);
            } else {
                setItems((prev) => {
                    const seen = new Set(prev.map((x) => x.bin));
                    return [...prev, ...data.items.filter((x) => !seen.has(x.bin))];
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
        load(1, sortKey, onlyWithHighLots, search || undefined);
    }, [sortKey, onlyWithHighLots, load]);

    function handleSearch(e: React.FormEvent) {
        e.preventDefault();
        setPage(1);
        load(1, sortKey, onlyWithHighLots, search || undefined);
    }

    return (
        <div className="space-y-4">
            {/* Slide-in panel */}
            {selectedBin && (
                <CustomerSlideIn bin={selectedBin} onClose={() => setSelectedBin(null)} />
            )}

            {/* Header */}
            <div className="flex flex-wrap items-center gap-3">
                <div>
                    <h2 className="text-lg font-semibold text-[var(--text-main)]">Заказчики</h2>
                    <p className="text-xs text-[var(--text-muted)]">{total} заказчиков в базе</p>
                </div>
                <div className="ml-auto flex flex-wrap gap-2 items-center">
                    <form onSubmit={handleSearch} className="flex gap-2">
                        <input
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            placeholder="Поиск по БИН / имени..."
                            className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-1 focus:ring-indigo-500"
                        />
                        <button type="submit" className="rounded-lg bg-indigo-500 px-3 py-1.5 text-sm text-white hover:bg-indigo-400 transition">
                            Найти
                        </button>
                    </form>

                    {/* Sort tabs */}
                    <div className="flex gap-1">
                        {(Object.keys(SORT_LABELS) as SortKey[]).map((k) => (
                            <button
                                key={k}
                                onClick={() => { setSortKey(k); setPage(1); }}
                                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${sortKey === k
                                    ? "bg-indigo-500 text-white"
                                    : "border border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"
                                    }`}
                            >
                                {SORT_LABELS[k]}
                            </button>
                        ))}
                    </div>

                    {/* Only with high risk */}
                    <button
                        onClick={() => { setOnlyWithHighLots((v) => !v); setPage(1); }}
                        className={`rounded-lg px-3 py-1.5 text-xs font-medium transition flex items-center gap-1.5 ${onlyWithHighLots
                            ? "bg-rose-500 text-white"
                            : "border border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"
                            }`}
                    >
                        <ShieldAlert className="h-3.5 w-3.5" />
                        Только HIGH
                    </button>

                    <button
                        onClick={() => { setPage(1); load(1, sortKey, onlyWithHighLots, search || undefined); }}
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
                            <th className="px-4 py-3 text-left font-medium text-[var(--text-muted)]">Заказчик</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">HIGH лоты</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">MED лоты</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">Тендеры</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">Лоты</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">Сумма</th>
                            <th className="px-4 py-3 text-right font-medium text-[var(--text-muted)]">Avg Score</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border)]">
                        {loading && items.length === 0 ? (
                            <tr>
                                <td colSpan={7} className="px-4 py-12 text-center text-[var(--text-muted)]">
                                    <RefreshCw className="h-5 w-5 animate-spin mx-auto mb-2" />
                                    Загрузка...
                                </td>
                            </tr>
                        ) : items.length === 0 ? (
                            <tr>
                                <td colSpan={7} className="px-4 py-12 text-center text-[var(--text-muted)]">Заказчики не найдены</td>
                            </tr>
                        ) : (
                            items.map((item, idx) => (
                                <tr
                                    key={`${item.bin}-${idx}`}
                                    onClick={() => setSelectedBin(item.bin)}
                                    className={`hover:bg-[var(--surface-hover)] transition cursor-pointer ${item.high_risk_lots > 0 ? "bg-rose-500/3" : ""}`}
                                >
                                    <td className="px-4 py-3">
                                        <div className="text-sm font-medium text-[var(--text-main)] truncate max-w-[240px]">
                                            {item.customer_name || "—"}
                                        </div>
                                        <div className="font-mono text-[10px] text-[var(--text-muted)]">{item.bin}</div>
                                    </td>
                                    <td className="px-4 py-3 text-right">
                                        {item.high_risk_lots > 0 ? (
                                            <span className="font-semibold text-rose-600 dark:text-rose-400">{item.high_risk_lots}</span>
                                        ) : (
                                            <span className="text-[var(--text-muted)]">0</span>
                                        )}
                                    </td>
                                    <td className="px-4 py-3 text-right">
                                        {item.medium_risk_lots > 0 ? (
                                            <span className="font-medium text-amber-600 dark:text-amber-400">{item.medium_risk_lots}</span>
                                        ) : (
                                            <span className="text-[var(--text-muted)]">0</span>
                                        )}
                                    </td>
                                    <td className="px-4 py-3 text-right text-[var(--text-main)]">{item.total_tenders}</td>
                                    <td className="px-4 py-3 text-right text-[var(--text-main)]">{item.total_lots}</td>
                                    <td className="px-4 py-3 text-right font-medium text-[var(--text-main)] whitespace-nowrap">
                                        {money(item.total_amount)}
                                    </td>
                                    <td className="px-4 py-3 text-right">
                                        <span className={`font-semibold ${riskColor(item.avg_risk_score)}`}>
                                            {item.avg_risk_score.toFixed(1)}
                                        </span>
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            {/* Load more */}
            {items.length < total && (
                <div className="flex justify-center">
                    <button
                        onClick={() => {
                            const next = page + 1;
                            setPage(next);
                            load(next, sortKey, onlyWithHighLots, search || undefined);
                        }}
                        disabled={loading}
                        className="rounded-xl border border-[var(--border)] px-6 py-2 text-sm text-[var(--text-muted)] hover:bg-[var(--surface-hover)] transition disabled:opacity-50"
                    >
                        {loading ? "Загрузка..." : `Загрузить ещё (${total - items.length})`}
                    </button>
                </div>
            )}
        </div>
    );
}
