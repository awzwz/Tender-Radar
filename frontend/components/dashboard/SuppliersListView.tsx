"use client";

import React, { useCallback, useEffect, useState } from "react";
import { api, SupplierListItem, SupplierProfile } from "@/lib/api";
import { money } from "@/components/shared/ui";
import { RefreshCw, X, AlertTriangle, ChevronDown, ChevronUp } from "lucide-react";

type SortKey = "total_sum" | "total_contracts" | "unique_customers";

interface SlideInProps {
    biin: string;
    onClose: () => void;
}

function SupplierSlideIn({ biin, onClose }: SlideInProps) {
    const [data, setData] = useState<SupplierProfile | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    useEffect(() => {
        setLoading(true);
        setError("");
        api.supplier(biin)
            .then(setData)
            .catch(() => setError("Не удалось загрузить профиль поставщика"))
            .finally(() => setLoading(false));
    }, [biin]);

    return (
        <div className="fixed inset-0 z-50 flex justify-end">
            <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={onClose} />
            <div className="relative w-full max-w-lg bg-[var(--surface)] border-l border-[var(--border)] shadow-2xl overflow-y-auto flex flex-col">
                <div className="sticky top-0 flex items-center justify-between px-5 py-4 border-b border-[var(--border)] bg-[var(--surface)] z-10">
                    <div>
                        <div className="text-sm font-semibold text-[var(--text-main)]">Профиль поставщика</div>
                        <div className="font-mono text-xs text-[var(--text-muted)]">{biin}</div>
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
                                <div className="text-xs text-[var(--text-muted)] mt-0.5">Зарегистрирован: {data.company.regdate?.slice(0, 10) || "—"}</div>
                                {data.rnu?.is_active && (
                                    <div className="mt-2 flex items-center gap-2 rounded-lg bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-600 dark:text-rose-400">
                                        <AlertTriangle className="h-4 w-4 flex-shrink-0" />
                                        В реестре недобросовестных поставщиков (РНУ)
                                    </div>
                                )}
                            </div>

                            <div className="grid grid-cols-3 gap-3">
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{data.stats.total_contracts}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Контракты</div>
                                </div>
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{money(data.stats.total_sum)}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Сумма</div>
                                </div>
                                <div className="rounded-xl bg-[var(--surface-hover)] p-3 text-center">
                                    <div className="text-lg font-bold text-[var(--text-main)]">{data.stats.unique_customers}</div>
                                    <div className="text-[10px] text-[var(--text-muted)]">Заказчиков</div>
                                </div>
                            </div>

                            {data.top_customers?.length > 0 && (
                                <div>
                                    <div className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-2">Топ заказчики</div>
                                    <div className="space-y-2">
                                        {data.top_customers.map((c) => (
                                            <div key={c.customer_bin} className="flex items-center justify-between rounded-lg bg-[var(--surface-hover)] px-3 py-2">
                                                <span className="font-mono text-xs text-[var(--text-muted)]">{c.customer_bin}</span>
                                                <div className="text-right">
                                                    <div className="text-xs font-medium text-[var(--text-main)]">{money(c.total_sum)}</div>
                                                    <div className="text-[10px] text-[var(--text-muted)]">{c.contract_count} контр.</div>
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

export default function SuppliersListView() {
    const [items, setItems] = useState<SupplierListItem[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState("");
    const [blacklisted, setBlacklisted] = useState<boolean | undefined>(undefined);
    const [sortKey, setSortKey] = useState<SortKey>("total_sum");
    const [selectedBiin, setSelectedBiin] = useState<string | null>(null);

    const load = useCallback(async (pg: number, sk: SortKey, bl?: boolean, q?: string) => {
        setLoading(true);
        try {
            const data = await api.suppliersList({
                page: pg,
                limit: 50,
                sort_by: sk,
                blacklisted: bl,
                search: q || undefined,
            });
            setTotal(data.total);
            if (pg === 1) {
                setItems(data.items);
            } else {
                setItems((prev) => {
                    const seen = new Set(prev.map((x) => x.biin));
                    return [...prev, ...data.items.filter((x) => !seen.has(x.biin))];
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
        load(1, sortKey, blacklisted, search || undefined);
    }, [sortKey, blacklisted, load]);

    function handleSearch(e: React.FormEvent) {
        e.preventDefault();
        setPage(1);
        load(1, sortKey, blacklisted, search || undefined);
    }

    function SortIcon({ k }: { k: SortKey }) {
        if (sortKey !== k) return null;
        return <ChevronDown className="inline h-3 w-3 ml-0.5" />;
    }

    return (
        <div className="space-y-4">
            {/* Slide-in panel */}
            {selectedBiin && (
                <SupplierSlideIn biin={selectedBiin} onClose={() => setSelectedBiin(null)} />
            )}

            {/* Header */}
            <div className="flex flex-wrap items-center gap-3">
                <div>
                    <h2 className="text-lg font-semibold text-[var(--text-main)]">Поставщики</h2>
                    <p className="text-xs text-[var(--text-muted)]">{total} поставщиков в базе</p>
                </div>
                <div className="ml-auto flex flex-wrap gap-2 items-center">
                    <form onSubmit={handleSearch} className="flex gap-2">
                        <input
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            placeholder="Поиск по БИИН / имени..."
                            className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-1 focus:ring-indigo-500"
                        />
                        <button type="submit" className="rounded-lg bg-indigo-500 px-3 py-1.5 text-sm text-white hover:bg-indigo-400 transition">
                            Найти
                        </button>
                    </form>
                    {/* RNU filter */}
                    <div className="flex gap-1">
                        {([undefined, true, false] as (boolean | undefined)[]).map((v) => {
                            const label = v === undefined ? "Все" : v ? "РНУ" : "Без РНУ";
                            const active = blacklisted === v;
                            return (
                                <button
                                    key={String(v)}
                                    onClick={() => { setBlacklisted(v); setPage(1); }}
                                    className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${active
                                        ? "bg-indigo-500 text-white"
                                        : "border border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"
                                        }`}
                                >
                                    {label}
                                </button>
                            );
                        })}
                    </div>
                    <button
                        onClick={() => { setPage(1); load(1, sortKey, blacklisted, search || undefined); }}
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
                            <th className="px-4 py-3 text-left font-medium text-[var(--text-muted)]">Поставщик</th>
                            <th
                                className="px-4 py-3 text-right font-medium text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-main)]"
                                onClick={() => setSortKey("total_contracts")}
                            >
                                Контракты <SortIcon k="total_contracts" />
                            </th>
                            <th
                                className="px-4 py-3 text-right font-medium text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-main)]"
                                onClick={() => setSortKey("total_sum")}
                            >
                                Сумма <SortIcon k="total_sum" />
                            </th>
                            <th
                                className="px-4 py-3 text-right font-medium text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-main)]"
                                onClick={() => setSortKey("unique_customers")}
                            >
                                Заказчики <SortIcon k="unique_customers" />
                            </th>
                            <th className="px-4 py-3 text-center font-medium text-[var(--text-muted)]">РНУ</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border)]">
                        {loading && items.length === 0 ? (
                            <tr>
                                <td colSpan={5} className="px-4 py-12 text-center text-[var(--text-muted)]">
                                    <RefreshCw className="h-5 w-5 animate-spin mx-auto mb-2" />
                                    Загрузка...
                                </td>
                            </tr>
                        ) : items.length === 0 ? (
                            <tr>
                                <td colSpan={5} className="px-4 py-12 text-center text-[var(--text-muted)]">Поставщики не найдены</td>
                            </tr>
                        ) : (
                            items.map((item, idx) => (
                                <tr
                                    key={`${item.biin}-${idx}`}
                                    onClick={() => setSelectedBiin(item.biin)}
                                    className={`hover:bg-[var(--surface-hover)] transition cursor-pointer ${item.is_blacklisted ? "bg-rose-500/5" : ""}`}
                                >
                                    <td className="px-4 py-3">
                                        <div className="text-sm font-medium text-[var(--text-main)] truncate max-w-[240px]">
                                            {item.name_ru || "—"}
                                        </div>
                                        <div className="font-mono text-[10px] text-[var(--text-muted)]">{item.biin}</div>
                                    </td>
                                    <td className="px-4 py-3 text-right font-medium text-[var(--text-main)]">
                                        {item.total_contracts}
                                    </td>
                                    <td className="px-4 py-3 text-right font-medium text-[var(--text-main)] whitespace-nowrap">
                                        {money(item.total_sum)}
                                    </td>
                                    <td className="px-4 py-3 text-right text-[var(--text-main)]">
                                        {item.unique_customers}
                                    </td>
                                    <td className="px-4 py-3 text-center">
                                        {item.is_blacklisted ? (
                                            <span className="rounded-md bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300 px-2 py-0.5 text-[10px] font-semibold">
                                                РНУ
                                            </span>
                                        ) : (
                                            <span className="text-[var(--text-muted)] text-xs">—</span>
                                        )}
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
                            load(next, sortKey, blacklisted, search || undefined);
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
