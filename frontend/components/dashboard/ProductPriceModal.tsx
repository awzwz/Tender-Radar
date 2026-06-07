"use client";

import React, { useEffect, useMemo, useState } from "react";
import { X, ExternalLink, AlertTriangle } from "lucide-react";
import { api, ProductBenchmark, ProductBenchmarkRow } from "@/lib/api";
import { money } from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";

const tenge = (n: number | null | undefined) => (n == null ? "—" : money(Number(n)));

/** Log-scale strip plot of unit prices for one product, with quartile markers. */
function Distribution({
    lots, bench, selectedId,
}: {
    lots: ProductBenchmark["lots"];
    bench: ProductBenchmarkRow;
    selectedId?: number;
}) {
    const prices = lots.map((l) => Number(l.unit_price)).filter((p) => p > 0);
    if (prices.length === 0) return null;
    const lo = Math.min(...prices, bench.q1, bench.min_price ?? bench.q1);
    const hi = Math.max(...prices, bench.upper_fence, bench.max_price ?? bench.upper_fence);
    const L = Math.log10(Math.max(lo, 1));
    const H = Math.log10(Math.max(hi, 10));
    const span = H - L || 1;
    const W = 1000, padX = 20;
    const x = (v: number) => padX + ((Math.log10(Math.max(v, 1)) - L) / span) * (W - 2 * padX);

    const marker = (v: number, color: string, label: string, dash = false) => (
        <g key={label}>
            <line x1={x(v)} x2={x(v)} y1={18} y2={78} stroke={color} strokeWidth={1.5} strokeDasharray={dash ? "4 3" : undefined} />
            <text x={x(v)} y={12} fill={color} fontSize={11} textAnchor="middle">{label}</text>
            <text x={x(v)} y={94} fill="var(--text-muted)" fontSize={10} textAnchor="middle">{money(v)}</text>
        </g>
    );

    return (
        <svg viewBox={`0 0 ${W} 104`} className="w-full" style={{ height: 130 }}>
            {/* baseline */}
            <line x1={padX} x2={W - padX} y1={48} y2={48} stroke="var(--border)" strokeWidth={1} />
            {/* quartile band Q1..Q3 */}
            <rect x={x(bench.q1)} y={40} width={Math.max(1, x(bench.q3) - x(bench.q1))} height={16} fill="rgb(16 185 129 / 0.15)" />
            {marker(bench.median_price, "rgb(16 185 129)", "median")}
            {marker(bench.q1, "var(--text-muted)", "Q1", true)}
            {marker(bench.q3, "var(--text-muted)", "Q3", true)}
            {marker(bench.upper_fence, "rgb(244 63 94)", "fence")}
            {/* dots */}
            {lots.map((l) => {
                const p = Number(l.unit_price);
                if (!p || p <= 0) return null;
                const over = l.overpriced;
                const sel = l.id === selectedId;
                return (
                    <circle
                        key={l.id}
                        cx={x(p)} cy={48}
                        r={sel ? 6 : 4}
                        fill={over ? "rgb(244 63 94)" : "rgb(99 102 241)"}
                        fillOpacity={sel ? 1 : 0.55}
                        stroke={sel ? "var(--text-main)" : "none"}
                        strokeWidth={sel ? 2 : 0}
                    />
                );
            })}
        </svg>
    );
}

export default function ProductPriceModal({
    enstruCode, unitCode, selectedLotId, onClose, onOpenLot,
}: {
    enstruCode: string;
    unitCode?: string | null;
    selectedLotId?: number;
    onClose: () => void;
    onOpenLot?: (id: number) => void;
}) {
    const { t } = useI18n();
    const [data, setData] = useState<ProductBenchmark | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        setLoading(true);
        api.productBenchmark(enstruCode)
            .then(setData)
            .catch(() => setData(null))
            .finally(() => setLoading(false));
    }, [enstruCode]);

    const bench = useMemo<ProductBenchmarkRow | null>(() => {
        if (!data?.benchmarks?.length) return null;
        return data.benchmarks.find((b) => b.unit_code === unitCode) ?? data.benchmarks[0];
    }, [data, unitCode]);

    const lots = useMemo(() => {
        if (!data) return [];
        const u = bench?.unit_code;
        const filtered = u ? data.lots.filter((l) => l.unit_code === u) : data.lots;
        return [...filtered].sort((a, b) => Number(b.unit_price) - Number(a.unit_price));
    }, [data, bench]);

    return (
        <div className="animate-fade-in fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4" onClick={onClose}>
            <div
                className="animate-scale-in w-full max-w-3xl max-h-[88vh] overflow-y-auto rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-2xl"
                onClick={(e) => e.stopPropagation()}
            >
                {/* Header */}
                <div className="sticky top-0 flex items-start justify-between gap-4 border-b border-[var(--border)] bg-[var(--surface)] px-5 py-4">
                    <div className="min-w-0">
                        <div className="text-sm font-semibold text-[var(--text-main)] truncate">
                            {bench?.enstru_name || data?.lots[0]?.name_ru || t("price.dist.title")}
                        </div>
                        <div className="text-[11px] text-[var(--text-muted)]">
                            {enstruCode}{bench?.unit_code ? ` · ${t("price.col.unit") || "ед."} ${bench.unit_code}` : ""}
                        </div>
                    </div>
                    <button onClick={onClose} className="rounded-lg p-1.5 text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)]">
                        <X className="h-5 w-5" />
                    </button>
                </div>

                <div className="p-5 space-y-5">
                    {loading && <div className="py-10 text-center text-sm text-[var(--text-muted)]">{t("price.loading")}</div>}
                    {!loading && !bench && (
                        <div className="py-10 text-center text-sm text-[var(--text-muted)]">{t("price.dist.noBench")}</div>
                    )}

                    {bench && (
                        <>
                            {/* Stats */}
                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                                {[
                                    { l: t("price.dist.median"), v: tenge(bench.median_price), c: "text-emerald-600 dark:text-emerald-300" },
                                    { l: t("price.dist.fence"), v: tenge(bench.upper_fence), c: "text-rose-600 dark:text-rose-300" },
                                    { l: "Q1 – Q3", v: `${money(bench.q1)} – ${money(bench.q3)}`, c: "text-[var(--text-main)]" },
                                    { l: t("price.dist.samples"), v: String(bench.n_samples), c: "text-[var(--text-main)]" },
                                ].map((s) => (
                                    <div key={s.l} className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                                        <div className="text-[10px] uppercase tracking-wider text-[var(--text-muted)]">{s.l}</div>
                                        <div className={`mt-1 text-sm font-semibold ${s.c}`}>{s.v}</div>
                                    </div>
                                ))}
                            </div>

                            {/* Distribution plot */}
                            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                                <div className="mb-1 text-xs text-[var(--text-muted)]">{t("price.dist.plotHint")}</div>
                                <Distribution lots={lots} bench={bench} selectedId={selectedLotId} />
                            </div>

                            {/* Lot list */}
                            <div className="rounded-xl border border-[var(--border)] overflow-hidden">
                                <div className="grid grid-cols-12 gap-2 px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-[var(--text-muted)] border-b border-[var(--border)]">
                                    <div className="col-span-6">{t("price.col.customer")}</div>
                                    <div className="col-span-2 text-right">{t("price.col.price")}</div>
                                    <div className="col-span-2 text-right">{t("price.dist.qty")}</div>
                                    <div className="col-span-2 text-right" />
                                </div>
                                {lots.map((l) => (
                                    <div
                                        key={l.id}
                                        className={`grid grid-cols-12 gap-2 px-3 py-2 text-xs border-b border-[var(--border)] last:border-0 items-center ${l.id === selectedLotId ? "bg-indigo-500/10" : ""}`}
                                    >
                                        <div className="col-span-6 min-w-0 truncate text-[var(--text-muted)]" title={l.customer_name || ""}>
                                            {l.overpriced && <AlertTriangle className="inline h-3 w-3 mr-1 text-rose-500" />}
                                            {l.customer_name || "—"}
                                        </div>
                                        <div className={`col-span-2 text-right font-medium ${l.overpriced ? "text-rose-600 dark:text-rose-300" : "text-[var(--text-main)]"}`}>
                                            {tenge(l.unit_price)}
                                        </div>
                                        <div className="col-span-2 text-right text-[var(--text-muted)]">{l.count ?? "—"}</div>
                                        <div className="col-span-2 text-right">
                                            {onOpenLot && (
                                                <button onClick={() => onOpenLot(l.id)} className="inline-flex items-center gap-1 text-indigo-500 hover:underline">
                                                    {t("action.open")} <ExternalLink className="h-3 w-3" />
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
