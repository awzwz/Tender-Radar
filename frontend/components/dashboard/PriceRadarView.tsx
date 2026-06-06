"use client";

import React, { useCallback, useEffect, useState } from "react";
import { TrendingUp, Search, AlertTriangle, Package, ExternalLink } from "lucide-react";
import { api, OverpricedLot, PriceStats } from "@/lib/api";
import { money } from "@/components/shared/ui";
import { StatCard } from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";
import ProductPriceModal from "@/components/dashboard/ProductPriceModal";

const tenge = (n: number | null | undefined) =>
    n == null ? "—" : money(Number(n));

function RatioBadge({ ratio }: { ratio: number | null | undefined }) {
    if (ratio == null) return null;
    const tone =
        ratio >= 3 ? "bg-rose-500/15 text-rose-600 dark:text-rose-300 border-rose-500/30"
            : ratio >= 1.5 ? "bg-amber-500/15 text-amber-600 dark:text-amber-300 border-amber-500/30"
                : "bg-slate-500/10 text-slate-600 dark:text-slate-300 border-slate-500/20";
    return (
        <span className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold ${tone}`}>
            ×{ratio.toFixed(2)}
        </span>
    );
}

/** Mini bar: how far the lot's unit price sits above the market median. */
function PriceBar({ unit, median }: { unit: number | null | undefined; median: number | null | undefined }) {
    if (!unit || !median || median <= 0) return null;
    const ratio = unit / median;
    const medianPct = Math.min(100 / ratio, 100); // median position relative to lot price (=100%)
    return (
        <div className="mt-1 h-2 w-full rounded-full bg-[var(--border)] overflow-hidden relative">
            {/* market median marker zone */}
            <div className="absolute inset-y-0 left-0 bg-emerald-500/70" style={{ width: `${medianPct}%` }} />
            {/* overpay zone */}
            <div className="absolute inset-y-0 bg-rose-500/70" style={{ left: `${medianPct}%`, right: 0 }} />
        </div>
    );
}

export default function PriceRadarView({ onOpenLot }: { onOpenLot?: (id: number) => void }) {
    const { t } = useI18n();
    const [stats, setStats] = useState<PriceStats | null>(null);
    const [items, setItems] = useState<OverpricedLot[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [search, setSearch] = useState("");
    const [minRatio, setMinRatio] = useState(1.5);
    const [loading, setLoading] = useState(false);
    const [product, setProduct] = useState<{ code: string; unit: string | null; lotId: number } | null>(null);

    useEffect(() => {
        api.priceStats().then(setStats).catch(() => setStats(null));
    }, []);

    const load = useCallback(async (pg = 1, append = false) => {
        setLoading(true);
        try {
            const data = await api.overpricedLots({ page: pg, limit: 50, min_ratio: minRatio, search: search || undefined });
            setTotal(data.total);
            setItems((prev) => (append ? [...prev, ...data.items] : data.items));
        } catch {
            if (!append) setItems([]);
        } finally {
            setLoading(false);
        }
    }, [minRatio, search]);

    useEffect(() => {
        setPage(1);
        load(1, false);
    }, [load]);

    return (
        <div className="space-y-5">
            {/* Header */}
            <div className="flex items-center gap-3">
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2.5">
                    <TrendingUp className="h-5 w-5 text-indigo-500" />
                </div>
                <div>
                    <h2 className="text-lg font-semibold text-[var(--text-main)]">{t("price.title")}</h2>
                    <p className="text-xs text-[var(--text-muted)]">{t("price.subtitle")}</p>
                </div>
            </div>

            {/* Stat cards */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <StatCard label={t("price.stat.overpriced")} value={stats?.overpriced_lots ?? "—"} tone="danger" subtitle={t("price.stat.overpriced.sub")} />
                <StatCard label={t("price.stat.overpay")} value={stats ? money(stats.total_overpay_estimate) : "—"} tone="warn" subtitle={t("price.stat.overpay.sub")} />
                <StatCard label={t("price.stat.products")} value={stats?.products_benchmarked ?? "—"} tone="neutral" subtitle={t("price.stat.products.sub")} />
                <StatCard label={t("price.stat.evaluated")} value={stats?.lots_evaluated ?? "—"} tone="ok" subtitle={t("price.stat.evaluated.sub")} />
            </div>

            {/* Controls */}
            <div className="flex flex-wrap items-center gap-3">
                <div className="relative flex-1 min-w-[220px]">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--text-muted)]" />
                    <input
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        placeholder={t("price.search.ph")}
                        className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] py-2.5 pl-10 pr-4 text-sm text-[var(--text-main)] focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    />
                </div>
                <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
                    <span>{t("price.minRatio")}</span>
                    {[1.5, 2, 3, 5].map((r) => (
                        <button
                            key={r}
                            onClick={() => setMinRatio(r)}
                            className={`rounded-lg border px-2.5 py-1.5 font-medium transition ${minRatio === r
                                ? "border-indigo-500 bg-indigo-500/10 text-indigo-600 dark:text-indigo-300"
                                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"}`}
                        >
                            ×{r}
                        </button>
                    ))}
                </div>
            </div>

            {/* Table */}
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
                <div className="grid grid-cols-12 gap-2 px-4 py-3 text-[11px] font-semibold uppercase tracking-wider text-[var(--text-muted)] border-b border-[var(--border)]">
                    <div className="col-span-4">{t("price.col.product")}</div>
                    <div className="col-span-3">{t("price.col.customer")}</div>
                    <div className="col-span-2 text-right">{t("price.col.price")}</div>
                    <div className="col-span-1 text-right">{t("price.col.market")}</div>
                    <div className="col-span-1 text-center">{t("price.col.ratio")}</div>
                    <div className="col-span-1 text-right">{t("price.col.overpay")}</div>
                </div>

                {loading && items.length === 0 && (
                    <div className="py-16 text-center text-sm text-[var(--text-muted)]">{t("price.loading")}</div>
                )}
                {!loading && items.length === 0 && (
                    <div className="py-16 text-center text-sm text-[var(--text-muted)] flex flex-col items-center gap-2">
                        <Package className="h-8 w-8" />
                        {t("price.empty")}
                    </div>
                )}

                {items.map((it) => (
                    <button
                        key={it.id}
                        onClick={() => it.enstru_code
                            ? setProduct({ code: it.enstru_code, unit: it.unit_code, lotId: it.id })
                            : onOpenLot?.(it.id)}
                        className="w-full grid grid-cols-12 gap-2 px-4 py-3 text-sm border-b border-[var(--border)] last:border-0 hover:bg-[var(--surface-hover)] transition text-left items-center"
                    >
                        <div className="col-span-4 min-w-0">
                            <div className="flex items-center gap-1.5 font-medium text-[var(--text-main)] truncate">
                                {(it.ratio ?? 0) >= 3 && <AlertTriangle className="h-3.5 w-3.5 flex-shrink-0 text-rose-500" />}
                                <span className="truncate">{it.enstru_name || it.name_ru || "—"}</span>
                                <ExternalLink className="h-3 w-3 flex-shrink-0 text-[var(--text-muted)] opacity-0 group-hover:opacity-100" />
                            </div>
                            <div className="text-[11px] text-[var(--text-muted)] truncate">
                                {it.enstru_code} · {t("price.sample", { n: it.sample_size ?? 0 })}
                            </div>
                            <PriceBar unit={it.unit_price} median={it.median_market} />
                        </div>
                        <div className="col-span-3 min-w-0 text-xs text-[var(--text-muted)] truncate" title={it.customer_name || ""}>
                            {it.customer_name || it.customer_bin || "—"}
                        </div>
                        <div className="col-span-2 text-right font-semibold text-rose-600 dark:text-rose-300">
                            {tenge(it.unit_price)}
                        </div>
                        <div className="col-span-1 text-right text-xs text-emerald-600 dark:text-emerald-300">
                            {tenge(it.median_market)}
                        </div>
                        <div className="col-span-1 text-center">
                            <RatioBadge ratio={it.ratio} />
                        </div>
                        <div className="col-span-1 text-right text-xs font-medium text-[var(--text-main)]">
                            {tenge(it.overpay_estimate)}
                        </div>
                    </button>
                ))}
            </div>

            {/* Footer / load more */}
            <div className="flex items-center justify-between text-xs text-[var(--text-muted)]">
                <span>{t("price.shown", { shown: items.length, total })}</span>
                {items.length < total && (
                    <button
                        onClick={() => { const n = page + 1; setPage(n); load(n, true); }}
                        disabled={loading}
                        className="rounded-xl border border-[var(--border)] px-4 py-2 text-[var(--text-main)] hover:bg-[var(--surface-hover)] disabled:opacity-50 transition"
                    >
                        {loading ? t("price.loading") : t("price.loadMore")}
                    </button>
                )}
            </div>

            {product && (
                <ProductPriceModal
                    enstruCode={product.code}
                    unitCode={product.unit}
                    selectedLotId={product.lotId}
                    onClose={() => setProduct(null)}
                    onOpenLot={onOpenLot}
                />
            )}
        </div>
    );
}
