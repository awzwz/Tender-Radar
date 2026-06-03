"use client";

import React, { useEffect, useMemo, useState } from "react";
import { geoMercator, geoPath } from "d3-geo";
import { MapPin, Plus, Minus, Maximize2 } from "lucide-react";
import { api, RegionStat } from "@/lib/api";
import { useI18n } from "@/components/providers/LanguageProvider";

const W = 760;
const H = 460;
const PAD = 14;
const MIN_ZOOM = 1;
const MAX_ZOOM = 8;

const lerp = (a: number, b: number, t: number) => Math.round(a + (b - a) * t);

// emerald (16,185,129) → amber (245,158,11) → rose (244,63,94)
function colorScale(t: number): string {
    const c = Math.max(0, Math.min(1, t));
    if (c < 0.5) {
        const k = c / 0.5;
        return `rgb(${lerp(16, 245, k)},${lerp(185, 158, k)},${lerp(129, 11, k)})`;
    }
    const k = (c - 0.5) / 0.5;
    return `rgb(${lerp(245, 244, k)},${lerp(158, 63, k)},${lerp(11, 94, k)})`;
}

// Short region labels (Russian) shown on the map.
const LABEL: Record<string, string> = {
    "Astana": "Астана",
    "Almaty": "Алматы",
    "Almaty Region": "Алматинская",
    "South Kazakhstan Region": "Туркестанская",
    "Karaganda Region": "Карагандинская",
    "Aktobe Region": "Актюбинская",
    "Atyrau Region": "Атырауская",
    "Kostanay Region": "Костанайская",
    "Pavlodar Region": "Павлодарская",
    "Kyzylorda Region": "Кызылординская",
    "Jambyl Region": "Жамбылская",
    "West Kazakhstan Region": "Западно-Казахст.",
    "East Kazakhstan Region": "Восточно-Казахст.",
    "North Kazakhstan Region": "Северо-Казахст.",
    "Akmola Region": "Акмолинская",
    "Mangystau Region": "Мангистауская",
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Feature = any;

export default function KZMap() {
    const { t } = useI18n();
    const [regions, setRegions] = useState<RegionStat[]>([]);
    const [features, setFeatures] = useState<Feature[]>([]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const [geo, setGeo] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [selected, setSelected] = useState<RegionStat | null>(null);
    const [hover, setHover] = useState<string | null>(null);
    const [zoom, setZoom] = useState(1);

    useEffect(() => {
        let alive = true;
        Promise.all([
            api.dashboardRegions().then((d) => d.regions || []).catch(() => [] as RegionStat[]),
            fetch("/geo/kz-regions.json").then((r) => r.json()).catch(() => null),
        ]).then(([rs, g]) => {
            if (!alive) return;
            setRegions(rs);
            if (g && g.features) { setGeo(g); setFeatures(g.features); }
            const top = [...rs].filter((r) => r.count > 0).sort((a, b) => b.avg_risk - a.avg_risk)[0] || null;
            setSelected(top);
            setLoading(false);
        });
        return () => { alive = false; };
    }, []);

    // Fit Kazakhstan to the frame, then build an SVG path generator.
    const pathGen = useMemo(() => {
        if (!geo) return null;
        const projection = geoMercator().fitExtent([[24, 18], [W - 24, H - 30]], geo);
        return geoPath(projection);
    }, [geo]);

    const byId = useMemo(() => {
        const m: Record<string, RegionStat> = {};
        regions.forEach((r) => { m[r.id] = r; });
        return m;
    }, [regions]);

    const [min, max] = useMemo(() => {
        const vals = regions.filter((r) => r.count > 0).map((r) => r.avg_risk);
        if (!vals.length) return [0, 1];
        return [Math.min(...vals), Math.max(...vals)];
    }, [regions]);

    const fillFor = (id: string): string => {
        const r = byId[id];
        if (!r || r.count === 0) return "#e2e8f0";
        const tt = max > min ? (r.avg_risk - min) / (max - min) : 0.5;
        return colorScale(tt);
    };

    const active = (hover && byId[hover]) || selected;
    const levelRows: [string, string, number][] = active
        ? [["high", "bg-rose-500", active.high], ["medium", "bg-amber-500", active.medium], ["low", "bg-emerald-500", active.low]]
        : [];

    const zoomBy = (f: number) => setZoom((z) => Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, +(z * f).toFixed(2))));
    const reset = () => setZoom(1);

    const btn = "h-8 w-8 flex items-center justify-center rounded-lg border border-[var(--border)] bg-[var(--surface)] text-[var(--text-muted)] hover:text-[var(--text-main)] hover:border-[var(--border-hover)] shadow-sm transition-colors disabled:opacity-40 disabled:cursor-not-allowed";

    return (
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-5">
            <div className="flex items-center gap-3 mb-1">
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2">
                    <MapPin className="h-4 w-4 text-indigo-500" />
                </div>
                <div>
                    <div className="text-sm font-semibold text-[var(--text-main)]">{t("map.title")}</div>
                    <div className="text-xs text-[var(--text-muted)]">{t("map.hint")}</div>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-[1fr_260px] gap-4 mt-4">
                {/* Map */}
                <div className="relative rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] overflow-hidden">
                    {/* Zoom controls */}
                    <div className="absolute top-2 right-2 z-10 flex flex-col gap-1.5">
                        <button onClick={() => zoomBy(1.6)} disabled={zoom >= MAX_ZOOM} title={t("map.zoomIn")} aria-label={t("map.zoomIn")} className={btn}>
                            <Plus className="h-4 w-4" />
                        </button>
                        <button onClick={() => zoomBy(1 / 1.6)} disabled={zoom <= MIN_ZOOM} title={t("map.zoomOut")} aria-label={t("map.zoomOut")} className={btn}>
                            <Minus className="h-4 w-4" />
                        </button>
                        <button onClick={reset} title={t("map.reset")} aria-label={t("map.reset")} className={btn}>
                            <Maximize2 className="h-4 w-4" />
                        </button>
                    </div>

                    {loading || !pathGen ? (
                        <div className="flex items-center justify-center h-[420px] text-sm text-[var(--text-muted)]">
                            {t("map.loading")}
                        </div>
                    ) : (
                        <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto block" role="img">
                            <g transform={`translate(${W / 2} ${H / 2}) scale(${zoom}) translate(${-W / 2} ${-H / 2})`}>
                                {features.map((f, i) => {
                                    const id = (f.properties?.shapeName as string) || `r${i}`;
                                    const r = byId[id];
                                    const d = pathGen(f) || "";
                                    const isActive = active?.id === id;
                                    return (
                                        <path
                                            key={id}
                                            d={d}
                                            fill={fillFor(id)}
                                            stroke={isActive ? "#4f46e5" : "#ffffff"}
                                            strokeWidth={(isActive ? 2 : 0.8) / zoom}
                                            style={{ cursor: r ? "pointer" : "default", transition: "fill 120ms" }}
                                            onClick={() => r && setSelected(r)}
                                            onMouseEnter={() => setHover(id)}
                                            onMouseLeave={() => setHover(null)}
                                        />
                                    );
                                })}
                                {features.map((f, i) => {
                                    const id = (f.properties?.shapeName as string) || `r${i}`;
                                    const c = pathGen.centroid(f);
                                    if (!c || isNaN(c[0]) || isNaN(c[1])) return null;
                                    const label = LABEL[id] || byId[id]?.name_ru || id;
                                    return (
                                        <text
                                            key={`label-${id}`}
                                            x={c[0]}
                                            y={c[1]}
                                            textAnchor="middle"
                                            dominantBaseline="central"
                                            fontSize={10 / zoom}
                                            fontWeight={600}
                                            fill="#1e293b"
                                            stroke="#ffffff"
                                            strokeWidth={3 / zoom}
                                            paintOrder="stroke"
                                            style={{ pointerEvents: "none", userSelect: "none" }}
                                        >
                                            {label}
                                        </text>
                                    );
                                })}
                            </g>
                        </svg>
                    )}

                    {/* Legend */}
                    <div className="flex items-center gap-2 px-4 py-2 text-[10px] text-[var(--text-muted)] border-t border-[var(--border)]">
                        <span>{t("map.legend.low")}</span>
                        <div className="h-2 flex-1 rounded-full" style={{ background: "linear-gradient(90deg, rgb(16,185,129), rgb(245,158,11), rgb(244,63,94))" }} />
                        <span>{t("map.legend.high")}</span>
                    </div>
                </div>

                {/* Stats panel */}
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-4 flex flex-col min-h-[280px]">
                    {active ? (
                        <>
                            <div className="text-sm font-semibold text-[var(--text-main)]">{active.name_ru}</div>
                            <div className="mt-3 flex items-end gap-2">
                                <div className="text-4xl font-bold text-[var(--text-main)] tabular-nums">{active.avg_risk}</div>
                                <div className="text-xs text-[var(--text-muted)] mb-1.5">{t("map.avgRisk")}</div>
                            </div>
                            <div className="mt-3 text-xs text-[var(--text-muted)]">
                                {t("map.lots")}: <span className="font-semibold text-[var(--text-main)] tabular-nums">{active.count}</span>
                            </div>
                            <div className="mt-4 space-y-2">
                                {levelRows.map(([k, c, v]) => (
                                    <div key={k} className="flex items-center gap-2 text-xs">
                                        <span className={`h-2 w-2 rounded-full ${c}`} />
                                        <span className="text-[var(--text-muted)] w-16">{t(`map.${k}`)}</span>
                                        <span className="font-semibold text-[var(--text-main)] tabular-nums">{v}</span>
                                    </div>
                                ))}
                            </div>
                        </>
                    ) : (
                        <div className="flex-1 flex items-center justify-center text-center text-xs text-[var(--text-muted)]">
                            {t("map.selectHint")}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
