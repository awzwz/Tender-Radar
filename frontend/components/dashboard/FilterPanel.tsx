"use client";

import React from "react";
import { Search } from "lucide-react";
import { Badge, ProgressBar } from "@/components/shared/ui";
import type { DashboardItem } from "@/lib/api";

interface FilterPanelProps {
    search: string;
    onSearchChange: (v: string) => void;
    minRisk: number;
    onMinRiskChange: (v: number) => void;
    levelFilter: string;
    onLevelFilterChange: (v: string) => void;
    selectedItem: DashboardItem | undefined;
    onOpenDetail: () => void;
}

export default function FilterPanel({
    search,
    onSearchChange,
    minRisk,
    onMinRiskChange,
    levelFilter,
    onLevelFilterChange,
    selectedItem,
    onOpenDetail,
}: FilterPanelProps) {
    return (
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
            {/* Search */}
            <div>
                <div className="text-xs text-[var(--text-muted)] mb-2 font-medium uppercase tracking-wider">Quick search</div>
                <div className="flex items-center gap-2 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2.5 focus-within:border-indigo-500/40 transition-colors">
                    <Search className="h-4 w-4 flex-shrink-0 text-[var(--text-muted)]" />
                    <input
                        value={search}
                        onChange={(e) => onSearchChange(e.target.value)}
                        placeholder="номер, заказчик..."
                        className="w-full bg-transparent text-sm outline-none placeholder:text-[var(--text-muted)] text-[var(--text-main)]"
                    />
                </div>
            </div>

            {/* Min risk slider */}
            <div>
                <div className="flex items-center justify-between text-xs text-[var(--text-muted)] mb-2">
                    <span className="font-medium uppercase tracking-wider">Min risk</span>
                    <span className="font-semibold text-slate-900 dark:text-white tabular-nums">{minRisk}</span>
                </div>
                <input
                    type="range"
                    min={0}
                    max={100}
                    value={minRisk}
                    onChange={(e) => onMinRiskChange(Number(e.target.value))}
                    className="w-full"
                />
            </div>

            {/* Level filter */}
            <div className="flex flex-wrap gap-1.5">
                {(["", "LOW", "MEDIUM", "HIGH"] as const).map((l) => (
                    <button
                        key={l}
                        onClick={() => onLevelFilterChange(l)}
                        className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-all ${levelFilter === l
                            ? "border-indigo-500/50 bg-indigo-50 dark:bg-indigo-500/20 text-indigo-700 dark:text-indigo-200 shadow-sm shadow-indigo-500/5 my-0.5"
                            : "border-[var(--border)] bg-[var(--surface-hover)] text-[var(--text-muted)] hover:border-[var(--border-hover)] hover:text-[var(--text-main)] my-0.5"
                            }`}
                    >
                        {l || "ALL"}
                    </button>
                ))}
            </div>

            {/* Selected lot preview */}
            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                <div className="flex items-center justify-between">
                    <div>
                        <div className="text-xs font-semibold text-[var(--text-main)]">Selected Lot</div>
                        <div className="text-[11px] text-[var(--text-muted)] truncate mt-0.5 max-w-[180px]">
                            {selectedItem?.tender_number || "Не выбран"}
                        </div>
                    </div>
                    {selectedItem && (
                        <button
                            onClick={onOpenDetail}
                            className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-2.5 py-1.5 text-[11px] text-[var(--text-muted)] hover:border-[var(--border-hover)] hover:text-[var(--text-main)] transition-all"
                        >
                            Open
                        </button>
                    )}
                </div>
                {selectedItem && (
                    <div className="mt-3 space-y-2">
                        <Badge level={selectedItem.risk_level} score={Math.round(selectedItem.risk_score)} />
                        <ProgressBar value={selectedItem.risk_score} />
                    </div>
                )}
            </div>
        </div>
    );
}
