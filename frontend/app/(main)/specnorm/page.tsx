"use client";

import React, { useMemo } from "react";
import { useRouter } from "next/navigation";
import {
  LayoutDashboard, List, ShieldAlert, Sparkles, RefreshCw,
  FileSearch, Users, Scale, TrendingUp,
} from "lucide-react";
import { useFindingsData } from "@/lib/specnorm/useSpecNormData";
import {
  getRiskTier, getDocRiskTier,
  type TenderCategory, type RiskTier,
  CATEGORY_LABELS, DOC_VIOLATION_LABELS, FLAG_LABELS,
} from "@/lib/specnorm/types";
import type { DocViolationType } from "@/lib/specnorm/types";
import {
  SpecNormShell, StatCard, ChartCard, DonutChart, MiniBar,
  RiskBadge, DocBadge, FlagChip, SeverityBar, ComplianceBar,
  CategoryBadge,
} from "@/lib/specnorm/components";

const money = (n: number) =>
  new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

export default function SpecNormDashboard() {
  const router = useRouter();
  const { data, loading } = useFindingsData();

  const stats = useMemo(() => {
    const total = data.length;
    const specFlagged = data.filter((d) => d.isFlagged).length;
    const avgSpec = total > 0 ? Math.round(data.reduce((s, d) => s + d.specDeviationScore, 0) / total) : 0;
    const avgDoc = total > 0 ? Math.round(data.reduce((s, d) => s + (d.docComplianceScore ?? 100), 0) / total) : 0;
    const docViolations = data.filter((d) => d.docViolations && d.docViolations.length > 0).length;

    const riskHigh = data.filter((d) => d.overallRiskTier === "HIGH").length;
    const riskMed = data.filter((d) => d.overallRiskTier === "MEDIUM").length;
    const riskLow = data.filter((d) => d.overallRiskTier === "LOW").length;

    const byCategory: Record<string, number> = {};
    data.forEach((d) => { byCategory[d.category] = (byCategory[d.category] || 0) + 1; });

    return { total, specFlagged, avgSpec, avgDoc, docViolations, riskHigh, riskMed, riskLow, byCategory };
  }, [data]);

  const violationTypes = useMemo(() => {
    const counts: Record<string, number> = {};
    data.forEach((d) => {
      (d.docViolations || []).forEach((v) => {
        counts[v.type] = (counts[v.type] || 0) + 1;
      });
    });
    return counts;
  }, [data]);

  const regionStats = useMemo(() => {
    const map: Record<string, number> = {};
    data.filter((d) => d.overallRiskTier === "HIGH" || d.overallRiskTier === "MEDIUM").forEach((d) => {
      map[d.region] = (map[d.region] || 0) + 1;
    });
    return Object.entries(map)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([label, value]) => ({ label, value, color: "bg-indigo-500" }));
  }, [data]);

  const topRisky = useMemo(() => {
    return [...data]
      .filter((d) => d.overallRiskTier === "HIGH" || d.overallRiskTier === "MEDIUM")
      .sort((a, b) => {
        const scoreA = a.specDeviationScore + (100 - (a.docComplianceScore ?? 100));
        const scoreB = b.specDeviationScore + (100 - (b.docComplianceScore ?? 100));
        return scoreB - scoreA;
      })
      .slice(0, 5);
  }, [data]);

  if (loading) {
    return (
      <SpecNormShell title="Tender Analysis" subtitle="Загрузка данных...">
        <div className="flex items-center justify-center py-24 text-[var(--text-muted)]">
          <RefreshCw className="h-6 w-6 animate-spin mr-2" /> Загрузка...
        </div>
      </SpecNormShell>
    );
  }

  return (
    <SpecNormShell title="Tender Analysis" subtitle="Двусторонняя проверка: Заказчик + Поставщик · Demo">
      <div className="space-y-5">
        {/* Section Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-2 shadow-sm shadow-[var(--border-hover)]">
              <LayoutDashboard className="h-4 w-4 text-indigo-500 dark:text-indigo-200" />
            </div>
            <div>
              <div className="text-sm font-semibold text-[var(--text-main)]">Dashboard</div>
              <div className="text-xs text-[var(--text-muted)]">Аналитика завершённых тендеров: Spec vs Norm + Document Check</div>
            </div>
          </div>
          <a
            href="/specnorm/lots"
            className="flex items-center gap-1.5 rounded-xl border border-indigo-500/30 bg-indigo-500/10 px-3 py-2 text-xs text-indigo-700 dark:text-indigo-200 hover:bg-indigo-500/20 transition"
          >
            <List className="h-3.5 w-3.5" /> Все тендеры
          </a>
        </div>

        {/* KPI Cards — Two rows */}
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
            <Scale className="h-3.5 w-3.5" /> Проверка Заказчика (Spec vs Norm)
          </div>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <StatCard label="Всего тендеров" value={stats.total} tone="neutral" />
            <StatCard label="С завышенными требованиями" value={stats.specFlagged} sub={`${stats.total > 0 ? Math.round((stats.specFlagged / stats.total) * 100) : 0}%`} tone="danger" />
            <StatCard label="Avg Spec Deviation" value={stats.avgSpec} tone="warn" />
            <StatCard label="Категории" value={Object.keys(stats.byCategory).length} sub={Object.entries(stats.byCategory).map(([k, v]) => `${CATEGORY_LABELS[k as TenderCategory] ?? k}: ${v}`).join(" · ")} tone="neutral" />
          </div>
        </div>

        <div className="space-y-3">
          <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
            <FileSearch className="h-3.5 w-3.5" /> Проверка Поставщика (Document Check)
          </div>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <StatCard label="С нарушениями поставщика" value={stats.docViolations} sub={`${stats.total > 0 ? Math.round((stats.docViolations / stats.total) * 100) : 0}%`} tone="danger" />
            <StatCard label="Avg Doc Compliance" value={`${stats.avgDoc}%`} tone={stats.avgDoc >= 70 ? "ok" : "warn"} />
            <StatCard label="HIGH risk" value={stats.riskHigh} tone="danger" />
            <StatCard label="MEDIUM risk" value={stats.riskMed} tone="warn" />
          </div>
        </div>

        {/* Charts Row */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <ChartCard title="Риск по тендерам">
            <DonutChart
              segments={[
                { label: "HIGH", value: stats.riskHigh, color: "#f43f5e" },
                { label: "MEDIUM", value: stats.riskMed, color: "#f59e0b" },
                { label: "LOW", value: stats.riskLow, color: "#10b981" },
              ]}
            />
          </ChartCard>

          <ChartCard title="Типы нарушений поставщиков">
            <MiniBar
              items={Object.entries(violationTypes)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 5)
                .map(([type, count]) => ({
                  label: DOC_VIOLATION_LABELS[type as DocViolationType] ?? type,
                  value: count,
                  color: "bg-rose-500",
                }))}
            />
          </ChartCard>

          <ChartCard title="Топ рискованных регионов">
            <MiniBar items={regionStats} />
          </ChartCard>
        </div>

        {/* Top Risky Tenders */}
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <ShieldAlert className="h-4 w-4 text-rose-500 dark:text-rose-300" />
              <span className="text-sm font-semibold text-[var(--text-main)]">Топ подозрительных тендеров</span>
            </div>
            <span className="text-xs text-[var(--text-muted)]">по совокупному риску</span>
          </div>

          <div className="space-y-2">
            {topRisky.map((item) => (
              <button
                key={item.tenderId}
                onClick={() => router.push(`/specnorm/lots/${item.tenderId}`)}
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] p-3 text-left hover:bg-[var(--surface-hover)] transition shadow-sm"
              >
                <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <CategoryBadge category={item.category} />
                      <span className="text-xs text-[var(--text-muted)] font-mono">{item.tenderId}</span>
                    </div>
                    <div className="mt-1 text-sm font-semibold text-[var(--text-main)] truncate">{item.titleRu}</div>
                    <div className="mt-1 text-xs text-[var(--text-muted)] truncate">
                      {item.region} · {money(item.amountKZT)}
                    </div>
                    <div className="mt-1.5 flex flex-wrap gap-1">
                      {(item.activeFlags || []).slice(0, 2).map((f) => (
                        <FlagChip key={f} code={f} />
                      ))}
                      {(item.docViolations || []).length > 0 && (
                        <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/25 bg-amber-500/10 px-2.5 py-0.5 text-[11px] font-medium text-amber-200">
                          <Users className="h-3 w-3" />
                          {item.docViolations.length} нарушен.
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="flex flex-col items-end gap-1.5 flex-shrink-0">
                    <RiskBadge score={item.specDeviationScore} label="Spec" />
                    <DocBadge score={item.docComplianceScore ?? 100} />
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>
    </SpecNormShell>
  );
}
