"use client";

import React, { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ListFilter, Search, RefreshCw, ChevronRight, Scale, FileSearch } from "lucide-react";
import { useFindingsData } from "@/lib/specnorm/useSpecNormData";
import type { TenderCategory, RiskTier } from "@/lib/specnorm/types";
import { CATEGORY_LABELS } from "@/lib/specnorm/types";
import {
  SpecNormShell, RiskBadge, DocBadge, FlagChip, CategoryBadge,
  EmptyState, ComplianceBar, SeverityBar,
} from "@/lib/specnorm/components";

const money = (n: number) =>
  new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

const RISK_FILTERS = [
  { label: "Все", value: "" },
  { label: "HIGH", value: "HIGH" },
  { label: "MEDIUM", value: "MEDIUM" },
  { label: "LOW", value: "LOW" },
];

const CAT_FILTERS = [
  { label: "Все", value: "" },
  { label: "Работы", value: "works" },
  { label: "Услуги", value: "services" },
  { label: "Товары", value: "goods" },
];

const CHECK_FILTERS = [
  { label: "Все", value: "" },
  { label: "Нарушения заказчика", value: "spec" },
  { label: "Нарушения поставщика", value: "doc" },
  { label: "Оба нарушения", value: "both" },
];

const PAGE_SIZE = 10;

export default function TenderList() {
  const router = useRouter();
  const { data, loading } = useFindingsData();

  const [search, setSearch] = useState("");
  const [riskFilter, setRiskFilter] = useState("");
  const [catFilter, setCatFilter] = useState("");
  const [checkFilter, setCheckFilter] = useState("");
  const [sortBy, setSortBy] = useState<"risk" | "spec" | "doc" | "amount" | "date">("risk");
  const [page, setPage] = useState(1);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = data.filter((d) => {
      if (riskFilter && d.overallRiskTier !== riskFilter) return false;
      if (catFilter && d.category !== catFilter) return false;
      if (checkFilter === "spec" && !d.isFlagged) return false;
      if (checkFilter === "doc" && (!d.docViolations || d.docViolations.length === 0)) return false;
      if (checkFilter === "both" && (!d.isFlagged || !d.docViolations || d.docViolations.length === 0)) return false;
      if (q && !d.tenderId.toLowerCase().includes(q) && !d.titleRu.toLowerCase().includes(q) && !(d.supplierName || "").toLowerCase().includes(q)) return false;
      return true;
    });

    list.sort((a, b) => {
      if (sortBy === "risk") {
        const sa = a.specDeviationScore + (100 - (a.docComplianceScore ?? 100));
        const sb = b.specDeviationScore + (100 - (b.docComplianceScore ?? 100));
        return sb - sa;
      }
      if (sortBy === "spec") return b.specDeviationScore - a.specDeviationScore;
      if (sortBy === "doc") return (a.docComplianceScore ?? 100) - (b.docComplianceScore ?? 100);
      if (sortBy === "amount") return b.amountKZT - a.amountKZT;
      return (b.publishDate ?? "").localeCompare(a.publishDate ?? "");
    });

    return list;
  }, [data, search, riskFilter, catFilter, checkFilter, sortBy]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageItems = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  if (loading) {
    return (
      <SpecNormShell backHref="/specnorm" backLabel="Dashboard" title="Tender Analysis" subtitle="Список тендеров">
        <div className="flex items-center justify-center py-24 text-[var(--text-muted)]">
          <RefreshCw className="h-6 w-6 animate-spin mr-2" /> Загрузка...
        </div>
      </SpecNormShell>
    );
  }

  return (
    <SpecNormShell backHref="/specnorm" backLabel="Dashboard" title="Tender Analysis" subtitle="Список тендеров">
      <div className="space-y-4">
        <div className="flex items-center gap-3">
          <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-2 shadow-sm shadow-[var(--border-hover)]">
            <ListFilter className="h-4 w-4 text-indigo-500 dark:text-indigo-200" />
          </div>
          <div>
            <div className="text-sm font-semibold text-[var(--text-main)]">Тендеры</div>
            <div className="text-xs text-[var(--text-muted)]">Фильтрация и поиск · {filtered.length} результатов</div>
          </div>
        </div>

        {/* Filters */}
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-3 shadow-sm shadow-[var(--border-hover)]">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 flex-1 min-w-[200px]">
              <Search className="h-4 w-4 text-[var(--text-muted)] flex-shrink-0" />
              <input
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(1); }}
                placeholder="ID, название, поставщик..."
                className="w-full bg-transparent text-sm text-[var(--text-main)] outline-none placeholder:text-[var(--text-muted)]"
              />
            </div>

            <select
              value={riskFilter}
              onChange={(e) => { setRiskFilter(e.target.value); setPage(1); }}
              className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 text-xs text-[var(--text-main)] outline-none"
            >
              {RISK_FILTERS.map((f) => <option key={f.value} value={f.value}>{f.label}</option>)}
            </select>

            <select
              value={catFilter}
              onChange={(e) => { setCatFilter(e.target.value); setPage(1); }}
              className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 text-xs text-[var(--text-main)] outline-none"
            >
              {CAT_FILTERS.map((f) => <option key={f.value} value={f.value}>{f.label}</option>)}
            </select>

            <select
              value={checkFilter}
              onChange={(e) => { setCheckFilter(e.target.value); setPage(1); }}
              className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 text-xs text-[var(--text-main)] outline-none"
            >
              {CHECK_FILTERS.map((f) => <option key={f.value} value={f.value}>{f.label}</option>)}
            </select>

            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
              className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 text-xs text-[var(--text-main)] outline-none"
            >
              <option value="risk">По общему риску</option>
              <option value="spec">По Spec Deviation</option>
              <option value="doc">По Doc Compliance</option>
              <option value="amount">По сумме</option>
              <option value="date">По дате</option>
            </select>
          </div>
        </div>

        {/* List */}
        {pageItems.length === 0 ? (
          <EmptyState text="Нет результатов. Измените фильтры." />
        ) : (
          <div className="space-y-2">
            {pageItems.map((item) => (
              <button
                key={item.tenderId}
                onClick={() => router.push(`/specnorm/lots/${item.tenderId}`)}
                className="w-full rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 text-left hover:bg-[var(--surface-hover)] transition group shadow-sm"
              >
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <CategoryBadge category={item.category} />
                      <span className="text-xs text-[var(--text-muted)] font-mono">{item.tenderId}</span>
                      {item.overallRiskTier === "HIGH" && (
                        <span className="rounded-full border border-rose-500/30 bg-rose-500/15 px-2 py-0.5 text-[10px] font-bold text-rose-700 dark:text-rose-300">
                          HIGH RISK
                        </span>
                      )}
                    </div>
                    <div className="mt-1 text-sm font-semibold text-[var(--text-main)] truncate">{item.titleRu}</div>
                    <div className="mt-1 text-xs text-[var(--text-muted)]">
                      {item.region} · {money(item.amountKZT)} · {item.publishDate?.slice(0, 10) ?? "—"}
                    </div>
                    {item.supplierName && (
                      <div className="mt-1 text-xs text-[var(--text-muted)]">
                        Поставщик: {item.supplierName}
                      </div>
                    )}
                    <div className="mt-1.5 flex flex-wrap gap-1">
                      {(item.activeFlags || []).slice(0, 2).map((f) => <FlagChip key={f} code={f} />)}
                    </div>
                  </div>

                  <div className="flex flex-col gap-2 flex-shrink-0 lg:items-end lg:w-48">
                    <div className="flex items-center gap-2">
                      <Scale className="h-3 w-3 text-[var(--text-muted)]" />
                      <span className="text-[10px] text-[var(--text-muted)] w-12">Spec</span>
                      <div className="w-24"><SeverityBar value={item.specDeviationScore} /></div>
                      <span className="text-xs font-mono text-[var(--text-muted)] w-8 text-right">{item.specDeviationScore}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <FileSearch className="h-3 w-3 text-[var(--text-muted)]" />
                      <span className="text-[10px] text-[var(--text-muted)] w-12">Doc</span>
                      <div className="w-24"><ComplianceBar value={item.docComplianceScore ?? 100} /></div>
                      <span className="text-xs font-mono text-[var(--text-muted)] w-8 text-right">{item.docComplianceScore ?? 100}%</span>
                    </div>
                    <ChevronRight className="h-4 w-4 text-[var(--text-muted)] group-hover:text-indigo-500 transition-colors self-end" />
                  </div>
                </div>
              </button>
            ))}
          </div>
        )}

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-center gap-2">
            {Array.from({ length: Math.min(totalPages, 10) }, (_, i) => i + 1).map((p) => (
              <button
                key={p}
                onClick={() => setPage(p)}
                className={`h-8 w-8 rounded-lg text-xs transition ${p === page
                  ? "bg-indigo-500 text-white"
                  : "border border-[var(--border)] bg-[var(--surface)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)]"}`}
              >
                {p}
              </button>
            ))}
            {totalPages > 10 && <span className="text-xs text-[var(--text-muted)]">... {totalPages}</span>}
          </div>
        )}
      </div>
    </SpecNormShell>
  );
}
