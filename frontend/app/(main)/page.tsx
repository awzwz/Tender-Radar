"use client";

import React, { useEffect, useMemo, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import {
  ShieldAlert,
  FileText,
  Bot,
  ListFilter,
  Sparkles,
  Download,
  Play,
  X,
} from "lucide-react";
import { api, DashboardItem, DashboardTenderItem } from "@/lib/api";
import Sidebar from "@/components/layout/Sidebar";
import type { NavId } from "@/components/layout/Sidebar";
import Header from "@/components/layout/Header";
import FilterPanel from "@/components/dashboard/FilterPanel";
import DashboardView from "@/components/dashboard/DashboardView";
import RiskListView from "@/components/dashboard/RiskListView";
import LotDetailView from "@/components/dashboard/LotDetailView";
import CasesView from "@/components/dashboard/CasesView";
import AIAssistant from "@/components/dashboard/AIAssistant";
import { money } from "@/components/shared/ui";

// ── Types ────────────────────────────────────────────────────────────────────
type CaseStatus = "NEW" | "IN_REVIEW" | "CONFIRMED" | "DISMISSED";
type CaseItem = {
  id: string; lotId: number; lotName: string; score: number; level: string;
  createdAt: string; status: CaseStatus; note: string;
};

type Nav = NavId;

export default function AIProcurePage() {
  const router = useRouter();

  // Auth check
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) router.push("/login");
  }, [router]);

  // Data state
  const [items, setItems] = useState<DashboardItem[]>([]);
  const [tenderItems, setTenderItems] = useState<DashboardTenderItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [globalStats, setGlobalStats] = useState<{ total_lots: number; scored_lots: number; high: number; medium: number; low: number; avg_score: number } | null>(null);
  const [globalTenderStats, setGlobalTenderStats] = useState<{
    total_tenders: number;
    scored_tenders: number;
    high: number;
    medium: number;
    low: number;
    avg_score: number;
    main_high: number;
    single_case_alerts: number;
  } | null>(null);
  const [tenderLoading, setTenderLoading] = useState(false);

  // UI state
  const [nav, setNav] = useState<Nav>("dashboard");
  const [search, setSearch] = useState("");
  const [minRisk, setMinRisk] = useState(0);
  const [levelFilter, setLevelFilter] = useState("");
  const [selectedLotId, setSelectedLotId] = useState<number | null>(null);
  const [cases, setCases] = useState<CaseItem[]>([]);
  const [onboardingOpen, setOnboardingOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  // Load dashboard stats (global HIGH/MEDIUM/LOW counts)
  const loadStats = useCallback(() => {
    api.dashboardStats().then(setGlobalStats).catch(() => setGlobalStats(null));
  }, []);
  const loadTenderStats = useCallback(() => {
    api.dashboardTenderStats().then(setGlobalTenderStats).catch(() => setGlobalTenderStats(null));
  }, []);
  const loadTenderData = useCallback(async () => {
    setTenderLoading(true);
    try {
      const data = await api.dashboardTenders({
        page: 1,
        limit: 50,
        category: "all",
        sort_by: "score",
      });
      setTenderItems(data.items || []);
    } catch {
      setTenderItems([]);
    } finally {
      setTenderLoading(false);
    }
  }, []);

  // Load dashboard data
  const loadData = useCallback(async (pg = 1, append = false, levelF = levelFilter, minR = minRisk) => {
    setLoading(true);
    loadStats(); // Refresh global stats
    loadTenderStats();
    try {
      const params: Record<string, string | number> = {
        page: pg,
        limit: 50,
        sort_by: "score",
      };
      if (levelF) params.level = levelF;
      const data = await api.dashboard(params);
      setTotal(data.total);
      setItems((prev) => {
        const scoreOrFallback = (x: { risk_score?: number | null; risk_level?: string }) =>
          x.risk_score ?? (x.risk_level === "HIGH" ? 20 : x.risk_level === "MEDIUM" ? 10 : 0);
        const incoming = data.items.filter((x) => scoreOrFallback(x) >= minR);
        return append ? [...prev, ...incoming] : incoming;
      });
    } catch {
      // silently fail — show empty state
    } finally {
      setLoading(false);
    }
  }, [levelFilter, minRisk, loadStats, loadTenderStats]);

  useEffect(() => {
    loadData(1, false, levelFilter, minRisk);
  }, [levelFilter]);

  useEffect(() => {
    loadStats();
    loadTenderStats();
    loadTenderData();
  }, [loadStats, loadTenderStats, loadTenderData]);

  // Onboarding: show once on first visit
  useEffect(() => {
    if (typeof window === "undefined") return;
    const seen = localStorage.getItem("tender_radar_onboarding_seen");
    if (!seen) setOnboardingOpen(true);
  }, []);

  function closeOnboarding() {
    localStorage.setItem("tender_radar_onboarding_seen", "1");
    setOnboardingOpen(false);
  }

  function runDemo() {
    setMinRisk(29);
    setLevelFilter("HIGH");
    loadData(1, false, "HIGH", 29);
    setNav("risk-list");
    closeOnboarding();
  }

  function exportCSV() {
    const rows = filteredItems;
    const headers = ["lot_id", "lot_name", "amount", "customer_name", "risk_score", "risk_level", "top_reasons"];
    const lines = [
      headers.join(","),
      ...rows.map((r) =>
        [
          r.lot_id,
          `"${(r.lot_name || "").replace(/"/g, '""')}"`,
          r.amount,
          `"${(r.customer_name || "").replace(/"/g, '""')}"`,
          r.risk_score,
          r.risk_level,
          `"${((r.top_reasons || []).map((x) => x.code).join("; ")).replace(/"/g, '""')}"`,
        ].join(",")
      ),
    ];
    const blob = new Blob(["\ufeff" + lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `tender_radar_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // Filtered items for Risk List
  const filteredItems = useMemo(() => {
    const q = search.trim().toLowerCase();
    const scoreOr = (l: DashboardItem) =>
      l.risk_score ?? (l.risk_level === "HIGH" ? 20 : l.risk_level === "MEDIUM" ? 10 : 0);
    return items
      .filter((l) => {
        const passRisk = scoreOr(l) >= minRisk;
        const passQ =
          !q ||
          l.tender_number?.toLowerCase().includes(q) ||
          l.lot_name?.toLowerCase().includes(q) ||
          l.customer_name?.toLowerCase().includes(q) ||
          l.customer_bin?.toLowerCase().includes(q);
        return passRisk && passQ;
      })
      .sort((a, b) => b.risk_score - a.risk_score);
  }, [items, search, minRisk]);

  // Selected lot info for sidebar
  const selectedItem = useMemo(() => items.find((x) => x.lot_id === selectedLotId), [items, selectedLotId]);

  function openLot(id: number) {
    setSelectedLotId(id);
    setNav("detail");
  }

  function createCase(note: string, lotId: number) {
    const lot = items.find((x) => x.lot_id === lotId);
    const newCase: CaseItem = {
      id: `CASE-${Date.now()}`,
      lotId,
      lotName: lot?.lot_name || `Лот #${lotId}`,
      score: lot?.risk_score || 0,
      level: lot?.risk_level || "UNKNOWN",
      createdAt: new Date().toLocaleString("ru-RU"),
      status: "NEW",
      note,
    };
    setCases((prev) => [newCase, ...prev]);
    setNav("cases");
  }

  function logout() {
    localStorage.removeItem("token");
    router.push("/login");
  }

  return (
    <div className="min-h-screen bg-[var(--root-bg)] text-[var(--root-fg)] transition-colors">
      {/* Onboarding modal */}
      {onboardingOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 dark:bg-black/60 backdrop-blur-sm">
          <div className="mx-4 max-w-md rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-6 shadow-2xl">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Sparkles className="h-5 w-5 text-indigo-500 dark:text-indigo-300" />
                <h2 className="text-lg font-semibold text-[var(--text-main)]">Добро пожаловать в Tender Radar</h2>
              </div>
              <button onClick={closeOnboarding} className="rounded-xl p-2 text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition">
                <X className="h-5 w-5" />
              </button>
            </div>
            <ul className="space-y-3 text-sm text-[var(--text-main)] mb-6">
              <li className="flex gap-2"><ShieldAlert className="h-4 w-4 flex-shrink-0 text-indigo-500 dark:text-indigo-300 mt-0.5" /><span><strong>Risk Score 0–100</strong> — общий уровень риска (LOW/MEDIUM/HIGH)</span></li>
              <li className="flex gap-2"><FileText className="h-4 w-4 flex-shrink-0 text-indigo-500 dark:text-indigo-300 mt-0.5" /><span><strong>Lot Detail</strong> — карточка лота с индикаторами и AI-объяснением</span></li>
              <li className="flex gap-2"><Bot className="h-4 w-4 flex-shrink-0 text-indigo-500 dark:text-indigo-300 mt-0.5" /><span><strong>Объяснить</strong> — AI анализирует риски и манипулятивные спецификации</span></li>
              <li className="flex gap-2"><ListFilter className="h-4 w-4 flex-shrink-0 text-indigo-500 dark:text-indigo-300 mt-0.5" /><span><strong>Фильтры</strong> — min risk, уровень, поиск по номеру/заказчику</span></li>
            </ul>
            <div className="flex gap-2">
              <button onClick={runDemo} className="flex-1 rounded-xl bg-indigo-500 px-4 py-2.5 text-sm font-medium text-white hover:bg-indigo-400 transition flex items-center justify-center gap-2">
                <Play className="h-4 w-4" /> Запустить демо
              </button>
              <button onClick={closeOnboarding} className="rounded-xl border border-[var(--border)] px-4 py-2.5 text-sm text-[var(--text-main)] hover:bg-[var(--surface-hover)] transition">
                Пропустить
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Background glow */}
      <div className="pointer-events-none fixed inset-0 -z-10 overflow-hidden">
        <div className="absolute left-[-10%] top-[-10%] h-[500px] w-[500px] rounded-full bg-indigo-500/15 blur-[100px]" />
        <div className="absolute right-[-10%] top-[10%] h-[400px] w-[400px] rounded-full bg-emerald-400/8 blur-[100px]" />
      </div>

      {/* ── Sidebar ── */}
      <Sidebar activeNav={nav} onNavChange={setNav} onLogout={logout} collapsed={sidebarCollapsed} onCollapsedChange={setSidebarCollapsed} />

      {/* ── Main area (shifts right for sidebar) ── */}
      <div className={`transition-all duration-300 min-h-screen flex flex-col ${sidebarCollapsed ? "lg:pl-[72px]" : "lg:pl-[260px]"}`}>
        {/* Header */}
        <Header
          totalLots={total || globalStats?.total_lots || 0}
          onRunDemo={runDemo}
          onExportCSV={exportCSV}
        />

        {/* Content */}
        <main className="flex-1 p-5">
          {/* Filter panel — shown on dashboard and risk-list views */}
          {(nav === "dashboard" || nav === "risk-list") && (
            <div className="mb-5">
              <FilterPanel
                search={search}
                onSearchChange={setSearch}
                minRisk={minRisk}
                onMinRiskChange={setMinRisk}
                levelFilter={levelFilter}
                onLevelFilterChange={setLevelFilter}
                selectedItem={selectedItem}
                onOpenDetail={() => setNav("detail")}
              />
            </div>
          )}

          {/* Main content area */}
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-5 min-w-0">
            {nav === "dashboard" && (
              <DashboardView
                items={filteredItems}
                total={total}
                loading={loading}
                onOpenLot={openLot}
                globalStats={globalStats}
                tenderStats={globalTenderStats}
                tenderItems={tenderItems}
                tenderLoading={tenderLoading}
              />
            )}
            {nav === "risk-list" && (
              <RiskListView
                items={filteredItems}
                loading={loading}
                onOpenLot={openLot}
                onLoadMore={() => {
                  const next = page + 1;
                  setPage(next);
                  loadData(next, true);
                }}
                hasMore={items.length < total}
              />
            )}
            {nav === "detail" && (
              <LotDetailView
                lotId={selectedLotId}
                onCreateCase={createCase}
              />
            )}
            {nav === "cases" && (
              <CasesView
                cases={cases}
                onOpenLot={openLot}
                onUpdate={(id, patch) =>
                  setCases((prev) => prev.map((c) => (c.id === id ? { ...c, ...patch } : c)))
                }
              />
            )}
            {nav === "ai-assistant" && (
              <AIAssistant items={items} selectedLotId={selectedLotId} />
            )}
            {nav === "specnorm" && (
              <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)] space-y-3">
                <FileText className="h-12 w-12" />
                <div className="text-sm">Tender Analysis загружается...</div>
                <a
                  href="/specnorm"
                  className="rounded-xl bg-indigo-500 px-4 py-2 text-xs font-medium text-white hover:bg-indigo-400 transition"
                >
                  Открыть Tender Analysis
                </a>
              </div>
            )}
            {nav === "bidcheck" && (
              <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)] space-y-3">
                <FileText className="h-12 w-12" />
                <div className="text-sm">BidCheck загружается...</div>
                <a
                  href="/bidcheck"
                  className="rounded-xl bg-emerald-500 px-4 py-2 text-xs font-medium text-white hover:bg-emerald-400 transition"
                >
                  Открыть BidCheck
                </a>
              </div>
            )}
            {nav === "suppliers" && (
              <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)] space-y-3">
                <FileText className="h-12 w-12" />
                <div className="text-sm">Suppliers — скоро здесь будет карточка поставщиков</div>
              </div>
            )}
          </div>
        </main>

        {/* Footer */}
        <footer className="px-6 py-4 border-t border-[var(--border)] flex flex-col gap-2 text-xs md:flex-row md:items-center md:justify-between text-[var(--text-muted)]">
          <div>
            API:{" "}
            <span className="rounded border border-[var(--border)] bg-[var(--surface-hover)] px-2 py-0.5 font-mono">GET /dashboard</span>{" "}
            <span className="rounded border border-[var(--border)] bg-[var(--surface-hover)] px-2 py-0.5 font-mono">GET /lots/:id</span>{" "}
            <span className="rounded border border-[var(--border)] bg-[var(--surface-hover)] px-2 py-0.5 font-mono">POST /notes</span>
          </div>
          <div className="flex items-center gap-2">
            <Download className="h-3.5 w-3.5" />
            <span>PDF/CSV export (post‑MVP)</span>
          </div>
        </footer>
      </div>
    </div>
  );
}
