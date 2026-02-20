"use client";

import React, { useEffect, useMemo, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import {
  Search,
  ShieldAlert,
  LayoutDashboard,
  ListFilter,
  FileText,
  Briefcase,
  Bot,
  ArrowRight,
  CheckCircle2,
  XCircle,
  BadgeInfo,
  Sparkles,
  ChevronRight,
  Download,
  RefreshCw,
} from "lucide-react";
import { api, DashboardItem, LotDetail, RiskLevel } from "@/lib/api";

// ── Helpers ──────────────────────────────────────────────────────────────────

const money = (n: number) =>
  new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

const clamp = (n: number, min = 0, max = 100) => Math.max(min, Math.min(max, n));

function levelFromScore(score: number): RiskLevel {
  if (score >= 70) return "HIGH";
  if (score >= 40) return "MEDIUM";
  return "LOW";
}

// ── Shared UI components ──────────────────────────────────────────────────────

function Badge({ level, score }: { level: string; score: number }) {
  const base = "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border";
  const styles: Record<string, string> = {
    LOW: "bg-emerald-500/10 border-emerald-500/25 text-emerald-200",
    MEDIUM: "bg-amber-500/10 border-amber-500/25 text-amber-200",
    HIGH: "bg-rose-500/10 border-rose-500/25 text-rose-200",
    UNKNOWN: "bg-slate-500/10 border-slate-500/25 text-slate-300",
  };
  return (
    <span className={`${base} ${styles[level] ?? styles.UNKNOWN}`}>
      <ShieldAlert className="h-3.5 w-3.5" />
      {level} · {score}
    </span>
  );
}

function Chip({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-xs text-white/80">
      {children}
    </span>
  );
}

function DividerSpacer() {
  return <div className="my-3 h-px w-full bg-white/10" />;
}

function ProgressBar({ value }: { value: number }) {
  const v = clamp(value);
  const level = levelFromScore(v);
  const color =
    level === "HIGH" ? "bg-rose-500" : level === "MEDIUM" ? "bg-amber-500" : "bg-emerald-500";
  return (
    <div className="h-1.5 w-full rounded-full bg-white/10 overflow-hidden">
      <div className={`h-full ${color} transition-all`} style={{ width: `${v}%` }} />
    </div>
  );
}

function SectionTitle({ icon, title, hint }: { icon: React.ReactNode; title: string; hint?: string }) {
  return (
    <div className="flex items-start gap-3">
      <div className="rounded-xl border border-white/10 bg-white/5 p-2">{icon}</div>
      <div>
        <div className="text-sm font-semibold text-white">{title}</div>
        {hint && <div className="text-xs text-white/60">{hint}</div>}
      </div>
    </div>
  );
}

function StatCard({ label, value, tone }: { label: string; value: number | string; tone: "danger" | "warn" | "ok" | "neutral" }) {
  const styles = {
    danger: "border-rose-500/20 bg-rose-500/10",
    warn: "border-amber-500/20 bg-amber-500/10",
    ok: "border-emerald-500/20 bg-emerald-500/10",
    neutral: "border-white/10 bg-white/5",
  }[tone];
  return (
    <div className={`rounded-3xl border p-4 ${styles}`}>
      <div className="text-xs text-white/60">{label}</div>
      <div className="mt-1 text-2xl font-semibold text-white">{value}</div>
      <div className="mt-2 h-px w-full bg-white/10" />
      <div className="mt-2 text-xs text-white/60">Updated daily</div>
    </div>
  );
}

function NavBtn({
  active, onClick, icon, title, hint,
}: {
  active: boolean; onClick: () => void; icon: React.ReactNode; title: string; hint: string;
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full rounded-2xl border px-3 py-2.5 text-left transition ${active
        ? "border-indigo-500/30 bg-indigo-500/10"
        : "border-white/10 bg-white/5 hover:bg-white/10"
        }`}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <div className="rounded-xl border border-white/10 bg-white/5 p-2 text-white/80">{icon}</div>
          <div>
            <div className="text-sm font-semibold text-white">{title}</div>
            <div className="text-xs text-white/60">{hint}</div>
          </div>
        </div>
        <ChevronRight className="h-4 w-4 text-white/40" />
      </div>
    </button>
  );
}

// ── Dashboard View ─────────────────────────────────────────────────────────────

function DashboardView({
  items, total, loading, onOpenLot,
}: {
  items: DashboardItem[]; total: number; loading: boolean; onOpenLot: (id: number) => void;
}) {
  const stats = useMemo(() => {
    const high = items.filter((x) => x.risk_level === "HIGH").length;
    const med = items.filter((x) => x.risk_level === "MEDIUM").length;
    const low = items.filter((x) => x.risk_level === "LOW").length;
    const scored = items.filter((x) => x.risk_score > 0);
    const avg = scored.length > 0 ? Math.round(scored.reduce((s, x) => s + x.risk_score, 0) / scored.length) : 0;
    return { high, med, low, avg };
  }, [items]);

  const top = [...items].sort((a, b) => b.risk_score - a.risk_score).slice(0, 5);

  return (
    <div className="space-y-4">
      <SectionTitle
        icon={<LayoutDashboard className="h-4 w-4 text-indigo-200" />}
        title="Dashboard"
        hint="Общий обзор рисков и быстрый доступ к top‑risk лотам"
      />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard label="HIGH" value={stats.high} tone="danger" />
        <StatCard label="MEDIUM" value={stats.med} tone="warn" />
        <StatCard label="LOW" value={stats.low} tone="ok" />
        <StatCard label="Avg risk" value={stats.avg} tone="neutral" />
      </div>

      {/* Risk Distribution Chart */}
      <div className="rounded-3xl border border-white/10 bg-slate-950/40 p-5">
        <div className="mb-4 flex items-center justify-between">
          <SectionTitle
            icon={<Sparkles className="h-4 w-4 text-emerald-200" />}
            title="Risk Distribution"
            hint="Распределение лотов по уровням риска"
          />
        </div>

        <div className="flex h-4 w-full overflow-hidden rounded-full bg-white/5">
          <div style={{ width: `${(stats.high / total) * 100}%` }} className="bg-rose-500 h-full transition-all duration-500" />
          <div style={{ width: `${(stats.med / total) * 100}%` }} className="bg-amber-500 h-full transition-all duration-500" />
          <div style={{ width: `${(stats.low / total) * 100}%` }} className="bg-emerald-500 h-full transition-all duration-500" />
        </div>

        <div className="mt-3 flex items-center justify-between text-xs text-white/60">
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-rose-500" /> HIGH ({((stats.high / total) * 100).toFixed(1)}%)
          </div>
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-amber-500" /> MEDIUM ({((stats.med / total) * 100).toFixed(1)}%)
          </div>
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-emerald-500" /> LOW ({((stats.low / total) * 100).toFixed(1)}%)
          </div>
        </div>
      </div>

      <div className="rounded-3xl border border-white/10 bg-slate-950/40 p-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-sm font-semibold">Top risky lots</div>
            <div className="text-xs text-white/60">Нажми, чтобы открыть карточку лота</div>
          </div>
          <div className="text-xs text-white/60">Total: {total}</div>
        </div>

        {loading ? (
          <div className="mt-4 flex items-center justify-center py-8 text-white/40">
            <RefreshCw className="h-5 w-5 animate-spin mr-2" /> Загрузка...
          </div>
        ) : top.length === 0 ? (
          <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/50 text-center">
            Данных пока нет — ETL ещё загружает лоты
          </div>
        ) : (
          <div className="mt-3 space-y-2">
            {top.map((l) => (
              <button
                key={l.lot_id}
                onClick={() => onOpenLot(l.lot_id)}
                className="w-full rounded-2xl border border-white/10 bg-white/5 p-3 text-left hover:bg-white/10 transition"
              >
                <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-semibold text-white truncate">{l.lot_name || "Без названия"}</div>
                    <div className="mt-1 text-xs text-white/60 truncate">
                      {l.tender_number} · {l.customer_name || l.customer_bin}
                    </div>
                  </div>
                  <div className="flex items-center gap-3 flex-shrink-0">
                    <div className="text-xs text-white/60">{money(l.amount)}</div>
                    <Badge level={l.risk_level} score={Math.round(l.risk_score)} />
                  </div>
                </div>
                <div className="mt-2">
                  <ProgressBar value={l.risk_score} />
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Risk List View ────────────────────────────────────────────────────────────

function RiskListView({
  items, loading, onOpenLot, onLoadMore, hasMore,
}: {
  items: DashboardItem[]; loading: boolean; onOpenLot: (id: number) => void;
  onLoadMore: () => void; hasMore: boolean;
}) {
  return (
    <div className="space-y-4">
      <SectionTitle
        icon={<ListFilter className="h-4 w-4 text-indigo-200" />}
        title="Risk List"
        hint="Фильтры находятся слева. Сортировка по риск‑скор по умолчанию."
      />

      <div className="rounded-3xl border border-white/10 bg-slate-950/40 p-4">
        {loading && items.length === 0 ? (
          <div className="flex items-center justify-center py-8 text-white/40">
            <RefreshCw className="h-5 w-5 animate-spin mr-2" /> Загрузка...
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/70 text-center">
            Нет результатов. Уменьши Min risk или измени фильтр.
          </div>
        ) : (
          <div className="space-y-2">
            {items.map((l) => (
              <div key={l.lot_id} className="rounded-2xl border border-white/10 bg-white/5 p-3">
                <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <div className="text-sm font-semibold text-white truncate">{l.lot_name || "Без названия"}</div>
                    </div>
                    <div className="mt-1 text-xs text-white/60 truncate">
                      {l.tender_number} · {l.customer_name || l.customer_bin} · {l.publish_date?.slice(0, 10)}
                    </div>
                    {l.top_reasons.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {l.top_reasons.slice(0, 3).map((r, i) => (
                          <Chip key={`${r.code}-${i}`}>{r.code}</Chip>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-3 flex-shrink-0">
                    <div className="text-xs text-white/60">{money(l.amount)}</div>
                    <Badge level={l.risk_level} score={Math.round(l.risk_score)} />
                    <button
                      onClick={() => onOpenLot(l.lot_id)}
                      className="rounded-xl bg-indigo-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-indigo-400 transition"
                    >
                      Open
                    </button>
                  </div>
                </div>
              </div>
            ))}
            {hasMore && (
              <button
                onClick={onLoadMore}
                disabled={loading}
                className="w-full rounded-2xl border border-white/10 bg-white/5 py-3 text-xs text-white/60 hover:bg-white/10 transition disabled:opacity-40"
              >
                {loading ? "Загрузка..." : "Загрузить ещё"}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Lot Detail View ───────────────────────────────────────────────────────────

const EVIDENCE_LABELS: Record<string, string> = {
  // Dates & Timings
  start_date: "Дата начала",
  end_date: "Дата окончания",
  sign_date: "Дата подписания",
  contract_sign_date: "Дата подписания договора",
  addendum_sign_date: "Дата допсоглашения",
  plan_exec_date: "Плановая дата исполнения",
  regdate: "Дата регистрации",
  last_update: "Последнее обновление",
  deadline_days: "Дней до дедлайна",
  days_to_addendum: "Дней до допсоглашения",
  execution_days: "Срок исполнения (дней)",
  company_age_days: "Возраст компании (дней)",
  hours_before_deadline: "Часов до дедлайна",

  // Counts & Stats
  bid_count: "Кол-во заявок",
  lot_count: "Кол-во лотов",
  supplier_contracts: "Контрактов поставщика",
  total_contracts: "Всего контрактов",
  top_customer_contracts: "Контрактов с топ-заказчиком",
  payment_count: "Кол-во платежей",
  act_count: "Кол-во актов",
  total_participated: "Всего участий",
  total_won: "Всего побед",
  avg_bids_per_tender: "Среднее кол-во заявок",
  unique_winners: "Уникальных победителей",
  rotation_count: "Кол-во ротаций",
  bidder_count: "Кол-во участников",

  // Financials
  total_sum: "Общая сумма",
  avg_lot_amount: "Средняя сумма лота",
  original_sum: "Исходная сумма",
  current_sum: "Текущая сумма",
  addendum_sum: "Сумма допсоглашения",
  lot_amount: "Сумма лота",
  contract_sum: "Сумма контракта",
  total_paid: "Всего оплачено",

  // Identifiers & Entities
  customer_bin: "БИН заказчика",
  supplier_biin: "БИН/ИИН поставщика",
  top_customer_bin: "БИН топ-заказчика",
  root_contract_id: "ID корневого договора",
  contract_id: "ID договора",
  lot_id: "ID лота",
  rnu_id: "ID в РНУ",
  system_id: "ID в системе",
  company_name: "Название компании",

  // Ratios & Thresholds
  win_rate_pct: "Процент побед (%)",
  concentration_pct: "Концентрация (%)",
  increase_pct: "Процент увеличения (%)",
  threshold: "Порог",
  threshold_pct: "Порог (%)",
  threshold_sum: "Пороговая сумма",

  // Misc
  anomaly: "Аномалия",
  reason: "Причина",
  dumping_flag: "Флаг демпинга",
  winner_sequence: "Последовательность победителей",
  common_phones: "Общие телефоны",
  common_emails: "Общие email",
  method: "Метод закупки",
  region: "Регион",
};

function LotDetailView({
  lotId, onCreateCase,
}: {
  lotId: number | null; onCreateCase: (note: string, lotId: number) => void;
}) {
  const [data, setData] = useState<LotDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [note, setNote] = useState("Проверить ТЗ/обоснование цены и историю побед поставщика у заказчика.");
  const router = useRouter();

  useEffect(() => {
    if (!lotId) return;
    setLoading(true);
    api.lot(lotId).then(setData).catch(() => setData(null)).finally(() => setLoading(false));
  }, [lotId]);

  if (!lotId) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-white/40 space-y-2">
        <FileText className="h-10 w-10" />
        <div className="text-sm">Выбери лот из Dashboard или Risk List</div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-white/40">
        <RefreshCw className="h-5 w-5 animate-spin mr-2" /> Загрузка...
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-64 text-rose-400 text-sm">
        Ошибка загрузки лота
      </div>
    );
  }

  const { lot, tender, contract, risk, flags } = data;
  const triggeredFlags = flags.filter((f) => f.triggered);

  return (
    <div className="space-y-4">
      <SectionTitle
        icon={<FileText className="h-4 w-4 text-indigo-200" />}
        title="Lot Detail"
        hint="Карточка расследования: риск + причины + evidence"
      />

      <div className="rounded-3xl border border-white/10 bg-slate-950/40 p-4 space-y-4">
        {/* Header */}
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div className="flex-1 min-w-0">
            <div className="text-lg font-semibold text-white">{lot.name_ru || "Лот"}</div>
            <div className="mt-1 text-sm text-white/60">
              {tender && <>№{tender.number_anno} · </>}
              Заказчик: <span className="text-white/80">{lot.customer_name || lot.customer_bin}</span>
              {contract && (
                <> · Поставщик:{" "}
                  <button
                    onClick={() => router.push(`/suppliers/${contract.supplier_biin}`)}
                    className="text-indigo-300 hover:underline"
                  >
                    {contract.supplier_biin}
                  </button>
                </>
              )}
            </div>
            <div className="mt-1 text-sm text-white/60">
              Сумма: <span className="text-white/80 font-medium">{money(lot.amount)}</span>
              {tender && (
                <> · Период: <span className="text-white/80">{tender.start_date?.slice(0, 10)} — {tender.end_date?.slice(0, 10)}</span></>
              )}
            </div>
            {lot.dumping_flag && (
              <span className="mt-2 inline-block rounded-full border border-amber-500/30 bg-amber-500/10 px-2.5 py-1 text-xs text-amber-200">⚠ Демпинг</span>
            )}
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-3 text-right flex-shrink-0">
            <div className="text-xs text-white/60">Final risk</div>
            <div className="mt-1"><Badge level={risk.level} score={Math.round(risk.score)} /></div>
            <div className="mt-2 w-36"><ProgressBar value={risk.score} /></div>
            <div className="mt-1 text-[11px] text-white/40">Индикаторов: {triggeredFlags.length}/16</div>
          </div>
        </div>

        <DividerSpacer />

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Triggered Flags */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-semibold">Triggered flags</div>
              <span className="text-xs text-white/50">evidence-based</span>
            </div>
            {triggeredFlags.length === 0 ? (
              <div className="text-xs text-white/40 py-4 text-center">Сигналов риска нет</div>
            ) : (
              <div className="space-y-2">
                {triggeredFlags.map((f, i) => (
                  <div key={`${f.code}-${i}`} className="rounded-2xl border border-white/10 bg-slate-950/40 p-3">
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[11px] text-white/70 font-mono">{f.code}</span>
                        {f.value !== null && (
                          <div className="mt-1 text-xs text-white/60">
                            Evidence: <span className="font-mono text-white/70">{String(f.value)}</span>
                          </div>
                        )}


                        {f.evidence && Object.keys(f.evidence).length > 0 && (
                          <div className="mt-1 text-xs text-white/50 space-y-0.5">
                            {Object.entries(f.evidence).slice(0, 3).map(([k, v]) => (
                              <div key={k}>
                                <span className="text-white/40">{EVIDENCE_LABELS[k] || k}:</span> {String(v)}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div className="mt-3 rounded-2xl border border-white/10 bg-white/5 p-3">
              <div className="text-xs text-white/60 mb-2">Что проверить дальше</div>
              <ul className="list-disc pl-4 text-sm text-white/80 space-y-1">
                <li>Сравнить цену с медианой в категории/регионе</li>
                <li>Проверить критерии допуска и сроки</li>
                <li>Историю побед supplier↔customer за 90 дней</li>
              </ul>
            </div>
          </div>

          {/* Create Case */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-semibold">Причины риска (top)</div>
              <span className="text-xs text-white/50">из модели</span>
            </div>
            <div className="space-y-2">
              {risk.top_reasons.map((r, i) => (
                <div key={`${r.code}-${i}`} className="rounded-2xl border border-white/10 bg-slate-950/40 px-3 py-2">
                  <span className="text-xs font-mono text-indigo-300">{r.code}</span>
                  {r.description && <div className="text-xs text-white/70 mt-0.5">{r.description}</div>}
                </div>
              ))}
              {risk.top_reasons.length === 0 && (
                <div className="text-xs text-white/40 py-2 text-center">Нет данных</div>
              )}
            </div>

            <DividerSpacer />

            <div className="rounded-2xl border border-white/10 bg-slate-950/40 p-3">
              <div className="text-xs text-white/60 mb-2">Create case</div>
              <textarea
                value={note}
                onChange={(e) => setNote(e.target.value)}
                className="h-20 w-full resize-none rounded-2xl border border-white/10 bg-white/5 p-3 text-sm text-white/80 outline-none placeholder:text-white/40"
              />
              <div className="mt-2 flex items-center justify-between gap-2">
                <div className="text-xs text-white/50">Станет меткой для ML</div>
                <button
                  onClick={() => onCreateCase(note, lot.id)}
                  className="rounded-xl bg-emerald-500 px-3 py-1.5 text-xs font-medium text-slate-950 hover:bg-emerald-400 transition"
                >
                  Create
                </button>
              </div>
            </div>

            <div className="mt-3 rounded-2xl border border-white/10 bg-white/5 px-3 py-2">
              <div className="flex items-center gap-2 text-xs text-white/60">
                <CheckCircle2 className="h-4 w-4 text-emerald-300" />
                Explainability: flags + evidence + причины
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Cases View ────────────────────────────────────────────────────────────────

type CaseStatus = "NEW" | "IN_REVIEW" | "CONFIRMED" | "DISMISSED";
type CaseItem = {
  id: string; lotId: number; lotName: string; score: number; level: string;
  createdAt: string; status: CaseStatus; note: string;
};

function CasesView({
  cases, onOpenLot, onUpdate,
}: {
  cases: CaseItem[]; onOpenLot: (id: number) => void;
  onUpdate: (id: string, patch: Partial<CaseItem>) => void;
}) {
  return (
    <div className="space-y-4">
      <SectionTitle
        icon={<Briefcase className="h-4 w-4 text-indigo-200" />}
        title="Cases"
        hint="Результаты проверки. Источник меток для ML."
      />
      <div className="rounded-3xl border border-white/10 bg-slate-950/40 p-4">
        {cases.length === 0 ? (
          <div className="rounded-2xl border border-white/10 bg-white/5 p-6 text-sm text-white/60 text-center">
            Кейсов пока нет. Создай кейс в Lot Detail.
          </div>
        ) : (
          <div className="space-y-2">
            {cases.map((c) => (
              <div key={c.id} className="rounded-2xl border border-white/10 bg-white/5 p-3">
                <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold">{c.id}</div>
                      <span className="rounded-md border border-white/10 bg-white/5 px-2 py-0.5 text-[11px] text-white/60">{c.status}</span>
                      <Badge level={c.level} score={Math.round(c.score)} />
                    </div>
                    <div className="mt-1 text-xs text-white/60">{c.createdAt}</div>
                    <button
                      onClick={() => onOpenLot(c.lotId)}
                      className="mt-1 text-sm text-indigo-300 hover:underline truncate max-w-full block text-left"
                    >
                      {c.lotName || `Лот #${c.lotId}`}
                    </button>
                    <div className="mt-2 rounded-2xl border border-white/10 bg-slate-950/40 p-3 text-sm text-white/80">
                      {c.note}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <select
                      value={c.status}
                      onChange={(e) => onUpdate(c.id, { status: e.target.value as CaseStatus })}
                      className="rounded-xl border border-white/10 bg-white/5 px-3 py-1.5 text-xs text-white/80 outline-none"
                    >
                      <option value="NEW">NEW</option>
                      <option value="IN_REVIEW">IN_REVIEW</option>
                      <option value="CONFIRMED">CONFIRMED</option>
                      <option value="DISMISSED">DISMISSED</option>
                    </select>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
        <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 px-3 py-2">
          <div className="flex items-center gap-2 text-xs text-white/60">
            <XCircle className="h-4 w-4 text-rose-300" />
            В проде: кейсы экспортируются как ground-truth labels для ML
          </div>
        </div>
      </div>
    </div>
  );
}

// ── AI Assistant ──────────────────────────────────────────────────────────────

type AssistantMsg = { role: "user" | "assistant"; text: string; meta?: string };

function AIAssistant({
  items, selectedLotId,
}: {
  items: DashboardItem[]; selectedLotId: number | null;
}) {
  const [input, setInput] = useState("");
  const [msgs, setMsgs] = useState<AssistantMsg[]>([
    {
      role: "assistant",
      text: "Привет! Я AI‑ассистент на базе Gemini 1.5 Flash. Задай вопрос о тендере, поставщике или попроси объяснить риски. Например: «Что значит флаг SINGLE_BIDDER?» или «Объясни риски лота с высоким скором».",
    },
  ]);
  const [busy, setBusy] = useState(false);
  const bottomRef = React.useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom on new message
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [msgs, busy]);

  // Build context string from top risky lots
  const lotsContext = useMemo(() => {
    const top = [...items].sort((a, b) => b.risk_score - a.risk_score).slice(0, 10);
    if (top.length === 0) return "Данные лотов ещё загружаются через ETL.";
    return top
      .map(
        (l, i) =>
          `${i + 1}. Тендер ${l.tender_number || "N/A"} (Лот #${l.lot_id}): ${l.lot_name || "Без названия"} | Риск: ${Math.round(l.risk_score)} (${l.risk_level}) | Заказчик: ${l.customer_name || l.customer_bin} | Сумма: ${money(l.amount)}${l.top_reasons.length > 0 ? " | Флаги: " + l.top_reasons.map((r) => r.code).join(", ") : ""}`
      )
      .join("\n");
  }, [items]);

  async function ask() {
    const text = input.trim();
    if (!text || busy) return;
    setBusy(true);
    const newMsgs: AssistantMsg[] = [...msgs, { role: "user", text }];
    setMsgs(newMsgs);
    setInput("");

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: newMsgs.filter((m) => m.role !== "assistant" || m !== newMsgs[0]),
          lotsContext,
        }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Ошибка сети" }));
        throw new Error(err.error || "Ошибка API");
      }

      const data = await res.json();
      setMsgs((m) => [...m, { role: "assistant", text: data.text }]);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "Неизвестная ошибка";
      setMsgs((m) => [
        ...m,
        { role: "assistant", text: `⚠ Ошибка: ${message}` },
      ]);
    } finally {
      setBusy(false);
    }
  }


  return (
    <aside className="rounded-3xl border border-white/10 bg-white/5 p-4 flex flex-col">
      <div className="flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-2">
          <div className="rounded-2xl border border-white/10 bg-white/5 p-2">
            <Bot className="h-5 w-5 text-emerald-200" />
          </div>
          <div>
            <div className="text-sm font-semibold">AI Assistant</div>
            <div className="text-xs text-white/60">MCP · tool-based answers</div>
          </div>
        </div>
        <span className="rounded-full border border-emerald-500/25 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-200">
          Safe mode
        </span>
      </div>

      <DividerSpacer />

      <div className="flex-1 overflow-auto pr-1 space-y-3 min-h-0" style={{ maxHeight: "420px" }}>
        {msgs.map((m, i) => (
          <div
            key={i}
            className={`rounded-2xl border px-3 py-2.5 text-sm leading-relaxed ${m.role === "assistant"
              ? "border-white/10 bg-slate-950/40"
              : "border-indigo-500/20 bg-indigo-500/10"
              }`}
          >
            <div className="whitespace-pre-wrap text-white/90">{m.text}</div>
            {m.meta && <div className="mt-1.5 text-[11px] text-white/40">{m.meta}</div>}
          </div>
        ))}
        {busy && (
          <div className="rounded-2xl border border-white/10 bg-slate-950/40 px-3 py-2.5 text-sm text-white/40">
            <RefreshCw className="h-4 w-4 animate-spin inline mr-2" />Gemini думает...
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="mt-3 flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-3 py-2 flex-shrink-0">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Задай любой вопрос про тендеры..."
          className="w-full bg-transparent text-sm text-white placeholder:text-white/40 outline-none"
          onKeyDown={(e) => { if (e.key === "Enter") ask(); }}
        />
        <button
          onClick={ask}
          disabled={busy}
          className="inline-flex items-center gap-1 rounded-xl bg-emerald-500 px-3 py-1.5 text-xs font-medium text-slate-950 disabled:opacity-50 hover:bg-emerald-400 transition flex-shrink-0"
        >
          Ask <ArrowRight className="h-3.5 w-3.5" />
        </button>
      </div>

      <p className="mt-2 text-xs text-white/40">
        Gemini 1.5 Flash · знает топ-10 лотов системы · контекст обновляется
      </p>
    </aside>
  );
}

// ── Main App ─────────────────────────────────────────────────────────────────

type Nav = "dashboard" | "risk-list" | "detail" | "cases";

export default function AIProcurePage() {
  const router = useRouter();

  // Auth check
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) router.push("/login");
  }, [router]);

  // Data state
  const [items, setItems] = useState<DashboardItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);

  // UI state
  const [nav, setNav] = useState<Nav>("dashboard");
  const [search, setSearch] = useState("");
  const [minRisk, setMinRisk] = useState(0);
  const [levelFilter, setLevelFilter] = useState("");
  const [selectedLotId, setSelectedLotId] = useState<number | null>(null);
  const [cases, setCases] = useState<CaseItem[]>([]);

  // Load dashboard data
  const loadData = useCallback(async (pg = 1, append = false, levelF = levelFilter, minR = minRisk) => {
    setLoading(true);
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
        const incoming = data.items.filter((x) => x.risk_score >= minR);
        return append ? [...prev, ...incoming] : incoming;
      });
    } catch {
      // silently fail — show empty state
    } finally {
      setLoading(false);
    }
  }, [levelFilter, minRisk]);

  useEffect(() => {
    loadData(1, false, levelFilter, minRisk);
  }, [levelFilter]);

  // Filtered items for Risk List
  const filteredItems = useMemo(() => {
    const q = search.trim().toLowerCase();
    return items
      .filter((l) => {
        const passRisk = l.risk_score >= minRisk;
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
    <div className="min-h-screen" style={{ background: "#020817", color: "#f1f5f9" }}>
      {/* Background glow */}
      <div className="pointer-events-none fixed inset-0 -z-10 overflow-hidden">
        <div className="absolute left-[-10%] top-[-10%] h-[500px] w-[500px] rounded-full bg-indigo-500/15 blur-[100px]" />
        <div className="absolute right-[-10%] top-[10%] h-[400px] w-[400px] rounded-full bg-emerald-400/8 blur-[100px]" />
      </div>

      <div className="mx-auto max-w-[1600px] px-4 py-5">
        {/* Header */}
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between mb-5">
          <div>
            <div className="flex items-center gap-2">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-2">
                <Sparkles className="h-5 w-5 text-indigo-200" />
              </div>
              <h1 className="text-xl font-semibold tracking-tight">Tender Radar</h1>
            </div>
            <p className="mt-1 text-sm" style={{ color: "rgba(255,255,255,0.5)" }}>
              MVP: Risk Scoring + Explainability + Cases + AI Assistant
            </p>
          </div>
          <div className="flex items-center gap-2">
            <div className="hidden md:flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-3 py-2">
              <BadgeInfo className="h-4 w-4" style={{ color: "rgba(255,255,255,0.5)" }} />
              <span className="text-xs" style={{ color: "rgba(255,255,255,0.6)" }}>
                {total > 0 ? `${total.toLocaleString("ru")} лотов · Обновляется ежедневно` : "ETL загружает данные..."}
              </span>
            </div>
            <button className="rounded-2xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 transition" style={{ color: "rgba(255,255,255,0.7)" }}>
              <Download className="h-4 w-4 inline mr-1" />
              Export CSV
            </button>
            <button
              onClick={logout}
              className="rounded-2xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 transition"
              style={{ color: "rgba(255,255,255,0.7)" }}
            >
              Выйти
            </button>
          </div>
        </div>

        {/* 3-panel layout */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[280px_1fr_340px]">
          {/* ── Left Sidebar ── */}
          <aside className="rounded-3xl border border-white/10 bg-white/5 p-3 lg:h-fit">
            <div className="space-y-2">
              <NavBtn active={nav === "dashboard"} onClick={() => setNav("dashboard")} icon={<LayoutDashboard className="h-4 w-4" />} title="Dashboard" hint="Сводка рисков" />
              <NavBtn active={nav === "risk-list"} onClick={() => setNav("risk-list")} icon={<ListFilter className="h-4 w-4" />} title="Risk List" hint="Фильтры и поиск" />
              <NavBtn active={nav === "detail"} onClick={() => setNav("detail")} icon={<FileText className="h-4 w-4" />} title="Lot Detail" hint="Причины риска" />
              <NavBtn active={nav === "cases"} onClick={() => setNav("cases")} icon={<Briefcase className="h-4 w-4" />} title="Cases" hint="Рабочее место" />
            </div>

            <DividerSpacer />

            {/* Filters */}
            <div className="rounded-2xl border border-white/10 bg-slate-950/40 p-3 space-y-3">
              <div className="text-xs" style={{ color: "rgba(255,255,255,0.6)" }}>Quick search</div>
              <div className="flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-3 py-2">
                <Search className="h-4 w-4 flex-shrink-0" style={{ color: "rgba(255,255,255,0.4)" }} />
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="номер, заказчик..."
                  className="w-full bg-transparent text-sm outline-none placeholder:text-white/30"
                />
              </div>

              <div>
                <div className="flex items-center justify-between text-xs mb-2" style={{ color: "rgba(255,255,255,0.6)" }}>
                  <span>Min risk</span>
                  <span className="font-medium" style={{ color: "rgba(255,255,255,0.8)" }}>{minRisk}</span>
                </div>
                <input
                  type="range" min={0} max={100} value={minRisk}
                  onChange={(e) => setMinRisk(Number(e.target.value))}
                  className="w-full"
                />
              </div>

              <div className="flex flex-wrap gap-1.5">
                {(["", "LOW", "MEDIUM", "HIGH"] as const).map((l) => (
                  <button
                    key={l}
                    onClick={() => setLevelFilter(l)}
                    className={`rounded-full border px-2.5 py-1 text-xs transition ${levelFilter === l
                      ? "border-indigo-500/50 bg-indigo-500/20 text-indigo-200"
                      : "border-white/10 bg-white/5 text-white/70 hover:bg-white/10"
                      }`}
                  >
                    {l || "ALL"}
                  </button>
                ))}
              </div>
            </div>

            <DividerSpacer />

            {/* Selected lot */}
            <div className="rounded-2xl border border-white/10 bg-white/5 p-3">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-sm font-semibold">Selected</div>
                  <div className="text-xs truncate" style={{ color: "rgba(255,255,255,0.5)" }}>
                    {selectedItem?.tender_number || "Лот не выбран"}
                  </div>
                </div>
                {selectedLotId && (
                  <button
                    onClick={() => setNav("detail")}
                    className="rounded-xl border border-white/10 bg-white/5 px-2.5 py-1.5 text-xs hover:bg-white/10 transition"
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
          </aside>

          {/* ── Main Content ── */}
          <section className="rounded-3xl border border-white/10 bg-white/5 p-4 min-w-0">
            {nav === "dashboard" && (
              <DashboardView
                items={filteredItems}
                total={total}
                loading={loading}
                onOpenLot={openLot}
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
          </section>

          {/* ── AI Assistant ── */}
          <AIAssistant items={items} selectedLotId={selectedLotId} />
        </div>

        {/* Footer */}
        <div className="mt-5 flex flex-col gap-2 text-xs md:flex-row md:items-center md:justify-between" style={{ color: "rgba(255,255,255,0.35)" }}>
          <div>
            API:{" "}
            <span className="rounded border border-white/10 bg-white/5 px-2 py-0.5 font-mono">GET /dashboard</span>{" "}
            <span className="rounded border border-white/10 bg-white/5 px-2 py-0.5 font-mono">GET /lots/:id</span>{" "}
            <span className="rounded border border-white/10 bg-white/5 px-2 py-0.5 font-mono">POST /notes</span>
          </div>
          <div className="flex items-center gap-2">
            <Download className="h-3.5 w-3.5" />
            <span>PDF/CSV export (post‑MVP)</span>
          </div>
        </div>
      </div>
    </div>
  );
}
