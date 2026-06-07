"use client";

import React, { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  FileText,
  ChevronRight,
  RefreshCw,
  Bot,
  CheckCircle2,
  X,
} from "lucide-react";
import { api, LotDetail, ExplanationResponse, IndicatorDetails } from "@/lib/api";
import { Badge, Chip, DividerSpacer, ProgressBar, SectionTitle, money } from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";

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

  // ML Anomaly Interpretations
  PAUSED_TENDER: "Задержка тендера (дней)",
  CANCELLED_TENDER: "Отмен тендера (дней/счётчик)",
  LAST_MINUTE_CHANGES: "Изменения в последний момент",
  CUSTOMER_WINNER_CONCENTRATION: "Концентрация победителя у заказчика (%)",
  NEW_COMPANY_BIG_CONTRACT: "Капитализация молодой компании (показатель)",
  SUPPLIER_CONCENTRATION: "Концентрация поставщика (показатель)",
  CAROUSEL_PATTERN: "Паттерн ротации победителей (индекс)",
  LOT_SPLITTING: "Дробление лотов (индекс сходства)",
};

function formatWinnerSequence(sequence: any) {
  if (!sequence || typeof sequence !== "string") return String(sequence);
  const items = sequence.split(",").map(s => s.trim());
  if (items.length === 0) return sequence;

  const uniqueItems = Array.from(new Set(items));
  const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const itemToLetter: Record<string, string> = {};

  uniqueItems.forEach((item, index) => {
    itemToLetter[item] = letters[index % letters.length];
  });

  return items.map(item => itemToLetter[item] || item).join(" → ");
}

function BinLink({ bin, label, isCustomer = false }: { bin: string; label?: string; isCustomer?: boolean }) {
  const router = useRouter();
  const { t } = useI18n();
  const href = isCustomer ? `/customers/${bin}` : `/suppliers/${bin}`;
  return (
    <button
      onClick={() => router.push(href)}
      className="text-indigo-300 hover:text-indigo-200 hover:underline font-mono text-xs"
      title={t("lotDetail.openCompanyCard")}
    >
      {label ?? bin}
    </button>
  );
}

function LotDetailView({
  lotId, onCreateCase,
}: {
  lotId: number | null; onCreateCase: (note: string, lotId: number) => void;
}) {
  const { t } = useI18n();
  const [data, setData] = useState<LotDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [note, setNote] = useState(t("lotDetail.defaultNote"));
  const [explanation, setExplanation] = useState<ExplanationResponse | null>(null);
  const [explainLoading, setExplainLoading] = useState(false);
  const [indicatorDetail, setIndicatorDetail] = useState<IndicatorDetails | null>(null);
  const [indicatorDetailCode, setIndicatorDetailCode] = useState<string | null>(null);
  const [indicatorDetailLoading, setIndicatorDetailLoading] = useState(false);
  const router = useRouter();

  async function openIndicatorDetail(code: string) {
    if (!lotId) return;
    setIndicatorDetailCode(code);
    setIndicatorDetailLoading(true);
    setIndicatorDetail(null);
    try {
      const d = await api.lotIndicatorDetails(lotId, code);
      setIndicatorDetail(d);
    } catch {
      setIndicatorDetail(null);
    } finally {
      setIndicatorDetailLoading(false);
    }
  }

  async function handleExplain(force = false) {
    if (!lotId) return;
    setExplainLoading(true);
    try {
      const result = await api.explainLot(lotId, force);
      setExplanation(result);
    } catch (err) {
      setExplanation({ explanation: t("lotDetail.explainError"), error: String(err) });
    } finally {
      setExplainLoading(false);
    }
  }

  useEffect(() => {
    if (!lotId) return;
    setLoading(true);
    api.lot(lotId).then(setData).catch(() => setData(null)).finally(() => setLoading(false));
  }, [lotId]);

  if (!lotId) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-[var(--text-muted)] space-y-2">
        <FileText className="h-10 w-10" />
        <div className="text-sm">{t("lotDetail.emptyState")}</div>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="space-y-4">
        <SectionTitle
          icon={<FileText className="h-4 w-4 text-indigo-200" />}
          title={t("lotDetail.title")}
          hint={t("lotDetail.subtitle")}
        />
        {/* Header card skeleton */}
        <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1 space-y-2.5">
              <div className="skeleton h-5 w-3/5" />
              <div className="skeleton h-3 w-2/5" />
            </div>
            <div className="skeleton h-8 w-24 rounded-full" />
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="rounded-xl border border-[var(--border)] p-3 space-y-2">
                <div className="skeleton h-2.5 w-1/2" />
                <div className="skeleton h-4 w-3/4" />
              </div>
            ))}
          </div>
        </div>
        {/* Flags skeleton */}
        <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-2.5">
          <div className="skeleton h-4 w-40 mb-1" />
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="flex items-center justify-between gap-3">
              <div className="skeleton h-3.5 w-2/3" />
              <div className="skeleton h-5 w-16 rounded-full" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-64 text-rose-500 dark:text-rose-400 text-sm">
        {t("lotDetail.loadError")}
      </div>
    );
  }

  const { lot, tender, contract, risk, flags } = data;
  const triggeredFlags = flags.filter((f) => f.triggered);

  return (
    <div className="space-y-4 animate-fade-in">
      <SectionTitle
        icon={<FileText className="h-4 w-4 text-indigo-200" />}
        title={t("lotDetail.title")}
        hint={t("lotDetail.subtitle")}
      />

      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4 space-y-4">
        {/* Header */}
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div className="flex-1 min-w-0">
            <div className="text-lg font-semibold text-[var(--text-main)]">{lot.name_ru || t("lotDetail.lotFallback")}</div>
            <div className="mt-1 text-sm text-[var(--text-muted)]">
              {tender && <>№{tender.number_anno} · </>}
              {t("lotDetail.customer")}: <span className="text-[var(--text-main)]">{lot.customer_name || lot.customer_bin}</span>
              {contract && (
                <> · {t("lotDetail.supplier")}:{" "}
                  <button
                    onClick={() => router.push(`/suppliers/${contract.supplier_biin}`)}
                    className="text-indigo-600 dark:text-indigo-300 hover:underline"
                  >
                    {contract.supplier_biin}
                  </button>
                </>
              )}
            </div>
            <div className="mt-1 text-sm text-[var(--text-muted)]">
              {t("lotDetail.amount")}: <span className="text-[var(--text-main)] font-medium">{money(lot.amount)}</span>
              {tender && (
                <> · {t("lotDetail.period")}: <span className="text-[var(--text-main)]">{tender.start_date?.slice(0, 10)} — {tender.end_date?.slice(0, 10)}</span></>
              )}
            </div>
            {lot.dumping_flag && (
              <span className="mt-2 inline-block rounded-full border border-amber-500/30 bg-amber-500/10 px-2.5 py-1 text-xs text-amber-600 dark:text-amber-200">{t("lotDetail.dumping")}</span>
            )}
          </div>
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 text-right flex-shrink-0">
            <div className="text-xs text-[var(--text-muted)]">{t("lotDetail.finalRisk")}</div>
            <div className="mt-1"><Badge level={risk.level} score={Math.round(risk.score_final ?? risk.score)} /></div>
            <div className="mt-2 w-36"><ProgressBar value={risk.score_final ?? risk.score} /></div>
            <div className="mt-1 text-[11px] text-[var(--text-muted)]">{t("lotDetail.indicatorsCount", { n: triggeredFlags.length })}</div>
            {/* Risk breakdown gauges */}
            <div className="mt-3 space-y-1.5 text-left">
              <div className="flex items-center justify-between text-[11px]">
                <span className="text-[var(--text-muted)]">{t("lotDetail.rules")}</span>
                <span className="text-[var(--text-main)] font-mono">{risk.score_rules != null ? Math.round(risk.score_rules) : '—'}</span>
              </div>
              <div className="h-1 w-full rounded-full bg-[var(--border)] overflow-hidden">
                <div className="h-full bg-amber-500 transition-all" style={{ width: `${risk.score_rules ?? 0}%` }} />
              </div>
              <div className="flex items-center justify-between text-[11px]">
                <span className="text-[var(--text-muted)]">{t("lotDetail.ml")}</span>
                <span className="text-[var(--text-main)] font-mono">{risk.score_ml != null ? (risk.score_ml * 100).toFixed(0) + '%' : '—'}</span>
              </div>
              <div className="h-1 w-full rounded-full bg-[var(--border)] overflow-hidden">
                <div className="h-full bg-indigo-500 transition-all" style={{ width: `${(risk.score_ml ?? 0) * 100}%` }} />
              </div>
              <div className="flex items-center justify-between text-[11px]">
                <span className="text-[var(--text-muted)]">{t("lotDetail.final")}</span>
                <span className="text-[var(--text-main)] font-mono">{risk.score_final != null ? Math.round(risk.score_final) : '—'}</span>
              </div>
              <div className="h-1 w-full rounded-full bg-[var(--border)] overflow-hidden">
                <div className={`h-full transition-all ${(risk.score_final ?? 0) >= 70 ? 'bg-rose-500' : (risk.score_final ?? 0) >= 40 ? 'bg-amber-500' : 'bg-emerald-500'}`} style={{ width: `${risk.score_final ?? 0}%` }} />
              </div>
            </div>
          </div>
        </div>

        <DividerSpacer />

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Triggered Flags */}
          <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface-hover)] p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-semibold text-[var(--text-main)]">{t("lotDetail.triggeredFlags")}</div>
              <span className="text-xs text-[var(--text-muted)]">{t("lotDetail.evidenceBased")}</span>
            </div>
            {triggeredFlags.length === 0 ? (
              <div className="text-xs text-[var(--text-muted)] py-4 text-center">{t("lotDetail.noRiskSignals")}</div>
            ) : (
              <div className="space-y-2">
                {triggeredFlags.map((f, i) => (
                  <button
                    key={`${f.code}-${i}`}
                    onClick={() => openIndicatorDetail(f.code)}
                    className="w-full rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-3 text-left hover:border-[var(--border-hover)] transition group"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1 min-w-0">
                        <span className="rounded-md border border-[var(--border)] bg-[var(--surface-hover)] px-2 py-0.5 text-[11px] text-[var(--text-main)] font-mono">{f.code}</span>
                        {f.code !== "CUSTOMER_WINNER_CONCENTRATION" && (
                          <span className="ml-2 text-[10px] text-[var(--text-muted)] group-hover:text-indigo-500 dark:group-hover:text-indigo-400">{t("lotDetail.more")}</span>
                        )}
                        {f.value !== null && (
                          <div className="mt-1 text-xs text-[var(--text-muted)]">
                            {t("lotDetail.evidence")}: <span className="font-mono text-[var(--text-main)]">{String(f.value)}</span>
                          </div>
                        )}
                        {f.evidence && Object.keys(f.evidence).length > 0 && (
                          <div className="mt-1 text-xs text-[var(--text-muted)] space-y-0.5">
                            {Object.entries(f.evidence).slice(0, 3).map(([k, v]) => (
                              <div key={k}>
                                <span className="opacity-70">{EVIDENCE_LABELS[k] || k}:</span> <span className="text-[var(--text-main)]">{String(v)}</span>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                      <ChevronRight className="h-4 w-4 text-[var(--text-muted)] group-hover:text-indigo-500 dark:group-hover:text-indigo-400 flex-shrink-0" />
                    </div>
                  </button>
                ))}
              </div>
            )}

            <div className="mt-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-3">
              <div className="text-xs text-[var(--text-muted)] mb-2">{t("lotDetail.whatToCheckNext")}</div>
              <ul className="list-disc pl-4 text-sm text-[var(--text-main)] space-y-1">
                <li>{t("lotDetail.check1")}</li>
                <li>{t("lotDetail.check2")}</li>
                <li>{t("lotDetail.check3")}</li>
              </ul>
            </div>

            {/* AI Explanation Button */}
            <div className="mt-3 rounded-2xl border border-indigo-500/20 bg-indigo-500/5 p-3">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <Bot className="h-4 w-4 text-indigo-500 dark:text-indigo-300" />
                  <span className="text-xs font-semibold text-indigo-700 dark:text-indigo-200">{t("lotDetail.aiExplanation")}</span>
                </div>
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => handleExplain(false)}
                    disabled={explainLoading}
                    className="rounded-xl bg-indigo-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-indigo-400 transition disabled:opacity-50"
                  >
                    {explainLoading ? <><RefreshCw className="h-3 w-3 animate-spin inline mr-1" />{t("lotDetail.generating")}</> : t("lotDetail.explain")}
                  </button>
                  {explanation?.cached && (
                    <button
                      onClick={() => handleExplain(true)}
                      disabled={explainLoading}
                      className="rounded-xl border border-indigo-500/30 px-2 py-1.5 text-xs text-indigo-600 dark:text-indigo-300 hover:bg-indigo-500/10 transition disabled:opacity-50"
                      title={t("lotDetail.regenerate")}
                    >
                      <RefreshCw className="h-3 w-3" />
                    </button>
                  )}
                </div>
              </div>
              {explanation && (
                <div className="space-y-2">
                  {explanation.error && !explanation.explanation && (
                    <div className="text-xs text-rose-500 dark:text-rose-300">{explanation.error}</div>
                  )}
                  {explanation.eligible === false && (
                    <div className="text-xs text-[var(--text-muted)]">{t("lotDetail.explanationUnavailable", { reason: explanation.reason ?? "" })}</div>
                  )}
                  {explanation.explanation && (
                    <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-3">
                      <div className="text-sm text-[var(--text-main)] whitespace-pre-wrap leading-relaxed">
                        {explanation.explanation}
                      </div>
                    </div>
                  )}
                  {explanation.spec_analysis && (
                    <div className={`rounded-xl border p-3 ${explanation.spec_analysis.risky ? "border-amber-500/30 bg-amber-500/10" : "border-emerald-500/20 bg-emerald-500/5"}`}>
                      <div className={`text-xs font-semibold mb-1 ${explanation.spec_analysis.risky ? "text-amber-600 dark:text-amber-200" : "text-emerald-600 dark:text-emerald-200"}`}>
                        {explanation.spec_analysis.risky ? t("lotDetail.specRisk") : t("lotDetail.specLowRisk")}
                      </div>
                      <p className="text-sm text-[var(--text-main)]">{explanation.spec_analysis.reasoning}</p>
                    </div>
                  )}
                  {explanation.checklist && explanation.checklist.length > 0 && (
                    <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 p-3">
                      <div className="text-xs font-semibold text-emerald-600 dark:text-emerald-200 mb-1">{t("lotDetail.whatToCheck")}</div>
                      <ul className="list-disc pl-4 text-sm text-[var(--text-main)] space-y-1">
                        {explanation.checklist.map((item, i) => (
                          <li key={i}>{item}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {explanation.model_used && (
                    <div className="text-[10px] text-[var(--text-muted)]">
                      {explanation.cached ? t("lotDetail.cache") : t("lotDetail.freshGeneration")} · {explanation.model_used}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* Create Case */}
          <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface-hover)] p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-semibold text-[var(--text-main)]">{t("lotDetail.topReasons")}</div>
              <span className="text-xs text-[var(--text-muted)]">{t("lotDetail.fromModel")}</span>
            </div>
            <div className="space-y-2">
              {risk.top_reasons.map((r: any, i: number) => (
                <div key={`${r.code}-${i}`} className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2">
                  <span className="text-xs font-mono text-indigo-600 dark:text-indigo-300">{r.code}</span>
                  {r.description && <div className="text-xs text-[var(--text-muted)] mt-0.5">{r.description}</div>}
                  {r.evidence && Object.keys(r.evidence).length > 0 && (
                    <div className="mt-1.5 space-y-0.5 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2 text-xs text-[var(--text-muted)]">
                      {Object.entries(r.evidence).map(([ek, ev]: [string, any]) => (
                        <div key={ek} className={`flex justify-between items-center gap-2 ${ek === "winner_sequence" ? "flex-col items-start" : ""}`}>
                          <span>{EVIDENCE_LABELS[ek] || ek}:</span>
                          <span className={`${ek === "winner_sequence" ? "text-indigo-500 dark:text-indigo-400 font-bold mt-1" : "text-[var(--text-main)]"} font-mono text-right truncate`}>
                            {ek === "winner_sequence" ? formatWinnerSequence(String(ev)) : String(ev)}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
              {risk.top_reasons.length === 0 && (
                <div className="text-xs text-[var(--text-muted)] py-2 text-center">{t("lotDetail.noData")}</div>
              )}
            </div>

            <DividerSpacer />

            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-3">
              <div className="text-xs text-[var(--text-muted)] mb-2">{t("lotDetail.createCase")}</div>
              <textarea
                value={note}
                onChange={(e) => setNote(e.target.value)}
                className="h-20 w-full resize-none rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 text-sm text-[var(--text-main)] outline-none placeholder:opacity-50"
              />
              <div className="mt-2 flex items-center justify-between gap-2">
                <div className="text-xs text-[var(--text-muted)]">{t("lotDetail.willBecomeLabel")}</div>
                <button
                  onClick={() => onCreateCase(note, lot.id)}
                  className="rounded-xl bg-emerald-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-400 transition"
                >
                  {t("lotDetail.create")}
                </button>
              </div>
            </div>

            <div className="mt-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2">
              <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
                <CheckCircle2 className="h-4 w-4 text-emerald-500 dark:text-emerald-300" />
                {t("lotDetail.explainability")}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Indicator Detail Modal */}
      {indicatorDetailCode && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 dark:bg-black/70 backdrop-blur-sm p-4"
          onClick={() => { setIndicatorDetailCode(null); setIndicatorDetail(null); }}
        >
          <div
            className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-4 border-b border-[var(--border)]">
              <h3 className="text-sm font-semibold text-[var(--text-main)] font-mono">{indicatorDetailCode}</h3>
              <button
                onClick={() => { setIndicatorDetailCode(null); setIndicatorDetail(null); }}
                className="rounded-lg p-1.5 text-[var(--text-muted)] hover:text-[var(--text-main)] hover:bg-[var(--surface-hover)]"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="p-4 overflow-auto flex-1">
              {indicatorDetailLoading ? (
                <div className="flex items-center justify-center py-12 text-[var(--text-muted)]">
                  <RefreshCw className="h-6 w-6 animate-spin mr-2" /> {t("lotDetail.loading")}
                </div>
              ) : indicatorDetail?.code === "CAROUSEL_PATTERN" && indicatorDetail.contracts ? (
                <div className="space-y-4">
                  <div className="text-xs text-[var(--text-muted)] space-y-1">
                    <p>{t("lotDetail.customer")}: <BinLink bin={indicatorDetail.customer_bin!} label={indicatorDetail.customer_name || indicatorDetail.customer_bin} isCustomer /> ({indicatorDetail.customer_bin})</p>
                    <p>{t("lotDetail.rotations")}: <span className="text-[var(--text-main)] font-mono">{indicatorDetail.rotation_count}</span> · {t("lotDetail.uniqueWinners")}: <span className="font-mono">{indicatorDetail.unique_winners}</span></p>
                  </div>
                  <div className="text-xs font-medium text-[var(--text-main)] mb-2">{t("lotDetail.contractChain")}</div>
                  <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-[var(--border)] text-[var(--text-muted)] text-left">
                          <th className="px-3 py-2">{t("lotDetail.contractNumber")}</th>
                          <th className="px-3 py-2">{t("lotDetail.date")}</th>
                          <th className="px-3 py-2">{t("lotDetail.supplierBin")}</th>
                          <th className="px-3 py-2">{t("lotDetail.amountCol")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {indicatorDetail.contracts.map((c, i) => (
                          <tr key={i} className="border-b border-[var(--border)] hover:bg-[var(--surface-hover)]">
                            <td className="px-3 py-2 font-mono text-[var(--text-main)]">{c.contract_number}</td>
                            <td className="px-3 py-2 text-[var(--text-muted)]">{c.sign_date?.slice(0, 10) ?? "—"}</td>
                            <td className="px-3 py-2">
                              {c.supplier_biin ? (
                                <BinLink bin={c.supplier_biin} label={c.supplier_name ? `${c.supplier_name} (${c.supplier_biin})` : c.supplier_biin} />
                              ) : "—"}
                            </td>
                            <td className="px-3 py-2 text-[var(--text-main)]">{money(c.contract_sum)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : indicatorDetail?.code === "RECURRING_WINNER" && indicatorDetail.contracts ? (
                <div className="space-y-4">
                  <div className="text-xs text-[var(--text-muted)] space-y-1">
                    <p>{t("lotDetail.customer")}: <BinLink bin={indicatorDetail.customer_bin!} label={indicatorDetail.customer_bin} isCustomer /></p>
                    <p>{t("lotDetail.supplier")}: <BinLink bin={indicatorDetail.supplier_biin!} label={indicatorDetail.supplier_name || indicatorDetail.supplier_biin} /> ({indicatorDetail.supplier_biin})</p>
                  </div>
                  <div className="text-xs font-medium text-[var(--text-main)] mb-2">{t("lotDetail.supplierContractsAtCustomer")}</div>
                  <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-[var(--border)] text-[var(--text-muted)] text-left">
                          <th className="px-3 py-2">{t("lotDetail.contractNumber")}</th>
                          <th className="px-3 py-2">{t("lotDetail.date")}</th>
                          <th className="px-3 py-2">{t("lotDetail.amountCol")}</th>
                          <th className="px-3 py-2">{t("lotDetail.tender")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {indicatorDetail.contracts.map((c, i) => (
                          <tr key={i} className="border-b border-[var(--border)] hover:bg-[var(--surface-hover)]">
                            <td className="px-3 py-2 font-mono text-[var(--text-main)]">{c.contract_number}</td>
                            <td className="px-3 py-2 text-[var(--text-muted)]">{c.sign_date?.slice(0, 10) ?? "—"}</td>
                            <td className="px-3 py-2 text-[var(--text-main)]">{money(c.contract_sum)}</td>
                            <td className="px-3 py-2 text-[var(--text-muted)]">{c.tender_number ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : indicatorDetail?.evidence && Object.keys(indicatorDetail.evidence).length > 0 ? (
                <div className="space-y-2 text-xs">
                  {Object.entries(indicatorDetail.evidence).map(([k, v]) => (
                    <div key={k} className="flex gap-2">
                      <span className="text-[var(--text-muted)] shrink-0">{EVIDENCE_LABELS[k] || k}:</span>
                      <span className="text-[var(--text-main)] break-all">
                        {Array.isArray(v) ? v.join(", ") : typeof v === "object" ? JSON.stringify(v) : String(v)}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-sm text-[var(--text-muted)] py-8 text-center">{t("lotDetail.noExtendedData")}</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default LotDetailView;
