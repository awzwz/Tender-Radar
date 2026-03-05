"use client";

import React, { useMemo } from "react";
import { useParams } from "next/navigation";
import {
  FileDown, ExternalLink, AlertTriangle, CheckCircle2, Info, RefreshCw,
  Scale, FileSearch, Users, ShieldAlert, Bot, FileText, XCircle,
} from "lucide-react";
import {
  generateTechSpecHtml,
  generateNormHtml,
  generateSupplierDocHtml,
  downloadHtml,
} from "@/lib/specnorm/exportDocs";
import { useFindingsData } from "@/lib/specnorm/useSpecNormData";
import {
  FLAG_LABELS, FLAG_BASE_WEIGHTS, PARAM_LABELS, PARAM_STRICTNESS,
  DOC_VIOLATION_LABELS, CATEGORY_LABELS,
  getRiskTier, getDocRiskTier, computeFlagContributions, isStricter,
} from "@/lib/specnorm/types";
import type { DocViolation, Severity } from "@/lib/specnorm/types";
import {
  SpecNormShell, RiskBadge, DocBadge, FlagChip, ViolationChip,
  SeverityBar, ComplianceBar, BreakdownBar, CategoryBadge,
} from "@/lib/specnorm/components";

const money = (n: number) =>
  new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

const SEVERITY_STYLES: Record<Severity, string> = {
  HIGH: "border-rose-500/25 bg-rose-500/5",
  MEDIUM: "border-amber-500/25 bg-amber-500/5",
  LOW: "border-yellow-500/25 bg-yellow-500/5",
};

export default function TenderDetail() {
  const params = useParams();
  const tenderId = params.tenderId as string;
  const { data, loading } = useFindingsData();

  const item = useMemo(() => data.find((d) => d.tenderId === tenderId), [data, tenderId]);

  const contributions = useMemo(
    () => (item ? computeFlagContributions(item.specDeviationScore, item.activeFlags || []) : []),
    [item],
  );

  const breakdownItems = useMemo(() => {
    if (!item) return [];
    const comboBonus = (item.activeFlags || []).length >= 2 ? 10 : 0;
    const result = contributions.map((c) => ({
      label: FLAG_LABELS[c.flag] ?? c.flag,
      value: c.contribution,
      color: "#f43f5e",
    }));
    if (comboBonus > 0) {
      result.push({ label: "Комбо-бонус (2+ флага)", value: comboBonus, color: "#94a3b8" });
    }
    return result;
  }, [contributions, item]);

  const comparisonRows = useMemo(() => {
    if (!item) return [];
    const allKeys = Array.from(
      new Set([...Object.keys(item.extracted || {}), ...Object.keys(item.norm || {})]),
    ).filter((k) => k !== "undefined");

    return allKeys.map((key) => {
      const ext = (item.extracted as Record<string, number | undefined>)?.[key];
      const norm = (item.norm as Record<string, number | undefined>)?.[key];
      const strict =
        ext !== undefined && norm !== undefined ? isStricter(key, ext, norm) : false;
      const dir = PARAM_STRICTNESS[key];
      const tooltip = dir === "higher_is_stricter"
        ? "Выше значение = строже требование"
        : dir === "lower_is_stricter"
          ? "Ниже значение = строже требование"
          : "";

      return { key, label: PARAM_LABELS[key] ?? key, ext, norm, strict, tooltip };
    });
  }, [item]);

  if (loading) {
    return (
      <SpecNormShell backHref="/specnorm/lots" backLabel="Список" title="Tender Analysis">
        <div className="flex items-center justify-center py-24 text-white/40">
          <RefreshCw className="h-6 w-6 animate-spin mr-2" /> Загрузка...
        </div>
      </SpecNormShell>
    );
  }

  if (!item) {
    return (
      <SpecNormShell backHref="/specnorm/lots" backLabel="Список" title="Tender Analysis">
        <div className="flex flex-col items-center justify-center py-24 text-white/40">
          <AlertTriangle className="h-10 w-10 mb-2" />
          <div className="text-sm text-[var(--text-muted)]">Тендер {tenderId} не найден</div>
        </div>
      </SpecNormShell>
    );
  }

  const specTier = getRiskTier(item.specDeviationScore);
  const docTier = getDocRiskTier(item.docComplianceScore ?? 100);
  const violations = item.docViolations || [];

  return (
    <SpecNormShell backHref="/specnorm/lots" backLabel="Список" title="Tender Analysis">
      <div className="space-y-5">
        {/* ── Hero Header ──────────────────────────────────────────────── */}
        <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-5 shadow-sm shadow-[var(--border-hover)]">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <CategoryBadge category={item.category} />
                <span className="text-xs text-[var(--text-muted)] font-mono">{item.tenderId}</span>
                {item.overallRiskTier === "HIGH" && (
                  <span className="rounded-full border border-rose-500/30 bg-rose-500/15 px-2.5 py-0.5 text-[10px] font-bold text-rose-500 dark:text-rose-300 animate-pulse">
                    HIGH RISK
                  </span>
                )}
              </div>
              <h1 className="mt-2 text-lg font-bold text-[var(--text-main)] leading-snug">{item.titleRu}</h1>
              <div className="mt-2 text-sm text-[var(--text-muted)]">
                {item.region} · {money(item.amountKZT)} · {item.publishDate?.slice(0, 10) ?? "—"}
              </div>
              {item.supplierName && (
                <div className="mt-1 text-sm text-[var(--text-muted)]">
                  <Users className="h-3.5 w-3.5 inline mr-1" />
                  Поставщик: <span className="text-[var(--text-muted)]">{item.supplierName}</span>
                </div>
              )}
            </div>

            <div className="flex flex-col items-end gap-3 flex-shrink-0">
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 text-center">
                  <div className="text-[10px] text-[var(--text-muted)] mb-1">Spec Deviation</div>
                  <RiskBadge score={item.specDeviationScore} size="lg" />
                  <div className="mt-2 w-32"><SeverityBar value={item.specDeviationScore} /></div>
                </div>
                <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3 text-center">
                  <div className="text-[10px] text-[var(--text-muted)] mb-1">Doc Compliance</div>
                  <DocBadge score={item.docComplianceScore ?? 100} size="lg" />
                  <div className="mt-2 w-32"><ComplianceBar value={item.docComplianceScore ?? 100} /></div>
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => {
                    const html = generateTechSpecHtml(item);
                    downloadHtml(html, `ТехСпец_${item.tenderId}.html`);
                  }}
                  className="flex items-center gap-1.5 rounded-xl border border-indigo-500/30 bg-indigo-500/10 px-3 py-2 text-xs text-indigo-700 dark:text-indigo-200 hover:bg-indigo-500/20 transition"
                >
                  <FileDown className="h-3.5 w-3.5" /> ТехСпец
                </button>
                <button
                  type="button"
                  onClick={() => {
                    const html = generateNormHtml(item);
                    downloadHtml(html, `Норматив_${item.tenderId}.html`);
                  }}
                  className="flex items-center gap-1.5 rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-200 hover:bg-amber-500/20 transition"
                >
                  <FileDown className="h-3.5 w-3.5" /> Норматив
                </button>
                <button
                  type="button"
                  onClick={() => {
                    const html = generateSupplierDocHtml(item);
                    downloadHtml(html, `Документы_поставщика_${item.tenderId}.html`);
                  }}
                  className="flex items-center gap-1.5 rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-3 py-2 text-xs text-cyan-700 dark:text-cyan-200 hover:bg-cyan-500/20 transition"
                >
                  <FileDown className="h-3.5 w-3.5" /> Документы поставщика
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* ── Two-column layout ────────────────────────────────────────── */}
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">

          {/* ── LEFT: Spec vs Norm (Заказчик) ──────────────────────────── */}
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Scale className="h-4 w-4 text-amber-500 dark:text-amber-300" />
              <span className="text-sm font-semibold text-[var(--text-main)]">Проверка Заказчика</span>
              <span className="text-xs text-[var(--text-muted)]">(Spec vs Norm)</span>
            </div>

            {/* Summary */}
            {(item.summary || []).length > 0 && (
              <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
                <div className="text-xs font-semibold text-[var(--text-muted)] mb-2">Выводы</div>
                <ul className="space-y-1.5">
                  {item.summary.map((s, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-[var(--text-muted)] hover:text-[var(--text-main)] transition-colors">
                      <AlertTriangle className="h-3.5 w-3.5 text-amber-500 dark:text-amber-400 flex-shrink-0 mt-0.5" />
                      {s}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Flags */}
            {(item.activeFlags || []).length > 0 && (
              <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
                <div className="text-xs font-semibold text-[var(--text-muted)] mb-2">Сработавшие флаги</div>
                <div className="flex flex-wrap gap-1.5 mb-3">
                  {item.activeFlags.map((f) => <FlagChip key={f} code={f} />)}
                </div>
                {breakdownItems.length > 0 && (
                  <BreakdownBar items={breakdownItems} total={item.specDeviationScore} />
                )}
              </div>
            )}

            {/* Param comparison table */}
            {comparisonRows.length > 0 && (
              <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
                <div className="text-xs font-semibold text-[var(--text-muted)] mb-2">Extracted vs Norm</div>
                <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-[var(--border)] text-[var(--text-muted)] text-left bg-[var(--surface-hover)]">
                        <th className="px-3 py-2.5">Параметр</th>
                        <th className="px-3 py-2.5">Из ТехСпец</th>
                        <th className="px-3 py-2.5">Норма</th>
                        <th className="px-3 py-2.5">Статус</th>
                      </tr>
                    </thead>
                    <tbody>
                      {comparisonRows.map((row) => (
                        <tr key={row.key} className={`border-b border-[var(--border)] ${row.strict ? "bg-rose-500/5" : ""}`}>
                          <td className="px-3 py-2.5 text-[var(--text-main)]">
                            <div className="flex items-center gap-1">
                              {row.label}
                              {row.tooltip && (
                                <span className="group relative">
                                  <Info className="h-3 w-3 text-[var(--text-muted)] cursor-help" />
                                  <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 hidden group-hover:block whitespace-nowrap rounded-lg border border-[var(--border)] bg-[var(--surface-hover)] px-2 py-1 text-[10px] text-[var(--text-main)] shadow-lg z-10">
                                    {row.tooltip}
                                  </span>
                                </span>
                              )}
                            </div>
                          </td>
                          <td className={`px-3 py-2.5 font-mono ${row.strict ? "text-rose-500 dark:text-rose-300 font-bold" : "text-[var(--text-main)]"}`}>
                            {row.ext !== undefined ? String(row.ext) : "—"}
                          </td>
                          <td className="px-3 py-2.5 font-mono text-[var(--text-muted)]">
                            {row.norm !== undefined ? String(row.norm) : "—"}
                          </td>
                          <td className="px-3 py-2.5">
                            {row.strict ? (
                              <span className="inline-flex items-center gap-1 rounded-full border border-rose-500/30 bg-rose-500/10 px-2 py-0.5 text-[10px] text-rose-300">
                                <AlertTriangle className="h-2.5 w-2.5" /> Строже
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-300">
                                <CheckCircle2 className="h-2.5 w-2.5" /> OK
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>

          {/* ── RIGHT: Document Check (Поставщик) ──────────────────────── */}
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <FileSearch className="h-4 w-4 text-cyan-500 dark:text-cyan-300" />
              <span className="text-sm font-semibold text-[var(--text-main)]">Проверка Поставщика</span>
              <span className="text-xs text-[var(--text-muted)]">(LLM Document Check)</span>
            </div>

            {/* Doc Summary */}
            {(item.docSummary || []).length > 0 && (
              <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
                <div className="text-xs font-semibold text-[var(--text-muted)] mb-2">Выводы</div>
                <ul className="space-y-1.5">
                  {item.docSummary.map((s, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-[var(--text-muted)] hover:text-[var(--text-main)] transition-colors">
                      {violations.length > 0
                        ? <XCircle className="h-3.5 w-3.5 text-rose-500 dark:text-rose-400 flex-shrink-0 mt-0.5" />
                        : <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 dark:text-emerald-400 flex-shrink-0 mt-0.5" />}
                      {s}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Violations list */}
            {violations.length > 0 ? (
              <div className="space-y-2">
                {violations.map((v, i) => (
                  <div key={i} className={`rounded-2xl border p-4 shadow-sm shadow-[var(--border-hover)] ${SEVERITY_STYLES[v.severity as Severity] ?? SEVERITY_STYLES.MEDIUM}`}>
                    <div className="flex items-center justify-between mb-2">
                      <ViolationChip type={v.type as any} severity={v.severity as Severity} />
                      <span className={`text-[10px] font-bold ${v.severity === "HIGH" ? "text-rose-500 dark:text-rose-300" : v.severity === "MEDIUM" ? "text-amber-500 dark:text-amber-300" : "text-yellow-500 dark:text-yellow-300"
                        }`}>
                        {v.severity}
                      </span>
                    </div>
                    <p className="text-sm text-[var(--text-main)] mb-2">{v.description}</p>
                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2">
                        <div className="text-[10px] text-[var(--text-muted)] mb-0.5">Требовалось:</div>
                        <div className="text-xs text-[var(--text-main)]">{v.requirement}</div>
                      </div>
                      <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2">
                        <div className="text-[10px] text-[var(--text-muted)] mb-0.5">Предоставлено:</div>
                        <div className="text-xs text-[var(--text-main)]">{v.provided}</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/5 p-4 text-center">
                <CheckCircle2 className="h-8 w-8 text-emerald-500 dark:text-emerald-400 mx-auto mb-2" />
                <div className="text-sm font-semibold text-emerald-600 dark:text-emerald-200">Нарушений не выявлено</div>
                <div className="text-xs text-[var(--text-muted)] mt-1">Документы поставщика соответствуют требованиям техспецификации</div>
              </div>
            )}

            {/* LLM Analysis */}
            {item.llmAnalysis && (
              <div className="rounded-2xl border border-indigo-500/20 bg-indigo-500/5 p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Bot className="h-4 w-4 text-indigo-500 dark:text-indigo-300" />
                  <span className="text-xs font-semibold text-indigo-600 dark:text-indigo-200">AI Анализ (OpenAI)</span>
                </div>
                <p className="text-sm text-[var(--text-main)] leading-relaxed">{item.llmAnalysis}</p>
                <div className="mt-2 text-[10px] text-[var(--text-muted)]">gpt-4o-mini · LLM-based document verification</div>
              </div>
            )}

            {/* Supplier info */}
            {item.supplierName && (
              <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
                <div className="text-xs font-semibold text-[var(--text-muted)] mb-2">Информация о поставщике</div>
                <div className="text-sm text-[var(--text-main)]">{item.supplierName}</div>
                <div className="mt-2 text-xs text-[var(--text-muted)]">
                  Doc Compliance Score: <span className="font-mono text-[var(--text-main)]">{item.docComplianceScore ?? 100}%</span>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </SpecNormShell>
  );
}
