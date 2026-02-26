/**
 * Generates HTML documents from TenderFinding JSON for download.
 * Each document contains real content from the analysis data.
 */

import type { TenderFinding } from "./types";
import { PARAM_LABELS, DOC_VIOLATION_LABELS, FLAG_LABELS, CATEGORY_LABELS } from "./types";

const money = (n: number) =>
  new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

function formatValue(v: unknown): string {
  if (v === undefined || v === null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function htmlDoc(title: string, body: string): string {
  return `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; max-width: 900px; line-height: 1.5; color: #1e293b; }
    h1 { font-size: 1.25rem; margin-bottom: 1rem; color: #0f172a; }
    h2 { font-size: 1rem; margin: 1.25rem 0 0.5rem; color: #334155; }
    table { width: 100%; border-collapse: collapse; margin: 0.5rem 0; }
    th, td { padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #e2e8f0; }
    th { background: #f8fafc; font-weight: 600; font-size: 0.8rem; color: #64748b; }
    .meta { font-size: 0.875rem; color: #64748b; margin-bottom: 1rem; }
    ul { margin: 0.5rem 0; padding-left: 1.25rem; }
    .violation { border-left: 3px solid #f59e0b; padding: 0.75rem; margin: 0.5rem 0; background: #fffbeb; }
    .severity-high { border-color: #ef4444; background: #fef2f2; }
    .severity-medium { border-color: #f59e0b; background: #fffbeb; }
    .severity-low { border-color: #22c55e; background: #f0fdf4; }
    .badge { display: inline-block; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    @media print { body { margin: 1rem; } }
  </style>
</head>
<body>
${body}
</body>
</html>`;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function generateTechSpecHtml(item: TenderFinding): string {
  const meta = `ID: ${escapeHtml(item.tenderId)} · ${escapeHtml(item.region)} · ${money(item.amountKZT)} · ${escapeHtml(CATEGORY_LABELS[item.category] ?? item.category)}`;
  const extracted = item.extracted ?? {};
  const keys = Object.keys(extracted).filter((k) => k !== "undefined");

  const paramsRows = keys
    .map(
      (key) =>
        `<tr><td>${escapeHtml(PARAM_LABELS[key] ?? key)}</td><td>${escapeHtml(formatValue(extracted[key]))}</td></tr>`,
    )
    .join("");

  const flags =
    (item.activeFlags ?? []).length > 0
      ? `<h2>Сработавшие флаги</h2><ul>${(item.activeFlags ?? [])
          .map((f) => `<li>${escapeHtml(FLAG_LABELS[f] ?? f)}</li>`)
          .join("")}</ul>`
      : "";

  const summary =
    (item.summary ?? []).length > 0
      ? `<h2>Выводы (Spec vs Norm)</h2><ul>${(item.summary ?? [])
          .map((s) => `<li>${escapeHtml(s)}</li>`)
          .join("")}</ul>`
      : "";

  const body = `
  <h1>Техническая спецификация — ${escapeHtml(item.titleRu)}</h1>
  <div class="meta">${meta}</div>
  ${summary}
  ${keys.length > 0 ? `<h2>Извлечённые параметры из ТехСпец</h2><table><thead><tr><th>Параметр</th><th>Значение</th></tr></thead><tbody>${paramsRows}</tbody></table>` : ""}
  ${flags}
  `;

  return htmlDoc(`ТехСпец — ${item.tenderId}`, body);
}

export function generateNormHtml(item: TenderFinding): string {
  const meta = `ID: ${escapeHtml(item.tenderId)} · ${escapeHtml(item.region)} · ${money(item.amountKZT)}`;
  const norm = item.norm ?? {};
  const keys = Object.keys(norm).filter((k) => k !== "undefined");

  const paramsRows = keys
    .map(
      (key) =>
        `<tr><td>${escapeHtml(PARAM_LABELS[key] ?? key)}</td><td>${escapeHtml(formatValue(norm[key]))}</td></tr>`,
    )
    .join("");

  const summary =
    (item.summary ?? []).length > 0
      ? `<h2>Соответствие нормам</h2><ul>${(item.summary ?? [])
          .map((s) => `<li>${escapeHtml(s)}</li>`)
          .join("")}</ul>`
      : "";

  const body = `
  <h1>Нормативные требования — ${escapeHtml(item.titleRu)}</h1>
  <div class="meta">${meta}</div>
  ${summary}
  ${keys.length > 0 ? `<h2>Параметры по нормативу</h2><table><thead><tr><th>Параметр</th><th>Значение</th></tr></thead><tbody>${paramsRows}</tbody></table>` : ""}
  `;

  return htmlDoc(`Норматив — ${item.tenderId}`, body);
}

export function generateSupplierDocHtml(item: TenderFinding): string {
  const meta = `ID: ${escapeHtml(item.tenderId)} · Поставщик: ${escapeHtml(item.supplierName ?? "—")} · Соответствие: ${item.docComplianceScore ?? "—"}%`;
  const violations = item.docViolations ?? [];
  const summary = item.docSummary ?? [];
  const llm = item.llmAnalysis ?? "";

  const violationsHtml =
    violations.length > 0
      ? `
  <h2>Выявленные несоответствия</h2>
  ${violations
    .map(
      (v) => `
  <div class="violation severity-${(v.severity ?? "LOW").toLowerCase()}">
    <strong>${escapeHtml(DOC_VIOLATION_LABELS[v.type] ?? v.type)}</strong>
    <span class="badge">${escapeHtml(v.severity ?? "")}</span>
    <p>${escapeHtml(v.description)}</p>
    <p><strong>Требование:</strong> ${escapeHtml(v.requirement ?? "")}</p>
    <p><strong>Предоставлено:</strong> ${escapeHtml(v.provided ?? "")}</p>
  </div>`,
    )
    .join("")}
  `
      : "<p>Несоответствий не обнаружено.</p>";

  const summaryHtml =
    summary.length > 0
      ? `<h2>Итог по документам</h2><ul>${summary.map((s) => `<li>${escapeHtml(s)}</li>`).join("")}</ul>`
      : "";

  const llmHtml = llm ? `<h2>Анализ</h2><p>${escapeHtml(llm)}</p>` : "";

  const body = `
  <h1>Документы поставщика — ${escapeHtml(item.titleRu)}</h1>
  <div class="meta">${meta}</div>
  ${summaryHtml}
  ${violationsHtml}
  ${llmHtml}
  `;

  return htmlDoc(`Документы поставщика — ${item.tenderId}`, body);
}

/** Triggers download of HTML content as a file */
export function downloadHtml(html: string, filename: string): void {
  const blob = new Blob([html], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
