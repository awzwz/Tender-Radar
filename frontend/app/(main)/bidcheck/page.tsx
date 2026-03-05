"use client";

import React, { useState, useCallback, useEffect } from "react";
import {
  FileText,
  Upload,
  RefreshCw,
  Copy,
  Download,
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Users,
  ChevronDown,
  ChevronUp,
  ShieldCheck,
  ShieldX,
  FileDown,
} from "lucide-react";
import { parseBidcheckPdf, parseAndAnalyzeCompliance } from "@/lib/api/bidcheck";
import type {
  RequirementsDoc,
  SupplierCandidate,
  RequirementItem,
  BidcheckEnd2endResponse,
} from "@/lib/bidcheck/types";

/* ── Shell (reuse SpecNorm style) ───────────────────────────────────────────── */

function BidCheckShell({
  children,
  title = "BidCheck",
  subtitle = "Parse PDF · Generate Suppliers",
}: {
  children: React.ReactNode;
  title?: string;
  subtitle?: string;
}) {
  return (
    <div className="min-h-screen bg-[var(--root-bg)] text-[var(--root-fg)]">
      <div className="mx-auto max-w-[1200px] px-4 py-5">
        <div className="flex items-center gap-3 mb-5">
          <a
            href="/"
            className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition"
          >
            <ArrowLeft className="h-3.5 w-3.5" /> Главная
          </a>
          <div className="flex items-center gap-2">
            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-2 shadow-sm shadow-[var(--border-hover)]">
              <FileText className="h-5 w-5 text-emerald-500 dark:text-emerald-200" />
            </div>
            <div>
              <div className="text-lg font-semibold tracking-tight text-[var(--text-main)]">{title}</div>
              <div className="text-xs text-[var(--text-muted)]">{subtitle}</div>
            </div>
          </div>
        </div>
        {children}
      </div>
    </div>
  );
}

/* ── JSON actions (Copy / Download) ─────────────────────────────────────────── */

function JsonActions({ data, filename }: { data: object; filename: string }) {
  const [copied, setCopied] = useState(false);
  const json = JSON.stringify(data, null, 2);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(json);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [json]);

  const handleDownload = useCallback(() => {
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }, [json, filename]);

  return (
    <div className="flex items-center gap-2">
      <button
        onClick={handleCopy}
        className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-2.5 py-1.5 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition shadow-sm"
      >
        {copied ? (
          <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 dark:text-emerald-400" />
        ) : (
          <Copy className="h-3.5 w-3.5" />
        )}
        {copied ? "Copied" : "Copy JSON"}
      </button>
      <button
        onClick={handleDownload}
        className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-2.5 py-1.5 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition shadow-sm"
      >
        <Download className="h-3.5 w-3.5" />
        Download JSON
      </button>
    </div>
  );
}

/* ── Requirements Viewer ───────────────────────────────────────────────────── */

function RequirementsViewer({ requirements }: { requirements: RequirementsDoc }) {
  const items = requirements.labor_requirements ?? [];
  const hasEquip = requirements.equipment_requirements && requirements.equipment_requirements.length > 0;
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <FileText className="h-4 w-4 text-emerald-500 dark:text-emerald-300" />
          <span className="text-sm font-semibold text-[var(--text-main)]">Requirements</span>
        </div>
        <JsonActions data={requirements} filename="requirements.json" />
      </div>
      <div className="text-xs text-[var(--text-muted)] mb-3">
        {requirements.source_filename} · {requirements.parsed_at_utc}
      </div>
      <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)] text-left">
              <th className="py-2.5 px-3 text-[var(--text-muted)]">Role</th>
              <th className="py-2.5 px-3 text-[var(--text-muted)]">Count</th>
              <th className="py-2.5 px-3 text-[var(--text-muted)]">Notes</th>
            </tr>
          </thead>
          <tbody>
            {items.map((r: RequirementItem, i: number) => (
              <tr key={i} className="border-b border-[var(--border)]">
                <td className="py-2.5 px-3 text-[var(--text-main)]">{r.role}</td>
                <td className="py-2.5 px-3 text-[var(--text-muted)]">{r.count ?? "—"}</td>
                <td className="py-2.5 px-3 text-[var(--text-muted)]">{r.notes ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hasEquip && (
        <div className="mt-3 pt-3 border-t border-white/10">
          <div className="text-xs text-white/50 mb-1">Equipment / other</div>
          <pre className="text-xs text-white/70 overflow-x-auto max-h-32 overflow-y-auto">
            {JSON.stringify(requirements.equipment_requirements, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

function SupplierCard({ supplier }: { supplier: SupplierCandidate }) {
  const [expanded, setExpanded] = useState(false);

  const getMatchColor = (score: number) => {
    if (score >= 80) return "text-emerald-500 dark:text-emerald-400";
    if (score >= 50) return "text-amber-500 dark:text-amber-400";
    return "text-rose-500 dark:text-rose-400";
  };

  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] shadow-sm shadow-[var(--border-hover)] overflow-hidden">
      {/* Header (Always visible) */}
      <div
        className="flex cursor-pointer items-center justify-between p-4 hover:bg-[var(--surface-hover)] transition"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-3">
          <div className="relative flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-[var(--border)] bg-[var(--surface-hover)]">
            <svg viewBox="0 0 36 36" className="absolute inset-0 h-full w-full -rotate-90">
              <circle cx="18" cy="18" r="16" fill="none" className="stroke-[var(--border)]" strokeWidth="2" />
              <circle
                cx="18" cy="18" r="16" fill="none"
                className={`stroke-current ${getMatchColor(supplier.confidence ? supplier.confidence * 100 : 0)} transition-all duration-1000 ease-out`}
                strokeWidth="2"
                strokeDasharray="100 100"
                strokeDashoffset={100 - (supplier.confidence ? supplier.confidence * 100 : 0)}
              />
            </svg>
            <span className={`text-[11px] font-bold ${getMatchColor(supplier.confidence ? supplier.confidence * 100 : 0)}`}>
              {supplier.confidence ? `${Math.round(supplier.confidence * 100)}%` : "0%"}
            </span>
          </div>
          <div>
            <div className="text-sm font-semibold text-[var(--text-main)] leading-tight">{supplier.name}</div>
            <div className="text-xs text-[var(--text-muted)] font-mono mt-0.5">BIN: {supplier.bin_iin ?? "—"}</div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {expanded ? <ChevronUp className="h-4 w-4 text-[var(--text-muted)]" /> : <ChevronDown className="h-4 w-4 text-[var(--text-muted)]" />}
        </div>
      </div>

      {/* Expanded Details */}
      {expanded && (
        <div className="border-t border-[var(--border)] p-4 bg-[var(--surface-hover)] space-y-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <div className="text-[10px] uppercase tracking-wider text-[var(--text-muted)] mb-1">Company Info</div>
              <div className="text-xs text-[var(--text-main)] space-y-1">
                <div><span className="text-[var(--text-muted)]">Capabilities:</span> {supplier.capabilities?.join(", ") || "—"}</div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Suppliers Viewer ──────────────────────────────────────────────────────── */

function SuppliersViewer({
  suppliers,
  summary,
}: {
  suppliers: SupplierCandidate[];
  summary?: string;
}) {
  const data = { suppliers, summary };
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Users className="h-4 w-4 text-indigo-500 dark:text-indigo-300" />
          <span className="text-sm font-semibold text-[var(--text-main)]">Suppliers</span>
        </div>
        <JsonActions data={data} filename="suppliers.json" />
      </div>
      {summary && (
        <div className="text-xs text-[var(--text-muted)] mb-3 p-2 rounded-xl bg-[var(--surface-hover)] border border-[var(--border)]">
          {summary}
        </div>
      )}
      <div className="space-y-3">
        {suppliers.map((s, i) => (
          <SupplierCard key={i} supplier={s} />
        ))}
      </div>
    </div>
  );
}

/* ── End2end Result Viewer (demo_bidcheck-3 style) ─────────────────────────── */

function End2endResultViewer({ data }: { data: BidcheckEnd2endResponse }) {
  const [expandedSupplier, setExpandedSupplier] = useState<string | null>(null);
  const roles = data.requirements?.labor_roles ?? [];
  const suppliers = data.suppliers ?? [];
  const summaries = data.summaries ?? [];

  return (
    <div className="space-y-4">
      {/* Requirements */}
      <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
        <div className="flex items-center justify-between mb-3">
          <span className="text-sm font-semibold text-[var(--text-main)]">Требования (labor_roles)</span>
          <JsonActions data={data} filename="end2end.json" />
        </div>
        <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)] text-left">
                <th className="py-2.5 px-3 text-[var(--text-muted)]">Роль</th>
                <th className="py-2.5 px-3 text-[var(--text-muted)]">Кол-во</th>
                <th className="py-2.5 px-3 text-[var(--text-muted)]">Документы</th>
              </tr>
            </thead>
            <tbody>
              {roles.map((r, i) => (
                <tr key={i} className="border-b border-[var(--border)]">
                  <td className="py-2.5 px-3 text-[var(--text-main)]">{r.role}</td>
                  <td className="py-2.5 px-3 text-[var(--text-muted)]">{r.count}</td>
                  <td className="py-2.5 px-3 text-[var(--text-muted)] text-xs">
                    {(r.required_documents ?? []).join(", ") || "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Suppliers + Summaries */}
      {suppliers.map((sup, i) => {
        const sum = summaries.find((s) => s.supplier_name === sup.supplier_name);
        const isExpanded = expandedSupplier === sup.supplier_name;
        const isPass = sum?.verdict === "PASS";
        return (
          <div
            key={i}
            className={`rounded-2xl border p-4 shadow-sm bg-[var(--surface)] ${isPass ? "border-emerald-500/20 text-[var(--text-main)]" : "border-amber-500/20 text-[var(--text-main)]"
              }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold text-[var(--text-main)]">{sup.supplier_name}</span>
                <span
                  className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium ${sup.profile === "FULL"
                    ? "border border-emerald-500/30 bg-emerald-500/10 text-emerald-500 dark:text-emerald-200"
                    : "border border-amber-500/30 bg-amber-500/10 text-amber-500 dark:text-amber-200"
                    }`}
                >
                  {sup.profile}
                </span>
                {sum && (
                  <span
                    className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium ${isPass
                      ? "border border-emerald-500/30 bg-emerald-500/10 text-emerald-500 dark:text-emerald-200"
                      : "border border-amber-500/30 bg-amber-500/10 text-amber-500 dark:text-amber-200"
                      }`}
                  >
                    {isPass ? <CheckCircle2 className="h-3 w-3" /> : <AlertTriangle className="h-3 w-3" />}
                    {sum.verdict}
                  </span>
                )}
              </div>
              <button
                onClick={() => setExpandedSupplier(isExpanded ? null : sup.supplier_name)}
                className="flex items-center gap-1 text-xs text-[var(--text-muted)] hover:text-[var(--text-main)] transition"
              >
                {isExpanded ? "Скрыть детали" : "Показать детали"}
                {isExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
              </button>
            </div>
            {sum && (
              <div className="mt-3 space-y-1 text-xs">
                {sum.checks?.map((c, j) => (
                  <div key={j} className="flex items-center gap-2 text-white/70">
                    <span
                      className={
                        c.status === "OK"
                          ? "text-emerald-400"
                          : c.status === "FAIL"
                            ? "text-rose-400"
                            : "text-amber-400"
                      }
                    >
                      {c.role}: {c.status}
                    </span>
                  </div>
                ))}
                {(sum.issues ?? []).map((issue, j) => (
                  <div key={j} className="text-rose-300/90">
                    {issue.category}: {issue.finding}
                  </div>
                ))}
              </div>
            )}
            {isExpanded && (
              <div className="mt-4 border-t border-[var(--border)] pt-4 space-y-4">
                {/* 1. Supplier extracted capabilities */}
                <div>
                  <div className="text-xs font-semibold text-[var(--text-main)] mb-2 flex items-center gap-1.5">
                    <ShieldCheck className="h-4 w-4 text-indigo-500 dark:text-indigo-400" /> Profiling (LLM)
                  </div>
                  <div className="overflow-x-auto rounded-xl border border-[var(--border)]">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-[var(--border)] bg-[var(--surface-hover)] text-left">
                          <th className="py-2.5 px-3 text-[var(--text-muted)]">Документ</th>
                          <th className="py-2.5 px-3 text-[var(--text-muted)]">Возможности</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(sup as any).documents_profiles?.map((dp: any, idx: number) => (
                          <tr key={idx} className="border-b border-[var(--border)]">
                            <td className="py-2.5 px-3 text-[var(--text-muted)] truncate max-w-[200px]" title={dp.document_name}>
                              {dp.document_name}
                            </td>
                            <td className="py-2.5 px-3 text-[var(--text-main)]">
                              <ul className="list-disc list-inside space-y-0.5">
                                {dp.extracted_capabilities?.map((c: string, cidx: number) => (
                                  <li key={cidx}>{c}</li>
                                ))}
                              </ul>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ── Upload Card ───────────────────────────────────────────────────────────── */

function BidcheckUploadCard({
  onFileSelect,
  disabled,
  accept = ".pdf",
}: {
  onFileSelect: (f: File) => void;
  disabled?: boolean;
  accept?: string;
}) {
  const [drag, setDrag] = useState(false);
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f && f.type === "application/pdf") onFileSelect(f);
  };
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (f) onFileSelect(f);
  };
  return (
    <label
      onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handleDrop}
      className={`block rounded-2xl border-2 border-dashed p-6 text-center cursor-pointer transition ${drag ? "border-emerald-500/50 bg-emerald-500/10" : "border-[var(--border)] hover:border-[var(--border-hover)] bg-[var(--surface-hover)]"
        } ${disabled ? "opacity-50 cursor-not-allowed pointer-events-none" : ""}`}
    >
      <Upload className="h-10 w-10 mx-auto mb-2 text-[var(--text-muted)]" />
      <div className="text-sm text-[var(--text-muted)]">Drop PDF or click to upload</div>
      <input
        type="file"
        accept={accept}
        onChange={handleChange}
        className="hidden"
        disabled={disabled}
      />
    </label>
  );
}

/* ── Page ─────────────────────────────────────────────────────────────────── */

type Tab = "parse" | "analyze" | "demo";

interface DemoCase {
  id: string;
  label: string;
  tenderId: string;
}

export default function BidCheckPage() {
  const [tab, setTab] = useState<Tab>("parse");
  const [file, setFile] = useState<File | null>(null);
  const [tsFile, setTsFile] = useState<File | null>(null);
  const [supplierFile, setSupplierFile] = useState<File | null>(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [parseResult, setParseResult] = useState<RequirementsDoc | null>(null);
  const [analyzeResult, setAnalyzeResult] = useState<BidcheckEnd2endResponse | null>(null);
  const [demoList, setDemoList] = useState<DemoCase[]>([]);
  const [demoResult, setDemoResult] = useState<BidcheckEnd2endResponse | null>(null);

  useEffect(() => {
    if (tab === "demo") {
      fetch("/demo/bidcheck/index.json")
        .then((r) => r.json())
        .then((data: { demos: DemoCase[] }) => setDemoList(data.demos || []))
        .catch(() => setDemoList([]));
    }
  }, [tab]);

  const loadDemoResult = async (id: string) => {
    setError(null);
    try {
      const res = await fetch(`/demo/bidcheck/${id}.end2end.json`);
      if (!res.ok) throw new Error("Demo not found");
      const data: BidcheckEnd2endResponse = await res.json();
      setDemoResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load demo");
    }
  };

  const runParse = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    try {
      const res = await parseBidcheckPdf(file);
      setParseResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Parse failed");
    } finally {
      setLoading(false);
    }
  };

  const runAnalyze = async () => {
    if (!tsFile || !supplierFile) return;
    setLoading(true);
    setError(null);
    setAnalyzeResult(null);
    try {
      const res = await parseAndAnalyzeCompliance(tsFile, supplierFile);
      setAnalyzeResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Анализ не выполнен");
    } finally {
      setLoading(false);
    }
  };

  const tabs: { id: Tab; label: string }[] = [
    { id: "parse", label: "Parse PDF" },
    { id: "analyze", label: "Анализ ТЗ vs поставщик" },
    { id: "demo", label: "Демо" },
  ];

  return (
    <BidCheckShell>
      <div className="space-y-4">
        <div className="flex gap-2 border-b border-[var(--border)] pb-2 overflow-x-auto">
          {tabs.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`rounded-xl px-4 py-2 text-sm font-medium transition whitespace-nowrap ${tab === t.id
                ? "border border-emerald-500/30 bg-emerald-500/10 text-emerald-500 dark:text-emerald-200"
                : "border border-[var(--border)] bg-[var(--surface)] text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)]"
                }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {error && (
          <div className="flex items-center gap-2 rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            <AlertTriangle className="h-4 w-4 flex-shrink-0" />
            {error}
          </div>
        )}

        {/* Section A — Parse PDF */}
        {tab === "parse" && (
          <div className="space-y-4">
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
              <h3 className="text-sm font-semibold text-[var(--text-main)] mb-3">Upload PDF</h3>
              <BidcheckUploadCard
                onFileSelect={(f) => {
                  setFile(f);
                  setParseResult(null);
                }}
                disabled={loading}
              />
              {file && (
                <div className="mt-2 text-xs text-[var(--text-muted)]">
                  Selected: {file.name} ({(file.size / 1024).toFixed(1)} KB)
                </div>
              )}
              <button
                onClick={runParse}
                disabled={!file || loading}
                className="mt-4 flex items-center gap-2 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm font-medium text-emerald-700 dark:text-emerald-200 hover:bg-emerald-500/20 disabled:opacity-50 disabled:cursor-not-allowed transition"
              >
                {loading ? (
                  <RefreshCw className="h-4 w-4 animate-spin" />
                ) : (
                  <FileText className="h-4 w-4" />
                )}
                Parse
              </button>
            </div>
            {parseResult && (
              <RequirementsViewer requirements={parseResult} />
            )}
          </div>
        )}

        {/* Section B — Анализ ТЗ vs поставщик */}
        {tab === "analyze" && (
          <div className="space-y-4">
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
              <h3 className="text-sm font-semibold text-[var(--text-main)] mb-2">Анализ соответствия</h3>
              <p className="text-xs text-[var(--text-muted)] mb-4">
                Загрузите ТЗ (техническую спецификацию) и документы поставщика — система проверит соответствие (PASS/FAIL по каждой роли).
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-semibold text-[var(--text-muted)] mb-2">ТЗ (приложение 7/8)</label>
                  <BidcheckUploadCard
                    onFileSelect={(f) => {
                      setTsFile(f);
                      setAnalyzeResult(null);
                    }}
                    disabled={loading}
                  />
                  {tsFile && (
                    <div className="mt-2 text-xs text-[var(--text-muted)]">Выбрано: {tsFile.name}</div>
                  )}
                </div>
                <div>
                  <label className="block text-xs font-semibold text-[var(--text-muted)] mb-2">Документ поставщика</label>
                  <BidcheckUploadCard
                    onFileSelect={(f) => {
                      setSupplierFile(f);
                      setAnalyzeResult(null);
                    }}
                    disabled={loading}
                  />
                  {supplierFile && (
                    <div className="mt-2 text-xs text-[var(--text-muted)]">Выбрано: {supplierFile.name}</div>
                  )}
                </div>
              </div>
              <button
                onClick={runAnalyze}
                disabled={!tsFile || !supplierFile || loading}
                className="mt-4 flex items-center gap-2 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm font-medium text-emerald-200 hover:bg-emerald-500/20 disabled:opacity-50 disabled:cursor-not-allowed transition"
              >
                {loading ? <RefreshCw className="h-4 w-4 animate-spin" /> : <FileText className="h-4 w-4" />}
                Запустить анализ
              </button>
            </div>
            {analyzeResult && <End2endResultViewer data={analyzeResult} />}
          </div>
        )}

        {/* Section: Demo — готовые кейсы */}
        {tab === "demo" && (
          <div className="space-y-4">
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
              <h3 className="text-sm font-semibold text-[var(--text-main)] mb-2">Готовые демо-кейсы</h3>
              <p className="text-xs text-[var(--text-muted)] mb-4">
                Результаты обработки реальных приложений 7. Выберите кейс — просмотрите готовый результат.
              </p>
              <div className="space-y-2">
                {demoList.map((d) => (
                  <div
                    key={d.id}
                    className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-3"
                  >
                    <div>
                      <div className="text-sm font-medium text-[var(--text-main)]">{d.label}</div>
                      <div className="text-xs text-[var(--text-muted)] font-mono">{d.id}</div>
                    </div>
                    <div className="flex items-center gap-2">
                      <a
                        href={`/demo/bidcheck/pdfs/${d.id}.pdf`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-xs text-[var(--text-muted)] hover:text-[var(--text-main)] hover:bg-[var(--surface-hover)] transition"
                      >
                        <FileDown className="h-3.5 w-3.5" />
                        PDF
                      </a>
                      <button
                        onClick={() => loadDemoResult(d.id)}
                        className="flex items-center gap-1.5 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-3 py-1.5 text-xs text-emerald-500 dark:text-emerald-200 hover:bg-emerald-500/20 transition"
                      >
                        Показать результат
                      </button>
                    </div>
                  </div>
                ))}
                {demoList.length === 0 && (
                  <div className="py-8 text-center text-sm text-[var(--text-muted)]">Загрузка списка демо...</div>
                )}
              </div>
            </div>
            {demoResult && <End2endResultViewer data={demoResult} />}
          </div>
        )}

      </div>
    </BidCheckShell>
  );
}
