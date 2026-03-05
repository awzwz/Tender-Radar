"use client";

import React from "react";
import {
  ShieldAlert, AlertTriangle, CheckCircle2, ArrowLeft, FileText,
  XCircle, Clock, UserX, Wrench, FileWarning, Ban,
} from "lucide-react";
import type { RiskTier, Severity, DocViolationType, TenderCategory } from "./types";
import { FLAG_LABELS, DOC_VIOLATION_LABELS, CATEGORY_LABELS, getRiskTier, getDocRiskTier } from "./types";

/* ── Risk Badge ────────────────────────────────────────────────────────────── */

const TIER_STYLE: Record<RiskTier, string> = {
  HIGH: "border-rose-500/30 bg-rose-500/15 text-rose-700 dark:text-rose-300",
  MEDIUM: "border-amber-500/30 bg-amber-500/15 text-amber-700 dark:text-amber-300",
  LOW: "border-emerald-500/30 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
};

export function RiskBadge({ score, size = "sm", label }: { score: number; size?: "sm" | "lg"; label?: string }) {
  const tier = getRiskTier(score);
  const base = size === "lg"
    ? "inline-flex items-center gap-2 rounded-xl px-4 py-2 text-sm font-bold border"
    : "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border";
  return (
    <span className={`${base} ${TIER_STYLE[tier]}`}>
      <ShieldAlert className={size === "lg" ? "h-4 w-4" : "h-3.5 w-3.5"} />
      {label ?? tier} · {score}
    </span>
  );
}

/* ── Doc Compliance Badge ──────────────────────────────────────────────────── */

export function DocBadge({ score, size = "sm" }: { score: number; size?: "sm" | "lg" }) {
  const tier = getDocRiskTier(score);
  const base = size === "lg"
    ? "inline-flex items-center gap-2 rounded-xl px-4 py-2 text-sm font-bold border"
    : "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border";
  const icon = tier === "LOW"
    ? <CheckCircle2 className={size === "lg" ? "h-4 w-4" : "h-3.5 w-3.5"} />
    : <AlertTriangle className={size === "lg" ? "h-4 w-4" : "h-3.5 w-3.5"} />;
  return (
    <span className={`${base} ${TIER_STYLE[tier]}`}>
      {icon}
      {score}%
    </span>
  );
}

/* ── Category Badge ────────────────────────────────────────────────────────── */

const CAT_STYLES: Record<TenderCategory, string> = {
  works: "border-amber-500/25 bg-amber-500/15 text-amber-700 dark:text-amber-200",
  services: "border-indigo-500/25 bg-indigo-500/15 text-indigo-700 dark:text-indigo-200",
  goods: "border-emerald-500/25 bg-emerald-500/15 text-emerald-700 dark:text-emerald-200",
};

export function CategoryBadge({ category }: { category: TenderCategory }) {
  return (
    <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${CAT_STYLES[category] ?? "border-[var(--border)] bg-[var(--surface-hover)] text-[var(--text-muted)]"}`}>
      {CATEGORY_LABELS[category] ?? category}
    </span>
  );
}

/* ── Flag Chip ──────────────────────────────────────────────────────────────── */

export function FlagChip({ code }: { code: string }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-rose-500/25 bg-rose-500/10 px-2.5 py-0.5 text-[11px] font-medium text-rose-700 dark:text-rose-200">
      <AlertTriangle className="h-3 w-3" />
      {FLAG_LABELS[code] ?? code}
    </span>
  );
}

/* ── Doc Violation Chip ────────────────────────────────────────────────────── */

const VIOLATION_ICONS: Record<string, React.ReactNode> = {
  MISSING_DOCUMENT: <FileWarning className="h-3 w-3" />,
  QUALIFICATION_MISMATCH: <UserX className="h-3 w-3" />,
  EXPIRED_LICENSE: <Clock className="h-3 w-3" />,
  INSUFFICIENT_EXPERIENCE: <Ban className="h-3 w-3" />,
  EQUIPMENT_MISMATCH: <Wrench className="h-3 w-3" />,
  FALSIFIED_DATA: <XCircle className="h-3 w-3" />,
};

const SEVERITY_CHIP: Record<Severity, string> = {
  HIGH: "border-rose-500/30 bg-rose-500/10 text-rose-700 dark:text-rose-200",
  MEDIUM: "border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-200",
  LOW: "border-yellow-500/30 bg-yellow-500/10 text-yellow-700 dark:text-yellow-200",
};

export function ViolationChip({ type, severity }: { type: DocViolationType; severity: Severity }) {
  return (
    <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${SEVERITY_CHIP[severity]}`}>
      {VIOLATION_ICONS[type] ?? <AlertTriangle className="h-3 w-3" />}
      {DOC_VIOLATION_LABELS[type] ?? type}
    </span>
  );
}

/* ── Stat Card (KPI) ────────────────────────────────────────────────────────── */

export function StatCard({
  label, value, sub, tone,
}: {
  label: string; value: string | number; sub?: string;
  tone: "danger" | "warn" | "ok" | "neutral";
}) {
  const styles: Record<string, string> = {
    danger: "border-rose-500/20 bg-rose-500/10",
    warn: "border-amber-500/20 bg-amber-500/10",
    ok: "border-emerald-500/20 bg-emerald-500/10",
    neutral: "border-[var(--border)] bg-[var(--surface-hover)]",
  };
  return (
    <div className={`rounded-2xl border p-4 shadow-sm shadow-[var(--border-hover)] ${styles[tone]}`}>
      <div className="text-xs text-[var(--text-muted)]">{label}</div>
      <div className="mt-1 text-2xl font-bold text-[var(--text-main)]">{value}</div>
      {sub && <div className="mt-1 text-[11px] text-[var(--text-muted)]">{sub}</div>}
    </div>
  );
}

/* ── Chart Card (wrapper) ──────────────────────────────────────────────────── */

export function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm shadow-[var(--border-hover)]">
      <div className="mb-3 text-sm font-semibold text-[var(--text-main)]">{title}</div>
      {children}
    </div>
  );
}

/* ── Mini bar ─────────────────────────────────────────────────────────────── */

export function MiniBar({ items }: { items: { label: string; value: number; color: string }[] }) {
  const max = Math.max(...items.map((i) => i.value), 1);
  return (
    <div className="space-y-2">
      {items.map((it) => (
        <div key={it.label}>
          <div className="flex items-center justify-between text-xs text-[var(--text-muted)] mb-1">
            <span>{it.label}</span>
            <span className="font-mono">{it.value}</span>
          </div>
          <div className="h-2 w-full rounded-full bg-[var(--surface-hover)] overflow-hidden">
            <div
              className={`h-full rounded-full ${it.color} transition-all duration-500`}
              style={{ width: `${(it.value / max) * 100}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── Donut Chart (pure CSS) ────────────────────────────────────────────────── */

export function DonutChart({
  segments, size = 160,
}: {
  segments: { label: string; value: number; color: string }[];
  size?: number;
}) {
  const total = segments.reduce((s, seg) => s + seg.value, 0) || 1;
  let cumPct = 0;
  const gradientParts = segments.map((seg) => {
    const startPct = cumPct;
    cumPct += (seg.value / total) * 100;
    return `${seg.color} ${startPct}% ${cumPct}%`;
  });
  const gradient = `conic-gradient(${gradientParts.join(", ")})`;

  return (
    <div className="flex items-center gap-4">
      <div
        className="rounded-full flex-shrink-0"
        style={{
          width: size, height: size, background: gradient,
          mask: `radial-gradient(circle ${size * 0.32}px at center, transparent 99%, black 100%)`,
          WebkitMask: `radial-gradient(circle ${size * 0.32}px at center, transparent 99%, black 100%)`,
        }}
      />
      <div className="space-y-1.5">
        {segments.map((seg) => (
          <div key={seg.label} className="flex items-center gap-2 text-xs font-medium" style={{ color: seg.color }}>
            <div className="h-2.5 w-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: seg.color }} />
            {seg.label} ({seg.value})
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── Severity Bar ──────────────────────────────────────────────────────────── */

export function SeverityBar({ value, max = 100 }: { value: number; max?: number }) {
  const pct = Math.min(100, (value / max) * 100);
  const color = pct >= 75 ? "bg-rose-500" : pct >= 55 ? "bg-amber-500" : "bg-emerald-500";
  return (
    <div className="h-2 w-full rounded-full bg-[var(--surface-hover)] overflow-hidden">
      <div className={`h-full ${color} transition-all duration-500 rounded-full`} style={{ width: `${pct}%` }} />
    </div>
  );
}

/* ── Compliance Bar (inverted — 100 is good) ─────────────────────────────── */

export function ComplianceBar({ value }: { value: number }) {
  const pct = Math.min(100, Math.max(0, value));
  const color = pct >= 70 ? "bg-emerald-500" : pct >= 40 ? "bg-amber-500" : "bg-rose-500";
  return (
    <div className="h-2 w-full rounded-full bg-[var(--surface-hover)] overflow-hidden">
      <div className={`h-full ${color} transition-all duration-500 rounded-full`} style={{ width: `${pct}%` }} />
    </div>
  );
}

/* ── Breakdown Stacked Bar ─────────────────────────────────────────────────── */

export function BreakdownBar({
  items, total,
}: {
  items: { label: string; value: number; color: string }[];
  total: number;
}) {
  const t = Math.max(total, 1);
  return (
    <div className="space-y-2">
      <div className="flex h-6 w-full overflow-hidden rounded-full bg-[var(--surface-hover)]">
        {items.map((it) => (
          <div
            key={it.label}
            className="h-full transition-all duration-500"
            style={{ width: `${(it.value / t) * 100}%`, backgroundColor: it.color }}
            title={`${it.label}: ${it.value}`}
          />
        ))}
      </div>
      <div className="flex flex-wrap gap-3">
        {items.map((it) => (
          <div key={it.label} className="flex items-center gap-1.5 text-[11px] text-[var(--text-muted)]">
            <div className="h-2 w-2 rounded-full" style={{ backgroundColor: it.color }} />
            {it.label}: {it.value}
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── Page Shell ───────────────────────────────────────────────────────────── */

export function SpecNormShell({
  children, backHref, backLabel, title, subtitle,
}: {
  children: React.ReactNode;
  backHref?: string;
  backLabel?: string;
  title?: string;
  subtitle?: string;
}) {
  return (
    <div className="min-h-screen bg-[var(--root-bg)] text-[var(--root-fg)]">
      <div className="mx-auto max-w-[1400px] px-4 py-5">
        <div className="flex items-center gap-3 mb-5">
          {backHref && (
            <a href={backHref} className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition">
              <ArrowLeft className="h-3.5 w-3.5" />
              {backLabel ?? "Назад"}
            </a>
          )}
          <a href="/" className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-xs text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text-main)] transition">
            <ArrowLeft className="h-3.5 w-3.5" /> Главная
          </a>
          <div className="flex items-center gap-2">
            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-2">
              <FileText className="h-5 w-5 text-indigo-500 dark:text-indigo-200" />
            </div>
            <div>
              <div className="text-lg font-semibold tracking-tight text-[var(--text-main)]">{title ?? "Tender Analysis"}</div>
              <div className="text-xs text-[var(--text-muted)]">{subtitle ?? "Spec vs Norm + Document Check · Demo"}</div>
            </div>
          </div>
        </div>
        {children}
      </div>
    </div>
  );
}

/* ── Empty state ───────────────────────────────────────────────────────────── */

export function EmptyState({ text }: { text: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-[var(--text-muted)]">
      <CheckCircle2 className="h-10 w-10 mb-2 opacity-50" />
      <div className="text-sm">{text}</div>
    </div>
  );
}
