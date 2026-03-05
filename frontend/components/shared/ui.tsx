"use client";

import React from "react";
import { ShieldAlert } from "lucide-react";

// ── Helpers ──────────────────────────────────────────────────────────────────
export const money = (n: number) =>
    new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(n) + " ₸";

export const clamp = (n: number, min = 0, max = 100) =>
    Math.max(min, Math.min(max, n));

export function levelFromScore(score: number) {
    if (score >= 20) return "HIGH";
    if (score >= 7) return "MEDIUM";
    return "LOW";
}

// ── Badge ────────────────────────────────────────────────────────────────────
export function Badge({ level, score }: { level: string; score: number }) {
    const base =
        "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border";
    const styles: Record<string, string> = {
        LOW: "bg-emerald-500/10 border-emerald-500/20 text-emerald-700 dark:text-emerald-300",
        MEDIUM: "bg-amber-500/10 border-amber-500/20 text-amber-700 dark:text-amber-300",
        HIGH: "bg-rose-500/10 border-rose-500/20 text-rose-700 dark:text-rose-300",
        UNKNOWN: "bg-slate-500/10 border-slate-500/20 text-slate-700 dark:text-slate-300",
    };
    return (
        <span className={`${base} ${styles[level] ?? styles.UNKNOWN}`}>
            <ShieldAlert className="h-3.5 w-3.5" />
            {level} · {score}
        </span>
    );
}

// ── Chip ─────────────────────────────────────────────────────────────────────
export function Chip({ children }: { children: React.ReactNode }) {
    return (
        <span className="inline-flex items-center rounded-full border border-[var(--border)] bg-[var(--surface-hover)] px-2.5 py-1 text-xs text-[var(--text-main)]">
            {children}
        </span>
    );
}

// ── Divider ──────────────────────────────────────────────────────────────────
export function DividerSpacer() {
    return <div className="my-3 h-px w-full bg-[var(--border)]" />;
}

// ── ProgressBar ──────────────────────────────────────────────────────────────
export function ProgressBar({ value }: { value: number }) {
    const v = clamp(value);
    const level = levelFromScore(v);
    const color =
        level === "HIGH"
            ? "bg-rose-500"
            : level === "MEDIUM"
                ? "bg-amber-500"
                : "bg-emerald-500";
    return (
        <div className="h-1.5 w-full rounded-full bg-[var(--border)] overflow-hidden">
            <div className={`h-full ${color} transition-all`} style={{ width: `${v}%` }} />
        </div>
    );
}

// ── SectionTitle ─────────────────────────────────────────────────────────────
export function SectionTitle({
    icon,
    title,
    hint,
}: {
    icon: React.ReactNode;
    title: string;
    hint?: string;
}) {
    return (
        <div className="flex items-start gap-3">
            <div className="rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2">{icon}</div>
            <div>
                <div className="text-sm font-semibold text-[var(--text-main)]">{title}</div>
                {hint && <div className="text-xs text-[var(--text-muted)]">{hint}</div>}
            </div>
        </div>
    );
}

// ── StatCard (enhanced with trend dot + glow) ────────────────────────────────
export function StatCard({
    label,
    value,
    tone,
    subtitle,
}: {
    label: string;
    value: number | string;
    tone: "danger" | "warn" | "ok" | "neutral";
    subtitle?: string;
}) {
    const border = {
        danger: "border-rose-500/20 bg-rose-500/10 hover:bg-rose-500/20",
        warn: "border-amber-500/20 bg-amber-500/10 hover:bg-amber-500/20",
        ok: "border-emerald-500/20 bg-emerald-500/10 hover:bg-emerald-500/20",
        neutral: "border-[var(--border)] bg-[var(--surface-hover)] hover:border-[var(--border-hover)]",
    }[tone];

    const dot = {
        danger: "bg-rose-500",
        warn: "bg-amber-500",
        ok: "bg-emerald-500",
        neutral: "bg-slate-400",
    }[tone];

    return (
        <div className={`group relative rounded-2xl border p-4 transition-all duration-300 cursor-default ${border}`}>
            <div className="absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-300 bg-gradient-to-br from-white/[0.02] to-transparent pointer-events-none" />
            <div className="relative">
                <div className="flex items-center gap-2 mb-2">
                    <div className={`h-2 w-2 rounded-full ${dot}`} />
                    <div className="text-xs text-[var(--text-muted)] uppercase tracking-wider font-medium">{label}</div>
                </div>
                <div className="text-3xl font-bold text-[var(--text-main)] tracking-tight">{value}</div>
                <div className="mt-2 h-px w-full bg-[var(--border)]" />
                <div className="mt-2 text-[11px] text-[var(--text-muted)]">{subtitle || "Updated daily"}</div>
            </div>
        </div>
    );
}
