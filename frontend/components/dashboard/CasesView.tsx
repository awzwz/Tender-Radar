"use client";

import React from "react";
import {
  Briefcase,
  XCircle,
} from "lucide-react";
import { Badge, SectionTitle } from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";

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
  const { t } = useI18n();
  const statusLabel = (s: CaseStatus) => t(`cases.status.${s}`);
  return (
    <div className="space-y-4">
      <SectionTitle
        icon={<Briefcase className="h-4 w-4 text-indigo-500 dark:text-indigo-200" />}
        title={t("cases.title")}
        hint={t("cases.hint")}
      />
      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4">
        {cases.length === 0 ? (
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-6 text-sm text-[var(--text-muted)] text-center">
            {t("cases.empty")}
          </div>
        ) : (
          <div className="space-y-2">
            {cases.map((c) => (
              <div key={c.id} className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold text-[var(--text-main)]">{c.id}</div>
                      <span className="rounded-md border border-[var(--border)] bg-[var(--surface)] px-2 py-0.5 text-[11px] text-[var(--text-muted)]">{statusLabel(c.status)}</span>
                      <Badge level={c.level} score={Math.round(c.score)} />
                    </div>
                    <div className="mt-1 text-xs text-[var(--text-muted)]">{c.createdAt}</div>
                    <button
                      onClick={() => onOpenLot(c.lotId)}
                      className="mt-1 text-sm text-indigo-500 dark:text-indigo-300 hover:underline truncate max-w-full block text-left"
                    >
                      {c.lotName || t("cases.lotFallback", { n: c.lotId })}
                    </button>
                    <div className="mt-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-3 text-sm text-[var(--text-main)]">
                      {c.note}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <select
                      value={c.status}
                      onChange={(e) => onUpdate(c.id, { status: e.target.value as CaseStatus })}
                      className="rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 text-xs text-[var(--text-main)] outline-none"
                    >
                      <option value="NEW">{statusLabel("NEW")}</option>
                      <option value="IN_REVIEW">{statusLabel("IN_REVIEW")}</option>
                      <option value="CONFIRMED">{statusLabel("CONFIRMED")}</option>
                      <option value="DISMISSED">{statusLabel("DISMISSED")}</option>
                    </select>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
        <div className="mt-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2">
          <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
            <XCircle className="h-4 w-4 text-rose-500 dark:text-rose-300" />
            {t("cases.mlNote")}
          </div>
        </div>
      </div>
    </div>
  );
}

export default CasesView;
