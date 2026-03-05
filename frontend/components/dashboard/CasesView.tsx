"use client";

import React from "react";
import {
  Briefcase,
  XCircle,
} from "lucide-react";
import { Badge, SectionTitle } from "@/components/shared/ui";

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
        icon={<Briefcase className="h-4 w-4 text-indigo-500 dark:text-indigo-200" />}
        title="Cases"
        hint="Результаты проверки. Источник меток для ML."
      />
      <div className="rounded-3xl border border-[var(--border)] bg-[var(--surface)] p-4">
        {cases.length === 0 ? (
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-6 text-sm text-[var(--text-muted)] text-center">
            Кейсов пока нет. Создай кейс в Lot Detail.
          </div>
        ) : (
          <div className="space-y-2">
            {cases.map((c) => (
              <div key={c.id} className="rounded-2xl border border-[var(--border)] bg-[var(--surface-hover)] p-3">
                <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold text-[var(--text-main)]">{c.id}</div>
                      <span className="rounded-md border border-[var(--border)] bg-[var(--surface)] px-2 py-0.5 text-[11px] text-[var(--text-muted)]">{c.status}</span>
                      <Badge level={c.level} score={Math.round(c.score)} />
                    </div>
                    <div className="mt-1 text-xs text-[var(--text-muted)]">{c.createdAt}</div>
                    <button
                      onClick={() => onOpenLot(c.lotId)}
                      className="mt-1 text-sm text-indigo-500 dark:text-indigo-300 hover:underline truncate max-w-full block text-left"
                    >
                      {c.lotName || `Лот #${c.lotId}`}
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
        <div className="mt-4 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2">
          <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
            <XCircle className="h-4 w-4 text-rose-500 dark:text-rose-300" />
            В проде: кейсы экспортируются как ground-truth labels для ML
          </div>
        </div>
      </div>
    </div>
  );
}

export default CasesView;
