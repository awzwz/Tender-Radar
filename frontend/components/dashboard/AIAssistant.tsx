"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
  Bot,
  ArrowRight,
  RefreshCw,
} from "lucide-react";
import { DashboardItem } from "@/lib/api";
import { DividerSpacer, money } from "@/components/shared/ui";

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
      text: "Привет! Я AI‑ассистент на базе OpenAI. Задай вопрос о тендере, поставщике или попроси объяснить риски. Например: «Что значит флаг SINGLE_BIDDER?» или «Объясни риски лота с высоким скором».",
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
      const token = typeof window !== "undefined" ? localStorage.getItem("token") : "";
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
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
    <aside className="rounded-3xl border border-[var(--border)] bg-[var(--surface-hover)] p-4 flex flex-col">
      <div className="flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-2">
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] p-2">
            <Bot className="h-5 w-5 text-emerald-600 dark:text-emerald-200" />
          </div>
          <div>
            <div className="text-sm font-semibold text-[var(--text-main)]">AI Assistant</div>
            <div className="text-xs text-[var(--text-muted)]">MCP · tool-based answers</div>
          </div>
        </div>
        <span className="rounded-full border border-emerald-500/25 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-700 dark:text-emerald-200">
          Safe mode
        </span>
      </div>

      <DividerSpacer />

      <div className="flex-1 overflow-auto pr-1 space-y-3 min-h-0" style={{ maxHeight: "420px" }}>
        {msgs.map((m, i) => (
          <div
            key={i}
            className={`rounded-2xl border px-3 py-2.5 text-sm leading-relaxed ${m.role === "assistant"
              ? "border-[var(--border)] bg-[var(--surface)]"
              : "border-indigo-500/20 bg-indigo-500/10"
              }`}
          >
            <div className="whitespace-pre-wrap text-[var(--text-main)]">{m.text}</div>
            {m.meta && <div className="mt-1.5 text-[11px] text-[var(--text-muted)]">{m.meta}</div>}
          </div>
        ))}
        {busy && (
          <div className="rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2.5 text-sm text-[var(--text-muted)]">
            <RefreshCw className="h-4 w-4 animate-spin inline mr-2 text-[var(--text-muted)]" />AI думает...
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="mt-3 flex items-center gap-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 flex-shrink-0">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Задай любой вопрос про тендеры..."
          className="w-full bg-transparent text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] outline-none"
          onKeyDown={(e) => { if (e.key === "Enter") ask(); }}
        />
        <button
          onClick={ask}
          disabled={busy}
          className="inline-flex items-center gap-1 rounded-xl bg-emerald-500 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50 hover:bg-emerald-400 transition flex-shrink-0"
        >
          Ask <ArrowRight className="h-3.5 w-3.5" />
        </button>
      </div>

      <p className="mt-2 text-xs text-[var(--text-muted)]">
        OpenAI · знает топ-10 лотов системы · контекст обновляется
      </p>
    </aside>
  );
}

export default AIAssistant;
