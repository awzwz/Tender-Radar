"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
  Bot,
  ArrowRight,
  RefreshCw,
} from "lucide-react";
import { DashboardItem } from "@/lib/api";
import { DividerSpacer, money } from "@/components/shared/ui";
import { useI18n } from "@/components/providers/LanguageProvider";

type AssistantMsg = { role: "user" | "assistant"; text: string; meta?: string };

const CHAT_STORAGE_KEY = "ai_chat_msgs";

function AIAssistant({
  items, selectedLotId,
}: {
  items: DashboardItem[]; selectedLotId: number | null;
}) {
  const { t, lang } = useI18n();
  const [input, setInput] = useState("");
  const [msgs, setMsgs] = useState<AssistantMsg[]>([]);
  const [busy, setBusy] = useState(false);
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const hydrated = React.useRef(false);

  // Restore any saved conversation once on mount, so it survives navigating
  // away to another tab and back (and full page reloads).
  useEffect(() => {
    if (hydrated.current) return;
    hydrated.current = true;
    try {
      const raw = localStorage.getItem(CHAT_STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as AssistantMsg[];
        if (Array.isArray(parsed) && parsed.some((m) => m.role === "user")) {
          setMsgs(parsed);
        }
      }
    } catch {
      /* ignore */
    }
  }, []);

  // Persist the conversation whenever it changes (only once a real exchange
  // exists), so it can be restored on remount.
  useEffect(() => {
    if (msgs.some((m) => m.role === "user")) {
      try {
        localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(msgs));
      } catch {
        /* ignore */
      }
    }
  }, [msgs]);

  // Seed/sync the initial assistant greeting in the active language while the
  // conversation has not started yet (no user message sent). This keeps the
  // greeting correct after the saved language is applied on mount.
  useEffect(() => {
    setMsgs((m) => {
      if (m.some((x) => x.role === "user")) return m;
      const greeting: AssistantMsg = { role: "assistant", text: t("ai.greeting") };
      if (m.length === 1 && m[0].role === "assistant") {
        if (m[0].text === greeting.text) return m;
        return [greeting];
      }
      if (m.length === 0) return [greeting];
      return m;
    });
  }, [t, lang]);

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
          messages: newMsgs,
          lotsContext,
        }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: t("ai.err.network") }));
        throw new Error(err.error || t("ai.err.api"));
      }

      const data = await res.json();
      setMsgs((m) => [...m, { role: "assistant", text: data.text }]);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : t("ai.err.unknown");
      setMsgs((m) => [
        ...m,
        { role: "assistant", text: `${t("ai.err.prefix")}${message}` },
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
            <div className="text-sm font-semibold text-[var(--text-main)]">{t("ai.title")}</div>
            <div className="text-xs text-[var(--text-muted)]">{t("ai.subtitle")}</div>
          </div>
        </div>
        <span className="rounded-full border border-emerald-500/25 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-700 dark:text-emerald-200">
          {t("ai.safeMode")}
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
            <RefreshCw className="h-4 w-4 animate-spin inline mr-2 text-[var(--text-muted)]" />{t("ai.thinking")}
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="mt-3 flex items-center gap-2 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 flex-shrink-0">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={t("ai.input.ph")}
          className="w-full bg-transparent text-sm text-[var(--text-main)] placeholder:text-[var(--text-muted)] outline-none"
          onKeyDown={(e) => { if (e.key === "Enter") ask(); }}
        />
        <button
          onClick={ask}
          disabled={busy}
          className="inline-flex items-center gap-1 rounded-xl bg-emerald-500 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50 hover:bg-emerald-400 transition flex-shrink-0"
        >
          {t("ai.ask")} <ArrowRight className="h-3.5 w-3.5" />
        </button>
      </div>

      <p className="mt-2 text-xs text-[var(--text-muted)]">
        {t("ai.footer")}
      </p>
    </aside>
  );
}

export default AIAssistant;
