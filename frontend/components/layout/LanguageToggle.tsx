"use client";

import { useI18n } from "@/components/providers/LanguageProvider";
import { LANGS } from "@/lib/i18n/dictionaries";

export function LanguageToggle() {
    const { lang, setLang } = useI18n();
    return (
        <div className="flex items-center rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-0.5 h-9">
            {LANGS.map((l) => (
                <button
                    key={l.code}
                    onClick={() => setLang(l.code)}
                    title={l.native}
                    aria-label={l.native}
                    className={`px-2 h-8 rounded-lg text-xs font-semibold transition-colors ${
                        lang === l.code
                            ? "bg-indigo-500 text-white shadow-sm shadow-indigo-500/30"
                            : "text-[var(--text-muted)] hover:text-[var(--text-main)]"
                    }`}
                >
                    {l.label}
                </button>
            ))}
        </div>
    );
}
