"use client";

import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import { dictionaries, Lang } from "@/lib/i18n/dictionaries";

type TParams = Record<string, string | number>;

interface LangContextValue {
    lang: Lang;
    setLang: (l: Lang) => void;
    t: (key: string, params?: TParams) => string;
}

const LangContext = createContext<LangContextValue | null>(null);

const VALID: Lang[] = ["en", "ru", "kk"];

export function LanguageProvider({ children }: { children: React.ReactNode }) {
    // Default "ru" on the server AND first client render → no hydration mismatch.
    // The saved language is applied in an effect after mount.
    const [lang, setLangState] = useState<Lang>("ru");
    const [mounted, setMounted] = useState(false);

    useEffect(() => {
        setMounted(true);
        try {
            const saved = localStorage.getItem("lang") as Lang | null;
            if (saved && VALID.includes(saved)) setLangState(saved);
        } catch {
            /* ignore */
        }
    }, []);

    useEffect(() => {
        if (!mounted) return;
        try {
            localStorage.setItem("lang", lang);
        } catch {
            /* ignore */
        }
        document.documentElement.lang = lang;
    }, [lang, mounted]);

    const setLang = useCallback((l: Lang) => setLangState(l), []);

    const t = useCallback(
        (key: string, params?: TParams) => {
            let s = dictionaries[lang]?.[key] ?? dictionaries.ru?.[key] ?? key;
            if (params) {
                for (const [k, v] of Object.entries(params)) {
                    s = s.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
                }
            }
            return s;
        },
        [lang],
    );

    return <LangContext.Provider value={{ lang, setLang, t }}>{children}</LangContext.Provider>;
}

export function useI18n(): LangContextValue {
    const ctx = useContext(LangContext);
    if (!ctx) throw new Error("useI18n must be used within <LanguageProvider>");
    return ctx;
}
