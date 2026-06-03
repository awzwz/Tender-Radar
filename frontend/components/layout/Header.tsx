"use client";

import React from "react";
import { Bell, Download, BadgeInfo } from "lucide-react";
import { ThemeToggle } from "./ThemeToggle";
import { LanguageToggle } from "./LanguageToggle";
import { useI18n } from "@/components/providers/LanguageProvider";

interface HeaderProps {
    totalLots: number;
    onExportCSV: () => void;
}

export default function Header({ totalLots, onExportCSV }: HeaderProps) {
    const { t } = useI18n();
    return (
        <header className="sticky top-0 z-30 flex items-center justify-between gap-4 border-b border-[var(--border)] bg-[var(--header)]/80 backdrop-blur-xl px-6 py-3">
            {/* Left: info */}
            <div className="flex items-center gap-3 min-w-0">
                <div className="hidden md:flex items-center gap-2 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2">
                    <BadgeInfo className="h-4 w-4 text-[var(--text-muted)] flex-shrink-0" />
                    <span className="text-xs text-[var(--text-muted)]">
                        {totalLots > 0
                            ? t("header.lots", { n: totalLots.toLocaleString("ru-RU") })
                            : t("header.noData")}
                    </span>
                </div>
            </div>

            {/* Right: actions */}
            <div className="flex items-center gap-2 flex-shrink-0">
                <button
                    onClick={onExportCSV}
                    className="flex items-center gap-1.5 rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] px-3 py-2 text-xs text-[var(--text-muted)] hover:text-[var(--text-main)] transition-all"
                >
                    <Download className="h-3.5 w-3.5" />
                    {t("action.export")}
                </button>

                <LanguageToggle />
                <ThemeToggle />

                {/* Notification bell */}
                <button className="relative rounded-xl border border-[var(--border)] bg-[var(--surface-hover)] p-2 text-[var(--text-muted)] hover:bg-[var(--surface)] hover:text-[var(--text-main)] transition-all">
                    <Bell className="h-4 w-4" />
                    <span className="absolute -top-1 -right-1 h-4 w-4 rounded-full bg-rose-500 border-2 border-[var(--header)] flex items-center justify-center">
                        <span className="text-[8px] font-bold text-white">3</span>
                    </span>
                </button>
            </div>
        </header>
    );
}
