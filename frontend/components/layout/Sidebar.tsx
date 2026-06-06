"use client";

import React, { useState, useEffect } from "react";
import { usePathname } from "next/navigation";
import {
    LayoutDashboard,
    ListFilter,
    FileText,
    Briefcase,
    Bot,
    Shield,
    FileCheck2,
    ChevronLeft,
    ChevronRight,
    LogOut,
    Menu,
    X,
    Building2,
    ClipboardList,
    TrendingUp,
} from "lucide-react";
import Image from "next/image";
import { useI18n } from "@/components/providers/LanguageProvider";

/* ── Types ──────────────────────────────────────────────────────────── */
export type NavId =
    | "dashboard"
    | "risk-list"
    | "tender-list"
    | "detail"
    | "cases"
    | "price-radar"
    | "specnorm"
    | "bidcheck"
    | "company-search"
    | "ai-assistant";

interface SidebarProps {
    activeNav: NavId;
    onNavChange: (id: NavId) => void;
    onLogout: () => void;
    collapsed?: boolean;
    onCollapsedChange?: (collapsed: boolean) => void;
}

interface NavItem {
    id: NavId;
    labelKey: string;
    icon: React.ReactNode;
    hintKey: string;
    badge?: string;
    group: "main" | "tools" | "ai";
}

const NAV_ITEMS: NavItem[] = [
    { id: "dashboard", labelKey: "nav.dashboard", icon: <LayoutDashboard className="h-5 w-5" />, hintKey: "nav.dashboard.hint", group: "main" },
    { id: "risk-list", labelKey: "nav.lots", icon: <ListFilter className="h-5 w-5" />, hintKey: "nav.lots.hint", group: "main" },
    { id: "tender-list", labelKey: "nav.tenders", icon: <ClipboardList className="h-5 w-5" />, hintKey: "nav.tenders.hint", group: "main" },
    { id: "detail", labelKey: "nav.detail", icon: <FileText className="h-5 w-5" />, hintKey: "nav.detail.hint", group: "main" },
    { id: "cases", labelKey: "nav.cases", icon: <Briefcase className="h-5 w-5" />, hintKey: "nav.cases.hint", group: "main" },
    { id: "price-radar", labelKey: "nav.price", icon: <TrendingUp className="h-5 w-5" />, hintKey: "nav.price.hint", group: "tools", badge: "New" },
    { id: "specnorm", labelKey: "nav.specnorm", icon: <FileCheck2 className="h-5 w-5" />, hintKey: "nav.specnorm.hint", group: "tools", badge: "AI" },
    { id: "bidcheck", labelKey: "nav.bidcheck", icon: <Shield className="h-5 w-5" />, hintKey: "nav.bidcheck.hint", group: "tools" },
    { id: "company-search", labelKey: "nav.company", icon: <Building2 className="h-5 w-5" />, hintKey: "nav.company.hint", group: "tools", badge: "Live" },
    { id: "ai-assistant", labelKey: "nav.ai", icon: <Bot className="h-5 w-5" />, hintKey: "nav.ai.hint", group: "ai", badge: "New" },
];

/* ── Group label ────────────────────────────────────────────────────── */
function GroupLabel({ label, collapsed }: { label: string; collapsed: boolean }) {
    if (collapsed) return <div className="my-3 h-px mx-2 bg-[var(--border)]" />;
    return (
        <div className="mt-5 mb-2 px-3 text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)]">
            {label}
        </div>
    );
}

/* ── Single nav item ────────────────────────────────────────────────── */
function SidebarNavItem({
    item, active, collapsed, onClick,
}: {
    item: NavItem; active: boolean; collapsed: boolean; onClick: () => void;
}) {
    const { t } = useI18n();
    const label = t(item.labelKey);
    return (
        <button
            onClick={onClick}
            title={collapsed ? label : undefined}
            className={`
        group relative w-full flex items-center gap-3 rounded-xl px-3 py-2.5
        transition-all duration-200
        ${collapsed ? "justify-center" : ""}
        ${active
                    ? "bg-indigo-500/15 text-indigo-700 dark:text-white"
                    : "text-[var(--text-muted)] hover:text-[var(--text-main)] hover:bg-[var(--surface-hover)]"
                }
      `}
        >
            {active && (
                <div className="absolute left-0 top-1/2 -translate-y-1/2 h-6 w-[3px] rounded-r-full bg-indigo-500 shadow-lg shadow-indigo-500/30" />
            )}

            <div className={`flex-shrink-0 transition-colors ${active ? "text-indigo-500 dark:text-indigo-400" : "text-[var(--text-muted)] group-hover:text-[var(--text-main)]"}`}>
                {item.icon}
            </div>

            {!collapsed && (
                <div className="flex-1 min-w-0 text-left">
                    <div className="text-[13px] font-medium leading-tight text-[var(--text-main)]">{label}</div>
                    <div className="text-[10px] text-[var(--text-muted)] mt-0.5 truncate">{t(item.hintKey)}</div>
                </div>
            )}

            {!collapsed && item.badge && (
                <span className="flex-shrink-0 rounded-md bg-indigo-600 px-1.5 py-0.5 text-[9px] font-semibold text-white uppercase tracking-wider">
                    {item.badge}
                </span>
            )}
        </button>
    );
}

/* ── Sidebar ────────────────────────────────────────────────────────── */
export default function Sidebar({ activeNav, onNavChange, onLogout, collapsed: collapsedProp, onCollapsedChange }: SidebarProps) {
    const { t } = useI18n();
    const [collapsedLocal, setCollapsedLocal] = useState(false);
    const collapsed = collapsedProp !== undefined ? collapsedProp : collapsedLocal;
    const setCollapsed = (val: boolean) => {
        setCollapsedLocal(val);
        onCollapsedChange?.(val);
    };
    const [mobileOpen, setMobileOpen] = useState(false);
    const pathname = usePathname();

    useEffect(() => { setMobileOpen(false); }, [pathname]);

    const renderGroup = (group: NavItem["group"]) =>
        NAV_ITEMS.filter((i) => i.group === group).map((item) => (
            <SidebarNavItem
                key={item.id}
                item={item}
                active={activeNav === item.id}
                collapsed={collapsed}
                onClick={() => { onNavChange(item.id); setMobileOpen(false); }}
            />
        ));

    const content = (
        <div className="flex h-full flex-col">
            {/* ── Brand ─────────────────────────────── */}
            <div className={`flex items-center gap-3 px-4 pt-5 pb-4 ${collapsed ? "justify-center" : ""}`}>
                <div className="relative flex-shrink-0">
                    <Image src="/logo.png" alt="Logo" width={40} height={40} className="rounded-2xl shadow-lg shadow-indigo-500/20" />
                    <div className="absolute -bottom-0.5 -right-0.5 h-3 w-3 rounded-full bg-emerald-500 border-2 border-[var(--sidebar)]" />
                </div>
                {!collapsed && (
                    <div className="overflow-hidden">
                        <div className="text-sm font-bold text-[var(--text-main)] tracking-tight">Tender Radar</div>
                        <div className="text-[10px] text-[var(--text-muted)] font-medium">{t("brand.subtitle")}</div>
                    </div>
                )}
            </div>

            {/* ── Navigation ────────────────────────── */}
            <nav className="flex-1 overflow-y-auto px-2 space-y-0.5">
                <GroupLabel label={t("nav.group.menu")} collapsed={collapsed} />
                {renderGroup("main")}
                <GroupLabel label={t("nav.group.tools")} collapsed={collapsed} />
                {renderGroup("tools")}
                <GroupLabel label={t("nav.group.ai")} collapsed={collapsed} />
                {renderGroup("ai")}
            </nav>

            {/* ── Bottom ────────────────────────────── */}
            <div className="border-t border-[var(--border)] px-2 py-3 space-y-1">
                <button
                    onClick={() => setCollapsed(!collapsed)}
                    className="hidden lg:flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-[var(--text-muted)] hover:text-[var(--text-main)] hover:bg-[var(--surface-hover)] transition-all"
                >
                    {collapsed
                        ? <ChevronRight className="h-4 w-4 mx-auto" />
                        : <><ChevronLeft className="h-4 w-4" /><span className="text-xs">{t("action.collapse")}</span></>
                    }
                </button>

                <button
                    onClick={onLogout}
                    className={`w-full flex items-center gap-3 rounded-xl px-3 py-2.5 text-[var(--text-muted)] hover:text-rose-500 hover:bg-rose-500/10 transition-all ${collapsed ? "justify-center" : ""}`}
                >
                    <LogOut className="h-4 w-4 flex-shrink-0" />
                    {!collapsed && <span className="text-xs">{t("action.logout")}</span>}
                </button>
            </div>
        </div>
    );

    return (
        <>
            {/* Mobile toggle button */}
            <button
                onClick={() => setMobileOpen(true)}
                className="lg:hidden fixed top-4 left-4 z-50 rounded-xl border border-white/10 bg-slate-900/90 backdrop-blur-sm p-2.5 text-white/70 hover:text-white shadow-lg"
            >
                <Menu className="h-5 w-5" />
            </button>

            {/* Mobile drawer overlay */}
            {mobileOpen && (
                <div className="lg:hidden fixed inset-0 z-50 bg-black/60 backdrop-blur-sm" onClick={() => setMobileOpen(false)}>
                    <div className="h-full w-72 bg-[var(--sidebar)] border-r border-[var(--border)] shadow-2xl" onClick={(e) => e.stopPropagation()}>
                        <button
                            onClick={() => setMobileOpen(false)}
                            className="absolute top-4 right-4 rounded-lg p-1.5 text-[var(--text-muted)] hover:text-[var(--text-main)] hover:bg-[var(--surface-hover)]"
                        >
                            <X className="h-5 w-5" />
                        </button>
                        {content}
                    </div>
                </div>
            )}

            {/* Desktop sidebar */}
            <aside className={`
        hidden lg:flex flex-col fixed left-0 top-0 h-screen
        bg-[var(--sidebar)] border-r border-[var(--border)] z-40
        transition-all duration-300
        ${collapsed ? "w-[72px]" : "w-[260px]"}
      `}>
                {content}
            </aside>
        </>
    );
}
