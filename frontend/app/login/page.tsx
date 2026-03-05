"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { Sparkles, Shield, BarChart3, Search, Lock, User, AlertTriangle } from "lucide-react";

export default function LoginPage() {
    const router = useRouter();
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState("");
    const [loading, setLoading] = useState(false);

    async function handleLogin(e: React.FormEvent) {
        e.preventDefault();
        setLoading(true);
        setError("");
        try {
            const data = await api.login(username, password);
            if (data.access_token) {
                localStorage.setItem("token", data.access_token);
                localStorage.setItem("theme", "light"); // Ensure light theme on entry
                router.push("/");
            } else {
                const detail = Array.isArray(data.detail) ? data.detail[0]?.msg : data.detail;
                setError(detail || "Неверные данные");
            }
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : "Ошибка входа";
            const isNetwork = /failed|load failed|network|fetch/i.test(msg);
            setError(isNetwork ? "Нет связи с сервером. Проверьте, что backend запущен на http://localhost:8000" : msg);
        } finally {
            setLoading(false);
        }
    }

    return (
        <div className="min-h-screen flex">
            {/* ── Left branding panel ── */}
            <div className="hidden lg:flex flex-1 flex-col items-center justify-center bg-[#F1F5F9] relative overflow-hidden px-12">
                {/* Subtle background circles */}
                <div className="absolute top-[-80px] left-[-80px] w-[400px] h-[400px] rounded-full bg-indigo-100/60" />
                <div className="absolute bottom-[-60px] right-[-60px] w-[320px] h-[320px] rounded-full bg-violet-100/50" />

                <div className="relative z-10 max-w-md text-center">
                    {/* Logo */}
                    <div className="inline-flex items-center justify-center w-20 h-20 rounded-3xl bg-gradient-to-br from-indigo-600 to-violet-600 shadow-2xl shadow-indigo-500/30 mb-8">
                        <Sparkles className="h-9 w-9 text-white" />
                    </div>

                    <h1 className="text-3xl font-extrabold text-slate-900 tracking-tight mb-2">
                        Tender Risk Radar
                    </h1>
                    <p className="text-slate-500 text-base mb-12">
                        AI-платформа анализа рисков государственных закупок
                    </p>

                    {/* Feature cards */}
                    <div className="space-y-4 text-left">
                        {[
                            { icon: <BarChart3 className="h-5 w-5 text-indigo-500" />, label: "Dashboard рисков", sub: "Сводная аналитика HIGH / MEDIUM / LOW" },
                            { icon: <Shield className="h-5 w-5 text-violet-500" />, label: "BidCheck", sub: "Проверка соответствия поставщиков ТЗ" },
                            { icon: <Search className="h-5 w-5 text-emerald-500" />, label: "Tender Analysis", sub: "LLM-анализ технических спецификаций" },
                        ].map(({ icon, label, sub }) => (
                            <div key={label} className="flex items-center gap-4 rounded-2xl bg-white border border-slate-200/80 px-4 py-3.5 shadow-sm">
                                <div className="flex-shrink-0 h-10 w-10 rounded-xl bg-slate-50 flex items-center justify-center border border-slate-100">
                                    {icon}
                                </div>
                                <div>
                                    <div className="text-sm font-semibold text-slate-800">{label}</div>
                                    <div className="text-xs text-slate-500 mt-0.5">{sub}</div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* ── Right panel (indigo gradient + white card) ── */}
            <div className="flex flex-1 items-center justify-center bg-gradient-to-br from-indigo-600 via-indigo-500 to-violet-600 relative overflow-hidden px-6 py-12">
                {/* Decorative circles */}
                <div className="absolute -bottom-20 -right-20 w-72 h-72 rounded-full bg-white/5" />
                <div className="absolute top-10 -right-10 w-48 h-48 rounded-full bg-white/5" />
                <div className="absolute bottom-32 left-[-60px] w-60 h-60 rounded-full border border-white/10" />

                {/* White form card */}
                <div className="relative z-10 w-full max-w-sm bg-white rounded-3xl shadow-2xl shadow-indigo-900/30 p-8">
                    {/* Mobile logo */}
                    <div className="flex lg:hidden items-center gap-3 mb-8">
                        <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-indigo-600 to-violet-600 flex items-center justify-center">
                            <Sparkles className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <div className="text-sm font-bold text-slate-900">Tender Risk Radar</div>
                            <div className="text-xs text-slate-500">AI Risk Platform</div>
                        </div>
                    </div>

                    <h2 className="text-2xl font-bold text-slate-900 mb-1">Добро пожаловать!</h2>
                    <p className="text-slate-500 text-sm mb-7">Войдите в систему для продолжения</p>

                    <form onSubmit={handleLogin} className="space-y-4">
                        {/* Username */}
                        <div>
                            <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">
                                Логин
                            </label>
                            <div className="relative">
                                <User className="absolute left-3.5 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
                                <input
                                    type="text"
                                    value={username}
                                    onChange={e => setUsername(e.target.value)}
                                    className="w-full pl-10 pr-4 py-3 rounded-xl border border-slate-200 bg-slate-50 text-slate-900 text-sm placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition"
                                    placeholder="Введите логин"
                                    required
                                />
                            </div>
                        </div>

                        {/* Password */}
                        <div>
                            <label className="block text-xs font-semibold text-slate-600 mb-1.5 uppercase tracking-wide">
                                Пароль
                            </label>
                            <div className="relative">
                                <Lock className="absolute left-3.5 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
                                <input
                                    type="password"
                                    value={password}
                                    onChange={e => setPassword(e.target.value)}
                                    className="w-full pl-10 pr-4 py-3 rounded-xl border border-slate-200 bg-slate-50 text-slate-900 text-sm placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition"
                                    placeholder="••••••••"
                                    required
                                />
                            </div>
                        </div>

                        {/* Error */}
                        {error && (
                            <div className="flex items-start gap-2 rounded-xl bg-rose-50 border border-rose-200 px-3.5 py-3 text-xs text-rose-600">
                                <AlertTriangle className="h-4 w-4 flex-shrink-0 mt-0.5" />
                                <span>{error}</span>
                            </div>
                        )}

                        {/* Submit */}
                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 text-white font-semibold py-3 rounded-xl transition-all duration-200 shadow-lg shadow-indigo-500/30 hover:shadow-indigo-500/50 mt-2"
                        >
                            {loading ? (
                                <span className="flex items-center justify-center gap-2">
                                    <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
                                    </svg>
                                    Вход...
                                </span>
                            ) : "Войти"}
                        </button>
                    </form>

                    <p className="mt-6 text-center text-xs text-slate-400">
                        Tender Risk Radar · AI-анализ госзакупок
                    </p>
                </div>
            </div>
        </div>
    );
}
