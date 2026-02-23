"use client";
import { useEffect, useState } from "react";
import { useRouter, useParams } from "next/navigation";
import { api, SupplierProfile } from "@/lib/api";

function formatMoney(n: number) {
    return new Intl.NumberFormat("ru-KZ", { style: "currency", currency: "KZT", maximumFractionDigits: 0 }).format(n);
}

export default function SupplierPage() {
    const router = useRouter();
    const params = useParams();
    const [data, setData] = useState<SupplierProfile | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const token = localStorage.getItem("token");
        if (!token) { router.push("/login"); return; }
        api.supplier(String(params.biin)).then(setData).finally(() => setLoading(false));
    }, [params.biin]);

    if (loading) return <div className="min-h-screen bg-gray-950 flex items-center justify-center text-gray-400">Загрузка...</div>;
    if (!data) return <div className="min-h-screen bg-gray-950 flex items-center justify-center text-red-400">Поставщик не найден</div>;

    return (
        <div className="min-h-screen bg-gray-950 text-gray-100">
            <header className="bg-gray-900 border-b border-gray-800 px-6 py-4 flex items-center gap-4">
                <button onClick={() => router.back()} className="text-gray-400 hover:text-white transition">← Назад</button>
                <div className="w-px h-5 bg-gray-700" />
                <h1 className="text-lg font-semibold text-white">{data.company.name_ru || data.company.biin}</h1>
                {data.rnu.is_active && (
                    <span className="ml-auto bg-red-900/50 text-red-400 border border-red-700 text-xs px-3 py-1 rounded-full font-semibold">⚠ РНП</span>
                )}
            </header>

            <main className="max-w-4xl mx-auto px-6 py-8 space-y-6">
                {/* RNU Warning */}
                {data.rnu.is_active && (
                    <div className="bg-red-900/30 border border-red-700 rounded-2xl p-5">
                        <div className="font-bold text-red-400 mb-1">🚫 Поставщик в реестре недобросовестных поставщиков</div>
                        <div className="text-sm text-gray-300">{data.rnu.reason}</div>
                        {data.rnu.start_date && <div className="text-xs text-gray-500 mt-1">С {data.rnu.start_date.slice(0, 10)}</div>}
                    </div>
                )}

                {/* Company Info */}
                <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                    <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Компания</h2>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                        <div><span className="text-gray-500">БИН/ИИН:</span> <span className="text-gray-200 font-mono">{data.company.biin}</span></div>
                        <div><span className="text-gray-500">Дата регистрации:</span> <span className="text-gray-200">{data.company.regdate?.slice(0, 10)}</span></div>
                        <div><span className="text-gray-500">Email:</span> <span className="text-gray-200">{data.company.email || "—"}</span></div>
                        <div><span className="text-gray-500">Телефон:</span> <span className="text-gray-200">{data.company.phone || "—"}</span></div>
                        {data.company.mark_small_employer === 1 && <div className="col-span-2"><span className="bg-blue-900/40 text-blue-400 border border-blue-700 text-xs px-2 py-1 rounded">МСБ</span></div>}
                    </div>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-3 gap-4">
                    {[
                        { label: "Контрактов", value: data.stats.total_contracts },
                        { label: "Заказчиков", value: data.stats.unique_customers },
                        { label: "Сумма контрактов", value: formatMoney(data.stats.total_sum) },
                    ].map(s => (
                        <div key={s.label} className="bg-gray-900 border border-gray-800 rounded-xl p-4">
                            <div className="text-xl font-bold text-white">{s.value}</div>
                            <div className="text-xs text-gray-400 mt-1">{s.label}</div>
                        </div>
                    ))}
                </div>

                {/* Top Customers */}
                {data.top_customers.length > 0 && (
                    <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                        <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Топ заказчики</h2>
                        <div className="space-y-2">
                            {data.top_customers.map(c => (
                                <div key={c.customer_bin} className="flex items-center justify-between text-sm py-2 border-b border-gray-800 last:border-0">
                                    <button onClick={() => router.push(`/customers/${c.customer_bin}`)} className="text-blue-400 hover:underline font-mono">{c.customer_bin}</button>
                                    <div className="text-right">
                                        <div className="text-gray-200">{c.contract_count} контр.</div>
                                        <div className="text-gray-500 text-xs">{formatMoney(c.total_sum)}</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </main>
        </div>
    );
}
