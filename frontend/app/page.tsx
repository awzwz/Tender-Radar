"use client";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { api, DashboardItem } from "@/lib/api";

const LEVEL_COLORS: Record<string, string> = {
  HIGH: "bg-red-100 text-red-700 border border-red-300",
  MEDIUM: "bg-yellow-100 text-yellow-700 border border-yellow-300",
  LOW: "bg-green-100 text-green-700 border border-green-300",
  UNKNOWN: "bg-gray-100 text-gray-500 border border-gray-300",
};

function formatMoney(n: number) {
  return new Intl.NumberFormat("ru-KZ", { style: "currency", currency: "KZT", maximumFractionDigits: 0 }).format(n);
}

export default function DashboardPage() {
  const router = useRouter();
  const [items, setItems] = useState<DashboardItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [level, setLevel] = useState("");
  const [sortBy, setSortBy] = useState("score");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) { router.push("/login"); return; }
    load();
  }, [page, level, sortBy]);

  async function load() {
    setLoading(true);
    setError("");
    try {
      const params: Record<string, string | number> = { page, limit: 50, sort_by: sortBy };
      if (level) params.level = level;
      const data = await api.dashboard(params);
      setItems(data.items);
      setTotal(data.total);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Ошибка загрузки");
    } finally {
      setLoading(false);
    }
  }

  function logout() {
    localStorage.removeItem("token");
    router.push("/login");
  }

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      {/* Header */}
      <header className="bg-gray-900 border-b border-gray-800 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-red-600 rounded-lg flex items-center justify-center text-white font-bold text-sm">⚠</div>
          <h1 className="text-xl font-bold text-white">Tender Risk Radar</h1>
          <span className="text-xs text-gray-500 ml-2">Goszakup.kz</span>
        </div>
        <button onClick={logout} className="text-sm text-gray-400 hover:text-white transition">Выйти</button>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8">
        {/* Stats bar */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          {[
            { label: "Всего лотов", value: total.toLocaleString("ru") },
            { label: "Показано", value: items.length },
            { label: "Высокий риск", value: items.filter(i => i.risk_level === "HIGH").length },
          ].map(s => (
            <div key={s.label} className="bg-gray-900 rounded-xl p-4 border border-gray-800">
              <div className="text-2xl font-bold text-white">{s.value}</div>
              <div className="text-sm text-gray-400 mt-1">{s.label}</div>
            </div>
          ))}
        </div>

        {/* Filters */}
        <div className="flex gap-3 mb-6">
          <select
            value={level}
            onChange={e => { setLevel(e.target.value); setPage(1); }}
            className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 focus:outline-none focus:border-red-500"
          >
            <option value="">Все уровни</option>
            <option value="HIGH">🔴 Высокий</option>
            <option value="MEDIUM">🟡 Средний</option>
            <option value="LOW">🟢 Низкий</option>
          </select>
          <select
            value={sortBy}
            onChange={e => { setSortBy(e.target.value); setPage(1); }}
            className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 focus:outline-none focus:border-red-500"
          >
            <option value="score">По риску</option>
            <option value="date">По дате</option>
            <option value="amount">По сумме</option>
          </select>
          <button
            onClick={load}
            className="ml-auto bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition"
          >
            Обновить
          </button>
        </div>

        {error && <div className="bg-red-900/30 border border-red-700 text-red-300 rounded-lg p-4 mb-4">{error}</div>}

        {loading ? (
          <div className="flex items-center justify-center py-20 text-gray-500">Загрузка...</div>
        ) : (
          <div className="space-y-3">
            {items.map(item => (
              <div
                key={item.lot_id}
                onClick={() => router.push(`/lots/${item.lot_id}`)}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5 cursor-pointer hover:border-gray-600 hover:bg-gray-800/50 transition group"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${LEVEL_COLORS[item.risk_level]}`}>
                        {item.risk_level}
                      </span>
                      {item.tender_number && (
                        <span className="text-xs text-gray-500">№{item.tender_number}</span>
                      )}
                    </div>
                    <h3 className="text-sm font-medium text-gray-100 truncate group-hover:text-white">
                      {item.lot_name || "Без названия"}
                    </h3>
                    <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                      <span>{item.customer_name || item.customer_bin}</span>
                      {item.publish_date && <span>{item.publish_date.slice(0, 10)}</span>}
                    </div>
                    {item.top_reasons.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {item.top_reasons.slice(0, 3).map(r => (
                          <span key={r.code} className="text-xs bg-gray-800 text-gray-400 px-2 py-0.5 rounded">
                            {r.code}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="text-right flex-shrink-0">
                    <div className="text-2xl font-bold text-white">{item.risk_score?.toFixed(0) ?? "—"}</div>
                    <div className="text-xs text-gray-500">/ 100</div>
                    <div className="text-sm font-medium text-gray-300 mt-2">{formatMoney(item.amount)}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Pagination */}
        {total > 50 && (
          <div className="flex items-center justify-center gap-4 mt-8">
            <button
              disabled={page === 1}
              onClick={() => setPage(p => p - 1)}
              className="px-4 py-2 bg-gray-800 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-700 transition"
            >
              ← Назад
            </button>
            <span className="text-sm text-gray-400">Страница {page} из {Math.ceil(total / 50)}</span>
            <button
              disabled={page >= Math.ceil(total / 50)}
              onClick={() => setPage(p => p + 1)}
              className="px-4 py-2 bg-gray-800 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-700 transition"
            >
              Вперёд →
            </button>
          </div>
        )}
      </main>
    </div>
  );
}
