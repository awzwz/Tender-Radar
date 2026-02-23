"use client";
import { useEffect, useState } from "react";
import { useRouter, useParams } from "next/navigation";
import { api, LotDetail, IndicatorDetails } from "@/lib/api";

const LEVEL_COLORS: Record<string, string> = {
    HIGH: "text-red-400",
    MEDIUM: "text-yellow-400",
    LOW: "text-green-400",
    UNKNOWN: "text-gray-400",
};

const LEVEL_BG: Record<string, string> = {
    HIGH: "bg-red-900/30 border-red-700",
    MEDIUM: "bg-yellow-900/30 border-yellow-700",
    LOW: "bg-green-900/30 border-green-700",
    UNKNOWN: "bg-gray-900/30 border-gray-700",
};

function formatMoney(n: number) {
    return new Intl.NumberFormat("ru-KZ", { style: "currency", currency: "KZT", maximumFractionDigits: 0 }).format(n);
}

const EVIDENCE_LABELS: Record<string, string> = {
    // Dates & Timings
    start_date: "Дата начала",
    end_date: "Дата окончания",
    sign_date: "Дата подписания",
    contract_sign_date: "Дата подписания договора",
    addendum_sign_date: "Дата допсоглашения",
    plan_exec_date: "Плановая дата исполнения",
    regdate: "Дата регистрации",
    last_update: "Последнее обновление",
    deadline_days: "Дней до дедлайна",
    days_to_addendum: "Дней до допсоглашения",
    execution_days: "Срок исполнения (дней)",
    company_age_days: "Возраст компании (дней)",
    hours_before_deadline: "Часов до дедлайна",

    // Counts & Stats
    bid_count: "Кол-во заявок",
    lot_count: "Кол-во лотов",
    supplier_contracts: "Контрактов поставщика",
    total_contracts: "Всего контрактов",
    top_customer_contracts: "Контрактов с топ-заказчиком",
    payment_count: "Кол-во платежей",
    act_count: "Кол-во актов",
    total_participated: "Всего участий",
    total_won: "Всего побед",
    avg_bids_per_tender: "Среднее кол-во заявок",
    unique_winners: "Уникальных победителей",
    rotation_count: "Кол-во ротаций",
    bidder_count: "Кол-во участников",

    // Financials
    total_sum: "Общая сумма",
    avg_lot_amount: "Средняя сумма лота",
    original_sum: "Исходная сумма",
    current_sum: "Текущая сумма",
    addendum_sum: "Сумма допсоглашения",
    lot_amount: "Сумма лота",
    contract_sum: "Сумма контракта",
    total_paid: "Всего оплачено",

    // Identifiers & Entities
    customer_bin: "БИН заказчика",
    supplier_biin: "БИН/ИИН поставщика",
    top_customer_bin: "БИН топ-заказчика",
    root_contract_id: "ID корневого договора",
    contract_id: "ID договора",
    lot_id: "ID лота",
    rnu_id: "ID в РНУ",
    system_id: "ID в системе",
    company_name: "Название компании",

    // Ratios & Thresholds
    win_rate_pct: "Процент побед (%)",
    concentration_pct: "Концентрация (%)",
    increase_pct: "Процент увеличения (%)",
    threshold: "Порог",
    threshold_pct: "Порог (%)",
    threshold_sum: "Пороговая сумма",

    // Misc
    anomaly: "Аномалия",
    reason: "Причина",
    dumping_flag: "Флаг демпинга",
    winner_sequence: "Последовательность победителей",
    common_phones: "Общие телефоны",
    common_emails: "Общие email",
    method: "Метод закупки",
    region: "Регион",
};

function BinLink({ bin, label, isCustomer = false }: { bin: string; label?: string; isCustomer?: boolean }) {
    const router = useRouter();
    const href = isCustomer ? `/customers/${bin}` : `/suppliers/${bin}`;
    return (
        <button onClick={() => router.push(href)} className="text-blue-400 hover:underline font-mono text-xs" title="Открыть карточку компании">
            {label ?? bin}
        </button>
    );
}

export default function LotDetailPage() {
    const router = useRouter();
    const params = useParams();
    const [data, setData] = useState<LotDetail | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [noteText, setNoteText] = useState("");
    const [noteLabel, setNoteLabel] = useState("NEEDS_REVIEW");
    const [noteSaving, setNoteSaving] = useState(false);
    const [indicatorDetail, setIndicatorDetail] = useState<IndicatorDetails | null>(null);
    const [indicatorDetailCode, setIndicatorDetailCode] = useState<string | null>(null);
    const [indicatorDetailLoading, setIndicatorDetailLoading] = useState(false);

    async function openIndicatorDetail(code: string) {
        if (!params.id) return;
        const lotId = Number(params.id);
        setIndicatorDetailCode(code);
        setIndicatorDetailLoading(true);
        setIndicatorDetail(null);
        try {
            const d = await api.lotIndicatorDetails(lotId, code);
            setIndicatorDetail(d);
        } catch {
            setIndicatorDetail(null);
        } finally {
            setIndicatorDetailLoading(false);
        }
    }

    useEffect(() => {
        const token = localStorage.getItem("token");
        if (!token) { router.push("/login"); return; }
        api.lot(Number(params.id)).then(setData).catch(e => setError(e.message)).finally(() => setLoading(false));
    }, [params.id]);

    async function saveNote() {
        if (!noteText.trim() || !data) return;
        setNoteSaving(true);
        try {
            await api.createNote({ entity_type: "lot", entity_id: String(data.lot.id), note_text: noteText, label: noteLabel });
            setNoteText("");
        } finally {
            setNoteSaving(false);
        }
    }

    if (loading) return <div className="min-h-screen bg-gray-950 flex items-center justify-center text-gray-400">Загрузка...</div>;
    if (error) return <div className="min-h-screen bg-gray-950 flex items-center justify-center text-red-400">{error}</div>;
    if (!data) return null;

    const { lot, tender, contract, risk, flags } = data;
    const triggeredFlags = flags.filter(f => f.triggered);

    return (
        <div className="min-h-screen bg-gray-950 text-gray-100">
            <header className="bg-gray-900 border-b border-gray-800 px-6 py-4 flex items-center gap-4">
                <button onClick={() => router.back()} className="text-gray-400 hover:text-white transition">← Назад</button>
                <div className="w-px h-5 bg-gray-700" />
                <h1 className="text-lg font-semibold text-white truncate">{lot.name_ru || "Лот"}</h1>
            </header>

            <main className="max-w-5xl mx-auto px-6 py-8 space-y-6">
                {/* Risk Score Card */}
                <div className={`rounded-2xl border p-6 ${LEVEL_BG[risk.level]}`}>
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm text-gray-400 mb-1">Уровень риска</div>
                            <div className={`text-4xl font-black ${LEVEL_COLORS[risk.level]}`}>{risk.score?.toFixed(0) ?? "—"}<span className="text-lg text-gray-500 font-normal"> / 100</span></div>
                            <div className={`text-xl font-bold mt-1 ${LEVEL_COLORS[risk.level]}`}>{risk.level}</div>
                        </div>
                        <div className="text-right">
                            <div className="text-sm text-gray-400">Сработало индикаторов</div>
                            <div className="text-3xl font-bold text-white">{triggeredFlags.length}<span className="text-gray-500 text-lg"> / 16</span></div>
                        </div>
                    </div>
                    {risk.top_reasons.length > 0 && (
                        <div className="mt-4 space-y-1">
                            <div className="text-xs text-gray-400 mb-2">Основные причины:</div>
                            {risk.top_reasons.map((r, i) => (
                                <div key={`${r.code}-${i}`} className="text-sm text-gray-200">• {r.description || r.code}</div>
                            ))}
                        </div>
                    )}
                </div>

                {/* Lot Info */}
                <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                    <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Информация о лоте</h2>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                        <div><span className="text-gray-500">Заказчик:</span> {lot.customer_bin ? <BinLink bin={lot.customer_bin} label={lot.customer_name || lot.customer_bin} isCustomer /> : <span className="text-gray-200">{lot.customer_name || lot.customer_bin}</span>}</div>
                        <div><span className="text-gray-500">Сумма:</span> <span className="text-gray-200 font-medium">{formatMoney(lot.amount)}</span></div>
                        {tender && <>
                            <div><span className="text-gray-500">Тендер №:</span> <button onClick={() => router.push(`/tenders/${tender.id}`)} className="text-blue-400 hover:underline">{tender.number_anno}</button></div>
                            <div><span className="text-gray-500">Дата публикации:</span> <span className="text-gray-200">{tender.publish_date?.slice(0, 10)}</span></div>
                            <div><span className="text-gray-500">Приём заявок:</span> <span className="text-gray-200">{tender.start_date?.slice(0, 10)} — {tender.end_date?.slice(0, 10)}</span></div>
                        </>}
                        {lot.dumping_flag && <div className="col-span-2"><span className="bg-orange-900/40 text-orange-400 border border-orange-700 text-xs px-2 py-1 rounded">⚠ Демпинг</span></div>}
                    </div>
                </div>

                {/* Contract Info */}
                {contract && (
                    <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                        <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Контракт</h2>
                        <div className="grid grid-cols-2 gap-4 text-sm">
                            <div><span className="text-gray-500">Поставщик:</span> <button onClick={() => router.push(`/suppliers/${contract.supplier_biin}`)} className="text-blue-400 hover:underline">{contract.supplier_biin}</button></div>
                            <div><span className="text-gray-500">Сумма контракта:</span> <span className="text-gray-200 font-medium">{formatMoney(contract.contract_sum_wnds)}</span></div>
                            <div><span className="text-gray-500">Дата подписания:</span> <span className="text-gray-200">{contract.sign_date?.slice(0, 10)}</span></div>
                            <div><span className="text-gray-500">Плановый срок:</span> <span className="text-gray-200">{contract.plan_exec_date?.slice(0, 10)}</span></div>
                            {contract.parent_id && <div className="col-span-2"><span className="bg-purple-900/40 text-purple-400 border border-purple-700 text-xs px-2 py-1 rounded">Допсоглашение (parent: {contract.parent_id})</span></div>}
                        </div>
                    </div>
                )}

                {/* Risk Flags */}
                {triggeredFlags.length > 0 && (
                    <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                        <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Сработавшие индикаторы ({triggeredFlags.length})</h2>
                        <div className="space-y-3">
                            {triggeredFlags.map((f, i) => (
                                <button
                                    key={`${f.code}-${i}`}
                                    onClick={() => openIndicatorDetail(f.code)}
                                    className="w-full text-left bg-red-900/20 border border-red-800/50 rounded-xl p-4 hover:bg-red-900/30 hover:border-red-700 transition"
                                >
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="font-mono text-sm font-bold text-red-400">{f.code}</span>
                                        {f.code !== "CUSTOMER_WINNER_CONCENTRATION" && (
                                          <span className="text-xs text-blue-400 ml-2">Подробнее →</span>
                                        )}
                                    </div>
                                    {f.value !== null && <div className="text-xs text-gray-400 mb-1">Значение: {f.value}</div>}
                                    {f.evidence && Object.keys(f.evidence).length > 0 && (
                                        <div className="text-xs text-gray-500 space-y-0.5">
                                            {Object.entries(f.evidence).slice(0, 5).map(([k, v]) => (
                                                <div key={k}><span className="text-gray-600">{EVIDENCE_LABELS[k] || k}:</span> {String(v)}</div>
                                            ))}
                                        </div>
                                    )}
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                {/* Analyst Note */}
                <div className="bg-gray-900 border border-gray-800 rounded-2xl p-6">
                    <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-wider mb-4">Заметка аналитика</h2>
                    <div className="flex gap-3 mb-3">
                        <select value={noteLabel} onChange={e => setNoteLabel(e.target.value)} className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-200 focus:outline-none">
                            <option value="NEEDS_REVIEW">Требует проверки</option>
                            <option value="SUSPICIOUS">Подозрительно</option>
                            <option value="FALSE_POSITIVE">Ложное срабатывание</option>
                            <option value="VERIFIED">Проверено</option>
                        </select>
                    </div>
                    <textarea
                        value={noteText}
                        onChange={e => setNoteText(e.target.value)}
                        placeholder="Добавить заметку..."
                        className="w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-200 focus:outline-none focus:border-red-500 resize-none h-24"
                    />
                    <button
                        onClick={saveNote}
                        disabled={noteSaving || !noteText.trim()}
                        className="mt-3 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white px-4 py-2 rounded-lg text-sm font-medium transition"
                    >
                        {noteSaving ? "Сохранение..." : "Сохранить"}
                    </button>
                </div>
            </main>

            {/* Indicator Detail Modal */}
            {indicatorDetailCode && (
                <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4"
                    onClick={() => { setIndicatorDetailCode(null); setIndicatorDetail(null); }}
                >
                    <div
                        className="rounded-2xl border border-gray-700 bg-gray-900 shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="flex items-center justify-between p-4 border-b border-gray-700">
                            <h3 className="text-sm font-semibold text-white font-mono">{indicatorDetailCode}</h3>
                            <button
                                onClick={() => { setIndicatorDetailCode(null); setIndicatorDetail(null); }}
                                className="text-gray-400 hover:text-white px-2 py-1"
                            >
                                ✕
                            </button>
                        </div>
                        <div className="p-4 overflow-auto flex-1">
                            {indicatorDetailLoading ? (
                                <div className="py-12 text-center text-gray-400">Загрузка...</div>
                            ) : indicatorDetail?.code === "CAROUSEL_PATTERN" && indicatorDetail.contracts ? (
                                <div className="space-y-4">
                                    <div className="text-xs text-gray-400 space-y-1">
                                        <p>Заказчик: <BinLink bin={indicatorDetail.customer_bin!} label={indicatorDetail.customer_name || indicatorDetail.customer_bin} isCustomer /> ({indicatorDetail.customer_bin})</p>
                                        <p>Ротаций: <span className="text-gray-200 font-mono">{indicatorDetail.rotation_count}</span> · Уникальных победителей: <span className="font-mono">{indicatorDetail.unique_winners}</span></p>
                                    </div>
                                    <div className="text-xs font-medium text-gray-300 mb-2">Цепочка контрактов (хронологически):</div>
                                    <div className="overflow-x-auto rounded-lg border border-gray-700">
                                        <table className="w-full text-xs">
                                            <thead>
                                                <tr className="border-b border-gray-700 text-gray-400 text-left">
                                                    <th className="px-3 py-2">№ контракта</th>
                                                    <th className="px-3 py-2">Дата</th>
                                                    <th className="px-3 py-2">Поставщик (БИН)</th>
                                                    <th className="px-3 py-2">Сумма</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {indicatorDetail.contracts.map((c, i) => (
                                                    <tr key={i} className="border-b border-gray-800 hover:bg-gray-800/50">
                                                        <td className="px-3 py-2 font-mono text-gray-200">{c.contract_number}</td>
                                                        <td className="px-3 py-2 text-gray-400">{c.sign_date?.slice(0, 10) ?? "—"}</td>
                                                        <td className="px-3 py-2">
                                                            {c.supplier_biin ? (
                                                                <BinLink bin={c.supplier_biin} label={c.supplier_name ? `${c.supplier_name} (${c.supplier_biin})` : c.supplier_biin} />
                                                            ) : "—"}
                                                        </td>
                                                        <td className="px-3 py-2 text-gray-300">{formatMoney(c.contract_sum)}</td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ) : indicatorDetail?.code === "RECURRING_WINNER" && indicatorDetail.contracts ? (
                                <div className="space-y-4">
                                    <div className="text-xs text-gray-400 space-y-1">
                                        <p>Заказчик: <BinLink bin={indicatorDetail.customer_bin!} label={indicatorDetail.customer_bin} isCustomer /></p>
                                        <p>Поставщик: <BinLink bin={indicatorDetail.supplier_biin!} label={indicatorDetail.supplier_name || indicatorDetail.supplier_biin} /> ({indicatorDetail.supplier_biin})</p>
                                    </div>
                                    <div className="text-xs font-medium text-gray-300 mb-2">Контракты поставщика у заказчика:</div>
                                    <div className="overflow-x-auto rounded-lg border border-gray-700">
                                        <table className="w-full text-xs">
                                            <thead>
                                                <tr className="border-b border-gray-700 text-gray-400 text-left">
                                                    <th className="px-3 py-2">№ контракта</th>
                                                    <th className="px-3 py-2">Дата</th>
                                                    <th className="px-3 py-2">Сумма</th>
                                                    <th className="px-3 py-2">Тендер</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {indicatorDetail.contracts.map((c, i) => (
                                                    <tr key={i} className="border-b border-gray-800 hover:bg-gray-800/50">
                                                        <td className="px-3 py-2 font-mono text-gray-200">{c.contract_number}</td>
                                                        <td className="px-3 py-2 text-gray-400">{c.sign_date?.slice(0, 10) ?? "—"}</td>
                                                        <td className="px-3 py-2 text-gray-300">{formatMoney(c.contract_sum)}</td>
                                                        <td className="px-3 py-2 text-gray-500">{c.tender_number ?? "—"}</td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ) : indicatorDetail?.evidence && Object.keys(indicatorDetail.evidence).length > 0 ? (
                                <div className="space-y-2 text-xs">
                                    {Object.entries(indicatorDetail.evidence).map(([k, v]) => (
                                        <div key={k} className="flex gap-2">
                                            <span className="text-gray-500 shrink-0">{EVIDENCE_LABELS[k] || k}:</span>
                                            <span className="text-gray-300 break-all">
                                                {Array.isArray(v) ? v.join(", ") : typeof v === "object" ? JSON.stringify(v) : String(v)}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="text-sm text-gray-500 py-8 text-center">Нет расширенных данных для этого индикатора</div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
