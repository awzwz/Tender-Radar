#!/usr/bin/env python3
"""
generate_demo_data.py
=====================
Downloads real technical specifications from goszakup.gov.kz,
generates matching norms and supplier documents via OpenAI,
creates PDFs, and outputs structured JSON for the frontend.

Usage:
    python3 scripts/generate_demo_data.py
"""

from __future__ import annotations

import json, os, re, sys, time, random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import requests
import pdfplumber
from openai import OpenAI
from fpdf import FPDF

# ── Config ──────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
OUT_DIR = ROOT / "frontend" / "public" / "demo" / "specnorm"
TECHSPEC_DIR = OUT_DIR / "techspec_pdfs"
NORM_DIR = OUT_DIR / "norm_pdfs"
SUPPLIER_DIR = OUT_DIR / "supplier_pdfs"

OWS_BASE = "https://ows.goszakup.gov.kz"
GRAPHQL_URL = f"{OWS_BASE}/v3/graphql"
FILE_DL_URL = "https://v3bl.goszakup.gov.kz/files/download_file"

FONT_PATH = "/Library/Fonts/Arial Unicode.ttf"

TARGET_COUNT = 50
CATEGORIES = {
    1: {"code": "goods", "label": "Товары", "count": 15},
    2: {"code": "works", "label": "Работы", "count": 20},
    3: {"code": "services", "label": "Услуги", "count": 15},
}

REGIONS = [
    "Алматинская область", "Астана", "Шымкент", "Караганда",
    "Мангистауская область", "Актюбинская область", "Атырауская область",
    "Восточно-Казахстанская область", "Западно-Казахстанская область",
    "Жамбылская область", "Кызылординская область", "Костанайская область",
    "Павлодарская область", "Северо-Казахстанская область",
    "Туркестанская область", "Акмолинская область",
]


def load_env() -> Dict[str, str]:
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


ENV = load_env()
OWS_TOKEN = ENV.get("OWS_TOKEN", "")
OPENAI_API_KEY = ENV.get("OPENAI_API_KEY", "")

if not OPENAI_API_KEY:
    sys.exit("ERROR: OPENAI_API_KEY not found in .env")

client = OpenAI(api_key=OPENAI_API_KEY)

HEADERS = {
    "Authorization": f"Bearer {OWS_TOKEN}",
    "Content-Type": "application/json",
}


# ── Helpers ─────────────────────────────────────────────────────────────────

def gql(query: str, variables: Optional[dict] = None) -> dict:
    payload: Dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(3):
        try:
            r = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    print(f"  GraphQL errors: {[e['message'] for e in data['errors'][:2]]}")
                return data.get("data", {})
            print(f"  HTTP {r.status_code}, retry {attempt+1}/3")
        except Exception as e:
            print(f"  Request error: {e}, retry {attempt+1}/3")
        time.sleep(2 * (attempt + 1))
    return {}


def download_file(file_id: int, dest: Path) -> bool:
    url = f"{FILE_DL_URL}/{file_id}/"
    try:
        r = requests.get(url, timeout=30, stream=True)
        if r.status_code == 200 and len(r.content) > 500:
            dest.write_bytes(r.content)
            return True
    except Exception as e:
        print(f"  Download error for {file_id}: {e}")
    return False


def extract_pdf_text(path: Path, max_pages: int = 10) -> str:
    try:
        with pdfplumber.open(str(path)) as pdf:
            texts = []
            for p in pdf.pages[:max_pages]:
                t = p.extract_text()
                if t:
                    texts.append(t)
            return "\n\n".join(texts)
    except Exception as e:
        print(f"  PDF extract error: {e}")
        return ""


def llm_call(system: str, user: str, max_tokens: int = 2000) -> str:
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=max_tokens,
                temperature=0.7,
            )
            return resp.choices[0].message.content or ""
        except Exception as e:
            print(f"  LLM error: {e}, retry {attempt+1}/3")
            time.sleep(3 * (attempt + 1))
    return ""


def llm_json(system: str, user: str, max_tokens: int = 3000) -> Optional[Union[dict, list]]:
    raw = llm_call(system, user, max_tokens)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```\w*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"[\[{].*[\]}]", raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                pass
        print(f"  Failed to parse LLM JSON. First 300 chars: {raw[:300]}")
        return None


# ── PDF Generation ──────────────────────────────────────────────────────────

def safe_text(text: str) -> str:
    """Clean text for PDF rendering: remove problematic chars, limit line length."""
    text = text.replace("\t", "    ")
    text = re.sub(r"[^\S\n]+", " ", text)
    text = "".join(c for c in text if c == "\n" or (ord(c) >= 32 and ord(c) != 127))
    return text


def make_pdf(title: str, subtitle: str, content: str, dest: Path):
    """Generate a professional-looking Cyrillic PDF."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    if Path(FONT_PATH).exists():
        pdf.add_font("Uni", "", FONT_PATH)
        pdf.add_font("Uni", "B", FONT_PATH)
        font_name = "Uni"
    else:
        font_name = "Helvetica"

    pdf.set_font(font_name, "B", 14)
    pdf.cell(0, 10, safe_text(title), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font(font_name, "", 9)
    pdf.cell(0, 6, safe_text(subtitle), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 6, f"Дата: {datetime.now().strftime('%d.%m.%Y')}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    content = safe_text(content)
    pdf.set_font(font_name, "", 10)
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            pdf.ln(3)
            continue
        try:
            if line.startswith("##"):
                pdf.set_font(font_name, "B", 11)
                pdf.multi_cell(0, 6, line.lstrip("#").strip())
                pdf.set_font(font_name, "", 10)
            elif line.startswith("#"):
                pdf.set_font(font_name, "B", 12)
                pdf.multi_cell(0, 7, line.lstrip("#").strip())
                pdf.set_font(font_name, "", 10)
            elif line.startswith(("-", "•", "–", "*")):
                pdf.multi_cell(0, 5, "  " + line)
            elif line.startswith("|"):
                pdf.set_font(font_name, "", 8)
                pdf.multi_cell(0, 4, line)
                pdf.set_font(font_name, "", 10)
            else:
                pdf.multi_cell(0, 5, line)
        except Exception:
            pass

    try:
        pdf.output(str(dest))
    except Exception as e:
        print(f"  PDF save error for {dest.name}: {e}")


# ── Step 1: Fetch real tenders ──────────────────────────────────────────────

FETCH_QUERY = """
query($filter: TrdBuyFiltersInput, $limit: Int, $after: Int) {
  TrdBuy(filter: $filter, limit: $limit, after: $after) {
    id
    numberAnno
    nameRu
    totalSum
    refSubjectTypeId
    startDate
    endDate
    refBuyStatusId
    orgBin
    orgNameRu
    Files {
      id
      nameRu
      originalName
      filePath
    }
  }
}
"""


def fetch_tenders_with_files(subject_type_id: int, limit: int) -> List[dict]:
    print(f"  Fetching tenders for category {subject_type_id} (limit={limit})...")
    collected: List[dict] = []
    after = 0

    while len(collected) < limit:
        data = gql(FETCH_QUERY, {
            "filter": {"refSubjectTypeId": subject_type_id, "refBuyStatusId": 350},
            "limit": 50,
            "after": after,
        })
        trd_buys = data.get("TrdBuy", [])
        if not trd_buys:
            break

        for t in trd_buys:
            files = t.get("Files") or []
            techspec_files = [
                f for f in files if f and f.get("originalName") and
                any(kw in (f.get("originalName") or "").lower()
                    for kw in ["тех", "спец", "техническ", "ts_", "приложение"])
            ]
            if techspec_files and t.get("nameRu"):
                collected.append({"raw": t, "techspec_file": techspec_files[0]})
                if len(collected) >= limit:
                    break

        last_id = trd_buys[-1].get("id", 0) if trd_buys else 0
        after = last_id
        time.sleep(0.5)

    return collected[:limit]


# ── Step 2: Synthetic techspec generation ───────────────────────────────────

TECHSPEC_TEMPLATES = {
    "works": [
        ("Капитальный ремонт автомобильной дороги республиканского значения", "дорожное строительство"),
        ("Строительство 9-этажного жилого дома", "жилищное строительство"),
        ("Реконструкция системы водоснабжения города", "водоснабжение"),
        ("Ремонт кровли здания общеобразовательной школы №47", "кровельные работы"),
        ("Строительство спортивного комплекса с бассейном", "строительство спортивных сооружений"),
        ("Благоустройство парковой зоны с устройством дорожек и освещения", "благоустройство"),
        ("Капитальный ремонт железобетонного моста через реку", "мостостроение"),
        ("Строительство канализационных коллекторных сетей", "инженерные сети"),
        ("Ремонт фасада административного здания акимата", "фасадные работы"),
        ("Устройство наружного освещения улиц", "электромонтаж"),
        ("Асфальтирование внутридворовых территорий жилого комплекса", "дорожные работы"),
        ("Строительство детской игровой площадки", "малые архитектурные формы"),
        ("Капитальный ремонт системы центрального отопления", "теплоснабжение"),
        ("Реконструкция электрических сетей 10 кВ", "электроснабжение"),
        ("Строительство металлического ограждения территории", "ограждение"),
        ("Ремонт внутренних помещений городской больницы №3", "внутренняя отделка"),
        ("Укрепление берегов водохранилища габионными конструкциями", "гидротехника"),
        ("Строительство подземного пешеходного перехода", "подземное строительство"),
        ("Монтаж автоматической системы пожарной сигнализации", "пожарная безопасность"),
        ("Капитальный ремонт легкоатлетического стадиона", "спортивные сооружения"),
    ],
    "services": [
        ("Услуги по организации и проведению международного форума", "организация мероприятий"),
        ("Охранные услуги объектов государственного учреждения", "охрана"),
        ("Услуги по техническому обслуживанию лифтового оборудования", "техобслуживание"),
        ("Клининговые услуги административных зданий", "клининг"),
        ("Услуги по разработке проектно-сметной документации", "проектирование"),
        ("Транспортные услуги для перевозки грузов", "логистика"),
        ("Аудиторские услуги — проверка финансовой отчётности", "аудит"),
        ("Услуги по сопровождению информационной системы", "IT-услуги"),
        ("Услуги по озеленению и содержанию зелёных насаждений", "озеленение"),
        ("Медицинские услуги — профилактический осмотр сотрудников", "медицина"),
        ("Услуги по повышению квалификации государственных служащих", "обучение"),
        ("Услуги по вывозу и утилизации твёрдых бытовых отходов", "утилизация"),
        ("Юридические консультационные услуги по арбитражу", "юриспруденция"),
        ("Услуги по ремонту и обслуживанию оргтехники", "техсервис"),
        ("Услуги по аттестации рабочих мест по условиям труда", "охрана труда"),
    ],
    "goods": [
        ("Закупка серверного и сетевого оборудования", "IT-оборудование"),
        ("Поставка офисной мебели для здания акимата", "мебель"),
        ("Закупка медицинских расходных материалов для больницы", "медматериалы"),
        ("Поставка строительных материалов (цемент, арматура, кирпич)", "стройматериалы"),
        ("Закупка спецодежды и средств индивидуальной защиты", "СИЗ"),
        ("Поставка продуктов питания для школьной столовой", "продовольствие"),
        ("Закупка ГСМ (бензин АИ-92, АИ-95, дизельное топливо)", "ГСМ"),
        ("Поставка канцелярских товаров и бумаги", "канцтовары"),
        ("Закупка компьютерной техники (ноутбуки, мониторы)", "компьютеры"),
        ("Поставка лабораторного оборудования для НИИ", "лабораторное оборудование"),
        ("Закупка учебников и учебных пособий для школ", "учебная литература"),
        ("Поставка сантехнического оборудования", "сантехника"),
        ("Закупка электроинструмента (дрели, болгарки, перфораторы)", "электроинструмент"),
        ("Поставка кондиционеров и сплит-систем", "климатическая техника"),
        ("Закупка спортивного инвентаря для ДЮСШ", "спортинвентарь"),
    ],
}

GEN_TECHSPEC_SYSTEM = """Ты специалист по составлению технических спецификаций для государственных закупок Республики Казахстан.
Создай ПОДРОБНУЮ и РЕАЛИСТИЧНУЮ техническую спецификацию. Пиши ТОЛЬКО на русском языке.

Требования к документу:
- Формат: официальный документ с нумерованными разделами
- Обязательные разделы: Предмет закупки, Объём и содержание, Технические требования (с конкретными числовыми параметрами), Требования к квалификации, Сроки исполнения, Гарантийные обязательства
- Используй конкретные числа: размеры в метрах, вес в кг/тоннах, мощность в кВт, температуру, давление
- Для работ: укажи объёмы (м², м³, п.м.), марки бетона/стали, толщины слоёв
- Для услуг: укажи требования к персоналу (образование, стаж, лицензии)
- Для товаров: укажи точные технические характеристики, стандарты (ГОСТ, СТ РК)
- Объём: 1.5-2 страницы текста"""


def generate_synthetic_techspec(title: str, area: str, category: str, region: str, amount: int) -> str:
    prompt = (
        f"Создай техническую спецификацию для тендера:\n"
        f"Название: {title}\nОбласть: {area}\nКатегория: {category}\n"
        f"Регион: {region}\nСумма: {amount:,} тенге\n\n"
        f"Документ должен выглядеть как настоящая техспецификация с портала goszakup.gov.kz."
    )
    return llm_call(GEN_TECHSPEC_SYSTEM, prompt, max_tokens=2500)


# ── Step 3: Generate norms + supplier docs + analysis ───────────────────────

NORM_SYSTEM = """Ты эксперт по нормативному регулированию государственных закупок РК.
На основании технической спецификации тендера, создай НОРМАТИВНЫЙ СПРАВОЧНЫЙ ДОКУМЕНТ.

Обязательно включи:
1. Ссылки на конкретные ГОСТы, СНиПы, СТ РК, Правила (с номерами и годами)
2. Числовые нормативные значения параметров из этих документов
3. Квалификационные требования согласно законодательству РК
4. Требования к материалам, оборудованию по нормативам
5. Предельные допустимые значения (минимумы и максимумы)

Формат: структурированный текст с разделами (# для заголовков).
Пиши ТОЛЬКО на русском. Будь максимально конкретен — реальные номера ГОСТов и числа."""

SUPPLIER_SYSTEM_CLEAN = """Ты генератор реалистичных квалификационных документов для госзакупок РК.
На основании техспецификации создай ПОЛНЫЙ пакет документов от поставщика, который ПОЛНОСТЬЮ СООТВЕТСТВУЕТ всем требованиям.

Включи:
1. Данные компании: название (ТОО "..."), БИН (12 цифр), юр. адрес, директор (казахское ФИО)
2. Опыт работы: 3-5 конкретных выполненных контрактов (номера, суммы, заказчики, годы)
3. Трудовые ресурсы: ФИО сотрудников, должности, образование (вузы Казахстана), дипломы, стаж
4. Материально-техническая база: конкретное оборудование (марки, год, кол-во)
5. Лицензии и сертификаты: номера, кем выданы, сроки действия (все актуальные!)
6. Финансовые показатели: оборот, налоги уплачены, отсутствие задолженностей

ВСЕ данные полностью соответствуют требованиям техспецификации. Поставщик идеально квалифицирован.
Используй реалистичные казахстанские данные (БИН, ИИН, города, вузы). Пиши на русском."""

SUPPLIER_SYSTEM_VIOLATIONS = """Ты генератор реалистичных квалификационных документов для госзакупок РК.
На основании техспецификации создай пакет документов от поставщика, в которых ЕСТЬ СКРЫТЫЕ НАРУШЕНИЯ.

Включи:
1. Данные компании: название (ТОО "..."), БИН (12 цифр), юр. адрес, директор
2. Опыт работы: контракты (но один из них — по другому профилю, не подходит)
3. Трудовые ресурсы: сотрудники, но {violation_detail}
4. Материально-техническая база: оборудование (но часть не соответствует требованиям или устарела)
5. Лицензии: есть, но {license_issue}
6. Финансовые показатели

Нарушения должны быть НЕОЧЕВИДНЫМИ — на первый взгляд документы выглядят нормально, но при внимательном анализе видно несоответствие.
Используй реалистичные казахстанские данные. Пиши на русском."""

VIOLATION_DETAILS = [
    ("один сотрудник без требуемого диплома или с дипломом по другой специальности",
     "одна лицензия истекла 2 месяца назад"),
    ("у ведущего специалиста стаж 2 года вместо требуемых 5",
     "сертификат ISO просрочен"),
    ("не указан электрик с группой допуска, хотя работы требуют электромонтаж",
     "лицензия выдана на другой вид деятельности"),
    ("отсутствует проектировщик с аттестацией, хотя техспец требует проектные работы",
     "отсутствует лицензия на проектирование"),
    ("сварщик без аттестации НАКС, хотя работы включают сварочные",
     "допуск СРО истёк в прошлом году"),
]

ANALYSIS_SYSTEM = """Ты AI-аналитик по проверке соответствия в государственных закупках Казахстана.
Тебе даны: 1) Техническая спецификация, 2) Нормативный документ, 3) Документы поставщика.

Проведи ДВУСТОРОННЮЮ ПРОВЕРКУ:

A) ПРОВЕРКА ЗАКАЗЧИКА (Spec vs Norm):
- Сравни параметры техспецификации с нормативами
- Есть ли завышение требований? (сужение конкуренции)
- specDeviationScore: 0-100 (0=норма, 100=грубое завышение)

B) ПРОВЕРКА ПОСТАВЩИКА (Docs vs TechSpec):
- Соответствуют ли документы поставщика требованиям техспецификации?
- docComplianceScore: 0-100 (100=полное соответствие, 0=полное несоответствие)
- Конкретные нарушения если есть

Ответь СТРОГО в JSON (без markdown):
{
  "specDeviationScore": 0-100,
  "specFlags": ["PARAM_INFLATION", "QUALIFICATION_OVERSPEC", "TIMELINE_UNREASONABLE", "MATERIAL_OVERSPEC"],
  "specSummary": ["вывод 1", "вывод 2"],
  "extracted": {"param1": значение, "param2": значение},
  "norm": {"param1": значение, "param2": значение},
  "docComplianceScore": 0-100,
  "docViolations": [
    {
      "type": "MISSING_DOCUMENT|QUALIFICATION_MISMATCH|EXPIRED_LICENSE|INSUFFICIENT_EXPERIENCE|EQUIPMENT_MISMATCH",
      "severity": "HIGH|MEDIUM|LOW",
      "description": "описание",
      "requirement": "что требовала техспец",
      "provided": "что дал поставщик"
    }
  ],
  "docSummary": ["вывод 1", "вывод 2"],
  "overallRiskTier": "HIGH|MEDIUM|LOW",
  "llmAnalysis": "развёрнутый текст анализа 4-6 предложений"
}"""


def generate_for_tender(tender_id: str, category: str, techspec_text: str, has_violations: bool) -> Optional[dict]:
    """Generate norm + supplier docs + analysis for one tender."""

    # 1) Norm document
    print(f"    Generating norm...")
    norm_content = llm_call(
        NORM_SYSTEM,
        f"Категория: {category}\n\nТехническая спецификация:\n{techspec_text[:4000]}",
        max_tokens=2000,
    )
    if not norm_content:
        return None

    # 2) Supplier documents
    print(f"    Generating supplier docs (violations={has_violations})...")
    if has_violations:
        vd = random.choice(VIOLATION_DETAILS)
        system = SUPPLIER_SYSTEM_VIOLATIONS.replace("{violation_detail}", vd[0]).replace("{license_issue}", vd[1])
    else:
        system = SUPPLIER_SYSTEM_CLEAN

    supplier_content = llm_call(
        system,
        f"Категория: {category}\n\nТехническая спецификация:\n{techspec_text[:4000]}",
        max_tokens=2500,
    )
    if not supplier_content:
        return None

    # 3) Analysis
    print(f"    Running LLM analysis...")
    analysis_input = (
        f"=== ТЕХНИЧЕСКАЯ СПЕЦИФИКАЦИЯ ===\n{techspec_text[:3000]}\n\n"
        f"=== НОРМАТИВНЫЙ ДОКУМЕНТ ===\n{norm_content[:2000]}\n\n"
        f"=== ДОКУМЕНТЫ ПОСТАВЩИКА ===\n{supplier_content[:2500]}"
    )
    analysis = llm_json(ANALYSIS_SYSTEM, analysis_input, max_tokens=3000)
    if not isinstance(analysis, dict):
        return None

    return {
        "norm_content": norm_content,
        "supplier_content": supplier_content,
        "analysis": analysis,
    }


def extract_supplier_name(content: str) -> str:
    for line in content.split("\n"):
        match = re.search(r'(?:ТОО|ИП|АО)\s*[«"]([^»"]+)[»"]', line)
        if match:
            return match.group(0)
    return "ТОО «КазСтройСервис»"


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Tender Radar — Demo Data Generator")
    print("=" * 60)

    for d in [TECHSPEC_DIR, NORM_DIR, SUPPLIER_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    real_processed: List[dict] = []

    # ── Phase 1+2: Generate synthetic techspecs ──
    need = TARGET_COUNT
    print(f"\n[Phase 1] Generating {need} synthetic techspecs via OpenAI...")

    synthetic: List[dict] = []
    idx_per_cat = {"goods": 0, "works": 0, "services": 0}

    # Distribute evenly across categories
    cats_cycle = []
    for cat_id, meta in CATEGORIES.items():
        real_in_cat = sum(1 for p in real_processed if p["category"] == meta["code"])
        remaining = meta["count"] - real_in_cat
        for _ in range(max(0, remaining)):
            cats_cycle.append(meta["code"])

    random.shuffle(cats_cycle)
    cats_cycle = cats_cycle[:need]

    for i, cat in enumerate(cats_cycle):
        templates = TECHSPEC_TEMPLATES[cat]
        tmpl_idx = idx_per_cat[cat] % len(templates)
        title, area = templates[tmpl_idx]
        idx_per_cat[cat] += 1

        region = random.choice(REGIONS)
        amount = random.randint(5_000_000, 600_000_000)
        tid = f"{cat.upper()}-{300001 + i}"

        print(f"  [{i+1}/{need}] Generating {tid}: {title[:50]}...")

        techspec_text = generate_synthetic_techspec(title, area, cat, region, amount)
        if not techspec_text or len(techspec_text) < 200:
            print(f"    Failed, skipping")
            continue

        # Save techspec as PDF
        ts_path = TECHSPEC_DIR / f"techspec_{tid}.pdf"
        make_pdf(
            "ТЕХНИЧЕСКАЯ СПЕЦИФИКАЦИЯ",
            f"Тендер № {tid} · {region}",
            techspec_text,
            ts_path,
        )

        pub_date = (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 420))).strftime("%Y-%m-%d")

        synthetic.append({
            "tender_id": tid,
            "category": cat,
            "category_label": {"goods": "Товары", "works": "Работы", "services": "Услуги"}[cat],
            "raw": {
                "nameRu": f"{title} — {region}",
                "totalSum": amount,
                "startDate": pub_date,
                "orgNameRu": f"ГУ «Управление {region}»",
            },
            "techspec_text": techspec_text,
            "is_real": False,
        })
        time.sleep(1)

    all_entries = real_processed + synthetic
    print(f"\n  Total entries: {len(all_entries)}")

    # ── Phase 3: Generate norms, supplier docs, analysis ──
    print(f"\n[Phase 3] Generating norms + supplier docs + analysis...")

    # ~42% will have violations
    violation_set = set(random.sample(
        range(len(all_entries)),
        min(int(len(all_entries) * 0.42), len(all_entries)),
    ))

    findings: List[dict] = []
    for i, entry in enumerate(all_entries):
        tid = entry["tender_id"]
        has_viol = i in violation_set

        print(f"\n  [{i+1}/{len(all_entries)}] {tid} (violations={has_viol})")

        result = generate_for_tender(
            tid, entry["category"], entry["techspec_text"], has_viol,
        )
        if not result:
            print(f"    FAILED — skipping")
            continue

        # Save norm PDF
        norm_path = NORM_DIR / f"norm_{tid}.pdf"
        make_pdf(
            "НОРМАТИВНЫЙ ДОКУМЕНТ",
            f"Справка к тендеру {tid}",
            result["norm_content"],
            norm_path,
        )

        # Save supplier PDF
        sup_path = SUPPLIER_DIR / f"supplier_{tid}.pdf"
        make_pdf(
            "КВАЛИФИКАЦИОННЫЕ ДОКУМЕНТЫ ПОСТАВЩИКА",
            f"К тендеру {tid}",
            result["supplier_content"],
            sup_path,
        )

        a = result["analysis"]
        raw = entry.get("raw", {})

        region = random.choice(REGIONS)
        org = raw.get("orgNameRu", "")
        for r in REGIONS:
            if r.lower().split()[0] in org.lower():
                region = r
                break

        spec_score = a.get("specDeviationScore", random.randint(10, 40))
        doc_score = a.get("docComplianceScore", random.randint(70, 100))

        finding = {
            "tenderId": tid,
            "titleRu": raw.get("nameRu", f"Тендер {tid}"),
            "region": region,
            "amountKZT": raw.get("totalSum", random.randint(5_000_000, 500_000_000)),
            "publishDate": (raw.get("startDate") or "2025-06-15")[:10],
            "category": entry["category"],

            "specDeviationScore": spec_score,
            "isFlagged": spec_score >= 55,
            "activeFlags": a.get("specFlags", []),
            "summary": a.get("specSummary", []),
            "extracted": a.get("extracted", {}),
            "norm": a.get("norm", {}),

            "supplierName": extract_supplier_name(result["supplier_content"]),
            "docComplianceScore": doc_score,
            "docViolations": a.get("docViolations", []),
            "docSummary": a.get("docSummary", []),
            "llmAnalysis": a.get("llmAnalysis", ""),
            "overallRiskTier": a.get("overallRiskTier", "LOW"),

            "techspecFile": f"techspec_pdfs/techspec_{tid}.pdf",
            "normFile": f"norm_pdfs/norm_{tid}.pdf",
            "supplierDocFile": f"supplier_pdfs/supplier_{tid}.pdf",
        }

        findings.append(finding)
        print(f"    specScore={spec_score}, docScore={doc_score}, risk={finding['overallRiskTier']}")
        time.sleep(0.5)

    # ── Phase 4: Write JSON ──
    print(f"\n[Phase 4] Writing findings.json ({len(findings)} entries)...")
    out_path = OUT_DIR / "findings.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(findings, f, ensure_ascii=False, indent=2)
    print(f"  Saved to: {out_path}")

    # Summary
    print("\n" + "=" * 60)
    high = sum(1 for f in findings if f["overallRiskTier"] == "HIGH")
    med = sum(1 for f in findings if f["overallRiskTier"] == "MEDIUM")
    low = sum(1 for f in findings if f["overallRiskTier"] == "LOW")
    flagged = sum(1 for f in findings if f["isFlagged"])
    doc_viol = sum(1 for f in findings if f["docViolations"])
    cats = {}
    for f in findings:
        cats[f["category"]] = cats.get(f["category"], 0) + 1

    print(f"  Total: {len(findings)} findings")
    print(f"  Risk tiers: HIGH={high}, MEDIUM={med}, LOW={low}")
    print(f"  Spec flagged: {flagged}")
    print(f"  With doc violations: {doc_viol}")
    print(f"  Categories: {cats}")
    print(f"  Real techspecs: {sum(1 for e in all_entries if e.get('is_real'))}")
    print(f"  Synthetic techspecs: {sum(1 for e in all_entries if not e.get('is_real'))}")
    print("=" * 60)


if __name__ == "__main__":
    main()
