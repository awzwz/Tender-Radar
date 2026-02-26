# -*- coding: utf-8 -*-
"""Patterns for appendix labor requirements extraction (from demo_bidcheck-3)."""
import re
from typing import List, Optional

SECTION_MARKERS = [
    "Трудовые ресурсы",
    "Еңбек ресурстары",
]

ROLE_ALIASES = {
    "Тракторист-машинист": [
        "Тракторист-машинист",
        "Тракторист машинист",
    ],
    "Инженер ИТР по сетям": [
        "Инженер ИТР по сетям",
        "Инженер ИТР",
        "Мастер ИТР",
        "ИТР",
    ],
    "Разнорабочие": [
        "Разнорабочие",
        "Жұмысшы",
        "Рабочие",
    ],
    "Сварщик": [
        "Сварщик",
        "Дәнекерлеуші",
    ],
}

DOC_KEYWORDS = {
    "удостоверение личности": ["удостоверение личности", "жеке куәлік"],
    "квалификационный аттестат": ["квалификационный аттестат", "аттестат"],
    "реестр ИТР": ["реестр", "тізілім"],
    "подтверждающие документы": ["подтверждающие", "растайтын", "подтверждение"],
}


def normalize_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\r\n|\r", "\n", s)
    return s


def find_section_window(lines: List[str], marker_variants: List[str], window: int = 60) -> List[str]:
    for i, line in enumerate(lines):
        for m in marker_variants:
            if m.lower() in line.lower():
                return lines[i : min(len(lines), i + window)]
    return []


def extract_count_near(text: str, role_variant: str) -> Optional[int]:
    p1 = re.compile(
        re.escape(role_variant) + r".{0,80}?(\d{1,3})",
        re.IGNORECASE | re.DOTALL,
    )
    m = p1.search(text)
    if m:
        try:
            return int(m.group(1))
        except (ValueError, IndexError):
            pass
    p2 = re.compile(
        r"(\d{1,3}).{0,80}?" + re.escape(role_variant),
        re.IGNORECASE | re.DOTALL,
    )
    m = p2.search(text)
    if m:
        try:
            return int(m.group(1))
        except (ValueError, IndexError):
            pass
    return None


def extract_required_docs_near(text: str) -> List[str]:
    found = []
    low = text.lower()
    for norm, kws in DOC_KEYWORDS.items():
        if any(kw.lower() in low for kw in kws):
            found.append(norm)
    return sorted(set(found))


def extract_required_docs_from_lines(lines: List[str]) -> List[str]:
    low = " ".join(lines).lower()
    found = []
    for norm, kws in DOC_KEYWORDS.items():
        if any(kw.lower() in low for kw in kws):
            found.append(norm)
    return sorted(set(found))


def extract_experience_cap(text: str) -> Optional[dict]:
    low = text.lower().replace("ё", "е")
    if re.search(r"не\s*более\s*(трех|3)\s*лет", low, flags=re.IGNORECASE):
        return {
            "type": "experience_cap",
            "max_years": 3,
            "text": "Стаж работника (при необходимости) не более трех лет.",
        }
    return None
