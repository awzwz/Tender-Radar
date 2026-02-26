# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Optional, Tuple

# Keywords for sections
SECTION_MARKERS = [
    "Трудовые ресурсы",
    "Еңбек ресурстары",
]

# Role aliases (RU/KZ variants encountered in typical appendices)
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

# Document keywords -> normalized doc types
DOC_KEYWORDS = {
    "удостоверение личности": ["удостоверение личности", "жеке куәлік"],
    "квалификационный аттестат": ["квалификационный аттестат", "аттестат"],
    "реестр ИТР": ["реестр", "тізілім"],
    "подтверждающие документы": ["подтверждающие", "растайтын", "подтверждение"],
}

def normalize_text(s: str) -> str:
    # Make spacing/punctuation more consistent for regex
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\r\n|\r", "\n", s)
    return s

def find_section_window(lines: List[str], marker_variants: List[str], window: int = 60) -> List[str]:
    """
    Find a section by marker and return a window of subsequent lines.
    If not found, return empty list.
    """
    for i, line in enumerate(lines):
        for m in marker_variants:
            if m.lower() in line.lower():
                start = i
                end = min(len(lines), i + window)
                return lines[start:end]
    return []

def extract_count_near(text: str, role_variant: str) -> Optional[int]:
    """
    Try to find a headcount near a role name.
    Handles patterns like: '<role> ... 5' or '5 ... <role>'
    """
    # role ... digits
    p1 = re.compile(re.escape(role_variant) + r".{0,80}?(\d{1,3})", re.IGNORECASE | re.DOTALL)
    m = p1.search(text)
    if m:
        try:
            return int(m.group(1))
        except:
            pass

    # digits ... role
    p2 = re.compile(r"(\d{1,3}).{0,80}?" + re.escape(role_variant), re.IGNORECASE | re.DOTALL)
    m = p2.search(text)
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    return None

def extract_required_docs_near(text: str) -> List[str]:
    """
    Detect required doc types by keywords in a local slice of text.
    """
    found = []
    low = text.lower()
    for norm, kws in DOC_KEYWORDS.items():
        if any(kw.lower() in low for kw in kws):
            found.append(norm)
    return sorted(set(found))

def extract_experience_cap(text: str) -> Optional[Dict]:
    """
    Some appendices include a phrase like 'stazh ... ne bolee treh let'.
    We'll capture that as a note (not always a strict requirement for each role).
    """
    low = text.lower()
    if "не более трех лет" in low or "не более трёх лет" in low:
        return {
            "type": "experience_cap",
            "max_years": 3,
            "text": "Стаж работника (при необходимости) не более трех лет (кроме случаев, когда законом предусмотрен более высокий стаж)."
        }
    return None


def extract_required_docs_from_lines(lines: List[str]) -> List[str]:
    """
    Detect required doc types from a small list of lines (usually a single row).
    This avoids "bleeding" keywords from other roles in the same section.
    """
    low = " ".join(lines).lower()
    found = []
    for norm, kws in DOC_KEYWORDS.items():
        if any(kw.lower() in low for kw in kws):
            found.append(norm)
    return sorted(set(found))

def extract_experience_cap(text: str) -> Optional[Dict]:
    """
    Capture phrases like:
      - 'не более трех лет'
      - 'не более 3 лет'
      - 'не более трёх лет'
    PDFs sometimes break words, so use regex.
    """
    low = text.lower().replace("ё", "е")
    # allow spaces/newlines between tokens
    if re.search(r"не\s*более\s*(трех|3)\s*лет", low, flags=re.IGNORECASE):
        return {
            "type": "experience_cap",
            "max_years": 3,
            "text": "Стаж работника (при необходимости) не более трех лет (кроме случаев, когда законом предусмотрен более высокий стаж)."
        }
    return None
