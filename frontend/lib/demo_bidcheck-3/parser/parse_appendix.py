# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List

import pdfplumber

from .patterns import (
    normalize_text,
    SECTION_MARKERS,
    ROLE_ALIASES,
    find_section_window,
    extract_count_near,
    extract_required_docs_near,
    extract_required_docs_from_lines,
    extract_experience_cap,
)

def pdf_to_text(pdf_path: Path) -> str:
    chunks: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            chunks.append(txt)
    return "\n".join(chunks)

def parse_labor_requirements(text: str) -> List[Dict[str, Any]]:
    """
    Extract labor requirements: roles + required headcount + required docs hints.
    This is intentionally pragmatic for hackathon demo PDFs.
    """
    text = normalize_text(text)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    window_lines = find_section_window(lines, SECTION_MARKERS, window=80)
    window_text = "\n".join(window_lines) if window_lines else text

    results: List[Dict[str, Any]] = []

    for canonical_role, variants in ROLE_ALIASES.items():
        best_count = None
        best_variant = None

        for v in variants:
            c = extract_count_near(window_text, v)
            if c is not None:
                best_count = c
                best_variant = v
                break

        if best_count is not None:
            # infer required docs from the specific line(s) where the role is mentioned
            docs = []
            if window_lines:
                for j, ln in enumerate(window_lines):
                    if best_variant.lower() in ln.lower():
                        docs = extract_required_docs_from_lines([ln, window_lines[j+1] if j+1 < len(window_lines) else ""])
                        break
            if not docs:
                # fallback to local slice
                idx = window_text.lower().find(best_variant.lower())
                local = window_text[max(0, idx-120): idx+240] if idx >= 0 else window_text
                docs = extract_required_docs_near(local)
            results.append({
                "role": canonical_role,
                "count": best_count,
                "source_role_variant": best_variant,
                "required_docs_hints": docs,
            })

    # de-duplicate roles if alias overlaps produced duplicates (shouldn't, but safe)
    dedup = {}
    for r in results:
        dedup[r["role"]] = r
    return list(dedup.values())

def parse_appendix(pdf_path: str | Path) -> Dict[str, Any]:
    pdf_path = Path(pdf_path)
    raw_text = pdf_to_text(pdf_path)
    labor = parse_labor_requirements(raw_text)
    note = extract_experience_cap(raw_text)

    out = {
        "source_pdf": str(pdf_path),
        "parsed_at_utc": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "sections": {
            "labor_requirements": labor,
            "notes": [note] if note else [],
        }
    }
    return out

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Path to appendix PDF")
    ap.add_argument("--out", required=True, help="Output JSON path")
    args = ap.parse_args()

    data = parse_appendix(args.pdf)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Parsed: {args.pdf}")
    print(f"[OK] Wrote:  {args.out}")

if __name__ == "__main__":
    main()
