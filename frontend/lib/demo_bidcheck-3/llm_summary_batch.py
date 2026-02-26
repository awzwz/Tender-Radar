# llm_summary_batch.py
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

from openai import OpenAI

SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["PASS", "FAIL"]},
        "checks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": "string"},
                    "required": {"type": "string"},
                    "status": {"type": "string", "enum": ["OK", "FAIL", "UNKNOWN"]},
                    "evidence": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["role", "required", "status", "evidence"],
                "additionalProperties": False,
            },
        },
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "finding": {"type": "string"},
                    "evidence": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["category", "finding", "evidence"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["verdict", "checks", "issues"],
    "additionalProperties": False,
}

def pdf_part(pdf_path: Path) -> dict:
    b64 = base64.b64encode(pdf_path.read_bytes()).decode("utf-8")
    return {
        "type": "input_file",
        "filename": pdf_path.name,
        "file_data": f"data:application/pdf;base64,{b64}",
    }

def run_one(client: OpenAI, model: str, effort: str, pdf_path: Path, supplier_txt: Path, req_json: Path) -> dict:
    supplier_text = supplier_txt.read_text(encoding="utf-8", errors="replace")
    req_text = req_json.read_text(encoding="utf-8", errors="replace")

    instructions = (
        "Ты — аудитор госзакупок. Проверь соответствие пакета документов Поставщика требованиям из PDF.\n\n"
        "Правила:\n"
        "1) Ты ОБЯЗАН заполнить checks[] по всем ролям/позициям трудовых ресурсов, найденным в PDF.\n"
        "2) Для каждой роли: role, required (кол-во + ключевые доки), status (OK/FAIL/UNKNOWN), evidence.\n"
        "   - OK: кол-во и документы подтверждены.\n"
        "   - FAIL: не хватает людей или отсутствует обязательный документ/подтверждение.\n"
        "   - UNKNOWN: в данных не хватает информации.\n"
        "3) issues[] заполняй только для FAIL (UNKNOWN — только если реально блокирует проверку).\n"
        "4) Не выдумывай. Если чего-то нет — пиши 'не найдено'.\n"
        "5) verdict: PASS только если ВСЕ checks.status == OK, иначе FAIL.\n"
    )

    user_parts = [
        {"type": "input_text", "text": instructions},
        pdf_part(pdf_path),
        {"type": "input_text", "text": "=== ПАКЕТ ДОКУМЕНТОВ ПОСТАВЩИКА (TXT) ===\n" + supplier_text},
        {"type": "input_text", "text": "=== ПОДСКАЗКА: PARSED REQUIREMENTS JSON ===\n" + req_text},
    ]

    resp = client.responses.create(
        model=model,
        reasoning={"effort": effort},
        input=[{"role": "user", "content": user_parts}],
        text={
            "format": {
                "type": "json_schema",
                "name": "tender_compliance_summary",
                "schema": SUMMARY_SCHEMA,
                "strict": True,
            }
        },
    )

    data = json.loads(resp.output_text)

    # страховка verdict
    if any(c["status"] == "FAIL" for c in data["checks"]):
        data["verdict"] = "FAIL"
    elif all(c["status"] == "OK" for c in data["checks"]):
        data["verdict"] = "PASS"
    else:
        data["verdict"] = "FAIL"  # если UNKNOWN — для процессуалки лучше FAIL

    return data

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gpt-5.2")
    ap.add_argument("--effort", default="low", choices=["none", "low", "medium", "high", "xhigh"])
    ap.add_argument("--only", default="", help="Если нужно только один PDF: часть имени, напр. 16441244")
    args = ap.parse_args()

    base = Path(__file__).parent
    inputs_dir = base / "inputs"
    outputs_dir = base / "outputs"
    llm_dir = outputs_dir / "llm"
    llm_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(inputs_dir.glob("*.pdf"))
    if args.only:
        pdfs = [p for p in pdfs if args.only in p.name]

    if not pdfs:
        raise SystemExit("Нет PDF в inputs/ (или фильтр --only не совпал)")

    client = OpenAI()

    for pdf in pdfs:
        stem = pdf.stem
        req = outputs_dir / f"{stem}.requirements.json"
        if not req.exists():
            print(f"[SKIP] Нет requirements для {pdf.name}: ожидаю {req.name}")
            continue

        # где лежат supplier TXT для этого файла
        sup_dir = outputs_dir / "suppliers" / stem
        if not sup_dir.exists():
            print(f"[SKIP] Нет supplier-папки для {pdf.name}: ожидаю {sup_dir}")
            continue

        out_dir = llm_dir / stem
        out_dir.mkdir(parents=True, exist_ok=True)

        supplier_txts = sorted(sup_dir.glob("*.txt"))
        if not supplier_txts:
            print(f"[SKIP] Нет supplier TXT в {sup_dir}")
            continue

        print(f"\n[PDF] {pdf.name}")
        for s_txt in supplier_txts:
            out_path = out_dir / f"{s_txt.stem}.summary.json"
            data = run_one(client, args.model, args.effort, pdf, s_txt, req)
            out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  [OK] {s_txt.name} -> {out_path}")

if __name__ == "__main__":
    main()