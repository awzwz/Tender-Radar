# llm_end2end_one.py
# End-to-end: PDF -> Requirements -> Supplier packs (synthetic text) -> Compliance summaries
# Run (PowerShell):
#   py .\llm_end2end_one.py --pdf ".\inputs\appendix_7_683805.pdf"

from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

from openai import OpenAI


# === JSON Schema: one response contains everything ===
SCHEMA = {
    "type": "object",
    "properties": {
        "requirements": {
            "type": "object",
            "properties": {
                "labor_roles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "role": {"type": "string"},
                            "count": {"type": "integer", "minimum": 0},
                            "required_documents": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "notes": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "evidence": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": ["role", "count", "required_documents", "notes", "evidence"],
                        "additionalProperties": False,
                    },
                },
                "global_notes": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["labor_roles", "global_notes"],
            "additionalProperties": False,
        },
        "suppliers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "supplier_name": {"type": "string"},
                    "profile": {"type": "string", "enum": ["FULL", "MINOR_MISSING"]},
                    "documents_text": {"type": "string"},
                },
                "required": ["supplier_name", "profile", "documents_text"],
                "additionalProperties": False,
            },
        },
        "summaries": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "supplier_name": {"type": "string"},
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
                "required": ["supplier_name", "verdict", "checks", "issues"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["requirements", "suppliers", "summaries"],
    "additionalProperties": False,
}


def pdf_as_input_file_part(pdf_path: Path) -> dict:
    b64 = base64.b64encode(pdf_path.read_bytes()).decode("utf-8")
    return {
        "type": "input_file",
        "filename": pdf_path.name,
        "file_data": f"data:application/pdf;base64,{b64}",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Path to appendix PDF (local)")
    ap.add_argument("--out", default=r".\outputs\llm_end2end", help="Output directory")
    ap.add_argument("--model", default="gpt-5.2")
    ap.add_argument("--effort", default="low", choices=["none", "low", "medium", "high", "xhigh"])
    args = ap.parse_args()

    pdf_path = Path(args.pdf)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    instructions = (
        "Ты — аудитор госзакупок. Работаешь строго по содержимому PDF (приложение/квалификационные требования).\n\n"
        "Задача (в ОДНОМ ответе JSON):\n"
        "A) Extract Requirements:\n"
        "   - Извлеки ВСЕ роли из раздела 'Трудовые ресурсы/Еңбек ресурстары' (любой список ролей: охранник, сантехник, тракторист и т.д.).\n"
        "   - Для каждой роли: role, count.\n"
        "   - required_documents: если в PDF прямо указаны подтверждающие документы/аттестаты/реестры/удостоверения — перечисли. Если не указано — [].\n"
        "   - notes: любые оговорки (про стаж, ограничения, требования к подтверждению).\n"
        "   - evidence: 1–3 коротких фрагмента из PDF, где видно роль и количество/условия.\n"
        "   - Не добавляй ролей, которых нет в PDF.\n\n"
        "B) Generate Suppliers (synthetic text, not real PDFs):\n"
        "   - Сгенерируй ровно 2 поставщика:\n"
        "     1) profile=FULL: должен ПОЛНОСТЬЮ закрыть требования по всем ролям.\n"
        "     2) profile=MINOR_MISSING: должен НЕ закрыть ровно ОДИН элемент (например: -1 человек по одной роли ИЛИ отсутствует один обязательный документ).\n"
        "   - documents_text: деловой текстовый список документов (как в примере): удостоверение личности, диплом/сертификат, трудовой договор, пенсионные, аттестат ИТР и т.п.\n"
        "   - Это синтетические данные. Считай documents_text единственным источником: если роль/документ не упомянуты — их нет.\n"
        "   - Избегай дубликатов: одно ФИО = одно удостоверение личности. Не используй одно и то же ФИО в разных ролях.\n\n"
        "C) Summaries:\n"
        "   - Для каждого поставщика выдай summary:\n"
        "     checks[] ОБЯЗАН включать все роли из requirements.labor_roles.\n"
        "     status:\n"
        "       OK — роль закрыта по количеству (и по required_documents если они есть).\n"
        "       FAIL — не хватает людей или отсутствует обязательный документ.\n"
        "       UNKNOWN — если в данных реально нет информации (но старайся избегать UNKNOWN).\n"
        "     issues[] — только для FAIL.\n"
        "   - verdict:\n"
        "     PASS только если ВСЕ checks.status == OK\n"
        "     FAIL если есть хотя бы один checks.status == FAIL\n"
        "   - Никаких рекомендаций. Никаких severity.\n"
        "   - Никаких выдумок: если документ/роль/подтверждение не найдено — так и пиши.\n"
    )

    client = OpenAI()

    resp = client.responses.create(
        model=args.model,
        reasoning={"effort": args.effort},
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": instructions},
                    pdf_as_input_file_part(pdf_path),
                ],
            }
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "tender_end2end",
                "schema": SCHEMA,
                "strict": True,
            }
        },
    )

    data = json.loads(resp.output_text)

    # hard-enforce verdict logic (processual rule)
    for s in data["summaries"]:
        if any(c["status"] == "FAIL" for c in s["checks"]):
            s["verdict"] = "FAIL"
        elif all(c["status"] == "OK" for c in s["checks"]):
            s["verdict"] = "PASS"
        else:
            s["verdict"] = "FAIL"

    # write combined JSON
    combined_path = out_dir / f"{pdf_path.stem}.end2end.json"
    combined_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # also write supplier texts as separate files (handy for UI/demo)
    suppliers_dir = out_dir / f"{pdf_path.stem}.suppliers"
    suppliers_dir.mkdir(parents=True, exist_ok=True)
    for sup in data["suppliers"]:
        safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in sup["supplier_name"])
        (suppliers_dir / f"{safe}.{sup['profile']}.txt").write_text(
            sup["documents_text"], encoding="utf-8"
        )

    print(f"[OK] Wrote: {combined_path}")
    print(f"[OK] Supplier texts: {suppliers_dir}")


if __name__ == "__main__":
    main()