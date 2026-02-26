"""
BidCheck API: Parse PDF specs and generate supplier candidates.
Public endpoints (no auth required) for demo/hackathon use.
"""
from typing import Any, Optional

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile

from app.services.bidcheck import parse_pdf, parse_and_generate_full, parse_and_analyze_compliance

router = APIRouter()


@router.post("/parse")
async def bidcheck_parse(
    file: UploadFile = File(..., description="PDF file (technical specification)"),
) -> dict[str, Any]:
    """
    Parse a PDF technical specification and extract structured requirements.
    Returns RequirementsDoc with labor_requirements, equipment_requirements, etc.
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    content = await file.read()
    if len(content) < 10:
        raise HTTPException(status_code=400, detail="File is empty or too small")
    try:
        result = await parse_pdf(content, file.filename or "document.pdf")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parse failed: {str(e)}")


@router.post("/parse-and-analyze-compliance")
async def bidcheck_parse_and_analyze_compliance(
    ts_file: UploadFile = File(..., description="PDF: техническая спецификация (приложение 7/8)"),
    supplier_file: UploadFile = File(..., description="PDF: документы поставщика"),
) -> dict[str, Any]:
    """
    Upload ТЗ + документ поставщика → анализ соответствия (PASS/FAIL по каждой роли).
    """
    for f, name in [(ts_file, "ТЗ"), (supplier_file, "документ поставщика")]:
        if not f.filename or not f.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"{name} должен быть PDF")
    ts_content = await ts_file.read()
    supplier_content = await supplier_file.read()
    if len(ts_content) < 10 or len(supplier_content) < 10:
        raise HTTPException(status_code=400, detail="Файлы пустые или слишком малы")
    result = await parse_and_analyze_compliance(
        ts_content, ts_file.filename or "ts.pdf",
        supplier_content, supplier_file.filename or "supplier.pdf",
    )
    if result is None:
        raise HTTPException(
            status_code=503,
            detail="Для анализа требуется OPENAI_API_KEY и достаточный объём текста в обоих PDF",
        )
    return result


@router.post("/parse-and-generate-full")
async def bidcheck_parse_and_generate_full(
    file: UploadFile = File(..., description="PDF (appendix 7/8 with qualification requirements)"),
) -> dict[str, Any]:
    """
    End2end (demo_bidcheck-3 style): Parse PDF -> requirements + suppliers with documents_text + summaries (PASS/FAIL).
    Returns rich schema: requirements.labor_roles, suppliers (with documents_text, profile FULL/MINOR_MISSING),
    summaries (verdict, checks, issues).
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    content = await file.read()
    if len(content) < 10:
        raise HTTPException(status_code=400, detail="File is empty or too small")
    result = await parse_and_generate_full(content, file.filename or "document.pdf")
    if result is None:
        raise HTTPException(
            status_code=503,
            detail="End2end requires OPENAI_API_KEY and sufficient extracted text from PDF",
        )
    return result
