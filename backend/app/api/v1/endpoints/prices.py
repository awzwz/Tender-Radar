"""
Price Radar API — overpricing detection based on per-product unit-price
benchmarks (ENSTRU + unit of measure).
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db
from app.core.security import require_viewer

router = APIRouter()


@router.get("/stats")
async def price_stats(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Headline numbers for the Price Radar dashboard."""
    row = (await db.execute(text("""
        SELECT
          (SELECT count(*) FROM price_benchmark) AS products,
          (SELECT count(*) FROM risk_flags WHERE indicator_code='OVERPRICED_UNIT') AS evaluated,
          (SELECT count(*) FROM risk_flags WHERE indicator_code='OVERPRICED_UNIT' AND flag_bool) AS overpriced,
          (SELECT COALESCE(sum((evidence_jsonb->>'overpay_estimate')::numeric),0)
             FROM risk_flags WHERE indicator_code='OVERPRICED_UNIT' AND flag_bool) AS total_overpay
    """))).first()
    return {
        "products_benchmarked": int(row.products or 0),
        "lots_evaluated": int(row.evaluated or 0),
        "overpriced_lots": int(row.overpriced or 0),
        "total_overpay_estimate": float(row.total_overpay or 0),
    }


@router.get("/overpriced")
async def overpriced_lots(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    min_ratio: float = Query(1.0, ge=0),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Lots flagged as overpriced, sorted by how far above the market they are."""
    offset = (page - 1) * limit
    where = ["f.indicator_code='OVERPRICED_UNIT'", "f.flag_bool = true", "f.value_numeric >= :min_ratio"]
    params = {"min_ratio": min_ratio, "limit": limit, "offset": offset}
    if search:
        where.append("(l.name_ru ILIKE :q OR l.customer_name ILIKE :q OR l.enstru_name ILIKE :q)")
        params["q"] = f"%{search}%"
    where_sql = " AND ".join(where)

    total = (await db.execute(text(
        f"""SELECT count(*) FROM risk_flags f JOIN lots l ON l.id = f.entity_id::bigint
            WHERE {where_sql}"""), params)).scalar() or 0

    rows = (await db.execute(text(f"""
        SELECT
            l.id, l.name_ru, l.enstru_name, l.enstru_code, l.unit_code,
            l.customer_bin, l.customer_name, l.trd_buy_id, l.amount, l.count,
            f.value_numeric AS ratio,
            (f.evidence_jsonb->>'unit_price')::numeric       AS unit_price,
            (f.evidence_jsonb->>'median_market')::numeric    AS median_market,
            (f.evidence_jsonb->>'upper_fence')::numeric      AS upper_fence,
            (f.evidence_jsonb->>'overpay_estimate')::numeric AS overpay_estimate,
            (f.evidence_jsonb->>'sample_size')::int          AS sample_size
        FROM risk_flags f
        JOIN lots l ON l.id = f.entity_id::bigint
        WHERE {where_sql}
        ORDER BY f.value_numeric DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """), params)).mappings().all()

    return {
        "total": int(total),
        "page": page,
        "limit": limit,
        "items": [dict(r) for r in rows],
    }


@router.get("/lot/{lot_id}")
async def lot_price_comparison(
    lot_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Unit-price comparison for one lot against its product benchmark."""
    row = (await db.execute(text("""
        SELECT
            l.id, l.name_ru, l.enstru_code, l.enstru_name, l.unit_code,
            l.amount, l.count, l.unit_price,
            b.median_price, b.q1, b.q3, b.upper_fence, b.min_price, b.max_price, b.n_samples,
            f.flag_bool, f.value_numeric AS ratio,
            (f.evidence_jsonb->>'overpay_estimate')::numeric AS overpay_estimate
        FROM lots l
        LEFT JOIN price_benchmark b
               ON l.enstru_code = b.enstru_code AND l.unit_code = b.unit_code
        LEFT JOIN risk_flags f
               ON f.entity_type='lot' AND f.entity_id = l.id::text
              AND f.indicator_code='OVERPRICED_UNIT'
        WHERE l.id = :lot_id
    """), {"lot_id": lot_id})).mappings().first()
    if not row:
        return {"found": False}
    d = dict(row)
    d["found"] = True
    d["has_benchmark"] = row["median_price"] is not None
    return d


@router.get("/product/{enstru_code}")
async def product_benchmark(
    enstru_code: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Benchmark + the lots that make up the distribution for one product."""
    bench = (await db.execute(text("""
        SELECT enstru_code, unit_code, enstru_name, n_samples,
               median_price, q1, q3, iqr, upper_fence, min_price, max_price, updated_at
        FROM price_benchmark WHERE enstru_code = :code
    """), {"code": enstru_code})).mappings().all()

    lots = (await db.execute(text("""
        SELECT l.id, l.name_ru, l.customer_name, l.unit_price, l.count, l.amount,
               l.unit_code, COALESCE(f.flag_bool, false) AS overpriced
        FROM lots l
        LEFT JOIN risk_flags f
               ON f.entity_type='lot' AND f.entity_id = l.id::text
              AND f.indicator_code='OVERPRICED_UNIT'
        WHERE l.enstru_code = :code AND l.unit_price IS NOT NULL AND l.unit_price > 0
        ORDER BY l.unit_price DESC
        LIMIT 200
    """), {"code": enstru_code})).mappings().all()

    return {
        "enstru_code": enstru_code,
        "benchmarks": [dict(b) for b in bench],
        "lots": [dict(l) for l in lots],
    }
