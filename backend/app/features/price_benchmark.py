"""
Price Radar — per-product unit-price benchmarks + overpricing detection.

Methodology (OCP "Analyzing unit prices" / World Bank price benchmarking,
as used by Datanomix in Kazakhstan): group competitive purchases of the same
product (ENSTRU code + unit of measure), build the unit-price distribution,
and flag purchases whose unit price exceeds the upper IQR fence
(Q3 + 1.5 * IQR) — a clear price anomaly.

Single-source procurements are excluded from the benchmark (their prices are
not competitively set and would bias the reference).

Two guards suppress data-quality artifacts (goszakup unit-entry inconsistencies)
from being reported as real overpricing:
  • count plausibility — a lot whose quantity is far below the group norm
    (count < median_count * MIN_COUNT_RATIO) has an unreliable unit price
    (e.g. a bulk order entered as "1"); excluded.
  • ratio ceiling — ratios above MAX_RATIO are treated as data anomalies,
    not trusted overpricing.
Such lots are still recorded (evidence.data_quality_suspect = true) but
flag_bool stays false so they don't pollute the overpriced list.
"""
import logging
from datetime import datetime

from sqlalchemy import text

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# Tunables (mirror weights.yaml indicator_thresholds.OVERPRICED_UNIT)
DEFAULT_MIN_SAMPLES = 5      # min competitive lots per product before we trust a benchmark
DEFAULT_MIN_COUNT_RATIO = 0.1  # lot.count must be >= this * group median count
DEFAULT_MAX_RATIO = 50.0    # ratios above this are data anomalies, not trusted overpricing

_BUILD_BENCHMARK_SQL = text("""
WITH stats AS (
    SELECT
        enstru_code,
        unit_code,
        max(enstru_name) AS enstru_name,
        count(*)         AS n,
        percentile_cont(0.5)  WITHIN GROUP (ORDER BY unit_price) AS median,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY unit_price) AS q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY unit_price) AS q3,
        percentile_cont(0.5)  WITHIN GROUP (ORDER BY count)      AS median_count,
        min(unit_price)  AS min_p,
        max(unit_price)  AS max_p
    FROM lots
    WHERE is_deleted = false
      AND unit_price IS NOT NULL AND unit_price > 0
      AND enstru_code IS NOT NULL
      AND unit_code IS NOT NULL
      AND COALESCE(singl_org_sign, 0) = 0          -- competitive only
    GROUP BY enstru_code, unit_code
    HAVING count(*) >= :min_samples
)
INSERT INTO price_benchmark
    (enstru_code, unit_code, enstru_name, n_samples, median_price,
     q1, q3, iqr, upper_fence, min_price, max_price, median_count, updated_at)
SELECT
    enstru_code, unit_code, enstru_name, n, median,
    q1, q3, (q3 - q1), (q3 + 1.5 * (q3 - q1)), min_p, max_p, median_count, :now
FROM stats
ON CONFLICT (enstru_code, unit_code) DO UPDATE SET
    enstru_name  = EXCLUDED.enstru_name,
    n_samples    = EXCLUDED.n_samples,
    median_price = EXCLUDED.median_price,
    q1           = EXCLUDED.q1,
    q3           = EXCLUDED.q3,
    iqr          = EXCLUDED.iqr,
    upper_fence  = EXCLUDED.upper_fence,
    min_price    = EXCLUDED.min_price,
    max_price    = EXCLUDED.max_price,
    median_count = EXCLUDED.median_count,
    updated_at   = EXCLUDED.updated_at
""")

# Write/refresh OVERPRICED_UNIT risk flags for every lot that has a benchmark.
# flag_bool = real overpricing only (passes both data-quality guards).
_WRITE_FLAGS_SQL = text("""
WITH joined AS (
    SELECT
        l.id, l.unit_price, l.count,
        b.median_price, b.q1, b.q3, b.upper_fence, b.n_samples, b.median_count,
        b.enstru_code, b.enstru_name, b.unit_code,
        (l.unit_price / NULLIF(b.median_price, 0)) AS ratio,
        (l.count / NULLIF(b.median_count, 0))      AS count_ratio
    FROM lots l
    JOIN price_benchmark b
      ON l.enstru_code = b.enstru_code AND l.unit_code = b.unit_code
    WHERE l.is_deleted = false
      AND l.unit_price IS NOT NULL AND l.unit_price > 0
),
scored AS (
    SELECT *,
        (count_ratio IS NOT NULL AND count_ratio < :min_count_ratio) AS count_suspect,
        (ratio IS NOT NULL AND ratio > :max_ratio)                   AS ratio_suspect
    FROM joined
)
INSERT INTO risk_flags
    (entity_type, entity_id, indicator_code, flag_bool, value_numeric, evidence_jsonb, computed_at)
SELECT
    'lot', id::text, 'OVERPRICED_UNIT',
    (unit_price > upper_fence AND NOT count_suspect AND NOT ratio_suspect),
    CASE WHEN median_price > 0 THEN round(ratio::numeric, 3) ELSE NULL END,
    jsonb_build_object(
        'unit_price',          round(unit_price::numeric, 2),
        'median_market',       round(median_price::numeric, 2),
        'q1',                  round(q1::numeric, 2),
        'q3',                  round(q3::numeric, 2),
        'upper_fence',         round(upper_fence::numeric, 2),
        'ratio_to_median',     CASE WHEN median_price > 0 THEN round(ratio::numeric, 2) ELSE NULL END,
        'overpay_estimate',    CASE WHEN unit_price > median_price AND count IS NOT NULL AND NOT count_suspect AND NOT ratio_suspect
                                    THEN round(((unit_price - median_price) * count)::numeric, 2) ELSE 0 END,
        'sample_size',         n_samples,
        'group_median_count',  round(median_count::numeric, 3),
        'lot_count',           count,
        'data_quality_suspect', (count_suspect OR ratio_suspect),
        'suspect_reason',      CASE
                                  WHEN count_suspect AND ratio_suspect THEN 'count_and_ratio'
                                  WHEN count_suspect THEN 'count_below_norm'
                                  WHEN ratio_suspect THEN 'ratio_too_high'
                                  ELSE NULL END,
        'enstru_code',         enstru_code,
        'enstru_name',         enstru_name,
        'unit_code',           unit_code
    ),
    :now
FROM scored
ON CONFLICT (entity_type, entity_id, indicator_code) DO UPDATE SET
    flag_bool      = EXCLUDED.flag_bool,
    value_numeric  = EXCLUDED.value_numeric,
    evidence_jsonb = EXCLUDED.evidence_jsonb,
    computed_at    = EXCLUDED.computed_at
""")


async def build_benchmarks(min_samples: int = DEFAULT_MIN_SAMPLES) -> dict:
    """(Re)compute the price_benchmark table from enriched competitive lots."""
    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        await db.execute(_BUILD_BENCHMARK_SQL, {"min_samples": min_samples, "now": now})
        await db.commit()
        n = (await db.execute(text("SELECT count(*) FROM price_benchmark"))).scalar()
    logger.info("Price benchmarks built: %s product groups (min_samples=%d)", n, min_samples)
    return {"benchmarks": int(n or 0), "min_samples": min_samples}


async def compute_overpricing_flags(
    min_count_ratio: float = DEFAULT_MIN_COUNT_RATIO,
    max_ratio: float = DEFAULT_MAX_RATIO,
) -> dict:
    """Write/refresh OVERPRICED_UNIT flags for all lots that have a benchmark."""
    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        await db.execute(_WRITE_FLAGS_SQL, {
            "now": now, "min_count_ratio": min_count_ratio, "max_ratio": max_ratio,
        })
        await db.commit()
        flagged = (await db.execute(text(
            "SELECT count(*) FROM risk_flags WHERE indicator_code='OVERPRICED_UNIT' AND flag_bool"
        ))).scalar()
        total = (await db.execute(text(
            "SELECT count(*) FROM risk_flags WHERE indicator_code='OVERPRICED_UNIT'"
        ))).scalar()
    logger.info("OVERPRICED_UNIT: %s overpriced of %s evaluated lots", flagged, total)
    return {"overpriced": int(flagged or 0), "evaluated": int(total or 0)}


async def rebuild_price_radar(
    min_samples: int = DEFAULT_MIN_SAMPLES,
    min_count_ratio: float = DEFAULT_MIN_COUNT_RATIO,
    max_ratio: float = DEFAULT_MAX_RATIO,
) -> dict:
    """Full Price Radar refresh: benchmarks + flags (assumes lots already enriched)."""
    b = await build_benchmarks(min_samples)
    f = await compute_overpricing_flags(min_count_ratio, max_ratio)
    return {**b, **f}
