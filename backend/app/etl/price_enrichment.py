"""
Price enrichment for the Price Radar feature.

OWS exposes per-lot quantity (`count`) and the standardized product code
(ENSTRU) only through the nested `Lots.Plans` object — not on the lot itself.
This module backfills those fields onto our `lots` rows and derives
`unit_price = amount / count`, which is the basis for overpricing detection.
"""
import asyncio
import logging
from datetime import datetime

from sqlalchemy import select, update

from app.core.database import AsyncSessionLocal
from app.etl.client import OWSClient
from app.models.procurement import Lot

logger = logging.getLogger(__name__)

# Pull count + ENSTRU (via the plan the lot was created from)
GQL_LOTS_ENRICH = """
query($ids: [Int]) {
  Lots(limit: 100, filter: {id: $ids}) {
    id
    count
    amount
    Plans {
      refEnstruCode
      refEnstruId
      nameRu
      refUnitsCode
    }
  }
}
"""


def _pick_plan(plans: list | None) -> dict:
    """Return the first plan entry that carries an ENSTRU code."""
    for p in plans or []:
        if p.get("refEnstruCode"):
            return p
    return (plans or [{}])[0] if plans else {}


async def enrich_lots(lot_ids: list[int] | None = None, batch_size: int = 50) -> dict:
    """Backfill count/ENSTRU/unit_price for the given lots (or all not-yet-enriched).

    Returns a summary dict. Safe to re-run (idempotent upsert of columns).
    """
    client = OWSClient()
    summary = {"requested": 0, "matched": 0, "with_enstru": 0, "with_unit_price": 0, "batches": 0, "errors": 0}

    async with AsyncSessionLocal() as db:
        if lot_ids is None:
            res = await db.execute(
                select(Lot.id).where(Lot.is_deleted == False, Lot.price_enriched_at.is_(None))
            )
            lot_ids = [r[0] for r in res.all()]

    summary["requested"] = len(lot_ids)
    logger.info("Price enrichment starting for %d lots", len(lot_ids))

    for start in range(0, len(lot_ids), batch_size):
        batch = [int(x) for x in lot_ids[start:start + batch_size]]
        try:
            result = await client.graphql(GQL_LOTS_ENRICH, {"ids": batch})
        except Exception as e:  # noqa: BLE001
            logger.error("OWS enrich batch failed (start=%d): %s", start, e)
            summary["errors"] += 1
            continue

        rows = (result.get("data") or {}).get("Lots") or []
        now = datetime.utcnow()

        async with AsyncSessionLocal() as db:
            for r in rows:
                lot_id = r.get("id")
                if lot_id is None:
                    continue
                count = r.get("count")
                amount = r.get("amount")
                plan = _pick_plan(r.get("Plans"))
                enstru_code = plan.get("refEnstruCode")
                enstru_id = plan.get("refEnstruId")
                enstru_name = plan.get("nameRu")
                unit_code = plan.get("refUnitsCode")

                unit_price = None
                try:
                    if count and float(count) > 0 and amount is not None:
                        unit_price = float(amount) / float(count)
                except (TypeError, ValueError, ZeroDivisionError):
                    unit_price = None

                await db.execute(
                    update(Lot)
                    .where(Lot.id == lot_id)
                    .values(
                        count=count,
                        enstru_code=enstru_code,
                        enstru_id=enstru_id,
                        enstru_name=enstru_name,
                        unit_code=str(unit_code) if unit_code is not None else None,
                        unit_price=unit_price,
                        price_enriched_at=now,
                    )
                )
                summary["matched"] += 1
                if enstru_code:
                    summary["with_enstru"] += 1
                if unit_price is not None:
                    summary["with_unit_price"] += 1
            await db.commit()

        summary["batches"] += 1
        if summary["batches"] % 20 == 0:
            logger.info("Enriched %d/%d lots...", summary["matched"], len(lot_ids))

    logger.info("Price enrichment DONE: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(asyncio.run(enrich_lots()))
