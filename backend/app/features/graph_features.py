"""
Graph-based features: co-bidding, bid rotation.
Computed from TrdApp and Contract; stored in graph_features table.
"""
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select, func, and_, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.procurement import TrdApp, Contract, Lot, GraphFeature

logger = logging.getLogger(__name__)

VERSION = "v1"
COBID_MIN_TOGETHER = 2  # min times two suppliers appear together to count as "frequent"


async def compute_graph_features(batch_size: int = 2000) -> dict:
    """
    Compute graph features for lots: co-bidding and rotation metrics.
    Idempotent: upserts graph_features by (entity_type, entity_id).
    """
    # 1) Per supplier_biin: total_tenders_participated, unique_customers_count, frequent_cobid_partners_count
    # 2) Per trd_buy_id (tender): unique_bidders, max_pair_cobid_history (we'll simplify: max co-occurrence count)
    # 3) Per customer_bin: top_supplier_win_share, win_rotation_index (simplified)

    async with AsyncSessionLocal() as db:
        # Suppliers: count participations and unique customers (from contracts they won)
        supplier_stats = await db.execute(
            text("""
            SELECT
                c.supplier_biin AS entity_id,
                COUNT(DISTINCT c.trd_buy_id) AS total_tenders_participated,
                COUNT(DISTINCT c.customer_bin) AS unique_customers_count
            FROM contract c
            WHERE c.supplier_biin IS NOT NULL AND c.is_deleted = FALSE
            GROUP BY c.supplier_biin
            """)
        )
        supplier_rows = supplier_stats.fetchall()

        # Frequent cobid partners: count pairs (supplier_a, supplier_b) that co-appeared in same buy_id >= COBID_MIN_TOGETHER
        cobid_pairs = await db.execute(
            text("""
            WITH bid_pairs AS (
                SELECT a.buy_id, a.supplier_biin AS biin1, b.supplier_biin AS biin2
                FROM trd_app a
                JOIN trd_app b ON a.buy_id = b.buy_id AND a.supplier_biin < b.supplier_biin
                WHERE a.supplier_biin IS NOT NULL AND b.supplier_biin IS NOT NULL
            ),
            pair_counts AS (
                SELECT biin1, biin2, COUNT(*) AS cnt
                FROM bid_pairs
                GROUP BY biin1, biin2
                HAVING COUNT(*) >= :min_together
            )
            SELECT biin1, biin2, cnt FROM pair_counts
            """),
            {"min_together": COBID_MIN_TOGETHER},
        )
        pair_list = cobid_pairs.fetchall()
        # Count per supplier how many partners they have
        supplier_partners = {}
        for row in pair_list:
            b1, b2, cnt = row[0], row[1], row[2]
            supplier_partners[b1] = supplier_partners.get(b1, 0) + 1
            supplier_partners[b2] = supplier_partners.get(b2, 0) + 1

        supplier_features = {}
        for row in supplier_rows:
            eid = row[0]
            supplier_features[eid] = {
                "total_tenders_participated": row[1] or 0,
                "unique_customers_count": row[2] or 0,
                "frequent_cobid_partners_count": supplier_partners.get(eid, 0),
            }

        # Per tender: unique_bidders
        tender_bidders = await db.execute(
            text("""
            SELECT buy_id, COUNT(DISTINCT supplier_biin) AS unique_bidders
            FROM trd_app
            WHERE buy_id IS NOT NULL AND supplier_biin IS NOT NULL
            GROUP BY buy_id
            """)
        )
        tender_features = {row[0]: {"unique_bidders": row[1]} for row in tender_bidders.fetchall()}

        # Per customer: top_supplier_win_share (concentration), win_rotation (simplified: count distinct winners)
        customer_stats = await db.execute(
            text("""
            SELECT
                c.customer_bin AS entity_id,
                COUNT(*) AS total_contracts,
                COUNT(DISTINCT c.supplier_biin) AS distinct_winners
            FROM contract c
            WHERE c.customer_bin IS NOT NULL AND c.is_deleted = FALSE
            GROUP BY c.customer_bin
            """)
        )
        customer_rows = customer_stats.fetchall()
        # Top supplier share: for each customer, get max count of wins by one supplier
        top_share = await db.execute(
            text("""
            SELECT customer_bin, supplier_biin, COUNT(*) AS wins
            FROM contract
            WHERE customer_bin IS NOT NULL AND supplier_biin IS NOT NULL AND is_deleted = FALSE
            GROUP BY customer_bin, supplier_biin
            """)
        )
        customer_max_wins = {}
        for row in top_share.fetchall():
            cust, supp, wins = row[0], row[1], row[2]
            if cust not in customer_max_wins:
                customer_max_wins[cust] = 0
            customer_max_wins[cust] = max(customer_max_wins[cust], wins)
        customer_total = {row[0]: (row[1], row[2]) for row in customer_rows}
        customer_features = {}
        for eid, (total, distinct_w) in customer_total.items():
            max_w = customer_max_wins.get(eid, 0)
            share = (max_w / total * 100.0) if total else 0.0
            customer_features[eid] = {
                "top_supplier_win_share": round(share, 2),
                "win_rotation_index": distinct_w,  # more winners = more rotation
            }

    # Build per-lot graph features
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Lot.id, Lot.trd_buy_id, Lot.customer_bin).where(Lot.is_deleted == False).limit(batch_size)
        )
        lots = result.all()

        # Lot -> supplier_biin
        lot_supplier = {}
        for lot_id, trd_buy_id, _ in lots:
            if trd_buy_id:
                r = await db.execute(
                    select(Contract.supplier_biin).where(
                        and_(
                            Contract.trd_buy_id == trd_buy_id,
                            Contract.is_deleted == False,
                        )
                    ).limit(1)
                )
                row = r.first()
                if row:
                    lot_supplier[lot_id] = row[0]

    now = datetime.utcnow()
    to_upsert = []
    for lot_id, trd_buy_id, customer_bin in lots:
        features = {}
        supplier_biin = lot_supplier.get(lot_id)
        if supplier_biin and supplier_biin in supplier_features:
            features["supplier_total_tenders_participated"] = supplier_features[supplier_biin]["total_tenders_participated"]
            features["supplier_unique_customers_count"] = supplier_features[supplier_biin]["unique_customers_count"]
            features["supplier_frequent_cobid_partners_count"] = supplier_features[supplier_biin]["frequent_cobid_partners_count"]
        else:
            features["supplier_total_tenders_participated"] = 0
            features["supplier_unique_customers_count"] = 0
            features["supplier_frequent_cobid_partners_count"] = 0

        if trd_buy_id and trd_buy_id in tender_features:
            features["tender_unique_bidders"] = tender_features[trd_buy_id]["unique_bidders"]
        else:
            features["tender_unique_bidders"] = 0

        if customer_bin and customer_bin in customer_features:
            features["customer_top_supplier_win_share"] = customer_features[customer_bin]["top_supplier_win_share"]
            features["customer_win_rotation_index"] = customer_features[customer_bin]["win_rotation_index"]
        else:
            features["customer_top_supplier_win_share"] = 0.0
            features["customer_win_rotation_index"] = 0

        to_upsert.append({
            "entity_type": "lot",
            "entity_id": str(lot_id),
            "features_jsonb": features,
            "computed_at": now,
            "version": VERSION,
        })

    for rec in to_upsert:
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(GraphFeature).values(rec)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_graph_features_entity",
                set_={
                    "features_jsonb": stmt.excluded.features_jsonb,
                    "computed_at": stmt.excluded.computed_at,
                    "version": stmt.excluded.version,
                },
            )
            await db.execute(stmt)
            await db.commit()

    return {"lots_updated": len(to_upsert), "version": VERSION}


async def get_graph_features_for_lot(lot_id: int) -> dict | None:
    """Return graph features dict for lot or None."""
    from sqlalchemy import and_
    async with AsyncSessionLocal() as db:
        r = await db.execute(
            select(GraphFeature.features_jsonb).where(
                and_(
                    GraphFeature.entity_type == "lot",
                    GraphFeature.entity_id == str(lot_id),
                )
            )
        )
        row = r.first()
    return row[0] if row else None
