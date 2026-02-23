"""
Smoke test: verify that ETL + scoring pipeline works end-to-end.
Run inside the backend container:
    python smoke_test.py

Or from host:
    docker compose exec backend python smoke_test.py
"""
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("smoke_test")


async def main():
    from sqlalchemy import select, func
    from app.core.database import AsyncSessionLocal, dispose_engine
    from app.models.procurement import (
        Lot, TrdBuy, Contract, Subject, Rnu, TrdApp,
        ContractAct, TreasuryPay, ContractPayment, ContractSpecSum,
        RiskScore, RiskFlag, TenderFeature,
    )

    async with AsyncSessionLocal() as db:
        # 1. Check data tables
        tables = {
            "trd_buy": TrdBuy,
            "lots": Lot,
            "subject": Subject,
            "contract": Contract,
            "trd_app": TrdApp,
            "rnu": Rnu,
            "contract_act": ContractAct,
            "treasury_pay": TreasuryPay,
            "contract_payment": ContractPayment,
            "contract_spec_sum": ContractSpecSum,
        }
        print("\n=== DATA TABLES ===")
        for name, model in tables.items():
            count = (await db.execute(select(func.count(model.id)))).scalar() or 0
            status = "OK" if count > 0 else "EMPTY"
            print(f"  {name:25s} {count:>8,d}  [{status}]")

        # 2. Check scoring tables
        print("\n=== SCORING TABLES ===")
        scored = (await db.execute(
            select(func.count(RiskScore.id)).where(RiskScore.entity_type == "lot")
        )).scalar() or 0
        flags = (await db.execute(
            select(func.count(RiskFlag.id)).where(RiskFlag.entity_type == "lot")
        )).scalar() or 0
        triggered = (await db.execute(
            select(func.count(RiskFlag.id)).where(RiskFlag.entity_type == "lot", RiskFlag.flag_bool == True)
        )).scalar() or 0
        features = (await db.execute(
            select(func.count(TenderFeature.id)).where(TenderFeature.entity_type == "lot")
        )).scalar() or 0

        total_lots = (await db.execute(
            select(func.count(Lot.id)).where(Lot.is_deleted == False)
        )).scalar() or 0

        print(f"  {'risk_scores':25s} {scored:>8,d}  [{'OK' if scored > 0 else 'EMPTY - run feature recompute!'}]")
        print(f"  {'risk_flags':25s} {flags:>8,d}  [{'OK' if flags > 0 else 'EMPTY - run feature recompute!'}]")
        print(f"  {'flags_triggered':25s} {triggered:>8,d}")
        print(f"  {'tender_features':25s} {features:>8,d}")

        # 3. Risk distribution
        print("\n=== RISK DISTRIBUTION ===")
        dist_result = await db.execute(
            select(RiskScore.level, func.count(RiskScore.id))
            .where(RiskScore.entity_type == "lot")
            .group_by(RiskScore.level)
        )
        for level, count in dist_result.all():
            print(f"  {level:10s} {count:>8,d}")

        avg_result = await db.execute(
            select(func.avg(RiskScore.score_final)).where(RiskScore.entity_type == "lot")
        )
        avg = avg_result.scalar()
        print(f"  {'Avg score':10s} {float(avg or 0):>8.1f}")

        # 4. Sample scored lot
        if scored > 0:
            print("\n=== SAMPLE SCORED LOT ===")
            sample = await db.execute(
                select(RiskScore).where(RiskScore.entity_type == "lot")
                .order_by(RiskScore.score_final.desc()).limit(1)
            )
            s = sample.scalar_one_or_none()
            if s:
                print(f"  Lot ID:      {s.entity_id}")
                print(f"  Score Rules: {s.score_rules}")
                print(f"  Score ML:    {s.score_ml}")
                print(f"  Score Final: {s.score_final}")
                print(f"  Level:       {s.level}")
                print(f"  Top Reasons: {len(s.top_reasons_jsonb or [])} items")
                for reason in (s.top_reasons_jsonb or [])[:3]:
                    print(f"    - {reason.get('code')}: weight={reason.get('weight')}")

        # 5. If no scores exist, try scoring one lot
        if scored == 0 and total_lots > 0:
            print("\n=== SCORING ONE LOT (on-demand test) ===")
            lot_result = await db.execute(select(Lot.id).where(Lot.is_deleted == False).limit(1))
            lot_id = lot_result.scalar()
            if lot_id:
                print(f"  Scoring lot {lot_id}...")
                from app.features.engine import FeatureEngine
                engine = FeatureEngine()
                result = await engine.compute_lot_score(lot_id)
                print(f"  Result: {result}")
                if result:
                    print(f"  Score: {result.get('score_final')}, Level: {result.get('level')}, Flags: {result.get('flags_triggered')}")
                    print("  SUCCESS: Scoring pipeline works!")
                else:
                    print("  WARNING: Lot not found or scoring returned empty")

    await dispose_engine()

    # Final verdict
    print("\n=== VERDICT ===")
    if scored > 0:
        print(f"  PASS: {scored} lots scored, {triggered} flags triggered")
    elif total_lots > 0:
        print("  WARN: Lots exist but no scores. Run: docker compose exec backend python -c \"from app.etl.tasks import run_feature_recompute; run_feature_recompute()\"")
    else:
        print("  WARN: No lots loaded. Run backfill first.")


if __name__ == "__main__":
    asyncio.run(main())
