#!/usr/bin/env python3
"""
Запуск FeatureEngine на данных из БД (по умолчанию первые 2000 лотов).
Использование:
  docker compose exec backend python scripts/run_feature_recompute.py
  docker compose exec backend python scripts/run_feature_recompute.py --limit 5000
  docker compose exec backend python scripts/run_feature_recompute.py --all
"""
import asyncio
import argparse
import logging
import os
import sys

# отключаем подробный SQL-лог для скорости
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
os.environ.setdefault("LOG_LEVEL", "INFO")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000, help="Макс. число лотов (0 = без лимита)")
    parser.add_argument("--all", action="store_true", help="Обработать все лоты (до 50000)")
    args = parser.parse_args()

    from app.features.engine import FeatureEngine
    from app.core.database import AsyncSessionLocal
    from sqlalchemy import select
    from app.models.procurement import Lot

    if args.all:
        lot_ids = None  # engine.run() возьмёт до 50000
        print("Запуск пересчёта по всем лотам (до 50000)...")
    else:
        async with AsyncSessionLocal() as db:
            r = await db.execute(
                select(Lot.id).where(Lot.is_deleted == False).limit(args.limit)
            )
            lot_ids = [row[0] for row in r.all()]
        if not lot_ids:
            print("В БД нет лотов. Сначала выполните ETL.")
            return
        print(f"Запуск пересчёта по {len(lot_ids)} лотам...")

    engine = FeatureEngine()
    summary = await engine.run(entity_ids=lot_ids)
    print("Готово:", summary)


if __name__ == "__main__":
    asyncio.run(main())
