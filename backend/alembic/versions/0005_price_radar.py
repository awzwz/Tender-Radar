"""Price Radar: per-unit price enrichment on lots + price_benchmark table.

Adds ENSTRU / quantity / unit-price columns to `lots` and a `price_benchmark`
table holding per-product (ENSTRU + unit) price distributions used by the
OVERPRICED_UNIT indicator.

Revision ID: 0005_price_radar
Revises: 0004_anomaly_weak_graph
Create Date: 2026-06-07

"""
from alembic import op
import sqlalchemy as sa

revision = "0005_price_radar"
down_revision = "0004_anomaly_weak_graph"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    lot_cols = {c["name"] for c in insp.get_columns("lots")}
    lot_indexes = {i["name"] for i in insp.get_indexes("lots")}

    # ── lots: price / ENSTRU enrichment columns ─────────────────────────────
    new_cols = [
        ("count", sa.Float()),
        ("enstru_code", sa.String(40)),
        ("enstru_id", sa.BigInteger()),
        ("enstru_name", sa.Text()),
        ("unit_code", sa.String(10)),
        ("unit_price", sa.Float()),
        ("price_enriched_at", sa.DateTime()),
    ]
    for name, col_type in new_cols:
        if name not in lot_cols:
            op.add_column("lots", sa.Column(name, col_type, nullable=True))

    if "ix_lots_enstru_code" not in lot_indexes:
        op.create_index("ix_lots_enstru_code", "lots", ["enstru_code"])

    # ── price_benchmark table ───────────────────────────────────────────────
    if "price_benchmark" not in set(insp.get_table_names()):
        op.create_table(
            "price_benchmark",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("enstru_code", sa.String(40), nullable=False),
            sa.Column("unit_code", sa.String(10), nullable=False),
            sa.Column("enstru_name", sa.Text()),
            sa.Column("n_samples", sa.Integer(), nullable=False),
            sa.Column("median_price", sa.Float(), nullable=False),
            sa.Column("q1", sa.Float(), nullable=False),
            sa.Column("q3", sa.Float(), nullable=False),
            sa.Column("iqr", sa.Float(), nullable=False),
            sa.Column("upper_fence", sa.Float(), nullable=False),
            sa.Column("min_price", sa.Float()),
            sa.Column("max_price", sa.Float()),
            sa.Column("updated_at", sa.DateTime()),
            sa.UniqueConstraint("enstru_code", "unit_code", name="uq_price_benchmark"),
        )
        op.create_index("ix_price_benchmark_enstru", "price_benchmark", ["enstru_code"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if "price_benchmark" in set(insp.get_table_names()):
        op.drop_index("ix_price_benchmark_enstru", table_name="price_benchmark")
        op.drop_table("price_benchmark")

    lot_indexes = {i["name"] for i in insp.get_indexes("lots")}
    if "ix_lots_enstru_code" in lot_indexes:
        op.drop_index("ix_lots_enstru_code", table_name="lots")

    lot_cols = {c["name"] for c in insp.get_columns("lots")}
    for name in ["price_enriched_at", "unit_price", "unit_code",
                 "enstru_name", "enstru_id", "enstru_code", "count"]:
        if name in lot_cols:
            op.drop_column("lots", name)
