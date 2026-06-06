"""Price Radar: add median_count to price_benchmark (count-plausibility guard).

Revision ID: 0006_price_radar_count
Revises: 0005_price_radar
Create Date: 2026-06-07

"""
from alembic import op
import sqlalchemy as sa

revision = "0006_price_radar_count"
down_revision = "0005_price_radar"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("price_benchmark")}
    if "median_count" not in cols:
        op.add_column("price_benchmark", sa.Column("median_count", sa.Float(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("price_benchmark")}
    if "median_count" in cols:
        op.drop_column("price_benchmark", "median_count")
