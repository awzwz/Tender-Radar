"""Hybrid AI pipeline: new tables, unique constraints, extended columns.

Revision ID: 0003_hybrid_pipeline
Revises: 702debef938d
Create Date: 2025-02-22 22:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers
revision = "0003_hybrid_pipeline"
down_revision = "702debef938d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── TrdBuy: add new columns ──────────────────────────────────────────────
    op.add_column("trd_buy", sa.Column("count_lots", sa.Integer(), nullable=True))
    op.add_column("trd_buy", sa.Column("parent_id", sa.BigInteger(), nullable=True))
    op.add_column("trd_buy", sa.Column("repeat_start_date", sa.DateTime(), nullable=True))
    op.add_column("trd_buy", sa.Column("repeat_end_date", sa.DateTime(), nullable=True))
    op.add_column("trd_buy", sa.Column("discus_start_date", sa.DateTime(), nullable=True))
    op.add_column("trd_buy", sa.Column("discus_end_date", sa.DateTime(), nullable=True))
    op.add_column("trd_buy", sa.Column("last_update_date", sa.DateTime(), nullable=True))
    op.create_index("ix_trd_buy_parent_id", "trd_buy", ["parent_id"])

    # ── Contract: add bank detail columns ────────────────────────────────────
    op.add_column("contract", sa.Column("supplier_iik", sa.String(34), nullable=True))
    op.add_column("contract", sa.Column("supplier_bik", sa.String(20), nullable=True))

    # ── TreasuryPay: add prepay and bank columns ─────────────────────────────
    op.add_column("treasury_pay", sa.Column("prepay_sum", sa.Numeric(20, 2), nullable=True))
    op.add_column("treasury_pay", sa.Column("iik_supplier", sa.String(34), nullable=True))
    op.add_column("treasury_pay", sa.Column("bik_supplier", sa.String(20), nullable=True))
    op.add_column("treasury_pay", sa.Column("vendor_id", sa.BigInteger(), nullable=True))

    # ── RiskScore: add score breakdown columns ───────────────────────────────
    op.add_column("risk_scores", sa.Column("score_rules", sa.Float(), nullable=True))
    op.add_column("risk_scores", sa.Column("score_ml", sa.Float(), nullable=True))
    op.add_column("risk_scores", sa.Column("score_final", sa.Float(), nullable=True))

    # ── UNIQUE constraints on risk_scores and risk_flags ─────────────────────
    # First drop existing non-unique indexes if they conflict, then add unique
    op.create_unique_constraint(
        "uq_risk_scores_entity", "risk_scores", ["entity_type", "entity_id"]
    )
    op.create_unique_constraint(
        "uq_risk_flags_entity_indicator", "risk_flags",
        ["entity_type", "entity_id", "indicator_code"],
    )

    # ── New table: contract_act ───────────────────────────────────────────────
    op.create_table(
        "contract_act",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("contract_id", sa.BigInteger(), nullable=False, index=True),
        sa.Column("act_number", sa.String(100)),
        sa.Column("act_date", sa.DateTime()),
        sa.Column("sum_act", sa.Numeric(20, 2)),
        sa.Column("sum_fine", sa.Numeric(20, 2)),
        sa.Column("day_overdue", sa.Integer(), default=0),
        sa.Column("ref_act_status_id", sa.Integer()),
        sa.Column("system_id", sa.Integer()),
        sa.Column("last_update_at", sa.DateTime()),
    )

    # ── New table: contract_payment ───────────────────────────────────────────
    op.create_table(
        "contract_payment",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("contract_id", sa.BigInteger(), nullable=False, index=True),
        sa.Column("act_id", sa.BigInteger()),
        sa.Column("pay_sum", sa.Numeric(20, 2)),
        sa.Column("pay_date", sa.DateTime()),
        sa.Column("ref_payment_type_id", sa.Integer()),
        sa.Column("system_id", sa.Integer()),
        sa.Column("last_update_at", sa.DateTime()),
    )

    # ── New table: contract_spec_sum ──────────────────────────────────────────
    op.create_table(
        "contract_spec_sum",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("contract_id", sa.BigInteger(), nullable=False, index=True),
        sa.Column("total_sum", sa.Numeric(20, 2)),
        sa.Column("fact_sum", sa.Numeric(20, 2)),
        sa.Column("system_id", sa.Integer()),
        sa.Column("last_update_at", sa.DateTime()),
    )

    # ── New table: tender_features ────────────────────────────────────────────
    op.create_table(
        "tender_features",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(20), nullable=False, server_default="lot"),
        sa.Column("entity_id", sa.String(255), nullable=False),
        sa.Column("features_jsonb", JSONB, nullable=False),
        sa.Column("label", sa.Integer()),
        sa.Column("label_source", sa.String(50)),
        sa.Column("computed_at", sa.DateTime()),
        sa.UniqueConstraint("entity_type", "entity_id", name="uq_tender_features_entity"),
    )
    op.create_index("ix_tender_features_entity", "tender_features", ["entity_type", "entity_id"])

    # ── New table: llm_explanations ───────────────────────────────────────────
    op.create_table(
        "llm_explanations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("entity_id", sa.String(255), nullable=False),
        sa.Column("risk_final", sa.Float()),
        sa.Column("risk_rules", sa.Float()),
        sa.Column("risk_ml", sa.Float()),
        sa.Column("explanation_text", sa.Text()),
        sa.Column("checklist_jsonb", JSONB),
        sa.Column("model_used", sa.String(50)),
        sa.Column("prompt_hash", sa.String(64)),
        sa.Column("created_at", sa.DateTime()),
        sa.UniqueConstraint("entity_type", "entity_id", name="uq_llm_explanations_entity"),
    )
    op.create_index("ix_llm_explanations_entity", "llm_explanations", ["entity_type", "entity_id"])


def downgrade() -> None:
    op.drop_table("llm_explanations")
    op.drop_table("tender_features")
    op.drop_table("contract_spec_sum")
    op.drop_table("contract_payment")
    op.drop_table("contract_act")

    op.drop_constraint("uq_risk_flags_entity_indicator", "risk_flags", type_="unique")
    op.drop_constraint("uq_risk_scores_entity", "risk_scores", type_="unique")

    op.drop_column("risk_scores", "score_final")
    op.drop_column("risk_scores", "score_ml")
    op.drop_column("risk_scores", "score_rules")

    op.drop_column("treasury_pay", "vendor_id")
    op.drop_column("treasury_pay", "bik_supplier")
    op.drop_column("treasury_pay", "iik_supplier")
    op.drop_column("treasury_pay", "prepay_sum")

    op.drop_column("contract", "supplier_bik")
    op.drop_column("contract", "supplier_iik")

    op.drop_index("ix_trd_buy_parent_id", "trd_buy")
    op.drop_column("trd_buy", "last_update_date")
    op.drop_column("trd_buy", "discus_end_date")
    op.drop_column("trd_buy", "discus_start_date")
    op.drop_column("trd_buy", "repeat_end_date")
    op.drop_column("trd_buy", "repeat_start_date")
    op.drop_column("trd_buy", "parent_id")
    op.drop_column("trd_buy", "count_lots")
