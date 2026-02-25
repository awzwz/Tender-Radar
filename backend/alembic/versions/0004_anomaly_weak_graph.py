"""Anomaly, weak labels, graph features: risk_scores new cols, weak_labels, graph_features.

Revision ID: 0004_anomaly_weak_graph
Revises: 0003_hybrid_pipeline
Create Date: 2025-02-25

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0004_anomaly_weak_graph"
down_revision = "0003_hybrid_pipeline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_tables = set(insp.get_table_names())
    risk_cols = {c["name"] for c in insp.get_columns("risk_scores")}
    risk_indexes = {i["name"] for i in insp.get_indexes("risk_scores")}

    # ── risk_scores: anomaly, weak, composite columns ───────────────────────
    if "anomaly_score" not in risk_cols:
        op.add_column("risk_scores", sa.Column("anomaly_score", sa.Float(), nullable=True))
    if "anomaly_model_version" not in risk_cols:
        op.add_column("risk_scores", sa.Column("anomaly_model_version", sa.String(50), nullable=True))
    if "anomaly_explanation_jsonb" not in risk_cols:
        op.add_column("risk_scores", sa.Column("anomaly_explanation_jsonb", JSONB, nullable=True))
    if "weak_proba" not in risk_cols:
        op.add_column("risk_scores", sa.Column("weak_proba", sa.Float(), nullable=True))
    if "weak_score" not in risk_cols:
        op.add_column("risk_scores", sa.Column("weak_score", sa.Float(), nullable=True))
    if "weak_model_version" not in risk_cols:
        op.add_column("risk_scores", sa.Column("weak_model_version", sa.String(50), nullable=True))
    if "weak_explanation_jsonb" not in risk_cols:
        op.add_column("risk_scores", sa.Column("weak_explanation_jsonb", JSONB, nullable=True))
    if "composite_score" not in risk_cols:
        op.add_column("risk_scores", sa.Column("composite_score", sa.Float(), nullable=True))
    if "components_jsonb" not in risk_cols:
        op.add_column("risk_scores", sa.Column("components_jsonb", JSONB, nullable=True))
    if "ix_risk_scores_anomaly_model_version" not in risk_indexes:
        op.create_index("ix_risk_scores_anomaly_model_version", "risk_scores", ["anomaly_model_version"])
    if "ix_risk_scores_weak_model_version" not in risk_indexes:
        op.create_index("ix_risk_scores_weak_model_version", "risk_scores", ["weak_model_version"])

    # ── weak_labels table ────────────────────────────────────────────────────
    if "weak_labels" not in existing_tables:
        op.create_table(
            "weak_labels",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("entity_type", sa.String(20), nullable=False, server_default="lot"),
            sa.Column("entity_id", sa.String(255), nullable=False),
            sa.Column("weak_proba", sa.Float(), nullable=False),
            sa.Column("version", sa.String(80), nullable=False),
            sa.Column("lf_votes_jsonb", JSONB, nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
        )
    weak_indexes = {i["name"] for i in insp.get_indexes("weak_labels")} if "weak_labels" in set(insp.get_table_names()) else set()
    weak_uq = {u["name"] for u in insp.get_unique_constraints("weak_labels")} if "weak_labels" in set(insp.get_table_names()) else set()
    if "ix_weak_labels_entity" not in weak_indexes:
        op.create_index("ix_weak_labels_entity", "weak_labels", ["entity_type", "entity_id"])
    if "ix_weak_labels_version" not in weak_indexes:
        op.create_index("ix_weak_labels_version", "weak_labels", ["version"])
    if "uq_weak_labels_entity" not in weak_uq:
        op.create_unique_constraint("uq_weak_labels_entity", "weak_labels", ["entity_type", "entity_id"])

    # ── graph_features table ──────────────────────────────────────────────────
    if "graph_features" not in existing_tables:
        op.create_table(
            "graph_features",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("entity_type", sa.String(20), nullable=False),
            sa.Column("entity_id", sa.String(255), nullable=False),
            sa.Column("features_jsonb", JSONB, nullable=False),
            sa.Column("computed_at", sa.DateTime(), nullable=True),
            sa.Column("version", sa.String(80), nullable=True),
        )
    graph_indexes = {i["name"] for i in insp.get_indexes("graph_features")} if "graph_features" in set(insp.get_table_names()) else set()
    graph_uq = {u["name"] for u in insp.get_unique_constraints("graph_features")} if "graph_features" in set(insp.get_table_names()) else set()
    if "ix_graph_features_entity" not in graph_indexes:
        op.create_index("ix_graph_features_entity", "graph_features", ["entity_type", "entity_id"])
    if "ix_graph_features_version" not in graph_indexes:
        op.create_index("ix_graph_features_version", "graph_features", ["version"])
    if "uq_graph_features_entity" not in graph_uq:
        op.create_unique_constraint("uq_graph_features_entity", "graph_features", ["entity_type", "entity_id"])


def downgrade() -> None:
    op.drop_constraint("uq_graph_features_entity", "graph_features", type_="unique")
    op.drop_index("ix_graph_features_version", "graph_features")
    op.drop_index("ix_graph_features_entity", "graph_features")
    op.drop_table("graph_features")

    op.drop_constraint("uq_weak_labels_entity", "weak_labels", type_="unique")
    op.drop_index("ix_weak_labels_version", "weak_labels")
    op.drop_index("ix_weak_labels_entity", "weak_labels")
    op.drop_table("weak_labels")

    op.drop_index("ix_risk_scores_weak_model_version", "risk_scores")
    op.drop_index("ix_risk_scores_anomaly_model_version", "risk_scores")
    op.drop_column("risk_scores", "components_jsonb")
    op.drop_column("risk_scores", "composite_score")
    op.drop_column("risk_scores", "weak_explanation_jsonb")
    op.drop_column("risk_scores", "weak_model_version")
    op.drop_column("risk_scores", "weak_score")
    op.drop_column("risk_scores", "weak_proba")
    op.drop_column("risk_scores", "anomaly_explanation_jsonb")
    op.drop_column("risk_scores", "anomaly_model_version")
    op.drop_column("risk_scores", "anomaly_score")
