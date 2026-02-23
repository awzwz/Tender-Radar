"""Initial migration - create all tables

Revision ID: 0001_initial
Revises:
Create Date: 2025-02-18
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Users ──────────────────────────────────────────────────────────────
    op.create_table('users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(100), nullable=False),
        sa.Column('email', sa.String(255), nullable=False),
        sa.Column('hashed_password', sa.String(255), nullable=False),
        sa.Column('role', sa.String(20), nullable=False, server_default='viewer'),
        sa.Column('is_active', sa.Integer(), server_default='1'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username'),
        sa.UniqueConstraint('email'),
    )

    # ── Raw Layer ──────────────────────────────────────────────────────────
    for table in ['raw_trdbuy', 'raw_lots', 'raw_trdapp', 'raw_contract', 'raw_subject', 'raw_rnu']:
        op.create_table(table,
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('payload_jsonb', postgresql.JSONB(), nullable=False),
            sa.Column('fetched_at', sa.DateTime(), nullable=False),
            sa.Column('source_version', sa.String(10), server_default='v3'),
            sa.PrimaryKeyConstraint('id'),
        )

    op.create_table('raw_journal',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('payload_jsonb', postgresql.JSONB(), nullable=False),
        sa.Column('fetched_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )

    # ── Subject ────────────────────────────────────────────────────────────
    op.create_table('subject',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('bin', sa.String(20)),
        sa.Column('iin', sa.String(20)),
        sa.Column('inn', sa.String(20)),
        sa.Column('unp', sa.String(20)),
        sa.Column('name_ru', sa.Text()),
        sa.Column('name_kz', sa.Text()),
        sa.Column('full_name_ru', sa.Text()),
        sa.Column('regdate', sa.DateTime()),
        sa.Column('crdate', sa.DateTime()),
        sa.Column('year', sa.Integer()),
        sa.Column('type_supplier', sa.Integer()),
        sa.Column('mark_small_employer', sa.Integer(), server_default='0'),
        sa.Column('mark_resident', sa.Integer(), server_default='1'),
        sa.Column('mark_patronymic_producer', sa.Integer(), server_default='0'),
        sa.Column('mark_national_company', sa.Integer(), server_default='0'),
        sa.Column('mark_world_company', sa.Integer(), server_default='0'),
        sa.Column('mark_state_monopoly', sa.Integer(), server_default='0'),
        sa.Column('mark_natural_monopoly', sa.Integer(), server_default='0'),
        sa.Column('oked_list', sa.BigInteger()),
        sa.Column('krp_code', sa.Integer()),
        sa.Column('kse_code', sa.Integer()),
        sa.Column('ref_kopf_code', sa.String(20)),
        sa.Column('qvazi', sa.Integer(), server_default='0'),
        sa.Column('customer', sa.Integer(), server_default='0'),
        sa.Column('supplier', sa.Integer(), server_default='0'),
        sa.Column('organizer', sa.Integer(), server_default='0'),
        sa.Column('is_single_org', sa.Integer(), server_default='0'),
        sa.Column('email', sa.String(255)),
        sa.Column('phone', sa.String(50)),
        sa.Column('website', sa.String(255)),
        sa.Column('country_code', sa.String(10)),
        sa.Column('system_id', sa.Integer()),
        sa.Column('last_update_at', sa.DateTime()),
        sa.Column('is_deleted', sa.Boolean(), server_default='false'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_subject_bin', 'subject', ['bin'])

    # ── TrdBuy ─────────────────────────────────────────────────────────────
    op.create_table('trd_buy',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('number_anno', sa.String(50)),
        sa.Column('name_ru', sa.Text()),
        sa.Column('name_kz', sa.Text()),
        sa.Column('ref_trade_methods_id', sa.Integer()),
        sa.Column('publish_date', sa.DateTime()),
        sa.Column('start_date', sa.DateTime()),
        sa.Column('end_date', sa.DateTime()),
        sa.Column('total_sum', sa.Numeric(20, 2)),
        sa.Column('ref_buy_status_id', sa.Integer()),
        sa.Column('org_bin', sa.String(20)),
        sa.Column('system_id', sa.Integer()),
        sa.Column('singl_org_sign', sa.Integer(), server_default='0'),
        sa.Column('is_light_industry', sa.Integer(), server_default='0'),
        sa.Column('is_construction_work', sa.Integer(), server_default='0'),
        sa.Column('last_update_at', sa.DateTime()),
        sa.Column('is_deleted', sa.Boolean(), server_default='false'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_trd_buy_publish_date', 'trd_buy', ['publish_date'])
    op.create_index('ix_trd_buy_org_bin', 'trd_buy', ['org_bin'])

    # ── Lots ───────────────────────────────────────────────────────────────
    op.create_table('lots',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('trd_buy_id', sa.BigInteger()),
        sa.Column('lot_number', sa.String(50)),
        sa.Column('name_ru', sa.Text()),
        sa.Column('name_kz', sa.Text()),
        sa.Column('amount', sa.Numeric(20, 2)),
        sa.Column('customer_bin', sa.String(20)),
        sa.Column('customer_name', sa.Text()),
        sa.Column('dumping_flag', sa.Boolean(), server_default='false'),
        sa.Column('union_lots_flag', sa.Boolean(), server_default='false'),
        sa.Column('ref_lot_status_id', sa.Integer()),
        sa.Column('singl_org_sign', sa.Integer(), server_default='0'),
        sa.Column('is_light_industry', sa.Integer(), server_default='0'),
        sa.Column('is_construction_work', sa.Integer(), server_default='0'),
        sa.Column('disable_person_id', sa.Integer(), server_default='0'),
        sa.Column('system_id', sa.Integer()),
        sa.Column('last_update_at', sa.DateTime()),
        sa.Column('is_deleted', sa.Boolean(), server_default='false'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_lots_trd_buy_id', 'lots', ['trd_buy_id'])
    op.create_index('ix_lots_customer_bin', 'lots', ['customer_bin'])

    # ── TrdApp ─────────────────────────────────────────────────────────────
    op.create_table('trd_app',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('buy_id', sa.BigInteger()),
        sa.Column('supplier_id', sa.BigInteger()),
        sa.Column('supplier_biin', sa.String(20)),
        sa.Column('cr_fio', sa.String(255)),
        sa.Column('mod_fio', sa.String(255)),
        sa.Column('prot_id', sa.BigInteger()),
        sa.Column('prot_number', sa.String(50)),
        sa.Column('date_apply', sa.DateTime()),
        sa.Column('system_id', sa.Integer()),
        sa.Column('last_update_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_trd_app_buy_id', 'trd_app', ['buy_id'])
    op.create_index('ix_trd_app_supplier_biin', 'trd_app', ['supplier_biin'])

    op.create_table('trd_app_lots',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('trd_app_id', sa.BigInteger()),
        sa.Column('lot_id', sa.BigInteger()),
        sa.Column('status_id', sa.Integer()),
        sa.Column('price', sa.Numeric(20, 2)),
        sa.Column('amount', sa.Numeric(20, 2)),
        sa.Column('discount_value', sa.Float()),
        sa.Column('discount_price', sa.Numeric(20, 2)),
        sa.PrimaryKeyConstraint('id'),
    )

    # ── Contract ───────────────────────────────────────────────────────────
    op.create_table('contract',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('trd_buy_id', sa.BigInteger()),
        sa.Column('contract_number', sa.String(100)),
        sa.Column('contract_number_sys', sa.String(100)),
        sa.Column('trd_buy_number_anno', sa.String(50)),
        sa.Column('customer_bin', sa.String(20)),
        sa.Column('supplier_biin', sa.String(20)),
        sa.Column('contract_sum_wnds', sa.Numeric(20, 2)),
        sa.Column('sign_date', sa.DateTime()),
        sa.Column('plan_exec_date', sa.DateTime()),
        sa.Column('fakt_exec_date', sa.DateTime()),
        sa.Column('fakt_sum', sa.Numeric(20, 2)),
        sa.Column('ref_contract_status_id', sa.Integer()),
        sa.Column('ref_contract_type_id', sa.Integer()),
        sa.Column('parent_id', sa.BigInteger()),
        sa.Column('root_id', sa.BigInteger()),
        sa.Column('supplier_legal_address', sa.Text()),
        sa.Column('customer_legal_address', sa.Text()),
        sa.Column('is_gu', sa.Integer(), server_default='0'),
        sa.Column('exchange_rate', sa.Float()),
        sa.Column('system_id', sa.Integer()),
        sa.Column('last_update_at', sa.DateTime()),
        sa.Column('is_deleted', sa.Boolean(), server_default='false'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_contract_trd_buy_id', 'contract', ['trd_buy_id'])
    op.create_index('ix_contract_customer_bin', 'contract', ['customer_bin'])
    op.create_index('ix_contract_supplier_biin', 'contract', ['supplier_biin'])
    op.create_index('ix_contract_root_id', 'contract', ['root_id'])

    # ── RNU ────────────────────────────────────────────────────────────────
    op.create_table('rnu',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('pid', sa.BigInteger()),
        sa.Column('supplier_biin', sa.String(20)),
        sa.Column('supplier_name_ru', sa.Text()),
        sa.Column('start_date', sa.DateTime()),
        sa.Column('end_date', sa.DateTime()),
        sa.Column('reason', sa.Text()),
        sa.Column('system_id', sa.Integer()),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_rnu_supplier_biin', 'rnu', ['supplier_biin'])

    # ── Treasury Pay ───────────────────────────────────────────────────────
    op.create_table('treasury_pay',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('nom_za', sa.String(100)),
        sa.Column('contract_id', sa.BigInteger()),
        sa.Column('dt_reg', sa.DateTime()),
        sa.Column('supplier', sa.Text()),
        sa.Column('rnn_supplier', sa.String(20)),
        sa.Column('nom_dog', sa.String(100)),
        sa.Column('dt_dog', sa.DateTime()),
        sa.Column('item_description', sa.Text()),
        sa.Column('pay_amount', sa.Numeric(20, 2)),
        sa.Column('pay_date', sa.DateTime()),
        sa.Column('ppn', sa.String(100)),
        sa.Column('espk', sa.String(50)),
        sa.Column('gu', sa.String(50)),
        sa.Column('fin_source', sa.String(50)),
        sa.Column('index_date', sa.DateTime()),
        sa.Column('system_id', sa.Integer()),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_treasury_pay_contract_id', 'treasury_pay', ['contract_id'])

    # ── Feature / Scoring Layer ────────────────────────────────────────────
    op.create_table('risk_flags',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('entity_type', sa.String(20), nullable=False),
        sa.Column('entity_id', sa.String(50), nullable=False),
        sa.Column('indicator_code', sa.String(50), nullable=False),
        sa.Column('flag_bool', sa.Boolean(), server_default='false'),
        sa.Column('value_numeric', sa.Float()),
        sa.Column('evidence_jsonb', postgresql.JSONB()),
        sa.Column('computed_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_risk_flags_entity', 'risk_flags', ['entity_type', 'entity_id'])

    op.create_table('risk_scores',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('entity_type', sa.String(20), nullable=False),
        sa.Column('entity_id', sa.String(50), nullable=False),
        sa.Column('score', sa.Float(), nullable=False, server_default='0'),
        sa.Column('level', sa.String(10), nullable=False, server_default='LOW'),
        sa.Column('top_reasons_jsonb', postgresql.JSONB()),
        sa.Column('computed_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_risk_scores_entity', 'risk_scores', ['entity_type', 'entity_id'])
    op.create_index('ix_risk_scores_score', 'risk_scores', ['entity_type', 'score'])

    op.create_table('analyst_notes',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('entity_type', sa.String(20), nullable=False),
        sa.Column('entity_id', sa.String(50), nullable=False),
        sa.Column('note_text', sa.Text()),
        sa.Column('label', sa.String(30)),
        sa.Column('created_by', sa.Integer()),
        sa.Column('created_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_analyst_notes_entity', 'analyst_notes', ['entity_type', 'entity_id'])

    # ── ETL Control ────────────────────────────────────────────────────────
    op.create_table('etl_runs',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('run_type', sa.String(20)),
        sa.Column('started_at', sa.DateTime()),
        sa.Column('finished_at', sa.DateTime()),
        sa.Column('status', sa.String(20)),
        sa.Column('summary_jsonb', postgresql.JSONB()),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_table('etl_cursors',
        sa.Column('source_name', sa.String(50), nullable=False),
        sa.Column('cursor_value', sa.String(255)),
        sa.Column('updated_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('source_name'),
    )


def downgrade() -> None:
    tables = [
        'etl_cursors', 'etl_runs', 'analyst_notes', 'risk_scores', 'risk_flags',
        'treasury_pay', 'rnu', 'contract', 'trd_app_lots', 'trd_app',
        'lots', 'trd_buy', 'subject',
        'raw_journal', 'raw_rnu', 'raw_subject', 'raw_contract', 'raw_trdapp', 'raw_lots', 'raw_trdbuy',
        'users',
    ]
    for table in tables:
        op.drop_table(table)
