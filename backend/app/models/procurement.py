from sqlalchemy import (
    Column, BigInteger, String, DateTime, Boolean, Numeric,
    Integer, Text, JSON, Float, Index, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from app.core.database import Base


# ─── RAW LAYER ───────────────────────────────────────────────────────────────

class RawTrdBuy(Base):
    __tablename__ = "raw_trdbuy"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawLots(Base):
    __tablename__ = "raw_lots"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawTrdApp(Base):
    __tablename__ = "raw_trdapp"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawContract(Base):
    __tablename__ = "raw_contract"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawSubject(Base):
    __tablename__ = "raw_subject"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawRnu(Base):
    __tablename__ = "raw_rnu"
    id = Column(BigInteger, primary_key=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    source_version = Column(String(10), default="v3")


class RawJournal(Base):
    __tablename__ = "raw_journal"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    payload_jsonb = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime, nullable=False)


# ─── NORMALIZED LAYER ────────────────────────────────────────────────────────

class TrdBuy(Base):
    __tablename__ = "trd_buy"
    id = Column(BigInteger, primary_key=True)
    number_anno = Column(String(255), index=True)
    name_ru = Column(Text)
    name_kz = Column(Text)
    ref_trade_methods_id = Column(Integer)
    publish_date = Column(DateTime, index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    total_sum = Column(Numeric(20, 2))
    count_lots = Column(Integer)
    ref_buy_status_id = Column(Integer)
    org_bin = Column(String(20), index=True)
    system_id = Column(Integer)
    singl_org_sign = Column(Integer, default=0)
    is_light_industry = Column(Integer, default=0)
    is_construction_work = Column(Integer, default=0)
    # Repeat / cancel / pause metadata
    parent_id = Column(BigInteger, index=True)
    repeat_start_date = Column(DateTime)
    repeat_end_date = Column(DateTime)
    discus_start_date = Column(DateTime)
    discus_end_date = Column(DateTime)
    last_update_date = Column(DateTime)
    last_update_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)

    __table_args__ = (
        Index("ix_trd_buy_publish_date", "publish_date"),
        Index("ix_trd_buy_org_bin", "org_bin"),
    )


class Lot(Base):
    __tablename__ = "lots"
    id = Column(BigInteger, primary_key=True)
    trd_buy_id = Column(BigInteger, index=True)
    lot_number = Column(String(255))
    name_ru = Column(Text)
    name_kz = Column(Text)
    amount = Column(Numeric(20, 2))
    customer_bin = Column(String(20), index=True)
    customer_name = Column(Text)
    dumping_flag = Column(Boolean, default=False)
    union_lots_flag = Column(Boolean, default=False)
    ref_lot_status_id = Column(Integer)
    singl_org_sign = Column(Integer, default=0)
    is_light_industry = Column(Integer, default=0)
    is_construction_work = Column(Integer, default=0)
    disable_person_id = Column(Integer, default=0)
    system_id = Column(Integer)
    last_update_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)
    # ── Price Radar enrichment (from OWS Lots.Plans) ──────────────────────────
    count = Column(Float)               # quantity ordered (for unit price)
    enstru_code = Column(String(40), index=True)  # standardized product code
    enstru_id = Column(BigInteger)
    enstru_name = Column(Text)
    unit_code = Column(String(10))      # unit of measure code
    unit_price = Column(Float)          # amount / count
    price_enriched_at = Column(DateTime)

    __table_args__ = (
        Index("ix_lots_trd_buy_id", "trd_buy_id"),
        Index("ix_lots_customer_bin", "customer_bin"),
        Index("ix_lots_amount", "amount"),
    )


class TrdApp(Base):
    __tablename__ = "trd_app"
    id = Column(BigInteger, primary_key=True)
    buy_id = Column(BigInteger, index=True)
    supplier_id = Column(BigInteger)
    supplier_biin = Column(String(20), index=True)
    cr_fio = Column(String(255))
    mod_fio = Column(String(255))
    prot_id = Column(BigInteger)
    prot_number = Column(String(255))
    date_apply = Column(DateTime)
    system_id = Column(Integer)
    last_update_at = Column(DateTime)


class TrdAppLot(Base):
    __tablename__ = "trd_app_lots"
    id = Column(BigInteger, primary_key=True)
    trd_app_id = Column(BigInteger, index=True)
    lot_id = Column(BigInteger, index=True)
    status_id = Column(Integer)
    price = Column(Numeric(20, 2))
    amount = Column(Numeric(20, 2))
    discount_value = Column(Float)
    discount_price = Column(Numeric(20, 2))


class Contract(Base):
    __tablename__ = "contract"
    id = Column(BigInteger, primary_key=True)
    trd_buy_id = Column(BigInteger, index=True)
    contract_number = Column(String(100))
    contract_number_sys = Column(String(100))
    trd_buy_number_anno = Column(String(50))
    customer_bin = Column(String(20), index=True)
    supplier_biin = Column(String(20), index=True)
    contract_sum_wnds = Column(Numeric(20, 2))
    sign_date = Column(DateTime)
    plan_exec_date = Column(DateTime)
    fakt_exec_date = Column(DateTime)
    fakt_sum = Column(Numeric(20, 2))
    ref_contract_status_id = Column(Integer)
    ref_contract_type_id = Column(Integer)
    parent_id = Column(BigInteger, index=True)
    root_id = Column(BigInteger, index=True)
    supplier_legal_address = Column(Text)
    customer_legal_address = Column(Text)
    # Bank details (for BANK_DETAILS_REUSE indicator)
    supplier_iik = Column(String(34))
    supplier_bik = Column(String(20))
    is_gu = Column(Integer, default=0)
    exchange_rate = Column(Float)
    system_id = Column(Integer)
    last_update_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)

    __table_args__ = (
        Index("ix_contract_trd_buy_id", "trd_buy_id"),
        Index("ix_contract_customer_bin", "customer_bin"),
        Index("ix_contract_supplier_biin", "supplier_biin"),
        Index("ix_contract_root_id", "root_id"),
    )


class ContractAct(Base):
    """Акт выполненных работ / acceptance act."""
    __tablename__ = "contract_act"
    id = Column(BigInteger, primary_key=True)
    contract_id = Column(BigInteger, index=True, nullable=False)
    act_number = Column(String(100))
    act_date = Column(DateTime)
    sum_act = Column(Numeric(20, 2))
    sum_fine = Column(Numeric(20, 2))
    day_overdue = Column(Integer, default=0)
    ref_act_status_id = Column(Integer)
    system_id = Column(Integer)
    last_update_at = Column(DateTime)


class ContractPayment(Base):
    """Paid amounts linked to a contract (from ContractPayment GraphQL type)."""
    __tablename__ = "contract_payment"
    id = Column(BigInteger, primary_key=True)
    contract_id = Column(BigInteger, index=True, nullable=False)
    act_id = Column(BigInteger)
    pay_sum = Column(Numeric(20, 2))
    pay_date = Column(DateTime)
    ref_payment_type_id = Column(Integer)
    system_id = Column(Integer)
    last_update_at = Column(DateTime)


class ContractSpecSum(Base):
    """Contract specification totals (factSum vs totalSum)."""
    __tablename__ = "contract_spec_sum"
    id = Column(BigInteger, primary_key=True)
    contract_id = Column(BigInteger, index=True, nullable=False)
    total_sum = Column(Numeric(20, 2))
    fact_sum = Column(Numeric(20, 2))
    system_id = Column(Integer)
    last_update_at = Column(DateTime)


class Subject(Base):
    __tablename__ = "subject"
    id = Column(BigInteger, primary_key=True)  # pid
    bin = Column(String(20), index=True)
    iin = Column(String(20))
    inn = Column(String(20))
    unp = Column(String(20))
    name_ru = Column(Text)
    name_kz = Column(Text)
    full_name_ru = Column(Text)
    regdate = Column(DateTime)   # Дата свидетельства о гос. регистрации
    crdate = Column(DateTime)    # Дата регистрации на портале
    year = Column(Integer)       # Год регистрации
    type_supplier = Column(Integer)  # 1=юр.лицо, 2=физ.лицо, 3=ИП
    mark_small_employer = Column(Integer, default=0)
    mark_resident = Column(Integer, default=1)
    mark_patronymic_producer = Column(Integer, default=0)
    mark_national_company = Column(Integer, default=0)
    mark_world_company = Column(Integer, default=0)
    mark_state_monopoly = Column(Integer, default=0)
    mark_natural_monopoly = Column(Integer, default=0)
    oked_list = Column(BigInteger)
    krp_code = Column(Integer)
    kse_code = Column(Integer)
    ref_kopf_code = Column(String(20))
    qvazi = Column(Integer, default=0)
    customer = Column(Integer, default=0)
    supplier = Column(Integer, default=0)
    organizer = Column(Integer, default=0)
    is_single_org = Column(Integer, default=0)
    email = Column(String(255))
    phone = Column(String(255))
    website = Column(String(255))
    country_code = Column(String(10))
    system_id = Column(Integer)
    last_update_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)


class Rnu(Base):
    __tablename__ = "rnu"
    id = Column(BigInteger, primary_key=True)
    pid = Column(BigInteger)
    supplier_biin = Column(String(20), index=True)
    supplier_name_ru = Column(Text)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    reason = Column(Text)
    system_id = Column(Integer)  # 1=Mitwork, 2=Samruk, 3=Goszakup
    is_active = Column(Boolean, default=True)


class TreasuryPay(Base):
    __tablename__ = "treasury_pay"
    id = Column(BigInteger, primary_key=True)
    nom_za = Column(String(100))
    contract_id = Column(BigInteger, index=True)
    dt_reg = Column(DateTime)
    supplier = Column(Text)
    rnn_supplier = Column(String(20))
    nom_dog = Column(String(100))
    dt_dog = Column(DateTime)
    item_description = Column(Text)
    pay_amount = Column(Numeric(20, 2))
    prepay_sum = Column(Numeric(20, 2))
    pay_date = Column(DateTime)
    ppn = Column(String(100))
    espk = Column(String(255))
    gu = Column(String(255))
    fin_source = Column(String(255))
    iik_supplier = Column(String(34))
    bik_supplier = Column(String(20))
    vendor_id = Column(BigInteger)
    index_date = Column(DateTime)
    system_id = Column(Integer)


# ─── FEATURE / SCORING LAYER ─────────────────────────────────────────────────

class RiskFlag(Base):
    __tablename__ = "risk_flags"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)   # lot/tender/supplier/customer
    entity_id = Column(String(255), nullable=False)
    indicator_code = Column(String(255), nullable=False)
    flag_bool = Column(Boolean, default=False)
    value_numeric = Column(Float)
    evidence_jsonb = Column(JSONB)
    computed_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", "indicator_code", name="uq_risk_flags_entity_indicator"),
        Index("ix_risk_flags_entity", "entity_type", "entity_id"),
    )


class RiskScore(Base):
    __tablename__ = "risk_scores"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(255), nullable=False)
    score = Column(Float, nullable=False, default=0.0)
    score_rules = Column(Float)
    score_ml = Column(Float)
    score_final = Column(Float)
    level = Column(String(10), nullable=False, default="LOW")  # LOW/MEDIUM/HIGH
    top_reasons_jsonb = Column(JSONB)
    computed_at = Column(DateTime)
    # Anomaly (IsolationForest)
    anomaly_score = Column(Float)
    anomaly_model_version = Column(String(50))
    anomaly_explanation_jsonb = Column(JSONB)
    # Weak supervision (Snorkel + GBM)
    weak_proba = Column(Float)
    weak_score = Column(Float)
    weak_model_version = Column(String(50))
    weak_explanation_jsonb = Column(JSONB)
    # Composite (when risk_scoring_mode=composite)
    composite_score = Column(Float)
    components_jsonb = Column(JSONB)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", name="uq_risk_scores_entity"),
        Index("ix_risk_scores_entity", "entity_type", "entity_id"),
        Index("ix_risk_scores_score", "entity_type", "score"),
    )


class TenderFeature(Base):
    """Computed feature vector per lot — used for ML training and inference."""
    __tablename__ = "tender_features"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False, default="lot")
    entity_id = Column(String(255), nullable=False)
    features_jsonb = Column(JSONB, nullable=False)  # {indicator_code: numeric_value, ...}
    label = Column(Integer)  # silver label: 0/1 or NULL
    label_source = Column(String(50))  # "rnu", "fines", "weak_rules", etc.
    computed_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", name="uq_tender_features_entity"),
        Index("ix_tender_features_entity", "entity_type", "entity_id"),
    )


class LlmExplanation(Base):
    """Cached LLM-generated explanations."""
    __tablename__ = "llm_explanations"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(255), nullable=False)
    risk_final = Column(Float)
    risk_rules = Column(Float)
    risk_ml = Column(Float)
    explanation_text = Column(Text)
    checklist_jsonb = Column(JSONB)
    model_used = Column(String(50))
    prompt_hash = Column(String(64))
    created_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("entity_type", "entity_id", name="uq_llm_explanations_entity"),
        Index("ix_llm_explanations_entity", "entity_type", "entity_id"),
    )


class AnalystNote(Base):
    __tablename__ = "analyst_notes"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(255), nullable=False)
    note_text = Column(Text)
    label = Column(String(30))  # SUSPICIOUS/FALSE_POSITIVE/NEEDS_REVIEW/VERIFIED
    created_by = Column(Integer)
    created_at = Column(DateTime)

    __table_args__ = (
        Index("ix_analyst_notes_entity", "entity_type", "entity_id"),
    )


class WeakLabel(Base):
    """Probabilistic weak labels from labeling functions (Snorkel-style)."""
    __tablename__ = "weak_labels"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False, default="lot")
    entity_id = Column(String(255), nullable=False)
    weak_proba = Column(Float, nullable=False)
    version = Column(String(80), nullable=False)
    lf_votes_jsonb = Column(JSONB)
    created_at = Column(DateTime)

    __table_args__ = (
        Index("ix_weak_labels_entity", "entity_type", "entity_id"),
        Index("ix_weak_labels_version", "version"),
    )


class GraphFeature(Base):
    """Precomputed graph-based features (co-bidding, rotation)."""
    __tablename__ = "graph_features"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(255), nullable=False)
    features_jsonb = Column(JSONB, nullable=False)
    computed_at = Column(DateTime)
    version = Column(String(80))

    __table_args__ = (
        Index("ix_graph_features_entity", "entity_type", "entity_id"),
        Index("ix_graph_features_version", "version"),
    )


class PriceBenchmark(Base):
    """Per-product (ENSTRU + unit) unit-price distribution used by OVERPRICED_UNIT.

    Built from competitive lots that buy the same product, following the
    OCP / World Bank unit-price benchmarking methodology (quartiles + IQR fence).
    """
    __tablename__ = "price_benchmark"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    enstru_code = Column(String(40), nullable=False)
    unit_code = Column(String(10), nullable=False)
    enstru_name = Column(Text)
    n_samples = Column(Integer, nullable=False)
    median_price = Column(Float, nullable=False)
    q1 = Column(Float, nullable=False)
    q3 = Column(Float, nullable=False)
    iqr = Column(Float, nullable=False)
    upper_fence = Column(Float, nullable=False)   # q3 + 1.5*iqr (overpricing fence)
    min_price = Column(Float)
    max_price = Column(Float)
    median_count = Column(Float)   # typical quantity for the group (count-plausibility guard)
    updated_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("enstru_code", "unit_code", name="uq_price_benchmark"),
        Index("ix_price_benchmark_enstru", "enstru_code"),
    )


# ─── ETL CONTROL ─────────────────────────────────────────────────────────────

class EtlRun(Base):
    __tablename__ = "etl_runs"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_type = Column(String(20))  # backfill/incremental
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    status = Column(String(20))  # running/success/partial/failed
    summary_jsonb = Column(JSONB)


class EtlCursor(Base):
    __tablename__ = "etl_cursors"
    source_name = Column(String(50), primary_key=True)
    cursor_value = Column(String(255))
    updated_at = Column(DateTime)
