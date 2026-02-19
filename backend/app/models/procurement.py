from sqlalchemy import Column, BigInteger, String, DateTime, Boolean, Numeric, Integer, Text, JSON, Float, Index
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
    ref_buy_status_id = Column(Integer)
    org_bin = Column(String(20), index=True)
    system_id = Column(Integer)
    singl_org_sign = Column(Integer, default=0)
    is_light_industry = Column(Integer, default=0)
    is_construction_work = Column(Integer, default=0)
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
    pay_date = Column(DateTime)
    ppn = Column(String(100))
    espk = Column(String(255))
    gu = Column(String(255))
    fin_source = Column(String(255))
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
        Index("ix_risk_flags_entity", "entity_type", "entity_id"),
        Index("ix_risk_flags_indicator", "entity_type", "entity_id", "indicator_code"),
    )


class RiskScore(Base):
    __tablename__ = "risk_scores"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(255), nullable=False)
    score = Column(Float, nullable=False, default=0.0)
    level = Column(String(10), nullable=False, default="LOW")  # LOW/MEDIUM/HIGH
    top_reasons_jsonb = Column(JSONB)
    computed_at = Column(DateTime)

    __table_args__ = (
        Index("ix_risk_scores_entity", "entity_type", "entity_id"),
        Index("ix_risk_scores_score", "entity_type", "score"),
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
