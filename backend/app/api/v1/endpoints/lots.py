import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.core.security import require_viewer
from app.models.procurement import Lot, TrdBuy, Contract, RiskFlag, RiskScore, Subject

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{lot_id}/indicators/{code}/details")
async def get_indicator_details(
    lot_id: int,
    code: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """
    Expanded evidence for an indicator: contracts, BINs with company names, etc.
    For auditors: full chain of contracts, parties, numbers.
    """
    lot_result = await db.execute(select(Lot).where(Lot.id == lot_id))
    lot = lot_result.scalar_one_or_none()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")

    customer_bin = lot.customer_bin

    if code == "CAROUSEL_PATTERN":
        if not customer_bin:
            raise HTTPException(status_code=400, detail="Lot has no customer_bin")
        # Fetch full contract chain for carousel: contract_number, sign_date, supplier_biin, sum
        result = await db.execute(
            select(
                Contract.id,
                Contract.contract_number,
                Contract.contract_number_sys,
                Contract.sign_date,
                Contract.supplier_biin,
                Contract.contract_sum_wnds,
                Contract.trd_buy_number_anno,
            )
            .where(
                Contract.customer_bin == customer_bin,
                Contract.is_deleted == False,
                Contract.supplier_biin.isnot(None),
            )
            .order_by(Contract.sign_date)
            .limit(50)
        )
        rows = result.all()

        # Get company names for unique supplier BINs
        biins = list(dict.fromkeys(r.supplier_biin for r in rows if r.supplier_biin))
        names = {}
        if biins:
            subj_result = await db.execute(
                select(Subject.bin, Subject.name_ru).where(Subject.bin.in_(biins))
            )
            for s in subj_result.all():
                names[s.bin] = s.name_ru or s.bin

        cust_subj = await db.execute(
            select(Subject.name_ru).where(Subject.bin == customer_bin).limit(1)
        )
        cust_row = cust_subj.first()
        customer_name = cust_row[0] if cust_row else lot.customer_name or customer_bin

        rotations = [
            {
                "contract_id": r.id,
                "contract_number": r.contract_number or r.contract_number_sys or str(r.id),
                "sign_date": str(r.sign_date) if r.sign_date else None,
                "supplier_biin": r.supplier_biin,
                "supplier_name": names.get(r.supplier_biin) if r.supplier_biin else None,
                "contract_sum": float(r.contract_sum_wnds or 0),
                "tender_number": r.trd_buy_number_anno,
            }
            for r in rows
        ]

        suppliers = [r.supplier_biin for r in rows if r.supplier_biin]
        seen = set()
        rot_count = 0
        for s in suppliers:
            if s in seen:
                rot_count += 1
                seen = {s}
            else:
                seen.add(s)

        return {
            "code": code,
            "customer_bin": customer_bin,
            "customer_name": customer_name,
            "rotation_count": rot_count,
            "unique_winners": len(set(suppliers)),
            "contracts": rotations,
        }

    if code == "RECURRING_WINNER":
        if not customer_bin:
            raise HTTPException(status_code=400, detail="Lot has no customer_bin")
        # Evidence has customer_bin, supplier_biin - return contracts between them
        flag_result = await db.execute(
            select(RiskFlag.evidence_jsonb).where(
                RiskFlag.entity_type == "lot",
                RiskFlag.entity_id == str(lot_id),
                RiskFlag.indicator_code == code,
            )
        )
        row = flag_result.first()
        ev = row[0] if row and row[0] else {}
        supplier_biin = ev.get("supplier_biin") if isinstance(ev, dict) else None
        if not supplier_biin:
            raise HTTPException(status_code=404, detail="RECURRING_WINNER evidence has no supplier_biin")

        c_result = await db.execute(
            select(
                Contract.id, Contract.contract_number, Contract.sign_date,
                Contract.contract_sum_wnds, Contract.trd_buy_number_anno,
            )
            .where(
                Contract.customer_bin == customer_bin,
                Contract.supplier_biin == supplier_biin,
                Contract.is_deleted == False,
            )
            .order_by(Contract.sign_date.desc())
            .limit(20)
        )
        contracts = [
            {
                "contract_id": r.id,
                "contract_number": r.contract_number or str(r.id),
                "sign_date": str(r.sign_date) if r.sign_date else None,
                "contract_sum": float(r.contract_sum_wnds or 0),
                "tender_number": r.trd_buy_number_anno,
            }
            for r in c_result.all()
        ]
        subj = await db.execute(
            select(Subject.name_ru).where(Subject.bin == supplier_biin).limit(1)
        )
        subj_row = subj.first()
        supplier_name = subj_row[0] if subj_row else supplier_biin
        return {
            "code": code,
            "customer_bin": customer_bin,
            "supplier_biin": supplier_biin,
            "supplier_name": supplier_name,
            "contracts": contracts,
        }

    if code == "COMMON_REQUISITES":
        # Evidence has common_phones, common_emails, bidder_count - already in evidence
        flag_result = await db.execute(
            select(RiskFlag.evidence_jsonb).where(
                RiskFlag.entity_type == "lot",
                RiskFlag.entity_id == str(lot_id),
                RiskFlag.indicator_code == code,
            )
        )
        row = flag_result.first()
        ev = row[0] if row and isinstance(row[0], dict) else {}
        return {"code": code, "evidence": ev}

    # Generic: return flag evidence as-is
    flag_result = await db.execute(
        select(RiskFlag).where(
            RiskFlag.entity_type == "lot",
            RiskFlag.entity_id == str(lot_id),
            RiskFlag.indicator_code == code,
        )
    )
    flag = flag_result.scalar_one_or_none()
    if not flag:
        raise HTTPException(status_code=404, detail=f"No flag found for indicator {code}")
    return {"code": code, "evidence": flag.evidence_jsonb or {}}


@router.get("/{lot_id}")
async def get_lot_detail(
    lot_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_viewer),
):
    """Full lot card: details + all risk flags with evidence + risk breakdown."""
    lot_result = await db.execute(select(Lot).where(Lot.id == lot_id))
    lot = lot_result.scalar_one_or_none()
    if not lot:
        raise HTTPException(status_code=404, detail="Lot not found")

    # Get tender
    tender = None
    if lot.trd_buy_id:
        t_result = await db.execute(select(TrdBuy).where(TrdBuy.id == lot.trd_buy_id))
        tender = t_result.scalar_one_or_none()

    # Get contract
    contract_result = await db.execute(
        select(Contract).where(
            Contract.trd_buy_id == lot.trd_buy_id,
            Contract.is_deleted == False,
        ).limit(1)
    )
    contract = contract_result.scalar_one_or_none()

    # Get risk score
    score_result = await db.execute(
        select(RiskScore).where(
            RiskScore.entity_type == "lot",
            RiskScore.entity_id == str(lot_id),
        )
    )
    risk_score = score_result.scalar_one_or_none()

    # On-demand scoring: if no score exists, compute it now
    if risk_score is None:
        logger.info(f"No risk score for lot {lot_id}, computing on-demand...")
        try:
            from app.features.engine import FeatureEngine
            engine = FeatureEngine()
            await engine.compute_lot_score(lot_id)
            # Re-fetch the score after computation (use a fresh query to see committed data)
            score_result = await db.execute(
                select(RiskScore).where(
                    RiskScore.entity_type == "lot",
                    RiskScore.entity_id == str(lot_id),
                )
            )
            risk_score = score_result.scalar_one_or_none()
            logger.info(f"On-demand scoring complete for lot {lot_id}: level={risk_score.level if risk_score else 'N/A'}")
        except Exception as e:
            logger.warning(f"On-demand scoring failed for lot {lot_id}: {e}")

    # Get all risk flags
    flags_result = await db.execute(
        select(RiskFlag).where(
            RiskFlag.entity_type == "lot",
            RiskFlag.entity_id == str(lot_id),
        )
    )
    flags = flags_result.scalars().all()

    return {
        "lot": {
            "id": lot.id,
            "name_ru": lot.name_ru,
            "amount": float(lot.amount or 0),
            "customer_bin": lot.customer_bin,
            "customer_name": lot.customer_name,
            "trd_buy_id": lot.trd_buy_id,
            "dumping_flag": lot.dumping_flag,
            "ref_lot_status_id": lot.ref_lot_status_id,
        },
        "tender": {
            "id": tender.id if tender else None,
            "number_anno": tender.number_anno if tender else None,
            "name_ru": tender.name_ru if tender else None,
            "publish_date": str(tender.publish_date) if tender and tender.publish_date else None,
            "start_date": str(tender.start_date) if tender and tender.start_date else None,
            "end_date": str(tender.end_date) if tender and tender.end_date else None,
            "ref_trade_methods_id": tender.ref_trade_methods_id if tender else None,
        } if tender else None,
        "contract": {
            "id": contract.id if contract else None,
            "supplier_biin": contract.supplier_biin if contract else None,
            "contract_sum_wnds": float(contract.contract_sum_wnds or 0) if contract else None,
            "sign_date": str(contract.sign_date) if contract and contract.sign_date else None,
            "plan_exec_date": str(contract.plan_exec_date) if contract and contract.plan_exec_date else None,
            "fakt_exec_date": str(contract.fakt_exec_date) if contract and contract.fakt_exec_date else None,
            "parent_id": contract.parent_id if contract else None,
        } if contract else None,
        "risk": {
            "score": risk_score.score_final if risk_score else None,
            "score_rules": risk_score.score_rules if risk_score else None,
            "score_ml": risk_score.score_ml if risk_score else None,
            "score_final": risk_score.score_final if risk_score else None,
            "level": risk_score.level if risk_score else "UNKNOWN",
            "top_reasons": risk_score.top_reasons_jsonb if risk_score else [],
            "computed_at": str(risk_score.computed_at) if risk_score else None,
        },
        "flags": [
            {
                "code": f.indicator_code,
                "triggered": f.flag_bool,
                "value": f.value_numeric,
                "evidence": f.evidence_jsonb,
            }
            for f in flags
        ],
    }
