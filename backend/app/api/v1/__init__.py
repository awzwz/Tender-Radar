from fastapi import APIRouter
from app.api.v1.endpoints import dashboard, lots, tenders, suppliers, customers, notes, auth, admin

router = APIRouter()

router.include_router(auth.router, prefix="/auth", tags=["Auth"])
router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
router.include_router(lots.router, prefix="/lots", tags=["Lots"])
router.include_router(tenders.router, prefix="/tenders", tags=["Tenders"])
router.include_router(suppliers.router, prefix="/suppliers", tags=["Suppliers"])
router.include_router(customers.router, prefix="/customers", tags=["Customers"])
router.include_router(notes.router, prefix="/notes", tags=["Notes"])
router.include_router(admin.router, prefix="/admin", tags=["Admin"])
