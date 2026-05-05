import logging
import os
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.v1 import router as api_router
from app.core.config import settings
from app.core.database import dispose_engine, engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("SKIP_DB_STARTUP_CHECK") != "1":
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.critical(
                "DATABASE_URL is invalid or the database is unreachable at startup. "
                "On Railway: Variables → DATABASE_URL must match your Postgres provider "
                "(postgresql+asyncpg://…). If you see tenant/user … not found, paste a fresh URI "
                "from Neon/Supabase/Railway Postgres. Underlying error: %s",
                e,
            )
            raise
    yield
    await dispose_engine()


app = FastAPI(
    title="Tender Risk Radar",
    description="API for detecting risky government procurements via Goszakup OWS V3",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

_origins = settings.cors_origins_list
_origin_regex = settings.cors_origin_regex_pattern
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_origin_regex=_origin_regex,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Must be registered so HTTPException is not swallowed by the generic Exception handler."""
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch unexpected errors only; HTTPException has its own handler above."""
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
