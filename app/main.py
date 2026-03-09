import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.routes import alerts, disasters, hazards, health, infrastructure, risk
from app.core.config import get_settings

settings = get_settings()
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ResilienceMap API",
    description=(
        "Open-source disaster risk intelligence platform. "
        "Provides composite risk scores per US census tract derived from "
        "FEMA flood zones, USGS seismic data, and NOAA storm alerts."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

# API routes
app.include_router(health.router, tags=["Health"])
app.include_router(risk.router, prefix=settings.api_prefix, tags=["Risk Scores"])
app.include_router(hazards.router, prefix=settings.api_prefix, tags=["Hazards"])
app.include_router(alerts.router, prefix=settings.api_prefix, tags=["Alerts"])
app.include_router(infrastructure.router, prefix=settings.api_prefix, tags=["Infrastructure"])
app.include_router(disasters.router, prefix=settings.api_prefix, tags=["Disaster History"])

# Serve map dashboard as static files
app.mount("/dashboard", StaticFiles(directory="dashboard", html=True), name="dashboard")


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("ResilienceMap API starting up — environment: %s", settings.app_env)
