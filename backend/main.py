from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging
import os

try:
    from .config import settings
    from .supabase_client import supabase
    from .routers import astrology, impact_map, polymarket, opportunities, trading, analytics, markets
    from .services.astrology import get_engine
except ImportError:
    from config import settings
    from supabase_client import supabase
    from routers import astrology, impact_map, polymarket, opportunities, trading, analytics, markets
    from services.astrology import get_engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    logger.info("Starting AstroEdge API...")
    
    # Pre-initialize astrology engine
    try:
        engine = get_engine()
        engine.initialize()
        logger.info("Astrology engine pre-initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to pre-initialize astrology engine: {e}")
    
    yield
    logger.info("Shutting down AstroEdge API...")


app = FastAPI(
    title="AstroEdge API",
    description="Astrological edge detection for prediction markets",
    version=settings.app_version,
    lifespan=lifespan,
)

# Configure CORS for Mini-App
origins = [
    "https://tg.dev",
    "http://localhost:5173",
    "http://localhost:8080",
    "http://localhost:8003",  # Mini-App served from same port
    "http://127.0.0.1:5173",
    "http://127.0.0.1:8080",
    "http://127.0.0.1:8003",  # Mini-App served from same port
    "*",  # Allow all origins for development
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(astrology.router)
app.include_router(impact_map.router)
app.include_router(polymarket.router)
app.include_router(opportunities.router)
app.include_router(trading.router)
app.include_router(analytics.router)
app.include_router(markets.router)
app.include_router(markets.api_router, prefix="/api")

# Mount static files for Mini-App
webapp_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "webapp")
if os.path.exists(webapp_path):
    app.mount("/miniapp", StaticFiles(directory=webapp_path, html=True), name="miniapp")
    logger.info(f"Mini-App mounted at /miniapp (serving from {webapp_path})")


@app.get("/health")
async def health_check():
    """Health check endpoint that verifies Supabase connectivity."""
    try:
        supabase_healthy = await supabase.health_check()

        if not supabase_healthy:
            raise HTTPException(status_code=503, detail="Supabase connection failed")

        return JSONResponse(
            status_code=200,
            content={
                "status": "healthy",
                "version": settings.app_version,
                "supabase": "connected",
            },
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.get("/version")
async def get_version():
    """Get application version information."""
    return JSONResponse(
        status_code=200,
        content={
            "version": settings.app_version,
            "service": "astroedge-api",
            "debug": settings.debug,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info",
    )
