from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

try:
    from .config import settings
    from .supabase_client import supabase
except ImportError:
    from config import settings
    from supabase_client import supabase

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    logger.info("Starting AstroEdge API...")
    yield
    logger.info("Shutting down AstroEdge API...")


app = FastAPI(
    title="AstroEdge API",
    description="Astrological edge detection for prediction markets",
    version=settings.app_version,
    lifespan=lifespan,
)


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
