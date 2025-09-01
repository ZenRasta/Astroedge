"""FastAPI router for astrology endpoints."""

import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import JSONResponse

try:
    from ..schemas import (
        GeneratePayload, GenerateResponse, AspectListResponse,
        AspectSummary, ErrorResponse, QuarterInfo
    )
    from ..services.astrology import compute_discordant_aspects, get_engine
    from ..services.supabase_repo import get_repo
    from ..services.quarters import parse_quarter, get_current_quarter
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from schemas import (
        GeneratePayload, GenerateResponse, AspectListResponse,
        AspectSummary, ErrorResponse, QuarterInfo
    )
    from services.astrology import compute_discordant_aspects, get_engine
    from services.supabase_repo import get_repo
    from services.quarters import parse_quarter, get_current_quarter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/astrology", tags=["astrology"])


@router.post(
    "/generate",
    response_model=GenerateResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def generate_aspects(payload: GeneratePayload):
    """Generate aspect events for a quarter and store in database.
    
    This endpoint computes all discordant planetary aspects (conjunctions,
    squares, oppositions) for the specified quarter and upserts them into
    the database. The operation is idempotent - running it multiple times
    will not create duplicates.
    
    Args:
        payload: Generation parameters including quarter and optional orb limits
        
    Returns:
        GenerateResponse with statistics about generated aspects
        
    Examples:
        POST /astrology/generate
        {
            "quarter": "2025-Q3",
            "orb_limits": {"square": 8.0, "opposition": 8.0, "conjunction": 6.0},
            "force_regenerate": false
        }
    """
    try:
        logger.info(f"Generating aspects for quarter {payload.quarter}")
        start_time = time.time()
        
        # Check if aspects already exist (unless force_regenerate)
        repo = get_repo()
        if not payload.force_regenerate:
            existing_count = await repo.count_aspect_events(payload.quarter)
            if existing_count > 0:
                logger.info(f"Quarter {payload.quarter} already has {existing_count} aspects")
                
                # Get summary for existing data
                summary = await repo.get_aspect_summary(payload.quarter)
                execution_time = time.time() - start_time
                
                return GenerateResponse(
                    quarter=payload.quarter,
                    inserted_or_updated=0,
                    total_aspects=existing_count,
                    execution_time_seconds=round(execution_time, 3),
                    summary={
                        "status": "already_exists",
                        "by_severity": summary.by_severity,
                        "by_aspect_type": summary.by_aspect_type,
                        "eclipse_count": summary.eclipse_count,
                        "average_orb": summary.average_orb
                    }
                )
        
        # Generate aspects
        events = compute_discordant_aspects(payload.quarter, payload.orb_limits)
        
        if not events:
            logger.warning(f"No aspects found for quarter {payload.quarter}")
            return GenerateResponse(
                quarter=payload.quarter,
                inserted_or_updated=0,
                total_aspects=0,
                execution_time_seconds=round(time.time() - start_time, 3),
                summary={"status": "no_aspects_found"}
            )
        
        # Store in database
        rows_affected = await repo.upsert_aspect_events(events)
        
        # Generate summary statistics
        summary_stats = {}
        if events:
            by_severity = {}
            by_aspect_type = {}
            eclipse_count = 0
            total_orb = 0.0
            
            for event in events:
                by_severity[event.severity] = by_severity.get(event.severity, 0) + 1
                by_aspect_type[event.aspect] = by_aspect_type.get(event.aspect, 0) + 1
                if event.is_eclipse:
                    eclipse_count += 1
                total_orb += event.orb_deg
            
            summary_stats = {
                "by_severity": by_severity,
                "by_aspect_type": by_aspect_type,
                "eclipse_count": eclipse_count,
                "average_orb": round(total_orb / len(events), 3),
                "status": "generated"
            }
        
        execution_time = time.time() - start_time
        
        return GenerateResponse(
            quarter=payload.quarter,
            inserted_or_updated=rows_affected,
            total_aspects=len(events),
            execution_time_seconds=round(execution_time, 3),
            summary=summary_stats
        )
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to generate aspects: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate aspects"
        )


@router.get(
    "/aspects",
    response_model=AspectListResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid quarter format"},
        404: {"model": ErrorResponse, "description": "No aspects found for quarter"}
    }
)
async def list_aspects(
    quarter: str = Query(..., description="Quarter string like '2025-Q3'"),
    aspect: Optional[str] = Query(None, description="Filter by aspect type"),
    severity: Optional[str] = Query(None, description="Filter by severity (major/minor)"),
    planet: Optional[str] = Query(None, description="Filter by planet (appears in either planet1 or planet2)"),
    eclipse_only: Optional[bool] = Query(None, description="Filter to eclipse aspects only"),
    limit: Optional[int] = Query(None, description="Maximum number of results")
):
    """List aspect events for a quarter with optional filters.
    
    Returns all aspect events for the specified quarter, ordered by peak time.
    Supports various filters to narrow down results.
    
    Args:
        quarter: Quarter string (required)
        aspect: Filter by aspect type (conjunction/square/opposition)
        severity: Filter by severity (major/minor)
        planet: Filter by planet (matches planet1 OR planet2)
        eclipse_only: If true, only return eclipse aspects
        limit: Maximum number of results to return
        
    Returns:
        AspectListResponse with filtered aspects and summary statistics
        
    Examples:
        GET /astrology/aspects?quarter=2025-Q3
        GET /astrology/aspects?quarter=2025-Q3&aspect=opposition&severity=major
        GET /astrology/aspects?quarter=2025-Q3&planet=MARS&eclipse_only=false
    """
    try:
        # Validate quarter format
        parse_quarter(quarter)
        
        repo = get_repo()
        
        # Build filters for repository call
        kwargs = {"quarter": quarter, "order_by": "peak_utc"}
        
        if aspect:
            kwargs["aspect"] = aspect.lower()
        if severity:
            kwargs["severity"] = severity.lower()
        if eclipse_only is not None:
            kwargs["is_eclipse"] = eclipse_only
        if limit:
            kwargs["limit"] = limit
        
        # Note: planet filter requires special handling since it can match planet1 OR planet2
        # For now, we'll fetch all and filter in Python (could optimize with OR query later)
        events = await repo.fetch_aspect_events(**kwargs)
        
        # Apply planet filter if specified
        if planet:
            planet_upper = planet.upper()
            events = [e for e in events if planet_upper in (e.planet1, e.planet2)]
            
        # Apply limit if specified (and not already applied at DB level)
        if limit and not kwargs.get("limit"):
            events = events[:limit]
        
        # Generate summary
        summary = await repo.get_aspect_summary(quarter)
        
        return AspectListResponse(
            quarter=quarter,
            aspects=events,
            total_count=len(events),
            summary={
                "total_in_quarter": summary.total_aspects,
                "filtered_count": len(events),
                "by_severity": summary.by_severity,
                "by_aspect_type": summary.by_aspect_type,
                "eclipse_count": summary.eclipse_count,
                "average_orb": summary.average_orb,
                "date_range": {
                    "earliest": summary.date_range.get("earliest"),
                    "latest": summary.date_range.get("latest")
                }
            }
        )
        
    except ValueError as e:
        logger.error(f"Invalid quarter format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid quarter format: {quarter}"
        )
    except Exception as e:
        logger.error(f"Failed to list aspects: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve aspects"
        )


@router.get(
    "/quarters/{quarter}",
    response_model=QuarterInfo,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid quarter format"}
    }
)
async def get_quarter_info(quarter: str):
    """Get information about a specific quarter.
    
    Args:
        quarter: Quarter string like "2025-Q3"
        
    Returns:
        QuarterInfo with quarter details and statistics
    """
    try:
        start_date, end_date = parse_quarter(quarter)
        current_quarter = get_current_quarter()
        
        # Calculate days
        total_days = (end_date - start_date).days
        days_remaining = None
        
        if quarter == current_quarter:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            if now < end_date:
                days_remaining = (end_date - now).days
            else:
                days_remaining = 0
        
        return QuarterInfo(
            quarter=quarter,
            start_date=start_date,
            end_date=end_date,
            current=(quarter == current_quarter),
            days_total=total_days,
            days_remaining=days_remaining
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid quarter format: {quarter}"
        )


@router.get(
    "/status",
    responses={
        200: {"description": "Astrology engine status"}
    }
)
async def get_astrology_status():
    """Get status of astrology engine and ephemeris data.
    
    Returns information about the Skyfield ephemeris and engine state.
    """
    try:
        engine = get_engine()
        
        status_info = {
            "ephemeris_file": engine.ephemeris_file,
            "initialized": engine._initialized,
            "planets_loaded": len(engine.bodies) if engine._initialized else 0,
            "current_quarter": get_current_quarter()
        }
        
        # Try to initialize if not already done
        if not engine._initialized:
            try:
                engine.initialize()
                status_info.update({
                    "initialized": True,
                    "planets_loaded": len(engine.bodies),
                    "initialization_status": "success"
                })
            except Exception as e:
                status_info["initialization_error"] = str(e)
        
        return JSONResponse(content=status_info)
        
    except Exception as e:
        logger.error(f"Failed to get astrology status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get astrology status"
        )


@router.delete(
    "/quarters/{quarter}",
    responses={
        200: {"description": "Aspects deleted successfully"},
        400: {"model": ErrorResponse, "description": "Invalid quarter format"}
    }
)
async def delete_quarter_aspects(quarter: str):
    """Delete all aspect events for a quarter.
    
    WARNING: This will permanently delete all computed aspects for the quarter.
    Use with caution.
    
    Args:
        quarter: Quarter string like "2025-Q3"
        
    Returns:
        Success message
    """
    try:
        # Validate quarter format
        parse_quarter(quarter)
        
        repo = get_repo()
        await repo.delete_aspect_events(quarter)
        
        return JSONResponse(
            content={
                "message": f"Deleted all aspects for quarter {quarter}",
                "quarter": quarter
            }
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid quarter format: {quarter}"
        )
    except Exception as e:
        logger.error(f"Failed to delete aspects: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete aspects"
        )